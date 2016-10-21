package reverseproxy

import (
    "io"
    "errors"
    "fmt"
    "log"
    "net/http"
    "net/url"
    "strings"
    "sync"
    "time"

    "github.com/oxtoacart/bpool"
)

// HTTP proxy application
// This interface encapsulates the methods that the HTTP 
// processing application needs to implement to 
// use the reverse proxy service
// - a handler to process HTTP Requests
// - a handler to process HTTP Responses
type HttpApplication interface {
    RequestHandler(req *http.Request) error
    ResponseHandler(resp *http.Response) error
}

// onExitFlushLoop is a callback set by tests to detect the state of the
// flushLoop() goroutine.
var onExitFlushLoop func()

// ErrResponseShortWrite means that a write accepted fewer bytes than requested
// but failed to return an explicit error.
var ErrResponseShortWrite = errors.New("short write")

// A BufferPool is an interface for getting and returing temporary
// byte slices for use by io.CopyBuffer.
type BufferPool struct {
    size int
    buf  []byte
}

func (pool *BufferPool) Get() []byte {
    buffer := pool.buf
    pool.buf = nil
    return buffer
}

func (pool *BufferPool) Put(v []byte) {
    if pool.buf != nil {
        fmt.Printf("Buffer already given out... should be nil")
        return
    }
    pool.buf = v
}

/*
 *  HTTP reverse proxy using the standard library reverse proxy 
 *  
 *    target: 
 */
type HttpReverseProxy struct {
    // Director must be a function that modified the request into a new
    // request to be sent using Transport. Its response is then copied
    // back to the client
    Director func(*http.Request)

    // The transport used to perform proxy requests.
    // If nil, http.DefaultTransport is used
    Transport *ProxyTransport

    // FlushInterval specifies the flush interval to flush to the client
    // while copying the response body. 
    // If zero, no periodic flushing is done 
    FlushInterval time.Duration

    // ErrorLog specifies an optional logger for errors that occur when
    // attempting to process the request. If nil, logging goes to os.Stderr
    // using the standard logger
    ErrorLog *log.Logger

    // BufferPool specifies a buffer pool to get byte slices for use by
    // io.CopyBuffer when copying HTTP request and response bodies
    BufferPool *BufferPool

    // The application that is processes the HTTP data as it is
    // streamed to/from the server being proxied
    app    HttpApplication
}


func NewHttpReverseProxy(target *url.URL, app HttpApplication) (*HttpReverseProxy, error) {

    pool := bpool.BytePool(20, 524288)
    director := func(req *http.Request) {
        targetQuery := target.RawQuery
        req.URL.Scheme = target.Scheme
        req.URL.Host = target.Host
        if targetQuery == "" || req.URL.RawQuery == "" {
            req.URL.RawQuery = targetQuery + req.URL.RawQuery
        } else {
            req.URL.RawQuery = targetQuery + "&" + req.URL.RawQuery
        }
        //update the host header in the request
        req.Host = target.Host
    }

    return &HttpReverseProxy{
        Director: director,
        Transport: &ProxyTransport{},
        BufferPool: pool,
        app: app,
    }, nil
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func (p *HttpReverseProxy) Start() error {
    fmt.Printf("Starting Object Store service on port 8080\n");
    http.ListenAndServe(":8080", p)
    return nil
}

// Hop-by-hop headers. These are removed when sent to the backend.
// http://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html
var hopHeaders = []string{
	"Connection",
	"Proxy-Connection", // non-standard but still sent by libcurl and rejected by e.g. google
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",      // canonicalized version of "TE"
	"Trailer", // not Trailers per URL above; http://www.rfc-editor.org/errata_search.php?eid=4522
	"Transfer-Encoding",
	"Upgrade",
}

func (p *HttpReverseProxy) processRequest(req *http.Request) (*http.Response, error) {
    transport := p.Transport

    err := transport.WriteHeader(req)
    if err != nil {
        fmt.Printf("proxy: Failed to send Request Headers to backend: %s\n", err)
        return nil, err
    }

    written := 0
    // write the body out, if there is one
    if req.Body != nil {
        src := req.Body
        fmt.Printf("Content Length is %d\n", req.Header.Get("Content-Length"))

        var buf []byte
        if p.BufferPool != nil {
            buf = p.BufferPool.Get()
        }
        for {
            nr, err := src.Read(buf)
            if nr > 0 {
                nw, err := transport.Write(req, buf[0:nr])
                if err != nil {
                    fmt.Printf("Error: Writing request body\n")
                    break
                }
                if nw != nr {
                    fmt.Printf("Error: ShortRequestWrite\n")
                    break
                }
                written += nw
                fmt.Printf("Written %d bytes\n", written)
            }
            if err == io.EOF {
                break
            }
        }
        if p.BufferPool != nil {
            p.BufferPool.Put(buf)
        }
    }
    resp, err := transport.ReadResponse(req)
    return resp, err
}

func (p *HttpReverseProxy) ServeHTTP(rw http.ResponseWriter, req *http.Request) {

	outreq := new(http.Request)
	*outreq = *req // includes shallow copies of maps, but okay
    if req.ContentLength == 0 {
        outreq.Body = nil
    }

	p.Director(outreq)
	outreq.Proto = "HTTP/1.1"
	outreq.ProtoMajor = 1
	outreq.ProtoMinor = 1
	outreq.Close = false

    // Remove hop-by-hop headers listed in the "Connection" header.
	// See RFC 2616, section 14.10.
	copiedHeaders := false
	if c := outreq.Header.Get("Connection"); c != "" {
		for _, f := range strings.Split(c, ",") {
			if f = strings.TrimSpace(f); f != "" {
				if !copiedHeaders {
					outreq.Header = make(http.Header)
					copyHeader(outreq.Header, req.Header)
					copiedHeaders = true
				}
				outreq.Header.Del(f)
			}
		}
	}
	// Remove hop-by-hop headers to the backend.  Especially
	// important is "Connection" because we want a persistent
	// connection, regardless of what the client sent to us.  This
	// is modifying the same underlying map from req (shallow
	// copied above) so we only copy it if necessary.
	for _, h := range hopHeaders {
		if outreq.Header.Get(h) != "" {
			if !copiedHeaders {
				outreq.Header = make(http.Header)
				copyHeader(outreq.Header, req.Header)
				copiedHeaders = true
			}
			outreq.Header.Del(h)
		}
	}

    //now call the app handlers if registered
    //p.app.RequestHandler(outreq)

    //send this request on its way out
    res, err := p.processRequest(outreq)

    //res, err := transport.RoundTrip(outreq)
	if err != nil {
        p.logf("http: proxy error: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer res.Body.Close()

	for _, h := range hopHeaders {
		res.Header.Del(h)
	}

    //now call the app handlers if registered
    p.app.ResponseHandler(res)

	copyHeader(rw.Header(), res.Header)

	rw.WriteHeader(res.StatusCode)
	p.copyResponse(rw, res.Body)
}

func (p *HttpReverseProxy) copyResponse(dst io.Writer, src io.Reader) (written int64, err error) {
	if p.FlushInterval != 0 {
		if wf, ok := dst.(writeFlusher); ok {
			mlw := &maxLatencyWriter{
				dst:     wf,
				latency: p.FlushInterval,
				done:    make(chan bool),
			}
			go mlw.flushLoop()
			defer mlw.stop()
			dst = mlw
		}
	}
    var buf []byte
    if p.BufferPool != nil {
        buf = p.BufferPool.Get()
    }
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
            // invoke the application callback, if registered
            // app may modify the request and return a different
            // buffer. Also buffer here needs to be larger than 
            // the data being read, so that app handler can
            // modify contents and return update buffer.
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = ErrResponseShortWrite
				break
			}
		}
		if er == io.EOF {
			break
		}
		if er != nil {
			err = er
			break
		}
	}
    if p.BufferPool != nil {
        p.BufferPool.Put(buf)
    }
    return written, err
}

func (p *HttpReverseProxy) logf(format string, args ...interface{}) {
	if p.ErrorLog != nil {
		p.ErrorLog.Printf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

type writeFlusher interface {
	io.Writer
	http.Flusher
}

type maxLatencyWriter struct {
	dst     writeFlusher
	latency time.Duration

	lk   sync.Mutex // protects Write + Flush
	done chan bool
}

func (m *maxLatencyWriter) Write(p []byte) (int, error) {
	m.lk.Lock()
	defer m.lk.Unlock()
	return m.dst.Write(p)
}

func (m *maxLatencyWriter) flushLoop() {
	t := time.NewTicker(m.latency)
	defer t.Stop()
	for {
		select {
		case <-m.done:
			if onExitFlushLoop != nil {
				onExitFlushLoop()
			}
			return
		case <-t.C:
			m.lk.Lock()
			m.dst.Flush()
			m.lk.Unlock()
		}
	}
}

func (m *maxLatencyWriter) stop() {
    m.done <- true
}
