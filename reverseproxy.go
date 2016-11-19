package reverseproxy

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/oxtoacart/bpool"
	"github.com/pborman/uuid"
)

// HTTP proxy application
// This interface encapsulates the methods that the HTTP
// processing application needs to implement to
// use the reverse proxy service
// - a handler to process HTTP Requests
// - a handler to process HTTP Responses
type HttpApplication interface {
	RequestHandler(flow *HttpFlow) error
	ResponseHandler(flow *HttpFlow) error
	RequestDataHandler(flow *HttpFlow, buf []byte) ([]byte, error)
	ResponseDataHandler(resp *HttpFlow, buf []byte) ([]byte, error)
}

// ErrResponseShortWrite means that a write accepted fewer bytes than requested
// but failed to return an explicit error.
var ErrResponseShortWrite = errors.New("short write")

type HttpFlow struct {
	Id       string
	Request  *http.Request
	Response *http.Response
}

func Uuid() string {
	return uuid.New()
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

	// ErrorLog specifies an optional logger for errors that occur when
	// attempting to process the request. If nil, logging goes to os.Stderr
	// using the standard logger
	ErrorLog *log.Logger

	// BufferPool specifies a buffer pool to get byte slices for use by
	// io.CopyBuffer when copying HTTP request and response bodies
	BufferPool *bpool.BytePool

	// The application that is processes the HTTP data as it is
	// streamed to/from the server being proxied
	app HttpApplication
}

func NewHttpReverseProxy(target *url.URL, app HttpApplication) (*HttpReverseProxy, error) {

	pool := bpool.NewBytePool(20, 524288)
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
		Director:   director,
		Transport:  &ProxyTransport{},
		BufferPool: pool,
		app:        app,
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
	fmt.Printf("Starting Reverse Proxy service on port 8080\n")
	//https://husobee.github.io/golang/tls/2016/01/27/golang-tls.html
	connStateHandler := func(c net.Conn, state http.ConnState) {
		// we are interested only in closed connections.
		// On a conn close, cleanup the corresponding backend connection
		// to the Server.
		if state == http.StateClosed {
			remote := c.RemoteAddr().String()
			transport := p.Transport

			transport.ClientClose(remote)
		}
	}
	server := &http.Server{
		Addr:      ":8080",
		ConnState: connStateHandler,
		Handler:   p,
	}
	server.ListenAndServe()
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

func (p *HttpReverseProxy) processRequest(flow *HttpFlow) error {
	transport := p.Transport

    req := flow.Request
	//first get the connection object
	conn, err := transport.GetConnection(req)
	if err != nil {
		fmt.Printf("proxy: Failed to get a connection to the backend server: %s\n", err)
		return err
	}

	err = transport.WriteHeader(conn, req)
	if err != nil {
		fmt.Printf("proxy: Failed to send Request Headers to backend: %s\n", err)
		return err
	}

	written := 0
	// write the body out, if there is one
	if req.Body != nil {
		src := req.Body
		fmt.Printf("Content Length is %d\n", req.Header.Get("Content-Length"))

		var buf []byte
		for {
			buf := p.BufferPool.Get()
			nr, err := io.ReadFull(src, buf)//.Read(buf)
			if nr > 0 {
                buf, err = p.app.RequestDataHandler(flow, buf[0:nr])
                nr = len(buf)
				nw, err := transport.Write(conn, req, buf[0:nr])
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
		p.BufferPool.Put(buf)
	}
	flow.Response, err = transport.ReadResponse(conn, req)
	transport.PutConnection(conn)
	return err
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
	// create the flow object
	flow := &HttpFlow{
		Id:       Uuid(),
		Request:  outreq,
		Response: nil,
	}
	//now call the app handlers if registered
	p.app.RequestHandler(flow)

	//send this request on its way out
	err := p.processRequest(flow)
	if err != nil {
		p.logf("http: proxy error: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer flow.Response.Body.Close()

	for _, h := range hopHeaders {
		flow.Response.Header.Del(h)
	}

	//now call the app handlers if registered
	p.app.ResponseHandler(flow)

	copyHeader(rw.Header(), flow.Response.Header)

	rw.WriteHeader(flow.Response.StatusCode)
	p.copyResponse(rw, flow)
}

func (p *HttpReverseProxy) copyResponse(dst io.Writer, flow *HttpFlow) (written int64, err error) {
	src := flow.Response.Body

	var buf []byte
	for {
		buf = p.BufferPool.Get()
		nr, er := io.ReadFull(src, buf)//.Read(buf)
		if nr > 0 {
			// invoke the application callback, if registered
			// app may modify the request and return a different
			// buffer. Also buffer here needs to be larger than
			// the data being read, so that app handler can
			// modify contents and return update buffer.
            buf, err = p.app.ResponseDataHandler(flow, buf[0:nr])
            nr = len(buf)
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
			fmt.Printf("Written %d bytes\n", written)
		}
		if er == io.EOF {
			break
		}
		if er != nil {
			err = er
			break
		}
	}
	p.BufferPool.Put(buf)
	return written, err
}

func (p *HttpReverseProxy) logf(format string, args ...interface{}) {
	if p.ErrorLog != nil {
		p.ErrorLog.Printf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}
