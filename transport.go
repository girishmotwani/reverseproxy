package reverseproxy

import (
    "fmt"
    "bufio"
    "crypto/tls"
    "errors"
    "io"
    "net"
    "net/http"
    "net/url"
    "strings"
    "sync"

)

var reqWriteExcludeHeader = map[string]bool{
    "Host":    true,
}

func readResponse(br *bufio.Reader, req *http.Request) (resp *http.Response, err error) {
    resp, err = http.ReadResponse(br, req)
    if err != nil {
        return nil, err
    }
    if resp.StatusCode == 100 {
        resp, err = http.ReadResponse(br, req)
        if err != nil {
            return nil, err
        }
    }
    return resp, err
}

type persistConn struct {
    t         *ProxyTransport
    conn      net.Conn
    br        *bufio.Reader
    bw        *bufio.Writer
    cacheKey  string
}

func (pc *persistConn) close() {
    pc.conn.Close()
}

type ProxyTransport struct {
    // pool of persistent connections. The connections are key'ed by the 
    // front-end client's address(ip:port). Also since the Std Library
    // HTTP server implementation serializes the processing of requests
    // from a client in a go-routine, a backend connection is 
    // guaranteed to have no contention.
    // Further, the ConnState Hook is invoked in the context of this 
    // goroutine, so cleanup is relatively simpler. 
    // In the event of a backend connection error, close the connection.
    // A future request would result in a new connection being setup and
    // the system recovering.
    idleMu    sync.Mutex
    idleConns map[string]*persistConn
}

// Called by the proxy to notify that the frontend
// client has closed its connection.
// The transport implemenation can use this to cleanup
// its state, if any
func (t *ProxyTransport) ClientClose(remote string) {

    fmt.Printf("Transport: Received notification to close conn %s\n", remote)

    // We dont return an error if there is no backend connection, since
    // a. the connection is setup on the first request and the client
    //    can close before sending out a request.
    // b. the backend connection may have closed due to error.. in this case
    //    again, there is no idleConn sitting around.
    t.idleMu.Lock()
    defer t.idleMu.Unlock()
    if pconn, ok := t.idleConns[remote]; ok {
        pconn.close()
        delete(t.idleConns, remote)
        return
    }
    return
}

func (t *ProxyTransport) ReadResponse(pconn *persistConn, req *http.Request) (resp *http.Response, err error) {
    resp, err = readResponse(pconn.br, req)
    if err != nil {
        fmt.Printf("transport: Error reading response from server %s\n", err)
        pconn.close()
        return nil, err
    }
    return resp, nil
}

// return the connection back to the pool.
func (t *ProxyTransport) PutConnection(pconn *persistConn) error {
    t.idleMu.Lock()
    defer t.idleMu.Unlock()

    key := pconn.cacheKey
    t.idleConns[key] = pconn
    return nil
}

type responseAndError struct {
	res *http.Response
	err error
}

type connCloser struct {
	io.ReadCloser
	conn net.Conn
}

func (this *connCloser) Close() error {
	this.conn.Close()
	return this.ReadCloser.Close()
}

// canonicalAddr returns url.Host but always with a ":port" suffix
func canonicalAddr(url *url.URL) string {
	addr := url.Host

	if !hasPort(addr) {
		if url.Scheme == "http" {
			return addr + ":80"
		} else {
			return addr + ":443"
		}
	}

	return addr
}


func (t *ProxyTransport) dial(req *http.Request) (net.Conn, error) {
	targetAddr := canonicalAddr(req.URL)

	c, err := net.Dial("tcp", targetAddr)

	if err != nil {
		return c, err
	}

	if req.URL.Scheme == "https" {
		c = tls.Client(c, &tls.Config{ServerName: req.URL.Host})

		if err = c.(*tls.Conn).Handshake(); err != nil {
			return nil, err
		}

		if err = c.(*tls.Conn).VerifyHostname(req.URL.Host); err != nil {
			return nil, err
		}
	}

	return c, nil
}

func hasPort(s string) bool {
    return strings.LastIndex(s, ":") > strings.LastIndex(s, "]")
}

func (t *ProxyTransport) GetConnection(req *http.Request) (*persistConn, error) {
    t.idleMu.Lock()
    defer t.idleMu.Unlock()

    if t.idleConns == nil {
        t.idleConns = make(map[string]*persistConn)
    }
    key := req.RemoteAddr
    if pconn, ok := t.idleConns[key]; ok {
        delete(t.idleConns, key)
        return pconn, nil
    }
    pconn := &persistConn{
        t:            t,
        cacheKey:     req.RemoteAddr,
    }
    conn, err := t.dial(req)
    if err != nil {
        return nil, err
    }
    pconn.conn = conn
    pconn.br = bufio.NewReader(pconn.conn)
    pconn.bw = bufio.NewWriter(pconn.conn)

    return pconn, nil
}

func (t *ProxyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
    if req.URL == nil {
        return nil, errors.New("http: nil Request.URL")
    }
    if  req.Header == nil {
        return nil, errors.New("http: nil Request Header")
    }
    if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
        return nil, errors.New("http: unsupported protocol scheme")
    }
    if req.URL.Host == "" {
        return nil, errors.New("http: no Host in request URL")
    }

    conn, err := t.dial(req)
    if err != nil {
        return nil, err
    }
    reader := bufio.NewReader(conn)
    writer := bufio.NewWriter(conn)
    readDone := make(chan responseAndError, 1)
    writeDone := make(chan error, 1)

    //Write the request
    go func() {
        fmt.Printf("Writing request out to connection\n")
        err := req.Write(writer)
        if err == nil {
            writer.Flush()
        }
        writeDone <- err
    }()

    // Read the response
    go func() {
        fmt.Printf("Waiting for response from server\n")
        resp, err := readResponse(reader, req)
        if err != nil {
            readDone <- responseAndError{nil, err}
            return
        }
        resp.Body =&connCloser{resp.Body, conn}
        readDone <- responseAndError{resp, nil}
    }()

    fmt.Printf("Received write_done for request\n")
    if err = <-writeDone; err != nil {
        return nil, err
    }
    r := <-readDone
    if r.err != nil {
        return nil, r.err
    }
    fmt.Printf("Received read done for response\n")
    return r.res, nil
}

func (t *ProxyTransport) Write(pconn *persistConn, req *http.Request, src []byte) (int, error) {
    w := pconn.bw
    nw, err := w.Write(src)
	if err != nil {
        pconn.close()
		return nw, err
	}
    return nw, nil
}

func (t *ProxyTransport) writeHeader(w *bufio.Writer, req *http.Request) error {
    host := req.Host
    if host == "" {
        if req.URL == nil {
            return ProxyErrMissingHost
        }
        host = req.URL.Host
    }

    ruri := req.URL.RequestURI()
    _, err := fmt.Fprintf(w, "%s %s HTTP/1.1\r\n", req.Method, ruri)
    if err != nil {
        return err
    }

    // Header lines
    _, err = fmt.Fprintf(w, "Host: %s\r\n", host)
    if err != nil {
        return err
    }

    h := req.Header.Get("Host")
    if h != "" {
        fmt.Printf("There is a host header with value %s\n", h)
    }
    // Since this is a proxy, assuming the incoming request should have
    // headers in correct format.
    err = req.Header.WriteSubset(w, reqWriteExcludeHeader)
    if err != nil {
        return err
    }

    _, err = io.WriteString(w, "\r\n")
    if err != nil {
        return err
    }

    // Flush only if there is no body in this request
    if req.ContentLength == 0 && req.Body == nil {
        return w.Flush()
    }
    return nil
}

var ProxyErrMissingHost = errors.New("proxy: Request with no Host or URL set")

func (t *ProxyTransport) WriteHeader(pconn *persistConn, req *http.Request) error {

    err := t.writeHeader(pconn.bw, req)
	if err != nil {
        pconn.close()
		return err
	}
    return nil
}

