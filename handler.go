package wshttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const (
	wsStatusHeader = "x-wshttp-status"

	msgClientClose = "close"
)

var (
	errFailedUpgrade  = errors.New("wshttp: failed to upgrade to websocket")
	errInvalidHeaders = errors.New("wshttp: invalid headers")
)

func WrapHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Connection") == "Upgrade" && r.Header.Get("Upgrade") == "websocket" {
			c, err := websocket.Accept(w, r, nil)
			if err != nil {
				http.Error(w, errFailedUpgrade.Error(), http.StatusInternalServerError)
				return
			}
			//nolint:contextcheck // as recommended by websocket we don't use the http context
			if err := handleWS(h, r.URL.String(), c); err != nil {
				_ = c.CloseNow()
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		h.ServeHTTP(w, r)
	})
}

func handleWS(h http.Handler, url string, c *websocket.Conn) error {
	ctx := context.Background()
	var headers []string
	if err := wsjson.Read(ctx, c, &headers); err != nil {
		return fmt.Errorf("wshttp: read headers: %w", err)
	}
	if len(headers)%2 != 0 {
		return errInvalidHeaders
	}

	wsReqBody := &wsBody{ctx: ctx, conn: c}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, wsReqBody)
	httpReq.Proto = "HTTP/2.0"
	httpReq.ProtoMajor = 2
	httpReq.ProtoMinor = 0
	if err != nil {
		return fmt.Errorf("wshttp: create proxy request: %w", err)
	}
	for i := 0; i < len(headers); i += 2 {
		httpReq.Header.Add(headers[i], headers[i+1])
	}

	wsRes := &wsResponseWriter{ctx: ctx, conn: c, hdr: http.Header{}}

	h.ServeHTTP(wsRes, httpReq)

	return nil
}

type wsBody struct {
	ctx  context.Context
	conn *websocket.Conn

	pending io.Reader
}

// Read implements io.Reader.
func (w *wsBody) Read(p []byte) (int, error) {
	if w.pending == nil {
		mt, r, err := w.conn.Reader(w.ctx)
		if mt == websocket.MessageText {
			// Command message.
			msg, err := io.ReadAll(r)
			if err != nil {
				return 0, fmt.Errorf("wshttp: read command: %w", err)
			}
			if string(msg) == msgClientClose {
				return 0, io.EOF
			}
			return 0, fmt.Errorf("wshttp: unknown command: %s", msg)
		}
		if err != nil {
			if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
				return 0, io.EOF
			}
			return 0, fmt.Errorf("wshttp: initialize reader: %w", err)
		}
		w.pending = r
	}

	if w.pending == nil {
		return 0, io.EOF
	}

	n, err := w.pending.Read(p)
	if err != nil {
		if errors.Is(err, io.EOF) {
			// This message is complete but not the stream, so drop the error.
			w.pending = nil
			return n, nil
		}
		return n, fmt.Errorf("wshttp: read: %w", err)
	}
	return n, nil
}

func (w *wsBody) Close() error {
	return w.conn.Close(websocket.StatusNormalClosure, "")
}

type wsResponseWriter struct {
	ctx  context.Context
	conn *websocket.Conn
	hdr  http.Header

	hdrSent bool

	err error
}

// Header implements http.ResponseWriter.
func (w *wsResponseWriter) Header() http.Header {
	return w.hdr
}

// Write implements http.ResponseWriter.
func (w *wsResponseWriter) Write(buf []byte) (int, error) {
	if !w.hdrSent {
		w.WriteHeader(http.StatusOK)
		w.hdrSent = true
	}

	if w.err != nil {
		return 0, w.err
	}

	if err := w.conn.Write(w.ctx, websocket.MessageBinary, buf); err != nil {
		return 0, fmt.Errorf("wshttp: write: %w", err)
	}

	return len(buf), nil
}

// WriteHeader implements http.ResponseWriter.
func (w *wsResponseWriter) WriteHeader(statusCode int) {
	headers := make([]string, 0, len(w.hdr)*2+2)
	headers = append(headers, wsStatusHeader, strconv.Itoa(statusCode))
	for k, v := range w.hdr {
		for _, vv := range v {
			headers = append(headers, k, vv)
		}
	}
	if err := wsjson.Write(w.ctx, w.conn, headers); err != nil {
		w.err = fmt.Errorf("wshttp: write headers: %w", err)
	}
}

// Flush implements http.Flusher.
func (w *wsResponseWriter) Flush() {
	// We assume callers buffer internally so don't add yet another one here.
	// It means we effectively flush on every call to Write and don't need to do it
	// here. We implement flusher anyways for frameworks like connect that require
	// it.
}
