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

func RoundTripper() http.RoundTripper {
	return wsRoundTripper{}
}

type wsRoundTripper struct{}

// RoundTrip implements http.RoundTripper.
func (w wsRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := context.Background()

	conn, _, err := websocket.Dial(ctx, req.URL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("wshttp: dial: %w", err)
	}

	var reqHdrs []string
	for k, v := range req.Header {
		for _, vv := range v {
			reqHdrs = append(reqHdrs, k, vv)
		}
	}
	if err := wsjson.Write(ctx, conn, reqHdrs); err != nil {
		return nil, fmt.Errorf("wshttp: write request headers: %w", err)
	}

	httpRes := &http.Response{
		Proto:         "HTTP/2.0",
		ProtoMajor:    2,
		ContentLength: -1,
		Header:        make(http.Header),
		Request:       req,
	}

	if req.Body != nil {
		go func() {
			var buf [4096]byte
			for {
				n, err := req.Body.Read(buf[:])
				if err != nil {
					if errors.Is(err, io.EOF) {
						if err := conn.Write(ctx, websocket.MessageText, []byte(msgClientClose)); err != nil {
							conn.Close(websocket.StatusInternalError, err.Error())
						}
						return
					}
					conn.Close(websocket.StatusInternalError, err.Error())
					return
				}
				if err := conn.Write(ctx, websocket.MessageBinary, buf[:n]); err != nil {
					conn.Close(websocket.StatusInternalError, err.Error())
					return
				}
			}
		}()
	}

	var resHdrs []string
	if err := wsjson.Read(ctx, conn, &resHdrs); err != nil {
		return nil, fmt.Errorf("wshttp: read response headers: %w", err)
	}
	if len(resHdrs)%2 != 0 {
		return nil, errInvalidHeaders
	}

	for i := 0; i < len(resHdrs); i += 2 {
		key, val := resHdrs[i], resHdrs[i+1]
		if key == wsStatusHeader {
			code, err := strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("wshttp: invalid status: %w", err)
			}
			httpRes.StatusCode = code
			httpRes.Status = http.StatusText(code)
			continue
		}
		httpRes.Header.Add(key, val)
	}

	httpRes.Body = &wsBody{ctx: ctx, conn: conn}

	return httpRes, nil
}
