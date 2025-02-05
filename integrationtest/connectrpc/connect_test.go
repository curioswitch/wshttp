package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/curioswitch/wshttp"
	"github.com/curioswitch/wshttp/integrationtest/connectrpc/gen"
	"github.com/curioswitch/wshttp/integrationtest/connectrpc/gen/genconnect"
)

func TestConnectRPC(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle(genconnect.NewPingServiceHandler(pingService{}, connect.WithCompression("gzip", nil, nil)))
	srv := httptest.NewServer(wshttp.WrapHandler(mux))
	defer srv.Close()

	ctx := context.Background()
	httpClient := &http.Client{
		Transport: wshttp.RoundTripper(),
	}
	client := genconnect.NewPingServiceClient(httpClient, fmt.Sprintf("http://%s", srv.Listener.Addr()))
	stream := client.CumSum(ctx)
	sum := 0
	for i := 1; i < 100; i++ {
		sum += i
		require.NoError(t, stream.Send(&gen.CumSumRequest{Number: int64(i)}))
		res, err := stream.Receive()
		require.NoError(t, err)
		require.Equal(t, int64(sum), res.GetSum())
	}
	require.NoError(t, stream.CloseRequest())
	_, err := stream.Receive()
	require.ErrorIs(t, err, io.EOF)
}
