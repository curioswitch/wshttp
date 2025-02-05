package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"connectrpc.com/connect"

	"github.com/curioswitch/wshttp"
	"github.com/curioswitch/wshttp/integrationtest/connectrpc/gen"
	"github.com/curioswitch/wshttp/integrationtest/connectrpc/gen/genconnect"
)

type pingService struct {
	genconnect.UnimplementedPingServiceHandler
}

func (pingService) CumSum(_ context.Context, stream *connect.BidiStream[gen.CumSumRequest, gen.CumSumResponse]) error {
	var sum int64
	for {
		msg, err := stream.Receive()
		if errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
		sum += msg.GetNumber()
		if err := stream.Send(&gen.CumSumResponse{Sum: sum}); err != nil {
			return err
		}
	}
}

func main() {
	mux := http.NewServeMux()
	mux.Handle(genconnect.NewPingServiceHandler(pingService{}, connect.WithCompression("gzip", nil, nil)))
	srv := &http.Server{
		Addr:              ":8080",
		Handler:           wshttp.WrapHandler(mux),
		ReadHeaderTimeout: 10 * time.Second,
	}
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
}
