package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
)

func TestListenStopsWithTheContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := listen(ctx, "127.0.0.1:0", http.NotFoundHandler()); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestListenRefusesATakenAddress(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := listen(context.Background(), l.Addr().String(), http.NotFoundHandler()); err == nil {
		t.Fatal("listened on a taken address")
	}
}
