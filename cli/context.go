package cli

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

func InterruptContext() context.Context {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-quit
		cancel()
	}()
	return ctx
}
