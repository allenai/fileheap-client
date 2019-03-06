package cli

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/beaker/fileheap/client"
)

type CLI struct {
	client      *client.Client
	concurrency int
	ctx         context.Context
}

func (c *CLI) Client() *client.Client {
	return c.client
}

func NewCLI(ctx context.Context, client *client.Client, concurrency int) (*CLI, error) {
	return &CLI{
		client:      client,
		concurrency: concurrency,
		ctx:         ctx,
	}, nil
}

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
