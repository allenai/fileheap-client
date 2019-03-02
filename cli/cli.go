package cli

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	auth "github.com/allenai/beaker-auth"
	"github.com/beaker/fileheap/client"
	jwt "github.com/dgrijalva/jwt-go"
	"github.com/pkg/errors"
)

const (
	addressEnv     = "FILEHEAP_ADDRESS"
	concurrencyEnv = "FILEHEAP_CONCURRENCY"
)

type CLI struct {
	client      *client.Client
	concurrency int
	ctx         context.Context
}

func (c *CLI) Client() *client.Client {
	return c.client
}

func NewCLI(ctx context.Context) (*CLI, error) {
	concurrency := 32
	if env := os.Getenv(concurrencyEnv); env != "" {
		var err error
		concurrency, err = strconv.Atoi(env)
		if err != nil {
			return nil, errors.Errorf("%s invalid: %s", concurrencyEnv, env)
		}
	}

	address := os.Getenv(addressEnv)
	if address == "" {
		return nil, errors.Errorf("%s not set", addressEnv)
	}

	signer := auth.NewSigner(&auth.KeyStoreTODO{
		KeyID: "temp",
		Key:   "not secure",
	})

	token, err := signer.NewToken(&auth.Claims{
		Scopes: []auth.Scope{
			{Permission: auth.Admin, Class: "dataset"},
			{Permission: auth.Admin, Class: "upload"},
		},
		StandardClaims: jwt.StandardClaims{
			Issuer:   "fh",
			IssuedAt: time.Now().Unix(),
		},
	})

	c, err := client.New(address, client.WithToken(token))
	if err != nil {
		return nil, err
	}

	return &CLI{
		client:      c,
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

func splitPath(s string) (dataset, path string, err error) {
	parts := strings.SplitN(s, "://", 2)
	if len(parts) == 1 {
		// This must be a file path.
		if len(s) == 0 {
			return "", "", errors.New("file paths may not be empty")
		}
		return "", s, nil
	}

	if scheme := parts[0]; scheme != "fh" {
		return "", "", errors.Errorf("%q is not a supported URL scheme; did you mean %q?", scheme, "fh://")
	}

	parts = strings.SplitN(parts[1], "/", 2)
	dataset = parts[0]
	if len(parts) > 1 {
		path = parts[1]
	}
	if len(dataset) == 0 {
		return "", "", errors.Errorf("invalid path %q, must include dataset", s)
	}
	return dataset, path, nil
}
