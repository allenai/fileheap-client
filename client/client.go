package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"time"

	"github.com/goware/urlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/allenai/bytefmt"
	"github.com/beaker/fileheap/api"
)

const userAgent = "fileheap/0.1.0"

const (
	// Maximum number of requests to send in a batch.
	batchSizeLimit = api.BatchSizeLimit

	// Maximum size of a request.
	requestSizeLimit = api.PutFileSizeLimit
)

// Client provides an API interface to FileHeap.
type Client struct {
	baseURL *url.URL
	token   string
	client  *http.Client
}

// New creates a new client connected the given address.
//
// Address should be in the form [scheme://]host[:port], where scheme defaults
// to "https" and port defaults to the standard port for the given scheme, i.e.
// 80 for http and 443 for https.
func New(address string, options ...Option) (*Client, error) {
	u, err := urlx.ParseWithDefaultScheme(address, "https")
	if err != nil {
		return nil, err
	}

	if u.Path != "" || u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return nil, errors.New("address must be base server address in the form [scheme://]host[:port]")
	}

	c := &Client{baseURL: u, client: &http.Client{Timeout: 5 * time.Minute}}
	for _, opt := range options {
		opt.Apply(c)
	}

	return c, nil
}

// BaseURL returns the base URL of the client.
func (c *Client) BaseURL() *url.URL {
	return &url.URL{
		Scheme: c.baseURL.Scheme,
		Host:   c.baseURL.Host,
	}
}

type tracedBody struct {
	body   io.ReadCloser
	result *TraceResult
	req    *http.Request
}

func (b *tracedBody) Close() error {
	logrus.
		WithFields(b.result.Fields()).
		WithField("ContentLength", bytefmt.New(b.req.ContentLength, bytefmt.Binary)).
		WithField("Method", b.req.Method).
		WithField("URL", b.req.URL.String()).
		Tracef("Completed FileHeap request")
	return b.body.Close()
}

func (b *tracedBody) Read(p []byte) (n int, err error) {
	return b.body.Read(p)
}

func (c *Client) do(ctx context.Context, req *http.Request) (*http.Response, error) {
	result := NewResult()
	resp, err := c.client.Do(req.WithContext(withClientTrace(ctx, result)))
	if err != nil {
		return nil, err
	}
	resp.Body = &tracedBody{body: resp.Body, result: result, req: req}
	return resp, nil
}

func (c *Client) newRequest(
	method string,
	path string,
	query url.Values,
	body io.Reader,
) (*http.Request, error) {
	u := c.baseURL.ResolveReference(&url.URL{Path: path, RawQuery: query.Encode()})
	req, err := http.NewRequest(method, u.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	return req, nil
}

// sendRequest sends a request with an optional JSON-encoded body and returns the response.
func (c *Client) sendRequest(
	ctx context.Context,
	method string,
	path string,
	query url.Values,
	body interface{},
) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		buf := getBuffer()
		defer putBuffer(buf)
		if err := json.NewEncoder(buf).Encode(body); err != nil {
			return nil, err
		}
		reader = buf
	}

	req, err := c.newRequest(method, path, query, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(ctx, req)
}

// errorFromResponse creates an error from an HTTP response, or nil on success.
func errorFromResponse(resp *http.Response) error {
	// Anything less than 400 isn't an error, so don't produce one.
	if resp.StatusCode < 400 {
		return nil
	}

	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "failed to read response")
	}

	var apiErr api.Error
	if err := json.Unmarshal(bytes, &apiErr); err != nil {
		return errors.Wrapf(err, "failed to parse response: %s", string(bytes))
	}

	return apiErr
}

// responseValue parses the response body and stores the result in the given value.
// The value parameter should be a pointer to the desired structure.
func parseResponse(resp *http.Response, value interface{}) error {
	if err := errorFromResponse(resp); err != nil {
		return err
	}

	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(bytes, value)
}

type TraceResult struct {
	Start                time.Time
	DNSStart             time.Time
	DNSDone              time.Time
	ConnectStart         time.Time
	ConnectDone          time.Time
	TLSHandshakeStart    time.Time
	TLSHandshakeDone     time.Time
	GotFirstResponseByte time.Time
}

func NewResult() *TraceResult {
	return &TraceResult{Start: time.Now()}
}

func (r *TraceResult) Fields() logrus.Fields {
	end := time.Now()
	return logrus.Fields{
		"DNS":       r.DNSDone.Sub(r.DNSStart).String(),
		"Connect":   r.ConnectDone.Sub(r.ConnectStart).String(),
		"TLS":       r.TLSHandshakeDone.Sub(r.TLSHandshakeStart).String(),
		"FirstByte": r.GotFirstResponseByte.Sub(r.Start).String(),
		"Total":     end.Sub(r.Start).String(),
	}
}

func withClientTrace(ctx context.Context, r *TraceResult) context.Context {
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		DNSStart: func(i httptrace.DNSStartInfo) {
			r.DNSStart = time.Now()
		},

		DNSDone: func(i httptrace.DNSDoneInfo) {
			r.DNSDone = time.Now()
		},

		ConnectStart: func(_, _ string) {
			r.ConnectStart = time.Now()
		},

		ConnectDone: func(network, addr string, err error) {
			r.ConnectDone = time.Now()
		},

		TLSHandshakeStart: func() {
			r.TLSHandshakeStart = time.Now()
		},

		TLSHandshakeDone: func(_ tls.ConnectionState, _ error) {
			r.TLSHandshakeDone = time.Now()
		},

		GotFirstResponseByte: func() {
			r.GotFirstResponseByte = time.Now()
		},
	})
}
