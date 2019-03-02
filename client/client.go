package client

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/beaker/fileheap/api"
	"github.com/goware/urlx"
	retryable "github.com/hashicorp/go-retryablehttp"
	"github.com/pkg/errors"
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

	c := &Client{baseURL: u}
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

func (c *Client) newRetryableRequest(
	method string,
	path string,
	query url.Values,
	body interface{},
) (*retryable.Request, error) {
	u := c.baseURL.ResolveReference(&url.URL{Path: path, RawQuery: query.Encode()})
	req, err := retryable.NewRequest(method, u.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	return req, nil
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
		b := &bytes.Buffer{}
		if err := json.NewEncoder(b).Encode(body); err != nil {
			return nil, err
		}
		reader = b
	}

	req, err := c.newRetryableRequest(method, path, query, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	return newRetryableClient(&http.Client{
		Timeout: 30 * time.Second,
	}).Do(req.WithContext(ctx))
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

func newRetryableClient(httpClient *http.Client) *retryable.Client {
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	return &retryable.Client{
		HTTPClient:   httpClient,
		Logger:       &errorLogger{Logger: log.New(os.Stderr, "", log.LstdFlags)},
		RetryWaitMin: 100 * time.Millisecond,
		RetryWaitMax: 30 * time.Second,
		RetryMax:     9,
		CheckRetry:   retryable.DefaultRetryPolicy,
		Backoff:      exponentialJitterBackoff,
		ErrorHandler: retryable.PassthroughErrorHandler,
	}
}

var random = rand.New(rand.NewSource(time.Now().UnixNano()))

// exponentialJitterBackoff implements exponential backoff with full jitter as described here:
// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
func exponentialJitterBackoff(
	minDuration, maxDuration time.Duration,
	attempt int,
	resp *http.Response,
) time.Duration {
	min := float64(minDuration)
	max := float64(maxDuration)

	backoff := min + math.Min(max-min, min*math.Exp2(float64(attempt)))*random.Float64()
	return time.Duration(backoff)
}

type errorLogger struct {
	Logger *log.Logger
}

func (l *errorLogger) Printf(template string, args ...interface{}) {
	if strings.HasPrefix(template, "[ERR]") {
		l.Logger.Printf(template, args...)
	}
}
