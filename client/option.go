package client

// Option allows a caller to configure additional options on a client.
type Option interface {
	Apply(c *Client)
}

// WithToken returns an Option which specifies a token to be used for authentication.
func WithToken(token string) Option {
	return withToken(token)
}

type withToken string

func (o withToken) Apply(c *Client) {
	c.token = string(o)
}
