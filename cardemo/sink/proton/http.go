package proton

import (
	"net/http"
	"net/url"
	"time"
)

type httpOptions struct {
	maxIdleConns        int
	maxIdleConnsPerHost int
	maxConnsPerHost     int
	idleConnTimeout     time.Duration
	connTimeout         time.Duration
}
type HTTPOption func(ops *httpOptions)

func WithMaxIdleConns(n int) HTTPOption {
	return func(ops *httpOptions) {
		ops.maxIdleConns = n
	}
}

func WithMaxIdleConnsPerHost(n int) HTTPOption {
	return func(ops *httpOptions) {
		ops.maxIdleConnsPerHost = n
	}
}

func WithMaxConnsPerHost(n int) HTTPOption {
	return func(ops *httpOptions) {
		ops.maxConnsPerHost = n
	}
}

func WithIdleConnTimeout(n time.Duration) HTTPOption {
	return func(ops *httpOptions) {
		ops.idleConnTimeout = n
	}
}

func WithConnTimeout(n time.Duration) HTTPOption {
	return func(ops *httpOptions) {
		ops.connTimeout = n
	}
}

func newHTTPClient(options ...HTTPOption) *http.Client {
	ops := httpOptions{}
	for _, o := range options {
		o(&ops)
	}

	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = ops.maxIdleConns
	t.MaxIdleConnsPerHost = ops.maxIdleConnsPerHost
	t.MaxConnsPerHost = ops.maxConnsPerHost
	t.IdleConnTimeout = ops.idleConnTimeout

	return &http.Client{
		Timeout:   ops.connTimeout,
		Transport: t,
	}
}

const retries = 2

func retryOnBrokenIdleConnection(action func() error) (err error) {
	for i := 0; i < retries; i++ {
		err = action()
		if err == nil {
			break
		}
		if err, ok := err.(*url.Error); ok {
			msg := err.Err.Error()
			// only retry "EOF" which (usually) means the Proton connection can't be used any more
			if msg == "EOF" || msg == "http: server closed idle connection" {
				continue
			}
		}
		break
	}
	return err
}
