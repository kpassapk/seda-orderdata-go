package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"net/http"
	"net/url"

	stderrs "errors"
)

var templatesApiError = stderrs.New("templates API error")
var executionsApiError = stderrs.New("executions API error")

type executionsClientConfig struct {
	endpoint string
	token    string
}

type ExecutionsClientOption func(*executionsClientConfig) error

func WithExecutionsEndpoint(endpoint string) ExecutionsClientOption {
	return func(c *executionsClientConfig) error {
		if err := validateEndpoint(endpoint); err != nil {
			return err
		}
		c.endpoint = endpoint
		return nil
	}
}

func validateEndpoint(endpoint string) error {
	_, err := url.Parse(endpoint)
	if err != nil {
		return errors.WithMessagef(err, "%q is not a valid URL", endpoint)
	}
	return nil
}

func defaultExecutionsClientOptions() *executionsClientConfig {
	return &executionsClientConfig{}
}

// ExecutionsClient is a client for the integrations executions service
type ExecutionsClient struct {
	http     *http.Client
	endpoint string
	token    string
}

// NewExecutionsClient creates a new executions client
func NewExecutionsClient(h *http.Client, opts ...ExecutionsClientOption) (*ExecutionsClient, error) {
	cfg := defaultExecutionsClientOptions()
	var err error
	for _, opt := range opts {
		err = opt(cfg)
		if err != nil {
			return nil, errors.WithMessage(err, "could not create executions client")
		}
	}
	return &ExecutionsClient{
		http:     h,
		endpoint: cfg.endpoint,
		token:    cfg.token,
	}, nil
}

func WithExecutionsToken(token string) ExecutionsClientOption {
	return func(c *executionsClientConfig) error {
		c.token = token
		return nil
	}
}

// CreateExecution creates a new execution in the executions service and returns the execution ID.
func (c *ExecutionsClient) CreateExecution(_ context.Context, flow []byte) (string, error) {
	reader := bytes.NewReader(flow)
	path := "executions"
	endpoint, err := url.JoinPath(c.endpoint, path)
	if err != nil {
		return "", errors.WithMessagef(err, "could not create full path with %q and %q", c.endpoint, path)
	}

	req, err := http.NewRequest("POST", endpoint, reader)
	if err != nil {
		return "", err
	}

	req.Header.Add("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", c.token))
	}

	res, err := c.http.Do(req)
	if err != nil {
		return "", errors.WithMessage(err, "could not create execution")
	}
	if res.StatusCode != http.StatusAccepted {
		return "", errors.WithMessagef(executionsApiError, "POST %s returned %d", endpoint, res.StatusCode)
	}
	type CreateExecutionResponse struct {
		ExecutionId string `json:"executionId"`
	}

	var e CreateExecutionResponse
	bt, err := io.ReadAll(res.Body)
	err = json.Unmarshal(bt, &e)
	if err != nil {
		return "", errors.WithMessage(err, "could not deserialize execution body as JSON")
	}
	return e.ExecutionId, nil
}

type templatesClientConfig struct {
	endpoint string
	token    string
}

type TemplatesClientOption func(*templatesClientConfig) error

func WithTemplatesEndpoint(endpoint string) TemplatesClientOption {
	return func(c *templatesClientConfig) error {
		if err := validateEndpoint(endpoint); err != nil {
			return err
		}
		c.endpoint = endpoint
		return nil
	}
}

func WithTemplatesToken(token string) TemplatesClientOption {
	return func(c *templatesClientConfig) error {
		c.token = token
		return nil
	}
}

func defaultTemplatesClientOptions() *templatesClientConfig {
	return &templatesClientConfig{}
}

type TemplatesClient struct {
	http     *http.Client
	endpoint string
	token    string
}

func NewTemplatesClient(h *http.Client, opts ...TemplatesClientOption) (*TemplatesClient, error) {
	cfg := defaultTemplatesClientOptions()
	var err error
	for _, opt := range opts {
		if err = opt(cfg); err != nil {
			return nil, errors.WithMessage(err, "could not create templates client")
		}
	}
	return &TemplatesClient{
		http:     h,
		endpoint: cfg.endpoint,
		token:    cfg.token,
	}, nil
}

func (c *TemplatesClient) RenderTemplate(_ context.Context, name string, args map[string]string) ([]byte, error) {
	path := "templates/%s/render"

	endpoint, err := url.JoinPath(c.endpoint, fmt.Sprintf(path, name))

	b, _ := json.Marshal(args)
	// We should e able to marshal every map[string]string

	br := bytes.NewReader(b)

	req, err := http.NewRequest("POST", endpoint, br)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Add("Authorization", "Bearer "+c.token)
	}

	r, err := c.http.Do(req)
	if err != nil {
		return nil, errors.WithMessagef(err, "could not get template %q", name)
	}
	if r.StatusCode != http.StatusOK {
		return nil, errors.WithMessagef(templatesApiError, "got status code %d while attempting to render tempalte %q", r.StatusCode, name)
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, errors.WithMessagef(err, "could not read template %q response body", name)
	}
	return body, nil
}
