// Package robotocore aims aws-sdk-go-v2 clients at a running robotocore
// twin.
//
// Thin wrapper: language-agnostic behavior lives server-side; this
// package only wires credentials/endpoints. Sits alongside the Python,
// TypeScript, and Rust bindings under bindings/, mirroring
// ../python/robotocore_client.
//
// Usage:
//
//	c, err := robotocore.NewSession("sqs") // talks to http://127.0.0.1:4566
//	sqs := c.(*sqs.Client)
package robotocore

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

const (
	// DefaultEndpoint is the twin URL used when neither the argument nor
	// ROBOTOCORE_ENDPOINT is set.
	DefaultEndpoint = "http://127.0.0.1:4566"
	// DefaultAccount is the account id used when neither the argument
	// nor ROBOTOCORE_ACCOUNT is set.
	DefaultAccount = "123456789012"
)

// Endpoint resolves the twin URL: override, ROBOTOCORE_ENDPOINT, then
// DefaultEndpoint; trailing slashes trimmed (empty override means unset).
func Endpoint(override string) string {
	url := override
	if url == "" {
		url = os.Getenv("ROBOTOCORE_ENDPOINT")
	}
	if url == "" {
		url = DefaultEndpoint
	}
	return strings.TrimRight(url, "/")
}

// Credentials returns the (accessKey, secret) pair for an account; a
// 12-digit access key IS the account id, so it is zero-padded to 12
// digits. Empty account falls back to ROBOTOCORE_ACCOUNT, then
// DefaultAccount. The secret is the static "test" the twin accepts.
func Credentials(account string) (key, secret string) {
	if account == "" {
		account = os.Getenv("ROBOTOCORE_ACCOUNT")
	}
	if account == "" {
		account = DefaultAccount
	}
	if len(account) < 12 {
		account = strings.Repeat("0", 12-len(account)) + account
	}
	return account, "test"
}

// Session pins one account/region on the twin. The zero value resolves
// like Credentials and Endpoint do (env, then defaults).
type Session struct {
	// Endpoint overrides the twin URL; empty resolves via Endpoint().
	Endpoint string
	// Region is the AWS region; empty means "us-east-1".
	Region string
	// Account is the 12-digit account id; empty resolves via Credentials().
	Account string
}

// NewSession returns an aws-sdk-go-v2 client for service ("s3" or "sqs")
// aimed at the twin the way Endpoint and Credentials resolve them.
// Unknown service names return an error — add the matching
// aws-sdk-go-v2/service module and an arm below to extend. Callers type
// -assert the result, e.g. NewSession("sqs").(*sqs.Client).
func NewSession(service string) (any, error) {
	return (&Session{}).Client(service)
}

// Config builds an aws.Config aimed at the session's account/region.
func (s *Session) Config(ctx context.Context) (aws.Config, error) {
	region := s.Region
	if region == "" {
		region = "us-east-1"
	}
	key, secret := Credentials(s.Account)
	return config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithBaseEndpoint(Endpoint(s.Endpoint)),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, "")),
	)
}

// Client returns the service client for the session ("s3" or "sqs").
func (s *Session) Client(service string) (any, error) {
	ctx := context.Background()
	cfg, err := s.Config(ctx)
	if err != nil {
		return nil, err
	}
	switch service {
	case "s3":
		return s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true }), nil
	case "sqs":
		return sqs.NewFromConfig(cfg), nil
	default:
		return nil, fmt.Errorf("robotocore: unknown service %q", service)
	}
}

// S3Client returns the path-style S3 client for the session.
func (s *Session) S3Client(ctx context.Context) (*s3.Client, error) {
	cfg, err := s.Config(ctx)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true }), nil
}

// SQSClient returns the SQS client for the session.
func (s *Session) SQSClient(ctx context.Context) (*sqs.Client, error) {
	cfg, err := s.Config(ctx)
	if err != nil {
		return nil, err
	}
	return sqs.NewFromConfig(cfg), nil
}
