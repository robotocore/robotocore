package robotocore

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func TestEndpoint(t *testing.T) {
	t.Setenv("ROBOTOCORE_ENDPOINT", "http://127.0.0.1:5000/")
	if got := Endpoint("http://localhost:5001/"); got != "http://localhost:5001" {
		t.Errorf("Endpoint(override) = %q, want trimmed override", got)
	}
	if got := Endpoint(""); got != "http://127.0.0.1:5000" {
		t.Errorf("Endpoint(\"\") via env = %q, want env value with trailing slash trimmed", got)
	}
	t.Setenv("ROBOTOCORE_ENDPOINT", "")
	if got := Endpoint(""); got != DefaultEndpoint {
		t.Errorf("Endpoint(\"\") default = %q, want %q", got, DefaultEndpoint)
	}
}

func TestCredentials(t *testing.T) {
	if key, secret := Credentials("42"); key != "000000000042" || secret != "test" {
		t.Errorf("Credentials(\"42\") = %q,%q; want zero-padded account + static secret", key, secret)
	}
	if key, _ := Credentials("999999999999"); key != "999999999999" {
		t.Errorf("Credentials(12-digit) = %q; want the account unchanged", key)
	}
}

func TestNewSessionUnknownService(t *testing.T) {
	if _, err := NewSession("dynamodb"); err == nil || !strings.Contains(err.Error(), `unknown service "dynamodb"`) {
		t.Errorf("NewSession(\"dynamodb\") error = %v; want unknown-service error", err)
	}
}

// TestSmokeLiveTwin exercises the helper against a running robotocore
// twin: SQS queue create/send/receive/delete plus an S3 object
// round-trip. Skipped unless ROBOTOCORE_ENDPOINT is set or -short is on:
//
//	ROBOTOCORE_ENDPOINT=http://127.0.0.1:4566 go test -v .
func TestSmokeLiveTwin(t *testing.T) {
	if testing.Short() {
		t.Skip("-short skips the live-twin smoke")
	}
	if os.Getenv("ROBOTOCORE_ENDPOINT") == "" {
		t.Skip("set ROBOTOCORE_ENDPOINT (e.g. http://127.0.0.1:4566) to run the live-twin smoke")
	}

	ctx := context.Background()
	suffix := fmt.Sprintf("%d", os.Getpid())
	session := &Session{}

	sqsClient, err := session.SQSClient(ctx)
	if err != nil {
		t.Fatalf("SQSClient: %v", err)
	}
	queueName := fmt.Sprintf("robotocore-go-smoke-%s", suffix)
	created, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: &queueName})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if created.QueueUrl == nil || !strings.HasSuffix(*created.QueueUrl, queueName) {
		t.Fatalf("CreateQueue QueueUrl = %v; want it to contain the queue name", created.QueueUrl)
	}
	body := "hello"
	sent, err := sqsClient.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: &body})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if sent.MessageId == nil {
		t.Fatal("SendMessage returned no MessageId")
	}
	received, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, MaxNumberOfMessages: 1})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	if len(received.Messages) != 1 || received.Messages[0].Body == nil || *received.Messages[0].Body != body {
		t.Fatalf("ReceiveMessage = %+v; want one \"hello\" message", received.Messages)
	}
	if _, err := sqsClient.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: created.QueueUrl}); err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}
	t.Logf("sqs %s create/send/receive/delete OK", queueName)

	// NewSession dispatch asserted to its concrete client type.
	anyS3, err := NewSession("s3")
	if err != nil {
		t.Fatalf("NewSession(\"s3\"): %v", err)
	}
	s3Client, ok := anyS3.(*s3.Client)
	if !ok {
		t.Fatalf("NewSession(\"s3\") returned %T; want *s3.Client", anyS3)
	}
	bucket := fmt.Sprintf("robotocore-go-smoke-%s", suffix)
	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("CreateBucket (us-east-1): %v", err)
	}
	key := "hello.txt"
	if _, err := s3Client.PutObject(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: &key, Body: strings.NewReader(body)}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	fetched, err := s3Client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	var got strings.Builder
	if _, err := io.Copy(&got, fetched.Body); err != nil {
		t.Fatalf("read GetObject body: %v", err)
	}
	if got.String() != body {
		t.Errorf("GetObject body = %q; want %q", got.String(), body)
	}
	if _, err := s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &key}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if _, err := s3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	t.Logf("s3 %s put/get/delete OK", bucket)

	// Any SQS client handed back by NewSession must be the real type.
	anySqs, err := NewSession("sqs")
	if err != nil {
		t.Fatalf("NewSession(\"sqs\"): %v", err)
	}
	if _, ok := anySqs.(*sqs.Client); !ok {
		t.Errorf("NewSession(\"sqs\") returned %T; want *sqs.Client", anySqs)
	}
}
