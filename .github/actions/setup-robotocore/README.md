# setup-robotocore

A GitHub Actions composite action that starts a [Robotocore](https://github.com/robotocore/robotocore) AWS emulator container for integration testing.

## Usage

### Basic

```yaml
steps:
  - uses: robotocore/robotocore/.github/actions/setup-robotocore@main

  - name: Run tests against local AWS
    run: |
      aws s3 mb s3://my-bucket
      aws sqs create-queue --queue-name my-queue
```

### With options

```yaml
steps:
  - uses: robotocore/robotocore/.github/actions/setup-robotocore@main
    id: robotocore
    with:
      image-tag: "v2026.3.10"
      services: "s3,sqs,dynamodb"
      iam-enforcement: "true"
      wait-timeout: "60"
      configuration: |
        DEBUG=1
        AUDIT_LOG_SIZE=1000

  - name: Run tests
    run: pytest tests/
    env:
      AWS_ENDPOINT_URL: ${{ steps.robotocore.outputs.endpoint }}

  - name: Cleanup
    if: always()
    run: .github/actions/setup-robotocore/cleanup.sh ${{ steps.robotocore.outputs.container-id }}
```

## Inputs

| Input | Description | Default |
|-------|-------------|---------|
| `image-tag` | Docker image tag to use | `latest` |
| `configuration` | Env vars for the container (newline-separated `KEY=VALUE`) | `""` |
| `wait` | Wait for Robotocore to be ready | `true` |
| `wait-timeout` | Seconds to wait for readiness | `30` |
| `services` | Comma-separated AWS services to enable | `""` (all) |
| `persistence` | Enable state persistence | `false` |
| `iam-enforcement` | Enable IAM policy enforcement | `false` |

## Outputs

| Output | Description |
|--------|-------------|
| `endpoint` | The Robotocore endpoint URL (`http://localhost:4566`) |
| `container-id` | The Docker container ID (for cleanup) |

## Environment

The action automatically sets these environment variables for subsequent steps:

- `AWS_ENDPOINT_URL=http://localhost:4566`
- `AWS_ACCESS_KEY_ID=test`
- `AWS_SECRET_ACCESS_KEY=test`
- `AWS_DEFAULT_REGION=us-east-1`

## Cleanup

Use the included `cleanup.sh` script in an `if: always()` step to ensure the container is removed even if tests fail:

```yaml
- name: Cleanup
  if: always()
  run: .github/actions/setup-robotocore/cleanup.sh ${{ steps.robotocore.outputs.container-id }}
```
