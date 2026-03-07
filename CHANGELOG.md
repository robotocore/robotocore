# Changelog

## 1.0.0 (2026-03-07)

### Overview

First GA release. Robotocore is an MIT-licensed, open-source AWS emulator built on Moto with drop-in LocalStack compatibility. Single Docker container, runs on ARM Mac, no registration, no telemetry.

### Service Coverage

- **147 registered services** (38 native + 109 Moto-backed)
- **147 services with automated compat tests** (100% coverage)
- **94/94 smoke tests passing**
- **5195+ total tests** (2520 unit + 2675 compat + 42 integration), 0 failures

### Native Providers (38)

Full behavioral fidelity with real execution semantics:

acm, apigateway, apigatewayv2, appsync, batch, cloudformation, cloudwatch,
cognito-idp, config, dynamodb, dynamodbstreams, ec2, ecr, ecs, es, events,
firehose, iam, kinesis, lambda, logs, opensearch, rekognition, resource-groups,
resourcegroupstaggingapi, route53, s3, scheduler, secretsmanager, ses, sesv2,
sns, sqs, ssm, stepfunctions, sts, support, xray

### Moto-backed Services (109)

Routing and request handling via Moto backends with protocol translation:

account, acmpca, amp, apigatewaymanagementapi, applicationautoscaling,
appmesh, athena, autoscaling, backup, bedrock, bedrockagent, budgets, ce,
clouddirectory, cloudfront, cloudhsmv2, cloudtrail, codebuild, codecommit,
codedeploy, codepipeline, cognitoidentity, comprehend, connect,
connectcampaigns, databrew, datapipeline, datasync, dax, dms, ds, dsql,
ec2instanceconnect, efs, eks, elasticache, elasticbeanstalk, elb, elbv2, emr,
emrcontainers, emrserverless, fsx, glacier, glue, greengrass, guardduty,
identitystore, inspector2, iot, iotdata, ivs, kafka, kinesisanalyticsv2,
kinesisvideo, kms, lakeformation, lexv2models, macie2, managedblockchain,
mediaconnect, medialive, mediapackage, mediapackagev2, mediastore, memorydb,
mq, networkfirewall, networkmanager, opensearchserverless, organizations, osis,
panorama, pinpoint, pipes, polly, quicksight, ram, rds, rdsdata, redshift,
redshiftdata, resiliencehub, route53domains, route53resolver, s3control,
s3tables, s3vectors, sagemaker, securityhub, servicecatalog,
servicecatalogappregistry, servicediscovery, ses, shield, signer, ssoadmin,
swf, synthetics, textract, timestreaminfluxdb, timestreamquery, timestreamwrite,
transcribe, transfer, vpclattice, wafv2, workspaces, workspacesweb

### Infrastructure Features

- **Gateway**: Single port (4566), full AWS protocol support (query, json, rest-json, rest-xml, ec2)
- **Chaos Engineering**: Inject ThrottlingException, latency, and custom faults via `/_robotocore/chaos/rules`
- **Resource Browser**: Cross-service resource overview via `/_robotocore/resources`
- **Audit Log**: Ring buffer of recent API calls via `/_robotocore/audit`
- **State Snapshots**: Named save/load with selective persistence via `/_robotocore/state/*`
- **IAM Enforcement**: Opt-in full policy evaluation engine via `ENFORCE_IAM=1`
- **Extensions**: Plugin system with entry point, env var, and directory discovery
- **Observability**: Structured JSON logging, tracing middleware, metrics, init hooks
- **Docker**: Multi-arch image (amd64/arm64), boots in <5s

### Migrating from LocalStack

```bash
# Before (LocalStack)
docker run -p 4566:4566 localstack/localstack

# After (Robotocore)
docker run -p 4566:4566 robotocore

# No code changes needed — same port, same endpoint URL, same AWS CLI flags
aws --endpoint-url=http://localhost:4566 s3 ls
```

### Known Limitations

The following services are NOT registered (Moto backends exist but all operations fail):
directconnect, ebs, forecast, personalize, sdb, servicequotas,
meteringmarketplace, sagemakermetrics, sagemakerruntime,
kinesisvideoarchivedmedia, mediastoredata

These may be added in future releases as Moto adds support.
