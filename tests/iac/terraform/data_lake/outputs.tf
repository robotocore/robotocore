output "bucket_name" {
  description = "Name of the landing zone S3 bucket"
  value       = aws_s3_bucket.landing_zone.id
}

output "stream_name" {
  description = "Name of the Kinesis ingest stream"
  value       = aws_kinesis_stream.ingest.name
}

output "table_name" {
  description = "Name of the DynamoDB catalog table"
  value       = aws_dynamodb_table.catalog.name
}
