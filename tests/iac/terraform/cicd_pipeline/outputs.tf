output "bucket_name" {
  description = "Name of the artifacts S3 bucket"
  value       = aws_s3_bucket.artifacts.id
}

output "role_arn" {
  description = "ARN of the pipeline IAM role"
  value       = aws_iam_role.pipeline.arn
}

output "topic_arn" {
  description = "ARN of the build notifications SNS topic"
  value       = aws_sns_topic.build_notifications.arn
}
