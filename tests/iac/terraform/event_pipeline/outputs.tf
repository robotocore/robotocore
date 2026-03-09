output "queue_url" {
  description = "SQS queue URL"
  value       = aws_sqs_queue.inbox.url
}

output "function_name" {
  description = "Lambda function name"
  value       = aws_lambda_function.processor.function_name
}

output "table_name" {
  description = "DynamoDB table name"
  value       = aws_dynamodb_table.events.name
}
