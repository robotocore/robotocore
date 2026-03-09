output "topic_arn" {
  description = "ARN of the SNS alarm notification topic"
  value       = aws_sns_topic.alarm_notifications.arn
}

output "alarm_name" {
  description = "Name of the CloudWatch CPU alarm"
  value       = aws_cloudwatch_metric_alarm.cpu_high.alarm_name
}

output "log_group_name" {
  description = "Name of the CloudWatch log group"
  value       = aws_cloudwatch_log_group.app_logs.name
}
