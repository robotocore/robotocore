terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

resource "aws_sns_topic" "alarm_notifications" {
  name = "${var.prefix}-alarm-notifications"
}

resource "aws_cloudwatch_metric_alarm" "cpu_high" {
  alarm_name          = "${var.prefix}-cpu-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/EC2"
  period              = 300
  statistic           = "Average"
  threshold           = 80
  alarm_description   = "CPU utilization exceeds 80%"

  alarm_actions = [aws_sns_topic.alarm_notifications.arn]
}

resource "aws_cloudwatch_log_group" "app_logs" {
  name              = "${var.prefix}-app-logs"
  retention_in_days = 7
}
