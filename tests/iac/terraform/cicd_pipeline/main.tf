terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

resource "aws_s3_bucket" "artifacts" {
  bucket = "${var.prefix}-artifacts"

  tags = {
    Name = "${var.prefix}-artifacts"
  }
}

resource "aws_iam_role" "pipeline" {
  name = "${var.prefix}-pipeline-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "codepipeline.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name = "${var.prefix}-pipeline-role"
  }
}

resource "aws_iam_role_policy" "pipeline_s3" {
  name = "${var.prefix}-pipeline-s3-policy"
  role = aws_iam_role.pipeline.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.artifacts.arn,
          "${aws_s3_bucket.artifacts.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_sns_topic" "build_notifications" {
  name = "${var.prefix}-build-notifications"

  tags = {
    Name = "${var.prefix}-build-notifications"
  }
}
