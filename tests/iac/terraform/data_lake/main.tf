terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

resource "aws_s3_bucket" "landing_zone" {
  bucket = "${var.prefix}-landing-zone"
}

resource "aws_kinesis_stream" "ingest" {
  name        = "${var.prefix}-ingest-stream"
  shard_count = 1
}

resource "aws_dynamodb_table" "catalog" {
  name         = "${var.prefix}-catalog"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "dataset_id"
  range_key    = "timestamp"

  attribute {
    name = "dataset_id"
    type = "S"
  }

  attribute {
    name = "timestamp"
    type = "S"
  }
}
