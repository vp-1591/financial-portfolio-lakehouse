# Infrastructure for the investment portfolio pipeline — demo environment.
#
# Creates:
#   - S3 bucket for demo Delta table storage
#   - IAM user with least-privilege access to the demo bucket
#   - IAM access key (store key ID and secret in GitHub Secrets as _DEMO variants)
#
# Usage:
#   cd terraform/demo
#   cp backend.tf.sample backend.tf   # first time only
#   # Edit backend.tf — set bucket to your S3 state bucket name
#   terraform init
#   terraform plan
#   terraform apply

# ------------------------------------------------------------------------------
# Variables
# ------------------------------------------------------------------------------

variable "aws_region" {
  description = "AWS region for the S3 bucket."
  type        = string
  default     = "eu-west-1"
}

variable "bucket_name" {
  description = "Globally unique S3 bucket name for demo data."
  type        = string
  default     = "investment-portfolio-pipeline-demo"
}

variable "s3_prefix" {
  description = "Key prefix within the S3 bucket for demo pipeline data."
  type        = string
  default     = "pipeline_demo"
}

variable "iam_user_name" {
  description = "Name of the IAM user for demo pipeline access."
  type        = string
  default     = "pipeline-demo"
}

# ------------------------------------------------------------------------------
# Provider
# ------------------------------------------------------------------------------

terraform {
  required_version = ">= 1.11"

  # Backend configuration is in backend.tf (gitignored) — copy from backend.tf.sample.
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ------------------------------------------------------------------------------
# S3 Bucket
# ------------------------------------------------------------------------------

resource "aws_s3_bucket" "pipeline_demo" {
  bucket = var.bucket_name

  tags = {
    Project = "investment-portfolio-pipeline-demo"
  }
}

resource "aws_s3_bucket_versioning" "pipeline_demo" {
  bucket = aws_s3_bucket.pipeline_demo.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "pipeline_demo" {
  bucket = aws_s3_bucket.pipeline_demo.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "pipeline_demo" {
  bucket = aws_s3_bucket.pipeline_demo.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ------------------------------------------------------------------------------
# IAM User
# ------------------------------------------------------------------------------

resource "aws_iam_user" "pipeline_demo" {
  name = var.iam_user_name

  tags = {
    Project = "investment-portfolio-pipeline-demo"
  }
}

resource "aws_iam_access_key" "pipeline_demo" {
  user = aws_iam_user.pipeline_demo.name
}

data "aws_iam_policy_document" "pipeline_demo" {
  statement {
    sid    = "ReadWritePipelineDataDemo"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]

    resources = [
      aws_s3_bucket.pipeline_demo.arn,
      "${aws_s3_bucket.pipeline_demo.arn}/${var.s3_prefix}/*",
    ]
  }
}

resource "aws_iam_policy" "pipeline_demo" {
  name   = "pipeline-demo-s3-access"
  policy = data.aws_iam_policy_document.pipeline_demo.json
}

resource "aws_iam_user_policy_attachment" "pipeline_demo" {
  user       = aws_iam_user.pipeline_demo.name
  policy_arn = aws_iam_policy.pipeline_demo.arn
}

# ------------------------------------------------------------------------------
# Outputs
# ------------------------------------------------------------------------------

output "s3_bucket" {
  description = "S3 bucket name for demo pipeline data."
  value       = aws_s3_bucket.pipeline_demo.bucket
}

output "access_key_id" {
  description = "IAM access key ID (store as GitHub Secret AWS_ACCESS_KEY_ID_DEMO)."
  value       = aws_iam_access_key.pipeline_demo.id
}

output "s3_prefix" {
  description = "S3 key prefix for demo pipeline data."
  value       = var.s3_prefix
}