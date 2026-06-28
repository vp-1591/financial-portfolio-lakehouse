# Infrastructure for the investment portfolio pipeline.
#
# Creates:
#   - S3 bucket for Delta table storage
#   - IAM user with least-privilege access to the bucket
#   - IAM access key (store key ID and secret in GitHub Secrets)
#
# Usage:
#   cd infra
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
  description = "Globally unique S3 bucket name."
  type        = string
  default     = "investment-portfolio-pipeline"
}

variable "s3_prefix" {
  description = "Key prefix within the S3 bucket for pipeline data."
  type        = string
  default     = "pipeline"
}

variable "iam_user_name" {
  description = "Name of the IAM user for pipeline access."
  type        = string
  default     = "pipeline"
}

# ------------------------------------------------------------------------------
# Provider
# ------------------------------------------------------------------------------

terraform {
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

resource "aws_s3_bucket" "pipeline" {
  bucket = var.bucket_name

  tags = {
    Project = "investment-portfolio-pipeline"
  }
}

resource "aws_s3_bucket_versioning" "pipeline" {
  bucket = aws_s3_bucket.pipeline.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "pipeline" {
  bucket = aws_s3_bucket.pipeline.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "pipeline" {
  bucket = aws_s3_bucket.pipeline.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ------------------------------------------------------------------------------
# IAM User
# ------------------------------------------------------------------------------

resource "aws_iam_user" "pipeline" {
  name = var.iam_user_name

  tags = {
    Project = "investment-portfolio-pipeline"
  }
}

resource "aws_iam_access_key" "pipeline" {
  user = aws_iam_user.pipeline.name
}

data "aws_iam_policy_document" "pipeline" {
  statement {
    sid    = "ReadWritePipelineData"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]

    resources = [
      aws_s3_bucket.pipeline.arn,
      "${aws_s3_bucket.pipeline.arn}/${var.s3_prefix}/*",
    ]
  }
}

resource "aws_iam_policy" "pipeline" {
  name   = "pipeline-s3-access"
  policy = data.aws_iam_policy_document.pipeline.json
}

resource "aws_iam_user_policy_attachment" "pipeline" {
  user       = aws_iam_user.pipeline.name
  policy_arn = aws_iam_policy.pipeline.arn
}

# ------------------------------------------------------------------------------
# Outputs
# ------------------------------------------------------------------------------

output "s3_bucket" {
  description = "S3 bucket name for pipeline data."
  value       = aws_s3_bucket.pipeline.bucket
}

output "access_key_id" {
  description = "IAM access key ID (store as GitHub Secret AWS_ACCESS_KEY_ID)."
  value       = aws_iam_access_key.pipeline.id
}

output "s3_prefix" {
  description = "S3 key prefix for pipeline data."
  value       = var.s3_prefix
}