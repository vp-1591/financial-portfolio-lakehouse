# Shared infrastructure for the investment portfolio pipeline.
#
# Creates:
#   - ECR repository for the pipeline Docker image
#   - IAM policy for ECR push/pull
#
# This repository is shared between staging and production deployments.
# Staging images are tagged `git-<sha>` and `staging-latest`.
# Production images are tagged `<version>` and `production-latest`.
#
# Usage:
#   cd terraform/shared
#   cp backend.tf.sample backend.tf   # first time only
#   # Edit backend.tf — set bucket to your S3 state bucket name
#   terraform init
#   terraform plan
#   terraform apply

# ------------------------------------------------------------------------------
# Variables
# ------------------------------------------------------------------------------

variable "aws_region" {
  description = "AWS region for the ECR repository."
  type        = string
  default     = "eu-west-1"
}

variable "ecr_repository_name" {
  description = "Name of the ECR repository for the pipeline Docker image."
  type        = string
  default     = "investment-portfolio-pipeline"
}

variable "keep_tagged_images" {
  description = "Maximum number of tagged images to retain in ECR."
  type        = number
  default     = 10
}

variable "expire_untagged_days" {
  description = "Number of days after which untagged images are expired."
  type        = number
  default     = 7
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
# ECR Repository
# ------------------------------------------------------------------------------

resource "aws_ecr_repository" "pipeline" {
  name                 = var.ecr_repository_name
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Project = "investment-portfolio-pipeline"
  }
}

# Expire untagged images after N days to prevent unbounded storage growth.
# Keep at most N tagged images so old versions are cleaned up.
resource "aws_ecr_lifecycle_policy" "pipeline" {
  repository = aws_ecr_repository.pipeline.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire untagged images after ${var.expire_untagged_days} days"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = var.expire_untagged_days
        }
        action = {
          type = "expire"
        }
      },
      {
        rulePriority = 2
        description  = "Keep at most ${var.keep_tagged_images} tagged images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = var.keep_tagged_images
        }
        action = {
          type = "expire"
        }
      },
    ]
  })
}

# ------------------------------------------------------------------------------
# IAM Policy for ECR Push/Pull
# ------------------------------------------------------------------------------

data "aws_iam_policy_document" "ecr_push_pull" {
  statement {
    sid    = "ECRAuth"
    effect = "Allow"

    actions = [
      "ecr:GetAuthorizationToken",
    ]

    resources = ["*"]
  }

  statement {
    sid    = "ECRPushPull"
    effect = "Allow"

    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:GetDownloadUrlForLayer",
      "ecr:BatchGetImage",
      "ecr:PutImage",
      "ecr:InitiateLayerUpload",
      "ecr:UploadLayerPart",
      "ecr:CompleteLayerUpload",
    ]

    resources = [aws_ecr_repository.pipeline.arn]
  }
}

resource "aws_iam_policy" "ecr_push_pull" {
  name   = "pipeline-ecr-push-pull"
  policy = data.aws_iam_policy_document.ecr_push_pull.json

  tags = {
    Project = "investment-portfolio-pipeline"
  }
}

# ------------------------------------------------------------------------------
# Outputs
# ------------------------------------------------------------------------------

output "ecr_repository_url" {
  description = "URL of the ECR repository for the pipeline Docker image."
  value       = aws_ecr_repository.pipeline.repository_url
}

output "ecr_repository_arn" {
  description = "ARN of the ECR repository."
  value       = aws_ecr_repository.pipeline.arn
}

output "ecr_push_pull_policy_arn" {
  description = "ARN of the IAM policy for ECR push/pull. Attach this to the pipeline IAM user."
  value       = aws_iam_policy.ecr_push_pull.arn
}