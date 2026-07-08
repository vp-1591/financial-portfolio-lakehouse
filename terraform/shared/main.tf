# Shared infrastructure for the investment portfolio pipeline.
#
# Creates:
#   - ECR repository for the pipeline Docker image
#   - IAM policy for ECR push/pull
#   - ECS cluster (shared across environments)
#   - IAM role for Step Functions (used by per-environment state machines)
#
# This repository is shared between staging and production deployments.
# Staging images are tagged `git-<sha>` and `staging-latest`.
# Production images are tagged `<version>` and `production-latest`.
#
# Apply order:
#   1. shared/ apply — creates ECR + IAM policy + cluster + SFN role
#   2. demo/ and prod/ apply — each creates its own state machine, EventBridge
#      rules, and environment-specific resources
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

variable "ecs_cluster_arn" {
  description = "ARN of the ECS cluster. If empty, the module creates one."
  type        = string
  default     = ""
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
# ECS Cluster
# ------------------------------------------------------------------------------

resource "aws_ecs_cluster" "pipeline" {
  name = "portfolio-pipeline-cluster"

  tags = {
    Project = "investment-portfolio-pipeline"
  }
}

# ------------------------------------------------------------------------------
# Step Functions IAM Role
# ------------------------------------------------------------------------------

data "aws_iam_policy_document" "sfn_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn" {
  name               = "pipeline-sfn-role"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json

  tags = {
    Project = "investment-portfolio-pipeline"
  }
}

# Step Functions needs ecs:RunTask to start Fargate tasks, ecs:StopTask to
# cancel them, ecs:DescribeTasks to poll for completion, and iam:PassRole
# to pass the task role to ECS. PassRole is scoped to a role-name prefix
# (pipeline-task-*-prod or -demo-*) so a new connector task role needs no
# policy edit.
data "aws_iam_policy_document" "sfn" {
  statement {
    sid    = "RunTask"
    effect = "Allow"
    actions = [
      "ecs:RunTask",
      "ecs:StopTask",
      "ecs:DescribeTasks",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "PassRole"
    effect = "Allow"
    actions = [
      "iam:PassRole",
    ]
    # Scope PassRole to task execution and task roles by environment prefix.
    # A new connector adds a role matching pipeline-task-{env}-{name} — no policy edit needed.
    resources = [
      "arn:aws:iam::*:role/pipeline-task-exec-prod-*",
      "arn:aws:iam::*:role/pipeline-task-exec-demo-*",
      "arn:aws:iam::*:role/pipeline-task-prod-*",
      "arn:aws:iam::*:role/pipeline-task-demo-*",
    ]
  }

  # Permissions for .sync service integrations (ecs:runTask.sync).
  # Step Functions creates a managed EventBridge rule to receive task completion
  # callbacks. Without these, CreateStateMachine fails with
  # "is not authorized to create managed-rule".
  # Rule name pattern covers both ECS and Step Functions sync integrations.
  statement {
    sid    = "SyncCallback"
    effect = "Allow"
    actions = [
      "events:PutTargets",
      "events:PutRule",
      "events:DescribeRule",
    ]
    resources = [
      "arn:aws:events:${var.aws_region}:*:rule/StepFunctions*",
    ]
  }

  statement {
    sid    = "DescribeExecution"
    effect = "Allow"
    actions = [
      "states:DescribeExecution",
      "states:StopExecution",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "sfn" {
  name   = "pipeline-sfn-policy"
  role   = aws_iam_role.sfn.id
  policy = data.aws_iam_policy_document.sfn.json
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

output "ecs_cluster_arn" {
  description = "ARN of the ECS cluster."
  value       = aws_ecs_cluster.pipeline.arn
}