# Shared infrastructure for the investment portfolio pipeline.
#
# Creates:
#   - ECR repository for the pipeline Docker image
#   - IAM policy for ECR push/pull
#   - ECS cluster (shared across environments)
#   - Step Functions orchestrator state machine
#   - EventBridge rules (S3 file arrival + daily schedule)
#   - IAM role for Step Functions
#
# This repository is shared between staging and production deployments.
# Staging images are tagged `git-<sha>` and `staging-latest`.
# Production images are tagged `<version>` and `production-latest`.
#
# Apply order:
#   1. shared/ apply #1 — ECR + IAM policy + cluster (state machine count=0
#      while task_def_arns is empty)
#   2. prod/ and demo/ apply — ECS task defs, VPCs, SSM params, etc.
#   3. shared/ apply #2 — state machine + EventBridge rules (connector ARN map
#      + bucket name + subnet ids + cluster ARN in terraform.tfvars)
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

# --- Orchestration variables ---

variable "scheduled" {
  description = "Whether to create an EventBridge daily-schedule trigger for the orchestrator."
  type        = bool
  default     = false
}

variable "xtb_enabled" {
  description = "Whether to create an EventBridge S3 file-arrival trigger for XTB uploads."
  type        = bool
  default     = true
}

variable "schedule_connectors" {
  description = "Connector names included in the daily-schedule execution input (no xtb_file)."
  type        = list(string)
  default     = ["ibkr", "t212"]
}

variable "file_arrival_connectors" {
  description = "Connector names included in the XTB file-arrival execution input."
  type        = list(string)
  default     = ["ibkr", "t212", "xtb"]
}

variable "task_def_arns" {
  description = "Map of connector name → ECS task definition ARN (fed from prod/demo outputs via tfvars)."
  type        = map(string)
  default     = {}
}

variable "consolidate_allocate_task_def_arn" {
  description = "ECS task definition ARN for the consolidate-allocate step."
  type        = string
  default     = ""
}

variable "xtb_staging_bucket_name" {
  description = "S3 bucket name for XTB staging (the bucket that receives XTB uploads)."
  type        = string
  default     = ""
}

variable "xtb_staging_prefix" {
  description = "S3 key prefix for XTB staging within the bucket."
  type        = string
  default     = "staging/xtb/"
}

variable "schedule_cron" {
  description = "Cron expression for the daily schedule EventBridge rule."
  type        = string
  default     = "cron(0 6 * * ? *)"
}

variable "ecs_cluster_arn" {
  description = "ARN of the ECS cluster. If empty, the module creates one."
  type        = string
  default     = ""
}

variable "subnet_ids" {
  description = "Subnet IDs for ECS tasks (from prod/demo environment)."
  type        = list(string)
  default     = []
}

variable "security_group_ids" {
  description = "Security group IDs for ECS tasks (from prod/demo environment)."
  type        = list(string)
  default     = []
}

variable "state_machine_name" {
  description = "Name of the Step Functions orchestrator state machine."
  type        = string
  default     = "portfolio-pipeline-orchestrator"
}

variable "env" {
  description = "Environment label (prod or demo) used in naming and execution input."
  type        = string
  default     = "prod"
}

variable "demo" {
  description = "Whether this is the demo environment."
  type        = bool
  default     = false
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
}

resource "aws_iam_role_policy" "sfn" {
  name   = "pipeline-sfn-policy"
  role   = aws_iam_role.sfn.id
  policy = data.aws_iam_policy_document.sfn.json
}

# ------------------------------------------------------------------------------
# Step Functions State Machine
# ------------------------------------------------------------------------------

# The orchestrator uses a Map state over $.connectors from execution input.
# Each item = {name, task_def_arn, command}. After the Map,
# ConsolidateAllocate runs using $.consolidate_allocate_task_def_arn.
# One generic definition — no per-connector ASL branches.
#
# ASL uses ".$" suffix on parameter keys to indicate JSON path references:
#   "TaskDefinition.$" = "$.task_def_arn"  →  read task_def_arn from state input
#   "Command.$"        = "$.command"        →  read command array from state input
#
# The container name "pipeline" is consistent across all task definitions
# (set in the ecs-task module) so the orchestrator can use a static name.
locals {
  sfn_definition = jsonencode({
    Comment = "Portfolio pipeline orchestrator — Map over connectors then consolidate-allocate"
    StartAt = "RunConnectors"
    States = {
      RunConnectors = {
        Type          = "Map"
        # Iterate over the connectors array within the execution input object.
        ItemsPath     = "$.connectors"
        MaxConcurrency = 3
        # ResultPath preserves the original input alongside Map output so
        # ConsolidateAllocate can access $.consolidate_allocate_task_def_arn.
        ResultPath = "$.connector_results"
        Iterator = {
          StartAt = "RunConnector"
          States = {
            RunConnector = {
              Type     = "Task"
              Resource = "arn:aws:states:::ecs:runTask.sync"
              Parameters = {
                "TaskDefinition.$" = "$.task_def_arn"
                ClusterArn         = var.ecs_cluster_arn != "" ? var.ecs_cluster_arn : aws_ecs_cluster.pipeline.arn
                LaunchType          = "FARGATE"
                NetworkConfiguration = {
                  AwsVpcConfiguration = {
                    Subnets        = var.subnet_ids
                    SecurityGroups = var.security_group_ids
                    AssignPublicIp  = "DISABLED"
                  }
                }
                Overrides = {
                  ContainerOverrides = [
                    {
                      Name         = "pipeline"
                      "Command.$"  = "$.command"
                    }
                  ]
                }
              }
              Retry = [
                {
                  ErrorEquals    = ["States.TaskFailed"]
                  IntervalSeconds = 30
                  MaxAttempts     = 2
                  BackoffRate     = 2.0
                }
              ]
              End = true
            }
          }
        }
        Next = "ConsolidateAllocate"
      }
      ConsolidateAllocate = {
        Type     = "Task"
        Resource = "arn:aws:states:::ecs:runTask.sync"
        Parameters = {
          "TaskDefinition.$" = "$.consolidate_allocate_task_def_arn"
          ClusterArn         = var.ecs_cluster_arn != "" ? var.ecs_cluster_arn : aws_ecs_cluster.pipeline.arn
          LaunchType          = "FARGATE"
          NetworkConfiguration = {
            AwsVpcConfiguration = {
              Subnets        = var.subnet_ids
              SecurityGroups = var.security_group_ids
              AssignPublicIp  = "DISABLED"
            }
          }
          Overrides = {
            ContainerOverrides = [
              {
                Name    = "pipeline"
                Command = ["run-consolidate-allocate", "--target-currency", "EUR"]
              }
            ]
          }
        }
        End = true
      }
    }
  })

  # Create the state machine only when at least one trigger is enabled
  # OR when task_def_arns are provided (manual execution).
  create_state_machine = var.scheduled || var.xtb_enabled || length(var.task_def_arns) > 0
}

resource "aws_sfn_state_machine" "orchestrator" {
  count = local.create_state_machine ? 1 : 0

  name     = var.state_machine_name
  role_arn = aws_iam_role.sfn.arn
  definition = local.sfn_definition

  tags = {
    Project = "investment-portfolio-pipeline"
    Env     = var.env
  }
}

# ------------------------------------------------------------------------------
# EventBridge — S3 File Arrival (XTB)
# ------------------------------------------------------------------------------

# Triggered when a file is uploaded to the XTB staging prefix.
# Uses a static input_transformer to build the execution input with the
# connector list. Adding a connector requires updating this template in
# Terraform — no ASL or CLI edit needed.
#
# The input_template uses <xtb_file> as a placeholder that EventBridge replaces
# with the actual S3 object key from the event. jsonencode() produces valid JSON
# with <xtb_file> as a literal string value, which EventBridge then substitutes.

resource "aws_cloudwatch_event_rule" "xtb_file_arrival" {
  count = var.xtb_enabled ? 1 : 0

  name        = "portfolio-pipeline-xtb-file-arrival"
  description = "Trigger the pipeline orchestrator when an XTB file arrives in S3 staging"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = {
        name = [var.xtb_staging_bucket_name]
      }
      object = {
        key = [{ prefix = var.xtb_staging_prefix }]
      }
    }
  })

  tags = {
    Project = "investment-portfolio-pipeline"
  }
}

resource "aws_cloudwatch_event_target" "xtb_file_arrival" {
  count = var.xtb_enabled ? 1 : 0

  rule      = aws_cloudwatch_event_rule.xtb_file_arrival[0].name
  target_id = "orchestrator"
  arn       = aws_sfn_state_machine.orchestrator[0].arn
  role_arn  = aws_iam_role.eventbridge[0].arn

  input_transformer {
    input_paths = {
      xtb_file = "$.detail.object.key"
    }
    # The input_template uses constant values for connector list and ARN map,
    # with $.detail.object.key interpolated for the XTB file URI.
    input_template = <<-TEMPLATE
    {
      "connectors": ${jsonencode([
        for name in var.file_arrival_connectors : {
          name = name
          task_def_arn = lookup(var.task_def_arns, name, "")
          command = name == "xtb" ? [
            "run-connector", name, "--xtb-file",
            "s3://${var.xtb_staging_bucket_name}/<xtb_file>"
          ] : [
            "run-connector", name, "--target-currency", "EUR"
          ]
        }
      ])},
      "consolidate_allocate_task_def_arn": "${var.consolidate_allocate_task_def_arn}",
      "demo": ${var.demo}
    }
    TEMPLATE
  }
}

# ------------------------------------------------------------------------------
# EventBridge — Daily Schedule
# ------------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "daily_schedule" {
  count = var.scheduled ? 1 : 0

  name                = "portfolio-pipeline-daily-schedule"
  description         = "Trigger the pipeline orchestrator on a daily schedule"
  schedule_expression = var.schedule_cron

  tags = {
    Project = "investment-portfolio-pipeline"
  }
}

resource "aws_cloudwatch_event_target" "daily_schedule" {
  count = var.scheduled ? 1 : 0

  rule      = aws_cloudwatch_event_rule.daily_schedule[0].name
  target_id = "orchestrator"
  arn       = aws_sfn_state_machine.orchestrator[0].arn
  role_arn  = aws_iam_role.eventbridge[0].arn

  input = jsonencode({
    connectors = [
      for name in var.schedule_connectors : {
        name         = name
        task_def_arn = lookup(var.task_def_arns, name, "")
        command = [
          "run-connector", name, "--target-currency", "EUR"
        ]
      }
    ]
    consolidate_allocate_task_def_arn = var.consolidate_allocate_task_def_arn
    demo                               = var.demo
  })
}

# ------------------------------------------------------------------------------
# EventBridge IAM Role (for both triggers)
# ------------------------------------------------------------------------------

data "aws_iam_policy_document" "eventbridge_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "eventbridge" {
  count = (var.xtb_enabled || var.scheduled) ? 1 : 0

  name               = "pipeline-eventbridge-role"
  assume_role_policy = data.aws_iam_policy_document.eventbridge_assume.json

  tags = {
    Project = "investment-portfolio-pipeline"
  }
}

data "aws_iam_policy_document" "eventbridge" {
  statement {
    sid    = "StartExecution"
    effect = "Allow"
    actions = [
      "states:StartExecution",
    ]
    resources = [
      local.create_state_machine ? aws_sfn_state_machine.orchestrator[0].arn : "arn:aws:states:${var.aws_region}:*:stateMachine:${var.state_machine_name}"
    ]
  }
}

resource "aws_iam_role_policy" "eventbridge" {
  count = (var.xtb_enabled || var.scheduled) ? 1 : 0

  name   = "pipeline-eventbridge-policy"
  role   = aws_iam_role.eventbridge[0].id
  policy = data.aws_iam_policy_document.eventbridge.json
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

output "state_machine_arn" {
  description = "ARN of the Step Functions orchestrator state machine (empty string if not created)."
  value       = local.create_state_machine ? aws_sfn_state_machine.orchestrator[0].arn : ""
}