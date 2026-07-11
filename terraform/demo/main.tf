# Infrastructure for the investment portfolio pipeline — demo environment.
#
# Creates:
#   - S3 bucket for demo Delta table storage
#   - IAM user with least-privilege access to the demo bucket
#   - IAM access key (store key ID and secret in GitHub Secrets as _DEMO variants)
#   - VPC with public subnets, Internet Gateway, and security group
#   - KMS key for SSM SecureString encryption
#   - SSM parameter names (values seeded out-of-band)
#   - ECS task definitions for each connector + consolidate-allocate
#   - S3 bucket notification for EventBridge
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

variable "ecr_repository_url" {
  description = "URL of the ECR repository (from terraform/shared outputs)."
  type        = string
}

variable "ecr_push_pull_policy_arn" {
  description = "ARN of the ECR push/pull policy (from terraform/shared outputs)."
  type        = string
}

variable "ecs_cluster_arn" {
  description = "ARN of the ECS cluster (from terraform/shared outputs)."
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block for the demo VPC."
  type        = string
  default     = "10.1.0.0/16"
}

variable "subnet_cidrs" {
  description = "CIDR blocks for public subnets (one per AZ)."
  type        = list(string)
  default     = ["10.1.1.0/24"]
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

# Resource renames: private subnets → public subnets (ADR 0054)
moved {
  from = aws_subnet.private
  to   = aws_subnet.public
}

# ------------------------------------------------------------------------------
# Local values
# ------------------------------------------------------------------------------

locals {
  env_label   = "demo"
  image_tag   = "staging-latest"
  az_suffixes = ["a"]

  # Connector definitions for the ecs-task module for_each.
  # Demo environment uses _DEMO-suffixed SSM parameter names AND _DEMO-suffixed
  # env var names, mirroring DEMO_SECRET_MAP in pipeline/secrets.py.
  # resolve_secret("IBKR_FLEX_TOKEN") in demo mode looks for IBKR_FLEX_TOKEN_DEMO
  # in the environment, so the ECS task must set the _DEMO-suffixed env var name.
  connectors = {
    ibkr = {
      command = ["run-connector", "ibkr", "--target-currency", "EUR"]
      secrets = [
        { env_var = "IBKR_FLEX_TOKEN_DEMO",   param_name = "/portfolio/demo/IBKR_FLEX_TOKEN_DEMO" },
        { env_var = "IBKR_FLEX_QUERY_ID_DEMO", param_name = "/portfolio/demo/IBKR_FLEX_QUERY_ID_DEMO" },
      ]
    }
    trading212 = {
      command = ["run-connector", "trading212", "--target-currency", "EUR"]
      secrets = [
        { env_var = "T212_API_KEY_DEMO",    param_name = "/portfolio/demo/T212_API_KEY_DEMO" },
        { env_var = "T212_API_SECRET_DEMO",  param_name = "/portfolio/demo/T212_API_SECRET_DEMO" },
      ]
    }
    xtb = {
      command = ["run-connector", "xtb", "--target-currency", "EUR"]
      secrets = []
    }
  }
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

# Enable EventBridge notification on the bucket so the XTB file-arrival
# rule in terraform/shared/ can detect uploads.
resource "aws_s3_bucket_notification" "pipeline_demo" {
  bucket = aws_s3_bucket.pipeline_demo.id

  eventbridge = true
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
    sid    = "ListBucket"
    effect = "Allow"

    actions = [
      "s3:ListBucket",
    ]

    resources = [
      aws_s3_bucket.pipeline_demo.arn,
    ]
  }

  statement {
    sid    = "ReadWriteObjects"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]

    resources = [
      "${aws_s3_bucket.pipeline_demo.arn}/*",
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

# Attach the ECR push/pull policy (defined in terraform/shared/) so the
# demo pipeline user can push Docker images during deploy and pull them at runtime.
# terraform/shared/ must be applied before terraform/demo/.
data "aws_iam_policy" "ecr_push_pull" {
  name = "pipeline-ecr-push-pull"
}

resource "aws_iam_user_policy_attachment" "ecr_push_pull" {
  user       = aws_iam_user.pipeline_demo.name
  policy_arn = data.aws_iam_policy.ecr_push_pull.arn
}

# ------------------------------------------------------------------------------
# VPC
# ------------------------------------------------------------------------------

resource "aws_vpc" "pipeline_demo" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Project = "investment-portfolio-pipeline-demo"
    Env     = local.env_label
  }
}

resource "aws_subnet" "public" {
  count             = length(var.subnet_cidrs)
  vpc_id            = aws_vpc.pipeline_demo.id
  cidr_block        = var.subnet_cidrs[count.index]
  availability_zone = "${var.aws_region}${local.az_suffixes[count.index]}"

  tags = {
    Project = "investment-portfolio-pipeline-demo"
    Env     = local.env_label
    Name    = "pipeline-${local.env_label}-public-${count.index}"
  }
}

resource "aws_internet_gateway" "pipeline_demo" {
  vpc_id = aws_vpc.pipeline_demo.id

  tags = {
    Project = "investment-portfolio-pipeline-demo"
    Env     = local.env_label
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.pipeline_demo.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.pipeline_demo.id
  }

  tags = {
    Project = "investment-portfolio-pipeline-demo"
    Env     = local.env_label
  }
}

resource "aws_route_table_association" "public" {
  count          = length(var.subnet_cidrs)
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "pipeline_demo" {
  name        = "pipeline-${local.env_label}-tasks"
  description = "Security group for pipeline ECS tasks (${local.env_label})"
  vpc_id      = aws_vpc.pipeline_demo.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all egress (AWS services via IGW, broker APIs)"
  }

  tags = {
    Project = "investment-portfolio-pipeline-demo"
    Env     = local.env_label
  }
}

# ------------------------------------------------------------------------------
# KMS Key for SSM SecureString
# ------------------------------------------------------------------------------

resource "aws_kms_key" "ssm" {
  description             = "KMS key for pipeline SSM SecureString parameters (${local.env_label})"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = {
    Project = "investment-portfolio-pipeline-demo"
    Env     = local.env_label
  }
}

resource "aws_kms_alias" "ssm" {
  name          = "alias/portfolio-pipeline-${local.env_label}-ssm"
  target_key_id = aws_kms_key.ssm.key_id
}

# ------------------------------------------------------------------------------
# SSM Parameter Names (values seeded out-of-band, never in Terraform state)
# Naming convention mirrors DEMO_SECRET_MAP in pipeline/secrets.py:
#   /portfolio/demo/<SECRET>_DEMO
# ------------------------------------------------------------------------------

# IBKR secrets (demo)
resource "aws_ssm_parameter" "ibkr_flex_token_demo" {
  name        = "/portfolio/demo/IBKR_FLEX_TOKEN_DEMO"
  description = "IBKR Flex Token (demo)"
  type        = "SecureString"
  key_id      = aws_kms_key.ssm.key_id
  value       = "PLACEHOLDER"

  lifecycle {
    ignore_changes = [value]
  }

  tags = {
    Project = "investment-portfolio-pipeline-demo"
    Env     = local.env_label
  }
}

resource "aws_ssm_parameter" "ibkr_flex_query_id_demo" {
  name        = "/portfolio/demo/IBKR_FLEX_QUERY_ID_DEMO"
  description = "IBKR Flex Query ID (demo)"
  type        = "SecureString"
  key_id      = aws_kms_key.ssm.key_id
  value       = "PLACEHOLDER"

  lifecycle {
    ignore_changes = [value]
  }

  tags = {
    Project = "investment-portfolio-pipeline-demo"
    Env     = local.env_label
  }
}

# Trading 212 secrets (demo)
resource "aws_ssm_parameter" "t212_api_key_demo" {
  name        = "/portfolio/demo/T212_API_KEY_DEMO"
  description = "Trading 212 API Key (demo)"
  type        = "SecureString"
  key_id      = aws_kms_key.ssm.key_id
  value       = "PLACEHOLDER"

  lifecycle {
    ignore_changes = [value]
  }

  tags = {
    Project = "investment-portfolio-pipeline-demo"
    Env     = local.env_label
  }
}

resource "aws_ssm_parameter" "t212_api_secret_demo" {
  name        = "/portfolio/demo/T212_API_SECRET_DEMO"
  description = "Trading 212 API Secret (demo)"
  type        = "SecureString"
  key_id      = aws_kms_key.ssm.key_id
  value       = "PLACEHOLDER"

  lifecycle {
    ignore_changes = [value]
  }

  tags = {
    Project = "investment-portfolio-pipeline-demo"
    Env     = local.env_label
  }
}

# ENCRYPTION_KEY (demo) — must match the key used to write existing demo Delta tables
resource "aws_ssm_parameter" "encryption_key_demo" {
  name        = "/portfolio/demo/ENCRYPTION_KEY_DEMO"
  description = "Fernet encryption key for Delta table values (demo) — must match existing data"
  type        = "SecureString"
  key_id      = aws_kms_key.ssm.key_id
  value       = "PLACEHOLDER"

  lifecycle {
    ignore_changes = [value]
  }

  tags = {
    Project = "investment-portfolio-pipeline-demo"
    Env     = local.env_label
  }
}

# ------------------------------------------------------------------------------
# ECS Task Definitions (via ecs-task module)
# ------------------------------------------------------------------------------

locals {
  ssm_arns = {
    "/portfolio/demo/IBKR_FLEX_TOKEN_DEMO"    = aws_ssm_parameter.ibkr_flex_token_demo.arn
    "/portfolio/demo/IBKR_FLEX_QUERY_ID_DEMO" = aws_ssm_parameter.ibkr_flex_query_id_demo.arn
    "/portfolio/demo/T212_API_KEY_DEMO"        = aws_ssm_parameter.t212_api_key_demo.arn
    "/portfolio/demo/T212_API_SECRET_DEMO"      = aws_ssm_parameter.t212_api_secret_demo.arn
    "/portfolio/demo/ENCRYPTION_KEY_DEMO"       = aws_ssm_parameter.encryption_key_demo.arn
  }

  connectors_with_arns = {
    for k, v in local.connectors : k => merge(v, {
      secrets = [
        for s in v.secrets : {
          env_var = s.env_var
          arn     = lookup(local.ssm_arns, s.param_name, "")
        }
      ]
    })
  }

  common_environment = {
    DEMO           = "true"
    STORAGE_TYPE    = "cloud"
    S3_BUCKET       = var.bucket_name
    S3_BUCKET_DEMO  = var.bucket_name
    S3_PREFIX_DEMO  = var.s3_prefix
    AWS_REGION      = var.aws_region
  }

  # Log group ARNs for all demo tasks (with :* suffix for log-stream access) —
  # used in the CI/CD IAM policy so the deploy workflow can read container logs
  # when a Step Function execution fails.
  cicd_log_group_arns = concat(
    [for k, v in module.connector_task : "${v.log_group_arn}:*"],
    ["${module.consolidate_allocate.log_group_arn}:*"],
  )
}

module "connector_task" {
  source   = "../modules/ecs-task"
  for_each = local.connectors_with_arns

  name       = each.key
  image      = "${var.ecr_repository_url}:${local.image_tag}"
  demo       = true
  cpu        = 256
  memory     = 512
  command    = each.value.command
  environment = merge(local.common_environment, {
    IBKR_ENABLED = each.key == "ibkr" ? "true" : "false"
    T212_ENABLED = each.key == "trading212" ? "true" : "false"
    XTB_ENABLED  = each.key == "xtb" ? "true" : "false"
  })
  secrets = concat(each.value.secrets, [
    { env_var = "ENCRYPTION_KEY_DEMO", arn = aws_ssm_parameter.encryption_key_demo.arn }
  ])
  bucket_arn    = aws_s3_bucket.pipeline_demo.arn
  s3_prefix     = var.s3_prefix
  ecr_policy_arn = var.ecr_push_pull_policy_arn
  kms_key_arn   = aws_kms_key.ssm.arn
  region        = var.aws_region
}

module "consolidate_allocate" {
  source = "../modules/ecs-task"

  name   = "consolidate-allocate"
  image  = "${var.ecr_repository_url}:${local.image_tag}"
  demo   = true
  cpu    = 256
  memory = 512
  command = ["run-consolidate-allocate", "--target-currency", "EUR"]
  environment = merge(local.common_environment, {
    IBKR_ENABLED = "true"
    T212_ENABLED = "true"
    XTB_ENABLED  = "true"
  })
  secrets = [
    { env_var = "ENCRYPTION_KEY_DEMO", arn = aws_ssm_parameter.encryption_key_demo.arn }
  ]
  bucket_arn    = aws_s3_bucket.pipeline_demo.arn
  s3_prefix     = var.s3_prefix
  ecr_policy_arn = var.ecr_push_pull_policy_arn
  kms_key_arn   = aws_kms_key.ssm.arn
  region        = var.aws_region
}

# ------------------------------------------------------------------------------
# Step Functions IAM Role (from shared infrastructure)
# ------------------------------------------------------------------------------

data "aws_iam_role" "sfn" {
  name = "pipeline-sfn-role"
}

# ------------------------------------------------------------------------------
# Orchestrator (Step Functions state machine + EventBridge triggers)
# ------------------------------------------------------------------------------

module "orchestrator" {
  source = "../modules/orchestrator"

  env                              = local.env_label
  demo                             = true
  ecs_cluster_arn                  = var.ecs_cluster_arn
  subnet_ids                       = aws_subnet.public[*].id
  security_group_ids               = [aws_security_group.pipeline_demo.id]
  task_def_arns                    = { for k, v in module.connector_task : k => v.task_definition_arn }
  consolidate_allocate_task_def_arn = module.consolidate_allocate.task_definition_arn
  sfn_role_arn                     = data.aws_iam_role.sfn.arn
  xtb_staging_bucket_name         = aws_s3_bucket.pipeline_demo.bucket
  xtb_staging_prefix              = "staging_demo/xtb/"
  scheduled                        = false    # no daily schedule for demo
  schedule_cron                    = "cron(0 6 * * ? *)"
  schedule_connectors              = ["ibkr", "trading212"]
  file_arrival_connectors          = ["ibkr", "trading212", "xtb"]
  state_machine_name               = "portfolio-pipeline-orchestrator-demo"
  aws_region                       = var.aws_region
}

# ------------------------------------------------------------------------------
# CI/CD IAM Policy (deploy workflow permissions)
# ------------------------------------------------------------------------------

# The deploy workflow authenticates as the demo IAM user and needs to:
#   - Describe ECS task definitions (to resolve the latest ARN at runtime)
#   - Start Step Functions executions (to trigger the demo orchestrator)
#   - Describe Step Functions executions (to poll for completion status)
#   - Get Step Functions execution history (to diagnose failures)
#   - Read CloudWatch Logs (to print container logs on failure)
# ecs:DescribeTaskDefinition does not support resource-level ARNs, so it must
# be granted on "*". states:StartExecution is scoped to the demo state machine.
# states:DescribeExecution and states:GetExecutionHistory are scoped to
# executions of the demo state machine (ARN differs from stateMachine: to
# execution:). CloudWatch Logs permissions are scoped to demo task log groups.
data "aws_iam_policy_document" "pipeline_demo_cicd" {
  statement {
    sid    = "ECSDescribeTaskDef"
    effect = "Allow"
    actions = [
      "ecs:DescribeTaskDefinition",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "SFNStartExecution"
    effect = "Allow"
    actions = [
      "states:StartExecution",
    ]
    resources = [
      module.orchestrator.state_machine_arn,
    ]
  }

  statement {
    sid    = "SFNDescribeExecution"
    effect = "Allow"
    actions = [
      "states:DescribeExecution",
      "states:GetExecutionHistory",
    ]
    resources = [
      "${replace(module.orchestrator.state_machine_arn, ":stateMachine:", ":execution:")}:*",
    ]
  }

  statement {
    sid    = "CloudWatchLogRead"
    effect = "Allow"
    actions = [
      "logs:FilterLogEvents",
    ]
    resources = local.cicd_log_group_arns
  }
}

resource "aws_iam_policy" "pipeline_demo_cicd" {
  name   = "pipeline-demo-cicd"
  policy = data.aws_iam_policy_document.pipeline_demo_cicd.json
}

resource "aws_iam_user_policy_attachment" "pipeline_demo_cicd" {
  user       = aws_iam_user.pipeline_demo.name
  policy_arn = aws_iam_policy.pipeline_demo_cicd.arn
}

# ------------------------------------------------------------------------------
# Outputs
# ------------------------------------------------------------------------------

output "s3_bucket" {
  description = "S3 bucket name for demo pipeline data."
  value       = aws_s3_bucket.pipeline_demo.bucket
}

output "s3_bucket_arn" {
  description = "ARN of the demo S3 bucket."
  value       = aws_s3_bucket.pipeline_demo.arn
}

output "access_key_id" {
  description = "IAM access key ID (store as GitHub Secret AWS_ACCESS_KEY_ID_DEMO)."
  value       = aws_iam_access_key.pipeline_demo.id
}

output "s3_prefix" {
  description = "S3 key prefix for demo pipeline data."
  value       = var.s3_prefix
}

output "subnet_ids" {
  description = "Public subnet IDs for demo ECS tasks."
  value       = aws_subnet.public[*].id
}

output "security_group_id" {
  description = "Security group ID for demo ECS tasks."
  value       = aws_security_group.pipeline_demo.id
}

output "kms_key_arn" {
  description = "ARN of the KMS key for demo SSM SecureString parameters."
  value       = aws_kms_key.ssm.arn
}

output "connector_task_def_arns" {
  description = "Map of connector name → ECS task definition ARN (demo)."
  value       = { for k, v in module.connector_task : k => v.task_definition_arn }
}

output "consolidate_allocate_task_def_arn" {
  description = "ECS task definition ARN for the consolidate-allocate step (demo)."
  value       = module.consolidate_allocate.task_definition_arn
}

output "state_machine_arn" {
  description = "ARN of the Step Functions orchestrator state machine (demo)."
  value       = module.orchestrator.state_machine_arn
}