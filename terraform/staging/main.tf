# Infrastructure for the investment portfolio pipeline — staging environment.
#
# Creates:
#   - S3 bucket for staging Delta table storage
#   - IAM user with least-privilege access to the staging bucket
#   - IAM access key (store key ID and secret in GitHub Secrets as _STAGING variants)
#   - VPC with public subnets, Internet Gateway, and security group
#   - KMS key for SSM SecureString encryption
#   - SSM parameter names (values seeded out-of-band)
#   - ECS task definitions for each connector + consolidate-allocate
#   - S3 bucket notification for EventBridge
#
# Usage:
#   cd terraform/staging
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
  description = "Globally unique S3 bucket name for staging data."
  type        = string
  default     = "investment-portfolio-pipeline-staging"
}

variable "iam_user_name" {
  description = "Name of the IAM user for staging pipeline access."
  type        = string
  default     = "pipeline-staging"
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
  description = "CIDR block for the staging VPC."
  type        = string
  default     = "10.1.0.0/16"
}

variable "subnet_cidrs" {
  description = "CIDR blocks for public subnets (one per AZ)."
  type        = list(string)
  default     = ["10.1.1.0/24"]
}

# Operational config — set per environment in connectors.auto.tfvars.
# Decision: docs/adr/0107-move-orchestrator-config-to-committed-auto-tfvars.md
variable "scheduled" {
  type = bool
}

variable "schedule_cron" {
  type = string
}

variable "schedule_connectors" {
  type = list(string)
}

variable "file_arrival_connectors" {
  type = list(string)
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
  region  = var.aws_region
  profile = "admin"
}

# Resource renames: private subnets → public subnets (ADR 0054)
moved {
  from = aws_subnet.private
  to   = aws_subnet.public
}

# ------------------------------------------------------------------------------
# State migration (B3): re-adopt renamed resources
# ------------------------------------------------------------------------------
# Terraform moved blocks for the Track B rename. The `from` addresses are the
# pre-rename resource labels — historical references, exempt from the rename
# grep bar. Re-adopting state prevents terraform from planning a destroy + create
# of these resources as orphaned + new pairs.
#
# S3 bucket: the bucket name argument is ForceNew, so the plan still shows a
# bucket replacement even with the move; data is protected by Migration B1, which
# copies every object into the new bucket before this config is applied. Apply to
# staging only — never apply prod.
moved {
  from = aws_s3_bucket.pipeline_demo
  to   = aws_s3_bucket.pipeline_staging
}

moved {
  from = aws_s3_bucket_versioning.pipeline_demo
  to   = aws_s3_bucket_versioning.pipeline_staging
}

moved {
  from = aws_s3_bucket_server_side_encryption_configuration.pipeline_demo
  to   = aws_s3_bucket_server_side_encryption_configuration.pipeline_staging
}

moved {
  from = aws_s3_bucket_public_access_block.pipeline_demo
  to   = aws_s3_bucket_public_access_block.pipeline_staging
}

moved {
  from = aws_s3_bucket_notification.pipeline_demo
  to   = aws_s3_bucket_notification.pipeline_staging
}

moved {
  from = aws_iam_user.pipeline_demo
  to   = aws_iam_user.pipeline_staging
}

moved {
  from = aws_iam_access_key.pipeline_demo
  to   = aws_iam_access_key.pipeline_staging
}

moved {
  from = aws_iam_policy.pipeline_demo
  to   = aws_iam_policy.pipeline_staging
}

moved {
  from = aws_iam_user_policy_attachment.pipeline_demo
  to   = aws_iam_user_policy_attachment.pipeline_staging
}

moved {
  from = aws_vpc.pipeline_demo
  to   = aws_vpc.pipeline_staging
}

moved {
  from = aws_internet_gateway.pipeline_demo
  to   = aws_internet_gateway.pipeline_staging
}

moved {
  from = aws_security_group.pipeline_demo
  to   = aws_security_group.pipeline_staging
}

moved {
  from = aws_iam_policy.pipeline_demo_cicd
  to   = aws_iam_policy.pipeline_staging_cicd
}

moved {
  from = aws_iam_user_policy_attachment.pipeline_demo_cicd
  to   = aws_iam_user_policy_attachment.pipeline_staging_cicd
}

# The state machine (module.orchestrator.aws_sfn_state_machine.orchestrator),
# the ECS task definitions (module.connector_task["*"].aws_ecs_task_definition.task
# and module.consolidate_allocate), and their log groups / task roles keep their
# Terraform addresses — only their AWS-side names changed (staging suffix). Those
# name attributes are ForceNew, so terraform replaces them; none of them carry
# pipeline data (the data lives in the S3 bucket above, which the moves protect).

# ------------------------------------------------------------------------------
# Local values
# ------------------------------------------------------------------------------

locals {
  env_label   = "staging"
  image_tag   = "staging-latest"
  az_suffixes = ["a"]

  # Connector definitions for the ecs-task module for_each.
  # Staging environment uses base env var names (e.g. IBKR_FLEX_TOKEN, not
  # IBKR_FLEX_TOKEN_STAGING).  Environment isolation is provided by the SSM
  # path prefix (/portfolio/staging/ vs /portfolio/prod/), not by env var suffix.
  connectors = {
    ibkr = {
      command = ["run-connector", "ibkr", "--mode", "staging", "--target-currency", "EUR"]
      secrets = [
        { env_var = "IBKR_FLEX_TOKEN", param_name = "/portfolio/staging/IBKR_FLEX_TOKEN" },
        { env_var = "IBKR_FLEX_QUERY_ID", param_name = "/portfolio/staging/IBKR_FLEX_QUERY_ID" },
      ]
    }
    trading212 = {
      command = ["run-connector", "trading212", "--mode", "staging", "--target-currency", "EUR"]
      secrets = [
        { env_var = "T212_API_KEY", param_name = "/portfolio/staging/T212_API_KEY" },
        { env_var = "T212_API_SECRET", param_name = "/portfolio/staging/T212_API_SECRET" },
      ]
    }
    xtb = {
      command = ["run-connector", "xtb", "--mode", "staging", "--target-currency", "EUR"]
      secrets = []
    }
  }
}

# ------------------------------------------------------------------------------
# S3 Bucket
# ------------------------------------------------------------------------------

resource "aws_s3_bucket" "pipeline_staging" {
  bucket = var.bucket_name

  tags = {
    Project = "investment-portfolio-pipeline-staging"
  }
}

resource "aws_s3_bucket_versioning" "pipeline_staging" {
  bucket = aws_s3_bucket.pipeline_staging.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "pipeline_staging" {
  bucket = aws_s3_bucket.pipeline_staging.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "pipeline_staging" {
  bucket = aws_s3_bucket.pipeline_staging.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enable EventBridge notification on the bucket so the XTB file-arrival
# rule in terraform/shared/ can detect uploads.
resource "aws_s3_bucket_notification" "pipeline_staging" {
  bucket = aws_s3_bucket.pipeline_staging.id

  eventbridge = true
}

# ------------------------------------------------------------------------------
# IAM User
# ------------------------------------------------------------------------------

resource "aws_iam_user" "pipeline_staging" {
  name = var.iam_user_name

  tags = {
    Project = "investment-portfolio-pipeline-staging"
  }
}

resource "aws_iam_access_key" "pipeline_staging" {
  user = aws_iam_user.pipeline_staging.name
}

data "aws_iam_policy_document" "pipeline_staging" {
  statement {
    sid    = "ListBucket"
    effect = "Allow"

    actions = [
      "s3:ListBucket",
    ]

    resources = [
      aws_s3_bucket.pipeline_staging.arn,
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
      "${aws_s3_bucket.pipeline_staging.arn}/*",
    ]
  }
}

resource "aws_iam_policy" "pipeline_staging" {
  name   = "pipeline-staging-s3-access"
  policy = data.aws_iam_policy_document.pipeline_staging.json
}

resource "aws_iam_user_policy_attachment" "pipeline_staging" {
  user       = aws_iam_user.pipeline_staging.name
  policy_arn = aws_iam_policy.pipeline_staging.arn
}

# Attach the ECR push/pull policy (defined in terraform/shared/) so the
# staging pipeline user can push Docker images during deploy and pull them at runtime.
# terraform/shared/ must be applied before terraform/staging/.
data "aws_iam_policy" "ecr_push_pull" {
  name = "pipeline-ecr-push-pull"
}

resource "aws_iam_user_policy_attachment" "ecr_push_pull" {
  user       = aws_iam_user.pipeline_staging.name
  policy_arn = data.aws_iam_policy.ecr_push_pull.arn
}

# ------------------------------------------------------------------------------
# VPC
# ------------------------------------------------------------------------------

resource "aws_vpc" "pipeline_staging" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Project = "investment-portfolio-pipeline-staging"
    Env     = local.env_label
  }
}

resource "aws_subnet" "public" {
  count             = length(var.subnet_cidrs)
  vpc_id            = aws_vpc.pipeline_staging.id
  cidr_block        = var.subnet_cidrs[count.index]
  availability_zone = "${var.aws_region}${local.az_suffixes[count.index]}"

  tags = {
    Project = "investment-portfolio-pipeline-staging"
    Env     = local.env_label
    Name    = "pipeline-${local.env_label}-public-${count.index}"
  }
}

resource "aws_internet_gateway" "pipeline_staging" {
  vpc_id = aws_vpc.pipeline_staging.id

  tags = {
    Project = "investment-portfolio-pipeline-staging"
    Env     = local.env_label
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.pipeline_staging.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.pipeline_staging.id
  }

  tags = {
    Project = "investment-portfolio-pipeline-staging"
    Env     = local.env_label
  }
}

resource "aws_route_table_association" "public" {
  count          = length(var.subnet_cidrs)
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "pipeline_staging" {
  name        = "pipeline-${local.env_label}-tasks"
  description = "Security group for pipeline ECS tasks (${local.env_label})"
  vpc_id      = aws_vpc.pipeline_staging.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all egress (AWS services via IGW, broker APIs)"
  }

  tags = {
    Project = "investment-portfolio-pipeline-staging"
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
    Project = "investment-portfolio-pipeline-staging"
    Env     = local.env_label
  }
}

resource "aws_kms_alias" "ssm" {
  name          = "alias/portfolio-pipeline-${local.env_label}-ssm"
  target_key_id = aws_kms_key.ssm.key_id
}

# ------------------------------------------------------------------------------
# SSM Parameter Names (values seeded out-of-band, never in Terraform state)
# Naming convention: /portfolio/staging/<SECRET> (no _STAGING suffix — environment
# isolation is provided by the SSM path prefix, not by env var suffixes).
# ------------------------------------------------------------------------------

# IBKR secrets (staging)
resource "aws_ssm_parameter" "ibkr_flex_token" {
  name        = "/portfolio/staging/IBKR_FLEX_TOKEN"
  description = "IBKR Flex Token (staging)"
  type        = "SecureString"
  key_id      = aws_kms_key.ssm.key_id
  value       = "PLACEHOLDER"

  lifecycle {
    ignore_changes = [value]
  }

  tags = {
    Project = "investment-portfolio-pipeline-staging"
    Env     = local.env_label
  }
}

resource "aws_ssm_parameter" "ibkr_flex_query_id" {
  name        = "/portfolio/staging/IBKR_FLEX_QUERY_ID"
  description = "IBKR Flex Query ID (staging)"
  type        = "SecureString"
  key_id      = aws_kms_key.ssm.key_id
  value       = "PLACEHOLDER"

  lifecycle {
    ignore_changes = [value]
  }

  tags = {
    Project = "investment-portfolio-pipeline-staging"
    Env     = local.env_label
  }
}

# Trading 212 secrets (staging)
resource "aws_ssm_parameter" "t212_api_key" {
  name        = "/portfolio/staging/T212_API_KEY"
  description = "Trading 212 API Key (staging)"
  type        = "SecureString"
  key_id      = aws_kms_key.ssm.key_id
  value       = "PLACEHOLDER"

  lifecycle {
    ignore_changes = [value]
  }

  tags = {
    Project = "investment-portfolio-pipeline-staging"
    Env     = local.env_label
  }
}

resource "aws_ssm_parameter" "t212_api_secret" {
  name        = "/portfolio/staging/T212_API_SECRET"
  description = "Trading 212 API Secret (staging)"
  type        = "SecureString"
  key_id      = aws_kms_key.ssm.key_id
  value       = "PLACEHOLDER"

  lifecycle {
    ignore_changes = [value]
  }

  tags = {
    Project = "investment-portfolio-pipeline-staging"
    Env     = local.env_label
  }
}

# ENCRYPTION_KEY (staging) — must match the key used to write existing staging Delta tables
resource "aws_ssm_parameter" "encryption_key" {
  name        = "/portfolio/staging/ENCRYPTION_KEY"
  description = "Fernet encryption key for Delta table values (staging) — must match existing data"
  type        = "SecureString"
  key_id      = aws_kms_key.ssm.key_id
  value       = "PLACEHOLDER"

  lifecycle {
    ignore_changes = [value]
  }

  tags = {
    Project = "investment-portfolio-pipeline-staging"
    Env     = local.env_label
  }
}

# ------------------------------------------------------------------------------
# ECS Task Definitions (via ecs-task module)
# ------------------------------------------------------------------------------

locals {
  ssm_arns = {
    "/portfolio/staging/IBKR_FLEX_TOKEN"    = aws_ssm_parameter.ibkr_flex_token.arn
    "/portfolio/staging/IBKR_FLEX_QUERY_ID" = aws_ssm_parameter.ibkr_flex_query_id.arn
    "/portfolio/staging/T212_API_KEY"       = aws_ssm_parameter.t212_api_key.arn
    "/portfolio/staging/T212_API_SECRET"    = aws_ssm_parameter.t212_api_secret.arn
    "/portfolio/staging/ENCRYPTION_KEY"     = aws_ssm_parameter.encryption_key.arn
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
    S3_BUCKET  = var.bucket_name
    AWS_REGION = var.aws_region
  }

  # Log group ARNs for all staging tasks (with :* suffix for log-stream access) —
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

  name        = each.key
  image       = "${var.ecr_repository_url}:${local.image_tag}"
  staging     = true
  cpu         = 256
  memory      = 512
  command     = each.value.command
  environment = local.common_environment
  secrets = concat(each.value.secrets, [
    { env_var = "ENCRYPTION_KEY", arn = aws_ssm_parameter.encryption_key.arn }
  ])
  bucket_arn     = aws_s3_bucket.pipeline_staging.arn
  ecr_policy_arn = var.ecr_push_pull_policy_arn
  kms_key_arn    = aws_kms_key.ssm.arn
  region         = var.aws_region
}

module "consolidate_allocate" {
  source = "../modules/ecs-task"

  name        = "consolidate-allocate"
  image       = "${var.ecr_repository_url}:${local.image_tag}"
  staging     = true
  cpu         = 256
  memory      = 512
  command     = ["run-consolidate-analytics", "--mode", "staging", "--target-currency", "EUR"]
  environment = local.common_environment
  secrets = [
    { env_var = "ENCRYPTION_KEY", arn = aws_ssm_parameter.encryption_key.arn }
  ]
  bucket_arn     = aws_s3_bucket.pipeline_staging.arn
  ecr_policy_arn = var.ecr_push_pull_policy_arn
  kms_key_arn    = aws_kms_key.ssm.arn
  region         = var.aws_region
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

  env                               = local.env_label
  staging                           = true
  ecs_cluster_arn                   = var.ecs_cluster_arn
  subnet_ids                        = aws_subnet.public[*].id
  security_group_ids                = [aws_security_group.pipeline_staging.id]
  task_def_arns                     = { for k, v in module.connector_task : k => v.task_definition_arn }
  consolidate_allocate_task_def_arn = module.consolidate_allocate.task_definition_arn
  sfn_role_arn                      = data.aws_iam_role.sfn.arn
  xtb_staging_bucket_name           = aws_s3_bucket.pipeline_staging.bucket
  xtb_staging_prefix                = "xtb_uploads/"
  scheduled                         = var.scheduled
  schedule_cron                     = var.schedule_cron
  schedule_connectors               = var.schedule_connectors
  file_arrival_connectors           = var.file_arrival_connectors
  state_machine_name                = "portfolio-pipeline-orchestrator-staging"
  aws_region                        = var.aws_region
}

# ------------------------------------------------------------------------------
# CI/CD IAM Policy (deploy workflow permissions)
# ------------------------------------------------------------------------------

# The deploy workflow authenticates as the staging IAM user and needs to:
#   - Describe ECS task definitions (to resolve the latest ARN at runtime)
#   - Start Step Functions executions (to trigger the staging orchestrator)
#   - Describe Step Functions executions (to poll for completion status)
#   - Get Step Functions execution history (to diagnose failures)
#   - Read CloudWatch Logs (to print container logs on failure)
# ecs:DescribeTaskDefinition does not support resource-level ARNs, so it must
# be granted on "*". states:StartExecution is scoped to the staging state machine.
# states:DescribeExecution and states:GetExecutionHistory are scoped to
# executions of the staging state machine (ARN differs from stateMachine: to
# execution:). CloudWatch Logs permissions are scoped to staging task log groups.
data "aws_iam_policy_document" "pipeline_staging_cicd" {
  statement {
    sid    = "ECSDescribeTaskDef"
    effect = "Allow"
    actions = [
      "ecs:DescribeTaskDefinition",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "SFNListStateMachines"
    effect = "Allow"
    actions = [
      "states:ListStateMachines",
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

resource "aws_iam_policy" "pipeline_staging_cicd" {
  name   = "pipeline-staging-cicd"
  policy = data.aws_iam_policy_document.pipeline_staging_cicd.json
}

resource "aws_iam_user_policy_attachment" "pipeline_staging_cicd" {
  user       = aws_iam_user.pipeline_staging.name
  policy_arn = aws_iam_policy.pipeline_staging_cicd.arn
}

# ------------------------------------------------------------------------------
# Outputs
# ------------------------------------------------------------------------------

output "s3_bucket" {
  description = "S3 bucket name for staging pipeline data."
  value       = aws_s3_bucket.pipeline_staging.bucket
}

output "s3_bucket_arn" {
  description = "ARN of the staging S3 bucket."
  value       = aws_s3_bucket.pipeline_staging.arn
}

output "access_key_id" {
  description = "IAM access key ID (store as GitHub Secret AWS_ACCESS_KEY_ID_STAGING)."
  value       = aws_iam_access_key.pipeline_staging.id
}

output "subnet_ids" {
  description = "Public subnet IDs for staging ECS tasks."
  value       = aws_subnet.public[*].id
}

output "security_group_id" {
  description = "Security group ID for staging ECS tasks."
  value       = aws_security_group.pipeline_staging.id
}

output "kms_key_arn" {
  description = "ARN of the KMS key for staging SSM SecureString parameters."
  value       = aws_kms_key.ssm.arn
}

output "connector_task_def_arns" {
  description = "Map of connector name → ECS task definition ARN (staging)."
  value       = { for k, v in module.connector_task : k => v.task_definition_arn }
}

output "consolidate_allocate_task_def_arn" {
  description = "ECS task definition ARN for the consolidate-allocate step (staging)."
  value       = module.consolidate_allocate.task_definition_arn
}

output "state_machine_arn" {
  description = "ARN of the Step Functions orchestrator state machine (staging)."
  value       = module.orchestrator.state_machine_arn
}