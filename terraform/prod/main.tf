# Infrastructure for the investment portfolio pipeline.
#
# Creates:
#   - S3 bucket for Delta table storage
#   - IAM user with least-privilege access to the bucket
#   - IAM access key (store key ID and secret in GitHub Secrets)
#   - VPC with private subnets, security group, and VPC endpoints
#   - KMS key for SSM SecureString encryption
#   - SSM parameter names (values seeded out-of-band)
#   - ECS task definitions for each connector + consolidate-allocate
#   - S3 bucket notification for EventBridge
#
# Usage:
#   cd terraform
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
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.0.0.0/16"
}

variable "subnet_cidrs" {
  description = "CIDR blocks for private subnets (one per AZ)."
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
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
# Local values
# ------------------------------------------------------------------------------

locals {
  env_label   = "prod"
  image_tag   = "production-latest"
  az_suffixes = ["a", "b", "c"]

  # Connector definitions for the ecs-task module for_each.
  # Each connector specifies its command and which SSM secrets it needs.
  # Adding a connector = one entry here + SSM params.
  connectors = {
    ibkr = {
      command = ["run-connector", "ibkr", "--target-currency", "EUR"]
      secrets = [
        { env_var = "IBKR_FLEX_TOKEN",   param_name = "/portfolio/prod/IBKR_FLEX_TOKEN" },
        { env_var = "IBKR_FLEX_QUERY_ID", param_name = "/portfolio/prod/IBKR_FLEX_QUERY_ID" },
      ]
    }
    trading212 = {
      command = ["run-connector", "trading212", "--target-currency", "EUR"]
      secrets = [
        { env_var = "T212_API_KEY",    param_name = "/portfolio/prod/T212_API_KEY" },
        { env_var = "T212_API_SECRET",  param_name = "/portfolio/prod/T212_API_SECRET" },
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

# Enable EventBridge notification on the bucket so the XTB file-arrival
# rule in terraform/shared/ can detect uploads.
resource "aws_s3_bucket_notification" "pipeline" {
  bucket = aws_s3_bucket.pipeline.id

  eventbridge = true
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

# Attach the ECR push/pull policy (defined in terraform/shared/) so the
# pipeline user can push Docker images during deploy and pull them at runtime.
# terraform/shared/ must be applied before terraform/prod/.
data "aws_iam_policy" "ecr_push_pull" {
  name = "pipeline-ecr-push-pull"
}

resource "aws_iam_user_policy_attachment" "ecr_push_pull" {
  user       = aws_iam_user.pipeline.name
  policy_arn = data.aws_iam_policy.ecr_push_pull.arn
}

# ------------------------------------------------------------------------------
# VPC
# ------------------------------------------------------------------------------

resource "aws_vpc" "pipeline" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Project = "investment-portfolio-pipeline"
    Env     = local.env_label
  }
}

resource "aws_subnet" "private" {
  count             = length(var.subnet_cidrs)
  vpc_id            = aws_vpc.pipeline.id
  cidr_block        = var.subnet_cidrs[count.index]
  availability_zone = "${var.aws_region}${local.az_suffixes[count.index]}"

  tags = {
    Project = "investment-portfolio-pipeline"
    Env     = local.env_label
    Name    = "pipeline-${local.env_label}-private-${count.index}"
  }
}

resource "aws_security_group" "pipeline" {
  name        = "pipeline-${local.env_label}-tasks"
  description = "Security group for pipeline ECS tasks (${local.env_label})"
  vpc_id      = aws_vpc.pipeline.id

  # Egress to VPC endpoints (no public internet access)
  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.pipeline.cidr_block]
  }

  # Egress to S3 via VPC endpoint (handled by Gateway endpoint, no SG rule needed,
  # but we allow DNS resolution through the VPC)
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all egress for VPC endpoint traffic (S3, ECR, CloudWatch)"
  }

  tags = {
    Project = "investment-portfolio-pipeline"
    Env     = local.env_label
  }
}

# VPC Gateway Endpoint for S3 (free, uses route tables)
resource "aws_vpc_endpoint" "s3" {
  vpc_id       = aws_vpc.pipeline.id
  service_name = "com.amazonaws.${var.aws_region}.s3"
  route_table_ids = [
    for subnet in aws_subnet.private : aws_vpc.pipeline.default_route_table_id
  ]

  tags = {
    Project = "investment-portfolio-pipeline"
    Env     = local.env_label
  }
}

# VPC Interface Endpoints for ECR API, ECR DKR, CloudWatch Logs, and SSM
# (required for Fargate tasks in private subnets with no public IP)
resource "aws_vpc_endpoint" "ecr_api" {
  vpc_id             = aws_vpc.pipeline.id
  service_name       = "com.amazonaws.${var.aws_region}.ecr.api"
  vpc_endpoint_type  = "Interface"
  private_dns_enabled = true
  subnet_ids         = aws_subnet.private[*].id
  security_group_ids = [aws_security_group.pipeline.id]

  tags = {
    Project = "investment-portfolio-pipeline"
    Env     = local.env_label
  }
}

resource "aws_vpc_endpoint" "ecr_dkr" {
  vpc_id             = aws_vpc.pipeline.id
  service_name       = "com.amazonaws.${var.aws_region}.ecr.dkr"
  vpc_endpoint_type  = "Interface"
  private_dns_enabled = true
  subnet_ids         = aws_subnet.private[*].id
  security_group_ids = [aws_security_group.pipeline.id]

  tags = {
    Project = "investment-portfolio-pipeline"
    Env     = local.env_label
  }
}

resource "aws_vpc_endpoint" "logs" {
  vpc_id             = aws_vpc.pipeline.id
  service_name       = "com.amazonaws.${var.aws_region}.logs"
  vpc_endpoint_type  = "Interface"
  private_dns_enabled = true
  subnet_ids         = aws_subnet.private[*].id
  security_group_ids = [aws_security_group.pipeline.id]

  tags = {
    Project = "investment-portfolio-pipeline"
    Env     = local.env_label
  }
}

resource "aws_vpc_endpoint" "ssm" {
  vpc_id             = aws_vpc.pipeline.id
  service_name       = "com.amazonaws.${var.aws_region}.ssm"
  vpc_endpoint_type  = "Interface"
  private_dns_enabled = true
  subnet_ids         = aws_subnet.private[*].id
  security_group_ids = [aws_security_group.pipeline.id]

  tags = {
    Project = "investment-portfolio-pipeline"
    Env     = local.env_label
  }
}

resource "aws_vpc_endpoint" "ssm_messages" {
  vpc_id             = aws_vpc.pipeline.id
  service_name       = "com.amazonaws.${var.aws_region}.ssmmessages"
  vpc_endpoint_type  = "Interface"
  private_dns_enabled = true
  subnet_ids         = aws_subnet.private[*].id
  security_group_ids = [aws_security_group.pipeline.id]

  tags = {
    Project = "investment-portfolio-pipeline"
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
    Project = "investment-portfolio-pipeline"
    Env     = local.env_label
  }
}

resource "aws_kms_alias" "ssm" {
  name          = "alias/portfolio-pipeline-${local.env_label}-ssm"
  target_key_id = aws_kms_key.ssm.key_id
}

# ------------------------------------------------------------------------------
# SSM Parameter Names (values seeded out-of-band, never in Terraform state)
# ------------------------------------------------------------------------------

# IBKR secrets
resource "aws_ssm_parameter" "ibkr_flex_token" {
  name        = "/portfolio/prod/IBKR_FLEX_TOKEN"
  description = "IBKR Flex Token (production)"
  type        = "SecureString"
  key_id      = aws_kms_key.ssm.key_id
  value       = "PLACEHOLDER" # Seed out-of-band: aws ssm put-parameter --name /portfolio/prod/IBKR_FLEX_TOKEN --value "..." --type SecureString --key-id <kms-key-id>

  lifecycle {
    ignore_changes = [value]
  }

  tags = {
    Project = "investment-portfolio-pipeline"
    Env     = local.env_label
  }
}

resource "aws_ssm_parameter" "ibkr_flex_query_id" {
  name        = "/portfolio/prod/IBKR_FLEX_QUERY_ID"
  description = "IBKR Flex Query ID (production)"
  type        = "SecureString"
  key_id      = aws_kms_key.ssm.key_id
  value       = "PLACEHOLDER"

  lifecycle {
    ignore_changes = [value]
  }

  tags = {
    Project = "investment-portfolio-pipeline"
    Env     = local.env_label
  }
}

# Trading 212 secrets
resource "aws_ssm_parameter" "t212_api_key" {
  name        = "/portfolio/prod/T212_API_KEY"
  description = "Trading 212 API Key (production)"
  type        = "SecureString"
  key_id      = aws_kms_key.ssm.key_id
  value       = "PLACEHOLDER"

  lifecycle {
    ignore_changes = [value]
  }

  tags = {
    Project = "investment-portfolio-pipeline"
    Env     = local.env_label
  }
}

resource "aws_ssm_parameter" "t212_api_secret" {
  name        = "/portfolio/prod/T212_API_SECRET"
  description = "Trading 212 API Secret (production)"
  type        = "SecureString"
  key_id      = aws_kms_key.ssm.key_id
  value       = "PLACEHOLDER"

  lifecycle {
    ignore_changes = [value]
  }

  tags = {
    Project = "investment-portfolio-pipeline"
    Env     = local.env_label
  }
}

# ENCRYPTION_KEY — must match the key used to write existing raw Delta tables
resource "aws_ssm_parameter" "encryption_key" {
  name        = "/portfolio/prod/ENCRYPTION_KEY"
  description = "Fernet encryption key for Delta table values (production) — must match existing data"
  type        = "SecureString"
  key_id      = aws_kms_key.ssm.key_id
  value       = "PLACEHOLDER"

  lifecycle {
    ignore_changes = [value]
  }

  tags = {
    Project = "investment-portfolio-pipeline"
    Env     = local.env_label
  }
}

# ------------------------------------------------------------------------------
# ECS Task Definitions (via ecs-task module)
# ------------------------------------------------------------------------------

# Flatten the secrets list for each connector to include SSM ARNs
locals {
  # Map SSM parameter names to their ARNs for the module
  ssm_arns = {
    "/portfolio/prod/IBKR_FLEX_TOKEN"    = aws_ssm_parameter.ibkr_flex_token.arn
    "/portfolio/prod/IBKR_FLEX_QUERY_ID" = aws_ssm_parameter.ibkr_flex_query_id.arn
    "/portfolio/prod/T212_API_KEY"        = aws_ssm_parameter.t212_api_key.arn
    "/portfolio/prod/T212_API_SECRET"      = aws_ssm_parameter.t212_api_secret.arn
    "/portfolio/prod/ENCRYPTION_KEY"       = aws_ssm_parameter.encryption_key.arn
  }

  # Build the connector map with resolved SSM ARNs for the module for_each
  connectors_with_arns = {
    for k, v in local.connectors : k => merge(v, {
      secrets = [
        for s in v.secrets : {
          env_var = s.env_var
          arn     = lookup(local.ssm_arns, s.param_name, "")
        }
      ]
      # Add ENCRYPTION_KEY to all connectors
    })
  }

  # Common environment variables for all connector task definitions
  common_environment = {
    DEMO         = "false"
    STORAGE_TYPE = "cloud"
    S3_BUCKET    = var.bucket_name
    AWS_REGION   = var.aws_region
  }
}

module "connector_task" {
  source   = "../modules/ecs-task"
  for_each = local.connectors_with_arns

  name       = each.key
  image      = "${var.ecr_repository_url}:${local.image_tag}"
  demo       = false
  cpu        = 256
  memory     = 512
  command    = each.value.command
  environment = merge(local.common_environment, {
    IBKR_ENABLED = each.key == "ibkr" ? "true" : "false"
    T212_ENABLED = each.key == "trading212" ? "true" : "false"
    XTB_ENABLED  = each.key == "xtb" ? "true" : "false"
  })
  secrets = concat(each.value.secrets, [
    { env_var = "ENCRYPTION_KEY", arn = aws_ssm_parameter.encryption_key.arn }
  ])
  bucket_arn    = aws_s3_bucket.pipeline.arn
  s3_prefix     = var.s3_prefix
  ecr_policy_arn = var.ecr_push_pull_policy_arn
  kms_key_arn   = aws_kms_key.ssm.arn
  region        = var.aws_region
}

# Consolidate-allocate task definition
module "consolidate_allocate" {
  source = "../modules/ecs-task"

  name   = "consolidate-allocate"
  image  = "${var.ecr_repository_url}:${local.image_tag}"
  demo   = false
  cpu    = 256
  memory = 512
  command = ["run-consolidate-allocate", "--target-currency", "EUR"]
  environment = merge(local.common_environment, {
    IBKR_ENABLED = "true"
    T212_ENABLED = "true"
    XTB_ENABLED  = "true"
  })
  secrets = [
    { env_var = "ENCRYPTION_KEY", arn = aws_ssm_parameter.encryption_key.arn }
  ]
  bucket_arn    = aws_s3_bucket.pipeline.arn
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
  demo                             = false
  ecs_cluster_arn                  = var.ecs_cluster_arn
  subnet_ids                       = aws_subnet.private[*].id
  security_group_ids               = [aws_security_group.pipeline.id]
  task_def_arns                    = { for k, v in module.connector_task : k => v.task_definition_arn }
  consolidate_allocate_task_def_arn = module.consolidate_allocate.task_definition_arn
  sfn_role_arn                     = data.aws_iam_role.sfn.arn
  xtb_staging_bucket_name         = aws_s3_bucket.pipeline.bucket
  xtb_staging_prefix              = "staging/xtb/"
  scheduled                        = true     # daily schedule for prod
  schedule_cron                    = "cron(0 6 * * ? *)"
  schedule_connectors              = ["ibkr", "trading212"]
  file_arrival_connectors          = ["ibkr", "trading212", "xtb"]
  state_machine_name               = "portfolio-pipeline-orchestrator"
  aws_region                       = var.aws_region
}

# ------------------------------------------------------------------------------
# Outputs
# ------------------------------------------------------------------------------

output "s3_bucket" {
  description = "S3 bucket name for pipeline data."
  value       = aws_s3_bucket.pipeline.bucket
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket."
  value       = aws_s3_bucket.pipeline.arn
}

output "access_key_id" {
  description = "IAM access key ID (store as GitHub Secret AWS_ACCESS_KEY_ID)."
  value       = aws_iam_access_key.pipeline.id
}

output "s3_prefix" {
  description = "S3 key prefix for pipeline data."
  value       = var.s3_prefix
}

output "subnet_ids" {
  description = "Private subnet IDs for ECS tasks."
  value       = aws_subnet.private[*].id
}

output "security_group_id" {
  description = "Security group ID for ECS tasks."
  value       = aws_security_group.pipeline.id
}

output "kms_key_arn" {
  description = "ARN of the KMS key for SSM SecureString parameters."
  value       = aws_kms_key.ssm.arn
}

output "connector_task_def_arns" {
  description = "Map of connector name → ECS task definition ARN."
  value       = { for k, v in module.connector_task : k => v.task_definition_arn }
}

output "consolidate_allocate_task_def_arn" {
  description = "ECS task definition ARN for the consolidate-allocate step."
  value       = module.consolidate_allocate.task_definition_arn
}

output "state_machine_arn" {
  description = "ARN of the Step Functions orchestrator state machine."
  value       = module.orchestrator.state_machine_arn
}