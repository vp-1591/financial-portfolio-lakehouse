# Infrastructure for the investment portfolio pipeline — demo environment.
#
# Creates:
#   - S3 bucket for demo Delta table storage
#   - IAM user with least-privilege access to the demo bucket
#   - IAM access key (store key ID and secret in GitHub Secrets as _DEMO variants)
#   - VPC with private subnets, security group, and VPC endpoints
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
  description = "CIDR blocks for private subnets (one per AZ)."
  type        = list(string)
  default     = ["10.1.1.0/24", "10.1.2.0/24", "10.1.3.0/24"]
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
  env_label = "demo"
  image_tag = "staging-latest"

  # Connector definitions for the ecs-task module for_each.
  # Demo environment uses _DEMO-suffixed SSM parameter names, mirroring
  # DEMO_SECRET_MAP in pipeline/secrets.py.
  connectors = {
    ibkr = {
      command = ["run-connector", "ibkr", "--target-currency", "EUR"]
      secrets = [
        { env_var = "IBKR_FLEX_TOKEN",   param_name = "/portfolio/demo/IBKR_FLEX_TOKEN_DEMO" },
        { env_var = "IBKR_FLEX_QUERY_ID", param_name = "/portfolio/demo/IBKR_FLEX_QUERY_ID_DEMO" },
      ]
    }
    trading212 = {
      command = ["run-connector", "trading212", "--target-currency", "EUR"]
      secrets = [
        { env_var = "T212_API_KEY",    param_name = "/portfolio/demo/T212_API_KEY_DEMO" },
        { env_var = "T212_API_SECRET",  param_name = "/portfolio/demo/T212_API_SECRET_DEMO" },
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

  eventbridge {}
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

resource "aws_subnet" "private" {
  count             = length(var.subnet_cidrs)
  vpc_id            = aws_vpc.pipeline_demo.id
  cidr_block        = var.subnet_cidrs[count.index]
  availability_zone = "${var.aws_region}${chr(97 + count.index)}"

  tags = {
    Project = "investment-portfolio-pipeline-demo"
    Env     = local.env_label
    Name    = "pipeline-${local.env_label}-private-${count.index}"
  }
}

resource "aws_security_group" "pipeline_demo" {
  name        = "pipeline-${local.env_label}-tasks"
  description = "Security group for pipeline ECS tasks (${local.env_label})"
  vpc_id      = aws_vpc.pipeline_demo.id

  # Egress for VPC endpoints (S3, ECR, CloudWatch, SSM)
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all egress for VPC endpoint traffic (S3, ECR, CloudWatch, SSM)"
  }

  tags = {
    Project = "investment-portfolio-pipeline-demo"
    Env     = local.env_label
  }
}

# VPC Gateway Endpoint for S3 (free)
resource "aws_vpc_endpoint" "s3" {
  vpc_id       = aws_vpc.pipeline_demo.id
  service_name = "com.amazonaws.${var.aws_region}.s3"
  route_table_ids = [
    for subnet in aws_subnet.private : aws_vpc.pipeline_demo.default_route_table_id
  ]

  tags = {
    Project = "investment-portfolio-pipeline-demo"
    Env     = local.env_label
  }
}

# VPC Interface Endpoints
resource "aws_vpc_endpoint" "ecr_api" {
  vpc_id             = aws_vpc.pipeline_demo.id
  service_name       = "com.amazonaws.${var.aws_region}.ecr.api"
  vpc_endpoint_type  = "Interface"
  private_dns_enabled = true
  subnet_ids         = aws_subnet.private[*].id
  security_group_ids = [aws_security_group.pipeline_demo.id]

  tags = {
    Project = "investment-portfolio-pipeline-demo"
    Env     = local.env_label
  }
}

resource "aws_vpc_endpoint" "ecr_dkr" {
  vpc_id             = aws_vpc.pipeline_demo.id
  service_name       = "com.amazonaws.${var.aws_region}.ecr.dkr"
  vpc_endpoint_type  = "Interface"
  private_dns_enabled = true
  subnet_ids         = aws_subnet.private[*].id
  security_group_ids = [aws_security_group.pipeline_demo.id]

  tags = {
    Project = "investment-portfolio-pipeline-demo"
    Env     = local.env_label
  }
}

resource "aws_vpc_endpoint" "logs" {
  vpc_id             = aws_vpc.pipeline_demo.id
  service_name       = "com.amazonaws.${var.aws_region}.logs"
  vpc_endpoint_type  = "Interface"
  private_dns_enabled = true
  subnet_ids         = aws_subnet.private[*].id
  security_group_ids = [aws_security_group.pipeline_demo.id]

  tags = {
    Project = "investment-portfolio-pipeline-demo"
    Env     = local.env_label
  }
}

resource "aws_vpc_endpoint" "ssm" {
  vpc_id             = aws_vpc.pipeline_demo.id
  service_name       = "com.amazonaws.${var.aws_region}.ssm"
  vpc_endpoint_type  = "Interface"
  private_dns_enabled = true
  subnet_ids         = aws_subnet.private[*].id
  security_group_ids = [aws_security_group.pipeline_demo.id]

  tags = {
    Project = "investment-portfolio-pipeline-demo"
    Env     = local.env_label
  }
}

resource "aws_vpc_endpoint" "ssm_messages" {
  vpc_id             = aws_vpc.pipeline_demo.id
  service_name       = "com.amazonaws.${var.aws_region}.ssmmessages"
  vpc_endpoint_type  = "Interface"
  private_dns_enabled = true
  subnet_ids         = aws_subnet.private[*].id
  security_group_ids = [aws_security_group.pipeline_demo.id]

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
    DEMO         = "true"
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
    { env_var = "ENCRYPTION_KEY", arn = aws_ssm_parameter.encryption_key_demo.arn }
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
    { env_var = "ENCRYPTION_KEY", arn = aws_ssm_parameter.encryption_key_demo.arn }
  ]
  bucket_arn    = aws_s3_bucket.pipeline_demo.arn
  s3_prefix     = var.s3_prefix
  ecr_policy_arn = var.ecr_push_pull_policy_arn
  kms_key_arn   = aws_kms_key.ssm.arn
  region        = var.aws_region
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
  description = "Private subnet IDs for demo ECS tasks."
  value       = aws_subnet.private[*].id
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