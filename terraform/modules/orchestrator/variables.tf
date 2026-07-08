# Variables for the orchestrator module.
#
# This module creates a Step Functions state machine and optional EventBridge
# triggers (daily schedule + S3 file arrival) for a single environment (demo or
# prod). The ASL definition is identical across environments — only the input
# values differ (subnets, task def ARNs, demo flag, bucket name).

variable "env" {
  description = "Environment label (prod or demo) used in naming and execution input."
  type        = string
}

variable "demo" {
  description = "Whether this is the demo environment (passed into the execution input)."
  type        = bool
}

variable "ecs_cluster_arn" {
  description = "ARN of the ECS cluster (from terraform/shared outputs)."
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for ECS tasks (from this environment's VPC)."
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security group IDs for ECS tasks (from this environment's VPC)."
  type        = list(string)
}

variable "task_def_arns" {
  description = "Map of connector name → ECS task definition ARN (from this environment's ecs-task modules)."
  type        = map(string)
}

variable "consolidate_allocate_task_def_arn" {
  description = "ECS task definition ARN for the consolidate-allocate step."
  type        = string
}

variable "sfn_role_arn" {
  description = "ARN of the Step Functions IAM role (pipeline-sfn-role, created in shared/)."
  type        = string
}

variable "xtb_staging_bucket_name" {
  description = "S3 bucket name for XTB staging (the bucket that receives XTB uploads)."
  type        = string
}

variable "xtb_staging_prefix" {
  description = "S3 key prefix for XTB staging within the bucket."
  type        = string
  default     = "staging/xtb/"
}

variable "scheduled" {
  description = "Whether to create an EventBridge daily-schedule trigger for the orchestrator."
  type        = bool
  default     = false
}

variable "schedule_cron" {
  description = "Cron expression for the daily schedule EventBridge rule."
  type        = string
  default     = "cron(0 6 * * ? *)"
}

variable "schedule_connectors" {
  description = "Connector names included in the daily-schedule execution input (no xtb_file)."
  type        = list(string)
  default     = ["ibkr", "trading212"]
}

variable "file_arrival_connectors" {
  description = "Connector names included in the XTB file-arrival execution input."
  type        = list(string)
  default     = ["ibkr", "trading212", "xtb"]
}

variable "state_machine_name" {
  description = "Name of the Step Functions orchestrator state machine."
  type        = string
}

variable "aws_region" {
  description = "AWS region (used for EventBridge IAM policy resource ARNs)."
  type        = string
  default     = "eu-west-1"
}