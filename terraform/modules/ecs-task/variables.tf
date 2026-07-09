# Variables for the ecs-task module.

variable "name" {
  description = "Connector name (e.g. ibkr, trading212, xtb, consolidate-allocate)."
  type        = string
}

variable "image" {
  description = "Docker image URI for the pipeline container."
  type        = string
}

variable "demo" {
  description = "Whether this is the demo environment."
  type        = bool
  default     = false
}

variable "cpu" {
  description = "CPU units for the ECS task (1024 = 1 vCPU)."
  type        = number
  default     = 256
}

variable "memory" {
  description = "Memory (MiB) for the ECS task."
  type        = number
  default     = 512
}

variable "command" {
  description = "Container command override (e.g. [\"run-connector\", \"ibkr\"])."
  type        = list(string)
}

variable "environment" {
  description = "Environment variables to set on the container."
  type        = map(string)
  default     = {}
}

variable "secrets" {
  description = "SSM secrets to inject. Each object has env_var (name) and arn (SSM parameter ARN)."
  type = list(object({
    env_var = string
    arn     = string
  }))
  default = []
}

variable "bucket_arn" {
  description = "ARN of the S3 bucket for pipeline data."
  type        = string
}

variable "s3_prefix" {
  description = "S3 key prefix for pipeline data within the bucket."
  type        = string
}

variable "ecr_policy_arn" {
  description = "ARN of the shared ECR push/pull IAM policy."
  type        = string
}

variable "kms_key_arn" {
  description = "ARN of the KMS key for decrypting SSM SecureString parameters."
  type        = string
}

variable "region" {
  description = "AWS region."
  type        = string
  default     = "eu-west-1"
}

variable "task_role_arn" {
  description = "ARN of a shared task role. If null, the module creates its own task role scoped to bucket/prefix."
  type        = string
  default     = null
}