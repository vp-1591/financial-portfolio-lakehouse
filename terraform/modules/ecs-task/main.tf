# ECS task definition module for the investment portfolio pipeline.
#
# Produces:
#   - aws_ecs_task_definition (Fargate, awsvpc)
#   - aws_iam_role (task execution role) + inline policy
#   - aws_iam_role (task role) + inline policy (or uses a shared role)
#   - aws_cloudwatch_log_group per task definition
#
# The module is called with `for_each` per connector per environment so that
# each connector gets its own task definition with only its own SSM secrets
# mounted (secret isolation — narrower blast radius than one task def with all
# secrets).

# ------------------------------------------------------------------------------
# Locals
# ------------------------------------------------------------------------------

locals {
  env_suffix     = var.demo ? "-demo" : ""
  env_label      = var.demo ? "demo" : "prod"
  # All task definitions use a consistent container name so the Step Functions
  # orchestrator can reference it without per-connector configuration.
  container_name = "pipeline"
}

# ------------------------------------------------------------------------------
# CloudWatch Log Group
# ------------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "task" {
  name              = "/ecs/portfolio-pipeline-${local.env_label}-${var.name}"
  retention_in_days = 7

  tags = {
    Project = "investment-portfolio-pipeline"
    Env     = local.env_label
  }
}

# ------------------------------------------------------------------------------
# Task Execution Role
# ------------------------------------------------------------------------------

data "aws_iam_policy_document" "task_execution_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "task_execution" {
  name               = "pipeline-task-exec-${local.env_label}-${var.name}"
  assume_role_policy = data.aws_iam_policy_document.task_execution_assume.json

  tags = {
    Project = "investment-portfolio-pipeline"
    Env     = local.env_label
  }
}

# Task execution role policy: ECR pull, CloudWatch Logs, SSM GetParameters,
# and KMS Decrypt for this connector's secrets only.
resource "aws_iam_role_policy" "task_execution" {
  name = "pipeline-task-exec-${local.env_label}-${var.name}"
  role = aws_iam_role.task_execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ECRPull"
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken",
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage",
        ]
        Resource = "*"
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "${aws_cloudwatch_log_group.task.arn}:*"
      },
      {
        Sid    = "SSMGetParameters"
        Effect = "Allow"
        Action = [
          "ssm:GetParameters",
          "kms:Decrypt",
        ]
        Resource = concat(
          [var.kms_key_arn],
          [for s in var.secrets : s.arn],
        )
      },
    ]
  })
}

# Attach the shared ECR push/pull policy so the task can pull images.
resource "aws_iam_role_policy_attachment" "ecr_push_pull" {
  role       = aws_iam_role.task_execution.name
  policy_arn = var.ecr_policy_arn
}

# ------------------------------------------------------------------------------
# Task Role (application permissions — S3 scoped to env prefix)
# ------------------------------------------------------------------------------

resource "aws_iam_role" "task" {
  count = var.task_role_arn == null ? 1 : 0

  name               = "pipeline-task-${local.env_label}-${var.name}"
  assume_role_policy = data.aws_iam_policy_document.task_execution_assume.json

  tags = {
    Project = "investment-portfolio-pipeline"
    Env     = local.env_label
  }
}

resource "aws_iam_role_policy" "task_s3" {
  count = var.task_role_arn == null ? 1 : 0

  name = "pipeline-task-s3-${local.env_label}-${var.name}"
  role = aws_iam_role.task[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3ReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          var.bucket_arn,
          "${var.bucket_arn}/${var.s3_prefix}/*",
        ]
      },
    ]
  })
}

# ------------------------------------------------------------------------------
# ECS Task Definition
# ------------------------------------------------------------------------------

locals {
  task_role_arn = var.task_role_arn != null ? var.task_role_arn : aws_iam_role.task[0].arn

  environment_vars = [
    for k, v in var.environment : {
      name  = k
      value = v
    }
  ]

  secret_vars = [
    for s in var.secrets : {
      name      = s.env_var
      valueFrom = s.arn
    }
  ]
}

resource "aws_ecs_task_definition" "task" {
  family                   = "portfolio-pipeline-${local.env_label}-${var.name}"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.cpu
  memory                   = var.memory
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn             = local.task_role_arn

  container_definitions = jsonencode([
    {
      name                   = local.container_name
      image                  = var.image
      essential              = true
      command                = var.command
      environment            = local.environment_vars
      secrets                = local.secret_vars
      readonlyRootFilesystem = true

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.task.name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = var.name
        }
      }
    }
  ])

  tags = {
    Project = "investment-portfolio-pipeline"
    Env     = local.env_label
  }
}

# ------------------------------------------------------------------------------
# Outputs
# ------------------------------------------------------------------------------

output "task_definition_arn" {
  description = "ARN of the ECS task definition."
  value       = aws_ecs_task_definition.task.arn
}

output "task_role_arn" {
  description = "ARN of the task IAM role (or the shared role if provided)."
  value       = local.task_role_arn
}

output "task_role_name" {
  description = "Name of the task IAM role (empty string if a shared role is provided)."
  value       = var.task_role_arn != null ? "" : aws_iam_role.task[0].name
}