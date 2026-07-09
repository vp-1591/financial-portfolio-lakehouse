# Orchestrator module — Step Functions state machine + EventBridge triggers.
#
# Creates a per-environment Step Functions orchestrator with:
#   - A Map state over $.connectors that runs each connector as an ECS task
#   - A ConsolidateAllocate state that runs the consolidate-allocate task
#   - Optional EventBridge triggers:
#     * S3 file arrival (XTB uploads) — always created
#     * Daily schedule — only when var.scheduled = true
#   - IAM role for EventBridge to start the state machine
#
# The ASL definition is identical across environments. Only the tfvars values
# differ (subnets, task def ARNs, demo flag, bucket name).

terraform {
  required_version = ">= 1.11"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
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
        ItemsPath     = "$.connectors"
        MaxConcurrency = 3
        ResultPath    = "$.connector_results"
        Iterator = {
          StartAt = "RunConnector"
          States = {
            RunConnector = {
              Type     = "Task"
              Resource = "arn:aws:states:::ecs:runTask.sync"
              Parameters = {
                "TaskDefinition.$" = "$.task_def_arn"
                Cluster             = var.ecs_cluster_arn
                LaunchType          = "FARGATE"
                NetworkConfiguration = {
                  AwsvpcConfiguration = {
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
          Cluster             = var.ecs_cluster_arn
          LaunchType          = "FARGATE"
          NetworkConfiguration = {
            AwsvpcConfiguration = {
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
}

resource "aws_sfn_state_machine" "orchestrator" {
  name       = var.state_machine_name
  role_arn   = var.sfn_role_arn
  definition = local.sfn_definition

  # Ensure IAM policy is fully propagated before creating the state machine.
  # Without this, CreateStateMachine can fail with "is not authorized to create
  # managed-rule" due to IAM eventual consistency.
  # Note: the sfn_role is created in shared/ and looked up via data source,
  # so we don't need an explicit depends_on here — the role already exists.

  # Logging is disabled because Step Functions managed-rule creation requires
  # logs:CreateLogDelivery on the service-linked role. Execution history is
  # available in the Step Functions console; ECS task logs go to per-task
  # CloudWatch log groups (configured in the ecs-task module).
  logging_configuration {
    level = "OFF"
  }

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
  name        = "portfolio-pipeline-xtb-file-arrival-${var.env}"
  description = "Trigger the pipeline orchestrator when an XTB file arrives in S3 staging (${var.env})"

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
    Env     = var.env
  }
}

resource "aws_cloudwatch_event_target" "xtb_file_arrival" {
  rule      = aws_cloudwatch_event_rule.xtb_file_arrival.name
  target_id = "orchestrator"
  arn       = aws_sfn_state_machine.orchestrator.arn
  role_arn  = aws_iam_role.eventbridge.arn

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

  name                = "portfolio-pipeline-daily-schedule-${var.env}"
  description         = "Trigger the pipeline orchestrator on a daily schedule (${var.env})"
  schedule_expression = var.schedule_cron

  tags = {
    Project = "investment-portfolio-pipeline"
    Env     = var.env
  }
}

resource "aws_cloudwatch_event_target" "daily_schedule" {
  count = var.scheduled ? 1 : 0

  rule      = aws_cloudwatch_event_rule.daily_schedule[0].name
  target_id = "orchestrator"
  arn       = aws_sfn_state_machine.orchestrator.arn
  role_arn  = aws_iam_role.eventbridge.arn

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
  name               = "pipeline-eventbridge-role-${var.env}"
  assume_role_policy = data.aws_iam_policy_document.eventbridge_assume.json

  tags = {
    Project = "investment-portfolio-pipeline"
    Env     = var.env
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
      aws_sfn_state_machine.orchestrator.arn
    ]
  }
}

resource "aws_iam_role_policy" "eventbridge" {
  name   = "pipeline-eventbridge-policy-${var.env}"
  role   = aws_iam_role.eventbridge.id
  policy = data.aws_iam_policy_document.eventbridge.json
}