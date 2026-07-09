output "state_machine_arn" {
  description = "ARN of the Step Functions orchestrator state machine."
  value       = aws_sfn_state_machine.orchestrator.arn
}