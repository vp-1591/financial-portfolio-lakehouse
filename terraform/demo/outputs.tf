# Sensitive outputs — the access key secret must be stored in GitHub Secrets
# as AWS_SECRET_ACCESS_KEY_DEMO.  It is marked sensitive so it won't appear in
# terraform show output.

output "access_key_secret" {
  description = "IAM secret access key (store as GitHub Secret AWS_SECRET_ACCESS_KEY_DEMO)."
  value       = aws_iam_access_key.pipeline_demo.secret
  sensitive   = true
}