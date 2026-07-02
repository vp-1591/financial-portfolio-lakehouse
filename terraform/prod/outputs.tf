# Sensitive outputs — the access key secret must be stored in GitHub Secrets
# as AWS_SECRET_ACCESS_KEY.  It is marked sensitive so it won't appear in
# terraform show output.

output "access_key_secret" {
  description = "IAM secret access key (store as GitHub Secret AWS_SECRET_ACCESS_KEY)."
  value       = aws_iam_access_key.pipeline.secret
  sensitive   = true
}