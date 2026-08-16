# Non-secret operational config — which connectors run and when.
# This file is committed and auto-loaded by Terraform (no -var-file flag, no
# main.tf edit). Toggle a broker by adding/removing it from the lists below.
# Secret-ish ARNs stay in the gitignored terraform.tfvars.
scheduled               = true
schedule_cron           = "cron(0 6 1 * ? *)"
schedule_connectors     = ["ibkr", "trading212"]
file_arrival_connectors = ["ibkr", "trading212", "xtb"]