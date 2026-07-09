# 0054: Public-Subnet ECS — Eliminate Perma-VPC Charges

> **Supersedes [ADR 0051](./0051-step-functions-orchestration.md)** — Decision #5 (VPC endpoints, no public IP) is superseded. Private subnets and VPC Interface Endpoints are replaced with public subnets and an Internet Gateway. All other ADR 0051 decisions remain in force.

## Context

ADR 0051 specified that each environment uses its own VPC with private subnets and VPC Interface Endpoints (ECR API, ECR DKR, CloudWatch Logs, SSM, SSM Messages) plus an S3 Gateway Endpoint. This ensures ECS Fargate tasks have no public IP and reach AWS services exclusively through VPC endpoints.

This architecture costs ~$36/env/month in VPC Interface Endpoint charges (5 endpoints × $0.01/hr × 730 hrs/month per environment with 1 AZ, scaled to $108+/month with 3 AZs). These charges are incurred 24/7 regardless of whether the pipeline runs.

For a personal portfolio dashboard that runs the pipeline infrequently, the security benefit of private subnets (no public IP on tasks) is marginal:
- Tasks are ephemeral (seconds to minutes of runtime)
- Tasks make only outbound connections to AWS services and broker APIs
- There is no inbound traffic to protect
- Multi-user isolation is not a concern (single owner)

ADR 0051 did not evaluate the public-subnet alternative — private subnets were the default choice.

## Decision

Switch ECS Fargate tasks to **public subnets with `AssignPublicIp = "ENABLED"`** and remove all VPC endpoints. Tasks reach AWS services (ECR, S3, CloudWatch, SSM) and broker APIs (IBKR, Trading 212, XTB) through the Internet Gateway.

Specific changes:
- Replace private subnets with public subnets (with Internet Gateway route)
- Set `AssignPublicIp = "ENABLED"` in the Step Functions ASL `NetworkConfiguration`
- Remove all 5 VPC Interface Endpoints (ECR API, ECR DKR, CloudWatch Logs, SSM, SSM Messages)
- Remove the S3 Gateway Endpoint (traffic goes through the IGW instead)
- Add `aws_internet_gateway` + `aws_route_table` + `aws_route_table_association`
- Simplify the security group to a single all-traffic egress rule
- Reduce to 1 AZ per environment (tasks are ephemeral and Step Functions retries on failure)

Alternatives considered:
- **Lambda instead of ECS**: Would eliminate the VPC entirely and likely fit within the free tier. Rejected for this change because it requires restructuring the container packaging and task definitions — a larger rewrite. Can be revisited later.
- **Private subnets + VPC endpoints (status quo)**: Rejected due to ~$36/env/month in idle charges for a personal project.
- **Split Terraform state files**: Useful for safe teardown of compute infra while preserving S3 buckets, but orthogonal to the networking change. Can be done as a separate follow-up.

## Constraints

- Step Functions orchestration, ECS task definitions, and the `Map`-over-input pattern remain unchanged.
- Per-environment isolation (separate VPC, S3 bucket, IAM user, KMS key, SSM params) is preserved.
- The ECS cluster remains in `shared/` (ADR 0051 Decision #6).
- ECR image pull, CloudWatch Logs shipping, and SSM secret retrieval all work through the Internet Gateway instead of VPC endpoints. IAM permissions are unchanged — only the network path differs.
- State files remain independent (ADR 0049).
- `ENCRYPTION_KEY` continuity is unaffected (SSM parameters unchanged).

### Out of scope

- Lambda migration (could be a future ADR).
- Splitting Terraform state files to separate storage from compute (follow-up).

## Consequences

- **Positive**: Zero hourly infrastructure charges for VPC resources. The only costs are Fargate task runtime (pay-per-second) and Step Functions state transitions.
- **Positive**: Simpler Terraform — no VPC Interface Endpoints, no S3 Gateway Endpoint, no route-table gymnastics.
- **Positive**: 1 AZ per environment is sufficient (no need for multi-AZ resilience with ephemeral tasks and built-in retries).
- **Negative**: ECS tasks receive ephemeral public IPs while running. They cannot be reached from the internet (security group has no ingress rules), but their outbound traffic traverses the public internet. For a single-user personal project, this is acceptable.
- **Negative**: S3 data transfer goes through the IGW instead of the AWS backbone (via the free S3 Gateway Endpoint). For the small data volumes in this project, the cost difference is negligible.
- **Neutral**: The Internet Gateway and route table are free AWS resources (no hourly charge).

## Validation

1. `terraform validate` in `prod/` and `demo/`.
2. `terraform plan` in `demo/` confirms: 6 VPC endpoints destroyed, IGW + route table + route table association created, security group simplified, subnets renamed.
3. `terraform apply` in `demo/`, then trigger a demo pipeline run via Step Functions console.
4. Verify the ECS task starts, pulls the ECR image, and completes successfully with CloudWatch Logs visible.
5. Apply to `prod/` after demo validates.
6. Check AWS Cost Explorer after the next billing cycle to confirm VPC endpoint charges are $0.