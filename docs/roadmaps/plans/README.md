# Phase 2 implementation plans

These three plans implement `docs/roadmaps/phase-2-step-functions-orchestration.md` (Step Functions
orchestration). They are split for focused review and to keep each developer-agent context small.
Implement and merge in order — each builds on the previous.

| Order | Plan | Scope | ADR |
|---|---|---|---|
| 1 | [PR 1 — Connector protocol](./phase-2-pr1-connector-protocol.md) | Self-describing connectors: `fetch_kwargs`, `fetch_cdc_kwargs`, `required_secrets`, `extract_holdings`, `enabled_env_var`. Behavior-neutral refactor. | — |
| 2 | [PR 2 — CLI subcommands](./phase-2-pr2-cli-subcommands.md) | `run-connector <name>` + `run-consolidate-allocate` subcommands, extracted `fetch_connector`/`transform_connector` helpers, XTB S3 key decoding. Behavior-neutral for existing commands. | — |
| 3 | [PR 3 — Terraform infrastructure](./phase-2-pr3-terraform-infrastructure.md) | ECS task module, shared orchestrator state machine + EventBridge triggers, per-env task defs + VPC + SSM + IAM, secrets runbook, apply sequencing. | ADR 0051 |

Each plan is self-contained (its own Context, reused-code references, steps, tests, verification) —
a developer agent can implement one without loading the others or the parent plan.

## Decisions (applied across all three plans)

- **Connector inclusion:** lists in Step Functions execution input, not per-connector ASL branches.
- **EventBridge input_transformer:** static constant templates (no Lambda).
- **`*_ENABLED` env var:** explicit `enabled_env_var` attribute on `BrokerConnector` (not derived
  from connector name — `trading212` → `T212_ENABLED`).
- **Secrets:** SSM `SecureString` per env, KMS-encrypted; values seeded out-of-band.
- **SSM naming:** `/portfolio/prod/<SECRET>` (prod), `/portfolio/demo/<SECRET>_DEMO` (demo).
- **Networking:** private subnets + S3/ECR/CloudWatch VPC interface endpoints, no public IP,
  **separate VPC per environment**.
- **CloudWatch Logs:** `/ecs/portfolio-pipeline-<env>-<connector>`, 7-day retention.
- **Apply order:** shared (ECR/cluster) → prod/demo → shared (orchestration with ARN map).