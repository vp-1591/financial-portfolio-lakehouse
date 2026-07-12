# 0063 Simplify ECR Tagging Strategy

## Context

The production pipeline failed because ECS tasks could not pull the `production-latest` image from ECR. Investigation showed the ECR lifecycle policy's "keep at most 10 tagged images" rule had evicted `production-latest`. The root cause was that staging deployments pushed a `git-{sha}` tag on every push to `main`, accumulating tagged images faster than production deploys. With 18 staging deploys between production releases, the count of tagged images exceeded 10, and `production-latest` — being on an older image — was the first to be evicted.

Similarly, production deployments pushed both a `v*` version tag and `production-latest` to the same image. While this meant `production-latest` always shared the newest push timestamp (and thus would never be the first evicted), the version tags added unnecessary clutter to the registry without providing rollback capability — ECR images can be referenced by digest for rollback regardless of tags.

## Decision

Simplify ECR to exactly two mutable tags:

- **`staging-latest`** — pushed on every commit to `main`
- **`production-latest`** — pushed on every `v*` tag

No `git-{sha}` tags, no `v*` version tags. Since `image_tag_mutability = "MUTABLE"`, each push moves the tag to the new image. The old image becomes untagged and is cleaned up by the lifecycle policy.

Remove the count-based lifecycle rule ("keep at most N tagged images"). With only 2 mutable tags, there are never more than 2 tagged images at any time, so a count limit is unnecessary. Keep only the time-based rule that expires untagged images after 7 days.

### Changes

1. **`deploy-staging.yml`** — Remove `git-{sha}` tag step. Push only `staging-latest`.
2. **`deploy-prod.yml`** — Remove `v*` version tag step. Push only `production-latest`.
3. **`terraform/shared/main.tf`** — Remove `keep_tagged_images` variable. Remove the count-based lifecycle rule. Keep only the untagged expiry rule.

### Alternatives considered

- **Protect tags with `tagPrefixList`**: ECR lifecycle rules support `tagPrefixList` to exempt specific tag prefixes from eviction. This adds complexity and still requires a count-based rule for version tags, which we're removing entirely.
- **Keep `v*` version tags with a higher count limit**: Adds registry clutter without meaningful benefit — ECR images can always be referenced by digest for rollback. A higher count limit still risks eviction if staging deploys accumulate.

## Constraints

- Must not break the demo or production Step Functions pipelines, which reference `staging-latest` and `production-latest` tags in their ECS task definitions.
- Must not change `image_tag_mutability = "MUTABLE"` — this is required for the mutable-tag workflow.

## Consequences

- **Simpler**: Two tags, one lifecycle rule. No count limits to tune.
- **No version history in ECR**: Previous production images are cleaned up as untagged after 7 days. Rollback to a specific version requires re-pushing the image (available from Docker cache or a previous build) or referencing it by digest before cleanup.
- **No `git-{sha}` traceability in ECR**: The git commit SHA is no longer stored as an ECR tag. It remains in the Docker image metadata and GitHub Actions logs if needed.

## Validation

- `terraform plan` in `terraform/shared/` shows the lifecycle policy change (count rule removed, variable removed) with no unexpected diffs.
- Push a commit to `main` and verify only `staging-latest` tag exists in ECR (no `git-*` tags).
- Push a `v*` tag and verify only `production-latest` tag exists in ECR (no version tags).
- Run the production pipeline end-to-end and confirm it pulls `production-latest` successfully.