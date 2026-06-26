# 0012 — Add pipeline data flow diagram to README

## Context

The Medallion Pipeline section of the README described the architecture textually but lacked a visual overview of how data flows from broker sources through the raw, normalized, and analytics layers. Users and contributors had to read the source code to understand the pipeline topology.

## Decision

Add a Mermaid flowchart diagram and a summary table to the "Medallion Pipeline" section of the README, directly after the introductory paragraph and before the "Setup" subsection.

The diagram uses `classDef` for per-layer coloring instead of subgraphs, because Mermaid subgraphs with mixed directions create disconnected-looking boxes when edges cross subgraph boundaries. Each node is colored by layer (blue for sources, orange for raw, green for normalized, purple for FX, light blue for analytics), making the flow easy to follow.

A companion table lists each Delta table with its layer, color, and contents.

## Consequences

- Readers can grasp the pipeline topology at a glance without diving into source code.
- GitHub renders Mermaid diagrams natively; other platforms may need a Mermaid plugin.
- The diagram must be kept in sync when connectors or layers change.

## Validation

Reviewed pipeline source (`pipeline/run.py`, `pipeline/connectors/`, `pipeline/raw/`, `pipeline/normalized/`, `pipeline/analytics/`) to confirm the diagram accurately reflects the current architecture.