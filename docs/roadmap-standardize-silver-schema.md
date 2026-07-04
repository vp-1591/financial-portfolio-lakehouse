# Standardize Silver Schema Roadmap

## Why this matters

The pipeline currently mixes broker-specific parsing logic with normalized output expectations. That makes the silver layer harder to evolve, because connectors and downstream consumers both depend on the same shape.

## Short note

We should standardize the silver schema so that all broker data is converted into the same logical model before analytics and reporting run. This keeps connectors focused on ingestion and transformation, while the silver layer becomes the stable contract for dashboards, reconciliation, and downstream data products.

## Goals

- Define one canonical schema per logical silver table (for example, holdings and CDC events).
- Keep connector code responsible for broker-specific extraction only.
- Make downstream consumers independent of broker-specific field names or quirks.

## Expected benefits

- Simpler onboarding for new brokers.
- Less duplicated logic across connectors.
- Better consistency for reporting and testing.
- Clear separation between ingestion, normalization, and consumption.
