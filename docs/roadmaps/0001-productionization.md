# Roadmap: Pipeline Architecture & Productionization

## Goal

Deliver the portfolio report without manual intervention: email the generated
HTML report on a schedule, and add a plain-English position-type cheat sheet to
the report.

## Current state

The pipeline is production-ready. Phases 1–3 (deployment strategy, Step
Functions orchestration, staging data quality gates) are complete, and the
reporting baseline (Phase 4) is delivered — the HTML report includes portfolio
summary, allocation by broker and currency, position-type breakdown, passive
income, cash flow, and data quality sections (ADRs 0070, 0082, 0083). What
remains is delivery and one reporting nicety:

- email delivery of the generated report
- a position-type cheat sheet that explains the portfolio mix in plain English

---

## What remains to build

### 4. Reporting baseline — position-type cheat sheet

Add a short cheat sheet to the HTML report that explains the portfolio mix by
position type in plain English.

### 5. Delivery and operational visibility — email delivery

Send the generated HTML report by email so it can be received and inspected
without manual intervention.

Planned work:

- email delivery of the generated report

---

## Suggested phases

### Phase 4 — Reporting baseline (remaining: cheat sheet)

Add the position-type cheat sheet to the HTML report.

### Phase 5 — Delivery and automation (remaining: email delivery)

Add email delivery of the generated report.

---

## Alternatives considered

No alternatives have been considered yet for the remaining work (email delivery
and the position-type cheat sheet).
