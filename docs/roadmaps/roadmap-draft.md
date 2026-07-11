Not in scope (follow-up)

 - Polars joins for snapshot instrument lookups — affects transform_snapshot(), separate from
 this CDC bug fix
 - Replace iter_raw_payloads() entirely — IBKR and XTB still use it; refactor in a separate step
 - Drop first_value() / nested_dict() from client.py — still used by snapshot transform;
 refactor when snapshot moves to Polars