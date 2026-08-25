# Adversarial Review — Bounded Bronze Retention and Incremental Events

**Subject:** `_bmad-output/specs/spec-bronze-retention-and-incremental-events/ARCHITECTURE-SPINE.md` (draft, 2026-08-22)
**Method:** two independent implementers, A and B, each obeying AD-1..AD-8 verbatim and never consulting each other, build the same spine in the existing `pipeline/` codebase. Each candidate divergence below is tested against the actual code (`raw/ingest.py`, `raw/models.py`, `normalized/models.py`, `connectors/{xtb,trading212,ibkr}/{fetch,transform}.py`, `analytics/quality.py`, `run.py`, `migrations/migrate_single_bronze.py`) and the pinned stack (deltalake 1.6.0, polars 1.42.0, pyarrow 24.0.0).

A finding is a pair of implementations that both plausibly "follow the AD" yet produce **different data** (different rows in raw/events tables, or different physical retention outcomes). Where the AD already pins the choice, it is noted and moved on.

---

## Verdict

The spine pins the *mechanism* well (write-time merge for raw, identity merge for events, run-aware freshness, per-run VACUUM, migration gate), but it leaves **five genuine divergence points** that two compliant builders will resolve differently, and four of them change the **rows** or **physical retention** of the pipeline:

1. **AD-4 event identity subset is not enumerated per broker, and XTB is omitted entirely** — builders pick different merge keys and the events tables end up with different rows. *(HIGH)*
2. **AD-1 does not pin the raw-merge behavior for `NULL` retention keys** (XTB unparseable filename) — plain-equality predicate re-inserts every run (unbounded raw/xtb growth), null-safe predicate collapses distinct accounts onto one row. The memlog decision ("append + in-batch dedup") is **not** carried into the spine. *(HIGH)*
3. **AD-3 "invoke `DeltaTable.vacuum()`" is a literal no-op under deltalake 1.6.0** — `vacuum()` defaults to `dry_run=True`, so a builder who obeys the AD letter-for-letter never physically deletes anything and CAP-4's bounded-storage success is silently unmet. *(HIGH)*
4. **AD-7 migration backfill scope** — filename-only parse (unparseable legacy rows become `NULL`, feeding divergence 2) vs. adding the payload-parse recovery at migration time → different `account_id` values in migrated `raw/xtb`. *(MEDIUM-HIGH)*
5. **AD-5 freshness table→broker mapping** — which tables receive a per-broker fetch-time override, and what "within the window" means for multi-broker tables (`events`, `consolidated_holdings`), is unpinned → different freshness PASS/WARN and different `data_quality` rows. *(MEDIUM)*

Plus lower-severity holes (batch row-order precedence in the raw merge, `>` vs `>=` on events, vacuum scope, merge flags/bootstrap) that are worth one-line pins but are unlikely to split two reasonable builders in the happy path.

---

## Focus 1 — AD-1: the raw MERGE-on-key write

AD-1 rule (spine line 79): *"the raw write for broker B is a `DeltaTable.merge` of the current fetch's batch against `raw/{B}`, keyed on B's retention key — XTB `account_id`, Trading 212 and IBKR `source`. Matched rows are replaced by the current fetch row (latest `fetched_at` wins by construction — every write is a current fetch); unmatched rows insert. Rows whose key is absent from the current batch are untouched. Before the merge, the batch is deduped on `(source, payload_hash)`..."*

### F1.1 — `NULL` retention keys (XTB unparseable filename): predicate semantics and merge-vs-append unpinned (HIGH)

**Divergence question.** The merge predicate is `s.account_id = t.account_id` (SQL). For a row with `account_id IS NULL`, `NULL = NULL` evaluates to *falsy* — the row **never matches** a target NULL row and always takes the `insert` path. What are the two defensible readings?

- **Builder A** writes the plain equality predicate. NULL-keyed XTB rows (unparseable filename, or a migrated legacy row with no parseable filename) are inserted on every fetch and **never replaced**, because the previous NULL row sits in the target and never matches. The in-batch dedup is `(source, payload_hash)` *before* the merge only — with the cross-run `dedup_raw` scan deleted by AD-1, a byte-identical re-upload of an unparseable-account report inserts a fresh row every run: raw/xtb grows **unboundedly** for that account, directly violating CAP-1's bounded-storage success criterion.
- **Builder B:** reasons that "merge on the retention key" requires a null-safe predicate — `s.account_id IS NOT DISTINCT FROM t.account_id` or `(s.account_id = t.account_id OR (s.account_id IS NULL AND t.account_id IS NULL))`. Now **all NULL-keyed rows collapse onto a single row**; two *distinct* unparseable XTB accounts become one row, and the account that was re-fetched last silently evicts the other account's report — a real row lost from `raw/xtb`.

The memlog (`decision` dated 2026-08-22) resolves this as "NULL retention keys … fall back to append + in-batch dedup, documented as the spec's acknowledged filename risk" — that is Builder A, *and it accepts unbounded growth*. This decision is **not** carried into any AD in the spine. AD-2 only forbids treating `(source, NULL account_id)` as a shared *cross-broker* key (line 96); it says nothing about what the merge does with a NULL key *within* `raw/xtb`.

**Assessment.** Genuine divergence: two builders produce different rows in `raw/xtb` (one row accumulating every run vs. one collapsed row). The memlog chooses A, but a builder reading only the spine (the build substrate) can pick B. Both "follow the AD." **Data difference: different rows in `raw/xtb`.**

**Anchor:** AD-1 line 79 (merge keyed on `account_id`; in-batch dedup only); AD-2 line 89 (null-key caveat covers only cross-broker collapse).

**Pin to add:** a one-line clause in AD-1: "an XTB row whose `account_id` is `NULL` is **never** a merge key: it is appended with only the in-batch `(source, payload_hash)` dedup (the memlog's filename-risk fallback), and this admitted growth is the accepted trade-off for unparseable filenames." Or, if collapse is preferred, say so explicitly with the distinct-account-loss consequence.

### F1.2 — "latest `fetched_at` wins by construction" vs. batch row order for multi-row-per-key batches (MEDIUM)

**Divergence.** The merge replaces each matched target row with the **current fetch row**, but delta-rs applies `whenMatchedUpdate` per source row in batch order; when a single batch (or a run) carries two rows with the **same retention key but different payloads**, the row that ends up in the table is the **last one applied — not the one with the highest `fetched_at`**. The in-batch dedup on `(source, payload_hash)` deliberately does *not* remove two different payloads.

When can two same-key rows occur? XTB `fetch_kwargs` returns one batch per `--xtb-file`, and the loop merges each batch sequentially — so two files for the same account in one run are two merges; the **loop order** (= CLI order), not `fetched_at`, decides which report wins. The spine's Deferred section admits content-time keying is out of scope and "latest `fetched_at` wins stays the rule" — but the rule is only true if the merge order is also fetched_at order, which the AD never states.

- **Builder A:** merges each batch as fetched (loop order = fetch order) → the *last* CLI-listed file wins, regardless of which file's `fetched_at` is larger.
- **Builder B:** coalesces all of a run's batches and sorts the combined batch by `fetched_at` ascending before one merge → the newest-fetched file wins.

In the normal single-file-per-account run both agree; they diverge exactly when the same account appears twice in one run with different content. Today's XTB upload path is one file per trigger, so this is an edge case — but it is also the case the "Deferred" line claims is settled.

**Assessment:** a real precedence hole, but only reachable in the multi-file-same-account run. Two builders *can* split here and the raw/xtb row differs.

**Anchor:** AD-1 line 79 ("latest `fetched_at` wins by construction"); spine Deferred (line 182).

**Pin:** "each run merges in fetch order; for a batch that contains two rows with the same retention key, the last row in `fetched_at`-ascending batch order wins" or "a run never merges two rows with the same key from the same run; dedup in-batch on the retention key too."

### F1.3 — `merge_schema` / `error_on_type_mismatch` flags and first-table bootstrap (LOW)

delta-rs 1.6.0 `DeltaTable.merge` defaults `merge_schema=False, error_on_type_mismatch=True`, and **requires the target table to already exist** (first-ever fetch, or an all-duplicates fetch, has no table). AD-1/AD-2 never mention either. Post-migration the schema matches (AD-7 gate), so happy-path data is identical; but a builder that bootstraps the first table with the batch's schema vs. an explicit empty `RAW_SCHEMA` table, or a builder that defensively flips `merge_schema=True`, silently diverges only in pre-migration/schema-drift environments. Note and move on.

---

## Focus 2 — AD-4 events MERGE

AD-4 rule (spine line 97): "the events write is a `DeltaTable.merge` of the run's normalized event rows (pre-deduped in-batch by `dedup_events`) on the **per-broker event identity — the existing subset containing `event_id` (IBKR, Trading 212 per ADR 0105)**. `whenMatchedUpdate` fires only when the incoming row's `fetched_at` is newer; `whenNotMatchedInsert` otherwise. Nothing is ever deleted by a merge."

### F2.1 — the event identity subset is not enumerated; XTB is missing entirely [HIGH]

**Divergence.** The AD says "the existing subset containing `event_id`", cites "IBKR, Trading 212 per ADR 0105", and **never mentions XTB**. The actual existing `dedup_events` subsets in today's code are:

| broker | today's dedup subset |
| --- | --- |
| IBKR | `["event_id"]` (ADR 0101/0105: globally unique) |
| Trading 212 | `["event_type", "event_id"]` (ADR 0105: `order.id` int→str collides with dividend/transaction `reference`) |
| XTB | `["event_type", "event_id", "account_id"]` (transform.py:366-370) |

Reasonable builders split on at least three points:

- **T212:** "the subset containing `event_id`" can be read as `["event_id"]` (subset *containing* event_id) or as the ADR-0105 subset `["event_type", "event_id"]` (the *existing* subset). Using `["event_id"]` for the merge **collapses** the dividend `reference="12345"` and order `id=12345` rows into one — the exact bug ADR 0105 was written to kill — and the two rows would merge-update each other instead of coexisting.
- **XTB:** nothing in AD-4 says XTB events are merged at all, or on which subset. `event_id` (operation_id) is only unique *within* an account; the existing dedup deliberately adds `account_id`. A builder using `["event_id"]` for XTB merges two different accounts' events with the same operation_id into one row and replaces one account's event with the other's — different `xtb_events` rows.
- **dedup_events vs merge key:** AD-4 says the batch is "pre-deduped … by `dedup_events` on the per-broker event identity", implying the dedup subset equals the merge key — but a builder could read "the existing subset containing event_id" for the merge while passing a *different* subset to `dedup_events` (e.g. dropping `account_id` from the merge key). Mismatched subsets mean the batch can contain two rows with the same merge key (different dedup keys) → two `whenMatchedUpdate` applications in one batch, last wins — nondeterministic.

**Assessment:** genuine and high-impact. Different merge keys → different rows in `{broker}_events`. AD-4's reference to ADR 0105 pins IBKR and T212 for the *dedup* subset, but the AD's own phrasing is loose enough that a builder can pick the wrong subset for the merge key.

**Anchor:** AD-4 line 97; ADR 0105 (T212 `["event_type","event_id"]`, IBKR `["event_id"]`); xtb `transform_events` dedup `["event_type","event_id","account_id"]`.

**Pin to add:** a table in AD-4: "the per-broker event identity **equals** the `dedup_events` subset, and is: IBKR `[event_id]`; Trading 212 `[event_type, event_id]`; XTB `[event_type, event_id, account_id]`." State that the merge key and the in-batch dedup subset are the same tuple.

### F2.2 — strict `>` vs `>=` on `fetched_at` [LOW]

**Divergence.** "only when the incoming row's `fetched_at` is newer" — is newer `s.fetched_at > t.fetched_at` or `>=`? For cross-run merges the run timestamp always differs, so both agree; the tie only fires when a re-run happens to land on the same UTC microsecond or when the same run's events are merged twice (idempotency path). A tied-but-different-content row is retained (`>`) or overwritten (`>=`). Marginal. `whenNotMatchedInsert` for the matched-but-older incoming row is pinned by the AD ("otherwise"), so that part is settled.

**Assessment:** real but negligible. **Pin (one line):** "strict `s.fetched_at > t.fetched_at`."

### F2.3 — empty normalized batch behavior [LOW]

If a run produces zero event rows (empty broker events payload), the events merge gets an empty source. A builder either skips the merge (no-op, table untouched — preserves append semantics) or attempts the merge on an empty frame. Both are no-ops in outcome; not a divergence. **Note and move on.**

---

## Focus 3 — AD-5 run-aware freshness

AD-5 rule (spine line 103): "the pipeline passes per-broker last-successful-fetch timestamps into the DQ freshness pass. A table whose broker was fetched successfully within the freshness window passes regardless of whether any payload changed. When the run context is absent (DQ invoked standalone), fall back to the existing table `max(fetched_at)` behavior unchanged (ADR 0072). No new metadata table: the run is the marker."

### F3.1 — table→broker mapping and multi-broker tables not pinned (MEDIUM-HIGH)

**Divergence.** `quality.py` validates 12 tables. Which of them get a per-broker fetch-time override, and whose?

- **Per-broker tables** (`{broker}_snapshot`, `{broker}_events`) map trivially by prefix — a builder can strip `_snapshot`/`_events` to the broker name.
- **Multi-broker tables** — `events` (consolidated) and `consolidated_holdings` (consolidated) are fed by *all* brokers. "A table whose broker was fetched" is undefined for these. Options: (a) pass if *any* contributing broker was fetched in the window; (b) pass only if *all* contributing brokers were; (c) never override them (fall back to table max). The current SFN shape matters here: the connector tasks each run `run_validation` with `tables=[{broker}_snapshot, {broker}_events]` in *their own process*, while `run-consolidate-analytics` runs the second validation in a *different* Fargate task that has **no** fetch timestamps in memory. "No new metadata table: the run is the marker" therefore means the per-connector validation *cannot* carry times across processes — so a builder must decide whether the consolidated-table validation is overridden at all (it cannot be, without persisting something) or whether the per-connector tables are the only override site.

These choices change the `freshness` PASS/WARN rows written into `data_quality`, and hence the deploy gate.

**Assessment:** genuine divergence. Two builders can implement AD-5 with different table→broker mapping and different "within the window" definitions for the consolidated tables. The current freshness check on the consolidated tables uses `max(fetched_at)`; with a re-fetch the per-broker silver tables are updated by the merge (fetched_at advances) so the *actual* freshness gap is mostly the empty-events case — which is exactly where the override matters. Severity MEDIUM (only the freshness/quality layer, not the underlying rows).

**Anchor:** AD-5 line 103; `quality.py` FRESHNESS_COLUMNS / run_validation; `run.py` run_connector vs run_consolidate_analytics.

**Pin:** "the per-broker fetch-time override applies only in the connector-task validation, to `{broker}_snapshot` and `{broker}_events`; `events` and `consolidated_holdings` are never overridden and always use table `max(fetched_at)` — because the run marker is in-memory and does not cross the connector→consolidate process boundary."

### F3.2 — what counts as a "successful fetch" [LOW-MEDIUM]

A broker whose fetch returned zero rows (empty events endpoint) — was it "fetched successfully"? If yes, an *empty* events table is marked fresh (masking genuine emptiness; note `check_non_empty` separately warns). If no, the false-stale path CAP-3 targets is not fully fixed. Not a row-level data divergence; a freshness-semantic choice. **Note.**

---

## Focus 4 — AD-3 VACUUM

AD-3 rule (spine line 91): "each broker run invokes `DeltaTable.vacuum()` with the default retention (tombstone 7-day; `retention_hours` omitted, `enforce_retention_duration` stays True). … VACUUM lands at the end of the run …".

### F4.1 — deltalake 1.6.0 `vacuum()` defaults to `dry_run=True` — a literal no-op (HIGH)

**Divergence.** The pinned stack is deltalake **1.6.0**, and its `DeltaTable.vacuum` signature is `vacuum(retention_hours=None, dry_run=True, enforce_retention_duration=True, ...)`. `dry_run` **defaults to True** — the call returns the list of delete-able files and deletes *nothing*. AD-3 says "invoke `DeltaTable.vacuum()` with the default retention (…; `retention_hours` omitted, `enforce_retention_duration` stays True)". It pins `retention_hours` and `enforce_retention_duration`, but **never pins `dry_run`**.

- **Builder A:** writes `dt.vacuum()` — physical deletion **never happens**. The merge tombstones pile up; raw storage grows with every replacement; CAP-4's bounded-storage success criterion is silently unmet, and no test catches it because `vacuum()` returns a file list even in dry-run.
- **Builder B:** writes `dt.vacuum(dry_run=False)` — physical deletion happens.

A faithful-to-the-letter builder can produce the exact failure CAP-4 exists to prevent. The "Prevents" clause (aggressive `enforce_retention_duration=False` override) is pinned, but the *more dangerous* default is not.

**Assessment:** genuine, high-impact, and cheap to fix with one word. Physical-retention divergence (CAP-4) even though both builders "obey the AD."

**Anchor:** AD-3 rule, spine line 91. **Pin:** "…with the default retention (`retention_hours` omitted, `enforce_retention_duration=True`) and **`dry_run=False`**."

### F4.2 — VACUUM scope: which tables (raw only vs. events too) [MEDIUM]

The events tables now merge too — `whenMatchedUpdate` rewrites files and creates tombstones that only VACUUM reclaims. AD-3 binds "raw maintenance" and its title says "per run", but the same run performs the events merge. A builder who vacuums `raw/{broker}` only leaves the events tables' stale files to accumulate (physical growth with stable row counts) over time. A builder who also vacuums the events table keeps both bounded. Logical rows identical; physical storage differs; a future `check_row_count_stability`/storage billing impact. **Pin: "VACUUM runs on `raw/{broker}`; the `{broker}_events` tables' tombstone accumulation is a follow-up decision, not part of AD-3."**

---

## Focus 5 — AD-7 raw-schema migration

AD-7 rule (spine line 127): "a migration rewrites each `raw/{broker}` to the new `RAW_SCHEMA`, backfilling XTB `account_id` by **parsing `source_file`** (the retained filename → account id), then drops `source_file`."

### F5.1 — backfill determinism: filename-only vs. payload-recovery fallback (MEDIUM-HIGH]

**Divergence.** The XTB filename pattern (`{CCY}_{account_id}_{from}_{to}.xlsx`, parsed by `_account_id_from_filename` returning `None` when it does not match) does not cover every legacy row — older reports, renamed files, S3 keys. The memlog decision says "the transform's payload-parse recovery stays as the mandated fallback for genuinely null rows" — but that clause targets the **silver** transform, not the migration. AD-7 says the migration backfills "from `source_file`" and drops `source_file`. Two builders:

- **Builder A:** parses only the filename; unparseable rows are migrated with `account_id = NULL`. These rows then enter AD-1's merge with a NULL key → the F1.1 divergence fires for **every legacy row that predates the naming convention** → re-insert every run (unbounded) or collapsed (builder-dependent).
- **Builder B:** notices the parser is available at migration time (the payload is being read anyway) and recovers `account_id` from the report's R1 account for unparseable filenames, producing **different, populated `account_id` values** for the same legacy rows. The migration's own `--dry-run` diff then shows different backfilled values.

**Assessment:** real. The memlog's split (filename at migration, payload recovery at transform) is *one* reading; a builder who wants the retention key to be "correct from day one" (the stated purpose of AD-7) will reasonably add payload recovery at migration time. Different rows in `raw/xtb`.

**Anchor:** AD-7 rule, spine line 115; SPEC constraint "XTB transform recovers it by parsing the raw payload when null."

**Pin:** "the migration backfills `account_id` **only** by parsing `source_file`; rows with unparseable filenames migrate with `account_id=NULL`, and the payload-parse recovery stays a silver-transform behavior (AD-2). Do not parse payloads during migration — migration and transform must agree on a single derivation path so `--dry-run` output is predictable."

### F5.2 — migration mechanics: in-place overwrite vs temp+swap [LOW]

The prior single-bronze migration (`migrate_single_bronze.py`) writes a fresh destination and refuses to clobber (ADR 0112/0113 A1). This migration rewrites the *same* table path. In-place `write_deltalake(mode="overwrite")` resets the Delta log snapshot; temp+swap preserves the transaction history (which AD-3 separately claims as the "30-day log history" mechanism). Idempotency detection ("already migrated" must be inferred from the schema — `source_file` gone — since re-running against a `source_file`-less table can't re-read it) is also unpinned. Both builders "rewrite each raw/{broker}", and successful runs converge to the same rows; failure modes and log history differ. LOW; note it in the migration AD.

---

## Cross-cutting interplay

- **F5.1 → F1.1 → F2.1:** a NULL account_id from a migrated legacy row (F5.1, Builder A) enters the raw merge with an unpinned predicate (F1.1) and re-inserts every run; its silver events, if merged on a subset missing `account_id` (F2.1, Builder B), then collapse across accounts. The three ADs need to be co-signed on one NULL account_id policy.
- **F4.1 interacts with F1.1:** the raw merge creates the tombstone; if VACUUM is a no-op (dry_run default), the tombstone never gets reclaimed, so the CAP-1 "bounded" claim depends on F4.1 being `dry_run=False` exactly when F1.1 also holds.
- **AD-4's "matched-but-older incoming rows are dropped" is unambiguous in the AD ("whenMatchedUpdate only when newer; whenNotMatchedInsert otherwise")** — that is the one part of the events merge two builders will agree on.

---

## Recommended pins (ranked)

1. **AD-4:** add a per-broker identity table (IBKR `[event_id]`, T212 `[event_type,event_id]`, XTB `[event_type,event_id,account_id]`) and state the merge key equals the dedup subset. (F2.1)
2. **AD-1:** state the NULL-retention-key behavior for XTB explicitly (append + in-batch dedup, accepted growth; never a null-safe merge). (F1.1)
3. **AD-3:** state `vacuum(dry_run=False)`, and whether the events tables are vacuumed. (F4.1, F4.2)
4. **AD-7:** state the backfill is filename-only and that payload recovery stays in the transform; define the idempotency probe. (F5.1, F5.2)
5. **AD-1/AD-5:** state the precedence rule for two rows with the same key in one run (F1.2) and which tables the freshness override covers (F3.1).
