# XTB Anonymized Sample — Internal Consistency Verification

**File:** `docs/xtb/xtb-report-sample/PLN_12345678_2006-01-01_2026-08-03.xlsx`
**Dump:** [xtb_sample_dump.txt](xtb_sample_dump.txt) (3 sheets, every cell — **current state**, regenerated after the pass-2 corrections)
**Verifier:** `verify_xtb.py` (independently recomputes every relationship from the dump)

**Companion docs:** implementation decisions & stages →
[xtb_overhaul_plan.md](xtb_overhaul_plan.md); current-state audit →
[xtb_code_audit.md](xtb_code_audit.md).

> **Pass-2 corrections applied** (throwaway scripts, since deleted): R012 Net
> Profit % 8.31→8.32 + R011 aggregate 29.76→29.77; R011 aggregate Open price
> 105→106.36 (volume-weighted); the 3 added open-order cash-op IDs reordered to
> be time-monotonic; **buy/sell reorder** — the pos 1334567890 purchase moved
> from 07-20 10:10 to 08-02 08:00 (after the 08-02 07:00 sell) so the
> chronological running balance never goes negative on a purchase (was
> −2537.14 on 07-20; now bottoms at 719.96); its ID 900035425→900045000 and
> Open Positions R13 Open time updated to match. Findings 2, 4, 5 reclassified
> INTENDED (see §2).
>
> **Row-numbering caveat:** the dump compacts an empty row in `Open Positions`,
> so dump row numbers ≠ openpyxl sheet rows for that sheet (dump R011 aggregate =
> sheet R12; dump R012 child = sheet R13). This doc keeps the dump's R-numbers for
> cross-reference with [xtb_sample_dump.txt](xtb_sample_dump.txt); the edit script uses true sheet
> rows. The dump is current (post-correction); cash-op R-numbers below are the
> current ones.

## 1. Summary table

| # | Check | Status | Computed | Expected |
|---|-------|--------|----------|----------|
| 1 | Closed Date from serial 38718 → 2006-01-01 | PASS | 2006-01-01 00:00:00 | 2006-01-01 |
| 2 | Closed Date to serial 46237.25 → 2026-08-03 06:00 | PASS | 2026-08-03 06:00:00 | 2026-08-03 06:00 |
| 3 | Cash Date from = Closed Date from | PASS | 38718 | 38718 |
| 4 | Cash Date to = Closed Date to | PASS | 46237.25 | 46237.25 |
| 5 | Open "Data as of" = Date to | PASS | 46237.25 | 46237.25 |
| 6 | Closed Open Time 46204.292 → 2026-07-01 07:00 | PASS | 2026-07-01 07:00:00 | 2026 |
| 7 | Closed Close Time 46236.292 → 2026-08-02 07:00 | PASS | 2026-08-02 07:00:00 | 2026 |
| 8 | Cash R006 tax timestamp → 2026-08-02 21:01 | PASS | 2026-08-02 21:01:00 | 2026 |
| 9 | Cash R007 interest timestamp → 2026-08-02 21:00 | PASS | 2026-08-02 21:00:00 | 2026 |
| 10 | Cash R008 transfer timestamp → 2026-08-02 20:00 | PASS | 2026-08-02 20:00:00 | 2026 |
| 11 | Cash R010 sell timestamp → 2026-08-02 07:00 | PASS | 2026-08-02 07:00:00 | 2026 |
| 12 | Cash R013 purchase timestamp → 2026-07-01 07:00 | PASS | 2026-07-01 07:00:00 | 2026 |
| 13 | Cash R014 sub+ timestamp → 2026-07-01 06:30 | PASS | 2026-07-01 06:30:00 | 2026 |
| 14 | Cash R015 sub- timestamp → 2026-07-01 06:30 | PASS | 2026-07-01 06:30:00 | 2026 |
| 15 | Cash R016 deposit timestamp → 2026-06-30 11:20 | PASS | 2026-06-30 11:20:10 | 2026 |
| 16 | Sale − Purchase = Profit/Loss | PASS | 5040.05 − 4300.04 = 740.01 | 740.01 |
| 17 | Gross Profit = Profit/Loss | PASS | 740.01 | 740.01 |
| 18 | Purchase Value = Vol×OpenPrice×OpenConvRate | PASS | 10.0001×100×4.3 = 4300.043 → 4300.04 | 4300.04 |
| 19 | Sale Value = Vol×ClosePrice×CloseConvRate | PASS | 10.0001×120×4.2 = 5040.0504 → 5040.05 | 5040.05 |
| 20 | Closed total Profit/Loss = sum | PASS | 740.01 | 740.01 |
| 21 | Closed total Gross Profit = sum | PASS | 740.01 | 740.01 |
| 22 | Cash ops Total = sum of 12 amounts | PASS | 1583.92 | 1583.92 |
| 23 | Free funds interest tax = −19% of interest | PASS | 19.0 = 19% of 100.01 | −19.0 |
| 24 | Cash stock sell = closed Sale Value | PASS | 5040.05 | 5040.05 |
| 25 | Cash stock purchase = −closed Purchase Value | PASS | −4300.04 | −4300.04 |
| 26 | Cash stock sell Position ID = closed Position ID | PASS | 1111122222 | 1111122222 |
| 27 | Cash stock purchase Position ID = closed Position ID | PASS | 1111122222 | 1111122222 |
| 28 | Transfer rate 0.230001 vs 1/conv-rate | INTENDED | 0.230001 (= 1/4.3478) outside [1/4.3, 1/4.2] | deliberate edge-case ccy rate; no same-time-same-pair conflict (see finding 5) |
| 29 | R011 Volume = R012+R013 | PASS | 11 = 7+4 | 11 |
| 30 | R011 Value = R012+R013 | PASS | 5544 = 3528+2016 | 5544 |
| 31 | R011 Net Profit = R012+R013 | PASS | 626.9 = 270.9+356 | 626.9 |
| 32 | R011 Net Profit % = R012% + R013% (sum) | PASS | 29.77 = 8.32+21.45 | 29.77 (fixed) |
| 33 | R014 Volume = R015 Volume | PASS | 4 = 4 | 4 |
| 34 | R014 Value = R015 Value | PASS | 3633.84 | 3633.84 |
| 35 | R014 Net Profit = R015 Net Profit | PASS | 313.84 | 313.84 |
| 36 | R014 Net Profit % = R015 Net Profit % | PASS | 9.45 | 9.45 |
| 37 | IP summary Value = R011 + R014 | PASS | 9177.84 = 5544+3633.84 | 9177.84 |
| 38 | IP summary Profit = R011 + R014 | PASS | 940.74 = 626.9+313.84 | 940.74 |
| 39 | My Trades Value = 0 | PASS | 0 | 0 |
| 40 | My Trades Profit = 0 | PASS | 0 | 0 |
| 41 | R012 Net Profit % formula | PASS | nprof/(val−nprof)×100 = 8.3172 → 8.32 | 8.32 (fixed) |
| 42 | R013 Net Profit % formula | PASS | nprof/(val−nprof)×100 = 21.4458 → 21.45 | 21.45 |
| 43 | R015 Net Profit % formula | PASS | nprof/(val−nprof)×100 = 9.4530 → 9.45 | 9.45 |
| 44 | R011 Net Profit % = sum of children | PASS | 8.32 + 21.45 = 29.77 | 29.77 (fixed; method = sum-of-children, see ambiguity 1) |
| 45 | R014 Net Profit % formula (aggregate) | PASS | nprof/(val−nprof)×100 = 9.4530 → 9.45 | 9.45 |
| 46 | Account number consistent across 3 sheets | PASS | 12345678 everywhere | 12345678 |
| 47 | Transfer comment source = main account | PASS | contains 12345678 | contains 12345678 |
| 48 | Subaccount transfer source = main account | PASS | both contain 12345678 | contains 12345678 |
| 49 | Open position IDs ≠ closed Position ID | PASS | {1334567890,1244567891,1244567890} vs 1111122222 | no overlap |

**Totals: 48 PASS, 0 FAIL, 1 INTENDED out of 49.** (Pass-2: checks 41 & 44 fixed
→ PASS; check 28 reclassified INTENDED. The aggregate-% sum-vs-weighted method
remains an open question — see §4 ambiguity 1 — but the sample is now internally
consistent under the sum-of-children method it adopts. The buy/sell reorder
(§3 correction 4) fixed a negative running balance; see the additional
running-balance check below the table.)

Additional checks performed outside the table:
- Open Positions detail timestamps: R012 open → 2026-08-02 08:00 (moved from 07-20 10:10 by the buy/sell reorder), R013 open → 2026-07-10 08:00, R015 open → 2026-07-10 08:00. All valid 2026 dates. PASS.
- R013 and R015 share implied open conversion rate = 4.15 PLN/EUR (from purchase = Value − NetProfit). R012's implied rate = 4.23 vs 4.15 for the other two SXR8.DE children — **intended edge-case rate variation** (different open timestamps 08-02 08:00 vs 07-10 08:00; ccy rates deliberately varied to stress conversion handling; see finding 2). R012's 4.23 also sits one hour after the closed position's close conv rate 4.2 (08-02 07:00) — a same-day distinct-timestamp pair, another intended edge case.
- Subaccount transfer comments: R011 (+10000) and R012 (−10000) both say "Transfer from 12345678 to 12348765" — **intended**: both legs describe the same transfer (My Trades → Investment Plans subaccount); signs differ by subaccount ledger (see finding 4).
- Cash-op ID order: the 3 added open-order rows are time-monotonic — 900035423/424 @07-10, and the pos 1334567890 purchase at 900045000 @08-02 08:00 (moved from 07-20 by the buy/sell reorder; id sits between the sell 900041122 @08-02 07:00 and the conversion 900051122 @08-02 20:00). Full series ID↑ = time↑. PASS (fixed).
- Chronological running balance: walking cash ops oldest→newest, the account balance never goes negative on a Stock purchase (min on a purchase = 719.96 after the 07-10 buys, before the 08-02 sell funds the 08-02 08:00 rebuy). Overall min = 0.00 at the net-zero subaccount-transfer leg (−10000 then +10000 at the same 07-01 06:30 timestamp), which is fine. PASS (fixed by the buy/sell reorder; was −2537.14 on 07-20 before it).

---

## 2. Findings

### Finding 1 — R012 Net Profit % was wrong by 0.01 (FIXED)

**Cell:** Open Positions, R012 (dump row; sheet row 13), column "Net Profit %" (col 13).
**Was:** 8.31. **Now:** 8.32.
**Formula:** `NetProfit / (Value − NetProfit) × 100` = 270.9 / (3528 − 270.9) × 100 = 270.9 / 3257.1 × 100 = **8.3172 → 8.32**. This is the only formula that reproduces the other two child rows exactly:
- R013: 356 / (2016 − 356) × 100 = 21.4458 → **21.45** ✓
- R015: 313.84 / (3633.84 − 313.84) × 100 = 9.4530 → **9.45** ✓

R012 was the outlier (stored 8.31 vs computed 8.32) — a rate-independent arithmetic typo from hand-anonymization. **Fixed to 8.32; R011 aggregate propagated 29.76 → 29.77** (= 8.32 + 21.45, preserving the sum-of-children method). Value/NetProfit (3528/270.9) are unchanged, so the open-order cash-op Amount −3257.1 = 3528 − 270.9 is unaffected.

### Finding 2 — R012's implied conversion rate differs from its siblings (INTENDED, not a bug)

The Open Positions sheet has no per-row conversion-rate column, but you can back out the open conversion rate from `purchase = Value − NetProfit` and `purchase = Vol × OpenPrice × openConvRate`:

| Row | Vol | Open | Value | NetProfit | Implied purchase (PLN) | Implied open conv rate |
|-----|-----|------|-------|-----------|------------------------|------------------------|
| R012 | 7 | 110 | 3528 | 270.9 | 3257.1 | **4.2300** |
| R013 | 4 | 100 | 2016 | 356 | 1660.0 | **4.1500** |
| R015 | 4 | 200 | 3633.84 | 313.84 | 3320.0 | **4.1500** |

R013 and R015 (both opened 2026-07-10 08:00) share open conv rate 4.15. R012 (opened 2026-08-02 08:00, after the buy/sell reorder — see §3 correction 4) implies 4.23. **This is intended:** the anonymized sample deliberately varies ccy rates across different timestamps to stress currency-conversion handling in the connector. R012's 4.23 now sits one hour after the closed position's close conv rate 4.2 (08-02 07:00) — a same-day, distinct-timestamp rate pair, exactly the kind of edge case the variation is meant to exercise. Per the user's rule, different timestamps → different rates are allowed; only a same-time-same-pair conflict would be a bug (none exists here — the two 07-10 rows share 4.15). Not re-anchored; R012's Value/NetProfit stay as-is. (The earlier "root cause of finding 1" framing is withdrawn — finding 1 was a pure % typo, independent of the rate.)

### Finding 3 — R011 aggregate OpenPrice was simple average, not volume-weighted (FIXED)

**Cell:** Open Positions, R011 (dump row; sheet row 12), "Open price" (col 9).
**Was:** 105. **Now:** 106.36.
Children: R012 open 110 (vol 7), R013 open 100 (vol 4).
- Simple average: (110 + 100) / 2 = **105** (was stored)
- Volume-weighted average: (7×110 + 4×100) / 11 = 1170/11 = **106.36** (now stored)

A real broker aggregate is volume-weighted; the simple average was an anonymization miscalc. **Fixed to 106.36.** R014 (NASDAQ 100, single child) is unchanged at 200 (both methods agree).

### Finding 4 — Subaccount transfer comments are identical with opposite signs (INTENDED, not a bug)

**Cells:** Cash Operations R014 and R015 (dump rows; sheet rows 14/15), "Comment" column.
- R011: amount **+10000**, comment "Transfer from 12345678 to 12348765", product "Investment Plans"
- R012: amount **−10000**, comment "Transfer from 12345678 to 12348765", product "My Trades"

Both comments are byte-identical ("from 12345678 to 12348765") and the amounts have opposite signs. **This is intended and correct:** the two rows are the two legs of a single internal transfer from the "My Trades" subaccount (12345678) to the "Investment Plans" subaccount (12348765). Each leg is booked on its own subaccount ledger, so the signs differ — My Trades debits −10000 (money leaves), Investment Plans credits +10000 (money arrives) — while the shared comment describes the same transfer direction. The earlier "flip the inbound comment to 'from 12348765 to 12345678'" recommendation is **withdrawn**; the duplicate comment is correct. (Processing-wise these rows are filtered out anyway — overhaul plan D7.)

### Finding 5 — Currency-conversion transfer rate inconsistent with position conversion rates (INTENDED edge case)

**Cell:** Cash Operations R008, comment "Exchange rate:0.230001" (PLN→EUR, amount −1000).
- Transfer: 1 PLN = 0.230001 EUR → 1 EUR = 4.3478 PLN.
- Closed position close conv rate (same day, 2026-08-02, 13 h earlier): 4.2 PLN/EUR.
- Closed position open conv rate: 4.3 PLN/EUR.
- Inverse range: 1/4.3 = 0.23256, 1/4.2 = 0.23810. The transfer rate 0.230001 sits **below** this range — i.e. the EUR is ~3.5% more expensive in the transfer than the close-conversion rate implies.

**Intended:** the anonymized sample deliberately uses a varied/non-round ccy rate here to exercise currency-conversion edge cases in the connector. The user's rule: a same-timestamp same-pair rate conflict would be a bug; this is not — every EUR/PLN rate in the sample sits at a distinct timestamp (4.3 @07-01 07:00; 4.15 @07-10 08:00 [two rows, same rate]; 4.2 @08-02 07:00; 4.23 @08-02 08:00; 4.3478 @08-02 20:00), so no conflict. (The 4.2 and 4.23 pair on 08-02 is one hour apart — same day, distinct timestamp, intended.) Not changed.

### Finding 6 — Float artifacts (BENIGN)

- R008 (Open Positions summary): Profit = 940.7399999999991 (should be 940.74)
- R011: Net Profit = 626.89999999999941 (should be 626.9), Net Profit % = 29.759999999999998 (was 29.76, now 29.77)
- R012: Net Profit = 270.89999999999964 (should be 270.9)
- R013: Net Profit = 355.99999999999977 (should be 356.0)

These are IEEE-754 representation artifacts from summing/subtracting decimal values, present in the original XLSX cell values. They are NOT anonymization bugs and will be handled by rounding at parse time. The connector should round Value/NetProfit/Profit columns to 2 decimals on read.

---

## 3. Final verdict

**Safe as a representative test fixture after the pass-2 corrections.** The
anonymization preserved all cross-sheet relationships (cash ops ↔ closed
positions, summary ↔ detail, dates, account numbers, totals). The per-row
inconsistencies that remained (R012 Net Profit % typo, R011 simple-average
OpenPrice, the 3 added cash-op IDs out of time order) are now fixed; the rate
and subaccount-comment "inconsistencies" are confirmed intended edge cases, not
bugs.

### Corrections applied (pass 2)

1. **R012 Net Profit %**: 8.31 → **8.32**; **R011 aggregate Net Profit %**:
   29.76 → **29.77** (sum-of-children preserved). ✓ DONE
2. **R011 aggregate Open price**: 105 → **106.36** (volume-weighted). ✓ DONE
3. **Cash-op ID order**: the 3 added open-order `Stock purchase` IDs reassigned
   to be time-monotonic — 900035423/424 @07-10 (positions 1244567890/1244567891)
   < 900035425 @07-20 (position 1334567890), all within 900035422 … 900041122.
   ✓ DONE (then superseded for pos 1334567890 by correction 4).
4. **Buy/sell reorder (running-balance fix)**: the pos 1334567890 `Stock
   purchase` was at 07-20 10:10 (id 900035425), which made the chronological
   running balance hit **−2537.14** — impossible on a cash/investment-plan
   account (the 10000 deposit can't cover all 4 buys totaling 12537.14 before
   the 08-02 sell). Moved to **08-02 08:00** (after the 08-02 07:00 sell,
   before the 08-02 20:00 conversion), id **900035425 → 900045000** (between
   sell 900041122 and conversion 900051122), and Open Positions R13 Open time
   updated 07-20 10:10 → 08-02 08:00 to match. Running balance now bottoms at
   **719.96** on a purchase (overall min 0.00 at the net-zero subaccount leg);
   Total unchanged 1583.92. ✓ DONE

### Withdrawn recommendations (reclassified INTENDED — no edit)

4. ~~R011 inbound subaccount transfer comment flip~~ — intended subaccount-ledger
   behavior (finding 4); both legs correctly share the direction comment.
5. ~~Transfer rate 0.230001 → 0.2381~~ — intended edge-case ccy rate (finding 5);
   no same-time-same-pair conflict.
6. ~~R012 rate re-anchor to 4.15~~ — intended edge-case rate variation across
   timestamps (finding 2); Value/NetProfit and the cash-op Amount −3257.1 stay.

---

## 4. Ambiguities — resolution in the overhaul plan

Fixture-specific ambiguities and how the plan resolves them
([xtb_overhaul_plan.md](xtb_overhaul_plan.md)):

| # | Ambiguity | Resolution |
|---|---|---|
| 1 | Aggregate Net Profit % — sum vs weighted? | **Open** — plan O3 (sample adopts sum-of-children 29.77; verify against a real multi-child report) |
| 2 | Aggregate OpenPrice — simple vs volume-weighted? | **Open** — plan O4 (sample now volume-weighted 106.36; verify) |
| 3 | No per-row conversion rate on Open Positions | Plan D5/D12 — use Value/NetProfit as-is in PLN; document as a known format limitation |
| 4 | Instrument column polymorphism (name vs numeric ID) | Plan D4 — key off Ticker, not Instrument |
| 5 | Aggregate rows have empty Type/Category/Current price/Open time | Plan D4 — identify aggregates by non-empty Instrument + empty Type + empty Current price |
| 6 | Cash-op ID ordering (ID↑ = time↑, not contiguous) | Plan D9 — order by Time, ID as tiebreaker; dedup by ID |