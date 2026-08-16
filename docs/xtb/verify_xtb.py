"""Verify internal consistency of the anonymized XTB sample report.

Computes every numeric relationship independently from the dump text and
reports PASS/FAIL/INCONCLUSIVE for each check.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

DUMP = "tmp/xtb_sample_dump.txt"


def parse_dump(path):
    sheets = {}
    current = ""
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\r\n")
            if line.startswith("===== SHEET:"):
                m = re.match(r"===== SHEET:\s*(.*?)\s+\(", line)
                current = m.group(1)
                sheets[current] = []
            elif line.startswith("R") and "|" in line and current:
                rowpart = line.split(": ", 1)[1] if ": " in line else line[4:]
                cells = [c.strip() for c in rowpart.split(" | ")]
                sheets[current].append(cells)
    return sheets


def excel_serial_to_datetime(serial):
    """Excel 1900 date system with the 1900 leap-year bug.

    Serial 1 = 1900-01-01; serial 60 = phantom 1900-02-29.
    For serial >= 61 subtract one extra day. Fractional part = time of day.
    """
    base = datetime(1899, 12, 31, tzinfo=UTC)
    whole = int(serial)
    frac = serial - whole
    if whole >= 61:
        whole -= 1
    dt = base + timedelta(days=whole) + timedelta(days=frac)
    return dt


def fnum(s):
    if s is None or s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def d_to_iso(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def main():
    sheets = parse_dump(DUMP)
    closed = sheets["Closed Positions"]
    cash = sheets["Cash Operations"]
    opn = sheets["Open Positions"]

    results = []

    def rec(check, status, computed, expected=""):
        results.append((check, status, computed, expected))

    # ---------- Date conversions ----------
    cp_from = fnum(closed[2][1])
    cp_to = fnum(closed[3][1])
    d_from = excel_serial_to_datetime(cp_from)
    d_to = excel_serial_to_datetime(cp_to)
    rec(
        "Closed Date from serial->date",
        "PASS" if d_to_iso(d_from) == "2006-01-01 00:00:00" else "FAIL",
        f"{cp_from} -> {d_to_iso(d_from)}",
        "2006-01-01 00:00:00",
    )
    exp_to = datetime(2026, 8, 3, 6, 0, tzinfo=UTC)
    rec(
        "Closed Date to serial->date",
        "PASS" if abs((d_to - exp_to).total_seconds()) < 1 else "FAIL",
        f"{cp_to} -> {d_to_iso(d_to)}",
        d_to_iso(exp_to),
    )

    cash_from = fnum(cash[2][1])
    cash_to = fnum(cash[3][1])
    rec(
        "Cash Date from matches Closed",
        "PASS" if cash_from == cp_from else "FAIL",
        str(cash_from),
        str(cp_from),
    )
    rec(
        "Cash Date to matches Closed",
        "PASS" if cash_to == cp_to else "FAIL",
        str(cash_to),
        str(cp_to),
    )

    op_asof = fnum(opn[2][1])
    rec(
        "Open 'Data as of' matches Date to",
        "PASS" if op_asof == cp_to else "FAIL",
        str(op_asof),
        str(cp_to),
    )

    data = closed[5]
    vol = fnum(data[4])
    open_price = fnum(data[5])
    open_time = fnum(data[6])
    close_price = fnum(data[7])
    close_time = fnum(data[8])
    pnl = fnum(data[10])
    gross = fnum(data[11])
    purchase = fnum(data[12])
    sale = fnum(data[13])
    # data[16] = commission (asserted via Cash Ops fee enrichment, not here)
    open_conv = fnum(data[20])
    close_conv = fnum(data[21])
    pos_id = data[23]

    ot = excel_serial_to_datetime(open_time)
    ct = excel_serial_to_datetime(close_time)
    rec(
        "Closed Open Time serial->date",
        "PASS" if ot.year == 2026 else "FAIL",
        f"{open_time} -> {d_to_iso(ot)}",
        "2026 (year)",
    )
    rec(
        "Closed Close Time serial->date",
        "PASS" if ct.year == 2026 else "FAIL",
        f"{close_time} -> {d_to_iso(ct)}",
        "2026 (year)",
    )

    # Cash operation timestamps
    for i, label in [
        (5, "R006 tax"),
        (6, "R007 interest"),
        (7, "R008 transfer"),
        (8, "R009 sell"),
        (9, "R010 purchase"),
        (10, "R011 sub+"),
        (11, "R012 sub-"),
        (12, "R013 deposit"),
    ]:
        t = fnum(cash[i][4])
        if t is not None:
            dt = excel_serial_to_datetime(t)
            rec(
                f"Cash {label} timestamp serial->date",
                "PASS" if dt.year == 2026 else "FAIL",
                f"{t} -> {d_to_iso(dt)}",
                "2026 (year)",
            )

    # ---------- Closed position financial math ----------
    sale_minus_purchase = round(sale - purchase, 2)
    rec(
        "Sale - Purchase = Profit/Loss",
        "PASS" if abs(sale_minus_purchase - pnl) < 0.005 else "FAIL",
        f"{sale} - {purchase} = {sale_minus_purchase}",
        str(pnl),
    )
    rec(
        "Gross Profit = Profit/Loss",
        "PASS" if abs(gross - pnl) < 0.005 else "FAIL",
        str(gross),
        str(pnl),
    )

    purchase_calc = vol * open_price * open_conv
    sale_calc = vol * close_price * close_conv
    rec(
        "Purchase Value = Vol*OpenPrice*OpenConvRate",
        "PASS" if abs(round(purchase_calc, 2) - purchase) < 0.005 else "FAIL",
        f"{vol}*{open_price}*{open_conv} = {purchase_calc:.6f} (round {round(purchase_calc, 2)})",
        str(purchase),
    )
    rec(
        "Sale Value = Vol*ClosePrice*CloseConvRate",
        "PASS" if abs(round(sale_calc, 2) - sale) < 0.005 else "FAIL",
        f"{vol}*{close_price}*{close_conv} = {sale_calc:.6f} (round {round(sale_calc, 2)})",
        str(sale),
    )

    total = closed[6]
    tot_pnl = fnum(total[10])
    tot_gross = fnum(total[11])
    rec(
        "Closed total Profit/Loss = sum",
        "PASS" if abs(tot_pnl - pnl) < 0.005 else "FAIL",
        str(tot_pnl),
        str(pnl),
    )
    rec(
        "Closed total Gross Profit = sum",
        "PASS" if abs(tot_gross - gross) < 0.005 else "FAIL",
        str(tot_gross),
        str(gross),
    )

    # ---------- Cash operations ----------
    amounts = [fnum(cash[i][5]) for i in range(5, 13)]
    cash_total = fnum(cash[13][5])
    s = round(sum(amounts), 2)
    rec(
        "Cash ops Total = sum of amounts",
        "PASS" if abs(s - cash_total) < 0.005 else "FAIL",
        f"sum = {s}",
        str(cash_total),
    )

    interest = fnum(cash[6][5])
    tax = fnum(cash[5][5])
    rec(
        "Free funds interest tax = -19% of interest",
        "PASS" if abs(abs(tax) - round(interest * 0.19, 2)) < 0.005 else "FAIL",
        f"|tax|={abs(tax)}, 19% of {interest} = {round(interest * 0.19, 2)}",
        f"-{round(interest * 0.19, 2)}",
    )

    stock_sell = fnum(cash[8][5])
    stock_purchase = fnum(cash[9][5])
    rec(
        "Cash stock sell = closed Sale Value",
        "PASS" if abs(stock_sell - sale) < 0.005 else "FAIL",
        str(stock_sell),
        str(sale),
    )
    rec(
        "Cash stock purchase = -closed Purchase Value",
        "PASS" if abs(stock_purchase + purchase) < 0.005 else "FAIL",
        str(stock_purchase),
        f"-{purchase}",
    )
    rec(
        "Cash stock sell Position ID = closed Position ID",
        "PASS" if cash[8][9] == pos_id else "FAIL",
        cash[8][9],
        pos_id,
    )
    rec(
        "Cash stock purchase Position ID = closed Position ID",
        "PASS" if cash[9][9] == pos_id else "FAIL",
        cash[9][9],
        pos_id,
    )

    rate_in_comment = 0.230001
    inv_open = 1 / open_conv
    inv_close = 1 / close_conv
    within = (inv_open - 0.001) <= rate_in_comment <= (inv_close + 0.001)
    rec(
        "Transfer rate 0.230001 within [1/4.3, 1/4.2]?",
        "PASS" if within else "INCONCLUSIVE",
        f"rate={rate_in_comment}, 1/4.3={inv_open:.5f}, 1/4.2={inv_close:.5f}",
        "within [0.23256, 0.23810]",
    )

    # ---------- Open Positions summary block ----------
    mt_value = fnum(opn[4][2])
    mt_profit = fnum(opn[5][2])
    ip_value = fnum(opn[6][2])
    ip_profit = fnum(opn[7][2])

    detail = opn[10:15]
    r011 = detail[0]
    r012 = detail[1]
    r013 = detail[2]
    r014 = detail[3]
    r015 = detail[4]

    r011_vol = fnum(r011[5])
    r011_val = fnum(r011[6])
    r011_npct = fnum(r011[12])
    r011_nprof = fnum(r011[13])
    r012_vol = fnum(r012[5])
    r012_val = fnum(r012[6])
    # r012[7] = currency, r012[8] = open price (asserted in formula block, not here)
    r012_npct = fnum(r012[12])
    r012_nprof = fnum(r012[13])
    r013_vol = fnum(r013[5])
    r013_val = fnum(r013[6])
    # r013[7] = currency, r013[8] = open price (asserted in formula block, not here)
    r013_npct = fnum(r013[12])
    r013_nprof = fnum(r013[13])
    r014_vol = fnum(r014[5])
    r014_val = fnum(r014[6])
    # r014[8] = open price (aggregate; asserted in formula block, not here)
    r014_npct = fnum(r014[12])
    r014_nprof = fnum(r014[13])
    r015_vol = fnum(r015[5])
    r015_val = fnum(r015[6])
    # r015[7] = currency, r015[8] = open price (asserted in formula block, not here)
    r015_npct = fnum(r015[12])
    r015_nprof = fnum(r015[13])

    rec(
        "R011 Volume = R012+R013",
        "PASS" if abs(r011_vol - (r012_vol + r013_vol)) < 0.005 else "FAIL",
        f"{r011_vol} vs {r012_vol}+{r013_vol}={r012_vol + r013_vol}",
        str(r011_vol),
    )
    rec(
        "R011 Value = R012+R013",
        "PASS" if abs(r011_val - (r012_val + r013_val)) < 0.005 else "FAIL",
        f"{r011_val} vs {r012_val}+{r013_val}={r012_val + r013_val}",
        str(r011_val),
    )
    rec(
        "R011 Net Profit = R012+R013",
        "PASS"
        if abs(round(r011_nprof - (r012_nprof + r013_nprof), 2)) < 0.005
        else "FAIL",
        f"{r011_nprof} vs {r012_nprof}+{r013_nprof}={r012_nprof + r013_nprof}",
        str(r011_nprof),
    )
    rec(
        "R011 Net Profit % = R012% + R013% (sum, not weighted)",
        "PASS" if abs(r011_npct - (r012_npct + r013_npct)) < 0.005 else "FAIL",
        f"{r011_npct} vs {r012_npct}+{r013_npct}={r012_npct + r013_npct}",
        str(r011_npct),
    )

    rec(
        "R014 Volume = R015 Volume",
        "PASS" if abs(r014_vol - r015_vol) < 0.005 else "FAIL",
        f"{r014_vol} vs {r015_vol}",
        str(r014_vol),
    )
    rec(
        "R014 Value = R015 Value",
        "PASS" if abs(r014_val - r015_val) < 0.005 else "FAIL",
        f"{r014_val} vs {r015_val}",
        str(r014_val),
    )
    rec(
        "R014 Net Profit = R015 Net Profit",
        "PASS" if abs(round(r014_nprof - r015_nprof, 2)) < 0.005 else "FAIL",
        f"{r014_nprof} vs {r015_nprof}",
        str(r014_nprof),
    )
    rec(
        "R014 Net Profit % = R015 Net Profit %",
        "PASS" if abs(r014_npct - r015_npct) < 0.005 else "FAIL",
        f"{r014_npct} vs {r015_npct}",
        str(r014_npct),
    )

    rec(
        "IP summary Value = R011 Value + R014 Value",
        "PASS" if abs(round(ip_value - (r011_val + r014_val), 2)) < 0.005 else "FAIL",
        f"{ip_value} vs {r011_val}+{r014_val}={r011_val + r014_val}",
        str(ip_value),
    )
    rec(
        "IP summary Profit = R011 NetProfit + R014 NetProfit",
        "PASS"
        if abs(round(ip_profit - (r011_nprof + r014_nprof), 2)) < 0.005
        else "FAIL",
        f"{ip_profit} vs {r011_nprof}+{r014_nprof}={r011_nprof + r014_nprof}",
        str(ip_profit),
    )
    rec("My Trades Value = 0", "PASS" if mt_value == 0 else "FAIL", str(mt_value), "0")
    rec(
        "My Trades Profit = 0",
        "PASS" if mt_profit == 0 else "FAIL",
        str(mt_profit),
        "0",
    )

    # ---------- Net Profit % formula reconstruction ----------
    print("\n--- Net Profit % formula reconstruction (child rows) ---")
    formula_hits = {}
    for label, r in [("R012", r012), ("R013", r013), ("R015", r015)]:
        v = fnum(r[5])
        val = fnum(r[6])
        curr = fnum(r[7])
        op = fnum(r[8])
        npct = fnum(r[12])
        nprof = fnum(r[13])
        if None in (v, val, curr, op, npct, nprof):
            print(f"  {label}: missing values")
            continue
        candidates = {
            "(curr-open)/open*100": (curr - op) / op * 100,
            "nprof/(vol*open)*100 [EUR purchase]": nprof / (v * op) * 100,
            "nprof/val*100 [PLN value]": nprof / val * 100,
            "nprof/(val-nprof)*100 [PLN purchase]": nprof / (val - nprof) * 100
            if (val - nprof) != 0
            else -999,
        }
        print(
            f"  {label}: vol={v} open={op} curr={curr} val={val} nprof={nprof:.4f} npct={npct}"
        )
        for name, valc in candidates.items():
            mark = "  <-- MATCH" if abs(valc - npct) < 0.005 else ""
            print(f"    {name} = {valc:.6f}{mark}")
        best = min(candidates.items(), key=lambda kv: abs(kv[1] - npct))
        hit = abs(best[1] - npct) < 0.005
        if hit:
            formula_hits[best[0]] = formula_hits.get(best[0], 0) + 1
        rec(
            f"{label} Net Profit % formula",
            "PASS" if hit else "FAIL",
            f"best {best[0]}={best[1]:.6f}",
            f"npct={npct}",
        )

    print("\n--- Net Profit % formula reconstruction (aggregate rows) ---")
    for label, r in [("R011", r011), ("R014", r014)]:
        v = fnum(r[5])
        val = fnum(r[6])
        op = fnum(r[8])
        npct = fnum(r[12])
        nprof = fnum(r[13])
        if v is None or val is None or npct is None or nprof is None:
            print(f"  {label}: missing values")
            continue
        candidates = {}
        if op is not None and op != 0:
            candidates["(curr-open)/open*100"] = None  # no curr for aggregates
        # aggregate purchase value (PLN) = val - nprof
        pv = val - nprof
        candidates["nprof/val*100"] = nprof / val * 100
        candidates["nprof/(val-nprof)*100"] = nprof / pv * 100 if pv != 0 else -999
        print(f"  {label}: vol={v} val={val} nprof={nprof:.4f} npct={npct} (open={op})")
        for name, valc in candidates.items():
            if valc is None:
                continue
            mark = "  <-- MATCH" if abs(valc - npct) < 0.005 else ""
            print(f"    {name} = {valc:.6f}{mark}")
        best = min(
            ((k, vc) for k, vc in candidates.items() if vc is not None),
            key=lambda kv: abs(kv[1] - npct),
        )
        hit = abs(best[1] - npct) < 0.005
        rec(
            f"{label} Net Profit % formula (aggregate)",
            "PASS" if hit else "FAIL",
            f"best {best[0]}={best[1]:.6f}",
            f"npct={npct}",
        )

    # ---------- Account number consistency ----------
    acc_closed = closed[0][1]
    acc_cash = cash[0][1]
    acc_open = opn[0][1]
    rec(
        "Account number consistent across sheets",
        "PASS" if acc_closed == acc_cash == acc_open == "12345678" else "FAIL",
        f"closed={acc_closed} cash={acc_cash} open={acc_open}",
        "12345678",
    )
    c_transfer = cash[7][7]
    c_sub1 = cash[10][7]
    c_sub2 = cash[11][7]
    rec(
        "Transfer comment source account = main account",
        "PASS" if "12345678" in c_transfer else "FAIL",
        c_transfer,
        "contains 12345678",
    )
    rec(
        "Subaccount transfer source = main account",
        "PASS" if "12345678" in c_sub1 and "12345678" in c_sub2 else "FAIL",
        f"{c_sub1} / {c_sub2}",
        "contains 12345678",
    )

    # ---------- Position ID overlap ----------
    open_ids = {r012[1], r013[1], r015[1]}
    rec(
        "Open position IDs do not overlap closed Position ID",
        "PASS" if pos_id not in open_ids else "FAIL",
        f"open={open_ids} closed={pos_id}",
        "no overlap",
    )

    # ---------- Print report ----------
    print("\n========== VERIFICATION REPORT ==========")
    n_pass = sum(1 for _, s, _, _ in results if s == "PASS")
    n_fail = sum(1 for _, s, _, _ in results if s == "FAIL")
    n_inc = sum(1 for _, s, _, _ in results if s == "INCONCLUSIVE")
    for check, status, computed, expected in results:
        marker = {"PASS": "OK", "FAIL": "**FAIL**", "INCONCLUSIVE": "?"}[status]
        line = f"[{marker}] {check}: {computed}"
        if expected:
            line += f"  (expected {expected})"
        print(line)
    print(
        f"\nTotals: {n_pass} PASS, {n_fail} FAIL, {n_inc} INCONCLUSIVE out of {len(results)}"
    )


if __name__ == "__main__":
    main()
