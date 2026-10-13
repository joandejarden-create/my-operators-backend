# GDI Day-1 vs Live Reconciliation — 2026-10-02

## Summary

| Metric | Day-1 freeze (2026-10-01) | Live (2026-10-02) | Explained? |
|---|---:|---:|---|
| Ready (strict / customer-ready corpus) | **37** | Share list **35** visible; Demand Report exec ready **35** | YES |
| Visible | **37** | **35** in client share opportunities payload | YES |
| FUTURE_WATCH status (corpus) | **19** | Live share watch classifier ~**16**; Demand Report watch cards **12** | YES |

Day-1 snapshot file hashes were **not modified** this run.

## Exact ready delta (37 → 35)

Two Day-1 strict-ready IDs are **absent from the current client share opportunities payload**:

1. `gdi_opp_regulatory_information_conference_20260930` — Regulatory Information Conference — event date **2026-09-30**
2. `gdi_opp_accp_annual_meeting_20261001` — ACCP Annual Meeting — event date **2026-10-01**

Both still exist in the FS opportunity store as `HIGH_PRIORITY` / `customerVisible: true`.

**Cause class:** live **share / presentation timing surface** (events at or before Day-1 start falling out of the current customer-facing share list / Demand Report ready denominator), **not** Day-1 snapshot corruption and **not** ID rotation.

**Measurement law:** Day-1 freeze remains the immutable measurement reference. Live report counts may diverge via selection/filter/timing semantics.

## Future Watch delta (19 → 12/16)

| Layer | Count | Meaning |
|---|---:|---|
| Day-1 freeze `status=FUTURE_WATCH` | 19 | Full corpus status grain |
| Live share watch-like rows | ~16 | Client DTO priority/status mix |
| Demand Report `futureWatch` cards | 12 | Ranked presentation card set (not full watch universe) |

Differences are **selection logic + surface state**, not deletion of IDs from the frozen snapshot.

## Integrity

- Opportunity IDs unchanged (no remapping)
- Share token unchanged (`gdisht_47c25d74c79216021fb36150`)
- Day-1 snapshot unchanged
- Do **not** rewrite Day-1 freeze to match live presentation counts

## COUNT DIFFERENCES EXPLAINED

**YES**
