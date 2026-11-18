# Commercial eligibility results (dry-run planner)

Eligibility gate: `gdi-full-e2e-airtable-persistence-eligibility-v1` via reconciliation V2 planner.

## Totals (8 hotels)

| Decision | Count |
|----------|------:|
| CREATE | **0** |
| UPDATE | **1** |
| UNCHANGED | 217 |
| DEFERRED | 334 |
| CONFLICT | 2 |
| REJECTED | 0 |

## CREATE candidates

**None.** No filesystem-only row in the eight-hotel set currently passes qualified GDO CREATE without founder override.

## UPDATE candidates

| Hotel | Opportunity ID | Rationale |
|-------|----------------|-----------|
| Bethesda | `gdi_opp_accp_annual_meeting_20261001` | Material commercial field change (priority HIGH → DISQUALIFIED) on shared ID |

## DEFERRED — by pattern

| Pattern | Hotels | Treatment |
|---------|--------|-----------|
| Disjoint ID namespaces (0 overlap) | YOTEL | FS `rx_d_` / `gdi_opp_yotel_fe2e_*` vs AT `gdi_opp_aifg_*` — all 150 DEFERRED |
| FS-only below eligibility | Radisson (36), Guadalajara (43) | Missing confirmed/reliable demand and/or credible entry point |
| Never persisted to GDO | Guadalajara | Provisional need periods ≠ GDO authorization |
| AT-only historical | YOTEL (108), Radisson (6), Bethesda (27), Nobu (65), Waterstone (7) | Preserve — no silent import |
| FS-only research continuation | YOTEL (42) | Not yet commercially eligible for GDO create |

## CONFLICT (blocks sync)

1. Radisson — `rx_d_f5c3bd3d073a` (org-name mismatch)
2. Bethesda — `gdi_opp_series_ctn_annual_conference_2027` (org-name variant)

## Demand thesis discipline

Speculative filesystem rows remain DEFERRED. Historical Airtable rows are not relabeled without evidence. No bulk import of 43 Guadalajara or 42 YOTEL FS-only rows.