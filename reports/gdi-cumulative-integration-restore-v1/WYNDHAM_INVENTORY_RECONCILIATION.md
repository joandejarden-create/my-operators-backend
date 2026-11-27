# Wyndham San Diego Bayside — Inventory Reconciliation

**Hotel ID:** `recnPB3Q02NO8Ma1o`  
**Date:** 2026-11-27  
**No paid rediscovery. No CURRENT publish. No write-set apply.**

## Why the founder browser showed 0 of 0

1. **Port 8080 was owned by the wrong checkout** — process cwd = `C:\Dev\dealality\wt-he-unified-theme-v1` (`feat/he-unified-theme-v1`), not the GDI cumulative workspace.
2. That HE worktree had **no Wyndham `STAGED.json`** (`hasStagedPublication: false`).
3. Localhost GDI defaults to **STAGED** founder inventory mode → API returned **409 `STAGED_PUBLICATION_PREVIEW_UNAVAILABLE`** → UI empty state **0 of 0**.
4. Concurrently, Executive Summary / List View assets were absent from that checkout → only the original three tabs appeared.

Restored by reclaiming `:8080` for `C:\Dev\dealality\ws-gdi` cumulative staging tip. Live STAGED API now returns **count=11** (HTTP 200).

## Counts (governed)

| Bucket | Count | Notes |
|--------|------:|-------|
| Original discovered inventory (research corpus) | 161 | Research store / discovery scale — **not** all qualified |
| CLEAN11 STAGED package | **11** | `gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261125_3283ee92` · `PRODUCTION_STAGED` |
| Locked accepted proposed qualified | **26** | `08-PROPOSED_WRITE_SET_QA.json` — **not activated** |
| Of which already in STAGED | 11 | |
| Additional accepted proposed (missing from STAGED) | **15** | Review via proposed-write-set surface — **do not apply without auth** |
| WHO/HOW continuation candidates | 25 | Continuation research — **not** auto-qualified |
| HOLD | 2 | Preserved hold role |
| CURRENT package | **0** | No customer CURRENT publication |

## CLEAN11 STAGED opportunity IDs

```
gdi_lmc_2f01609319f56f89
gdi_lmc_b8ba887bff4dfc2e
gdi_opp_prospect_mv8p5ddc_75804200
gdi_opp_prospect_mv8p5dde_1a0faf00
gdi_opp_prospect_mv8p5dde_f892ef70
gdi_opp_prospect_mv8p5ddg_f7b6d183
gdi_opp_prospect_mv8p5ddh_7edb8e7c
gdi_opp_prospect_mv8p5ddh_b4fad256
gdi_opp_prospect_mv8p5ddj_c848701a
gdi_opp_prospect_mv8p5ddj_cf54ecae
gdi_sc_a7618f7bcd1270a7
```

**Package identity:**  
`publicationId=gdi_pubpkg_recnPB3Q02NO8Ma1o_197f86fdf8e9e6ff`  
`publicationVersion=gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261125_3283ee92`

## Founder review access (no activate / no publish)

- STAGED CLEAN11: default local authorized GDI Opportunities inventory.
- Locked proposed 26: `GET /api/admin/group-demand-intelligence/hotels/:hotelId/proposed-write-set-review` + report `reports/gdi-wyndham-final-commercial-completion-v1/08-PROPOSED_WRITE_SET_QA.json` / ES founder review HTML.
- PR #620 remains independently governed for commercial research readiness.
