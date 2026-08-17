# Sibling parent-brand CALA enrichment summary

**Date:** 2026-07-24  
**Scope:** Non-Active affiliations under Hilton / Accor / Marriott / IHG (fill-blank only; official sources).

## Applied updates

| Parent | Path | Records updated | Notes |
|--------|------|----------------:|-------|
| Accor | JSON-LD / meta on `all.accor.com` | **448** | Batches 80 + 150 + 218; descriptions (+ amenities when blank) |
| IHG | JSON-LD / meta on `ihg.com` | **236** | Batches 80 + 156; 2 blocked |
| Hilton | Directory Website/PID | **1** | Hampton; Garden Inn / Tru / DoubleTree / Embassy already filled or unmatched |
| Hilton | Amenities sync | **0** | Already present or no CTYHOCN |
| Marriott | Bazaarvoice descriptions | **0** | 17 MARSHA candidates; BV had no Description |
| Marriott | Country sitemap Website/PID | **3** | Most CALA Marriott W/PID already present |

**Total Airtable updates this pass: ~688**

## Before → after (PID→blank description)

| Affiliation | Before PID→desc | After (re-audit) |
|-------------|----------------:|-----------------:|
| ibis | 194 | **0** |
| ibis Styles | 52 | **0** |
| Mercure | 56 | **0** |
| ibis budget | 64 | **0** |
| Novotel | 43 | **0** |
| Holiday Inn Express | 87 | **0** |
| Holiday Inn | 89 | **2** |
| InterContinental / Crowne / Staybridge / etc. | high | largely cleared where Website present |

## Residual gaps (automation dead-ends)

1. **Blank Website / Property ID** — largest remaining score drivers (Garden Inn 24, Tru 14, Hampton 14, Accor soft brands without URL, Six Senses 10, aloft 7). Need directory match or steward URL paste; Hilton unmatched census names are not in the public directory.
2. **Marriott amenities** — Ritz-Carlton still ~11 PID→blank amen; overview pages Akamai-blocked; BV has no amenity payload.
3. **Marriott descriptions** — BV empty for remaining MARSHAs; steward overview HTML or wait for product API coverage.
4. **IHG soft-block** — 2 pages empty/blocked this run; rest worked.
5. **Choice / Wyndham / BWH** — not in this sibling pass (parent APIs blocked; steward-only).

## Scripts added

- `scripts/audit-sibling-parent-brand-cala-gaps.mjs`
- `scripts/backfill-accor-sibling-descriptions.mjs`
- `scripts/run-hilton-sibling-enrichment.mjs`
- `scripts/backfill-marriott-sibling-bazaarvoice.mjs`
- `scripts/backfill-ihg-sibling-descriptions.mjs`

## Reports

- `reports/sibling-parent-brand-cala-gaps.{md,csv,json}` (re-audited after apply)
- `reports/accor-sibling-descriptions-apply-log.json`
- `reports/ihg-sibling-descriptions-apply-log.json`
- `reports/hilton-sibling-enrichment-summary.json`
- `reports/marriott-sibling-bazaarvoice-plan.json`
- `reports/marriott-census-enrichment-apply-log.csv`

## Manual QA

- [ ] Spot-check 5 Accor ibis descriptions vs `all.accor.com` hotel pages
- [ ] Spot-check 5 Holiday Inn / Express descriptions vs `ihg.com` hoteldetail
- [ ] Confirm fill-blank: no overwrites on already-populated Description
- [ ] Hilton Garden Inn blank-Website rows still blank (expected — unmatched)

## Change impact

**High** — Airtable Hotel Census writes (Description, Website, Property ID, Amenities).  
**Rollback:** restore from Airtable revision history / prior field values for affected `rec…` IDs in apply logs.
