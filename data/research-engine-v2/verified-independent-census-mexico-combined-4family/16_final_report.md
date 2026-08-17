# Verified Independent Census — Mexico Combined 4-Family Baseline

**Status:** `mexico_vic_4family_baseline_locked_staging_ready`  
**Locked at:** 2026-08-04T23:13:45.164Z  
**Staging only** · No Airtable · No Brand Explorer activation · No Webhound · No production overwrite · No cross-family auto-merge

---

## 1. Executive summary

Locked **666** independently reconstructed Mexico hotel records across four parent families:

| Family | Records |
|--------|--------:|
| IHG (Wave 1A) | 195 |
| Hilton (Wave 1B) | 102 |
| Choice (Wave 1C) | 68 |
| Marriott (Wave 1D) | 301 |
| **Total** | **666** |

Data-eligible (staging): **580**. Cross-family fuzzy auto-merges: **0**. Fake temporal start dates / rooms / owners: **0**. Marriott steward overlay included; frozen Wave 1D artifacts **not** modified.

---

## 2. Freeze decision

**LOCKED** — `mexico_vic_4family_baseline_locked_staging_ready`

- Prior 3-family freeze: `cd7887e30418a9df1a91d275ec9c358ced113e6e8e9368b3ec8d0bcb1c8f574e`
- Combined 4-family freeze hash: `c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3`
- Record fingerprint: `d6089b875c4d6c896a74b3d4113093e34482d0e352098f33ab94925f9bb926cd`

---

## 3. Family comparison

| Family | Records | Unique Physical | Core % | Material % | Data-Eligible | Primary Source | Notes |
|--------|--------:|----------------:|-------:|-----------:|--------------:|----------------|-------|
| IHG | 195 | 195 | 100 | 56 | 191 | Official IHG Mexico directory / hoteldetail | Wave 1A locked; strong rooms coverage relative to later waves |
| Hilton | 102 | 102 | 100 | 71 | 102 | Official Hilton Mexico brand location pages (+ structured GraphQL) | Wave 1B locked; strong openDate; rooms mostly Unknown |
| Choice | 68 | 68 | 97 | 56 | 50 | Official Choice Mexico regional JSON-LD + MX* sitemap union | Wave 1C locked; 50 regional-complete data-eligible + 18 sitemap-only union |
| Marriott | 301 | 301 | 97 | 40 | 237 | Official Marriott Mexico country hotel-sitemap (sitemap-heavy) | Wave 1D locked after steward review; sitemap-heavy; material 40%; staging census safe, not production-ready; 5 Brand Unconfirmed overlay decisions |
| **Total** | **666** | — | — | — | **580** | — | Staging census |

Marriott unique physical = **source-unique / identity-safe staging count** (301 MARSHA), **not** fully coordinate-verified physical count.

---

## 4. Combined 666-record baseline

Families remain separately traceable to Wave 1A / 1B / 1C / 1D freeze hashes. Slim index: `01_combined_4family_index.json`.

---

## 5. Source lineage

See `03_source_lineage_map.json`. Marriott steward overlay path: `wave1d-marriott/steward-review/` (overlay only).

---

## 6. Property Identity V1 summary

- Combined records: **666**
- Unique physical by family: IHG 195 · Hilton 102 · Choice 68 · Marriott 301
- Cross-family auto-merges: **0**
- Campus / sibling / high-sim kept distinct: **9** (Marriott steward)
- Marriott coords: all **Unknown** (never 0,0)
- `Number(null) === 0` coords bug: **fixed and guarded**

---

## 7. Temporal Affiliation V1 summary

- Current affiliation: **As of discovery**
- Fake affiliation start dates: **0**
- Fake opening dates: **0**
- Prior affiliations: **Unknown** unless independently sourced
- Blocked source cases preserved
- No current affiliation inferred from legacy

---

## 8. Marriott steward review summary

Status: `wave1d_marriott_steward_review_minor_holds_ready_for_4_family_baseline_lock`

- Identity safe: 301 MARSHA / 301 records
- Four Points / Sheraton verified clean (11 / 5)
- City Express family verified (0 misparses)
- Cross-family exact/probable: **0**
- Blocking issues: **none**
- Freeze unmodified; overlay only

Minor holds:
- 5 Brand Unconfirmed (1 confirm_brand overlay candidate)
- 9 campus/high-sim physical steward pairs (no merge)
- All 301 records missing coordinates (sitemap limitation) — staging OK
- Material completeness 40% — census identity only; not production-ready

---

## 9. Brand Unconfirmed overlay

| Property | Action | Brand / note |
|----------|--------|--------------|
| Mexico City Marriott Reforma Hotel | `confirm_brand` | Marriott Hotels — map miss, overlay only |
| Gran Hotel de Puebla by HNF | `exclude_from_brand_completion` | — |
| Hotel Guadalajara Country Club by HNF | `exclude_from_brand_completion` | — |
| CASA MAYOR Saltillo | `steward_manual_review_required` | — |
| SJ Grand Hotel Monterrey | `steward_manual_review_required` | — |

Frozen Wave 1D source artifacts **not** altered.

---

## 10. Completeness by family

| Family | Core % | Material % |
|--------|-------:|-----------:|
| IHG | 100 | 56 |
| Hilton | 100 | 71 |
| Choice | 97 | 56 |
| Marriott | 97 | 40 |

---

## 11. Data-eligible summary

| Family | Data-eligible |
|--------|--------------:|
| IHG | 191 |
| Hilton | 102 |
| Choice | 50 |
| Marriott | 237 |
| **Total** | **580** |

---

## 12. Brand coverage by family

Marriott independently found **28** brands, including:

| Brand | Count |
|-------|------:|
| City Express by Marriott | 89 |
| Courtyard by Marriott | 29 |
| Design Hotels | 25 |
| City Express Plus by Marriott | 24 |
| City Express Junior by Marriott | 21 |
| Four Points by Sheraton | 11 |
| Sheraton | 5 |
| Marriott Bonvoy — Brand Unconfirmed | 5 |

Full table: `08_brand_coverage_by_family.json`

---

## 13. Cross-family steward queue

| Class | Count |
|-------|------:|
| Exact — steward review | 0 |
| Probable — steward review | 0 |
| Auto-merges | **0** |

---

## 14. Rejected fuzzy matches

| Class | Count |
|-------|------:|
| Rejected fuzzy | 6 |
| Insufficient evidence — no merge | 8 |

---

## 15. Brand Explorer completion readiness

**Small BE completion pilot ready** — **no activation**.

- Prefer non-Marriott stronger-material brands for first pilot
- Marriott brands: mostly `completion_partial` / `completion_hold` until material enrichment
- Brand Unconfirmed: `excluded_from_brand_completion` or `steward_review_required` unless overlay confirms

---

## 16. Staging migration readiness

| Gate | Status |
|------|--------|
| Staging migration ready | **YES** |
| Production overwrite ready | **NO** |

**Safe staging fields:** family, brand, property/canonical name, city, state/region, country, source URL/type/lineage, as-of discovery date, identity key, confidence.

**Unsafe:** missing rooms, owner, operator, open date, missing coordinates, unconfirmed brands without overlay confirmation, temporal start dates, images.

---

## 17. Limitations

- Marriott sitemap-only: no address/coords/rooms/owner/open date
- Choice property pages often Blocked (≠ closed)
- Hilton/Choice rooms weak
- Soft brands and campus annexes need FP enrichment before production
- Combined unique physical is family-sum staging identity — not a single geo-verified building graph

---

## 18. Recommended next steps

1. Optional: apply Marriott Reforma `confirm_brand` overlay in staging views (freeze unchanged)
2. Small BE completion pilot (non-Marriott first, or Marriott brands with steward-approved identity only)
3. First-party enrichment for Marriott material fields (rooms/coords) via safe paths
4. Do **not** overwrite production Hotel Census

---

## Acceptance

- [x] Combined reconciles to **666** (195+102+68+301)
- [x] Families separately traceable
- [x] Marriott steward overlay included; Wave 1D freeze unmodified
- [x] Cross-family fuzzy auto-merges = 0
- [x] Fake temporal starts / rooms / owners = 0
- [x] Missing coordinates remain Unknown
- [x] BE readiness + staging migration documented; production overwrite **not** ready
- [x] No Airtable / BE activation / production overwrite / Webhound
- [x] Freeze hash created: `c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3`
- [x] Status: `mexico_vic_4family_baseline_locked_staging_ready`
