# Verified Independent Census — Mexico Combined Baseline

**Status:** `mexico_vic_baseline_locked_ready_for_marriott_wave1d`  
**Locked at:** 2026-08-04T22:55:52.330Z  
**Staging only** · No Airtable writes · No Brand Explorer activation · No Webhound · No legacy-as-evidence · No production overwrite

---

## 1. Executive summary

Verified Independent Census (VIC) has reconstructed **365** independent Mexico hotel records across **IHG (195)**, **Hilton (102)**, and **Choice (68)**. Families remain separately traceable. Property Identity V1 and Temporal Affiliation V1 are locked into the baseline. Cross-family fuzzy auto-merges: **0**. Fake temporal start dates: **0**.

**Data-eligible (staging):** **343** (IHG 191 + Hilton 102 + Choice 50).

**Next:** Marriott Mexico Wave 1D — ready when steward approves; **not launched**.

---

## 2. IHG / Hilton / Choice comparison

| Metric | IHG Wave 1A | Hilton Wave 1B | Choice Wave 1C |
|--------|-------------|----------------|----------------|
| Independent hotels | 195 | 102 | 68 |
| Unique physical (family) | 195 | 102 | 68 |
| Core % | 100% | 100% | 97% |
| Material % | 56% | 71% | 56% |
| Data-eligible | 191 | 102 | 50 |
| Exact legacy matches | 151 | 0 | 29 |
| Probable legacy | — | 2 | 11 |
| Independent-only | 29 | 100 | 28 |
| Legacy-only | 63 | 24 | 13 |
| Firewall pre-freeze blocked | yes | yes | yes |
| External research cost | $0 | $0 | $0 |

---

## 3. Combined 365-record Mexico VIC

| Family | Records | Traceability |
|--------|---------|--------------|
| IHG | 195 | `verified-independent-census-v1` |
| Hilton | 102 | `verified-independent-census-wave1b-hilton` |
| Choice | 68 | `verified-independent-census-wave1c-choice` |
| **Total** | **365** | Combined index + per-wave freeze hashes |

Slim combined index: `data/research-engine-v2/verified-independent-census-mexico-combined/02-combined-record-index.json`

Combined freeze hash: `cd7887e30418a9df1a91d275ec9c358ced113e6e8e9368b3ec8d0bcb1c8f574e`

---

## 4. Property Identity V1 performance

- Module: `lib/research-engine-v2/clean-census/property-identity.js`
- Choice Wave 1C: **68** records → **68** unique physical properties
- Intra-Choice collapses: **0**
- Fuzzy-name-only merges: **0**
- Cross-family auto-merges: **0**
- Evidence gates: official ID / URL / strong coords / address — name alone insufficient

---

## 5. Temporal Affiliation V1 performance

- Module: `lib/research-engine-v2/clean-census/temporal-affiliation.js`
- Current affiliation seeded as **As of [discovery date]**
- Precision: exact | as_of | before | unknown
- Fabricated start dates: **0**

---

## 6. Core / material completeness by family

| Family | Core | Material |
|--------|------|----------|
| IHG | 100% | 56% |
| Hilton | 100% | 71% |
| Choice | 97% | 56% |

---

## 7. Data-eligible by family

| Family | Data-eligible |
|--------|---------------|
| IHG | 191 |
| Hilton | 102 |
| Choice | 50 (50 regional-complete; 18 sitemap-only not data-eligible) |
| **Total** | **343** |

Image eligibility remains separate — generally **Needs First-Party Media** / not production-ready.

---

## 8. Independent-only vs legacy-only vs overlap

| Family | Exact match | Probable | Independent-only | Legacy-only |
|--------|-------------|----------|------------------|-------------|
| IHG | 151 | — | 29 | 63 |
| Hilton | 0 | 2 | 100 | 24 |
| Choice | 29 | 11 | 28 | 13 |

Hilton note: legacy Parent=Hilton Mexico cohort was sparse; high independent-only is a product signal, not a matcher failure (hardened identity matching).

---

## 9. Known gaps

1. **Rooms** — Hilton/Choice weak; IHG strong  
2. **Open date** — Hilton strong; IHG/Choice weak  
3. **Management / owner** — weak across families  
4. **Property pages 403** — Choice; Blocked ≠ closed  
5. **Steward review required** before any staging migration write

---

## 10. Brand Explorer completion readiness

**READY FOR SMALL BRAND COMPLETION PILOT** — do not activate.

Use independent Census + FP packs + PVQL/Tab Factory. RIA relationship documented independently; no Mexico directory rows labeled “Radisson Individuals Americas”; Faranda-named **0**; El Cid appeared as Ascend on Choice.

---

## 11. Staging-only migration recommendation

**PILOT MIGRATION READY** — staging Verified Independent Hotel Census table only.

Do **not** overwrite legacy Hotel Census. Do **not** write Airtable in this lock step.

---

## 12. Marriott Mexico readiness recommendation

**READY TO LAUNCH WAVE 1D WHEN STEWARD APPROVES** — not launched by this baseline lock.

Rationale: three-family Mexico VIC locked; architecture generalized; identity + temporal affiliation V1 in place; Marriott is next highest Census/BE volume opportunity.

---

## Acceptance checklist

- [x] Combined Mexico VIC reconciles to **365**
- [x] IHG, Hilton, Choice separately traceable
- [x] No cross-family fuzzy auto-merges
- [x] No fake temporal start dates
- [x] No production writes
- [x] No Airtable writes
- [x] No Webhound dependency
- [x] Status: `mexico_vic_baseline_locked_ready_for_marriott_wave1d`
