# Cvent Sourcing Policy
## Phase 4 — Targeted Remediation

**Date:** 2026-10-03  
**Policy:** `source-policy-v1`  
**Mode:** VERIFY BEFORE REPLACE · no broad rewrite · no deletes  

---

### A. Executive Summary

Phase 4 processed all **2** Cvent-only canonical records, all **11** scoring-impact exposures, and all **6** customer-visible exposures. Mixed provenance (**179**) was classified without blanket research.

| Outcome | Result |
|---------|--------|
| mx092 Rooms/Keys | **VERIFIED_INDEPENDENTLY** — 110 confirmed via Choice México DG quoted in Periódico AM; provenance upgraded off Cvent |
| mx226 Rooms/Keys | **REMOVE_FROM_CURRENT_CANONICAL_USE** — Cvent 41 conflicts with Tier C directories (36); value retained; Low + steward_review; scoring/display blocked |
| Live Airtable writes | **2** targeted provenance patches only |
| Records deleted | **0** |
| Mass overwrites | **0** |
| Active Cvent-only customer-visible hotel facts | **0** |
| Active Cvent-only verified scoring | **0** |

---

### B. P0 Cvent-Only Canonical

| Hotel | Identity | Live before | Classification | After |
|-------|----------|-------------|----------------|-------|
| Comfort Inn & Suites Irapuato | `ind_choice_mx_mx092` / `recabSgALHHvys0In` | 110 · Cvent Medium | VERIFIED_INDEPENDENTLY | 110 · High · AM.com.mx · trusted_secondary_source |
| Comfort Inn Queretaro Tecnologico | `ind_choice_mx_mx226` / `rec8OtmFD9eqKORYs` | 41 · Cvent Medium | REMOVE_FROM_CURRENT_CANONICAL_USE | 41 retained · Low · steward_review · needsSourceReview |

Both remain `Production Use Status = Census Only / Not Owner-Facing`. Cvent provenance preserved in steward notes / historical source URL where not superseded.

Evidence: `P0_CVENT_ONLY_CANONICAL.csv`, `VERIFICATION_EVIDENCE.json`.

---

### C. Scoring-Impact Exposure

**11 / 11 processed.**

| Disposition | Count |
|-------------|------:|
| SAFE_CONFIRMED | 2 |
| SAFE_CORRECTED | 0 |
| BLOCK_FROM_SCORING | 9 |
| NEEDS_MANUAL_REVIEW | 0 |

- Live consumer: mx092 confirmed safe under independent provenance; mx226 blocked from scoring.
- 9 HI/GDI report artifacts: historical only; marked BLOCK_FROM_SCORING / not live Airtable scoring stores. No ADP/GDI score recomputation run.

---

### D. Customer-Visible Exposure

**6 / 6 processed.**

| Disposition | Count |
|-------------|------:|
| DISPLAY_SAFE | 5 |
| DISPLAY_CORRECTED | 0 |
| HIDE_PENDING_VERIFICATION | 1 |

- mx092 display-safe under independent verification (still Census Only / Not Owner-Facing).
- mx226 hidden pending verification (already Not Owner-Facing).
- ADP published Waterstone evidence cites `cvent.com/venues/results/…` market search — not a hotel property SoT claim; Phase 2 ADP guard still blocks Cvent-only attribute activation.
- Mention-only fixtures: display-safe.

**Current Cvent-only customer-visible hotel facts: 0.**

---

### E. Mixed Provenance

Classified **without** researching all 179:

| Class | Meaning | Count |
|-------|---------|------:|
| A | Already independently supported | 0 |
| B | Cvent + acceptable / normalize only | 167 |
| C | Weak / research-now (report + scoring artifacts) | 11 |
| D | Cvent effectively carries fact | 0 (within mixed-179; P0 handled separately) |

Only C (and future D) are research candidates. A/B left untouched except P0 safe writes already applied.

---

### F. ADP Impact

| Metric | Value |
|--------|-------|
| ADP hotels with live attribute mutation | **0** |
| ADP remeasurement required | **0** |
| Fixture/evidence packages rewritten | **0** |

Phase 2 runtime guards remain in force. Choice MX P0 rows are Census Only / Not Owner-Facing — no ADP attribute activation path touched.

---

### G. GDI Impact

| Metric | Value |
|--------|-------|
| GDI hotels with live fit recompute | **0** |
| Opportunities reclassified | **0** |
| Market discovery re-run | **No** |

Historical GDI ledgers with Cvent venue URLs inventoried and blocked from verified scoring use; no opportunity promotion.

---

### H. Remaining Risk

1. **mx226 Rooms/Keys = 41** still stored (retained by policy) with Cvent source URL — Low confidence + steward_review + Not Owner-Facing. Needs Tier A Choice/official verify before any canonical restore (directories say 36).
2. **144 Cvent+HBX shells** — Class B; provenance normalization backlog (Phase 5 optional).
3. **Choice.com** bot-blocked this run — Tier A brand HTML unavailable; mx092 closed via press quoting Choice México DG.
4. Historical HI/GDI JSON under `reports/` still contain Cvent venue URLs — offline artifacts only.

---

### I. Cost

| Item | Count / estimate |
|------|------------------|
| New external fetches | 3 (Choice×2 blocked/timeout + AM.com.mx confirm) |
| Airtable reads | 2 |
| Airtable writes | 2 |
| Web search assists | 2 (mx092 + mx226 directories) |
| Estimated verification cost | **~$0.00–0.05** (no paid enrichment / no Webhound) |
| Records verified without new paid fetch | 2 (curated evidence + free press fetch) |

---

### J. Recommended Phase 5

1. Tier A verify mx226 via Choice brand/API or official fact sheet; correct 41→verified value only then.
2. Optional provenance normalization pass on Class B mixed (144 shells) — labels only, no value rewrite.
3. Live Airtable HI Commercial/Event Space `sourceUrl` host sweep (read-only) beyond local inventory.
4. Do **not** re-run ADP measurement or GDI market discovery unless a material verified property truth changes on an active ADP/GDI hotel.

---

**Verdict: PASS** — acceptance criteria met; targeted only; historical provenance preserved.
