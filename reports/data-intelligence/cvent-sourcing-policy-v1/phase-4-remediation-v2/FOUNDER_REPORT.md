# Cvent Sourcing Policy
## Phase 4 — Targeted Remediation V2

**Date:** 2026-10-03  
**Policy:** `source-policy-v1`  
**Law:** VERIFY BEFORE REPLACE · no broad research sweep  

### Population separation (binding)

| Population | Count | Role in this phase |
|------------|------:|--------------------|
| P0 Cvent-only **canonical** | **2** | Active remediation |
| P1 scoring-impact | **11** | Active remediation |
| P2 customer-visible | **6** | Active remediation |
| P3 mixed provenance | **179** | Classify; research C/D only |
| Discovery shells (Cvent-only) | **256** | **Separate** — not canonical exposure |

The **256** prior discovery shells are **not** the same population as the **2** currently canonical Cvent-only records.

---

### A. Executive Summary

V2 confirms live HPC state after V1 safe writes and produces a normalized queue with explicit priority groups + cross-references.

| Gate | Result |
|------|--------|
| P0 processed | 2/2 |
| P1 processed | 11/11 |
| P2 processed | 6/6 |
| Cvent-only customer-visible verified truth | **0** |
| Cvent-only verified scoring | **0** |
| Records deleted | **0** |
| Mass overwrites | **0** |
| Verdict | **PASS** |

---

### B. Canonical Exposure

| Hotel | Classification | Live Rooms | Source |
|-------|----------------|------------|--------|
| Comfort Inn & Suites Irapuato | VERIFIED_INDEPENDENTLY | 110 | trusted_secondary_source · https://www.am.com.mx/guanajuato/2019/07/03/llega-nuevo-hote |
| Comfort Inn Queretaro Tecnologico | REMOVE_FROM_CURRENT_CANONICAL_USE | 41 | steward_review · https://www.cvent.com/venues/es-ES/queretaro/hotel/comfort-i |

- **Verified independently:** 1
- **Corrected:** 0
- **Still unverified / removed from canonical use:** 1

Both remain `Census Only / Not Owner-Facing`. Historical Cvent claims retained in steward notes / source history where applicable.

---

### C. Scoring Exposure

| Disposition | Count |
|-------------|------:|
| SAFE_CONFIRMED | 2 |
| SAFE_CORRECTED | 0 |
| BLOCK_FROM_SCORING | 9 |

No live ADP/GDI score recomputation. Historical HI/GDI ledgers blocked from verified scoring.

---

### D. Customer-Visible Exposure

| Disposition | Count |
|-------------|------:|
| DISPLAY_SAFE | 5 |
| DISPLAY_CORRECTED | 0 |
| HIDE_PENDING_VERIFICATION | 1 |

No Cvent-only hotel fact remains displayed as verified truth.

---

### E. Mixed Provenance

| Class | Count | Action |
|-------|------:|--------|
| A | 0 | Leave |
| B | 169 | Provenance normalize only |
| C | 9 | Active research candidates (report/scoring artifacts) |
| D | 0 | Active research (none asserted this pass) |

Classified total (excl. double-counting discovery-256): **178** (prior label 179 = 144 HBX-mixed shells + inventory B rows).

---

### F. Discovery Shells

| Classification | Count |
|----------------|------:|
| DISCOVERY_ONLY_ACTIVE | 0 |
| DISCOVERY_ONLY_STALE | 256 |
| SUPERSEDED | 0 |
| NEEDS_FUTURE_VERIFICATION | 0 |

**No spend** on the 256 unless elevated by customer/scoring impact or founder approval.

---

### G. ADP Impact

- Hotels impacted: **0**
- Attributes changed: **0**
- Remeasurement required: **0**

Phase 2 Cvent-discovery ADP guard remains active.

---

### H. GDI Impact

- Hotels impacted: **0**
- Opportunities reclassified: **0**

No market discovery re-run. No fit-score recompute.

---

### I. Remaining Risk

1. mx226 Rooms=41 retained with Cvent URL history — Low + steward_review; needs Tier A before restore.
2. Choice.com still bot-blocked for brand HTML.
3. 144 Class B mixed shells — optional label normalization later.
4. 256 discovery shells — inventory only.

---

### J. Cost

| Item | Value |
|------|------:|
| Closed without new fetch | 2 |
| New external fetches | 0 |
| Airtable reads | 2 |
| Airtable writes | 0 |
| Estimated $ | 0.00 |

Reused V1 verification evidence; V2 = live confirm + structured queue.

---

### K. Recommended Phase 5

1. Tier A verify mx226 (Choice brand/API or official fact sheet).
2. Optional Class B provenance label pass (no value rewrite).
3. Live HI Commercial/Event `sourceUrl` host sweep (read-only) if founder approves.
4. Do not spend on 256 discovery shells unless elevated.

---

**Verdict: PASS**
