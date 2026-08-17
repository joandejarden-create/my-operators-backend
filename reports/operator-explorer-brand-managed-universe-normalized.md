# Operator Explorer — Brand-Managed Universe Normalized

**Date:** 2026-08-10  
**Mode:** Design / data-normalization only  
**No Airtable changes · No Operator Fit changes**  
**Supersedes overlapping single-axis classes in:** `reports/operator-explorer-brand-managed-universe-discovery.md`

---

## 1. Two independent axes

### A. Operating Model (mutually exclusive — company form)

Answers: *What kind of operating company is this?*

| Value | Meaning | Prefer over |
| ----- | ------- | ----------- |
| **Third-Party Operator** | Independent manager; not a brand parent platform | — |
| **Brand / Operator** | Brand company whose commercial posture centers on branding ± operating/managing hotels (incl. franchise-primary brand platforms) | Old “Franchise / Brand Only” as an *operating model* |
| **Integrated Brand / Operator** | Brand company that materially owns and operates branded hotels; third-party management selective | Collapsing into “Confirmed Direct Manager” |
| **Integrated Owner / Brand / Operator** | Owns brand + significant owned portfolio + operates (e.g. Iberostar, RIU-class) | — |
| **Owner-Operator** | Operates owned assets; not a major multi-brand franchisor parent | — |
| **Asset Manager** | Asset-management-led; hotel ops via others or selective | — |
| **Hybrid** | Material mix of franchise + managed +/or owned ops at corporate scale (typical large groups) | Forcing a single of Brand vs Integrated when both are material |
| **To Be Confirmed** | Insufficient basis for a single model | Multi-select Operating Model |

**Reuse note:** Aligns with Fit/Shortlist “Third-party / Brand-managed / Hybrid” *ideas*, Company Profile operating-model concepts, and Core 5 `(Managed)` Masters — but **Operating Model ≠ Management Availability** and ≠ `submission_status`.

**Multi-select:** Not introduced. If a firm cannot fit one value, use **Hybrid** (document mix) or **To Be Confirmed**.

### B. Management Availability (mutually exclusive — owner engageability)

Answers: *Can a third-party hotel owner reasonably engage this company/platform to manage a hotel?*

| Value | Meaning |
| ----- | ------- |
| **Confirmed Direct Management** | Evidenced managed-hotel / development program or clear managed portfolio offered to third-party owners (still **scoped** by brand/geo/segment) |
| **Conditional / Scoped Management** | Management exists or is plausible only for limited brands, markets, ownership structures, or circumstances — or predominantly franchise with residual/limited management |
| **No Direct Management Identified** | Current research supports no material direct-management pathway for third-party owners (soft brands, pure franchise with no evidenced management, non-hotel parents) |
| **Management Availability Unknown** | Not yet evidenced either way |

**Does not mean:** “this company operates hotels it owns.” Owned ops ≠ availability to third-party owners.

### C. Management Scope (relational — not a company enum)

Company-level Confirmed Direct Management **never** means every brand everywhere.

Capture on typed **Operator Intelligence - Brand Relationships** (proposed):

`Relationship Type = Brand Managed Capability`

Scoped by: Brand · Brand Parent · Geography · Region · Segment/scale · Hotel type · Current/Historical · Evidence · Last verified.

**Never** infer project approval.

---

## 2. Reclassified Brand Basics parents (n = 34)

Evidence strength: **Strong** = Dealality source pack / official managed program already in repo or unambiguous public model used for architecture only; **Moderate** = well-known industry model pending dry-run source capture; **Weak** = thin/Draft Brand Basics only; **None** = unclassified stub.

| Parent | Operating Model | Management Availability | Management Entity | Scope Known? | Evidence Strength | Deep Calibration? |
| ------ | --------------- | ----------------------- | ----------------- | ------------ | ----------------- | ----------------- |
| Marriott International, Inc. | Hybrid | Confirmed Direct Management | Marriott International (Managed) — MxM alias | Partial (MxM docs; brand/geo TBD row-level) | Strong | Yes — Track 2 |
| Hilton Worldwide | Hybrid | Confirmed Direct Management | Hilton (Managed) — HMS alias | Partial | Strong | Yes — Track 2 |
| AccorHotels | Hybrid | Confirmed Direct Management | Accor (Managed) | Partial | Strong | Yes — Track 2 |
| Hyatt Hotels Corporation | Hybrid | Confirmed Direct Management | Hyatt (Managed) — *Master pending* | Partial | Moderate | Yes — Track 2 |
| InterContinental Hotels Group | Hybrid | Conditional / Scoped Management | IHG Hotels & Resorts (Managed) | Partial (franchise-heavy; managed subset) | Moderate | Yes — Track 2 |
| Minor Hotel Group Limited | Hybrid | Confirmed Direct Management | Minor Hotels (Managed); NH = brand scope not Master | Partial | Moderate | Yes — Track 2 |
| Sonesta International Hotels Corporation | Brand / Operator | Confirmed Direct Management | Sonesta — *Master pending* | Limited | Moderate | Yes — Track 2 |
| Radisson Hotel Group | Hybrid | Conditional / Scoped Management | Radisson Hotel Group — *Master pending* | Limited | Moderate | Yes — Track 2 |
| Four Seasons Hotels and Resorts | Brand / Operator | Confirmed Direct Management | Four Seasons — *Master pending* | Limited (managed model; geo TBD) | Moderate | Yes — Track 2 |
| Rosewood Hotel Group | Brand / Operator | Confirmed Direct Management | Rosewood — *Master pending* | Limited | Moderate | Yes — Track 2 |
| Mandarin Oriental Hotel Group | Integrated Brand / Operator | Conditional / Scoped Management | Mandarin Oriental — *Master pending* | Limited | Moderate | Yes — Track 2 |
| Shangri-La Hotels and Resorts | Integrated Brand / Operator | Conditional / Scoped Management | Shangri-La — *Master pending* | Limited | Weak | Yes — Track 2 |
| Iberostar Hotels & Resorts | Integrated Owner / Brand / Operator | Conditional / Scoped Management | Grupo Iberostar (existing) | Partial | Moderate | Yes — **Track 1 only** |
| Banyan Tree Hotels & Resorts | Integrated Brand / Operator | Conditional / Scoped Management | — | No | Weak | No — classify-only |
| The Peninsula Hotels | Integrated Brand / Operator | Conditional / Scoped Management | — | No | Weak | No — classify-only |
| Oetker Hotels | Integrated Brand / Operator | Conditional / Scoped Management | — | No | Weak | No — classify-only |
| Aman Group | Integrated Brand / Operator | Conditional / Scoped Management | — | No | Weak | No — classify-only |
| Wyndham Hotels & Resorts | Brand / Operator | Conditional / Scoped Management | — | No | Weak | No — classify-only |
| Choice Hotels International | Brand / Operator | Conditional / Scoped Management | — | No | Weak | No — classify-only |
| BWH Hotels | Brand / Operator | Conditional / Scoped Management | — | No | Weak | No — classify-only |
| Red Roof Franchise, UK | Brand / Operator | No Direct Management Identified | — | N/A | Weak | No |
| Preferred Hotels & Resorts | Brand / Operator | No Direct Management Identified | — | N/A | Moderate | No |
| Small Luxury Hotels of the World | Brand / Operator | No Direct Management Identified | — | N/A | Moderate | No |
| Leading Hotels of the World | Brand / Operator | No Direct Management Identified | — | N/A | Moderate | No |
| Hyatt Vacation Ownership | To Be Confirmed | No Direct Management Identified | — | N/A | Weak | No |
| Dealality Operator Setup placeholder | To Be Confirmed | No Direct Management Identified | — | N/A | None | No |
| Staycity Ltd | Owner-Operator | Management Availability Unknown | — | No | None | No |
| Northland Properties | Owner-Operator | Management Availability Unknown | — | No | None | No |
| Dovetail + Co | To Be Confirmed | Management Availability Unknown | — | No | None | No |
| Prem Group | To Be Confirmed | Management Availability Unknown | — | No | None | No |
| AmeriVu Inn and Suites | To Be Confirmed | Management Availability Unknown | — | No | None | No |
| Edyn Limited | To Be Confirmed | Management Availability Unknown | — | No | None | No |
| Coast Hotels Limited | To Be Confirmed | Management Availability Unknown | — | No | None | No |
| *(no parent)* | To Be Confirmed | Management Availability Unknown | — | No | None | No |

### Reconciliation — Management Availability (must = 34)

| Management Availability | Count |
| ----------------------- | ----: |
| Confirmed Direct Management | **8** |
| Conditional / Scoped Management | **12** |
| No Direct Management Identified | **6** |
| Management Availability Unknown | **8** |
| **Total** | **34** |

### Reconciliation — Operating Model (must = 34)

| Operating Model | Count |
| --------------- | ----: |
| Hybrid | **7** |
| Brand / Operator | **10** |
| Integrated Brand / Operator | **6** |
| Integrated Owner / Brand / Operator | **1** |
| Owner-Operator | **2** |
| Asset Manager | **0** |
| Third-Party Operator | **0** *(none of the Brand Basics parents)* |
| To Be Confirmed | **8** |
| **Total** | **34** |

---

## 3. Overlapping examples resolved

| Company | Operating Model | Management Availability | Rationale |
| ------- | --------------- | ----------------------- | --------- |
| **Four Seasons** | Brand / Operator | Confirmed Direct Management | Classic managed-brand model for owner-owned assets; not primarily an owned-estate REIT story |
| **Rosewood** | Brand / Operator | Confirmed Direct Management | Management-led luxury brand platform |
| **Mandarin Oriental** | Integrated Brand / Operator | Conditional / Scoped Management | Material owned/JV estate + selective management; do not treat as open global MxM-style offer |
| **Aman** | Integrated Brand / Operator | Conditional / Scoped Management | Owned/collection posture; third-party management not assumed |
| **Belmond** *(not in Brand Basics parents)* | Integrated Brand / Operator | Conditional / Scoped Management | LVMH collection; classify-only until Brand Basics parent exists |
| **Kerzner** *(seed only)* | Integrated Owner / Brand / Operator | Conditional / Scoped Management | Owner-developer-operator resorts |
| **Iberostar** | Integrated Owner / Brand / Operator | Conditional / Scoped Management | Owns+operates branded resorts; Track 1 Master only |
| **Meliá** *(seed; not Brand Basics parent yet)* | Hybrid | Conditional / Scoped Management | Owned + managed + franchise mix — Track 2 deep |
| **Barceló** *(seed)* | Integrated Owner / Brand / Operator | Conditional / Scoped Management | Owner-operator leaning; Track 2 for CALA/Europe diversity |
| **RIU / Palladium** *(seed)* | Integrated Owner / Brand / Operator | Conditional / Scoped Management | Classify-only |
| **Loews / Omni / Montage** *(seed)* | Integrated Brand / Operator or Brand / Operator | Conditional / Scoped → verify | Classify-only; Montage/Auberge lifestyle may skew Brand / Operator + Confirmed after sources |

Keep **Operating Model** separate from **whether owners can hire them**.

---

## 4. Franchise-oriented groups

| Company | Operating Model | Management Availability | Note |
| ------- | --------------- | ----------------------- | ---- |
| Choice | Brand / Operator | Conditional / Scoped Management | Franchise-predominant; not permanently “brand-only”; dry-run may move to No Direct Management Identified |
| Wyndham | Brand / Operator | Conditional / Scoped Management | Same |
| BWH | Brand / Operator | Conditional / Scoped Management | Membership/franchise |
| Sercotel *(seed only)* | Brand / Operator | Conditional / Scoped Management | Confirm before No Direct |
| Red Roof Franchise UK | Brand / Operator | No Direct Management Identified | Franchise naming + no evidenced BM pathway in current research |

---

## 5. Exact 27 deep-calibration entities

**Unique canonical companies = 12 + 15 = 27. No double-count.**

### Track 1 — Third-Party / Regional (12)

| # | Canonical entity | Master ID |
| - | ---------------- | --------- |
| 1 | Arbor Lodging (CALA) | `recF5Z87OAqFgndoq` |
| 2 | Hotel Equities (CALA) | `recWPKu5laVZxsvpn` |
| 3 | GHL Hoteles (GHL Holding) | `reciI2tYQBfMoMK9G` |
| 4 | Aimbridge Hospitality (LATAM) | `recGWxIJqnYHkJZFD` |
| 5 | Playa Hotels & Resorts | `rec3TUHT9Z4AnFp5P` |
| 6 | Grupo Hotelero Santa Fe | `reckyv9O0Y3auYpJJ` |
| 7 | Highgate | `recLjxtxIIVJaGbXK` |
| 8 | Driftwood Hospitality Management | `recKVILWcRLqrQlWs` |
| 9 | Atlantica Hotels International (AHI) | `recfwDdU5t9h4uFnZ` |
| 10 | Cenote Azul Operadores | `recQ6Cf8O2z0tiqBz` |
| 11 | Grupo Iberostar | `recwEHUotSGpfkZEJ` |
| 12 | Álvarez Argüelles Hoteles | `recjgHXqTJktijFUR` |

### Track 2 — Brand / Managed / Integrated deep (15)

| # | Canonical entity | Master ID | Operating Model | Management Availability |
| - | ---------------- | --------- | --------------- | ----------------------- |
| 1 | Marriott International (Managed) | `recGmiPhRt6hiayd9` | Hybrid | Confirmed Direct Management |
| 2 | Hilton (Managed) | `rec3Uwxe6ovpiokuN` | Hybrid | Confirmed Direct Management |
| 3 | Accor (Managed) | `recF2WqLqNVyKGz9E` | Hybrid | Confirmed Direct Management |
| 4 | IHG Hotels & Resorts (Managed) | `rec7IXYQYpKMYsrDl` | Hybrid | Conditional / Scoped Management |
| 5 | Hyatt (Managed) | *pending* | Hybrid | Confirmed Direct Management |
| 6 | Minor Hotels (Managed) | `rec8SrT3VjRkkYTxm` | Hybrid | Confirmed Direct Management |
| 7 | Sonesta International | *pending* | Brand / Operator | Confirmed Direct Management |
| 8 | Four Seasons Hotels and Resorts | *pending* | Brand / Operator | Confirmed Direct Management |
| 9 | Rosewood Hotel Group | *pending* | Brand / Operator | Confirmed Direct Management |
| 10 | Mandarin Oriental Hotel Group | *pending* | Integrated Brand / Operator | Conditional / Scoped Management |
| 11 | Radisson Hotel Group | *pending* | Hybrid | Conditional / Scoped Management |
| 12 | Meliá Hotels International | *pending* | Hybrid | Conditional / Scoped Management |
| 13 | Auberge Resorts Collection | *pending* | Brand / Operator | Confirmed Direct Management *(pending source confirm)* |
| 14 | Shangri-La Group | *pending* | Integrated Brand / Operator | Conditional / Scoped Management |
| 15 | Barceló Hotel Group | *pending* | Integrated Owner / Brand / Operator | Conditional / Scoped Management |

**Iberostar** is only in Track 1 — not duplicated in Track 2.

### Removed from calibration as duplicate entity names (aliases, not Masters)

| Alias / program | Resolves to |
| --------------- | ----------- |
| Managed by Marriott (MxM) | Marriott International (Managed) |
| Hilton Management Services | Hilton (Managed) |
| AccorHotels / Accor Group | Accor (Managed) |
| InterContinental Hotels Group (as separate Master) | IHG Hotels & Resorts (Managed) |
| NH Hotels (as separate Operator Master) | Minor Hotels (Managed) + brand-scope relationships |
| Iberostar (Managed) if proposed | Grupo Iberostar |

### Added because management capability discovered

| Entity | Why |
| ------ | --- |
| **Sonesta International** | In Brand Basics; management-heavy Brand / Operator — not in original marketing seed prominence |

### Remaining Brand Basics (classify-only — no deep Assignments)

All parents in §2 with Deep Calibration? = **No**, plus seed-only firms (Belmond, Kerzner, Dorchester, Langham, Loews, Omni, Montage, RIU, Palladium, H10, Piñero, Pestana, Eurostars, Sercotel, …): entity resolution + Operating Model + Management Availability + ready scope/evidence only.

### Unknown-management remaining (Brand Basics)

Dovetail + Co · Staycity Ltd · Prem Group · AmeriVu · Edyn · Coast Hotels · Northland Properties · *(no parent)* — **8**.

---

## 6. Correction to prior discovery language

| Prior overlapping label | Replacement |
| ----------------------- | ----------- |
| Confirmed Direct Manager | **Management Availability** = Confirmed Direct Management *(plus Operating Model separately)* |
| Limited / Conditional Direct Managers | **Management Availability** = Conditional / Scoped Management |
| Integrated Brand / Operators | **Operating Model** only — not a management-availability value |
| Franchise / Brand Only | Prefer Operating Model **Brand / Operator** + Availability Conditional or No Direct Management Identified |
| Management Availability Unknown | Unchanged axis value |

---

## 7. Confirmations

- No Airtable changes in this normalization.  
- No Operator Fit / scoring changes.  
- Owner pilot remains disabled.
