# Operator Explorer Architecture — Founder Review

**Date:** 2026-08-09  
**Mode:** Architecture audit only  
**Branch/Commit:** `app-shell-left-nav` / `3c88c0b`  
**Live base:** `appvtnDurnMSjINP6`  
**Confirmations:** No Airtable changes · No scoring changes · Owner pilot remains disabled (`OPERATOR_FIT_ENGINE_V2=0`, `OPERATOR_FIT_INTERNAL_PILOT=0`)

**Package index**

| Doc | Path |
| --- | ---- |
| Baseline | `reports/operator-explorer-architecture-baseline.md` |
| Brand reuse | `reports/operator-explorer-brand-research-reuse-audit.md` |
| Tables | `reports/operator-explorer-airtable-table-inventory.md` |
| Fields (886) | `reports/operator-explorer-airtable-field-inventory.md` |
| Dependencies | `reports/operator-explorer-field-code-dependency-map.md` |
| Universe | `reports/operator-explorer-current-universe-audit.md` |
| Assignments | `reports/operator-explorer-assignment-storage-audit.md` |
| Brand relationships | `reports/operator-explorer-brand-relationship-storage-audit.md` |
| Taxonomies | `reports/operator-explorer-taxonomy-audit.md` |
| Duplications | `reports/operator-explorer-schema-duplication-analysis.md` |
| Field recs | `reports/operator-explorer-field-recommendations.md` |
| UI code | `reports/operator-explorer-current-ui-code-audit.md` |
| Cohort | `reports/operator-explorer-calibration-cohort-recommendation.md` |
| Min profile | `docs/product/operator-explorer-minimum-profile.md` |
| Content model | `docs/product/operator-explorer-content-model.md` |
| Pipeline | `docs/process/operator-explorer-research-pipeline.md` |
| Publication policy | `docs/policy/operator-explorer-publication-policy.md` |
| Fixture isolation | `docs/data/operator-test-fixture-isolation-plan.md` |
| Proposed Airtable | `docs/architecture/operator-explorer-proposed-airtable-model.md` |
| Migration | `docs/architecture/operator-explorer-schema-migration-plan.md` |
| Live dump | `reports/operator-explorer-architecture-live-schema-dump.json` |

---

## 1. What Operator Explorer should become

An owner-useful, evidence-backed operator intelligence product — parallel to Brand Explorer in **research discipline**, not in franchise DAM/economics. It publishes honest operator facts (footprint, assignments, structures, brand experience) and feeds **Operator Fit** with structured evidence — without storing project Fit scores on the operator master.

## 2. What Brand Explorer infrastructure can be reused

- PI Source Library + governance / source ranking  
- Dry-run → backup → apply → post-write validation shell  
- Tab Factory / OS gate **pattern** (OE already has its own)  
- Publication policy concepts; exception queues for hard cases  
- Protected baseline freeze **pattern** (Arbor + Hotel Equities)  
- Evidence card shapes (momentum/openings) as UX patterns  

## 3. What cannot be reused

Brand Status universe / 62 freeze, Scene7/image uniqueness, Flexibility taxonomy, FDD/fee stacks, PRIMARY_RELEASE lists, per-brand writers as-is, Brand Explorer OS merged with OE OS.

## 4. Current Airtable table map

**26 operator-related tables**, including Master + 13 Operator Setup children, Claims, Market Presence, Shortlist, ODR, PI tables, Company Profile/Companies/Contacts.  
Canonical Explorer/Fit entity: **`Operator Setup - Master`** (`tbl4YPJ3XhnYLHLsD`).  
Legacy `3rd Party Operator - *` **not present** in live meta.

## 5. Current Airtable field map

**886 fields** fully inventoried from live meta (no sampling).  
Critical Fit/Explorer structured fields remain sparsely populated on Active (n=24): e.g. Conversion 0%, Offered Services ~8%, Active Countries improved to ~42% vs Aug 3’s 8% but still uneven; chain scales 100%.

## 6. Current operator universe quality

| Class | Count |
| ----- | ----: |
| Production Real | 15 |
| Real — Research Required | 9 |
| Real — Research Stage | 3 |
| Beta / Dummy | 9 |
| **Masters total** | **36** |

## 7. Dummy / test record findings

Nine In Review / demo companies (Antillano Norte + eight synthetic Spanish names). Must be isolated before owner-facing Explorer or Fit production. See isolation plan.

## 8. Current Operator Master strengths

- Clear canonical Master + child split  
- Lifecycle includes **Active** and **Research Stage**  
- PI + Claims + Market Presence + Shortlist already linked  
- Gold baseline operators exist with real fixtures  
- Fit v2 + publication policy code already drafted  
- Profile brand links + chain scales widely populated  

### Master field groups (audit)

| Group | Examples | Nature |
| ----- | -------- | ------ |
| Identity | company_name, operator_id | Objective |
| Lifecycle | submission_status | Workflow |
| Governance | Validation Status, Usage Permission, Company Validated, External Display | Workflow / trust |
| Research metadata | Data Confidence, Source Type, Last Reviewed, Refresh Due, Evidence Notes | Meta |
| Explorer presentation meta | Explorer Hero* | Presentation |
| Graph links | Children, PI, Claims, Presence, Shortlist, ODR, Users | Structural |

Missing: explicit **Record Purpose** (Production/Research/Test).

## 9. Current schema weaknesses

- Presentation completeness ≠ Fit readiness  
- Case Studies polluted taxonomies; weak assignment model  
- Brand Relationships table is **presentation**, not approvals  
- Claims enums mostly free text; only 28 claims  
- Dual geo models (Active Countries vs Presence)  
- Hundreds of sparse commercial/platform fields (setup debt)  
- Dummy companies share same Master table without Test Purpose  
- Company Profile vs Operator Master boundary easy to confuse  

## 10. Duplicate concepts

Active Countries ↔ Market Presence · brands ↔ Brand Families ↔ presentation Brand Rel · conversion flags ↔ case situations · Case Studies ↔ needed Assignments · Platform presence type ↔ Intelligence presence type · multiple Master “status-like” fields.

## 11. Claims-table verdict

**Keep and extend as the generalized claim/evidence backbone.**  
Good spine (21 fields); needs select normalization + PI Source links; volume still pilot-scale (28).

## 12. Market Presence verdict

**Keep as geographic SoT for Fit eligibility.**  
Presence types are correct; add optional city/property-count grain later; do not treat Strategic Interest as current ops.

## 13. Case Studies verdict

**Keep for owner stories.**  
**Not** sufficient long-term assignment inventory.

## 14. Recommended Assignment architecture

**Option C:** new `Operator Intelligence - Assignments` + keep Case Studies for selected stories. Conceptual entity covers operator/hotel/location/brand/keys/segment/development flags/dates/currentness/evidence (essential vs optional tiers in audit).

## 15. Brand Relationship verdict

**Need typed `Operator Intelligence - Brand Relationships` eventually.**  
Keep presentation table for Brand tab. Never infer global approval from one hotel.

## 16. Taxonomy issues

Worst: Case Study `situation` / `branded_independent` pollution.  
Lifecycle: prefer additive Record Purpose over a third Explorer Status enum.  
Claims: free text → selects.

## 17–23. Field recommendation classes

| Class | Direction |
| ----- | --------- |
| **KEEP** | Master identity/links; chain scales; structures; Presence; Claims spine; Case Study stories; PI; Shortlist/ODR; presentation row stores |
| **KEEP + NORMALIZE** | submission_status pairing; Claims enums; Case Study selects; Service Models; Brand Families; Data Confidence options |
| **DERIVE** | Active Countries summary; conversion/resort/urban flags; brand family lists; verified assignment counts |
| **MOVE / REHOME** | Assignment inventory out of Case Studies; typed brand edges out of presentation |
| **DEPRECATE LATER** | Platform flat Market Presence Type for scoring; bf_* score weight; opaque geo JSON as SoT |
| **ADD** | Record Purpose; Assignments table; typed Brand Relationships; Claims↔PI link; optional Presence grain |

## 24. Proposed Operator Explorer content sections

Overview · Operating Footprint · Portfolio Profile · Relevant Experience · Brand Relationships · Selected Assignments · Operating Structures · Differentiating Capabilities · Market Presence · Recent Momentum · Evidence / Last Verified.

## 25. Minimum Explorer Publishable profile

Identity + Active · chain scales · ≥1 typed current/office/portfolio presence · structures · brands list with honesty · ≥3 verified assignments or strong case studies · source/last-reviewed footnote.  
(See minimum profile doc.)

## 26. Fit Data Ready (separate)

Production Active · strong geo for project **or** honest conditional · structures · scales · evidenced comps/assignments for project type · brand depth when branded · evidence classes on material inputs · **no** Class 3 fiction.

## 27. Shared research architecture

```text
Shared Intelligence Research Core (thin)
  source discover/rank/capture · evidence · claims · conflicts · publish policy · write plan · backup · validate · refresh
        ├── Brand Research Adapter → Brand Explorer
        └── Operator Research Adapter → Operator Explorer → Operator Fit
```

Direct reuse PI/governance; adapt wave shell; **do not** merge OS machines; duplicate brand writers temporarily rather than force generics.

## 28. Scalable wave process

Company list → resolve → audit → plan → sources → assignments → claims → presence → brand edges → conflicts → publish resolver → dry-run → apply → validate → Explorer gate → Fit gate → exceptions.  
Founder approves policy, not every fact.

## 29. Proposed Airtable architecture

Master + existing children + **Claims** + **Market Presence** + PI + **proposed Assignments** + **proposed typed Brand Relationships** + Shortlist/ODR workflow-only. Minimize new schema.

## 30. Migration sequence

Phase 0 classify (now) → Phase 1 safe additions → Phase 2 dual-write/derive → Phase 3 consumer migration → Phase 4 deprecate → Phase 5 retire.

## 31. Recommended calibration cohort (12)

Arbor, Hotel Equities, GHL, Aimbridge LATAM, Playa, Santa Fe, Highgate, Driftwood, Atlantica, Cenote Azul, Iberostar, Álvarez Argüelles (Research Stage).  
Details: cohort report.

## 32. Exact founder decisions required before implementation

1. Approve **Record Purpose** (or Test Only) approach for dummy isolation  
2. Approve **Assignments table** (Option C) vs extending Case Studies only  
3. Approve eventual **typed Brand Relationships** table (timing: with Assignments vs later)  
4. Confirm Claims as SoT spine + select normalization  
5. Confirm Market Presence as geo SoT; Active Countries becomes hybrid/derived  
6. Approve calibration cohort of 12 (or edit list)  
7. Confirm Explorer Publishable ≠ Fit Data Ready gates  
8. Confirm publication policy draft classes  
9. Confirm shared core + adapters (not merged Brand/OE OS)  
10. Confirm owner pilot stays **OFF** until separate gate package  
11. Confirm no scoring changes in next research-build phase unless explicitly opened  

## 33. Recommended next phase

**Phase 0 execution + Phase 1 design spike (still mostly non-destructive):**

1. Mark/exclude dummy universe in loaders  
2. Founder approve Assignments + Record Purpose additions (dry-run schema ensure only)  
3. Implement operator research wave **dry-run** CLI against cohort (no apply)  
4. Extend Claims/Presence writers behind dry-run  
5. Keep Fit flags and owner pilot disabled  

---

## Stop-point answers (quick)

| Question | Answer |
| -------- | ------ |
| What tables exist? | 26 inventoried — see table inventory |
| What fields? | 886 — see field inventory |
| Populated? | Identity/scales strong; geo/structures/services/conversion weak–uneven |
| Used? | Critical scoring + Explorer maps yes; large commercial surface soft |
| Redundant? | Geo, brands, experience flags, case vs assignment |
| Legacy? | bf_*, flat presence type, outreach Companies |
| Derive? | Countries, experience flags, brand lists, assignment counts |
| Missing? | Assignments table, typed brand graph, Record Purpose |
| Reuse Brand code? | PI + gates shell + policy concepts — not DAM/FDD/Status |
| Shared core? | Thin yes; adapters; don’t merge OS |
| Assignments table? | **Yes (recommended)** |
| Brand Relationships table? | **Yes typed (recommended)** |
| Case Studies enough? | **No** for inventory |
| Real / RS / dummy? | 15+9 real-ish Active split, 3 RS, 9 dummy |
| Autonomous list processing? | Yes eventually under policy; exceptions for conflicts/sensitive/graduation |
| Min profile / page / Fit feed? | See product docs — Explorer facts vs Fit derivatives |

---

# ADDENDUM — Brand-Managed Operator Universe

**Detail (superseded classes):** `reports/operator-explorer-brand-managed-universe-discovery.md`  
**Normalized (authoritative):** `reports/operator-explorer-brand-managed-universe-normalized.md`  
**Tracks:** `reports/operator-explorer-brand-managed-calibration-tracks.md`  
**Entity policy:** `docs/data/operator-explorer-management-entity-resolution-policy.md`  
**Fit diagnostic (no code):** `docs/architecture/operator-fit-brand-managed-pathway-diagnostic.md`  
**Phase 0–1 dry-run schema:** `docs/architecture/operator-explorer-phase-0-1-schema-dry-run.md`

## Verdict

Track 1 (12) is not the full OE universe. Brand companies use the **same** intelligence model. **Operating Model** and **Management Availability** are **independent** axes — not one overlapping Candidate Type list.

## Classification normalization (2026-08-10)

| Axis | Role |
| ---- | ---- |
| Operating Model | Company form (Third-Party / Brand⁄Operator / Integrated… / Hybrid / …) |
| Management Availability | Can a third-party owner engage them to manage? |
| Brand Managed Capability | Relational scope — never global, never project approval |

Prior single-axis labels (Confirmed Direct Manager vs Integrated vs Franchise/Brand Only) are **retired** as one taxonomy.

## Normalized counts (Brand Basics parents = 34)

| Management Availability | n | Operating Model | n |
| ----------------------- | - | --------------- | - |
| Confirmed Direct Management | 8 | Hybrid | 7 |
| Conditional / Scoped Management | 12 | Brand / Operator | 10 |
| No Direct Management Identified | 6 | Integrated Brand / Operator | 6 |
| Management Availability Unknown | 8 | Integrated Owner / Brand / Operator | 1 |
| | | Owner-Operator | 2 |
| | | To Be Confirmed | 8 |
| **Total** | **34** | **Total** | **34** |

Deep-calibration unique entities: **27** (12+15). Iberostar once (Track 1).

## Extra founder decisions (BM)

1. Approve two-axis model.  
2. Approve Operating Model vocab (field later).  
3. Approve Brand Managed Capability on typed Brand Relationships.  
4. Approve entity alias policy (MxM/HMS/NH → existing Masters).  
5. Approve exact **27** deep-calibration entities.  
6. Approve pending Track 2 Masters only after dry-run — not now.
