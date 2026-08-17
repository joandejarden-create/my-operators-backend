# Operator Explorer — Field Recommendations

**Date:** 2026-08-09  
**Scope:** Operator-relevant fields (886 inventoried). Recommendations are **policy only** — not applied.  
**Legend:** KEEP · KEEP + NORMALIZE · DERIVE · MOVE / REHOME · MERGE · DEPRECATE LATER · RETIRE CANDIDATE · ADD

Full field list: `reports/operator-explorer-airtable-field-inventory.md`  
Dependencies: `reports/operator-explorer-field-code-dependency-map.md`

---

## Master (Operator Setup - Master)

| Field / group | Rec | Rationale | Dependency risk |
| ------------- | --- | --------- | --------------- |
| company_name, operator_id | KEEP | Identity | High |
| submission_status | KEEP + NORMALIZE | Universe gate; add Test Only or pair with Record Purpose | High |
| Validation Status, Usage Permission, Company Validated*, External Display Status | KEEP | Governance / trust | Medium |
| Data Confidence Level, Source Type, Last Updated/Reviewed, Refresh Due | KEEP + NORMALIZE | Research metadata | Medium |
| Evidence Notes, Missing Data Flags, Internal Notes | KEEP | Internal | Low |
| Links to children / PI / Claims / Presence / Shortlist | KEEP | Graph | High |
| Explorer Hero* | KEEP | Presentation meta | Low |
| Record Purpose (Production/Research/Test Fixture) | **ADD** | Isolate dummies without overloading submission_status | Low if additive |
| Operator Explorer publish gate fields (if needed) | ADD only if submission_status insufficient | Prefer reuse first | — |

\*Company Validated checkbox currently 0% on Active — still KEEP for governance.

---

## Profile & Positioning

| Field / group | Rec | Rationale | Risk |
| ------------- | --- | --------- | ---- |
| chainScalesSupported | KEEP | High Fit + coverage | High |
| Service Models Supported | KEEP + NORMALIZE | Align vocab | Medium |
| brands (Brand Basics link) | KEEP | Brand list | High |
| Brand Families Operated | KEEP + NORMALIZE → eventual DERIVE | Soft duplicate of brands | Medium |
| Positioning / narrative / card blobs | KEEP for Explorer; DEPRECATE LATER for Fit | Marketing | Medium |
| Parent / HQ-like fields if present on profile | KEEP | Identity | Medium |

---

## Platform & Markets

| Field / group | Rec | Rationale | Risk |
| ------------- | --- | --------- | ---- |
| Active Countries | KEEP + eventual DERIVE/hybrid | Still read by Fit; Presence is richer SoT | High |
| Active Markets / Cities | KEEP + NORMALIZE | Soft geo | Medium |
| Market Presence Type (flat) | DEPRECATE LATER (scoring) | Superseded by Presence table | Medium |
| Platform JSON / geography blobs | DEPRECATE LATER | Opaque dual store | Medium |
| Other sparse platform fields | KEEP until binding audit; many RETIRE CANDIDATE later | Unknown consumers | High if deleted |

---

## Commercial Fit & Terms

| Field / group | Rec | Rationale | Risk |
| ------------- | --- | --------- | ---- |
| Management Structures Supported | KEEP | Critical Fit | High |
| New-Build / Pre-Opening / Conversion experience | KEEP + future DERIVE | Empty conversion; assign-derived | Medium |
| bf_selected_deal_structures | MERGE toward Management Structures; DEPRECATE LATER | Legacy | Medium |
| bf_not_ideal_for | KEEP for Explorer caution copy; DEPRECATE LATER for score weight | Marketing | Medium |
| Fee / commercial narratives | KEEP internal; never auto-publish Class 3 | Sensitive | High if exposed |
| Large unused commercial surface | DEPRECATE LATER / RETIRE CANDIDATE after binding proof | Setup form debt | High if premature |

---

## Governance, Delivery & Diligence

| Field / group | Rec | Rationale | Risk |
| ------------- | --- | --------- | ---- |
| Offered Services | KEEP; scoring caution | Catalog useful; table-stakes risk | Medium |
| Owner Reporting / Governance Cadence | KEEP | Fit medium | Medium |
| RM / Sales / F&B / HR capability selects | KEEP; do not treat presence as differentiation | Generic | Medium |

---

## Case Studies

| Field | Rec | Rationale | Risk |
| ----- | --- | --------- | ---- |
| property_name, Operator, outcome, owner_relevance, image | KEEP | Explorer stories | Medium |
| Why Comparable, Comparability Strength | KEEP | Fit comps | Medium |
| situation, branded_independent | KEEP + NORMALIZE | Polluted options | Medium |
| Full assignment semantics | MOVE / REHOME → Assignments table | Wrong long-term home | Medium |

---

## Brand Relationships (presentation)

| Field | Rec | Rationale | Risk |
| ----- | --- | --------- | ---- |
| section/row_key/title/body/extra | KEEP | Explorer Brand tab | High for OE UI |
| Typed approval graph | ADD as separate intel table | Presentation ≠ graph | — |

---

## Operator Intelligence - Claims

| Field | Rec | Rationale | Risk |
| ----- | --- | --------- | ---- |
| Claim ID, Operator, Subject/Predicate/Values, scopes, dates, evidence, verification, publication, conflict, scoring relevance, currentness, notes, limitations, Source URLs | KEEP | Claim spine | High for research |
| Free-text enums (category, statuses) | KEEP + NORMALIZE → selects | Consistency | Medium |
| Link to PI Source Library (multi) | ADD | Prefer over URL-only | Low additive |
| Hotel segment / structure scopes as selects | ADD if missing | Model doc requires | Low |

---

## Operator Intelligence - Market Presence

| Field | Rec | Rationale | Risk |
| ----- | --- | --------- | ---- |
| Operator, Country, Region, Presence Type, Current/Historical, dates, evidence, claim id, publication, confidence, notes, limitations | KEEP | Geo SoT | High |
| City/metro, property count, office flag | ADD (optional grain) | Explorer footprint depth | Low additive |
| State/province | ADD only when needed | Avoid over-modeling | — |

---

## Operator Fit - Shortlist / ODR

| Field | Rec | Rationale | Risk |
| ----- | --- | --------- | ---- |
| All shortlist snapshot fields | KEEP | Workflow only | High for pilot |
| ODR alignment fields | KEEP | Outreach snapshot | High |
| Never copy scores onto Master | — | Ownership boundary | — |

---

## PI Source Library / Facts / Published

| Field | Rec | Rationale | Risk |
| ----- | --- | --------- | ---- |
| Existing PI schemas | KEEP | Shared research core | High |
| Operator links | KEEP | Already supported | Medium |

---

## Company Profile / Companies / Contacts

| Field | Rec | Rationale | Risk |
| ----- | --- | --------- | ---- |
| Entire tables | KEEP separate | Not Explorer SoT | High if merged wrongly |
| Optional future link Master ↔ Company Profile | ADD later | Identity bridge | Medium |

---

## Genuinely ADD (entities/fields)

| ADD | Why existing insufficient |
| --- | ------------------------- |
| Record Purpose (or Test Only status) | Dummy isolation |
| Operator Intelligence - Assignments | Case Studies cannot be inventory SoT |
| Operator Intelligence - Brand Relationships (typed) | Presentation table is not approval/experience graph |
| Claims↔PI Source link field | URL-only weak |
| Presence city / property count (optional) | Footprint depth |
| Assignment-derived summary fields on Profile/Commercial (hybrid) | After Assignments exist |

---

## Recommendation counts (directional)

| Class | Directional volume |
| ----- | ----------------- |
| KEEP | Majority of linked + scoring + presentation row stores |
| KEEP + NORMALIZE | Status, taxonomies, Claims enums, Case Study selects, soft brand families |
| DERIVE | Active Countries summary; conversion/resort/urban flags; brand family lists; verified assignment counts |
| MOVE / REHOME | Assignment semantics out of Case Studies; typed brand edges out of presentation |
| MERGE | bf_structures → Management Structures |
| DEPRECATE LATER | Platform flat Market Presence Type for scoring; bf_* score weight; opaque JSON blobs |
| RETIRE CANDIDATE | Unused commercial/platform fields after exhaustive binding proof only |
| ADD | Record Purpose; Assignments table; typed Brand Relationships; PI link on Claims; optional Presence grain |
