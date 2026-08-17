# Operator Fit Engine — Current-State Airtable Audit

**Date:** 2026-08-03  
**Mode:** Read-only (`scripts/audit-operator-fit-airtable-readonly.mjs`)  
**Live report:** `reports/operator-fit-airtable-readonly.json`  
**Schema authority:** `docs/platform-reference/airtable-deals-fields.md`, `docs/operator-*-airtable-fields.md`, `docs/operator-alignment-airtable-options-audit.md`, `.env.example`  
**Assumption label:** Completeness stats below are for **Active** `Operator Setup - Master` records fetched at audit time (**n = 24**), plus linked child rows. They do not measure Draft/inactive profiles.

---

## 1. Verdict

Airtable **has the right skeleton** for operator identity, Explorer presentation, deal intake, brand match, and a first OAS score — but **structured scoring fields are sparsely populated** outside a small backfilled subset. Brand Setup depth for Brand Match v2 is materially richer than operator structured scoring coverage.

---

## 2. Table inventory (operator / matching relevant)

| Airtable Table | Role | Field doc / mapping | Used in code? | Matching? | Explorer? |
| -------------- | ---- | ------------------- | ------------- | --------- | --------- |
| Operator Setup - Master | Identity, status, governance meta | brand-operator-validation-fields-plan; `.env` Master table | Yes | Completeness / confidence | Yes |
| Operator Setup - Profile & Positioning | Scale, brands, service models | operator-brand-explorer-airtable-fields | Yes | Chain scale, brands, models | Yes |
| Operator Setup - Platform & Markets | Geo, markets, platform JSON | operator-operating-platform-airtable-fields | Yes | Geography | Yes |
| Operator Setup - Commercial Fit & Terms | Structures, openings, bf_* | bindings JSON | Yes | Structure, stage | Partial |
| Operator Setup - Governance, Delivery & Diligence | Offered services, reporting, RM/F&B | operator-infrastructure-explorer-airtable-fields | Yes | Services, reporting | Yes |
| Operator Setup - Case Studies | Comparable proof | operator-case-study-airtable-fields | Explorer + narrative | **Not in numeric OAS** | Yes |
| Operator Setup - Engagement & Reporting | Child engagement rows | engagement-reporting fields doc | Explorer | No | Yes |
| Operator Setup - Explorer Materials | Media/presentation | materials fields doc | Explorer | No | Yes |
| Operator Setup - Leadership* / Operating Platform / Brand Relationships / Infrastructure | Child PI tables | respective docs | Explorer | No (today) | Yes |
| Operator Deal Requests | Deal↔operator junction + stored alignment | airtable-deals-fields | Yes | Persists score | No |
| Deals | Owner opportunity hub | airtable-deals-fields | Yes | Yes | Context |
| Location & Property | Geo, scale, rooms, building | airtable-deals-fields | Yes | Geo, scale, asset | Context |
| Market - Performance - Deal & Capital Structure | Fees, legacy preferred structure | airtable-deals-fields | Yes | Fee placeholder; legacy structure | Context |
| Strategic Intent - Operational - Key Challenges | Operator/brand preferences | airtable-deals-fields; capability inputs | Yes | Primary OAS deal inputs | Context |
| Brand Deal Requests / Deal Brand Cache | Brand outreach + cached scores | airtable-deals-fields | Yes | Brand Match | No |
| Brand Setup -* | Brand attributes for Match v2 | Brand Setup docs | Yes | Brand Match | Brand Explorer |
| Hotel Census | Density / footprint | census docs | Brand Match density | Soft brand factor | No |
| Partner Intelligence -* | Source/extracted/published | PI field docs | Explorer governance | Evidence future | Partial |
| Legacy `3rd Party Operator - *` | Deprecated parity path | case-study doc | Still readable in places | Avoid for new scoring | Legacy |

---

## 3. Field inventory (scoring / fit critical)

Representative inventory. Full metadata dump (names/types/options) is in `reports/operator-fit-airtable-readonly.json` → `fieldCatalog` (redacted samples only).

| Airtable Table | Field | Type | Example / Allowed Values | Required? | Source | Used in Code? | Used for Matching? | Used in Explorer? | Data Quality Concern |
| -------------- | ----- | ---- | ------------------------ | --------- | ------ | ------------- | ------------------ | ----------------- | -------------------- |
| Master | company_name | text | Operator legal/trade name | De facto | Operator Setup | Yes | Identity | Yes | Complete (100%) |
| Master | submission_status | select | Active (env-driven) | Yes for universe | Operator | Yes | Eligibility filter | List filter | Complete |
| Master | Data Confidence Level | select | Inferred / … | No | Dealality | Yes (display) | Completeness narrative | Footnote | **16.7%** populated |
| Master | Source Type | select | Imported sample data / … | No | Dealality | Display | Evidence layer missing | Partial | **12.5%** |
| Master | Last Updated Date | date | — | No | Dealality | Display | Freshness | Partial | **8.3%** |
| Platform | Active Countries | multi | Live options registry | No | Operator/Dealality | Yes geo factor | **High** | Yes | **8.3% (2/24)** — critical |
| Platform | Active Markets / Cities | multi | Live options | No | Operator/Dealality | Yes geo | **High** | Yes | **8.3%** |
| Platform | Market Presence Type | multi | Active operations; Prior; Pipeline; Target; None; Unknown | No | Operator | Yes geo tiers | High | Yes | **8.3%** |
| Profile | chainScalesSupported | multi | Chain scale vocab | No | Operator | Yes | **High** | Yes | **100%** — usable |
| Profile | Service Models Supported | multi | — | No | Operator | Partial | Medium | Yes | **95.8%** |
| Profile | brands | link → Brand Basics | rec… | No | Operator | Brand portfolio factor | Medium | Yes | **87.5%** — usable |
| Profile | Brand Families Operated | multi/text | — | No | Operator | Display / soft | Low–Med | Yes | **100%** but taxonomy soft |
| Commercial | Management Structures Supported | multi | Full third-party; Brand-managed; Franchise support; … | No | Operator | Structure factor | **High** | Partial | **12.5%** |
| Commercial | New-Build Opening Experience | select/text | — | No | Operator | Asset/stage extras | Medium | Partial | **8.3%** |
| Commercial | Pre-Opening Support Capability | select/text | — | No | Operator | Services/stage | Medium | Partial | **8.3%** |
| Commercial | Conversion / Reflag Experience | select/text | — | No | Operator | Intended differentiator | High if used | Partial | **0%** |
| Commercial | bf_selected_deal_structures | multi | Legacy bf_* | No | Operator | Structure fallback | Medium | Explorer cards | **12.5%** |
| Commercial | bf_not_ideal_for | text/multi | Less-ideal situations | No | Operator | Negative-fit text | Low weight | Explorer | **95.8%** — often marketing-ish |
| Governance | Offered Services | multi | Structured service options | No | Operator | Services factor | **High** | Yes | **8.3%** |
| Governance | Owner Reporting Level | select | Institutional monthly package; … | No | Operator | Reporting factor | Medium | Yes | **8.3%** |
| Governance | Governance Cadence | select/text | — | No | Operator | Reporting | Medium | Yes | **8.3%** |
| Governance | Revenue Management Capability | select | — | No | Operator | Folded into services extras | **Generic risk** | Yes | **8.3%** — table-stakes if scored as presence |
| Governance | Sales Platform | select/text | — | No | Operator | Same | Generic risk | Yes | **8.3%** |
| Governance | F&B Capability Level | select | — | No | Operator | Weak in numeric OAS | Medium | Yes | **8.3%** |
| Case Studies | property_name, hotel_type, region, situation, outcome, … | mixed | Child proof | No | Research/operator | Explorer / narrative | **Not numeric OAS** | Yes | Sparse / uneven; highest future differentiation value |
| SI (deal) | Preferred Management Structure | multi | Full third-party management; Brand-managed; Franchise with third-party operator; Owner-operated; … | No | Owner | Structure | High | — | Structured path exists (Phase 5B+) |
| SI | Brand Agreement Structure | select | Franchise; Management; License; Soft brand… | No | Owner | Annotated in structure | High | — | Separated from operator path (5E) |
| SI | Operating Model | select | Owner-operated; Third-party managed; Brand-managed; Hybrid; Undecided; N/A | No | Owner | Structure | High | — | Preserve |
| SI | Must-Have / Required / Nice-to-Have Operator Services | multi | Structured service options | No | Owner | Services | High | — | Prefer over legacy free-text |
| SI | Preferred Brands | link/multi | Brand records | No | Owner | Brand portfolio factor | Medium | — | |
| SI | Top 3 Deal Breakers | text/multi | — | No | Owner | Negative-fit | Low weight (2) | — | Weak penalty design |
| SI | Owner Control Preference / Reporting Expectations | select | — | No | Owner | Owner relations / reporting | Weak compare today | — | |
| MP | Royalty/Marketing/Loyalty Fee Expectations | text | — | No | Owner | feeCommercial | **Placeholder 75** | — | Not comparable economics |
| MP | Preferred Deal Structure | select | Franchise Only; … | Legacy | Owner | Fallback only if SI structured absent | Confusing | — | Historical conflation with operator structure |
| Deals | Project Type, F&B Complexity, Opening Timeline, Current Operating Model | select | Canonical project types | Partial | Owner | Asset/stage / OCS | Medium–High | — | |
| Location | Country, City, Hotel Chain Scale, Building Type, Stage, Rooms | mixed | — | Core | Owner | Geo/scale/asset | High | — | Stronger than operator geo coverage |
| Operator Deal Requests | Alignment Score / Band / Data Confidence | number/select | Persisted at outreach | Workflow | System | Snapshot of score | Medium | — | Not a learning loop yet |

---

## 4. Completeness statistics (Active operators, 2026-08-03)

| Field | Operators Populated | Completeness | Scoring Importance | Risk |
| ----- | ------------------: | -----------: | ------------------ | ---- |
| company_name | 24/24 | 100% | Identity | Low |
| submission_status | 24/24 | 100% | Eligibility | Low |
| chainScalesSupported | 24/24 | 100% | High | Low |
| Brand Families Operated | 24/24 | 100% | Medium | Medium (taxonomy soft) |
| Service Models Supported | 23/24 | 95.8% | Medium | Low |
| bf_not_ideal_for | 23/24 | 95.8% | Low (penalty weight 2) | Medium (marketing language) |
| brands (linked) | 21/24 | 87.5% | Medium | Medium |
| Management Structures Supported | 3/24 | 12.5% | High | **Critical** |
| bf_selected_deal_structures | 3/24 | 12.5% | Medium | Critical |
| Data Confidence Level | 4/24 | 16.7% | Evidence | Critical |
| Active Countries | 2/24 | 8.3% | High | **Critical** |
| Active Markets / Cities | 2/24 | 8.3% | High | **Critical** |
| Market Presence Type | 2/24 | 8.3% | High | Critical |
| Offered Services | 2/24 | 8.3% | High | **Critical** |
| Owner Reporting Level | 2/24 | 8.3% | Medium | Critical |
| Revenue Management Capability | 2/24 | 8.3% | Low if presence-scored | Critical / generic |
| Pre-Opening Support Capability | 2/24 | 8.3% | Medium | Critical |
| New-Build Opening Experience | 2/24 | 8.3% | Medium | Critical |
| Conversion / Reflag Experience | 0/24 | 0% | High for conversions | **Critical** |
| Case-study performance metrics | n/a in numeric OAS | Low usage | High for future fit | Critical gap |

**Interpretation:** Profile “display” fields are relatively complete. **OAS differentiators (geo, structures, offered services, openings, conversion)** are populated for roughly the Phase 5C sample subset (~2–3 of 24), not the Active universe.

---

## 5. Linked-record relationships

| Source | Destination | Cardinality | Used properly? | Concern |
| ------ | ----------- | ----------- | -------------- | ------- |
| Deals → Location / MP / SI | Child tables | 1:1 typical | Yes for scoring context | Must load all three for OAS |
| Operator Deal Requests → Deal + Operator Setup | N:1 + N:1 | Yes for outreach | Not a full shortlist/compare store |
| Profile.brands → Brand Basics | M:N | Yes when IDs present | Still some family-label-only rows historically |
| Master ← child Operator Setup tables | 1:N / 1:1 | Yes via `Operator` link | Explorer depends on links; scoring completeness checks profile/platform |
| Case Studies → Master | 1:N | Explorer | **Not wired into numeric score** — unstructured advantage unused |
| Brand Relationships child table | Master | 1:N | Explorer/PI | Not used as brand–operator compatibility engine |

**Unstructured duplication risks:** fee/commercial narratives (`feeStructureSummary`, `cap_profile_commercial`, `ov_card_*`); less-ideal text vs structured `bf_not_ideal_for`; legacy `Preferred Deal Structure` vs SI management/operating fields; Explorer JSON blobs vs structured multi-selects.

---

## 6. Operating-structure values (preserve)

Confirmed in options audit / bindings (do **not** rename without migration notes):

**Deal / SI path (structured):**  
`Preferred Management Structure` includes Full third-party management; Brand-managed; Franchise with third-party operator; Owner-operated; Owner-operated with commercial support; …  
`Operating Model`: Owner-operated; Third-party managed; Brand-managed; Hybrid / project-specific; Undecided; Not applicable.  
`Brand Agreement Structure`: Franchise; Management; License; Soft brand / collection affiliation; Brand-managed; Undecided; Not applicable.

**Product language in the Operator Fit brief** (Third-Party Management; Franchise + Operator; Franchise Only; Owner-Operated; Lease; Asset Management; To Be Confirmed) **partially overlaps** live options. **Lease** and explicit **Franchise Only** as operator-path values need founder confirmation vs legacy MP `Preferred Deal Structure` / commercial structures — do not silently rename.

**Operator offered structures:** Full third-party management; Brand-managed; Franchise support; Commercial-only support; Pre-opening / transition support; Asset management support; Hybrid / project-specific; Other.

---

## 7. Formula / rollup / lookup risks

- Brand Match uses Hotel Census density soft factor — geography confidence matters; unconfident geo excludes density (good).  
- OAS has **no Airtable formula score**; score is Node-computed. Distortion risk is in **JS placeholders**, not Airtable formulas.  
- Explorer JSON / presentation fields can look “complete” while structured scoring fields are empty — **presentation ≠ match readiness**.

---

## 8. What supports future Operator Fit vs what does not

**Genuinely useful today:** Active status; chain scales; linked brands; deal SI structured management/operating/service fields; Location geo/scale; Operator Deal Requests junction; Case Study table shape (even if sparse).

**Generic / low-value if scored as presence:** Revenue management / sales / procurement / HR / digital distribution checkboxes without project-comparable evidence; flat fee narrative presence; owner-relations keyword text; `bf_not_ideal_for` marketing blurbs.

**Missing for credible fit:** Comparable property performance; brand approval/compatibility records; organizational capacity; execution-risk flags; evidence confidence per claim; structured economics; pathway objects (brand × structure × operator); outcome learning fields.

---

## Verification

- Airtable inspection: **read-only** script only.  
- No field renames, deletes, or record writes.  
- Sensitive values redacted in report samples.
