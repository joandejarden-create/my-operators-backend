# Permanent Editorial Quality and Full E2E Factory Certification — Part 8 Final Report

**Date:** 2026-11-19  
**Workspace:** `C:\Dev\dealality\ws-brand-explorer` (`ws/brand-explorer`)  
**Authorization this cycle:** dry-run / encode / verify only — **no** mass paid research, **no** production Airtable writes, **no** publication, **no** automatic founder approval  
**Curio / Ascend:** **NOT APPROVED**  
**Gate 1:** AWAITING_FOUNDER_APPROVAL  
**Gate 2:** BLOCKED_UNTIL_GATE1  
**Production publication activation:** `productionWritesAuthorized: false` (`configs/brand-explorer-founder-publication-activation-v1.json`)

---

## Verdict

**Editorial defect (repeated card headings):** fixed, gated, and browser-verified on Comparison Studio.  
**Universal factory:** architecture, Airtable SoT, dry-run publication path, and permanent lessons exist and are wired — **not** certified for multibrand population. Gate 1 remains unapproved; Gate 2 remains blocked; dual-brand cohort is NOT APPROVED; live Active/Live inventory (68) has diverged from the protected 62 public-full freeze and must be reconciled before controlled scale-out.

**Readiness to begin controlled multibrand population:** **NO — NOT READY.**

---

## 1. Repeated-heading correction (with browser screenshots)

| Item | Result |
| --- | --- |
| Contract | `BE_NO_REPEATED_CARD_HEADING_V1` |
| Module | `lib/brand-explorer/be-no-repeated-card-heading-v1.js` + browser mirror |
| Assembler | RIGHT cumulative proposal strips leading title echoes |
| Renderer defense | Atelier `explorerMergedBody` / proof cards body-only + strip |
| Automated gate | `npm run test:be-no-repeated-card-heading-v1` → **PASS** |
| Ascend body | `Choice Hotels positions Ascend as an upscale soft collection…` (no title echo) |
| Lesson | `BE-QL-NO-REPEATED-HEADING-001` → **BROWSER_VERIFIED** |
| Detail report | `reports/brand-explorer/no-repeated-card-heading-v1/00-completion-report.md` |

Screenshots:

1. `reports/brand-explorer/no-repeated-card-heading-v1/screenshots/01-curio-proof-no-repeated-heading-RIGHT.png`  
   — RIGHT proofs Title Case; bodies begin with substance.
2. `reports/brand-explorer/no-repeated-card-heading-v1/screenshots/02-curio-dealality-view-no-title-echo.png`  
   — RIGHT Dealality View starts `Curio reads as…` (no title prefix).

---

## 2. Permanent lessons regression results

Registry: `configs/brand-explorer-founder-quality-lessons-registry-v1.json`  
Master register: `reports/brand-explorer/founder-requirements-reconciliation/01-master-founder-requirements-register.json`  
Permanent standards: `configs/brand-explorer-permanent-content-standards-contract-v1.json`

| Lesson / gate | Status this cycle |
| --- | --- |
| `test:be-no-repeated-card-heading-v1` | PASS |
| `test:be-editorial-title-case-v1` | PASS (RIGHT proofs Title Case; LEFT unchanged) |
| `test:be-property-image-integrity-v1` | PASS |
| `test:be-dealality-insight-advisory-v1` | PASS (Curio advisory starts clean; Ascend pack OK) |
| `test:be-founder-quality-lessons-registry-v1` | PASS (Gate 2 still blocked) |
| Gallery six-image standard | Prior BROWSER_VERIFIED (`BE-QL-GALLERY-001`) |
| Title Case headings | Prior BROWSER_VERIFIED (`BE-QL-TITLECASE-001`) |
| Property image integrity | Prior BROWSER_VERIFIED (`BE-QL-PROPERTY-IMAGE-001`) |
| Dealality Insight advisory | Prior BROWSER_VERIFIED (`BE-QL-DEALALITY-INSIGHT-001`) |
| No repeated heading | **BROWSER_VERIFIED this cycle** (`BE-QL-NO-REPEATED-HEADING-001`) |

Prior founder requirements remain operational via the **existing** registry + permanent standards contract — no parallel standards system was created. Many earlier phase lessons remain `ENCODED` / `TESTED` (not all browser-verified); that is an honest gap, not a silent claim of full VERIFIED coverage.

---

## 3. Complete E2E factory architecture

Desired sequence mapped to existing Dealality machinery (brand-agnostic; no new parallel OS):

| Step | Capability | Primary locations |
| --- | --- | --- |
| 1. Identify canonical brand | Brand Basics `rec…` primary key; Active/Live universe loader | `be-airtable-integration-sot-v1.js` · `brand-explorer-active-universe.js` |
| 2. Load Airtable + related | Brand Library + Brand Setup children + Presentation | `api/brand-library.js` · Brand Setup child tables |
| 3. Research (authorized) | Tab Factory source packs / OS stages — **not run paid this cycle** | Tab Factory build operation · active-profile factory stages |
| 4. Evidence + provenance | Source provenance by tab | `brand-explorer-source-provenance-by-tab.js` |
| 5. Ten-tab profile | Canonical tabs + subsection inventory | `BE_CANONICAL_TAB_IDS` · `brand-explorer-canonical-subsection-inventory-v1.json` |
| 6. Contracts | Permanent content standards + founder lessons | permanent-content-standards + lessons registry |
| 7–8. Depth / imagery / KPIs / loyalty / competitors | Tab factory audit, section pattern, gallery, HPC footprint SoT, advisory | Tab factory modules · SoT · gallery · advisory gates |
| 9. Production components | Atelier / Brand Explorer mounts in Comparison Studio | `brand-explorer-compare.html` · product component mounts |
| 10. Comparison Studio | CURRENT vs cumulative PROPOSED; Highlight Changes | Comparison Studio V1 |
| 11. Founder regression | Lessons registry + permanent gates | `test:be-*` suite |
| 12. Founder approval | Exact candidate hash; Gate 1 then Gate 2 | Publication activation + approval workflow |
| 13–14. Airtable write + readback | Presentation-table write plan; Basics identity preserved | `be-founder-approval-publication*` (writes gated off) |
| 15–16. Publish + verify + history | Promote only after activation + gates | activation JSON · publication audit trails |

**Safety defaults:** `BE_FACTORY_SAFETY.dryRunDefault=true`, `noAutoPublish=true`, per-brand failure isolation via blocked gates (no customer promotion of partials).

---

## 4. Actual Airtable record and field mappings

Source of truth module: `lib/brand-explorer/be-airtable-integration-sot-v1.js` (`BE_AIRTABLE_INTEGRATION_SOT_V1`).

| Layer | Table / system | Ownership | Factory write class |
| --- | --- | --- | --- |
| Canonical identity | `Brand Setup - Brand Basics` (`AIRTABLE_BASE_ID`) | brand_basics | Preserve record IDs; no minting duplicates |
| Presentation content | `Brand Setup - Brand Explorer Presentation` | presentation | factory_stage_only → founder_publish when activated |
| Brand Setup children | Footprint, Project Fit, Portfolio & Performance, Standards, Fee Structure, Deal Terms, Operational Support, Legal Terms, Loyalty & Commercial, Sustainability & ESG | brand_setup_child | system_owned / supporting — no silent overwrite of company-validated structured fields |
| Portfolio / HPC footprint | `Hotel Property Census` (`AIRTABLE_BASE_ID_ALT`) | hpc | system_owned_no_factory_write for inventory counts |
| Transitional census | `Hotel Census` | hotel_census | transitional until HPC rollup certified |
| Aliases | `Brand Alias Mapping` | identity matchers | never competing primary keys |
| Research evidence | Presentation + research artifacts / held-out issues | factory_research | stage; do not dump HPC/certState into customer slots |
| Images / assets | Presentation gallery slots + asset governance | presentation | verified photography + captions |
| Founder review / publication history | Comparison Studio audits + publication-run JSON | system | dry-run audits under `reports/brand-explorer/comparison-studio/` |

Cohort Brand Basics IDs (unchanged):

| Brand | Brand Basics id |
| --- | --- |
| Curio Collection | `receQkxgjlezsc1xg` |
| Ascend Hotel Collection | (Ascend Basics id used by publication dry-run / assembler packs) |

Publication write plan is **Presentation-table oriented** (`presentationTableOnly: true` in activation hardStops). Canonical Brand Basics IDs are preserved; factory must not duplicate Basics facts into unrelated fields.

---

## 5. Existing-brand inventory and coverage

| Inventory | Count | Authority |
| --- | --- | --- |
| Live Active/Live universe (this dry-run) | **68** | `npm run brand-explorer-active-universe-source-of-truth -- --dry-run` → `reports/brand-explorer-active-universe-source-of-truth.json` |
| Protected public-full quality-clean freeze | **62** | `docs/data-intelligence/brand-explorer-protected-baseline-rules.md` |
| Public-full clean (subset in SoT report) | listed in SoT report | includes Curio + Ascend |
| Public-full failing PVQL | 6 | delta, fairfield, four-points, hyatt-centric, hyatt-regency, thompson |
| Content remediation needed | hotel-indigo, kimpton | SoT report |
| Restored pending validation | radisson-blu | SoT report |

Factory discovery must use **live Brand Status Active/Live**, not a hardcoded brand count. The 62 freeze remains the protected quality bar for Active/Live public-full work; the live 68 count is a **reconciliation outstanding** before multibrand population claims.

---

## 6. Curio and Ascend certification results

| Criterion | Curio | Ascend |
| --- | --- | --- |
| Comparison Studio mount | PASS (browser) | Covered by automated packs / dry-run |
| No repeated heading in bodies | PASS (browser + gate) | PASS (gate: Collection Scale Context body clean) |
| Title Case white headings (RIGHT) | PASS | Via universal enforcer |
| Property image integrity | PASS (Royal Palm corrected on RIGHT) | Gate green |
| Dealality advisory (no HPC/certState) | PASS (browser) | Pack OK; no customer-facing leak in gate |
| Canonical subsection coverage | Inventory frozen from live Curio; Gate 2 not regenerated | Same Gate 2 block |
| Content-padding regressions | Permanent standards enforce; Gate 2 still blocked for full reconstruction | Same |
| Production component rendering | PASS in Compare Studio product mounts | Same path |
| Airtable field mapping | SoT + write plan generated | Write plan generated |
| Dry-run publication | BLOCKED_GATES (expected) | BLOCKED_GATES (expected) |
| Founder approval | **NOT APPROVED** | **NOT APPROVED** |
| Live content untouched | YES | YES |

Curio dry-run plan (no writes): update 168 · create 46 · deactivate 9 · hash `c0cc375b…`  
Ascend dry-run plan (no writes): update 0 · create 214 · deactivate 0 · hash `b90ae03e…`

**Certification claim:** cohort proves **editorial enforcement + dry-run publication plumbing**, not founder-approved full E2E publication readiness.

---

## 7. Missing requirements / unresolved capabilities

1. **Gate 1** not founder-approved → Gate 2 remains blocked; no full content reconstruction certification.  
2. **Curio / Ascend NOT APPROVED** — Approve & Publish correctly refuses.  
3. **Production writes** not activated (`productionWritesAuthorized: false`).  
4. **Live Active/Live 68 vs protected 62** — must be explicitly reconciled before scale-out.  
5. Many registry lessons still `ENCODED`/`TESTED` without browser evidence.  
6. Paid research refresh pipeline + cost controls designed but **not executed** this cycle (by authorization).  
7. Concurrent-modification / rollback paths exist in dry-run workflow docs but are **unproven on live writes** (writes forbidden).  
8. ADP/GDI brand-level read contracts remain RESERVED_READ_ONLY stubs.  
9. Ascend Presentation appears create-heavy in dry-run (214 creates) — indicates incomplete existing Presentation coverage vs Curio update-heavy plan; needs founder review before any apply.

---

## 8. Publication dry-run results

Command: `npm run be-founder-approval-publication:dry-run`  
Status report: `reports/brand-explorer/comparison-studio/publication-dry-run-v1/00-FOUNDER_PUBLICATION_STATUS.md`

| Brand | Status | Airtable writes | publicationApplied | Gate failures |
| --- | --- | --- | --- | --- |
| curio-collection | BLOCKED_GATES | 0 | false | GATE1_NOT_APPROVED, GATE2_BLOCKED, SCORECARD_NOT_FOUNDER_APPROVED |
| ascend | BLOCKED_GATES | 0 | false | same |

Workflow steps 1–12 are encoded; apply/promote remain gated off. Partial customer exposure is prevented by hardStops.

---

## 9. Expected research cost controls and refresh approach

| Control | Approach |
| --- | --- |
| Initial full research | Per-brand Tab Factory / active-profile stages; founder review before apply |
| Incremental refresh | Field-level dedupe + changes-since-previous via Comparison Studio diffs; reuse verified Presentation + Brand Setup facts |
| Source freshness | Provenance-by-tab + held-out research gaps; do not re-pay for unchanged verified fields |
| Cost isolation | Per-brand processing; failure on one brand must not mutate another (publication path already brand-scoped) |
| Authorization | No mass paid research until founder explicitly authorizes a cohort + budget |
| Idempotency | Candidate content hash locking; restartable dry-runs; Presentation-only writes when activated |
| Retention | Keep research evidence / audits under `reports/`; never overwrite Company Validated / Registry / Brand Status without ownership rules |

---

## 10. Recommendation — readiness for controlled multibrand population

**Recommendation: DO NOT begin controlled multibrand population yet.**

Required before YES:

1. Founder Gate 1 approval on Curio renderer parity.  
2. Gate 2 reconstruction unblocked and founder-approved for Curio + Ascend exact candidate hashes.  
3. Explicit reconcile of live Active/Live inventory vs protected 62 freeze.  
4. Separate founder activation of `productionWritesAuthorized` (still false).  
5. At least one successful dry-run → approve → (activated) write → readback → customer verify on a single brand with rollback rehearsal.  
6. Confirm Ascend create-heavy plan is intentional and complete before any apply.

Until then, continue operating in **Comparison Studio + dry-run publication** mode, stacking permanent lessons (as done for gallery, Title Case, property images, advisory, and no-repeated-heading).

---

## Authorization & stop state

- Airtable writes this cycle: **0**  
- Paid research this cycle: **none**  
- Publication: **none**  
- Automatic founder approval: **none**  
- Existing BE worktree / Integration Controller workflow: **preserved**  
- Browser: unlocked after evidence capture  

**Final status:** `AWAITING_INTEGRATION_CONTROLLER` is N/A (no PR opened this cycle). Product status for this certification request:

`BLOCKED — FOUNDER_GATES_AND_ACTIVATION_REQUIRED`
