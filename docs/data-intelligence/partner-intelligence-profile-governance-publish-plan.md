# Partner Intelligence → Profile Governance Publish Flow Plan

**Date:** 2026-07-06  
**Status:** Implemented (audit + publish scripts). **Operational runbook:** [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md) — Arbor Lodging (CALA) pilot completed 2026-07-06.

> **Authority:** [INTELLIGENCE_GOVERNANCE.md](./INTELLIGENCE_GOVERNANCE.md), [brand-operator-validation-fields-plan.md](./brand-operator-validation-fields-plan.md), [governance-read-path-trust-label-plan.md](./governance-read-path-trust-label-plan.md), [partner-intelligence-repository-mvp-plan.md](../partner-intelligence-repository-mvp-plan.md), `api/lib/partner-intelligence-field-map.js`

---

## Purpose

This plan defines how Dealality should convert **reviewed Partner Intelligence (PI) sources** into **profile-level governance updates** on Brand/Operator Setup root tables — so Explorer trust chips, API `governance` objects, and future snapshot footnotes reflect human-approved trust status without duplicating source-level metadata on Setup rows.

**In scope:** Profile-level P1 governance fields on four Setup root tables.  
**Out of scope (this plan):** Field-level Explorer overlay publish (existing `lib/partner-intelligence/publish-overlay.js` → Published Explorer Fields), scoring changes, snapshot footnotes, owner-facing PI badges.

---

## Current State

| Layer | Status |
|-------|--------|
| **Partner Intelligence tables** | Live on primary base (`AIRTABLE_BASE_ID`); schema verified in `reports/brand-operator-validation-schema-diff.md`. Field map: `api/lib/partner-intelligence-field-map.js`. |
| **Brand/Operator P1 governance** | 14 columns on Brand Basics, Brand Explorer Presentation, Operator Master, Operator Explorer Materials (audit: 64 exact matches, 0 high-priority gaps on P1 tables). |
| **Read path** | `normalizeProfileGovernance()` attached to brand/operator detail APIs. |
| **Explorer UI** | Trust chips render `governance.displayLabel` / `displaySubtitle` when non-null. **External labels are conservative** — internal `Validation Status` (e.g. Company Published) is mapped to safer owner-facing copy; confidence is internal only. |
| **E2E pilot** | `pilot-profile-governance-values` applied QA values to Best Western Plus + Viento Sur Gestión Hotelera; chips validated. |
| **Operator confidence alias** | Operator Master uses **`Data Confidence Level`** (not `Confidence Level`) — read path and pilot script map this alias. |
| **PI field-level publish** | `publish-overlay.js` + `PARTNER_INTELLIGENCE_PUBLISH_ENABLED` write/read **Published Explorer Fields** for Explorer content overlay — **not** Setup profile governance. |
| **Profile governance publish** | **Implemented** — `npm run publish-partner-intelligence-profile-governance` (dry-run default). Arbor pilot applied 2026-07-06. See [runbook](./partner-intelligence-profile-governance-runbook.md). |

---

## Architectural Principle

| Layer | Responsibility |
|-------|----------------|
| **Partner Intelligence** | Source-level evidence, capture, extraction, human review, field-level publish overlay, outreach intake. Holds URLs, files, evidence quotes, per-field facts, publish workflow status. |
| **Brand/Operator Setup roots** | Profile-level trust/status, Explorer display readiness, company validation state. No per-document URLs on P1 governance columns (`Source URL / File Path`, `Source Date` excluded from Setup roots — PI SSOT). |
| **Explorer UI** | Normalized `governance` display only (`displayLabel`, `displaySubtitle`, `sourceBasis`). No raw PI fields or raw `validationStatus` in owner views. Confidence stored on `governance.confidenceLevel` for internal use only — **not** in `displaySubtitle`. |
| **Scoring / OAS** | Unchanged in this phase. OAS `Data Confidence` on deal requests remains deal-scoped. |

**Rule:** PI proves *why* a profile trust label is justified; Setup roots record *what* the platform may show. AI may suggest; humans approve; publish scripts respect do-not-overwrite.

---

## Partner Intelligence Tables

### Partner Intelligence - Source Library

| | |
|--|--|
| **Purpose** | Canonical registry of documents, URLs, decks, captures supporting brand/operator intelligence. |
| **Relevant fields** | `Profile Type`, `Brand`, `Operator / Management Company`, `Region`, `Source Type`, `Source Origin`, `Source Quality`, `Verified Source?`, `Status`, `Last Reviewed`, `Reviewed By`, `Approved for Extraction?`, `Approved for Explorer Use?`, `Source Date`, `Capture Date` |
| **Review/publish status** | `Status`: Found, Captured, Classified, Extracted, Needs Review, **Approved**, Rejected, **Stale** (`VAL_PARTNER_SOURCE_SELECTS` in field map) |
| **Linkage** | Links to `Brand Setup - Brand Basics`, `Operator Setup - Master` |
| **Verification** | Live per schema audit |
| **Open questions** | Whether `Approved` on Source Library alone is sufficient for profile-governance publish, or only supports field-level publish. **Needs Verification:** rollup rules when multiple sources disagree. |

### Partner Intelligence - Extracted Facts

| | |
|--|--|
| **Purpose** | Staging for AI/human-extracted facts before Explorer use. Not live until published. |
| **Relevant fields** | `Source Record`, `Brand`, `Operator / Management Company`, `Evidence Text`, `Confidence Level`, `Confidence Score`, `Extraction Type`, `Human Review Status`, `Approved Value`, `Data Gap?`, `Reviewed At`, `Reviewed By`, `Source Quality` (mirror) |
| **Review/publish status** | `Human Review Status`: Pending, **Approved**, **Edited**, Rejected, Needs More Source |
| **Linkage** | Source Library + Brand/Operator links |
| **Verification** | Live per schema audit |
| **Open questions** | Profile-governance publish may use **rollup/summary facts** (e.g. registry keys `profileGovernance.validationStatus`) — **Needs Verification** whether dedicated fact rows exist or must be added to field registry. |

### Partner Intelligence - Published Explorer Fields

| | |
|--|--|
| **Purpose** | Human-approved values that overlay Explorer **content** reads (field-level). Distinct from Setup profile governance writes. |
| **Relevant fields** | `Publish Status` (Draft, **Published**, Superseded, Withdrawn), `Overall Source Confidence`, `Last Reviewed Date`, `Stale?`, `Primary Source`, `Supporting Facts`, `Approved Value`, `Field Name`, `Explorer Section` |
| **Review/publish status** | `Publish Status = Published` and `Stale? = false` for active overlay rows |
| **Linkage** | Brand, Operator, Supporting Facts, Primary Source |
| **Verification** | Live; `publish-overlay.js` implements field-level publish eligibility |
| **Open questions** | Docs list rollup rows `Overall Source Confidence`, `Last Reviewed Date` per profile — **Needs Verification** for canonical `Field Name` keys used to drive profile-governance publish. |

### Partner Intelligence - Helena Outreach Intake

| | |
|--|--|
| **Purpose** | Company-provided material requests/receipts; feeds Source Library when materials arrive. |
| **Relevant fields** | `Profile Type`, `Brand`, `Operator / Management Company`, `Received Materials`, `Source Origin` (Brand Provided / Operator Provided), `Uploaded to Partner Source Library?`, `Linked Source Record`, `Extraction Status`, `Permission / Visibility Notes` |
| **Review/publish status** | `Extraction Status`: Not Started, Ready for Extraction, Extracted, Needs Review |
| **Linkage** | Brand, Operator, Source Library |
| **Verification** | Live per schema audit |
| **Open questions** | Company-provided intake may justify `Company Published` or future `Company Validated` — workflow order: Helena → Source Library → Facts → profile publish. **Needs Verification** for attestation fields on intake. |

---

## Profile Governance Target Tables

### Brand Setup - Brand Basics

| | |
|--|--|
| **Governance fields** | Full P1 set (14 columns). Header trust resolves from **Basics first** per [governance-read-path-trust-label-plan.md](./governance-read-path-trust-label-plan.md). |
| **Allowed PI publish updates** | `Validation Status` (≤ Company Published from public sources), `Usage Permission` (≤ Platform Display Allowed by default), `Source Type`, `Source Region`, `Last Reviewed Date`, `Refresh Due Date`, `Confidence Level`, `Evidence Notes`, `Missing Data Flags`, `Reviewed By`, `External Display Status` (human-gated), `Internal Notes` (append-only recommendation preferred) |
| **Human approval required** | `External Display Status = Show Trust Label`, any upgrade to `Scoring Allowed` / `External Snapshot Allowed`, `Company Published` from third-party-only sources |
| **Never auto-overwrite** | `Company Validated`, `Company Validation Date`, rows already Company Validated |

### Brand Setup - Brand Explorer Presentation

| | |
|--|--|
| **Governance fields** | Same P1 set per presentation row |
| **Allowed PI publish** | Section-level governance only in **later phase** — Phase 1 profile publish targets **Basics header** only |
| **Human approval** | Same as Basics |
| **Never auto-overwrite** | Same; must not elevate trust above Basics without merge rules |

### Operator Setup - Master

| | |
|--|--|
| **Governance fields** | P1 set; confidence writes to **`Data Confidence Level`** alias |
| **Allowed PI publish updates** | Same pattern as Brand Basics |
| **Human approval required** | Same |
| **Never auto-overwrite** | Same |

### Operator Setup - Explorer Materials

| | |
|--|--|
| **Governance fields** | P1 set per materials row |
| **Allowed PI publish** | Deferred — materials slot governance in later phase |
| **Human approval** | Same |
| **Never auto-overwrite** | Same; Master wins for header |

---

## Publish Eligibility Rules

A PI-driven **profile governance publish** may proceed only when **all** applicable checks pass:

| # | Rule | Source of truth |
|---|------|-----------------|
| 1 | Supporting source `Status` ≠ **Stale** | Source Library |
| 2 | Supporting source `Approved for Explorer Use` = **Yes** | Source Library |
| 3 | Supporting source `Source Quality` ≥ **Medium** (or Low with explicit reviewer override flag — **Needs Verification**) | Source Library |
| 4 | ≥1 supporting Extracted Fact with `Human Review Status` ∈ **Approved**, **Edited** | Extracted Facts |
| 5 | For field-level overlay path: Published row `Publish Status` = **Published**, `Stale?` = false | Published Explorer Fields |
| 6 | Profile rollup governance fact or approved publish package exists — **Needs Verification** (registry key TBD) | Extracted Facts / Published |
| 7 | Entity linkage: `Brand` or `Operator / Management Company` link resolves to exactly one Setup record | PI links |
| 8 | Proposed `Usage Permission` ≠ **Do Not Use** | Derived mapping |
| 9 | Target profile passes [do-not-overwrite](#do-not-overwrite-rules) checks | Setup row |
| 10 | `Reviewed By` and/or `Last Reviewed Date` present on PI review chain | Facts / Source / Published |
| 11 | Proposed `Validation Status` ≠ **Company Validated** unless company attestation workflow completed (**never** from public extraction alone) | Mapping rules |

**Reuse:** Align with `validatePublishEligibility()` in `lib/partner-intelligence/publish-overlay.js` for shared source/fact gates; extend for profile-governance-specific mapping.

---

## Field Mapping: Partner Intelligence → Profile Governance

| Target (Setup) | PI source field(s) | Mapping rule | AI may suggest | Auto write | Human approval | Do-not-overwrite |
|----------------|-------------------|--------------|----------------|------------|----------------|------------------|
| **Validation Status** | Mapped profile **Source Type** from PI `Source Type` (`mapSourceToProfileSourceType`); basis via `assessPublishScopeSourceBasis` / `inferValidationStatus` in `profile-governance-publish-readiness.js`. **All company-controlled** approved publish-scope sources → **Company Published** (brand or operator). **Mixed** company + reviewed/public → **Source-Informed**. **All reviewed/public** → **Source-Informed**. **Unknown** → **Source-Informed** unless company-origin-only fallback. AI synthesis → **AI-Assisted** only. **Never** → Company Validated from PI alone. | Yes (suggest) | No | **Yes** | If target = Company Validated |
| **Usage Permission** | Source visibility, reviewer package | Default **Platform Display Allowed** for reviewed public profile publish. Scoring / External Snapshot require explicit reviewer flag. | Suggest | No | **Yes** for Scoring Allowed, External Snapshot Allowed | If Do Not Use |
| **Source Type** | Source `Source Type` | Map PI source types to P1 options (e.g. Development Brochure → Company PDF / Brochure). Unmapped → **Needs Verification** log. | Yes | Dry-run apply only | Review on mismatch | — |
| **Source Region** | Source `Region`, `Country / Market` | CALA → **CALA-Specific**; Global → **Global Reference**; else Regional / Market-Specific per [SOURCE_RANKING_GUIDE.md](./SOURCE_RANKING_GUIDE.md) | Yes | Dry-run apply only | If regional conflict | — |
| **Last Reviewed Date** | `Reviewed At`, `Last Reviewed`, `Last Reviewed Date` | Max date across approved supporting chain | No | Dry-run apply only | If target date is newer | If target **newer** |
| **Refresh Due Date** | Source `Source Date` + policy | Optional: source date + staleness window — **Needs Verification** policy | Suggest | No | Yes | — |
| **Confidence Level** / **Data Confidence Level** | Facts `Confidence Level`, Published `Overall Source Confidence`, Source `Source Quality` | Roll up conservatively (min of fact confidence and source quality). Operator Master → **Data Confidence Level** column. | Suggest | Dry-run apply only | If downgrade would hide chip | — |
| **Evidence Notes** | Facts `Evidence Text`, Source `Source Title` + URL | Append summary pointer to PI source(s); do not paste full URLs in owner-facing fields if policy restricts — internal notes OK | Yes | Append mode only | Yes | Do not truncate company validation notes |
| **Missing Data Flags** | Facts `Data Gap?`, `Follow-up Question` | Summarize open gaps from approved gap facts | Yes | Dry-run apply only | Yes | — |
| **Company Validated** | Helena intake, company attestation | **Manual only** — direct company confirmation | No | **Never** | **Always** | Always if true |
| **Company Validation Date** | Company attestation record | **Manual only** | No | **Never** | **Always** | Always if present |
| **Reviewed By** | `Reviewed By` on Source/Facts/Published | Copy reviewer identifier (text field on Setup) | No | Dry-run apply only | — | — |
| **External Display Status** | Reviewer publish package | **Show Trust Label** only after explicit human publish approval | Suggest | **Never** auto | **Yes** | If Do Not Display |
| **Internal Notes** | PI `Reviewer Notes`, publish audit | Append publish audit line; prefer not to replace hold/review notes | Yes | Append only | Yes | If notes contain hold markers — **Needs Verification** pattern |

---

## Do-Not-Overwrite Rules

Publish flow **must skip write** (or emit pending recommendation only) when target profile has:

| Condition | Action |
|-----------|--------|
| `Company Validated` = true | Skip |
| `Validation Status` = Company Validated | Skip |
| `Company Validation Date` present | Skip |
| `Usage Permission` = Do Not Use | Skip |
| `External Display Status` = Do Not Display | Skip |
| Target `Last Reviewed Date` **newer** than PI proposal | Skip (or queue for review) |
| `Internal Notes` contains hold/review sentinel (e.g. `HOLD`, `do not publish`) — **Needs Verification** | Skip |
| Source `Status` = Stale or Published `Stale?` = true | Skip |
| Source `Source Quality` = Low without override | Skip |
| Conflicting approved facts with no resolution | Skip → report conflict |

**Mirror:** `scripts/pilot-profile-governance-values.mjs` protection logic — reuse in future publish script.

---

## Conflict Handling

When sources or profiles disagree:

| Scenario | Resolution |
|----------|------------|
| Multiple PI sources disagree | Do not merge silently; create conflict report; prefer hierarchy below |
| PI says Company Published, profile says Company Validated | **Profile wins** — no downgrade, no overwrite |
| Newer source has lower confidence than older | Prefer **higher validation level** + **more recent review date**; never auto-downgrade Company Validated |
| CALA-specific vs global | Prefer **regional** label for CALA product surfaces when `Source Region` = CALA-Specific; global remains separate fact |
| Public source vs company-provided Setup | Company-provided wins for conflicting claims |
| Extracted Facts vs Published Explorer Fields | **Published** wins for field overlay; profile governance uses **rollup reviewer decision**, not single fact |

**Hierarchy (strongest → weakest):**

1. Company Validated (regional when scoped)
2. Recent company-published regional data
3. Recent company-published global data
4. Credible third-party source-informed data
5. AI-assisted interpretation

AI inference **never** overrides sourced facts or company-validated data.

---

## Profile-Level vs Field-Level Governance

| Phase | Scope |
|-------|--------|
| **This plan — Phase 1 publish** | Profile-level P1 fields on Setup **roots** (Brand Basics, Operator Master) only |
| **Existing PI overlay** | Field-level `Published Explorer Fields` → Explorer content merge (`publish-overlay.js`) |
| **Later** | Field-level confidence on high-impact Explorer fields (CALA presence, conversion relevance, third-party management, regional capability, brand standards themes, scoring-critical fields) |

Do not conflate profile trust chip (`displayLabel`) with per-field published values.

### External Explorer trust-label mapping (read path)

PI publish may write internal `Validation Status` values (e.g. **Company Published**, **Source-Informed**). Explorer chips use **`normalizeProfileGovernance()`** to map to conservative external copy:

| Internal `validationStatus` | External `displayLabel` | `sourceBasis` (subtitle) |
|-----------------------------|-------------------------|--------------------------|
| Company Validated (gated) | Company-Validated Profile | — |
| Company Reviewed | Company-Reviewed Profile | — |
| Company Published | AI-Assisted Profile | Company Materials |
| Source-Informed | Source-Informed Profile | Reviewed Sources |
| AI-Assisted | AI-Assisted Profile | AI-assisted research |

Subtitle: `Last Reviewed: [date] · Source Basis: [basis] · Region: [region]` — **no** `Confidence:` line. Confidence columns remain on Setup for QA only.

---

## Publish Modes

### Mode 1 — Report Only

`audit-partner-intelligence-publish-readiness.mjs` lists eligible PI packages ( **approved Explorer-use publish scope** ), publish-scope blockers, full-package diagnostics, and proposed profile governance — **no writes**.

**Publish scope (2026-07-06):** Profile-governance publish eligibility is evaluated from approved Explorer-use sources and their approved/edited facts only. Non-approved linked sources remain visible as `excludedFromPublishScope` / `fullPackageWarnings` but do not block eligibility unless included in publish scope.

**Sparse confidence cap (2026-07-06):** Proposed **Confidence Level** is capped at **Medium** when publish scope has fewer than 3 approved facts and/or identity-only coverage. **High** requires ≥3 approved facts, at least one identity fact, at least one positioning/overview/development/capability/geography/owner-consideration fact, and ≥1 approved Explorer-use source. Evidence Notes append sparse/identity-only warnings. External trust chip mapping unchanged.

### Mode 2 — Dry Run

`publish-partner-intelligence-profile-governance.mjs` (future) shows exact Setup field patches — **no writes**. Default.

### Mode 3 — Apply

Same script with `--apply`. Writes approved profile-level fields only. Respects do-not-overwrite. Batch ≤10 records per Airtable pattern.

### Mode 4 — Pending Review Queue (future)

Write recommendations to internal queue table or JSON artifact; human approves in admin UI before Mode 3.

---

## Required Scripts Later

| Script | Purpose | Status |
|--------|---------|--------|
| `scripts/audit-partner-intelligence-publish-readiness.mjs` | Find PI records eligible for profile-governance publish; report missing review/linkage; detect stale/conflict; simulate do-not-overwrite blocks | **Implemented (read-only)** — `npm run audit-partner-intelligence-publish-readiness` |
| `scripts/publish-partner-intelligence-profile-governance.mjs` | Publish approved PI governance rollup to Brand/Operator Setup roots; `--dry-run` default; `--apply` required; alias `Data Confidence Level` on Operator Master | **Implemented** — `npm run publish-partner-intelligence-profile-governance` |
| `scripts/test-partner-intelligence-profile-governance-publish.mjs` | Unit tests: publish mapping, protections, operator confidence alias | **Implemented** — `npm run test:partner-intelligence-profile-governance-publish` |
| `scripts/test-partner-intelligence-publish-readiness.mjs` | Unit tests: eligibility, mapping, protections, conflicts, alias handling | **Implemented** — `npm run test:partner-intelligence-publish-readiness` |

**Not in scope now:** Modifying `publish-overlay.js` field-level publish.

---

## Report Outputs Later

| File | Contents |
|------|----------|
| `reports/partner-intelligence-publish-readiness.json` | Machine-readable eligibility audit |
| `reports/partner-intelligence-publish-readiness.md` | Human-readable readiness summary |
| `reports/partner-intelligence-profile-governance-publish.json` | Dry-run/apply patch preview or results |
| `reports/partner-intelligence-profile-governance-publish.md` | Field diffs, protections, expected `normalizeProfileGovernance()` output |

---

## API / UI Impact

| Area | Impact |
|------|--------|
| **Publish flow itself** | No UI required — script/admin workflow first |
| **Explorer trust chips** | Auto-update when Setup profile governance changes (existing read path) |
| **Explorer content** | Unchanged — still Setup + optional PI overlay |
| **Scoring / OAS** | No change |
| **Snapshot footnotes** | Phase 4 in governance-read-path plan — defer |

---

## Manual Workflow For Now

Until automation ships:

1. Add source → **Partner Intelligence - Source Library** (`Approved for Extraction` / `Approved for Explorer Use` when ready).
2. Run extraction → **Extracted Facts** (`Human Review Status` = Pending → Approved/Edited).
3. Approve field-level publish if needed → **Published Explorer Fields** (`Publish Status` = Published).
4. Decide profile trust rollup (validation level, region, confidence, display) in review.
5. Apply profile governance via **`npm run pilot-profile-governance-values`** (controlled QA) or manual Airtable edit on Setup root.
6. Verify Explorer trust chip + API `governance` object.
7. Record audit trail in **Evidence Notes** / **Internal Notes** on Setup row; keep PI reviewer notes in PI tables.

---

## Risks / Open Questions

| Topic | Notes |
|-------|-------|
| **PI select option parity** | Confirm live options match `VAL_*_SELECTS` in field map before any write script. |
| **Entity linkage** | PI Brand/Operator links must resolve 1:1 to Setup records; ambiguous links block publish. |
| **Published vs Setup Presentation** | Field overlay may duplicate presentation slots — profile governance must stay on roots only. |
| **Strongest vs latest source** | Recommend **display-ready status** = conservative validation level + freshest **human-reviewed** package, not raw latest capture. |
| **AI extraction → Company Validated** | **Forbidden** without company attestation — highest reputational risk. |
| **Publish history** | Need append-only audit (Internal Notes + future publish log table?) — **Needs Verification**. |
| **Company validation outreach order** | Helena intake → Source Library first; Setup `Company Validated` only after explicit confirmation — **Needs Verification** for attestation field design. |
| **Profile governance registry keys** | Need canonical `Field Name` values for profile rollup facts in PI registry — **Needs Verification**. |
| **Low quality override** | Whether reviewers can force publish at Low quality — default **no**. |

---

## Recommended Next Step

**Run the read-only publish readiness audit** and review reports before any publish script:

```bash
npm run audit-partner-intelligence-publish-readiness
```

Reports: `reports/partner-intelligence-publish-readiness.{md,json}`

When audit output is trusted, run `npm run publish-partner-intelligence-profile-governance` (dry-run default) mirroring `pilot-profile-governance-values.mjs` safety patterns. Example Arbor dry-run:

```bash
npm run publish-partner-intelligence-profile-governance -- --entity-type operator --target-rec-id recF5Z87OAqFgndoq --dry-run
```

**Stewardship before publish:** [partner-intelligence-stewardship-fix-plan.md](./partner-intelligence-stewardship-fix-plan.md) — audit-driven manual fixes (Arbor Lodging CALA first).

---

## Related

- [governance-read-path-trust-label-plan.md](./governance-read-path-trust-label-plan.md)
- [brand-operator-validation-fields-plan.md](./brand-operator-validation-fields-plan.md)
- [partner-intelligence-repository-mvp-plan.md](../partner-intelligence-repository-mvp-plan.md)
- `lib/profile-governance/normalize-profile-governance.js`
- `scripts/pilot-profile-governance-values.mjs`
- `lib/partner-intelligence/publish-overlay.js` (field-level only today)
