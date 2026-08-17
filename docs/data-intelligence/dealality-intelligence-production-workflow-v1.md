# Dealality Intelligence Production Workflow v1

**Date:** 2026-07-06  
**Status:** v1 — plan wrapper + dry-run orchestration  
**Command:** `npm run intelligence-profile-workflow`  
**Batch queue (v1.1):** `npm run intelligence-production-queue`

> **Authority:** [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md), [partner-intelligence-profile-governance-publish-plan.md](./partner-intelligence-profile-governance-publish-plan.md)

---

## 1. Purpose

Dealality Intelligence Production Workflow v1 is the **governed intelligence production layer** for the platform — not only Explorer profile copy.

Approved intelligence packages support:

- **Brand Explorer** and **Operator Explorer** trust chips and profile fields
- **Brand / Operator / Capability alignment snapshots** (BAS, OAS, OCS read paths — workflow does not change scoring)
- **Deal Readiness** evidence and gap questions (read-only consumption)
- **Owner and brand/operator outreach preparation**
- Future matching, capital-partner, and market-anchor workflows

The workflow **automates mechanics** (status, audits, dry-runs, next-command guidance) while **preserving human approval gates** at every write boundary.

---

## 2. Supported entity types

| Entity type | v1 implementation | Notes |
|-------------|---------------------|-------|
| **Brand** | Yes | Brand Setup - Brand Basics root |
| **Operator** | Yes | Operator Setup - Master root |
| Owner / ownership group | Later | Documented, not orchestrated |
| Capital partner / lender | Later | Documented, not orchestrated |
| Advisor / consultant | Later | Documented, not orchestrated |
| Market / demand anchor | Later | Documented, not orchestrated |
| Deal-level evidence | Later | Documented, not orchestrated |

---

## 3. Workflow stages

### Stage 0 — Resolve entity

- Entity type (`brand` | `operator`)
- Company / brand display name
- Airtable root record ID (`rec…`)
- Live governance state (Setup root fields)
- PI package state (sources, facts, publish scope)

**v1 command:** `npm run intelligence-profile-workflow -- --entity-type … --target-rec-id rec… --plan`

**v1.1 batch command (preferred for priority tracker):** `npm run intelligence-production-queue -- --plan`

### Stage 1 — Source discovery

- Official website URLs
- Company PDFs / brochures / decks
- Local reference folders (Brand + Operator roots)
- Existing Source Library rows

**Outputs:** URL shortlist, folder plan, gap list. **No writes.**

### Stage 2 — Source capture / register

- `partner-reference:init-folder` (dry-run → apply)
- `partner-reference:download` (dry-run → apply → register)
- Supports **Brand Reference Material** and **Operator Reference Material**

**Rule:** First-pass official profile uses **company-controlled** sources unless explicitly scoped otherwise.

### Stage 3 — Source stewardship

- Approve only reviewed sources for **Approved for Explorer Use = Yes**
- Advance status **Captured → Approved** (or **Extracted** after extraction)
- Keep excluded / diagnostic sources out of publish scope
- **Never** approve noisy, wrong-entity, or quarantined sources

**Command:** `npm run steward-partner-intelligence -- … --dry-run --recompute` → explicit `--apply --approve-stewardship --approve-source-ids …`

### Stage 4 — Extraction dry-run

- Candidate facts only; default **no Airtable fact writes**
- Company-specific narrow scripts when registered (Hotel Equities, GHL Hoteles, Curio recovery)
- Report: unsupported keys, duplicates, weak/noisy facts, quality tier

**Command examples:**

- `npm run hotel-equities-extract -- --dry-run`
- `npm run ghl-hoteles-extract -- --dry-run`

Apply requires **explicit** `--apply --approve-<company>-extract` flags.

### Stage 5 — Fact stewardship

- Approve only **strong** facts (`Approved` / `Edited`)
- Leave weak facts **Pending** or **Rejected**
- Prevent wrong-brand contamination (Curio lesson)
- Avoid overclaiming (management structure, ownership, third-party flags)

**Command:** `npm run steward-partner-intelligence -- … --approve-fact-ids "rec…,rec…"` (after review)

### Stage 6 — Governance publish dry-run

Proposes (never applies in dry-run):

- Validation Status
- Usage Permission
- Source Type / Source Region
- Last Reviewed Date
- Data Confidence Level
- Evidence Notes
- External Display Status

**Never writes:** Company Validated, Company Validation Date

**Command:** `npm run publish-partner-intelligence-profile-governance -- … --dry-run`

### Stage 7 — Governance apply

- Only after founder approval of dry-run diff
- Verify **no_op** on subsequent audit + publish dry-run

**Command:** `npm run publish-partner-intelligence-profile-governance -- --apply --entity-type … --target-rec-id rec…`

### Stage 8 — Platform usage

Intelligence is consumed by product surfaces and GTM workflows. Re-verify with:

```bash
npm run audit-partner-intelligence-publish-readiness
npm run intelligence-profile-workflow -- --entity-type … --target-rec-id rec… --verify
```

---

## 4. Approval gates

| Gate | Human approval required | Automated in v1 |
|------|-------------------------|---------------|
| Source capture/register | Yes (per URL) | No — prints commands |
| Source Explorer approval | Yes | No |
| Extraction apply | Yes (`--approve-*-extract`) | No |
| Fact approval | Yes (`--approve-fact-ids`) | No |
| Governance publish apply | Yes | No |
| Company Validated | **Company attestation only** | **Never** |

---

## 5. Command consolidation (v1)

**Primary command:**

```bash
npm run intelligence-profile-workflow -- --entity-type operator --target-rec-id reciI2tYQBfMoMK9G --plan
```

**Flags (v1):**

| Flag | Behavior |
|------|----------|
| `--plan` | Read-only status + next commands (default) |
| `--verify` | Plan + readiness audit + governance dry-run |
| `--all-dry-run` | Plan + audit + steward dry-run + narrow extract dry-run (if registered) + governance dry-run |
| `--publish-dry-run` | Governance publish dry-run only |
| `--extract` | Narrow extract dry-run only (if registered) |
| `--steward-sources` / `--steward-facts` | Stewardship dry-run only |
| `--capture` | Prints capture guidance (not orchestrated) |
| `--publish-apply` | **Rejected in v1** — use explicit publish script |

Reports: `reports/intelligence-profile-workflow.{md,json}`

Underlying scripts remain **unchanged** — workflow orchestrates read-only paths only.

---

## 6. Safety rules

- Default **dry-run** everywhere writes are possible
- Explicit **apply** and **approve-*** flags on every write script
- **No Company Validated** or **Company Validation Date** writes from PI publish
- **No governance publish** if publish-scope has **no approved facts**
- **No fact approval** from sources not in publish scope (steward enforces linkage)
- **No scoring / BAS / OAS / OCS / Deal Readiness** changes
- **No UI** changes
- **No overwrite** of stronger live governance (protection rules in publish layer)
- **No use** of excluded / rejected / quarantined facts in recommendations
- **No Airtable schema** changes from this workflow

---

## 7. Source basis rules

| Evidence basis | Internal Validation Status | External chip |
|----------------|---------------------------|---------------|
| Official website, brochure, PDF, deck (company-controlled) | **Company Published** | **AI-Assisted Profile** · Source Basis: **Company Materials** |
| Reviewed third-party / mixed public sources | **Source-Informed** | **Source-Informed Profile** · Source Basis: **Reviewed Sources** |
| Company-reviewed attestation | Company-reviewed path | **Company-Reviewed Profile** |
| Company validated (attestation only) | Company Validated | **Company-Validated Profile** |

Website Capture on official domains maps to **Company Published** when publish scope is company-controlled (see `profile-governance-publish-readiness.js`).

---

## 8. Profile completeness thresholds

| Tier | Criteria |
|------|----------|
| **Minimum viable** | ≥1 approved Explorer-use source **and** ≥2 approved facts |
| **Sparse** | ≥2 approved facts (confidence capped at **Medium**) |
| **Useful** | ≥5 approved facts with substantive coverage |
| **High confidence** | Only when identity + substantive fields are well supported (readiness caps apply) |

**Do not publish** identity-only packages unless explicitly allowed; readiness emits `sparse_publish_scope_fact_set` warnings.

---

## 9. Lessons from pilots

| Pilot | Lesson |
|-------|--------|
| **Arbor Lodging** | Operator path with existing materials; Source-Informed when mixed public evidence |
| **Kimpton Hotels** | Brand company materials → Company Published / AI-Assisted Profile |
| **Curio Collection** | Wrong-brand contamination — quarantine + narrow re-extract; keep diagnostic sources excluded |
| **Hotel Equities** | Website + PDF paths; PDF manual curation when deck noise; narrow extract allowlists |
| **GHL Hoteles** | From-scratch official capture → 5 EN sources → narrow extract → 5 approved facts → governance **no_op** stable |

---

## 10. Implementation (v1)

| Module | Role |
|--------|------|
| `lib/partner-intelligence/intelligence-profile-workflow.js` | Stage detection, completeness, next commands |
| `scripts/intelligence-profile-workflow.mjs` | CLI plan + read-only orchestration |
| `scripts/test-intelligence-profile-workflow.mjs` | Lightweight stage / safety tests |

**Not removed:** `hotel-equities-extract`, `ghl-hoteles-extract`, `steward-partner-intelligence`, `publish-partner-intelligence-profile-governance`, etc.

## 11. Implementation (v1.1) — batch queue

| Module | Role |
|--------|------|
| `lib/partner-intelligence/intelligence-production-queue.js` | Priority list, batch aggregation, queue markdown |
| `scripts/intelligence-production-queue.mjs` | Read-only batch CLI |
| `scripts/test-intelligence-production-queue.mjs` | Queue helper tests |

**Command:**

```bash
npm run intelligence-production-queue -- --plan
```

**Filters:** `--entity-type brand|operator`, `--stage N`, `--ready-only`, `--blocked-only`

**Reports:** `reports/intelligence-production-queue.{md,json}`

**Priority source:** hardcoded list from [partner-intelligence-priority-profile-production-tracker.md](./partner-intelligence-priority-profile-production-tracker.md) (10 entities; TBD IDs reported as unresolved).

**When to use:**

| Tool | Use when |
|------|----------|
| `intelligence-production-queue` | Review all priority profiles; find blockers; pick next entity |
| `intelligence-profile-workflow` | Deep dive one entity; print step-specific commands |

**Next implementation options:**

1. Register more entities in `NARROW_EXTRACT_BY_ENTITY`
2. Optional `--all-dry-run` CI job per priority profile
3. v2 apply orchestration only with per-step confirmation tokens (not in v1/v1.1)

## 12. Next milestone — Platform field publishing (v1)

After batch queue v1.1, the next step is **mapping approved facts to product fields** without blind overwrite:

```bash
npm run approved-intelligence-field-publishing-audit -- --entity-type operator --target-rec-id rec...
```

See [approved-intelligence-platform-field-publishing-v1.md](./approved-intelligence-platform-field-publishing-v1.md).

## 13. Field suggestions (Mode B review)

```bash
npm run approved-intelligence-field-suggestions -- --entity-type operator --target-rec-id rec...
```

See [approved-intelligence-field-suggestions-v1.md](./approved-intelligence-field-suggestions-v1.md).

## 14. Controlled platform field publish (v2)

```bash
npm run controlled-platform-field-publishing -- --entity-type operator --target-rec-id rec... --fact-id rec... --destination-field specificMarkets --dry-run
```

See [controlled-platform-field-publishing-v2.md](./controlled-platform-field-publishing-v2.md).

## 15. Controlled publish queue (v2.1)

```bash
npm run controlled-publish-queue -- --plan
```

Preferred batch view for product-field publishing readiness across priority entities.

See [controlled-publish-queue-v2-1.md](./controlled-publish-queue-v2-1.md).

## 16. Approved fact correction (v1)

```bash
npm run approved-fact-correction -- --fact-id rec... --correct-value "..." --reason "..." --dry-run
```

Use when product fields were corrected but the underlying approved PI fact still has an older value.

See [approved-fact-correction-v1.md](./approved-fact-correction-v1.md).

---

## Change Impact

| Tier | **Low–Medium** — new plan wrapper; no schema changes; no apply orchestration |
| Rollback | Remove package script; delete workflow module; existing scripts unaffected |

## Regression checklist

- [ ] `npm run test:intelligence-profile-workflow`
- [ ] `npm run test:intelligence-production-queue`
- [ ] `npm run intelligence-production-queue -- --plan`
- [ ] `npm run intelligence-profile-workflow -- --plan` for GHL (`no_op` / stage 8)
- [ ] `npm run intelligence-profile-workflow -- --plan` for Hotel Equities
- [ ] Confirm no `--publish-apply` path in workflow CLI
- [ ] Confirm Company Validated unchanged after plan/verify runs
