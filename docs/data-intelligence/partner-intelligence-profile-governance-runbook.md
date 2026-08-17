# Partner Intelligence → Profile Governance Runbook

**Date:** 2026-07-06  
**Status:** Operational — Arbor Lodging (CALA) pilot completed end-to-end.

> **Authority:** [partner-intelligence-profile-governance-publish-plan.md](./partner-intelligence-profile-governance-publish-plan.md), [INTELLIGENCE_GOVERNANCE.md](./INTELLIGENCE_GOVERNANCE.md), [governance-read-path-trust-label-plan.md](./governance-read-path-trust-label-plan.md)

---

## Purpose

This runbook defines the **repeatable operating process** for turning **reviewed Partner Intelligence (PI) packages** into **profile-level governance updates** on Brand/Operator Setup root tables and **Explorer trust labels**.

Use it for founder reviews, steward workflows, and Cursor sessions. Default is **dry-run / report-only** until explicit human approval.

---

## What This Pipeline Does

```
Source evidence (PI Source Library)
  → extraction + fact review (Extracted Facts)
  → publish readiness audit
  → source stewardship (controlled fixes)
  → profile governance dry-run
  → profile governance apply (Setup root)
  → API normalized governance object
  → Explorer trust chip
```

**Outcome:** Explorer shows an honest trust label (`governance.displayLabel` + `displaySubtitle`) derived from reviewed PI evidence and Setup profile governance — not from legacy hero text or unreviewed extraction.

---

## What This Pipeline Does Not Do

- Does **not** set **Company Validated** unless directly confirmed by the company.
- Does **not** set **Company Validation Date**.
- Does **not** change scoring or OAS/BAS snapshot logic.
- Does **not** replace human review of sources and facts.
- Does **not** bulk-approve facts.
- Does **not** overwrite protected profiles.
- Does **not** publish field-level Explorer overlay rows (that is a separate `publish-overlay.js` path).

---

## Architecture

| Layer | Role |
|-------|------|
| **Partner Intelligence** | Source-level evidence, capture, extraction, human review, publish readiness gates. Tables: Source Library, Extracted Facts, Published Explorer Fields. |
| **Brand / Operator Setup roots** | Profile-level governance and trust-label fields. Brand: **Brand Setup - Brand Basics**. Operator: **Operator Setup - Master**. |
| **Governance normalizer** | `lib/profile-governance/normalize-profile-governance.js` — consistent API `governance` object, `displayLabel`, `displaySubtitle`. |
| **Explorer UI** | Renders trust chip only when `governance.displayLabel` is safe and present. Internal warnings stay server-side. |

**Principle:** PI holds document-level evidence; Setup roots hold profile-level trust rollup. Never duplicate PI URLs on P1 governance columns.

---

## Standard Workflow

### Step 1 — Run Publish Readiness Audit

```bash
npm run audit-partner-intelligence-publish-readiness
```

**Purpose:**

- Find eligible packages (**approved Explorer-use publish scope**).
- Report full linked-package diagnostics (excluded/non-approved sources) separately.
- Identify blockers on the **publish scope** (not blocked by excluded sources).
- Detect missing entity links.
- Detect target profile protection.
- Preview proposed governance values and expected Explorer chip.

**Publish scope rule:** Eligibility uses only sources with **Approved for Explorer Use = Yes** and **Approved/Edited** facts linked to those sources. Origin conflicts are evaluated within publish scope only. Non-approved linked sources appear as diagnostics (`excludedFromPublishScope`, `fullPackageWarnings`) and do not block publish eligibility.

**Sparse confidence cap:** `sparse_publish_scope_fact_set` is a **warning**, not a blocker. Proposed **Confidence Level** is capped at **Medium** when publish scope has &lt;3 approved facts and/or identity-only coverage. **High** requires richer substantive coverage (see `assessPublishScopeConfidence` in readiness lib). Evidence Notes append sparse/identity-only warnings; external trust chip mapping is unchanged.

**Outputs:**

- `reports/partner-intelligence-publish-readiness.md`
- `reports/partner-intelligence-publish-readiness.json`

---

### Step 2 — Review Blockers

| Blocker | Meaning | Typical fix |
|---------|---------|-------------|
| `no_approved_facts` | No Extracted Fact with Human Review Status = Approved or Edited | Approve a small, evidence-backed fact subset after review |
| `approved_for_explorer_use_no` | Source not approved for Explorer | Set **Approved for Explorer Use = Yes** only after source review (publish-scope blocker only when source is in approved set) |
| `no_approved_explorer_sources` | No sources in publish scope | Approve at least one clean source for Explorer Use |
| `missing_entity_link` | Source/fact not linked to Brand Basics or Operator Master `rec…` | Link PI record to correct Setup root |
| `source_status_not_ready:Found` / `Captured` | Source not through review workflow | Advance status after review (e.g. Found → Approved) |
| `source_stale` | Source Status = Stale | Refresh or replace source |
| `source_quality_low` | Quality below Medium | Raise quality after review or defer package |
| `conflict:mixed_company_and_public_source_origins` | Mixed company + public origins **within approved publish scope** | Narrow approved Explorer-use sources; full-package mixed origins are diagnostic only |
| `protected:*` | Setup profile blocked (Company Validated, Do Not Use, etc.) | Manual resolution — do not bypass |

See [partner-intelligence-stewardship-fix-plan.md](./partner-intelligence-stewardship-fix-plan.md) for package-specific guidance.

**Package integrity exception — Curio Collection by Hilton (`receQkxgjlezsc1xg`):** Dry-run stewardship is useful for inventory, but **do not apply** until [curio-pi-package-integrity-cleanup-plan.md](./curio-pi-package-integrity-cleanup-plan.md) is complete (wrong-brand facts from Mexico FDD extraction + mixed origins).

---

### Step 3 — Steward Sources / Facts

**Preferred — generalized stewardship assistant (any brand or operator package):**

```bash
npm run steward-partner-intelligence -- --entity-type brand --target-rec-id recCKuXCmGvxHPfb3 --dry-run
npm run steward-partner-intelligence -- --entity-type operator --target-rec-id recF5Z87OAqFgndoq --dry-run
```

Optional: `--recompute` to ignore cached readiness JSON and rebuild package from live PI tables; `--approve-source-ids` / `--approve-fact-ids` for apply.

**Outputs:** `reports/partner-intelligence-stewardship-package.{md,json}`

**Apply (explicit IDs only):**

```bash
npm run steward-partner-intelligence -- --apply --approve-stewardship \
  --entity-type brand --target-rec-id rec... \
  --approve-source-ids "rec...,rec..." \
  --approve-fact-ids "rec...,rec..."
```

**Pilot-specific examples (legacy — same safety rules):**

**Arbor (CALA) — dry-run:**

```bash
npm run steward-arbor-pi-pilot -- --dry-run
```

**Arbor — apply only after review:**

```bash
npm run steward-arbor-pi-pilot -- --apply --approve-arbor-stewardship
```

Optional fact approval (explicit IDs only):

```bash
npm run steward-arbor-pi-pilot -- --apply --approve-arbor-stewardship --approve-fact-ids "rec...,rec..."
```

**Rules:**

- Do not bulk-approve facts.
- Do not set Company Validated.
- Only approve sources/facts after evidence review.
- Re-run readiness audit after stewardship.

**Reports:** `reports/arbor-pi-stewardship-pilot.{md,json}`

For packages without a dedicated script: fix blockers manually in Airtable per stewardship fix plan, then re-audit.

**Kimpton (brand pilot):** `npm run steward-kimpton-pi-pilot` → `reports/kimpton-pi-stewardship-pilot.{md,json}`. Apply requires `--apply --approve-kimpton-stewardship` and explicit `--approve-fact-ids`.

---

### Step 4 — Re-run Readiness Audit

```bash
npm run audit-partner-intelligence-publish-readiness
```

**Target:**

- Package shows **eligible**.
- Target profile is **not protected**.
- Proposed governance values and expected chip look correct.

---

### Step 5 — Dry-run Profile Governance Publish

```bash
npm run publish-partner-intelligence-profile-governance -- --entity-type operator --target-rec-id recF5Z87OAqFgndoq --dry-run
```

For brands, use `--entity-type brand --target-rec-id rec…`.

**Purpose:**

- Preview Setup field-by-field diff.
- Confirm protection checks pass.
- Confirm expected Explorer chip.
- Confirm Company Validated / Company Validation Date are not in the patch.

**Outputs:**

- `reports/partner-intelligence-profile-governance-publish.md`
- `reports/partner-intelligence-profile-governance-publish.json`

Optional: `--recompute` to refresh eligibility from live Airtable instead of the report file.

---

### Step 6 — Apply Profile Governance Publish

**Only after founder/steward approval:**

```bash
npm run publish-partner-intelligence-profile-governance -- --apply --entity-type operator --target-rec-id recF5Z87OAqFgndoq
```

**Rules:**

- Dry-run first — always.
- Apply only **eligible** packages (`--only-eligible` is default).
- Never set Company Validated from PI.
- Never bypass protection rules.
- No force mode in v1.

---

### Step 7 — Verify API / Explorer UI

**Optional API checks:**

```http
GET /api/intake/third-party-operators/<recordId>
GET /api/brand-library/brand?brandId=<recordId>
```

Confirm response includes `governance.displayLabel` and `governance.displaySubtitle`.

**Manual UI checks:**

1. Open Explorer profile (operator or brand).
2. Confirm trust chip appears in header.
3. Confirm **no internal warnings** are visible in UI.
4. Confirm label does **not** overclaim validation (e.g. not “Company Validated” without attestation).
5. Confirm subtitle matches Last Reviewed, Source Basis, and Region — **not** Confidence.

**External display mapping (Explorer only):** Internal `Validation Status` values are mapped to conservative `displayLabel` / `sourceBasis` in `normalize-profile-governance.js`. Example: internal **Company Published** → external **AI-Assisted Profile** · Source Basis: Company Materials. See [governance-read-path-trust-label-plan.md](./governance-read-path-trust-label-plan.md#recommended-ui-labels).

**Post-apply audit (recommended):**

```bash
npm run audit-partner-intelligence-publish-readiness
npm run test:profile-governance-normalizer
```

Eligible package with **change class `no_op`** after apply means Setup already matches the PI proposal.

---

## Arbor Lodging Pilot Result

| Item | Value |
|------|-------|
| Operator | Arbor Lodging (CALA) |
| Record | `recF5Z87OAqFgndoq` |
| Target table | Operator Setup - Master |
| Stewarded sources | 6 (Explorer Use + Found→Approved on `recyY5faXntjMFkZp`) |
| Facts auto-approved | 0 |
| Publish readiness after stewardship | **Eligible** |
| Profile governance apply | **8 fields** written |
| Company Validated | **Untouched** |
| Company Validation Date | **Untouched** |
| Source Type | **Skipped** (proposed `Unknown`) |
| Validation Status | Source-Informed |
| Usage Permission | Platform Display Allowed |
| Source Region | CALA-Specific |
| Last Reviewed Date | 2026-06-10 |
| Data Confidence Level | Medium |
| External Display Status | Show Trust Label |
| Expected chip | **Source-Informed Profile** · Last Reviewed: Jun 10, 2026 · Source Basis: Reviewed Sources · Region: CALA-specific |
| Post-apply readiness | **Eligible / `no_op`** — profile governance matches proposal |

**Pilot reports:** `reports/arbor-pi-stewardship-pilot.md`, `reports/partner-intelligence-profile-governance-publish.md`, `reports/partner-intelligence-publish-readiness.md`

---

## Hotel Equities (CALA) — governance remap (dry-run)

| Item | Value |
|------|-------|
| Operator | Hotel Equities (CALA) |
| Record | `recWPKu5laVZxsvpn` |
| Approved sources | 3 official website captures (`Website Capture` → Company Website) |
| Approved facts | 5 |
| Live Validation Status (pre-remap) | Source-Informed |
| Proposed Validation Status | **Company Published** |
| Change class | **upgrade** |
| Expected chip | **AI-Assisted Profile** · Last Reviewed: Jul 6, 2026 · Source Basis: **Company Materials** |
| Company Validated | **Untouched** |

**Why remap:** PI `Source Origin` was `Public Web` on website captures, but mapped profile source type is **Company Website** (company-controlled). Source basis — not entity type — drives validation. Same rule as Kimpton FDD / brand company materials.

**Dry-run only until founder approval:**

```bash
npm run publish-partner-intelligence-profile-governance -- --entity-type operator --target-rec-id recWPKu5laVZxsvpn --dry-run
```

---

## Protection Rules

Publish script **skips** the target profile when:

- Company Validated = true
- Validation Status = Company Validated
- Company Validation Date is present
- Usage Permission = Do Not Use
- External Display Status = Do Not Display
- Last Reviewed Date on Setup is **newer** than PI proposal date
- Internal Notes contains **HOLD**, **DO NOT USE**, **REVIEW**, or similar markers
- Proposed update would **downgrade** validation status or confidence vs current values

Do not bypass protection. Resolve manually or leave unchanged.

---

## Governance Mapping

| Field | Rule |
|-------|------|
| **Validation Status** | Driven by **approved publish-scope source basis** (same rules for brand and operator). **Company Published** when all approved sources map to company-controlled profile source types (Company Website, Company PDF / Brochure, Official Website, Company Materials, etc.). **Source-Informed** for reviewed public/third-party, mixed company+public packages, or unknown default. **AI-Assisted** only for reviewed AI/platform-derived interpretation. **Never Company Validated from PI alone.** |
| **External chip** | `normalizeProfileGovernance()` maps internal status conservatively: Company Published → **AI-Assisted Profile** · Source Basis: Company Materials; Source-Informed → **Source-Informed Profile** · Source Basis: Reviewed Sources. Confidence never in subtitle. |
| **Usage Permission** | **Platform Display Allowed** for approved Explorer-ready packages. Do not set **Scoring Allowed** in v1. **Do Not Use** blocks publish. |
| **Confidence** | Brand → **Confidence Level**. Operator → **Data Confidence Level** (alias). **Internal/QA only** — never in Explorer `displaySubtitle`. |
| **External Display Status** | **Show Trust Label** only for eligible, reviewed packages. Otherwise Needs Review / no chip. |
| **Company Validated / Company Validation Date** | Human/company confirmation only — never from PI public-source extraction. |
| **Source Type** | Written only when proposed value is safe; **Unknown** is skipped. |

---

## Human Approval Points

Founder or designated steward must approve:

1. **Approved for Explorer Use = Yes** on each source used.
2. **Human Review Status** on each fact used for governance (Approved or Edited).
3. Advancing source status from **Found/Captured** to **Approved** (or equivalent).
4. **External Display Status = Show Trust Label** on Setup.
5. **Profile governance apply** (`--apply` on publish script).
6. Any future **Company Validated** status (company attestation required).

---

## Troubleshooting

### Package remains blocked after stewardship

Re-run `npm run audit-partner-intelligence-publish-readiness` and read `blockReasons` in JSON. Fix remaining source/fact/link issues; do not publish until eligible.

### Expected chip does not appear

Check:

- External Display Status = **Show Trust Label**
- Usage Permission is not Internal Only / Do Not Use
- Validation Status is not Needs Review / Do Not Use
- API returns `governance.displayLabel` (non-null)
- Explorer page loads `profile-governance-trust-chip.js`
- Governance fields are on the **canonical profile root table** (Brand Basics / Operator Master)

### Operator confidence not showing correctly

Operator Setup - Master uses **Data Confidence Level** as the live column; normalizer maps it to `governance.confidenceLevel`.

### Source Type skipped

If proposed Source Type is **Unknown**, publish script skips it. Set manually in Airtable after mapping PI source type, or improve PI source classification.

### Profile protected

Do not bypass. Review protection reasons in publish dry-run report. Resolve with founder or leave unchanged.

---

## Recommended Next Package

**Kimpton Hotels** (`brand:recCKuXCmGvxHPfb3`) — simpler than Curio:

- Entity link is resolved.
- Blockers are primarily **source approval** + **fact review** (no approved facts yet).
- No mixed-origin conflict in the first audit.

**Defer Curio** (`brand:receQkxgjlezsc1xg`) until link/conflict cleanup unless explicitly prioritized (missing links on two sources, mixed company/public origins).

---

## Standard Command Sequence (Arbor reference)

```bash
npm run audit-partner-intelligence-publish-readiness
npm run steward-arbor-pi-pilot -- --dry-run
npm run steward-arbor-pi-pilot -- --apply --approve-arbor-stewardship
npm run audit-partner-intelligence-publish-readiness
npm run publish-partner-intelligence-profile-governance -- --entity-type operator --target-rec-id recF5Z87OAqFgndoq --dry-run
npm run publish-partner-intelligence-profile-governance -- --apply --entity-type operator --target-rec-id recF5Z87OAqFgndoq
```

For non-Arbor packages, replace stewardship commands with manual Airtable fixes per [partner-intelligence-stewardship-fix-plan.md](./partner-intelligence-stewardship-fix-plan.md), then use publish script with the correct `--entity-type` and `--target-rec-id`.

---

## Related Docs and Scripts

| Resource | Path |
|----------|------|
| Publish plan | [partner-intelligence-profile-governance-publish-plan.md](./partner-intelligence-profile-governance-publish-plan.md) |
| Stewardship fix plan | [partner-intelligence-stewardship-fix-plan.md](./partner-intelligence-stewardship-fix-plan.md) |
| Trust label read path | [governance-read-path-trust-label-plan.md](./governance-read-path-trust-label-plan.md) |
| Validation fields plan | [brand-operator-validation-fields-plan.md](./brand-operator-validation-fields-plan.md) |
| Data dictionary | [DATA_DICTIONARY.md](../platform-reference/DATA_DICTIONARY.md) |
| Readiness audit | `scripts/audit-partner-intelligence-publish-readiness.mjs` |
| Profile governance publish | `scripts/publish-partner-intelligence-profile-governance.mjs` |
| Arbor stewardship | `scripts/steward-arbor-partner-intelligence-pilot.mjs` |
| Governance QA pilot (separate) | `scripts/pilot-profile-governance-values.mjs` |
| Readiness logic | `lib/partner-intelligence/profile-governance-publish-readiness.js` |
| Publish helpers | `lib/partner-intelligence/profile-governance-publish.js` |
| Normalizer | `lib/profile-governance/normalize-profile-governance.js` |
