# Tribute Portfolio Package Pipeline v1

**Date:** 2026-07-07  
**Status:** Implemented — dry-run default  
**Scope:** Source-backed text/governance path for one non-Choice brand — Tribute Portfolio by Marriott (`recCvV0PuZOi8c3hC`).

> **Authority:** [tribute-portfolio-package-pipeline.js](../../lib/partner-intelligence/tribute-portfolio-package-pipeline.js) · builds on [tribute-portfolio-package-apply-plan.js](../../lib/partner-intelligence/tribute-portfolio-package-apply-plan.js) and [tribute-portfolio-brand-package.js](../../lib/partner-intelligence/tribute-portfolio-brand-package.js). See also [tribute-portfolio-brand-package-pilot.md](./tribute-portfolio-brand-package-pilot.md).

---

## Purpose

The Tribute apply plan ended at a single `partner-reference:download` registration
command. This pipeline replaces step-by-step manual execution with **one command**
that orchestrates the full source-backed text/governance path, reusing the existing
Dealality Intelligence Factory primitives (no parallel one-off pipeline).

---

## Commands

```bash
# Dry-run (default) — plans all stages from live Airtable state, no writes
npm run tribute-portfolio-package-pipeline -- --dry-run

# Apply — runs only pending safe stages in order (requires approval flag)
npm run tribute-portfolio-package-pipeline -- --apply --approve-tribute-portfolio-package-pipeline
```

Reports: `reports/tribute-portfolio-package-pipeline.{md,json}`

---

## Pipeline stages (in order)

| Stage | Primitive reused | Apply behavior |
|-------|------------------|----------------|
| A. Source registration | `airtable-source.createPartnerSource` + apply-plan payloads | Register 6 company-controlled sources; skip duplicates (URL / local path); never register JS-shell newsroom |
| B. Source stewardship | `airtable-source.patchPartnerSource` | Status → Approved, Explorer Use → Yes, Extraction → Yes; FDD/loyalty extraction restrictions advisory |
| C. Extraction | `run-extraction.runPartnerBrandExtraction` | Pending facts only; requires `PARTNER_INTELLIGENCE_EXTRACTION_ENABLED=1` (halts if off) |
| D. Fact stewardship | `airtable-facts.patchPartnerFact` | Approve clean source-backed target facts; **HOLD** FDD economics/fees/Item 19/legal (Internal Only); weak/inferred → human review |
| E. Brand Setup completion | apply-plan `brandSetupCompletionDraft` | **Report/staging only — never written**; existing fields + hero + logo preserved |
| F. Governance publish | `profile-governance-publish-readiness` + `profile-governance-publish` | Publish only if readiness clean (≥3 approved facts + ≥1 approved Explorer source, no downgrade/protection) → Company Materials |
| G. Verification | live readiness re-check | Confirms governed Platform Ready, chip, and that Company Validated / Validation Date stayed untouched |

Source-ID wiring is automatic via `brand-source-auto-resolver` — no manual config.

---

## Governance target

- Validation Status: **Company Published**
- Usage Permission: **Platform Display Allowed**
- External Display Status: **Show Trust Label**
- External chip: **AI-Assisted Profile** · Source Basis: **Company Materials**
- Company Validated: **unchanged (never written)** · Company Validation Date: **unchanged**

---

## Apply safety

- `--apply` requires `--approve-tribute-portfolio-package-pipeline`.
- Runs only safe pending stages; re-detects live state after each stage.
- Skips duplicate sources/facts; no-ops when governance is already stable.
- Halts before a risky stage on any blocker (e.g. extraction disabled, protection).

---

## Guardrails (does not do)

- Rebuild Explorer; overwrite Brand Setup content / hero / image / logo.
- Download images (asset governance is a **future module**).
- Auto-approve held/weak facts or FDD economics; publish FDD economics externally.
- Set Company Validated / Company Validation Date; imply Marriott validation.
- Change UI, scoring, BAS, OAS, OCS, Deal Readiness, or schema.

---

## Current state (dry-run 2026-07-07)

Stage **Source Registration Needed** — 0 live PI sources, 6 company-controlled sources
ready to register, 12 target fact keys planned (7 approvable, 2 FDD-economics HELD).
Regression green: mini-batch 1/2/3 Platform Ready, active-brand 11/11 Platform Ready,
`test:partner-intelligence-publish-readiness` + `…-profile-governance-publish` ok.
Airtable not modified.

---

## Extraction-quality fix — targeted extraction (2026-07-07)

**Symptom:** after registration + stewardship, the generic extraction stage produced
24 Pending facts, **0 approvable** — all registry-wide data-gap placeholders.

**Root cause (not a text problem):** the generic extractor
(`run-extraction.runPartnerBrandExtraction`) is *pilot-keyed*.
`resolveBrandExtractionContext` only resolves brands in `PILOT_BRANDS`, and
`getBrandFieldHints` returns `null` when there is no pilot profile
(`BRAND_HINT_PROFILES` only has `kimptonHotels` + `curioCollection`). Tribute is not
a pilot, so every field fell through to `gapFact`. Source text loads fine
(FDD ~1.3M chars, Bonvoy 10.5k, consumer 2.4k, brand page 3.6k, dev captures 3–6k)
and the fact steward correctly refused to approve gaps — the pipeline protected us.

**Fix:** `lib/partner-intelligence/tribute-portfolio-targeted-extract.js` +
`npm run tribute-portfolio-targeted-extract -- --dry-run|--apply`. A conservative,
evidence-required Tribute (Marriott soft-brand) pattern set scans the *actual* loaded
text of approved sources and emits a fact **only** when a pattern matches
(with an evidence excerpt). No gap facts, no placeholders, no title-only (except
identity), no FDD economics/fees/Item 19/legal (kept Internal Only / human review).
Facts are created **Pending only**, tagged `extractionRunId=tribute-targeted-*` so
reruns de-duplicate.

**Dry-run result:** 9 proposed facts — **7 approvable** (brandName, parentCompany,
Marriott Bonvoy, positioning summary + tagline "Stay independent.", guest promise,
development model) + **2 human-review** (why-value, typical use case; AI-interpreted).
Governance would become eligible after approving the source-backed set. The 24
placeholder facts stay Pending, then move to Needs Review after clean facts approve.

Reusable for future Marriott soft brands: add a brand config to `TRIBUTE_RULES`.

---

## Asset & PR package governance (v1 — read-only, 2026-07-07)

Tribute is **text/governance Platform Ready** but hero remains Mock/Demo and visual
assets are not governed. New read-only module:

`npm run brand-asset-pr-package-governance -- --brand tribute-portfolio --dry-run`

→ `reports/brand-asset-pr-package-governance.{md,json}` · see
[brand-asset-pr-package-governance-v1.md](./brand-asset-pr-package-governance-v1.md).

Does not download images, overwrite logo/hero fields, or write Airtable. Flags
**Rendered Source Capture v1** for `news.marriott.com` PR/openings (JS-shell).
