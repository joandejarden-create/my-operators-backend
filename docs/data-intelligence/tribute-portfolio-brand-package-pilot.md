# Tribute Portfolio by Marriott — Full Brand Intelligence Package Pilot (v1)

> First **non-Choice** full Brand Intelligence Package pilot. Goal: bring Tribute
> Portfolio to Kimpton / Radisson Blu level using the existing factory, not a
> one-off workflow. **Dry-run only** — no Airtable writes.

## What this pilot is

Choice Legacy governance is complete (mini-batch 1/2/3 all Platform Ready). This
pilot tests whether the Dealality Intelligence Factory can build a full package
for a Marriott soft brand:

- official sources · PDFs / development materials · images / logo · PR / openings
- extracted facts · AI-drafted Brand Setup completion · owner considerations
- governance / trust chip

## Reusable pipeline fix shipped with this pilot

**Friction removed:** the Choice pipeline previously required hand-copying
`allowlistedSourceIds` into `choice-legacy-batch-config.js` after Source Library
rows were registered.

**Fix:** `lib/partner-intelligence/brand-source-auto-resolver.js` resolves approved,
linked, company-controlled Source Library rows by brand record id, classifies each
by role (local PDF / consumer / development / press / PR-opening / image), selects
extraction-eligible sources, de-duplicates, and generates the allowlist — no manual
source IDs.

The Choice pipeline now uses it via `resolveExtractConfigFromStewardshipBrand`:
a **non-empty manifest allowlist is still preferred** (shipped batches stay stable),
and the resolver only auto-generates when the manifest is empty. Regression:
mini-batch 1/2/3 dry-runs remain Platform Ready. Validated against Ascend live
sources — the resolver reproduces the exact 3 IDs previously copied by hand.

## Command

```bash
npm run tribute-portfolio-brand-package -- --dry-run          # full plan + report
npm run tribute-portfolio-brand-package -- --dry-run --skip-url-probe   # offline
```

Read-only planner. `--apply` / `--register` are rejected; registration and
governance happen through the existing stewardship/extraction/governance scripts
once sources exist.

## Outputs

- `reports/tribute-portfolio-brand-package.md`
- `reports/tribute-portfolio-brand-package.json`

## Brand record

| Field | Value |
|-------|-------|
| Record | `recCvV0PuZOi8c3hC` |
| Name | Tribute Portfolio |
| Parent | Marriott International, Inc. |
| Brand Status | Active |
| Existing profile | **Strong Existing Profile** (but hero = Mock/Demo, not source-backed) |
| PI sources / facts before pilot | 0 / 0 |

## Source package (proposed, dry-run)

Company-controlled only:

1. Consumer — `https://tribute-portfolio.marriott.com/` (reachable; register website capture)
2. Marriott Bonvoy — `https://www.marriott.com/loyalty.mi` (guest promise / Bonvoy relationship)
3. Local FDD — `Marriott International/fdd/Tribute Portfolio/2026-tribute-portfolio-fdd-3-31-2026.pdf` (secondary factual: parent, development model, fees)
4. Development context — local Marriott development HTML captures (provenance)

**Not usable as-is:** `news.marriott.com` (JS shell, ~0 readable text → provenance
only); `development.marriott.com` (unreachable at probe time → use local captures).

## Governance recommendation

Sources are Marriott-controlled → **Company Materials** posture:

- Validation Status: Company Published
- Usage Permission: Platform Display Allowed
- External Display Status: Show Trust Label
- External chip: **AI-Assisted Profile** · Source Basis: **Company Materials**
- Company Validated: **false / unchanged** · Company Validation Date: **unchanged**

Fallback: if the consumer page fails stewardship extraction and only the FDD is
usable, hold at **Source-Informed** (do not publish) until a second official web
page is registered.

## Guardrails

- Do not overwrite existing (demo) Brand Setup content — back it with staged facts.
- Do not set Company Validated / Company Validation Date; do not imply Marriott review.
- No third-party / OTA pages as primary evidence.
- No image downloads — asset-governance is a **future module** (does not exist yet).
- No UI / scoring / BAS / OAS / OCS / Deal Readiness / schema changes.

## Apply plan (stage 2 — `tribute-portfolio-package-apply-plan`)

`npm run tribute-portfolio-package-apply-plan -- --dry-run` turns the package
dry-run into a concrete, staged apply plan. Read-only: it writes **nothing** to
Airtable and does not register/extract/approve/publish. It reuses the package
report, the Source Library field map (validated payloads), `brand-source-auto-resolver`
role classification, and the publish-readiness taxonomy.

Output: `reports/tribute-portfolio-package-apply-plan.{md,json}`.

- **Registration plan (6 sources, all valid, all company-controlled):** 2 web
  (`partner-reference:download --register`) + 4 local (FDD + 3 HTML captures via a
  local-file register step — see `register-radisson-blu-pdf-sources.mjs` pattern).
  All register with Approved-for-Extraction / Explorer = **No** (stewardship decides).
- **Proposed facts:** 12 Pending across the target keys; **7** approvable by
  source-backed stewardship, **2 HELD** (`be.economics.royaltyPct`,
  `be.economics.initialFranchiseFee`) as **Internal Only** with FDD Item 19 / fees /
  legal kept human-review.
- **Brand Setup completion draft** is staging output only — existing populated fields,
  logo, and Mock/Demo hero are marked **do-not-overwrite**.
- **Governance readiness path:** 6-gate sequence (register → steward → extract →
  approve → readiness-clean ≥3 approved facts → publish Company Materials). Company
  Validated / Validation Date never set.

Regression (all green): mini-batch 1/2/3 Platform Ready, active-brand 11/11 Platform
Ready, `test:partner-intelligence-publish-readiness` + `…-profile-governance-publish` ok.

## One-command pipeline (stage 3 — `tribute-portfolio-package-pipeline`)

`npm run tribute-portfolio-package-pipeline -- --dry-run` replaces step-by-step
manual execution with a single orchestrator over the existing factory primitives:
registration (`createPartnerSource`) → stewardship (`patchPartnerSource`) →
extraction (`runPartnerBrandExtraction`) → fact stewardship (`patchPartnerFact`,
FDD economics HELD) → governance publish (`profile-governance` readiness + diff) →
verification. Source-ID wiring is automatic via `brand-source-auto-resolver`.

Apply is gated behind `--apply --approve-tribute-portfolio-package-pipeline`, runs
only safe pending stages, skips duplicates, no-ops on stable governance, and halts
on any blocker (e.g. extraction disabled, governance protection). Full spec:
[tribute-portfolio-package-pipeline-v1.md](./tribute-portfolio-package-pipeline-v1.md).

## Honest remaining work to reach Kimpton / Radisson Blu level

1. Register + approve the consumer page and Tribute FDD (dry-run then apply via
   existing source scripts).
2. Extract facts (FDD-heavy) → fact stewardship (Pending → Approved) for the ~11
   target keys; keep economics/Item 19 human-review / internal.
3. Capture recent openings / PR (needs a rendered snapshot; newsroom is a JS shell).
4. Replace Mock/Demo hero + add verified property imagery — **blocked on a future
   asset-governance module**.
5. Governance publish (Company Materials, AI-Assisted Profile chip) once facts approved.
