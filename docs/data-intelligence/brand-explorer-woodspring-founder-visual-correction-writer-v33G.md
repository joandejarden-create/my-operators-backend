# Brand Explorer WoodSpring Founder Visual QA Correction v33G

**Writer:** `brand-explorer-woodspring-founder-visual-correction-writer`  
**Version:** v33G  
**Brand:** WoodSpring Suites (`woodspring-suites` / `recsOd51NzRPYsMko`)

## Purpose

Correct founder-visible WoodSpring UI issues that pass mechanical Final QA but fail visual review:

1. `overview.scenario.3` showing **Boutique Resort Adjacency** + **IMAGE** placeholder (atelier hardcoded fallback)
2. `overview.bestAt.1–3` risky performance/economics language (ADR, fees, amenity stack, tier-appropriate QA)
3. v33C-R3 linking all three opening rows to the same Charlotte registry asset instead of property-specific assets

## Scope

- `overview.scenario.3` and duplicate/stale rows for that slot
- `overview.bestAt.1–3` copy
- Presentation → registry links for 3 openings + 3 gallery rows (link field only)
- UI/API rendering audit via atelier simulation

## Out of scope

- Company Validated / Company Validation Date
- Source Library approval statuses
- Momentum, proof, standards rows
- Opening copy (except registry link)
- Summary URL / View Summary URL
- Image field writes (hide scenario.3 if no safe image instead)
- Non-target brands

## Usage

Dry-run (default):

```bash
npm run brand-explorer-woodspring-founder-visual-correction-writer -- --brand woodspring-suites --dry-run
```

Apply (all gates required):

```bash
npm run brand-explorer-woodspring-founder-visual-correction-writer -- --brand woodspring-suites --apply \
  --approve-brand-explorer-v33G-woodspring-founder-visual-correction \
  --confirm-no-company-validation-claim \
  --confirm-no-summary-url-field \
  --confirm-no-source-library-changes \
  --confirm-no-momentum-proof-standard-changes \
  --confirm-woodspring-only
```

## Post-run validation

```bash
npm run brand-explorer-final-qa-auditor -- --brand woodspring-suites --dry-run
npm run brand-explorer-complete-build -- --brand woodspring-suites --dry-run --target-quality active-profile
npm run brand-explorer-visual-display-defect-audit -- --brand woodspring-suites --dry-run
npm run test:partner-intelligence-publish-readiness
npm run test:partner-intelligence-profile-governance-publish
```

## Root cause notes

The atelier renderer (`brand-explorer-atelier-from-api.js`) uses hardcoded scenario titles including **Boutique Resort Adjacency** when no `overview.scenario.N` block supplies a title/image via the Brand Library API. Rows with `External Display Status = Do Not Display` are excluded from API blocks.

## Change impact

**High** — Airtable presentation writes (copy, display status, registry links). Rollback: revert presentation row patches from report JSON; re-run v33C-R3 if registry links need reset.
