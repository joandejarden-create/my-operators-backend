# Brand Asset & PR Package Governance v1

**Status:** Read-only pilot (Tribute Portfolio first)  
**Module:** `lib/partner-intelligence/brand-asset-pr-package-governance.js`  
**Script:** `npm run brand-asset-pr-package-governance -- --brand tribute-portfolio --dry-run`

## Purpose

Tribute Portfolio is **text/governance Platform Ready** but lacks **visual-package parity** with Kimpton / Radisson Blu. This v1 module inspects the current asset and PR layer, identifies gaps, surfaces official Marriott-controlled candidates, and produces a reusable governance report.

It does **not** download images, overwrite Brand Setup fields, or write Airtable.

## Commands

```bash
npm run brand-asset-pr-package-governance -- --brand tribute-portfolio --dry-run
npm run brand-asset-pr-package-governance -- --brand tribute-portfolio --dry-run --skip-url-probe
```

`--apply` is intentionally unsupported in v1.

## Outputs

- `reports/brand-asset-pr-package-governance.md`
- `reports/brand-asset-pr-package-governance.json`

## What it inspects

1. **Brand Setup - Brand Basics** media fields (logo, hero, images, PDFs, PR-related columns)
2. **Partner Intelligence** approved sources and facts (provenance only for assets)
3. **Local reference material** under `Marriott International/` (images, PDFs, HTML captures)
4. **Official URL candidates** — HTML probed for image URL references only (no image binary download)

## Asset statuses

`Candidate` · `Source-Confirmed` · `Approved For Explorer Use` · `Needs Usage Review` · `Do Not Use` · `Mock/Demo` · `Missing`

## Asset types

`Logo` · `Hero Image` · `Exterior / Property` · `Guestroom` · `Lobby / Public Space` · `Restaurant / Bar / Lifestyle` · `PR / Opening Image` · `PDF / Brochure` · `Press Link` · `Recent Opening Link`

## Source basis

`Company Materials` · `Marriott-Controlled Source` · `Rendered Official Source` · `Local Reference Material` · `Third-Party Context` · `Unknown / Do Not Use`

## Tribute pilot (recCvV0PuZOi8c3hC)

Known state entering v1:

- Text/governance Platform Ready (7 approved facts, 6 approved sources, AI-Assisted Profile)
- Hero: **Mock/Demo** — do not use
- Logo: present in Brand Setup — **source confirmation needed**
- No governed property/design/lifestyle image package
- PR/recent openings: **not captured** — `news.marriott.com` is JS-shell

## PR / recent openings

v1 **investigates** the Marriott newsroom URL but does **not** solve JS-shell extraction. The report flags **Rendered Source Capture v1** as required future tooling.

## Registry workflow (v1 — staging layer)

After governance identifies candidates, run:

`npm run brand-asset-registry-workflow -- --brand tribute-portfolio --dry-run`

→ `reports/brand-asset-registry-workflow.{md,json}` · see
[brand-asset-registry-workflow-v1.md](./brand-asset-registry-workflow-v1.md).

Proposes `Partner Intelligence - Brand Asset Registry` schema and stages Tribute approval plan. Schema apply gated behind `--apply --approve-brand-asset-registry-schema`.

## Does not do (v1)

- Download images or scrape OTA/booking images
- Overwrite logo, hero, image, or attachment fields
- Write Airtable or invent schema fields
- Set Company Validated / Company Validation Date
- Imply Marriott validated assets

## Future v2 (not in scope)

- Rendered Source Capture v1 for news.marriott.com
- Asset download + rights registry
- Airtable asset governance columns
- Staged Explorer hero/logo writer

## Related

- [brand-asset-registry-workflow-v1.md](./brand-asset-registry-workflow-v1.md)
- [brand-explorer-visual-slot-requirements-v1.md](./brand-explorer-visual-slot-requirements-v1.md) — slot-specific image requirements + registry audit
- [tribute-portfolio-package-pipeline-v1.md](./tribute-portfolio-package-pipeline-v1.md)
- [tribute-portfolio-brand-package-pilot.md](./tribute-portfolio-brand-package-pilot.md)
