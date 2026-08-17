# Lifestyle / Independent Collection Priority Pack v35

- Generated: 2026-07-14T12:17:37.910Z
- Mode: **dry-run** (no Airtable writes)

## Naming ambiguity
- **Tribute Collection** → use **Marriott Tribute Portfolio** (`tribute-portfolio`). Do not create a duplicate brand record.

## Strategic ranking
1. **Tribute Portfolio** (`tribute-portfolio`) — combined 71, speed 63, fit 77, asset pack `config_required_but_assets_present`
2. **Design Hotels** (`design-hotels`) — combined 58, speed 5, fit 93, asset pack `early_stage_needs_source_capture`
3. **Small Luxury Hotels of the World** (`small-luxury-hotels-of-the-world`) — combined 58, speed 5, fit 93, asset pack `early_stage_needs_source_capture`
4. **Autograph Collection** (`autograph-collection`) — combined 48, speed 5, fit 77, asset pack `early_stage_needs_source_capture`
5. **Vignette Collection** (`vignette-collection`) — combined 46, speed 5, fit 74, asset pack `early_stage_needs_source_capture`
6. **MGallery Collection** (`mgallery-collection`) — combined 46, speed 5, fit 74, asset pack `early_stage_needs_source_capture`
7. **Handwritten Collection** (`handwritten-collection`) — combined 46, speed 5, fit 74, asset pack `early_stage_needs_source_capture`

## Recommended activation sequence
- **First 2 (live owner/project):** design-hotels, small-luxury-hotels-of-the-world
- **Note:** Strategic priority remains Design Hotels + SLH for independent/lifestyle owner relevance, but both need Source Library capture, presentation buildout, and v35 factory config registration before v34D staged apply.
- **Fastest technical prepare:** tribute-portfolio
- **Next 3 (benchmarks):** tribute-portfolio, autograph-collection, vignette-collection
- **Defer:** autograph-collection, vignette-collection, mgallery-collection, handwritten-collection

## Design Hotels
- Slug: `design-hotels` · Record: `rec02zPClpWUTCyXM`
- Parent: Marriott International, Inc.
- Company Validated: **no**
- Final QA: blocked (30/100, 23 defects)
- Complete Build: blocked · readyForActiveProfile: false
- Sources: 0 approved / 0 total
- Presentation: 0 visible rows · gallery API imageUrl 0/6 · property examples w/ image 0/3
- Registry: 0 rows (0 approved)
- Factory preflight: **FAIL** (5 blockers) · config registered: no
- Asset pack feasibility: **early_stage_needs_source_capture**
- Lens: Design Hotels fits independent, design-led, culturally distinctive hotels seeking curated global distribution without a standardized franchise prototype.
- Risks:
  - Zero approved sources in last complete-build snapshot
  - Critical visual defects halted prior complete build
  - Factory config not registered — cannot run v34D asset-pack yet
  - Franchise-language contamination risk if copy uses Marriott flag framing

## Small Luxury Hotels of the World
- Slug: `small-luxury-hotels-of-the-world` · Record: `recjjSnY2opb8P4DG`
- Parent: Small Luxury Hotels of the World
- Company Validated: **no**
- Final QA: blocked (30/100, 20 defects)
- Complete Build: blocked · readyForActiveProfile: false
- Sources: 0 approved / 0 total
- Presentation: 0 visible rows · gallery API imageUrl 0/6 · property examples w/ image 0/3
- Registry: 0 rows (0 approved)
- Factory preflight: **FAIL** (5 blockers) · config registered: no
- Asset pack feasibility: **early_stage_needs_source_capture**
- Lens: SLH fits independent luxury/boutique hotels seeking global luxury distribution and consortium credibility without a chain flag.
- Risks:
  - Expansion backlog wave 8 — highest source/image governance complexity
  - Likely limited Source Library coverage
  - Factory config not registered
  - Independent consortium — avoid parent-franchise language

## Autograph Collection
- Slug: `autograph-collection` · Record: `recEJCTDj1zrsjPM6`
- Parent: Marriott International, Inc.
- Company Validated: **no**
- Final QA: blocked (26/100, 24 defects)
- Complete Build: blocked · readyForActiveProfile: false
- Sources: 0 approved / 0 total
- Presentation: 0 visible rows · gallery API imageUrl 0/6 · property examples w/ image 0/3
- Registry: 0 rows (0 approved)
- Factory preflight: **FAIL** (5 blockers) · config registered: no
- Asset pack feasibility: **early_stage_needs_source_capture**

## Tribute Portfolio
- Slug: `tribute-portfolio` · Record: `recCvV0PuZOi8c3hC`
- Parent: Marriott International, Inc.
- Company Validated: **no**
- Final QA: ready (95/100, 5 defects)
- Complete Build: ready · readyForActiveProfile: false
- Sources: 9 approved / 9 total
- Presentation: 163 visible rows · gallery API imageUrl 6/6 · property examples w/ image 5/3
- Registry: 27 rows (0 approved)
- Factory preflight: **FAIL** (15 blockers) · config registered: no
- Asset pack feasibility: **config_required_but_assets_present**

## Vignette Collection
- Slug: `vignette-collection` · Record: `recDwzv86TWnz2gGB`
- Parent: InterContinental Hotels Group
- Company Validated: **no**
- Final QA: blocked (30/100, 20 defects)
- Complete Build: blocked · readyForActiveProfile: false
- Sources: 0 approved / 0 total
- Presentation: 0 visible rows · gallery API imageUrl 0/6 · property examples w/ image 0/3
- Registry: 0 rows (0 approved)
- Factory preflight: **FAIL** (5 blockers) · config registered: no
- Asset pack feasibility: **early_stage_needs_source_capture**

## MGallery Collection
- Slug: `mgallery-collection` · Record: `recrWCD1LMqu864oU`
- Parent: AccorHotels
- Company Validated: **no**
- Final QA: blocked (30/100, 20 defects)
- Complete Build: blocked · readyForActiveProfile: false
- Sources: 0 approved / 0 total
- Presentation: 0 visible rows · gallery API imageUrl 0/6 · property examples w/ image 0/3
- Registry: 0 rows (0 approved)
- Factory preflight: **FAIL** (5 blockers) · config registered: no
- Asset pack feasibility: **early_stage_needs_source_capture**

## Handwritten Collection
- Slug: `handwritten-collection` · Record: `rec7hTXwMRC81EPqz`
- Parent: AccorHotels
- Company Validated: **no**
- Final QA: blocked (30/100, 20 defects)
- Complete Build: blocked · readyForActiveProfile: false
- Sources: 0 approved / 0 total
- Presentation: 0 visible rows · gallery API imageUrl 0/6 · property examples w/ image 0/3
- Registry: 0 rows (0 approved)
- Factory preflight: **FAIL** (5 blockers) · config registered: no
- Asset pack feasibility: **early_stage_needs_source_capture**

## Factory next steps (top 2)
### design-hotels
```bash
npm run brand-explorer-active-profile-preflight -- --brand design-hotels --dry-run
npm run brand-explorer-active-profile-asset-pack -- --brand design-hotels --dry-run
```

### small-luxury-hotels-of-the-world
```bash
npm run brand-explorer-active-profile-preflight -- --brand small-luxury-hotels-of-the-world --dry-run
npm run brand-explorer-active-profile-asset-pack -- --brand small-luxury-hotels-of-the-world --dry-run
```

