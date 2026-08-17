# v42A-R1 — Design Hotels Founder Minor Cleanup

Resolves Design Hotels v42 recommendation **`approve_after_minor_cleanup`** with affiliation/curation polish only.

Stock `brand-explorer-v42a-founder-minor-cleanup` **refuses** Design Hotels (Everhome/Kimpton only). Use this R1 path instead.

## Dry-run

```bash
npm run brand-explorer-v42a-r1-design-hotels-minor-cleanup -- --brand design-hotels --dry-run
```

## Apply (only if dry-run projects `approve_for_active_release`)

```bash
npm run brand-explorer-v42a-r1-design-hotels-minor-cleanup -- --brand design-hotels --apply \
  --approve-brand-explorer-v42A-R1-design-hotels-minor-cleanup \
  --confirm-no-company-validation-claim \
  --confirm-no-active-profile-approval \
  --confirm-no-source-library-changes \
  --confirm-no-registry-changes \
  --confirm-no-image-field-changes \
  --confirm-external-profile-remains-locked \
  --confirm-released-golden-brands-unchanged \
  --confirm-design-hotels-only \
  --confirm-cala-examples-preserved
```

## Scope

| Do | Do not |
|----|--------|
| Tone polish on Title / Body / Case Summary | Unlock / active approval / Company Validated |
| Affiliation / curation language | Franchise / FDD / LOI / fee-stack language |
| Keep Wake BioHotel, Condesa DF, Carlota | Hide or rewrite CALA examples |
| Stub short-body expansions only | Generic filler / full-profile rewrite |
| Image QA spot-check in report | Image / Registry / Source Library writes |

## Projection target

`approve_for_active_release` (founder OK still required). Active release is **not** applied by v42A-R1 — use v43 later.

v42 founder packet may still show `approve_after_minor_cleanup` when intentional short label bodies remain as taste spot-checks. R1 projects `approve_for_active_release` when no safe Presentation polish patches remain and hard gates pass.

## Outputs

- `reports/brand-explorer-v42a-r1-design-hotels-minor-cleanup.json`
- `reports/brand-explorer-v42a-r1-design-hotels-minor-cleanup.md`

## Modules

- `lib/partner-intelligence/brand-explorer-v42a-r1-design-hotels-minor-cleanup.js`
- `scripts/brand-explorer-v42a-r1-design-hotels-minor-cleanup.mjs`
