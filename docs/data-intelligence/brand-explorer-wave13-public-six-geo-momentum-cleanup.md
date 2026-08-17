# Brand Explorer — Wave 13 Public Six Geo + Momentum Cleanup

Patches Geographic Footprint (≥3 region cards) and Recent Momentum (structured date + URL cards) for the six Active Wave 13 public brands.

## Scope

- In: mama-shelter, mercure, ibis, novotel, pullman, fairmont-hotels-and-resorts
- Out: SO/, House of Originals, Morgans Originals, Radisson Collection, protected 39 content

## Failures addressed

1. `geographic_footprint` — only `footprint.region.cala` filled → ≥3 source-supported regions
2. `recent_momentum` — missing structured date lines + linked announcement URLs

## Implementation notes

- Stage: `public-six-geo-momentum-cleanup`
- Packages: `lib/partner-intelligence/brand-explorer-wave13-public-six-geo-momentum-packages.js`
- Hide legacy momentum with **External Display Status = Do Not Display** (Active:false alone does not persist on legacy rows)
- Keep CALA-first Sort Order (do not date-sort Pipeline/Directory ahead of CALA)
- Openings with Accor ALL hotel-code URLs: strip Body URLs rather than hiding cards (preserves property image uniqueness)

## Forbidden writes

Brand Status, release fields, images, CV / Source Library / Registry, broad rewrites, SO/.

## Commands

```bash
npm run brand-explorer-wave13-factory -- --stage public-six-geo-momentum-cleanup --dry-run
npm run brand-explorer-wave13-factory -- --stage public-six-geo-momentum-cleanup --apply \
  --approve-wave13-public-six-geo-momentum-cleanup \
  --confirm-six-public-brand-scope \
  --confirm-target-brands-only \
  --confirm-so-held-and-untouched \
  --confirm-house-of-originals-excluded \
  --confirm-no-morgans-originals-writes \
  --confirm-no-radisson-collection-changes \
  --confirm-no-protected-39-content-writes \
  --confirm-targeted-geo-and-momentum-fixes-only \
  --confirm-no-brand-status-changes \
  --confirm-no-release-field-writes \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-image-writes \
  --confirm-no-broad-rewrites \
  --confirm-no-adr \
  --confirm-no-revpar \
  --confirm-no-fee-stack \
  --confirm-no-raw-urls \
  --confirm-recent-momentum-structured \
  --confirm-geo-footprint-source-supported
```

## Ready statement

`wave13_public_six_geo_momentum_clean_ready_for_45_or_so_decision`

Do **not** freeze a 45 baseline in this task. SO/ remains Under Review / held.
