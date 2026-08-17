# Live Golden-Quality Content Rebuild

In-place content remediation for live `active_profile_ready` brands that pass structural release gates but fail founder visual quality.

## Targets

- `hotel-indigo`
- `mgallery-collection`
- `small-luxury-hotels-of-the-world`

## Protected (never modified)

- `everhome-suites`
- `kimpton`
- `radisson-individuals-by-choice`
- `design-hotels`
- `tribute-portfolio` (benchmark / reference only)

## What it does

Rewrites / creates Presentation and Brand Basics owner-facing copy to Kimpton / Tribute / Radisson / Design Hotels quality depth:

- Deep scenario cards (distinct images from openings/gallery inventory)
- Why Value Is Strongest
- Proof points
- Featured application + case summary
- Differentiators / Best At / portfolio / positioning / development model
- Geographic footprint + growth editorial
- Deeper property examples (fix SLH CALA mislabels)

## Commands

```bash
npm run brand-explorer-live-golden-quality-rebuild -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --dry-run
```

### Apply

```bash
npm run brand-explorer-live-golden-quality-rebuild -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --apply \
  --approve-live-golden-quality-rebuild \
  --confirm-keep-active-profile-ready \
  --confirm-no-company-validation-changes \
  --confirm-no-active-release-field-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-protected-brands-unchanged \
  --confirm-no-unsupported-metrics \
  --confirm-no-empty-bullets \
  --confirm-no-blank-cards \
  --confirm-no-stub-chips \
  --confirm-no-duplicate-scenario-images \
  --confirm-brand-specific-copy \
  --confirm-benchmark-quality-met
```

## Quality test

```bash
npm run test:brand-explorer-golden-content-quality -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world
```

## Guardrails

- Brands remain `active_profile_ready`
- No Company Validated / release-field writes
- No Source Library / Registry approval changes
- No protected-brand writes
- No invented hotel/room/pipeline/fee metrics
- Image reassignment only from already-materialized inventory

## Change impact

**High** (live Presentation + Brand Basics content).

Rollback: restore previous Presentation/Basics values from Airtable history or pre-apply report JSON before/after snapshots.
