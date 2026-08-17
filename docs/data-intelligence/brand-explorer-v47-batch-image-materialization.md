# v47 — Batch Image Materialization + Draft Readiness

Converts **v46 accepted visual asset packs** into Brand Explorer Presentation Image materialization plans for:

- `hotel-indigo`
- `mgallery-collection`
- `small-luxury-hotels-of-the-world`

Protects released golden brands: Everhome, Kimpton, Radisson Individuals, Design Hotels.

## Command

```bash
npm run brand-explorer-v47-batch-image-materialization -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --dry-run
```

### Apply (explicit gates required)

```bash
npm run brand-explorer-v47-batch-image-materialization -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --apply \
  --approve-brand-explorer-v47-batch-image-materialization \
  --confirm-no-company-validation-claim \
  --confirm-no-active-profile-approval \
  --confirm-no-source-library-changes \
  --confirm-no-released-brand-changes \
  --confirm-external-profiles-remain-locked \
  --confirm-official-source-images-only \
  --confirm-six-gallery-imageurls-projected \
  --confirm-three-property-example-imageurls-projected \
  --confirm-brand-only
```

## What it does

1. Confirms OS `image_remediation` / `draft_applied_with_defects` + external lock for targets
2. Protects v44 released baseline
3. Ingests v46 accepted gallery (6) / property (3) / scenario (3) candidates
4. Validates brand-specific image rules (Indigo Scene7 MARSHA paths, Accor ahstatic, SLH media)
5. Builds Presentation Image materialization plans (`materials.gallery.*`, `footprint.openings.*`, `overview.scenario.*`)
6. Projects live API readiness 6/6 + 3/3 + 3/3 after apply
7. Classifies `build_draft_ready` / `apply_draft_allowed` (projected) — **never unlocks**

## Allowed writes (apply only)

- Brand Explorer Presentation rows for visual slots
- Presentation `Image` attachments
- Brand Asset Registry **Candidate Only** rows (traceability)

## Forbidden

- Active-profile approval / release fields
- Company Validated / Company Validation Date
- Source Library changes
- Released brand content
- External unlock
- Raw URLs in owner-facing Body/Title

## Outputs

- `reports/brand-explorer-v47-batch-image-materialization.{json,md}`
- `reports/brand-explorer-v47-hotel-indigo-materialization.md`
- `reports/brand-explorer-v47-mgallery-materialization.md`
- `reports/brand-explorer-v47-slh-materialization.md`
- `reports/brand-explorer-v47-released-baseline-protection.md`

## Prerequisite

```bash
npm run brand-explorer-v46-image-remediation-batch -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --dry-run
```

## Change impact

**High** (Presentation Image writes on apply). Dry-run is **Medium** (read/plan only).

Rollback: revert Presentation Image patches / delete created openings rows for the three target brands only.
