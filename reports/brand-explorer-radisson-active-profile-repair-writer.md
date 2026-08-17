# Brand Explorer Radisson Active Profile Repair Writer v28E

- Generated: 2026-07-10T06:30:51.032Z
- Brand: **Radisson by Choice** (`recywbx1YQSTCPqW1`)
- v28E exists: **yes**
- Mode: **dry-run**
- Final QA before: **55** (blocked)
- Visual defects before: **1** (critical 1, high 0, titleOnlyOrThin 0)
- Pending facts: **3**
- Airtable modified: **no**
- Company Validated untouched: **yes**
- Expected Final QA after apply: **~90** (blocked)
- Expected active-profile ready: **no**
- Separate fact/governance apply needed: **yes**

## Blocker diagnosis
- Create valueOwners.scenario.1–4 presentation rows with owner-facing bodies
- Add materials.gallery.1–6 prototype captions (keep existing images)
- Rephrase footprint.region.cala and Riviera Panama case study to avoid visual-audit Radisson Blu phrase
- Normalize writer-batch Sort Order on seven editorial/standards rows
- Keep three pending facts in stewardship—no auto-approval in this package

## Pending facts
- `rec1F8M6YcWa2Lc6g` **be.footprint.geoIntro** → needs_source_confirmation (Geo intro extract is a dated scale fragment only—needs a fuller approved footprint narrative before Explorer surfacing.)
- `recBJrPntYbQ5X0e0` **be.overview.typicalUseCase** → needs_founder_review (Typical use case value is a generic fragment (“travelers worldwide”)—not owner-facing copy.)
- `reckqeeACfDkkv9A4` **be.overview.whyValue** → needs_founder_review (Why-value extract is a placeholder label (“value proposition”)—requires rewritten owner bullets.)

## Rows to create
Count: **0**

## Rows to update
Count: **0**

## Exact apply command
```bash
npm run brand-explorer-radisson-active-profile-repair-writer -- --brand radisson --apply --approve-brand-explorer-v28E-radisson-active-profile-repair --founder-reviewed-radisson-copy-repair --confirm-no-company-validation-claim
```