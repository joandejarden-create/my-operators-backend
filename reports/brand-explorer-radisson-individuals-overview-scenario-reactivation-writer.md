# Brand Explorer Radisson Individuals Overview Scenario Reactivation v31H-R1

- Generated: 2026-07-10T22:13:15.270Z
- Brand: **Radisson Individuals by Choice**
- v31H-R1 exists: **yes**
- Mode: **apply**
- Company Validated untouched: **yes**
- Airtable modified: **yes**
- Images approved: **no**

## Overview scenario diagnosis

### overview.scenario.1 — Boutique Independent Conversion
- Record: `recpe1vIxIsaKq1XX`
- Quarantined: true · Image: true
- Owner-facing copy: yes
- Safe to reactivate: **yes**
- Hidden reason: External Display Status = Do Not Display — api/brand-library.js excludes row from brand.brandExplorer.blocks[]

### overview.scenario.2 — CALA Hand-Selected Growth
- Record: `recVkMzf3C2yekcj7`
- Quarantined: true · Image: true
- Owner-facing copy: yes
- Safe to reactivate: **yes**
- Hidden reason: External Display Status = Do Not Display — api/brand-library.js excludes row from brand.brandExplorer.blocks[]

### overview.scenario.3 — Preserve Uniqueness + Choice Scale
- Record: `recv6KKeedfO9Jk1E`
- Quarantined: true · Image: true
- Owner-facing copy: yes
- Safe to reactivate: **yes**
- Hidden reason: External Display Status = Do Not Display — api/brand-library.js excludes row from brand.brandExplorer.blocks[]

## Rows to reactivate

- `recpe1vIxIsaKq1XX` overview.scenario.1 — Boutique Independent Conversion
- `recVkMzf3C2yekcj7` overview.scenario.2 — CALA Hand-Selected Growth
- `recv6KKeedfO9Jk1E` overview.scenario.3 — Preserve Uniqueness + Choice Scale

## Registry plan

- Create: 3
- Update: 0

## Expected UI result

- After apply: 3 scenario blocks in API with images
- Three scenario-card--visual cards with title, body, and imageUrl — same atelier component as Radisson by Choice

## Expected active-profile result

- Scenarios become visible in Explorer draft after reactivation, but images remain Pending Review — active-profile evidence still requires founder registry approval (not done in v31H-R1).

## Apply command

```bash
npm run brand-explorer-radisson-individuals-overview-scenario-reactivation-writer -- --brand radisson-individuals-by-choice --apply --approve-brand-explorer-v31H-R1-overview-scenario-reactivation --founder-reviewed-radisson-individuals-overview-scenarios --confirm-no-image-approval --confirm-no-company-validation-claim
```
