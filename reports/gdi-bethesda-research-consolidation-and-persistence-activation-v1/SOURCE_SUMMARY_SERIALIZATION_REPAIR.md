# Source summary serialization repair

## Defect

`buildResearchInventoryFields` used `text(s.url || s.sourceUrl || s)`. When a source entry was a structured object without `url`/`sourceUrl`, `text(object)` became `[object Object]`.

## Fix

Shared helpers:
- `formatResearchSourceEvidencePart`
- `buildResearchSourceSummary`

Prefer URL → label/title/name → pathRel. Never coerce objects via `String(obj)`. Reject leftover `[object Object]` placeholders when recoverable sources exist.

## Regeneration

- Authorization: `gdi-bethesda-research-inventory-27-creates-v1`
- Creates: **27**
- Composite keys unchanged: **true**
- Baseline 659 field builds with zero `[object Object]`: **true**
- Production writes: **not applied**
