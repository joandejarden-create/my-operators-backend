# State / Region pipeline

## Root cause
Geography resolver previously omitted State / Region; dry-run never `add()`d it.

## Fix
- `resolveStateRegion()` deterministic maps (Mexico entity/alias, DR province)
- `resolveDealalityGeography()` now emits `state_region`
- `classifyFieldWrites()` claim path includes State / Region when staging value exists

## Staging resolution on V3 cohort (recomputed)
- Resolved: **32/150** (21%)
- Unresolved: mostly Brazil postal-code cities / non-entity city labels

## Writer
AUTO_WRITE_SAFE path implemented. **Not written in this corrective coord run** (no State/Region in coord dry-run).
