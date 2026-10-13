# Property dropdown — root cause

## Canonical format (implementation SoT)
`listGdiSelectableHotels()` in `lib/group-demand-intelligence/repository.js`:

```
optionLabel = `${hotelName} — ${locationLine}`
locationLine = [city, state || country].filter(Boolean).join(", ")
```

Examples after fix:
- Bethesda Marriott — Bethesda, Maryland
- W Rome — Rome, Italy
- YOTEL Geneva Lake — Founex, Switzerland
- AC Hotel A Coruña — A Coruña, Galicia

## W Rome inconsistency (before)
Label rendered as **`W Rome — Rome`** (city only).

### Root cause
1. **Shared formatter gap** — `locationLine` used only `city` + `state`. International hotel configs store **`country`** at config top-level (`Italy`, `Switzerland`) while `state` is null.
2. Profile `identity.country` is often an ISO-2 code (`IT`) — not used as the human-readable second segment.
3. **Not** a wrong hotel name, alias, or separator. Hotel identity unchanged.
4. **Visual risk** — `.gdi-page .filter-select` overrode shell `select.filter-select` padding (8px/12px vs 0/2rem/14px) which can clip closed-state glyphs; shared CSS parity applied (not W-Rome-specific).

## Fix
- Prefer `config.country` (human-readable) when `state` is absent.
- Also read top-level `config.city`.
- Align `.gdi-page select.filter-select` metrics with shell filter-select.

## Non-changes
- Hotel identity / census ID unchanged
- No W-Rome-only hardcode
- ADP / share tokens unchanged
