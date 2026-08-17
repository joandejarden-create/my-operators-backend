# Production Census — Active Brand Setup Scope

**Status:** ready (read-only Brand Setup)  
**Writes Brand Setup / Brand Explorer:** never  

## Control list

Source of truth:

- Brand Basics `Brand Status` ∈ {Active, Live}
- `lib/brand-status-active.js`
- `lib/partner-intelligence/brand-explorer-active-universe.js`
- Frozen baseline via `loadActiveBrandUniverse()` for offline/plan

Excluded unless Active/Live:

- Under Review / Inactive / held
- Four Points Flex by Sheraton
- Radisson Collection, House of Originals, Morgans Originals (held list)

## Match to Census

1. Exact slug / brand match  
2. Approved aliases  
3. Approved soft-brand mappings only  
4. No fuzzy auto-match  
5. Unmatched active brands → `source_discovery_needed`  
6. Unmatched Census with brand string → steward candidates  

## Autopilot flag

```bash
--scope active-brand-setup
```

Optional `--parent-company` further filters the active control list.
