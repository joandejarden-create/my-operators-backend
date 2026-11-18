# Bethesda visibility — root cause (Phase 1.3)

**Hotel:** Bethesda Marriott  
**Census id:** `recLuxvwwxID7U2B8`  
**ADP property:** `adp_bethesda_marriott`

## Root cause (confirmed)

Bethesda is a **Hotel Property Census** record, not a listable **Hotel Census** pin.

| Check | Hotel Census (`tblgj2qEwxjTcg6q0`) | Hotel Property Census (`tbl9aY5ijiuIzzWam`) |
|-------|-------------------------------------|---------------------------------------------|
| Schema | Legacy STR-style (`name`, `city`, `country`, `Affiliation`) | Production census (`Property Name`, `City`, `Country`, `Current Brand`) |
| `RECORD_ID()='recLux…'` list | **0 records** | **1 record** |
| GET by id | Returns payload (quirky cross-resolve) but not listable | Canonical home for Bethesda |
| Radar `/api/brand-presence` map pool | Built from Hotel Census list → **Bethesda absent** | Not used for map SoT |

Evidence probes:

- `phase-1.3/evidence/bethesda-two-tables.json`
- `phase-1.3/evidence/bethesda-schema-diff.json`
- `phase-1.3/evidence/bethesda-airtable-meta.json`

Public map response (`GET /api/brand-presence`): **15,692 hotels, Bethesda absent**.

## Why not “just remove a filter”?

There is **no visibility filter** hiding Bethesda inside Hotel Census. The record is simply not in that table’s list universe. Switching Radar SoT wholesale to Hotel Property Census would be a census-architecture change (out of Phase 1.3 scope / would risk CALA map regressions).

## Desired secure behavior

| Surface | Bethesda |
|---------|----------|
| Authorized internal Radar (admin / local Dealality admin / DEV_AUTH_BYPASS admin) | Included via authenticated merge from HPC |
| Public `/api/brand-presence` | Excluded |
| Customer share packs | Excluded (no Bethesda in `hotel-explorer-share.html`) |
| `internalPreview=1` query alone | **Does not authorize** |

## Implementation

1. `lib/hotel-intelligence/radar-internal-preview-hotels-v1.js` — HPC → brand-presence DTO; auth helpers; merge without duplicates  
2. `GET /api/brand-presence/internal-preview-hotels` — `memberstackAuth` + `requireDealalityUser` + `requireAdminAccess`  
3. Client merge in `brand-presence-mapping.js` (skipped in share mode; fail-closed on 401/403)  
4. `requireDealalityUser` local `DEV_AUTH_BYPASS` admin fallback for founder localhost parity  
5. Phase 1.2 identity aliases in `api/brand-presence.js` retained; by-id can also fall back to HPC formatting when Hotel Census identity is unknown

Validated locally: map pool becomes **15,693** with `added: [recLuxvwwxID7U2B8]`; HE opens **Bethesda Marriott**.
