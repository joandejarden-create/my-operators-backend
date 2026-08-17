# Wave 13 SO/ — Steward Data Decision

Generated: 2026-07-28T06:49:52.102Z
Brand: **SO/** (`so-hotels-and-resorts`) · Basics: `recTJdPlr4mDs9app`

## Decision summary

| Field area | Decision | Rationale |
|------------|----------|-----------|
| `snapshot.*` scale / launch / typical keys | **Leave cleanly unavailable** | Wave 13 source pack has positioning + property pages only — no verified keys, launch year, or regional scale inventory. |
| `snapshot.parent_company` | Keep live Basics value | Already populated (`AccorHotels`) — do not invent Ennismore JV naming into Basics without founder governance. |
| `snapshot.brand_website` | **Optional source-supported fill** | `https://so-hotels.com/en/` — only if apply includes steward confirmation flags. Default: leave as-is / cleanly unavailable. |
| `footprint.primary_regions` | **Do not invent chip list** | No official primary-region inventory. Instead create ≥3 International Reference region cards so Geographic Footprint is not broken. |
| `footprint.region.cala` | **Cleanly unavailable copy** | `calaAvailability: none_found` in Stage 3 pack. |
| Europe / Maldives / Americas diligence regions | **Fill from source** | Paris + Berlin Das Stue + Maldives property pages (International Reference). |

## Source basis

- Accor Group SO/ brand page
- so-hotels.com (Paris, Maldives)
- Accor ALL Berlin Das Stue (`B1Y6`)
- Accor Brandbook positioning (identity only — not room counts)

## Explicit non-fills

Do **not** invent: typical keys, launch year, CALA operating inventory, fee/ADR/RevPAR, loyalty economics, or unsupported geographic density claims.

## Approval posture for Basics website write

Requires apply flags:
- `--confirm-steward-fields-source-supported-or-left-cleanly-unavailable`
- `--approve-so-basics-website-steward-write` (optional extra; without it, website is left untouched)

Default remediation apply leaves Basics steward fields **untouched**.

