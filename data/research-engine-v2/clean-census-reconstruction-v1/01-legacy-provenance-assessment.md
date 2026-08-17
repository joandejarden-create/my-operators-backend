# Legacy Provenance Assessment (Hotel Census)

## Context

`Hotel Census` on the Platform base is documented as **STR-backed production census** (~15k rows) in `docs/platform-reference/DATA_DICTIONARY.md`.

This is **not** a legal conclusion. It is a technical provenance assessment for reconstruction architecture.

## Classification model

- **Independent**
- **First-Party Validated**
- **Legacy-Origin — Unreconstructed**
- **Mixed Provenance**
- **Unknown Provenance**

## Field-origin map (typical patterns)

| Field family | Typical origin class | Notes |
|--------------|---------------------|-------|
| `name`, `Affiliation`, `Parent Company`, `status`, `rooms`, `country`, `city` | **Legacy-Origin — Unreconstructed** or **Mixed** | Core STR/client seed + later directory fill-blanks |
| `Market`, `Submarket` (STR-era) | **Legacy-Origin** | STR taxonomy imports; product now prefers Dealality corridors |
| `Dealality Market` / corridor `Submarket` | **Independent** / Dealality-derived | Post-seed geography work |
| `Website`, `Property ID`, `Brand Property Code` | **Mixed** → often later **Independent** | Brand-directory enrichment scripts (IHG/Hilton/Marriott/Choice) |
| `Amenities`, descriptions, lat/lng | **Mixed** / **Independent** where directory-sourced | Hilton/Marriott amenity syncs etc. |
| `STR Number`, performance/rate fields | **Legacy-Origin** | Must not substantiate independent production claims |
| Images | Separate rights class | Not STR facts; see image-rights design |
| Records added after initial seed (e.g. Hilton directory creates) | Often **Independent** discovery + enrichment | Still need provenance stamps |

## Pilot cohort reference snapshot

Legacy Indigo+Kimpton Mexico rows loaded **only after freeze** for comparison: **16** quarantined reference rows.

Production behavior is **unchanged** — no Airtable writes, no legacy deletion.
