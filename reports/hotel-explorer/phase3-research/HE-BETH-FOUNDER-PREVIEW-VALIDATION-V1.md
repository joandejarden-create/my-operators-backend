# Hotel Explorer Founder Preview — Bethesda validation

**Date:** 2026-11-19  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Server:** `http://127.0.0.1:8091`  
**Airtable writes:** none (GET-only preview)

## Bookmark URL

[http://127.0.0.1:8091/hotel-explorer-preview.html?hotelExplorer=recLuxvwwxID7U2B8&previewMode=proposed&hexTab=hotel](http://127.0.0.1:8091/hotel-explorer-preview.html?hotelExplorer=recLuxvwwxID7U2B8&previewMode=proposed&hexTab=hotel)

Live mode: append `&previewMode=live`.

## Modes

| Mode | Behavior |
|------|----------|
| **Live Airtable** | Canonical golden-demo ownership path only — persisted HI graph / HPC fields. |
| **Phase 3 Proposed** | Live base + staged dry-run overlay (`HE-BETH-PHASE3-HI-GRAPH-DRY-RUN-V1.json`). Banner + footer: **PROPOSED · NOT PERSISTED**. |

Opening/refreshing the preview never writes to Airtable (`X-Dealality-Write: forbidden`).

## Browser confirmation (2026-11-19)

| Check | Result |
|-------|--------|
| Bethesda loads automatically | PASS |
| Overview default | PASS |
| Ownership / Contacts / Organization / Relationships / Sources | PASS |
| Proposed visually distinguished | PASS (banner, mode chip, footer badge, PROPOSED labels) |
| Sources includes Phase 3 evidence (11 citations) | PASS (SDAT, MREC portfolio/contact/about, MHM, HWE, Ramsfield, WBJ, …) |
| GET-triggered writes | PASS — none |
| Radar HE path unchanged | PASS — preview is separate page; golden-demo ownership route untouched |
| Tabs 7–9 (Submarket / Area / Demand) still present | PASS (unchanged HE chrome) |

## Screenshots

Under `reports/hotel-explorer/phase3-research/founder-preview-screenshots/`:

- `he-founder-preview-overview.png`
- `he-founder-preview-ownership.png`
- `he-founder-preview-contacts.png`
- `he-founder-preview-organization.png`
- `he-founder-preview-relationships.png`
- `he-founder-preview-sources.png`

## Limitations

- Proposed overlay is Bethesda-scoped for Phase 3 staging; other hotels in Proposed mode fall back to live with a notice.
- UBO, qualified signatories, and personal emails/phones remain **UNRESOLVED**.
- Organization portfolio “Owned / Controlled” cohort counts may still show live-graph gaps until Airtable apply.
- Submarket / Area / Demand map embeds may be empty without Radar census context (expected for standalone preview).
- Apply remains **blocked** until founder authorization.

## PR status

Feature branch work in progress on `feat/he-beth-phase3-sources-evidence-v1`. **PR not opened** (founder’s prior STOP before apply / open when asked). No production deploy.
