# Brand Explorer Openings Display Parity Audit v31K

- Generated: 2026-07-10T21:30:41.077Z
- Section: **Openings / Examples / Properties** (`footprint.openings`)
- Left: **Radisson by Choice** (`radisson`)
- Right: **Radisson Individuals by Choice** (`radisson-individuals-by-choice`)
- v31K exists: **yes**
- Mode: **dry-run** (audit only)
- Company Validated untouched: **yes**
- Airtable modified: **no**

## 1. Summary comparison

| Metric | Radisson by Choice | Radisson Individuals |
|--------|-------------------|----------------------|
| Airtable rows | 4 | 8 |
| Quarantined | 0 | 8 |
| Visible in API | 4 | 0 |
| With image (API) | 4 | 0 |
| Complete labels | 4 | 8 |
| Internal-language hits | 0 | 41 |
| Frontend mode | property-example-card grid | propertyShell() × 3 empty shells |

## 2. API comparison

- Left blocks: **4**
- Right blocks: **0**

## 3. Frontend rendering

- Left renders images: **true**
- Right renders images: **false**
- Same component: **yes** (property-example-card)
- Parity gap: **yes**

## 4. Row detail — Radisson Individuals

### Radisson Individuals — Medellín, Colombia
- Record: `recM0XfO2UlkNBd5x`
- Quarantined: true · In API: false
- Image: Airtable false · API false
- Labels: title=true loc=true meta=true scenario=true teaser=true tags=true

### Radisson Individuals — Cartagena, Colombia
- Record: `recA57HKv0Zd2bGnx`
- Quarantined: true · In API: false
- Image: Airtable false · API false
- Labels: title=true loc=true meta=true scenario=true teaser=true tags=true
- Internal language: consumer_site_label

### Radisson Individuals — Panama City, Panama
- Record: `recFKCA1auFtGwwjY`
- Quarantined: true · In API: false
- Image: Airtable false · API false
- Labels: title=true loc=true meta=true scenario=true teaser=true tags=true

### Radisson Individual — Panama, Panama
- Record: `recLHEhgtaFWGjACc`
- Quarantined: true · In API: false
- Image: Airtable false · API false
- Labels: title=true loc=true meta=true scenario=true teaser=true tags=true
- Internal language: census_property_url, active_property_page, confirm_fees_fdd, fdd_label, gateway_cala_capture, listed_on_choicehotels, choice_affiliated_listed, census_url_extract

### Radisson Individual — Barranquilla, Colombia
- Record: `rec0uiWsD44ePqr6M`
- Quarantined: true · In API: false
- Image: Airtable false · API false
- Labels: title=true loc=true meta=true scenario=true teaser=true tags=true
- Internal language: census_property_url, active_property_page, confirm_fees_fdd, fdd_label, gateway_cala_capture, listed_on_choicehotels, choice_affiliated_listed, census_url_extract

### Radisson Individual — Bogota, Colombia
- Record: `recto7QMu58eMf5jV`
- Quarantined: true · In API: false
- Image: Airtable false · API false
- Labels: title=true loc=true meta=true scenario=true teaser=true tags=true
- Internal language: census_property_url, active_property_page, confirm_fees_fdd, fdd_label, gateway_cala_capture, listed_on_choicehotels, choice_affiliated_listed, census_url_extract

### Radisson Individual — Cali, Colombia
- Record: `rect0VNHSr1f5ImGx`
- Quarantined: true · In API: false
- Image: Airtable false · API false
- Labels: title=true loc=true meta=true scenario=true teaser=true tags=true
- Internal language: census_property_url, active_property_page, confirm_fees_fdd, fdd_label, gateway_cala_capture, listed_on_choicehotels, choice_affiliated_listed, census_url_extract

### Radisson Individual — Cucuta, Colombia
- Record: `recVtiPqVGo8gUtpO`
- Quarantined: true · In API: false
- Image: Airtable false · API false
- Labels: title=true loc=true meta=true scenario=true teaser=true tags=true
- Internal language: census_property_url, active_property_page, confirm_fees_fdd, fdd_label, gateway_cala_capture, listed_on_choicehotels, choice_affiliated_listed, census_url_extract

## 5. Root cause

- **data**: Radisson exposes 4 openings in API; Radisson Individuals exposes 0 — likely quarantine (Do Not Display) on all Individuals rows
- **data**: 8/8 Individuals openings rows quarantined — API filter excludes them before blocks[]
- **data**: Frontend uses identical property-example-card logic; Individuals shows empty shells because API blocks[] is empty — not a brand-specific render branch
- **data**: Individuals openings copy has 41 internal-language marker hit(s) — v31C quarantine trigger
- **api**: API image exposure differs: left 4 vs right 0
- **image_governance**: Radisson Individuals is expansion_backlog — stricter registry/image governance before active display

## 6. Recommended fix

- [P1] openings_rebuild_or_reactivate: Radisson Individuals has footprint.openings rows in Airtable but none in API blocks — run v31L openings rebuild (owner-facing copy + approved images + clear Do Not Display) before expecting parity with Radisson.
- [P1] repair_internal_language: Replace internal/census/source-capture labels in openings body and case-summary fields with owner-facing copy per brand-explorer-openings-ui-quarantine-governance.js proposeOwnerFacingOpeningsCopy pattern.
- [P3] no_frontend_patch_needed: Rendering difference is data-driven (empty shells vs real cards). Optional: hide openings section entirely when zero API blocks — uniform for all brands.
