# 001 — Data Source Inventory

READ-ONLY. No live research.

## Critical absences

- `data/ownership-v3/` — **ABSENT**
- `reports/ownership-v3/` — **ABSENT**
- `data/hotel-intelligence/ownership/` — **ABSENT (layout only)**
- `data/hotel-intelligence/contact-intelligence/` packages — **EMPTY**
- `data/inegi-denue/` — **ABSENT in this checkout**

## Authoritative path?

- **Hotel → Owner:** PARTIAL — `HOTEL_TO_OWNER` (4 hotels) + owner-portfolio fixtures. Not CALA-scale.
- **Owner → Person/Contact:** PARTIAL — declared via Contact Intelligence store + owner-target-resolver; runtime empty; KGPV showcase bypasses.
- **Demo fixtures still bypass:** YES (golden-demo API + static showcase).
- **Flat Owner Name as truth:** RISK — GTM/Census management fields must not be treated as PropCo evidence.
- **Contacts at hotel vs org level:** Showcase mixes; portfolio people are org-level but usually without channels.

## Sources

### HOTEL_TO_OWNER map
- Path: `lib/hotel-intelligence/ownership/owner-control/portfolio-store.js`
- Type: CANONICAL
- Authoritative: true
- R/W: READ (hardcoded map)
- Evidence: false | Currentness: false | Verification: false
- Explorer: true | Census: false | Research: true
- Status: CURRENT

### Owner portfolio fixtures
- Path: `fixtures/hotel-intelligence/owner-portfolio/`
- Type: DEMO / FIXTURE
- Authoritative: PARTIAL — golden demo authority only
- R/W: READ + local fixture write on materialize
- Evidence: true | Currentness: PARTIAL | Verification: false
- Explorer: true | Census: false | Research: true
- Status: CURRENT_DEMO

### Golden-demo ownership cohorts
- Path: `fixtures/golden-demo/*-ownership-cohort*.json / mexico-explorer-demo-cohort`
- Type: DEMO / FIXTURE
- Authoritative: false
- R/W: READ
- Evidence: true | Currentness: true | Verification: false
- Explorer: true | Census: false | Research: true
- Status: CURRENT_DEMO

### Deep research fixtures (4 controls)
- Path: `fixtures/golden-demo/*-deep-research-v1.json`
- Type: RESEARCH ARTIFACT / DEMO
- Authoritative: CONTROL only
- R/W: READ
- Evidence: true | Currentness: true | Verification: PARTIAL
- Explorer: true | Census: false | Research: true
- Status: CURRENT_DEMO

### KGPV contact showcase
- Path: `public/data/hotel-contact-intelligence/kgpv-recUNycnMwOVFX0hc.json`
- Type: DEMO / FIXTURE
- Authoritative: false
- R/W: READ (static)
- Evidence: true | Currentness: true | Verification: true
- Explorer: true | Census: false | Research: false
- Status: STAGING_SHOWCASE

### Contact Intelligence file store
- Path: `data/hotel-intelligence/contact-intelligence/`
- Type: CANONICAL (runtime empty)
- Authoritative: true
- R/W: READ/WRITE local files; Airtable writes=0
- Evidence: true | Currentness: true | Verification: true
- Explorer: true | Census: false | Research: true
- Status: CURRENT_EMPTY

### Local ownership repository
- Path: `data/hotel-intelligence/ownership/`
- Type: CANONICAL layout — MISSING on disk
- Authoritative: designed yes; populated no
- R/W: designed R/W
- Evidence: true | Currentness: true | Verification: true
- Explorer: false | Census: false | Research: true
- Status: CURRENT_EMPTY

### ownership-v3 stores
- Path: `data/ownership-v3/ and reports/ownership-v3/`
- Type: UNKNOWN / ABSENT
- Authoritative: false
- R/W: N/A
- Evidence: false | Currentness: false | Verification: false
- Explorer: false | Census: false | Research: false
- Status: ABSENT_IN_THIS_CHECKOUT
- Note: No ownership-v3 directory or references found on branch cursor/local-system-startup-recovery

### CI12 case plan
- Path: `lib/hotel-intelligence/contact-intelligence/cohort-12-v1.js`
- Type: STAGING
- Authoritative: false
- R/W: READ (code)
- Evidence: false | Currentness: false | Verification: false
- Explorer: false | Census: false | Research: true
- Status: STAGING

### GTM Owner Targets (parallel product)
- Path: `reports/gtm-owner-* / docs/gtm-owner-target-list.md`
- Type: LEGACY / PARALLEL PRODUCT
- Authoritative: GTM only — not HI ownership graph
- R/W: GTM Airtable
- Evidence: PARTIAL | Currentness: PARTIAL | Verification: PARTIAL
- Explorer: false | Census: false | Research: false
- Status: LEGACY_PARALLEL

### DENUE MX hotel indexes
- Path: `data/inegi-denue/`
- Type: STAGING / REGISTRY
- Authoritative: official MX registry when present
- R/W: READ
- Evidence: true | Currentness: PARTIAL | Verification: false
- Explorer: false | Census: false | Research: true
- Status: ABSENT_IN_THIS_CHECKOUT
- Note: Referenced in code; data/inegi-denue not present in this workspace snapshot
