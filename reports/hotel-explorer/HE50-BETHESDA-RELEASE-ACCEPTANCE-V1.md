# HE 5.0 — Bethesda Release Acceptance V1

**Status:** `BETHESDA_RELEASE_ACCEPTANCE = PASS`  
**FULL_HE_E2E_VALIDATED:** `NO` (eight-hotel / full program still required)  
**PR:** [#628](https://github.com/joandejarden-create/my-operators-backend/pull/628) (includes Contact Intelligence V3)  
**Serve checkout:** `C:\Dev\dealality\_verify_pr627_main`  
**HEAD at test:** see `HE50-RELEASE-ACCEPTANCE-GATES-V1.json`  
**URL:** `http://localhost:8080/app#/opportunity-radar` → Bethesda Marriott HE popup

## Browser checks (authenticated Radar → Hotel Explorer)

| Check | Result |
|---|---|
| Active tabs | **11** (Guest Sentiment absent) |
| Year Built | **1979** |
| Last Renovated | **2015** |
| Guestroom mix | **171 / 236 / 8** |
| Meeting space | **18,719 sq. ft.** · **27** rooms · Grand Ballroom **4,592** |
| Ownership | **Proposed** Mullen · **Marbeth** PropCo |
| Org & Portfolio rooms | **407** |
| Contact Intelligence | **V3 directory table** · 5 Bethesda contacts · correct Marriott/Mullen orgs · no Recommended First |
| Unified theme / contact assets | `hotel-contact-intelligence.js?v=11` served from PR head |

## Persistence classification

| Attribute | Canonical Airtable (HPC/HI) | Staged / product overlay |
|---|---|---|
| Rooms 407 | YES — HPC | — |
| Year Built 1979 / Reno 2015 | YES — HPC mapped DTO | — |
| Meeting / guestroom mix | YES — HPC physical-product adapter (`sourceOfTruth=HOTEL_PROPERTY_CENSUS`) | — |
| Economic ownership **Proposed** | HI may show Validated | **Product classification overlay** (shared VM) |
| Contacts + org mapping | YES — HI Org Contacts / Organizations | Surfe session-only |

## Gates

See `reports/hotel-explorer/HE50-RELEASE-ACCEPTANCE-GATES-V1.json`.

`HE_AUTHENTICATED_LIVE_RENDERING = YES` (Bethesda only).  
`FULL_HE_E2E_VALIDATED = NO`.
