# Multi-hotel reconciliation

**Live Airtable GDO filter field:** `hotelId`  
**FS source:** `data/group-demand-intelligence/hotels/{hotelId}/opportunities.json`  
**Publications:** pointer files under `lib/group-demand-intelligence/bundled-publication-packages/{hotelId}/`  
**Evidence:** `_live_multi_hotel.json`, `EVIDENCE_REGISTER.json`

## Summary table

| Hotel | Hotel ID | FS ops | Airtable GDO | Research Targets | Generator Fit | GDI Need Periods | STAGED | CURRENT |
|-------|----------|-------:|-------------:|-----------------:|--------------:|-----------------:|--------|---------|
| Bethesda Marriott | `recLuxvwwxID7U2B8` | 86 | 86 | 53 | 20 | 9 | yes | yes |
| Sheraton Guadalajara Expo | `recCAGqkERSXgiku4` | 43 | **0** | 0 | 0 | **8** (provisional) | yes* | — |
| Wyndham San Diego Bayside | `recnPB3Q02NO8Ma1o` | 82 | 82 | 0† | 0† | 0 | — | — |
| Castillo Hotel Son Vida | `rec82D9zpB8fede1I` | 20 | 20 | 0† | 0† | 0 | — | — |
| YOTEL Geneva Lake | `recrPQcZg7SFARRb2` | 42 | **108** | 14 | 7 | 0 | yes | yes |
| Radisson Blu Toronto Downtown | `recH9h3McgMNFQEla` | 66 | **36** | 0† | 0† | 0 | yes | yes |
| Waterstone Resort & Marina | `recgMYovrrZDJMqzX` | 29 | 36 | 22 | 11 | 4 (test mix) | — | — |

\*Publication pointer presence from local package dir scan; opportunity counts inside pointers often null in this scan.  
†`Hotel ID` filter returned 0 — targets/fit may use alternate field naming or be absent for that hotel.

## Hotel notes

### Bethesda Marriott
- FS 86 = Airtable 86; identity overlap `inBoth=86`, onlyAT=0, onlyFS=0.
- Prior audit 86 GDO still accurate.
- Campaign `gdi_e2e_14seg_mwladic1_ff9898`: 165 raw / 20 processed / **0** Airtable writes — research remains FS/ledger until authorized apply.
- Need periods include HOTEL_REPORTED Active windows — treat as production need-date inputs.

### Sheraton Guadalajara Expo
- Research inventory lives in FS (43); **no GDO rows** in Airtable.
- Need periods exist as **provisional Test Scenario** only — not hotel-confirmed; no GDO join expected from discovery yet.
- Do not invent GDOs from provisional periods alone.

### Wyndham San Diego Bayside / Castillo Son Vida
- FS and GDO counts match (82/82, 20/20).
- Thin Research Target / Fit footprint via `Hotel ID` filter.

### YOTEL Geneva Lake
- **Major drift:** Airtable GDO 108 vs FS 42 vs published package lineage present.
- Do **not** assume higher Airtable count = more customer-approved opportunities.
- Founder decision required: which store is research SoR for YOTEL before Full E2E writes.

### Radisson Blu Toronto Downtown
- FS 66 vs Airtable 36 — FS ahead of Airtable (or different subsets).
- STAGED/CURRENT packages exist locally.
- Persistence must upsert by `opportunityId`, not replace FS research history from publication.

### Waterstone
- Airtable GDO 36 vs FS 29 — Airtable slightly ahead.
- Synthetic commercial profile + test need periods present — isolate from confirmed commercial claims.

## Bethesda seven dry-run operations (revalidated)

Original plan for processed-20: **DG×3 + GDO×3 + PE venue×1** (16 deferred audit-only).

| Proposed write | Against actual table purpose | Recommendation |
|----------------|------------------------------|----------------|
| Demand Generators ×3 | Org SoR — correct layer | **KEEP** if orgs are credible / retainGlobally |
| Group Demand Opportunities ×3 | Hotel-qualified opportunity SoR — correct | **KEEP only if** hotel-qualified/watchlist; else DEFER |
| Private Event Venues ×1 | Venue SoR — correct for PE/preferred lodging | **KEEP** if venue segment |
| Demand Programs / Signals | Not in the 7 | Do not invent from this batch |
| Research Targets | Not in the 7 | Optional later for monitoring — not forced |
| Need periods / GDI commercial | Not in the 7 | **Must remain out** |
| Research Runs summary | Not in the 7 | Optional run metadata after auth |

**Acceptable to reduce the 7** after identity collision checks against existing 86 Bethesda GDOs / 95 DGs (idempotent upsert may become UNCHANGED/DUPLICATE_LINKED).
