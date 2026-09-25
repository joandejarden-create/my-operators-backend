# W Rome Canonical Census Stewardship V1 — Founder Report

**Date:** 2026-09-25  
**Branch:** `deploy/gdi-pe-v1-7-customer-closure`  
**HPC base:** `AIRTABLE_BASE_ID_ALT` = `appCCUsuGsE1ifoLk` (table `tbl9aY5ijiuIzzWam`)  
**Note:** GDI/ADP intelligence base remains `appa2cE7FTRmIbB32`. HPC production SoT is platform ALT (not legacy MVP).

## O. FINAL VERDICT

**W ROME CENSUS STEWARDSHIP PASSES — RESUME CROSS-MARKET REPLICATION**

---

## A. DUPLICATE CHECK

| | |
|--|--|
| EXISTING EXACT MATCH | **NO** |
| POSSIBLE MATCHES | **0** (full HPC scan ~19.4k) |
| DECISION | **NO_EXISTING_CANONICAL_MATCH** |

---

## B. SOURCE RESEARCH

| | |
|--|--|
| SOURCES CHECKED | 6+ |
| FIRST-PARTY | Marriott overview (EN/CN) · rooms page · W Hotels brand page |
| AUTHORITATIVE SECONDARY | Cvent venue page (rooms corroboration) |
| LANGUAGES | en, zh (Marriott CN), it address context |
| NETWORK COST | **~$0** paid research (Webhound not required; Mapbox geocode only) |

---

## C. CANONICAL IDENTITY

| | |
|--|--|
| HOTEL | W Rome |
| BRAND | W Hotels |
| PROPERTY CODE | **ROMWV** (verified in Marriott URL path) |
| ADDRESS | 26/36 Via Liguria |
| CITY | Rome |
| REGION | UNKNOWN (not on first-party address line) |
| POSTAL | 00187 |
| COUNTRY | Italy |
| COUNTRY CODE | IT (derived) |
| LAT | 41.906211 |
| LON | 12.488489 |
| ROOMS | **148** |
| OFFICIAL URL | https://www.marriott.com/en-us/hotels/romwv-w-rome/overview/ |
| PHONE | +39 06-894121 |
| STATUS | Open (live bookable) |
| IDENTITY CONFIDENCE | **HIGH** |

---

## D. FIELD EVIDENCE

| Field | Value | Source | Authority | Status |
|-------|-------|--------|-----------|--------|
| Name | W Rome | marriott.com ROMWV | FIRST_PARTY | VERIFIED_FIRST_PARTY |
| Brand | W Hotels | w-hotels.marriott.com | FIRST_PARTY | VERIFIED_FIRST_PARTY |
| Code | ROMWV | URL `/romwv-w-rome/` | FIRST_PARTY | VERIFIED_FIRST_PARTY |
| Address | 26/36 Via Liguria | marriott rooms page | FIRST_PARTY | VERIFIED_FIRST_PARTY |
| City/Postal/Country | Rome / 00187 / Italy | marriott rooms page | FIRST_PARTY | VERIFIED_FIRST_PARTY |
| Rooms | 148 | marriott.com.cn overview | FIRST_PARTY | VERIFIED_FIRST_PARTY |
| Rooms corroboration | 148 | Cvent | SECONDARY | VERIFIED_CORROBORATED |
| Lat/Lon | 41.906211 / 12.488489 | Mapbox rooftop of validated address | DERIVED | VERIFIED_CORROBORATED |
| Phone | +39 06-894121 | marriott rooms page | FIRST_PARTY | VERIFIED_FIRST_PARTY |
| Region (Lazio) | — | — | — | UNKNOWN |

---

## E. CONFLICTS

| | |
|--|--|
| MATERIAL | **0** blocking |
| RESOLVED | 1 (rooms 148 vs weak 148–162 tertiary → first-party 148) |
| UNRESOLVED | 0 |

---

## F. HPC WRITE

| | |
|--|--|
| CREATED | **YES** |
| HPC RECORD ID | **`rece0or38cxo3Fymb`** |
| FIELDS WRITTEN | 38 allowlisted |
| FIELDS LEFT UNKNOWN | State/Region · Sub-Continent (no Europe option in schema) · owner/AM · opening date |
| READBACK MATCH | **PASS** (0 mismatches) |
| WRONG BASE | **0** |

---

## G. PROVISIONAL BIND

| | |
|--|--|
| FROM | `gdi_hotel_w_rome` |
| TO | `rece0or38cxo3Fymb` |
| STATUS | **PASS** |
| DUPLICATED FITS | **0** |
| DUPLICATED TARGETS | **0** |

Generic fix: onboard seed resolves provisional/ADP aliases → canonical HPC id before Fit/Target ID generation.

---

## H. CANONICAL RECREATE

| | |
|--|--|
| PROVISIONAL FITS | 7 |
| CANONICAL FITS | 7 |
| FIT OVERLAP | **100%** |
| PROVISIONAL TARGETS | 14 |
| CANONICAL TARGETS | 14 |
| TARGET OVERLAP | **100%** |
| MANUAL W INJECTION | **NO** |

---

## I. GDI APPLY GATE

| | |
|--|--|
| BEFORE | `census_missing_blocking_apply` |
| AFTER | **CLEARED** (`censusStatus=RESOLVED`, `applyBlocked=false`) |
| CANONICAL APPLY READY | **YES** |
| APPLY EXECUTED | **YES** — 7 Fits + 14 Targets to canonical hotel id |

---

## J. ADP CENSUS

| | |
|--|--|
| CENSUS LINK | **PASS** (`adp_w_rome` → `rece0or38cxo3Fymb`) |
| ADP IDENTITY | `adp_w_rome` |
| STATUS | **ADP_CENSUS_READY** |
| BASELINE | Not run (this cycle identity-only) |

---

## K. JEV

| | |
|--|--|
| CALLS | 0 this cycle (routing not required; first-party resolved identity) |
| CANONICAL FIELD DECISIONS BY JEV | **0** |
| APPLY | **OFF** |

---

## L. DATA INTEGRITY

| | |
|--|--|
| WRONG-BASE WRITES | **0** |
| CROSS-HOTEL LINKS | **0** |
| DUPLICATE HOTEL RECORDS | **0** |
| PROVISIONAL DURABLE REFERENCES LEFT | alias/redirect only (not durable Fit/Target hotelId) |

---

## M. GENERALIZATION

| | |
|--|--|
| W ROME PROD HARDCODES | **NO** |
| ITALY PROD HARDCODES | **NO** |
| ROME PROD HARDCODES | **NO** |
| GENERIC IMPROVEMENTS | Postal Code on insert allowlist · provisional→canonical hotelId resolution in onboard seed · Europe Continent without inventing Sub-Continent |

---

## N. DECISION

1. Hidden under another HPC identity? **NO**  
2. Authoritative evidence? **YES** (Marriott first-party + Cvent/Mapbox)  
3. Remaining unknown? Region/Lazio · Sub-Continent · owner · opening date  
4. Physical identity unambiguous? **YES**  
5. Canonical HPC created safely? **YES** `rece0or38cxo3Fymb`  
6. Provisional rebound without duplication? **YES**  
7. Seed recreate 7/14? **YES · 100% overlap**  
8. GDI apply unblocked? **YES** (applied)  
9. ADP census linkage unblocked? **YES · ADP_CENSUS_READY**  
10. W/Italy/Rome production hardcode? **NO**  
11. Jev routing value? **N/A** (not needed)  
12. Can Cross-Market Replication V1 resume? **YES**

---

## PERSISTENCE / GENERALIZATION

**CODE / CONFIG:**
- `scripts/w-rome-census-stewardship-*.mjs` (schema/dup, apply, bind)
- `scripts/test-w-rome-census-stewardship-v1.mjs`
- `lib/research-engine-v2/census-autopilot-source-discovery.js` (Postal Code allowlist)
- `lib/group-demand-intelligence/research-coverage/onboard-hotel-research-graph.js` (canonical resolve)
- `config/group-demand-intelligence/hotels/rece0or38cxo3Fymb.json` + provisional redirect
- `fixtures/hotel-census/adp-gdi-hotel-alias-map-v1.json`
- `fixtures/ai-demand-positioning/census-links-v1.json` + w-rome profile

**DATA RECORDS CREATED:** HPC `rece0or38cxo3Fymb` · GDI Fits/Targets for that hotel  

**W ROME PROD HARDCODES:** NO · **ITALY PROD HARDCODES:** NO · **JEV PROD BEHAVIOR:** NO  

**FINAL SHA:** *(after commit)* · **PUSH:** *(after push)*

---

## STOP

Resume Cross-Market Replication V1 live weekly / ADP stewardship from canonical hotel **`rece0or38cxo3Fymb`**.
