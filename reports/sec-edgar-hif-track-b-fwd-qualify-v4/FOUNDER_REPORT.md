# Track B V4 — Forward-Looking Decision Windows & Track A Qualification

**As of:** 2026-11-23T10:01:21.431Z  
**Publication:** CUSTOMER_BLOCKED · **Cost:** $0  
**Airtable:** NO NEW APPLY (founder STOP)

## 1. Verified management / franchise / lease dates
| Asset | Type | Verified date fact | Precision |
|---|---|---|---|
| Westin Monterrey Valle | MANAGEMENT (Aimbridge) | Active as of 2026-09-30; expiry unknown | UNKNOWN |
| JW Marriott Monterrey Arboleda/Valle | MANAGEMENT (Aimbridge) | Active as of 2026-09-30; expiry unknown | UNKNOWN |
| Holiday Inn Monterrey Valle | FRANCHISE | Renewed FY2025; new end unknown; ops internalized 2025-09-01 | DAY (ops) |
| Courtyard GDL (ex-Wyndham Acueducto) | FRANCHISE | Conversion approved FY2025 | COMMITTED |
| Fairfield Villahermosa → InHouse | LEASE | Commenced 2026-07-01 | DAY |
| Cd. Carmen → InHouse | LEASE | Commenced 2026-04-01 | DAY |
| Yucatán Resort | LEASE target | Ops suspended 2026-04-30 | DAY |
| Westin Boston Seaport (DRH) | FRANCHISE | Expires 12/2026 | MONTH |
| Desert Ridge (RHP) | MANAGEMENT | Expires 2027 + 5× auto 10yr if thresholds | YEAR_ONLY |

## 2. Renewal / termination mechanics
- **FINN gov (xix):** at third-party HMA expiration → incorporate into internal ops (Servicios Hoteleros FINN).
- **FINN gov (xx):** early Aimbridge termination authorized only for **100%-owned** hotels (28-hotel buyout).
- Co-owned Westin/JW **excluded** from early buyout; residual Aimbridge HMAs continue.
- HMA notice / performance / change-of-control clauses for residuals: **not published**.
- Desert Ridge: automatic renewals if performance thresholds met.

## 3. Owner / operator decision authority
- FINN residuals: Technical Committee + co-owners; Aimbridge operates; brands separate.
- FIHO leases: FibraHotel controls lease conversions (executed for Villahermosa/Cd Carmen).
- DRH/RHP: US REIT owners control franchise/HMA decisions.

## 4. New potential decision windows
- Westin Monterrey Valle Aimbridge HMA (EMERGING / D) — expiry unknown; internalization-at-expiry constrains competing MCT.
- JW Marriott Monterrey Aimbridge HMA (EMERGING / D) — same + identity alias.
- Yucatán Resort lease lessee/brand (EMERGING / D) — may still be open if unsigned.
- DRH Westin Boston franchise 12/2026 (D).
- Desert Ridge 2027 (D, auto-renewal caveat).

## 5. Historical / committed excluded
HI Monterrey franchise renewal; GDL Courtyard conversion; Villahermosa & Cd Carmen InHouse leases; 28-hotel Aimbridge buyout; HI Puebla/Monterrey HMA internalizations; DRH Highgate Feb 2026 transitions.

## 6. HPC / Track C
See `TRACK_C_IDENTITY_HANDOFF.json` — Arboleda vs Valle; Acueducto vs Andares; Fairfield vs Fiesta Inn Villahermosa; Santander no-create.

## 7. Track A qualification
| Class | Count |
|---|---|
| HISTORICAL_COMPLETED | 0 |
| COMMITTED_NOT_OPEN | 4 |
| CONTRACT_WATCH | 1 |
| POTENTIAL_DECISION_WINDOW | 5 |
| **VERIFIED_OPEN_OPPORTUNITY** | **0** |

**No customer-facing Qualified Opportunities proposed.**

## 8. SEC extraction yield
No new bulk SEC this pack. Prior V4 on #560: DRH hotel schedules + SVC Crowne Plaza. Host/Sunstone/Ashford YE2025 diminishing returns → next EX-10 only.

## 9. Airtable dedup / readiness
CALA 9 findings / 8 contracts / 4 milestones already applied (re-apply = noops). Coverage-audit 54 + V4 DRH/SVC untouched. This pack `persistEligible=false`. **STOP before expanded writes.**

## 10. Research cost
**$0** (free IR PDFs + prior EDGAR caches).

## 11. PR / CI
- #556 MERGED @ `05b9d231`
- #560 OPEN (decision-window extraction) — append this research commit for IC
- No production deploy / no automation / no customer publication

## 12. Blockers / approvals required
1. Founder authorization for any new Airtable apply of `qual_tb_fwd_*` (or updates to CALA residual rows).
2. Track C resolve Arboleda/Valle and Acueducto/Andares before any customer signal.
3. Primary-source HMA expiry for residual Aimbridge hotels (still missing).
4. Yucatán lease counterparty disclosure.
5. IC merge of #560 after CI green.
