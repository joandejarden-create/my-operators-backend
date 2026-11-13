# GDI Market Scope — Hotel Actionability V1

Generated from authorized Bethesda inventory (dynamic counts; not hardcoded).

## Counts

| Scope | Count | Meaning |
|-------|------:|---------|
| **All** | **67** | Deduplicated authorized sales inventory |
| **Hotel** | **61** | Commercially actionable for Bethesda Marriott |
| **Competitive** | **10** | Recovered CMI ledger (6 verified / 3 needs verification / 1 historical) |

Dual membership: **4** (AFCEA historical future-cycle + 3 Needs Verification research-continuation rows).  
Competitive-only: **6** (verified competitor bookings without independent target-hotel demand).  
All never double-counts dual rows.

## Hotel exclusions (6)

Confirmed competitor-booked business with no open overflow / open sourcing / future-cycle thesis:

| ID | Competitor | Reason |
|----|------------|--------|
| `gdi_opp_acvnu_renal_week_20270412` | The Bethesdan | Confirmed block; target sourcing not established |
| `gdi_opp_nado_ddaa_washcon_2027_overflow` | Crystal Gateway Marriott | Confirmed host; overflow not established |
| `gdi_opp_nado_ddaa_washcon_2028` | Crystal Gateway Marriott | Confirmed block; target sourcing not established |
| `gdi_opp_ecs_251_2027_overflow` | Marriott Marquis | Confirmed venue; overflow not established |
| `gdi_s4912_6c8a62552bfa` | Bethesda North Marriott | ICON venue confirmed; overflow not established |
| `gdi_s4912_732496c2f8aa` | Bethesda North Marriott | CASSS host confirmed; overflow not established |

## Competitive inventory note

PR #226 recovery report cited **11** (6 / 4 / 1). Authorized CMI ledger currently has **10** entries (6 / 3 / 1). Counts are computed dynamically from ledger + actionability rules — no hardcoded 67 / 61 / 10 / 11.

Unverified competitive claims remain under investigation (Needs Verification) without being treated as confirmed bookings. Soft competitor-name associations stay on Hotel only and do not inflate Competitive.

## Guards preserved

- Published authorization boundary: **19**
- Fourteen demand segments
- CMI confidence labels + Competitive Status filter
- CSV integrity / research history unchanged
