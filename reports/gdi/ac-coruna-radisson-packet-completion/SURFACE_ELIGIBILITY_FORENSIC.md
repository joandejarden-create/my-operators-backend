# SURFACE_ELIGIBILITY FORENSIC

## Verdict
**LEGITIMATE_EVIDENCE_BLOCKER** (primary) — not a surface-logic bug.

`isGdiCustomerOpportunityReady` fails `surface_eligibility` first when
`isCustomerSurfaceActiveEligible` → `classifyCustomerSurfaceOpportunity` returns `keepActive: false`.

## Field-by-field path
research → lead → packet → opportunity builder → FS/Airtable → readiness gate → surface filter → API → UI

## Disposition mix (pre-forensic Watch)

| Disposition | Meaning |
|-------------|---------|
| PAST_CLOSED | Event end date before 2026-10-07 — dominant AC/RAD Watch failure |
| DOWNGRADE_TO_MARKET_ENTITY / DEMAND_GENERATOR | No hotel thesis / lodging / team proof |
| INVALID_ENTITY | Faculty calendar placeholders / no addressable entity |
| DQ_OTHER | Year-only 2026 / unknown date without future evidence |
| KEEP_ACTIVE | Rare — needs lodging + team pillars |

## Bug classes checked

| Class | Result |
|-------|--------|
| LEGITIMATE_EVIDENCE_BLOCKER | **YES** — missing traveling entity + lodging thesis + many past dates |
| TRAVELING_ENTITY_FIELD_MISSING | YES on most Watch rows (0 proven before pass) |
| GROUP_MOTION_FIELD_MISSING | YES |
| BUYER_PATH_MAPPING_GAP | Partial — contact stamps persist when set; homepage alone still rejects Ready |
| HOTEL_MOTION_MAPPING_GAP | YES — empty lodgingEvidence → market_entity_without_hotel_opportunity_depth |
| FUTURE_DECISION_MAPPING_GAP | Partial — many rows had dates but past |
| STALE_FLAG | YES — large stale cohort incorrectly left as FUTURE_WATCH |
| WRONG_ENUM | NO |
| SURFACE_LOGIC_BUG | **NO** — runtime revalidation is authoritative (YOTEL P0.5 deadlock already fixed) |
| FIELD_PERSISTENCE_BUG | **NO** on this pass — stamps written to FS retain traveling/group/buyer/hotel/future fields |

## Root cause (final)
**Discovery bags filled with past cycles + wrong-destination events + organizer shells without lodging controllers.** Surface eligibility correctly blocked Ready. Packet completion forensics close stale/wrong-dest and enrich the few in-market futures (notably IAPS 2027 AC; SDQ MICE series intelligence RAD) without lowering Ready.
