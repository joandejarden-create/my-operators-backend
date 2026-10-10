# FOUNDER REPORT — GDI International Discovery V2

Generated: 2026-10-07T18:05:16.253Z

## Verdict
International yield gap is **structural public-data + intermediary process**, not missing Bases.  
V2 adds **Demand Controller–first / historical-process / account-first** routes that resolve **who controls lodging** and **best route in** without lowering Ready/Watch.

**Finalized Ready yield did not inflate.** Commercial actionability (controllers, buyer paths, pursuit-eligible contacts) improved.

## Architecture
- DEMAND_CONTROLLER_MODEL_IMPLEMENTED: **YES**
- PARTICIPANT_FIRST_IMPLEMENTED: **YES**
- DEMAND_CONTROLLER_FIRST_IMPLEMENTED: **YES**
- ACCOUNT_FIRST_IMPLEMENTED: **YES**
- HISTORICAL_PROCESS_FIRST_IMPLEMENTED: **YES**
- HOTEL_HISTORY_FIRST_IMPLEMENTED: **YES**
- ALL_PATHS_CONVERGE_TO_SAME_PACKET: **YES**
- MARKET_DISCOVERY_PROFILE_IMPLEMENTED: **YES**
- PATH_SELECTION_ENGINE_IMPLEMENTED: **YES**
- NEXT_BLOCKER_ENGINE_IMPLEMENTED: **YES**
- EQUIVALENT_EVIDENCE_POLICY_IMPLEMENTED: **YES**
- PURSUIT_HOTEL_SUPPLIED_EVIDENCE_LOOP_IMPLEMENTED: **YES**
- INTERMEDIARY_GRAPH_IMPLEMENTED: **YES**
- READY_THRESHOLD_CHANGED: **NO**
- WATCH_THRESHOLD_CHANGED: **NO**
- APIFY_USED: **NO**

## Pilot snapshots

### AC Coruña
| Metric | Value |
|--------|------|
| OLD PATH READY | 0 |
| V2 READY | 0 |
| OLD PATH WATCH | 2 |
| V2 WATCH | 2 |
| NEW CONTROLLERS | 3 |
| NEW NAMED ACCOUNTS | 1 |
| NEW TRAVELING ENTITIES | 1 |
| NEW BUYER PATHS | 3 |
| NEW LODGING EVIDENCE | 1 |
| NEW COMPLETE_STRONG | 0 |
| NEW COMPLETE_PLAUSIBLE | 2 |
| TOP NEW DISCOVERY SPINE | DEMAND_CONTROLLER_FIRST |
| TOP REMAINING BLOCKER | HOTEL_LODGING_EVIDENCE |

### Radisson Santo Domingo
| Metric | Value |
|--------|------|
| OLD PATH READY | 0 |
| V2 READY | 0 |
| OLD PATH WATCH | 3 |
| V2 WATCH | 3 |
| NEW CONTROLLERS | 6 |
| NEW NAMED ACCOUNTS | 0 |
| NEW TRAVELING ENTITIES | 0 |
| NEW BUYER PATHS | 6 |
| NEW LODGING EVIDENCE | 4 |
| NEW COMPLETE_STRONG | 0 |
| NEW COMPLETE_PLAUSIBLE | 3 |
| TOP NEW DISCOVERY SPINE | DEMAND_CONTROLLER_FIRST |
| TOP REMAINING BLOCKER | TARGET_HOTEL_FIT |

### Westin Grand München
| Metric | Value |
|--------|------|
| OLD PATH READY | 0 |
| V2 READY | 0 |
| OLD PATH WATCH | 0 |
| V2 WATCH | 0 |
| NEW CONTROLLERS | 2 |
| NEW NAMED ACCOUNTS | 1 |
| NEW TRAVELING ENTITIES | 1 |
| NEW BUYER PATHS | 1 |
| NEW LODGING EVIDENCE | 1 |
| NEW COMPLETE_STRONG | 0 |
| NEW COMPLETE_PLAUSIBLE | 1 |
| TOP NEW DISCOVERY SPINE | DEMAND_CONTROLLER_FIRST |
| TOP REMAINING BLOCKER | HOTEL_LODGING_EVIDENCE |

## Quality / safety
| Check | Result |
|-------|--------|
| READY THRESHOLD CHANGED | NO |
| WATCH THRESHOLD CHANGED | NO |
| US READY WEAKENED | NO |
| HISTORICAL AS CURRENT | NO |
| GENERIC ORGANIZER → ACCOUNT | NO |
| GENERIC DMC WITHOUT CAMPAIGN | NO |
| SPECULATIVE LODGING | NO |
| HOTEL_SUPPLIED PROVENANCE | YES |
| MULTI-PATH DEDUPE | YES |
| APIFY | NO |

## Final answers
- **Biggest root cause of US vs Intl yield gap:** Public participant/housing list availability + intermediary-controlled unpublished selection (PUBLIC_DATA_CEILING), not missing Bases.  
- **Spine with most value:** DEMAND_CONTROLLER_FIRST  
- **Evidence route with most value:** Official housing / secretariat / PCO accommodation + multilingual functional contact paths  
- **Market improved most (actionability):** Radisson Santo Domingo (most controllers + lodging-authority evidence)  
- **Finalized opportunity yield improved without lowering standards?** **YES** for controller/buyer-path/pursuit-eligible utility; **NO Ready inflation** (correct).  
- **Top remaining limitation:** Current-cycle hotel-list / room-block publication still required for Ready; controllers unlock outreach, not auto-Ready.  
- **FINAL VERDICT:** International Discovery V2 is the right architecture — same truth, more routes. Ship spines + market profiles; keep participant-first; pursue OUTREACH_NOW via resolved controllers while monitors wait for lists.
