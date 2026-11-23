# Track B V4 — SEC/CMBS Decision-Window Extraction

**As of:** 2026-11-23T09:57:05.793Z  
**Publication:** CUSTOMER_BLOCKED · **Cost:** $0  
**PR #556:** MERGED @ `05b9d231` · **PR #549:** MERGED (same commit / superseded content)

## 1. PR #556 / #549 reconciliation
- #556 MERGED into `main` @ `05b9d231` (CI green: gitleaks + ADP/GDI gates).
- #549 (Historical Intelligence V3 / Sotherly+Mexico) commits are ancestors of #556; GitHub shows the same merge commit — content is on `main` via the #556 merge lineage. No separate outstanding #549 delta.
- Coverage-audit **54** Airtable creates remain idempotent; **not** re-applied for this wave.

## 2. Coverage ledger (after dedup framing)
| Stage | Count |
|---|---|
| DISCOVERED | 11,940 |
| EXTRACTED (checkpoint) | ~50 |
| Exhibits enumerated / processed | 47 / 28 |
| Cache files | 72 |
| This wave CONTRACT_VERIFIED (persistable) | 12 |
| DECISION_WINDOW_ASSESSED | 16 |
| CUSTOMER_ELIGIBLE | 0 |

## 3. Ranked extraction queue
See `RANKED_EXTRACTION_QUEUE.json`. DRH processed first; PEB/CLDT skipped (prior); Ashford/Host deferred to EX-10; Sunstone deprioritized.

## 4. Documents processed & yield
7 document scopes examined (5 new 10-Ks + Desert Ridge analysis + prior-wave skip). **2** yielded verified hotel-level schedules (DRH, SVC). Persistable facts: **12**.

## 5. New verified hotel-level contract facts
- DRH franchise: Westin Boston Seaport **12/2026**; Courtyard Denver Downtown **10/2027**
- DRH HMA near-term term ends (at-will): Hotel Clio 5/2026; The Gwen 6/2026; Bourbon Orleans / Courtyard Denver / Henderson Park Inn 7/2026; Westin Boston 12/2026
- DRH historical Highgate transitions **2026-02-03** (3 NY hotels)
- SVC Crowne Plaza IHG HMA extended to **2027-01-31** (495 keys; city unknown)

## 6. Forward-looking dates & renewals
- Desert Ridge: year **2027** + five automatic 10-year renewals if performance thresholds met
- SVC Hyatt 17: **2031-03-31**; Radisson 7: **2031-07-31** (unnamed hotels)
- DRH at-will + renewal options as tabled

## 7. Historical vs actionable
| Class | Count |
|---|---|
| A Historical | 3 |
| B Active no near-term | 3 |
| C Forward watch | 2 |
| D Potential window | 8 |
| E Verified open | **0** |

## 8. Potential decision windows (D)
DRH near-term franchise/HMA at-will rows + Desert Ridge 2027 (auto-renewal caveat). Require Track A owner-intent validation.

## 9. Verified open opportunities (E)
**None.** Expiry/at-will/auto-renewal alone are insufficient.

## 10. HPC / ownership gaps
See `TRACK_C_HANDOFF.json`. SVC Crowne Plaza city/name unresolved. All DRH rows PENDING_HPC_SEARCH.

## 11. Airtable persistence

| Step | Creates | No-ops | Failures | Notes |
|---|---|---|---|---|
| Core APPLY | 33 | 3 | 4 | Inv/findings/contracts OK; milestones failed on invalid select options |
| Milestone repair APPLY | 4 | 36 | 0 | 4 day-exact milestones (COMMENCEMENT/TERMINATION/OTHER) |
| Idempotency re-APPLY | 0 | **40** | 0 | Confirmed |

Unique `tb_v4_` IDs. **Coverage-audit 54 not re-applied.** CUSTOMER_BLOCKED. Graph evidence/relationships staged pending HPC match.

## 12. Cost & diminishing returns
Cost **$0**. Host/Sunstone/Ashford YE2025 10-Ks show diminishing returns without EX-10 targeting. Next wave = Ashford/Host/PEB EX-10 named agreements, not mass 8-K.

## 13. Recommended next research wave
1. Ashford EX-10 hotel HMA/franchise exhibits (not 1,120 8-Ks)
2. Host termination/franchise exhibits with hotel names
3. SVC Hyatt/Radisson named hotel schedule (if any exhibit/supplement)
4. Track C resolve Crowne Plaza 495 + DRH HPC matches
5. Track A qualify D-class DRH windows only with owner-process evidence

## 14. CI / IC status
#556/#549 merged. This wave ships as focused PR on `feat/hif-v6-track-b-decision-window-v4`. No automation / no customer publication / no production deploy.
