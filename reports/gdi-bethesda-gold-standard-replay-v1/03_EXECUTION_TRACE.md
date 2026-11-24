# 3. Execution trace (this diagnostic)

| Stage | Executed? | Input | Evidence reuse | Notes |
|-------|-----------|-------|----------------|-------|
| Canonical dry-run entry | YES | hotel `recLuxvwwxID7U2B8`, mode dry-run, $0 | YES | See `CANONICAL_DRY_RUN_TRACE.json` |
| Discovery paid Exa | NO | n/a | n/a | Not authorized |
| Deep research paid | NO | n/a | n/a | Not authorized |
| WHO/ALT/HOW live providers | NO | n/a | Prior research store reused by hotel-wide inventory | Agent OFF |
| Hotel-wide reconciliation | YES | research ∪ STAGED ∪ CURRENT ∪ gold seed | YES | 86 canonical rows reviewed |
| FQO certification | YES | per inventory row | YES | Gold presentation shape fails evidence gate |
| STAGED candidate write | YES | PASS rows only | YES | `gdi_hotel_wide_fqo_…_36d7d473` · **20** opps · pointer **not** activated |
| Surfe / Findymail / Airtable | NO | | | |
| localhost:8080 | NOT TOUCHED | | | Preview on **8091** only |

Hotel-wide dispositions: PASS 28 · HOLD 20 · PREVIOUSLY_APPROVED_REVIEW 38 · REJECT 0 · HISTORICAL 0  
(PASS→package shrink: 28 → 20 — defect R5)
