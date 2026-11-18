# Phase 2A — Thirty-three failure root-cause register

**Reproduced:** 2026-11-18 — `npm run test:adp-customer-surface-contract` → exit 1, `failureCount: 33`  
**Source report:** `reports/ai-demand-positioning/adp-customer-surface-contract-v1-latest.json`  
**Machine register:** `03_THIRTY_THREE_FAILURE_ROOT_CAUSE_REGISTER.json`  
**Prior Phase 1.1:** 36 → 33 after software fixes; **Phase 2A delta: 33 → 33 (unchanged)**

## Before / after

| Metric | Phase 1.1 close | Phase 2A re-run |
|--------|----------------:|----------------:|
| Customer-surface failures | 33 | **33** |
| Software defects fixed in Phase 1.1 | Scan / hybrid / share / print / BPP affiliation maps / displacement harness | Retained |
| New software regressions found | — | **None** |

## Classification of the 33

| Gate | Count | Class | Code fixable now? | Requires |
|------|------:|-------|-------------------|----------|
| `TERRITORY_SUBJECT_RATES` | 5 | Historical certification / locked Bethesda expectations | **No** (constant flip forbidden) | Founder pin to certified period **or** authorized re-cert |
| `BPP` | 24 | Missing measurement / peer freeze / smoke UNKNOWN | **No** (READY fail-closed) | Governed BPP measurement + peer freeze; exclude synthetic from commercial claims |
| `ALL_EVIDENCE_LINKS_NONEMPTY` | 1 | Empty / incomplete evidence packs | **No** without restoring packs | Durable runtimes or self-contained packs |
| `ALL_EVIDENCE_READABLE` | 1 | Evidence readability / pack integrity | Same | Same |
| `ALL_EVIDENCE_LINKS_CLIENT_READY` | 1 | Client-ready evidence packaging | Same | Same |
| `DISPLACEMENT_COUNT_EVIDENCE_EXACT_PARITY` | 1 | Harness + synthetic fixture drift / missing DSS | Partial harness already hardened | Ignore smoke synthetic; validate published DSS on real hotels |

### Class buckets (founder view)

1. **Actual software/integration defects** — *none remaining among the 33 after Phase 1.1.*  
2. **Missing authoritative evidence** — evidence-link trio (EMPTY / readable / client-ready).  
3. **Historical certification gaps** — Bethesda territory rates (5).  
4. **Report/data consistency** — displacement exact parity (1; largely synthetic noise).  
5. **Missing production persistence / measurement** — BPP 24 (`MEASUREMENT_NOT_ATTEMPTED` / `PEER_SET_NOT_FROZEN`).  
6. **Publication/authorization** — surface FAIL blocks production *claim*; does not by itself authorize writes.

## Doctrine

- Do not weaken gates.  
- Do not invent evidence packs.  
- Do not mark historical certification complete without records.  
- Do not grandfather Bethesda rates.
