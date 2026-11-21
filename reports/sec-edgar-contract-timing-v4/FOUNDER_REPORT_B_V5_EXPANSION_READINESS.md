# Founder Report B — V5 SEC Expansion Pilot (READINESS ONLY)

**Status:** `BLOCKED — AWAITING_V4_IC_MERGE`  
**Do not open V5 implementation PR until #499 is merged by Integration Controller.**

---

## Gate

V4 date integrity closeout shows **0** confirmed contractual expirations in the previously “VERIFIED” CMBS cohort after re-audit. Expanding research volume before that correction would have multiplied false decision windows.

V5 remains the approved next sequential workstream (`DISCOVER_EXPAND` design already on #499) but **implementation PR is deferred**.

## Planned pilot (post-IC merge) — not executed in this packet

| Item | Plan |
|---|---|
| Candidate exhibits | 50–100 EX-10 / amendment / IBR docs |
| Budget | ≤ $25 incremental metered |
| Preference | Free EDGAR + existing SEC-API subscription |
| Mix | HMA, franchise, lease, license, amendments, renewals, terminations — as available (no artificial balance) |
| Engine path | DISCOVER_EXPAND → EXHIBIT_HARVEST → EXTRACT → VERIFY → ENRICH_CONTRACT_TERMS → RECONCILE → PERSIST_RESEARCH → REPORT |
| Ledger | Document-grain coverage ledger V3 |
| Geo | Persist globally; HPC prioritize Americas/Europe |
| Publication | CUSTOMER_BLOCKED |

## Metrics reserved for V5 execution report (currently N/A)

- Issuers discovered beyond registry: **not run**
- SEC filings searched / exhibits retrieved: **not run**
- Agreement types / hotel identities / terms / renewals / amendments: **not run**
- Actual research spend: **$0.00** this packet

## Scaling recommendations (unchanged from expansion roadmap)

1. EX-10 HMA/franchise exhibit harvest (highest intel per dollar after integrity fix)  
2. CMBS deal graph with **header-validated** franchise expiration columns only  
3. Adaptive unregistered issuer queue  
4. Later-document executed renewal verification (affirmative evidence only)  
5. REIT schedule backfill + IBR  
6. Intl non-SEC adapters  

Roadmap SoT: `reports/sec-edgar-discovery-expansion/SEC_DISCOVERY_EXPANSION_ROADMAP.md`
