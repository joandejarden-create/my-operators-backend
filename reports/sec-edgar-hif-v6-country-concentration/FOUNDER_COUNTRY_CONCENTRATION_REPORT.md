# HIF V6 — Five-Country Concentration Report (Geography-Gated)

**As of:** 2026-11-21  
**Publication:** CUSTOMER_BLOCKED · **Spend:** $0  
**Integration:** #503/#506/#507/#508 MERGED · #510–#515 OPEN · this wave PRs follow

## Phase 0 baseline
- `origin/main`: `beb22be64`
- Registered workspaces: `ws-hif-v6-track-a`, `ws-hif-v6-track-b`
- Dirty HE/ADP/GDI/BE: preserved (not touched)

## Progress toward 1,000
| Measure | Count |
|--------|------:|
| Prior inventory | 194 |
| Net-new **geography-supported** prospects | 1 |
| Geography-unverified HOLD (not country-credited) | 38 |
| Quarantined earlier (brand-only/noise) | 63 |
| **Toward 1,000** | **195** |
| Remaining | 805 |

## Country ambitions (geography-supported only)

| Country | Ambition min | Geo-supported prospects | % of min |
|---------|-------------:|------------------------:|---------:|
| United States | 300 | 0 | 0% |
| Mexico | 150 | 0 | 0% |
| Brazil | 150 | 0 | 0% |
| United Kingdom | 150 | 1 | 0.7% |
| Canada | 100 | 0 | 0% |

## Key findings
- **US dated CMBS**: no net-new extractable franchise/management expirations this wave (prior inventory exhaustion on current extractor formats).
- **MX/BR/GB/CA**: native regulator document chains still thin; SEC keyword harvest produced many geography-unverified rows (held, not counted toward country ambitions).
- **Track B**: HMLR ULPD authenticated (supporting); CH API key absent; CVM/BMV/SEDAR+ portals retrieved; no invented verified contract dates.
- Ownership enrichment remains deferred.

## Next wave recommendations
1. US — broaden CMBS HTML schedule extractor; harvest new annex formats.
2. MX — BMV/Emisnet issuer PDFs with named Mexican hotels + agreement clauses.
3. BR — CVM filing document download + condo-hotel vs HMA classification.
4. GB — Companies House document API once key present; UK transaction RNS.
5. CA — SEDAR+ exhibit/MD&A retrieval for hospitality REITs.
