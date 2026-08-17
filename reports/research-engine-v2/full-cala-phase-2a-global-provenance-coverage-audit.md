# Phase 2A — Global Census Provenance + Coverage Audit

**AUDIT_STATUS:** `production_census_full_cala_phase_2a_global_provenance_coverage_audit_complete_with_gaps`  
**Production writes:** **false** (read-only)  
**Table:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)  
**Generated:** 2026-08-09T19:35:49.213Z

## Executive return

| Field | Value |
| --- | ---: |
| PRODUCTION_CENSUS_COUNT | 5956 |
| Shells expected / found | 3395 / 3395 |
| PROVENANCE_ANOMALIES | 0 |
| PROTECTED_FIELD_ANOMALIES | 0 |
| HBX_CODE_DUPLICATES | 0 |
| HIGH_CONFIDENCE_DUPLICATE_GROUPS | 0 |
| REVIEW_DUPLICATE_GROUPS | 219 |
| TOTAL_HELD_CANDIDATES | 9633 |
| BRAZIL_HELD_COUNT | 4842 |
| MEXICO_HELD_COUNT | 1276 |
| COLOMBIA_HELD_COUNT | 299 |
| CURRENTLY_DISCOVERED_POTENTIAL_UNIVERSE | 15589 |
| ESTIMATED_UNDISCOVERED_GAP (excl. weak HOLDs) | 9042 |
| NEXT_RECOMMENDED_ACTION | PROCEED_SOURCE_GAP_DISCOVERY |
| FOUNDER_DECISION_REQUIRED | NO |

## Gap reconciliation

```
CURRENT PRODUCTION CENSUS                         5956
+ IDENTIFIED HELD UNIQUE CANDIDATES               9633
= CURRENTLY DISCOVERED POTENTIAL UNIVERSE         15589

QUALIFIED PRODUCTION ONLY                         5956
WEAK/REVIEW HELD (approx)                         9631

ESTIMATED CALA TARGET (~)                         15000
− PRODUCTION (qualified)                          5956
≈ GAP IF WEAK HOLDS ARE NOT COUNTED AS COVERED    9042
```

Universe = production Census + unique HOLD ledger candidates. Many HOLDs are weak Cvent-only and are NOT production-ready hotels. 15k is aspirational. Prefer ESTIMATED_GAP_EXCLUDING_WEAK_HOLDS when judging true source-coverage shortfall vs weak-identity backlog.

### Discovered but needs enrichment
- Production shells pending enrichment: **3395**
- Held weak identity: **9633**

### Shell source mix
- HBX-only: **1908**
- Cvent + HBX: **640**
- Cvent-only: **847**

### HBX linkage
- Records with HBX Hotel Code: **3016**
- Unique codes: **3016**
- Duplicate codes: **0**

## Production by country (top)

| Country | Count |
| --- | ---: |
| Mexico | 2181 |
| Colombia | 967 |
| Costa Rica | 748 |
| Dominican Republic | 654 |
| Brazil | 494 |
| Panama | 325 |
| Argentina | 129 |
| Jamaica | 78 |
| Chile | 65 |
| Peru | 57 |

## Held candidates (top)

| Country | Held |
| --- | ---: |
| Brazil | 4842 |
| Mexico | 1276 |
| Argentina | 788 |
| Chile | 311 |
| Peru | 303 |
| Colombia | 299 |
| Costa Rica | 202 |
| Jamaica | 170 |
| Belize | 118 |
| Saint Martin | 98 |

## Recommendation

**PROCEED_SOURCE_GAP_DISCOVERY**

Large held weak pools (Brazil dominant) plus a ~9k shortfall vs the aspirational 15k target when weak HOLDs are not counted as covered hotels. Shell integrity is clean; the binding constraint is discovery/source coverage, not provenance contamination.

Do not restart shell insertion. Do not weaken the SAFE+HBX gate.

## Secondary flags

- Duplicate review needed: **true** (219 review-level groups; 0 high-confidence HBX dups)
- Provenance remediation needed: **false**
- Source coverage gaps: **true**
- Enrichment backlog: **true**
- Weak identity HOLD backlog: **true**

## Minor lineage notes

- 2 shell records missing Shell Insert Batch ID / Country Batch metadata (Costa Rica Batch 1 likely; country totals still reconcile to 641).
- Shell Insert Batch ID is intentionally shared: `full-cala-15k-census-shell-insert-v1`.
- Shell country totals match expected Phase 1 inserts exactly (DR 416, CR 641, PA 280, CO 793, MX 1265).

## Safety

- No production Census writes
- No Brand Explorer / Brand Setup / VIC / old Census writes
- Shell identity gate not weakened
- Phase 1 shell insertion not restarted

## Command

```bash
npm run census:full-cala-phase-2a-provenance-coverage-audit
```
