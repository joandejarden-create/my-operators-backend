# HIF V6 — Name-Free SEC Discovery Scale-Up

**As of:** 2026-11-21  
**Publication:** CUSTOMER_BLOCKED  
**Research cost:** $0  

## Phase 0 — Integration

| PR | Status | Merge SHA |
|----|--------|-----------|
| #519 Source Intelligence Audit | **MERGED** | `882fe5be5ae46703d40ceeabed7bcf7d85581158` |
| #516 Country Track A | **MERGED** (prior) | — |
| #517 Country Track B | OPEN — reconcile separately (unique intl probes) | — |

Dirty product worktrees preserved. Integration checkout untouched (remote merge).

## Engine strengthening

- Combined CMBS column  
  `Expiration/Termination of Related License/ Franchise Agreement, Operating Agreement or Management Agreement`  
  now classified as **`COMBINED_LICENSE_FRANCHISE_OPERATING_OR_MANAGEMENT`**
- **Not** auto-labeled as franchise
- Loan maturity isolated
- Reserve/Trigger prose excluded from extractable scoring
- Plaintext + HTML paths updated

## Scale-up run (paginated EFTS)

| Metric | Result |
|--------|-------:|
| Docs attempted | 35 |
| Docs OK | 35 |
| Extractable schedule docs | 16 |
| Net-new hotel×date events | **154** |
| Active dated (toward 1,000 incremental) | **118** |
| Historical preserved | 36 |
| Agreement type unresolved (combined column) | 154 |
| Resolved franchise expirations | 0 |
| Resolved management expirations | 0 |
| Quarantined noisy names | 6 |
| Cost | $0 |

Yield stayed healthy through 35 docs (trailing ~4.4 net-new/doc) — source family **not exhausted**.

Best query family: `"Expiration/Termination of Related License"` on **424B2**.

## Canonical inventory (event-key dedupe)

**Do not use 192 + 89 = 281.** Authoritative reconcile:

| Classification | Count |
|----------------|------:|
| Active dated | 226 |
| Active undated evidence-backed | 159 |
| Historical / superseded | 136 |
| **Toward 1,000 (canonical)** | **385** |
| Agreement type unresolved (among toward) | 263 |
| Franchise resolved (among toward) | 74 |
| Management resolved (among toward) | 48 |
| Country | US 385 |

Provenance among toward-1000 (event rows may carry multiple tags; primary incremental waves):

- baseline_strict / wave2 / wave34 / yield_wave (prior layers)
- sia_name_free_audit: **89**
- name_free_scaleup: **118**

Naive 192+89=281 understates once scale-up (+118) is included; event-deduped total after all layers = **385** (vs 192+89+118=399 → **14** overlaps removed).

## Commercial relevance

- Physical hotel + US country from CMBS property schedules
- Exact source URL + contractual date retained
- Historical vs active dated classified
- Future date ≠ commercially available (explicit flag)
- Combined-column events kept as dated prospects with **unresolved agreement type**
- Ownership enrichment deferred

## Parallel SEC (mgmt gap)

Management-resolved expirations remain scarce (48 in toward set; 0 new resolved in this scale-up). Next: 10-K HMA schedules, EX-10 management exhibits, 8-K operator changes — separate from combined CMBS column.

## Recommended next expansion

1. Continue paginating Related License 424B2 beyond current URL set (yield not exhausted)
2. Property-level agreement-type resolution where exhibit text distinguishes franchise vs HMA
3. Merge/reconcile #517 Track B intl probes without overwriting name-free inventory
4. Keep five-country native chains secondary until US CMBS schedule harvest plateaus

## Governance

CUSTOMER_BLOCKED · no HPC/Meta · no customer publication · Exa Agent manual · Jev shadow · IC merges only
