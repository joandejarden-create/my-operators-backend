# HIF V6 — Inventory Reconciliation + Yield Wave + Integration Status

**As of:** 2026-11-21  
**Publication:** CUSTOMER_BLOCKED  
**Research cost:** $0

## 1) Integration Controller status

| PR | Title | Status | Merge SHA |
|----|-------|--------|-----------|
| #510 | CH identity/API docs | **MERGED** | `3e099e773ea7929c1d375cee057cc9e5cd2c8dcf` |
| #511 | Track A Wave 2 | **MERGED** | `8397c85bbc3a43d9a2fc94be96ec71b4235bb784` |
| #512 | Track B Wave 2 | **MERGED** | `bbc934a4c66d776d860fb908bc420ba94303cdcc` |
| #513 | HMLR ULPD | **MERGED** | `58b9382ad30d6ec4b8785c2d5fc31906623e6625` |
| #514 | Track A Waves 3–4 | **MERGED** | `a1426776b8a2f402ee6bc16f303c76c4cdb42f68` |
| #515 | Track B Waves 3–4 | **MERGED** | `6dea52ed4080eda72c3e2ecd082e33710c1ce26e` |
| #516/#517 | Country concentration | OPEN | — |
| #518 | Inventory reconcile + yield wave | OPEN | — |

Integration primary checkout left **dirty/untouched** (remote merges only). Shared `package.json` conflicts resolved during #512/#513/#515.

## 2) Exact reconciliation of the reported ~194

### Reported additive claim
- Baseline QUALIFIED_COMMERCIAL_TARGETS: **139** (136 BCT + 4 MCT in scorecard; lists are 136+4)
- Waves 2–3 active dated claimed: **11**
- Wave 4 clean undated claimed: **44**
- **Reported sum: 194**

### Exact hotel-key registry (strict baseline lists + wave artifacts + yield clean)

| Classification | Count | Counts toward 1,000? |
|----------------|------:|----------------------|
| Active dated | 19 | YES |
| Active undated evidence-backed | 173 | YES |
| Historical / superseded | 60 | NO (preserved) |
| Quarantined / invalid | 20 | NO |
| **Exact toward 1,000** | **192** | — |

### Franchise vs management (toward 1,000)
- Franchise: **134**
- Management: **58**

### Country (toward 1,000)
- US: **192** (this auditable registry is US-heavy; country-concentration geo-verified rows remain near-zero)

### Why 192 ≠ 194
Hotel-level dedupe of strict BCT/MCT lists + wave increments + quality gates yields **192**. The additive 139+11+44 estimate slightly over-counted relative to historical-date exclusions inside the baseline 136 BCT rows (many verifiedEndDates are already historical as of 2026-11-21).

**Milestone rule:** only ACTIVE_DATED + ACTIVE_UNDATED_EVIDENCE_BACKED count. Historical events are preserved, not inflated into the 1,000.

## 3) Yield wave (conversion / operator / transaction signals)

Executed free EDGAR 8-K harvest for reflagging, franchise termination/nonrenewal, operator replacement, management appointment/departure, and transaction+conversion language.

| Metric | Count |
|--------|------:|
| Documents retrieved | 35 |
| Raw prospects | 83 |
| Clean after name-quality quarantine | **57** |
| Brand conversion list | 57 |
| Management contract list | 0 |
| Cost | $0 |

Examples (clean): LaPlaya Beach Resort; Estancia La Jolla Hotel; Newport Harbor Island Resort; Tranquility Bay Resort; Bourbon Orleans Hotel; Henderson Park Inn.

These 57 are included in the **192** toward-1000 figure above.

## 4) Discovery-before-ownership

Ownership enrichment remains deferred. Owner/operator captured only when inline in researched evidence. Missing owners do not block research qualification.

## 5) Recommended next wave (measured)

1. Finish IC merge of #512–#515 once CI green (shared `package.json` reconciliation required).
2. Scale conversion/operator 8-K path (highest incremental undated BCT yield this session).
3. Expand management-specific operator-replacement queries (0 MCT from this yield pack).
4. US CMBS: invest in HTML schedule extractor breadth (dated franchise yield exhausted on current formats).
5. MX/BR/GB/CA: native regulator document retrieval (not SEC keyword bleed).
