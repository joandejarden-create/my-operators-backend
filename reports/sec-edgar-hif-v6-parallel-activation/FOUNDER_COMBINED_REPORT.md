# HIF V6 — Parallel Track Activation Founder Report

**As of:** 2026-11-21  
**V5 merge SHA (verified on `origin/main`):** `9783ff27bc940fcee3b692088353c38674e6e188`  
**Founder authorization:** Parallel free-source discovery activated  
**CUSTOMER_BLOCKED** · Agent `MANUAL_ESCALATION_ONLY` · Jev shadow-only · Lease deferred · Spend **$0**

## Phase 0

| Check | Result |
|---|---|
| Merge on `origin/main` | CONFIRMED |
| Stale execution gate | Updated to `v6WaveExecutionAuthorized=true` with merge SHA |
| Dual-track + weekly scorecard tests | PASS (on HE tree / shared modules) |
| Track workspaces | Created `ws-hif-v6-track-a` / `ws-hif-v6-track-b` from merge SHA (IC registry proposal filed — product chat cannot edit registry) |
| Dirty HE/ADP/GDI/BE | Preserved — no development in dirty HE tree |

## 1. Track A — actual research

| Metric | Value |
|---:|---:|
| EX-10 exhibits harvested (Wave 1) | 62 queued · 61 retrieved · 32 existence · 10 terms |
| CMBS docs processed this wave | 1 (JPMC n2032 424B annex, cache-hit) |
| Fresh CMBS schedule hotel rows | 57 |
| Distinct hotels with franchise expiration dates | **57** |
| Management expiration hotels | 0 (this CMBS annex) |
| Historical / future split | See weekly snapshot `*-track-a-cmbs` |
| Owner/operator deed-verified | **0** (schedule borrower fields not present on extracted rows) |
| Appraisal research | Deprioritized |
| Spend | $0 |

## 2. Track B — actual research

| Portal | Access | Hotel/entity yield |
|---|---|---|
| Brazil CVM | REACHABLE | Landing/catalog validated; structured hotel extract next |
| Japan EDINET | REACHABLE | Portal validated; API key may be required for bulk |
| UK Companies House | REACHABLE (HTML) / API key for REST | **61** company-entity candidates from free HTML search |
| Singapore SGX | REACHABLE | Access validated |
| Hong Kong HKEX | REACHABLE | Access validated |
| Canada SEDAR+ | REACHABLE | Access validated |
| Verified contract expirations | 0 | Property linking not yet complete |
| Spend | $0 | |

## 3–5. Verified dates / owners / prospects

- **Independently re-extracted CMBS franchise expiration rows:** 57 hotel-level dates (US), carried with verification class from schedule extractor; **not** auto-promoted to QUALIFIED_ACTIONABLE without reconcile.
- **New deed-verified owners:** 0 this wave.
- **Evidence-backed commercial prospects (strict dual-list deltas):** not inflated this wave — candidates staged for qualification.
- **UK entity candidates:** 61 RESEARCH_CANDIDATE org labels for reverse-entity follow-up.

## 6. Country coverage (actual research, not eligibility)

| Country | Coverage note |
|---|---|
| US | Actual CMBS + EX-10 research |
| GB | Corporate registry HTML search executed |
| BR, JP, SG, HK, CA | Portal access validated; hotel property extraction pending |

## 7. Incremental yield by source

| Source family | Incremental verified hotel dates | Notes |
|---|---:|---|
| SEC_CMBS | 57 | Fresh extract of known high-yield annex |
| EX10 | (Wave 1) 32 existence / 10 terms | Prior sequential PR #505 content |
| Corporate registries | 0 contract dates | 61 UK entities |
| Intl regulators (CVM/EDINET/SGX/HKEX/SEDAR+) | 0 contract dates | Access wave |

## 8. Spending

**$0** metered · $0 Agent · $0 Jev · no paid court.

## 9. Blockers / next waves

1. IC register `ws-hif-v6-track-a` / `ws-hif-v6-track-b` (proposal attached).
2. HE home still dirty — post-merge sync blocked by preservation.
3. Track A: expand additional CMBS annex URLs beyond single JPMC deal; owner deed linking; recorded-MA county pilots.
4. Track B: CVM/EDINET structured datasets; Companies House API key (founder budget if needed); property-link UK entities; SEDAR+/HKEX/SGX disclosure retrieval.
5. Shared weekly scorecard combine + dedupe across tracks.

## 10. PRs for Integration Controller

- Track A combined (EX-10 Wave 1 + CMBS owner wave): branch `feat/hif-v6-track-a-cmbs-owner` (supersedes standalone #505 content)
- Track B regulatory wave 1: branch `feat/hif-v6-track-b-regulatory-wave1`
