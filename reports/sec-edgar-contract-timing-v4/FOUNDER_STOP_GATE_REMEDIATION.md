# V4 Founder Stop-Gate Remediation Report

**Date:** 2026-11-20  
**PR:** https://github.com/joandejarden-create/my-operators-backend/pull/499  
**Base:** `origin/main` @ `36248d3aa` (#495 merged)  

## Hard stops (unchanged)

- **No** Meta schema apply executed  
- **No** HPC creates  
- **No** full 481 migration apply  
- **No** customer publication  
- **No** live Research Mode persist (schema + persist auth still required)  

---

## 1. Corrected schema packet

Artifact: `CORRECTED_SCHEMA_APPLY_PACKET.json` (also mirrored to `SCHEMA_APPLY_PACKET.json`)

| Blueprint | Count |
|---|---:|
| HI Hotel Contracts (complete) | **28** fields |
| HI Contract Milestones (complete) | **19** fields |
| Link-graph edges | **12** |

### Exact link graph

| From | Field | To |
|---|---|---|
| HI Hotel Contracts | Discovery Property | HI Hotel Discovery Inventory |
| HI Hotel Contracts | HPC Hotel | Hotel Property Census |
| HI Hotel Contracts | Counterparty Org | HI Organizations |
| HI Hotel Contracts | Supersedes Contract | HI Hotel Contracts |
| HI Hotel Contracts | Superseded By Contract | HI Hotel Contracts |
| HI Contract Milestones | **Contract** (multipleRecordLinks) | HI Hotel Contracts |
| HI Contract Milestones | Discovery Property | HI Hotel Discovery Inventory |
| HI Contract Milestones | HPC Hotel | Hotel Property Census |
| HI Contract Milestones | Related Finding | HI Discovery Findings |
| HI Contract Milestones | Superseded By Milestone | HI Contract Milestones |
| HI Discovery Findings | Linked Contract | HI Hotel Contracts |
| HI Discovery Findings | Linked Milestone | HI Contract Milestones |

Publication State default: **CUSTOMER_BLOCKED** (application-enforced; Meta singleSelect has no reliable server default).

Live Meta read: Contracts/Milestones **still absent**.  
Tests: `test-hi-hotel-contracts-milestones-schema-v4.mjs` — PASS (incl. link targets + idempotent reapply simulation).

**Authorization required:** explicit founder approval of this corrected packet + `HIF_SCHEMA_APPLY=1`.

---

## 2–4. Migration quality audit

Artifact: `MIGRATION_QUALITY_AUDIT.json`

| Finding quality | Count |
|---|---:|
| VERIFIED | 306 |
| RESEARCH_ONLY | 171 |
| CONFLICTED | 1 |
| REJECTED | 3 |

| Agreement quality | Count |
|---|---:|
| VERIFIED | **258** |
| RESEARCH_ONLY | **113** |
| CONFLICTED | **2** |
| (excluded findings) | 3 |

Investigations:

- **Maple Grove RV Resort** → REJECTED (manufactured housing / non-hotel)  
- **Bellagio city=Clark** → CONFLICTED (county-as-city; multi-city vs Las Vegas)  
- Stable keys ≠ legal agreement identities; franchise wording / CMBS column required for VERIFIED  

Full 481 migration remains **blocked**.

---

## 5–8. US pilot manifest (corrected)

| Classification | Universe (post-collision) | Enriched shortlist (25) |
|---|---:|---:|
| LINK_EXISTING | 0 | 0 |
| **CREATE_READY** | 0 → **18** after Exa corroboration | **18** |
| CREATE_CANDIDATE_PENDING_VERIFICATION | 50 | 7 |
| RESEARCH_REQUIRED | 87 | 0 (within enriched 25) |
| REJECT_AMBIGUOUS | 8 | — |
| HISTORICAL_ONLY | 0 | 0 |

CREATE_READY now requires: source URLs + filing accessions + verified location + live HPC collision (candidatesReviewed list) + **independent current-property evidence** (brand official pages accepted).

**18 < 25 — not padded.**

Manifest: `HPC_CREATION_MANIFEST_DRY_RUN.json`  
SHA-256: `0dbc136ad15e2f912e1d46a05aee2478f4c0d5fbe4bb0ce8df4b5b3abe226e64`

---

## 9–11. Research Mode pilot + spend

Artifact: `RESEARCH_MODE_PILOT_PACKET.json` — **20** rows prepared  
Status: `PREPARED_PENDING_SCHEMA_AND_FOUNDER_PERSIST_AUTH`  
Live persistence / read-back / idempotency rerun: **not executed**

Research spend this remediation: **$0.125** Exa (then cache hits $0). Ceiling $25 intact.

---

## 12. Remaining founder decisions

1. Approve **corrected** schema packet → authorize Meta apply (`HIF_SCHEMA_APPLY=1`)  
2. Authorize Research Mode persist for the **20-row pilot only** (not full migration)  
3. Review **18 CREATE_READY** hotels; per-row approve before any HPC CREATE  
4. Decide whether to expand enrichment beyond 25 to attempt ≥25 CREATE_READY without padding  

---

## Success criterion (honest)

| Criterion | Status |
|---|---|
| Schema can safely represent agreements/milestones | Packet corrected + tested; **not applied** |
| CREATE_READY reflects verified properties | **Yes** — 18 with brand/SEC evidence; 0 fabricated |
| Live Research Mode pilot | Prepared; **awaiting schema + persist auth** |
