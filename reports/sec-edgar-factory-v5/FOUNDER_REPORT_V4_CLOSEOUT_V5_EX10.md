# Founder Report — V4 Integration Closeout → V5 EX-10 Discovery Activation

**Date:** 2026-11-21  
**Workspace:** `C:\Dev\dealality\ws-hotel-explorer` (Hotel Explorer)  
**Publication:** CUSTOMER_BLOCKED throughout  
**HPC creates:** 0  

---

## Phase 1 — V4 integration outcome

| Item | Result |
|------|--------|
| PR #499 | **MERGED** |
| Merge commit SHA | `b0711ba4d2b695b79d22d8705619d76d515c341a` |
| Approved tip contained in main | Yes (`6abc9d408` is ancestor of `origin/main`) |
| Pre-merge tip | `6abc9d408` (integrity + date-only unconflict) |
| Live Airtable | 258 date-meaning corrections; unsupported decision windows disabled; 259/259 contract↔milestone links; 259/259 CUSTOMER_BLOCKED |
| Date-semantic regression suite | Present on main (`test:sec-edgar-date-semantic-gate-v4`) |
| Worktree sync | **Not claimed clean.** HE remains dirty (~972) on `feat/hotel-intelligence-sec-edgar-contract-v1`. Post-merge sync = FETCH_ONLY / preserve. Integration primary may be SKIP_INTEGRATION_DIRTY. |

---

## Phase 2 — Preserve hotel facts (architecture)

Identity layers remain separate (`contract-intelligence-identity-layers-v1`):

1. **HOTEL_PROPERTY_FACT** — opening / year-built (preserve; not franchise expiration; no HPC copy without separate auth)
2. **AGREEMENT_EXISTENCE**
3. **AGREEMENT_TERMS**
4. **CONTRACTUAL_EVENT**
5. **UNRESOLVED_OR_CONFLICTING**

Date-only extraction errors do **not** force whole-agreement `CONFLICTED` unless agreement evidence itself is contradictory (`contractStatusAfterDateQuarantine`). Unsupported contract claims stay quarantined; decision windows stay disabled without CURRENT_STATUS proof.

---

## Phase 3–4 — V5 PR + first EX-10 harvest

| Item | Result |
|------|--------|
| V5 branch | `feat/hotel-intelligence-factory-v5` (from `origin/main` @ `b0711ba4d`) |
| V5 PR | **#503** — https://github.com/joandejarden-create/my-operators-backend/pull/503 |
| V5 HEAD | `4bd43da53415952a170220badd72f4fac62eb331` |
| Pilot | `node scripts/sec-edgar-v5-ex10-discovery-pilot.mjs --run --limit=80` |
| Budget ceiling | $25 incremental |
| Actual spend | **$0.184** (Exa lite discovery; EDGAR retrieval cache-first; SEC-API live calls failed/network — mined prior SEC-API cache instead) |

### Discovery metrics (pilot)

| Metric | Count |
|--------|------:|
| Candidate SEC filings / exhibits enumerated (selected) | 80 |
| Exhibits retrieved | 80 |
| Distinct issuers beyond original 32-registry | **29** |
| Coverage ledger unique CIKs (cumulative pilot ledger) | 33 |
| Coverage ledger unique accessions | 59 |

### Agreement classification

| Class | Count |
|-------|------:|
| MANAGEMENT | 57 |
| FRANCHISE | 9 |
| LICENSE | 4 |
| OTHER | 10 |
| Amendments | 20 |
| Renewal/extension instruments (document-class) | 9 |
| Termination instruments | 1 |
| Form-of agreements | 1 |
| Draft/proposed | 1 |
| Named-hotel scope | 38 |
| Portfolio-level | 14 |
| Form without property | 28 |

**Executed vs form:** instrument classifier marks many exhibits `UNKNOWN` when cover language is ambiguous; do **not** treat that as executed-agreement proof. Form/draft kept separate. No hotel attribution invented for portfolio-level contracts (`hotelAttributionAllowed` only for NAMED_HOTEL).

### Verification gates

| Gate | Count |
|------|------:|
| AGREEMENT_EXISTENCE_VERIFIED | 73 |
| CONTRACT_TERMS_VERIFIED | 23 |
| CURRENT_STATUS_VERIFIED | **0** |

Historical exhibit existence ≠ current contractual status. Renewal *options* ≠ executed renewals. Later-document probes did not independently confirm executed renewals/terminations in this pilot.

### Terms extraction (partial)

| Signal | Count |
|--------|------:|
| Initial term years extracted | 7 |
| Renewal provisions found | 16 |
| Notice deadlines found | 33 |
| Financial terms mentioned | 14 |
| Independently confirmed contract expirations | **0** |
| Earliest evidence-backed decision windows | **none** (CURRENT_STATUS_VERIFIED = 0; windows remain disabled) |

### Research persistence

- Research-ready package: `reports/sec-edgar-factory-v5/EX10_RESEARCH_PERSIST_READY.json` (eligible rows only; CUSTOMER_BLOCKED)
- Airtable Research Mode **apply:** not executed this cycle (no Meta schema apply; no new fields without authorization)
- Read-back: deferred until authorized Research Mode writer apply
- No HPC creates; no customer publication

---

## Coverage & backlog

**Proven this wave:** factory can discover substantive hotel contractual exhibits **beyond** the 32-issuer registry (29 new CIKs in pilot selection), retrieve originals via free EDGAR, classify type/scope/instrument, extract partial terms, and keep the three verification gates independent.

**Not claimed:** exhaustive SEC coverage; confirmed executed renewals; customer-ready expirations.

**Recommended next expansion wave**

1. Authorize Research Mode persist + read-back for EXISTENCE+TERMS eligible rows (existing HI schema first; Meta packet only if gaps).
2. Free EDGAR exhibit-index crawl for lodging REITs / managers discovered in this wave (Americas + Europe priority).
3. Targeted later-filing search for the 20 amendments + 9 renewal-class instruments to pursue CURRENT_STATUS_VERIFIED.
4. Re-run EX-10 harvest with improved executed-language detection; diversify franchise/lease/license yield.
5. Keep opening/year-built facts in hotel-fact quarantine — never expand the faulty historical expiration dataset.

---

## Success criterion

- **V4:** Safely integrated with corrected date semantics (merge `b0711ba4d`).
- **V5:** Proved document-level discovery beyond the original issuer list with evidence-backed partial lifecycle intelligence under $25 — without claiming exhaustive coverage or unproven executed renewals.
