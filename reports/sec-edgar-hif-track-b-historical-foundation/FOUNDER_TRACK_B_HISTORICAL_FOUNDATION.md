# HIF Track B — Historical Hotel Intelligence Foundation & Market Coverage

**As of:** 2026-11-21  
**Stream:** Track B (`ws-hif-v6-track-b` · `feat/hif-v6-track-b-historical-foundation`)  
**Publication:** **CUSTOMER_BLOCKED**  
**Research cost:** **$0** (free sources only; no metered research)  
**E2E classification:** **PERSISTENCE_VALIDATED** (hotel-level creates + read-back + idempotent NOOP) — not FULL_E2E / not CUSTOMER_PUBLISHED  
**Merge:** Integration Controller only — this chat does **not** merge

---

## Baseline

| Item | Status |
|------|--------|
| `origin/main` tip (workspace base) | `a91d34b77` (PR **#537** MERGED) |
| Track A foundation PR **#538** | **OPEN** — parallel; Lakeville reserved |
| Architecture reused | HPC `tbl9aY5ijiuIzzWam`, HI Inventory/Findings, foundation persistence v1, Track A reserved IDs |
| Competing inventories | **None** — discovery inventory only; no HPC creates |
| Dirty worktrees | **Preserved** |

---

## 1. Hotel-universe coverage by country and market

HPC base `appCCUsuGsE1ifoLk` full-country probe (verified physical country):

| Priority | HPC hotels | External branded denominator | Coverage label |
|----------|------------|------------------------------|----------------|
| **US** | **15** | ~37k ESTIMATED | PARTIAL — contractual dense, census thin |
| **MX** | **3,772** | branded UNKNOWN (total ~27,948) | PARTIAL_NO_DENOMINATOR |
| **BR** | **6,179** | branded UNKNOWN | PARTIAL_NO_DENOMINATOR |
| **GB** | **1** | branded UNKNOWN (total ~13,415) | PARTIAL_NO_DENOMINATOR |
| **CA** | **1** | ~2,148 ESTIMATED (Horwath) | PARTIAL |

**Worldwide eligibility:** unrestricted. Top HPC countries remain CALA-heavy (Brazil, Mexico, Argentina, Colombia, …). US HPC = 15 is a **real census skew**, not a probe bug.

Hotel-universe coverage ≠ contractual event count.

Artifacts: `HOTEL_UNIVERSE_COVERAGE.json`, `hpc-country-probe.json`, `MATRIX_COUNTRY_TIMING_GAP.json`.

---

## 2. Verified branded / managed hotel counts

- **HPC verified (all countries):** ~19,462 properties (CALA-dominant).
- **Priority branded denominators:** only US (~37k) and CA (~2,148) treated as ESTIMATED; MX/BR/GB branded share remains **UNKNOWN**.
- **This batch hotel-level identities persisted (not in HPC):** 3  
  - The Yorkville Royal Sonesta (Toronto, CA)  
  - Sonesta ES Suites Toronto Thornhill (CA)  
  - Hilton Garden Inn São Paulo Rebouças → Grand Mercure conversion (BR)

---

## 3. New contractual / historical intelligence

| Property | Country | Intelligence | Status class |
|----------|---------|--------------|--------------|
| Yorkville Royal Sonesta | CA | HMA amendment / extension evidence (SEC + Justia) → horizon ~2043 | AGREEMENT_EXTENDED_OR_AMENDED |
| Sonesta ES Thornhill | CA | HMA amendment evidence | AGREEMENT_EXTENDED_OR_AMENDED |
| HGI Rebouças → Grand Mercure | BR | Brand conversion (Hilton → Accor Grand Mercure) | BRAND_CHANGED |
| Choice Canada MFA transition | CA | Portfolio master-franchise — **STAGED** (not hotel inventory) | — |
| Norte19 / City Express migration | MX | Portfolio brand migration — **STAGED** (no hotel-level IDs) | HISTORICALLY_VERIFIED_CURRENT_UNKNOWN |

Agreement-type discipline: no automatic franchise/management classification of combined columns; loan maturity ≠ hotel agreement expiration.

---

## 4. Historical / current-state reconciliation

| Outcome | Count (batch) |
|---------|----------------|
| AGREEMENT_EXTENDED_OR_AMENDED | 2 (CA Sonesta HMAs) |
| BRAND_CHANGED | 1 (BR Rebouças) |
| STAGED portfolio (HOLD) | 2 |
| Lack-of-newer-filing used as proof of currency | **0** (explicitly forbidden) |
| Track A Lakeville touch | **0** (reserved IDs honored) |

Historical evidence retained even when current brand/operator changes.

---

## 5. Country × Timing × Freshness Matrix (priority highlight)

| Country | HPC universe | Contractual events (ledger) | Franchise / Mgmt resolved | Current-status corroborated (batch) | Critical-mass assessment |
|---------|--------------|-----------------------------|---------------------------|-------------------------------------|--------------------------|
| **US** | 15 | **1,004** | 74 / 209 (721 unresolved type) | 0 this batch | **CONTRACTUAL_CRITICAL_MASS_APPROACHING** — universe critically thin |
| **MX** | 3,772 | 1 | sparse | 0 | **HOTEL_UNIVERSE_SUBSTANTIAL** — contractual sparse |
| **BR** | 6,179 | 1+ | 1 franchise | 1 | **HOTEL_UNIVERSE_SUBSTANTIAL** — contractual sparse |
| **GB** | 1 | 0 | — | 0 | **UNDER_RESEARCHED** |
| **CA** | 1 | 3 | 0 / 2 | 3 | **UNDER_RESEARCHED** universe; contractual foothold started |

Full worldwide HPC country list in `MATRIX_COUNTRY_TIMING_GAP.json`.

---

## 6. Market research gaps

1. **US:** Expand HPC / census identity coverage to match SEC contractual density.  
2. **MX/BR:** Hotel-level contractual schedules and native filings; branded denominators remain UNKNOWN.  
3. **GB/CA:** Thin HPC; UK HMLR/CCOD and Canadian chain census linkage next.  
4. **Portfolio MFA / Norte19:** Need hotel-level IDs before inventory create.  
5. **Agreement-type resolution:** 721 US unresolved types remain research work — do not auto-classify.

---

## 7. Airtable persistence ledger

| Phase | Creates | Updates | No-ops | Staged / research-only | Conflicts |
|-------|---------|---------|--------|------------------------|-----------|
| DRY_RUN | 6 planned | 0 | 0 | 2 | 0 |
| FIRST APPLY | **6** | 0 | 0 | 2 | 0 |
| Re-APPLY (idempotent) | 0 | 0 | **6** | 2 | 0 |

**Created records (read-back OK):**

| Role | Airtable ID | Discovery / Finding ID |
|------|-------------|------------------------|
| Inventory | `recCqubHHjbha20Kl` | `disc_tb_yorkville_royal_sonesta_toronto_2025` |
| Finding | `rec2yMruTvZzW9mSI` | `find_tb_yorkville_sonesta_hma_amend_2043` |
| Inventory | `recZIF5yHqHN5qPzB` | `disc_tb_sonesta_es_toronto_thornhill_2025` |
| Finding | `rechGbGPhDmkkzt4w` | `find_tb_thornhill_sonesta_hma_amend_2043` |
| Inventory | `rec9CS84hmFPJQMU8` | `disc_tb_hgi_sp_reboucas_grand_mercure_2026` |
| Finding | `recZLKPqW3ZOB5z8o` | `find_tb_reboucas_hgi_to_grand_mercure_2026` |

Ledgers: `PERSISTENCE_LEDGER_FIRST_APPLY.json`, `PERSISTENCE_LEDGER_DRY_RUN.json`, `PERSISTENCE_LEDGER_APPLY.json`.

Track A Lakeville (`disc_cb1_lakeville_inn_mn_2026` → `recPXlLWkd63mLK5h` / `recjL2vx3ExNlReF1`) **not** duplicated.

---

## 8. Schema limitations / conflicts

- No schema mutations.  
- Finding Type lacks a dedicated COMMERCIAL_OPPORTUNITY option — mapped to existing BRAND_AFFILIATION / MANAGEMENT + Field Name (same pattern as Track A).  
- Portfolio MFA / Norte19 held in STAGED — cannot safely fit hotel inventory without hotel-level IDs.  
- `schemaGaps: []` for persisted hotel-level rows.

---

## 9. Evidence verification

- SEC EDGAR (Sonesta / HPT HMA exhibits) + Justia contract mirror for CA amendments.  
- Brazil brand conversion corroborated via Accor / property announcement sources in research batch.  
- All persisted rows: `Publication State = CUSTOMER_BLOCKED`, `Evidence Confidence = HIGH` where hotel-level.  
- Idempotent re-apply: 6× NOOP — identities stable.

---

## 10. Research cost

**$0 USD** — free SEC / press / industry PDF denominators only.

---

## 11. PR, tests, Integration Controller handoff

| Item | Value |
|------|-------|
| Test | `npm run test:hif-track-b-historical-foundation` → **PASS** |
| Scripts | `hif-track-b-hotel-universe-coverage`, `hif-track-b-historical-research-batch`, `hif-track-b-foundation-persist-batch`, `hif-track-b-probe-hpc-country` |
| Modules | `hif-track-b-historical-foundation-v1.js` + shared `hif-foundation-persistence-v1.js` (identical hash to Track A #538) |
| PR | *opened after push — see IC handoff below* |
| Merge | **Do not merge from Track B chat** — Integration Controller owns merge + post-merge sync |
| Parallel | Track A **#538** remains OPEN |

---

## 12. Commercial critical-mass assessment

| Market | Approaching critical mass? | Basis |
|--------|----------------------------|--------|
| **US** | **Yes — contractual** | ~1,004 events toward 1k contractual; **No — hotel universe** (15 HPC) |
| **MX** | **Yes — hotel universe** | 3,772 HPC; contractual still sparse |
| **BR** | **Yes — hotel universe** | 6,179 HPC; contractual still sparse; 1 conversion persisted |
| **GB** | No | Thin on both axes |
| **CA** | Early foothold | 2 HMAs + 1 staged MFA; HPC=1 |

**Recommendation for next batches:** US identity enrichment into HPC/discovery; MX/BR hotel-level contracts; GB/CA census linkage; resolve staged portfolios to hotel IDs.

---

## Weekly automation

Prepared (signals + idempotent upsert pattern) — **scheduler not activated**. See `WEEKLY_AUTOMATION_READINESS.md`.

---

## Final status for Integration Controller

**AWAITING_INTEGRATION_CONTROLLER**

Preserve dirty worktrees. CUSTOMER_BLOCKED. No production deployment. No customer publication.
