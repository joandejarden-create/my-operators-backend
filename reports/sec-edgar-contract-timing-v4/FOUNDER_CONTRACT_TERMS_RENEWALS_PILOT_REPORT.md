# Founder Report — Contract Terms, Renewals & Commercial Decision Windows

**PR:** [#499](https://github.com/joandejarden-create/my-operators-backend/pull/499) · Hotel Intelligence Factory V4  
**Date:** 2026-11-21  
**Publication:** `CUSTOMER_BLOCKED`  
**Meta schema apply:** **NOT PERFORMED** (additive proposal only)  
**HPC writes:** **NONE**  
**Incremental paid spend:** **$0.052** of $25 ceiling (Exa); free SEC EDGAR cache-first = $0

---

## 1. Pilot cohort and selection criteria

- Source: 258 VERIFIED live-reconciled franchise agreements (`VERIFIED_MIGRATION_RECONCILE.json`).
- Selected: **40 FRANCHISE** agreements (target 30–50).
- Brand mix: Hilton 16 · IHG 11 · Marriott 11 · Choice 2.
- **Franchise-only:** verified inventory has **zero** management agreements — franchises were **not** relabeled.
- Separate **management discovery subset:** 5 EX-10 / exhibit leads via Exa (SEC-API key optional; $0 subscription path preferred).

Artifacts: `CONTRACT_TERMS_COHORT_SELECTION.json`, `CONTRACT_TERMS_MANAGEMENT_DISCOVERY_SUBSET.json`.

## 2. Baseline vs new findings

| Capability | Baseline (pre-pilot) | After ENRICH_CONTRACT_TERMS |
|---|---|---|
| Scheduled expiration | Present on VERIFIED cohort | Retained (40/40) |
| Structured renewal options | Coarse free-text field only | Extractor + proposed structured fields |
| Notice / exercise deadlines | Not calculated | Calculated when notice-days disclosed |
| Executed vs option available | Not classified | Explicit renewal status classes |
| Decision windows | Expiration only | Multi-kind windows + commercial relevance tags |
| Management agreements | Absent from verified inventory | Separate discovery + EX-10 enrichment |

## 3. Structured contractual-term extraction (franchise cohort)

From `CONTRACT_TERMS_QUALITY_METRICS.json` (denom = 40):

| Metric | Count | % |
|---|---:|---:|
| Source documents retrieved | 40 | 100% |
| Original expiration found | 40 | 100% |
| Original term years found | 0 | 0% |
| Commencement date found | 0 | 0% |
| Renewal options found | 0 | 0% |
| Notice deadlines found | 3 | 7.5% |
| Automatic renewal language | 3 | 7.5% |
| Termination rights found | 0 | 0% |
| Financial terms found | 0 | 0% |
| Decision windows dated | 40 | 100% |
| Incremental beyond expiration | 3 | 7.5% |
| Status unresolved | 21 | 52.5% |
| Extraction errors | 0 | — |

**Evidence quality note:** CMBS 424B property schedules reliably disclose hotel identity + a date column, but rarely the full franchise instrument (term years, renewal options, fees). Most “expiration” milestones in the VERIFIED set align to year-opened / schedule columns rather than negotiated franchise end dates — treated as historical baseline, not invented renewals.

## 4. Renewal options and notice deadlines

- Franchise CMBS filings: limited notice/automatic-renewal hits via portfolio-context scan (3/40), marked low-confidence / not property-attributed when applicable.
- **Management EX-10 leads** (separate subset) produced the high-value structured terms:

| Lead | Term yrs | Renewal opts | Years/opt | Notice days | Mode | Fee |
|---|---:|---:|---:|---:|---|---|
| Sunstone form HMA (`dex103.htm`) | 20 | 2 | 5 | 90 | ELECTIVE | — |
| Exhibit 99.6 | — | — | — | 30 | AUTOMATIC_CONDITIONAL | — |
| EX-10.5 | 10 | — | — | 30 | AUTOMATIC_CONDITIONAL | — |
| R46 | — | — | — | — | — | 1.50% base mgmt |
| EX-10.2 | — | — | — | 30 | — | — |

## 5. Executed renewals and amendments

Later-document Exa probes (12 expired hotels, budget-capped):

- **RENEWAL_EXECUTED_VERIFIED:** 0  
- **EXTENSION_EXECUTED_VERIFIED:** 0  
- **RENEWAL_OPTION_AVAILABLE / AUTOMATIC_CONDITIONAL / CURRENT_TERMS_CORROBORATED / STATUS_UNCONFIRMED:** used per classifier rules  

Rules enforced in code:

- Unexercised options ≠ executed renewals  
- Same brand after expiration ≠ proof of franchise renewal  
- Absence of termination announcement ≠ proof of renewal  

## 6. Future contractual decision windows

For every cohort row with a scheduled date, windows include:

- `SCHEDULED_EXPIRATION` (EXPLICITLY_DISCLOSED when baseline date exists)  
- `RENEWAL_NOTICE_DEADLINE` (CALCULATED_FROM_VERIFIED_TERMS only when notice-days disclosed)  
- Option / termination / performance / CoC windows as CONDITIONAL or UNDETERMINED when not precisely dated  

Commercial relevance is tagged: `CONTRACTUALLY_CONFIRMED` · `EVIDENCE_BACKED_POTENTIAL_WINDOW` · `REQUIRES_MORE_RESEARCH`. Speculative leads are **not** published as verified opportunities.

## 7. Management vs franchise coverage

| Stream | N | Notes |
|---|---:|---|
| Franchise VERIFIED cohort | 40 | No relabeling |
| Management discovery leads | 5 | Separate subset |
| Management leads with disclosed terms | 5 | EX-10 prose yields lifecycle terms |

## 8. Evidence-quality assessment

- **Strong:** Free EDGAR retrieval 40/40; management EX-10 term/renewal/notice extraction with quoted excerpts.  
- **Weak:** CMBS franchise schedules as a source for full contractual lifecycle — insufficient for royalties, cure periods, CoC calendars.  
- **Honest gap:** Factory-wide franchise lifecycle intelligence requires EX-10 / franchise agreement exhibits and later-document amendment research, not CMBS tables alone.  
- Manual review sample: `CONTRACT_TERMS_MANUAL_EVIDENCE_REVIEW.json` (8 hotels).

## 9. Sample hotel-level profiles (representative)

Franchise (baseline + decision window; limited incremental terms):

1. [Homewood Suites Mobile East Bay Daphne](https://www.sec.gov/Archives/edgar/data/1013611/000153949720000474/n2032_x6-424h.htm) — Hilton · FRANCHISE  
2. [Hilton Garden Inn Mobile East Bay Daphne](https://www.sec.gov/Archives/edgar/data/1013611/000153949720000474/n2032_x6-424h.htm) — Hilton  
3. [Holiday Inn Express Trinidad](https://www.sec.gov/Archives/edgar/data/1013611/000153949720000474/n2032_x6-424h.htm) — IHG  
4. Holiday Inn Arlington Northeast Rangers Ballpark — IHG  
5. Hampton Inn Suites Waco South — Hilton  
6. Courtyard Tyler — Marriott  
7. Hilton Garden Inn Edison Raritan Center — Hilton  
8. Hampton Inn Suites Bloomington Normal — Hilton  

Management discovery (high-value terms — **not** hotel-verified inventory rows):

9. [Form of Hotel Management Agreement (Sunstone)](https://www.sec.gov/Archives/edgar/data/1295810/000119312504143744/dex103.htm) — 20-year initial term · 2×5-year owner renewal options · 90-day notice  
10. Additional EX-10 leads in `CONTRACT_TERMS_MANAGEMENT_ENRICH.json`

Full profiles: `CONTRACT_TERMS_SAMPLE_PROFILES.json`.

## 10. Exact proposed Airtable schema additions

**Status: `PROPOSED_PENDING_FOUNDER_META_APPLY`** — do not apply without explicit founder Meta authorization.

Reused existing: Commencement Date, Scheduled Expiration Date, Term Years, Renewal Options Summary, Termination Rights Summary, Contract Status, Agreement Role, evidence/source fields.

**Additive (HI Hotel Contracts):**

- Original Agreement Date · Original Effective Date · Current Corroborated Expiration  
- Renewal Options Count · Renewal Option Years Each · Renewal Exercise Party · Renewal Mode  
- Renewal Notice Days · Renewal Notice Deadline · Option Exercise Deadline · Renewal Status Class  
- Termination Rights Structured · Change Of Control Rights Summary · Financial Terms Summary  
- Terms Evidence JSON · Decision Window Next Date · Decision Window Class · Decision Window Notes  
- Terms Schema Version  

**Milestone type additions:** RENEWAL_NOTICE_DEADLINE · OPTION_EXERCISE_DEADLINE · PERFORMANCE_TEST_REVIEW · CHANGE_OF_CONTROL_TRIGGER · EARLIEST_TERMINATION_WINDOW  

Packet: `CONTRACT_TERMS_ADDITIVE_SCHEMA_PROPOSAL.json`.

## 11. Proposed Research Mode persistence changes

- Persist structured terms to research JSON / `Notes / Quarantine` + existing summary fields until Meta apply.  
- Prefer **one stable contract identity** + linked milestones for renewals/amendments/options (Agreement Role: ORIGINAL / AMENDMENT / RENEWAL_EXTENSION).  
- Never overwrite historical dates with inferred dates.  
- Keep `Publication State = CUSTOMER_BLOCKED`.  
- Independent read-back required on any future authorized apply (same pattern as 20-row Research Mode pilot).

## 12. Factory-stage integration and tests

- Stage: **`ENRICH_CONTRACT_TERMS`** in `stages-v1.js` / allowed in RESEARCH mode only.  
- Engine wires `runEnrichContractTermsStage` → durable `ENRICH_CONTRACT_TERMS.json`.  
- Modules: extractor · renewal classifier · decision windows · cohort · schema proposal · enrich stage.  
- Tests: `node scripts/test-sec-edgar-contract-terms-renewals-v4.mjs` — **12/12 PASS**.  
- Research mode suite still **6/6 PASS**.

## 13. Actual research spend

| Provider | USD |
|---|---:|
| Exa (management discovery + later-doc probes) | 0.052 |
| SEC-API.io | 0.000 (subscription / unused or $0 unit) |
| Free SEC EDGAR | 0.000 |
| **Total** | **0.052** |
| Ceiling remaining | **24.948** |

Unfinished at budget (not spent further): deep per-hotel amendment exhibit chase; EX-10 franchise agreement matching for the 40 CMBS hotels; SEC-API full-text management harvest at scale.

## 14. Recommendations for factory-wide scaling

1. **Authorize Meta apply** for additive terms fields after founder review of the proposal packet.  
2. Add a dedicated **EX-10 franchise/management exhibit harvest** stage upstream of `ENRICH_CONTRACT_TERMS` (CMBS schedules are insufficient alone).  
3. Keep management discovery as a **parallel family**, never a franchise relabel.  
4. Remediate VERIFIED milestone date quality (year-opened vs franchise expiration) before customer-facing decision windows.  
5. Scale later-document renewal verification with cache-first EDGAR + capped Exa, requiring explicit executed-renewal language.  
6. Persist terms enrichments via Research Mode only; HPC / customer publication remain separately gated.

---

### Governance checklist

- [x] PR #499 active branch target  
- [x] Existing live research records preserved (no destructive overwrite)  
- [x] No unauthorized Meta schema apply  
- [x] No HPC hotel creates / canonical hotel updates  
- [x] No production deployment / customer publication  
- [x] No fabricated agreement terms or executed renewals  
- [x] CUSTOMER_BLOCKED maintained  

### Success criterion assessment

**PARTIAL PASS for architecture + management-agreement terms path; LIMITED PASS for CMBS franchise lifecycle depth.**

Dealality can now run a reusable `ENRICH_CONTRACT_TERMS` stage that distinguishes options vs executed renewals, calculates evidence-backed decision windows, and extracts rich terms from management EX-10 exhibits. Franchise CMBS inventory alone does **not** yet yield factory-wide contractual lifecycle intelligence beyond expiration — that requires the EX-10 / amendment harvest recommended above.
