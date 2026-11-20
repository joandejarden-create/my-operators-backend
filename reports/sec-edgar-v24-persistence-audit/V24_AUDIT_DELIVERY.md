# SEC EDGAR V2.4 — Persistence Audit, Hotel Exhibits & Historical Expansion

**PR:** [#492](https://github.com/joandejarden-create/my-operators-backend/pull/492)  
**Branch:** `feat/hotel-intelligence-sec-edgar-contract-v1`  
**Workspace:** `C:\Dev\dealality\ws-hotel-explorer`  
**Spend:** **$0**  
**Merge / deploy / customer publication:** **NOT authorized**

---

## Phase 1 — Authoritative state

| Item | Value |
|---|---|
| Tables | `HI Hotel Discovery Inventory` `tblx77Nmh56h4xa9z` · `HI Discovery Findings` `tblGjTWNm8G4UOF1e` |
| Base | `appCCUsuGsE1ifoLk` (HPC) |
| Live inventory / findings | **124** / **10** |
| Identity classes (post-remediation) | **41 VERIFIED** · **83 PROBABLE** |
| Publication | **124/124 CUSTOMER_BLOCKED** |
| HPC links | **0** |
| Unauthorized HPC creates / field mods | **NOT DETECTED** |
| Graph Evidence reuse for unmatched | **DO_NOT_REUSE** |
| Schema/table IDs match generated-ids | **YES** |

### Founder authorization audit

- Assessment: **CONDITIONAL_APPROVED_WITH_IMPLIED_APPLY**
- `flagOutsideApproval`: **false**
- Basis: V2.3 founder schema direction + “Proceed with the approved design direction”; Meta API `--apply` created only the two approved tables; no extra tables; no HPC core mutations.
- Flag: authorization was design-proceed / deliverable-implied rather than a standalone “run `--apply` now” sentence — documented, not treated as unauthorized.
- V2.4 Sunstone persist reused existing approved tables only (no new schema).

---

## Phase 2 — Identity remediation

| Action | Count |
|---|---|
| False Hyatt quarantines restored | 3 |
| Narrative / thin-brand / geo-bleed quarantined | 6 |
| Footnote name normalizations | 3 |
| VERIFIED remaining | **41** |
| Precision (full remaining VERIFIED sample) | **41/41 = 100%** (≥95% gate **PASS**) |
| Research history deleted | **No** |

Evidence: `reports/sec-edgar-v24-persistence-audit/RESTORE_REMEDIATE_APPLY.json`, live re-audit.

---

## Phase 3–4 — Hotel-specific EX-10 / EX-21 / managers

| Metric | Value |
|---|---|
| Companies | 5 (DRH, Sunstone, Host, RLJ, Apple Hospitality) |
| Filings inspected | 10 historical 10-Ks (2008–2016) |
| Schedule hotels extracted | 94 (57 unique identity keys) |
| Exhibit-index hotel agreements (IBR) | 5 (Form of Hotel Management Agreement / WLS Management Agreement) |
| Attached EX-10s examined | 12 |
| Non-hotel EX-10s skipped (employment/credit) | 5 |
| Attached hotel EX-10 full texts in package | 0 (agreements incorporated by reference) |
| Unique managers (HISTORICALLY_VERIFIED) | **10** — Interstate SHP, Marriott, Hilton, Hyatt, IHR, Highgate, Davidson, Sage, Crestline, Dimension |
| Hotel SPE candidates (EX-21) | 14 |
| Subsidiary entities extracted | 230 |
| Explicit fee-simple legal owners | 0 (correct — registrant not auto-owner) |
| Property-level contract expiration dates from schedules | Limited / often absent in these HTML tables |

Guarantees held: employment/credit filtered; EX-21 ≠ named-hotel ownership; registrant ≠ owner; managers **HISTORICALLY_VERIFIED only** (do not replace current HPC managers); no HPC writes.

Evidence: `reports/sec-edgar-v24-hotel-exhibits/HOTEL_EXHIBITS_REPORT.json`

---

## Phase 5–6 — Historical expansion & persistence

- Eligible inventory (prior V2.4 wave): **~11,940** filings across **32** high-priority registrants since 1994.
- This cycle: **no new Airtable tables**; **no additional cohort migration** until audit/remediation completed.
- Live inventory remains **124** rows / **10** findings; all **CUSTOMER_BLOCKED**; unmatched retained; HPC enrichment **0**.
- Taxonomy: existing 14-family search taxonomy retained; brand-family anchors expanded (Hyatt/IHG/Choice) to prevent false quarantines.

---

## Phase 7 — Results checklist

| Question | Answer |
|---|---|
| Latest GitHub HEAD (pre this push) | `f53817f401af5ec333d59c71023c3805161f09ba` |
| CI (that HEAD) | Required checks **SUCCESS** (gitleaks + ADP/GDI/Radar gates) |
| Live Airtable schema | Validated; IDs match; 124/10 records |
| Founder auth | CONDITIONAL_APPROVED_WITH_IMPLIED_APPLY — not outside approval |
| Records remediated/quarantined | 3 restored · 6 quarantined · 3 normalized |
| Verified hotel identities remaining | **41** @ **100%** precision |
| New EX-10 agreements examined | 5 exhibit-index hotel HMAs (IBR) + 12 attached EX-10s (5 skipped non-hotel) |
| Verified property-level contractual dates | Schedule columns often lack expirations; IBR HMAs identified for follow-on fetch |
| Verified legal owners | None inferred from registrant; SPE candidates listed without ownership claim |
| Management companies | 10 unique HISTORICALLY_VERIFIED schedule managers |
| Companies / filings this exhibit wave | 5 / 10 |
| Cumulative coverage | CORE_SIX + EXTENDED + UNIVERSAL inventory retained; exhibit ledger updated |
| HPC enrichment | **0** writes / **0** links |
| Research spend | **$0** |
| Highest-value next opportunities | (1) Follow IBR to original S-11/8-K packages for full HMA text + dates (2) Host/Pebblebrook/Park historical HTML schedules (3) Persist HISTORICALLY_VERIFIED manager findings into HI Discovery Findings without HPC overwrite (4) Expand SPE↔property linking only after strong identity match |

---

## Code / tests

- `lib/hotel-intelligence/sec-edgar/filing-package-exhibits-v2.js` — hotel EX-10 filter + exhibit-index parser
- `lib/hotel-intelligence/sec-edgar/ex21-subsidiary-extractor-v24.js` — EX-21 SPE candidates
- `scripts/sec-edgar-v24-hotel-exhibits-research.mjs`
- `scripts/sec-edgar-v24-persistence-audit.mjs` / restore-remediate / deep-review
- `node scripts/test-sec-edgar-v22-exhibits-relationships.mjs` — **10 tests passed**

---

## Non-goals (confirmed)

- No merge into `main`
- No deploy
- No customer publication
- No automatic HPC hotel creation
- Concurrent Hotel Explorer dirty worktrees preserved (not committed in this push)
