# HIF V6 Research Wave 2 — Combined Founder Report

**As of:** 2026-11-21  
**Publication:** CUSTOMER_BLOCKED  
**Spend:** $0 (free public sources only)  
**Phase 0:** PR #509 CLOSED (gitleaks false positive on `curl -u` docs). PR #510 OPEN — safe CH activation docs; CI gitleaks PASS; handed to Integration Controller.

---

## Track A — Contractual intelligence

| Metric | Result |
|--------|--------|
| CMBS/UW documents retrieved | 12 (+ expand attempts) |
| Management source-priority pointers retrieved | 21 / 21 |
| Schedule rows extracted this wave | 260 |
| Distinct hotels in wave extract | 71 |
| Reprocessed prior hotels (not counted as new) | 228 |
| **Net-new hotels with verified franchise expirations** | **14** |
| Net-new management expirations | 0 |
| Recorded management agreements verified | 0 |
| Court-contract exhibits verified | 0 |
| Property-verified owner/operator relationships | 0 |
| Owner/sponsor text hints (unverified; noisy) | present — not counted |

### Newly retrieved contract-bearing evidence (examples)

All dates below are hotel-level **Franchise Expiration Date** columns from CMBS 424B property schedules (`VERIFIED_EXPIRATION`), deduplicated against prior Wave 1 inventory.

| Hotel | Expiration | Source |
|-------|------------|--------|
| Fairfield Inn – Oak Creek | 2029-06-22 | [n1164_424b2-x16](https://www.sec.gov/Archives/edgar/data/850779/000153949718000207/n1164_424b2-x16.htm) |
| Holiday Inn Express – Fond Du Lac | 2035-06-01 | same |
| Baymont Inn – Madison | 2036-01-31 | same |
| Holiday Inn Express - Milwaukee Airport | 2030-06-22 | same |
| Homewood Suites Memphis Southwind | 2029-03-31 | [n1234_424b2-x11](https://www.sec.gov/Archives/edgar/data/1005007/000153949718000821/n1234_424b2-x11.htm) |
| Hampton Inn Memphis Southwind | 2029-03-31 | same |
| The Residence Inn by Marriott South Beach | 2046-12-31 | [n1872_424b2-x20](https://www.sec.gov/Archives/edgar/data/1013611/000153949719002159/n1872_424b2-x20.htm) |
| Hilton Cincinnati Netherland Plaza | 2029-11-30 | same |

Historical expirations also recovered (e.g. Holiday Inn – Madison West 2020-07-21; Comfort Inn Milwaukee Airport 2019-06-01) — retained; not mistaken for loan maturity.

### Management pointer recovery

- 21 / 21 appraisal-routed management source-priority URLs retrieved.
- Several documents are portfolio-level draft/proposed exhibits or franchise schedule annexes (not hotel-specific HMA expiration tables).
- Status mix: `HOTEL_SCHEDULE_ROWS_FOUND` (franchise dates), `EXHIBIT_TERMS_PARTIAL_NO_HOTEL_DATE`, `RETRIEVED_NO_VERIFIED_CONTRACT_DATE`.
- **No net-new independently verified management agreement expirations** this wave.

### Recorded MA / court

- Free search probes: CourtListener (WAF/human-verification block on API), GovInfo, EDGAR full-text for “memorandum of management agreement” / EX-10 HMA.
- **0 verified recorded MA**; **0 verified court exhibits** (search candidates only — not counted as facts).
- Paid PACER not used (not authorized).

---

## Track B — Government & regulatory

| Metric | Result |
|--------|--------|
| Companies House API auth | **KEY_ABSENT_PUBLIC_HTML_ONLY** |
| Blocker | `COMPANIES_HOUSE_API_KEY` missing in environment / local `.env` |
| Candidates investigated (deepen) | 40 of 61 cohort |
| Legal entities verified (status/name) | 40 |
| Active + SIC 55100 research candidates | 5+ |
| Physical hotels linked (title/accounts/contract) | **0** |
| Property owners verified | **0** |
| Operators verified | **0** |
| New contractual expirations | **0** |
| International portals with content retrieved | 5 / 5 probed (CVM, EDINET, SGX, HKEX, SEDAR+) |
| Property-level international disclosures | **0** |

### Companies House

- Public HTML path used for company profile + filing history + charges pages.
- Active hospitality-name entities include e.g. **MARRIOTT HOTELS LIMITED** `[01235583]`, **HOTEL MANAGEMENT INTERNATIONAL LIMITED** `[01051132]` (Wave 1 cohort), other Hotel Management* actives.
- SIC 55100 + Active is **explicitly not** treated as property ownership.
- Charge/filing lexicon scraping produced mostly page-chrome noise — **not** upgraded to property links.
- Authenticated API live test deferred until founder sets `COMPANIES_HOUSE_API_KEY` (no key logged or committed).

### International portals

- Reachability alone is **not** counted as country coverage.
- Content bodies retrieved for BR/JP/SG/HK/CA probes; hotel-lexicon present on some pages.
- **No issuer → named hotel → agreement date chain completed** this wave.
- Brazil CVM dataset title harvest returned 0 usable hotel-issuer titles in this run.

---

## Combined incremental yield

| Combined metric | Wave 2 |
|-----------------|--------|
| Net-new hotels with verified agreement dates | **14** (Track A) |
| Net-new verified agreement events | **14** franchise expirations |
| Net-new property-owner relationships | **0** |
| Net-new evidence-backed commercial prospects (BCT/MCT ready) | **14** franchise expiration hotels for Brand Conversion Target review (CUSTOMER_BLOCKED — not published) |
| Track B contribution to verified hotel facts | **0** |
| Cost per incremental verified finding | **$0** |
| Research cost | **$0** |

### Country / region coverage (actual research, not portal pings)

- **US / North America:** actual CMBS schedule research + net-new franchise dates.
- **GB / Western Europe:** actual Companies House company research (identity); no property-verified hotels yet.
- **BR/JP/SG/HK/CA:** portal content retrieved; **no** property-level agreement facts → coverage note remains research-in-progress, not geographic inflation.

### Weekly scorecard

Immutable weekly snapshots written under `reports/sec-edgar-hif-weekly-regional-yield/` for `*-track-a-wave2` and `*-track-b-wave2`. Reprocessed prior hotels excluded from “newly discovered.”

---

## Research gaps

1. **Management agreement expirations** still near-zero in free CMBS schedules (franchise columns dominate).
2. **Recorded MA / court exhibits** require free document retrieval paths that are WAF-gated or sparse without paid PACER.
3. **Companies House API key** absent — blocks filing JSON, document metadata, PSC/officers structured pulls at scale.
4. **Property ownership verification** needs title records, named-asset accounts, or executed contracts — not SIC/name.
5. **International portals** need issuer discovery → filing download → hotel extract pipeline beyond landing pages.
6. Owner/borrower reverse search from annex prose is too noisy without structured borrower columns.

---

## Next recommended discovery allocations

1. **Founder:** set `COMPANIES_HOUSE_API_KEY` locally (never commit) → re-run Track B API path on full 61 + charges/PSC/accounts document fetch for Active SIC 55100.
2. **Track A:** continue source-driven CMBS issuer discovery targeting annexes with **Management Agreement Expiration** columns; reverse-search verified franchise hotels’ sponsors only from structured fields.
3. **Track A:** EX-10 harvest focused on executed HMAs (not draft portfolio MSAs); pull underlying agreement PDFs where free EDGAR hosts them.
4. **Track B:** for Marriott UK entities, retrieve specific filed accounts PDFs via CH document API (needs key) and search named freehold/leasehold hotel assets.
5. **International:** pick 1–2 issuers per portal (start BR CVM hotel REIT / SGX hospitality) and complete one full filing→hotel→date chain before expanding.
6. **Budget request (optional):** CourtListener RECAP bulk or limited PACER only if founder authorizes — expected value: hotel bankruptcy HMA/franchise exhibits; not required for CMBS franchise yield which remains free.

---

## Success criterion assessment

Wave 2 produced **measurable incremental hotel-specific contractual intelligence** via Track A (**14 net-new independently verified franchise expirations**). Track B advanced legal-identity investigation and portal content retrieval but **did not yet** convert that into property-verified owners, operators, or contract dates — reported honestly as zero on those success metrics.
