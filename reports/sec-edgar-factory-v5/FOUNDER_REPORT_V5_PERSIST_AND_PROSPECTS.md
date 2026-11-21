# Founder Report — V5 Research Persistence, Timing Discovery & Dual Prospect Lists

**Date:** 2026-11-21  
**PR:** [#503](https://github.com/joandejarden-create/my-operators-backend/pull/503)  
**Publication:** CUSTOMER_BLOCKED  
**HPC creates:** 0  

---

## 1. Research records persisted and read back

| Action | Count |
|--------|------:|
| EX-10 documents classified | 24 |
| HI Discovery Findings created | **24** |
| Independent finding read-backs OK | **24** |
| HI Hotel Contracts created/reused (named-hotel only) | **12** (unique hotels fewer; SF Hilton multi-accession) |
| Inventory rows created/reused | yes (idempotent) |
| Contract Publication State read-back | **CUSTOMER_BLOCKED** (all) |
| Forms/drafts written as executed contracts | **0** |

Evidence: `reports/sec-edgar-factory-v5/EX10_PERSIST_RESULTS.json`

### Execution classification

| Class | Count | Persist policy |
|-------|------:|----------------|
| AMENDMENT_OR_EXTENSION_INSTRUMENT | 22 | Contract only if NAMED_HOTEL + quality hotel name |
| FORM_AGREEMENT | 1 | Finding only |
| DRAFT_OR_PROPOSED_AGREEMENT | 1 | Finding only |

Named-hotel contracts written (examples): Embassy Suites Las Vegas; SpringHill Suites Denver Airport; Hilton San Francisco Financial District; Hilton Philadelphia Airport. Status = `EXPIRED_STATUS_UNCONFIRMED` (current applicability not verified).

### Schema gap (stopped Meta apply)

Additive proposal written (not applied):  
`EX10_DOCUMENT_EXECUTION_SCHEMA_PROPOSAL.json` — Document Execution Class, Scope Class, End Date Status Class, Notice fields.  
Interim: encoded in Notes / Quarantine + Findings-only path for forms/drafts/portfolio.

---

## 2. Forms, drafts, portfolio retained separately

- Form + draft → Findings only (`CONTRACT_SKIPPED_BY_POLICY`)
- Portfolio-level instruments → no invented hotel attribution; document evidence in Findings
- No form/draft promoted to verified executed hotel contract

---

## 3–6. Timing & later-filing

| Metric | Count |
|--------|------:|
| Named-hotel EX-10 with quality hotel extract | 17 |
| Scheduled end dates extracted (plausible ≤2055) | 5 |
| Notice periods discovered (persist cohort) | 22 |
| Later-filing instruments probed (deduped) | 20 |
| Executed renewals/terminations confirmed | **0** |

End-date status classes used: `EXPLICIT_CONTRACT_END_DATE` / `CALCULATED_FROM_VERIFIED_TERMS` → stored as `HISTORICAL_DATE_STATUS_UNCONFIRMED` until CURRENT_STATUS verified.  
Early termination **rights** are not treated as executed terminations.

---

## 7–8. Dual commercial prospect lists (classes A–D)

Outputs:
- `reports/sec-edgar-commercial-targets-v1/BRAND_CONVERSION_TARGETS_V5.json`
- `reports/sec-edgar-commercial-targets-v1/MANAGEMENT_CONTRACT_TARGETS_V5.json`

### Brand Conversion Targets — 437

| Class | Count |
|-------|------:|
| A_VERIFIED_DECISION_WINDOW | 0 |
| B_VERIFIED_END_DATE_STATUS_UNCONFIRMED | **77** |
| C_VERIFIED_COMMERCIAL_SIGNAL | **360** |
| D_RESEARCH_REQUIRED | 0 |
| CLOSED_SUPERSEDED | 0 |

### Management Contract Targets — 80

| Class | Count |
|-------|------:|
| A_VERIFIED_DECISION_WINDOW | 0 |
| B_VERIFIED_END_DATE_STATUS_UNCONFIRMED | **3** |
| C_VERIFIED_COMMERCIAL_SIGNAL | **77** |
| D / CLOSED | 0 |

Shared research identities; a hotel may appear on both lists with distinct evidence. CMBS opening years remain quarantined (not in A/B).

---

## 9. Owner / operator coverage

| Signal | Count |
|--------|------:|
| Brand on prospect rows | 196 |
| Owner or SEC registrant hint | 132 |
| Verified management company | 0 |
| Paid personal-contact enrichment | none |

Registrant hints are labeled `SEC_REGISTRANT_HINT_NOT_VERIFIED_PROPERTY_OWNER` — not assumed decision authority.

---

## 10. New issuers and exhibits

| Metric | Count |
|--------|------:|
| New issuers crawled (beyond 32-registry) | **29** |
| EX-10 exhibits enumerated (free EDGAR indexes) | **62** |

Evidence: `EX10_NEW_ISSUER_EXHIBIT_CRAWL.json`

---

## 11. Cost

| | |
|--|--|
| This wave spend | **$0.044** |
| Prior V5 spend (approx) | $0.184 |
| Ceiling | $25 |
| Remaining (approx) | **~$24.77** |
| Cost per verified A/B target (approx) | **$0.0028** |

Free EDGAR + cached documents used broadly; low spend ≠ stopping criterion.

---

## 12. Rejected false-positive dates

From prior commercial wave (still binding): **264** opening/year-built false “franchise expirations” quarantined.  
This wave additionally rejected implausible calculated ends (>2055) and non-property hotel-name boilerplate.

---

## 13. Highest-yield sources

1. CMBS 424B Franchise Expiration Date columns (re-audited) → Brand Conversion class B  
2. EX-10 named-hotel amendments with notice language → Management Contract seeds + notice coverage  
3. Free EDGAR exhibit indexes on new CIKs → 62 EX-10 follow-ups  
4. Identity inventory → class C commercial signals  

---

## 14. Next recommended expansion

1. Retrieve/extract the **62** newly enumerated EX-10 exhibits for end dates + notice (still under $25).  
2. Owner/asset-manager join for class B hotels with end dates in next 36 months.  
3. Founder Meta authorize Document Execution Class fields when ready.  
4. Continue later-filing verification until CURRENT_STATUS can promote B → A or CLOSE.  

---

## 15. PR #503 status / IC readiness

| Item | Status |
|------|--------|
| PR | **#503 OPEN** |
| Ready for IC | Yes — Research Mode persist + dual lists + date-semantic discipline |
| Dirty worktrees | Preserved (HE/GDI/ADP/BE) |
| Home sync claimed | No |

**Success criterion:** Two growing, deduplicated, evidence-backed research lists (Brand Conversion + Management Contract) prioritizing verified end dates/notice while retaining credible class C hotels without dates — without inflating A/B via opening years or form agreements.
