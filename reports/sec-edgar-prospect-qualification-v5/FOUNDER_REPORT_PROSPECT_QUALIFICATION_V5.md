# FOUNDER REPORT — Global Prospect Qualification & Next-Wave Plan

Audited: 2026-11-21T01:51:57.410Z (regenerated closeout narrative)
PR #503 closeout audit · CUSTOMER_BLOCKED · no research horizon · no geo exclusion

## Phase 1 — Integration Controller closeout

| Gate | Result |
|---|---|
| PR | [#503](https://github.com/joandejarden-create/my-operators-backend/pull/503) |
| Latest HEAD (PR tip) | `caa98a6f3ceef2853cb3b350cfb6536458dffe5b` |
| Required CI | **ALL SUCCESS** — Admin tabs + AVA · Radar HE + ADP free · Spice fixture · GDI recovery · gitleaks |
| Mergeable | `MERGEABLE` |
| Merge state | `BEHIND` main (IC should FF/rebase strategy; do not force from product chat) |
| Date-semantic safeguards | Present in `commercial-target-discovery-v1.js` (`CONTRACT_DATE_CLASS`, `APP_DATE_FILTER` UI-only) |
| Research persistence | 24 EX-10 findings + dual V5 prospect lists already on branch; CUSTOMER_BLOCKED |
| CUSTOMER_BLOCKED | Maintained on all prospect / qualification rows |
| HPC / customer publication | None |
| Meta Document Execution Class apply | Proposal only — **not applied** |
| Product chat merge | **No** — Integration Controller owns merge |
| Sequential work | Heavy 62-exhibit retrieval / owner enrichment / international adapters → **post-merge PR**, not indefinite #503 extension |

Preserve unrelated dirty worktrees (HE checkout remains on other dirty feature work; closeout uses commit-tree onto #503 tip).

---

## 1. Distinct hotels in each list (global)

| List | Rows | Distinct hotels |
|---|---:|---:|
| Brand Conversion V5 | 437 | **431** |
| Management Contract V5 | 80 | **78** |
| Brand strict (reconcile) | 136 | (higher-confidence dual projection) |
| Management strict named EX-10 | 4 | (named-hotel MANAGEMENT only) |

No country/region eligibility filter on research retention. Coverage remains **US SEC–dominant**; international eligibility ≠ international coverage (see §11).

## 2–5. Verified franchise / management expirations

| Bucket | Brand Conversion | Management Contract |
|---|---:|---:|
| Verified end dates | 77 | 3 |
| Historical verified expirations | **60** | **0** |
| Future verified expirations | **17** | **3** |
| Unknown / no end date | 360 | 77 |

Franchise-leaning verified ends dominate Brand Conversion. Management V5 end-date count (3) aligns with named evidence; do **not** treat the remaining 77 management rows as verified management expirations.

## 6. Notice periods verified and attributed

| Applicability | Count |
|---|---:|
| Total notice rows with days | **22** |
| Hotel-specific | **11** |
| Portfolio-level | **3** |
| Generic form language | **6** |
| Unresolved | **2** |

| Notice kind (heuristic) | Count |
|---|---:|
| Extension / exercise notice | 9 |
| Other notice | 13 |
| Nonrenewal / early termination typed | 0 (needs clause-text wave) |

Deadline calc tentatively ready (named hotel + end date + notice days): **3** — still require clause-type verification before deadline promotion. Portfolio/generic notice must **not** be assigned to property deadlines.

## 7. Renewal / termination / reflagging

No mass renewal/termination/reflag certification in this qualification wave. Historical Tier B rows (60 Brand) are labeled `VERIFY_RENEWAL_OR_SUCCESSOR_STATUS`. Joplin / related Agent pilot leads remain research signals, not certified successor status. Sequential PR owns subsequent-disclosure status resolution.

## 8. Verified current owners and management companies

| Signal | Brand | Management |
|---|---:|---:|
| Property owner field depth verified | **0** | **0** |
| SEC registrant hint only | **60** | **72** |
| Management company populated (unverified depth) | sparse | sparse |

Registrant ≠ property owner. Owner/operator enrichment is sequential-wave work via HE/HI relationships.

## 9. Named relevant decision-makers

**0** certified named decision-makers in this wave. No paid personal contact enrichment executed or authorized.

## 10. Prospect quality by tier and evidence strength

| Quality | Brand | Management |
|---|---:|---:|
| VERIFIED_COMMERCIAL_PROSPECT | **77** | **3** |
| PROMISING_RESEARCH_CANDIDATE | **359** | **2** |
| WEAK_OR_GENERIC_SIGNAL | 1 | **71** |
| INVALID_OR_MISATTRIBUTED | 0 | **4** |

Management V5 inflation: 71 weak (UNKNOWN/OTHER typed as management-list projection) + 4 invalid (typo / TRS subsidiary language). Strict named EX-10 management remains **4**.

## 11. Additional countries and sources covered

- **US SEC / EDGAR + EX-10:** primary (this inventory).
- **International registries / exchange filings / multilingual disclosures:** designed as eligibility/expansion roadmap; **not yet harvested** at volume.
- Blind spot: treating global eligibility as global coverage — **rejected**.

## 12. New valid targets per research dollar

This closeout wave: **$0 paid** (offline qualification audit). Prior V5 foundation spend remains on separate ledgers (Search primary; Agent pilot $15 separate; Jev shadow $0 fact writes). Incremental verified commercial yield this wave = reclassification quality, not new paid discovery.

## 13. Invalid prospects removed or reclassified

Not deleted from inventory (research universe preserved). Reclassified:

- Management invalid (4): Chartlotte Lakeside Hotel; Comfort Inn TRS Subsidiary; Quality Inn TRS Leasing; Comfort Inn TRS Leasing
- Management weak (71): inflated UNKNOWN/OTHER → `RECLASSIFY_OR_CONFIRM_MANAGEMENT_AGREEMENT_EVIDENCE`
- Brand: effectively 0 invalid; 1 weak

## 14. Top 20 hotel-level research profiles (evidence-backed)

See `TOP_20_RESEARCH_PROFILES.json`. Leading verified Brand rows include:

1. Hilton Garden Inn Joplin — FRANCHISE end 2026-10-31 — SEC 424B2 evidence — VERIFY_RENEWAL_OR_SUCCESSOR_STATUS
2. Hampton Inn Mount Vernon — end 2020-10-31 — historical
3. Hampton Inn Joplin — end 2033-01-31 — future
4. Hampton Inn Anderson — end 2020-05-31 — historical
5. Hampton Inn Marion — end 2033-02-28 — future
6. Holiday Inn – Madison West — end 2020-07-21 — historical
7. Fairfield Inn – Oak Creek — end 2029-06-22 — future
8. Holiday Inn Express – Fond Du Lac — end 2035-06-01 — future
9. Baymont Inn – Madison — end 2036-01-31 — future
10. Comfort Inn & Suites - Appleton Airport — end 2024-07-30 — historical

(Full 20 in artifact; all CUSTOMER_BLOCKED.)

## 15. Remaining gaps and recommended next discovery wave

1. **IC merge #503** after behind-main resolution + green CI (already green on tip).
2. **Sequential PR — EX-10 harvest:** retrieve/extract 62 enumerated exhibits (46 HIGH_TEXT first; 16 image/low deprioritized).
3. **Sequential PR — notice typing:** hotel-specific clause class + deadline calc only when verified.
4. **Sequential PR — owner/operator enrichment** for VERIFIED + PROMISING (HE/HI reuse; no paid contacts).
5. **Sequential PR — status resolution** for historical Tier B (renewal / terminate / reflag / supersede).
6. **Sequential PR — international source adapters** (report intl coverage separately from US SEC).

---

## Count reconciliation (hotels / agreements / opportunities)

- Brand V5 rows **437** ≠ strict Brand **136** — V5 is broader research projection; qualification separates verified (77) vs promising (359).
- Management V5 **80** ≠ strict named EX-10 **4** — 71 weak + 4 invalid explain inflation; 3 verified + 2 promising remain commercially serious.
- Distinct hotels ≈ rows (Brand 431/437; Mgmt 78/80) — limited multi-agreement duplication at hotel grain.
- Commercial opportunities with verified timing: Brand 77 end dates; Mgmt 3 end dates.

## Governance

- CUSTOMER_BLOCKED · no HPC · no unauthorized Meta apply · no invented renewals
- Preserve ALL historical and future dates; UI owns 12/24/36/60-month filters
- Exa Agent / Jev must not certify contractual dates without independent source evidence
- Product chat status: **AWAITING_INTEGRATION_CONTROLLER**
