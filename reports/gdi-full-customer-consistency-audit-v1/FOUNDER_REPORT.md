# GDI Full Customer Experience, Content & Commercial Consistency Audit

**Schema:** `gdi-full-customer-consistency-audit-v1`  
**Branch / build:** `fix/gdi-regression-recovery-v1` @ `6ca9ea39c` (+ this audit commit)  
**Consolidated PR:** https://github.com/joandejarden-create/my-operators-backend/pull/233  
**Served assets:** `?v=gdi-regression-recovery-v1` on `http://127.0.0.1:8080`  
**Policy:** Do not deploy, publish, or merge without founder authorization.

---

## 1. Executive assessment

| Score | Value |
|---|---|
| **Overall** | **85.6 / 100** |
| Presentation / commercial consistency | **100 / 100** |
| Research-queue completeness | **52 / 100** |

**Verdict:** On the consolidated recovery baseline, **customer-facing presentation is coherent** across titles, competitor-vs-target sourcing, Market Scope, Opportunity Type filter contracts, verification-pill removal, and recommended-action safety. The overall score is **not perfect** because **64 opportunities** across the portfolio still lack a canonical demand-segment mapping (internal Other / Unclassified) — these are **research-required**, not hidden UI cosmetics.

A hotel sales director can read cards/drawers on the recovery build without the previously reported Host-suffix titles, “Venue Confirmed” collapse, duplicated “Qualify Overflow”, or Market Scope / demand-type confusion. Remaining work is **research classification** of lifecycle rows (`FUTURE_WATCH` / `FUTURE_CYCLE` without `demandSegmentId`), not another presentation patch set.

**Release recommendation:** Safe to merge the consolidated recovery PR (#233) for presentation baseline **after founder authorization**. Do **not** treat research-queue gaps as closed.

---

## 2. Scope audited

| Metric | Count |
|---|---|
| Hotels scanned | **17** (fixture/test hotels excluded) |
| Opportunities audited | **699** |
| Deterministic UI contract checks | Market Scope, Reset View, nav title, guided tour, Other exclusion, cache token, card verification pills |
| Browser hotels | Bethesda (67/67), Wyndham San Diego Bayside (47/47), Waterstone (toolbar; 0 customer-visible / 29 raw) |

Machine-readable outputs:

- `00-audit-summary.json`
- `01-findings.json` (stable issue IDs)
- `02-field-source-of-truth.json`
- `03-opportunities-with-issues.json`
- `screenshots/`

---

## 3. Issues by severity and category

| Severity | Count | Meaning |
|---|---|---|
| P0 Critical | **0** | — |
| P1 High | **0** (after remediation) | — |
| P2 Medium | **0** | — |
| P3 Low | **0** | — |
| REVIEW_REQUIRED | **64** | Research-queue demand-segment gaps |

| Category | Count |
|---|---|
| CLASSIFICATION | 64 |
| TITLE / SOURCING / NARRATIVE / UI (presentation) | 0 remaining |

---

## 4. Issue counts by hotel (research-queue only)

| Hotel ID | REVIEW_REQUIRED |
|---|---|
| recG66DQJKP2c0UNh | 12 |
| recjhQdAUiSyxfCqE | 10 |
| recUOyzOXn2Zdp98I | 8 |
| rec2PVBDavppGpenm | 7 |
| rec82D9zpB8fede1I | 7 |
| recFaxTEFF9ILHWC9 | 6 |
| recKRJjcPnb4tVDDS | 5 |
| recIwaP1etgx2g9nA | 3 |
| recrPQcZg7SFARRb2 | 2 |
| recGkME49yYuxQl0u | 2 |
| rec35fExUxCClpOP6 | 1 |
| recnPB3Q02NO8Ma1o | 1 |
| Bethesda `recLuxvwwxID7U2B8` | **0** |

Bethesda sales inventory remains clean on classification after prior remediation.

---

## 5. Phase 1 — Canonical baseline & source of truth

**Running implementation:** main checkout `fix/gdi-regression-recovery-v1`, server `node server.js` on **:8080** only (non-canonical :8095 worktree stopped). Assets `gdi-regression-recovery-v1`.

**Contained / superseded open PRs (do not merge independently):**

| PR | Role in baseline |
|---|---|
| #230 | Card quality + Market Scope toolbar |
| #231 | Surfe on-demand + guided tour polish |
| #232 | Permanent title contract (extended v1.1 competitor/host) |
| **#233** | Consolidated recovery + CI baseline |

**Field SoT (summary)** — full table in `02-field-source-of-truth.json`:

| Field | Canonical source | Transform | UI | Tests |
|---|---|---|---|---|
| displayTitle | Title contract + CMI ledger `rem.title` | DTO / sanitize | Card, drawer, overview | title-contract + title-sourcing |
| hotelSourcingStatusLabel | `composeCustomerFacingHotelSourcingLabel` | overview re-stamp | Card meta, overview | title-sourcing |
| Demand segment / Opportunity Type | 14-segment map | never CMI-as-type | Opportunity Type filter | opportunity-type-classification |
| Market Scope | `annotateInventoryPopulations` | radiogroup only | All / Hotel / Competitive | market-scope-toggle |
| recommendedAction | `recommendCommercialAction` + ledger | RFP/overflow sanitize | Pill + drawer | contradiction + title-sourcing |
| Contacts | sanitize → tile preview + Surfe | on-demand reveal | Card + Who to Contact | surfe + compact-tile |
| Hotel feedback | `feedback.json` / Airtable | presentation-only | Validation / Action / Outcome | feedback persistence suites |

**Competing logic eliminated in recovery:** overview “Venue Confirmed” overwrite; Competitive Status selector; card verification pills; CMI as Opportunity Type.

---

## 6. Before / after examples

### Titles
| Before | After |
|---|---|
| `NADO & DDAA Washington Conference 2027 — Crystal Gateway Marriott Host` | `NADO & DDAA Washington Conference 2027` |
| `ACVNU Renal Week 2027 — Competitor Room Block at the…` | `ACVNU Renal Week 2027` |

### Sourcing (NADO 2027)
| Before | After |
|---|---|
| Overview: `Venue Confirmed` + Location `Host hotel contracted — Crystal Gateway Marriott` | `Competitor venue confirmed (Crystal Gateway Marriott); target-hotel overflow sourcing unconfirmed` + Location `Arlington, VA / Washington, DC metro` |

### Narratives
| Before | After |
|---|---|
| `Qualify Overflow — Qualify Overflow — …` | Single action body (no duplicated prefix) |
| `… is dated in Host hotel contracted — Crystal Gateway Marriott with hotel sourcing unconfirmed…` | Competitor-host wording; overflow unconfirmed |
| Waterstone `… VIP / Overflow Housing` (sourcing UNKNOWN) | Softened to overflow hypothesis (unconfirmed) via sanitizer |

---

## 7. Root causes addressed (code paths)

| Defect class | Files |
|---|---|
| Title suffixes | `customer-facing-opportunity-title-contract-v1.js` |
| Sourcing collapse | `events-opportunity-overview-v1.js`, `opportunity-list-dto.js`, `commercial-presentation-sanitizer-v1.js`, `dealality-gdi-ui.js` |
| Malformed narratives | `commercial-presentation-sanitizer-v1.js`, `hotel-relevance-research-v1.js` |
| CMI vs demand type | `cmi-evidence-ledger-v1.js`, `opportunity-type-filter-v1.js`, `legacy-demand-segment-alias-map-v1.js` |
| Overflow-without-evidence copy | `sanitizeOverflowPresentation` in `commercial-presentation-sanitizer-v1.js` |
| Permanent audit gate | `customer-consistency/gdi-full-customer-consistency-audit-v1.js` + CI |

---

## 8. Fixed in this audit pass

1. **Reusable full-portfolio audit** (`gdi:full-customer-consistency-audit`) with stable issue IDs.  
2. **Waterstone Palm Beach Boat Show** overflow affirmative language when target sourcing unconfirmed.  
3. **Audit severity model:** store-level Other/Unclassified → `REVIEW_REQUIRED` (research queue); UI still proves Other is never a customer filter option.  
4. Wired into `test:gdi-regression-recovery-v1` and GDI CI workflow.

---

## 9. Requires additional research (64)

Internal Other / Unclassified rows — typically `opportunityType` lifecycle enums (`FUTURE_WATCH`, `FUTURE_CYCLE`, …) with **null `demandSegmentId`**. Customer Opportunity Type filter **excludes** Other. Full IDs in `01-findings.json` (`*-TYPE-UNCLASSIFIED`).

Prior portfolio remediation already recovered 182; **~70** research-flagged remains consistent with this 64-store sample (some hotels / inventory slices differ).

---

## 10. Founder policy decisions (none auto-applied)

| Topic | Recommendation | Status |
|---|---|---|
| Auto-map FUTURE_WATCH → a default demand segment | **Do not** without evidence — keep research queue | Awaiting authorization |
| Waterstone sales-team visibility (0 shown / 29 raw) | Review publication / inventory-audience rules | Open investigation — not a UI regression |
| Merge #233 vs keep #230/#231/#232 open | Merge #233 only; then close 230–232 as superseded | Awaiting authorization |

---

## 11. Regression tests

| Suite | Result |
|---|---|
| `test:gdi-regression-recovery-v1` (incl. classification, title-sourcing, **full consistency smoke**) | PASS |
| `test:gdi-full-customer-consistency-audit-v1` | PASS |
| `verify:gdi-regression-recovery-browser-v1` | PASS |
| CI on #233 (`GDI recovery regression suite`) | PASS (prior tip; re-runs on this commit) |

---

## 12. Browser screenshots

- `screenshots/bethesda-marriott-toolbar.png` / `bethesda-marriott-drawer.png`
- `screenshots/wyndham-san-diego-bayside-toolbar.png`
- `screenshots/waterstone-boca-raton-toolbar.png`

Confirmed: Market Scope height parity (48px), recovery cache token, Bethesda 67/67, no “X updated” header.

---

## 13. Remaining known limitations

1. **Layer C semantic LLM review** of every drawer paragraph was **not** run as a paid/model pass — deterministic + cross-field layers cover titles, sourcing, RFP/overflow contradictions, classifications, and UI contracts. Subtler prose tone issues may remain.  
2. **Waterstone customer surface empty** under sales-team inventory despite raw opportunities — separate publication/audience issue.  
3. Contact-tile short-circuit / Proper-Case mixed-title / revenue-icon muted paths identified in prior exploration are **out of this audit’s remediation set** unless re-opened as a follow-on task.  
4. Audit does not mutate hotel feedback or eligibility thresholds.

---

## 14. Final release recommendation

| Gate | Status |
|---|---|
| Presentation coherence (titles, sourcing dimensions, Market Scope, actions) | **PASS** |
| Competitive ≠ open host RFP | **PASS** on audited remediated paths |
| Card verification pills absent | **PASS** |
| Research-queue classification complete | **FAIL / OPEN** (64 REVIEW_REQUIRED) |
| Founder merge authorization | **REQUIRED** |

**Recommend:** Authorize merge of consolidated recovery baseline (#233) as the protected product floor; schedule a dedicated **demand-segment research wave** for the 64 REVIEW_REQUIRED IDs; do not claim a perfect customer-data score until that wave closes.
