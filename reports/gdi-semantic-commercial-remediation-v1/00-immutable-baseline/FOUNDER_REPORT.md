# GDI Full Semantic & Commercial Consistency Audit V1

**Schema:** `gdi-full-semantic-commercial-audit-v1`  
**Generated:** 2026-11-13  
**Baseline:** local `main` (PR #233 consolidated recovery)  
**Policy:** No merge / deploy / publish in this task. No paid OpenAI or paid research credits spent.

---

## 1. Executive assessment

| Dimension | Score |
|---|---|
| Technical consistency (prior Layer A–C contracts) | **100** |
| Semantic consistency | **81.1** |
| Commercial accuracy | **78.6** |
| Evidence reliability | **85.4** |
| Readability | **73.8** |
| Content quality | **73.7** |
| Research completeness | **99.9** (1 OTHER_UNCLASSIFIED open) |
| **Overall** | **85.1 / 100** |

**This is not a 100/100 content score.** The earlier structural audit’s presentation “100” remains only the technical-consistency dimension. Overall uses the weighted methodology in `05-quality-scorecard.json` and is computed only after 100% semantic `REVIEWED` coverage.

**First-pass overall:** 84.6 → **post-shared-fix scorecard:** 85.1 (research-completeness refresh; agent scores still first-pass).

**Verdict:** Every in-scope opportunity was rendered through the live drawer UI path and received a Cursor-agent paragraph-level semantic review. Confirmed P0 closed-sourcing vs “Win Room Block” and Host-hotel-contracted thesis glue were fixed in shared generators. Material P1 copy/commercial clusters remain open and must not be waved away by test-suite green.

---

## 2. Inventory reconciliation vs prior audit

| Metric | Prior structural audit | This semantic audit |
|---|---|---|
| Hotels with opportunity inventory | 17 | **17** |
| Opportunities | 699 | **699** |
| Parity-universe hotels excluded (no store) | — | **4** |

### Excluded selectable hotels (no `opportunities.json`)

- `recRkGGNKslJptFIL` — Falls Church Marriott Fairview Park (selectable)
- `recC2kbCaS48jFjFF` — Nobu Hotel London Portman Square (selectable)
- `rec0eTEnCqlsGJJt9` — The Mayflower Hotel, Autograph Collection (selectable)
- `recdmo0CnhIZAwHGC` — The Westin Tampa Waterside (selectable + active share)

These were excluded because there is **zero opportunity inventory** to review—not because they were sampled out.

---

## 3. What was reviewed

For each of **699** opportunities:

1. Canonical load → CMI ledger remediation → commercial sanitize → customer presentation sanitize → DTO  
2. Final drawer HTML via `DealalityGdiUi.intelligenceDetailHtml`  
3. Card fields (title, summary, priority, sales action, timing, sourcing)  
4. Named drawer sections (overview, timing, location, hotel sourcing, demand, competitor/future business, why this hotel, advantages/limitations, why act now, facts/uncertainties, contacts, next step, evidence, validation)  
5. Cursor-agent semantic review (18 batches) producing one `REVIEWED` object each

Coverage proof: `06-coverage-manifest.json` · batch I/O: `08-review-batches/` · rendered packs: `07-rendered-packs/`

**Engine:** Cursor agent (`composer`), not paid OpenAI (`--authorize-paid-semantic` was not granted).

---

## 4. Findings summary

Harness artifacts **excluded from product counts**: `SEM_HOTEL_VALIDATION_PLACEHOLDER`, `SEM_OTHER_DRAWER_BLOB_DUPLICATION` (audit validation placeholder + full-text duplication into `other_drawer_text`).

| Severity | Product count (first-pass agent file) |
|---|---|
| P0 | 6 (3 Renaissance opportunities × 2 codes) — **remediated in shared resolver** |
| P1 | 207 |
| Suggested corrections logged | 514 |
| Research-required rows | 64 (1 still OTHER_UNCLASSIFIED; 63 remapped → `CLASSIFICATION_REMAP_VERIFY`) |

### Top product contradiction codes

| Code | Count |
|---|---|
| `SEM_DUPLICATE_EVIDENCE_URL` | 96 |
| `AGENT_URGENCY_VS_MONITOR_STANCE` | 84 |
| `SEM_POTENTIAL_BUSINESS_COPIED_TO_WHY_ACT_NOW` | 70 |
| `SEM_TOURNAMENT_HOUSING_TEMPLATE_LEAK` | 55 |
| `SEM_MONITOR_CARD_VS_ACTIVE_HOUSING_REC` | 53 |
| `SEM_PIPELINE_SLUG_CUSTOMER_SUBJECT` | 40 |
| `SEM_INTERNAL_HOTEL_ID_IN_CUSTOMER_COPY` | 32 |
| `SEM_CARD_ACTION_VS_PASSIVE_RECOMMENDATION` | 31 |
| `SEM_PAST_CYCLE_VS_FUTURE_EVENT_DATES` | 31 |
| `SEM_PAST_CYCLE_VS_BUILD_ACCOUNT` | 20 |

Details: `02-contradictions.json`, summary `02b-product-contradictions-summary.json`, reviews `01-semantic-opportunity-reviews.json`.

---

## 5. Corrections applied (shared generators — safe)

| Defect | Fix | Status |
|---|---|---|
| `dated in Host hotel contracted` in thesis / hotel-relevance fields | Expanded `sanitizeDuplicatedActionNarrative` to `whyHotelDemand`, `hotelRelevanceResearch`, nested briefs | **Done** |
| Card `Win Room Block` while sourcing closed / monitor-only guidance | `isClosedSourcingOrPassiveMonitorStance` → **Monitor** in `customer-sales-action-resolver-v1.js` | **Done** |
| `gdi_hotel_*` / Airtable `rec…` / demand enum leaks in prose | `scrubInternalResearchLanguage` extended | **Done** |

Post-remediation spot check (`09-post-remediation-spot-check.json`): Renaissance NAMT / Spring Road / Boutique Investment now show **Monitor**; NADO Host-contracted glue cleared.

**Not done (needs continued shared work + re-review):** duplicate evidence URL blocks, potential-business copied into why-act-now, tournament housing template leaks, pipeline slug subjects, remaining monitor-vs-urgency copy conflicts, past-cycle vs future-date mismatches.

No factual research conclusions, eligibility, publication, or hotel feedback were silently rewritten.

---

## 6. Scoring methodology

```
Overall =
  0.20 × technicalConsistency +
  0.25 × semanticConsistency +
  0.25 × commercialAccuracy +
  0.15 × evidenceReliability +
  0.10 × readability +
  0.05 × researchCompleteness
```

Overall is **withheld** unless every in-scope opportunity has `reviewStatus=REVIEWED`. Heuristic-only assists do not unlock overall scoring.

---

## 7. Deliverables checklist

| File | Status |
|---|---|
| `FOUNDER_REPORT.md` | This report |
| `01-semantic-opportunity-reviews.json` | 699 REVIEWED |
| `02-contradictions.json` | Full contradiction ledger |
| `03-content-corrections.json` | Suggested corrections |
| `04-research-required.json` | Classification / research queue |
| `05-quality-scorecard.json` | Scores + methodology |
| `06-coverage-manifest.json` | Coverage proof |
| `07-rendered-packs/` | Rendered customer-facing packs |
| `08-review-batches/` | Batch inputs + `*-reviews.json` |
| `screenshots/` | Bethesda + Wyndham toolbar/drawer |
| `09-post-remediation-spot-check.json` | Targeted re-check |

NPM: `npm run gdi-full-semantic-commercial-audit-v1`, `npm run test:gdi-full-semantic-commercial-audit-v1`, `npm run gdi-semantic-review-batch-merge-v1`.

---

## 8. Limitations

1. Semantic engine = Cursor agents (approved). Paid OpenAI path exists (`--authorize-paid-semantic`) but was **not** used.  
2. No paid Surfe / Hunter / Exa / web re-verification of evidence URLs.  
3. Live Hotel Validation / Action / Outcome feedback was not injected into every rendered pack (structural placeholder) — browser checks still required for feedback persistence.  
4. Agent scores are first-pass on pre-fix packs; a full second-pass re-score after remaining P1 generator fixes is recommended.  
5. 63 formerly unclassified rows remapped by legacy alias classifier need research confirmation (`CLASSIFICATION_REMAP_VERIFY`).  
6. Waterstone customer-visible inventory remains a separate publication/authorization investigation.

---

## 9. Release readiness

| Gate | Status |
|---|---|
| 100% inventory rendered | **PASS** |
| 100% semantic agent review | **PASS** |
| Confirmed P0 closed-sourcing vs Win Room Block | **REMEDIATED** |
| Host-hotel-contracted thesis glue | **REMEDIATED** |
| Remaining P1 commercial/copy clusters | **OPEN** |
| Classification research queue | **OPEN** |
| Paid model/research spend | **None** |
| Merge / deploy / publish | **NOT AUTHORIZED** |

**Recommend:** Accept this package as the completed semantic baseline. Continue shared-generator remediation for top P1 codes; authorize a second-pass re-score wave after those land; keep the 63 remap-verify IDs on the research queue.
