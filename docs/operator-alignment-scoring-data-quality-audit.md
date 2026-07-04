# Operator Alignment — Scoring & Data Quality Audit (Phase 5A)

**Date:** 2026-05-25  
**Repo:** `deal-capture-proxy`  
**Scope:** Read-only audit and planning. No code, Airtable, weight, BAS, OCS, or PDF layout changes.  
**Related:** [operator-alignment-field-matrix.md](./operator-alignment-field-matrix.md), [operator-alignment-snapshot-phase-4.md](./operator-alignment-snapshot-phase-4.md), [operator-alignment-recommended-airtable-fields.md](./operator-alignment-recommended-airtable-fields.md)

**Live diagnostic artifact:** `docs/_audit-scoring-sample-recIeGRZP21udmTnt.json` (generated via `node scripts/audit-operator-alignment-scoring.mjs recIeGRZP21udmTnt`)

---

## Executive summary

Company-level Operator Alignment scores are **not failing because of layout** or because operators lack Mexico/Cancún market strings on this sample deal. For `recIeGRZP21udmTnt` (Aeropuerto Cancún, Mexico, Upper Midscale, New Build), **all 10 active operators score geo = 100** and cluster **54–69** (average **64.1**), almost entirely in **Moderate** or **Conditional** bands.

The primary score suppressors on this deal are:

1. **Deal structure mismatch** — deal `Preferred Deal Structure` is **Franchise Only** while Operator Setup profiles document third-party / management-style structures → factor score **20** (weight **12**), identical across operators.
2. **Service must-haves token mismatch** — deal must-haves (`Strong Distribution and Marketing Support`, `Experienced Operator`, etc.) do not overlap operator service arrays → **30** (weight **8**), identical pattern.
3. **Partial asset/stage fit** — New Build + Mid-Rise vs operator asset/situation multis → ~**59.5** (weight **14**).
4. **Ceiling from non-discriminating factors** — `systemsReporting` and `ownerRelations` score **70–90** from operator-side presence alone; `feeCommercial` returns flat **75** when both sides have any text — these do not separate operators much.

**Repetitive PDF/UI copy** is largely a **presentation-layer issue**: `alignmentSignalsFromBreakdown` emits templated phrases; `humanizeCompanyAlignmentSignal` / `humanizeCompanyReviewConsideration` collapse factor labels into the same bullets; `buildCompanyWhatSupports` / `buildCompanyWhatNeedsValidation` **merge hardcoded default bullets** for every company; `companyReviewStatus` maps entire bands to one status string; `companyKeyConsideration` takes only the first humanized review line (often the same management-structure sentence).

**Missing data is often scored as weak alignment** (e.g. empty operator markets → geography **35**, empty chain scales → **45**), not **Insufficient Data**. Only company-level completeness gating (`assessOperatorDataCompleteness`) blocks scoring entirely.

---

## 1. Current scoring inputs (`scoreOperatorMatchForDeal`)

**Source:** `api/my-deals.js` — `OPERATOR_MATCH_WEIGHTS`, `scoreOperatorMatchForDeal` (~2794–3038).  
**Denominator:** Sum of all factor weights = **90** (every factor always contributes weight; null scores add weight but **zero** to numerator).

| Factor | Weight | Deal fields used | Operator fields used (prefill keys) | Scoring method | Score range / bands | Missing-data behavior | API signal text (via `alignmentSignalsFromBreakdown`) | Contributes to repetition? |
|--------|--------|------------------|-------------------------------------|----------------|-------------------|----------------------|--------------------------------------------------------|---------------------------|
| **Geography & Markets** | 18 | `Country` (Location / deal); note in breakdown | `specificMarkets`, `market_fit`, `topMarkets`, `regionsSupported`, `bestFitGeographies` | If deal country and op markets: substring `market.includes(country)` → **100** else **35**. No country → **60**. No op markets → **35** | 35, 60, 100 | Empty op markets → **35** (weak), not gap | ≥75: "Potential alignment on geography & markets…"; 50–74: conditional; &lt;50: limited | **Yes** — same geo line when all pass substring (Cancún list includes "Mexico" via city strings) |
| **Chain Scale** | 8 | `Hotel Chain Scale` (Location) | `chainScale`, `chainScalesYouSupport`, `chain_scales` | Exact → **100**; partial substring → **65**; else **25**; no op scale → **45** | 25, 45, 65, 100 | No deal scale → factor **null** (—); no op → **45** | Same template per factor label | **Yes** — "chain scale" phrase identical |
| **Asset / Project / Stage Fit** | 14 | `Project Type`, `Building Type`, `Stage of Development` | `bestFitAssetTypes`, `propertyTypesManaged`, `operatingSituations`, token scan on asset/stage keys | `overlapScore` 70% project/building vs assets + 30% stage vs situations; partial default **30–35** | ~30–100 via overlap | Both empty → **null** | Conditional/limited templates | **Yes** — "asset / project / stage fit" |
| **Deal Structure / Assignment** | 12 | `Preferred Deal Structure` (MP) | `bestFitDealStructures`, `typicalAssignmentTypes`, `serviceModels`, structure token scan | Exact → **100**; partial substring → **65**; else **20**; no op → **45** | 20, 45, 65, 100 | No deal structure → **null** | **Yes** — dominant suppressor on sample deal; identical "limited… deal structure" |
| **Fee / Commercial** | 10 | Royalty / Marketing / Loyalty fee expectations (MP env aliases); structure in display | `feeStructureSummary`, `operatorFeeApproach`, commercial narratives, fee token scan | Both sides present → **75**; one side → **55**; neither → **null** | 55, 75 | No comparison of fee levels | "Potential alignment on fee / commercial" | **Yes** — flat 75 for most operators |
| **Service Offerings** | 8 | `Must-Haves From Brand/Operator` | `primaryServices`, granular service arrays, `serviceDifferentiators`, etc. | No must-haves → **75**; else `overlapScore` (partial **30**) | 30, 75, overlap | Empty op services with must-haves → low overlap | "Review service offerings…" | **Yes** — must-have token mismatch → same **30** |
| **Systems & Reporting** | 6 | Reporting fields read for display only (`Owner Reporting Frequency`, etc.) | `technologySystems`, `systemsStack`, `reportTypesProvided`, `ownerReportingCadence` | Op systems + reporting → **90**; systems only → **70**; neither → **40** | 40, 70, 90 | **Does not compare** deal vs operator cadence | Often "strong" despite no deal match | **Yes** — inflates similarly for all |
| **Owner Relations** | 6 | **Hardcoded** deal line: "responsive communication and collaboration" | `ownerCommunicationStyle`, `operatingCollaborationMode`, etc. | Keywords → **90**; else **70**; no op text → **45** | 45, 70, 90 | Deal side not from intake | Conditional template | **Yes** — same deal value every operator |
| **Brand / Portfolio Relevance** | 6 | `Preferred Brands` (SI) | `brands`, `brandsManaged` (ids resolved to names) | No deal brands → **70**; else `overlapScore` (partial **25**) | 25, 70, overlap | Brand name token mismatch | Brand review lines | **Yes** when preferred brands set |
| **Negative-Fit Penalty** | 2 | `Top 3 Deal Breakers` | `lessIdealSituations`, `less_proven_areas` | Breaker substring in less-ideal → **20**; else **100** | 20, 100 | Usually **100** | Rarely shown | Low |

**Overlap helper (`overlapScore`):** Returns **null** if either set empty; **partial** (default 25–35) if intersection empty; else **40 + (intersection/dealSize)×60**.

**Company band mapping** (`lib/operator-alignment-company-utils.js`): Strong ≥80, Moderate ≥65, Conditional ≥50, Limited ≥35, else Insufficient Data — after `assessOperatorDataCompleteness` allows scoring.

---

## 2. Sample deal diagnosis — `recIeGRZP21udmTnt`

**Deal context (live Airtable):** Mexico, Upper Midscale, New Build, `Preferred Deal Structure`: **Franchise Only**, must-haves include distribution/marketing and experienced operator.

**Distribution (10 active operators):** 0 Strong (80+); 5 Moderate (65–79); 5 Conditional (50–64). Average **64.1**.

### Top 10 operators (live breakdown summary)

| Operator company | Total score | Band | Strong factors (≥75) | Conditional (50–74) | Limited (&lt;50) | Missing factors | Main suppressors (drag = weight×(100−score)) | Repeated narrative drivers | Data completeness |
|------------------|------------|------|------------------------|----------------------|------------------|-------------------|---------------------------------------------|---------------------------|-------------------|
| Viento Sur Gestión Hotelera | **69.4** | Moderate | geo, chain, fee, systems, penalty | asset, owner, brand | **structure (20)**, services (30) | structure 960; asset 567; services 560 | Same 5 `alignmentSignals`; structure + services `reviewConsiderations`; UI defaults | sufficient |
| Mangle Azul Hospitalidad | **66.4** | Moderate | geo, chain, fee, systems, penalty | asset, owner | structure (20), services (30), brand (25) | structure 960; asset 567; services 560; brand 450 | + brand review line | sufficient |
| Panamerican Lodging Partners S.A. | **66.4** | Moderate | same pattern | same | same | same | identical signal ordering | sufficient |
| Barrio Hotelero CDMX | **66.4** | Moderate | same | same | same | same | identical | sufficient |
| Metro Lodging São Paulo | **66.4** | Moderate | same | same | same | same | identical | sufficient |
| Cenote Azul Operadores | **64.7** | Conditional | geo, fee, systems, penalty | chain, asset, owner | structure, services, brand | structure 960; services 560 | band → "Review if owner confirms operating path" | sufficient |
| Antillano Norte Hospitality Group | **64.7** | Conditional | same | same | same | same | same status string as other Conditional | sufficient |
| Río Plata Hotel Partners | **63.3** | Conditional | geo, fee, systems, penalty | chain (65), asset, owner | structure, services, brand | + chain partial 280 | chain conditional wording only delta | sufficient |
| Oro Verde Lodge & Hotel Operators | **59.7** | Conditional | geo, fee, systems, penalty | asset, owner | chain, structure, services, brand | chain 280 + structure 960 | more "limited" signals | sufficient |
| Hotel Equities (CALA) | **54.0** | Conditional | geo, systems, penalty | fee, owner | chain, asset, structure, services, brand | chain 560; structure 960; services 560 | lowest score; still same UI fallbacks | sufficient |

**Why scores sit in the 60s (not geography):**

- Geography is **100** for all ten (market strings include Mexico/Cancún).
- **Structure score 20** for all: franchise-only deal vs operator profiles oriented to management / third-party structures — treated as **misalignment**, not "needs validation" or insufficient data.
- **Services score 30** for all with must-haves: free-text must-haves do not token-match granular service checkboxes.
- **Asset ~59.5** for most: partial overlap on New Build / select-service vs broad asset multis.
- **Moderate band starts at 65** — scores 66–69 are "good enough" on geo/chain/systems but cannot reach 80 without fixing structure/services/asset factors.
- **No operator reaches Strong** because no profile clears structure + services + asset simultaneously at high weights.

**Fields causing score suppression (deal-side):** `Preferred Deal Structure` = Franchise Only (likely inconsistent with operator pathway on this deal); `Must-Haves From Brand/Operator` phrasing; optional `Preferred Brands` driving brand factor 25.

**Fields causing repeated narrative:** Factor-level templates + `public/js/operator-alignment-snapshot.js` hardcoded bullets in `buildCompanyWhatSupports` / `buildCompanyWhatNeedsValidation`; `companyReviewStatus` / `companyKeyConsideration`.

---

## 3. Missing / weak deal fields (intake)

| Field (requested) | Exists today? | Existing field name | Quality | Recommended Airtable action | Suggested type / options | Scoring impact | Narrative impact | Risk if changed |
|-------------------|---------------|---------------------|---------|----------------------------|--------------------------|----------------|------------------|-----------------|
| Operator Review Status | Partial | `Operator Strategy Status` (SI) | partial — gating, not in `scoreOperatorMatchForDeal` | Keep; add explicit `Operator Review Status` if workflow differs | single select | Gates OAS sections; future weight 0 until mapped | Pathway copy | Form + readiness |
| Preferred Operator Type | Partial | `Preferred Third-Party Operator Profile` (SI multi) | partial — taxonomy mismatch | Keep; normalize to archetype ids | multi select | Profile layer; future factor | Pathway table | Option migration |
| Required Operator Services | Partial | `Services Required From Operator` (SI) | partial — not in overlap with must-haves | Keep; align tokens with operator granular list | multi select | High — service factor | Specific service bullets | Break overlap if rename |
| Must-Have Services | Partial | `Must-Haves From Brand/Operator` | partial — used; token mismatch | Normalize options + synonym map | multi select | High — currently drags to 30 | Differentiated validation list | SI PATCH aliases |
| Preferred Management Structure | Partial | `Preferred Deal Structure` (MP) | **weak on sample** — franchise vs operator path | Add dedicated field or split brand vs operator structure | multi select | **Critical** — structure factor | Stops false "limited" for all | MP semantics |
| Owner Control Preference | Partial | `Owner Control Priorities` (SI) | weak | Add `Owner Control Preference` select | single select | New owner-relations compare | Owner-operated pathway | New field |
| Owner Reporting Expectations | Partial | `Owner Reporting Frequency`, `Owner Reporting Package`, `Preferred Reporting Frequency` | partial — duplicate semantics | Consolidate display; keep one canonical | select + text | Fix systemsReporting compare | Reporting-specific line | Alias cleanup |
| Pre-Opening Support Needed | Partial | `Operator Capability Priorities`, `Opening / Transition Phase` | partial | Add boolean or level on SI | select | Stage/asset factor | Pre-opening sentence when true | OCS overlap |
| Opening Timeline | Partial | `Stage of Development`, `Opening / Transition Phase` | partial | Add date or horizon select on Deals | select / date | Stage factor | Timeline-specific narrative | Low |
| Brand Affiliation Path | Partial | `Current Brand Affiliation`, brand strategy fields | weak | Add `Brand Affiliation Path` (franchise / mgmt / soft brand / unbranded) | select | Structure + brand factors | Pathway row 5 | New |
| Brand / Operator Responsibility Split | Missing | — | missing | **Add** SI multi | multi select | New factor (documentation) | F&B / ops split bullets | New |
| F&B Complexity | Partial | `F&B Outlets?`, F&B program (Deals/Location) | partial — not in engine | Keep; map to level | select | Future factor | F&B validation bullet | Low |
| Commercial Priority | Partial | `Revenue / Yield Management Importance`, `Marketing & Distribution Importance` | partial | Add `Commercial Priority` rank | select | Weight service/RM subscores | Commercial emphasis line | New |
| Market Presence Requirement | Missing | — | missing | **Add** (active ops vs pipeline) | select | Geography semantics | "Active Cancún ops" vs pipeline | New |
| Local Labor / HR Support Needed | Partial | priorities / services | weak | Add explicit SI boolean | checkbox | Service overlap | HR bullet | New |
| Procurement Support Needed | Partial | services / priorities | weak | Add explicit SI boolean | checkbox | Service overlap | Procurement bullet | New |
| Owner Internal Ops Capability | Missing | — | missing | **Add** SI level | select | Owner-operated pathway | Delegation narrative | New |

---

## 4. Missing / weak operator setup fields

| Field (requested) | Exists today? | Existing field / source | Quality | Recommended action | Type / options | Scoring | Narrative | Operator Setup UI | Explorer | Admin-only |
|-------------------|---------------|-------------------------|---------|-------------------|----------------|---------|-----------|---------------------|----------|------------|
| Active Countries | Partial | Footprint `geo_*`; prefill `regionsSupported` | weak — inferred | **Add** multi country | multi | Geography | Country-specific line | Yes | Yes | No |
| Active Markets / Cities | Partial | `specificMarkets`, `mkt_*` | partial — long text | Normalize to multi | multi | Geography | "Cancún" not only "Mexico" | Yes | Yes | No |
| Market Presence Type | Missing | — | missing | **Add** | select: Active / Pipeline / Both | Geography | Active vs pipeline | Yes | Yes | No |
| Number of Hotels in Market | Partial | footprint totals | weak | **Add** per-market or region count | number | Low | Proof point | Optional | Yes | No |
| Service Models Supported | Partial | `primaryServiceModel`, `bf_selected_deal_structures` | partial | Keep; normalize | multi | Structure + services | Service model line | Yes | Yes | No |
| Chain Scales Supported | Strong | `chainScalesSupported` | strong | Keep | multi | Chain factor | Scale line | Yes | Yes | No |
| Preferred Asset Types | Strong | `bf_selected_asset_types`, prefill assets | strong | Keep | multi | Asset factor | Select-service mention | Yes | Yes | No |
| New-Build Opening Experience | Partial | `bf_selected_situation_types`, case studies | partial | **Add** explicit tag | multi | Asset/stage | Pre-opening sentence | Yes | Yes | No |
| Conversion / Reflag Experience | Partial | `brand_signal_*`, situations | partial | **Add** level | select | Asset factor | Conversion line | Yes | Partial | No |
| Pre-Opening Support Capability | Partial | granular services, `cap_kpi_transition` | partial | **Add** level | select | Stage | Pre-opening | Yes | Partial | No |
| Brand Families Operated | Strong | `brands` link | strong | Keep | link | Brand factor | Hilton/Marriott line | Yes | Yes | No |
| Soft Brand / Lifestyle Experience | Partial | `brand_signal_soft_retention` | partial | **Add** multi | multi | Brand/profile | Lifestyle pathway | Yes | Yes | No |
| F&B Capability Level | Partial | granular F&B services | partial | **Add** level | select | Future | F&B bullet | Yes | Partial | No |
| Revenue Management Capability | Partial | `revenueManagementServices` | partial | Aggregate level | select | Services | RM line | Yes | Yes | No |
| Sales Platform | Partial | `salesMarketingSupport` | partial | Aggregate | select | Services | Distribution line | Yes | Partial | No |
| Owner Reporting Level | Partial | `ownerReportingCadence`, `infra_*` | partial | **Add** select | select | Systems | Governance line | Yes | Yes | No |
| Governance Cadence | Partial | reporting fields | partial | Same as reporting | select | Systems | Same | Yes | Partial | No |
| Management Structure Preference | Partial | `bf_selected_deal_structures` | partial | **Add** explicit | multi | Structure | Stops franchise false negative | Yes | Yes | No |
| Minimum Key Count | Missing | — | missing | **Add** | number | Future filter | Explorer filter | Optional | Yes | No |
| Typical Fee Structure | Partial | narratives, legacy Deal Terms | weak | **Add** structured | select + text | Fee factor | Neutral fee note | Partial | No | Partial |
| Termination Flexibility | Partial | legacy termination fields | partial — not in new-base read path | Wire to prefill | select | Commercial | Review only | Partial | No | Yes |
| Similar Project Case Studies | Strong | Case Studies child | strong | Keep | child | Inference only | Case study sentence | Yes | Yes | No |
| Data Confidence / Source Type | Missing | — | missing | **Add** admin | select | Gating | Insufficient Data | No | No | **Yes** |
| Last Updated Date | Partial | Airtable `Last modified` | partial | Surface in API | datetime | Staleness flag | "Data as of…" | No | Optional | Yes |

---

## 5. Missing-data scoring behavior (audit)

| Current behavior | Where | Problem |
|------------------|-------|---------|
| Null factor score still counts in **denominator** | `scoreOperatorMatchForDeal` loop | Missing factor pulls total down vs excluding weight |
| Empty operator geography → **35** | geography factor | Reads as weak CALA fit, not "unknown markets" |
| Empty operator chain → **45** | chain factor | Same |
| Empty operator structure → **45** | structure factor | Same |
| `systemsReporting` / `ownerRelations` almost always numeric | factors | Missing deal preferences still score operator |
| Company `assessOperatorDataCompleteness` | company-utils | Blocks only when profile/markets/scales missing — partial records still score |
| No factor-level **Insufficient Data** | engine | Only company band can be Insufficient Data |

### Recommended handling per factor (future — Phase 5E)

| Factor | Recommend when deal or operator input missing |
|--------|-----------------------------------------------|
| Geography | Op markets missing → **data gap** + exclude weight OR cap band; deal country missing → gap, no penalty |
| Chain scale | Op missing → gap; deal missing → exclude |
| Asset/stage | Either missing → gap for that sub-score; exclude weight if both empty |
| Structure | **Do not score 20** if deal structure is franchise-only but operator path is third-party — map brand vs operator structure; if op missing → gap |
| Fee/commercial | Missing → gap only; if both present but not comparable → "Needs validation" not 75 |
| Services | Must-haves missing → exclude; op services missing → gap not 30 |
| Systems/reporting | Deal reporting missing → exclude or gap; require cadence match for points |
| Owner relations | Require deal field; no hardcoded deal line |
| Brand | No preferred brands → exclude weight; op brands missing → gap |
| Negative penalty | No breakers → exclude weight (already 100) |

**Principle:** Missing data → **Insufficient Data** / **Needs Validation** / listed **Data gap** — not automatic low match score.

**When to hide numeric score:** completeness `insufficient`, or &lt;3 scored factors, or &gt;40% factors in gap state (proposed).

---

## 6. Revised scoring strategy (recommendation — not implemented)

**Goals:** Separate true weak alignment, missing data, conditional fit, and strong fit; remain explainable and non-advisory.

### Proposed model (documentation weights sum to 100)

| Proposed factor | Weight | Required deal fields | Required operator fields | Optional fields | Missing handling |
|-----------------|--------|----------------------|--------------------------|-----------------|------------------|
| Geography & active presence | 16 | Country, market presence requirement | Active countries, active markets, presence type | City, pipeline | Gap → exclude; no 35 penalty |
| Chain scale | 8 | Hotel Chain Scale | Chain scales supported | — | Gap if op empty |
| Asset & stage fit | 12 | Project Type, Building Type, Stage | Asset types, situations, new-build tag | Opening phase | Partial overlap; gap if op empty |
| Management structure fit | 14 | Preferred **operator** structure (new field) | Management structures supported | Brand affiliation path | Mismatch → weak; missing → gap |
| Service platform overlap | 10 | Required services, must-haves | Granular services (normalized) | Commercial priority | Token map; gap if op empty |
| Commercial documentation | 8 | Fee expectations (optional) | Fee structure summary (structured) | — | Present → "needs validation" not 75 |
| Systems & reporting fit | 8 | Owner reporting expectations | Reporting level, systems | — | Compare tokens; gap if deal empty |
| Owner collaboration fit | 6 | Owner control preference | Collaboration mode | — | Both required or gap |
| Brand portfolio overlap | 8 | Preferred brands | Brand families operated | Soft brand flag | Exclude if no preferred brands |
| Negative-fit check | 4 | Deal breakers | Less ideal situations | — | Penalty only on explicit overlap |
| Data confidence modifier | 4 | — | Data confidence, last updated | — | Caps band or hides score |

**Bands (proposed):** Strong ≥82, Moderate 68–81, Conditional 52–67, Limited 36–51, Insufficient Data below 36 or gating failed.

**Numeric score visibility:** Show only when ≥6 factors scored with real inputs and confidence ≥ medium; else band + gap list.

---

## 7. Narrative differentiation (recommendation — Phase 5F)

**Root causes today:**

1. `alignmentSignalsFromBreakdown` — five templates keyed only by factor label and score tier.  
2. `humanizeCompanyAlignmentSignal` — maps factor names to seven fixed sentences.  
3. `buildCompanyWhatSupports` — merges signals with **four hardcoded bullets** for every company.  
4. `companyReviewStatus` — one string per band (e.g. all Moderate → "Review if market coverage is confirmed").  
5. `companyKeyConsideration` — first review line only → same management-structure sentence.

**Recommendations (use only real fields):**

| Condition | Example output (neutral) |
|-----------|---------------------------|
| `specificMarkets` contains Cancún and deal submarket airport | "Operator Setup lists Cancún among active markets." |
| `bf_selected_situation_types` includes new-build / pre-opening | "Operating situations include new-build or pre-opening." |
| `chainScalesSupported` exact match Upper Midscale | "Chain scale includes Upper Midscale in Operator Setup." |
| `brands` resolves to Hilton/Choice/Marriott | "Brand portfolio includes [names] from Operator Setup." |
| `ownerReportingCadence` = Weekly | "Documented owner reporting cadence: Weekly." |
| Structure mismatch franchise vs management | "Preferred deal structure (franchise-only) differs from management structures documented on the operator profile — validation needed." **Not** "limited alignment." |
| Must-haves present, overlap null | "Must-haves are recorded on the deal; service overlap was not matched to Operator Setup checkboxes — confirm scope." |
| Factor gap | Single bullet: "Supported markets not documented in Operator Setup." |

**Remove** unconditional default bullets from `buildCompanyWhatSupports` / `buildCompanyWhatNeedsValidation`; show **Data Gaps** section when gaps exist.

**Do not** invent market or brand facts; use breakdown `dealValue` / `operatorValue` strings when specific.

---

## 8. Airtable schema changes

See **[operator-alignment-recommended-airtable-fields.md](./operator-alignment-recommended-airtable-fields.md)** for the prioritized field list (Priority 1–3).

---

## 9. Implementation sequence

| Phase | Scope | Deliverables |
|-------|--------|--------------|
| **5A** (this doc) | Diagnostics only | Audit docs, `scripts/audit-operator-alignment-scoring.mjs`, sample JSON |
| **5B** | Airtable schema + normalization | New fields, option vocabularies, synonym maps (no scoring change yet) |
| **5C** | Operator Setup forms | Capture Priority 1–2 operator fields |
| **5D** | Deal Intake | Capture Priority 1–2 deal fields; structure split |
| **5E** | Scoring logic | **Done 2026-05-25** — see `docs/operator-alignment-phase-5e-score-wiring-results.md` |
| **5F** | Narrative | Remove hardcoded merges; field-specific copy |
| **5G** | Operator Explorer | Completeness %, deal-context badges |

---

## 10. Recommended next Cursor prompt (Phase 5B)

After you review this audit, use:

```text
Implement Phase 5B only: Operator Alignment Airtable schema additions and normalization per docs/operator-alignment-recommended-airtable-fields.md Priority 1 and Priority 2.

Constraints:
- Do NOT change scoreOperatorMatchForDeal weights or logic yet.
- Do NOT change Brand Alignment Snapshot, Operator Capability Snapshot, or OAS PDF layout.
- Do NOT add advisory/recommendation language in UI copy.
- Add Airtable fields on Deals (Strategic Intent / Market Performance) and Operator Setup new-base tables as documented.
- Add normalization maps (deal structure vs operator structure, service must-have synonyms, country/market parsing) in lib/ or api/lib/ without wiring them into scoring yet.
- Update deal-setup-fields.js and operator setup field bindings so forms read/write new fields.
- Provide a short migration checklist for backfilling existing operator records (manual steps OK).
- Run existing validate scripts; add a script to verify new fields are readable via prefill.

Reference: docs/operator-alignment-scoring-data-quality-audit.md and docs/operator-alignment-recommended-airtable-fields.md
```

---

## Appendix A — UI repetition map (company cards)

| Mechanism | File | Effect |
|-----------|------|--------|
| `buildCompanyWhatSupports` defaults | `public/js/operator-alignment-snapshot.js` ~1680–1690 | Every card gets "Market overlap indicated…", "Chain-scale overlap…", etc. |
| `buildCompanyWhatNeedsValidation` defaults | ~1696–1706 | Same validation bullets on every card |
| `companyReviewStatus` | ~521–527 | Band → single status string |
| `companyKeyConsideration` | ~530–542 | First review consideration only |
| `humanizeCompanyAlignmentSignal` | ~1562+ | Factor label → fixed phrase |
| `alignmentSignalsFromBreakdown` | `lib/operator-alignment-company-utils.js` ~219 | "Potential alignment on {label}…" |

---

## Appendix B — Code references

- Weights and factors: `api/my-deals.js` (`OPERATOR_MATCH_WEIGHTS`, `scoreOperatorMatchForDeal`)
- Company wrap: `lib/operator-alignment-company-utils.js`
- Field catalog (legacy): `lib/third-party-operator-airtable-fields-used.js`
- Operator Setup read: `api/lib/operator-setup-new-base-read.js`
- Deal fields: `api/schemas/deal-setup-fields.js`
