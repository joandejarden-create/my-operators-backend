# Operator Fit Engine — Audit Executive Summary

**Date:** 2026-08-03  
**Scope:** Read-only current-state audit + architecture recommendation  
**Evidence:** Code inventory, live Airtable completeness (`reports/operator-fit-airtable-readonly.json`, Active n=24), synthetic score simulation (`reports/operator-fit-score-simulation.json`), prior OAS Phase 5A–5F docs/reports  
**Production changes:** None (scoring, mappings, UI behavior, Airtable writes untouched)

---

## Direct answers

### 1. What operator matching functionality exists today?

Deterministic **Operator Alignment Score (OAS)** ranking Active Operator Setup companies for a deal; profile-level pathway signals; Operator Strategy table; Operator Deal Requests with persisted alignment; deal-aware Explorer panel. Product copy intentionally avoids “recommended operators.”

### 2. Where is it located?

- Score: `api/my-deals.js` → `scoreOperatorMatchForDeal`  
- Factors/weights: `lib/operator-alignment-scoring-factors.js`, `lib/operator-alignment-scoring-weight-config.js`  
- Companies snapshot: `lib/operator-alignment-company-utils.js`, `api/operator-alignment-snapshot.js`  
- UI: `/operator-alignment-snapshot`, My Deals Operator Strategy (`public/js/operator-strategy-my-deals.js`)  
- Brand side (related): `api/match-score-server.js` Brand Match v2  

### 3. What information does it use?

Deal Location/MP/SI (structured-first) vs operator prefill: geo, chain scale, assets/stages, management structures, services, systems/reporting, owner-relations text, brands, fee text, less-ideal text.

### 4. What Airtable tables/fields support it?

Operator Setup Master + Profile + Platform + Commercial + Governance; Deals + Location + MP + SI; Operator Deal Requests. Critical structured fields (Active Countries, Offered Services, Management Structures, etc.) are **mostly empty** on Active operators (often ~8–12% populated).

### 5. How is the current score calculated?

Weighted average of up to 10 factors (weights sum 90). Null factors excluded. Notable: fee factor **flat 75** when any text both sides; owner-relations **70/90 keywords**; negative “penalty” weight **2** defaulting to **100**; no hard eligibility gates.

### 6. Why are operators insufficiently differentiated?

Combination of: (A) sparse structured data; (B) broad taxonomies; (C) scoring that rewards table-stakes service/systems presence and breadth; (D) incomplete mandate inputs / weak governance compare; (E) single score without pathway/evidence/risk layers. Synthetic sim: generic claims operator ranked #1 in **4/8** scenarios; sparse-data operator reached **94.9** when few factors scored.

### 7. Which current data is genuinely useful?

Active status; chain scales (100% on Active); linked brands (~87%); deal SI structured management/operating/service fields; Location geo/scale; Case Study **table shape**; Brand Match v2 for brands; OAS breakdown UI patterns.

### 8. Which current data is generic or low-value (for scoring)?

Presence of RM/sales/marketing/procurement/HR/digital/accounting checkboxes; fee narrative presence; owner-relations keyword fluff; undifferentiated `bf_not_ideal_for` marketing; Explorer JSON as if it were evidence.

### 9. Which current owner questions are useful?

Project Type, geo/scale/rooms, Brand Agreement Structure, Operating Model, Preferred Management Structure, Must-Have/Required Operator Services, Market Presence Requirement, Pre-Opening Support, reporting expectations, Preferred Brands, deal breakers (if strengthened).

### 10. Which additional owner questions are truly necessary?

Primary **operating mandate**; ranked top value-creation objectives; stronger **approval-rights / involvement** (extend existing); conditional **complexity flags** (residences, meetings); conditional **operating challenges** for turnaround. Do **not** duplicate table-stakes service asks as scored positives. Preserve structure value set with mapping discipline.

### 11. Which operator information is missing?

Verified comparables/performance; brand approval compatibility; capacity/execution risk; economics bands; evidence source status at claim level; conversion/reflag experience population; outcome learning.

### 12. What can be visualized immediately?

Top-N alignment cards, band/confidence badges, factor bars, why/validate/concerns panels, print PDF — using OAS/BAS patterns.

### 13. What visualizations need more data?

Pathway matrices (brand×operator×structure), performance evidence tiers, true eligibility chips, sensitivity that is trustworthy, lender-grade proof rows.

### 14. What existing UI components can be reused?

`match-score-breakdown-ui`, OAS/BAS snapshot book + print, My Deals Strategy table, deal-compare sticky compare, Explorer chips/badges/export.

### 15. Smallest credible first Operator Fit Engine?

**Eligibility gates + de-genericized deterministic Operator–Project fit + Top-5 alignment cards with layer/factor explanations and explicit Unknowns** — still powered by structured fields, **without** claiming verified performance. Keep Brand Match v2 for brands. Feature-flag beside legacy OAS.

### 16. What should be retained?

Brand Match v2; deal intake mappings; OAS/BAS UX patterns; Operator Setup new-base schema; Case Studies; Operator Deal Requests junction; structured SI operator fields; weight-config SSOT pattern; Explorer as evidence/presentation.

### 17. What should be revised?

OAS factor design (no positive points for table-stakes); fee/owner-relations placeholders; negative-fit as real risk layer; missing-data inflation; dual band vocab; populate structured geo/services/structures; wire comparables into fit later.

### 18. What should be deprecated?

Reliance on legacy MP `Preferred Deal Structure` for operator path; legacy `brand-fit-analyzer` / Brand Explorer fixed fit constants as if they were Fit Engine; scoring from Explorer marketing JSON; GTM/CoStar conflation with product fit.

### 19. What should not be built yet?

LLM numeric scoring; full outcome auto-learning into weights; radar-first UI; wholesale intake replacement; performance-heavy rankings before evidence warehouse; unconstrained owner weight sliders.

### 20. Founder decisions required?

See below.

---

## Current-state verdict

**Partially usable, fragmented, and unsafe to extend by “tuning weights only.”**

More precisely: the foundation is **real server-side scoring + solid brand matching + strong presentation patterns**, but operator scoring is **structurally weak for differentiation** (generic-capability bias + sparse Airtable population + single opaque average). Explorer content quality can outrun the numeric model. Treat as **extendable only after eligibility/evidence refactor**, not as a finished Fit Engine.

## Recommended MVP boundary

1. Freeze principles: unknown≠no; table-stakes ≠ points; eligibility first.  
2. Populate eligibility-critical operator fields on Active universe (geo, structures, non-generic differentiators).  
3. Ship **Top-5 Operator Alignment** cards + breakdown (honest labeling) on a feature-flagged new fit DTO.  
4. Keep Brand Match v2 + intake unchanged.  
5. Defer pathway matrix, performance scores, and sensitivity until Phase 4–5 data exists.

## Recommended first visualization

**Ranked Top-5 Operator Alignment cards + existing horizontal factor bars** (OAS/BAS visual language).

## Founder decisions required

1. **Structure vocabulary:** map product list (incl. Lease, Franchise Only, Asset Management, To Be Confirmed) onto live SI/Commercial options without silent renames.  
2. **Brand-managed pathways:** are brand-managed options first-class pathways beside third-party operators in v1?  
3. **Scoring philosophy:** eligibility-first multi-layer vs continue single OAS average.  
4. **Generic services:** confirm table-stakes never add positive points.  
5. **Operator shortlist:** new Airtable object vs extend Operator Deal Requests.  
6. **Confidence policy:** can Strong band appear when evidence is only operator-reported?  
7. **Data enrichment ownership:** operator-provided vs Dealality-researched vs outreach-only for comps/approvals.  
8. **Deprecation timeline** for legacy OAS numeric display once new engine ships.

---

## Verification checklist

| Check | Status |
| ----- | ------ |
| Repo searched beyond operator/match filenames | Yes |
| Airtable read-only | Yes (`audit-operator-fit-airtable-readonly.mjs`) |
| No production logic altered | Yes |
| No mappings changed | Yes |
| No validated data overwritten | Yes |
| Conclusions cite code/Airtable/reports | Yes |
| Assumptions labeled | Yes (Active n=24 snapshot; synthetic fixtures) |
| Unknown not called absent | Yes |
| Marketing claims ≠ verified performance | Yes |
| Proposed questions avoid duplicating intake | Yes |
| Tests run | Phase 5E **pass**; companies validator **pass**; OAS page test **2 pre-existing FAILs** (My Deals action/compact preview contracts); synthetic sim **ran**; no exhaustive pure unit suite for all factors |

---

## Documents in this package

| Document | Path |
| -------- | ---- |
| Code audit | `docs/audits/operator-fit-current-state-code-audit.md` |
| Airtable audit | `docs/audits/operator-fit-current-state-airtable-audit.md` |
| User flow | `docs/audits/operator-fit-current-user-flow.md` |
| Scoring audit | `docs/audits/operator-fit-current-scoring-audit.md` |
| Visualization options | `docs/audits/operator-fit-visualization-options.md` |
| Proposed architecture | `docs/architecture/operator-fit-engine-proposed-architecture.md` |
| Build plan | `docs/architecture/operator-fit-engine-build-plan.md` |
| This summary | `docs/audits/operator-fit-audit-executive-summary.md` |
| Read-only AT report | `reports/operator-fit-airtable-readonly.json` |
| Score simulation | `reports/operator-fit-score-simulation.json` |
| Audit scripts | `scripts/audit-operator-fit-airtable-readonly.mjs`, `scripts/audit-operator-fit-score-simulation.mjs` |
