# Operator Fit Engine — Current Scoring Audit

**Date:** 2026-08-03  
**Engines audited:** Operator Alignment Score (`scoreOperatorMatchForDeal`); Brand Match v2 (context only)  
**SSOT weights:** `lib/operator-alignment-scoring-weight-config.js`  
**Factor helpers:** `lib/operator-alignment-scoring-factors.js`  
**Aggregation:** `api/my-deals.js` lines ~3162–3190  
**Synthetic sim:** `reports/operator-fit-score-simulation.json` (local fixtures; no Airtable writes)  
**Live historical sample:** Phase 5F report `reports/operator-alignment-scoring-phase5f-final-recIeGRZP21udmTnt.json` (10 operators, deal `recIeGRZP21udmTnt`)

**Rule followed:** Formula not changed during this audit.

---

## 1. Exact current operator scoring logic

### Weights (fixed)

| Criterion | Weight |
| --------- | -----: |
| geographyMarkets | 18 |
| chainScale | 8 |
| assetProjectStageFit | 14 |
| dealStructureAssignment | 12 |
| feeCommercial | 10 |
| serviceOfferings | 8 |
| systemsReporting | 6 |
| ownerRelations | 6 |
| brandPortfolioRelevance | 6 |
| negativeFitPenalty | 2 |
| **Documented total** | **90** |

### Aggregation

```
finalScore = round( sum(score_i × weight_i) / sum(weight_i for non-null scores) , 1 )
clipped to [0, 100]
```

- Missing/null factors: **excluded from denominator** (`OPERATOR_MATCH_AGGREGATION`).  
- Company UI may hide numeric score unless completeness sufficient and ≥3 factors scored (`lib/operator-alignment-company-utils.js`).

### Criterion table

| Criterion | Source Field (deal / operator) | Trigger | Weight | Positive Points | Negative Points | Missing-Data Treatment | Gate or Score? | Evidence Requirement |
| --------- | ------------------------------ | ------- | -----: | --------------: | --------------: | ---------------------- | -------------- | -------------------- |
| Geography & Markets | Deal Country/City/Market Presence Requirement; Op Active Countries/Markets/Presence | Country/city/region match tiers | 18 | Up to 100 (city/country hits ~92–100; regional ~58) | Low tier ~48 unclear | Op geo empty → **null exclude**; deal country missing softer path | Score | None beyond field presence |
| Chain Scale | Location Hotel Chain Scale; Op chainScalesSupported | Exact / partial / else | 8 | 100 / 65 | 25 mismatch | Either empty → null exclude | Score | None |
| Asset / Project / Stage | Project Type, Building Type, Stage; Op assets/situations + pre-opening extras | Overlap blend | 14 | Overlap up to 100 | Partial defaults ~30–60 | Parts renormalized; empty → null | Score | None |
| Deal Structure | SI Preferred Management Structure, Operating Model, Brand Agreement; Op Management Structures Supported | Exact 100 / partial 72 / fuzzy ~48; legacy path 100/65/20 | 12 | Structure path match | Legacy mismatch 20 | Op structures empty → null + needs_validation | Score | None |
| Fee / Commercial | MP fee expectations; Op feeStructureSummary / commercial text | Both sides have **any** text | 10 | **Flat 75** | None | One/both missing → null | Score | **None — placeholder** |
| Service Offerings | Must-Have/Required Operator Services (+ scope); Op Offered Services + RM/pre-opening extras | Canonical overlap / fuzzy | 8 | Overlap 40–100; no deal reqs + op services → **75** | Limited overlap ~42 floor patterns | Op empty + deal reqs → null needs_validation | Score | Presence/overlap only |
| Systems & Reporting | Owner reporting fields; Op systems + reporting level/cadence | Op presence / some cadence | 6 | Typically 70–90 when present | Lower if absent systems | Often presence-weighted | Score | No verified reporting samples |
| Owner Relations | Owner Control Preference (weak); Op communication/collaboration text | Keyword weekly/monthly/collaborat/owner ref/advisory → 90 else 70 | 6 | 90 / 70 | None | Op empty → null | Score | Keyword only |
| Brand / Portfolio | Preferred Brands; Op brands | overlapScore | 6 | Overlap | Partial 25 | Either empty → null | Score | Link IDs preferred |
| Negative-Fit Penalty | Top 3 Deal Breakers; Op lessIdealSituations | Substring conflict → 20 else **100** | 2 | 100 default | 20 on conflict | If no breakers or no op text → **100** (not excluded!) | Score (not true gate) | None |

### Score range / normalization

| Item | Value |
| ---- | ----- |
| Theoretical max | 100 (all factors 100) |
| Theoretical min | Approaches low teens if all scored factors mismatch (e.g. structure 20–48, scale 25, geo low) — **not a hard floor** |
| Normalization | Weighted average of available factors only (renormalizes when factors drop out) |
| Weights dynamic? | **No** — fixed for every project |
| Hard eligibility gates? | **No** at operator engine (unlike Brand Match v2) |

### Company bands (UI)

| Min | Band |
| --: | ---- |
| 80 | Strong Alignment Signals |
| 65 | Moderate (company util) / 50 UI class medium depending on surface |
| 50 | Conditional |
| 35 | Limited |
| else / unscorable | Insufficient Data |

(UI class bands in weight config use 80/50/25/0; company util uses 80/65/50/35 — **dual band vocab** is a product confusion risk.)

---

## 2. Brand Match v2 (context — not operator score)

Weights in `lib/brand-match-scoring-weight-config.js` (sum 100 soft factors). Preferred bonus +4. Hard gates force 0. Insufficient if &lt;40% soft weight evaluable. This is the **Brand–Project Fit** engine today — stronger than OAS on gates and missing-data honesty.

---

## 3. Double-counting / generic capability inflation

| Issue | Evidence |
| ----- | -------- |
| Table-stakes services scored positively | `scoreServiceOfferingsFactor` overlaps Must-Have lists that include Revenue management, Sales, Owner reporting — same lists operators claim (`api/my-deals.js` service collection includes RM, sales, procurement, HR, tech, etc.) |
| Systems presence ≈ quality | `scoreSystemsReportingFactor` rewards systems/reporting fields without verified lender packs |
| Owner relations keyword | 90 vs 70 on tokens — not mandate fit |
| Fee flat 75 | Both sides text → 75 (`my-deals.js` ~3092–3097) |
| Negative fit defaults to 100 | Weight only 2; absence of less-ideal text yields perfect penalty factor |
| No deal services → 75 if operator lists services | `scoreServiceOfferingsFactor` when dealMust empty |
| Portfolio breadth ≈ relevance | Broad multi-select asset/geo/brand lists raise overlap odds |

---

## 4. Synthetic scenario results (current formula, unchanged)

Fixture operators: Generic Full-Service Claims; Yucatán Select Specialist; CALA Resort Luxury; Caribbean Turnaround; Commercial Support Boutique; Institutional Reporting Platform; Sparse Data; Wrong-Geo Broad Claims.

| Scenario | n | Mean | Median | Min | Max | Stdev | Within 5 of top | Top operator | Differentiation notes |
| -------- | - | ---: | -----: | --: | --: | ----: | --------------: | ------------ | --------------------- |
| Upper-upscale urban new build | 8 | 79.5 | 80.3 | 68.9 | 89.3 | 6.4 | 2 | **Generic Full-Service Claims** | Clustering; sparse scored 84.9 |
| Luxury leisure resort | 8 | 68.2 | 68.9 | 45.1 | 87.8 | 14.3 | 2 | CALA Resort Luxury | Best niche separation |
| Select-service conversion | 8 | 75.7 | 74.8 | 61.9 | 87.8 | 9.6 | 3 | **Generic** | Specialist not #1 |
| Mixed-use + residences | 8 | 72.0 | 68.3 | 60.5 | 86.2 | 8.3 | 2 | **Generic** | No residences factor |
| Large group/convention | 8 | 70.8 | 71.1 | 56.6 | 85.0 | 9.5 | 2 | **Generic** | No meetings capability factor |
| Independent lifestyle soft brand | 8 | 74.2 | 70.9 | 59.1 | 94.9 | 12.4 | 2 | **Sparse Data (94.9)** | Missing-data inflation extreme |
| Turnaround | 8 | 75.4 | 74.9 | 60.3 | 87.8 | 9.4 | 3 | Caribbean Turnaround | Niche can win when geo+situation align |
| Institutional lender reporting | 8 | 74.0 | 73.4 | 60.0 | 85.0 | 8.7 | 3 | Institutional Reporting Platform | Reporting still weakly comparative |

**Cross-scenario:** Generic ranked first in **4/8**. Factors non-differentiating (avg≥70 across nearly all ops) in **all 8** scenarios: `systemsReporting`, `ownerRelations`, `serviceOfferings`, `negativeFitPenalty`. Also frequent: `feeCommercial`, `dealStructureAssignment`.

### Live historical sample (Phase 5F, after backfill)

Deal Aeropuerto Cancún: avg **80.7**, range 74.3–85.8, **7 Strong / 3 Moderate**, stdev small — improved vs pre-5E 64.1 cluster, but still **tight band** once structured fields filled similarly.

---

## Why the Current Model Does or Does Not Differentiate Operators

### A. Data limitation

| Problem | Evidence |
| ------- | -------- |
| Structured geo/services/structures sparsely populated on Active universe | Live audit: Active Countries **2/24 (8.3%)**, Offered Services **2/24**, Management Structures **3/24**, Conversion experience **0/24** (`reports/operator-fit-airtable-readonly.json`) |
| Case studies not in numeric score | Case Studies table exists; `scoreOperatorMatchForDeal` never reads performance outcomes |
| Fee/economics not comparable | Flat 75 in `api/my-deals.js` |
| Evidence confidence not scored | Master Data Confidence **16.7%**; display-only |
| Brand approval / compatibility records unused | Brand Relationships child table not in engine |

### B. Taxonomy limitation

| Problem | Evidence |
| ------- | -------- |
| Asset types broad multi-selects | `bestFitAssetTypes` / propertyTypes token scans inflate overlap (`my-deals.js` collectValuesByKeyToken) |
| Conversion / reflag / turnaround conflated into stage overlap | `scoreAssetStageFactor` blend; Conversion field 0% populated |
| Dual band thresholds | Weight-config UI bands vs company-util bands |
| Product structure labels vs live options | Franchise Only / Lease vs SI structured values — migration caution |

### C. Scoring limitation

| Problem | Evidence |
| ------- | -------- |
| Generic capabilities over-rewarded | Service/systems/owner-relations design; sim non-differentiating factors |
| Too few penalties | `negativeFitPenalty` weight **2**; defaults to 100 |
| No eligibility gates | Contrast Brand Match hard gates |
| Fixed weights every project | `OPERATOR_MATCH_WEIGHTS` |
| Missing data can inflate | Sparse operator **94.9** lifestyle scenario (sim) |
| Portfolio breadth advantage | Generic fixture wins 4/8 |
| Single opaque average | No separate Brand–Operator / Structure / Evidence / Risk layers |
| Brand-managed pathways not first-class | Registry is Explorer link only |

### D. Product-input limitation

| Problem | Evidence |
| ------- | -------- |
| Mandate / control / complexity partially present but weakly scored | Owner Control Preference exists; ownerRelations still keyword |
| Operating structure may still be legacy MP on older deals | 5E fallback path |
| No ranked owner objectives driving dynamic weights | No sensitivity UI |
| Institutional reporting asked but not strictly compared | systemsReporting asymmetry |

### E. Visualization limitation

| Problem | Evidence |
| ------- | -------- |
| One optional overall score + band | OAS companies cards |
| Breakdown exists but not comparative | `match-score-breakdown-ui.js` per operator |
| No pathway view brand×operator×structure | Absent |
| No evidence-confidence meter driving rank | Confidence badge only |
| No operator shortlist compare matrix | Target List is brands; Deal Compare is proposals |

---

## 5. Additional owner questions (evaluate, do not implement)

Prefer existing → infer → conditional structured → long-form last.

| Proposed Question | Why Needed | Current Data Already Available? | Scoring Dimension | Input Type | Conditional Logic | User Burden | Recommended? |
| ----------------- | ---------- | ------------------------------- | ----------------- | ---------- | ----------------- | ----------- | ------------ |
| Primary operating mandate (stabilize NOI / open on time / turnaround / reposition / residential ops) | Differentiates turnaround vs opening vs stabilize | Partial via Project Type + priorities | Project-fit / risk | singleSelect | Always | Low | **Yes** |
| Rank top 3 value-creation objectives | Dynamic emphasis without free-form | Partial Commercial Priority | Project-fit weights | ranked multi (max 3) | Always | Low–Med | **Yes** |
| Desired ownership involvement / approval rights | Governance fit | Owner Control Preference partial | Ownership-governance | singleSelect + multi approvals | If not owner-operated exclusive | Low | **Yes (extend existing)** |
| Openness to brand-managed vs third-party vs franchise+operator | Pathway eligibility | Operating Model + Preferred Management Structure | Structure / eligibility | multiSelect (preserve current values) | If undecided | Low | **Normalize existing — don’t duplicate** |
| Reporting / lender package requirement | Institutional differentiation | Owner Reporting Expectations / Package | Governance / evidence | singleSelect | If institutional owner type known/inferred | Low | **Yes (ensure structured)** |
| Project complexity flags (residences, meetings, complex F&B, mixed-use) | Niche fit | F&B Complexity; Project Type mixed-use; weak meetings | Comparable experience | multiSelect | Show if building/project suggests | Low | **Yes conditional** |
| Pre-opening intensity needed | Opening risk | Pre-Opening Support Needed | Stage / capacity | singleSelect | New build / conversion | Low | Keep/ensure used |
| Fee/contract priorities | Economics layer | Fee expectations text weak | Economics | singleSelect priorities (not raw %) | Optional | Med | **Later / outreach** |
| Current operating challenges | Turnaround signal | Deal breakers / challenges free text | Risk | multiSelect | Existing operating hotel | Med | **Yes conditional** |
| Exit / hold / refinance horizon | Mandate | Capital fields partial | Explanation only initially | singleSelect | Optional | Low | Explanation-only first |
| Long-form strategy narrative | Context | Multiple long fields exist | Explanation | longText | Optional | High | **Only if short structured insufficient** |

**Do not add** duplicate checkboxes for table-stakes “needs revenue management / sales / marketing” as scored positives — treat as default full-management scope unless commercial-support-only path.

Preserve structure values: Third-Party Management; Franchise + Operator; Franchise Only; Owner-Operated; Lease; Asset Management; To Be Confirmed — **map carefully to live SI options; founder approval before renames**.

---

## 6. Proposed operator-data matrix (recommendation only — not implemented)

Classification key: **EU** existing usable · **EN** existing needs normalization · **EP** existing poorly populated · **D** derivable · **MO** missing owner · **MOp** missing operator · **MD** missing Dealality research · **OR** outreach-only · **OL** outcome learning

| Domain | Field (proposed) | Class | Airtable home | Type | In score? | Explanation only? | Owner-facing? |
| ------ | ---------------- | ----- | ------------- | ---- | --------- | ----------------- | ------------- |
| Eligibility | Active status | EU | Master.submission_status | select | Gate | | Badge |
| Eligibility | Active Countries / Markets / Presence Type | EP | Platform | multi | Gate+score | | Yes |
| Eligibility | Management Structures Supported | EP | Commercial | multi | Gate | | Yes |
| Eligibility | Brand approval status per brand | MOp/MD | Brand Relationships | link+select | Gate | | Selective |
| Comparable experience | Case study property + situation + scale + geo | EP | Case Studies | child | Score (future similarity) | | Yes |
| Performance | GOP/NOI/RevPAR index vs competitive set (verified) | MD/OR | Case Studies / Performance child | number+source | Performance layer | | Careful |
| Commercial fit | Non-generic differentiators only (meetings, residences, complex F&B levels) | EN/EP | Governance/Commercial | select | Score | | Yes |
| Table-stakes services | RM/Sales/Accounting/HR… | EP | Governance | multi | **Eligibility checkbox only — not positive points** | Yes as scope | Yes as scope |
| Governance | Reporting level, cadence, portal, audit rights | EP | Governance / Engagement | select | Score vs deal | | Yes |
| Brand–operator | Years / keys / approvals per brand | MOp | Brand Relationships | structured | Compatibility layer | | Yes |
| Structure | Lease / asset mgmt / franchise support | EN | Commercial | multi | Structure layer | | Yes |
| Economics | Fee posture bands (not marketing blurb) | MOp/OR | Commercial | select bands | Soft / explanation | | Selective |
| Capacity | Regional team, openings in progress, max concurrent | MOp/MD | Platform/Master | number/select | Risk | | Selective |
| Risk | Conflicts, departures, renewals lost | MD/OR | Diligence | multi/text+source | Risk penalties | | Selective |
| Evidence | Source URL, date, validation status per claim | EP/MD | PI Source Library links | link | Confidence layer | | Footnotes |
| Outcomes | Shortlisted/selected/terms/satisfaction/stabilization | OL | Operator Deal Requests + new outcome fields | mixed | Learning | | Internal first |

---

## Verification

- Scoring formula **not modified**.  
- Synthetic sim + prior Phase 5F live report cited.  
- Assumptions labeled (fixture operators are synthetic; live completeness is Active-n=24 snapshot).
