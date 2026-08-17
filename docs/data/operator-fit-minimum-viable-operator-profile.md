# Minimum Viable Operator Profile (MVOP)

**Purpose:** Define the minimum information required for credible Operator Fit ranking.  
**Authority:** `docs/architecture/decisions/operator-fit-enrichment-founder-decisions.md`  
**Schema authority:** `docs/*-airtable-fields.md`, Operator Setup bindings — do not invent fields.  
**Date:** 2026-08-03

---

## Levels

### Level A — Identity and eligibility

Must include: operator name; active status; operator type; parent company where applicable; **structured** active countries or supported geographies; operating structures supported; hotel segments / chain scales; typical key-count range where available; brand-managed vs third-party classification; known project exclusions.

### Level B — Project differentiation

Urban / resort / luxury / lifestyle / select-service / extended-stay / all-inclusive / mixed-use / branded-residence / new-build / conversion / reflagging / turnaround / F&B intensity / meeting-group / regional operating infrastructure.

Prefer structured comparable evidence over marketing yes/no checklists. Do **not** treat generic offered-service tokens as primary ranking differentiators.

### Level C — Brand and structure relationships

Brands operated; brand parent; brand segment; relationship status; approval status; approval source; date verified; relevant property count; direct brand-management availability; applicable regions; known restrictions.

### Level D — Evidence and comparables

Comparable hotel metadata (name, market, country, segment, keys, asset type, development type, brand, operating structure, opening/takeover date, why comparable); performance evidence where available; evidence class; source; source date; verification status.

### Level E — Later-stage validation (outreach)

Regional resources; proposed leadership; opening pipeline; capacity; competitive conflicts; fees; centralized charges; contract term; performance test; owner approval rights; data access; references; operator investment; project-specific commercial proposal.

Level E is **not** required for baseline Ranking Ready.

---

## Field catalog

| Field | Level | Required for Ranking? | Data Type | Source | Evidence Requirement | Current Airtable Location | Coverage | Action |
| ----- | ----- | --------------------- | --------- | ------ | -------------------- | ------------------------- | -------: | ------ |
| Operator name | A | Yes | text | Dealality / operator | Identity | Master.`company_name` | 100% | Retain |
| Active status | A | Yes | select | Dealality | Confirmed Active | Master.`submission_status` | 100% | Retain |
| Operator type | A | Yes | select/text | Dealality | Classified | Master / Profile | Partial | Normalize vocab |
| Parent company | A | Preferred | text/link | Research | Documented | Master / Profile | Partial | Enrich |
| Active countries | A | Yes (structured) | multi | Dealality research | Structured multi-select — **not** prose blurbs | Platform.`Active Countries` | ~8% | **Critical enrich** |
| Active markets / cities | A | Conditional | multi | Research | Structured preferred | Platform.`Active Markets / Cities` | ~8% | Enrich after countries |
| Operating structures | A | Yes | multi | Research / operator-reported→validated | Controlled vocab | Commercial.`Management Structures Supported` | 12.5% | **Critical enrich** |
| Chain scales / segments | A | Yes | multi | Research | Controlled vocab | Profile.`chainScalesSupported` | 100% | Retain; QA taxonomy |
| Typical key-count range | A | Preferred | number/range | Research | Documented | Profile / Commercial | Sparse | Enrich |
| Brand-managed vs third-party | A | Yes for BM path | enum + confirmation flags | Brand / Dealality | Independent confirmation for BM | Candidate type + Brand Relationships (proposed) | Sparse | **Architecture** |
| Known exclusions | A | Preferred | multi/text | Research | Documented | Commercial.`bf_not_ideal_for` | High but soft | Normalize; do not over-weight |
| Urban / resort / luxury / … experience | B | ≥1 meaningful dimension | structured flags + comps | Research | Prefer comps over marketing yes | Commercial asset/situation + Case Studies | Soft / inferred | Structured experience table |
| Conversion / reflag | B | Project-dependent | select + comps | Research | Documented | Commercial.`Conversion / Reflag Experience` | 0% | **High enrich** |
| F&B / meetings intensity | B | Project-dependent | select | Research | Documented | Governance F&B / meetings | Sparse | Enrich when project-relevant |
| Regional infrastructure | B | Preferred | structured | Research | Documented | Platform (proposed) | 0% | Later |
| Generic offered services | B | No (table stakes) | multi | Operator-reported | Do not score presence | Governance.`Offered Services` | ~8% | Deprioritize for ranking |
| Brands operated | C | Preferred | link | Research | Linked Brand Basics | Profile.`brands` | ~88% | Retain; verify |
| Brand relationship / approval | C | High for branded deals | linked record | Brand / Dealality | Source + date verified | Brand Relationships child (extend) | 0% verified | **New/normalize** |
| Direct brand management availability | C | Required if BM candidate | boolean + source | Brand confirmation | Founder 2.3 rules | Brand Relationships | Unconfirmed today | Confirm before points |
| Comparable assignments | D | High | linked records | Research | Full metadata + why comparable | Case Studies (extend) | Sparse / unused in score | **Wire + enrich** |
| Evidence source URL/type/date | D | Yes for material claims | linked evidence | PI / research | Evidence class + verification | PI Source Library / proposed Evidence table | 0% usable | **Critical** |
| Evidence class | D | Yes | enum | System | Per config | Derived + stored | Weak | Enforce on write |
| Fee / commercial terms | E | No (baseline) | structured | Outreach | Project-specific | Outreach / future responses | N/A | Collect later |
| Capacity / conflicts / leadership | E | No (baseline) | structured | Outreach | Operator-reported then validated | Project-specific responses table | N/A | Post-shortlist |

---

## Ranking Ready checklist (founder 2.5)

An operator may enter production Top-5 only when **all** are true:

1. Active status known  
2. Geography known via **structured** countries (or explicitly conditional + validated)  
3. Operating-structure compatibility known  
4. Hotel segment / chain-scale relevance known  
5. ≥1 meaningful project-experience dimension known  
6. Material positive claims have ≥1 identified source  
7. No unresolved critical eligibility conflict  
8. Project-specific Data Coverage ≥ **50%**

Otherwise surface as **Additional Candidate Requiring Research** — do not pad Top-5.

---

## Data Contract Snapshot

- **Tables:** Operator Setup Master / Profile / Platform / Commercial / Governance / Case Studies; proposed Brand Relationships + Evidence + Experience + Geo Coverage.  
- **Mapping:** `ENRICHMENT_FIELD_CATALOG` in `lib/operator-fit/readiness.js`; operator adapter `adaptOperatorFromPrefill`.  
- **Required for Ranking:** Level A critical + evidence source + ≥1 Level B dimension.  
- **Optional:** Level E; generic services.  
- **Select options:** existing schema docs / ensure scripts — never invent.  
- **UI output:** readiness status, coverage buckets, enrichment queue row, internal readiness page.
