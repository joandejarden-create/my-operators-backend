# Dealality Research Engine — Forensic Review vs Webhound Tests 1–6

**Date:** 2026-08-04  
**Status:** Read-only architecture review. **No Webhound launch. No production code changes. No additional credits.**  
**Evidence:** Repo inspection + `data/webhound-test1-mexico-owner-intel/` Tests 1–6  
**Canvas:** `webhound-vs-dealality-research-engine.canvas.tsx`

---

## Most important answer

Webhound’s transferable lesson is not “scrape better.” It is:

1. **Plan research by claim type and geography**, not by presentation tab completeness.  
2. **Actively try to disprove** current Dealality fields (contradiction-first), especially Status and Affiliation.  
3. **Order evidence in time** (pipeline → open; Elegant → Tribute).  
4. **Emit proposed corrections with provenance**, never auto-write SoT.  
5. **Stop when enough** (T6 left ~$3.89 unused).

Dealality already has the **governance half** (Tab Factory, PVQL, CV bans, dry-run apply). It lacks the **independent research half**. Building that half internally covers most of Test 6’s value; keep Webhound for government discovery, opaque ownership, and periodic blind audits.

---

## 1. Current Dealality research architecture (what actually exists)

### Reality check

Dealality is primarily a **presentation factory + quality governance + census enrichment** system — not an autonomous open-web research engine.

```text
Docs protocols (aspirational lifecycle)
    ↓
Cursor/agent build rules (AGENTS.md, AI build system)
    ↓
Explorer Tab Factory / OS / PVQL / freeze gates
    ↓
Airtable Brand Basics + Presentation + Census (SoT)
    ↓
API render (brand-library, operator-explorer)

Parallel:
Brand-directory fetch (cheerio/puppeteer) → match/plan → dry-run → apply
Independent census (OSM/Wikidata/directory candidates)
Webhound MCP → data/webhound-* only (not wired to writers)
```

### Layer map

| Layer | What exists | Live web? |
|-------|-------------|-----------|
| **Brand Explorer** | Tab contracts, Tab Factory audit/remediate, source provenance-by-tab, PVQL, 54-baseline, curated source packs, authored content modules, apply-gate-enforcer | Almost never — Airtable/fixtures/static packs |
| **Operator Explorer** | Quality baselines (Arbor/HE), Tab Factory, provenance, OS queue, factory content packs, overlay apply | No live research |
| **Hotel Census** | Field maps, parent resolve, affiliation audits, brand-directory fetchers (Marriott/Hilton/Choice/Accor/Wyndham/Design Hotels), Hilton **live status audit**, plan/apply enrichment | **Yes** (directory/status) |
| **Independent census** | `lib/independent-census/*` — OSM, Wikidata, brand directory candidates, promote/evidence apply, source-policy | **Partial live** |
| **Company Validated** | Profile governance labels; hard write bans in factory/remediation; AGENTS/INTELLIGENCE_GOVERNANCE policy | Protection, not capture loop |
| **Protocols** | DATA_VALIDATION_PROTOCOL, SOURCE_RANKING_GUIDE, INTELLIGENCE_GOVERNANCE, CONTENT_EXTRACTION_TEMPLATE | Documentary / agent OS |
| **Webhound** | Test artifacts under `data/webhound-test1-mexico-owner-intel/` | External sidecar |

### What “research” means today

| Domain | Meaning in practice |
|--------|---------------------|
| Brand/Operator Explorer | Curated sources + hand-authored modules + quality gates |
| Hotel Census | Directory opportunistic enrichment + match/apply |
| Protocols | Human/Cursor operating manual |
| Webhound | Experimental independent research (candidate only) |

### Gaps visible from code

- No research planner / query generator for open web.  
- No contradiction-first pass (Hilton status audit is the closest).  
- No temporal claim model.  
- No unified cross-table integrity engine (only pairwise audits).  
- Source Library schema proposed; capture→extract→Validation Status not a closed Node runtime.  
- Confidence fragmented (Operator Fit ≠ profile governance ≠ census Data Confidence).

---

## 2. Webhound research behaviors observed (Tests 1–6)

| Test | Spend | Behavior highlight |
|------|-------|-------------------|
| 1 | ~$4.88 free | SEMARNAT-via-press discovery; thin ownership |
| 2 | $4.84 | Primary Gaceta + Tortuga institutional resolve |
| 3 | $4.86 | Gaceta portable to BCS; UBO fail |
| 4 | $4.80 | Non-gov weak for pre-filing; investor/hiring useful |
| 5 | $4.87 | Relationship maps for institutional owners |
| 6 | **$6.11 / $10** | Mexico/CALA BE validation; contradiction/freshness ROI |

### Repeatable behaviors

**Planning:** Prompt priority order; depth over breadth; skip low-value targets; T6 early stop when schema complete.  
**Queries:** Seed→deepen; authority-first; geography-bound; entity-type separation (project vs parent vs hotel directory).  
**Sources:** SEMARNAT Gaceta; corporate/IR; brand booking directories; press as lead not SoT.  
**Evidence:** confirmed vs inferred vs unknown; confidence enums; honest nulls.  
**Temporal:** filing→authorization windows; Pipeline→Operating; reflags (Elegant→Tribute).  
**Contradiction:** Independent evidence vs Dealality after the fact; Aluna vs Tres Ríos no blind merge.  
**Entity chains:** SPV→platform→PE/FO; Choice dual-region Individuals.  
**Failures:** emails, UBO/fideicomiso, opaque private owners, global full census (correctly avoided in T6).

---

## 3. Gap analysis / capability matrix

| Capability | Dealality today | Webhound | Action |
|------------|-----------------|----------|--------|
| Research planning | Weak | Strong | Improve Dealality |
| Query generation | Weak | Strong | Copy WH behavior |
| Source discovery | Partial | Strong | Hybrid |
| Primary-source prioritization | Partial | Strong | Improve |
| Evidence extraction | Partial | Strong | Improve |
| Provenance | Partial (tab domains) | Strong | Improve |
| Confidence | Partial / fragmented | Partial | Improve |
| Contradiction detection | **Missing** | Strong (T6) | **Copy** |
| Temporal validation | Partial (Hilton) | Strong | **Copy** |
| Entity resolution | Partial | Strong (institutional) | Hybrid |
| Parent resolution | Partial | Strong | Improve |
| Ownership resolution | Weak | Partial | WH + specialist |
| Hotel census validation | Partial | Strong MX/CALA | Improve |
| Brand validation | Strong (gates) | Strong (fresh) | Hybrid |
| Operator validation | Strong (gates) | Partial | Keep Dealality + WH assist |
| Cross-table reconciliation | Partial | Strong (T6) | Improve |
| Missing-record discovery | Partial (indep. census) | Strong | Hybrid |
| Change detection | Weak | Strong | **Copy** |
| Research stopping rules | Missing | Partial | Improve |
| Cost/effort allocation | Missing | Partial | Improve |

---

## 4. Top 10 improvements

### 1. Contradiction-first freshness pass — Impact: Transformational · Difficulty: Medium

- **Today:** Enrichment searches for supporting content; Hilton alone compares live Open vs census.  
- **Problem:** Stale Pipeline/Affiliation persist until ad-hoc waves or paid WH.  
- **WH lesson:** T6 Indigo Pipeline→Open; Barbados Tribute reflags.  
- **Proposed:** For each census/BE claim, generate **disproof queries** (opened, book now, reflag, new brand) and prefer newer conflicting primary evidence.  
- **Files:** new `lib/research-engine/` module; extend `audit-hilton-census-status.js` pattern; scripts for IHG/Marriott/Choice.  
- **Cost impact:** accuracy, freshness, lower external spend.

### 2. Claim-specific source hierarchy + search packs — High · Medium

- **Today:** SOURCE_RANKING_GUIDE is universal trust tiers.  
- **Problem:** Wrong source type for claim (press used as status SoT).  
- **WH lesson:** Directory for status; Gaceta for projects; IR for parents.  
- **Proposed:** Claim→source pack map in config.  
- **Files:** `docs/data-intelligence/` + `lib/research-engine/claim-source-packs.js`.

### 3. Generalize live status audit beyond Hilton — High · Medium

- **Today:** `lib/hotel-census/audit-hilton-census-status.js` + Hilton GraphQL.  
- **Problem:** Indigo/Kimpton/Tribute freshness not automated.  
- **WH lesson:** IHG/Marriott/Choice directories decided T6.  
- **Proposed:** Parent adapters: Hilton, IHG, Marriott, Choice.  
- **Files:** `lib/*-hotel-*-fetch.js`, new `audit-*-census-status.js`, npm scripts.

### 4. Cross-entity integrity engine — High · High

- **Today:** Pairwise audits (affiliation↔brand setup).  
- **Problem:** Avani census without BE; Faranda without OE; Affiliation vs directory brand.  
- **WH lesson:** T6 cross-table themes.  
- **Proposed:** Nightly report: BE↔Census↔OE↔Parent contradictions → proposed packs.  
- **Files:** new `lib/research-engine/cross-entity-integrity.js`; reports under `reports/`.

### 5. Explicit research modes — High · Medium

- Modes: Discovery, Validation, Freshness, Reconciliation, Deep Resolution, Monitoring.  
- Map existing jobs into modes; stop treating Tab Factory as “research.”

### 6. Temporal claim model — High · High

- Fields: value, validFrom, validUntil, evidenceDate, lastVerified, supersedes, confidence, source.  
- Distinguishes “was Pipeline” from “is Open.”

### 7. Proposed-change packs (never auto-SoT) — High · Medium

- Codify T6 reconcile output shape into Dealality CLI; feed existing dry-run/apply gates.

### 8. Entity-chain templates — Medium · Medium

- SPV→Platform→Parent→DM; Choice dual-region template; reuse for Tortuga-class enrichment (internal or WH).

### 9. Source Library capture→extract runtime — Medium · High

- Close the gap between DATA_VALIDATION_PROTOCOL and CV write bans.

### 10. Dynamic stop rules — Medium · Medium

- Stop when required claims filled + contradiction pass complete; avoid budget burn.

---

## 5. Proposed Research Engine V2

Do **not** replace Tab Factory / PVQL. Add a research plane that feeds **proposed changes** into existing gates.

```text
RESEARCH TARGET (+ mode)
    ↓
RESEARCH PLAN (claim packs, geography, effort budget)
    ↓
INDEPENDENT EVIDENCE COLLECTION (directory fetch / WH / gov)
    ↓
SOURCE QUALITY CLASSIFICATION (claim-specific)
    ↓
CLAIM EXTRACTION
    ↓
TEMPORAL ORDERING
    ↓
CONTRADICTION SEARCH (disproof queries)
    ↓
ENTITY RESOLUTION
    ↓
CONFIDENCE ASSESSMENT
    ↓
CROSS-ENTITY RECONCILIATION
    ↓
PROPOSED CHANGES (JSON pack)
    ↓
GOVERNANCE / VALIDATION GATES (existing dry-run / CV bans / PVQL)
    ↓
SOURCE OF TRUTH (human-approved apply only)
```

Reuse: Hilton audit pattern, independent-census promote flow, apply-gate-enforcer, SOURCE_RANKING_GUIDE (extended claim-specific).

---

## 6. Research modes (justified)

| Mode | Justified? | Basis |
|------|------------|-------|
| Discovery | Yes | T1/T3 gov; independent-census candidates |
| Validation | Yes | T6 independent BE; directory audits |
| Freshness | **Yes — priority** | T6 Pipeline/reflag; Hilton audit |
| Reconciliation | Yes | T6 Cursor reconcile pattern |
| Deep Resolution | Yes (hybrid) | T2/T5 institutional; WH for hard cases |
| Monitoring | Yes | Scheduled freshness on Active/Live + CALA census |

---

## 7. Source / evidence architecture

Keep SOURCE_RANKING_GUIDE tiers, but add **claim-specific overrides**:

| Claim type | Strongest source |
|------------|------------------|
| Brand parent / collection | Corporate / IR / official brand architecture pages |
| Hotel operating status | Official brand/hotel booking directory |
| Affiliation / reflag | Official directory + parent press on same date |
| Project entitlement | Government/regulatory (SEMARNAT-class) |
| Operator | Owner/operator announcement or management disclosure |
| Pipeline opening date | Parent development portal + property page |
| Ownership / UBO | Registry/specialist (not press) |

Regional relevance (CALA > global for CALA) remains binding.

---

## 8. Contradiction-first validation

**Today’s bias:** “What supports this field?”  
**Needed bias:** “What would make this field false now?”

Generic algorithm:

1. Load current claim (e.g. Status=Pipeline).  
2. Generate disproof query pack by claim type.  
3. Collect primary evidence dated after `lastVerified`.  
4. If stronger/newer contradictory primary evidence → flag `Webhound/Dealality appears stale` → proposed correction.  
5. If conflicting peers (Aluna vs Tres Ríos) → `Evidence conflicting` (no auto-merge).

---

## 9. Temporal intelligence

Recommended claim envelope:

`value | validFrom | validUntil | evidenceDate | lastVerified | supersedes | supersededBy | confidence | source`

Material for Census Status, Affiliation, Brand Status, openings/momentum dates. Presentation cards should cite `evidenceDate`, not only narrative tense.

---

## 10. Cross-table integrity

Detect automatically:

- BE Mexico hotel count ≠ Census Affiliation count (scope-normalized).  
- Census Brand A but directory Brand B.  
- Census Pipeline but booking accepts reservations.  
- OE operator X vs owner announcement operator Z.  
- Census brand present, BE Active/Live absent (Avani).  
- Individuals Faranda hotels without OE Faranda pack.

Output: integrity report + proposed packs; never silent SoT mutation.

---

## 11. Where Webhound still wins

| Keep Webhound | Why not build now |
|---------------|-------------------|
| SEMARNAT / gov project discovery | Fragile, jurisdiction-specific, high maintenance |
| Opaque private ownership / UBO | Specialist + legal; WH only partial |
| Blind periodic BE audits (Dealality forbidden as source) | Independence value |
| Deep long-tail open-web when directories fail | Economics of general crawling |

| Do not pay WH for | Internalize |
|-------------------|------------|
| Known-brand status freshness | Multi-parent directory audits |
| Obvious reflags on directory brands | Contradiction-first affiliation check |
| Cross-table hygiene reports | Integrity engine |
| Tab Factory quality | Already Dealality’s strength |

---

## 12. Economic implications

| Model | Cost signal | Quality | Notes |
|-------|-------------|---------|-------|
| A. Dealality today | Low $ / high human | Strong presentation; weak freshness | Missed T6-class issues |
| B. Research Engine V2 | Eng time then low marginal | High freshness on known entities | Best ROI next |
| C. Webhound always | ~$1–5 per scoped job | High for discovery/hard cases | Overpay if used for status hygiene |
| D. Hybrid (recommended) | V2 for freshness + WH for discovery/blind audit | Best quality/$ | Matches Tests 1–6 evidence |

Observed: T6 ~$1.22/brand · ~$0.70–1.00/material correction · funded pool ~$25.48 spent · ~$29.52 left · Test 7 not authorized.

---

## 13. Top 3 to build now (fast + high business value)

### 1. Contradiction-first status & affiliation checker

- Effort: 1–2 weeks  
- Improves: freshness, accuracy, consistency  
- Cost: reduces WH freshness spend  
- Risk: Low if proposed-only  
- Proof: Re-run Indigo/Kimpton/Tribute Mexico; catch Pipeline→Open and Barbados Tribute without WH

### 2. Multi-parent live directory status audit (IHG + Marriott + Choice)

- Effort: 1–2 weeks  
- Extends proven Hilton module  
- Proof: dry-run Status diff report vs official directories

### 3. Cross-table integrity report (proposed corrections only)

- Effort: ~1 week  
- Surfaces Avani-without-BE, Affiliation conflicts, operator gaps  
- Proof: weekly report mirroring T6 discrepancy classes at $0 credit

---

## 14. Future A/B benchmark (do not run now)

- Select 5 unseen Active/Live brands (or 5 Mexico-heavy affiliations).  
- Same objective: independent MX/CALA validation + proposed corrections.  
- Arm A: Dealality Research Engine V2 only.  
- Arm B: Webhound $10 cohort (blind to Dealality).  
- Compare: accuracy, completeness, freshness, evidence quality, contradiction hits, time, cost.  
- Decide WH residual value only after V2 exists.

---

## Definition of success for this pause

Paid Webhound testing paused. Architecture understood from code + Tests 1–6. Next spend (if any) should follow V2 top-3 builds — not Test 7 — unless Joan explicitly wants another WH discovery/blind-audit experiment.
