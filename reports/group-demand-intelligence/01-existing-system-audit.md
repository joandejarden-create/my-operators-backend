# Group Demand Intelligence — Existing System Audit (Phase 0)

**Date:** 2026-09-12  
**Repo:** `c:\Dev\deal-capture-proxy`

---

## 1. Canonical research engine

| Layer | Path | Reuse |
|-------|------|-------|
| HI Research Center | `lib/hotel-intelligence/research/` | REUSE orchestrator, providers, spend-guard, audit-log |
| Research Methods | `lib/hotel-intelligence/research-methods/` | EXTEND with GDI playbook pack |
| Escalation | `escalation-policy.js` Levels 0–4 | REUSE mapping; GDI Level 5 = Webhound within $5 |
| Research Engine V2 | `lib/research-engine-v2/` | REUSE census patterns only; not event calendars |
| API | `api/hotel-intelligence-research.js` | Pattern for admin Run Research |

**Expected GDI architecture:** thin product orchestrator that escalates through HI methods rather than a second engine.

---

## 2–3. Hotel / Census

| Item | Detail |
|------|--------|
| Bethesda census | `recLuxvwwxID7U2B8` |
| ADP profile (RO) | `fixtures/ai-demand-positioning/bethesda-marriott-property-profile.json` |
| Rooms | 407 (MEDIUM) |
| Meeting space | ~18,719 sq ft; 27 rooms; Grand Ballroom 4,592 sq ft / ~450 capacity |
| Comp set (declared) | Hyatt Regency Bethesda, Bethesdan/Tapestry, Marriott Bethesda Downtown, Bethesda North Marriott, AC Bethesda |
| Demand anchors | NIH, Walter Reed, Downtown Bethesda, Rockville Pike, DC metro |

Reuse: census-read + fixture as seed for Hotel Group Demand Profile. Do not write ADP fixtures.

---

## 4–5. ADP demand territory / scenarios

| Item | Path | GDI rule |
|------|------|----------|
| Territories | `territory-dictionary.js`, `intent-territory-labels.js` | READ-ONLY; do not add GDI intents |
| Scenarios | `scenario-registry.js`, `standard-scenarios.js` | Do not merge GDI scenarios |
| Experimental catalog pattern | `scenario-expansion-catalog-v1.js` | Copy isolation pattern into GDI namespace |
| Observations | `data-model.js`, runtime under `data/ai-demand-positioning/` | Never write |

---

## 6–8. Evidence, provenance, confidence

| System | Path | Gap for GDI |
|--------|------|-------------|
| HI evidence-store | `lib/hotel-intelligence/evidence-store.js` | Hotel-field oriented |
| Confidence | `lib/hotel-intelligence/confidence.js` | Reuse tiers; add explicit FACT/INFERENCE on GDI rows |
| Output contracts | `research/prompts/output-contracts.js` | VERIFIED/PROBABLE/SIGNAL — map carefully |

**Required extension:** opportunity-scoped evidence rows with field, value, FACT|INFERENCE, source metadata, method, cost attribution.

---

## 9–12. Web research, providers, Ampfy, Webhound

| Provider | Status | GDI use |
|----------|--------|---------|
| SerpAPI | Present (`providers/serpapi.js`) | Level 3 search |
| Native fetch/PDF | Present | Level 2–3 extraction |
| Apify | Present | Level 4 supplemental scrape |
| **Ampfy** | **Absent** | Reserved adapter; not SoT |
| Webhound | Present + $5 pilot cap | Level 5 selective deep research |

---

## 13–16. Company / org / person / event research

| Capability | Status |
|------------|--------|
| Ownership / pubco people | Strong (Mexico pack) |
| Profile discovery (LinkedIn via SerpAPI) | REUSE carefully for planners |
| Association meeting timelines | **MISSING — net new** |
| Medical/gov event calendars | **MISSING — net new** |
| Market Demand categories | Group/Medical/Government exist as category scores, not event instances |

---

## 17–20. Geo, competitors, jobs, audit

| Area | Reuse |
|------|-------|
| Geography | Dealality Market/Submarket; HI nearby |
| Competitors | Start from declared ADP/census comp set + opportunity-specific history |
| Jobs | HI orchestrator pattern; GDI runs admin-triggered |
| Audit | HI audit-log + spend-ledger patterns → GDI run artifacts |

---

## 21–25. UI / design / build rules

| Pattern | Best template |
|---------|---------------|
| Standalone page | `/market-demand` |
| Admin Run Research | HI Research Center + Leak Audit admin |
| Tables + drawer | `admin-ai-demand-reviews` / ADP evidence drawer |
| Filters | radar/explorer filter drawers |
| Auth | `adminAuth` for Run Research; read may use Dealality user |
| Feature flag | Operator Fit env default OFF |

---

## Required extensions (build list)

1. `GroupDemandHotelProfile` + demand priority config  
2. Opportunity / event / meeting-history / evidence / contact / competitor / planner-observation models  
3. Scoring: Hotel Fit (weighted) + Evidence Confidence + Priority + Booking Window  
4. Research orchestrator with cost ledger + escalation audit  
5. GDI research methods (association, medical, gov, weekend, contact, history)  
6. Standalone UI: Opportunities, Weekly Brief, Research Audit  
7. Feedback capture without auto-retrain  
8. Isolation tests (no ADP mutation)  
9. Bethesda pilot run + commercial review report  

---

## Missing capabilities (accepted V1 gaps)

- Delphi / CRM / Cvent RFP / pace / ADR  
- Full Ampfy product integration  
- Airtable tables for GDI entities  
- Portfolio-scale shared event graph (designed for; not fully populated)  

---

## Expected provider usage (Bethesda pilot)

| Level | Provider | Expected spend |
|-------|----------|----------------|
| 1 | Census + ADP RO fixture + cached seed | $0 |
| 2 | GDI playbooks / SerpAPI / official sites | Low / shared SerpAPI quota |
| 3 | Standard web fetch + extraction | Low |
| 4 | Apify if needed; Ampfy N/A | $0 unless Actor run approved |
| 5 | Webhound | **Hard cap $5.00** on handful of High Priority candidates |

---

## Estimated cost model (directional)

| Scale | Assumption | Rough research cost/hotel |
|-------|------------|---------------------------|
| 1 hotel (pilot) | Deep + Webhound $5 | $5–15 including search/LLM |
| 10 hotels | Shared org/event cache | $3–8 amortized |
| 100 hotels | Shared event intelligence mandatory | Must reuse events; target <$2–5 incremental |
| 1,000 hotels | Event graph + caching required | Unviable if each hotel re-researches same associations |

---

## Implementation risks

1. Scraped “association lists” without specific meeting instances → low commercial value  
2. Confusing Bethesda Marriott with Bethesda North Marriott / Downtown Marriott → identity exclusions required  
3. Treating Ampfy/Apify as SoT  
4. Auto-running research on page load  
5. Mixing Experimental Planner Consideration into official ADP  

---

## Expected architecture (summary)

```
hotelId
  → GroupDemandHotelProfile (+ demand config)
  → ResearchRun (admin-triggered)
      → L1 existing knowledge
      → L2 GDI research methods
      → L3 web research
      → L4 Apify/Ampfy supplemental
      → L5 Webhound ≤ $5
  → Candidate opportunities
  → Qualification + scoring + evidence
  → Contacts + competitors + optional planner obs
  → Salesperson UI + Weekly Brief + Admin Audit + Feedback
```

**Blocking issues:** None. Proceed to build.
