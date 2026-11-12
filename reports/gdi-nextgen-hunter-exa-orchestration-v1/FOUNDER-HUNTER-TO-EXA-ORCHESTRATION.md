# GDI Next-Gen Hunter → Exa Research Orchestration V1

**Contract:** `gdi-nextgen-hunter-to-exa-orchestration-v1`  
**Paid research executed:** **No** (`$0`)  
**Fourteen demand segments preserved:** Yes  
**CMI as demand segment:** No (orthogonal)

---

## Objective delivered

Each first-wave hunter discovers via a real source adapter (public / historical official evidence). Discoveries enter shared normalize → screen → dedupe → L1/L2/L3 depth selection → ORS/qualification handoff. Paid Exa/Apify are **planned only** until founder authorization.

Ten PowerShell hunters remain independently editable and executable.

---

## Architecture

| Layer | Module |
|---|---|
| Normalize | `orchestration/normalize-discovery-v1.js` |
| Dedupe | `orchestration/discovery-dedupe-v1.js` |
| L1/L2/L3 depth | `orchestration/research-depth-selector-v1.js` |
| Measurements | `orchestration/hunter-measurement-v1.js` |
| Orchestrator | `orchestration/hunter-to-exa-orchestration-v1.js` |
| Public adapters | `adapters/public-source-adapters-v1.js` |
| Exa bridge (plan/quote) | `adapters/exa-hunter-bridge-v1.js` |
| Apify bridge (mock/validate) | `adapters/apify-hunter-bridge-v1.js` |

Reuses: `runExaAgentResearch` / `quoteExaAgentRun`, `inspectExaLiveDiscoveryReadiness`, Apify discovery provider V1, independent demand discovery, Phase-2 qualification handoff, weekly supervisor soft plan.

---

## First-wave source adapters (zero-paid default)

| Hunter | Adapter path |
|---|---|
| Government Procurement | SAM.gov NIH SBPO historical official notice (+ optional live GET) |
| Project Workforce | USASpending historical official surface; optional CPE independent public |
| Corporate Meeting Triggers | PCMA public program surface; optional CMT independent public |
| Sports Housing | Maryland SoccerPlex public program + Party Bus rejection fixtures |
| University Demand | Georgetown University events public calendar |

Second/third-wave hunters remain independently editable entry points (cache/stubs) — unchanged by first-wave adapter work.

---

## Research levels

| Level | Behavior |
|---|---|
| **L1** | Discovery + screening (default; always free) |
| **L2** | Targeted evidence-gap research — **planned** when gaps justify; **not executed** without founder auth |
| **L3** | Deep qualification — **planned** only with eligibility + budget; **not executed** |

Paid retrieval is skipped when existing evidence is sufficient. High-impact unresolved questions (named buyer, future date, thesis gaps) are queued first.

---

## Acceptance results

### 1) Genuine zero-paid discovery + qualification (Bethesda)

- **Hunter:** `government_procurement`
- **Evidence:** SAM.gov NIH SBPO Vendor Outreach Session (`HISTORICAL_RESEARCH_REPLAY`)
- **Incremental unique candidates:** 1
- **ORS routed:** 1 (`QUEUED_FOR_QUALIFICATION`)
- **Sales Ready / auto-publish:** 0
- **Spend:** $0
- Artifact: `zero-paid-gov/government_procurement-recLuxvwwxID7U2B8-orchestration.json`

### 2) Bounded paid pilots prepared (not executed)

- 5 first-wave hunters × $3 ceiling = **$15 planned ceiling**
- Exa quotes + Apify actor validations only
- Artifact: `bounded-paid-pilots-prepared.json`
- Gates still required: founder budget auth, `GDI_EXA_AGENT_LIVE` / `GDI_APIFY_LIVE`, hunter-base paid unlock

### 3) Hunter independence

- Disabling `sports_housing` via config does **not** disable `government_procurement`
- Other nine runners still resolve
- Sports leisure rejection fixtures stay local to sports hunter

---

## Measurements (per hunter)

`sourceDiscoveries`, `incrementalUniqueCandidates`, `evidenceGroundedHotelTheses`, `qualifiedOpportunities`, `usefulContacts`, `rejectedLeads`, `duplicates`, `researchCostUsd`, `costPerQualifiedOpportunityUsd`

Success metric remains **incremental commercially credible opportunities**, not API call volume.

---

## Commands

```bash
npm run test:gdi-nextgen-hunter-exa-orchestration-v1
npm run test:gdi-nextgen-discovery-architecture-v1
npm run gdi:nextgen-hunter-exa-orchestration-demo
# Independent PowerShell (unchanged entrypoints):
# scripts/gdi-nextgen-hunters/Invoke-GdiGovernmentProcurementHunter.ps1 -HotelId rec… -DryRun
```

---

## Merge readiness

Focused extension of merged nextgen architecture. No auto-promote, no publication widening, no paid spend, no fifteenth segment. Ready when CI is green.
