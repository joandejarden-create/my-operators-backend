# Packet 2.8 — Research Run Inventory

> Frozen from existing artifacts only. **No new paid research.**  
> Generated: 2026-09-08 · Corpus: `HOTEL_INTELLIGENCE_LEARNING_CORPUS_V1`

## Summary

| Metric | Value |
|--------|------:|
| Hotels | 4 |
| Real Webhound sessions | **5** (not 4) |
| Recorded provider spend | **~$25.4** |
| Full HI dossiers (customer-visible) | 4 |
| Research addenda | 1 (KGPV Change & Opportunity) |
| CALA Census baseline (reports) | **5,956** hotels |

**Do not assume 1 hotel = 1 research run.** KGPV has Full HI + Change & Opportunity.

---

## Runs

### 1. KGPV — Full Hotel Intelligence Investigation

| Field | Value |
|--------|--------|
| Hotel | Krystal Grand Puerto Vallarta (`recUNycnMwOVFX0hc`) |
| Request / run | `req_kgpv_full_hi_run1` / `run_kgpv_full_hi_run1` |
| Webhound session | `23969ba8-60d2-402d-887d-c94f555d3e3b` |
| Report type | `FULL_INVESTIGATION` |
| Template | `FULL_HOTEL_INTELLIGENCE` 1.0.0 |
| Provider | WEBHOUND |
| Started → completed | 2026-09-03 → 2026-09-04 |
| Cost | **$5.4** actual (budget archive noted $20) |
| Dossier | `dossier_kgpv_full_hi_v1` |
| Status | COMPLETED |

**Artifacts:** `data/hotel-intelligence/research/hotels/recUNycnMwOVFX0hc/`; module archive `fixtures/hotel-intelligence/dossier/source-archive/`; deep `fixtures/golden-demo/krystal-grand-pv-deep-research-v1.json`; dossier `fixtures/hotel-intelligence/dossier/kgpv-full-hotel-intelligence-investigation-v1.json`

---

### 2. KGPV — Change & Opportunity Research Addendum

| Field | Value |
|--------|--------|
| Hotel | same |
| Request / run | `req_3c8fd04d50713755` / `run_681a137646388167` |
| Webhound session | `9839ce88-4b31-4f41-baac-208921f7a06e` |
| Report type | `RESEARCH_ADDENDUM` |
| Template | `CHANGE_OPPORTUNITY` 1.1.0 |
| Provider | WEBHOUND |
| Dates | 2026-09-04 |
| Cost | **$5** |
| Report id | `addendum_change_opportunity_ff3e5bf2` |
| Status | COMPLETED |

**Artifacts:** `data/hotel-intelligence/research/addenda/addendum_change_opportunity_ff3e5bf2.json`

---

### 3. Cambridge Beaches — Full HI

| Field | Value |
|--------|--------|
| Hotel | Cambridge Beaches Resort & Spa (`recIwaP1etgx2g9nA`) |
| Request | `req_cambridge_full_hi_run1` |
| Run in store | empty `runs[]` (request + raw artifacts only) |
| Webhound session | `9d6b0a8d-e038-44ce-ab61-2ee92a2a4807` |
| Template | `FULL_HOTEL_INTELLIGENCE` 1.0.0 |
| Provider | WEBHOUND |
| Dates | 2026-09-04 |
| Cost | **$5** |
| Dossier | `dossier_cambridge_beaches_full_hi_v3` (v1/v2 superseded) |
| Status | COMPLETED (customer-visible v3) |

**Artifacts:** `data/hotel-intelligence/research/hotels/recIwaP1etgx2g9nA/`; deep `fixtures/golden-demo/cambridge-beaches-deep-research-v1.json`; dossier v3 under `fixtures/hotel-intelligence/dossier/`

---

### 4. Sheraton Guadalajara Expo — Full HI

| Field | Value |
|--------|--------|
| Hotel | Sheraton Guadalajara Expo (`recsYJb2R1jarPpK3`) |
| Request / run | `req_sheraton_gdl_full_hi_run1` / `run_sheraton_gdl_full_hi_run1` |
| Webhound session | `e4a61251-a016-4c47-a5e4-a1ff57b996cb` |
| Template | `FULL_HOTEL_INTELLIGENCE` 1.0.0 · prompt `FULL_HOTEL_INTELLIGENCE_PROMPT_V1` |
| Provider | WEBHOUND |
| Observed date | 2026-09-08 |
| Cost | **$5** |
| Dossier | `dossier_sheraton_gdl_expo_full_hi_v1` |
| Status | COMPLETED |

**Artifacts:** research store + `fixtures/golden-demo/sheraton-gdl-expo-deep-research-v1.json` + dossier fixture; sessions `data/hotel-intelligence/mexico-explorer-demo-webhound-v1/SESSIONS.json`

---

### 5. voco / Real Inn Cancún — Full HI

| Field | Value |
|--------|--------|
| Hotel | voco Cancún Zona Hotelera / fka Real Inn (`recTYaiA4S6fR6ixx`) |
| Request / run | `req_real_inn_cancun_full_hi_run1` / `run_real_inn_cancun_full_hi_run1` |
| Webhound session | `799762c4-5a98-488c-890c-d8e5acc8c616` |
| Template | `FULL_HOTEL_INTELLIGENCE` 1.0.0 · `FULL_HOTEL_INTELLIGENCE_PROMPT_V1` |
| Provider | WEBHOUND |
| Observed date | 2026-09-08 |
| Cost | **$5** |
| Dossier | `dossier_real_inn_cancun_full_hi_v1` |
| Status | COMPLETED |

**Artifacts:** parallel to Sheraton under Real Inn paths + `fixtures/golden-demo/real-inn-cancun-deep-research-v1.json`

---

## Factory gap snapshot (Packet 2.8 → 2.8A update)

| Target | Status |
|--------|--------|
| Research run inventory | **THIS DOC** |
| Learning corpus freeze | Manifest `HOTEL_INTELLIGENCE_LEARNING_CORPUS_V1.json` |
| `ResearchLearningRegistry` | **PRESENT** — `RESEARCH_LEARNING_REGISTRY.json` (5 CANDIDATE records) |
| Generic `HotelExplorerViewModel` / assembler | **PARTIAL** — shared stub `hotel-explorer-assembler.js`; golden four still on property-specific cohorts (gsf / cambridge / mexico) |
| Domain completeness engine | **PRESENT** — `completeness-evaluator.js` |
| Research planner / playbook router | **PRESENT** — `research-planner.js` (+ templates / planner-triggers / Mexico pubco) |
| Org / brand / market reuse graph | PARTIAL (strategy + pilot clusters; factory reuse pass not executed) |
| Full HI `finding_id` binding | **COMPLETE** — KGPV Full 8/8 · Cambridge 8/8 · Sheraton 6/6 · voco 6/6 · KGPV C&O 8/8 |
| Prompt compiler / investigation spec (2.8A) | **PRESENT** — `compileWebhoundPrompt` + `prompt_hash`; gate `test:hotel-intelligence-prompt-compiler-2-8a` PASSES |
| CALA 25→250→2,500 factory | 25 **SELECTED** (CONFIRMED_IDs) · research exec **HARD STOP** pending founder review · 250+ not started |

## Hard stops honored

- Webhound runs this packet: **0**
- New provider spend: **$0**
- Hotel #5 / Golden Demo #3: **not started**
- 25-hotel research execution: **not started** (selection-only until approved — Packet 2.8A `batch_cala_25_v1` status `selected_only`)
