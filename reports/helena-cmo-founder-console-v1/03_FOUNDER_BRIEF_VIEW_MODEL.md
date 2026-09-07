# 03 — Founder Brief View Model

**Schema:** `helena-cmo-founder-brief-v1`  
**Builder:** `lib/helena-cmo/founder-console/brief-view-model.js` → `buildFounderBriefViewModel()`  
**API:** `GET /api/admin/helena-cmo/brief`

## Top-level fields

| Field | Type | Notes |
|---|---|---|
| `ok` | boolean | false if Week pack missing |
| `sourceWeek` | object | label, path, generatedAt |
| `safety` | object | execute/recurring/OS-write flags |
| `attentionCount` | number | pending decisions + draft/pending approvals |
| `thisWeekInOneParagraph` | string | CMO synthesis |
| `top3[]` | cards | max 3 |
| `commercialScoreboard` | tier1/tier2 | UNKNOWN/DATA_GAP allowed |
| `whatIsWorking[]` | strings | |
| `whatIsNotWorking[]` | strings | |
| `whatIsUnknown[]` | strings | must stay visible |
| `helenaRecommends[]` | strings | max 5 |
| `joanNeedsToDecide[]` | decision cards | max 3 normal |
| `nextWeek[]` | strings | |
| `redFlags[]` | {id,severity,text} | exceptions only |
| `approvals[]` | PREPARE queue | |
| `performance` | accountability + measurement | |
| `adpPilots` | pilots[] + candidates[] | |
| `history[]` | prior packs | |
| `evidence.links[]` | supporting paths | |
| `dataSourceMap` | field → source | no duplicate truths |

## Decision card shape

`id`, `decision`, `helenaRecommendation`, `whyNow`, `evidence`, `confidence`, `founderLock`, `icp`, `gtmTrack`, `product`, `productMaturity`, `consequenceOfDoingNothing`, `artifacts[]`, `status`
