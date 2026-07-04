# Operator Alignment Snapshot — Phase 1 (Profile-Level)

**Date:** 2026-05-25  
**Status:** Implemented (API + utilities + fixture; no UI)

## What was implemented

Phase 1 adds a **profile-level** Operator Alignment Snapshot foundation:

- Five **operator profile archetypes** (config fixture, no Airtable changes)
- **Deal signal evaluation** and alignment bands (qualitative; numeric score intentionally `null`)
- **Operator review signal** (High / Medium / Low / Insufficient Data)
- **GET API** returning deal context, profiles for review, data gaps, and suggested workflow actions

This phase does **not** score named operators (`scoreOperatorMatchForDeal` unchanged).

## Files created

| File | Purpose |
|------|---------|
| `fixtures/operator-profile-archetypes.json` | Five profile categories + copy templates |
| `lib/operator-alignment-profile-utils.js` | Archetype load, deal context, signal eval, snapshot build |
| `api/operator-alignment-snapshot.js` | HTTP handler for profile mode |
| `scripts/validate-operator-profile-archetypes.mjs` | Fixture + copy + sample deal validation |
| `docs/operator-alignment-snapshot-phase-1.md` | This document |

## Files modified

| File | Change |
|------|--------|
| `server.js` | Register `GET /api/operator-alignment-snapshot/:dealId/profile` with `myDealsDealAuth` |
| `server.upload-ready.js` | Same route registration (parity with main server) |

## Why `lib/` instead of `api/lib/`

Shared alignment logic lives in **`lib/operator-alignment-profile-utils.js`**, matching **`lib/brand-alignment-rationale.js`** (used by `api/brand-alignment-snapshot.js`). Keeps API modules thin and avoids duplicating `api/lib` vs `lib` operator-setup mirrors for new code.

## API route

```
GET /api/operator-alignment-snapshot/:dealId/profile
```

- **Auth:** Same as My Deals deal routes (`myDealsDealAuth` → `requireDealRecordAccess`)
- **dealId:** Airtable Deals record id (`rec…`)
- **Data load:** `fetchDealScoringContext` from `api/my-deals.js` (Deals + Location + Strategic Intent + Market Performance)

### Example request

```bash
curl -s -H "Authorization: Bearer YOUR_TOKEN" \
  "http://localhost:3000/api/operator-alignment-snapshot/recXXXXXXXXXXXXXX/profile"
```

Replace host/port and auth with your local Dealality proxy setup (Memberstack/session cookie as required by `myDealsDealAuth`).

### Example response shape

```json
{
  "success": true,
  "dealId": "recXXXXXXXXXXXXXX",
  "generatedAt": "2026-05-25T12:00:00.000Z",
  "featureName": "Operator Alignment Snapshot",
  "mode": "profile",
  "methodologyNote": "…",
  "dealContext": {
    "dealName": "…",
    "country": "Mexico",
    "cityOrMarket": "…",
    "primaryMarketRegion": "CALA",
    "roomCount": 186,
    "projectType": "New Build",
    "projectTypeKind": "new_build",
    "assetStatus": "…",
    "brandStatus": "…",
    "currentOperatingModel": "…",
    "desiredOperatingModel": "…",
    "serviceModel": "…",
    "chainScale": "…",
    "indicators": { "conversionOrReflag": false, "operatorInScope": true, … }
  },
  "operatorReviewSignal": {
    "level": "High",
    "rationale": "…",
    "matchedSignals": ["new_build_project", "cala_market", …]
  },
  "profilesForReview": [
    {
      "profileKey": "regional_cala_full_service",
      "displayLabel": "Regional CALA Full-Service Operator",
      "shortLabel": "Regional CALA Full-Service",
      "alignmentBand": "Moderate Alignment Signals",
      "alignmentScoreOptional": null,
      "alignmentSignals": ["…"],
      "reviewConsiderations": ["…"],
      "questionsToClarify": ["…"],
      "dataGaps": ["…"],
      "suggestedWorkflowActions": ["…"],
      "explanation": "…",
      "matchedDealSignals": ["…"],
      "missingDealSignals": [],
      "sortPriority": 10
    }
  ],
  "dataGaps": ["…"],
  "suggestedWorkflowActions": ["…"]
}
```

## Alignment bands (Phase 1)

- Strong Alignment Signals  
- Moderate Alignment Signals  
- Conditional Alignment Signals  
- Limited Alignment Signals  
- Insufficient Data  

`alignmentScoreOptional` is **`null`** for all profiles in Phase 1 (no false precision).

## How this differs from Operator Capability Snapshot (OCS)

| | OCS | OAS Phase 1 |
|---|-----|-------------|
| Focus | Deal capability **themes** from deal-only fields | **Operator profile categories** for review |
| Operators named | No | No |
| Scoring | Status gating (`allowed` / `limited` / `blocked`) | Alignment **bands** per profile archetype |
| API | `GET /api/deals/:dealId/operator-capability-snapshot` | `GET /api/operator-alignment-snapshot/:dealId/profile` |

OCS and OAS can be shown side-by-side later; neither replaces the other.

## How this differs from specific-operator scoring

`scoreOperatorMatchForDeal` in `api/my-deals.js` compares a deal to **one** Operator Setup record via prefill. Phase 1 does **not** call it. Phase 4 will add specific-operator rows using that engine.

## Known limitations

- Profile taxonomy maps from legacy `Preferred Third-Party Operator Profile` options (Regional, International, etc.), not the five display labels stored in Airtable.
- Geography matching is rule-based (country, CALA set, region)—not operator footprint data.
- No persistence on deal; response is computed per request.
- No UI, print page, or My Deals action (later phases).
- `server.upload-ready.js` still lacks OCS routes (pre-existing); OAS route **is** registered on both servers.

## Validation

```bash
node scripts/validate-operator-profile-archetypes.mjs
```

Validates fixture structure, five keys, required fields, banned phrasing, and a sample run on `fixtures/sample-deals/aeropuerto-cancun-select-service.example.json`.

## Future naming cleanup (not done in Phase 1)

Existing code still uses internal names such as **Operator Match** (`scoreOperatorMatchForDeal`, `operator-match-score-breakdown`). New code uses **Operator Alignment Snapshot** only. Rename in a dedicated pass after UI ships.

## Next phase recommendation

1. **Phase 2:** Standalone print page (clone Brand Alignment Snapshot shell) consuming this API.  
2. **Phase 3:** My Deals modal + `data-action="operator-alignment"`.  
3. **Phase 4:** Specific-operator alignment via `scoreOperatorMatchForDeal` + Operator Setup bundles.  

See [operator-alignment-snapshot-implementation-checklist.md](./operator-alignment-snapshot-implementation-checklist.md).
