# Brand Explorer Comparison Studio (temporary internal QA)

**Branch:** `feat/be-comparison-studio-v1`  
**Contract:** founder visual QA for Phase 2E candidates vs live Presentation  
**Airtable writes:** 0 · **Paid research:** 0 · **Publication:** none

## Local URL

Preferred (BE-owned isolated port — does not use ADP/GDI/HE servers):

```text
http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1
```

Optional brand deep-link:

```text
http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1&brand=receQkxgjlezsc1xg
http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1&brand=reclkgOzvAcBheUSo
```

Start from registered BE workspace:

```bash
# if node_modules missing: junction to HE modules (BE-local only) OR npm install
set PORT=8082
node server.js
```

## Admin Resources

Nav entry: **Admin Resources → Brand Explorer Comparison** (`/brand-explorer-compare`, `roles: ['admin']`, `internalRunbookOnly: true`).

Shared `public/app.js` change — Integration Controller should review on merge.

## Panels

| Side | Source |
|---|---|
| Left | Live `GET /api/brand-library/brand` Presentation (read-only). If unavailable, Curio may use **labeled** frozen baseline fallback; Ascend shows unavailable (no silent substitute). |
| Right | Phase 2E quality candidate overlay (`*-quality-candidate.json`) via real `BrandExplorerAtelierFromApi` |

## Safety

- Review decisions: `localStorage` + optional `POST /api/brand-explorer-compare/review` → `reports/brand-explorer/comparison-studio/` only
- Never writes production Presentation / Brand Basics
- Separate from open PR #447 (focused comparison PR)

## Test

```bash
npm run test:be-comparison-studio-v1
```
