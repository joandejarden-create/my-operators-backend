# Root cause — GDI opportunity drawer "Failed to fetch"

## Baseline

- PR #308 merged at `0bb869e5db09d8e02cb222389f2cb408a4075068` (ancestor of current `main`).
- Worktree branch: `fix/gdi-opportunity-drawer-fetch-remediation-v1`.
- Dirty primary checkout preserved.

## Reproduction (Radisson `recH9h3McgMNFQEla`)

| Check | Result |
| --- | --- |
| Staged list | 66 opportunities, HM 50 / CMI 16 |
| Fingerprint | `533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d` |
| FHLIP id | `rx_d_2400ebe4c2bb` |
| Package detail (publicationPackage=1) | HTTP 200, ~20KB |
| Live detail (`?live=1`) | HTTP 404 — FHLIP is **staged-only**, not in live store (36 rows) |
| Decision enrich | HTTP 403 `user_not_found` (unauth local) |
| Pursuit enrich | HTTP 200, `pursuit: null` |

## Root causes (shared, not card-specific)

1. **Drawer open blocked on optional enrichments**  
   `openDetail` used `Promise.all([detail, decision, pursuit])`.  
   `.catch` only handles **rejections**. If `/api/hotels/.../decision` **hangs** (Airtable/Memberstack), the drawer stays on Loading until the browser aborts fetch → raw **"Failed to fetch — Try again"**.

2. **List/detail staged preview mismatch**  
   List `opportunitiesApiPath` adds `preview=staged`.  
   `detailUrl` only added `publicationPackage=1&surface=customer` — missing `preview=staged`.  
   Detail API called `resolveOpportunityFromApprovedPackage({ preview: true })` where boolean `true` does **not** set `preferStaged` (`String(true) !== "staged"`).

3. **Live fallthrough for package misses**  
   When a publication package resolved but the ID was absent, detail fell through to `loadOpportunityForDetail` (slow / 404 for staged-only cards) instead of failing closed on the package.

## Fixes

- Detail-first drawer load; decision/pursuit raced with 2.5s timeout.
- Stale-open sequence guard.
- Map network failures to a friendly drawer message (no raw "Failed to fetch").
- Propagate `&preview=staged` on detail URLs.
- Detail API: `preferStaged` + `preview: "staged"` when requested; 404 on `OPPORTUNITY_NOT_IN_PUBLICATION_PACKAGE` without live hang.
- Permanent regression: `npm run test:gdi-opportunity-drawer-fetch-remediation-v1` (all 66 + FHLIP).
