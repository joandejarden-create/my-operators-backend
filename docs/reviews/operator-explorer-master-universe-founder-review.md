# Operator Explorer — Master Universe Founder Review

**Generated:** 2026-08-10T15:42:02.013Z
**Fit/scoring unchanged · Owner pilot disabled · No broad research**

## Which list should I look at?

| If you want… | Look here |
| ------------ | --------- |
| Every Operator Master | Airtable `Operator Setup - Master` (46) / OE — All / `operator-universe-canonical.json` |
| Every **real** operator | Production + Research (37) — exclude Test Fixtures |
| Operators **approved for Explorer** (publishable) | Canonical Explorer Publishable (**9**) |
| Operators still being researched | Record Purpose = Research (13) + Production needing enrichment |
| Operators Fit can evaluate (diagnostic) | Fit Data Ready (**4**) — Fit engine not rewired |
| Test/dummy records | Record Purpose = Test Fixture (9) |
| Internal admin reconciliation | `/internal/operator-explorer-universe.html` |

## 1. Why lists looked inconsistent

Different lists answer different questions (46 Masters ≠ 27 calibration ≠ 34 Brand Basics parents). Phase 1 also used a stricter readiness classifier than dry-run. Only **Grid view** exists on Master — no OE Purpose views yet.

## 2–5. Current 46-Master universe

- Total Masters: **46**
- Production: **24**
- Research: **13**
- Test Fixtures: **9**
- Real: **37**

## 6–7. Calibration 27 & Brand Basics 34

- All 27 resolve to Masters: **YES**
- Brand parents with Masters: **13**
- No Master required / N/A: **8**
- Unknown / research required: **13**
- Aliases (MxM/HMS/NH/…): see alias map — **not** separate Masters

## 8–11. Key distinctions

- Research content-complete but gated: **8**
- Production Explorer Publishable: **9**
- Production needing enrichment: **15**
- Canonical Strong: **5**
- Fit Data Ready: **4**

## 12–18. Artifacts

- Definitions: `docs/product/operator-explorer-universe-definitions.md`
- Why lists differ: `docs/product/operator-explorer-why-lists-differ.md`
- Crosswalk: `reports/operator-explorer-universe-crosswalk.md`
- Resolver: `lib/operator-explorer/operator-universe.js` (**created**)
- Readiness: `lib/operator-explorer/readiness.js` (**created**; canonical counts recalculated)
- View audit/spec: reports + `docs/data/operator-explorer-airtable-view-spec.md`
- Dashboard: `data/operator-explorer/operator-universe-dashboard.json` + internal HTML

## 19. Founder approvals required

1. Canonical universe definitions
2. Canonical resolver module
3. Canonical readiness module
4. Airtable OE view naming/filter spec (create views)
5. Keep Research content-complete gated (recommended: **yes**)
6. Individual Research graduation later only
7. Internal universe dashboard as authoritative admin view
8. Next phase after reconciliation

## 20. Recommended next phase

Adopt shared readiness in Phase 1 payload path → create OE Airtable views → targeted enrichment (named CALA / Webhound when complete). **Still no Fit/owner.**
