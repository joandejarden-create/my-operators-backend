# Operator Explorer — new-base Operator Setup integration (Phase D)

**Date:** 2026-05-25  
**Scope:** Operator Explorer profile/research layer aligned with OAS and Operator Strategy data spine. No Airtable schema, scoring weight, BAS, OCS, or OAS PDF layout changes.

---

## 1. Audit (pre-implementation)

### List data source

| Item | Finding |
|------|---------|
| UI | `public/operator-explorer.html`, `public/js/operator-explorer.js` |
| API | `GET /api/third-party-operators?activeOnly=1` → `api/third-party-operators-list.js` → `buildNewBaseListRow` |
| Normalization | `normalizeOperator()` maps `companyName`, `chainScale`, `regionsSupported`, `companyDescription`, etc. |

### Detail / profile data source

| Item | Finding |
|------|---------|
| UI | `public/operator-explorer-gold-mock.html` + `public/js/operator-explorer-gold-mock-data.js` |
| API | `GET /api/intake/third-party-operators/:id` → `loadNewBaseOperatorBundle` + `buildPrefillObjectFromNewBaseRows` |
| List join | `fetchOperatorBundle()` also loads list row from `third-party-operators` for chain-scale stripe |

### Mock / fallback (before Phase D)

| Path | Behavior | User-visible? |
|------|----------|----------------|
| `api/operator-explorer.js` `MOCK_OPERATORS` | Legacy `GET /api/operator-explorer/operator` for non-`rec…` ids | Only if client called legacy route |
| Gold-mock with no `id` | Embedded sample layout in HTML | Yes — preview page only |
| Gold-mock load failure with `rec…` id | Error panel (no silent demo) | Yes — error message |
| `bootstrap()` `onDemoFallback` | Optional callback; HTML path does not auto-demo on failed live load | No for Strategy/Explorer live opens |

### Fields displayed before Phase D

- Rich tab panels from `explorerProfileJson` + prefixed narrative keys (`overview_*`, `cap_*`, …).
- Partial new-base prefill on operating/markets tabs (`specificMarkets`, `chainScalesSupported`, service offering groups).
- **Gap:** OAS-critical camelCase fields (`activeCountries`, `serviceModelsSupported`, `dataConfidenceLevel`, governance, opening support) were not merged consistently into explorer `ex` or surfaced in a dedicated profile summary.

### Fields OAS / Strategy already use (target for Explorer)

Identity, market presence, operating profile, services, opening/transition, governance, brand/portfolio, data confidence — see `api/lib/operator-setup-new-base-phase-b-fields.json` and `lib/operator-alignment-company-utils.js`.

---

## 2. Implementation summary

### Files modified

| File | Change |
|------|--------|
| `public/js/operator-explorer-new-base-profile.js` | **New** — field map, badges, snapshot sections A–G, deal-aware alignment panel |
| `public/js/operator-explorer-gold-mock-data.js` | Merge new-base keys; snapshot rail on Profile tab; alignment mount; profile chrome |
| `public/operator-explorer-gold-mock.html` | DOM slots, CSS, demo banner, script include |
| `public/js/operator-explorer.js` | Popup only for `rec…` ids; optional `dealId` on URL |
| `public/operator-explorer.html` | Same popup guards + `dealId` passthrough |
| `api/operator-explorer.js` | `MOCK_OPERATORS` behind `OPERATOR_EXPLORER_ALLOW_MOCKS=1`; sample meta flag when allowed |
| `scripts/validate-operator-explorer-new-base-integration.mjs` | **New** static validation |
| `docs/operator-side-end-state-consistency-audit.md` | Explorer row updated |
| `docs/operator-alignment-snapshot-implementation-checklist.md` | Phase D items |

### Data source (after)

- **List:** unchanged — new-base Active operators API.
- **Profile:** unchanged spine — intake detail prefill; **added** explicit merge/display of Phase B / OAS field keys.
- **Deal context:** `GET /api/operator-alignment-snapshot/:dealId/companies` when `?dealId=` present.

### Profile sections (Operator Profile rail — tab “Profile & Positioning”)

A. Profile Snapshot  
B. Market Presence  
C. Operating Profile  
D. Services & Platform  
E. Opening / Transition Support  
F. Owner Reporting & Governance  
G. Brand / Portfolio Experience  

Empty subsections are omitted from the rail grid.

### Badges

Live chips under hero from new-base fields (countries, markets, chain scale, service models, pre-opening, revenue management, owner reporting, data confidence, last updated). Not hardcoded mock labels.

### Deal-aware mode

URL: `/operator-explorer-gold-mock.html?id={rec…}&dealId={dealId}&embed=1` (also `operatorId` / `recordId` aliases).

Shows **Alignment Context** panel: band, informational score, alignment signals, items to validate, key consideration, link to full OAS. Neutral disclaimer; no recommendation language.

If operator not in companies list: “Alignment context is not available for this operator and deal.”

Without `dealId`: panel hidden.

### Mock fallback handling

| Case | Behavior |
|------|----------|
| `rec…` load failure | Error panel — no mock operator shown as live |
| No id (preview page) | **Sample operator profile** banner + embedded demo layout |
| `GET /api/operator-explorer/operator` non-rec | **404** unless `OPERATOR_EXPLORER_ALLOW_MOCKS=1` (dev); response includes `meta.sampleOperatorProfile` when mock served |
| Explorer list popup | Only opens for `rec…` ids |

`OPERATOR_EXPLORER_HIDE_TEST_RECORDS=1` — unchanged on list API.

### Operator Strategy CTA

`openMyDealsOperatorProfileForDeal(operatorId, dealId)` →  
`/operator-explorer-gold-mock.html?id={rec…}&embed=1&dealId={dealId}`  
(Table layout unchanged.)

---

## 3. Remaining gaps

| Gap | Notes |
|-----|--------|
| `companyLogo` | Read when attachment exists; writer still skipped (multipart) |
| `geo_*` footprint grid | Mostly static form; list uses computed regions |
| Explorer consumer writer gaps | ~51 fields still without new-base writer (see field coverage diff) — display when present in prefill |
| Legacy tab narratives | `overview_*` / `cap_*` JSON still drive depth tabs; not removed |
| Per-operator breakdown drawer | Deferred (checklist Phase 5) |

---

## 4. Next recommended phase

1. Backfill / save path validation for Active Countries, markets, and opening-support fields in staging.  
2. Operator Review Set table (Phase 5) using same companies API + Explorer profile CTA.  
3. Optional: collapse redundant “Best Fit” tab copy to neutral “Deal profile” language (product copy pass only).

---

## 5. Claim status

**Improved claim (with caveats):**  
“New-base Operator Setup supports Operator Explorer, OAS, and Operator Strategy” for **live `rec…` records** with shared prefill keys and alignment context — **provided** records are populated and list/detail APIs return data.

**Still not claimed:** full writer coverage, zero mock code paths, or production new-base writer flag enabled.

**Confirmed unchanged in this phase:** Airtable schema, scoring weights, BAS, OCS, OAS PDF layout, Operator Strategy table UX, `OPERATOR_SETUP_USE_NEW_BASE_WRITER` production default, legacy writer.
