# Brand Alignment Snapshot (Output 2) — Pre-Build Audit

**Date:** 2026-05-21  
**Scope:** Audit only (no implementation).  
**Working name:** **Brand Alignment Snapshot** (alternatives: Brand Fit Assessment, Brand Review Snapshot, Brand Alignment Output).

**Purpose:** For a **specific deal**, show which brands appear aligned for review based on current deal inputs, why they appear aligned, what watchouts exist, and what should be clarified before outreach—**without** telling the owner which brand to choose.

**Not in scope:** Brand-level “Standards & Owner Considerations” tab (separate Brand Explorer output; see `docs/standards-owner-considerations-v1-audit.md`).

**Sample reference:** Product has an external “Brand Fit Assessment” document (e.g. Project Águila) with advisory sections to reframe—not found as a committed fixture in this repo.

---

## Executive summary

| Capability | Status | Notes |
|------------|--------|--------|
| **Server-side Match Score v2** | **Production-ready** | `api/match-score-server.js` — 9 soft factors + preferred bonus + hard gates; config in `lib/brand-match-scoring-weight-config.js` |
| **Deal + linked record load** | **Exists** | `fetchDealWithMergedLinkedRecords()` in `api/my-deals.js` (same pattern as Deal Readiness) |
| **Owner-selected brands** | **Strategic Intent** | `Preferred Brands` / `Preferred Brands (up to 4)` on linked SI record |
| **Owner shortlist (pipeline)** | **Target List table** | `api/target-list.js` — per-deal brands with status, match score, breakdown JSON |
| **Pre-computed cache** | **Deal Brand Cache** | Preferred scores, per-brand breakdowns, top alternatives, `bestMatchBrand` |
| **Document output shell** | **DRS pattern** | `deal-readiness-snapshot.html` + `.js` + `.css` + modal in `my-deals.html` |
| **Simplified fit API** | **Secondary** | `api/brand-explorer.js` `fit-to-deal` — 5 dimensions, not Match Score New |
| **Legacy fit analyzer** | **Avoid** | `api/brand-fit-analyzer.js` — advisory copy, different scoring model |
| **Brand Alignment Snapshot route** | **Missing** | No `brand-alignment-snapshot.html` or dedicated API yet |

**Recommendation:** Build Output 2 as a **new deterministic API + shared document renderer** (clone DRS book pattern), reusing **`computeMatchScoreForDealBrand`** and **`getBreakdownNewDetails`** only—never client-side scoring, never `computeRecommendedBrand` as user-facing “the answer.”

---

## 1. Existing brand fit / alignment logic

### Primary (reuse for Output 2)

| File | Role | Reuse |
|------|------|--------|
| `api/match-score-server.js` | **Match Score v2** (0–100): 9 soft factors + preferred bonus + hard gates; legacy 19-factor helpers unused on product path | **Core engine** — call `computeMatchScoreForDealBrand()` |
| `api/my-deals.js` | Loads deal + Location, Market Performance, Strategic Intent; `refreshDealBrandCacheForRecordId`; `getMatchScoreBreakdown`; `getAlternativeBrands`; `addRecommendedBrand` | **Data orchestration** — mirror `deal-readiness-review` load path |
| `api/deal-readiness-review.js` | `POST /api/ai/deal-readiness-review` → `buildReadinessFromFields` | **Pattern** for new `brand-alignment-snapshot` endpoint |

**Match Score v2 soft factors** (`BRAND_MATCH_NEW_WEIGHTS` in `lib/brand-match-scoring-weight-config.js`, sum 100%):

| Factor key | Weight % |
|------------|----------|
| geographyPriority | 18 |
| chainScaleProximity | 14 |
| brandStandardsCompatibility | 14 |
| feesToleranceCompatibility | 12 |
| serviceModelAlignment | 10 |
| keyMoneyWillingnessCompatibility | 10 |
| softHardPreference | 8 |
| incentivesMatchCompatibility | 8 |
| agreementsTypeCompatibility | 6 |

**Also:** preferred brand = **+4 bonus** after base (cap 100). Hard gates (key money, agreement type, chain scale 2+, rooms, project type, markets to avoid) → overall **0**. Building type + project stage are breakdown-only.

**Exports used today:**

- `computeMatchScoreForDealBrand(dealFields, locationData, mpData, siData, brandName, baseId, apiKey)` → `{ scoreNew, breakdownNewDetails, … }`
- `getBreakdownNewDetails(...)` → per-factor `label`, `weight`, `brandValue`, `dealValue`, `note`, `score`
- `computeRecommendedBrand(...)` → highest score among **catalog** candidates (excludes preferred); used for cache / “add recommended brand” (internal; not presented as final answer)
- `computeTopAlternativeBrands(...)` → top N non-preferred brands

**Not used (retired):** `passesStrictPreFilters` was deleted in Match Score cleanup P3 — recommendations use chain-scale candidate ordering + Match Score New only (plus key-money hard gate inside scoring).

**Brand data loaded per score** (inside `computeMatchScoreForDealBrand`): Brand Basics, Project Fit (`brandFit`), Footprint, Brand Standards, Fee Structure, Deal Terms, Operational Support — same tables as Brand Setup.

### Secondary / do not use as Output 2 engine

| File | Role | Why avoid |
|------|------|-----------|
| `api/brand-explorer.js` | `POST /api/brand-explorer/fit-to-deal` — 5 heuristic dimensions (project_fit, economics, …), scores ~65–100 | Different model; not aligned with My Deals breakdown UI |
| `api/brand-fit-analyzer.js` | `POST /api/brand-fit-analyzer` — ranks all brands, `generateRecommendations()` with “prioritize”, “excellent fit” | **Advisory language**; hardcoded legacy Deals table id; separate product |
| Former `calculateMatchScoreFromDealData` / client 19-factor calculators | Heuristic / dashboard-side legacy | **Retired** in Match Score cleanup P1–P3 — product uses Match Score New only |

### Scripts / batch (not Output 2 runtime)

| File | Purpose |
|------|---------|
| `scripts/seed-bdr-activity-from-existing-deals.mjs` | Seeds BDR activity; reads Preferred Brands |
| Various census `matchScore` | Independent hotel census — unrelated to deal–brand fit |

---

## 2. Existing data sources

### Deals and linked records

| Source | Airtable / field | Used for scoring |
|--------|------------------|------------------|
| **Deals** | `Deals` table (`AIRTABLE_TABLE_DEALS`) | Project Type, Stage, rooms, chain scale, F&B, parking, amenities, brand status fields |
| **Location & Site** | Linked from deal | Country, chain scale, service model, rooms, building type |
| **Market Performance** | Linked | Preferred deal structure, fee expectations, capital/funding |
| **Strategic Intent** | Linked (`STRATEGIC_INTENT_LINK_FIELD`) | **Preferred Brands**, must-haves, deal breakers, soft/hard preference, incentive interest |
| **Contact Uploads** | Linked (optional) | Key-money filter preference |

Loader: `fetchDealWithMergedLinkedRecords(baseId, apiKey, recordId)` merges linked fields onto deal payload (same as Deal Readiness).

### Owner-selected brands

| Storage | Field / table | API access |
|---------|---------------|------------|
| **Strategic Intent** | `Preferred Brands` or `Preferred Brands (up to 4)` (text or linked records → resolved to names) | `preferredBrandsMapFromSiDataMapResolved()` in `my-deals.js` |
| **Target List** | `Target List` table: `Brand Name`, `Match Score`, `Score Breakdown`, `Status`, `Deal_ID` | `GET /api/target-list/:dealId` |
| **Deal Brand Cache** | `Preferred Brands`, `Preferred Scores` (JSON map), `Breakdown Details By Brand`, `Top Alternatives`, **`Best Match Brand`**, **`Best Match Score`** | Built by `refreshDealBrandCacheForRecordId` |

**Cap:** Max **5** preferred brands (`MAX_PREFERRED_BRANDS` in `addRecommendedBrand` / list shaping).

### Brand catalog / metadata

| Source | Notes |
|--------|--------|
| `Brand Setup - Brand Basics` | Active/Live brands; name, parent, chain scale, positioning |
| `GET /api/brand-library/brands` | List for explorer |
| `GET /api/brand-library/brand?brandId=` | Full profile (not required for alignment snapshot if scoring path loads brand slices internally) |

### Operator / brand status on deal

Deal fields used in readiness and deal setup (available to alignment narrative):

- `Is the hotel currently branded?`, `Current Brand Name`, `Hotel Chain Scale`, `Project Type`, `Stage of Development`, `Deal Status`, etc. (see `docs/deal-readiness-scoring-audit.md`).

### Fit signals / breakdowns today

| Artifact | Exists? |
|----------|---------|
| Per-brand numeric score (Match Score New) | Yes |
| Per-factor score + brand/deal values + notes | Yes — `breakdownNewDetails` |
| Pre-filter exclusion (no score) | No — `passesStrictPreFilters` retired (P3). Key-money gate still forces score 0 when filter=Yes and brand does not offer key money |
| Natural-language “fit signals” separate from factors | **No** — must be **derived** from factor scores + deal/SI context |
| Persisted Alignment Snapshot on deal | **No** |

### Brand criteria (brand side)

Stored in **Brand Setup** linked tables, especially **Project Fit** and **Brand Standards** — already read inside `computeMatchScoreForDealBrand`.

---

## 3. Existing UI work

### My Deals (`public/my-deals.html`) — primary workspace

| Tab / feature | Behavior | Relation to Output 2 |
|---------------|----------|----------------------|
| **Deal Information** | Core deal grid | Launch point for new icon (like readiness) |
| **Matched Brands** | Rows per preferred brand; match score; “Match Details”; alternative brands modal | **Overlaps** snapshot table/cards — today modal not document |
| **Brand Shortlist** | Cross-deal shortlist table (`shortlistTable`) | Portfolio view; different from per-deal snapshot |
| **Brand Shortlist (per deal)** | `target-list` tab — pipeline statuses | Source list for “brands for owner review” |
| **Deal Compare** | iframe `deal-compare.html`; winner modal **hidden/disabled** in release | Comparison UX; not alignment doc |
| **Deal Readiness** | SVG action + modal; `DealReadinessSnapshot.render` embed | **Template for Output 2** |

**DRS launch pattern (reuse):**

1. Action icon `data-action="deal-readiness"` → modal → `POST /api/ai/deal-readiness-review` → `DealReadinessSnapshot.render(..., { embed: true, fullPageHref: '/deal-readiness-snapshot.html?dealId=…' })`
2. Standalone `public/deal-readiness-snapshot.html` with `?dealId=&embed=1&print=1`

### Brand Development Dashboard (`public/brand-development-dashboard.js`)

- Match score badges, **Match Details** via `GET /api/my-deals/:id/match-score-breakdown?brand=`
- **Alternative Brand Suggestions** modal (`getAlternativeBrands`)
- Copy risk: `Strong fit — advance diligence…` / `Weak fit — clarify gaps…` in workflow strings

### Brand Explorer (`public/brand-explorer-combined.html`)

- Brand **profile** exploration, not deal-level alignment document
- Marketing line: “shortlist with confidence” — keep out of snapshot

### Legacy / redirected

| Route / file | Status |
|--------------|--------|
| `/recommended-fit-list` | Redirects to **My Deals** (`server.js`, `app.js`) |
| `public/archive/deal-brand-fit-analyzer.html` | Legacy UI; route `GET /deal-brand-fit-analyzer` still in `server.js` |
| `public/brand-library-compare.html` | Side-by-side **brand** compare (no deal) |
| `public/brand-review.html` | Match score display; promotional copy |

### SVG / modal / print components to reuse

| Asset | Path |
|-------|------|
| Document renderer | `public/js/deal-readiness-snapshot.js` — `render()`, book pages, `OUTPUT_NOTE`, print button |
| Document CSS | `public/css/deal-readiness-snapshot.css` — paper, book flip, print |
| Modal shell | `my-deals.html` — `.my-deals-readiness-modal-*` (clone as brand-alignment modal) |
| Auth fetch | `dealality-memberstack-auth.js` / `fetchMyDealsApi` pattern on standalone page |

---

## 4. Existing language risk

### High risk — do not port into Output 2

| Location | Phrase / pattern | Action |
|----------|------------------|--------|
| `api/brand-fit-analyzer.js` | “prioritizing this brand”, “Excellent fit”, “worth exploring”, “Limited fit” | **Do not use** this module for Output 2 |
| `api/my-deals.js` `addRecommendedBrand` | “recommended brand”, “No recommended brand found” | API name OK internally; **UI label** “Add computed brand” or hide from snapshot |
| `api/match-score-server.js` `getBreakdownNewDetails` | “strong match” in chain scale **note** | Rephrase in **snapshot-only** narrative layer, not in shared breakdown strings (or accept as factor glossary) |
| `public/brand-development-dashboard.js` | “Strong fit — advance…”, “Weak fit — clarify…” | Replace in any shared copy generator |
| Choice/Radisson **fixtures** | “Strong fit when… Weak fit when…” | Not used by match-score API; avoid in snapshot **generated** copy |
| `public/brand-explorer-combined.html` | “shortlist with confidence” | Marketing only |
| `public/deal-setup.html` / `new-deal-setup.html` | “What is a Good Fit”, “lower priority” | Owner **importance radar** — separate product surface |
| `api/deal-readiness-review.js` | `workflowRecommendation`, `scoreImprovementPlan.priorityActions` | Deal readiness only |
| `api/match-score-server.js` | `computeRecommendedBrand`, `Best Match Brand` cache field | Show as **“Highest computed score in catalog (informational)”** with disclaimer, not “recommended brand” |

### Medium risk — rename in Output 2 UI

| Current (sample doc / UI) | Suggested Output 2 label |
|---------------------------|---------------------------|
| Recommended Brand Shortlist | **Brands for Owner Review** |
| Brand Strategy Recommendation | **Brand Pathway View** |
| Recommended Outreach Sequence | **Review sequence (owner/advisor)** or omit v1 |
| Dealality Recommendation | **Remove** |
| Suggested Action | **Review status** |
| Strong Potential Fit | **Higher alignment signal** |
| Potential Fit | **Moderate alignment signal** |
| Conditional Fit | **Conditional review signal** |
| Lower Review Priority | **Lower alignment signal** |
| Why It May Fit | **Alignment rationale** |
| Suggested Next Step | **Clarification before outreach** |
| Best fit / Best option | **Avoid** |
| Prioritize / Proceed | **Avoid** (except owner outreach form fields unrelated to snapshot) |
| Shortlist | **OK** only for **owner-curated** Target List / Preferred Brands (“owner shortlist”) |
| Winner / Select winning brand | **Disabled** in My Deals compare — do not introduce in snapshot |

### Lower risk — acceptable with care

| Location | Notes |
|----------|--------|
| `insight.similar` / Dealality Insight tab | “illustrative peers” — not deal alignment |
| DRS workflow step “Brand alignment review” | Process step name, not brand pick |
| Target List status “Considering” | Pipeline state, not fit verdict |

---

## 5. Proposed v1 architecture

### Routes and files (recommended)

| Piece | Path |
|-------|------|
| Standalone page | `public/brand-alignment-snapshot.html?dealId=rec…&embed=1` |
| Renderer | `public/js/brand-alignment-snapshot.js` (fork from DRS: 3-page book — cover, narrative, detail) |
| Styles | `public/css/brand-alignment-snapshot.css` (fork `deal-readiness-snapshot.css` tokens) |
| API | `api/brand-alignment-snapshot.js` |
| Server | `server.js`: `GET` redirects + `POST /api/ai/brand-alignment-snapshot` (+ optional `GET …/meta`) |
| Launch | `my-deals.html` + optionally `new-deal-setup.html`: icon, modal, `fullPageHref` |

**Do not** score in the browser. Single `POST` returns full snapshot JSON.

### API design (recommended)

**`POST /api/ai/brand-alignment-snapshot`**

Body: `{ dealId: "rec…" }`

Server steps:

1. `fetchDealWithMergedLinkedRecords` (require Strategic Intent link; return 400 if missing).
2. Resolve **brand universe** for snapshot:
   - **Primary:** owner preferred brand names (SI, max 5).
   - **Union:** active Target List brands for deal (status ≠ Deleted).
   - **Optional v1.1:** include `Deal Brand Cache.topAlternatives` as “Additional brands for review” section—clearly labeled **not owner-selected**, computed from catalog.
3. For each brand: `computeMatchScoreForDealBrand` (server-side); collect `scoreNew` + `breakdownNewDetails`.
4. Optionally refresh cache async if stale (`POST refresh-brand-cache`) — or read cache first for speed, recompute on demand when `?fresh=1`.
5. Build **neutral narrative layer** in API (template strings from factor scores, missing fields, pre-filter failures):
   - Alignment tier from score bands (configurable, non-advisory labels).
   - Top signals = top 3–5 factors by weighted contribution.
   - Watchouts = factors below threshold + SI deal breakers + pre-filter misses.
   - Clarifications = merge `generateDiligenceQuestions` patterns + deal field gaps.
6. Return JSON contract (see §6); **no** field named `recommendedBrand` in response.

**Reuse:** `getAlternativeBrands` / cache shape for “additional brands” block only.

**Avoid as primary output:** `computeRecommendedBrand` — if surfaced, rename to `highestCatalogAlignmentScore` with explicit non-recommendation disclaimer.

### Client rendering

Mirror `DealReadinessSnapshot.render(root, data, { embed, fullPageHref, dealId, listDeal, footerHtml })`.

- Page 0: Cover (deal meta, status Draft for validation, generated date).
- Page 1: Narrative (summary, pathway view, top signals, review considerations, clarifications, output note).
- Page 2: Table + per-brand cards + factor breakdown + methodology appendix.

### Why not extend existing endpoints only

| Endpoint | Gap |
|----------|-----|
| `match-score-breakdown?brand=` | One brand; no multi-brand narrative |
| `alternative-brands` | Excludes preferred; no document structure |
| `fit-to-deal` | Wrong scoring model |

---

## 6. Recommended data contract (v1)

```json
{
  "success": true,
  "version": 1,
  "generatedAt": "ISO-8601",
  "outputType": "Internal owner/advisor review",
  "statusLabel": "Draft for validation",
  "disclaimer": "Neutral OUTPUT_NOTE-style paragraph (no recommendation / no endorsement)",
  "deal": {
    "id": "rec…",
    "name": "Project Águila",
    "market": "City, Country",
    "keys": 180,
    "projectType": "Conversion / Reflag",
    "targetPositioning": "Upper-midscale …",
    "stage": "…",
    "currentBrandStatus": "…"
  },
  "brandsForOwnerReview": [
    {
      "brandName": "Radisson Blu",
      "source": "preferred | target_list | catalog_scan",
      "alignmentScore": 78.5,
      "alignmentTier": "higher_alignment_signal",
      "reviewStatus": "requires_validation",
      "alignmentRationale": ["…"],
      "reviewConsiderations": ["…"],
      "clarificationsBeforeOutreach": ["…"],
      "fitSignals": [
        { "factorKey": "projectTypeCompatibility", "label": "Project Type Compatibility", "score": 100, "weight": 10, "summary": "…" }
      ],
      "breakdownNewDetails": { }
    }
  ],
  "pathwayView": {
    "ownerSelectedBrands": ["…"],
    "targetListBrands": ["…"],
    "additionalBrandsForReview": []
  },
  "snapshotTable": [
    { "brandName": "…", "alignmentScore": 0, "alignmentTier": "…", "reviewStatus": "…", "preferredBrandFactor": "yes|no" }
  ],
  "topAlignmentSignals": [],
  "primaryReviewConsiderations": [],
  "clarificationAreas": [],
  "methodology": {
    "scoreModel": "Match Score New",
    "factorWeights": { },
    "tierBands": [
      { "id": "higher_alignment_signal", "min": 75, "max": 100 },
      { "id": "moderate_alignment_signal", "min": 55, "max": 74.9 },
      { "id": "conditional_review_signal", "min": 35, "max": 54.9 },
      { "id": "lower_alignment_signal", "min": 0, "max": 34.9 }
    ],
    "prefilterNotes": "Brands failing strict pre-filters may be omitted or listed with explanation."
  },
  "dataCompleteness": {
    "missingDealFields": [],
    "missingStrategicIntent": false,
    "brandsSkipped": [{ "brandName": "…", "reason": "pre_filter | no_brand_data" }]
  }
}
```

**Tier bands** are illustrative—product should calibrate without implying “approval.”

---

## 7. Neutral language mapping (implementation checklist)

| Avoid | Use instead |
|-------|-------------|
| Dealality recommends | This output summarizes alignment signals based on current inputs |
| Recommended brand | Highest computed catalog score (informational) / brand added to preferred list |
| Best fit / Best option | Higher alignment signal (based on score band) |
| Strong fit / Weak fit | Higher / lower alignment signal; review consideration |
| Prioritize (brand) | Brands listed for owner review |
| Proceed | Continue diligence / confirm with brand |
| Suggested action | Review status |
| Guaranteed / Approved | Confirm with brand / subject to agreement |
| Shortlist (system) | Owner-selected brands / Target List (owner pipeline) |

---

## 8. Validation scenarios (content pattern only)

Use **Project Águila** as a **layout/copy pattern**, not hardcoded `dealId`.

| Scenario | Expected behavior |
|----------|---------------------|
| Multiple owner-selected brands | All scored; snapshot table sorted by score or owner order (product choice); no “winner” |
| Conversion / repositioning | Project type + stage factors drive signals; PIP/clarification bullets |
| New build | Room range + stage factors; different watchouts |
| Sparse / incomplete deal | `dataCompleteness.missingDealFields`; low-confidence disclaimer; tier may be conditional |
| No SI / no preferred brands | 400 or empty “brands for review” + clarifications to complete SI |
| Pre-filter failures | Brand listed in `brandsSkipped` or row with “did not pass initial screening” |
| Cache stale | Scores from cache with `generatedAt`; optional `?fresh=1` recompute |

---

## 9. Implementation plan (phased)

### Phase 0 — Product / legal copy

1. Approve **OUTPUT_NOTE**-style disclaimer for brand alignment.
2. Approve score → tier bands and whether to show numeric score on Page 1.
3. Decide brand universe: preferred only vs preferred + Target List vs include catalog alternatives.
4. Decide whether `bestMatchBrand` from cache appears (informational footnote only).

### Phase 1 — API

1. Add `api/brand-alignment-snapshot.js` with `buildBrandAlignmentFromDeal(...)`.
2. Wire `POST /api/ai/brand-alignment-snapshot` in `server.js`.
3. Unit-test: deal with 2+ preferred brands; sparse deal; missing SI.

### Phase 2 — Document UI

1. Copy DRS book structure → `brand-alignment-snapshot.js` + CSS.
2. Add `brand-alignment-snapshot.html` standalone.
3. Add My Deals modal + row action icon (parallel to readiness).

### Phase 3 — Language hardening

1. Grep gate on new strings (forbidden words list).
2. Refactor BDD “Strong fit/Weak fit” strings if shared helpers introduced.
3. Document neutral labels in `docs/brand-alignment-snapshot.md`.

### Phase 4 — Optional

1. Save snapshot summary fields to Deals table (like readiness score/stage).
2. PDF via print CSS only (no server PDF).
3. Link from Deal Compare / Brand Shortlist tab.

---

## 10. Open questions

1. **Brand universe:** Preferred brands only, or union with Target List and/or top 5 alternatives?
2. **Sort order:** By alignment score vs preserve owner entry order?
3. **Show numeric score** on Page 1 or only tiers + factors?
4. **Pre-filtered brands:** Omit entirely vs include appendix “did not pass initial screening”?
5. **Sample doc:** Can Product add `fixtures/brand-alignment-snapshot-aguila.example.json` (redacted) for renderer QA?
6. **`add-recommended-brand`:** Expose in snapshot UI or keep only in Matched Brands tab?
7. **Persist output:** Cache last snapshot JSON on deal vs compute on every open?
8. **Brand Fit Assessment legacy routes:** Retire `deal-brand-fit-analyzer` and `brand-fit-analyzer` API for public users?

---

## Appendix A — Files and routes index

| Path | Purpose | Output 2 |
|------|---------|----------|
| `api/match-score-server.js` | Match Score New engine | **Reuse** |
| `api/my-deals.js` | Deals, cache, breakdown, alternatives | **Reuse** |
| `api/deal-readiness-review.js` | DRS API pattern | **Mirror** |
| `api/brand-alignment-snapshot.js` | (proposed) | **Create** |
| `api/target-list.js` | Owner shortlist pipeline | **Read** |
| `api/brand-explorer.js` | Explorer + fit-to-deal | **Leave alone** |
| `api/brand-fit-analyzer.js` | Legacy analyzer | **Do not use** |
| `api/brand-library.js` | Brand profiles | Optional |
| `api/deal-compare.js` | Proposals compare | Separate |
| `public/my-deals.html` | Launch UI | **Modify** |
| `public/deal-readiness-snapshot.html` | DRS standalone | **Template** |
| `public/js/deal-readiness-snapshot.js` | DRS renderer | **Fork** |
| `public/css/deal-readiness-snapshot.css` | DRS styles | **Fork** |
| `public/brand-development-dashboard.js` | Match modals | Language audit |
| `public/brand-library-compare.html` | Brand vs brand | Separate |
| `public/archive/deal-brand-fit-analyzer.html` | Legacy | Retire? |
| `server.js` | Routes | **Add** routes |
| `public/app.js` | Shell routes | Optional deep link |

### API routes (existing, relevant)

| Method | Path |
|--------|------|
| POST | `/api/ai/deal-readiness-review` |
| GET | `/api/my-deals/:recordId/match-score-breakdown?brand=` |
| GET | `/api/my-deals/:dealId/alternative-brands` |
| POST | `/api/my-deals/:recordId/add-recommended-brand` |
| POST | `/api/my-deals/:recordId/refresh-brand-cache` |
| GET/POST/PATCH/DELETE | `/api/target-list/...` |
| POST | `/api/brand-explorer/fit-to-deal` |
| POST | `/api/brand-fit-analyzer` |

---

*End of audit.*
