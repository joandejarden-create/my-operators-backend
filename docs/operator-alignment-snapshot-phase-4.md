# Operator Alignment Snapshot — Phase 4 (Operating Companies for Consideration)

**Date:** 2026-05-25  
**Status:** Phase 4 implemented; QA, route fix, and **BAS-parity formatting pass** complete (2026-05-25)  
**Depends on:** [operator-alignment-snapshot-phase-3.md](./operator-alignment-snapshot-phase-3.md)

## Audit conclusion

Named operating companies **can be shown safely** when all of the following are true:

1. Airtable is configured and Operator Setup — Master records load via REST.
2. Operator `submission_status` is **Active** (same rule as Operator Explorer list).
3. Master id is a real `rec…` record (mock Explorer ids like `op-1` are never used).
4. Operator Setup profile has enough structured fields to score (markets, chain scale, services/structures).
5. At least **3** operators pass completeness + produce a non–Insufficient Data band (configurable via env).

If not, the API returns `companiesAvailable: false` with neutral gating copy — **no mock companies**, no recommendation language.

### Data sources audited

| Source | Path | Status | Safe for OAS companies? |
|--------|------|--------|-------------------------|
| Operator Setup — Master/Profile/Platform/Commercial/Governance | `api/lib/operator-setup-new-base-read.js` | **Live** | Yes — primary source |
| Operator list | `api/third-party-operators-list.js` | **Live** | Yes — same tables, Active filter |
| Operator detail | `api/third-party-operator-detail.js` | **Live** | Yes — uses `loadNewBaseOperatorBundle` |
| Operator Explorer | `api/operator-explorer.js` | List live; detail **mock fallback** | List only — OAS does not use mock array |
| Scoring engine | `api/my-deals.js` → `scoreOperatorMatchForDeal` | **Live** | Yes — wrapped, not rewritten |
| Breakdown drill-down | `GET /api/my-deals/:id/operator-match-score-breakdown` | **Live** | Unchanged; optional future UI |
| Legacy `3rd Party Operator - *` | Various | Legacy / empty | **Not used** |
| Brand Target List / Deal Brand Cache | `api/target-list.js` | Brand only | **No operator shortlist** (Phase 5) |

### Risks mitigated

- Mock operators excluded (`rec…` only, no `op-*` ids).
- Test/shadow operator names filtered when `OPERATOR_EXPLORER_HIDE_TEST_RECORDS=1`.
- Weak profiles → `Insufficient Data` or excluded from ranked list.
- Numeric score shown only when ≥3 factors scored and completeness is **sufficient**.
- Copy avoids recommend / best / preferred / top operator language.

## Files created

| File | Purpose |
|------|---------|
| `lib/operator-alignment-company-utils.js` | Load Active operators, completeness gate, wrap scoring, build companies payload |
| `public/css/operator-alignment-companies.css` | Full snapshot company cards |
| `scripts/validate-operator-alignment-companies.mjs` | Phase 4 validation |
| `docs/operator-alignment-snapshot-phase-4.md` | This document |

## Files modified

| File | Change |
|------|--------|
| `api/my-deals.js` | Export `scoreOperatorMatchForDeal` (no logic change) |
| `api/operator-alignment-snapshot.js` | `GET …/companies` handler |
| `server.js`, `server.upload-ready.js` | Register companies route + auth shim |
| `public/js/operator-alignment-snapshot.js` | Companies section + My Deals preview; QA visibility (`attachCompaniesSnapshot`, `buildOutputNote`, always-on section) |
| `public/operator-alignment-snapshot.html` | Parallel fetch; `attachCompaniesSnapshot` on all outcomes |
| `public/my-deals.html` | Fetch companies; `attachCompaniesSnapshot`; preview uses `resolveCompaniesPayload` |
| `public/css/operator-alignment-companies.css` | Gated block print-friendly spacing |
| `scripts/qa-oas-companies-sample-deal.mjs` | QA dump for sample deal companies API |
| `docs/operator-alignment-snapshot-implementation-checklist.md` | Phase 4 items |

**Not modified:** Airtable schema, BAS, OCS, `operator-match-score-breakdown` behavior, scoring weights.

## API

```
GET /api/operator-alignment-snapshot/:dealId/companies
```

Auth: same as profile (`myDealsDealAuth`).

Response highlights:

- `mode`: `"companies"`
- `sectionName`: `"Operating Companies for Consideration"`
- `companiesAvailable`: boolean
- `gatingReason`: string when gated
- `companiesForConsideration`: array (ranked, live only)
- `dataCompletenessSummary`: `{ activeOperatorRecords, scorableOperators, rankedCompanies, minScorableRequired }`

Per company: `operatorId`, `operatorName`, `alignmentBand`, optional `alignmentScoreOptional`, `alignmentSignals`, `reviewConsiderations`, `dataGaps`, `dataCompleteness`, `sourceStatus` (`live` | `needs review` | `incomplete`).

### Env tuning

| Variable | Default | Meaning |
|----------|---------|---------|
| `OPERATOR_ALIGNMENT_MIN_SCORABLE_OPERATORS` | `3` | Min scorable Active operators to show ranked list |
| `OPERATOR_ALIGNMENT_MAX_COMPANIES` | `25` | Max operators scored per request |

## Scoring approach

- `lib/operator-alignment-company-utils.js` calls exported `scoreOperatorMatchForDeal` with deal + `buildPrefillObjectFromNewBaseRows`.
- Weighted score → OAS alignment band (Strong / Moderate / Conditional / Limited / Insufficient Data).
- Factor rows → neutral **Company-level alignment signals** (no “recommend” wording).

## UI

### My Deals modal

- After profile load, fetches `/companies`.
- If available: up to **3** company cards with band + signals + completeness note.
- If gated: neutral placeholder (replaces Phase 3 static future text).

### Standalone snapshot

- Section **Operating Companies for Consideration** always renders after **Operator Profiles for Review** (company cards or gated block — never omitted).
- When `companiesAvailable`: company cards + neutral company-level note (`COMPANY_LEVEL_NOTE`).
- When gated or companies fetch fails: primary gating copy + supporting line about profile-level alignment above.
- Footer limitation note is **dynamic** via `buildOutputNote()` (profile-only vs profile + company variants).

### My Deals modal

- Always shows **Operating companies for consideration** with up to 3 cards or the same gated/support copy (Phase 3 placeholder removed).
- Uses `attachCompaniesSnapshot` so a failed `/companies` request still shows gated state, not a blank block.

## BAS-Parity Formatting Pass (2026-05-25)

### BAS patterns reused

| BAS pattern | OAS application |
|-------------|-----------------|
| `bas-cover-page` hero + logo footer | Cover fills ~88vh; print min-height 7.25in; summary flows on same page (no forced blank page) |
| `bas-brief-card` + grid | **Snapshot Summary** — Deal Context + Operator Review Signal side-by-side |
| `bas-brand-card` header hierarchy | Profile + company cards: title → band → lead/chips → compact bullets |
| `bas-section--keep` / `bas-avoid-break` | Section headers stay with first card; cards avoid page splits |
| Compact narrative vs. report | Truncated bullets; section-level notes instead of per-card boilerplate |

### Formatting issues addressed

- Half-empty cover page on PDF
- Text-heavy profile cards (unlimited lists)
- Company cards leading with markets; repeated explanation paragraph per card
- Prominent “Alignment detail score” label
- Mechanical API signal/review copy repeated on every card
- Same data gaps repeated 10×
- 13-page PDF with all 10 companies at full detail

### Company card display rules

| Context | Limit | Notes |
|---------|-------|-------|
| Full snapshot (screen) | 5 visible + **Show all** button | Extra cards use `.oas-company-card--limited-extra` |
| Full snapshot (print/PDF) | **5** | Copy: “Showing 5 of N companies with sufficient Operator Setup data.” |
| My Deals modal | **3** | Company name first; market chips; 2 humanized signals |

- **Markets:** first 5 chips + `+N more` (full list in `title` tooltip).
- **Score:** `Informational score: 69` — small secondary line; band is primary.
- **Section intro:** single `OAS_COMPANY_SECTION_NOTE` (no per-card “derived from weighted comparison…” text).
- **Common data gaps:** shared box when ≥50% of companies share the same gap; cards show unique gaps only (max 1) or “No major data gaps surfaced.”
- **Signals / review:** UI-layer `humanizeCompanyAlignmentSignal` / `humanizeCompanyReviewConsideration` (scoring engine unchanged).

### Remaining limitations

- Screen “Show all” reveals full company list (may lengthen PDF if user prints after expanding).
- Profile cards still show five categories; content truncated, not removed from API.
- No per-company detail drawer (breakdown API exists but not wired in OAS UI).

## BAS-Parity Formatting Pass 2 (2026-05-25)

### Remaining issues found in PDF (pass 1)

- Snapshot Summary used BAS **dark** `.bas-brief-card` styles (root has `brand-alignment-snapshot` class) — white/muted text on dark blocks, dense and unlike BAS narrative **light** panels.
- Profile cards were single-column, tall, with per-card overflow boilerplate.
- Company titles competed with BAS inherited styles; score/markets lacked strict header/body separation.
- Data Gaps + Workflow Actions listed every item (long tail pages).

### Fixes

| Area | Change |
|------|--------|
| **Company names** | `resolveCompanyDisplayName()` — `operatorName` / `companyName`; `data-oas-company-name` on title; API adds `companyName` mirror; fallback + gap note if missing |
| **Summary** | `oas-brief-card` light cards; compact fact list (6 fields); max 6 relevance chips |
| **Profiles** | 2-column grid; `oas-card-header` (large title + band); 2-col bullets (max 2 each side) |
| **Companies** | Header-only identity + badges + small score; body = markets then signals; 2-column grid; max 2 signals / 1 review line |
| **Closing** | **Key Follow-Ups** — max 5 gaps + max 5 actions; platform overflow note |
| **Print** | Tighter padding; `print-color-adjust` on titles/bands; target **6–8 pages** |

### Display limits (pass 2)

| Item | Limit |
|------|-------|
| Print companies | 5 |
| My Deals companies | 3 |
| Profile bullets per column | 2 |
| Company alignment signals | 2 |
| Company review considerations | 1 |
| Key data gaps (closing) | 5 |
| Workflow actions (closing) | 5 |

### Remaining limitations

- Two-column print grid may collapse to one column on very narrow print margins.
- All 5 profile categories still print (compact); cannot hide categories without product change.

## Brand Assessment Structure Parity Pass (2026-05-25)

### Why the old OAS looked different from Brand Assessment

- **Brand Alignment Snapshot was not changed** — it still uses the 3-page **book** layout (cover → narrative → detail) defined in `brand-alignment-snapshot.js` + `brand-alignment-snapshot.css`.
- **Operator Alignment Snapshot was built separately** as a single scrolling document with 2-column cards (passes 1–2). That architecture never matched the Brand Assessment PDF, even when company data worked.
- The summary block also inherited **dark** `.bas-brief-card` styles from the shared `brand-alignment-snapshot` root class while OAS tried to force a light scroll layout — another visual mismatch.

### New document structure (full snapshot / PDF)

| Page | Content |
|------|---------|
| 1 | Full-page dark cover (DEALALITY OPERATOR ALIGNMENT SNAPSHOT) |
| 2 | **Operator Alignment Narrative** — stat cards, sections 1–6, output note |
| 3 | **Operator Alignment Detail** — snapshot table, operator-by-operator cards (top 5), common questions, methodology |

**Narrative sections:** Operator Alignment Summary · Operator Pathway View · Operating Companies for Owner Review (table, up to 8) · Primary Review Considerations · Clarification Areas · Current Review Status.

**Detail sections:** Operator Alignment Snapshot Table · Operator-by-Operator Review Cards (BAS-style full-width) · Common Questions · Methodology Note.

### My Deals vs full PDF

| Surface | UX |
|---------|-----|
| **My Deals modal** | Compact preview only (top 3 profiles + top 3 companies) |
| **Full snapshot** | Brand Assessment book (3 pages screen; print expands all pages ~8–12) |

### Company names

- API: `operatorName` + `companyName` (mirror).
- UI: `resolveCompanyDisplayName()` + `data-oas-company-name` on detail card titles.
- Tables and cards lead with company name before markets/score.

### PDF length target

**8–12 pages** in print (aligned with Brand Assessment ~12), not the previous 5–6 page card-compression target.

### Remaining limitations

- Screen UI shows 3 book pages with flip controls; print outputs all pages sequentially.
- Pathway table uses profile archetypes, not named operators.
- Detail cards limited to **5** operators; tables show up to **8**.

## Final Brand Assessment Parity Polish (2026-05-25)

### Cover page fix

- Operator cover HTML now matches Brand Assessment structure (`bas-cover-page` only — no `bas-avoid-break` on the cover section).
- Removed OAS-only `oas-cover` min-height rules that fought the book layout.
- Print CSS mirrors BAS: first book page breaks after cover; `.bas-cover-page` uses `page-break-inside: avoid` and **273mm** min-height so title, metadata, disclaimer, and logo stay on **one dark cover page**. Page 2 starts **Operator Alignment Narrative**.

### Company name on detail cards

- Every operator-by-operator card renders **company name** in `h3.bas-brand-card-title` with `data-oas-company-name` **before** the alignment score line.
- Missing names show **Operating Company** plus a visible gap note.

### Rationale and card copy

- `buildCompanyOwnerRationale()` — company-specific paragraph using operator name, alignment band, and theme phrase.
- Structured sections: What Supports Review · What Needs Validation · What Could Weaken Alignment · Owner Questions · Alignment Factors Reviewed (polished labels, not raw factor keys).

### Raw scoring-label cleanup (UI only)

- `stripTechnicalScoringTail()`, `humanizeCompanyAlignmentSignal()`, and `humanizeCompanyReviewConsideration()` suppress fragments such as `before advancing — Compares…` and lone `fee / commercial`.
- Owner-facing replacements (e.g. fee/commercial assumptions may need validation) without changing `scoreOperatorMatchForDeal` or Airtable.

### Current Review Status logic

- **Ready for controlled operator review after owner/advisor validation** when `companiesAvailable`, **≥ 3** companies in the review set, and operator review signal is **High** or **Medium**.
- Otherwise: additional information needed (gated or insufficient company set).
- Sample deal `recIeGRZP21udmTnt` (10 companies, High signal) should show the **ready** line.

### Remaining limitations

- Detail cards still capped at **5** in print; pathway table remains profile-level archetypes.
- Scoring engine and breakdown API strings are unchanged server-side; humanization is presentation-layer only.

**Cache bust:** `?v=bas-parity-polish` on standalone HTML assets.

## Print Cover and Detail Card Header Fix (2026-05-25)

### Root cause — cover split across two PDF pages

The OAS print stylesheet set `min-height: 273mm` with `height: auto` on `.bas-cover-page`, and applied `page-break-inside: avoid` separately to `.bas-cover-block`, `.bas-cover-disclaimer`, and `.bas-cover-hero`. When the in-flow cover block plus hero exceeded the printable area, the browser fragmented the cover: page 1 held the title/metadata block; page 2 held the disclaimer/logo on a mostly blank sheet. Brand Assessment avoids this with a fixed printable height, absolute disclaimer positioning, compact print padding (in `brand-alignment-snapshot.css`), and a single `page-break-inside: avoid` on the cover container—not on each child.

OAS also used `@page { margin: 12mm 14mm }` without `size: A4`, slightly reducing usable height versus BAS (`12mm` margins).

### Root cause — company name missing before score in PDF

The renderer did emit `h3.bas-brand-card-title` before the score line, but it was wrapped in `header.bas-brand-card-header`. Combined with technical-page print inheritance, the title was easy to miss in PDF output while the score line (`bas-brand-card-score` / first visible block) read as the card opener. The company name only appeared again inside the rationale paragraph.

### Fixes

| Area | Change |
|------|--------|
| Cover DOM | Unchanged BAS order: all elements remain inside one `section.bas-cover-page` (disclaimer → hero before `</section>`). |
| Cover print CSS | `height`/`max-height: 273mm`, flex column, absolute disclaimer, compact block padding; removed per-child `page-break-inside` rules; `@page { size: A4; margin: 12mm }`. |
| Detail cards | Flat markup: `h3.oas-operator-detail-title[data-oas-company-name]` then `p.oas-operator-detail-meta` (score line). |
| Print CSS | Explicit `display: block`, dark ink colors for title/meta in technical page and print host. |

### Expected PDF sequence

1. Full dark cover (title, metadata, disclaimer, logo)
2. Operator Alignment Narrative
3. Operator Alignment Detail (tables + operator-by-operator cards with **company name first**)

**Cache bust:** `?v=print-cover-detail-header`

## BAS print parity fix (2026-05-25)

### Logo missing on cover

OAS print CSS had set `max-height: 273mm` and `overflow: hidden` on `.bas-cover-page`, which **clipped** the in-flow `.bas-cover-hero` / logo block. Brand Assessment uses `min-height: 273mm` with `height: auto` and `overflow: hidden` only on the BAS sheet (logo remains in the flex column). OAS now defers cover dimensions to `brand-alignment-snapshot.css` and forces `overflow: visible` on the print clone.

`printSnapshot()` now activates all book pages on the clone, resets viewport flex heights, and waits for cover `img` load (up to 1.5s) before `window.print()`.

### Blank pages in PDF

Screen full-page rules (`height: 100vh`, flex book viewport) were persisting into print, leaving empty sheets. Print CSS resets `#bas-print-host` descendants to `height: auto`, `min-height: 0`, `display: block`.

**Cache bust:** `?v=bas-print-parity`

## OAS print flat sheets + cover footer (2026-05-25)

Follow-up PDF review still showed title on page 1, disclaimer on page 2, and no logo. Causes:

1. **Absolute disclaimer** in the shared BAS cover CSS does not paginate reliably inside the 3D book wrapper when combined with OAS screen flex layout.
2. **`bas-book-page` wrappers** retained print height/page-break behavior, leaving blank sheets.

**Fixes (OAS only):**

- `renderCover()` wraps disclaimer + logo in `.oas-cover-footer` (in-flow flex row — disclaimer left, logo right).
- `flattenBookForPrint()` unwraps book pages into `.oas-print-sheet` divs before `window.print()`.
- Print CSS: one cover sheet (`min-height: 273mm`, `page-break-inside: avoid`), narrative/detail sheets flow with `height: auto`.
- Logo preload in HTML + image reload on print clone.

**Cache bust:** `?v=oas-print-flat`

## OAS print compact pagination (2026-05-25)

PDF review: 10 pages with disclaimer isolated on page 2, one card per page with large whitespace, blank page 10.

**Causes:** BAS absolute disclaimer still paginated outside in-flow footer; `page-break-inside: avoid` on narrative sections and detail cards (`bas-section--keep`) forced whole blocks to the next page.

**Fixes:**

- Cover: `min-height: 273mm` on `.oas-print-sheet--cover` only; in-flow flex footer; all cover elements `position: static` in print.
- `relaxPrintBreakGuards()` on print clone strips `bas-section--keep` from detail cards and `bas-table-wrap--keep` from narrative tables.
- Print CSS overrides keep-together rules under `#bas-print-host` to `break-inside: auto`.
- Last sheet: no trailing `page-break-after`.
- Detail cards: `oas-print-breakable` (no `bas-section--keep`).

**Cache bust:** `?v=oas-print-compact`

## OAS dedicated print cover (2026-05-25)

PDF still split cover: page 1 title only, page 2 disclaimer only (no logo). Root cause: shared BAS `.bas-cover-disclaimer { position: absolute }` — Chrome prints absolute footer after the in-flow title block.

**Fix:** `rebuildPrintCover()` replaces the cover sheet before print with `.oas-print-cover` markup (no `bas-cover-disclaimer` class). Disclaimer uses `.oas-print-cover__note` in a flex footer with local `/deal-capture-logo-outline.svg` (CDN fallback on error).

**Cache bust:** `?v=oas-print-cover-v2`

## Phase 5 (not in this phase)

- **Deal Operator Review Set / Operator Shortlist Cache** — no existing operator analogue to Target List; document only.
- Explorer `?dealId=` alignment badges.
- Batch persist scores to Airtable.
- “Add to review set” button (no safe structure yet).

## QA: Sample Deal Company-Level Response

**Deal:** `recIeGRZP21udmTnt`  
**Script:** `node scripts/qa-oas-companies-sample-deal.mjs` (requires `.env` Airtable credentials)

| Field | Value (2026-05-25 QA) |
|-------|------------------------|
| `companiesAvailable` | **true** |
| `companiesForConsideration.length` | **10** (capped by `OPERATOR_ALIGNMENT_MAX_COMPANIES`, default 25) |
| `gatingReason` | `null` |
| `dataCompletenessSummary` | `activeOperatorRecords: 10`, `scorableOperators: 10`, `rankedCompanies: 10`, `minScorableRequired: 3` |

Sample operator names (first five): Viento Sur Gestión Hotelera, Mangle Azul Hospitalidad, Panamerican Lodging Partners S.A., Barrio Hotelero CDMX, Metro Lodging São Paulo.

**API shape (success):**

```json
{
  "success": true,
  "mode": "companies",
  "sectionName": "Operating Companies for Consideration",
  "companiesAvailable": true,
  "gatingReason": null,
  "companiesForConsideration": [ "…array of company objects…" ],
  "dataCompletenessSummary": { "activeOperatorRecords": 10, "scorableOperators": 10, "rankedCompanies": 10, "minScorableRequired": 3 },
  "dataGaps": [],
  "suggestedWorkflowActions": []
}
```

When gated, expect `companiesAvailable: false`, non-empty `gatingReason`, empty `companiesForConsideration`, and `dataCompletenessSummary` explaining active vs scorable counts.

## QA Fix: Browser Route/Auth Loading (2026-05-25)

### Root cause

The **running Node process was stale**: it had been started before `GET /api/operator-alignment-snapshot/:dealId/companies` was added. The profile route already returned **401** (route matched); `/companies` hit the global API 404 fallback and returned `{ error: "API route not found" }`. The frontend wrapped that JSON error in a generic parenthetical, so the PDF showed “API route not found” and the profile-only footer.

This was **not** an Airtable, auth-helper URL rewrite, or path typo — `server.js` and `server.upload-ready.js` already registered the route before the API 404 middleware.

### Fixes

1. **Restart required** after pulling Phase 4 server changes (startup log now lists OAS profile + companies routes).
2. **`companiesFetchUserMessage()`** — distinct copy for 401/403, 404, 500, network, and `companiesAvailable: false` (no more collapsing all failures into “route not found”).
3. **`fetchCompaniesApiPack()`** — companies fetch no longer fails the whole page on auth errors; passes `status` into `attachCompaniesSnapshot`.
4. **Cache bust** — `operator-alignment-snapshot.js?v=phase4-companies-route` on standalone + My Deals.

### API direct test (after restart)

| Endpoint | Unauthenticated |
|----------|-----------------|
| `GET …/profile` | **401** `authentication_required` |
| `GET …/companies` | **401** `authentication_required` (not 404) |

Authenticated browser session should return **200** with `companiesAvailable: true` for `recIeGRZP21udmTnt` (10 companies per `qa-oas-companies-sample-deal.mjs`).

### PDF expectation (sample deal, signed in)

- **Operating Companies for Consideration** with up to 10 company cards
- Footer: “This snapshot includes profile-level alignment and company-level alignment signals…”
- DevTools on localhost or `?oasDebug=1`: `[OAS companies QA] companiesAvailable: true`, `companyCards: 10`
- No “API route not found” or profile-only footer when companies load successfully

### QA findings (visibility pass)

**Root cause (PDF profile-only):** Standalone HTML only attached `companiesSnapshot` when `/companies` returned `success`; the renderer returned an empty string when payload was missing; footer used static profile-only `methodologyNote` / `OUTPUT_NOTE`.

**Fixes:**

- `attachCompaniesSnapshot()` on standalone + My Deals so failed/missing API still sets `mode: "companies"` gated payload.
- `renderCompaniesForConsiderationSection()` always emits the section title and either cards or gated block.
- `buildOutputNote()` switches footer copy when companies are available.
- Dev-only `console.info("[OAS companies QA]", …)` on `localhost` or `?oasDebug=1`.

**PDF / print expectation:** After server restart and hard refresh, `/operator-alignment-snapshot.html?dealId=recIeGRZP21udmTnt` should print **Operating Companies for Consideration** with up to 10 company cards and footer text mentioning company-level alignment signals (not the legacy “does not evaluate named operators” line).

## How to test locally

```bash
node scripts/validate-operator-alignment-companies.mjs
node scripts/test-operator-alignment-snapshot-page.mjs
node scripts/qa-oas-companies-sample-deal.mjs recIeGRZP21udmTnt
```

1. Restart Node server (registers `/companies` route).
2. Sign in on My Deals.
3. Open Operator Alignment Snapshot on a deal with intake + Active operators in Airtable.
4. Confirm modal company block or gating message.
5. Open full snapshot — company section or gating copy.

**URLs:**

- `http://localhost:8080/operator-alignment-snapshot.html?dealId=recXXXXXXXX`
- API: `GET /api/operator-alignment-snapshot/recXXXXXXXX/companies`

## Known limitations

- Browser must be signed in (Memberstack) for `/companies` to succeed; otherwise UI shows gated copy (not live cards).
- Scoring caps at 25 Active operators per request (performance).
- Explorer detail mock fallback still exists for unrelated pages.
- No per-factor drill-down UI in OAS (breakdown API exists).
- `alignmentScoreOptional` hidden when data is partial or factors &lt; 3.
- No operator shortlist persistence.
