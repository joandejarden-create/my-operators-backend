# AVA V4 — Final 38-Hotel Customer Quality Pass

**Verdict:** `AVA_V4_FINAL_38_HOTEL_CUSTOMER_QUALITY_PASS`

**Branch:** `fix/ava-v4-customer-quality-remediation-v1`  
**Commit:** `514ec0ce04a71434d1b217eb1507ce2346317e3e`  
**Scope:** Local regeneration + local Admin Draft only (no Railway, no publish, no paid research)

---

## A. Root causes fixed

### Pagination
Chromium ignored `page-break-inside: avoid` on styled takeaway divs and `page-break-before: avoid` produced empty highlighted boxes with narrative on the next page.

**Fix:** Wrap Executive Takeaway in a single-cell keep-together table; remove `page-break-before: avoid`; measure remaining printable height before `page.pdf()` and force the whole block to the next page when it will not fit; modest vertical spacing trim on the executive section (no global typography shrink).

### Desired outcomes
A catch-all regex mapped amenities / city-view / events / suites / positioning onto one repeated generic sentence.

**Fix:** Theme-specific `buildDesiredOutcome` branches + uniqueness via `usedOutcomes` Set across the Canonical Action Register; CQA rejects repeated generics.

### Platform-assessment specificity
Registry fallback used “Platform presence identified or still requiring exact-listing confirmation” for multiple statuses.

**Fix:** Distinct copy for exact listing + field review, access-restricted partial assessment, URL-inventoried vs URL-unconfirmed research follow-up, verified corrections, and no-material-issue-in-scope — without inventing verification.

---

## B. Six-hotel independent-review sample

| Hotel | Pages | Actions | SHA-256 | Takeaway pagination |
|---|---:|---:|---|---|
| Waterstone Resort & Marina | 8 | 3 | `4d2c4e31e932020f4ef64c910ccff9f2af58521d111fa4d3c82943ef7445949d` | Intact on page 3 (moved as one block) |
| AC Hotel A Coruña | 9 | 5 | `0fb4017bf0fe754025ded7cd293c3fccf0843589e2e443b6bfd36f3f9772bfe4` | Intact on page 3 |
| Castillo Hotel Son Vida | 11 | 5 | `7bbf03b58732d3c7f0ade73d20b1a1b4386126e1da27a6f9143492c5c174dac0` | Intact on page 3 |
| Cambridge Beaches Resort & Spa | 10 | 5 | `2709d898b7390c910d04a8a41b3e4824ed361f989b612af55f17485578e0097b` | Intact on page 2 (no empty box) |
| Hilton New York Times Square | 9 | 5 | `0dabdfa6a343714fea6a923d3f554852b7fc0e781159ddd2d6d22bead82d796f` | Intact on page 2 |
| Spice Island Beach Resort | 10 | 6 | `a784c489bf6e9f5bd6ed7e35e57ef5abb1ef28ce071e83372d86efdddb8b547f` | Intact on page 2 (no empty box) |

**Remaining editorial notes (non-blocking):** When takeaway moves to page 3, Priority Findings starts on the same page with normal whitespace below the first finding card — not a near-empty final page and not a split takeaway.

Raster review: every page of each of the six hotels inspected via Playwright + pdf.js canvas PNGs under `AVA_V4_FINAL_PDF_VISUAL_QA/`.

---

## C. Full portfolio

| Check | Result |
|---|---|
| Hotels regenerated | **38/38** |
| Priority conflicts | **0** |
| Empty platform assessments | **0** |
| Unsupported verification claims | **0** |
| Generic desired outcomes (repeated catch-all) | **0** |
| Split Executive Takeaways | **0** |
| Empty Takeaway boxes | **0** |
| Orphan headings | **0** |
| Near-empty final pages | **0** |
| Material HIGH findings lost | **0** (63 HIGH/CRITICAL canonical actions retained/reconciled) |
| PDF visual QA (every page rasterized) | **38/38** |
| Manifest contentFlags | **0** hotels flagged |

**New manifest:**  
`reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_38_HOTEL_V4_LOCAL_REGENERATION_MANIFEST.json`

**Visual QA report:**  
`reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_V4_FINAL_PDF_VISUAL_QA/AVA_V4_FINAL_PDF_VISUAL_QA_REPORT.json`

Method: Playwright + pdf.js full-page canvas raster of every PDF page (not HTML section screenshots).

---

## D. Local Admin

| Check | Result |
|---|---|
| Drafts staged into `C:\Dev\dealality\my-operators-backend\data\ava-data` | **38/38** |
| Preview/Download body SHA == manifest | **38/38** |
| Preview header `X-AVA-PDF-Sha256` == manifest | **38/38** |
| Preview ↔ Download parity | **38/38** |
| Published PDFs modified | **0** (`publishedUntouched=true` on promote) |
| Production writes | **0** |

HTTP verify artifact:  
`reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_V4_FINAL_LOCAL_ADMIN_HTTP_VERIFY.json`

Local server: `node server.js` on port **8080** serving Admin at `/app#/admin/ai-demand?tab=visibility-audit`.

---

## E. Release readiness

- **Branch:** `fix/ava-v4-customer-quality-remediation-v1`
- **Commit:** `514ec0ce04a71434d1b217eb1507ce2346317e3e`
- **Files changed (code):** V4 takeaway HTML/CSS, PDF height measurement, Canonical Action Register outcomes, platform assessment composer, CQA gates, convergence takeaway detectors, visual QA + Admin HTTP verify scripts
- **Ready for clean AVA-only PR:** **Yes** (local quality gate complete; still no production deploy / publish in this pass)

---

## Final instruction satisfied

Customer PDFs are regenerated, raster-verified, specific, and served from localhost Admin Draft matching the new manifest.
