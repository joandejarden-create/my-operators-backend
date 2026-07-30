# Many Futures — Phase B Return

**Status:** Local prototype complete. Webflow not modified. Not published.

**Branch:** `cursor/many-futures-phase-b-38e7`

**Local preview:** `webflow-embeds/many-futures/preview.html`

---

## 1–6. Breakpoint and question-state captures

Artifacts directory: `/opt/cursor/artifacts/many-futures/phase-b/`

| Delivery item | File |
|---|---|
| Desktop ~1440px (default Q1) | `mf-phase-b-desktop-1440.png` |
| Laptop ~1200px | `mf-phase-b-laptop-1200.png` |
| Tablet ~768px | `mf-phase-b-tablet-768.png` |
| Mobile ~390px | `mf-phase-b-mobile-390.png` |
| Q1–Q6 desktop states | `mf-phase-b-state-{rebrand,operators,affiliation,residences,proposals,clarify}-1440.png` |
| Q1–Q6 mobile states | `mf-phase-b-mobile-state-*.png` |
| Unique feature closeups (11) | `mf-phase-b-feature-*.png` |

---

## 7. Exact verified proposal-response workflow name

**Verified live product label: Submit Proposal**

Evidence basis (Phase A / Phase B product inspection):

| Surface | Exact visible label |
|---|---|
| Brand deal-request form heading | **Submit Proposal** |
| Owner comparison surface | **Deal Compare** |
| Public titles rejected | “Proposal Submissions”, “Structured Deal Responses”, “Offer Request”, “Proposal Responses” (none confirmed as public module titles) |
| Operator proposal submission | **Not available yet** |

Prototype usage:

- Q4 / Q5 supporting panel title: **Submit Proposal** (brand Deal Request form)
- Interface descriptor: **Brand Deal Request**
- Q2 does **not** use Submit Proposal for operators (see substitution below)

---

## 8. Feature visual-truth audit

### Screenshot crops (navy-framed product crops)

| Panel file | Product source | Truth notes |
|---|---|---|
| `brand-explorer-*` | Brand Explorer | Crop of live brand positioning / identity UI |
| `operator-explorer-*` | Operator Explorer | Crop of live operator identity + metrics |
| `fee-estimator-*` | Fee Estimator | Crop of live fee results cards |
| `radar-*` | Dealality Radar | Crop of live Radar market/map UI |
| `opportunity-review-*` | Deal Brief | Marketed as **Opportunity Review**; visual is Deal Brief; descriptor **Deal Brief** |
| `deal-compare-*` | Deal Compare | Crop of live side-by-side commercial comparison |
| `smart-matching-*` | Existing matched-brands / Match Score table | **Not** a fabricated “Smart Matching” app page; marketing title + descriptor **Match Score & Fit Signals** |

### Simplified reconstructions (labeled)

These panels carry **INTERFACE SIMPLIFIED FOR PRESENTATION** and use only fields/actions known to exist:

| Panel file | Product label | Truth notes |
|---|---|---|
| `deal-readiness-*` | Deal Readiness Snapshot | Simplified score, completeness, commercial-input style signals |
| `clause-library-*` | Clause Library | Simplified searchable clause list presentation |
| `financial-term-library-*` | Financial Term Library | Simplified term list presentation |
| `submit-proposal-*` | Submit Proposal | Simplified brand form: Agreement Type, Term & Renewal, Royalty %, Marketing %, Initial Franchise Fee, Key Money |

Demo/illustrative values in simplified panels are presentation placeholders, not claimed live deal data.

### Question → feature mapping (as shipped)

| Q | Primary | Supporting | Supporting | Outcome theme |
|---|---|---|---|---|
| 1 Rebrand | Brand Explorer | Smart Matching | Dealality Radar | Broader credible brand paths |
| 2 Operators | Operator Explorer | Smart Matching | **Deal Readiness** *(substituted)* | Operators that fit—not only known ones |
| 3 Affiliation | Dealality Radar | Deal Readiness | Fee Estimator | Affiliation value vs cost/flexibility |
| 4 Residences | Opportunity Review (Deal Brief) | Brand Explorer | Submit Proposal | Residences vs wider strategy |
| 5 Proposals | Deal Compare | Submit Proposal | Fee Estimator | Full owner value—not headline terms |
| 6 Clarify | Deal Readiness | Clause Library | Financial Term Library | Gaps, terms, decision risks |

Reusable visuals: 11 unique desktop + matching mobile variants (22 PNGs), reused across questions.

---

## 9. Smart Matching confirmation

Smart Matching is shown as a **public marketing capability**, not a fabricated standalone application module:

- Panel title: **Smart Matching**
- Interface descriptor: **Match Score & Fit Signals**
- Visual: existing Match Score / preferred-brand table crop
- No invented “Smart Matching” route, dashboard chrome, or non-existent fields

---

## 10. Keyboard behavior summary

| Interaction | Behavior |
|---|---|
| Tab | Moves focus through question `<button>` elements |
| Focus on question | Desktop/tablet: previews that question’s workspace (same as hover) without pinning |
| Enter / Space | Pins the focused question as active (`aria-pressed="true"`) |
| Click | Pins the question |
| Mouse leave questions column | Restores the pinned question (desktop/tablet) |
| `aria-live="polite"` on workspace | Announces panel changes |
| Mobile | Hover/focus preview disabled; click/Enter/Space select; workspace DOM moves immediately after the active question |

---

## 11. Reduced-motion summary

- Detects `prefers-reduced-motion: reduce`
- Adds `mf-reduced-motion` on the root
- CSS disables transitions/animations on questions, panels, features, and connector paths
- Active transforms suppressed

---

## 12. Estimated production component size

From `node build.cjs` (minified inline embed with CDN-style asset URLs):

| Part | Size |
|---|---|
| CSS | ~10.5 KB |
| JS | ~4.0 KB |
| Markup | ~25.0 KB |
| **Total inline embed** | **~39.6 KB** (~40,548 characters) |

Feature PNGs and hotel image are **external** (not inlined). Combined feature PNG payload is roughly ~2.9 MB on disk (22 files); production should serve from CDN.

---

## 13. CDN-loader implications

- Inline ~40 KB is within a typical Webflow HtmlEmbed comfort zone.
- Still recommended for Phase C+: host CSS/JS/markup/assets on CDN and use the short loader in `cdn-loader.html` (~0.7 KB) so asset updates do not require re-pasting a large embed.
- Loader pattern: link CSS → fetch `markup.html` into host → append `many-futures.js`.
- **Phase B did not upload assets or modify Webflow.** CDN base in `build.cjs` is a placeholder until a real CDN/git tag is chosen.

---

## 14. Functionality that could not be represented truthfully

1. **Operator proposal / response collection** — no sufficiently developed live operator submission UI. Preferred label “Proposal Submissions” was **not** confirmed. **Q2 third panel substituted with Deal Readiness** (preparation before inviting operators).
2. **“Structured Deal Responses”** — not a live public product title; not used.
3. **Branded Residences module** — does not exist; Q4 uses Opportunity Review / Deal Brief + Brand Explorer + Submit Proposal.
4. **Combined “Clause and Term Library”** — not used; shown as separate **Clause Library** and **Financial Term Library**.
5. **Smart Matching as app module** — avoided; capability + Match Score crop only.
6. **Simplified panels** (Deal Readiness, Clause Library, Financial Term Library, Submit Proposal) are reconstructions labeled for presentation—not pixel-perfect live screenshots. Field names match known product language; scores/demo economics are illustrative.
7. **Hotel image** remains temporary (`TEMPORARY IMAGE` / `ILLUSTRATIVE OPPORTUNITY`).

---

## Layout confirmation

| Viewport | Behavior |
|---|---|
| Desktop / laptop | Hotel \| compact questions \| workspace: primary left, two supports stacked right, outcome beneath |
| Tablet | Hotel + questions, then full-width workspace |
| Mobile | Accordion: active question immediately followed by its three feature panels + owner outcome |

Default active question: **Should I rebrand?**

`#platform-features` untouched. Webflow untouched. Not published.
