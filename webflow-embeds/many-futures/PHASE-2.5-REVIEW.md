# Phase 2.5 — Local refinement review

**Status:** Local only. No Webflow insertion, asset upload, or publish.

**Production Embed character count:** **33,472** (target: &lt;40,000; preferred 35–38k)

**Branch:** `cursor/many-futures-local-prototype-38e7`

---

## 1–4. Updated breakpoint screenshots

| Viewport | File |
|---|---|
| Desktop ~1440px | `screenshots/many-futures-desktop-1440.png` |
| Laptop ~1200px | `screenshots/many-futures-laptop-1200.png` |
| Tablet ~768px | `screenshots/many-futures-tablet-768.png` |
| Mobile ~390px | `screenshots/many-futures-mobile-390.png` |

## 5. Mobile eyebrow treatments (comparison)

| Option | Treatment | Files |
|---|---|---|
| **A** (default in prototype) | Two-line compact: `STRATEGIC CLARITY` / `BEFORE COMMITMENT` | `screenshots/eyebrow-option-a-compare.png`, full: `many-futures-mobile-390.png` |
| **B** | Single line: `BEFORE COMMITMENT` | `screenshots/eyebrow-option-b-compare.png`, full: `many-futures-mobile-eyebrow-b-390.png` |

Preview pages: `preview.html` (A on mobile), `preview-eyebrow-b.html` (B).

---

## 6. New Operator — visual-truth audit

| Check | Result |
|---|---|
| Source screen | Real **Operator Explorer** profile (`operator-explorer.png`) |
| Curated crop | Desktop + mobile crops focus on operator identity + **decision metrics** (HQ, years, hotels, rooms, asset focus, brand mix) |
| Highlight | Restrained accent frame around the metrics row only |
| Removed / subdued | Dense lower story text and full-page chrome reduced via crop + outside dimming |
| Invented UI? | **No** — no new fields, buttons, or analysis |
| Shown interface | Operator Explorer metrics region |
| SUPPORTED BY | Operator Explorer · Smart Matching · Structured Responses |
| Implies all capabilities visible in crop? | **No** — labeled as supporting capabilities, not screenshot contents |
| Path copy | Evaluative: “Compare operating fit, economics, and execution capability.” |

Crops: `assets/crops/new-operator-desktop.png`, `assets/crops/new-operator-mobile.png`

---

## 7. Visual-truth audits — other paths

### Rebrand → Brand Explorer
- Crop focuses on **Brand Positioning / Audience** (highlighted); value scenarios remain visible but subdued.
- SUPPORTED BY: Brand Explorer · Smart Matching · Market Intelligence
- Path copy: “Test whether a different brand could strengthen positioning and asset value.”
- No invented brand modules.

### Soft Brand → Fee Estimator
- Crop focuses on **fee results KPIs** (highlighted) with tier context visible.
- SUPPORTED BY: Brand Explorer · Fee Estimator · Proposal Comparison
- Path copy: “Add distribution and affiliation while preserving greater flexibility.”
- Note: Proposal Comparison is listed as a supporting capability; it is **not** depicted in this crop.

### Independent → Radar
- Crop focuses on summary statistics including **Independent 11,440 / 73%** (highlighted) with regional map context.
- SUPPORTED BY: Market Intelligence · Commercial Readiness · Operating Structure Analysis
- Path copy: “Assess whether independence could strengthen owner control and economics.”
- Note: Commercial Readiness / Operating Structure Analysis are supporting capabilities; crop shows Radar market independence signal only.

### Branded Residences → Opportunity Review
- Crop focuses on **Deal Brief** summary pills + opportunity / property / structure panels (highlighted).
- SUPPORTED BY: Opportunity Review · Brand Explorer · Partner Evaluation
- Path copy: “Evaluate whether residences could strengthen the wider asset strategy.”
- Note: illustrative Deal Brief content remains demo opportunity data from the real UI; no fictional residence-only dashboard invented.

---

## 8. Proposed final hotel image

| Item | Detail |
|---|---|
| File | `assets/hotel-proposed-final.jpg` (also `screenshots/hotel-proposed-final.jpg`) |
| Local in-use | `assets/hotel-temp.jpg` (same image; marked **Temporary image** in UI) |
| Subject | Multi-story white hotel / hospitality building with balconies; tropical urban courtyard palms |
| Fit rationale | Vertical multi-story massing plausible for San Juan urban / upper-upscale conversion; not a low-rise resort poolscape |
| Status | **Temporary — pending approval before Phase 3.** Do not upload to Webflow yet. |

---

## 9. Production Embed size

```
webflow-embeds/many-futures-section.html → 33,472 characters
```

Minified production only (`node build.cjs`). Source CSS/JS/markup remain readable.

## 10. No unavailable functionality introduced

Confirmed:
- Interaction model unchanged (hover preview, click pin, keyboard, mobile accordion).
- No Escape handler, no carousel controls, no decorative unexplained circles.
- No fake product modules, buttons, or outputs beyond curated presentation of existing screens.
- Progressive enhancement, reduced-motion, and scoped selectors preserved.
- Hotel + curated crops remain **local** until Phase 3 approval / CDN upload.

## 11. Before → after summary

| Area | Before (Phase 2) | After (Phase 2.5) |
|---|---|---|
| Product visuals | Full dense platform screenshots | Curated desktop + mobile crops with subdued chrome + one highlight |
| Capability labeling | Capability string alone | **SUPPORTED BY** + capability labels in preview footer |
| Active-path info | Detached caption under preview + decorative circles | Attached preview footer: Active Path / title / sentence / Supported by |
| Path copy | Broader marketing tone | Concise evaluative sentences (approved set) |
| Hotel image | Tropical resort poolscape | Temporary multi-story urban hospitality exterior |
| Mobile | Shrunk desktop screens; large eyebrow pill | Dedicated mobile crops; Option A/B eyebrow treatments |
| Desktop hierarchy | Preview strong; active path softer | Preview dominant; slightly stronger active card + connector (restrained) |
| Embed size | ~44.5k | **33.5k** |

---

## Phase 3 blockers (explicit)

1. Approve hotel image (or request alternate).
2. Choose mobile eyebrow A or B.
3. Approve curated crops / visual-truth audits.
4. Then: upload crop + hotel assets to Webflow CDN, rewrite asset URLs, insert embed above `#platform-features` — **only after approval**.
