# Phase C — Image Quality & Performance Gate

**Status:** Gate completed with documented residuals. Embed inserted in Webflow Designer (not published).  
**Date:** 2026-07-30  
**Branch:** `cursor/many-futures-phase-b-38e7`  
**CDN pin:** `d2aee9c5201f671fde32d4b3ee1e810d7d58c9f0`  
**Site / page:** Old Home `#many-futures` HtmlEmbed `faf5dede-519c-f93e-9c82-66cf832f1bf7`

## Deployment note

- Full style+JS inline Embed ≈ **62 KB** → exceeds Webflow Code Embed ~50 KB limit.
- Phase C production Embed: **linked CSS + linked JS + inline HTML** ≈ **37 KB**.
- Async CDN markup loader remains **backup only** (`cdn-loader.html`).
- `#platform-features` confirmed present and untouched.
- **Not published.**

Webflow Designer canvas / `element_snapshot_tool` timed out repeatedly during this run. Preview QA screenshots below are **local `preview.html` parity captures** of the same markup/CSS/JS deployed to the Embed (assets served locally; production uses the pinned jsDelivr URLs).

---

## 1–6. Deployed product visuals

| Visual | Source (W×H) | Max rendered (CSS px, measured) | Effective scale (disp/src) | Format | Size | Type |
|---|---|---|---|---|---|---|
| Brand Explorer desk | 1024×576 | Primary ≈ **395×252** (1440); tablet primary ≈ **766×488** | 0.39 / **0.75** | PNG | 228 KB | Crop |
| Brand Explorer mob | 780×520 | ≈ **388×247** (390); **318×203** (320) | 0.50 / 0.41 | PNG | 83 KB | Crop |
| Smart Matching (brand) desk | 1024×576 | Support ≈ **282×158** | 0.28 | PNG | 154 KB | Crop |
| Smart Matching (brand) mob | 780×520 | ≈ **388×259** | 0.50 | PNG | 141 KB | Crop |
| Dealality Radar desk | 1024×576 | Support ≈ **282×158** | 0.28 | PNG | 167 KB | Crop |
| Dealality Radar mob | 780×520 | ≈ **388×259** | 0.50 | PNG | 140 KB | Crop |
| Operator Explorer desk | 1024×576 | Primary ≈ **395×252** | 0.39 | PNG | 189 KB | Crop |
| Operator Explorer mob | 780×520 | ≈ **388×247** | 0.50 | PNG | 109 KB | Crop |
| Proof & Track Record desk | 1024×576 | Support ≈ **282×158** | 0.28 | PNG | 332 KB | Crop |
| Proof & Track Record mob | 780×520 | ≈ **388×259** | 0.50 | PNG | 192 KB | Crop |
| Fee Estimator desk | 1024×576 | Support ≈ **282×158** | 0.28 | PNG | 126 KB | Crop |
| Fee Estimator mob | 780×520 | ≈ **388×259** | 0.50 | PNG | 85 KB | Crop |
| Opportunity Review desk | 1024×576 | Primary ≈ **395×252** | 0.39 | PNG | 266 KB | Crop |
| Opportunity Review mob | 780×520 | ≈ **388×247** | 0.50 | PNG | 151 KB | Crop |
| Deal Compare desk | 1024×576 | Primary ≈ **395×252** | 0.39 | PNG | 248 KB | Crop |
| Deal Compare mob | 780×520 | ≈ **388×247** | 0.50 | PNG | 152 KB | Crop |
| Hotel JPG fallback | 1024×1536 | Card ≈ **213×320** (desk) … **359×538** (mob 390) | ≤1.0 when WebP selected | JPG | 381 KB | Crop |
| Hotel WebP 640 | 640×960 | Selected for many 1x desktop/tablet widths | ≤1.0 | WebP | 127 KB | Responsive |
| Hotel WebP 960 | 960×1440 | Selected for mobile 2x | ≤1.0 | WebP | 232 KB | Responsive |
| Hotel WebP 1280 | 1280×1920 | Available for large/retina card widths | ≤1.0 | WebP | 331 KB | Responsive |

### HTML/CSS reconstructions (no raster)

| Visual | Questions | Type |
|---|---|---|
| Operator Smart Matching (Operator Strategy / Alignment Score) | Q2 support | Simplified HTML/CSS |
| Deal Readiness Snapshot | Q3 support, Q6 primary | Simplified HTML/CSS |
| Submit Proposal | Q5 support | Simplified HTML/CSS |
| Clause Library | Q6 support | Simplified HTML/CSS |
| Financial Term Library | Q6 support | Simplified HTML/CSS |

Unused legacy rasters for those reconstructions remain under `assets/features/*` but are **not referenced** by the Embed.

Original hotel source kept outside deployed hot path: `assets/sources-original/hotel-final-source.png` (1024×1536).

---

## 7–8. Screenshots at actual rendered size

Artifacts: `/opt/cursor/artifacts/many-futures/phase-c-quality/`  
Repo copies: `webflow-embeds/many-futures/visual-review/phase-c/`

| Viewport | File |
|---|---|
| Desktop 1440 Q1 | `qa-desktop-1440-q1-rebrand.png` |
| Laptop 1200 Q1 | `qa-laptop-1200-q1-rebrand.png` |
| Tablet 768 Q1 | `qa-tablet-768-q1-rebrand.png` |
| Mobile 390 Q1 | `qa-mobile-390-q1-rebrand.png` |
| Mobile 320 Q1 | `qa-mobile-320-q1-rebrand.png` |
| Desktop 1440 @2x Q1 | `qa-desktop-1440-2x-q1-rebrand.png` |
| Mobile 390 @2x Q1 | `qa-mobile-390-2x-q1-rebrand.png` |
| Zoom 125% / 200% | `qa-desktop-1440-zoom125-q1.png`, `qa-desktop-1440-zoom200-q1.png` |
| All six desktop | `qa-desktop-1440-{rebrand,operators,affiliation,residences,proposals,clarify}.png` |
| All six mobile 390 | `qa-mobile-390-{rebrand,operators,affiliation,residences,proposals,clarify}.png` |
| Primary panel crops | `qa-panel-primary-desk-*.png`, `qa-panel-primary-mob-*.png` |

Raw metrics: `quality-metrics.json`

---

## 9. Upscale confirmation

- **Product PNG crops:** No CSS upscale on desktop/laptop/mobile 1x (scale 0.28–0.50).  
- **Tablet primary raster panels:** display ≈ **766 CSS px** vs source **1024** → scale ≈ **0.75** at 1x (OK). At **2x tablet**, physical need ≈ **1532 px** → **source shortfall** (see §14).  
- **Hotel:** WebP candidates 640/960/1280; `sizes` updated to `280px` desktop / `42vw` tablet / `92vw` mobile so retina prefers larger candidates. No intentional CSS upscale of a low-res file.

---

## 10. Key interface readability

| Panel | Readable feature name / purpose? | Screenshot UI text | Verdict |
|---|---|---|---|
| Brand Explorer | Yes (labels + copy) | Body text soft on mobile crop; headers OK | **Borderline** — needs higher-res source or HTML recon for body copy |
| Smart Matching (brand) | Yes | Scores/table readable at display size | Pass |
| Dealality Radar | Yes | Map + summary stats identifiable | Pass |
| Operator Explorer | Yes | Identity/metrics identifiable | Pass |
| Proof & Track Record | Yes | Experience blocks identifiable | Pass |
| Fee Estimator | Yes | Fee results identifiable | Pass |
| Opportunity Review | Yes | Brief structure identifiable | Pass |
| Deal Compare | Yes | Side-by-side terms identifiable | Pass |
| HTML UIs (5) | Yes — crisp vector text | N/A | **Pass** |
| Hotel | Facade/entrance readable; ILLUSTRATIVE OPPORTUNITY only | N/A | Pass |

Feature name / purpose / why-it-maps-to-the-question are carried by **panel chrome copy**, not microscopic screenshot text.

---

## 11–12. Image weight

| State | Approx weight |
|---|---|
| **Default Q1 eager rasters** (hotel JPG fallback + Brand Explorer + Smart Matching + Radar desk PNGs) | **≈ 929 KB** |
| Default with preferred hotel WebP-960 instead of JPG | **≈ 781 KB** |
| **All deployed raster assets** (desk+mob feature PNGs + hotel JPG + 3 WebP) | **≈ 3.8 MB** |
| **Additional after visiting all six** (vs default eager set) | **≈ 2.9 MB** |

Behavior:

- Inactive panel `img.mf-feat-img` sources deferred (`src` → `data-mf-src`) until activation.
- Default Q1 desk images `loading="eager"`; others lazy / deferred.
- Hotel uses `<picture>` WebP srcset; JPG fallback.
- Question switches: **0 incomplete images** after activate wait; measured CLS ≈ **0.03** (low). No blank workspace observed in capture script.

---

## 13. Lighthouse / performance observations

Local `preview.html` Lighthouse (performance only, headless Chrome):

| Audit | Score / value |
|---|---|
| Performance category | **0.81** |
| Cumulative Layout Shift | **0.99** (≈ 0.041) |
| Unsized images | Pass |
| Offscreen images deferred | Pass |
| Efficient image encoding | Pass |
| Properly size images | **Fail** (tablet/desktop PNG sources larger than some display slots — expected headroom; not upscaling) |
| Next-gen formats | **Fail** for product PNGs (intentional: UI text prefers PNG over lossy WebP) |
| Total byte weight (default load) | ≈ **941 KB** network |
| LCP | 0.28 (local preview shell; not production Webflow chrome) |

JSON: `/opt/cursor/artifacts/many-futures/phase-c-quality/lighthouse.json`

Other:

- Explicit `width`/`height` + CSS `aspect-ratio` → low CLS.
- `decoding="async"`; no base64 in Embed.
- CSS/JS from pinned CDN; HTML inline (not async markup loader).
- Default does **not** preload all six states.
- Designer `element_snapshot_tool` timed out; site **not published**.

---

## 14. Panels still needing higher-resolution sources

1. **All tablet-primary raster panels @2x** — sources are 1024 px wide; tablet primary renders ~766 CSS px (~1532 physical @2x). Prefer **≥1600 px** exports from original product UI, or convert those primaries to HTML where truth allows.  
2. **Brand Explorer (esp. mobile crop)** — positioning/audience body text shows softening / compression halos at actual mobile display size. Prefer recapture at ≥2x or HTML reconstruction of the Positioning/Audience blocks.  
3. **Operator Track Record desk PNG (332 KB)** — heaviest crop; resolution OK at desktop support size, but a tighter crop would reduce weight.  
4. **Guidance gap vs brief:** desktop primary guidance asked for **1400–1600 px** sources; current product sources top out at **1024**. Desktop 1x is not upscaling, but the brief’s 2× headroom is not fully met for tablet/retina primary panels.

---

## QA checklist (parity preview)

| Check | Result |
|---|---|
| 1. No pixelation (desk 1x) | Pass for deployed crops |
| 2. No blurred small text | **Fail/borderline** Brand Explorer mobile body text |
| 3. No compression artifacts | **Borderline** Brand Explorer mobile |
| 4. No unintended cropping | Pass (hotel object-position set per breakpoint) |
| 5. No stretched images | Pass (`object-fit: cover` + aspect-ratio) |
| 6. No blank loading on switch | Pass (flashLog incomplete=0) |
| 7. No layout shift | Pass (CLS ≈ 0.03) |
| 8. No unreadable mobile content | Pass for chrome + HTML UIs; Brand Explorer body soft |
| 9. No false functionality | Pass (truth audit preserved; HTML UIs labeled “Interface simplified for presentation”) |
| 10. Consistent treatment across six Qs | Pass |

---

## Hotel final asset

- Candidate **#2** installed as `hotel-final.*`.
- **TEMPORARY IMAGE** removed from UI.
- **ILLUSTRATIVE OPPORTUNITY** retained.
- No brand signage; reads as urban hotel facade/entrance.
- Responsive WebP variants + object-position per breakpoint.

---

## Stop condition

Phase C embed inserted; quality report and Preview-parity screenshots returned. **Do not publish.**
