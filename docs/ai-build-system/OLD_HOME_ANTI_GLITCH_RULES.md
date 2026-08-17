# Old Home anti-glitch (FOUC) rules

> **Last updated:** 2026-08-01  
> **Scope:** `/old-home` marketing page (Webflow + CDN scripts in `public/marketing/`).

## Why glitches keep appearing

Almost every “flash / settle” bug is the same race:

1. First paint (or FOUC reveal) shows state **A**.
2. A later script or stylesheet rewrites the same chrome to state **B**.
3. The user sees A → B.

Examples we already hit: Explore CTA pill → small, globe soft texture, quotes header stuck at opacity 0, **Benefits missing then appearing in the top nav**.

## Durable rule (non-negotiable)

**Whatever the user sees at `html.oh-ready` must already be the final visual contract.**

Secondary scripts may:

- no-op when a gate stamp says the final state is already applied, or
- only enhance behavior that does **not** change layout/copy/order (e.g. click handlers).

Secondary scripts must **not**:

- rebuild nav / CTAs / headlines to a different order or size after FOUC reveal,
- inject competing CSS that overrides FOUC/freeform final sizes,
- mark incomplete work as “done” with a stamp that blocks the real SoT script.

## Single sources of truth

| Chrome | Final SoT | Gate that must bake it before reveal |
|--------|-----------|--------------------------------------|
| Desktop/mobile nav labels + hrefs | `dealality-old-home-nav-cleanup.v20260801a.js` `ORDER` | `old-home-fouc-gate` `NAV_ORDER` + `__ohNavCleanup >= 202608011` |
| Explore CTA size/shape | Connected Process primary (10px / 40px / `#6C72FF`) | FOUC `#oh-fouc-gate-style` + freeform-head Explore rules |
| Hero rotator width | measured `--hr-w` / max-content (never live `.on` measure flash) | FOUC wait for `oh-rotator-ready` + `document.fonts` (`01f`+) |
| Hero Explore CTA | Connected Process primary (40px / 10px / `#6C72FF`) | FOUC + freeform — **never** site-head `#hero #fsw-btn { height:2.55rem }` (that forces tall pill → shrink flash) |
| Hero eyebrow | FAQ pill + FOUC/hero-fit margin (`.4rem` base / `.85–.95rem` xl) | FOUC bake + freeform margin match — avoid freeform `2.25rem` |
| Wide/tall hero CTAs | `#hero-inner` flex + `#form-subscribe-wrap { margin-top:auto }` + pb for cue | FOUC `01j` + hero-fit `01b` + freeform-head `01e` — buttons near fold; stack gaps unchanged |
| Hero rotator line-height | CSS `--hr-lh` (FOUC/hero-fit/rotator style) | Rotator `01c+` must **not** write `--hr-lh` from probe (`Math.max(...,40)` crushed tall heroes 70→40) |
| Section max-width rails | **1120** content · **1320** learn/CTA/footer | `dealality-old-home-width-bands.v20260801a.css` — no 1040/1070/1100/1180/1232/1360 |
| Section reveal (quotes header) | motion prod section roots (`#trust` for quotes) | motion must observe the live section id |
| Section reveal (How / Platform / Ecosystem) | live ids: `#oh-how-we-do-it` (dealality-process_*), `#many-futures`, `#ecosystem` | `old-home-motion.prod` `01c+` — never stale `#platform-features` / `.oh-how-head` only |
| Section enter contract | header stack + **one** body group | `01f+`: Platform = `#dealality-many-futures` after `mf-js-ready` (not hotel-only); Ecosystem = `.oh-eco-stage`; FAQ = per-`details`; CTA includes `#cta-band-note`; Manual Process header-only (diagram owns path-draw). Sticky rail gated until `#many-futures.oh-m-section-in` |
| Globe texture | self-hosted earthmap **1k** (not 512) | globe JS `TEX` + boot preload |

When you change a SoT, **update the FOUC/boot bake in the same change**. Do not ship a “cleanup” script that corrects FOUC later.

## Checklist before publishing any old-home chrome change

1. Does FOUC (or critical CSS) already paint the **final** size/order/copy?
2. If a late script still runs, will it **no-op** via a high enough `__oh*` stamp?
3. Hard-refresh QA: nav labels (incl. Benefits), Explore vs Demo sizes, hero headline, quotes header on first scroll.
4. Prefer one script owning one chrome. Do not add a third rewriter.

## Nav contract (current)

Order: **How It Works → Benefits → Platform → FAQs → Insights**  
Hrefs: `#oh-how-we-do-it` · `#modules` · `#many-futures` · `#faq` · `#insights`

FOUC gate `v20260801e+` must include Benefits. Older FOUC builds that omit Benefits cause the exact reload flash reported 2026-08-01.
