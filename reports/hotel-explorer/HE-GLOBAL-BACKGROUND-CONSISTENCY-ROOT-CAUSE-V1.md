# Hotel Explorer — Global Background Consistency Root Cause (V1)

**Date:** 2026-11-28  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Host:** authenticated Opportunity Radar popup on `:8080`  
**Prior theme work:** PR #625 (`hotel-explorer-theme-tokens-v1.css`) merged; canvas shell continuous, but nested surfaces still patchy.

## Diagnosis (browser computed styles)

Hierarchy observed:

| Layer | Selector | Computed background |
| --- | --- | --- |
| Radar host page | `body` | `rgb(8, 15, 37)` canvas |
| Explorer overlay | `.hex-root` | `transparent` |
| Shell / scroll / main / tab panels | `.hex-shell`, `.hex-scroll`, `.tab-panel.active` | `rgb(8, 15, 37)` `--he-canvas` |
| KPI / fact cards | `.hex-rms-metric`, `.hex-fact` | `rgb(16, 25, 53)` `--he-section` |
| Table cells | `.hex-rms-table th/td` | `rgb(13, 21, 44)` `--he-table` |
| Nested list/chip rows | `.hdp-context-list__item`, `.hdp-competitive-chip`, `.hdp-overview__row`, `.hiw-snap`, `.hiw-kpi`, … | **`rgba(8, 15, 37, 0.28–0.55)`** |

Tokens on `.hex-root`: `--he-canvas #080f25`, `--he-section #101935`, `--he-elevated #212c4d`, `--he-table #0d152c`. Theme CSS loads last (`?v=2`) after module sheets — load order is correct.

## Root cause

Not missing tokens and not iframes.

1. **Translucent navy fills** (`rgba(8,15,37,α)`) remain on nested panel/list/chip surfaces across HIW, HDP, OI, HCI, and explorer modules.
2. Those fills **composite over section cards** (`#101935`) and produce muddy patches / seams that look like “discoloration.”
3. Theme-token overrides from #625 covered shell chrome and a few cards, but **not the full inventory** of nested surfaces.
4. Secondary: standalone `.ogp-page` still ships a `#0b1228` gradient (org profile page); scoped out of Radar embed but must not leak into HE hosts.
5. Accent status chips / map dots use intentional non-navy colors — **approved**, not seams.

## Tabs affected

All 11 active tabs that render nested list/KPI/chip surfaces (Overview, Rooms, Ownership, Contact, Org/Portfolio, Relationships, Submarket, Area Hotels, Demand, Access, Sources). Guest Sentiment remains disabled.

## Fix direction (minimal, reuse palette)

1. Keep canonical `--he-*` tokens; no new colors.
2. Force nested panel/list/KPI surfaces inside `.hex-root` onto solid `--he-section` / `--he-table` / `--he-elevated`.
3. Replace source `rgba(8,15,37,*)` panel backgrounds with tokens where safe.
4. Keep accent tints / hover / status chips.
5. Add browser gate `HE_GLOBAL_BACKGROUND_CONSISTENCY` (computed + screenshots; fail if skipped).
