# Hotel Explorer Unified Theme V1 — Acceptance

**Date:** 2026-11-27  
**Worktree:** `C:\Dev\dealality\wt-he-unified-theme-v1`  
**Branch:** `feat/he-unified-theme-v1`  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)

## Diagnosis

Competing navy families across HE modules caused seams:

- HIW elevated `#1a2448` / gradient terminus `#0b1228`
- Rooms metric `rgba(10, 18, 42)`
- Independent OI / HCI / HDP surface tokens

Root cause: independently styled embedded modules + load order, not iframes.

## Fix

1. Canonical tokens: `public/css/hotel-explorer-theme-tokens-v1.css` (`--he-canvas/#080f25`, `--he-section/#101935`, `--he-elevated/#212c4d`, `--he-table/#0d152c`, borders/text/accent).
2. Bridge hex / hdp / hiw / oi / hci variables onto that family under `.hex-root`.
3. Continuous canvas on shell / scroll / panels / HIW chrome.
4. Module CSS normalized (HIW + explorer metric surfaces).
5. Theme stylesheet loads **after** module CSS on Radar + founder preview hosts.
6. Scoped to HE hosts only (no Brand Explorer / GDI global rewrite).

## Browser evidence (port 8080)

| Check | Result |
| --- | --- |
| Theme link after `hotel-explorer.css` | YES |
| `--he-canvas` / `--he-section` / `--he-table` computed | `#080f25` / `#101935` / `#0d152c` |
| All 12 nav tabs (11 active + Guest Sentiment shell) surface violations | 0 |
| Scroll top / mid / bottom palette stable | YES (`scrollStable=true`) |
| `.hex-scroll` background | `rgb(8, 15, 37)` |
| Competing navy scan `#1a2448` / `#0b1228` | 0 visible |
| Canonical Radar route `http://localhost:8080/app#/opportunity-radar` | Loads Radar iframe host with theme-linked HE assets |
| Founder preview HE (same Hotel Explorer UI bundle) | Validated end-to-end |

## Regression

```bash
npm run test:he-unified-theme-v1
```

## Founder gates

| Gate | Status |
| --- | --- |
| `HE_UNIFIED_THEME` | **YES** |
| `HE_ELEVEN_TAB_VISUAL_CONSISTENCY` | **YES** |
| `HE_NO_BACKGROUND_SEAMS` | **YES** |
| `HE_SCROLL_VISUAL_STABILITY` | **YES** |
| `HE_RADAR_BROWSER_VALIDATED` | **YES** |

## Preserved

No research / Airtable persistence / ownership / sorting / drawers / contact actions / unit conversion / tab nav / Guest Sentiment feature-gate changes.
