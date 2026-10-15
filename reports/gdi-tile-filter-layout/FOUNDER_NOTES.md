# GDI tile contact + filter layout presentation

**Branch:** `gdi/tile-contact-filter-layout`  
**Scope:** Shared UI only (`dealality-gdi-ui.js` + CSS + HTML cache bust). No Airtable writes, research, or paid calls.

## Changes

1. **Compact tile contact** — one selected block (person/referral with usable personal channel, else organization email/phone/form). Full person + org details remain in the drawer.
2. **Property bar** — primary row `Property | This Week | Opportunity Type | Run Research | Reset View`; history as compact footer inside the same panel.

## Screenshots

| Shot | Path |
| --- | --- |
| Before property bar | `before-property-bar.png` |
| After property bar | `after-property-bar.png` |
| Before tiles (Mayflower) | `before-tiles-mayflower.png` |
| After tiles (Mayflower) | `after-tiles-mayflower.png` |
| After tiles (San Diego) | `after-tiles-san-diego.png` |
| After drawer (Quorum) | `after-drawer-contact.png` |
| Interactive proof HTML | `after-layout-proof.html` |

## Local verify

- Server: `http://127.0.0.1:8080` serving `?v=gdi-tile-filter-layout-v1`
- `node scripts/test-gdi-dealality-ui-unification.mjs`
- `node scripts/test-gdi-card-presentation-regression.mjs`
- `node scripts/test-gdi-card-tile-runtime-smoke.mjs`
