# QUALIFY badge root cause

## Root cause
`bookingWindowStatus: QUALIFY_NOW` maps to customer pill label **QUALIFY** in `dealality-gdi-ui.js` (`ACTION_STATUS_DISPLAY` / `ACTION_PILL_DISPLAY`).

Historically QUALIFY_NOW meant foundational research ("qualify buyer/housing") — **incompatible** with customer-READY cards.

## Fix applied
- Surviving READY records persist `bookingWindowStatus: CONTACT_NOW` (customer label **PURSUE NOW**)
- Demoted records use `WATCH`
- UI maps `QUALIFY_NOW` → **CONTACT** when legacy rows remain (shared component; not YOTEL-only)

## Counts
QUALIFY on frozen cohort before: **0**
QUALIFY+Ready contradictions after: **0** (must be 0)
