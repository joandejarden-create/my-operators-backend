# GDI PDF Document Shell Parity V3 — Founder Report

**Date:** 2026-10-02  
**SoT ADP:** `Bethesda Marriott - AI Demand Performance Review - Dealality Oct2026.pdf`  
**SoT GDI (before):** `Bethesda Marriott - Group & Demand Intelligence - Dealality Oct012026.pdf`  
**New GDI:** `GDI_v6_shell_parity.pdf`

---

## Root cause (proven)

GDI inlined `dealality-report-system-v1.css`, which contains:

```css
@page :first { margin: 0; }
```

That **zeros Playwright/CSS margins on page 1**, so the navy cover paints edge-to-edge on top/left/right. The bottom white band remained only because Playwright’s footer chrome still reserved bottom space.

ADP SoT retained inset white margins (14mm / 12mm) because its live render path did not effectively apply that first-page zeroing the same way in the founder binary — but GDI’s setContent bundle **did**.

Measured SoT (2× raster):

| | ADP SoT | GDI SoT (v1) |
|--|---------|--------------|
| top white | 81px | 2px |
| left white | 69px | 0px |
| right white | 68px | 1px |
| full-bleed | NO | YES |

---

## Fix

1. **`adp-monthly-review-report-v1.css`** — restore `@page :first { margin: 14mm 12mm 20mm }` for the executive report family (loads after report-system; overrides dossier full-bleed).
2. **Shared document shell** — `lib/dealality-report-family/dealality-pdf-document-shell-v1.js` (Playwright options + body class `hid-print-pdf-export`).
3. **GDI** uses same host classes: `adp-mr-pdf-host brand-alignment-snapshot` + `adp-mr-report-body`.
4. **Compact disclaimer** (ADP footprint).
5. **Market:** `Washington Metropolitan Area` (not bare DMV).
6. **Body family CSS** aligned to ADP cards/tables/headings (content unchanged).

---

## Geometry acceptance (v6 vs ADP SoT)

| Gate | Result |
|------|--------|
| TOP WHITE MARGIN | YES (81 = 81) |
| LEFT WHITE MARGIN | YES (69 = 69) |
| RIGHT WHITE MARGIN | YES (68 = 68) |
| BOTTOM WHITE BAND | YES (119 = 119) |
| DARK PANEL BOUNDS | YES (69,81,1054×1485) |
| COVER PIXEL-STRUCTURE | **PASS** |
| Overlay mean Δ | 1.38 / changed 1.25% (text antialias only) |

Artifacts: `ADP_PAGE1.png`, `GDI_PAGE1.png`, `OVERLAY_50.png`, `ABS_DIFF.png`, `GEOMETRY_V6.json`

---

## Archive

Prior binaries preserved under `archive/`:

- v1 SoT founder full-bleed  
- v2–v5 prior corrected / production / parity builds  

**New version:** v6 (`GDI_v6_shell_parity.pdf`) — also persisted to `reports/group-demand-intelligence/pdf/recLuxvwwxID7U2B8/report.pdf`

---

## Body family

GDI pages 2+ now use ADP heading rules, pale table headers, top-border callout cards, and shared KPI band. Content layouts remain GDI-specific by design.

**BODY FAMILY PARITY:** PASS (visual system); intentional content-structure differences retained.
