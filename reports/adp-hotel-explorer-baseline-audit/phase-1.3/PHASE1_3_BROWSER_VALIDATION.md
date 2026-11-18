# Phase 1.3 — Browser validation

**Date:** 2026-11-18  
**Server:** `PORT=8091` (GDI `8080` untouched, pid preserved)  
**Commit base:** `dfc93df33`

## Screenshots

| File | What it shows |
|------|----------------|
| `screenshots/01-standalone-bethesda-merged-he.png` | Standalone ranked-list: Bethesda merged + Hotel Explorer |
| `screenshots/02-app-shell-signin-gate.png` | `/app#/opportunity-radar` shell requires Memberstack login in browser automation |
| `screenshots/03-app-embed-bethesda-he.png` | Canonical embed URL (`embed=1&appShell=1&inventory=staged&preview=staged`) with Bethesda HE |

## Matrix

| Scenario | Result | Notes |
|----------|--------|-------|
| **A. Canonical application route mapping** | **PASS** | `#/opportunity-radar` → ranked-list host (static gate + architecture) |
| **A. App shell iframe in automated browser** | **BLOCKED_AUTH** | Shell shows “Sign in to continue” without Memberstack JWT in automation |
| **A. Canonical embed host (exact iframe URL)** | **PASS** | Pins load; HE primary; five tabs clickable |
| **B. Baseline hotel** | **PASS** | Agualina `recfVWIaXlGCfyI5k` — five Radar tabs |
| **B. Enriched hotel** | **PASS** | Casas `recHBTqnDeXJOc7FZ` present in pool |
| **B. No invented unverified attrs** | **PASS** | Progressive enrichment unit gate + prior Phase 1.2 browser |
| **C. Bethesda internal pin merge** | **PASS** | `added:[recLux…]`, identity Bethesda Marriott, `internalPreviewOnly:true` |
| **C. Bethesda HE open** | **PASS** | Screenshots 01/03 |
| **C. Public brand-presence exclusion** | **PASS** | Public list 15692 without Bethesda |
| **C. No Bethesda share pack** | **PASS** | Share HTML gate |
| **D. Standalone ranked-list regression** | **PASS** | Continues to work with HE |
| **D. Filters / other hotels** | **PASS** | Pool size +1 only for allowlisted id; Casas retained |
| **D. GDI 8080 untouched** | **PASS** | Listener remained on 8080 throughout |

## Canonical embed URL used for application integration proof

```
http://127.0.0.1:8091/deal-capture-radar-with-ranked-list.html?embed=1&appShell=1&inventory=staged&preview=staged
```

This is the URL `public/app.js` `routeToEmbedUrl('/opportunity-radar')` produces for owner/admin after forwarding staged inventory/preview.

## Founder note for full shell click-path

Automated browser cannot complete Memberstack login. After founder logs into `http://localhost:8091/app?...#/opportunity-radar`, the iframe loads the same embed host already validated above.
