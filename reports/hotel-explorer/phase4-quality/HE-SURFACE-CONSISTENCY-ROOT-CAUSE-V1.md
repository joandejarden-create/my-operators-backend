# Hotel Explorer — Surface Consistency Root Cause (v1)

**Date:** 2026-11-19  
**Scope:** Shared HE styling architecture (Radar + Founder Preview)  
**PR:** #463

## Root-cause inventory

| Cause | Evidence | Effect |
| --- | --- | --- |
| Third navy for sections | `--panel-soft: #0c132c` on `.hex-root .hiw-section` | Section cards read as a different patch vs canvas `#080f25` |
| My Deals table surface nested without token | Hardcoded `#0d152c` on tables/operator groups | Valid table surface, but sat on `#0c132c` → muddy nested contrast |
| Translucent navy overlays | `rgba(13,21,44,0.85)`, `rgba(8,15,37,0.45/0.55)` on scope, HCI, OI, footprint | Alpha over mismatched parents creates unexplained shade shifts |
| Duplicate surface systems | `--panel` / `--panel-soft` / `--hdp-bg*` / `--runbook-surface` / `--hiw-*` | Same hierarchy level used different hex values |
| Legacy scroller/dossier colors | `#0b1224` dossier layer | Off-canvas navy unrelated to platform token |
| Footprint overrides | `.hex-radar-host` used `#101935` headers + translucent wrap | Tables diverged from ada-table / My Deals |

## Canonical hierarchy (binding)

| Level | Token | Value | Use |
| --- | --- | --- | --- |
| 0 Canvas | `--hex-surface-canvas` | `#080f25` | Shell, scroll, tab rail |
| 1 Section / card | `--hex-surface-section` | `#101935` | `hiw-section`, hex-card, hero/panel, HCI/OI cards |
| 2 Table | `--hex-surface-table` | `#0d152c` | ada/dl tables, operator groups, meas prefs |
| Border | `--hex-surface-border` | `#37446b` | Section borders |
| Table border | `--hex-surface-border-table` | `#2a3550` | My Deals / runbook table chrome |

`--panel-soft` is now an **alias of section** (no `#0c132c`).

## Non-goals

- Do not restyle My Deals / Brand Explorer globally.
- Do not invent additional navy steps between section and table.
- Expanded operator body stays transparent (inherits table group surface).
