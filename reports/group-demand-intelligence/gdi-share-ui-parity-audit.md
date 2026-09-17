# GDI share UI parity audit — 2026-09-17

## Comparison table

| Concern | Local/Auth GDI | Production Share GDI |
|---|---|---|
| HTML entry | `public/group-demand-intelligence.html` | `public/group-demand-intelligence-share.html` |
| JS entry | `dealality-gdi-ui.js` + `app.js` | `dealality-gdi-ui.js` + `share-app.js` |
| CSS | `group-demand-intelligence.css` | same file |
| Detail body | Auth historically used custom 17-section drawer; share uses `UI.intelligenceDetailHtml` | `UI.intelligenceDetailHtml` (WHAT WE SEE / WHY IT MATTERS / WHY NOW / ACTION / CONTACT / EVIDENCE / VALIDATION) |
| Validation UI | Was **"Hotel Feedback"** + Validation/Action/Outcome lifecycle (`app.js`) | **"Hotel Validation"** form via share-only markup |
| Validation API | `/feedback` + `/api/decisions/...` | `POST .../share/.../validation` → share-validation store + Decision ingest bridge |
| Action / Outcome | Auth full Decision Events | Share: validation-focused (no action/outcome on Rad link) |
| Permissions | Memberstack authenticated | Token capabilities (`CAN_VALIDATE` etc.) |
| Cache bust (before fix) | `?v=gdi-decision-outcome-20260917` | `?v=gdi-property-bar-tight-20260916` (**stale**) |

## Root cause of visual difference

**UI DRIFT ROOT CAUSE: two validation implementations + divergent asset cache versions.**

1. Share and auth did not share one validation form component.  
2. Auth titled the block "Hotel Feedback" with denser Decision lifecycle controls.  
3. Share HTML/JS/CSS query strings lagged auth, so production browsers could keep older share bundles after deploy.

## Fixes implemented

1. Canonical `DealalityGdiUi.shareCustomerValidationFormHtml` — single customer validation form used by share.  
2. Auth section retitled to **Hotel Validation** with shared CSS classes (`gdi-section--validation`, `gdi-validation-panel`).  
3. Permission context: `canValidate` / `CAN_VALIDATE` (legacy `opportunity_detail` tokens still validate).  
4. Aligned cache bust to `?v=gdi-share-parity-20260917` on both HTML entrypoints.  
5. Customer-safe inactive link component `inactiveShareLinkHtml`.

## Intentional remaining differences

| Difference | Why allowed |
|---|---|
| Auth Action + Outcome steps | Requires authenticated Decision write surfaces; share Rad token is validation-capable, not full admin |
| Memberstack chrome / hotel selector | Share is single-hotel scoped by token |
| Research / audit tabs | Internal only |

## SAME COMPONENT?

Detail sections: **YES** (`intelligenceDetailHtml`)  
Hotel Validation form: **YES** for share path (shared UI helper); auth retains fuller lifecycle under same panel naming/classes (**PARTIAL unification** — presentation aligned, permission modes differ)

## Permission note on Bethesda token

`mode: read_only` historically still allowed validation writes. Now explicit: surfaces including `opportunity_detail` → `CAN_VALIDATE`. Do not overload `read_only` as “no writes.”
