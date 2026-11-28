# DEALALITY_RESPONSIVE_DESIGN_STANDARD_V1

**Status:** ADOPTED (founder GO 2026-11-28) — amended  
**Date:** 2026-11-28  
**Evidence baseline:** `RESPONSIVE_VIEWPORT_AUDIT_V1.md` + audit screenshots + HE supplemental + GDI CX audit  
**Governance:** Desktop-first · tablet-capable · mobile-accessible. Targeted remediation only — not a comprehensive redesign.

---

## 1. Purpose

Define a permanent, product-aware responsive contract so Dealality can fix commercially meaningful layout and interaction defects without redesigning approved desktop templates (especially GDI_CANONICAL_HOTEL_TEMPLATE_V1).

## 2. Product design philosophy

- **Desktop-first** — complete approved functionality at D-* viewports.
- **Tablet-capable** — core commercial workflows usable at T-PORT.
- **Mobile-accessible** — commercially important intelligence, contacts, product navigation, and essential actions at M-PHONE.
- Do **not** force complex desktop analytical functionality into unsuitable mobile layouts.
- Preserve all approved desktop product designs unless a functional defect correction is separately authorized.

## 3. Mandatory validation viewports

| ID | Size | Class |
|---|---|---|
| D-WIDE | 1920 × 1080 | Desktop |
| D-STD | 1440 × 900 | Desktop / laptop |
| D-COMPACT | 1366 × 768 | Business laptop |
| T-PORT | 768 × 1024 | Tablet portrait |
| M-PHONE | 390 × 844 | Phone |

All **five** viewports must be validated for every affected customer-facing route before declaring a responsive remediation batch complete.

Optional diagnostic (not required for every change): 1536 × 864, 1024 × 768.

Do **not** claim PASS for untested routes or viewports.

## 4. Support tiers (product expectations)

### Tier A — Full customer desktop + usable tablet + mobile core

Products: **GDI** (three approved tabs), **ADP owner/share**, **Brand Explorer application** (directory / brand selection / commercial intelligence), **Brand Explorer share**, **Hotel Explorer** (customer-facing tabs), **Opportunity Radar** (authenticated + share as applicable).

| Class | Must |
|---|---|
| Desktop (D-*) | All approved customer functionality accessible. No unintended page-level horizontal overflow. No inaccessible filters. No overlapping critical chrome. No clipped essential intelligence. |
| Tablet (T-PORT) | Core workflows usable: nav, hotel/property selection, primary intelligence, filters (or equivalent), contacts, detail drawers. |
| Mobile (M-PHONE) | Prioritize: product access, hotel selection, commercial intelligence / ES, opportunity browsing, contacts (email/tel/LinkedIn actions), drawers, commercial recommendations, essential CTAs. Complex analytical tables may use **controlled internal** horizontal scroll. Complex Compare / dense analytical workflows may remain desktop-primary. |

### Tier B — Desktop-primary (mobile partial / documented)

Products: **Authenticated `/app` Deal Pulse / dense workspace shell**, some admin/AI analytics tables, Ranked-list analytical density on Radar when map+sidebar concurrent.

| Class | Must |
|---|---|
| Desktop | Full usability |
| Tablet | Core path usable or explicit EXPECTED_DESKTOP_ONLY note |
| Mobile | Do **not** force full desktop parity; document EXPECTED_DESKTOP_ONLY surfaces; if product is customer-facing on phone, provide a reduced safe path (nav + primary cards) without claiming full feature parity |

### Tier C — Desktop-only (explicit)

Documented analytical/admin tools that are not sales-floor phone workflows. Failures at M-PHONE are **EXPECTED_DESKTOP_ONLY**, not unexpected defects — provided desktop remains correct.

## 5. Hard rules (all Tier A customer surfaces)

1. **Page overflow:** Target zero unintended page-level horizontal overflow. Allow a maximum of **2 CSS pixels** solely for browser rounding tolerance. Do **not** accept ~24px mobile overflow as normal. Controlled **internal** horizontal scrolling remains acceptable for genuinely wide tables and analytical components. Do **not** conceal inaccessible content with blanket `overflow-x: hidden` rules.
2. Sticky headers/nav must not permanently intercept primary filter clicks (functional defect across viewports).
3. Native/custom selects must open within viewport or scroll internally; essential filters must be openable, usable, and closable (Radar mobile).
4. Drawers/modals must remain closable (Escape + Close) and content scrollable.
5. **Full information accessibility:** Truncation may be commercially appropriate when the **full value remains accessible** via existing touch, keyboard, or detail interactions. A desktop `title` attribute alone is **insufficient** for mobile. Pay particular attention to hotel / brand / organization / contact names, professional titles, email, telephone, opportunity names, and commercial descriptions.
6. **Do not redesign** approved desktop GDI/ADP/BE/HE/Radar templates to “make mobile pretty.” Mobile fixes are additive stacking/wrapping/containment.

## 6. Evidence status vocabulary

| Status | Meaning |
|---|---|
| PASS | Fully loaded customer content validated at the claimed viewport(s); overflow and essential interactions OK |
| FAIL | Confirmed defect against this standard |
| PARTIAL_RESPONSIVE_SUPPORT | Usable with documented limitations (e.g. truncated label with accessible full value elsewhere) |
| EXPECTED_DESKTOP_ONLY | Intentionally not required on tablet/phone |
| NOT_VALIDATED | Route/viewport/content not tested |
| BLOCKED | Cannot complete validation (auth, content load, environment) |

Only **fully loaded** customer content can support a final responsive PASS. A loading screen, preview header, or partially hydrated interface is insufficient.

## 7. Desktop-only functionality (explicit inventory — initial)

| Surface | Desktop-only elements | Mobile expectation |
|---|---|---|
| Opportunity Radar | Dense multi-filter toolbar + map+sidebar concurrent analysis | Filter drawer / stacked controls; map usable; not full density |
| `/app` Deal Pulse | Multi-column pipeline + activity overlay | Reduced stack; recoverable nav |
| ADP Property Snapshot 5-KPI row | Five-across KPIs | Stack 1–2 columns |
| GDI List View wide tables | Many columns | Internal horizontal scroll OK |
| BE Compare / dense analytics | Complex compare workflows | Desktop-primary; directory + brand intel remain mobile-core |
| BE app sidebar | Persistent 260px sidebar | Collapse / off-canvas with recoverable Menu |

## 8. Regression contract (Playwright)

Extend existing Playwright (do not replace). Per in-scope route after a responsive batch:

- Test all five mandatory viewports.
- Assert page overflow ≤ 2px.
- Assert no critical overlap on primary CTA / enabled filters.
- Assert drawer open/close where applicable.
- Record screenshots under `reports/dealality-responsive-regression/`.
- Authenticated Radar changes require live `/app#/opportunity-radar` session evidence (NOT share/ranked-list alone). Ranked-host remediation may proceed independently; auth remains BLOCKED without Memberstack test session.

## 9. Non-goals

- Full mobile redesign of GDI three tabs  
- New products, KPIs, research engines  
- Changing ADP/GDI methodology, persistence, STAGED/CURRENT pointers, or Airtable schema  
- Blanket overflow hiding  

## 10. Related docs

- Audit: `RESPONSIVE_VIEWPORT_AUDIT_V1.md`  
- Plan: `RESPONSIVE_REMEDIATION_PLAN_V1.md`  
- HE supplemental: `he-supplemental/`  
- Regression: `scripts/playwright-dealality-responsive-regression-v1.mjs`  
