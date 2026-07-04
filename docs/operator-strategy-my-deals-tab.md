# Operator Strategy — My Deals tab

**Status:** Native table / work-queue layout (2026-05-25).  
**Route:** My Deals → **Operator Strategy** (`?tab=operator-strategy`)

---

## Layout (native My Deals)

```
[Search]  [Alignment Signal ▼]  [Reset View]

Filtered to: Deal Name  [Clear]   ← only when deep-linked

Showing N operator-company rows    [Bulk Actions ▼]   ← count left; bulk control right (above CTA column)

[table — checkbox column + column headers with sort arrows (except Call to Action)]
```

No in-panel title, explanatory subcopy, or section heading above the table; no Project / Deal dropdown or refresh button. Tab name remains **Operator Strategy** in the left nav.

**Column sort:** Click a header to sort ascending; click again for descending. **Score** sorts numerically (empty scores last). **Call to Action** has no sort control.

**Selection:** Header checkbox selects all visible rows; each row has a checkbox to the left of Project / Deal. **Bulk Actions** (top right) opens a menu; actions are disabled until operator review / outreach workflows ship (`Add to Operator Review`, `Prepare Outreach`).

---

## Removed chrome (Phase C simplification)

| Removed | Notes |
|---------|--------|
| In-panel **Operator Strategy** title (`h2`) | Tab label in nav only; panel uses `aria-label` |
| Header subcopy paragraph | — |
| **Operating Companies for Consideration** heading | Row count is muted text above table |
| **Project / Deal** dropdown filter | Filter via table column + deep-link chip only |
| **Refresh Operator Strategy** button | Data reloads when tab is opened (`loadPipeline(true)` on activate) |

Still removed from earlier phases: deal selector, Switch Deal, Deal Actions, Operating Pathways, summary cards.

---

## Remaining controls

| Control | Behavior |
|---------|----------|
| **Search** | `Search company or deal…` — company, deal, location, status, consideration |
| **Alignment Signal** | All / Strong / Moderate / Conditional / Limited / Insufficient |
| **Reset View** | Clears search, alignment filter, and deep-link deal filter |
| **Deep-link chip** | `?tab=operator-strategy&dealId=rec…` → **Filtered to: [name]** + **Clear filter** (no dropdown) |

---

## Table columns

Project / Deal · Operating Company · **Project Location** · Score · Review Status · Key Consideration · Data Confidence · Call to Action

Alignment band is available via the **Alignment Signal** filter (not a table column).

| CTA | Status |
|-----|--------|
| View Operator Alignment Snapshot | Active (icon) |
| View Operator Capability Snapshot | Active (icon) |
| Open Operator Profile | Active when `rec…` operator id (icon) |
| **More (⋯)** | Opens menu with disabled **Add to Operator Review** and **Prepare Outreach** (SVG + labels) |

---

## Data loading

- On tab activate: **full reload** (no visible refresh control).
- Up to 40 deals from My Deals list; `GET /api/operator-alignment-snapshot/:dealId/companies` per deal.
- `onDealsLoaded` invalidates cache when deal list changes.

---

## Loading / empty

| State | Copy |
|-------|------|
| Loading | Loading operator strategy… |
| Empty | Operator strategy rows will appear once operator alignment signals are available for active deals. |
| Partial API failure | Some operator strategy rows could not be loaded. |

---

## How to test locally

1. Open project folder in Cursor; run dev server (`npm start` or your usual command).
2. Open **My Deals** → **Operator Strategy**.
3. Confirm: title + 3 filter controls + table only (no subcopy, no section H3, no deal dropdown, no refresh).
4. Deep link: `/my-deals.html?tab=operator-strategy&dealId=recYOUR_DEAL_ID` — chip appears, no dropdown.
5. Run: `node scripts/validate-operator-strategy-my-deals-tab.mjs`

---

## Out of scope (unchanged)

- Airtable schema · scoring weights · BAS · OCS · OAS PDF · Review Set table · production new-base writer flag

---

## Files

- `public/my-deals.html` — `#sectionOperatorStrategy`
- `public/js/operator-strategy-my-deals.js`
- `public/css/operator-strategy-my-deals.css`
- `scripts/validate-operator-strategy-my-deals-tab.mjs`
