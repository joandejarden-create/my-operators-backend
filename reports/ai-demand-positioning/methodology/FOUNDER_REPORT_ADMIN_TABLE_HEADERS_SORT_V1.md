# FOUNDER REPORT — AI Admin Table Headers & Sorting Restoration

**Date:** 2026-11-19  
**Workspace:** `C:\Dev\dealality\ws-adp`  
**PR:** stacked on open [#437](https://github.com/joandejarden-create/my-operators-backend/pull/437)  
**STOP before merge.**

## 1. Root cause

AI Demand Reviews **thead had only 7 columns** (Hotel / Published / New Draft / Customer Access / PDF / Actions + checkbox) while coverage-mode **tbody rendered 15 columns** (hotel, location, reporting month, research date, prior run, version, review status, generation, coverage, customer issued, PDF, actions count, generated, row actions).

Browsers still showed the short header row, so labels did not correspond to body cells — appearing as “missing headers.”

Secondary issues:

- Reviews sort clicks updated data order but did not consistently paint active sort classes / indicators.
- Shared sort indicator used CSS triangles; founder asked for SVG chevrons.
- AVA / Action Plan / GDI Research Audit headers existed but lacked `data-sort` + shared paint wiring.
- GDI Full E2E Execution matrices already had headers; contrast strengthened.

## 2. Tabs / tables affected

| Tab | Change |
|---|---|
| AI Demand Reviews | Restored full 15-column header aligned to coverage schema; SVG sort paint |
| ADP Action Plan | `data-sort` on meaningful columns; shared paint + client sort |
| GDI Reports | Already complete; shared SVG paint via existing controller |
| Report Archive | Snapshot marked nonsortable; shared SVG paint |
| AI Visibility Audit | `data-sort` + sort/paint wiring |
| GDI Research Audit | `data-sort` + sort/paint for runs table |
| GDI Full E2E Execution | Clearer sticky header contrast; labeled matrix headers |

## 3. Before / after (Reviews)

**Before:** thead keys `publishedMonthYear`, `newDraftLabel`, `customerAccessLabel`, `pdfLabel` (7 cells).  
**After:** Hotel · Location · Reporting Month · Research Date · Prior Run · Version · Review Status · Generation · Coverage · Customer Issued · PDF · Actions # · Generated · Actions (15 cells).

Screenshots: use live Admin after hard-refresh (`?v=ada-table-headers-sort-v1`). Memberstack may block Cursor browser on live Admin; structural gate covers HTML/JS parity. Fixture evidence for row actions remains under `/app/admin/_fixtures/`.

## 4. Sorting verified (contract)

Shared `DealalityAiAdminTable.paintSortState` + `sortRows`:

- First click ascending, second descending
- Dates / numbers / strings typed correctly
- Nulls sort last
- Active SVG chevron highlighted (`sort-asc` / `sort-desc`)

## 5. Intentionally nonsortable

- Checkbox select columns
- Actions (⋮) columns
- Action Plan long-text columns (Observed Issue, Implementation Steps, Target Sources, Definition of Done, Expected Signal)
- Report Archive Snapshot column
- GDI Full E2E execution matrices (status grids; not primary sort surfaces)

## 6. Responsive

`ada-table-shell` keeps horizontal scroll; sticky headers retained; Reviews `min-width: max-content` for wide column set.

## 7. Regression

```
npm run test:adp-admin-table-headers-sort-v1     # PASS
npm run test:adp-admin-reviews-primary-actions-v1 # PASS (View/Download/Copy preserved)
npm run test:adp-admin-workspace-navigation-v1    # PASS
npm run test:adp-methodology-consistency-v1       # PASS (38/38)
npm run dealality:ci-precheck                     # PASS
```

## 8. PR / workspace

- Branch: `feat/adp/methodology-consistency-admin-actions-v1`
- Hand-off to Integration Controller after push
- Preserved untracked: `.env.local.pr406-parked`, recovery handoff md, `tmp/…`
- No paid research / shares / publication / prod deploy

**AWAITING_INTEGRATION_CONTROLLER**
