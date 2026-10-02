# AI Demand Admin — GDI Reports Row UX V1

## A. Result

| Gate | Result |
|------|--------|
| ROW-BASED UI | **PASS** |
| PDF IN ROW | **PASS** |
| GDI CLIENT IN ROW | **PASS** |
| ADP CLIENT IN ROW | **PASS** |
| ARCHIVE ACCESS | **PASS** |
| SEARCH | **PASS** |
| FILTERS | **PASS** |

## B. Previous Workflow

1. Open GDI Reports tab  
2. Select hotel from dropdown  
3. Use global Generate / View / Download buttons  
4. Scroll to External Client Facing block for ADP + GDI Open/Copy  
5. Glance at informational table (no row actions)  
6. Repeat for next hotel  

This forced hotel selection outside the table and split PDF vs client retrieval.

## C. New Workflow

1. Open GDI Reports tab  
2. Read summary cards (GDI Hotels / Report Ready / PDF Ready / Needs PDF / Blocked)  
3. Optionally filter by hotel/market, report status, PDF status  
4. Operate entirely from the hotel row:
   - View / Download / Generate PDF  
   - GDI Open / Copy URL  
   - ADP Open / Copy URL  
   - Archive (jumps to Report Archive filtered to that hotel + GDI)  
   - Regenerate  

**One hotel = one operating row.**

## D. Columns

| Column | Behavior |
|--------|----------|
| Hotel | Display name + optional market/city second line |
| Ready | Live customer-ready count |
| Action Set | Live action-set count |
| Watch | Live future-watch count |
| Report Status | Chip: READY / WATCH ONLY / NO READY OPPORTUNITIES / NEEDS BUILD / BLOCKED |
| PDF | READY → View + Download; MISSING → Generate; GENERATING / FAILED overlays |
| GDI Client | Open + Copy URL when share exists; else — |
| ADP Client | Open + Copy URL when share exists; else — |
| Last Generated | ISO → `YYYY-MM-DD HH:mm UTC` from PDF store |
| More | Open GDI, Regenerate, Archive |

## E. Hotel State QA (live catalog smoke)

| Hotel | Report | PDF | GDI share | ADP share |
|-------|--------|-----|-----------|-----------|
| Bethesda Marriott | READY (35/16/12) | READY | yes | yes |
| Cambridge Beaches | WATCH_ONLY (0/0/7) | MISSING | no | yes |
| Hilton NYTS | READY (12/12/12) | MISSING | no | no |
| NOW NOW NOHO | WATCH_ONLY (0/0/8) | MISSING | no | yes |
| Renaissance NYTS | READY (11/11/12) | MISSING | no | yes |
| Waterstone Boca | READY (15/15/9) | MISSING | no | yes |

Counts are live — not hardcoded in UI.

## F. Visual Parity

Reuses AI Demand Reviews classes:

- `.adr-counts` / `.adr-count`
- `.adr-filters` / `.adr-btn` / `.adr-btn--tiny`
- `.adr-table-wrap` / `.adr-table`
- `.adr-badge` / `--ok` / `--warn` / `--danger`
- `.adr-subtitle` / `.adr-actions`

Same Admin shell; no separate GDI design system.

## G. Regression

| Area | Risk | Status |
|------|------|--------|
| GDI opportunity / ready counts | Catalog still reads `getGdiPdfReportData` | Unchanged logic |
| PDF generation | Same POST generate + GET pdf routes | Unchanged |
| GDI client share | Open/Copy fetch existing links; `createIfMissing` not used by default | No silent token rotate |
| ADP client share | Same | No auto-create |
| Report Archive | Deep-link via sessionStorage focus; no duplicate archive UI | Added handoff only |

## FINAL VERDICT

**GDI REPORTS ROW UX PASSES — PRIMARY ASSETS AVAILABLE PER HOTEL ROW**
