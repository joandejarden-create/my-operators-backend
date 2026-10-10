# GDI External Client Experience Restore V1

## Phase 1 — Introducing commits

| Role | Commit | Summary |
|------|--------|---------|
| **INTRODUCING (PDF buttons)** | `2d6e79f` | Add Admin external ADP/GDI client URLs and share PDF access — added View PDF / Download PDF to share property bar |
| **INTRODUCING (Demand Report)** | `fd919a1` | Add GDI client Demand Report view on existing share URLs — added Demand Report tab + report renderer + default tab=`report` |
| **LAST_GOOD_EXTERNAL_GDI** | `bed31c9` (`2d6e79f^`) | Last commit before external PDF buttons; share UX = opportunities-only |

### FILES_CHANGED (introducing)

**2d6e79f (PDF):** `share-app.js`, `group-demand-intelligence-share.html` (cache), plus Admin link APIs (kept).

**fd919a1 (Demand Report):** `share-app.js`, `dealality-gdi-ui.js` (`GDI_REPORT_TAB`), `group-demand-intelligence.css` (`.gdi-report-client`), HTML cache bust.

## Phase 2 — Original UX inventory (`bed31c9`)

- **Header / shell:** `hotelShellHtml({ mode: "share", badges: { readOnly: true } })` — read-only badge present
- **Tabs:** single tab `GDI_MAIN_TAB` — label `Group &\nDemand Intelligence` (id `opportunities`)
- **Top buttons:** only **Reset View** (`#gdiResetViewBtn`)
- **No:** Demand Report tab, View PDF, Download PDF
- **Property bar:** hotel name/location, weekly filter, last research, run status
- **Browse chrome:** priority / booking facets, weekly delta, sort, tiles/list, export, opportunity count
- **Content:** opportunity cards grid; empty/filter empty states
- **Detail:** drawer `#gdiShareDrawer` opportunity detail (read-only share mode)
- **Disclaimer:** “Share brief for review only…”
- **Default tab:** `opportunities`

## Phases 3–7 — Restore applied

Restored external surface only:

- `public/js/group-demand-intelligence/share-app.js` ← exact `bed31c9`
- Removed `GDI_REPORT_TAB` from `dealality-gdi-ui.js`
- Removed `.gdi-report-client*` CSS
- Cache-bust `gdi-external-restore-v1` on share HTML
- Removed share `pdfAvailable` from resolve payload
- Removed external routes:
  - `GET .../share/hotels/:hotelId/pdf-report`
  - `GET .../share/hotels/:hotelId/report-pdf`
- Bethesda token `gdisht_47c25d74c79216021fb36150` **unchanged**
- Surfaces remain: `brief`, `opportunities`, `opportunity_detail`, `summary` (no `report`/`pdf`/`archive`)
- Admin GDI PDF / Report Archive routes **preserved**

## Acceptance matrix

See `ACCEPTANCE.json` — **FINAL_VERDICT: PASS**.

| Check | Result |
|-------|--------|
| INTRODUCING | `2d6e79f` (PDF) → `fd919a1` (Demand Report) |
| LAST_GOOD | `bed31c9` |
| Demand Report / View PDF / Download PDF | REMOVED on external |
| External share PDF routes | 404 |
| Admin GDI PDF generate/view | 401 unauth (routes present) |
| Bethesda token `gdisht_47c25d74…` | UNCHANGED (`sha12=9237540e872c`) |
| Production deploy | `6e4b657f` SUCCESS |
| Screenshots | `RESTORED_PRODUCTION.png` byte-size matches `LAST_GOOD_REFERENCE.png` (902232) |

### Other shares browser smoke

Do not create missing shares. Hilton NYTS: no active share. Renaissance / Waterstone / Cambridge Beaches / NOW NOW NOHO: restored layout (no Demand Report / PDF).
