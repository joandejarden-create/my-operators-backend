# GDI Reports Admin — Before / After

## Before

```
┌ Summary text line ─────────────────────────────────────┐
│ 6 GDI hotels · 4 report-available · 1 PDF ready        │
└────────────────────────────────────────────────────────┘

┌ Filters ───────────────────────────────────────────────┐
│ Hotel [dropdown ▼]  [Generate PDF] [View] [Download]   │
└────────────────────────────────────────────────────────┘

┌ External Client Facing ────────────────────────────────┐
│ ADP: [Open Client View] [Copy Client URL]              │
│ GDI: [Open Client View] [Copy Client URL]              │
└────────────────────────────────────────────────────────┘

┌ Meta ──────────────────────────────────────────────────┐
│ Select a hotel… / Ready 35, action set 16…             │
└────────────────────────────────────────────────────────┘

┌ Table (informational only) ────────────────────────────┐
│ Hotel | Ready | Action set | Watch | PDF | Status      │
│ Bethesda… | 35 | 16 | 12 | READY | Available           │
│ … no row buttons …                                     │
└────────────────────────────────────────────────────────┘
```

## After

```
┌ Cards (Reviews pattern) ───────────────────────────────┐
│ GDI Hotels  Report Ready  PDF Ready  Needs PDF  Blocked│
│     6            4            1          5         0   │
└────────────────────────────────────────────────────────┘

┌ Filters ───────────────────────────────────────────────┐
│ Search [Hotel or market]  Report Status [All]          │
│ PDF Status [All]  [Apply]                              │
└────────────────────────────────────────────────────────┘

┌ Operating table ───────────────────────────────────────┐
│ Hotel | Ready | Action Set | Watch | Report Status |   │
│ PDF | GDI Client | ADP Client | Last Generated | More  │
│                                                        │
│ Bethesda Marriott                                      │
│ Washington metropolitan area                           │
│ 35 | 16 | 12 | READY                                   │
│ [View PDF] [Download]                                  │
│ [Open] [Copy URL]   [Open] [Copy URL]                  │
│ 2026-10-02 10:59 UTC                                   │
│ [Open GDI] [Regenerate] [Archive]                      │
└────────────────────────────────────────────────────────┘
```

## Removed

- Hotel dropdown as primary control
- Global Generate / View / Download toolbar
- Separate External Client Facing panel
- Selected-hotel meta panel

## Added

- Summary cards from live `counts`
- Search + Report Status + PDF Status filters
- Per-row PDF / GDI client / ADP client / Archive actions
- Catalog fields: `reportStatus`, `pdfStatus`, `lastGeneratedAt`, share availability flags
- Archive tab deep-link (`dealality_report_archive_focus`)
