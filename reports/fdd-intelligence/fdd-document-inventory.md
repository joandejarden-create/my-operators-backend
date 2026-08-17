# FDD Document Inventory

Generated: 2026-08-11T19:08:06.112Z

## Summary

| Metric | Count |
|---|---|
| Total FDD PDFs (deduped paths) | 89 |
| Total extracted FDD text documents | 32 |
| Unique brands (from PDF filenames) | 54 |
| Unique parent companies | 5 |
| FDD years represented | 2024, 2025, 2026 |
| Brands with multiple historical years | 12 |
| Brands with economics extraction | Kimpton, Curio Collection by Hilton |
| Brands with Item 19 extraction (PDF-side) | 18 |
| PDF rows with source provenance missing/partial | 81 |

### Multi-year brands
- Curio Collection by Hilton: 2024, 2025, 2026
- Park Inn by Radisson: 2025, 2026
- Radisson Individuals: 2025, 2026
- Radisson: 2024, 2026
- Radisson Blu: 2024, 2026
- Everhome Suites: 2024, 2026
- Country Inn & Suites by Radisson: 2025, 2026
- Cambria Hotels: 2025, 2026
- Ascend Hotel Collection: 2025, 2026
- Clarion: 2025, 2026
- AC Hotels: 2025, 2026
- Aloft: 2025, 2026

### Notes
- Choice numbered PDFs live primarily under Google Drive `Choice Hotels International/FDDs`; repo holds extracted text in `fixtures/choice-fdd-text`.
- `uploads/fdd-intelligence` contains duplicate AC Hotels uploads plus Mexico Hilton brand FDDs.
- Kimpton/Indigo/other IHG MN filings live under reference library `IHG Hotels & Resorts/fdd`.
- SHA256 deferred for files >15MB.

Full machine-readable inventory: `fdd-document-inventory.csv`