# Wave 15 Stage 6 — Post-Image Cleanup Failures

Generated: 2026-08-04T20:38:48.748Z

## Summary

- Total rows: **8**
- Actionable: **8**
- Blank typical_keys: **4**
- Stale Portfolio≠Project Fit: **4**
- Primary theme: **snapshot.typical_keys Portfolio sync from Project Fit**

## Notes

- Stage 5 left overview.scenario images cleared; uniqueness/role-match/no-empty/golden PASS.
- Primary Stage 6 blocker: snapshot.typical_keys derived from Portfolio min/max rooms (not Presentation).
- No Presentation caption/momentum/openings residual fails in Stage 5 post-validate beyond typical_keys.
- Owner-facing unavailable phrasing cannot pass rendered-completeness — numeric Portfolio fields required.

## Failure table

| Brand | Slug | Record ID | Section | Field | Failure Type | Current Value | Proposed Fix | Source Support | Steward? |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Hilton Hotels & Resorts | `hilton-hotels-and-resorts` | `recWubG3rhiS1BaWi` | Brand Snapshot | snapshot.typical_keys | typical_keys_stale_portfolio_mismatch | 200–1000 rooms | Reconcile Portfolio to Project Fit → "120–450 rooms" (replace stale 200–1000 rooms) | Brand Setup - Project Fit Min/Max Room Count (segment/brand steward apply; Choice-style Portfolio sync) | no |
| Homewood Suites by Hilton | `homewood-suites-by-hilton` | `recZjYI4nYflGHFNR` | Brand Snapshot | snapshot.typical_keys | typical_keys_stale_portfolio_mismatch | 200–1000 rooms | Reconcile Portfolio to Project Fit → "80–160 rooms" (replace stale 200–1000 rooms) | Brand Setup - Project Fit Min/Max Room Count (segment/brand steward apply; Choice-style Portfolio sync) | no |
| Home2 Suites by Hilton | `home2-suites-by-hilton` | `reccZ4zV6wMav7a2i` | Brand Snapshot | snapshot.typical_keys | typical_keys_stale_portfolio_mismatch | 200–1000 rooms | Reconcile Portfolio to Project Fit → "90–160 rooms" (replace stale 200–1000 rooms) | Brand Setup - Project Fit Min/Max Room Count (segment/brand steward apply; Choice-style Portfolio sync) | no |
| Tru by Hilton | `tru-by-hilton` | `recJLiMTv4W8VgO9L` | Brand Snapshot | snapshot.typical_keys | typical_keys_stale_portfolio_mismatch | 85–120 rooms | Reconcile Portfolio to Project Fit → "40–120 rooms" (replace stale 85–120 rooms) | Brand Setup - Project Fit Min/Max Room Count (segment/brand steward apply; Choice-style Portfolio sync) | no |
| DoubleTree by Hilton | `doubletree-by-hilton` | `rechVYWQ5ikRnr99B` | Brand Snapshot | snapshot.typical_keys | typical_keys_blank_cleanly_unavailable | (blank) | Copy Project Fit Min/Max Room Count → Portfolio Minimum Property Size (Rooms) / Maximum Property Size (Rooms) → render "120–450 rooms" | Brand Setup - Project Fit Min/Max Room Count (segment/brand steward apply; Choice-style Portfolio sync) | no |
| Hampton by Hilton | `hampton-by-hilton` | `rectRvOWQPaL6FkzZ` | Brand Snapshot | snapshot.typical_keys | typical_keys_blank_cleanly_unavailable | (blank) | Copy Project Fit Min/Max Room Count → Portfolio Minimum Property Size (Rooms) / Maximum Property Size (Rooms) → render "70–200 rooms" | Brand Setup - Project Fit Min/Max Room Count (segment/brand steward apply; Choice-style Portfolio sync) | no |
| Hilton Garden Inn | `hilton-garden-inn` | `recrvdAjRlXxPvPPF` | Brand Snapshot | snapshot.typical_keys | typical_keys_blank_cleanly_unavailable | (blank) | Copy Project Fit Min/Max Room Count → Portfolio Minimum Property Size (Rooms) / Maximum Property Size (Rooms) → render "90–250 rooms" | Brand Setup - Project Fit Min/Max Room Count (segment/brand steward apply; Choice-style Portfolio sync) | no |
| Spark by Hilton | `spark-by-hilton` | `recfv66er4Ch2vJDO` | Brand Snapshot | snapshot.typical_keys | typical_keys_blank_cleanly_unavailable | (blank) | Copy Project Fit Min/Max Room Count → Portfolio Minimum Property Size (Rooms) / Maximum Property Size (Rooms) → render "60–120 rooms" | Brand Setup - Project Fit Min/Max Room Count (segment/brand steward apply; Choice-style Portfolio sync) | no |
