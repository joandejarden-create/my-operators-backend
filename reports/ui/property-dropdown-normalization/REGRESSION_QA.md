# Property dropdown — regression QA

| Hotel | optionLabel | Pass |
|---|---|---|
| Bethesda | Bethesda Marriott — Bethesda, Maryland | YES |
| YOTEL | YOTEL Geneva Lake — Founex, Switzerland | YES |
| AC | AC Hotel A Coruña — A Coruña, Galicia | YES |
| Spice | Spice Island Beach Resort — St George's, Grenada | YES |
| Cambridge | Cambridge Beaches Resort & Spa — Sandys Parish, Bermuda | YES |
| NOHO | NOW NOW NOHO — New York, New York | YES |
| WRome | W Rome — Rome, Italy | YES |

All focus hotels have City + Region/Country second segment: **YES**

API: `GET /api/group-demand-intelligence/hotels` returns `listGdiSelectableHotels()` rows with `optionLabel`.
UI: `dealality-gdi-ui.js` uses `h.optionLabel` for `#gdiHotel` options.
