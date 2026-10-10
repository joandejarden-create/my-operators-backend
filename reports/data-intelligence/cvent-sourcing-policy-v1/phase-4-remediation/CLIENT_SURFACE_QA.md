# Client Surface QA — Cvent Phase 4

**Recheck date:** 2026-10-03  
**Active Cvent-only hotel facts on customer surfaces:** **0**

## Surfaces checked

| Surface | Exposure | Disposition | Notes |
|---------|----------|-------------|-------|
| HPC Rooms/Keys mx092 | Census Only / Not Owner-Facing | DISPLAY_SAFE | Provenance = AM.com.mx (independent); not Cvent |
| HPC Rooms/Keys mx226 | Census Only / Not Owner-Facing | HIDE_PENDING_VERIFICATION | Value retained; Low; not owner-facing; not verified display |
| ADP Waterstone published evidence | `cvent.com/venues/results/…` | DISPLAY_SAFE | Market/event discovery citation — not hotel Rooms/meeting SoT |
| ADP Waterstone report | Cvent mention | DISPLAY_SAFE | No venue/hotel SoT claim |
| ADP NOW NoHo evidence | Cvent mention | DISPLAY_SAFE | No venue/hotel SoT claim |
| HI / GDI report JSON under `reports/` | Offline | N/A (not customer UI) | Blocked from verified scoring |

## Confirmations

- [x] No Cvent venue/hotel page remains the active Rooms Source URL for a customer-facing verified fact (mx092 upgraded; mx226 not owner-facing + Low).
- [x] No Cvent-only hotel fact displayed as authoritative ADP attribute (Phase 2 guard + no ADP writes).
- [x] Corrected/verified fact (mx092=110) uses independent source URL on census.
- [x] Historical Cvent URLs preserved where superseded (steward notes / prior apply reports).
