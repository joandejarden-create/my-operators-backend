# Need-Date Match data model review

## Authority (recommended)

| Attribute | Canonical table | Notes |
|-----------|-----------------|-------|
| Need periods | **GDI Hotel Need Periods** | Confirmed vs inferred via Status / Source |
| Change history | GDI Need Period Change History | Versioned |
| Opportunity overlap / match | **GDI Need Period Opportunity Matches** | Match strength + links |
| Hotel commercial selection profile | GDI Hotel Commercial Profiles | Portal selection-first |
| Facility commercial profile | Hotel Commercial Profiles | ADP/facility — not GDI need-date authority |
| Legacy seasonality | Hotel Seasonality & Need Periods | **Legacy** — do not dual-write |

## Application consumption

- Hotel-level status resolved live at auth/share serve (`resolveHotelNeedDateConfiguration`) — **not** inside publication fingerprint.
- Per-opportunity match tiles from match table + opportunity dates.

## Conflicts

Duplication risk between facility seasonality tables and GDI need-period tables. Template must document GDI tables as authority for Need-Date Match.
