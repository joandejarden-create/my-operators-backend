# Multi-hotel visual verification

Measured with `resolveOpportunityRevenueStreamIcons` after the shared restore (Research store bytes unchanged).

| Hotel | Hotel ID | Rows | Guestroom active | Catering active | Both muted | Notes |
|---|---|---|---|---|---|---|
| Sheraton Guadalajara Expo | `recCAGqkERSXgiku4` | 43 | 43 | 25 | 0 | Matches historical SVG repair baseline |
| YOTEL Geneva Lake | `recrPQcZg7SFARRb2` | 42 | 42 | 16 | 0 | Same class+sales-key restore |
| Bethesda Marriott | `recLuxvwwxID7U2B8` | 86 | 68 | 19 | 11 | Already had frozen icons; unchanged honesty for unconfirmed |
| Wyndham San Diego Bayside | `recnPB3Q02NO8Ma1o` | 82 | 66 | 39 | 7 | Evidence-derived (no class/keys); not class-restore path |
| Waterstone (local slug store) | `gdi_hotel_waterstone_boca_raton` | 29 | 7 | 4 | 19 | Sparse classification; muted remains honest |

## Consistency rules verified

- Same shared card renderer / CSS classes for all hotels
- No hotel-specific icon color overrides introduced
- Research sanitize and PACKAGE attach agree on thin class+sales-key rows
- Unconfirmed / no sales-key rows stay muted (no invention)

## Research / Staged / Published

| Surface | Icon policy |
|---|---|
| Research Preview | Shared restore via `resolveOpportunityRevenueStreamIcons` + sanitize |
| Staged Preview | Frozen package icons or same restore helper |
| Published | Locked package bytes preferred; restore only when icons missing/muted |

Authorization differences remain legitimate; icon color meaning is shared.
