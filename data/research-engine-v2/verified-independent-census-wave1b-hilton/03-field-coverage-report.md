# Hilton Mexico — Independent Field Coverage

## Summary

| Metric | Value |
|--------|-------|
| Hotels discovered | 102 |
| Core field support | **100%** (816/816) |
| Material field support | **71%** (1948/2754) |
| Target material | ≥65% (unknown preferred over unsupported) |

## Core fields (name, Affiliation, Parent Company, status, country, city, Website, Property ID)

Per-hotel core completeness average: 100%

## Material field hits (hotels with independently supported value)

- **name**: 102/102 (100%)
- **Affiliation**: 102/102 (100%)
- **Parent Company**: 102/102 (100%)
- **country**: 102/102 (100%)
- **city**: 102/102 (100%)
- **Website**: 102/102 (100%)
- **Property ID**: 102/102 (100%)
- **Brand Property Code**: 102/102 (100%)
- **status**: 102/102 (100%)
- **Latitude**: 102/102 (100%)
- **Longitude**: 102/102 (100%)
- **Address 1**: 102/102 (100%)
- **Postal Code**: 102/102 (100%)
- **Amenities**: 102/102 (100%)
- **Market**: 102/102 (100%)
- **Open Date**: 101/102 (99%)
- **Telephone**: 101/102 (99%)
- **State**: 100/102 (98%)
- **Restaurant (Y/N)**: 80/102 (78%)
- **Conference (Y/N)**: 61/102 (60%)
- **Spa (Y/N)**: 20/102 (20%)
- **Resort (Y/N)**: 14/102 (14%)
- **Location**: 14/102 (14%)
- **Boutique (Y/N)**: 13/102 (13%)
- **Golf (Y/N)**: 1/102 (1%)

## Hilton structured-data lift

| Field | Hotels |
|-------|--------|
| Latitude | 102 |
| Longitude | 102 |
| Amenities (from amenityIds) | 102 |
| Open Date | 101 |
| Restaurant (Y/N) | 80 |
| Spa (Y/N) | 20 |
| Conference (Y/N) | 61 |
| Rooms | 0 |

Rooms remain largely **Unknown** — Hilton Mexico locations JSON does not expose room count; GraphQL status query also omits rooms. Unknown is correct.

## Proprietary firewall

STR Market / STR Submarket / proprietary Chain Scale: **not migrated**. Dealality Market = Mexico (country grain).
