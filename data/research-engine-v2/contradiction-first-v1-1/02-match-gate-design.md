# Match Gate Design (V1.1)

## Levels

Exact | High | Medium | Low | Reject

## Material proposals require Exact or High

Medium → Review only. Low/Reject → research history only (no proposed queue).

## Signals

normalized name (distinctive tokens), city, country, property ID / MARSHA / mnemonic, official URL, property-level brand.

## Geography

Hard country align. City align or **explicit** `GEO_ALIAS_MAP_V1_1` only. Cancun ⊄ Riviera Maya auto-match.

## Brand contamination

IHG directory candidates filtered to Dealality property brand (Indigo≠InterContinental). Parent domain ≠ brand proof.

## Corroboration

Pipeline→Open: Exact/High + official bookable; dual page signals (Book Now + New Hotel) → High; single primary → Medium proposed; weak match → Review.

Reflag: property-level brand label + Exact/High entity match; else Review / Insufficient Evidence.
