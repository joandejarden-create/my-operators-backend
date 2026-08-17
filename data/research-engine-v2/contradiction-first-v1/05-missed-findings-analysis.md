# Missed findings analysis (honest)

High-confidence rediscovery rate: **87.5%** (7 found + 0 partial / 8)
All material (incl. medium): **70.8%**

## Misses

- **barbados-autograph-to-tribute** (Crystal Cove / Turtle Beach Barbados): Barbados Crystal Cove / Turtle Beach not in amenities-blank census snapshot; no Autograph Barbados rows to reflag
- **tulum-tribute-vs-design-hotels** (Tulum Tribute / Design Hotels): No Design Hotels Tulum / Tribute page conflict check in V1 adapters
- **choice-faranda-extra-hotels** (V Grand Medellin / Faranda Collection Cartagena): Choice adapter checked existing census rows only; no Choice directory-gap pass in V1

## Partials

- **kimpton-aluna-identity**: Aluna Open in census but no IHG directory match — identity/freshness risk flagged as Unverified/Unknown, not auto-corrected

## Why misses occurred

1. **Census snapshot coverage** — amenities-blank CSV lacked Barbados Autograph rows (Crystal Cove / Turtle Beach), so reflag path never ran.
2. **No Choice directory-gap pass** — Choice adapter only verified existing census hotels.
3. **No Design Hotels page conflict probe** for Tulum Tribute cases.
4. **Weak name→URL matching** produced some false reflags (Holiday Inn / InterContinental) — match confidence gate needed.
