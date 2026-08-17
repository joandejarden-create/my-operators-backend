# Invalid Market Value Policy

**Status:** Design only — do not apply clears without Joan authorization + downstream confirmation.

## Classes in scope

- `COUNTRY_AS_MARKET` — Country string copied into Market (except explicit single-market allowlist)
- `STATE_AS_MARKET` — State/Region copied into Market without registry rule
- `CITY_AS_MARKET` — City label used as Market when not a registered canonical Market / alias

## Preferred principle

**KNOWN WRONG must not remain as if valid.**

If Market is provably semantically invalid **and** no deterministic Dealality Market replacement exists:

→ correct to **BLANK** with separate governance status `UNRESOLVED`

Prefer blank over Country/State/City contamination.

## Allowed clear conditions (`SAFE_MARKET_INVALID_CLEAR`)

1. Current value classified COUNTRY/STATE/CITY_AS_MARKET or INVALID (frozen baseline)
2. `resolveDealalityMarketStrict` returns no replacement
3. Downstream Airtable formulas/views tolerate blank Market (see `86-downstream-market-impact.md`)
4. Explicit authorization for clear class

## Not allowed

- Clear a Market that is CONFIRMED_VALID
- Clear when a deterministic SAFE_MARKET_CORRECTION exists
- Write Country/State/City back into Market
- Use STR / Cvent / legacy Market as replacement

## Migration design (no schema change this task)

Short-term: blank Market + research claim store `market_resolution_status=UNRESOLVED`  
Long-term: dedicated status fields (see `87-geography-resolution-status-design.md`)
