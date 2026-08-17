# Temporal Affiliation Model — Minimal Design

## Recommendation

**Yes — introduce a durable `property_identity` separate from current brand affiliation.**

Wave 1B cross-family scan: 0 historical / 0 probable-review pairs between IHG and Hilton Mexico independent universes.

## Minimal schema (do not build full history DB yet)

```
property_identity   // stable Dealality id for the physical hotel
affiliation         // brand string at a point in time
parent_company      // IHG | Hilton | …
valid_from          // date | null
valid_to            // date | null (null = current)
current_affiliation // boolean
evidence            // claim refs / URLs / discovery source
```

## Rules

1. Never overwrite prior affiliation when a reflag is detected — close prior row (`valid_to`) and open new.
2. Dual-branded campuses: two current affiliations allowed with shared `property_identity` only when coords/address prove same campus AND official dual-brand evidence exists.
3. Fuzzy name alone is **never** sufficient to merge identities.
4. Legacy comparison may *hint* historical affiliation; it never creates the independent claim.

## Implementation scope for now

Design + cross-family detection artifacts only. No Airtable schema migration in Wave 1B.
