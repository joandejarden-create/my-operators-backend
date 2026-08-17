# Property Identity V1

Implemented in `lib/research-engine-v2/clean-census/property-identity.js`.

## Model

```
property_id
canonical_property_name
address
coordinates
city
country
official_property_identifiers
official_urls
phones
current_affiliation
affiliation_history
known_aliases
identity_confidence
evidence
```

## Rules

- Brand is **not** the durable primary key.
- Fuzzy name alone **never** merges properties.
- Exact/High requires official ID/URL and/or strong coords/address.

## Wave 1C result

- Independent records: 68
- Unique physical properties (intra-Choice): **68**
- Intra-cohort duplicate links: 0
