# Wave 13 Public Six — Geo + Momentum Failures

Generated: 2026-07-27T20:59:31.351Z

Scope: six Active Wave 13 public brands only. SO/ excluded.

## Summary

- Failure rows: 36
- Brands: 6

| Brand | Section | Record ID | Field | Failure Type | Current Value | Required Pattern | Proposed Fix | Source Support |
|---|---|---|---|---|---|---|---|---|
| mama-shelter | geographic_footprint | recXCZCK05XXYX7Q8 | footprint.region.* | filled_regions_below_min | filledRegionCount=1 | ≥3 filled regional cards (am/cala/eu/mea/apac as relevant)… | Fill at least 3 source-supported regions (keep CALA; add EU/AM/APAC/ME… | Wave 13 source packs |
| mama-shelter | recent_momentum | recXCZCK05XXYX7Q8 | footprint.momentum | momentum_structure | {"cardCount":2,"titledCount":2,"datedCount":1,"geoSignalCount":2,"ownerSignalCou | ≥2 titled cards; structured dateLine (YYYY|Mon YYYY|Qn YYYY|… | Replace momentum cards via buildRecentMomentumCard from source-pack re… | Wave 13 source pack recentMomentumCandidates + propertyExamples |
| mercure | geographic_footprint | recevrLJ3m6rIug3S | footprint.region.* | filled_regions_below_min | filledRegionCount=1 | ≥3 filled regional cards (am/cala/eu/mea/apac as relevant)… | Fill at least 3 source-supported regions (keep CALA; add EU/AM/APAC/ME… | Wave 13 source packs |
| mercure | recent_momentum | recevrLJ3m6rIug3S | footprint.momentum | momentum_structure | {"cardCount":3,"titledCount":3,"datedCount":0,"geoSignalCount":3,"ownerSignalCou | ≥2 titled cards; structured dateLine (YYYY|Mon YYYY|Qn YYYY|… | Replace momentum cards via buildRecentMomentumCard from source-pack re… | Wave 13 source pack recentMomentumCandidates + propertyExamples |
| ibis | geographic_footprint | reclFXbpZ5XzLWbGP | footprint.region.* | filled_regions_below_min | filledRegionCount=1 | ≥3 filled regional cards (am/cala/eu/mea/apac as relevant)… | Fill at least 3 source-supported regions (keep CALA; add EU/AM/APAC/ME… | Wave 13 source packs |
| ibis | recent_momentum | reclFXbpZ5XzLWbGP | footprint.momentum | momentum_structure | {"cardCount":2,"titledCount":2,"datedCount":1,"geoSignalCount":2,"ownerSignalCou | ≥2 titled cards; structured dateLine (YYYY|Mon YYYY|Qn YYYY|… | Replace momentum cards via buildRecentMomentumCard from source-pack re… | Wave 13 source pack recentMomentumCandidates + propertyExamples |
| novotel | geographic_footprint | recQE2lSSSSyuUrMQ | footprint.region.* | filled_regions_below_min | filledRegionCount=1 | ≥3 filled regional cards (am/cala/eu/mea/apac as relevant)… | Fill at least 3 source-supported regions (keep CALA; add EU/AM/APAC/ME… | Wave 13 source packs |
| novotel | recent_momentum | recQE2lSSSSyuUrMQ | footprint.momentum | momentum_structure | {"cardCount":2,"titledCount":2,"datedCount":1,"geoSignalCount":2,"ownerSignalCou | ≥2 titled cards; structured dateLine (YYYY|Mon YYYY|Qn YYYY|… | Replace momentum cards via buildRecentMomentumCard from source-pack re… | Wave 13 source pack recentMomentumCandidates + propertyExamples |
| pullman | geographic_footprint | recFW9kfqKfOjv7Z1 | footprint.region.* | filled_regions_below_min | filledRegionCount=1 | ≥3 filled regional cards (am/cala/eu/mea/apac as relevant)… | Fill at least 3 source-supported regions (keep CALA; add EU/AM/APAC/ME… | Wave 13 source packs |
| pullman | recent_momentum | recFW9kfqKfOjv7Z1 | footprint.momentum | momentum_structure | {"cardCount":2,"titledCount":2,"datedCount":1,"geoSignalCount":2,"ownerSignalCou | ≥2 titled cards; structured dateLine (YYYY|Mon YYYY|Qn YYYY|… | Replace momentum cards via buildRecentMomentumCard from source-pack re… | Wave 13 source pack recentMomentumCandidates + propertyExamples |
| fairmont-hotels-and-resorts | geographic_footprint | recJhPaDVU3YUDQUt | footprint.region.* | filled_regions_below_min | filledRegionCount=1 | ≥3 filled regional cards (am/cala/eu/mea/apac as relevant)… | Fill at least 3 source-supported regions (keep CALA; add EU/AM/APAC/ME… | Wave 13 source packs |
| fairmont-hotels-and-resorts | recent_momentum | recJhPaDVU3YUDQUt | footprint.momentum | momentum_structure | {"cardCount":2,"titledCount":2,"datedCount":0,"geoSignalCount":2,"ownerSignalCou | ≥2 titled cards; structured dateLine (YYYY|Mon YYYY|Qn YYYY|… | Replace momentum cards via buildRecentMomentumCard from source-pack re… | Wave 13 source pack recentMomentumCandidates + propertyExamples |

## Exact audit failure strings

- **mama-shelter / geographic_footprint**: filled_regions_below_min:1
- **mama-shelter / recent_momentum**: dated_cards_below_min:1; linked_announcement_url_below_min:0; structured_date_line_below_min:0
- **mercure / geographic_footprint**: filled_regions_below_min:1
- **mercure / recent_momentum**: dated_cards_below_min:0; linked_announcement_url_below_min:0; structured_date_line_below_min:0
- **ibis / geographic_footprint**: filled_regions_below_min:1
- **ibis / recent_momentum**: dated_cards_below_min:1; linked_announcement_url_below_min:0; structured_date_line_below_min:0
- **novotel / geographic_footprint**: filled_regions_below_min:1
- **novotel / recent_momentum**: dated_cards_below_min:1; linked_announcement_url_below_min:0; structured_date_line_below_min:0
- **pullman / geographic_footprint**: filled_regions_below_min:1
- **pullman / recent_momentum**: dated_cards_below_min:1; linked_announcement_url_below_min:0; structured_date_line_below_min:0
- **fairmont-hotels-and-resorts / geographic_footprint**: filled_regions_below_min:1
- **fairmont-hotels-and-resorts / recent_momentum**: dated_cards_below_min:0; linked_announcement_url_below_min:0; structured_date_line_below_min:0

## Notes

- All six: only ootprint.region.cala filled; am/eu/mea/apac empty.
- All six: momentum cards lack structured date lines and linked announcement URLs (urlCount=0).
- Do not patch until cleanup packages are applied via public-six-geo-momentum-cleanup.

