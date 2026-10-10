# AdpHotelIdentityContract

Module: `adp-hotel-identity-contract-v1.js`

Required fields: canonicalName, brand, officialDomain, officialPropertyUrl, address/geo when available, aliases[], formerNames[], commonShorthand[], brandShorthand[], neighborhoodAliases[], propertyEntityId, portfolio/loyalty identity, ownedDomains[].

Pre-flight outcomes: IDENTITY_PASS | IDENTITY_REVIEW | IDENTITY_FAIL

Canaries: canonical full name, common short name, brand+location shorthand, known alternate name.

Hilton lesson: missing "Hilton Times Square" must fail the identity canary (automated regression).
