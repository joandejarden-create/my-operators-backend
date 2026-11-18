# Airtable read-back verification

Status: **PENDING_PRODUCTION_APPLY**

Offline memory-store read-back matched: **true**

Production plan:

1. After each authorized CREATE/UPDATE, call `readBackResearchInventory`
2. Assert `compositeKey`, `hotelId`, `opportunityId`
3. Assert contact + thesis fields survive
4. Record failures in durable persistence ledger; do not claim success without match
