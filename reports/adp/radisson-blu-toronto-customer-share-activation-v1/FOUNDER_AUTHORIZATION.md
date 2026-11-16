# FOUNDER_AUTHORIZATION

AUTHORIZATION TIMESTAMP: **2026-11-16T23:17:19.502Z**

Transition:

- FOUNDER_REVIEWED_STAGED → FOUNDER_APPROVED_FOR_CUSTOMER → CUSTOMER_READY → PUBLISHED

Flags set (Toronto only):

- founderApprovedForCustomer = true
- founderApprovedForCustomerAt = 2026-11-16T23:17:19.502Z
- publicationAuthorized / founderPublicationAuthorized / customerPublicationAuthorized = true
- customerReadyAuthorized / customerReady = true
- customerPublished = true
- customerReadinessStatus = CUSTOMER_READY
- publicationAuthorizationStatus = AUTHORIZED

Surfaces updated:

- Local durable AVA store
- Git-tracked production/store seed
- Production Railway durable volume (/app/data/adp-durable-published/ava-data/...) — Toronto only
