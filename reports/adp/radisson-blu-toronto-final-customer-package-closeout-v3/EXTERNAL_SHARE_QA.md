# External share QA

- Existing Toronto share token retained (no new token)
- Share resolves current_published Standard ADP via `getPublishedOwnerReport` → payload.executiveRead
- Verified locally: payload.executiveMetrics.considerationRate = 33.2; executiveRead has 33.2%; stale 29% absent
- Token reference pack: reports/adp/radisson-blu-toronto-customer-share-activation-v1/
