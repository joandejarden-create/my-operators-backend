# ADP Production Activation Plan

**STOP — not authorized this phase.**

## Preconditions (all must be true)

1. Founder authorizes Group B corrected package publication (21 hotels).
2. Founder authorizes production application deploy containing:
   - Permanent Full E2E certification gate (`S8_MEASUREMENT_RECONCILIATION`)
   - Provider Presence grain lock using frozen eligibleScenarioIds
   - Customer render fail-closed behavior
3. Shared Dealality production release coordinator clear of AVA/GDI conflicts.
4. Production runtime period observations available for serve-time repair **or** corrected packages published to durable current.
5. ADP / GDI / AVA regression gates run on candidate SHA.
6. Rollback target = prior known-good Railway deployment.
7. Durable publication volume + share tokens preserved (no rotation).

## Activation sequence (future)

1. Deploy application code only (certification gates).
2. Publish 21 corrected packages to durable store as **new current versions** (prior retained historical).
3. Generate/update PDFs for corrected currents; retain prior PDF SHAs historically.
4. Run `certifyAdpCustomerVisibleParity` against production Admin + share + browser + PDF for all 38.
5. Mark CUSTOMER_READY only after parity PASS.
6. Optional separate authorization: Airtable history backfill with certificationHash linkage.

## Explicitly excluded until authorized

- Paid AI research
- New monitoring runs
- Airtable writes/backfills
- Share token rotation
- Historical data deletion
- GDI / AVA behavior changes

## Rollback

- Application: redeploy prior Railway deployment ID
- Data: durable registry keeps prior package versions; switch `current` pointer back if needed
- Shares: unchanged tokens continue resolving to `current_published`
