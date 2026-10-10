# E2E Completion Definition (updated)

E2E complete MUST verify:

1. Property in GDI selector
2. Customer page loads
3. claimedReady == Ready gate count
4. claimedValidWatch == Watch gate count
5. `assertCustomerPublicationCountsMatch` OK
6. API Ready/Watch match published customerVisible counts
7. UI card count matches API
8. Cards open when present
9. HOLD_WATCH / internal-only never customer-facing
10. Empty state truthful when Ready=0 and Valid Watch=0

- Script: `scripts/gdi-mallorca-son-vida-dual-e2e.mjs` → `CUSTOMER_PUBLICATION_GATE.json`
- Test: `npm run test:gdi-customer-publication-invariants-v1`
