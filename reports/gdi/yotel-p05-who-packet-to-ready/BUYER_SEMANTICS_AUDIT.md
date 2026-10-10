# Buyer Semantics Audit — YOTEL P0.5

## Observation
P0 reported PUBLIC CONTACT PATHS = 37 and BUYER ENTITIES RESOLVED = 0.

## Root cause
**`buildOpportunity()` field omission (mapping bug)** + secondary ORG_PATH / surface-gate issues — not an overly strict buyer definition.

1. Campaign children were created with `buyerEntity`, `publicContactPath`, `organizationContactUrl`, and `contactResearchAttempted: true`.
2. `enrichGdiOpportunityForCustomer` → `buildOpportunity()` rebuilt a **allowlisted** object and **dropped** buyer/WHO fields (not in the factory return shape).
3. Airtable payload therefore persisted `whoPathClass = NOT_RESEARCHED` with empty buyer/contact fields.
4. Secondary: `classifyWhoHowPath` did not treat `publicContactPath` alone as ORG_PATH (now does).
5. Secondary: `isCustomerSurfaceActiveEligible` used stored `customerVisible === false` as a gate input, deadlocking never-promoted packets that already `classify` as `KEEP_ACTIVE`.
6. P0 RETURN counted empty `buyerEntity` after strip → 0.

## Fix in P0.5
- Preserve buyer/WHO/parent-link fields in `opportunity-factory.js` `buildOpportunity`.
- Accept `publicContactPath` as ORG_PATH in `opportunity-who-resolution-v1.js`.
- Readiness surface gate uses classify (not stored visibility) unless `honorStoredVisibility`.
- Re-apply deterministic buyer entity + role from evidence packs + `resolveGdiDemandBuyer`.
- Do **not** relabel random URLs as buyers without org/role relationship.
- Surfe not used. Named people not invented.

## Proof relationship
Buyer entity = organization (or housing desk / secretariat) that controls or influences hotel sourcing / group travel for the campaign child. Contact path = public URL for that org/housing channel.
