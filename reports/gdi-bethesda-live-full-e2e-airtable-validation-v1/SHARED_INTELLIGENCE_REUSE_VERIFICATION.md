# SHARED_INTELLIGENCE_REUSE_VERIFICATION

Generated: 2026-11-18T18:38:25.436Z
Status: **VERIFIED**

## Intent

Operate the shared GDI intelligence model we already built. Do **not** recreate Demand Generators, Programs, Signals, Contacts, or competing identity systems.

## Reused modules

- `reuse-prior-intelligence-v1`
- `shared-demand-model-v1`
- `demand-generators/entities + airtable-stores`
- `gdi-full-e2e-airtable-persistence-contract/adapter/eligibility`
- `gdi-full-e2e-research-persistence-bridge-v1`
- `gdi-paid-deep-two-stage-dedupe-v1`
- `hotel-research-opportunity-field-map`

## Identity (stable across hotels)

| Field | Value |
| --- | --- |
| Organization | GuidePoint Security |
| Demand Generator ID | `dg_ed65f0a5fba6ec9c` |
| Org identity key | `guidepoint security` |
| Bethesda research composite | `recLuxvwwxID7U2B8::disc_guidepoint_shared` |
| Peer hotel research composite | `recPeerHotelSharedDg01::disc_guidepoint_shared_peer` |

Same underlying company → same `demandGeneratorId`. Separate hotel research rows via `hotelId::opportunityId`. Hotel-scoped sales opportunity identity remains independent.

## Routing

- Shared org → Demand Generators (+ Hotel Demand Generator Fit when hotel-qualified)
- Hotel research → GDI Hotel Research Opportunities (bridge)
- Qualified GDO → Group Demand Opportunities (**separate gate**)
- EXCLUDE → no customer-facing table writes

## Cross-hotel fixture result

| Hotel | CREATED | Shared DG id reused |
| --- | ---: | --- |
| Bethesda | 1 | yes |
| Peer | 1 | yes |

Private sales feedback is **not** cross-shared.

## Paid-deep inventory reuse

Inventory / reuse-ledger org keys skip repeat paid deep for known organizations (`EXISTING_HOTEL_INVENTORY_ORG` path).

## Canonical Full E2E link (this PR)

`runCanonicalFullE2eResearch` now builds the existing `reuse-prior-intelligence` ledger before the research chain and merges organization identity keys into paid-deep inventory dedupe. Airtable DG/program/signal **reads** still require `loadSharedIntelligenceFromAirtable=true` (read-only). Writes remain dual-authorized.

## Checks

```json
{
  "stableDemandGeneratorIdAcrossHotels": true,
  "orgIdentityKeyNormalized": true,
  "sharedOrgRoutesToDemandGenerators": true,
  "hotelFitRoutedSeparately": true,
  "hotelOpportunityUsesResearchOrGdoPath": true,
  "excludedDoesNotWriteCustomerTables": true,
  "excludedEligibilityBlocksWrites": true,
  "canonicalIdentitiesResolve": true,
  "sharedEligibilityAllowsGenerators": true,
  "hotelResearchRowsCreatedSeparately": true,
  "compositeKeysDifferByHotel": true,
  "salesOpportunityIdentityHotelScoped": true,
  "inventoryDedupeSkipsKnownSharedOrg": true,
  "productionWriteGuardIntact": true,
  "gdoNotAutoWrittenByResearchBridge": true,
  "publicationNotAutoWritten": true,
  "noNewTablesInvented": true,
  "reuseModulesRetained": true
}
```

Failed: (none)

## Missing integrations

- None beyond the canonical reuse-ledger link shipped in this update.

## Safeguards preserved

- Production Airtable writes blocked by default
- Research bridge does not write GDO or publication
- EXCLUDE candidates do not become customer-facing rows
