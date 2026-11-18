# Airtable complete research storage design

## Three distinct lifecycle layers

| Layer | Purpose | Destination |
|-------|---------|-------------|
| **A. Research inventory** | All valid non-duplicate research opportunities (exploratory, watchlist, continuation) | Proposed: **GDI Hotel Research Opportunities** (schema gate) |
| **B. Qualified GDO registry** | Commercially eligible hotel opportunities (demand + contact standards) | Existing: **Group Demand Opportunities** (`tblRuReslJMwsfRQj`) |
| **C. Customer publication** | Founder-authorized customer-visible packages | FS: `bundled-publication-packages/{hotelId}/CURRENT.json` |

These layers must never collapse into a single ambiguous status.

## Existing tables — correct uses

- **Demand Generators / Programs / Signals** — global identity authority; reuse; no hotel-private sales activity
- **Group Demand Opportunities** — qualified commercial registry only
- **Private Event Venues / Signals / Hotel Venue Fit** — venue identity + hotel fit
- **GDI Research Targets / Runs / Target Runs** — monitoring coverage & run provenance — **not** an opportunity registry
- **Hotel Commercial Profiles / Need Periods** — hotel commercial profile — **not** opportunity storage

## Gap

Existing Airtable model **cannot** durably retain research-only hotel opportunities without either:

1. Misusing GDO (forces unqualified research into commercial rows — **rejected**), or
2. Misusing Research Targets / Runs as opportunity registries — **rejected**, or
3. Adding a minimal **GDI Hotel Research Opportunities** table — **recommended**

Until schema approval, filesystem `opportunities.json` remains the operational research SoT while dry-run manifests enumerate every research row that must land in Airtable after approval.

## Proposed table (not created)

```
{
  "name": "GDI Hotel Research Opportunities",
  "proposedTableId": null,
  "status": "REQUIRES_FOUNDER_SCHEMA_APPROVAL",
  "identityField": "opportunityId",
  "purpose": "Durable Airtable mirror of complete hotel research inventory without implying commercial GDO qualification or customer publication.",
  "requiredFields": [
    "opportunityId",
    "hotelId",
    "organizationName",
    "researchLifecycleStatus",
    "commercialEligibilityStatus",
    "demandThesis",
    "priority",
    "title",
    "segment",
    "sourceSummary",
    "firstDiscoveredAt",
    "firstDiscoveredRunId",
    "lastResearchedAt",
    "lastMaterialChangeAt",
    "dgGeneratorId",
    "researchPayloadJson",
    "schemaVersion"
  ],
  "forbiddenUses": [
    "Do not treat rows as customer-published",
    "Do not auto-promote into CURRENT packages",
    "Do not use as substitute for Demand Generators",
    "Do not recreate YOTEL legacy 108 GDO IDs"
  ],
  "reuseExistingInstead": {
    "qualifiedCommercial": "Group Demand Opportunities",
    "orgIdentity": "Demand Generators",
    "researchTargets": "GDI Research Targets",
    "filesystemFallback": "data/group-demand-intelligence/hotels/{hotelId}/opportunities.json remains operational SoT until table is approved and backfilled"
  }
}
```

## Contract constants

Base: `appa2cE7FTRmIbB32` (AI Demand Positioning)
