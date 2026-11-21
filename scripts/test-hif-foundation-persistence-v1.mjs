#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  HIF_FOUNDATION_PERSISTENCE,
  WRITE_CATEGORY,
  FOUNDATION_DESTINATIONS,
  EXISTING_FINDING_TYPES,
  mapCommercialCategoryToFindingType,
  commercialOpportunityFieldName,
  classifyWriteCategory,
  buildDiscoveryInventoryFields,
  buildCommercialFindingFields,
  planFoundationPersistBatch,
} from "../lib/hotel-intelligence/sec-edgar/hif-foundation-persistence-v1.js";
import { TABLE_HI_HOTEL_DISCOVERY_INVENTORY } from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";

assert.equal(HIF_FOUNDATION_PERSISTENCE, "hif-foundation-persistence-v1");
assert.equal(FOUNDATION_DESTINATIONS.baseId, "appCCUsuGsE1ifoLk");
assert.ok(FOUNDATION_DESTINATIONS.discoveryInventory.tableId);
assert.ok(FOUNDATION_DESTINATIONS.hotelContracts.tableId);

// 1–2 dedupe keys exist
assert.equal(FOUNDATION_DESTINATIONS.discoveryInventory.idempotencyKey, "Discovery Property ID");
assert.equal(FOUNDATION_DESTINATIONS.discoveryFindings.idempotencyKey, "Finding ID");

// 8–9 finding type mapping uses existing options only
assert.equal(mapCommercialCategoryToFindingType("BRAND_CONVERSION_TARGET"), EXISTING_FINDING_TYPES.BRAND_AFFILIATION);
assert.equal(mapCommercialCategoryToFindingType("MANAGEMENT_CONTRACT_TARGET"), EXISTING_FINDING_TYPES.MANAGEMENT);
assert.equal(commercialOpportunityFieldName("BCT"), "COMMERCIAL_OPPORTUNITY_BCT");
assert.ok(!Object.values(EXISTING_FINDING_TYPES).includes("COMMERCIAL_OPPORTUNITY"));

// 10 no false Priority A via research-only
assert.equal(
  classifyWriteCategory({ speculation: true, evidenceSupported: true, sourceUrl: "https://x" }).category,
  WRITE_CATEGORY.RESEARCH_ONLY,
);

// 11 destination mapping — HPC create forbidden
assert.equal(
  classifyWriteCategory({
    destination: "Hotel Property Census",
    createHpc: true,
    evidenceSupported: true,
    sourceUrl: "https://x",
    verified: true,
  }).category,
  WRITE_CATEGORY.RESEARCH_ONLY,
);

// 12–13 non-destructive / eligible inventory
assert.equal(
  classifyWriteCategory({
    destination: TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
    verified: true,
    identityClass: "VERIFIED_PROPERTY",
    evidenceSupported: true,
    sourceUrl: "https://example.com",
  }).category,
  WRITE_CATEGORY.AUTO_PERSIST_ELIGIBLE,
);

// 14–15 conflict hold
assert.equal(
  classifyWriteCategory({ conflictHold: true, evidenceSupported: true, sourceUrl: "https://x" }).category,
  WRITE_CATEGORY.CONFLICT_HOLD,
);

// 16–17 graph evidence without HPC
assert.equal(
  classifyWriteCategory({
    destination: "HI Graph Evidence",
    evidenceSupported: true,
    sourceUrl: "https://x",
    verified: true,
  }).category,
  WRITE_CATEGORY.RESEARCH_ONLY,
);

const inv = buildDiscoveryInventoryFields({
  discoveryPropertyId: "disc_cb1_lakeville_inn_mn",
  hotelName: "Lakeville Inn",
  city: "Lakeville",
  stateProvince: "MN",
  physicalCountry: "US",
  sourceUrl: "https://www.startribune.com/example",
});
assert.equal(inv["Discovery Property ID"], "disc_cb1_lakeville_inn_mn");
assert.equal(inv["Publication State"], "CUSTOMER_BLOCKED");

const finding = buildCommercialFindingFields({
  findingId: "find_cb1_lakeville_bct_2026",
  commercialCategory: "BRAND_CONVERSION_TARGET",
  commercialThesis: "Owner seeking new brand by end of 2026 after Hampton termination",
  commercialRelevance: "VERIFIED_RELEVANT",
  commercialTiming: "0_6_MONTHS",
  commercialAccessibility: "ACCESSIBLE_PROBABLE",
  evidencePathway: "OWNER_LED_PRIVATE_DECISION",
  sourceUrl: "https://www.startribune.com/example",
  countsTowardFirst100: true,
});
assert.equal(finding["Finding Type"], "BRAND_AFFILIATION");
assert.equal(finding["Field Name"], "COMMERCIAL_OPPORTUNITY_BCT");
assert.match(finding["Value Observed"], /first100=YES/);

const plan = planFoundationPersistBatch([
  {
    destination: TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
    verified: true,
    identityClass: "VERIFIED_PROPERTY",
    evidenceSupported: true,
    sourceUrl: "https://x",
  },
  { researchOnly: true },
  { conflictHold: true },
  {
    destination: TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
    verified: true,
    identityClass: "VERIFIED_PROPERTY",
    evidenceSupported: true,
    sourceUrl: "https://x",
    existingAirtableId: "recABC",
    fieldsUnchanged: true,
  },
]);
assert.equal(plan.counts.creates, 1);
assert.equal(plan.counts.researchOnly, 1);
assert.equal(plan.counts.conflicts, 1);
assert.equal(plan.counts.noops, 1);

// 18–19 no unauthorized schema / publication
assert.equal(inv["Publication State"], "CUSTOMER_BLOCKED");
assert.equal(finding["Notes"].includes("CUSTOMER_BLOCKED"), true);

// 20 weekly change readiness — destination IDs stable
assert.equal(FOUNDATION_DESTINATIONS.hotelContracts.tableId, "tblMjUCBtXF2o3r9u");
assert.equal(FOUNDATION_DESTINATIONS.contractMilestones.tableId, "tbljzAYjYs3dyHWZk");

console.log("PASS test-hif-foundation-persistence-v1");
