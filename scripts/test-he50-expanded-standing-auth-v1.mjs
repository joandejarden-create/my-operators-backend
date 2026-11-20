#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  HE50_STANDING_AUTH_VERSION,
  HE50_ALLOWED_DESTINATION_TABLES,
  HE50_DEMAND_ANCHORS_TABLE_ID,
  HE50_TRAVEL_INFRASTRUCTURE_TABLE_ID,
  HE50_HI_ORGANIZATIONS_TABLE_ID,
  HE50_HI_HOTEL_ORG_RELATIONSHIPS_TABLE_ID,
  HE50_HI_ORG_CONTACTS_TABLE_ID,
  HE50_HI_GRAPH_EVIDENCE_TABLE_ID,
  HE50_STANDING_APPLY_GATE,
  isAllowedDestinationTable,
  isLegacyHotelCensusDestination,
} from "../lib/hotel-intelligence/he-5-0-full-e2e/standing-authorization-v1.js";
import {
  HE50_HPC_TABLE_ID,
  HE50_HOTEL_CENSUS_TABLE_ID_RETIRED,
} from "../lib/hotel-intelligence/he-5-0-full-e2e/contract.js";

assert.match(HE50_STANDING_AUTH_VERSION, /v2-expanded/);
assert.equal(Object.keys(HE50_ALLOWED_DESTINATION_TABLES).length, 7);
assert.ok(isAllowedDestinationTable(HE50_HPC_TABLE_ID));
assert.ok(isAllowedDestinationTable(HE50_DEMAND_ANCHORS_TABLE_ID));
assert.ok(isAllowedDestinationTable(HE50_TRAVEL_INFRASTRUCTURE_TABLE_ID));
assert.ok(isAllowedDestinationTable(HE50_HI_ORGANIZATIONS_TABLE_ID));
assert.ok(isAllowedDestinationTable(HE50_HI_HOTEL_ORG_RELATIONSHIPS_TABLE_ID));
assert.ok(isAllowedDestinationTable(HE50_HI_ORG_CONTACTS_TABLE_ID));
assert.ok(isAllowedDestinationTable(HE50_HI_GRAPH_EVIDENCE_TABLE_ID));
assert.ok(isLegacyHotelCensusDestination(HE50_HOTEL_CENSUS_TABLE_ID_RETIRED));
assert.equal(isAllowedDestinationTable(HE50_HOTEL_CENSUS_TABLE_ID_RETIRED), false);
assert.equal(HE50_STANDING_APPLY_GATE.surfePersistenceAllowed, false);
assert.equal(HE50_STANDING_APPLY_GATE.ownershipProposedMustRemainProposed, true);
assert.equal(HE50_STANDING_APPLY_GATE.schemaChangesAllowed, false);

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:he50-expanded-standing-auth-v1",
      certifications: {
        CANONICAL_PERSISTENCE_ALLOWLIST_EXPANDED: true,
        LEGACY_CENSUS_BLOCKED: true,
        SURFE_PERSISTENCE_FORBIDDEN: true,
        PROPOSED_OWNERSHIP_PROTECTED: true,
      },
      tableCount: Object.keys(HE50_ALLOWED_DESTINATION_TABLES).length,
    },
    null,
    2,
  ),
);
