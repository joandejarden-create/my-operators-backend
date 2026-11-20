#!/usr/bin/env node
/**
 * HE 5.0 standing authorization + conditional approval gates (no live writes).
 */
import assert from "node:assert/strict";
import {
  APPROVAL_CLASS,
  HE50_STANDING_APPLY_GATE,
  isLegacyHotelCensusDestination,
  isAllowedDestinationTable,
} from "../lib/hotel-intelligence/he-5-0-full-e2e/standing-authorization-v1.js";
import {
  classifyPilotMutation,
  classifyPilotMutationPackage,
} from "../lib/hotel-intelligence/he-5-0-full-e2e/conditional-approval-v1.js";
import { buildHe50DryRunMutationPackage } from "../lib/hotel-intelligence/he-5-0-full-e2e/dry-run-mutation-package.js";
import {
  APPLY_GATE,
  HE50_GLOBAL_USD_CAP,
  HE50_HOTEL_CENSUS_TABLE_ID_RETIRED,
} from "../lib/hotel-intelligence/he-5-0-full-e2e/contract.js";
import { runHe50PilotSupervisor } from "../lib/hotel-intelligence/he-5-0-full-e2e/supervisor.js";

assert.equal(HE50_STANDING_APPLY_GATE.standingAuthorization, true);
assert.equal(HE50_STANDING_APPLY_GATE.legacyHotelCensusWritesAllowed, false);
assert.equal(HE50_STANDING_APPLY_GATE.surfeAutomatic, false);
assert.equal(HE50_STANDING_APPLY_GATE.schemaChangesAllowed, false);
assert.equal(HE50_STANDING_APPLY_GATE.hiOrgContactsAuthorized, true);
assert.equal(HE50_STANDING_APPLY_GATE.hiContactChannelsAuthorized, true);
assert.equal(HE50_STANDING_APPLY_GATE.ownershipRelationshipPromotionAuthorized, false);
assert.equal(APPLY_GATE.standingAuthorization, true);
assert.equal(HE50_GLOBAL_USD_CAP, 50);

assert.equal(isLegacyHotelCensusDestination(HE50_HOTEL_CENSUS_TABLE_ID_RETIRED), true);
assert.equal(isLegacyHotelCensusDestination("tbl9aY5ijiuIzzWam"), false);
assert.equal(isAllowedDestinationTable("tbl9aY5ijiuIzzWam"), true);
assert.equal(isAllowedDestinationTable("tblgj2qEwxjTcg6q0"), false);
assert.equal(isAllowedDestinationTable("tbldJAj0nK1l39JZL"), true);
assert.equal(isAllowedDestinationTable("HI Org Contacts"), true);

const { buildBethHiOrgContactsMutationPackage } = await import(
  "../lib/hotel-intelligence/he-5-0-full-e2e/hi-org-contacts-beth-package-v1.js"
);
const bethContacts = buildBethHiOrgContactsMutationPackage();
const bethClassified = classifyPilotMutationPackage(bethContacts.mutations);
assert.ok(bethClassified.summary.executable >= 5);
assert.equal(
  bethContacts.mutations.some((m) => m.proposedValue?.createEconomicOwnerEdge === true),
  false
);
const surfeBlocked = classifyPilotMutation({
  id: "M-BAD-SURFE",
  action: "UPSERT",
  destinationTable: "tbldJAj0nK1l39JZL",
  canonicalEntityId: "recLuxvwwxID7U2B8",
  verificationStatus: "PUBLICLY_VERIFIED",
  proposedValue: {
    displayName: "Test Person",
    affiliationDedupeKey: "person::test::org",
    surfeDerived: true,
  },
});
assert.equal(surfeBlocked.approvalClass, APPROVAL_CLASS.BLOCKED_OUT_OF_SCOPE);

const dry = buildHe50DryRunMutationPackage();
const classified = classifyPilotMutationPackage(dry.mutations);
assert.equal(classified.summary.total, dry.mutationCount);
assert.ok(classified.summary.executable >= 5);

const pap = dry.mutations.find((m) => m.id.includes("papagayo"));
const papClass = classifyPilotMutation(pap);
assert.equal(papClass.approvalClass, APPROVAL_CLASS.AUTO_APPROVED_CANONICAL_CREATE);
assert.equal(papClass.executable, true);
assert.ok(!JSON.stringify(papClass.operations[0].fields).includes("mullenAssociation"));

const noOp = dry.mutations.find((m) => m.action === "NO_OP");
assert.equal(
  classifyPilotMutation(noOp).approvalClass,
  APPROVAL_CLASS.SKIPPED_NO_CHANGE
);

const legacyBlocked = classifyPilotMutation({
  id: "M-BAD",
  action: "UPDATE",
  destinationTable: "tblgj2qEwxjTcg6q0",
  canonicalEntityId: "recLuxvwwxID7U2B8",
  field: "Rooms / Keys",
  proposedValue: 1,
  verificationStatus: "VERIFIED_FREE",
});
assert.equal(legacyBlocked.approvalClass, APPROVAL_CLASS.BLOCKED_OUT_OF_SCOPE);

const bethYear = dry.mutations.find((m) => m.id === "M-BETH-001");
const bethClass = classifyPilotMutation(bethYear);
assert.equal(bethClass.approvalClass, APPROVAL_CLASS.AUTO_APPROVED_CANONICAL_UPDATE);
assert.equal(bethClass.operations[0].patch["Opening Date"], "1979-01-01");

const supervisor = runHe50PilotSupervisor({ enablePaid: false });
assert.match(supervisor.stopMessage, /STANDING CONDITIONAL AUTHORIZATION/);
assert.equal(supervisor.standingApplyGate.standingAuthorization, true);
assert.ok(supervisor.mutationClassification.summary.total >= 10);

console.log(
  JSON.stringify(
    {
      executable: classified.summary.executable,
      byClass: classified.summary.byClass,
    },
    null,
    2
  )
);
console.log("test:he-5-0-controlled-apply-v1 PASS");
