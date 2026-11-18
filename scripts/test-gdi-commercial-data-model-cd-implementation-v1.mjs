#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  MAP_GDI_OPPORTUNITY as F,
  MAP_GDI_OPPORTUNITY_DG_IDENTITY,
} from "../lib/group-demand-intelligence/opportunity-field-map.js";
import { MAP_GDI_DG_LINK } from "../lib/group-demand-intelligence/demand-generators/airtable-field-map.js";
import {
  compactOpportunityPayloadForAirtable,
  opportunityToAirtableFields,
  airtableRecordToOpportunity,
} from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import {
  assertCommercialPayloadRoundTrip,
  REQUIRED_COMMERCIAL_PAYLOAD_KEYS,
  stripHotelPrivateKeys,
} from "../lib/group-demand-intelligence/workflow/gdi-commercial-payload-durability-v1.js";
import {
  assertDescriptionRewriteIsNonMaterial,
  buildDurableWeeklyChangeAnnotation,
  WEEKLY_CHANGE_DURABILITY_CONTRACT,
} from "../lib/group-demand-intelligence/workflow/gdi-weekly-change-durability-v1.js";
import {
  assertCrossHotelSharedIdentityIsolation,
  mayUpsertDemandProgram,
  applySharedDemandIdentityToOpportunity,
} from "../lib/group-demand-intelligence/workflow/gdi-shared-demand-identity-v1.js";
import { executeFullE2eAirtablePersistence } from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-airtable-persistence-adapter-v1.js";
import { WEEKLY_DELTA_STATE } from "../lib/group-demand-intelligence/weekly-delta.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

// CD-3: core map includes live DG identity fields; aligns with MAP_GDI_DG_LINK.
assert.equal(F.dgGeneratorId, "dgGeneratorId");
assert.equal(F.dgProgramId, "dgProgramId");
assert.equal(F.dgSignalId, "dgSignalId");
assert.equal(F.hotelGeneratorFitId, "hotelGeneratorFitId");
assert.equal(F.dgGeneratorLink, "Demand Generator");
assert.equal(MAP_GDI_OPPORTUNITY_DG_IDENTITY.dgGeneratorId, MAP_GDI_DG_LINK.dgGeneratorId);
assert.equal(MAP_GDI_DG_LINK.dgSignalsLink, "Demand Generator Signals");

// CD-2 / CD-5 / CD-7: commercial keys survive compaction.
const rich = {
  opportunityId: "gdi_opp_test_cd",
  hotelId: "recHotelA",
  organizationName: "Shared Org LLC",
  title: "Shared Org LLC — annual meeting",
  description: "Original description",
  salesActionKey: "BUILD_ACCOUNT",
  salesActionPillLabel: "Build account",
  commercialStageKey: "ACCOUNT_DEVELOPMENT",
  revenueStreamClass: "GUESTROOMS",
  bookingWindowStatus: "CONTACT_NOW",
  competitorHotel: "Competitor Inn",
  cmiCompetitorHotel: "Competitor Inn",
  competitorVenueStatus: "HISTORICAL_PLACEMENT",
  cmiEvidenceStatus: "HISTORICAL_COMPETITOR_COMMITMENT",
  weeklyDeltaState: WEEKLY_DELTA_STATE.UPDATED,
  firstSeenRunId: "run_prior",
  firstSeenAt: "2026-11-01T00:00:00.000Z",
  lastMaterialChangeAt: "2026-11-10T00:00:00.000Z",
  lastMaterialChangeRunId: "run_prior",
  eligibilityGaps: ["MISSING_CREDIBLE_CONTACT"],
  salesReadinessBlockers: ["MISSING_CREDIBLE_CONTACT"],
  salesReadinessRuleVersion: "sales_ready_v1",
  estimatedRoomNights: 120,
  estimatedRoomNightsClaimKind: "RELIABLE_INFERRED",
  estimatedRoomNightsMethodology: "peakRooms * stayLength",
  dgGeneratorId: "dg_shared_001",
  dgProgramId: "prog_shared_001",
  dgSignalId: "sig_shared_001",
  hotelGeneratorFitId: "fit_hotel_a_001",
  hotelSalesFeedback: "PRIVATE_NOTE_DO_NOT_SHARE",
  exaRawHits: Array.from({ length: 200 }, (_, i) => ({ i, blob: "x".repeat(500) })),
};
const compacted = compactOpportunityPayloadForAirtable(rich);
assert.equal(compacted.salesActionKey, "BUILD_ACCOUNT");
assert.equal(compacted.bookingWindowStatus, "CONTACT_NOW");
assert.equal(compacted.competitorHotel, "Competitor Inn");
assert.equal(compacted.weeklyDeltaState, WEEKLY_DELTA_STATE.UPDATED);
assert.deepEqual(compacted.eligibilityGaps, ["MISSING_CREDIBLE_CONTACT"]);
assert.equal(compacted.estimatedRoomNights, 120);
assert.ok(!compacted.exaRawHits);
const roundTrip = assertCommercialPayloadRoundTrip(rich, compacted);
assert.equal(roundTrip.ok, true, `missing ${roundTrip.missing.join(",")}`);
assert.ok(REQUIRED_COMMERCIAL_PAYLOAD_KEYS.includes("salesActionKey"));

// Typed Airtable field mapping includes DG ids + provenance.
const fields = opportunityToAirtableFields(rich, { hotelId: "recHotelA", runId: "run_x" });
assert.equal(fields[F.dgGeneratorId], "dg_shared_001");
assert.equal(fields[F.dgProgramId], "prog_shared_001");
assert.equal(fields[F.dgSignalId], "sig_shared_001");
assert.equal(fields[F.hotelGeneratorFitId], "fit_hotel_a_001");
assert.equal(fields[F.firstDiscoveredRunId], "run_prior");
assert.equal(fields[F.lastMaterialChangeRunId], "run_prior");
const payload = JSON.parse(fields[F.opportunityPayloadJson]);
assert.equal(payload.salesActionKey, "BUILD_ACCOUNT");
assert.equal(payload.competitorHotel, "Competitor Inn");

const reconstructed = airtableRecordToOpportunity({
  id: "recTest",
  fields: {
    ...fields,
    [F.opportunityPayloadJson]: fields[F.opportunityPayloadJson],
  },
});
assert.equal(reconstructed.dgGeneratorId, "dg_shared_001");
assert.equal(reconstructed.weeklyDeltaState, WEEKLY_DELTA_STATE.UPDATED);
assert.equal(reconstructed.salesActionKey, "BUILD_ACCOUNT");

// CD-1: description rewrite is not material; durability contract present.
const prior = {
  opportunityId: "gdi_opp_test_cd",
  organizationName: "Shared Org LLC",
  title: "Shared Org LLC — annual meeting",
  eventStartDate: "2027-03-01",
  venueStatus: "OPEN_UNRESOLVED",
  opportunityType: "PRIMARY_PURSUIT",
  priority: "HIGH_PRIORITY",
  roomDemandStatus: "ESTIMATED_ROOM_DEMAND",
  firstSeenRunId: "run_prior",
  firstSeenAt: "2026-11-01T00:00:00.000Z",
};
const nonMaterial = assertDescriptionRewriteIsNonMaterial({
  prior,
  rewrittenDescription: "Completely rewritten marketing prose that must not force NEW/UPDATED",
  runId: "run_now",
});
assert.equal(nonMaterial.ok, true, JSON.stringify(nonMaterial));
assert.ok(WEEKLY_CHANGE_DURABILITY_CONTRACT.storage.includes("filesystem.hotel.opportunities"));
const material = buildDurableWeeklyChangeAnnotation({
  baselineOpportunity: prior,
  currentOpportunity: { ...prior, eventStartDate: "2027-04-01" },
  runId: "run_now",
  latestCompletedWeeklyRunId: "run_now",
});
assert.equal(material.weeklyDeltaState, WEEKLY_DELTA_STATE.UPDATED);
assert.equal(material.isNewThisWeek, false);

// Cross-hotel shared identity isolation.
const hotelA = applySharedDemandIdentityToOpportunity(
  {
    opportunityId: "opp_a",
    hotelId: "recHotelA",
    hotelSalesFeedback: "A private",
  },
  { dgGeneratorId: "dg_shared_001", dgProgramId: "prog_shared_001" },
);
const hotelB = applySharedDemandIdentityToOpportunity(
  {
    opportunityId: "opp_b",
    hotelId: "recHotelB",
    hotelSalesFeedback: "B private",
  },
  { dgGeneratorId: "dg_shared_001", dgProgramId: "prog_shared_001" },
);
const isolation = assertCrossHotelSharedIdentityIsolation({
  hotelAOpportunity: hotelA,
  hotelBOpportunity: hotelB,
});
assert.equal(isolation.ok, true, JSON.stringify(isolation));
assert.ok(!("hotelSalesFeedback" in stripHotelPrivateKeys(hotelA)));

// CD-4: programs only with recurrence evidence.
assert.equal(
  mayUpsertDemandProgram({
    recurrenceStatus: "RECURRING_CONFIRMED",
    evidenceConfirmed: true,
  }).allowed,
  true,
);
assert.equal(
  mayUpsertDemandProgram({
    recurrenceStatus: "POSSIBLE_RECURRING",
    evidenceConfirmed: true,
  }).allowed,
  false,
);
assert.equal(
  mayUpsertDemandProgram({
    recurrenceStatus: "RECURRING_CONFIRMED",
    evidenceConfirmed: true,
    onboardingMirror: true,
  }).allowed,
  false,
);

// CD-8: persistence apply remains fail-closed.
const blocked = await executeFullE2eAirtablePersistence({
  hotelId: "recHotelA",
  apply: true,
  authorizeProductionWrites: true,
  exactWriteSetAuthorization: "test-should-still-block",
  candidates: [],
});
assert.equal(blocked.ok, false);
assert.equal(blocked.code, "AIRTABLE_PRODUCTION_WRITES_NOT_AUTHORIZED");

const reportDir = path.join(
  ROOT,
  "reports/gdi-commercial-data-model-cd-implementation-v1",
);
assert.ok(fs.existsSync(path.join(reportDir, "README.md")));

console.log("PASS test:gdi-commercial-data-model-cd-implementation-v1");
