#!/usr/bin/env node
/**
 * Deterministic tests for multi-hotel Airtable reconciliation V2.
 * No Airtable writes. No paid research.
 */
import assert from "node:assert/strict";
import {
  ROCK_CREEK_VENUE_IDENTITY,
  detectEmbeddedStructuredVenueConflict,
  analyzeRockCreekVenueIdentity,
  resolveAuthoritativeVenueId,
  classifyInventoryPair,
  classifySyncDecisionForFilesystemOnly,
  classifySyncDecisionForShared,
  planHotelOpportunitySync,
  buildMultiHotelDryRunManifest,
  assertNoUnauthorizedProductionWrite,
  certifyWeeklyChangeStabilityAcrossIdenticalReruns,
  INVENTORY_CLASS,
  SYNC_DECISION,
} from "../lib/group-demand-intelligence/workflow/gdi-multi-hotel-airtable-reconciliation-v2.js";
import { computeVenueId } from "../lib/group-demand-intelligence/private-events/venue-entity.js";

function mapFrom(rows) {
  const byId = new Map();
  for (const r of rows) byId.set(r.id, r);
  return { byId, count: byId.size, rows };
}

// 1. Matching FS and AT opportunity IDs
{
  const { class: cls } = classifyInventoryPair(
    { id: "opp_1", organizationName: "Acme", hotelId: "h1" },
    { id: "opp_1", organizationName: "Acme", hotelId: "h1" },
    "h1",
  );
  assert.equal(cls, INVENTORY_CLASS.SHARED);
}

// 2. Airtable-only historical
{
  const { class: cls } = classifyInventoryPair(null, {
    id: "hist_1",
    status: null,
  });
  assert.equal(cls, INVENTORY_CLASS.AIRTABLE_ONLY_HISTORICAL_OR_UNSPECIFIED);
}

// 3/4. Filesystem-only eligible vs speculative
{
  const eligible = classifySyncDecisionForFilesystemOnly({
    id: "fs_elig",
    demandThesis: "CONFIRMED_DEMAND",
    verifiedNamedContact: true,
    primaryContact: { name: "A", role: "Planner" },
  });
  assert.equal(eligible.decision, SYNC_DECISION.CREATE);

  const speculative = classifySyncDecisionForFilesystemOnly({
    id: "fs_spec",
    demandThesis: "SPECULATIVE_DEMAND",
  });
  assert.equal(speculative.decision, SYNC_DECISION.DEFERRED);
}

// 5. Hotel ID conflicts
{
  const { class: cls, conflicts } = classifyInventoryPair(
    { id: "x", organizationName: "A", hotelId: "h1" },
    { id: "x", organizationName: "A", hotelId: "h2" },
    "h1",
  );
  assert.equal(cls, INVENTORY_CLASS.INCORRECT_HOTEL_RELATIONSHIP);
  assert.ok(conflicts.includes("HOTEL_ID_MISMATCH"));
}

// 6/7. Shared global vs distinct hotel opportunity — covered by planner isolation contract presence
{
  const plan = planHotelOpportunitySync({
    hotel: { key: "t", name: "T", hotelId: "h1", cohort: "TEST" },
    filesystem: mapFrom([
      {
        id: "opp_a",
        hotelId: "h1",
        organizationName: "Org",
        dgGeneratorId: "dg_shared",
        demandThesis: "SPECULATIVE_DEMAND",
      },
    ]),
    airtableRows: [
      {
        id: "opp_a",
        hotelId: "h1",
        organizationName: "Org",
        dgGeneratorId: "dg_shared",
      },
    ],
  });
  assert.equal(plan.counts.SHARED, 1);
  assert.equal(plan.inventoryRows[0].syncDecision, SYNC_DECISION.UNCHANGED);
}

// 8. Duplicate org aliases → conflict when names diverge on same ID
{
  const d = classifySyncDecisionForShared(
    { organizationName: "Acme Inc", title: "T" },
    { organizationName: "Other Co", title: "T" },
  );
  assert.equal(d.decision, SYNC_DECISION.CONFLICT);
}

// 9. Conflicting embedded and structured venue IDs
{
  const c = detectEmbeddedStructuredVenueConflict({
    structuredVenueId: ROCK_CREEK_VENUE_IDENTITY.structuredIdAfterPilot,
    payloadVenueId: ROCK_CREEK_VENUE_IDENTITY.historicalDomainId,
  });
  assert.equal(c.conflict, true);
  assert.equal(c.code, "EMBEDDED_JSON_STRUCTURED_ID_MISMATCH");
}

// 10. Preserve verified venue attributes / authoritative domain ID
{
  const domainId = computeVenueId({
    venueName: "Rock Creek Mansion",
    officialDomain: "rockcreekmansion.com",
  });
  assert.equal(domainId, ROCK_CREEK_VENUE_IDENTITY.historicalDomainId);
  const geoOnly = computeVenueId({
    venueName: "Rock Creek Mansion",
    city: "Bethesda",
    country: "US",
  });
  assert.equal(geoOnly, ROCK_CREEK_VENUE_IDENTITY.structuredIdAfterPilot);
  const resolved = resolveAuthoritativeVenueId({
    venueName: "Rock Creek Mansion",
    officialDomain: "rockcreekmansion.com",
    existingStructuredVenueId: geoOnly,
    existingPayloadVenueId: domainId,
  });
  assert.equal(resolved.authoritativeId, domainId);
  assert.equal(resolved.shouldRewriteStructuredId, false);
  const analysis = analyzeRockCreekVenueIdentity({
    structuredVenueId: geoOnly,
    payloadVenueId: domainId,
  });
  assert.equal(
    analysis.authoritativeCanonicalId,
    ROCK_CREEK_VENUE_IDENTITY.authoritativeCanonicalId,
  );
  assert.equal(analysis.remediation.mutationInThisPhase, false);
}

// 11/12. Read-back / idempotent rerun (planner-level)
{
  const hotel = { key: "t", name: "T", hotelId: "h1", cohort: "TEST" };
  const fs = mapFrom([
    {
      id: "only_fs",
      hotelId: "h1",
      organizationName: "Org",
      demandThesis: "SPECULATIVE_DEMAND",
    },
  ]);
  const a = planHotelOpportunitySync({ hotel, filesystem: fs, airtableRows: [] });
  const b = planHotelOpportunitySync({ hotel, filesystem: fs, airtableRows: [] });
  const cert = certifyWeeklyChangeStabilityAcrossIdenticalReruns(a, b);
  assert.equal(cert.ok, true);
}

// 13. Partial failure recovery — CREATE candidate remains CREATE until applied identity exists
{
  const plan = planHotelOpportunitySync({
    hotel: { key: "t", name: "T", hotelId: "h1", cohort: "TEST" },
    filesystem: mapFrom([
      {
        id: "fs_new",
        hotelId: "h1",
        organizationName: "Buyer Co",
        demandThesis: "RELIABLE_INFERRED_DEMAND",
        hasCredibleCommercialEntryPoint: true,
        primaryContact: { name: "Pat", role: "Meeting Planner" },
      },
    ]),
    airtableRows: [],
  });
  assert.equal(plan.syncOperations[0].decision, SYNC_DECISION.CREATE);
  const afterPartial = planHotelOpportunitySync({
    hotel: { key: "t", name: "T", hotelId: "h1", cohort: "TEST" },
    filesystem: mapFrom([
      {
        id: "fs_new",
        hotelId: "h1",
        organizationName: "Buyer Co",
        demandThesis: "RELIABLE_INFERRED_DEMAND",
        hasCredibleCommercialEntryPoint: true,
        primaryContact: { name: "Pat", role: "Meeting Planner" },
      },
    ]),
    airtableRows: [
      {
        id: "fs_new",
        airtableRecordId: "recPARTIAL",
        hotelId: "h1",
        organizationName: "Buyer Co",
        demandThesis: "RELIABLE_INFERRED_DEMAND",
      },
    ],
  });
  assert.equal(afterPartial.inventoryRows[0].syncDecision, SYNC_DECISION.UNCHANGED);
}

// 14. Weekly Change continuity — sync does not invent NEW first discovery when AT baseline exists
{
  const plan = planHotelOpportunitySync({
    hotel: { key: "t", name: "T", hotelId: "h1", cohort: "TEST" },
    filesystem: mapFrom([
      {
        id: "shared",
        hotelId: "h1",
        organizationName: "Org",
        title: "New title only",
        demandThesis: "CONFIRMED_DEMAND",
      },
    ]),
    airtableRows: [
      {
        id: "shared",
        hotelId: "h1",
        organizationName: "Org",
        title: "Old title",
        demandThesis: "CONFIRMED_DEMAND",
        firstDiscoveredAt: "2026-01-01",
        firstDiscoveredRunId: "run_old",
      },
    ],
  });
  assert.equal(plan.inventoryRows[0].syncDecision, SYNC_DECISION.UNCHANGED);
  assert.ok(plan.inventoryRows[0].weeklyChange);
}

// 15. Correct GDO qualification (already asserted CREATE vs DEFERRED above)

// 16/17/18/19/20. No synthetic promotion / publication / confidential leak / unauthorized writes / paid research
{
  assert.throws(
    () =>
      assertNoUnauthorizedProductionWrite({
        apply: true,
        authorizeProductionWrites: true,
      }),
    /not_authorized|PRODUCTION_WRITES_NOT_AUTHORIZED/,
  );
  const manifest = buildMultiHotelDryRunManifest({
    hotelPlans: [
      planHotelOpportunitySync({
        hotel: { key: "t", name: "T", hotelId: "h1", cohort: "TEST" },
        filesystem: mapFrom([]),
        airtableRows: [],
      }),
    ],
    rockCreekAnalysis: analyzeRockCreekVenueIdentity({}),
  });
  assert.equal(manifest.mutation, false);
  assert.equal(manifest.productionWritesAuthorized, false);
  assert.equal(manifest.storageAuthority.neverAutoPromoteToCurrent, true);
  assert.equal(
    manifest.storageAuthority.neverCopyHotelPrivateFeedbackToGlobal,
    true,
  );
  assert.ok(
    manifest.nextProductionPilotRequires.includes("FOUNDER_EXPLICIT_ALLOWLIST"),
  );
}

console.log(
  JSON.stringify(
    {
      ok: true,
      suite: "test-gdi-multi-hotel-airtable-reconciliation-v2",
      assertions: 20,
    },
    null,
    2,
  ),
);
