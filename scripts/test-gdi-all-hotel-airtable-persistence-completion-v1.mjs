#!/usr/bin/env node
import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  RESEARCH_LIFECYCLE_LAYER,
  PROPOSED_GDI_HOTEL_RESEARCH_OPPORTUNITIES,
  COMPLETE_PERSISTENCE_CONTRACT,
  classifyOpportunityLifecycleLayer,
  planHotelCompletePersistence,
  assertNoBulkProductionApply,
  discoverLocalHotelUniverse,
  loadLocalOpportunities,
  loadYotelForbiddenRecreateIds,
} from "../lib/group-demand-intelligence/workflow/gdi-complete-research-persistence-v1.js";
import { YOTEL_LEGACY_DELETE_AUTH } from "../lib/group-demand-intelligence/workflow/gdi-yotel-legacy-gdo-deletion-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

// schema proposal present but not applied
assert.equal(
  PROPOSED_GDI_HOTEL_RESEARCH_OPPORTUNITIES.status,
  "REQUIRES_FOUNDER_SCHEMA_APPROVAL",
);
assert.equal(PROPOSED_GDI_HOTEL_RESEARCH_OPPORTUNITIES.proposedTableId, null);
assert.equal(COMPLETE_PERSISTENCE_CONTRACT.schemaChangeGuard.blocked, true);

// research-only classification (no GDO)
{
  const cls = classifyOpportunityLifecycleLayer(
    {
      opportunityId: "opp_research_1",
      organizationName: "Example Org",
      demandThesis: "WATCHLIST",
      verifiedNamedContact: false,
    },
    { hotelId: "recTEST" },
  );
  assert.equal(cls.layer, RESEARCH_LIFECYCLE_LAYER.RESEARCH_INVENTORY);
  assert.equal(cls.schemaReady, false);
  assert.equal(cls.commerciallyQualified, false);
  assert.ok(!cls.eligibility.allowedTables.includes("Group Demand Opportunities"));
}

// qualified GDO classification (research + GDO promotion)
{
  const cls = classifyOpportunityLifecycleLayer(
    {
      opportunityId: "opp_gdo_1",
      organizationName: "Example Org",
      demandThesis: "CONFIRMED_DEMAND",
      verifiedNamedContact: true,
      primaryContact: { name: "Ada Lovelace", role: "Meeting Planner" },
    },
    { hotelId: "recTEST" },
  );
  assert.equal(cls.layer, RESEARCH_LIFECYCLE_LAYER.RESEARCH_INVENTORY);
  assert.equal(cls.commerciallyQualified, true);
  assert.equal(cls.gdoLayer, RESEARCH_LIFECYCLE_LAYER.QUALIFIED_GDO);
  assert.equal(cls.schemaReady, false);
}

// YOTEL forbidden recreate
{
  const forbidden = new Set(["opp_legacy_1", "opp_legacy_2"]);
  const plan = planHotelCompletePersistence({
    hotel: { hotelId: YOTEL_LEGACY_DELETE_AUTH.hotelId, hotelName: "YOTEL" },
    filesystem: {
      count: 2,
      rows: [
        {
          opportunityId: "opp_legacy_1",
          organizationName: "X",
          demandThesis: "CONFIRMED_DEMAND",
          verifiedNamedContact: true,
          primaryContact: { name: "A", role: "B" },
        },
        {
          opportunityId: "opp_new_research",
          organizationName: "Y",
          demandThesis: "EXPLORATORY",
        },
      ],
      byId: new Map([
        ["opp_legacy_1", {}],
        ["opp_new_research", {}],
      ]),
    },
    airtableGdos: [],
    forbiddenRecreateIds: forbidden,
  });
  assert.equal(plan.counts.forbiddenBlocked, 1);
  assert.equal(plan.counts.researchInventoryPendingSchema, 1);
  assert.equal(plan.counts.qualifiedGdoCreate, 0);
}

// existing GDO preserved even without re-qualification thesis
{
  const plan = planHotelCompletePersistence({
    hotel: { hotelId: "recTEST", hotelName: "Test" },
    filesystem: {
      count: 1,
      rows: [
        {
          opportunityId: "opp_existing",
          organizationName: "Same Org",
          demandThesis: "SPECULATIVE_DEMAND",
        },
      ],
      byId: new Map([["opp_existing", {}]]),
    },
    airtableGdos: [
      {
        opportunityId: "opp_existing",
        organizationName: "Same Org",
        _airtableRecordId: "recExist",
      },
    ],
  });
  assert.equal(plan.counts.qualifiedGdoUnchanged, 1);
  assert.equal(plan.counts.researchInventoryPendingSchema, 1);
  assert.equal(plan.counts.qualifiedGdoCreate, 0);
}

// no bulk apply / schema create
assert.throws(
  () => assertNoBulkProductionApply({ apply: true }),
  (err) => err && err.code === "BULK_PRODUCTION_WRITES_NOT_AUTHORIZED",
);
assert.throws(
  () => assertNoBulkProductionApply({ createSchema: true }),
  (err) => err && err.code === "SCHEMA_CHANGES_REQUIRE_FOUNDER_APPROVAL",
);
assert.deepEqual(assertNoBulkProductionApply({}), { ok: true });

// universe discovery finds YOTEL / Radisson baselines
{
  const universe = discoverLocalHotelUniverse(ROOT);
  const ids = new Set(universe.map((h) => h.hotelId));
  assert.ok(ids.has(YOTEL_LEGACY_DELETE_AUTH.hotelId));
  assert.ok(ids.has("recH9h3McgMNFQEla"));
  assert.ok(ids.has("recLuxvwwxID7U2B8"));
  const yotel = loadLocalOpportunities(ROOT, YOTEL_LEGACY_DELETE_AUTH.hotelId);
  assert.equal(yotel.count, 42);
  const forbidden = loadYotelForbiddenRecreateIds(ROOT);
  assert.ok(forbidden.size >= 100, `expected ~108 forbidden ids, got ${forbidden.size}`);
}

// org-name conflict quarantined
{
  const plan = planHotelCompletePersistence({
    hotel: { hotelId: "recH9h3McgMNFQEla", hotelName: "Radisson" },
    filesystem: {
      count: 1,
      rows: [
        {
          opportunityId: "opp_conflict",
          organizationName: "Org A",
          demandThesis: "CONFIRMED_DEMAND",
          verifiedNamedContact: true,
          primaryContact: { name: "A", role: "Director" },
        },
      ],
      byId: new Map([["opp_conflict", {}]]),
    },
    airtableGdos: [
      {
        opportunityId: "opp_conflict",
        organizationName: "Org B Different",
        _airtableRecordId: "recXYZ",
      },
    ],
  });
  assert.equal(plan.counts.conflicts, 1);
  assert.equal(plan.counts.qualifiedGdoCreate, 0);
}

console.log("test-gdi-all-hotel-airtable-persistence-completion-v1: PASS");
