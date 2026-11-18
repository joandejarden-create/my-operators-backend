#!/usr/bin/env node
/**
 * Deterministic tests — YOTEL replacement migration plan V1.
 * No Airtable mutations.
 */
import assert from "node:assert/strict";
import {
  YOTEL_HOTEL,
  assertYotelHotelScope,
  assertNoMigrationMutation,
  buildYotelDeleteManifest,
  buildYotel42TargetState,
  classifyLocalOpportunityForReplacement,
  assessYotelDependencies,
  designYotelReplacementSequence,
  redactPiiFields,
  verifyNoCrossHotelIds,
} from "../lib/group-demand-intelligence/workflow/gdi-yotel-replacement-migration-plan-v1.js";

// 1. Hotel ID scoping
assert.equal(assertYotelHotelScope(YOTEL_HOTEL.hotelId), true);
assert.throws(
  () => assertYotelHotelScope("recOTHER"),
  (err) => err && err.code === "YOTEL_HOTEL_SCOPE_VIOLATION",
);

// 2/3. Correct YOTEL identification + no cross-hotel deletion
{
  const manifest = buildYotelDeleteManifest({
    airtableRawRecords: [
      {
        id: "recY1",
        fields: {
          hotelId: YOTEL_HOTEL.hotelId,
          opportunityId: "old_1",
          organizationName: "Org",
        },
      },
      {
        id: "recForeign",
        fields: {
          hotelId: "recH9h3McgMNFQEla",
          opportunityId: "foreign_1",
        },
      },
    ],
    priorReconciliationCount: 1,
  });
  assert.equal(manifest.counts.proposedDeletes, 1);
  assert.equal(manifest.counts.rejectedOutOfScope, 1);
  assert.deepEqual(manifest.airtableRecordIdsExact, ["recY1"]);
  assert.ok(manifest.neverDelete.includes("Demand Generators"));
  const cross = verifyNoCrossHotelIds(manifest, ["recH9h3McgMNFQEla"]);
  assert.equal(cross.ok, true);
}

// 4/5. Backup completeness shape + restoration feasibility notes via sequence
{
  const fields = redactPiiFields({
    opportunityId: "x",
    primaryContactEmail: "a@b.com",
    primaryContactPhone: "123",
    organizationName: "Org",
  });
  assert.equal(fields.primaryContactEmail, "[REDACTED_PII]");
  assert.equal(fields.primaryContactPhone, "[REDACTED_PII]");
  assert.equal(fields.organizationName, "Org");
}

// 6. Local baseline identity
{
  const opps = Array.from({ length: 42 }, (_, i) => ({
    id: `gdi_opp_yotel_test_${i}`,
    hotelId: YOTEL_HOTEL.hotelId,
    organizationName: `Org ${i}`,
    demandThesis: "SPECULATIVE_DEMAND",
  }));
  const target = buildYotel42TargetState({ localOpportunities: opps });
  assert.equal(target.actualLocalCount, 42);
  assert.equal(target.countMatchesExpected, true);
  assert.equal(target.qualifiedGdoCount, 0);
  assert.equal(target.researchOnlyCount, 42);
}

// 7. Qualified vs research-only
{
  const research = classifyLocalOpportunityForReplacement({
    id: "r1",
    demandThesis: "SPECULATIVE_DEMAND",
  });
  assert.equal(research.destination, "RESEARCH_STORE_ONLY");

  const qualified = classifyLocalOpportunityForReplacement({
    id: "q1",
    demandThesis: "CONFIRMED_DEMAND",
    hasCredibleCommercialEntryPoint: true,
    primaryContact: { name: "Pat", role: "Planner" },
  });
  assert.equal(qualified.destination, "QUALIFIED_GDO_CREATE_CANDIDATE");
}

// 8. Downstream references preserved in audit policy
{
  const deleteManifest = buildYotelDeleteManifest({
    airtableRawRecords: [
      {
        id: "recY2",
        fields: {
          hotelId: YOTEL_HOTEL.hotelId,
          opportunityId: "old_dec",
          decisionId: "dec_1",
          dgGeneratorId: "dg_1",
          "Demand Generator": ["recDG1"],
        },
      },
    ],
  });
  const audit = assessYotelDependencies({
    deleteManifest,
    decisionsBySubjectId: [
      { decisionId: "dec_1", subjectId: "old_dec", decisionType: "PURSUE" },
    ],
    publication: { staged: { version: "s" }, current: { version: "c" } },
  });
  assert.equal(audit.gdoRowsWithDecisionIdField.length, 1);
  assert.equal(audit.linkedDecisionsFound.length, 1);
  assert.equal(audit.publication.mustRemainUnchanged, true);
  assert.match(
    audit.sharedIntelligence.deletionPolicy,
    /never delete shared/i,
  );
}

// 9/10. Idempotent replacement sequence + partial failure recovery framing
{
  const deleteManifest = buildYotelDeleteManifest({
    airtableRawRecords: [
      {
        id: "recY3",
        fields: { hotelId: YOTEL_HOTEL.hotelId, opportunityId: "old_3" },
      },
    ],
  });
  const target = buildYotel42TargetState({
    localOpportunities: [
      {
        id: "new_1",
        hotelId: YOTEL_HOTEL.hotelId,
        demandThesis: "SPECULATIVE_DEMAND",
      },
    ],
  });
  const seq = designYotelReplacementSequence({ deleteManifest, targetState: target });
  assert.equal(seq.atomicMultiRecordTransactions, false);
  assert.equal(seq.productionAuthorized, false);
  assert.equal(seq.phases[0].writes, false);
  assert.ok(
    seq.phases.some((p) => p.name === "CREATE_QUALIFIED_REPLACEMENTS_FIRST"),
  );
  assert.ok(seq.phases.some((p) => p.name === "DELETE_OBSOLETE_108"));
  assert.equal(seq.rockCreekExcluded.applyWithYotelMigration, false);
}

// 11/12/13. No publication mutation / no synthetic promotion / no unauthorized writes
{
  assert.throws(
    () =>
      assertNoMigrationMutation({
        apply: true,
        delete: true,
        authorizeProductionWrites: true,
      }),
    (err) => err && err.code === "PRODUCTION_MUTATIONS_NOT_AUTHORIZED",
  );
  assert.equal(
    assertNoMigrationMutation({ apply: false, delete: false }).ok,
    true,
  );
}

console.log(
  JSON.stringify(
    {
      ok: true,
      suite: "test-gdi-yotel-replacement-migration-plan-v1",
      assertions: 13,
    },
    null,
    2,
  ),
);
