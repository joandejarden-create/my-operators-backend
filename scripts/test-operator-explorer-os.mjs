/**
 * Unit gate: Operator Explorer OS state machine + factory queue next-operator signal.
 * No Airtable writes.
 */
import assert from "node:assert/strict";
import {
  OPERATOR_EXPLORER_OS_STATES,
  resolveOperatorExplorerOsState,
} from "../lib/partner-intelligence/operator-explorer-os.js";
import {
  OPERATOR_FACTORY_QUEUE,
  getOperatorFactoryQueueEntry,
  listOperatorFactoryQueue,
} from "../lib/partner-intelligence/operator-explorer-factory-queue.js";
import {
  FACTORY_INIT_FIXTURE_STEMS,
  runOperatorExplorerFactoryInit,
} from "../lib/partner-intelligence/operator-explorer-factory-init.js";
import {
  AIRTABLE_APPLY_REQUIRED_FLAGS,
  buildOverlayAirtablePreview,
  runOperatorExplorerOverlayAirtableApply,
} from "../lib/partner-intelligence/operator-explorer-overlay-airtable-apply.js";

function main() {
  assert.ok(OPERATOR_EXPLORER_OS_STATES.includes("founder_review_ready"));
  assert.ok(OPERATOR_EXPLORER_OS_STATES.includes("active_profile_ready"));

  const notStarted = resolveOperatorExplorerOsState({ hasMasterRecord: false });
  assert.equal(notStarted.state, "not_started");

  const scaffold = resolveOperatorExplorerOsState({
    hasMasterRecord: true,
    hasFixturePack: false,
    provenancePass: false,
  });
  assert.equal(scaffold.state, "factory_scaffolded");

  const seeded = resolveOperatorExplorerOsState({
    hasMasterRecord: true,
    hasFixturePack: false,
    provenancePass: true,
  });
  assert.equal(seeded.state, "sources_seeded");

  const founder = resolveOperatorExplorerOsState({
    hasMasterRecord: true,
    hasFixturePack: true,
    provenancePass: true,
    fieldAuditPass: true,
    sectionPatternPass: true,
    tabFactoryPass: true,
    founderReviewPassed: false,
  });
  assert.equal(founder.state, "founder_review_ready");

  const ghl = getOperatorFactoryQueueEntry("ghl-hoteles");
  assert.ok(ghl);
  assert.equal(ghl.recordId, "reciI2tYQBfMoMK9G");
  assert.equal(ghl.domain, "ghlhoteles.com");
  assert.equal(listOperatorFactoryQueue({ status: "queued" })[0].slug, "tafer-hotels-resorts");
  assert.ok(getOperatorFactoryQueueEntry("ghl-hoteles"));
  assert.ok(getOperatorFactoryQueueEntry("aimbridge-latam"));
  assert.ok(getOperatorFactoryQueueEntry("tafer-hotels-resorts"));
  assert.ok(getOperatorFactoryQueueEntry("highgate"));
  assert.ok(OPERATOR_FACTORY_QUEUE.length >= 9);

  const initDry = runOperatorExplorerFactoryInit({
    operators: ["ghl-hoteles"],
    apply: false,
  });
  assert.equal(initDry.dryRun, true);
  assert.equal(initDry.results[0].domain, "ghlhoteles.com");
  assert.ok(FACTORY_INIT_FIXTURE_STEMS.length >= 8);

  assert.ok(AIRTABLE_APPLY_REQUIRED_FLAGS.includes("confirmAirtableWrite"));
  const preview = buildOverlayAirtablePreview(
    { prefillOverlay: { companyHistory: "x" }, _meta: { intentionalSuppress: {} } },
    ghl
  );
  assert.equal(preview.validation.pass, true);
  assert.equal(preview.exactFieldMapping[0].prefillKey, "companyHistory");

  const applyGate = runOperatorExplorerOverlayAirtableApply({
    operators: ["ghl-hoteles"],
    apply: false,
  });
  assert.equal(applyGate.airtableWrites, false);

  let blocked = false;
  try {
    runOperatorExplorerOverlayAirtableApply({
      operators: ["ghl-hoteles"],
      apply: true,
      approveOverlayAirtableApply: true,
      confirmAirtableWrite: true,
      confirmNoCompanyValidationChanges: true,
      confirmNoSourceLibraryStatusChanges: true,
      confirmNoRegistryApprovalChanges: true,
      confirmFixtureOverlayReviewed: true,
    });
  } catch (err) {
    blocked = /not enabled in v1/i.test(err?.message || "");
  }
  assert.equal(blocked, true, "live Airtable apply must hard-stop in v1");

  console.log(
    JSON.stringify(
      {
        ok: true,
        nextQueuedOperator: listOperatorFactoryQueue({ status: "queued" })[0]?.slug,
        ghl: ghl.slug,
        aimbridge: getOperatorFactoryQueueEntry("aimbridge-latam")?.recordId,
        tafer: getOperatorFactoryQueueEntry("tafer-hotels-resorts")?.recordId,
      },
      null,
      2
    )
  );
}

try {
  main();
} catch (err) {
  console.error("[test:operator-explorer-os]", err?.message || err);
  process.exit(1);
}
