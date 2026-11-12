#!/usr/bin/env node
/**
 * Hotel Validation / Action / Outcome — section-safe persistence + timestamps.
 * Uses filesystem Decision Outcomes sandbox (no production Airtable writes).
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const require = createRequire(import.meta.url);

const tmpRoot = fs.mkdtempSync(
  path.join(os.tmpdir(), "gdi-hotel-feedback-persist-")
);
process.env.DECISION_OUTCOMES_PERSISTENCE = "filesystem";
process.env.DECISION_OUTCOMES_ROOT = path.join(tmpRoot, "decision-outcomes");
process.env.GDI_DATA_ROOT = path.join(tmpRoot, "gdi-data");

const {
  saveShareValidationItem,
  getShareValidationForOpportunity,
  restoreShareValidationItemAfterFailedPersist,
  loadShareValidation,
} = await import("../lib/group-demand-intelligence/share-validation.js");
const {
  hotelFeedbackToWeeklyLearningItem,
  formatHotelFeedbackLastSavedLabel,
  hotelValidationFingerprint,
} = await import(
  "../lib/group-demand-intelligence/hotel-feedback-subsection-persistence-v1.js"
);
const { feedbackToResearchTask } = await import(
  "../lib/group-demand-intelligence/workflow/weekly-refresh-contract-v1.js"
);
const {
  ensureGdiOpportunityDecision,
  recordAction,
  recordOutcome,
  ingestGdiShareValidation,
  projectCommercialProgression,
  loadEvents,
  getSubjectDecision,
} = await import("../lib/decision-outcomes/index.js");

const hotelId = `recGdiFeedbackSandbox_${Date.now().toString(36)}`;
const opportunityId = `gdi_opp_feedback_sandbox_${Date.now().toString(36)}`;
const opportunity = {
  id: opportunityId,
  title: "Sandbox Feedback Opportunity",
  priority: "HIGH_PRIORITY",
  recommendedAction: "Contact sandbox organizer",
};

function loadUi() {
  const code = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
    "utf8"
  );
  const sandbox = { window: {}, globalThis: {} };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  // eslint-disable-next-line no-new-func
  const fn = new Function("window", "globalThis", code + "\n;return window.DealalityGdiUi;");
  return fn(sandbox, sandbox);
}

// --- UI last-saved + section markup ---
const UI = loadUi();
assert.equal(
  UI.formatHotelFeedbackLastSavedLabel(null),
  "Not yet submitted"
);
assert.ok(
  UI.formatHotelFeedbackLastSavedLabel("2026-11-12T14:18:00.000Z").startsWith(
    "Last saved:"
  )
);
const html = UI.customerFeedbackLifecycleHtml(
  {
    id: opportunityId,
    shareValidation: {
      familiarityStatus: "ALREADY_KNOWN",
      commercialValue: "WORTH_WATCHING",
      validatedAt: "2026-11-12T14:18:00.000Z",
      hotelValidationUpdatedAt: "2026-11-12T14:18:00.000Z",
    },
  },
  UI.CUSTOMER_VALIDATION_ENUMS,
  { canValidate: true, canRecordAction: true, canRecordOutcome: true },
  {
    latestActionType: "CONTACTED",
    salesActionUpdatedAt: "2026-11-12T13:42:00.000Z",
    latestOutcomeType: null,
    salesOutcomeUpdatedAt: null,
  }
);
assert.ok(html.includes("gdiSvLastSaved"));
assert.ok(html.includes("gdiDoActionLastSaved"));
assert.ok(html.includes("gdiDoOutcomeLastSaved"));
assert.ok(html.includes('value="CONTACTED" selected'));
assert.ok(html.includes("Not yet submitted")); // outcome

// --- Hotel validation local persist + history + identical re-submit ---
const first = saveShareValidationItem(hotelId, {
  opportunityId,
  validator: "TEST_REVIEWER",
  familiarityStatus: "ALREADY_KNOWN",
  commercialValue: "WORTH_PURSUING_NOW",
  contactPersonAssessment: "RIGHT_PERSON",
  emailAssessment: "USEFUL",
  phoneAssessment: "DIRECT_USABLE",
  note: "Sandbox note",
});
assert.equal(first.created, true);
assert.ok(first.hotelValidationUpdatedAt);
const ts1 = first.hotelValidationUpdatedAt;

const same = saveShareValidationItem(hotelId, {
  opportunityId,
  validator: "TEST_REVIEWER",
  familiarityStatus: "ALREADY_KNOWN",
  commercialValue: "WORTH_PURSUING_NOW",
  contactPersonAssessment: "RIGHT_PERSON",
  emailAssessment: "USEFUL",
  phoneAssessment: "DIRECT_USABLE",
  note: "Sandbox note",
});
assert.equal(same.unchanged, true);
assert.equal(same.hotelValidationUpdatedAt, ts1);

const second = saveShareValidationItem(hotelId, {
  opportunityId,
  validator: "TEST_REVIEWER",
  familiarityStatus: "ALREADY_KNOWN",
  commercialValue: "WORTH_WATCHING",
  contactPersonAssessment: "WRONG_PERSON",
  emailAssessment: "WRONG",
  phoneAssessment: "NOT_TESTED",
  note: "Updated note",
});
assert.equal(second.created, true);
assert.notEqual(second.hotelValidationUpdatedAt, ts1);
const doc = loadShareValidation(hotelId);
assert.ok((doc.submissionHistory || []).length >= 2);
assert.equal(
  doc.submissionHistory.at(-1).previousValues.commercialValue,
  "WORTH_PURSUING_NOW"
);

// concurrency conflict
assert.throws(
  () =>
    saveShareValidationItem(
      hotelId,
      {
        opportunityId,
        familiarityStatus: "UNSURE",
        commercialValue: "WORTH_WATCHING",
      },
      { expectedHotelValidationUpdatedAt: ts1 }
    ),
  (err) => err.code === "hotel_validation_conflict"
);

// rollback helper restores prior
const beforeFail = getShareValidationForOpportunity(hotelId, opportunityId);
const mid = saveShareValidationItem(hotelId, {
  opportunityId,
  familiarityStatus: "NOT_RELEVANT",
  commercialValue: "NOT_WORTH_PURSUING",
  note: "should roll back",
});
assert.equal(mid.item.familiarityStatus, "NOT_RELEVANT");
restoreShareValidationItemAfterFailedPersist(hotelId, opportunityId, beforeFail);
const restored = getShareValidationForOpportunity(hotelId, opportunityId);
assert.equal(restored.familiarityStatus, beforeFail.familiarityStatus);
assert.equal(restored.validatedAt, beforeFail.validatedAt);

// --- Decision Outcomes: section isolation ---
const ensured = await ensureGdiOpportunityDecision({ hotelId, opportunity });
assert.ok(ensured.decision?.decisionId);
const decisionId = ensured.decision.decisionId;

await ingestGdiShareValidation({
  hotelId,
  opportunityId,
  opportunity,
  validation: restored,
  actor: { userId: "TEST_REVIEWER", role: "OTHER" },
});

const action1 = await recordAction(hotelId, decisionId, {
  actionType: "CONTACTED",
  userId: "TEST_REVIEWER",
});
assert.equal(action1.created, true);
assert.ok(action1.salesActionUpdatedAt);
const actionTs = action1.salesActionUpdatedAt;

const actionSame = await recordAction(hotelId, decisionId, {
  actionType: "CONTACTED",
  userId: "TEST_REVIEWER",
});
assert.equal(actionSame.unchanged, true);
assert.equal(actionSame.salesActionUpdatedAt, actionTs);

const eventsAfterAction = await loadEvents(hotelId, decisionId);
const progressionAfterAction = projectCommercialProgression(
  ensured.decision,
  eventsAfterAction
);
assert.equal(progressionAfterAction.salesActionUpdatedAt, actionTs);
const validationTsBeforeOutcome =
  progressionAfterAction.hotelValidationUpdatedAt;

const outcome1 = await recordOutcome(hotelId, decisionId, {
  outcomeType: "NO_RESPONSE",
  userId: "TEST_REVIEWER",
});
assert.equal(outcome1.created, true);
assert.ok(outcome1.salesOutcomeUpdatedAt);

const eventsFinal = await loadEvents(hotelId, decisionId);
const progressionFinal = projectCommercialProgression(
  ensured.decision,
  eventsFinal
);
assert.equal(progressionFinal.salesActionUpdatedAt, actionTs);
assert.ok(progressionFinal.salesOutcomeUpdatedAt);
assert.equal(
  progressionFinal.hotelValidationUpdatedAt,
  validationTsBeforeOutcome
);

// action conflict
await assert.rejects(
  () =>
    recordAction(hotelId, decisionId, {
      actionType: "FOLLOW_UP_REQUIRED",
      expectedSalesActionUpdatedAt: "2000-01-01T00:00:00.000Z",
    }),
  (err) => err.code === "sales_action_conflict"
);

// --- GDI learning adapter ---
const learning = hotelFeedbackToWeeklyLearningItem({
  hotelId,
  opportunityId,
  validation: restored,
  actionType: "RFP_RECEIVED",
  outcomeType: null,
});
const taskKnown = feedbackToResearchTask(
  hotelFeedbackToWeeklyLearningItem({
    hotelId,
    opportunityId,
    validation: { familiarityStatus: "ALREADY_KNOWN" },
  })
);
assert.equal(taskKnown.suppressNewDiscoveryLabel, true);
assert.equal(taskKnown.hotelSpecific, true);

const taskWrong = feedbackToResearchTask(
  hotelFeedbackToWeeklyLearningItem({
    hotelId,
    opportunityId,
    validation: {
      contactPersonAssessment: "WRONG_PERSON",
      emailAssessment: "WRONG",
    },
  })
);
assert.equal(taskWrong.queueKind, "contact_enrichment");

const taskPursue = feedbackToResearchTask(
  hotelFeedbackToWeeklyLearningItem({
    hotelId,
    opportunityId,
    validation: { commercialValue: "WORTH_PURSUING_NOW" },
  })
);
assert.equal(taskPursue.priority, "HIGH");

const taskRfp = feedbackToResearchTask(
  hotelFeedbackToWeeklyLearningItem({
    hotelId,
    opportunityId,
    validation: { familiarityStatus: "ACTIVELY_PURSUING" },
    actionType: "RFP_RECEIVED",
  })
);
assert.ok(taskRfp);
assert.equal(taskRfp.preservePursuitState, true);
assert.ok(learning.action === "RFP_RECEIVED" || learning.familiarityStatus);

assert.equal(
  hotelValidationFingerprint(restored),
  hotelValidationFingerprint(getShareValidationForOpportunity(hotelId, opportunityId))
);

const subject = await getSubjectDecision(hotelId, {
  productModule: "GDI",
  subjectId: opportunityId,
});
assert.ok(subject?.decision);

console.log(
  JSON.stringify(
    {
      ok: true,
      hotelId,
      opportunityId,
      decisionId,
      hotelValidationUpdatedAt: restored.validatedAt,
      salesActionUpdatedAt: progressionFinal.salesActionUpdatedAt,
      salesOutcomeUpdatedAt: progressionFinal.salesOutcomeUpdatedAt,
      submissionHistoryCount: (loadShareValidation(hotelId).submissionHistory || [])
        .length,
      eventCount: eventsFinal.length,
      labelSample: formatHotelFeedbackLastSavedLabel(
        progressionFinal.salesActionUpdatedAt
      ),
    },
    null,
    2
  )
);
// Cleanup sandbox decision-outcome files written under data/
try {
  const outcomesHotel = path.join(
    ROOT,
    "data",
    "decision-outcomes",
    "hotels",
    hotelId
  );
  fs.rmSync(outcomesHotel, { recursive: true, force: true });
  const gdiHotel = path.join(ROOT, "data", "group-demand-intelligence", "hotels", hotelId);
  // GDI_DATA_ROOT override may place share-validation under tmp — also try default
  fs.rmSync(gdiHotel, { recursive: true, force: true });
} catch {
  /* best-effort */
}

console.log("PASS gdi hotel feedback airtable persistence v1");
