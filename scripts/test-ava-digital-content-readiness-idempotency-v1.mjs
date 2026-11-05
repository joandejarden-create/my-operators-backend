/**
 *   node scripts/test-ava-digital-content-readiness-idempotency-v1.mjs
 */
import assert from "node:assert/strict";
import {
  appendDigitalContentReadinessActions,
  finalizeDigitalContentReadinessBlock,
  DCR_QUESTION_CLASSES,
} from "../lib/ai-visibility-audit-action-plan/digital-content-readiness-v1.js";

const dcr = finalizeDigitalContentReadinessBlock({
  propertyId: "adp_test",
  hotelName: "Test",
  executiveAssessment: "Test assessment",
  sourceInventory: [
    {
      platformName: "Hotel site",
      url: "https://example.test/",
      sourceCategory: "HOTEL_OPERATED_WEBSITE",
      evidenceUrl: "https://example.test/",
    },
    {
      platformName: "Hotel site",
      url: "https://example.test/",
      sourceCategory: "HOTEL_OPERATED_WEBSITE",
      evidenceUrl: "https://example.test/",
    },
  ],
  travelerQuestionCoverage: [
    {
      questionId: "q1",
      travelerIntent: "Meetings?",
      classification: DCR_QUESTION_CLASSES.PARTIALLY_ANSWERED,
    },
    { questionId: "q1", travelerIntent: "Meetings duplicate?" },
  ],
  actions: [
    {
      actionId: "AVA-TEST-DCR-001",
      headline: "Fix meetings facts",
      priority: "HIGH",
      whyItMatters: "Demand",
      accountableOwner: "Hotel",
      recommendedSteps: ["Confirm", "Update", "Verify"],
      evidenceLinks: [{ label: "Site", url: "https://example.test/" }],
    },
  ],
});

assert.equal(dcr.sourceInventory.length, 1);
assert.equal(dcr.travelerQuestionCoverage.length, 1);
assert.ok(dcr.actions[0].completionCriteria);
assert.ok(dcr.actions[0].technicalDetails);

let payload = {
  property: { propertyId: "adp_test", name: "Test" },
  actions: [{ actionId: "AVA-TEST-001", headline: "Core" }],
  actionOverview: [{ actionId: "AVA-TEST-001", headline: "Core" }],
};

payload = appendDigitalContentReadinessActions(payload, dcr);
payload = appendDigitalContentReadinessActions(payload, dcr);
payload = appendDigitalContentReadinessActions(payload, dcr);

assert.equal(
  payload.actions.filter((a) => a.actionId === "AVA-TEST-DCR-001").length,
  1
);
assert.equal(
  payload.actionOverview.filter((a) => a.actionId === "AVA-TEST-DCR-001").length,
  1
);

console.log(
  JSON.stringify(
    {
      ok: true,
      sources: dcr.sourceInventory.length,
      questions: dcr.travelerQuestionCoverage.length,
      actionsAfterTripleMerge: 1,
    },
    null,
    2
  )
);
