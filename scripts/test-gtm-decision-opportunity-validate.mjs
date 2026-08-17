/**
 * Stage 1 Decision Opportunity validation + schema-spec tests.
 *
 *   node scripts/test-gtm-decision-opportunity-validate.mjs
 *   npm run test:gtm-decision-opportunity
 */
import test from "node:test";
import assert from "node:assert/strict";
import {
  MAP_DECISION_OPPORTUNITY,
  MAP_DECISION_OPPORTUNITY_EVIDENCE,
  VAL_DECISION_STATUS,
  VAL_DECISION_OPEN_CONFIDENCE,
  VAL_DECISION_BRAND_STATUS,
} from "../lib/gtm-owner-target/decision-opportunity-field-map.js";
import {
  validateDecisionOpportunityWrite,
  validateDecisionOpportunityEvidenceWrite,
  toDecisionOpportunityAirtableFields,
  toDecisionOpportunityEvidenceAirtableFields,
} from "../lib/gtm-owner-target/decision-opportunity-validate.js";
import {
  buildDecisionOpportunityCoreFields,
  buildDecisionOpportunityEvidenceFields,
  classifyFieldEnsureAction,
  getDecisionRadarSchemaSummary,
  diffSelectChoices,
} from "../lib/gtm-owner-target/decision-opportunity-schema-spec.js";

const M = MAP_DECISION_OPPORTUNITY;
const E = MAP_DECISION_OPPORTUNITY_EVIDENCE;

function sampleEvidence(overrides = {}) {
  return toDecisionOpportunityEvidenceAirtableFields({
    evidenceId: "ev-1",
    decisionOpportunity: ["recOpp1"],
    sourceName: "SEMARNAT Gaceta",
    sourceUrl: "https://example.com/filing.pdf",
    sourceType: "Government Filing",
    supportsField: "Project Exists",
    evidenceConfidence: "High",
    evidenceDirection: "Supports Open",
    ...overrides,
  });
}

test("valid discovered opportunity passes with minimal identity", () => {
  const fields = toDecisionOpportunityAirtableFields({
    opportunityName: "Oleum Riviera Maya — brand path",
    projectHotelName: "Oleum Eco-Tourism Hotel",
    country: "Mexico",
    status: "Discovered",
    visibility: "internal_only",
  });
  const result = validateDecisionOpportunityWrite(fields);
  assert.equal(result.ok, true, result.failures.join("; "));
  assert.equal(result.lifecycle.qualificationRequired, false);
});

test("valid founder-review opportunity with evidence passes", () => {
  const fields = toDecisionOpportunityAirtableFields({
    opportunityName: "Punta Colorada boutique hotel",
    projectHotelName: "Punta Colorada",
    country: "Mexico",
    cityMarket: "La Ribera / East Cape",
    trigger: "Planning / Environmental Approval",
    likelyDecisionType: "Brand Selection",
    decisionStage: "Early Signal",
    decisionStillOpen: "Uncertain",
    decisionOpenConfidence: "Probable",
    brandStatus: "Not Publicly Identified",
    whyNow:
      "Primary MIA accessed; no public brand; further qualification required before treating as open.",
    status: "Founder Review",
    visibility: "internal_only",
  });
  const result = validateDecisionOpportunityWrite(fields, {
    evidenceRecords: [sampleEvidence()],
  });
  assert.equal(result.ok, true, result.failures.join("; "));
  assert.equal(result.lifecycle.qualificationRequired, true);
});

test("founder-review without evidence is rejected", () => {
  const fields = toDecisionOpportunityAirtableFields({
    opportunityName: "Test Project",
    projectHotelName: "Test Hotel",
    country: "Mexico",
    trigger: "New Development",
    likelyDecisionType: "Brand Selection",
    whyNow: "Pipeline asset without disclosed brand.",
    decisionStillOpen: "Uncertain",
    decisionOpenConfidence: "Inferred",
    decisionStage: "Early Signal",
    status: "Founder Review",
  });
  const result = validateDecisionOpportunityWrite(fields, { evidenceRecords: [] });
  assert.equal(result.ok, false);
  assert.ok(
    result.failures.some((f) => /evidence/i.test(f)),
    result.failures.join("; ")
  );
});

test("qualified without evidence is rejected", () => {
  const fields = toDecisionOpportunityAirtableFields({
    opportunityName: "Qualified Gap",
    projectHotelName: "Hotel X",
    country: "Dominican Republic",
    trigger: "Independent / Unbranded",
    likelyDecisionType: "Conversion",
    whyNow: "Independent asset may evaluate brand.",
    decisionStillOpen: "Uncertain",
    decisionOpenConfidence: "Inferred",
    status: "Qualified",
  });
  const result = validateDecisionOpportunityWrite(fields);
  assert.equal(result.ok, false);
  assert.ok(result.failures.some((f) => /evidence/i.test(f)));
});

test("invalid enum is rejected", () => {
  const fields = toDecisionOpportunityAirtableFields({
    opportunityName: "Bad Enum",
    projectHotelName: "Hotel Y",
    status: "Discovered",
    trigger: "NOT_A_REAL_TRIGGER",
  });
  const result = validateDecisionOpportunityWrite(fields);
  assert.equal(result.ok, false);
  assert.ok(result.failures.some((f) => /Invalid Trigger/i.test(f)));
});

test("confidence semantics: Confirmed open + Not Publicly Identified fails", () => {
  const fields = toDecisionOpportunityAirtableFields({
    opportunityName: "False precision",
    projectHotelName: "Hotel Z",
    country: "Costa Rica",
    status: "Discovered",
    brandStatus: "Not Publicly Identified",
    decisionStillOpen: "Yes",
    decisionOpenConfidence: "Confirmed",
  });
  const result = validateDecisionOpportunityWrite(fields);
  assert.equal(result.ok, false);
  assert.ok(
    result.failures.some((f) => /cannot be Confirmed solely/i.test(f)),
    result.failures.join("; ")
  );
  assert.ok(VAL_DECISION_OPEN_CONFIDENCE.includes("Confirmed"));
  assert.ok(VAL_DECISION_BRAND_STATUS.includes("Not Publicly Identified"));
});

test("signed/exclusive conflict with open brand selection fails", () => {
  const fields = toDecisionOpportunityAirtableFields({
    opportunityName: "Too late Marriott",
    projectHotelName: "Hotel Signed",
    country: "Colombia",
    status: "Discovered",
    brandStatus: "Signed / Exclusive",
    exclusivityStatus: "Signed",
    likelyDecisionType: "Brand Selection",
    decisionStillOpen: "Yes",
    decisionOpenConfidence: "Probable",
  });
  const result = validateDecisionOpportunityWrite(fields);
  assert.equal(result.ok, false);
  assert.ok(
    result.failures.some((f) => /Contradictory decision state/i.test(f)),
    result.failures.join("; ")
  );
});

test("outreach ready contract encodes requirements without auto-transition", () => {
  const fields = toDecisionOpportunityAirtableFields({
    opportunityName: "Outreach candidate",
    projectHotelName: "Hotel Reach",
    country: "Guatemala",
    status: "Outreach Ready",
    trigger: "New Development",
    likelyDecisionType: "Brand Selection",
    whyNow: "Pre-brand development.",
    decisionStillOpen: "Uncertain",
    decisionOpenConfidence: "Probable",
    // missing owner, makers, founderReviewed, evidence
  });
  const result = validateDecisionOpportunityWrite(fields, { evidenceRecords: [] });
  assert.equal(result.ok, false);
  assert.equal(result.lifecycle.outreachReadyContract, true);
  assert.ok(result.failures.some((f) => /Owner Target/i.test(f)));
  assert.ok(result.failures.some((f) => /Founder Reviewed/i.test(f)));
  assert.ok(result.failures.some((f) => /evidence/i.test(f)));
});

test("evidence validator accepts too-late direction", () => {
  const fields = sampleEvidence({
    supportsField: "Decision Still Open",
    evidenceDirection: "Supports Closed / Too Late",
    evidenceExcerpt: "Brand announced exclusive management agreement.",
  });
  const result = validateDecisionOpportunityEvidenceWrite(fields);
  assert.equal(result.ok, true, result.failures.join("; "));
});

test("evidence invalid enum rejected", () => {
  const fields = sampleEvidence({ evidenceConfidence: "Ultra" });
  const result = validateDecisionOpportunityEvidenceWrite(fields);
  assert.equal(result.ok, false);
  assert.ok(result.failures.some((f) => /Evidence Confidence/i.test(f)));
});

test("schema ensure classification is idempotent for compatible fields", () => {
  const core = buildDecisionOpportunityCoreFields();
  const statusField = core.find((f) => f.name === M.status);
  assert.ok(statusField);
  const existing = {
    name: M.status,
    type: "singleSelect",
    options: { choices: VAL_DECISION_STATUS.map((name) => ({ name })) },
  };
  const action = classifyFieldEnsureAction(existing, statusField);
  assert.equal(action.action, "skip");

  const again = classifyFieldEnsureAction(existing, statusField);
  assert.equal(again.action, "skip");
});

test("schema ensure detects type conflict safely", () => {
  const desired = buildDecisionOpportunityCoreFields().find(
    (f) => f.name === M.whyNow
  );
  const existing = { name: M.whyNow, type: "singleLineText" };
  const action = classifyFieldEnsureAction(existing, desired);
  assert.equal(action.action, "conflict");
});

test("schema ensure detects missing select choices", () => {
  const desired = buildDecisionOpportunityCoreFields().find(
    (f) => f.name === M.status
  );
  const existing = {
    name: M.status,
    type: "singleSelect",
    options: { choices: [{ name: "Discovered" }, { name: "Researching" }] },
  };
  const action = classifyFieldEnsureAction(existing, desired);
  assert.equal(action.action, "add_choices");
  assert.ok(action.missingChoices.includes("Founder Review"));
  const diff = diffSelectChoices(existing, VAL_DECISION_STATUS);
  assert.ok(diff.missing.length > 0);
});

test("schema summary exposes table contract", () => {
  const summary = getDecisionRadarSchemaSummary();
  assert.equal(summary.opportunitiesTable, "Decision Opportunities");
  assert.equal(summary.evidenceTable, "Decision Opportunity Evidence");
  assert.ok(summary.opportunityCoreFieldCount > 20);
  assert.ok(buildDecisionOpportunityEvidenceFields({}).length >= 10);
  assert.ok(Object.values(MAP_DECISION_OPPORTUNITY_EVIDENCE).includes("Supports Field"));
});
