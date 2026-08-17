/**
 * Stage 2 acquisition classification unit tests.
 *
 *   node scripts/test-acquisition-intelligence-classify.mjs
 *   npm run test:acquisition-intelligence-classify
 */
import test from "node:test";
import assert from "node:assert/strict";
import { classifyAcquisitionRelationship, classificationFieldsEqual } from "../lib/acquisition-intelligence/classify-relationship.js";
import { MAP_ACQUISITION_RELATIONSHIP as R } from "../lib/acquisition-intelligence/field-map.js";
import { buildClassificationReviewSample } from "../lib/acquisition-intelligence/classify-batch.js";

function classify(position, company, extra = {}) {
  return classifyAcquisitionRelationship({ position, company, ...extra }).result;
}

test("1. hotel owner CIO → high direct prospect", () => {
  const r = classify("Chief Investment Officer", "Harbor Hotels Group");
  assert.equal(r.directProspectPotential, "High");
  assert.ok(["Direct Prospect", "Strategic Relationship"].includes(r.acquisitionRole));
  assert.notEqual(r.researchQueueEligibility, "No Research Yet");
});

test("2. family office principal → elevated direct / owner class", () => {
  const r = classify("Principal", "Andes Family Office");
  assert.ok(["High", "Medium"].includes(r.directProspectPotential));
  assert.equal(r.personCompanyClass, "Family Office");
});

test("3. hotel acquisition director → high direct", () => {
  const r = classify("Director of Acquisitions & Development", "Coastal Hospitality Investment");
  assert.equal(r.directProspectPotential, "High");
  assert.ok(r.scoreExplanation.includes("Acquisitions"));
});

test("4. Marriott brand development → not direct prospect; high connector/decision", () => {
  const r = classify("VP Development CALA", "Marriott International");
  assert.ok(["Low", "Unknown"].includes(r.directProspectPotential));
  assert.equal(r.connectorPotential, "High");
  assert.ok(["High", "Medium"].includes(r.decisionVisibility));
  assert.ok(["Decision-Signal Source", "Owner Connector"].includes(r.acquisitionRole));
  assert.equal(r.personCompanyClass, "Brand");
});

test("5. hospitality attorney → high connector", () => {
  const r = classify("Hospitality Attorney", "CAM Law Group");
  assert.equal(r.connectorPotential, "High");
  assert.equal(r.personCompanyClass, "Attorney");
});

test("6. hotel lender → high connector", () => {
  const r = classify("Managing Director, Hospitality Lending", "Pacific Capital Bank");
  assert.equal(r.connectorPotential, "High");
  assert.ok(["Lender", "Capital Provider"].includes(r.personCompanyClass));
});

test("7. architect → connector medium/high", () => {
  const r = classify("Principal Architect — Hotels", "Studio Form Arquitectos");
  assert.ok(["High", "Medium"].includes(r.connectorPotential));
  assert.equal(r.personCompanyClass, "Architect");
});

test("8. generic software salesperson → low relevance", () => {
  const r = classify("Account Executive", "HotelTech SaaS Inc");
  assert.equal(r.directProspectPotential, "Low");
  assert.ok(["Low Relevance", "Unclassified"].includes(r.acquisitionRole));
  assert.equal(r.researchQueueEligibility, "No Research Yet");
});

test("9. ambiguous Partner without company context → not auto-owner", () => {
  const r = classify("Partner", "");
  assert.notEqual(r.directProspectPotential, "High");
  assert.ok(r.scoreExplanation.toLowerCase().includes("ambiguous") || r.classificationConfidence === "Low");
});

test("10. hotel asset manager → asset manager class + connector", () => {
  const r = classify("SVP Asset Management", "Lodging Partners Portfolio");
  assert.equal(r.personCompanyClass, "Asset Manager");
  assert.ok(["High", "Medium"].includes(r.connectorPotential) || ["High", "Medium"].includes(r.directProspectPotential));
});

test("11. non-hospitality executive → low / no research", () => {
  const r = classify("VP Supply Chain", "Global Packaging Co");
  assert.equal(r.researchQueueEligibility, "No Research Yet");
  assert.ok(["Low", "Unknown"].includes(r.directProspectPotential));
});

test("12. missing company → confidence not high for owner claims", () => {
  const r = classify("Director of Acquisitions", "");
  assert.notEqual(r.classificationConfidence, "High");
});

test("13. missing title → unknown/low bands without inventing owner", () => {
  const r = classify("", "Some Company LLC");
  assert.notEqual(r.personCompanyClass, "Hotel Owner");
});

test("14. manual override preserved", () => {
  const out = classifyAcquisitionRelationship({
    position: "CIO",
    company: "Harbor Hotels Group",
    existingFields: { [R.classificationSource]: "Manual" },
  });
  assert.equal(out.skipped, true);
  assert.equal(out.reason, "manual_override");
  assert.equal(out.fields, null);
});

test("15. repeat classification idempotent field equality", () => {
  const a = classifyAcquisitionRelationship({
    position: "Hospitality Attorney",
    company: "CAM Law Group",
  });
  const b = classifyAcquisitionRelationship({
    position: "Hospitality Attorney",
    company: "CAM Law Group",
    existingFields: a.fields,
  });
  assert.equal(b.skipped, false);
  assert.equal(classificationFieldsEqual(a.fields, b.fields), true);
});

test("existing owner target exact match flagged", () => {
  const ownerIndexByKey = new Map([
    ["harbor hotels group", { id: "recOwner1", ownerName: "Harbor Hotels Group" }],
  ]);
  const r = classify("CIO", "Harbor Hotels Group", { ownerIndexByKey });
  assert.equal(r.existingOwnerTargetMatch, "Yes");
  assert.ok(r.scoreExplanation.includes("Owner Target"));
});

test("review sample builder returns top lists", () => {
  const rows = [
    {
      name: "A",
      position: "CIO",
      company: "X",
      acquisitionRole: "Direct Prospect",
      directProspectPotential: "High",
      connectorPotential: "Low",
      decisionVisibility: "Medium",
      calaRelevance: "Mexico",
      classificationConfidence: "High",
      researchQueueEligibility: "Research Priority",
      scoreExplanation: "CIO",
    },
    {
      name: "B",
      position: "Attorney",
      company: "Y",
      acquisitionRole: "Owner Connector",
      directProspectPotential: "Low",
      connectorPotential: "High",
      decisionVisibility: "High",
      calaRelevance: "Unknown",
      classificationConfidence: "Medium",
      researchQueueEligibility: "Research Priority",
      scoreExplanation: "Attorney",
    },
  ];
  const sample = buildClassificationReviewSample(rows);
  assert.equal(sample.topDirectProspects[0].name, "A");
  assert.equal(sample.topConnectors[0].name, "B");
  assert.ok(sample.dedupedReviewPool.length >= 2);
});

test("relationship strength never set by classifier", () => {
  const out = classifyAcquisitionRelationship({
    position: "CIO",
    company: "Harbor Hotels Group",
  });
  assert.equal(out.fields[R.relationshipStrength], undefined);
});
