#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  buildQualityScorecard,
  repairFailedFields,
  evaluateFieldQuality,
  QUALITY_GATE_IDS,
} from "../lib/brand-explorer/be-phase2e-field-quality-gates-v1.js";
import { RECONSTRUCTION_TARGETS } from "../lib/brand-explorer/be-phase2e-universal-reconstruction-v1.js";
import { buildProductionTemplateInventory } from "../lib/brand-explorer/be-phase2d-template-inventory-v1.js";

assert.ok(QUALITY_GATE_IDS.includes("actual_rendering"));

const target = RECONSTRUCTION_TARGETS.ascend;
const inventory = buildProductionTemplateInventory();
const evidencePack = {
  evidence: [
    { id: "identity.brand_name", claim: "Ascend Hotel Collection", confidence: "high" },
    { id: "identity.parent", claim: "Choice Hotels", confidence: "high" },
    { id: "basics.positioning", claim: "Ascend is Choice's pioneering soft brand for distinctive hotels.", confidence: "high" },
    { id: "loyalty.honors_value", claim: "Choice Privileges drives demand to Ascend hotels.", confidence: "high" },
    { id: "competitive.soft_brand_peers", claim: "Curio; Autograph; Tribute", confidence: "medium" },
  ],
};

const thin = evaluateFieldQuality(
  {
    slotKey: "overview.why_value",
    body: "Ascend Hotel Collection soft brand affiliation note for owners.",
    disposition: "APPLICABLE_POPULATED_POSITIVE",
    completeness: "populated",
    evidenceIds: ["basics.positioning"],
  },
  { target, evidencePack, renderCtx: { renderValidated: false } },
);
assert.equal(thin.gateResults.actual_rendering, "FAIL", "untested render must FAIL");
assert.ok(thin.failReasons.includes("rendering_required_but_untested"));

const goodBody =
  "Ascend Hotel Collection (Choice Hotels): pioneering soft-brand affiliation for distinctive independents. Owners retain character while accessing Choice Privileges and CRS — underwrite conversion PIP with comps; confirm fees in current disclosure.";
const good = evaluateFieldQuality(
  {
    slotKey: "overview.why_value",
    body: goodBody,
    disposition: "APPLICABLE_POPULATED_POSITIVE",
    completeness: "populated",
    evidenceIds: ["basics.positioning"],
  },
  { target, evidencePack, renderCtx: { renderValidated: true, renderedHtml: `<div>${goodBody}</div>` } },
);
assert.equal(good.qualityPass, true, good.notes.join("; "));

const candidate = {
  rows: inventory.applicableSlotKeys.slice(0, 5).map((slotKey, i) => ({
    slotKey,
    body: i === 0 ? "Ascend" : "",
    title: "",
    disposition: i === 0 ? "APPLICABLE_POPULATED_POSITIVE" : "RESEARCH_REQUIRED",
    completeness: i === 0 ? "populated" : "research_required",
    evidenceIds: i === 0 ? ["identity.brand_name"] : [],
  })),
};
// Use tiny inventory slice via fake inventory object
const tinyInv = { applicableSlotKeys: candidate.rows.map((r) => r.slotKey) };
const scorecard = buildQualityScorecard(candidate, {
  target,
  evidencePack,
  inventory: tinyInv,
  renderCtx: { renderValidated: true, renderedHtml: "<div>Ascend</div>" },
});
assert.ok(scorecard.metrics.A_populationCompleteness < 1);
assert.ok(scorecard.failureRegister.length >= 1);

const repaired = repairFailedFields(candidate, scorecard, { target, evidencePack });
assert.ok(repaired.repairedSlotKeys.length >= 1);
assert.ok(!/hilton honors/i.test(JSON.stringify(repaired.rows)));

console.log(JSON.stringify({ ok: true, untestedRenderFails: true, goodPass: good.qualityPass }, null, 2));
