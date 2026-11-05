/**
 *   node scripts/test-ava-customer-quality-gate-cannot-skip-v1.mjs
 */
import assert from "node:assert/strict";
import { runMandatoryAvaEvidenceStages } from "../lib/ai-visibility-audit-action-plan/ava-full-e2e-pipeline-v1.js";

let threw = false;
try {
  await runMandatoryAvaEvidenceStages(
    { property: { propertyId: "x", name: "x" }, actions: [] },
    { skipCustomerQualityAssurance: true }
  );
} catch (err) {
  threw = true;
  assert.equal(err.code, "AVA_CUSTOMER_QUALITY_ASSURANCE_CANNOT_BE_SKIPPED");
}
assert.equal(threw, true);

threw = false;
try {
  await runMandatoryAvaEvidenceStages(
    { property: { propertyId: "x", name: "x" }, actions: [] },
    { skipEvidenceStages: true }
  );
} catch (err) {
  threw = true;
  assert.equal(err.code, "AVA_EVIDENCE_STAGES_CANNOT_BE_SKIPPED");
}
assert.equal(threw, true);

console.log(JSON.stringify({ ok: true, skipGuards: true }, null, 2));
