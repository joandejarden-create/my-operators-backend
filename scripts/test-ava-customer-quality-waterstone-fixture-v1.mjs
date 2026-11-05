/**
 * Waterstone defective fixture must FAIL CQA; remediated payload must PASS.
 *   node scripts/test-ava-customer-quality-waterstone-fixture-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { assessCustomerQuality } from "../lib/ai-visibility-audit-action-plan/customer-quality-assurance-v1.js";
import { runCustomerQualityAssuranceLoop } from "../lib/ai-visibility-audit-action-plan/customer-quality-remediation-v1.js";
import { CQA_DEFECT_TYPES } from "../lib/ai-visibility-audit-action-plan/customer-quality-assurance-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const FIXTURE = path.join(
  ROOT,
  "fixtures/ava-customer-quality/waterstone-defective-customer-quality-v1.json"
);

const defective = JSON.parse(fs.readFileSync(FIXTURE, "utf8"));
const before = assessCustomerQuality(defective);

assert.equal(
  before.overallPass,
  false,
  "Defective Waterstone fixture must fail overall CQA"
);

const requiredTypes = [
  CQA_DEFECT_TYPES.CRITICAL_FACTUAL_CONTRADICTION,
  CQA_DEFECT_TYPES.ACTION_COUNT_MISMATCH,
  CQA_DEFECT_TYPES.INVALID_EVIDENCE_CLASSIFICATION,
  CQA_DEFECT_TYPES.INTERNAL_LANGUAGE_LEAK,
];
const foundTypes = new Set(before.defects.map((d) => d.type));
for (const t of requiredTypes) {
  assert.ok(
    foundTypes.has(t),
    `Expected defect type ${t} on defective Waterstone fixture`
  );
}

// Specific Waterstone regressions
assert.ok(
  before.defects.some((d) =>
    /both.*verified|Hilton.*VERIFIED|brand.*VERIFIED/i.test(
      `${d.message} ${d.originalText || ""}`
    )
  ),
  "Must detect Hilton verified contradiction"
);
assert.ok(
  before.defects.some((d) => /3 actions|says 3/i.test(d.message)),
  "Must detect action count mismatch (3 vs 4+)"
);
assert.ok(
  before.defects.some((d) => /first-party|external mention/i.test(d.message)),
  "Must detect hotel site classified as external mention"
);

const loop = await runCustomerQualityAssuranceLoop(defective, {
  limits: { maxIterations: 3, maxPdfRegenerations: 0 },
});

assert.ok(loop.totalRepairs > 0, "Remediation must perform repairs");
assert.equal(
  loop.customerQualityCertified,
  true,
  `Remediated Waterstone must certify; remaining: ${JSON.stringify(
    loop.unresolvedMaterialIssues?.slice(0, 5),
    null,
    2
  )}`
);
assert.equal(loop.founderApprovedForCustomer, false);
assert.equal(loop.publicationAuthorized, false);

const after = assessCustomerQuality(loop.payload);
assert.equal(after.gates.researchQuality.pass, true);
assert.equal(after.gates.evidenceQuality.pass, true);
assert.equal(after.gates.communicationQuality.pass, true);
assert.equal(after.gates.pdfPresentationQuality.pass, true);

// Generalized: Hilton evidence no longer VERIFIED_PUBLIC
const hiltonVerified = JSON.stringify(loop.payload).match(
  /Hilton[^"]{0,80}VERIFIED_PUBLIC/i
);
assert.equal(
  hiltonVerified,
  null,
  "Hilton evidence must not remain VERIFIED_PUBLIC after remediation"
);

console.log(
  JSON.stringify(
    {
      ok: true,
      beforeDefects: before.defectCount,
      afterDefects: after.defectCount,
      repairs: loop.totalRepairs,
      certified: loop.customerQualityCertified,
    },
    null,
    2
  )
);
