#!/usr/bin/env node
/**
 * Permanent regression: Canonical Full E2E Standard Lock V1.
 * Asserts the 14 founder requirements at shared entry + persistence boundary.
 * Does NOT re-certify Bethesda live Airtable data.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_CANONICAL_FULL_E2E_STANDARD_LOCK_V1,
  CANONICAL_FULL_E2E_PERMANENT_REQUIREMENTS,
  assessCanonicalFullE2eStandardAdoption,
} from "../lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-standard-lock-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-canonical-full-e2e-standard-lock-v1",
);
fs.mkdirSync(OUT, { recursive: true });

assert.equal(
  GDI_CANONICAL_FULL_E2E_STANDARD_LOCK_V1,
  "gdi-canonical-full-e2e-standard-lock-v1",
);
assert.equal(CANONICAL_FULL_E2E_PERMANENT_REQUIREMENTS.length, 14);

const assessment = assessCanonicalFullE2eStandardAdoption({
  nextPhase: "PHASE_2_WYNDHAM_SAN_DIEGO_BAYSIDE",
});

fs.writeFileSync(
  path.join(OUT, "00-ADOPTION_ASSESSMENT.json"),
  `${JSON.stringify(assessment, null, 2)}\n`,
);

assert.equal(
  assessment.ok,
  true,
  `Canonical standard lock gaps: ${JSON.stringify(assessment.gaps, null, 2)}`,
);
assert.equal(assessment.enforcedCount, 14);
assert.equal(assessment.totalRequirements, 14);
assert.equal(
  assessment.bethesdaRecertification.startsWith("NOT_REQUIRED"),
  true,
);
assert.equal(
  assessment.productionExecutionControls.defaultProductionWriteSetPolicy,
  "VALIDATED_FQO_ONLY",
);
assert.equal(
  assessment.productionExecutionControls.requiresExactWriteSetAuthorization,
  true,
);

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: GDI_CANONICAL_FULL_E2E_STANDARD_LOCK_V1,
      enforcedCount: assessment.enforcedCount,
      nextPhase: assessment.nextPhase,
    },
    null,
    2,
  ),
);
console.log("OK test:gdi-canonical-full-e2e-standard-lock-v1");
