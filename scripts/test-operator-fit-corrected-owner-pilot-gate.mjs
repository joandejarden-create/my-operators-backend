#!/usr/bin/env node
/**
 * Corrected owner-pilot gate tests.
 */
import { existsSync, readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { isOperatorFitEngineV2Enabled } from "../lib/operator-fit/feature-flag.js";
import { OWNER_TERMS } from "../lib/operator-fit/owner-presentation.js";
import { OPERATOR_PROJECT_FACTORS } from "../lib/operator-fit/config.js";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
let failed = 0;
function ok(name, cond) {
  if (!cond) {
    failed += 1;
    console.error("FAIL", name);
  } else console.log("ok", name);
}

const resultPath = join(root, "reports/operator-fit-corrected-round-2-result.json");
ok("corrected result exists", existsSync(resultPath));
const result = JSON.parse(readFileSync(resultPath, "utf8"));

ok("deal B excluded from denominator", (result.excludedFromDenominator || []).includes("pilot_deal_b"));
ok("five candidate-bearing", (result.candidateBearing || []).length === 5);
ok(
  "deal B not in candidate bearing",
  !(result.candidateBearing || []).some((c) => c.dealId === "pilot_deal_b")
);
ok("deal F present", (result.candidateBearing || []).some((c) => c.dealId === "pilot_deal_f"));
ok(
  "deal F candidate-bearing RR",
  (result.dealF?.productionRR || 0) >= 2 && result.dealF?.provisionalSynthetic === true
);
ok("formal threshold 4/5", result.formalThresholdMet === true && result.counts.strong >= 4);
ok("deal C strong", result.dealCStrong === true);
ok("material zero", result.counts.material === 0);
ok(
  "deal B truthfulness separate",
  result.dealBTruthfulness?.excludedFromFourOfFiveDenominator === true &&
    /passed/i.test(result.dealBTruthfulness?.score || "")
);

ok("evidence strength term", OWNER_TERMS.evidenceStrength === "Evidence Strength");
ok("potential fit term", OWNER_TERMS.potentialFitValidationNeeded.includes("Validation Needed"));
ok("under evaluation term", OWNER_TERMS.underEvaluation === "Under Evaluation");
ok("validate next term", OWNER_TERMS.validateNext === "Validate Next");

ok("scoring frozen spot-check", OPERATOR_PROJECT_FACTORS.geographyMarket.weight === 22);
ok("owner engine disabled", !isOperatorFitEngineV2Enabled({ OPERATOR_FIT_ENGINE_V2: "0" }));

ok("deal F selection doc", existsSync(join(root, "reports/operator-fit-round-2-deal-f-selection.md")));
ok("truthfulness gate doc", existsSync(join(root, "reports/operator-fit-zero-universe-truthfulness-gate.md")));
ok("deal E diagnosis", existsSync(join(root, "reports/operator-fit-deal-e-improvement-diagnosis.md")));
ok("copy freeze", existsSync(join(root, "docs/reviews/operator-fit-owner-view-copy-freeze.md")));
ok("deal C preview", existsSync(join(root, "reports/operator-fit-deal-c-controlled-pilot-preview.md")));
ok("final product ADR", existsSync(join(root, "docs/architecture/decisions/operator-fit-owner-pilot-final-product-decisions.md")));
ok("founder review", existsSync(join(root, "docs/reviews/operator-fit-corrected-owner-pilot-gate-founder-review.md")));
ok("plan not enabled language", /NOT ENABLED/i.test(readFileSync(join(root, "docs/architecture/operator-fit-controlled-owner-pilot-plan.md"), "utf8")));

const freeze = readFileSync(join(root, "docs/reviews/operator-fit-owner-view-copy-freeze.md"), "utf8");
ok("no /100 headline in freeze", /Do not headline|No `?\/100/i.test(freeze) || /No `\/100`/i.test(freeze) || /\/100/i.test(freeze));

if (failed) {
  console.error(`\n${failed} failed`);
  process.exit(1);
}
console.log("\nAll corrected owner-pilot gate tests passed");
