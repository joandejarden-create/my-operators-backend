#!/usr/bin/env node
/**
 * Apply residual 5-case taxonomy amendments (Joan Rules 1–5).
 * DEFER cases are not written. Holdout blocked.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  amendGoldenSetV2GroundTruth,
  AMENDMENT_ACTIONS,
} from "../lib/ai-visibility/validation/golden-set-ground-truth-amendment.js";
import { questionStatusFromRecommendationRole } from "../lib/ai-visibility/recommendation-classifier-v4_1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RES = path.join(
  __dirname,
  "../data/ai-visibility/validation/taxonomy-resolution-5-residual-cases.json"
);
const OUT = path.join(
  __dirname,
  "../data/ai-visibility/validation/taxonomy-resolution-5-residual-apply-result.json"
);

const applyFlag = process.argv.includes("--apply");
const doc = JSON.parse(fs.readFileSync(RES, "utf8"));
const results = [];

for (const c of doc.cases) {
  if (c.DECISION === "DEFER" || c.DECISION === "KEEP") {
    results.push({
      caseId: c.CASE_ID,
      action: c.DECISION,
      written: false,
      reason: c.REASON,
    });
    continue;
  }
  if (c.holdoutSplit === "holdout") {
    results.push({ caseId: c.CASE_ID, action: "BLOCKED_HOLDOUT", written: false });
    continue;
  }
  if (c.DECISION !== "AMEND") {
    results.push({ caseId: c.CASE_ID, action: c.DECISION, written: false });
    continue;
  }

  const role = c.FINAL_PROPOSED_ROLE;
  const qs = questionStatusFromRecommendationRole(role, true);
  const payload = {
    caseId: c.CASE_ID,
    action: AMENDMENT_ACTIONS.CORRECT_HUMAN_LABEL,
    reviewer:
      "Joan (authorized taxonomy Rules 1–5 via Residual GT Cleanup — Hybrid Prototype)",
    amendmentReason: `${c.REASON} | taxonomyRuleApplied=${(c.taxonomyRuleApplied || []).join(",")}`,
    amendedLabels: {
      expectedRecommendationRole: role,
      expectedRecommendationClass: role,
      expectedFirstRecommendation: role === "first_recommendation",
      expectedQuestionStatus: qs,
      expectedEntityPresent: true,
    },
  };

  if (!applyFlag) {
    results.push({
      caseId: c.CASE_ID,
      written: false,
      preview: { from: c.CURRENT_HUMAN_LABEL, to: role, qs },
    });
    continue;
  }

  const written = amendGoldenSetV2GroundTruth(payload, { apply: true });
  // Enrich last history with taxonomyRuleApplied
  results.push({
    caseId: c.CASE_ID,
    written: written.written === true,
    from: c.CURRENT_HUMAN_LABEL,
    to: role,
    qs,
    taxonomyRuleApplied: c.taxonomyRuleApplied,
  });
}

const out = {
  apply: applyFlag,
  TOTAL: doc.TOTAL,
  AMENDED: results.filter((r) => r.written).length,
  KEPT: results.filter((r) => r.action === "KEEP").length,
  DEFERRED: results.filter((r) => r.action === "DEFER").length,
  results,
  HOLDOUT_ACCESSED: false,
};
fs.writeFileSync(OUT, JSON.stringify(out, null, 2));
console.log(JSON.stringify(out, null, 2));
