#!/usr/bin/env node
/**
 * Part B — apply 17 taxonomy amendments (Joan-authorized Rules 1–5).
 * Holdout blocked. Original labels preserved in amendment history.
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
const RES =
  path.join(__dirname, "../data/ai-visibility/validation/taxonomy-resolution-17-cases.json");
const OUT = path.join(
  __dirname,
  "../data/ai-visibility/validation/taxonomy-resolution-17-apply-result.json"
);

const applyFlag = process.argv.includes("--apply");
const doc = JSON.parse(fs.readFileSync(RES, "utf8"));
const results = [];

for (const c of doc.cases) {
  if (c.DECISION !== "AMEND") {
    results.push({ caseId: c.CASE_ID, action: c.DECISION, written: false });
    continue;
  }
  if (c.holdoutSplit === "holdout") {
    results.push({ caseId: c.CASE_ID, action: "BLOCKED_HOLDOUT", written: false });
    continue;
  }

  const role = c.FINAL_PROPOSED_ROLE;
  const qs = questionStatusFromRecommendationRole(role, true);
  const firstRec = role === "first_recommendation";

  const payload = {
    caseId: c.CASE_ID,
    action: AMENDMENT_ACTIONS.CORRECT_HUMAN_LABEL,
    reviewer: "Joan (authorized taxonomy Rules 1–5 via AI Intelligence Taxonomy Resolution)",
    amendmentReason: `${c.REASON} | taxonomyRuleApplied=${(c.taxonomyRuleApplied || []).join(",")}`,
    amendedLabels: {
      expectedRecommendationRole: role,
      expectedRecommendationClass: role,
      expectedFirstRecommendation: firstRec,
      expectedQuestionStatus: qs,
      expectedEntityPresent: true,
    },
  };

  const preview = amendGoldenSetV2GroundTruth(payload, { apply: false });
  if (!applyFlag) {
    results.push({
      caseId: c.CASE_ID,
      written: false,
      preview: {
        from: c.CURRENT_HUMAN_LABEL,
        to: role,
        qs,
        rules: c.taxonomyRuleApplied,
      },
    });
    continue;
  }

  const written = amendGoldenSetV2GroundTruth(payload, { apply: true });
  results.push({
    caseId: c.CASE_ID,
    written: written.written === true,
    from: c.CURRENT_HUMAN_LABEL,
    to: role,
    qs,
    rules: c.taxonomyRuleApplied,
    taxonomyRuleApplied: c.taxonomyRuleApplied,
  });
}

const out = {
  apply: applyFlag,
  TOTAL: doc.TOTAL,
  AMENDED: results.filter((r) => r.written).length,
  PREVIEW_ONLY: !applyFlag,
  results,
  HOLDOUT_ACCESSED: false,
};
fs.writeFileSync(OUT, JSON.stringify(out, null, 2));
console.log(JSON.stringify({ apply: applyFlag, AMENDED: out.AMENDED, TOTAL: out.TOTAL, out: OUT }, null, 2));
