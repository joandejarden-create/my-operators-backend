#!/usr/bin/env node
/**
 * Apply INVALIDATE_CANDIDATE_SUBJECT to residual entity ground-truth audit cases.
 * Dry-run by default. --apply to write. Holdout cases blocked.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  amendGoldenSetV2GroundTruth,
  AMENDMENT_ACTIONS,
  readGoldenSetV2Fixture,
} from "../lib/ai-visibility/validation/golden-set-ground-truth-amendment.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const REASON =
  "Candidate subject was over-nominated. The stored response does not contain an unambiguous reference to the specific canonical entity. Generic/common-language or parent-company references are insufficient.";
const REVIEWER = "joan@dealality (founder — chat-authorized INVALIDATE_CANDIDATE_SUBJECT 2026-08-15)";

const CASE_IDS = [
  "v2_cand_1674948a",
  "v2_cand_17d64be9",
  "v2_cand_27ec7204",
  "v2_cand_2dd0fd2a",
  "v2_cand_3515312a",
  "v2_cand_465be86c",
  "v2_cand_4cf733c7",
  "v2_cand_52172a1d",
  "v2_cand_5ae0142a",
  "v2_cand_8d9c220b",
  "v2_cand_a07ee6c3",
  "v2_cand_adfdf95e",
];

const { doc } = readGoldenSetV2Fixture();
const byId = new Map((doc.cases || []).map((c) => [c.caseId, c]));

const dry = {
  CASES_REQUESTED: CASE_IDS.length,
  CASES_FOUND: 0,
  PLAYA_CASES: 0,
  IHG_CASES: 0,
  VALID_FOR_INVALIDATION: [],
  BLOCKED: [],
  HOLDOUT_CASES: [],
  ALREADY_INVALIDATED: [],
};

for (const id of CASE_IDS) {
  const c = byId.get(id);
  if (!c) {
    dry.BLOCKED.push({ caseId: id, reason: "CASE_NOT_FOUND" });
    continue;
  }
  dry.CASES_FOUND += 1;
  if (/^Playa/i.test(c.candidateEntity || "")) dry.PLAYA_CASES += 1;
  if (/IHG/i.test(c.candidateEntity || "")) dry.IHG_CASES += 1;
  if (c.holdoutSplit === "holdout") {
    dry.HOLDOUT_CASES.push(id);
    dry.BLOCKED.push({ caseId: id, reason: "HOLDOUT" });
    continue;
  }
  if (c.groundTruthInvalidated === true || c.reviewStatus === "INVALIDATED_CANDIDATE_SUBJECT") {
    dry.ALREADY_INVALIDATED.push(id);
    continue;
  }
  dry.VALID_FOR_INVALIDATION.push(id);
}

console.log(JSON.stringify({ mode: APPLY ? "APPLY" : "DRY_RUN", ...dry }, null, 2));

if (dry.HOLDOUT_CASES.length) {
  console.error("BLOCKED: holdout cases present — abort");
  process.exit(3);
}
if (dry.BLOCKED.length) {
  console.error("BLOCKED: unresolved cases — abort");
  process.exit(4);
}

if (!APPLY) {
  console.log("\nDry-run OK. Re-run with --apply to write amendments.");
  process.exit(0);
}

const results = { INVALIDATED: [], FAILED: [], HISTORY_PRESERVED: true };
for (const id of dry.VALID_FOR_INVALIDATION) {
  try {
    const r = amendGoldenSetV2GroundTruth(
      {
        caseId: id,
        action: AMENDMENT_ACTIONS.INVALIDATE_CANDIDATE_SUBJECT,
        reviewer: REVIEWER,
        amendmentReason: REASON,
      },
      { apply: true }
    );
    if (r.written) results.INVALIDATED.push(id);
    else results.FAILED.push({ caseId: id, reason: "not_written" });
  } catch (err) {
    results.FAILED.push({ caseId: id, reason: err.code || err.message });
    results.HISTORY_PRESERVED = results.HISTORY_PRESERVED && false;
  }
}

const after = readGoldenSetV2Fixture().doc;
const out = {
  ...results,
  INVALIDATED_COUNT: results.INVALIDATED.length,
  FAILED_COUNT: results.FAILED.length,
  GOLDEN_SET_V2_VERSION_UPDATED: after.groundTruthAmendmentVersion || null,
  lastGroundTruthAmendmentAt: after.lastGroundTruthAmendmentAt || null,
  AMENDMENT_HISTORY_PRESERVED: results.INVALIDATED.every((id) => {
    const c = (after.cases || []).find((x) => x.caseId === id);
    return Array.isArray(c?.labelAmendmentHistory) && c.labelAmendmentHistory.length >= 1;
  }),
};

const reportPath = path.join(
  ROOT,
  "data/ai-visibility/validation/human-review/residual-entity-ground-truth/invalidation-apply-result.json"
);
fs.writeFileSync(reportPath, JSON.stringify(out, null, 2), "utf8");
console.log(JSON.stringify(out, null, 2));
if (results.FAILED.length) process.exit(1);
