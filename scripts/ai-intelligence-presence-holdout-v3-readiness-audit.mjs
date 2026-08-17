#!/usr/bin/env node
/**
 * Presence Holdout v3 readiness audit — READ ONLY.
 * Does not select, freeze, score, or mutate labels/resolver.
 *
 *   node scripts/ai-intelligence-presence-holdout-v3-readiness-audit.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { runPresenceHoldoutV3ReadinessAudit } from "../lib/ai-visibility/validation/presence-holdout-v3-readiness-audit.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-holdout-v3-readiness-audit.json"
);

const report = runPresenceHoldoutV3ReadinessAudit();
fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(report, null, 2) + "\n", "utf8");

const p = report.pool;
const f = report.feasibility;
const a = report.proposedAllocation;
const i = report.integrity;
const L = report.leakage;
const lab = report.labelIntegrity;
const prov = report.providers;
const lang = report.languages;
const geo = report.geographies;
const neg = report.negativeControls.categories;

function fmtDim(d) {
  if (!d) return "pairN=0 uniqueResponseN=0 presentN=0 notPresentN=0";
  return `pairN=${d.pairN} uniqueResponseN=${d.uniqueResponseN} presentN=${d.presentN} notPresentN=${d.notPresentN}`;
}

console.log("PRESENCE_HOLDOUT_V3_READINESS_AUDIT_COMPLETE");
console.log("");
console.log("## Pool");
console.log(`PAIR_N: ${p.PAIR_N}`);
console.log(`UNIQUE_RESPONSE_N: ${p.UNIQUE_RESPONSE_N}`);
console.log(`PRESENT: ${p.PRESENT}`);
console.log(`NOT_PRESENT: ${p.NOT_PRESENT}`);
console.log(`INVALID: ${p.INVALID}`);
console.log(`DEFERRED: ${p.DEFERRED}`);
console.log("");
console.log("## Providers");
console.log(`OPENAI: ${fmtDim(prov.OPENAI)}`);
console.log(`GEMINI: ${fmtDim(prov.GEMINI)}`);
console.log(`PERPLEXITY: ${fmtDim(prov.PERPLEXITY)}`);
console.log(`CLAUDE: ${fmtDim(prov.CLAUDE)}`);
console.log("");
console.log("## Languages");
console.log(`ENGLISH: ${fmtDim(lang.ENGLISH)}`);
console.log(`SPANISH: ${fmtDim(lang.SPANISH)}`);
console.log("");
console.log("## Geographies");
console.log(`GLOBAL: ${fmtDim(geo.GLOBAL)}`);
console.log(`CALA: ${fmtDim(geo.CALA)}`);
console.log(`MEXICO: ${fmtDim(geo.MEXICO)}`);
console.log(`EUROPE: ${fmtDim(geo.EUROPE)}`);
console.log(`NORTH_AMERICA: ${fmtDim(geo.NORTH_AMERICA)}`);
console.log("");
console.log("## Integrity");
console.log(`UNIQUE_CASE_IDS: ${i.UNIQUE_CASE_IDS}`);
console.log(`UNIQUE_ENTITY_RESPONSE_PAIRS: ${i.UNIQUE_ENTITY_RESPONSE_PAIRS}`);
console.log(
  `DUPLICATES: caseId=${i.CASE_ID_DUPLICATES} entityResponse=${i.ENTITY_RESPONSE_DUPLICATES}`
);
console.log(`RESPONSE_LEVEL_GOVERNANCE: ${i.RESPONSE_LEVEL_GOVERNANCE}`);
console.log(`MAX_PAIRS_PER_RESPONSE: ${i.MAX_PAIRS_PER_RESPONSE}`);
console.log(`LEAKAGE: cases=${L.LEAKAGE_CASES} responses=${L.LEAKAGE_RESPONSES}`);
console.log(
  `LABEL_INTEGRITY: ${lab.ok ? "OK" : "ISSUES"} missingLabels=${lab.missingHumanLabels} missingEntity=${lab.missingCanonicalEntity} missingResponse=${lab.missingSourceResponse} malformed=${lab.malformedResponse} ambiguity=${lab.identityAmbiguityUnresolved} invalid=${lab.invalid} deferred=${lab.deferred} dupActions=${lab.duplicateReviewActions}`
);
console.log("");
console.log("## Holdout v3 Feasibility");
console.log(`ELIGIBLE_PAIR_N: ${f.ELIGIBLE_PAIR_N}`);
console.log(`ELIGIBLE_UNIQUE_RESPONSE_N: ${f.ELIGIBLE_UNIQUE_RESPONSE_N}`);
console.log(`ELIGIBLE_PRESENT: ${f.ELIGIBLE_PRESENT}`);
console.log(`ELIGIBLE_NOT_PRESENT: ${f.ELIGIBLE_NOT_PRESENT}`);
console.log(`TARGET_60_40_FEASIBLE: ${f.TARGET_60_40_FEASIBLE}`);
console.log(`TARGET_UNIQUE_RESPONSE_80_FEASIBLE: ${f.TARGET_UNIQUE_RESPONSE_80_FEASIBLE}`);
console.log(`PROJECTED_UNIQUE_RESPONSE_N: ${f.PROJECTED_UNIQUE_RESPONSE_N}`);
console.log(`NEGATIVE_CONTROL_COVERAGE_SUFFICIENT: ${f.NEGATIVE_CONTROL_COVERAGE_SUFFICIENT}`);
console.log("");
console.log("## Negative controls (human-final NOT_PRESENT)");
for (const [k, v] of Object.entries(neg)) console.log(`${k}: ${v}`);
console.log("");
console.log("## Short-name / contextual");
console.log(JSON.stringify(report.shortNameContextual));
console.log("");
console.log("## Proposed Allocation");
console.log(`OPENAI: ${a.OPENAI}`);
console.log(`GEMINI: ${a.GEMINI}`);
console.log(`PERPLEXITY: ${a.PERPLEXITY}`);
console.log(`CLAUDE: ${a.CLAUDE}`);
console.log(`ENGLISH: ${a.ENGLISH}`);
console.log(`SPANISH: ${a.SPANISH}`);
console.log(`GLOBAL: ${a.GLOBAL}`);
console.log(`CALA: ${a.CALA}`);
console.log(`MEXICO: ${a.MEXICO}`);
console.log(`EUROPE: ${a.EUROPE}`);
console.log(`NORTH_AMERICA: ${a.NORTH_AMERICA}`);
console.log("");
console.log("## Gate");
console.log("PRECISION_THRESHOLD: 98%");
console.log("RECALL_THRESHOLD: 98%");
console.log("");
console.log("## Holdout v2");
console.log(`STATUS: ${report.holdoutV2.HOLDOUT_V2_STATUS}`);
console.log(`REUSED: ${report.holdoutV2.REUSED}`);
console.log("");
console.log("## Regionalization");
console.log(`STATUS: ${report.regionalization.STATUS}`);
console.log("");
console.log(`## Next Step\n${report.nextStep}`);
console.log("");
console.log(`Final status:\n${report.status}`);
console.log(`Wrote ${path.relative(ROOT, OUT)}`);
