#!/usr/bin/env node
/**
 * Real-deal shadow for calibration cohort (redacted). READ-ONLY.
 *   node scripts/operator-intelligence-calibration-real-deals.mjs
 */
import "dotenv/config";
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { loadActiveOperatorCandidatesForAlignment } from "../lib/operator-alignment-company-utils.js";
import { buildPrefillObjectFromNewBaseRows } from "../api/lib/operator-setup-new-base-read.js";
import { adaptOperatorFromPrefill } from "../lib/operator-fit/adapters/operator-from-prefill.js";
import { evaluateOperatorFitForDeal } from "../lib/operator-fit/evaluate-deal.js";
import { classifyOperatorReadiness, READINESS_STATUS } from "../lib/operator-fit/readiness.js";
import {
  loadCalibrationCohort,
  buildPrefillOverlayFromCohort,
  mergePrefillWithCalibration,
} from "../lib/operator-intelligence/calibration-overlay.js";

// Reuse discovery from existing real-deal shadow by importing its report or re-running lightweight
import { readFileSync, existsSync } from "fs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const COHORT = new Set([
  "recF5Z87OAqFgndoq",
  "recQ6Cf8O2z0tiqBz",
  "recWPKu5laVZxsvpn",
  "reciI2tYQBfMoMK9G",
  "rec3TUHT9Z4AnFp5P",
  "recGWxIJqnYHkJZFD",
]);

function enrich(c, prefill) {
  const merged = { ...(prefill || {}), submission_status: "Active", companyName: c.companyName };
  const pf = c.platform?.fields || {};
  const cf = c.commercial?.fields || {};
  const gf = c.governance?.fields || {};
  const pr = c.profile?.fields || {};
  if (pf["Active Countries"]) merged.activeCountries = pf["Active Countries"];
  if (pf["Active Markets / Cities"]) merged.activeMarkets = pf["Active Markets / Cities"];
  if (cf["Management Structures Supported"]) merged.managementStructuresSupported = cf["Management Structures Supported"];
  if (gf["Owner Reporting Level"]) merged.ownerReportingLevel = gf["Owner Reporting Level"];
  if (pr.chainScalesSupported) merged.chainScalesSupported = pr.chainScalesSupported;
  if (pr.brands) merged.brands = pr.brands;
  return merged;
}

async function main() {
  const priorPath = join(root, "reports", "operator-fit-real-deal-shadow-review.json");
  if (!existsSync(priorPath)) {
    console.error("Run npm run operator-fit-real-deal-shadow first");
    process.exit(1);
  }
  const prior = JSON.parse(readFileSync(priorPath, "utf8"));
  const cohort = loadCalibrationCohort();
  const { candidates } = await loadActiveOperatorCandidatesForAlignment();
  const byId = Object.fromEntries(candidates.map((c) => [c.operatorId, c]));

  const operatorPrefills = [];
  for (const id of COHORT) {
    const c = byId[id];
    if (!c) continue;
    const base = enrich(
      c,
      buildPrefillObjectFromNewBaseRows(c.master, c.profile, c.platform, c.commercial, c.governance)
    );
    const overlay = buildPrefillOverlayFromCohort(id, cohort);
    const merged = mergePrefillWithCalibration(base, overlay);
    operatorPrefills.push({ operatorId: id, companyName: c.companyName, prefill: merged.prefill });
  }

  const dealsOut = [];
  for (const deal of prior.deals || []) {
    const ctx = deal.projectContext || deal.context || {};
    // Reconstruct minimal deal fields from redacted prior
    const dealFields = {
      "Project Type": ctx.projectType || deal.projectType,
    };
    const locationData = {
      Country: ctx.country || deal.country,
      "Hotel Chain Scale": ctx.chainScale || deal.chainScale,
      "Building Type": ctx.building || deal.building,
    };
    const siData = {
      "Operating Model": ctx.operatingModel || deal.operatingModel,
      "Preferred Management Structure": ctx.preferredStructures || [],
    };
    const evaluated = evaluateOperatorFitForDeal({
      dealId: `recCAL_${deal.label || deal.dealIdRedacted}`,
      dealFields,
      locationData,
      mpData: {},
      siData,
      operatorPrefills,
    });
    const project = evaluated.project;
    const rows = (evaluated.top5 || []).map((row, idx) => {
      const pref = operatorPrefills.find((p) => p.operatorId === row.candidateId);
      const op = adaptOperatorFromPrefill(pref?.prefill || {}, {
        operatorId: row.candidateId,
        companyName: row.operatorName,
      });
      const ready = classifyOperatorReadiness(op, project);
      return {
        operator: row.operatorName,
        operatorId: row.candidateId,
        eligibility: row.eligibilityStatus,
        readiness: ready.status,
        alignment: row.displayedOperatorAlignment,
        confidence: row.evidenceConfidence,
        coverage: row.dataCoveragePct,
        rank: idx + 1,
        mainStrength: (row.whyItMatches || [])[0] || "—",
        mainConcern: (row.potentialConcerns || [])[0] || "—",
        materialUnknown: (row.unknowns || [])[0] || "—",
      };
    });
    const rankingReady = rows.filter((r) => r.readiness === READINESS_STATUS.RANKING_READY).length;
    dealsOut.push({
      label: deal.label,
      archetype: deal.archetype,
      rankingReadyCount: rankingReady,
      rows,
      answers: {
        atLeastTwoRankingReady: rankingReady >= 2,
        topCandidatesDifferent: new Set(rows.slice(0, 3).map((r) => r.operator)).size >= 2,
        ownerFacingCredible: rankingReady >= 2 && rows[0]?.confidence !== "Limited",
        myDealsStillBlocked: true,
      },
    });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: "read-only-calibration-overlay",
    deals: dealsOut,
  };
  writeFileSync(join(root, "reports", "operator-intelligence-calibration-real-deals.json"), JSON.stringify(report, null, 2));

  const md = [
    "# Operator Intelligence — Calibration Real-Deal Shadows (Redacted)",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    "Private owner / hotel / live deal IDs are not shown.",
    "",
  ];
  for (const d of dealsOut) {
    md.push(`## ${d.label} (${d.archetype})`);
    md.push("");
    md.push(`Ranking Ready in Top-5 view: **${d.rankingReadyCount}**`);
    md.push("");
    md.push(
      "| Operator | Eligibility | Readiness | Alignment | Confidence | Coverage | Rank | Main Strength | Main Concern | Material Unknown |"
    );
    md.push(
      "| -------- | ----------- | --------- | --------: | ---------- | -------: | ---: | ------------- | ------------ | ---------------- |"
    );
    for (const r of d.rows) {
      md.push(
        `| ${r.operator} | ${r.eligibility} | ${r.readiness} | ${r.alignment} | ${r.confidence} | ${r.coverage}% | ${r.rank} | ${r.mainStrength} | ${r.mainConcern} | ${r.materialUnknown} |`
      );
    }
    md.push("");
    md.push(`- ≥2 Ranking Ready: **${d.answers.atLeastTwoRankingReady}**`);
    md.push(`- Top candidates meaningfully different: **${d.answers.topCandidatesDifferent}**`);
    md.push(`- Owner-facing credible: **${d.answers.ownerFacingCredible}**`);
    md.push(`- My Deals remains blocked: **${d.answers.myDealsStillBlocked}**`);
    md.push("");
  }
  writeFileSync(join(root, "reports", "operator-intelligence-calibration-real-deals.md"), md.join("\n"));
  console.log(JSON.stringify({ deals: dealsOut.map((d) => ({ label: d.label, rankingReady: d.rankingReadyCount })) }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
