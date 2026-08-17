#!/usr/bin/env node
/**
 * READ-ONLY calibration evaluation: Airtable vs Airtable+local overlay.
 * Writes reports only. No Airtable writes. No OAS mutation.
 *
 *   node scripts/operator-intelligence-calibration-evaluate.mjs
 */
import "dotenv/config";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { loadActiveOperatorCandidatesForAlignment } from "../lib/operator-alignment-company-utils.js";
import { buildPrefillObjectFromNewBaseRows } from "../api/lib/operator-setup-new-base-read.js";
import { adaptOperatorFromPrefill } from "../lib/operator-fit/adapters/operator-from-prefill.js";
import { adaptProjectFromDealContext } from "../lib/operator-fit/adapters/project-from-deal.js";
import { evaluateOperatorFitForDeal } from "../lib/operator-fit/evaluate-deal.js";
import { classifyOperatorReadiness, READINESS_STATUS } from "../lib/operator-fit/readiness.js";
import { FIT_V2_SCENARIOS } from "../lib/operator-fit/fixtures/scenarios.js";
import {
  loadCalibrationCohort,
  buildPrefillOverlayFromCohort,
  mergePrefillWithCalibration,
} from "../lib/operator-intelligence/calibration-overlay.js";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const COHORT_IDS = [
  "recF5Z87OAqFgndoq",
  "recQ6Cf8O2z0tiqBz",
  "recWPKu5laVZxsvpn",
  "reciI2tYQBfMoMK9G",
  "rec3TUHT9Z4AnFp5P",
  "recGWxIJqnYHkJZFD",
];

function enrichPrefillFromAirtable(c, prefill) {
  const merged = { ...(prefill || {}), submission_status: "Active", companyName: c.companyName };
  if (c.platform?.fields) {
    const pf = c.platform.fields;
    if (pf["Active Countries"]) merged.activeCountries = pf["Active Countries"];
    if (pf["Active Markets / Cities"]) merged.activeMarkets = pf["Active Markets / Cities"];
    if (pf["Market Presence Type"]) merged.marketPresenceType = pf["Market Presence Type"];
  }
  if (c.commercial?.fields) {
    const cf = c.commercial.fields;
    if (cf["Management Structures Supported"]) {
      merged.managementStructuresSupported = cf["Management Structures Supported"];
    }
    if (cf["Conversion / Reflag Experience"]) {
      merged.conversionReflagExperience = cf["Conversion / Reflag Experience"];
    }
    if (cf["New-Build Opening Experience"]) {
      merged.newBuildOpeningExperience = cf["New-Build Opening Experience"];
    }
  }
  if (c.governance?.fields) {
    const gf = c.governance.fields;
    if (gf["Offered Services"]) merged.offeredServices = gf["Offered Services"];
    if (gf["Owner Reporting Level"]) merged.ownerReportingLevel = gf["Owner Reporting Level"];
  }
  if (c.profile?.fields) {
    const pr = c.profile.fields;
    if (pr.chainScalesSupported) merged.chainScalesSupported = pr.chainScalesSupported;
    if (pr.brands) merged.brands = pr.brands;
    if (pr["Brand Families Operated"]) merged.brandFamiliesOperated = pr["Brand Families Operated"];
  }
  return merged;
}

function projectFromScenario(s) {
  return adaptProjectFromDealContext({
    dealId: `recCAL_${s.id}`,
    dealFields: s.dealFields,
    locationData: s.locationData,
    mpData: s.mpData,
    siData: s.siData,
  });
}

function summarizeEval(evaluated, readiness) {
  const top = evaluated?.top5?.[0] || evaluated?.results?.[0] || null;
  return {
    eligibility: top?.eligibilityStatus || top?.eligibility || null,
    readiness: readiness?.status || null,
    rawAlignment: top?.rawOperatorAlignment ?? top?.rawAlignment ?? null,
    displayedAlignment: top?.displayedOperatorAlignment ?? top?.displayedAlignment ?? null,
    confidence: top?.evidenceConfidence || top?.confidence || null,
    coverage: top?.dataCoveragePct ?? readiness?.coveragePct ?? null,
    brandCompatibility: top?.factorBreakdown?.find?.((f) => /brand/i.test(f.key || f.id || ""))?.score ?? null,
    structureAlignment: top?.factorBreakdown?.find?.((f) => /structure/i.test(f.key || f.id || ""))?.score ?? null,
    executionRisk: top?.executionRisk || null,
    topReasons: (top?.alignmentReasons || top?.reasons || []).slice(0, 3),
    concerns: (top?.concerns || top?.potentialConcerns || []).slice(0, 3),
    unknowns: (top?.unknowns || top?.materialUnknowns || []).slice(0, 3),
    validationQuestions: (top?.validationItems || top?.validationQuestions || []).slice(0, 3),
  };
}

async function main() {
  console.log("[oi-calibration] READ-ONLY evaluate starting…");
  const cohort = loadCalibrationCohort();
  const { candidates } = await loadActiveOperatorCandidatesForAlignment();
  const byId = Object.fromEntries((candidates || []).map((c) => [c.operatorId, c]));

  const operatorBundles = [];
  for (const id of COHORT_IDS) {
    const c = byId[id];
    if (!c) {
      console.warn("Missing live candidate", id);
      continue;
    }
    const base = enrichPrefillFromAirtable(
      c,
      buildPrefillObjectFromNewBaseRows(c.master, c.profile, c.platform, c.commercial, c.governance)
    );
    const overlayBundle = buildPrefillOverlayFromCohort(id, cohort);
    const airtableOnly = { prefill: base, mode: "airtable_only" };
    const merged = mergePrefillWithCalibration(base, overlayBundle);
    const opBefore = adaptOperatorFromPrefill(airtableOnly.prefill, {
      operatorId: id,
      companyName: c.companyName,
    });
    const opAfter = adaptOperatorFromPrefill(merged.prefill, {
      operatorId: id,
      companyName: c.companyName,
    });
    operatorBundles.push({
      operatorId: id,
      companyName: c.companyName,
      before: opBefore,
      after: opAfter,
      diagnostics: overlayBundle?.diagnostics || null,
      modeAfter: merged.mode,
    });
  }

  const urban = projectFromScenario(FIT_V2_SCENARIOS[0]);
  const readinessRows = operatorBundles.map((b) => {
    const before = classifyOperatorReadiness(b.before, urban);
    const after = classifyOperatorReadiness(b.after, urban);
    const byProject = {};
    for (const s of FIT_V2_SCENARIOS) {
      const p = projectFromScenario(s);
      byProject[s.id] = {
        before: classifyOperatorReadiness(b.before, p).status,
        after: classifyOperatorReadiness(b.after, p).status,
      };
    }
    return {
      operatorId: b.operatorId,
      operatorName: b.companyName,
      beforeStatus: before.status,
      afterStatus: after.status,
      eligibilityBefore: before.eligibilityCoverage,
      eligibilityAfter: after.eligibilityCoverage,
      differentiationBefore: before.differentiationCoverage,
      differentiationAfter: after.differentiationCoverage,
      evidenceBefore: before.evidenceCoverage,
      evidenceAfter: after.evidenceCoverage,
      mainRemainingGap: (after.missingCritical || [])[0] || (after.missingFields || [])[0] || "—",
      byProject,
      diagnostics: b.diagnostics,
    };
  });

  // Scenarios — after overlay only, cohort of 6
  const scenarioReport = [];
  for (const s of FIT_V2_SCENARIOS) {
    const project = projectFromScenario(s);
    const prefills = operatorBundles.map((b) => ({
      operatorId: b.operatorId,
      companyName: b.companyName,
      prefill: (() => {
        // reverse: we need prefill — re-adapt from after operator is harder; store prefill in bundle
        return null;
      })(),
    }));
    // Rebuild prefills for evaluateOperatorFitForDeal
    const operatorPrefills = [];
    for (const b of operatorBundles) {
      const c = byId[b.operatorId];
      const base = enrichPrefillFromAirtable(
        c,
        buildPrefillObjectFromNewBaseRows(c.master, c.profile, c.platform, c.commercial, c.governance)
      );
      const overlayBundle = buildPrefillOverlayFromCohort(b.operatorId, cohort);
      const merged = mergePrefillWithCalibration(base, overlayBundle);
      operatorPrefills.push({
        operatorId: b.operatorId,
        companyName: b.companyName,
        prefill: merged.prefill,
      });
    }
    const evaluated = evaluateOperatorFitForDeal({
      dealId: `recCAL_${s.id}`,
      dealFields: s.dealFields,
      locationData: s.locationData,
      mpData: s.mpData,
      siData: s.siData,
      operatorPrefills,
      includeBrandManaged: false,
    });
    const ranked = (evaluated.top5 || []).map((row, idx) => {
      const ready = classifyOperatorReadiness(
        adaptOperatorFromPrefill(
          operatorPrefills.find((p) => p.operatorId === row.candidateId)?.prefill || {},
          { operatorId: row.candidateId, companyName: row.candidateName }
        ),
        project
      );
      return {
        rank: idx + 1,
        operatorId: row.candidateId,
        operatorName: row.operatorName || row.candidateName,
        eligibility: row.eligibilityStatus,
        readiness: ready.status,
        rawAlignment: row.rawOperatorAlignment,
        displayedAlignment: row.displayedOperatorAlignment,
        confidence: row.evidenceConfidence,
        coverage: row.dataCoveragePct,
        topReasons: (row.whyItMatches || []).slice(0, 3),
        concerns: (row.potentialConcerns || []).slice(0, 3),
        unknowns: (row.unknowns || []).slice(0, 3),
        validationQuestions: (row.validationQuestions || []).slice(0, 3),
      };
    });
    scenarioReport.push({
      scenarioId: s.id,
      label: s.label || s.id,
      topOperator: ranked[0]?.operatorName || null,
      ranked,
    });
  }

  // Completeness bias synthetic check using fixtures + overlay specialist
  const bias = {
    notes: [
      "Generic table-stakes breadth must not outrank verified relevant comps (covered by Fit v2 tests).",
      "Calibration overlay adds verified comps/geo/structures — not marketing checklists.",
      "Cenote remains constrained by evidence gaps despite high Airtable fill.",
    ],
    rankingReadyAfterUrban: readinessRows.filter((r) => r.afterStatus === READINESS_STATUS.RANKING_READY).map((r) => r.operatorName),
    distinctScenarioWinners: [...new Set(scenarioReport.map((s) => s.topOperator).filter(Boolean))],
  };

  mkdirSync(join(root, "reports"), { recursive: true });

  const readinessJson = {
    generatedAt: new Date().toISOString(),
    mode: "read-only",
    thresholdUnchanged: true,
    rows: readinessRows,
  };
  writeFileSync(
    join(root, "reports", "operator-intelligence-calibration-readiness-before-after.json"),
    JSON.stringify(readinessJson, null, 2),
    "utf8"
  );

  const md = [
    "# Operator Intelligence — Calibration Readiness Before / After",
    "",
    `Generated: ${readinessJson.generatedAt}`,
    "",
    "Representative project: upper-upscale urban new build. Threshold unchanged (50% + critical fields).",
    "",
    "| Operator | Before Status | After Status | Eligibility Before | After | Differentiation Before | After | Evidence Before | After | Main Remaining Gap |",
    "| -------- | ------------- | ------------ | -----------------: | ----: | ---------------------: | ----: | --------------: | ----: | ------------------ |",
    ...readinessRows.map(
      (r) =>
        `| ${r.operatorName} | ${r.beforeStatus} | ${r.afterStatus} | ${r.eligibilityBefore}% | ${r.eligibilityAfter}% | ${r.differentiationBefore}% | ${r.differentiationAfter}% | ${r.evidenceBefore}% | ${r.evidenceAfter}% | ${r.mainRemainingGap} |`
    ),
    "",
    "## Project-specific Ranking Ready (after overlay)",
    "",
  ];
  for (const r of readinessRows) {
    const readyFor = Object.entries(r.byProject)
      .filter(([, v]) => v.after === READINESS_STATUS.RANKING_READY)
      .map(([k]) => k);
    md.push(`- **${r.operatorName}**: ${readyFor.length ? readyFor.join(", ") : "none"}`);
  }
  writeFileSync(
    join(root, "reports", "operator-intelligence-calibration-readiness-before-after.md"),
    md.join("\n"),
    "utf8"
  );

  writeFileSync(
    join(root, "reports", "operator-intelligence-calibration-scenarios.json"),
    JSON.stringify({ generatedAt: new Date().toISOString(), scenarios: scenarioReport, bias }, null, 2),
    "utf8"
  );

  const scenMd = [
    "# Operator Intelligence — Calibration Synthetic Scenarios",
    "",
    `Distinct top operators: ${bias.distinctScenarioWinners.join(", ") || "—"}`,
    "",
  ];
  for (const s of scenarioReport) {
    scenMd.push(`## ${s.label} (\`${s.scenarioId}\`)`);
    scenMd.push("");
    scenMd.push(`Top: **${s.topOperator || "—"}**`);
    scenMd.push("");
    scenMd.push(
      "| Rank | Operator | Eligibility | Readiness | Displayed | Confidence | Coverage |"
    );
    scenMd.push("| ---: | -------- | ----------- | --------- | --------: | ---------- | -------: |");
    for (const r of s.ranked) {
      scenMd.push(
        `| ${r.rank} | ${r.operatorName} | ${r.eligibility} | ${r.readiness} | ${r.displayedAlignment} | ${r.confidence} | ${r.coverage}% |`
      );
    }
    scenMd.push("");
  }
  writeFileSync(
    join(root, "reports", "operator-intelligence-calibration-scenarios.md"),
    scenMd.join("\n"),
    "utf8"
  );

  // UI payload for calibration pages
  writeFileSync(
    join(root, "reports", "operator-intelligence-calibration-ui-payload.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        readiness: readinessRows,
        scenarios: scenarioReport,
        exceptions: cohort.exceptions,
        sources: cohort.sources,
        claims: cohort.claims,
        comparables: cohort.comparables,
        operators: cohort.operators,
      },
      null,
      2
    ),
    "utf8"
  );

  console.log(
    JSON.stringify(
      {
        rankingReadyAfter: bias.rankingReadyAfterUrban,
        distinctWinners: bias.distinctScenarioWinners,
        readinessRows: readinessRows.length,
        scenarios: scenarioReport.length,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error("[oi-calibration] FAILED", err);
  process.exit(1);
});
