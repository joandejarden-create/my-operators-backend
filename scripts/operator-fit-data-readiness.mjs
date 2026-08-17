#!/usr/bin/env node
/**
 * READ-ONLY — Operator Fit data readiness classification + enrichment queue.
 *
 *   node scripts/operator-fit-data-readiness.mjs
 *   node scripts/operator-fit-enrichment-queue.mjs   (alias wrapper)
 *
 * Loads Active Operator Setup candidates (read-only), classifies readiness,
 * writes JSON/MD/CSV reports. NO Airtable writes.
 */
import "dotenv/config";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { loadActiveOperatorCandidatesForAlignment } from "../lib/operator-alignment-company-utils.js";
import { buildPrefillObjectFromNewBaseRows } from "../api/lib/operator-setup-new-base-read.js";
import { adaptOperatorFromPrefill } from "../lib/operator-fit/adapters/operator-from-prefill.js";
import { adaptProjectFromDealContext } from "../lib/operator-fit/adapters/project-from-deal.js";
import {
  classifyOperatorReadiness,
  buildEnrichmentQueueRow,
  validateOperatorTaxonomy,
  validateOperatorEvidence,
  ENRICHMENT_FIELD_CATALOG,
  READINESS_STATUS,
  PRODUCTION_COVERAGE_THRESHOLD_PCT,
} from "../lib/operator-fit/readiness.js";
import { FIT_V2_SCENARIOS } from "../lib/operator-fit/fixtures/scenarios.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

function csvEscape(v) {
  const s = String(v ?? "");
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

async function loadOperators() {
  const { candidates, airtableConfigured } = await loadActiveOperatorCandidatesForAlignment();
  const rows = [];
  for (const c of candidates || []) {
    const prefill = buildPrefillObjectFromNewBaseRows(
      c.master,
      c.profile,
      c.platform,
      c.commercial,
      c.governance
    );
    const merged = {
      ...(prefill || {}),
      submission_status: "Active",
      companyName: c.companyName,
      dataConfidenceLevel:
        (c.master?.fields && c.master.fields["Data Confidence Level"]) ||
        prefill?.dataConfidenceLevel,
      sourceType: (c.master?.fields && c.master.fields["Source Type"]) || prefill?.sourceType,
    };
    // Map Airtable-ish keys into prefill aliases used by adapter
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
      if (cf["Pre-Opening Support Capability"]) {
        merged.preOpeningSupportCapability = cf["Pre-Opening Support Capability"];
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
      if (pr["Service Models Supported"]) merged.serviceModelsSupported = pr["Service Models Supported"];
    }

    const op = adaptOperatorFromPrefill(merged, {
      operatorId: c.operatorId,
      companyName: c.companyName,
    });
    rows.push({ candidate: c, prefill: merged, operator: op });
  }
  return { airtableConfigured, rows };
}

function fieldCompleteness(rows) {
  const out = {};
  for (const f of ENRICHMENT_FIELD_CATALOG) {
    let present = 0;
    for (const r of rows) {
      const ready = classifyOperatorReadiness(r.operator);
      if (ready.presence[f.id]) present += 1;
    }
    out[f.id] = {
      label: f.label,
      present,
      total: rows.length,
      missing: rows.length - present,
      completenessPct: rows.length ? Math.round((present / rows.length) * 1000) / 10 : 0,
      priority: f.priority,
      differentiationValue: f.differentiationValue,
      tableStakes: f.tableStakes,
      eligibilityImpact: f.eligibilityImpact,
      alignmentImpact: f.alignmentImpact,
      confidenceImpact: f.confidenceImpact,
      sourcingDifficulty: f.sourcingDifficulty,
    };
  }
  return out;
}

async function main() {
  console.log("[operator-fit-data-readiness] READ-ONLY starting…");
  const { airtableConfigured, rows } = await loadOperators();
  if (!airtableConfigured) {
    console.error("Airtable not configured — cannot classify live operators.");
    process.exit(1);
  }

  // Representative project for project-specific coverage (urban branded)
  const urbanProject = adaptProjectFromDealContext({
    dealId: "recREADINESS_URBAN",
    dealFields: FIT_V2_SCENARIOS[0].dealFields,
    locationData: FIT_V2_SCENARIOS[0].locationData,
    mpData: FIT_V2_SCENARIOS[0].mpData,
    siData: FIT_V2_SCENARIOS[0].siData,
  });
  const conversionProject = adaptProjectFromDealContext({
    dealId: "recREADINESS_CONV",
    dealFields: FIT_V2_SCENARIOS[2].dealFields,
    locationData: FIT_V2_SCENARIOS[2].locationData,
    mpData: FIT_V2_SCENARIOS[2].mpData,
    siData: FIT_V2_SCENARIOS[2].siData,
  });

  const classifications = [];
  const queue = [];
  const taxonomyAll = [];
  const evidenceAll = [];

  for (const r of rows) {
    const baseline = classifyOperatorReadiness(r.operator, null);
    const urban = classifyOperatorReadiness(r.operator, urbanProject);
    const conversion = classifyOperatorReadiness(r.operator, conversionProject);
    const tax = validateOperatorTaxonomy(r.prefill);
    const evid = validateOperatorEvidence(r.operator);
    taxonomyAll.push({ operatorId: r.operator.operatorId, issues: tax });
    evidenceAll.push({ operatorId: r.operator.operatorId, issues: evid });

    const primary = urban; // pipeline-relevant default
    classifications.push({
      operatorId: r.operator.operatorId,
      operatorName: r.candidate.companyName,
      baseline,
      byProjectType: {
        urbanBranded: urban,
        selectServiceConversion: conversion,
      },
      taxonomyIssues: tax,
      evidenceIssues: evid,
    });
    queue.push(
      buildEnrichmentQueueRow(r.operator, primary, {
        baselineStatus: baseline.status,
        conversionStatus: conversion.status,
      })
    );
  }

  // Deterministic sort: Critical first, then coverage ascending
  const pri = { Critical: 0, High: 1, Medium: 2, Low: 3 };
  queue.sort((a, b) => {
    const p = (pri[a.researchPriority] ?? 9) - (pri[b.researchPriority] ?? 9);
    if (p) return p;
    return (a.overallCoverage || 0) - (b.overallCoverage || 0);
  });

  const counts = {
    rankingReady: classifications.filter((c) => c.byProjectType.urbanBranded.status === READINESS_STATUS.RANKING_READY)
      .length,
    conditionallyRankable: classifications.filter(
      (c) => c.byProjectType.urbanBranded.status === READINESS_STATUS.CONDITIONALLY_RANKABLE
    ).length,
    researchRequired: classifications.filter(
      (c) => c.byProjectType.urbanBranded.status === READINESS_STATUS.RESEARCH_REQUIRED
    ).length,
    outOfScope: 0,
  };

  const fieldStats = fieldCompleteness(rows);
  const avg = (key) => {
    const vals = classifications.map((c) => c.byProjectType.urbanBranded[key] || 0);
    return vals.length ? Math.round((vals.reduce((a, b) => a + b, 0) / vals.length) * 10) / 10 : 0;
  };

  const report = {
    mode: "read-only",
    generatedAt: new Date().toISOString(),
    airtableConfigured,
    activeOperatorCount: rows.length,
    productionCoverageThresholdPct: PRODUCTION_COVERAGE_THRESHOLD_PCT,
    counts,
    averages: {
      eligibilityCoverage: avg("eligibilityCoverage"),
      differentiationCoverage: avg("differentiationCoverage"),
      evidenceCoverage: avg("evidenceCoverage"),
      overallCoverage: avg("coveragePct"),
    },
    fieldCompleteness: fieldStats,
    operators: classifications.map((c) => ({
      operatorId: c.operatorId,
      operatorName: c.operatorName,
      readinessStatus: c.byProjectType.urbanBranded.status,
      researchPriority: c.byProjectType.urbanBranded.researchPriority,
      coveragePct: c.byProjectType.urbanBranded.coveragePct,
      eligibilityCoverage: c.byProjectType.urbanBranded.eligibilityCoverage,
      differentiationCoverage: c.byProjectType.urbanBranded.differentiationCoverage,
      evidenceCoverage: c.byProjectType.urbanBranded.evidenceCoverage,
      brandRelationshipCoverage: c.byProjectType.urbanBranded.brandRelCoverage,
      missingCritical: c.byProjectType.urbanBranded.missingCritical,
      reason: c.byProjectType.urbanBranded.reason,
      urbanStatus: c.byProjectType.urbanBranded.status,
      conversionStatus: c.byProjectType.selectServiceConversion.status,
      taxonomyIssueCount: c.taxonomyIssues.length,
      evidenceIssueCount: c.evidenceIssues.length,
    })),
    enrichmentQueue: queue,
    taxonomyIssues: taxonomyAll.filter((t) => t.issues.length),
    evidenceIssues: evidenceAll.filter((e) => e.issues.length),
  };

  mkdirSync(join(root, "reports"), { recursive: true });
  const jsonPath = join(root, "reports", "operator-fit-operator-readiness.json");
  writeFileSync(jsonPath, JSON.stringify(report, null, 2), "utf8");

  const csvHeaders = [
    "Operator ID",
    "Operator name",
    "Readiness status",
    "Current overall coverage",
    "Eligibility-field coverage",
    "Differentiation-field coverage",
    "Evidence-field coverage",
    "Brand-relationship coverage",
    "Highest-priority missing field",
    "Second-priority missing field",
    "Third-priority missing field",
    "Recommended source type",
    "Expected scoring impact",
    "Research priority",
    "Notes",
  ];
  const csvLines = [csvHeaders.join(",")];
  for (const q of queue) {
    csvLines.push(
      [
        q.operatorId,
        q.operatorName,
        q.readinessStatus,
        q.overallCoverage,
        q.eligibilityCoverage,
        q.differentiationCoverage,
        q.evidenceCoverage,
        q.brandRelationshipCoverage,
        q.highestPriorityMissingField,
        q.secondPriorityMissingField,
        q.thirdPriorityMissingField,
        q.recommendedSourceType,
        q.expectedScoringImpact,
        q.researchPriority,
        q.notes,
      ]
        .map(csvEscape)
        .join(",")
    );
  }
  const csvPath = join(root, "reports", "operator-fit-operator-enrichment-queue.csv");
  writeFileSync(csvPath, csvLines.join("\n"), "utf8");

  const md = [
    "# Operator Fit — Operator Readiness Classification",
    "",
    `Generated: ${report.generatedAt} (read-only)`,
    "",
    `Active operators: **${report.activeOperatorCount}**`,
    "",
    `| Status | Count |`,
    `| ------ | ----: |`,
    `| Ranking Ready | ${counts.rankingReady} |`,
    `| Conditionally Rankable | ${counts.conditionallyRankable} |`,
    `| Research Required | ${counts.researchRequired} |`,
    "",
    `Production coverage threshold: **${PRODUCTION_COVERAGE_THRESHOLD_PCT}%** (project-applicable).`,
    "",
    "## Averages (urban branded representative project)",
    "",
    `- Eligibility coverage: ${report.averages.eligibilityCoverage}%`,
    `- Differentiation coverage: ${report.averages.differentiationCoverage}%`,
    `- Evidence coverage: ${report.averages.evidenceCoverage}%`,
    `- Overall coverage: ${report.averages.overallCoverage}%`,
    "",
    "## Operators",
    "",
    `| Operator | Status | Coverage | Priority | Critical missing |`,
    `| -------- | ------ | -------: | -------- | ---------------- |`,
    ...report.operators.map(
      (o) =>
        `| ${o.operatorName} | ${o.readinessStatus} | ${o.coveragePct}% | ${o.researchPriority} | ${(o.missingCritical || []).join(", ") || "—"} |`
    ),
    "",
    "## Note",
    "",
    "An operator can be Ranking Ready for one project type and Research Required for another — see `conversionStatus` vs `urbanStatus` in JSON.",
    "",
  ];
  const mdPath = join(root, "reports", "operator-fit-operator-readiness.md");
  writeFileSync(mdPath, md.join("\n"), "utf8");

  // Also write a compact JSON for the internal UI
  writeFileSync(
    join(root, "reports", "operator-fit-data-readiness-ui-payload.json"),
    JSON.stringify(
      {
        generatedAt: report.generatedAt,
        summary: {
          activeOperators: report.activeOperatorCount,
          ...counts,
          averages: report.averages,
        },
        operators: report.operators,
        fieldCompleteness: Object.values(fieldStats).sort(
          (a, b) => a.completenessPct - b.completenessPct
        ),
        queue,
      },
      null,
      2
    ),
    "utf8"
  );

  console.log(
    JSON.stringify(
      {
        activeOperatorCount: report.activeOperatorCount,
        counts,
        averages: report.averages,
        wrote: [jsonPath, csvPath, mdPath],
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error("[operator-fit-data-readiness] FAILED:", err);
  process.exit(1);
});
