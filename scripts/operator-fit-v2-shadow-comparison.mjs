#!/usr/bin/env node
/**
 * Shadow comparison: legacy OAS vs Operator Fit Engine v2 (synthetic only).
 *   node scripts/operator-fit-v2-shadow-comparison.mjs
 * Writes reports/operator-fit-v2-shadow-comparison.json and .md
 * No Airtable writes.
 */
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { scoreOperatorMatchForDeal } from "../api/my-deals.js";
import { evaluateOperatorFitForDeal } from "../lib/operator-fit/evaluate-deal.js";
import { FIT_V2_SCENARIOS, FIT_V2_OPERATORS } from "../lib/operator-fit/fixtures/scenarios.js";
import { EVIDENCE_CLASSES } from "../lib/operator-fit/config.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const outJson = join(root, "reports", "operator-fit-v2-shadow-comparison.json");
const outMd = join(root, "reports", "operator-fit-v2-shadow-comparison.md");

function legacyRank(scenario) {
  const rows = FIT_V2_OPERATORS.map((o) => {
    const { score, breakdownDetails } = scoreOperatorMatchForDeal(
      scenario.dealFields,
      scenario.locationData,
      scenario.mpData,
      scenario.siData,
      o.prefill
    );
    const fee = breakdownDetails?.feeCommercial?.score;
    return {
      id: o.id,
      name: o.companyName,
      score,
      feeScore: fee,
      genericServiceHeavy: (o.prefill.offeredServices || []).length >= 8,
    };
  }).sort((a, b) => (b.score ?? -1) - (a.score ?? -1));
  return rows;
}

const scenarios = [];
for (const s of FIT_V2_SCENARIOS) {
  const legacy = legacyRank(s);
  const v2 = evaluateOperatorFitForDeal({
    dealId: "recSHADOW_" + s.id,
    dealFields: s.dealFields,
    locationData: s.locationData,
    mpData: s.mpData,
    siData: s.siData,
    operatorPrefills: FIT_V2_OPERATORS.map((o) => ({
      operatorId: o.id,
      companyName: o.companyName,
      prefill: o.prefill,
    })),
    brandManagedCandidates:
      s.id === "luxury-leisure-resort"
        ? [
            {
              brandName: "Four Seasons",
              offersBrandManagement: true,
              markets: ["Dominican Republic"],
              scales: ["Luxury"],
              evidenceClasses: [EVIDENCE_CLASSES.PORTFOLIO_LEVEL],
            },
          ]
        : [],
  });

  const legacyTop = legacy[0];
  const v2Top = v2.top5[0] || null;
  const sparseLegacy = legacy.find((r) => r.id === "sparse-data-operator");
  const sparseV2 = (v2.allEvaluated || []).find((r) => r.candidateId === "sparse-data-operator");
  const genericLegacyRank = legacy.findIndex((r) => r.id === "generic-full-service-claims") + 1;
  const genericV2Rank =
    (v2.allEvaluated || [])
      .filter((e) => e.eligibilityStatus !== "Not Currently Eligible")
      .sort((a, b) => b.displayedOperatorAlignment - a.displayedOperatorAlignment)
      .findIndex((r) => r.candidateId === "generic-full-service-claims") + 1;

  let changeReason = "Similar ranking";
  if (legacyTop?.id !== v2Top?.candidateId) {
    changeReason = `Top changed from ${legacyTop?.name} (legacy ${legacyTop?.score}) to ${
      v2Top?.operatorName || "none"
    } (v2 displayed ${v2Top?.displayedOperatorAlignment ?? "—"}) due to eligibility/evidence/de-genericized factors.`;
  }

  scenarios.push({
    scenarioId: s.id,
    label: s.label,
    legacyRanking: legacy.map((r, i) => ({ rank: i + 1, id: r.id, name: r.name, score: r.score })),
    newRanking: v2.top5.map((r) => ({
      rank: r.rank,
      id: r.candidateId,
      name: r.operatorName,
      displayed: r.displayedOperatorAlignment,
      raw: r.rawOperatorAlignment,
      eligibility: r.eligibilityStatus,
      confidence: r.evidenceConfidence,
      coverage: r.dataCoveragePct,
    })),
    legacyTopScore: legacyTop?.score ?? null,
    newRawAlignment: v2Top?.rawOperatorAlignment ?? null,
    newDisplayedAlignment: v2Top?.displayedOperatorAlignment ?? null,
    evidenceConfidence: v2Top?.evidenceConfidence ?? null,
    dataCoverage: v2Top?.dataCoveragePct ?? null,
    eligibility: v2Top?.eligibilityStatus ?? null,
    confidenceCeilingApplied: v2Top?.confidenceCeilingApplied ?? null,
    mainReasonForRankingChange: changeReason,
    genericCapabilityInfluence: {
      legacyGenericRank: genericLegacyRank || null,
      v2GenericRankAmongEligible: genericV2Rank || null,
      note: "v2 does not award positive points for table-stakes service lists",
    },
    unknownDataInfluence: {
      sparseLegacyScore: sparseLegacy?.score ?? null,
      sparseV2Displayed: sparseV2?.displayedOperatorAlignment ?? null,
      sparseV2Confidence: sparseV2?.evidenceConfidence ?? null,
      sparseV2Eligibility: sparseV2?.eligibilityStatus ?? null,
    },
    risksAndValidation: v2Top
      ? {
          concerns: v2Top.potentialConcerns,
          unknowns: v2Top.unknowns,
          validation: v2Top.validationQuestions,
          riskPenalty: v2Top.executionRiskPenalty,
        }
      : null,
    diagnostics: v2.diagnostics,
  });
}

const report = {
  generatedAt: new Date().toISOString(),
  mode: "synthetic-shadow-no-airtable-writes",
  engineVersion: v2Version(),
  scenarios,
  summary: {
    genericRankedFirstLegacy: scenarios.filter((s) => s.legacyRanking[0]?.id === "generic-full-service-claims")
      .length,
    genericRankedFirstV2: scenarios.filter((s) => s.newRanking[0]?.id === "generic-full-service-claims")
      .length,
    sparseAbove69Legacy: scenarios.filter((s) => (s.unknownDataInfluence.sparseLegacyScore || 0) > 69)
      .length,
    sparseAbove69V2: scenarios.filter((s) => (s.unknownDataInfluence.sparseV2Displayed || 0) > 69)
      .length,
  },
};

function v2Version() {
  return "operator-fit-v2.1.0";
}

mkdirSync(dirname(outJson), { recursive: true });
writeFileSync(outJson, JSON.stringify(report, null, 2), "utf8");

const lines = [
  "# Operator Fit v2 — Shadow Comparison (Synthetic)",
  "",
  `Generated: ${report.generatedAt}`,
  "",
  "Legacy OAS vs Operator Fit Engine v2. No Airtable writes.",
  "",
  "## Summary",
  "",
  `| Metric | Count |`,
  `| ------ | ----: |`,
  `| Scenarios where generic claims ranked #1 (legacy) | ${report.summary.genericRankedFirstLegacy} |`,
  `| Scenarios where generic claims ranked #1 (v2 Top-5) | ${report.summary.genericRankedFirstV2} |`,
  `| Scenarios where sparse legacy score > 69 | ${report.summary.sparseAbove69Legacy} |`,
  `| Scenarios where sparse v2 displayed > 69 | ${report.summary.sparseAbove69V2} |`,
  "",
];

for (const s of scenarios) {
  lines.push(`## ${s.label} (\`${s.scenarioId}\`)`, "");
  lines.push(
    `| | Legacy OAS | Operator Fit v2 |`,
    `| -- | -- | -- |`,
    `| Top candidate | ${s.legacyRanking[0]?.name} | ${s.newRanking[0]?.name || "—"} |`,
    `| Score | ${s.legacyTopScore} | raw ${s.newRawAlignment} / displayed ${s.newDisplayedAlignment} |`,
    `| Evidence confidence | n/a | ${s.evidenceConfidence} |`,
    `| Data coverage | n/a | ${s.dataCoverage}% |`,
    `| Eligibility | n/a | ${s.eligibility} |`,
    `| Ceiling applied | n/a | ${s.confidenceCeilingApplied ?? "none"} |`,
    ""
  );
  lines.push(`**Ranking change:** ${s.mainReasonForRankingChange}`, "");
  lines.push(
    `**Generic influence:** legacy rank ${s.genericCapabilityInfluence.legacyGenericRank}; v2 eligible rank ${s.genericCapabilityInfluence.v2GenericRankAmongEligible}`,
    ""
  );
  lines.push(
    `**Sparse unknown influence:** legacy score ${s.unknownDataInfluence.sparseLegacyScore}; v2 displayed ${s.unknownDataInfluence.sparseV2Displayed} (${s.unknownDataInfluence.sparseV2Confidence}, ${s.unknownDataInfluence.sparseV2Eligibility})`,
    ""
  );
}

writeFileSync(outMd, lines.join("\n"), "utf8");
console.log("Wrote", outJson);
console.log("Wrote", outMd);
console.log(JSON.stringify(report.summary, null, 2));
