#!/usr/bin/env node
/**
 * Wave 2 + calibration evaluation (READ-ONLY). Local overlay + Airtable enrich.
 *   node scripts/operator-intelligence-wave-2-evaluate.mjs
 */
import "dotenv/config";
import { writeFileSync, readFileSync, existsSync, mkdirSync } from "fs";
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
  loadOperatorIntelligenceUniverse,
  buildPrefillOverlayFromCohort,
  mergePrefillWithCalibration,
} from "../lib/operator-intelligence/calibration-overlay.js";
import { resolvePublicationDecision } from "../lib/operator-intelligence/publication-policy.js";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const WAVE2_IDS = [
  "recLjxtxIIVJaGbXK", // Highgate
  "recfwDdU5t9h4uFnZ", // Atlantica
  "recKVILWcRLqrQlWs", // Driftwood
  "reckyv9O0Y3auYpJJ", // Santa Fe
];
const CAL_IDS = [
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
  }
  if (c.commercial?.fields) {
    const cf = c.commercial.fields;
    if (cf["Management Structures Supported"]) {
      merged.managementStructuresSupported = cf["Management Structures Supported"];
    }
  }
  if (c.governance?.fields) {
    const gf = c.governance.fields;
    if (gf["Owner Reporting Level"]) merged.ownerReportingLevel = gf["Owner Reporting Level"];
  }
  if (c.profile?.fields) {
    const pr = c.profile.fields;
    if (pr.chainScalesSupported) merged.chainScalesSupported = pr.chainScalesSupported;
    if (pr.brands) merged.brands = pr.brands;
  }
  return merged;
}

function projectFromScenario(s) {
  return adaptProjectFromDealContext({
    dealId: `recW2_${s.id}`,
    dealFields: s.dealFields,
    locationData: s.locationData,
    mpData: s.mpData,
    siData: s.siData,
  });
}

function buildPrefills(ids, byId, cohort) {
  const out = [];
  for (const id of ids) {
    const c = byId[id];
    if (!c) continue;
    const base = enrichPrefillFromAirtable(
      c,
      buildPrefillObjectFromNewBaseRows(c.master, c.profile, c.platform, c.commercial, c.governance)
    );
    const overlay = buildPrefillOverlayFromCohort(id, cohort);
    const merged = mergePrefillWithCalibration(base, overlay);
    out.push({
      operatorId: id,
      companyName: c.companyName,
      prefill: merged.prefill,
      mode: merged.mode,
      airtableOnly: base,
      diagnostics: merged.diagnostics,
    });
  }
  return out;
}

function summarizeRow(row, ready) {
  return {
    operator: row.operatorName,
    operatorId: row.candidateId,
    eligibility: row.eligibilityStatus,
    readiness: ready?.status || null,
    rawAlignment: row.rawOperatorAlignment,
    displayedAlignment: row.displayedOperatorAlignment,
    confidence: row.evidenceConfidence,
    coverage: row.dataCoveragePct,
    structureAlignment: row.factorBreakdown?.find?.((f) => /structure/i.test(f.key || ""))?.score ?? null,
    brandCompatibility: row.factorBreakdown?.find?.((f) => /brand/i.test(f.key || ""))?.score ?? null,
    executionRisk: row.executionRisk || null,
    mainStrengths: (row.whyItMatches || []).slice(0, 3),
    mainConcerns: (row.potentialConcerns || []).slice(0, 3),
    unknowns: (row.unknowns || []).slice(0, 3),
    validationQuestions: (row.validationItems || []).slice(0, 3),
    rank: row.rank || null,
  };
}

async function main() {
  console.log("[oi-wave2] READ-ONLY evaluate…");
  const universe = loadOperatorIntelligenceUniverse();
  const wave2Only = loadCalibrationCohort(join(root, "data", "operator-intelligence", "wave-2-cohort"));
  const { candidates } = await loadActiveOperatorCandidatesForAlignment();
  const byId = Object.fromEntries(candidates.map((c) => [c.operatorId, c]));

  const wave2Prefills = buildPrefills(WAVE2_IDS, byId, wave2Only);
  const universePrefills = buildPrefills([...CAL_IDS, ...WAVE2_IDS], byId, universe);

  // Readiness by scenario for Wave 2
  const readiness = [];
  const scenarioReport = [];
  for (const s of FIT_V2_SCENARIOS) {
    const project = projectFromScenario(s);
    const evaluated = evaluateOperatorFitForDeal({
      dealId: `recW2_${s.id}`,
      dealFields: s.dealFields,
      locationData: s.locationData,
      mpData: s.mpData || {},
      siData: s.siData || {},
      operatorPrefills: wave2Prefills.map((p) => ({
        operatorId: p.operatorId,
        companyName: p.companyName,
        prefill: p.prefill,
      })),
    });
    const rows = (evaluated.top5 || evaluated.results || []).map((row, idx) => {
      const pref = wave2Prefills.find((p) => p.operatorId === row.candidateId);
      const op = adaptOperatorFromPrefill(pref?.prefill || {}, {
        operatorId: row.candidateId,
        companyName: row.operatorName,
      });
      const ready = classifyOperatorReadiness(op, project);
      return summarizeRow({ ...row, rank: idx + 1 }, ready);
    });
    scenarioReport.push({
      scenarioId: s.id,
      label: s.label,
      rankingReady: rows.filter((r) => r.readiness === READINESS_STATUS.RANKING_READY).length,
      rows,
    });
  }

  for (const p of wave2Prefills) {
    const byProject = {};
    for (const s of FIT_V2_SCENARIOS) {
      const project = projectFromScenario(s);
      const op = adaptOperatorFromPrefill(p.prefill, { operatorId: p.operatorId, companyName: p.companyName });
      byProject[s.id] = classifyOperatorReadiness(op, project).status;
    }
    const rr = Object.entries(byProject)
      .filter(([, st]) => st === READINESS_STATUS.RANKING_READY)
      .map(([id]) => id);
    readiness.push({
      operatorId: p.operatorId,
      operatorName: p.companyName,
      rankingReadyProjects: rr,
      byProject,
      publishedFacts: p.diagnostics?.publishedFactsAdded ?? 0,
      qualifiedFacts: p.diagnostics?.qualifiedFactsAdded ?? 0,
      internalOnly: p.diagnostics?.internalOnly ?? 0,
    });
  }

  // Real deals
  const priorPath = join(root, "reports", "operator-fit-real-deal-shadow-review.json");
  const prior = existsSync(priorPath) ? JSON.parse(readFileSync(priorPath, "utf8")) : { deals: [] };
  const realDeals = [];
  for (const deal of prior.deals || []) {
    const ctx = {
      projectType: deal.projectType,
      country: deal.country,
      chainScale: deal.chainScale,
      building: deal.buildingType,
      operatingModel: deal.operatingModel,
      preferredStructures: deal.preferredStructures || [],
    };
    const evaluated = evaluateOperatorFitForDeal({
      dealId: `recW2_${deal.label}`,
      dealFields: { "Project Type": ctx.projectType },
      locationData: {
        Country: ctx.country,
        "Hotel Chain Scale": ctx.chainScale,
        "Building Type": ctx.building,
      },
      mpData: {},
      siData: {
        "Operating Model": ctx.operatingModel,
        "Preferred Management Structure": ctx.preferredStructures || [],
      },
      operatorPrefills: universePrefills.map((p) => ({
        operatorId: p.operatorId,
        companyName: p.companyName,
        prefill: p.prefill,
      })),
    });
    const project = evaluated.project;
    const rows = (evaluated.top5 || []).map((row, idx) => {
      const pref = universePrefills.find((p) => p.operatorId === row.candidateId);
      const op = adaptOperatorFromPrefill(pref?.prefill || {}, {
        operatorId: row.candidateId,
        companyName: row.operatorName,
      });
      const ready = classifyOperatorReadiness(op, project);
      return summarizeRow({ ...row, rank: idx + 1 }, ready);
    });
    realDeals.push({
      label: deal.label,
      archetype: deal.archetype,
      country: deal.country,
      projectType: deal.projectType,
      chainScale: deal.chainScale,
      rankingReady: rows.filter((r) => r.readiness === READINESS_STATUS.RANKING_READY).length,
      rows,
    });
  }

  // Active universe readiness summary (10 operators with overlay)
  const universeReadiness = [];
  const rep = FIT_V2_SCENARIOS[0];
  const repProject = projectFromScenario(rep);
  for (const p of universePrefills) {
    const byProject = {};
    for (const s of FIT_V2_SCENARIOS) {
      const project = projectFromScenario(s);
      const op = adaptOperatorFromPrefill(p.prefill, { operatorId: p.operatorId, companyName: p.companyName });
      byProject[s.id] = classifyOperatorReadiness(op, project).status;
    }
    const op = adaptOperatorFromPrefill(p.prefill, { operatorId: p.operatorId, companyName: p.companyName });
    const repReady = classifyOperatorReadiness(op, repProject);
    universeReadiness.push({
      operatorId: p.operatorId,
      operatorName: p.companyName,
      cohort: WAVE2_IDS.includes(p.operatorId) ? "wave-2" : "calibration",
      representativeStatus: repReady.status,
      rankingReadyProjects: Object.entries(byProject)
        .filter(([, st]) => st === READINESS_STATUS.RANKING_READY)
        .map(([id]) => id),
      conditionallyRankableProjects: Object.entries(byProject)
        .filter(([, st]) => st === READINESS_STATUS.CONDITIONALLY_RANKABLE)
        .map(([id]) => id),
      researchRequiredProjects: Object.entries(byProject)
        .filter(([, st]) => st === READINESS_STATUS.RESEARCH_REQUIRED)
        .map(([id]) => id),
      coveragePct: repReady.coveragePct,
      byProject,
    });
  }

  // Publication summary for Wave 2
  const pub = (wave2Only.claims || []).map((c) => {
    const d = resolvePublicationDecision(c, { sources: wave2Only.sources });
    return { claimId: c.id, operatorId: c.operatorId, status: d.status, reason: d.reason };
  });

  const out = {
    generatedAt: new Date().toISOString(),
    featureFlag: process.env.OPERATOR_FIT_ENGINE_V2 || "0",
    wave2Cohort: WAVE2_IDS,
    readiness,
    scenarios: scenarioReport,
    realDeals,
    universeReadiness,
    publication: {
      autoPublish: pub.filter((p) => /Auto-Publish/i.test(p.status)).length,
      qualified: pub.filter((p) => /Evidence Label|Publish With/i.test(p.status)).length,
      internalOnly: pub.filter((p) => /Internal/i.test(p.status)).length,
      other: pub.filter(
        (p) => !/Auto-Publish|Evidence Label|Publish With|Internal/i.test(p.status)
      ).length,
      decisions: pub,
    },
    exceptions: wave2Only.exceptions || [],
  };

  mkdirSync(join(root, "reports"), { recursive: true });
  writeFileSync(join(root, "reports", "operator-intelligence-wave-2-readiness.json"), JSON.stringify(out, null, 2));
  writeFileSync(join(root, "reports", "operator-intelligence-wave-2-scenarios.json"), JSON.stringify(scenarioReport, null, 2));
  writeFileSync(join(root, "reports", "operator-intelligence-wave-2-real-deals.json"), JSON.stringify(realDeals, null, 2));
  writeFileSync(
    join(root, "reports", "operator-intelligence-active-universe-readiness.json"),
    JSON.stringify({ generatedAt: out.generatedAt, operators: universeReadiness }, null, 2)
  );

  const mdReady = [
    "# Operator Intelligence — Wave 2 Readiness",
    "",
    `Generated: ${out.generatedAt}`,
    "",
    "| Operator | Ranking Ready projects |",
    "| -------- | ---------------------- |",
    ...readiness.map(
      (r) => `| ${r.operatorName} | ${r.rankingReadyProjects.length ? r.rankingReadyProjects.join(", ") : "none"} |`
    ),
    "",
  ];
  writeFileSync(join(root, "reports", "operator-intelligence-wave-2-readiness.md"), mdReady.join("\n"));

  const mdSc = [
    "# Operator Intelligence — Wave 2 Synthetic Scenarios",
    "",
    ...scenarioReport.flatMap((s) => [
      `## ${s.label}`,
      "",
      `Ranking Ready: **${s.rankingReady}**`,
      "",
      "| Rank | Operator | Readiness | Alignment | Confidence | Coverage |",
      "| ---: | -------- | --------- | --------: | ---------- | -------: |",
      ...s.rows.map(
        (r) =>
          `| ${r.rank} | ${r.operator} | ${r.readiness} | ${r.displayedAlignment} | ${r.confidence} | ${r.coverage}% |`
      ),
      "",
    ]),
  ];
  writeFileSync(join(root, "reports", "operator-intelligence-wave-2-scenarios.md"), mdSc.join("\n"));

  const mdRd = [
    "# Operator Intelligence — Wave 2 Real Deals (Calibration + Wave 2 universe)",
    "",
    ...realDeals.flatMap((d) => [
      `## ${d.label} (${d.archetype}) — ${d.country}`,
      "",
      `Ranking Ready: **${d.rankingReady}**`,
      "",
      "| Rank | Operator | Readiness | Alignment | Confidence | Concern |",
      "| ---: | -------- | --------- | --------: | ---------- | ------- |",
      ...d.rows.map(
        (r) =>
          `| ${r.rank} | ${r.operator} | ${r.readiness} | ${r.displayedAlignment} | ${r.confidence} | ${(r.mainConcerns || [])[0] || "—"} |`
      ),
      "",
    ]),
  ];
  writeFileSync(join(root, "reports", "operator-intelligence-wave-2-real-deals.md"), mdRd.join("\n"));

  const mdUniv = [
    "# Operator Intelligence — Active Universe Readiness (Calibration + Wave 2 overlays)",
    "",
    `Generated: ${out.generatedAt}`,
    "",
    "| Operator | Cohort | Ranking Ready # | Conditionally Rankable # | Research Required # |",
    "| -------- | ------ | --------------: | -----------------------: | ------------------: |",
    ...universeReadiness.map(
      (r) =>
        `| ${r.operatorName} | ${r.cohort} | ${r.rankingReadyProjects.length} | ${r.conditionallyRankableProjects.length} | ${r.researchRequiredProjects.length} |`
    ),
    "",
    "## Highest-impact remaining gaps",
    "",
    "- Argentina current operating presence (Deal B blocker)",
    "- Brand approval verification (property-scoped vs global)",
    "- Project-specific fees / availability / team (never auto-published)",
    "- Performance metrics (internal / unavailable)",
    "",
    "## Wave 3 likely beneficiaries",
    "",
    "- Remington Hospitality (deferred from Wave 2)",
    "- Argentina-capable regional operators (project-specific outreach)",
    "- Additional conversion specialists with CALA inventory",
    "",
  ];
  writeFileSync(join(root, "reports", "operator-intelligence-active-universe-readiness.md"), mdUniv.join("\n"));

  writeFileSync(
    join(root, "reports", "operator-intelligence-wave-2-exceptions.md"),
    [
      "# Operator Intelligence — Wave 2 Exceptions",
      "",
      ...((wave2Only.exceptions || []).map(
        (e) =>
          `- **${e.operatorId}**: ${e.claim} — ${e.reviewStatus}. ${e.disposition || e.reasonForEscalation}`
      ) || ["(none)"]),
      "",
      "## Publication counts",
      "",
      `- Auto-Publish: ${out.publication.autoPublish}`,
      `- Publish With Evidence Label: ${out.publication.qualified}`,
      `- Internal Only: ${out.publication.internalOnly}`,
      `- Other: ${out.publication.other}`,
      "",
    ].join("\n")
  );

  // Update UI payload merge
  const uiPath = join(root, "reports", "operator-intelligence-calibration-ui-payload.json");
  let ui = existsSync(uiPath) ? JSON.parse(readFileSync(uiPath, "utf8")) : {};
  ui.wave2 = {
    operators: wave2Only.operators,
    readiness,
    claims: wave2Only.claims,
    sources: wave2Only.sources,
    exceptions: wave2Only.exceptions,
    publication: out.publication,
    realDeals,
  };
  ui.universeReadiness = universeReadiness;
  ui.airtablePersistence = {
    appliedAt: "see operator-intelligence-calibration-apply-result.json",
    featureFlag: out.featureFlag,
  };
  writeFileSync(uiPath, JSON.stringify(ui, null, 2));

  console.log(
    JSON.stringify(
      {
        wave2RankingReadyOps: readiness.filter((r) => r.rankingReadyProjects.length).map((r) => r.operatorName),
        realDealRankingReady: realDeals.map((d) => ({ label: d.label, n: d.rankingReady })),
        publication: out.publication,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error("[oi-wave2] FAILED", err);
  process.exit(1);
});
