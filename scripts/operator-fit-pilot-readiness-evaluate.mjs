#!/usr/bin/env node
/**
 * Pilot readiness real-deal + active-universe report (read-only).
 *   node scripts/operator-fit-pilot-readiness-evaluate.mjs
 */
import "dotenv/config";
import { writeFileSync, readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { loadActiveOperatorCandidatesForAlignment } from "../lib/operator-alignment-company-utils.js";
import { buildPrefillObjectFromNewBaseRows } from "../api/lib/operator-setup-new-base-read.js";
import { adaptOperatorFromPrefill } from "../lib/operator-fit/adapters/operator-from-prefill.js";
import { evaluateOperatorFitForDeal } from "../lib/operator-fit/evaluate-deal.js";
import { classifyOperatorReadiness, READINESS_STATUS } from "../lib/operator-fit/readiness.js";
import { FIT_V2_SCENARIOS } from "../lib/operator-fit/fixtures/scenarios.js";
import { adaptProjectFromDealContext } from "../lib/operator-fit/adapters/project-from-deal.js";
import {
  loadOperatorIntelligenceUniverse,
  loadCalibrationCohort,
  buildPrefillOverlayFromCohort,
  mergePrefillWithCalibration,
} from "../lib/operator-intelligence/calibration-overlay.js";
import { countriesWithStrongPresence } from "../lib/operator-intelligence/market-presence.js";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");

function enrich(c, prefill) {
  const merged = { ...(prefill || {}), submission_status: "Active", companyName: c.companyName };
  const pf = c.platform?.fields || {};
  const cf = c.commercial?.fields || {};
  if (pf["Active Countries"]) merged.activeCountries = pf["Active Countries"];
  if (cf["Management Structures Supported"]) merged.managementStructuresSupported = cf["Management Structures Supported"];
  return merged;
}

function summarize(row, ready) {
  return {
    operator: row.operatorName,
    operatorId: row.candidateId,
    eligibility: row.eligibilityStatus,
    readiness: ready?.status || null,
    alignment: row.displayedOperatorAlignment,
    confidence: row.evidenceConfidence,
    coverage: row.dataCoveragePct,
    rank: row.rank,
    whyFit: (row.whyItMatches || [])[0] || "—",
    concern: (row.potentialConcerns || [])[0] || "—",
    unknown: (row.unknowns || [])[0] || "—",
  };
}

async function main() {
  const universe = loadOperatorIntelligenceUniverse();
  const w3Path = join(root, "data", "operator-intelligence", "wave-3-cohort");
  const w3 = existsSync(join(w3Path, "operators.json")) ? loadCalibrationCohort(w3Path) : null;

  const { candidates } = await loadActiveOperatorCandidatesForAlignment();
  const byId = Object.fromEntries(candidates.map((c) => [c.operatorId, c]));

  const masterIds = (universe.operators || []).map((o) => o.operatorId).filter((id) => id && !String(id).startsWith("research_"));
  const prefills = [];
  for (const id of masterIds) {
    const c = byId[id];
    if (!c) continue;
    const base = enrich(
      c,
      buildPrefillObjectFromNewBaseRows(c.master, c.profile, c.platform, c.commercial, c.governance)
    );
    const overlay = buildPrefillOverlayFromCohort(id, universe);
    const merged = mergePrefillWithCalibration(base, overlay);
    prefills.push({ operatorId: id, companyName: c.companyName, prefill: merged.prefill });
  }

  // Research-stage Wave 3 prefills (no Master) — for Deal B diagnosis only
  const researchPrefills = [];
  if (w3) {
    for (const op of w3.operators || []) {
      const overlay = buildPrefillOverlayFromCohort(op.operatorId, w3);
      if (!overlay) continue;
      researchPrefills.push({
        operatorId: op.operatorId,
        companyName: op.operatorName,
        prefill: mergePrefillWithCalibration({ submission_status: "Active", companyName: op.operatorName }, overlay).prefill,
        researchStage: true,
      });
    }
  }

  const prior = JSON.parse(readFileSync(join(root, "reports", "operator-fit-real-deal-shadow-review.json"), "utf8"));
  const dealsOut = [];
  for (const deal of prior.deals || []) {
    const evaluated = evaluateOperatorFitForDeal({
      dealId: `recPILOT_${deal.label}`,
      dealFields: { "Project Type": deal.projectType },
      locationData: {
        Country: deal.country,
        "Hotel Chain Scale": deal.chainScale,
        "Building Type": deal.buildingType,
      },
      mpData: {},
      siData: {
        "Operating Model": deal.operatingModel,
        "Preferred Management Structure": deal.preferredStructures || [],
        "Market Presence Requirement": "Active country operations required",
      },
      operatorPrefills: prefills,
    });
    const project = evaluated.project;
    const rows = (evaluated.top5 || []).map((row, idx) => {
      const pref = prefills.find((p) => p.operatorId === row.candidateId);
      const op = adaptOperatorFromPrefill(pref?.prefill || {}, {
        operatorId: row.candidateId,
        companyName: row.operatorName,
      });
      return summarize({ ...row, rank: idx + 1 }, classifyOperatorReadiness(op, project));
    });
    const rankingReady = rows.filter((r) => r.readiness === READINESS_STATUS.RANKING_READY);
    const research = rows.filter((r) => r.readiness !== READINESS_STATUS.RANKING_READY);

    // Research-stage Argentina candidates for Deal B only (informational)
    let researchStage = [];
    if (deal.label === "Deal B" && researchPrefills.length) {
      const ev2 = evaluateOperatorFitForDeal({
        dealId: `recPILOT_B_research`,
        dealFields: { "Project Type": deal.projectType },
        locationData: {
          Country: deal.country,
          "Hotel Chain Scale": deal.chainScale,
          "Building Type": deal.buildingType,
        },
        mpData: {},
        siData: { "Market Presence Requirement": "Active country operations required" },
        operatorPrefills: researchPrefills,
      });
      researchStage = (ev2.top5 || []).map((row, idx) => {
        const pref = researchPrefills.find((p) => p.operatorId === row.candidateId);
        const op = adaptOperatorFromPrefill(pref?.prefill || {}, {
          operatorId: row.candidateId,
          companyName: row.operatorName,
        });
        return {
          ...summarize({ ...row, rank: idx + 1 }, classifyOperatorReadiness(op, project)),
          researchStage: true,
        };
      });
    }

    dealsOut.push({
      label: deal.label,
      archetype: deal.archetype,
      country: deal.country,
      rankingReadyCount: rankingReady.length,
      ranked: rankingReady,
      additionalResearch: research,
      researchStageArgentina: researchStage,
      thinOrZero:
        rankingReady.length === 0
          ? "zero"
          : rankingReady.length < 5
            ? "thin"
            : "full",
      outcome:
        rankingReady.length >= 2
          ? "Outcome A — ≥2 Ranking Ready"
          : rankingReady.length === 0 && researchStage.some((r) => r.readiness === READINESS_STATUS.RANKING_READY)
            ? "Outcome B path — production universe constrained; research-stage Argentina candidates identified"
            : rankingReady.length === 0
              ? "Outcome B — documented constrained universe"
              : "Thin production pool",
    });
  }

  // Active universe summary
  const universeRows = [];
  for (const p of prefills) {
    const geos = (universe.geography || []).filter((g) => g.operatorId === p.operatorId);
    const strong = countriesWithStrongPresence(geos);
    const byProject = {};
    for (const s of FIT_V2_SCENARIOS) {
      const project = adaptProjectFromDealContext({
        dealId: `recU_${s.id}`,
        dealFields: s.dealFields,
        locationData: s.locationData,
        mpData: s.mpData,
        siData: s.siData,
      });
      const op = adaptOperatorFromPrefill(p.prefill, { operatorId: p.operatorId, companyName: p.companyName });
      byProject[s.id] = classifyOperatorReadiness(op, project).status;
    }
    const rr = Object.entries(byProject)
      .filter(([, st]) => st === READINESS_STATUS.RANKING_READY)
      .map(([id]) => id);
    universeRows.push({
      operatorId: p.operatorId,
      operatorName: p.companyName,
      strongCountries: strong,
      marketPresence: geos.map((g) => `${g.country}: ${g.presenceType}`),
      rankingReadyProjects: rr,
      argentinaReady: strong.includes("Argentina"),
      mexicoReady: strong.includes("Mexico"),
      highestGap: strong.includes("Argentina")
        ? "brandApprovals / project-specific"
        : geos.some((g) => /Argentina/i.test(g.country || ""))
          ? "Argentina presence type not strong"
          : "Argentina / Southern Cone coverage",
    });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    featureFlag: process.env.OPERATOR_FIT_ENGINE_V2 || "0",
    deals: dealsOut,
    universe: universeRows,
    wave3ResearchStageOnly: true,
  };

  writeFileSync(join(root, "reports", "operator-fit-pilot-readiness-real-deals.json"), JSON.stringify(dealsOut, null, 2));
  writeFileSync(
    join(root, "reports", "operator-fit-pilot-readiness-real-deals.md"),
    [
      "# Operator Fit — Pilot Readiness Real Deals",
      "",
      `Generated: ${report.generatedAt}`,
      "",
      ...dealsOut.flatMap((d) => [
        `## ${d.label} (${d.archetype}) — ${d.country}`,
        "",
        `Thin/zero state: **${d.thinOrZero}** · ${d.outcome}`,
        "",
        `Ranking Ready (production universe): **${d.rankingReadyCount}**`,
        "",
        "| Operator | Eligibility | Ranking Readiness | Alignment | Evidence Confidence | Coverage | Rank | Why Fit | Concern | Unknown |",
        "| -------- | ----------- | ----------------- | --------: | ------------------- | -------: | ---: | ------- | ------- | ------- |",
        ...[...d.ranked, ...d.additionalResearch].map(
          (r) =>
            `| ${r.operator} | ${r.eligibility} | ${r.readiness} | ${r.alignment} | ${r.confidence} | ${r.coverage}% | ${r.rank} | ${r.whyFit} | ${r.concern} | ${r.unknown} |`
        ),
        "",
        d.researchStageArgentina?.length
          ? [
              "### Research-stage Argentina candidates (not in production Master)",
              "",
              ...d.researchStageArgentina.map(
                (r) =>
                  `- ${r.operator}: ${r.readiness} · alignment ${r.alignment} · ${r.whyFit}`
              ),
              "",
            ].join("\n")
          : "",
      ]),
    ].join("\n")
  );

  writeFileSync(
    join(root, "reports", "operator-fit-pilot-readiness-active-universe.md"),
    [
      "# Operator Fit — Pilot Readiness Active Universe",
      "",
      `Generated: ${report.generatedAt}`,
      "",
      "| Operator | Strong Market Presence countries | Ranking Ready # | Argentina ready? | Highest gap |",
      "| -------- | -------------------------------- | --------------: | ---------------- | ----------- |",
      ...universeRows.map(
        (r) =>
          `| ${r.operatorName} | ${r.strongCountries.join(", ") || "—"} | ${r.rankingReadyProjects.length} | ${r.argentinaReady ? "Yes" : "No"} | ${r.highestGap} |`
      ),
      "",
      "## Coverage rolls",
      "",
      `- Ready for Argentina (strong presence): **${universeRows.filter((r) => r.argentinaReady).length}**`,
      `- Ready for Mexico: **${universeRows.filter((r) => r.mexicoReady).length}**`,
      `- Operators with ≥1 Ranking Ready project type: **${universeRows.filter((r) => r.rankingReadyProjects.length).length}**`,
      "",
      "Wave 3 Argentina operators remain **research-stage** (no Master ID) — see wave-3-cohort.",
      "",
    ].join("\n")
  );

  console.log(
    JSON.stringify(
      {
        deals: dealsOut.map((d) => ({
          label: d.label,
          rr: d.rankingReadyCount,
          thinOrZero: d.thinOrZero,
          outcome: d.outcome,
          researchStageRR: (d.researchStageArgentina || []).filter((r) => r.readiness === READINESS_STATUS.RANKING_READY)
            .length,
        })),
        argentinaReadyProduction: universeRows.filter((r) => r.argentinaReady).length,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
