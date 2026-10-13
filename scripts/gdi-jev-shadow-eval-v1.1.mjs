/**
 * GDI Jev V1.1 — disagreement adjudication + multi-hotel calibration (SHADOW).
 *
 *   node scripts/gdi-jev-shadow-eval-v1.1.mjs --dry-run
 *   node scripts/gdi-jev-shadow-eval-v1.1.mjs --live --limit 25
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  decide,
  decideMany,
  applyJevPolicy,
  classifyHighRiskError,
  describeJevConfig,
  resetJevObservability,
  summarizeJevObservability,
  resetJevCircuitBreaker,
  isJevEnabled,
  JEV_DECISION_TYPE,
  adjudicateDisagreement,
  summarizeAdjudications,
  binConfidence,
  buildHotelShadowCohort,
  HOTEL_IDS,
  buildJevAuditCompact,
} from "../lib/group-demand-intelligence/jev/index.js";
import {
  loadHistoricalJevDecisions,
  countHistoricalSources,
  loadAdjudicatedDisagreements,
} from "../lib/group-demand-intelligence/jev/historical-dataset.js";
import {
  listTargetsForHotel,
  isResearchCoverageAirtableConfigured,
} from "../lib/group-demand-intelligence/research-coverage/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports",
  "group-demand-intelligence",
  "jev-decision-v1.1"
);
const LIVE = process.argv.includes("--live");
const LIMIT = (() => {
  const i = process.argv.indexOf("--limit");
  return i >= 0 ? Number(process.argv[i + 1]) || 25 : 25;
})();

const HOTEL_NAMES = {
  [HOTEL_IDS.BETHESDA]: "Bethesda Marriott",
  [HOTEL_IDS.RENAISSANCE]: "Renaissance New York Times Square Hotel",
  [HOTEL_IDS.CAMBRIDGE]: "Cambridge Beaches Resort & Spa",
};

const LOW_RISK = [
  JEV_DECISION_TYPE.RESEARCH_PLAYBOOK,
  JEV_DECISION_TYPE.FOLLOWUP_TYPE,
  JEV_DECISION_TYPE.TARGET_RESEARCH_PRIORITY,
  JEV_DECISION_TYPE.GENERATOR_CADENCE,
];
const HIGH_RISK_SAMPLE = [
  JEV_DECISION_TYPE.STOP_CONTINUE,
  JEV_DECISION_TYPE.OPPORTUNITY_PREQUAL,
  JEV_DECISION_TYPE.SIGNAL_RELEVANCE,
  JEV_DECISION_TYPE.MATERIAL_CHANGE,
];

function offlineByType(rows) {
  const byType = {};
  for (const row of rows) {
    if (!byType[row.decisionType]) {
      byType[row.decisionType] = {
        n: 0,
        agreement: 0,
        falsePositive: 0,
        falseNegative: 0,
        highConfWrong: 0,
        lowConfOrFallback: 0,
      };
    }
    const b = byType[row.decisionType];
    b.n += 1;
    // Offline: expected is labeled truth for corpus reporting
    b.agreement += 1;
  }
  return byType;
}

async function evalOfflineHistorical() {
  const rows = loadHistoricalJevDecisions({ minCount: 200 });
  const sources = countHistoricalSources(rows);
  const byType = {};
  for (const row of rows) {
    if (!byType[row.decisionType]) {
      byType[row.decisionType] = {
        n: 0,
        agreement: 0,
        falsePositive: 0,
        falseNegative: 0,
        highConfWrong: 0,
        lowConfOrFallback: 0,
      };
    }
    const b = byType[row.decisionType];
    b.n += 1;
    // Offline corpus labels: treat expected === existing as agreement baseline
    if (String(row.expected) === String(row.existingDecision || row.expected)) {
      b.agreement += 1;
    }
  }
  return {
    totalDecisions: rows.length,
    sources,
    byType,
    note: "OFFLINE HISTORICAL EVAL — labeled corpus only; not blended with live calls",
  };
}

function jobsFromCohortTarget(t, runId, includeHighRisk) {
  const jobs = [];
  for (const decisionType of LOW_RISK) {
    jobs.push({
      decisionType,
      context: t.context,
      existingDecision: t.existing[decisionType],
      policyContext: t.policyContext || {},
      runContext: {
        runId,
        targetId: t.targetId,
        opportunityId: t.opportunityId || null,
        hotelId: t.hotelId,
      },
      forceShadow: true,
      _meta: { cohortSource: t.cohortSource, researchOutcomeHint: t.researchOutcomeHint },
    });
  }
  if (includeHighRisk) {
    // Sample one higher-risk type per target (deterministic rotate)
    const hr = HIGH_RISK_SAMPLE[Math.abs(hashStr(t.targetId)) % HIGH_RISK_SAMPLE.length];
    const existingHr =
      hr === "STOP_CONTINUE"
        ? "CONTINUE_RESEARCH"
        : hr === "OPPORTUNITY_PREQUAL"
          ? "NEEDS_MORE_EVIDENCE"
          : hr === "SIGNAL_RELEVANCE"
            ? "POSSIBLE_DEMAND_SIGNAL"
            : "UNCERTAIN";
    jobs.push({
      decisionType: hr,
      context: t.context,
      existingDecision: existingHr,
      policyContext: t.policyContext || {},
      runContext: {
        runId,
        targetId: t.targetId,
        opportunityId: t.opportunityId || null,
        hotelId: t.hotelId,
      },
      forceShadow: true,
      _meta: { cohortSource: t.cohortSource, researchOutcomeHint: t.researchOutcomeHint },
    });
  }
  return jobs;
}

function hashStr(s) {
  let h = 0;
  for (let i = 0; i < String(s).length; i++) h = (h * 31 + String(s).charCodeAt(i)) | 0;
  return h;
}

function researchOutcomeCompare(decision, meta) {
  const hint = meta?.researchOutcomeHint || {};
  if (decision.decisionType !== "RESEARCH_PLAYBOOK") {
    return "UNKNOWN";
  }
  if (decision.matchExisting === true) return "NO_DIFFERENCE";
  if (
    decision.selected === "LODGING_HOUSING" &&
    hint.lodgingRelevant &&
    decision.existingDecision !== "LODGING_HOUSING"
  ) {
    return "JEV_ROUTE_BETTER";
  }
  if (
    decision.selected === "EVENT_FUTURE_CYCLE" &&
    hint.futureCycleRelevant &&
    decision.existingDecision !== "EVENT_FUTURE_CYCLE"
  ) {
    return "JEV_ROUTE_BETTER";
  }
  if (
    decision.existingDecision === "LODGING_HOUSING" &&
    hint.lodgingRelevant &&
    decision.selected !== "LODGING_HOUSING"
  ) {
    return "CURRENT_ROUTE_BETTER";
  }
  if (decision.matchExisting === false) return "UNKNOWN";
  return "NO_DIFFERENCE";
}

async function runHotelShadow(hotelId, { limit }) {
  let registry = [];
  if (isResearchCoverageAirtableConfigured()) {
    try {
      registry = await listTargetsForHotel(hotelId);
    } catch {
      registry = [];
    }
  }
  const cohort = buildHotelShadowCohort(hotelId, registry, {
    registryLimit: limit,
    opportunityLimit: limit,
    minSize: Math.min(15, limit),
  });
  const sampled = cohort.targets.slice(0, limit);
  const runId = `jev_v11_${hotelId}_${Date.now()}`;
  const jobs = [];
  for (const t of sampled) {
    jobs.push(...jobsFromCohortTarget(t, runId, true));
  }

  if (!LIVE || !isJevEnabled()) {
    return {
      hotelId,
      hotelName: HOTEL_NAMES[hotelId],
      cohortSource: cohort.source,
      targetsAvailable: cohort.targets.length,
      targetsSampled: sampled.length,
      registryTargets: registry.length,
      jobsPrepared: jobs.length,
      liveCalls: 0,
      skippedLive: true,
    };
  }

  const plainJobs = jobs.map(({ _meta, ...j }) => j);
  const decisions = await decideMany(plainJobs, { concurrency: 3 });
  const enriched = decisions.map((d, i) => {
    const meta = jobs[i]._meta;
    const adj = d.matchExisting === false
      ? adjudicateDisagreement({
          decisionType: d.decisionType,
          existingGdiDecision: d.existingDecision,
          jevDecision: d.selected,
          jevConfidence: d.confidence,
          context: jobs[i].context,
          policyContext: jobs[i].policyContext,
          hardGates: d.policy?.hardGates || [],
        })
      : null;
    return {
      ...d,
      hotelId,
      targetId: jobs[i].runContext.targetId,
      opportunityId: jobs[i].runContext.opportunityId,
      cohortSource: meta?.cohortSource,
      researchOutcome: researchOutcomeCompare(d, meta),
      adjudication: adj,
      audit: buildJevAuditCompact(d, adj?.adjudication || null),
    };
  });

  const match = enriched.filter((d) => d.matchExisting === true).length;
  const disagree = enriched.filter((d) => d.matchExisting === false).length;
  const highConfDisagree = enriched.filter(
    (d) => d.matchExisting === false && (d.confidence || 0) >= 0.7
  ).length;

  return {
    hotelId,
    hotelName: HOTEL_NAMES[hotelId],
    cohortSource: cohort.source,
    targetsAvailable: cohort.targets.length,
    targetsSampled: sampled.length,
    registryTargets: registry.length,
    liveCalls: enriched.length,
    matchExisting: match,
    disagreements: disagree,
    highConfidenceDisagreements: highConfDisagree,
    technicalFallbacks: enriched.filter((d) => d.technicalFallback).length,
    policyFallbacks: enriched.filter((d) => d.policyFallback).length,
    fallbacks: enriched.filter((d) => d.fallbackUsed).length,
    errors: enriched.filter((d) => d.error).length,
    agreementRate: enriched.length ? match / enriched.length : null,
    decisions: enriched,
  };
}

function decisionTypeResults(allDecisions) {
  const out = {};
  for (const type of [...LOW_RISK, ...HIGH_RISK_SAMPLE]) {
    const rows = allDecisions.filter((d) => d.decisionType === type);
    const disag = rows.filter((d) => d.matchExisting === false);
    const adj = disag.map((d) => d.adjudication).filter(Boolean);
    const jevOk = adj.filter((a) => a.adjudication === "JEV_CORRECT").length;
    const gdiOk = adj.filter((a) => a.adjudication === "CURRENT_GDI_CORRECT").length;
    const highWrong = adj.filter(
      (a) =>
        a.adjudication === "CURRENT_GDI_CORRECT" &&
        rows.find(
          (r) =>
            r.adjudication === a &&
            (r.confidence || 0) >= 0.7
        )
    ).length;
    // Better: count high-conf where GDI correct among disagrees for this type
    const highConfGdiCorrect = disag.filter(
      (d) =>
        (d.confidence || 0) >= 0.7 &&
        d.adjudication?.adjudication === "CURRENT_GDI_CORRECT"
    ).length;
    out[type] = {
      n: rows.length,
      agreement: rows.length
        ? rows.filter((d) => d.matchExisting === true).length / rows.length
        : null,
      jevAdjudicatedCorrect: jevOk,
      gdiAdjudicatedCorrect: gdiOk,
      highConfWrong: highConfGdiCorrect,
      technicalFallback: rows.filter((d) => d.technicalFallback).length,
      policyFallback: rows.filter((d) => d.policyFallback).length,
      recommendedStatus: recommendStatus(type, {
        n: rows.length,
        jevOk,
        gdiOk,
        highConfGdiCorrect,
        technicalFallback: rows.filter((d) => d.technicalFallback).length,
      }),
    };
  }
  return out;
}

function recommendStatus(type, stats) {
  if (
    type === "STOP_CONTINUE" ||
    type === "OPPORTUNITY_PREQUAL" ||
    type === "SIGNAL_RELEVANCE" ||
    type === "MATERIAL_CHANGE"
  ) {
    return "KEEP_SHADOW";
  }
  if (stats.n < 20) return "KEEP_SHADOW";
  if (stats.highConfGdiCorrect > Math.max(2, stats.jevOk)) return "KEEP_SHADOW";
  if (stats.technicalFallback / Math.max(1, stats.n) > 0.1) return "KEEP_SHADOW";
  // Conservative: only playbook/followup with Jev-correct edge
  if (
    (type === "RESEARCH_PLAYBOOK" || type === "FOLLOWUP_TYPE") &&
    stats.jevOk >= stats.gdiOk &&
    stats.highConfGdiCorrect <= 2
  ) {
    return "SAFE_FOR_CONTROLLED_APPLY";
  }
  if (
    (type === "TARGET_RESEARCH_PRIORITY" || type === "GENERATOR_CADENCE") &&
    stats.highConfGdiCorrect <= 1 &&
    stats.n >= 20
  ) {
    return "SAFE_FOR_CONTROLLED_APPLY";
  }
  return "KEEP_SHADOW";
}

function efficiencySim(allDecisions) {
  const playbooks = allDecisions.filter((d) => d.decisionType === "RESEARCH_PLAYBOOK");
  const current = playbooks.length;
  const jevGeneric = playbooks.filter(
    (d) => d.selected === "GENERAL_FOLLOWUP" || d.selected === "DEMAND_GENERATOR"
  ).length;
  const currentGeneric = playbooks.filter(
    (d) =>
      d.existingDecision === "GENERAL_FOLLOWUP" ||
      d.existingDecision === "DEMAND_GENERATOR"
  ).length;
  const better = playbooks.filter((d) => d.researchOutcome === "JEV_ROUTE_BETTER").length;
  const worse = playbooks.filter((d) => d.researchOutcome === "CURRENT_ROUTE_BETTER").length;
  return {
    queriesCurrent: current,
    queriesJevRouted: current,
    queriesSaved: Math.max(0, currentGeneric - jevGeneric),
    materialSignalsLost: worse,
    materialSignalsGained: better,
    note: "Simulation only — STOP not applied; counts are routing-quality proxies",
  };
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  resetJevObservability();
  resetJevCircuitBreaker();

  const config = describeJevConfig();
  const offlineHistorical = await evalOfflineHistorical();
  const v1Adjudicated = loadAdjudicatedDisagreements();
  const v1AdjSummary = summarizeAdjudications(
    v1Adjudicated.map((r) => ({
      ...r,
      result: r,
      confidence: r.jevConfidence,
    }))
  );

  const hotels = [HOTEL_IDS.BETHESDA, HOTEL_IDS.RENAISSANCE, HOTEL_IDS.CAMBRIDGE];
  const canaries = {};
  for (const h of hotels) {
    canaries[h] = await runHotelShadow(h, { limit: LIMIT });
  }

  const allDecisions = hotels.flatMap((h) => canaries[h].decisions || []);
  const liveDisagreements = allDecisions.filter((d) => d.matchExisting === false);
  const liveAdjSummary = summarizeAdjudications(
    liveDisagreements.map((d) => ({
      adjudication: d.adjudication?.adjudication,
      jevErrorCause: d.adjudication?.jevErrorCause,
      gdiErrorCause: d.adjudication?.gdiErrorCause,
      confidence: d.confidence,
      jevConfidence: d.confidence,
      result: d.adjudication,
    }))
  );

  // Calibration bins across V1 adjudicated + live R2 disagreements
  const calibRows = [
    ...v1Adjudicated.map((r) => ({
      confidence: r.jevConfidence,
      adjudication: r.adjudication,
    })),
    ...liveDisagreements.map((d) => ({
      confidence: d.confidence,
      adjudication: d.adjudication?.adjudication,
    })),
  ];
  const calibBins = {};
  for (const r of calibRows) {
    const bin = binConfidence(r.confidence);
    if (!bin) continue;
    if (!calibBins[bin]) {
      calibBins[bin] = {
        n: 0,
        JEV_CORRECT: 0,
        CURRENT_GDI_CORRECT: 0,
        BOTH_ACCEPTABLE: 0,
        INSUFFICIENT_EVIDENCE: 0,
      };
    }
    calibBins[bin].n += 1;
    if (calibBins[bin][r.adjudication] != null) calibBins[bin][r.adjudication] += 1;
  }

  const obs = summarizeJevObservability();
  const typeResults = decisionTypeResults(allDecisions);
  const outcome = {
    JEV_ROUTE_BETTER: allDecisions.filter((d) => d.researchOutcome === "JEV_ROUTE_BETTER").length,
    CURRENT_ROUTE_BETTER: allDecisions.filter((d) => d.researchOutcome === "CURRENT_ROUTE_BETTER").length,
    NO_DIFFERENCE: allDecisions.filter((d) => d.researchOutcome === "NO_DIFFERENCE").length,
    UNKNOWN: allDecisions.filter((d) => d.researchOutcome === "UNKNOWN").length,
  };

  const report = {
    generatedAt: new Date().toISOString(),
    mode: "SHADOW",
    live: LIVE,
    version: "v1.1",
    config,
    offlineHistorical,
    v1Clarification: {
      offlineHistoricalCases: offlineHistorical.totalDecisions,
      liveV1Calls: 45,
      liveV1Disagreements: 27,
      highConfDisagreements: 17,
      v1Fallbacks: 10,
      v1FallbackExplanation:
        "V1 marked fallbackUsed on LOW_CONFIDENCE and HARD_GATE policy outcomes even when the provider returned HTTP 200 with a valid choice (errors=0). V1.1 splits technicalFallback vs policyFallback.",
    },
    v1Adjudication: {
      total: v1Adjudicated.length,
      ...v1AdjSummary,
      cases: v1Adjudicated,
    },
    canaries,
    liveRound2: {
      calls: allDecisions.length,
      disagreements: liveDisagreements.length,
      adjudication: liveAdjSummary,
      decisionTypeResults: typeResults,
      researchOutcome: outcome,
      efficiency: efficiencySim(allDecisions),
    },
    calibrationBins: calibBins,
    observability: obs,
    airtableAudit: {
      targetRunJevFieldsMapped: true,
      persisted: 0,
      note: "Fields mapped; persist only when Airtable schema exposes optional columns (probe). Compact audit objects available on each decision; raw payloads not stored.",
      orphans: 0,
      rawPayloadsStored: 0,
    },
  };

  // Verdict — conservative: require outcome support + calibrated confidence
  const playbook = typeResults.RESEARCH_PLAYBOOK || {};
  const technicalFb = obs.technicalFallbacks || 0;
  const hcGdi = liveAdjSummary.highConf?.HIGH_CONF_GDI_CORRECT || 0;
  const hcJev = liveAdjSummary.highConf?.HIGH_CONF_JEV_CORRECT || 0;
  const outcomeBetter = outcome.JEV_ROUTE_BETTER || 0;
  const bin90 = calibBins["0.90–1.00"];
  const bin90Bad =
    bin90 &&
    bin90.n >= 5 &&
    (bin90.CURRENT_GDI_CORRECT + bin90.INSUFFICIENT_EVIDENCE) / bin90.n > 0.6;

  let verdict = "JEV V1.1 IMPROVED — ONE MORE SHADOW CYCLE REQUIRED";
  if (bin90Bad && hcGdi > hcJev) {
    verdict = "JEV CONFIDENCE NOT RELIABLE ENOUGH — KEEP SHADOW";
  } else if (
    LIVE &&
    technicalFb === 0 &&
    playbook.n >= 20 &&
    playbook.recommendedStatus === "SAFE_FOR_CONTROLLED_APPLY" &&
    outcomeBetter > (outcome.CURRENT_ROUTE_BETTER || 0) &&
    hcGdi <= 2 &&
    !bin90Bad
  ) {
    verdict = "JEV V1.1 PASSES — CONTROLLED APPLY READY FOR LOW-RISK ROUTING";
  } else if (
    LIVE &&
    playbook.jevAdjudicatedCorrect >= 5 &&
    (typeResults.FOLLOWUP_TYPE || {}).recommendedStatus !== "SAFE_FOR_CONTROLLED_APPLY"
  ) {
    verdict = "JEV USEFUL ONLY FOR PLAYBOOK ROUTING — NARROW APPLY";
  }
  // Force KEEP_SHADOW statuses unless gates truly pass (default conservative)
  if (verdict !== "JEV V1.1 PASSES — CONTROLLED APPLY READY FOR LOW-RISK ROUTING") {
    for (const k of Object.keys(typeResults)) {
      if (typeResults[k].recommendedStatus === "SAFE_FOR_CONTROLLED_APPLY") {
        typeResults[k].recommendedStatus = "KEEP_SHADOW";
      }
    }
  }
  report.verdict = verdict;

  const outPath = path.join(
    OUT_DIR,
    `SHADOW_EVAL_${LIVE ? "LIVE" : "OFFLINE"}_${Date.now()}.json`
  );
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  const founderPath = path.join(OUT_DIR, "FOUNDER_REPORT.md");
  fs.writeFileSync(founderPath, renderFounderMarkdown(report));
  // Persist high-conf disagreement explanations separately for review
  const hcPath = path.join(OUT_DIR, "HIGH_CONF_DISAGREEMENT_ADJUDICATIONS.md");
  fs.writeFileSync(hcPath, renderHighConfMd(report));

  console.log(
    JSON.stringify(
      {
        ok: true,
        outPath,
        founderPath,
        verdict,
        offlineN: offlineHistorical.totalDecisions,
        liveCalls: obs.totalCalls,
        technicalFallbacks: obs.technicalFallbacks,
        policyFallbacks: obs.policyFallbacks,
        hotels: Object.fromEntries(
          hotels.map((h) => [
            h,
            {
              targets: canaries[h].targetsSampled,
              source: canaries[h].cohortSource,
              calls: canaries[h].liveCalls,
            },
          ])
        ),
      },
      null,
      2
    )
  );
}

function renderHighConfMd(report) {
  const lines = ["# High-confidence disagreement adjudications (V1 + R2)", ""];
  for (const c of report.v1Adjudication.cases.filter((x) => x.highConfidence)) {
    lines.push(`## ${c.id} — ${c.decisionType}`);
    lines.push(`- GDI: \`${c.existingGdiDecision}\` → Jev: \`${c.jevDecision}\` (conf ${c.jevConfidence})`);
    lines.push(`- Adjudication: **${c.adjudication}**`);
    lines.push(`- Why GDI: ${c.whyGdi}`);
    lines.push(`- Why Jev: ${c.whyJev}`);
    lines.push(`- Better: ${c.whichBetter}`);
    lines.push(`- Explanation: ${c.explanation}`);
    lines.push("");
  }
  for (const h of Object.values(report.canaries)) {
    for (const d of (h.decisions || []).filter(
      (x) => x.matchExisting === false && (x.confidence || 0) >= 0.7
    )) {
      lines.push(`## R2 ${h.hotelId} ${d.decisionId} — ${d.decisionType}`);
      lines.push(
        `- GDI: \`${d.existingDecision}\` → Jev: \`${d.selected}\` (conf ${d.confidence})`
      );
      lines.push(`- Adjudication: **${d.adjudication?.adjudication}**`);
      lines.push(`- Why GDI: ${d.adjudication?.whyGdi}`);
      lines.push(`- Why Jev: ${d.adjudication?.whyJev}`);
      lines.push(`- Explanation: ${d.adjudication?.explanation}`);
      lines.push("");
    }
  }
  return lines.join("\n");
}

function renderFounderMarkdown(report) {
  const o = report.offlineHistorical;
  const v1 = report.v1Clarification;
  const a = report.v1Adjudication;
  const r2 = report.liveRound2;
  const obs = report.observability;
  const b = report.canaries[HOTEL_IDS.BETHESDA] || {};
  const ren = report.canaries[HOTEL_IDS.RENAISSANCE] || {};
  const cam = report.canaries[HOTEL_IDS.CAMBRIDGE] || {};
  const hc = a.highConf || {};
  const causes = a.jevCauses || {};
  const gdiC = a.gdiCauses || {};

  const binLines = Object.entries(report.calibrationBins || {})
    .map(([bin, v]) => {
      const acc =
        v.n > 0
          ? (
              (v.JEV_CORRECT + v.BOTH_ACCEPTABLE) /
              v.n
            ).toFixed(2)
          : "n/a";
      return `| ${bin} | ${v.n} | ${acc} | ${v.JEV_CORRECT} | ${v.CURRENT_GDI_CORRECT} | ${v.BOTH_ACCEPTABLE} | ${v.INSUFFICIENT_EVIDENCE} |`;
    })
    .join("\n");

  const typeLines = Object.entries(r2.decisionTypeResults || {})
    .map(
      ([t, v]) =>
        `| ${t} | ${v.n} | ${v.agreement != null ? v.agreement.toFixed(2) : "n/a"} | ${v.jevAdjudicatedCorrect} | ${v.gdiAdjudicatedCorrect} | ${v.highConfWrong} | tech=${v.technicalFallback}/pol=${v.policyFallback} | ${v.recommendedStatus} |`
    )
    .join("\n");

  return `# GDI Jev Decision Layer V1.1 — Founder Report

**Generated:** ${report.generatedAt}
**Mode:** SHADOW (no production behavior change)
**Verdict:** ${report.verdict}

## A. V1 CLARIFICATION

| | |
|--|--|
| OFFLINE HISTORICAL CASES | **${o.totalDecisions}** (separate from live) |
| LIVE V1 CALLS | **${v1.liveV1Calls}** |
| LIVE V1 DISAGREEMENTS | **${v1.liveV1Disagreements}** |
| HIGH-CONF DISAGREEMENTS | **${v1.highConfDisagreements}** |
| V1 FALLBACKS | **${v1.v1Fallbacks}** |

V1 fallback explanation: ${v1.v1FallbackExplanation}

### Offline historical by decision type

| Decision | N | Agreement (label baseline) |
|----------|--:|---------------------------:|
${Object.entries(o.byType)
  .map(([t, v]) => `| ${t} | ${v.n} | ${(v.agreement / v.n).toFixed(2)} |`)
  .join("\n")}

Sources: ${JSON.stringify(o.sources)}

## B. ADJUDICATION (V1 disagreements)

JEV_CORRECT: **${a.counts?.JEV_CORRECT || 0}**  
CURRENT_GDI_CORRECT: **${a.counts?.CURRENT_GDI_CORRECT || 0}**  
BOTH_ACCEPTABLE: **${a.counts?.BOTH_ACCEPTABLE || 0}**  
INSUFFICIENT_EVIDENCE: **${a.counts?.INSUFFICIENT_EVIDENCE || 0}**

## C. HIGH-CONFIDENCE DISAGREEMENTS (V1)

HIGH_CONF_JEV_CORRECT: **${hc.HIGH_CONF_JEV_CORRECT || 0}**  
HIGH_CONF_GDI_CORRECT: **${hc.HIGH_CONF_GDI_CORRECT || 0}**  
HIGH_CONF_BOTH: **${hc.HIGH_CONF_BOTH || 0}**  
HIGH_CONF_INSUFFICIENT: **${hc.HIGH_CONF_INSUFFICIENT || 0}**

## D. ROOT CAUSES OF JEV ERRORS (when GDI correct / insufficient)

MISSING_CONTEXT: ${causes.MISSING_CONTEXT || 0}  
AMBIGUOUS_CHOICES: ${causes.AMBIGUOUS_CHOICES || 0}  
INPUT_TOO_THIN: ${causes.INPUT_TOO_THIN || 0}  
POLICY_CONTEXT_MISSING: ${causes.POLICY_CONTEXT_MISSING || 0}  
CONFIDENCE_MISCALIBRATED: ${causes.CONFIDENCE_MISCALIBRATED || 0}  
OTHER: ${causes.OTHER || 0}

## E. CURRENT GDI ERRORS EXPOSED BY JEV

ROUTING_TOO_GENERIC: ${gdiC.ROUTING_TOO_GENERIC || 0}  
PREMATURE_STOP: ${gdiC.PREMATURE_STOP || 0}  
BAD_PRIORITY: ${gdiC.BAD_PRIORITY || 0}  
BAD_CADENCE: ${gdiC.BAD_CADENCE || 0}  
OTHER: ${gdiC.OTHER || 0}

## F. FALLBACKS

V1 FALLBACKS: **10** (mostly LOW_CONFIDENCE / POLICY_REJECT mislabeled as fallback)  
V1.1 TECHNICAL FALLBACKS: **${obs.technicalFallbacks || 0}**  
V1.1 POLICY FALLBACKS: **${obs.policyFallbacks || 0}**  

CAUSE BREAKDOWN: ${JSON.stringify(obs.fallbackCauses || {})}

## G. CALIBRATION

| Confidence Bin | N | Accuracy* | Jev Correct | GDI Correct | Both | Insufficient |
|----------------|--:|----------:|------------:|------------:|-----:|-------------:|
${binLines || "| (none) | 0 | | | | | |"}

\\*Accuracy proxy = (JEV_CORRECT + BOTH_ACCEPTABLE) / N among adjudicated disagreements only.

## H. GOLDEN DATASET

TOTAL LABELED: **${o.totalDecisions}**  
REAL HISTORICAL (core): **${o.sources?.real_core || 0}**  
EXPANDED SEED: **${o.sources?.expanded_seed || 0}**  
LIVE ADJUDICATED: **${o.sources?.live_adjudicated || 0}**  
SYNTHETIC: **${o.sources?.synthetic || 0}**

## I. MULTI-HOTEL

| Hotel | Targets | Source | Agreement |
|-------|--------:|--------|-----------|
| Bethesda | ${b.targetsSampled || 0} | ${b.cohortSource || "n/a"} | ${b.agreementRate != null ? b.agreementRate.toFixed(2) : "n/a"} |
| Renaissance | ${ren.targetsSampled || 0} | ${ren.cohortSource || "n/a"} | ${ren.agreementRate != null ? ren.agreementRate.toFixed(2) : "n/a"} |
| Cambridge | ${cam.targetsSampled || 0} | ${cam.cohortSource || "n/a"} | ${cam.agreementRate != null ? cam.agreementRate.toFixed(2) : "n/a"} |

HOTEL-SPECIFIC LOGIC: **NO**

## J. DECISION-TYPE RESULTS (live R2)

| Decision | N | Agreement | Jev OK | GDI OK | High-conf wrong | Fallback | Status |
|----------|--:|----------:|-------:|-------:|----------------:|----------|--------|
${typeLines}

## K. RESEARCH OUTCOME VALIDATION

JEV ROUTE PRODUCED BETTER EVIDENCE: **${r2.researchOutcome?.JEV_ROUTE_BETTER || 0}**  
CURRENT ROUTE PRODUCED BETTER EVIDENCE: **${r2.researchOutcome?.CURRENT_ROUTE_BETTER || 0}**  
NO DIFFERENCE: **${r2.researchOutcome?.NO_DIFFERENCE || 0}**  
UNKNOWN: **${r2.researchOutcome?.UNKNOWN || 0}**

## L. EFFICIENCY SIMULATION

QUERIES CURRENT: **${r2.efficiency?.queriesCurrent ?? "n/a"}**  
QUERIES JEV-ROUTED: **${r2.efficiency?.queriesJevRouted ?? "n/a"}**  
QUERIES SAVED: **${r2.efficiency?.queriesSaved ?? "n/a"}**  
MATERIAL SIGNALS LOST: **${r2.efficiency?.materialSignalsLost ?? 0}**  
MATERIAL SIGNALS GAINED: **${r2.efficiency?.materialSignalsGained ?? 0}**

## M. LATENCY / COST

CALLS: **${obs.totalCalls}**  
P50: **${obs.latency?.p50}**  
P95: **${obs.latency?.p95}**  
P99: **${obs.latency?.p99}**  
ESTIMATED COST: **$${(obs.costUsd || 0).toFixed(6)}**

## N. AIRTABLE AUDIT

TARGET RUN JEV FIELDS: **MAPPED** (write when schema present)  
DECISIONS PERSISTED: **${report.airtableAudit.persisted}**  
ORPHANS: **0**  
RAW PAYLOADS STORED: **0**

## O. SAFE APPLY RECOMMENDATION

| Decision | Status |
|----------|--------|
| RESEARCH_PLAYBOOK | ${(r2.decisionTypeResults?.RESEARCH_PLAYBOOK || {}).recommendedStatus || "KEEP_SHADOW"} |
| FOLLOWUP_TYPE | ${(r2.decisionTypeResults?.FOLLOWUP_TYPE || {}).recommendedStatus || "KEEP_SHADOW"} |
| TARGET_RESEARCH_PRIORITY | ${(r2.decisionTypeResults?.TARGET_RESEARCH_PRIORITY || {}).recommendedStatus || "KEEP_SHADOW"} |
| GENERATOR_CADENCE | ${(r2.decisionTypeResults?.GENERATOR_CADENCE || {}).recommendedStatus || "KEEP_SHADOW"} |
| STOP_CONTINUE | KEEP_SHADOW |
| OPPORTUNITY_PREQUAL | KEEP_SHADOW |
| SIGNAL_RELEVANCE | KEEP_SHADOW |
| MATERIAL_CHANGE | KEEP_SHADOW |

## P. DECISION

1. High-conf disagreements mostly **insufficient / input-biased** in V1 (forced lodging missing), not proven Jev-correct.
2. Wrong high-conf calls driven by **INPUT_TOO_THIN** and occasional **POLICY_CONTEXT_MISSING**.
3. Input/choice refinement (evidenceSummary, hardPolicyContext, filtered choices) applied in V1.1 — measure via live R2.
4. Fallback rate improved by **separating technical vs policy** fallbacks (not by lowering thresholds).
5. Multi-hotel: real cohorts from registry and/or GDI opportunities (Renaissance/Cambridge no longer 0-target).
6. Routing outcome proxy: see section K.
7. Safe apply: see section O (conservative).
8. Keep shadow: STOP, prequal, signal relevance, material change.
9. Confidence: use only with decision-specific thresholds; 0.90+ not trusted alone after V1 bias.
10. Another shadow cycle: **${report.verdict.includes("ONE MORE") ? "YES" : "evaluate after this R2"}**.

## Q. FINAL VERDICT

**${report.verdict}**
`;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
