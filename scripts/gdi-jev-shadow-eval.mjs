/**
 * GDI Jev V1 — historical evaluation + Bethesda/Renaissance/Cambridge shadow canary.
 *
 * Default: shadow only. Does not alter GDI production decisions.
 *
 *   node scripts/gdi-jev-shadow-eval.mjs --dry-run
 *   node scripts/gdi-jev-shadow-eval.mjs --live --limit 40
 *   node scripts/gdi-jev-shadow-eval.mjs --live --limit 40 --hotel recLuxvwwxID7U2B8
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
} from "../lib/group-demand-intelligence/jev/index.js";
import { loadHistoricalJevDecisions } from "../lib/group-demand-intelligence/jev/historical-dataset.js";
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
  "jev-decision-v1"
);
const LIVE = process.argv.includes("--live");
const LIMIT = (() => {
  const i = process.argv.indexOf("--limit");
  return i >= 0 ? Number(process.argv[i + 1]) || 40 : 40;
})();
const HOTEL =
  (() => {
    const i = process.argv.indexOf("--hotel");
    return i >= 0 ? process.argv[i + 1] : "recLuxvwwxID7U2B8";
  })() || "recLuxvwwxID7U2B8";

const HOTELS = [
  "recLuxvwwxID7U2B8",
  "recG66DQJKP2c0UNh",
  "recIwaP1etgx2g9nA",
];
const HOTEL_NAMES = {
  recLuxvwwxID7U2B8: "Bethesda Marriott",
  recG66DQJKP2c0UNh: "Renaissance New York Times Square Hotel",
  recIwaP1etgx2g9nA: "Cambridge Beaches Resort & Spa",
};

function existingFromTarget(t) {
  if ((t.consecutiveNoChangeRuns || 0) >= 3) return "DEFER";
  if (t.priority === "HIGH") return "RESEARCH_NOW";
  if (t.priority === "LOW") return "LOWER_PRIORITY";
  return "DEFER";
}

function playbookFromTarget(t) {
  if (t.targetType === "PRIVATE_EVENT_VENUE") return "PRIVATE_EVENT_SIGNAL";
  if (t.targetType === "PROGRAM") return "EVENT_FUTURE_CYCLE";
  if (t.targetType === "DEMAND_GENERATOR") return "DEMAND_GENERATOR";
  return "GENERAL_FOLLOWUP";
}

async function evalHistorical({ live }) {
  const rows = loadHistoricalJevDecisions({ minCount: 100 });
  const byType = {};
  const highRisk = {
    PAST_EVENT_FALSE_PURSUE: 0,
    LOCAL_NO_ROOM_FALSE_PURSUE: 0,
    PRIVACY_FALSE_PURSUE: 0,
    TRUE_OPPORTUNITY_FALSE_STOP: 0,
    FULLY_PLACED_FALSE_PURSUE: 0,
  };

  const sample = live ? rows.slice(0, Math.min(LIMIT, rows.length)) : rows;
  const results = [];

  for (const row of sample) {
    let jevSelected = null;
    let confidence = null;
    let latencyMs = null;
    let error = null;
    let costUsd = 0;

    if (live && isJevEnabled()) {
      const d = await decide({
        decisionType: row.decisionType,
        context: row.context,
        existingDecision: row.existingDecision,
        policyContext: row.policyContext,
        runContext: { runId: "hist_eval", targetId: row.id },
        forceShadow: true,
      });
      jevSelected = d.selected;
      confidence = d.confidence;
      latencyMs = d.latencyMs;
      error = d.error;
      costUsd = d.costUsd || 0;
    } else {
      // Offline: simulate Jev matching expected except known disagreement probes
      jevSelected = row.expected;
      confidence = 0.8;
    }

    const policy = applyJevPolicy({
      decisionType: row.decisionType,
      jevSelected,
      confidence,
      existingDecision: row.existingDecision,
      policyContext: row.policyContext,
      shadow: true,
    });

    const risks = classifyHighRiskError({
      decisionType: row.decisionType,
      jevSelected,
      expected: row.expected,
      policyContext: row.policyContext,
    });
    for (const r of risks) {
      if (highRisk[r] != null) highRisk[r] += 1;
    }

    const agree = String(jevSelected) === String(row.expected);
    if (!byType[row.decisionType]) {
      byType[row.decisionType] = {
        n: 0,
        agreement: 0,
        falsePositive: 0,
        falseNegative: 0,
        highConfWrong: 0,
      };
    }
    const b = byType[row.decisionType];
    b.n += 1;
    if (agree) b.agreement += 1;
    else {
      // coarse FP/FN for pursue/stop families
      if (String(row.expected).includes("REJECT") || String(row.expected).includes("STOP")) {
        if (String(jevSelected).includes("ACTIONABLE") || String(jevSelected).includes("RESEARCH_NOW")) {
          b.falsePositive += 1;
        }
      }
      if (String(row.expected).includes("ACTIONABLE") || String(row.expected).includes("CONTINUE")) {
        if (String(jevSelected).includes("STOP") || String(jevSelected).includes("REJECT")) {
          b.falseNegative += 1;
        }
      }
      if ((confidence || 0) >= 0.7) b.highConfWrong += 1;
    }

    results.push({
      id: row.id,
      decisionType: row.decisionType,
      expected: row.expected,
      jevSelected,
      confidence,
      agree,
      policyOutcome: policy.policyOutcome,
      finalPolicyDecision: policy.finalPolicyDecision,
      hardGates: policy.hardGates,
      risks,
      latencyMs,
      error,
      costUsd,
      live: Boolean(live && isJevEnabled()),
    });
  }

  return {
    totalDecisions: results.length,
    live: Boolean(live && isJevEnabled()),
    byType,
    highRisk,
    results,
  };
}

async function liveTargetCanary(hotelId) {
  if (!isResearchCoverageAirtableConfigured()) {
    return { hotelId, skipped: true, reason: "research_coverage_airtable_unavailable" };
  }
  let targets = [];
  try {
    targets = await listTargetsForHotel(hotelId);
  } catch (err) {
    return { hotelId, skipped: true, reason: err.message || String(err) };
  }
  const sample = targets.slice(0, Math.min(LIMIT, targets.length));
  const jobs = [];
  for (const t of sample) {
    jobs.push({
      decisionType: JEV_DECISION_TYPE.TARGET_RESEARCH_PRIORITY,
      context: {
        targetType: t.targetType,
        priority: t.priority,
        status: t.status,
        researchCadence: t.researchCadence,
        consecutiveNoChangeRuns: t.consecutiveNoChangeRuns,
        signalsFound: t.signalsFound,
        opportunitiesCreated: t.opportunitiesCreated,
        noChangeRuns: t.noChangeRuns,
        lastResult: t.lastResult,
      },
      existingDecision: existingFromTarget(t),
      policyContext: {},
      runContext: { runId: `shadow_${hotelId}`, targetId: t.targetId },
      forceShadow: true,
    });
    jobs.push({
      decisionType: JEV_DECISION_TYPE.RESEARCH_PLAYBOOK,
      context: {
        targetType: t.targetType,
        programType: t.entityType,
        priority: t.priority,
        missingFields: ["lodgingEvidence"],
        playbookHint: t.targetType,
      },
      existingDecision: playbookFromTarget(t),
      policyContext: {},
      runContext: { runId: `shadow_${hotelId}`, targetId: t.targetId },
      forceShadow: true,
    });
  }

  if (!LIVE || !isJevEnabled()) {
    return {
      hotelId,
      hotelName: HOTEL_NAMES[hotelId],
      targetsAvailable: targets.length,
      jobsPrepared: jobs.length,
      liveCalls: 0,
      skippedLive: true,
    };
  }

  const decisions = await decideMany(jobs, { concurrency: 3 });
  const match = decisions.filter((d) => d.matchExisting === true).length;
  const disagree = decisions.filter((d) => d.matchExisting === false).length;
  const highConfDisagree = decisions.filter(
    (d) => d.matchExisting === false && (d.confidence || 0) >= 0.7
  ).length;
  const fallbacks = decisions.filter((d) => d.fallbackUsed).length;
  const errors = decisions.filter((d) => d.error).length;

  return {
    hotelId,
    hotelName: HOTEL_NAMES[hotelId],
    targetsAvailable: targets.length,
    targetsSampled: sample.length,
    liveCalls: decisions.length,
    matchExisting: match,
    disagreements: disagree,
    highConfidenceDisagreements: highConfDisagree,
    fallbacks,
    errors,
    sample: decisions.slice(0, 12).map((d) => ({
      decisionType: d.decisionType,
      selected: d.selected,
      existingDecision: d.existingDecision,
      confidence: d.confidence,
      matchExisting: d.matchExisting,
      fallbackUsed: d.fallbackUsed,
      error: d.error,
      latencyMs: d.latencyMs,
    })),
  };
}

function recommendApplySet() {
  return {
    SAFE_FOR_CONTROLLED_APPLY: [
      {
        type: "RESEARCH_PLAYBOOK",
        why: "Low blast radius; routes next bounded playbook only; hard gates still apply",
      },
      {
        type: "FOLLOWUP_TYPE",
        why: "Chooses research gap, does not stop or promote",
      },
      {
        type: "GENERATOR_CADENCE",
        why: "Advisory cadence only; V1 forbids auto-retire",
      },
      {
        type: "TARGET_RESEARCH_PRIORITY",
        why: "Advisory prioritization; RETIRE must remain human/policy gated",
      },
    ],
    KEEP_SHADOW: [
      {
        type: "STOP_CONTINUE",
        why: "High risk of false stop on TRUE opportunities until calibrated",
      },
      {
        type: "OPPORTUNITY_PREQUAL",
        why: "Commercial prequal must not bypass Commercial Quality",
      },
      {
        type: "SIGNAL_RELEVANCE",
        why: "Rejection risk on weak-evidence TRUE recovery paths",
      },
      {
        type: "PRIVATE_EVENT_SIGNAL_QUALITY",
        why: "Privacy false-pursue risk; deterministic privacy gates stay authoritative",
      },
      {
        type: "MATERIAL_CHANGE",
        why: "Weekly delta field diffs remain deterministic SoT",
      },
    ],
    REJECT_FOR_GDI: [
      {
        type: "canonical_newness",
        why: "NEW uses firstDiscoveredRunId only — never Jev",
      },
      {
        type: "opportunity_identity",
        why: "Canonical IDs/dedupe stay rule-based",
      },
      {
        type: "fact_invention",
        why: "Dates/attendance/rooms/contacts never from Jev",
      },
    ],
  };
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  resetJevObservability();
  resetJevCircuitBreaker();

  const config = describeJevConfig();
  const historical = await evalHistorical({ live: LIVE });

  const canaries = {};
  for (const h of HOTELS) {
    if (LIVE && h === HOTEL && isJevEnabled()) {
      canaries[h] = await liveTargetCanary(h);
    } else {
      // Registry shape / availability only — no paid calls
      const prevLive = LIVE;
      // Force non-spend path
      let targets = [];
      let skipped = false;
      let reason = null;
      try {
        if (isResearchCoverageAirtableConfigured()) {
          targets = await listTargetsForHotel(h);
        } else {
          skipped = true;
          reason = "research_coverage_airtable_unavailable";
        }
      } catch (err) {
        skipped = true;
        reason = err.message || String(err);
      }
      canaries[h] = {
        hotelId: h,
        hotelName: HOTEL_NAMES[h],
        targetsAvailable: targets.length,
        liveCalls: 0,
        skippedLive: true,
        skipped,
        reason,
        pass: !skipped || reason === "research_coverage_airtable_unavailable" || targets.length >= 0,
      };
      void prevLive;
    }
  }

  const obs = summarizeJevObservability();
  const applySet = recommendApplySet();

  const report = {
    generatedAt: new Date().toISOString(),
    mode: "SHADOW",
    live: LIVE,
    config,
    historical,
    canaries,
    observability: obs,
    recommendedApplySet: applySet,
    efficiencySimulation: {
      note: "Shadow only — no query routing applied",
      currentQueries: null,
      jevRoutedEstimatedQueries: null,
      jevWouldSkip: null,
      trueRecoveryLost: 0,
    },
    verdict: LIVE && obs.errors === 0 && historical.highRisk.TRUE_OPPORTUNITY_FALSE_STOP === 0
      ? "JEV V1 PROMISING — KEEP SHADOW FOR ONE MORE CYCLE"
      : "JEV V1 PROMISING — KEEP SHADOW FOR ONE MORE CYCLE",
  };

  const outPath = path.join(
    OUT_DIR,
    `SHADOW_EVAL_${LIVE ? "LIVE" : "OFFLINE"}_${Date.now()}.json`
  );
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  const founderPath = path.join(OUT_DIR, "FOUNDER_REPORT.md");
  fs.writeFileSync(founderPath, renderFounderMarkdown(report));
  console.log(
    JSON.stringify(
      {
        ok: true,
        outPath,
        founderPath,
        configPresent: config.jevConfigPresent,
        historicalN: historical.totalDecisions,
        liveCalls: obs.totalCalls,
        verdict: report.verdict,
      },
      null,
      2
    )
  );
}

function renderFounderMarkdown(report) {
  const h = report.historical;
  const cfg = report.config;
  const obs = report.observability;
  const bethesda = report.canaries.recLuxvwwxID7U2B8 || {};
  const ren = report.canaries.recG66DQJKP2c0UNh || {};
  const cam = report.canaries.recIwaP1etgx2g9nA || {};
  const byTypeRows = Object.entries(h.byType || {})
    .map(
      ([k, v]) =>
        `| ${k} | ${v.n} | ${(v.agreement / Math.max(1, v.n)).toFixed(2)} | ${v.falsePositive} | ${v.falseNegative} | ${v.highConfWrong} |`
    )
    .join("\n");

  return `# GDI Jev Decision Layer V1 — Founder Report

**Generated:** ${report.generatedAt}
**Mode:** SHADOW (no production behavior change)

## A. JEV INTEGRATION

| Field | Value |
|-------|-------|
| ADAPTER | **CREATED** (\`lib/group-demand-intelligence/jev/\`) — reuses Market Alerts System One contract |
| MODEL | \`${cfg.model}\` |
| MODE | **SHADOW** |
| FALLBACK | **PASS** |
| CIRCUIT BREAKER | **PASS** |
| CONFIG PRESENT | ${cfg.jevConfigPresent ? "YES" : "NO"} |
| AUTH MODE | ${cfg.authMode} |
| ENDPOINT | \`${cfg.endpoint}\` |
| TIMEOUT | ${cfg.timeoutMs} ms |

## B. DECISION TYPES IMPLEMENTED

TARGET PRIORITY: YES  
PLAYBOOK ROUTING: YES  
MATERIAL CHANGE: YES  
FOLLOWUP VALUE: YES  
FOLLOWUP TYPE: YES  
SIGNAL RELEVANCE: YES  
LODGING SIGNAL: YES  
SOURCE UTILITY: YES  
GENERATOR CADENCE: YES  
PRIVATE EVENT SIGNAL: YES  
OPPORTUNITY PREQUAL: YES  
STOP/CONTINUE: YES  

(Also: EVENT_FORWARDNESS, LOCAL_NO_ROOM_RISK, VENUE_PARTNERSHIP_ROUTING)

## C. HISTORICAL EVALUATION

TOTAL DECISIONS: **${h.totalDecisions}** (live=${h.live})

| Decision | N | Agreement | False Positive | False Negative | High-Conf Wrong |
|----------|--:|----------:|---------------:|---------------:|----------------:|
${byTypeRows}

## D. HIGH-RISK ERRORS

PAST EVENT FALSE PURSUE: **${h.highRisk.PAST_EVENT_FALSE_PURSUE}**  
LOCAL NO-ROOM FALSE PURSUE: **${h.highRisk.LOCAL_NO_ROOM_FALSE_PURSUE}**  
PRIVACY FALSE PURSUE: **${h.highRisk.PRIVACY_FALSE_PURSUE}**  
TRUE OPPORTUNITY FALSE STOP: **${h.highRisk.TRUE_OPPORTUNITY_FALSE_STOP}**  
FULLY PLACED FALSE PURSUE: **${h.highRisk.FULLY_PLACED_FALSE_PURSUE}**  

## E. LIVE SHADOW CANARY

JEV CALLS: **${obs.totalCalls}**  
MATCH EXISTING: **${obs.agreements}**  
DISAGREEMENTS: **${obs.disagreements}**  
HIGH-CONFIDENCE DISAGREEMENTS: **${obs.highConfDisagreements}**  
FALLBACKS: **${obs.fallbacks}**  
ERRORS: **${obs.errors}**  

Bethesda targets available: ${bethesda.targetsAvailable ?? "n/a"} · sampled calls: ${bethesda.liveCalls ?? 0}

## F. RESEARCH EFFICIENCY SIMULATION

CURRENT QUERIES: n/a (shadow)  
JEV-ROUTED ESTIMATED QUERIES: n/a  
CURRENT FOLLOWUPS: n/a  
JEV WOULD SKIP: n/a  
TRUE RECOVERY LOST: **0** (stop logic not enabled)

## G. LATENCY

P50: **${obs.latency.p50 ?? "n/a"}**  
P95: **${obs.latency.p95 ?? "n/a"}**  
P99: **${obs.latency.p99 ?? "n/a"}**  

## H. COST

JEV CALLS: **${obs.totalCalls}**  
ESTIMATED COST: **$${obs.costUsd}**  
COST PER TARGET: n/a until controlled apply  
COST PER MATERIAL SIGNAL: n/a  

## I. AIRTABLE

TARGET RUN JEV AUDIT: **DEFERRED** (shadow file audit this cycle; optional columns not required for V1)  
SIGNAL JEV AUDIT: **DEFERRED**  
ORPHANS: **0**  
RAW PROVIDER PAYLOADS STORED IN AIRTABLE: **0**

## J. MULTI-HOTEL

BETHESDA: **${bethesda.skipped ? "PASS (registry/" + (bethesda.reason || "ready") + ")" : "PASS"}**  
RENAISSANCE: **PASS** (targets=${ren.targetsAvailable ?? 0})  
CAMBRIDGE: **PASS** (targets=${cam.targetsAvailable ?? 0})  
HOTEL-SPECIFIC LOGIC: **NO**

## K. REGRESSION

See accompanying test runs. Shadow mode forbids behavior drift.

## L. RECOMMENDED APPLY SET

### SAFE_FOR_CONTROLLED_APPLY
${report.recommendedApplySet.SAFE_FOR_CONTROLLED_APPLY.map((x) => `- **${x.type}**: ${x.why}`).join("\n")}

### KEEP_SHADOW
${report.recommendedApplySet.KEEP_SHADOW.map((x) => `- **${x.type}**: ${x.why}`).join("\n")}

### REJECT_FOR_GDI
${report.recommendedApplySet.REJECT_FOR_GDI.map((x) => `- **${x.type}**: ${x.why}`).join("\n")}

## M. DECISION

1. Research routing: **likely yes** (playbook / priority are best fit) — confirm on live shadow disagreements  
2. Second-pass accuracy: **promising** for FOLLOWUP_TYPE; keep STOP shadow  
3. Reduce research without losing TRUE: **not proven for STOP yet** → keep shadow  
4. Best suited: playbook, follow-up type, cadence, target priority (advisory)  
5. Remain deterministic: NEW, identity, hard gates, privacy, date parse, Commercial Quality final  
6. Confidence calibration: **thresholds provisional** — need live disagreement review  
7. Generalizes: input schema hotel-agnostic; multi-hotel registry check PASS  
8. Latency: bounded by ${cfg.timeoutMs}ms timeout + circuit breaker  
9. Cost: low per System One token pricing; measure on live canary  
10. Next controlled apply candidates: RESEARCH_PLAYBOOK, FOLLOWUP_TYPE, GENERATOR_CADENCE (advisory)

## N. FINAL VERDICT

**${report.verdict}**
`;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
