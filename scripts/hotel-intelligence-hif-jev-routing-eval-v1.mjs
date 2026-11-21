#!/usr/bin/env node
/**
 * Evaluate Jev vs deterministic HIF research escalation on the frozen 30-case Agent pilot.
 *
 * Modes:
 *   --deterministic-only   no live Jev
 *   --live-shadow          call System One in shadow (uses JEZ/JEV/TYPESAFE key)
 *   --ceiling-usd N        max Jev spend for this eval (default 1.00)
 *
 * Jev never writes facts / HPC / customer publication.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

import {
  decideHifResearchEscalation,
  decideHifResearchEscalationDeterministic,
  HIF_RESEARCH_ACTION,
  describeHifJevRoutingContract,
} from "../lib/hotel-intelligence/sec-edgar/hif-jev-research-routing-v1.js";
import { describeJevConfig, isJevEnabled, resetJevCircuitBreaker } from "../lib/group-demand-intelligence/jev/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
dotenv.config({ path: path.join(ROOT, ".env") });

const OUT = path.join(ROOT, "reports", "sec-edgar-hif-jev-routing-v1");
const COHORT = path.join(ROOT, "reports/sec-edgar-exa-agent-pilot-v1/COHORT_30_CASES.json");
const BASELINE = path.join(ROOT, "reports/sec-edgar-exa-agent-pilot-v1/SEARCH_BASELINE_FREEZE.json");
const AGENT = path.join(ROOT, "reports/sec-edgar-exa-agent-pilot-v1/AGENT_INVESTIGATION_RESULTS.json");
const EVAL_AGENT = path.join(ROOT, "reports/sec-edgar-exa-agent-pilot-v1/COMPARATIVE_EVALUATION.json");

function parseArgs(argv) {
  const out = { liveShadow: false, deterministicOnly: false, ceilingUsd: 1.0, limit: null };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--live-shadow") out.liveShadow = true;
    else if (a === "--deterministic-only") out.deterministicOnly = true;
    else if (a === "--ceiling-usd" && argv[i + 1]) out.ceilingUsd = Number(argv[++i]) || 1;
    else if (a === "--limit" && argv[i + 1]) out.limit = Number(argv[++i]);
  }
  if (!out.liveShadow && !out.deterministicOnly) out.deterministicOnly = !isJevEnabled();
  return out;
}

function readJson(p, fb = null) {
  if (!fs.existsSync(p)) return fb;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8");
}

/**
 * Oracle labels from Agent pilot rescored incremental outcomes.
 * Ideal routing = what would have maximized verified targets / $ by avoiding useless Agent runs.
 */
function buildOracle(agentEval, agentResults, baselinesById) {
  const incrIds = new Set((agentEval?.incrementalFindings || []).map((f) => f.caseId));
  const falseIds = new Set((agentEval?.falsePositives || []).map((f) => f.caseId));
  const byId = new Map((agentResults?.results || []).map((r) => [r.caseId, r]));
  const oracle = {};

  for (const [caseId, r] of byId) {
    const base = baselinesById.get(caseId) || {};
    if (falseIds.has(caseId)) {
      oracle[caseId] = {
        ideal: HIF_RESEARCH_ACTION.STOP_LOW_VALUE_RESEARCH,
        reason: "agent_false_or_unsupported",
      };
      continue;
    }
    if (incrIds.has(caseId)) {
      const f = (agentEval.incrementalFindings || []).find((x) => x.caseId === caseId);
      if (f?.incremental?.ownerOrOperator && !f?.incremental?.newEndDate && !f?.incremental?.statusResolution) {
        oracle[caseId] = {
          ideal: HIF_RESEARCH_ACTION.VERIFY_OWNER_OPERATOR,
          reason: "incremental_owner_operator",
        };
      } else {
        oracle[caseId] = {
          ideal: HIF_RESEARCH_ACTION.INVESTIGATE_WITH_EXA_AGENT,
          reason: "incremental_status_or_end_date",
        };
      }
      continue;
    }
    // No incremental value from Agent — ideal was not Agent
    if (base.contractEndDate && !base.noticePeriodDays) {
      oracle[caseId] = {
        ideal: HIF_RESEARCH_ACTION.SEARCH_ADDITIONAL_SOURCES,
        reason: "no_agent_value_prefer_search_for_notice",
      };
    } else if (!base.contractEndDate) {
      oracle[caseId] = {
        ideal: HIF_RESEARCH_ACTION.VERIFY_CONTRACT_DATE,
        reason: "no_agent_value_prefer_date_verify",
      };
    } else {
      oracle[caseId] = {
        ideal: HIF_RESEARCH_ACTION.HOLD_INSUFFICIENT_EVIDENCE,
        reason: "no_agent_value_hold",
      };
    }
  }
  return oracle;
}

function scoreRouter(rows, field) {
  let matchOracle = 0;
  let agentEscalations = 0;
  let agentEscalationsHitIncr = 0;
  let falseAgentEscalations = 0;
  for (const r of rows) {
    const action = r[field];
    if (action === r.oracleIdeal) matchOracle += 1;
    if (action === HIF_RESEARCH_ACTION.INVESTIGATE_WITH_EXA_AGENT) {
      agentEscalations += 1;
      if (r.hadIncrementalAgentValue) agentEscalationsHitIncr += 1;
      else falseAgentEscalations += 1;
    }
  }
  const n = rows.length || 1;
  return {
    n: rows.length,
    oracleMatchRate: Number(((matchOracle / n) * 100).toFixed(1)),
    agentEscalations,
    agentEscalationPrecision:
      agentEscalations > 0
        ? Number(((agentEscalationsHitIncr / agentEscalations) * 100).toFixed(1))
        : null,
    agentEscalationRecall:
      rows.filter((r) => r.hadIncrementalAgentValue).length > 0
        ? Number(
            (
              (agentEscalationsHitIncr / rows.filter((r) => r.hadIncrementalAgentValue).length) *
              100
            ).toFixed(1),
          )
        : null,
    falseAgentEscalations,
    estimatedAgentSpendUsdIfFollowed: Number((agentEscalations * 0.025).toFixed(4)),
  };
}

async function main() {
  const args = parseArgs(process.argv);
  fs.mkdirSync(OUT, { recursive: true });
  resetJevCircuitBreaker();

  const cohort = readJson(COHORT, { cases: [] });
  const baseline = readJson(BASELINE, { baselines: [] });
  const agent = readJson(AGENT, { results: [] });
  const agentEval = readJson(EVAL_AGENT, {});
  const baselinesById = new Map((baseline.baselines || []).map((b) => [b.caseId, b]));
  const oracle = buildOracle(agentEval, agent, baselinesById);
  const incrIds = new Set((agentEval.incrementalFindings || []).map((f) => f.caseId));

  let cases = (cohort.cases || []).map((c) => ({
    ...c,
    searchBaseline: baselinesById.get(c.caseId) || {},
    agentBudgetRemainingUsd: 14,
  }));
  if (args.limit) cases = cases.slice(0, args.limit);

  const contract = describeHifJevRoutingContract();
  writeJson(path.join(OUT, "ROUTING_CONTRACT.json"), contract);

  const rows = [];
  let jevSpend = 0;
  let jevCalls = 0;
  let jevBlocked = null;

  for (const c of cases) {
    const det = decideHifResearchEscalationDeterministic(c);
    let live = null;

    if (args.liveShadow && !args.deterministicOnly) {
      if (jevSpend >= args.ceilingUsd) {
        jevBlocked = "JEV_EVAL_CEILING";
        live = {
          finalAction: det.action,
          source: "DETERMINISTIC_BUDGET_STOP",
          jev: null,
          agreementWithDeterministic: true,
        };
      } else if (!isJevEnabled()) {
        jevBlocked = "JEV_API_KEY_ABSENT";
        live = await decideHifResearchEscalation(c, { deterministicOnly: true, forceShadow: true });
      } else {
        live = await decideHifResearchEscalation(c, {
          forceShadow: true,
          runId: "hif-jev-routing-eval-v1",
        });
        const cost = Number(live.jev?.costUsd || 0);
        jevSpend += cost;
        if (live.jev) jevCalls += 1;
      }
    }

    const oracleIdeal = oracle[c.caseId]?.ideal || null;
    rows.push({
      caseId: c.caseId,
      hotel: c.hotel,
      cohort: c.cohort,
      deterministicAction: det.action,
      deterministicRationale: det.rationale,
      jevSelected: live?.jev?.selected || null,
      jevConfidence: live?.jev?.confidence ?? null,
      jevCostUsd: live?.jev?.costUsd ?? 0,
      jevMatchDeterministic: live?.agreementWithDeterministic ?? null,
      oracleIdeal,
      oracleReason: oracle[c.caseId]?.reason || null,
      hadIncrementalAgentValue: incrIds.has(c.caseId),
      alwaysAgentAction: HIF_RESEARCH_ACTION.INVESTIGATE_WITH_EXA_AGENT,
    });
  }

  const scores = {
    alwaysAgent: scoreRouter(rows, "alwaysAgentAction"),
    deterministic: scoreRouter(rows, "deterministicAction"),
    jevShadow: args.liveShadow
      ? scoreRouter(
          rows.map((r) => ({
            ...r,
            jevOrDet: r.jevSelected || r.deterministicAction,
            hadIncrementalAgentValue: r.hadIncrementalAgentValue,
            oracleIdeal: r.oracleIdeal,
          })),
          "jevOrDet",
        )
      : null,
  };

  // Benefit test: does Jev improve Agent precision vs always-Agent and vs deterministic?
  const recommendation = recommend(scores, jevSpend, jevBlocked || (args.liveShadow ? null : "DETERMINISTIC_ONLY"));

  const summary = {
    version: "hif-jev-routing-eval-v1",
    evaluatedAt: new Date().toISOString(),
    mode: args.liveShadow ? "LIVE_SHADOW" : "DETERMINISTIC_ONLY",
    jevEnabled: isJevEnabled(),
    jevConfig: describeJevConfig(),
    jevBlocked,
    jevSpendUsd: Number(jevSpend.toFixed(6)),
    jevCalls,
    ceilingUsd: args.ceilingUsd,
    caseCount: rows.length,
    scores,
    recommendation,
    forbiddenConfirmed: [
      "no_contract_fact_writes",
      "no_verification_gate_declarations",
      "no_hpc_authorization",
      "no_customer_publication",
    ],
  };

  writeJson(path.join(OUT, "ROUTING_EVAL_ROWS.json"), { rows });
  writeJson(path.join(OUT, "ROUTING_EVAL_SUMMARY.json"), summary);
  writeFounder(summary, rows);

  console.log(JSON.stringify({
    mode: summary.mode,
    jevSpendUsd: summary.jevSpendUsd,
    scores: summary.scores,
    recommendation: summary.recommendation,
  }, null, 2));
}

function recommend(scores, jevSpend, blocker) {
  const always = scores.alwaysAgent;
  const det = scores.deterministic;
  const jev = scores.jevShadow;

  if (blocker === "JEV_API_KEY_ABSENT") {
    return {
      policy: "DETERMINISTIC_ONLY",
      autoEnableJev: false,
      rationale: ["Jev API key absent — cannot measure live routing benefit."],
    };
  }
  if (!jev) {
    return {
      policy: "DETERMINISTIC_ONLY_PENDING_LIVE_SHADOW",
      autoEnableJev: false,
      rationale: [
        "Deterministic router already concentrates Agent escalation vs always-Agent.",
        `Deterministic Agent precision ${det.agentEscalationPrecision}% vs always-Agent ${always.agentEscalationPrecision}%; estimated Agent spend if followed $${det.estimatedAgentSpendUsdIfFollowed} vs $${always.estimatedAgentSpendUsdIfFollowed}.`,
        "Run --live-shadow before enabling Jev apply routing.",
      ],
      approvalsRequired: [
        "Founder approval after live shadow comparison",
        "Confirm Jev spend ceiling for HIF stream",
        "Keep CUSTOMER_BLOCKED / no HPC",
      ],
    };
  }

  const jevBetterPrecision =
    jev.agentEscalationPrecision != null &&
    det.agentEscalationPrecision != null &&
    jev.agentEscalationPrecision > det.agentEscalationPrecision + 5;
  const jevBetterOracle = jev.oracleMatchRate > det.oracleMatchRate + 5;
  const jevWorthCost = jevSpend > 0 && (jevBetterPrecision || jevBetterOracle);

  if (jevWorthCost) {
    return {
      policy: "CONDITIONAL_JEV_SHADOW_CONTINUE_THEN_FOUNDER_APPLY",
      autoEnableJev: false,
      rationale: [
        "Live shadow shows Jev improving escalation quality vs deterministic on this sample.",
        `Jev precision ${jev.agentEscalationPrecision}% / oracle ${jev.oracleMatchRate}% vs deterministic ${det.agentEscalationPrecision}% / ${det.oracleMatchRate}%; Jev spend $${jevSpend}.`,
        "Do not auto-apply until founder approval; keep write/HPC/publication forbidden.",
      ],
      approvalsRequired: [
        "Founder approval to leave shadow",
        "Separate HIF Jev budget",
        "No fact writes / HPC / customer publication",
      ],
    };
  }

  return {
    policy: "DETERMINISTIC_ONLY",
    autoEnableJev: false,
    rationale: [
      "Measured Jev shadow benefit does not justify additional cost/complexity vs deterministic escalation on this 30-case sample.",
      `Deterministic Agent precision ${det.agentEscalationPrecision}% (spend $${det.estimatedAgentSpendUsdIfFollowed}) vs Jev ${jev.agentEscalationPrecision}% (Jev spend $${jevSpend} + Agent $${jev.estimatedAgentSpendUsdIfFollowed}).`,
      "Keep Jev available as optional shadow comparator; default HIF routing = deterministic.",
    ],
    approvalsRequired: [
      "Founder override required to enable Jev apply routing despite insufficient measured benefit",
    ],
  };
}

function writeFounder(summary, rows) {
  const lines = [];
  lines.push("# FOUNDER REPORT — HIF Jev Research Routing Evaluation");
  lines.push("");
  lines.push(`Generated: ${summary.evaluatedAt}`);
  lines.push("");
  lines.push("## Objective");
  lines.push("Evaluate reusing GDI Jev (TypeSafe System One) to control HIF research escalation — especially when to call Exa Agent — vs deterministic routing alone.");
  lines.push("");
  lines.push("## Contract");
  lines.push("- Reuses existing GDI Jev client/auth/decide/cost observability — no duplicate adapter.");
  lines.push("- Decision type: `HIF_CONTRACT_RESEARCH_ESCALATION`");
  lines.push("- Jev **must not** write contractual facts, declare gates verified, authorize HPC, or publish customer intelligence.");
  lines.push(`- Mode: **${summary.mode}** · Jev enabled: ${summary.jevEnabled} · Blocker: ${summary.jevBlocked || "none"}`);
  lines.push(`- Jev eval spend: **$${summary.jevSpendUsd}** / ceiling $${summary.ceilingUsd}`);
  lines.push("");
  lines.push("## Scores (30-case Agent pilot cohort)");
  lines.push("");
  lines.push("| Router | Oracle match % | Agent escalations | Precision % | Recall % | Est. Agent $ if followed |");
  lines.push("|---|---:|---:|---:|---:|---:|");
  for (const [name, s] of Object.entries(summary.scores)) {
    if (!s) continue;
    lines.push(
      `| ${name} | ${s.oracleMatchRate} | ${s.agentEscalations} | ${s.agentEscalationPrecision ?? "n/a"} | ${s.agentEscalationRecall ?? "n/a"} | ${s.estimatedAgentSpendUsdIfFollowed} |`,
    );
  }
  lines.push("");
  lines.push("## Recommendation");
  lines.push(`- **${summary.recommendation.policy}**`);
  lines.push(`- Auto-enable Jev: **${summary.recommendation.autoEnableJev}**`);
  for (const r of summary.recommendation.rationale || []) lines.push(`- ${r}`);
  lines.push("");
  lines.push("## Sample disagreements (deterministic vs oracle)");
  const disagree = rows.filter((r) => r.deterministicAction !== r.oracleIdeal).slice(0, 8);
  for (const r of disagree) {
    lines.push(`- ${r.caseId} ${r.hotel}: det=${r.deterministicAction} oracle=${r.oracleIdeal} (${r.oracleReason})`);
  }
  lines.push("");
  lines.push("## Approvals");
  for (const a of summary.recommendation.approvalsRequired || []) lines.push(`- ${a}`);
  lines.push("");
  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT_HIF_JEV_ROUTING.md"), lines.join("\n"), "utf8");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
