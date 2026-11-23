#!/usr/bin/env node
/**
 * GDI Universal Cohort Processing V2 CLI
 *
 *   node scripts/gdi-universal-cohort-processing-v2.mjs --mode preflight --hotels-file path.txt
 *   node scripts/gdi-universal-cohort-processing-v2.mjs --mode execute-paid --hotels-file path.txt \
 *     --confirm-founder-spend-go FOUNDER_GDI_UNIVERSAL_COHORT_PAID \
 *     --budget-usd 12 --per-hotel-budget-usd 4
 *   node scripts/gdi-universal-cohort-processing-v2.mjs --mode replay --hotels-file path.txt
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_UNIVERSAL_COHORT_PROCESSING_V2,
  UNIVERSAL_COHORT_FOUNDER_SPEND_PHRASE,
  buildUniversalCohortPreflight,
  runUniversalCohortProcessing,
  runUniversalCohortZeroPaidReplay,
  getUniversalCohortReportRoot,
} from "../lib/group-demand-intelligence/workflow/gdi-universal-cohort-processing-v2.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function arg(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}
function has(flag) {
  return process.argv.includes(flag);
}

if (!process.env.EXA_API_KEY && !process.env.EXA_KEY) {
  const icEnv = "C:/Dev/dealality/my-operators-backend/.env";
  if (fs.existsSync(icEnv)) {
    const raw = fs.readFileSync(icEnv, "utf8");
    for (const line of raw.split(/\r?\n/)) {
      const m = line.match(/^(EXA_API_KEY|EXA_KEY|GDI_EXA_AGENT_LIVE)=(.*)$/);
      if (m && !process.env[m[1]]) {
        process.env[m[1]] = m[2].replace(/^["']|["']$/g, "");
      }
    }
  }
}

const mode = (arg("--mode") || "preflight").toLowerCase();
const reportRoot = arg("--out") || getUniversalCohortReportRoot();
fs.mkdirSync(reportRoot, { recursive: true });

const hotelsOpts = {
  hotelId: arg("--hotel-id") || arg("--hotel"),
  hotels: arg("--hotels"),
  hotelsFile: arg("--hotels-file"),
  root: ROOT,
  maxCandidates: Number(arg("--max-candidates") || 4) || 4,
  perHotelCeilingUsd: Number(arg("--per-hotel-budget-usd") || 4) || 4,
  aggregateCeilingUsd: arg("--budget-usd") ? Number(arg("--budget-usd")) : undefined,
  reportRoot,
  portfolioRunId: arg("--portfolio-run-id") || undefined,
  skipHpc: has("--skip-hpc"),
};

if (mode === "preflight" || mode === "plan") {
  const plan = await buildUniversalCohortPreflight(hotelsOpts);
  fs.writeFileSync(
    path.join(reportRoot, "PROVIDER_CALL_PLAN.json"),
    `${JSON.stringify(plan, null, 2)}\n`,
  );
  console.log(
    JSON.stringify(
      {
        ok: plan.ok,
        executeReady: plan.executeReady === true,
        mode: "preflight",
        contract: GDI_UNIVERSAL_COHORT_PROCESSING_V2,
        hotelIds: plan.hotelIds,
        blockers: plan.blockers,
        spentUsd: 0,
        reportRoot,
        founderSpendPhrase: UNIVERSAL_COHORT_FOUNDER_SPEND_PHRASE,
      },
      null,
      2,
    ),
  );
  process.exit(plan.ok ? 0 : 2);
}

if (mode === "replay") {
  const replay = await runUniversalCohortZeroPaidReplay(hotelsOpts);
  console.log(JSON.stringify(replay, null, 2));
  process.exit(replay.noAdditionalPaidCalls ? 0 : 1);
}

if (mode === "execute-paid" || mode === "paid") {
  const confirm = arg("--confirm-founder-spend-go");
  if (confirm !== UNIVERSAL_COHORT_FOUNDER_SPEND_PHRASE) {
    console.error(
      JSON.stringify(
        {
          ok: false,
          error: "FOUNDER_SPEND_CONFIRMATION_REQUIRED",
          required: UNIVERSAL_COHORT_FOUNDER_SPEND_PHRASE,
          spentUsd: 0,
        },
        null,
        2,
      ),
    );
    process.exit(2);
  }
  if (String(process.env.GDI_EXA_AGENT_LIVE || "") !== "1") {
    console.error(
      JSON.stringify(
        { ok: false, error: "GDI_EXA_AGENT_LIVE_REQUIRED", spentUsd: 0 },
        null,
        2,
      ),
    );
    process.exit(2);
  }
  const result = await runUniversalCohortProcessing({
    ...hotelsOpts,
    executePaid: true,
  });
  console.log(
    JSON.stringify(
      {
        ok: result.ok,
        mode: "execute-paid",
        portfolioRunId: result.portfolioRunId,
        aggregateSpentUsd: result.aggregateSpentUsd,
        hotels: (result.hotels || []).map((h) => ({
          hotelId: h.hotelId,
          spentUsd: h.spentUsd,
          ok: h.ok,
          blocked: h.blocked,
          status: h.status,
        })),
        reportRoot: result.reportRoot,
      },
      null,
      2,
    ),
  );
  process.exit(result.ok ? 0 : 1);
}

console.error(
  JSON.stringify({
    ok: false,
    error: "UNKNOWN_MODE",
    supported: ["preflight", "execute-paid", "replay"],
  }),
);
process.exit(2);
