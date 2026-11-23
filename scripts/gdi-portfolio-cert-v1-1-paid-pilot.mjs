#!/usr/bin/env node
/**
 * GDI Portfolio Cert V1.1 paid pilot CLI.
 *
 *   # Zero-cost preflight (required before spend)
 *   node scripts/gdi-portfolio-cert-v1-1-paid-pilot.mjs --mode preflight
 *
 *   # Controlled paid execution (only after preflight ok + founder gates)
 *   set GDI_EXA_AGENT_LIVE=1
 *   node scripts/gdi-portfolio-cert-v1-1-paid-pilot.mjs --mode execute-paid \
 *     --confirm-founder-spend-go FOUNDER_GDI_PORTFOLIO_CERT_V1_1_PAID_25
 *
 *   # Zero-paid replay
 *   node scripts/gdi-portfolio-cert-v1-1-paid-pilot.mjs --mode replay
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_PORTFOLIO_CERT_V1_1_PAID_PILOT,
  PILOT_AGGREGATE_CEILING_USD,
  PILOT_PER_HOTEL_CEILING_USD,
  PILOT_HOTELS,
  buildProviderCallPlan,
  verifyDualSpendCaps,
  runPortfolioPaidPilot,
  runZeroPaidReplay,
  getPilotReportRoot,
} from "../lib/group-demand-intelligence/workflow/gdi-portfolio-cert-v1-1-paid-pilot-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const FOUNDER_SPEND_PHRASE = "FOUNDER_GDI_PORTFOLIO_CERT_V1_1_PAID_25";

function arg(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}
function has(flag) {
  return process.argv.includes(flag);
}

// Prefer Integration .env when WT has no local .env (do not print secrets).
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
const reportRoot = arg("--out") || getPilotReportRoot();
fs.mkdirSync(reportRoot, { recursive: true });

if (mode === "preflight" || mode === "plan") {
  const plan = buildProviderCallPlan({
    maxCandidatesPerHotel: Number(arg("--max-candidates") || 4) || 4,
  });
  fs.writeFileSync(
    path.join(reportRoot, "PROVIDER_CALL_PLAN.json"),
    `${JSON.stringify(plan, null, 2)}\n`,
  );
  const caps = verifyDualSpendCaps();
  fs.writeFileSync(
    path.join(reportRoot, "DUAL_SPEND_CAPS.json"),
    `${JSON.stringify(caps, null, 2)}\n`,
  );
  console.log(
    JSON.stringify(
      {
        ok: plan.ok,
        executeReady: plan.executeReady === true,
        mode: "preflight",
        contract: GDI_PORTFOLIO_CERT_V1_1_PAID_PILOT,
        aggregateCeilingUsd: PILOT_AGGREGATE_CEILING_USD,
        perHotelCeilingUsd: PILOT_PER_HOTEL_CEILING_USD,
        hotels: PILOT_HOTELS.map((h) => h.hotelId),
        blockers: plan.blockers,
        executeBlockers: plan.executeBlockers,
        spentUsd: 0,
        reportRoot,
      },
      null,
      2,
    ),
  );
  process.exit(plan.ok ? 0 : 2);
}

if (mode === "replay") {
  const replay = await runZeroPaidReplay({
    reportRoot,
    maxCandidates: Number(arg("--max-candidates") || 4) || 4,
  });
  console.log(JSON.stringify(replay, null, 2));
  process.exit(replay.noAdditionalPaidCalls ? 0 : 1);
}

if (mode === "execute-paid" || mode === "paid") {
  const confirm = arg("--confirm-founder-spend-go");
  if (confirm !== FOUNDER_SPEND_PHRASE) {
    console.error(
      JSON.stringify(
        {
          ok: false,
          error: "FOUNDER_SPEND_CONFIRMATION_REQUIRED",
          required: FOUNDER_SPEND_PHRASE,
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
        {
          ok: false,
          error: "GDI_EXA_AGENT_LIVE_REQUIRED",
          spentUsd: 0,
        },
        null,
        2,
      ),
    );
    process.exit(2);
  }

  const result = await runPortfolioPaidPilot({
    executePaid: true,
    reportRoot,
    portfolioRunId: arg("--portfolio-run-id") || undefined,
    maxCandidates: Number(arg("--max-candidates") || 4) || 4,
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
          ok: h.ok,
          spentUsd: h.spentUsd,
          error: h.error || null,
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
    usage:
      "--mode preflight|execute-paid|replay",
  }),
);
process.exit(2);
