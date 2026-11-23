#!/usr/bin/env node
/**
 * Design Hotels Live Research Pilot V1 CLI.
 *
 *   node scripts/gdi-design-hotels-live-research-pilot-v1.mjs --mode preflight
 *   set GDI_EXA_AGENT_LIVE=1
 *   node scripts/gdi-design-hotels-live-research-pilot-v1.mjs --mode execute-paid \
 *     --confirm-founder-spend-go FOUNDER_GDI_DESIGN_HOTELS_LIVE_RESEARCH_12
 *   node scripts/gdi-design-hotels-live-research-pilot-v1.mjs --mode replay
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_DESIGN_HOTELS_LIVE_RESEARCH_PILOT_V1,
  DH_LIVE_AGGREGATE_CEILING_USD,
  DH_LIVE_PER_HOTEL_CEILING_USD,
  DH_LIVE_FOUNDER_SPEND_PHRASE,
  DH_LIVE_HOTELS,
  buildDhLivePreflight,
  runDesignHotelsLiveResearch,
  runDhLiveZeroPaidReplay,
  getDhLiveReportRoot,
} from "../lib/group-demand-intelligence/workflow/gdi-design-hotels-live-research-pilot-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function arg(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}
function has(flag) {
  return process.argv.includes(flag);
}

// Prefer Integration .env Exa keys when WT has none (do not print secrets).
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
const reportRoot = arg("--out") || getDhLiveReportRoot();
fs.mkdirSync(reportRoot, { recursive: true });
const maxCandidates = Number(arg("--max-candidates") || 4) || 4;

if (mode === "preflight" || mode === "plan") {
  const plan = buildDhLivePreflight({ maxCandidates });
  fs.writeFileSync(
    path.join(reportRoot, "PROVIDER_CALL_PLAN.json"),
    `${JSON.stringify(plan, null, 2)}\n`
  );
  console.log(
    JSON.stringify(
      {
        ok: plan.ok,
        executeReady: plan.executeReady === true,
        mode: "preflight",
        contract: GDI_DESIGN_HOTELS_LIVE_RESEARCH_PILOT_V1,
        aggregateCeilingUsd: DH_LIVE_AGGREGATE_CEILING_USD,
        perHotelCeilingUsd: DH_LIVE_PER_HOTEL_CEILING_USD,
        hotels: DH_LIVE_HOTELS.map((h) => h.hotelId),
        blockers: plan.blockers,
        executeBlockers: plan.executeBlockers,
        spentUsd: 0,
        reportRoot,
      },
      null,
      2
    )
  );
  process.exit(plan.ok ? 0 : 2);
}

if (mode === "replay") {
  const replay = await runDhLiveZeroPaidReplay({
    reportRoot,
    maxCandidates,
  });
  console.log(JSON.stringify(replay, null, 2));
  process.exit(replay.noAdditionalPaidCalls ? 0 : 1);
}

if (mode === "execute-paid" || mode === "paid") {
  const confirm = arg("--confirm-founder-spend-go");
  if (confirm !== DH_LIVE_FOUNDER_SPEND_PHRASE) {
    console.error(
      JSON.stringify(
        {
          ok: false,
          error: "FOUNDER_SPEND_CONFIRMATION_REQUIRED",
          required: DH_LIVE_FOUNDER_SPEND_PHRASE,
          spentUsd: 0,
        },
        null,
        2
      )
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
        2
      )
    );
    process.exit(2);
  }

  const result = await runDesignHotelsLiveResearch({
    executePaid: true,
    reportRoot,
    portfolioRunId: arg("--portfolio-run-id") || undefined,
    maxCandidates,
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
          hotelName: h.hotelName,
          ok: h.ok,
          blocked: h.blocked,
          spentUsd: h.spentUsd,
          error: h.error || null,
          processedCount: h.canonical?.processedCount ?? null,
        })),
        publicationAuthorized: false,
        airtableApply: false,
        reportRoot: result.reportRoot,
      },
      null,
      2
    )
  );
  process.exit(result.ok ? 0 : 1);
}

console.error(JSON.stringify({ ok: false, error: "UNKNOWN_MODE", mode }));
process.exit(2);
