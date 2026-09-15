#!/usr/bin/env node
/**
 * Bethesda Marriott GDI pilot research run (admin/script path).
 * Usage:
 *   node scripts/gdi-bethesda-pilot-run.mjs
 *   node scripts/gdi-bethesda-pilot-run.mjs --webhound-session=<id> --webhound-cost=5
 *   node scripts/gdi-bethesda-pilot-run.mjs --webhound-calls=id1:5:q1,id2:5:q2,id3:5:q3
 */

import {
  runGroupDemandResearch,
  PILOT_HOTEL_ID,
} from "../lib/group-demand-intelligence/index.js";
import {
  WAVE1_WEBHOUND_SESSION_ID,
  DEEPEN_WEBHOUND_SESSION_ID,
  DISCOVERY_WEBHOUND_SESSION_ID,
} from "../lib/group-demand-intelligence/discovery-opportunities-v2.js";

function arg(name) {
  const prefix = `--${name}=`;
  const hit = process.argv.find((a) => a.startsWith(prefix));
  return hit ? hit.slice(prefix.length) : null;
}

const webhoundSessionId = arg("webhound-session");
const webhoundCost = arg("webhound-cost");
const webhoundCallsRaw = arg("webhound-calls");
const dryRun = process.argv.includes("--dry-run");
const useFullPilotSpend = process.argv.includes("--full-pilot-spend");

/** @type {Array<{sessionId:string,costUsd:number,question:string,url:string}>|undefined} */
let webhoundCalls;
if (webhoundCallsRaw) {
  webhoundCalls = webhoundCallsRaw.split(",").map((part) => {
    const [sessionId, cost, ...rest] = part.split(":");
    return {
      sessionId: sessionId.trim(),
      costUsd: Number(cost || 0),
      question: rest.join(":") || "GDI Bethesda Webhound call",
      url: `https://webhound.ai/session/${sessionId.trim()}`,
    };
  });
} else if (useFullPilotSpend) {
  webhoundCalls = [
    {
      sessionId: WAVE1_WEBHOUND_SESSION_ID,
      costUsd: 5,
      question: "Wave 1 — Bethesda group demand seed enrichment",
      url: `https://webhound.ai/session/${WAVE1_WEBHOUND_SESSION_ID}`,
    },
    {
      sessionId: DEEPEN_WEBHOUND_SESSION_ID,
      costUsd: 5,
      question: "Deepen Mediums — NICE SHOW AFCEA MSYSA",
      url: `https://webhound.ai/session/${DEEPEN_WEBHOUND_SESSION_ID}`,
    },
    {
      sessionId: DISCOVERY_WEBHOUND_SESSION_ID,
      costUsd: 5,
      question: "Association discovery + corporate public-source test",
      url: `https://webhound.ai/session/${DISCOVERY_WEBHOUND_SESSION_ID}`,
    },
  ];
}

const result = await runGroupDemandResearch({
  hotelId: PILOT_HOTEL_ID,
  trigger: useFullPilotSpend ? "cli_bethesda_pilot_full_15" : "cli_bethesda_pilot",
  webhoundSessionId: webhoundCalls ? undefined : webhoundSessionId,
  webhoundCostUsd:
    webhoundCalls || webhoundCost == null ? undefined : Number(webhoundCost),
  webhoundCalls,
  webhoundUrl: webhoundSessionId
    ? `https://webhound.ai/session/${webhoundSessionId}`
    : null,
  webhoundQuestion:
    "Bethesda Marriott future group demand opportunities (associations, medical, weekend, gov contractor)",
  allowWebhoundWithoutFlag: Boolean(webhoundSessionId || webhoundCalls?.length),
  dryRun,
});

const active = (result.opportunities || []).filter((o) => o.priority !== "DISQUALIFIED");
const high = active.filter((o) => o.priority === "HIGH_PRIORITY");
const medium = active.filter((o) => o.priority === "MEDIUM_PRIORITY");
const watch = active.filter((o) => o.priority === "WATCHLIST");

console.log(
  JSON.stringify(
    {
      ok: result.ok,
      dryRun: result.dryRun,
      runId: result.run.id,
      summary: result.summary,
      highPriorityTitles: high.map((o) => o.title),
      mediumTitles: medium.map((o) => o.title),
      watchlistTitles: watch.map((o) => o.title),
      qualifiedTitles: active.map((o) => ({
        priority: o.priority,
        title: o.title,
        hotelFit: o.hotelFitScore,
        evidence: o.evidenceConfidence,
        booking: o.bookingWindowStatus,
        contact: o.primaryContact?.email || o.primaryContact?.name || null,
      })),
      incrementalRoi: result.run?.cost?.incrementalRoi || [],
    },
    null,
    2
  )
);
