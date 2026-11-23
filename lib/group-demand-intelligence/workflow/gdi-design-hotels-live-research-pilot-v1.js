/**
 * GDI Design Hotels Live Research Pilot V1
 *
 * Controlled Exa-only paid research for Condesa / LaLuna / Memmo.
 * Dual spend caps (fail-closed):
 *   - Per hotel: $4 durable ledger
 *   - Aggregate: $12
 *
 * Reuses V1.1 paid-pilot hotel executor + durable ledgers.
 * No Surfe / Findymail / PDL / FullEnrich. No Airtable apply. No publication.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { EXA_AGENT_EFFORT } from "../research/exa-agent/exa-agent-provider-adapter-v1.js";
import {
  buildProviderCallPlan,
  executeHotelPaidPilot,
  verifyDualSpendCaps,
  runZeroPaidReplay as runZeroPaidReplayV11,
} from "./gdi-portfolio-cert-v1-1-paid-pilot-v1.js";
import { runCanonicalFullE2eResearch, CANONICAL_EXECUTION_MODE } from "./gdi-canonical-full-e2e-entry-v1.js";
import { loadOrCreatePaidBudgetLedger, remainingBudgetUsd } from "../research/gdi-authorized-paid-budget-ledger-v1.js";
import { resolveHotelIdentity } from "./gdi-portfolio-processing-certification-v1.js";

export const GDI_DESIGN_HOTELS_LIVE_RESEARCH_PILOT_V1 =
  "gdi-design-hotels-live-research-pilot-v1";

export const DH_LIVE_AGGREGATE_CEILING_USD = 12;
export const DH_LIVE_PER_HOTEL_CEILING_USD = 4;
export const DH_LIVE_FOUNDER_SPEND_PHRASE =
  "FOUNDER_GDI_DESIGN_HOTELS_LIVE_RESEARCH_12";

export const DH_LIVE_HOTELS = Object.freeze([
  {
    hotelId: "rece0NdV9FCnaFlVS",
    hotelName: "Condesa DF Hotel",
    market: "Mexico City, Mexico",
    authorizationId: "FOUNDER_GDI_DH_LIVE_V1_CONDESA_4",
    perHotelCeilingUsd: DH_LIVE_PER_HOTEL_CEILING_USD,
    prioritySegments: [
      "corporate_meetings_incentives",
      "associations_conferences_events",
      "corporate_accounts_scaling",
      "projects_infrastructure",
      "sports_entertainment",
      "professional_event_planners",
      "government_public_sector",
      "social_special_events",
    ],
  },
  {
    hotelId: "recaokE8psIbuVeuH",
    hotelName: "LaLuna Boutique Hotels & Villas",
    market: "Grenada",
    authorizationId: "FOUNDER_GDI_DH_LIVE_V1_LALUNA_4",
    perHotelCeilingUsd: DH_LIVE_PER_HOTEL_CEILING_USD,
    prioritySegments: [
      "travel_trade_group_intermediaries",
      "social_special_events",
      "corporate_celebrations_private_events",
      "sports_entertainment",
      "corporate_meetings_incentives",
      "government_public_sector",
      "preferred_lodging_partnerships",
    ],
  },
  {
    hotelId: "rec1t35e3sPerzxYu",
    hotelName: "Memmo Príncipe Real",
    market: "Lisbon, Portugal",
    authorizationId: "FOUNDER_GDI_DH_LIVE_V1_MEMMO_4",
    perHotelCeilingUsd: DH_LIVE_PER_HOTEL_CEILING_USD,
    prioritySegments: [
      "corporate_meetings_incentives",
      "corporate_accounts_scaling",
      "associations_conferences_events",
      "professional_event_planners",
      "travel_trade_group_intermediaries",
      "social_special_events",
      "preferred_lodging_partnerships",
    ],
  },
]);

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function nowIso() {
  return new Date().toISOString();
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function writeJson(filePath, body) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, `${JSON.stringify(body, null, 2)}\n`, "utf8");
  return filePath;
}

export function getDhLiveReportRoot(root = ROOT) {
  return path.join(root, "reports", "gdi-design-hotels-live-research-pilot-v1");
}

export function buildDhLivePreflight(opts = {}) {
  const hotels = opts.hotels || DH_LIVE_HOTELS;
  const plan = buildProviderCallPlan({
    hotels,
    maxCandidatesPerHotel: opts.maxCandidates ?? 4,
    effort: opts.exaEffort || EXA_AGENT_EFFORT.MEDIUM,
    root: opts.root || ROOT,
  });
  // Override safety ceilings in returned plan for this pilot
  plan.contract = GDI_DESIGN_HOTELS_LIVE_RESEARCH_PILOT_V1;
  plan.safety = {
    ...(plan.safety || {}),
    aggregateCeilingUsd: DH_LIVE_AGGREGATE_CEILING_USD,
    perHotelCeilingUsd: DH_LIVE_PER_HOTEL_CEILING_USD,
    publication: false,
    airtableApply: false,
    surfe: false,
    findymail: false,
    pdl: false,
    fullenrich: false,
    weeklyAutomation: false,
    forceSpendForbidden: true,
  };
  // Cap affordability messaging to $4 / $12
  const caps = verifyDualSpendCaps({
    hotels,
    aggregateCeilingUsd: DH_LIVE_AGGREGATE_CEILING_USD,
    effort: opts.exaEffort || EXA_AGENT_EFFORT.MEDIUM,
    root: opts.root || ROOT,
  });
  plan.caps = caps;
  if (!caps.ok) {
    plan.ok = false;
    plan.executeReady = false;
    plan.blockers = [...(plan.blockers || []), ...(caps.errors || [])];
  }
  return plan;
}

export async function runDesignHotelsLiveResearch(opts = {}) {
  const reportRoot = opts.reportRoot || getDhLiveReportRoot();
  const portfolioRunId =
    text(opts.portfolioRunId) ||
    `gdi_dh_live_${new Date()
      .toISOString()
      .replace(/[-:TZ.]/g, "")
      .slice(0, 14)}`;
  ensureDir(reportRoot);

  const plan = buildDhLivePreflight({
    maxCandidates: opts.maxCandidates ?? 4,
    root: opts.root,
  });
  writeJson(path.join(reportRoot, "PROVIDER_CALL_PLAN.json"), plan);
  writeJson(path.join(reportRoot, "RESEARCH_STRATEGY.json"), {
    contract: GDI_DESIGN_HOTELS_LIVE_RESEARCH_PILOT_V1,
    hotels: DH_LIVE_HOTELS,
    aggregateCeilingUsd: DH_LIVE_AGGREGATE_CEILING_USD,
    perHotelCeilingUsd: DH_LIVE_PER_HOTEL_CEILING_USD,
    note: "Priority segments only — remaining of 14 deferred for cost/relevance",
  });

  if (opts.executePaid !== true) {
    return {
      ok: plan.ok,
      mode: "preflight",
      spentUsd: 0,
      plan,
      reportRoot,
      note: "Set executePaid:true only after founder preflight verification",
    };
  }

  if (!plan.ok || plan.executeReady !== true) {
    return {
      ok: false,
      blocked: true,
      error: "PREFLIGHT_BLOCKED_BEFORE_SPEND",
      plan,
      spentUsd: 0,
      reportRoot,
    };
  }

  const hotels = [];
  let aggregateSpend = 0;
  for (const hotelSpec of DH_LIVE_HOTELS) {
    if (aggregateSpend >= DH_LIVE_AGGREGATE_CEILING_USD - 1e-9) {
      hotels.push({
        hotelId: hotelSpec.hotelId,
        hotelName: hotelSpec.hotelName,
        status: "BLOCKED_AGGREGATE_BUDGET",
        spentUsd: 0,
      });
      continue;
    }
    const hotelRunId = `${portfolioRunId}__${hotelSpec.hotelId}`;
    const row = await executeHotelPaidPilot(hotelSpec, {
      ...opts,
      reportRoot,
      hotelRunId,
      maxCandidates: opts.maxCandidates ?? 4,
      exaEffort: opts.exaEffort || EXA_AGENT_EFFORT.MEDIUM,
    });
    hotels.push(row);
    aggregateSpend = Number(
      (aggregateSpend + Number(row.spentUsd || 0)).toFixed(6),
    );
    if (aggregateSpend > DH_LIVE_AGGREGATE_CEILING_USD + 1e-9) {
      writeJson(path.join(reportRoot, "AGGREGATE_OVERSPEND_ALERT.json"), {
        aggregateSpend,
        ceiling: DH_LIVE_AGGREGATE_CEILING_USD,
        atHotel: hotelSpec.hotelId,
      });
      break;
    }
  }

  const summary = {
    ok: hotels.every((h) => h.ok !== false || h.status === "BLOCKED_AGGREGATE_BUDGET"),
    contract: GDI_DESIGN_HOTELS_LIVE_RESEARCH_PILOT_V1,
    portfolioRunId,
    mode: "execute-paid",
    aggregateCeilingUsd: DH_LIVE_AGGREGATE_CEILING_USD,
    perHotelCeilingUsd: DH_LIVE_PER_HOTEL_CEILING_USD,
    aggregateSpentUsd: aggregateSpend,
    hotels,
    publicationAuthorized: false,
    airtableApply: false,
    surfe: false,
    finishedAt: nowIso(),
    reportRoot,
  };
  writeJson(path.join(reportRoot, "EXECUTION_SUMMARY.json"), summary);
  return summary;
}

/**
 * Zero-paid replay using durable evidence — no additional Exa spend.
 */
export async function runDhLiveZeroPaidReplay(opts = {}) {
  const reportRoot = opts.reportRoot || getDhLiveReportRoot();
  const hotels = [];
  let additionalSpend = 0;
  for (const hotelSpec of DH_LIVE_HOTELS) {
    const ledgerBefore = loadOrCreatePaidBudgetLedger({
      authorizationId: hotelSpec.authorizationId,
      hardCeilingUsd: hotelSpec.perHotelCeilingUsd,
      effort: EXA_AGENT_EFFORT.MEDIUM,
      resume: true,
      allowCeilingRaise: false,
    });
    const spentBefore = Number(ledgerBefore.ledger?.spentUsd || 0);
    const identityBefore = resolveHotelIdentity(hotelSpec.hotelId);
    const outDir = path.join(
      reportRoot,
      "replay",
      hotelSpec.hotelId,
      `replay_${Date.now().toString(16)}`,
    );
    ensureDir(outDir);
    const result = await runCanonicalFullE2eResearch({
      hotelId: hotelSpec.hotelId,
      mode: CANONICAL_EXECUTION_MODE.DRY_RUN,
      dryRun: true,
      authorizePaidDiscovery: false,
      authorizePaidResearch: false,
      authorizeExaAgentLive: false,
      authorizePublish: false,
      budgetCeilingUsd: 0,
      authorizationId: hotelSpec.authorizationId,
      maxCandidates: opts.maxCandidates ?? 4,
      allowPartial: true,
      outDir,
      segmentIds: hotelSpec.prioritySegments,
      allowHistoricalReplay: true,
      planResearchPersistence: false,
      authorizeResearchPersistence: false,
      applyResearchPersistence: false,
      skipHpc: true,
    });
    const ledgerAfter = loadOrCreatePaidBudgetLedger({
      authorizationId: hotelSpec.authorizationId,
      hardCeilingUsd: hotelSpec.perHotelCeilingUsd,
      effort: EXA_AGENT_EFFORT.MEDIUM,
      resume: true,
      allowCeilingRaise: false,
    });
    const spentAfter = Number(ledgerAfter.ledger?.spentUsd || 0);
    const delta = Number((spentAfter - spentBefore).toFixed(6));
    additionalSpend = Number((additionalSpend + Math.max(0, delta)).toFixed(6));
    const identityAfter = resolveHotelIdentity(hotelSpec.hotelId);
    hotels.push({
      hotelId: hotelSpec.hotelId,
      hotelName: hotelSpec.hotelName,
      spentBefore,
      spentAfter,
      additionalSpendUsd: delta,
      opportunityIdsStable:
        JSON.stringify(identityBefore.opportunityIds || []) ===
        JSON.stringify(identityAfter.opportunityIds || []),
      opportunityCountBefore: identityBefore.opportunityIds?.length || 0,
      opportunityCountAfter: identityAfter.opportunityIds?.length || 0,
      canonicalOk: result?.ok !== false,
      remainingUsd: remainingBudgetUsd(ledgerAfter.ledger),
    });
  }
  const summary = {
    ok: additionalSpend <= 1e-9 && hotels.every((h) => h.opportunityIdsStable !== false),
    mode: "zero-paid-replay",
    contract: GDI_DESIGN_HOTELS_LIVE_RESEARCH_PILOT_V1,
    noAdditionalPaidCalls: additionalSpend <= 1e-9,
    additionalSpendUsd: additionalSpend,
    hotels,
    finishedAt: nowIso(),
    reportRoot,
  };
  writeJson(path.join(reportRoot, "ZERO_PAID_REPLAY.json"), summary);
  return summary;
}
