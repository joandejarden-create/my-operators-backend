/**
 * GDI Universal Cohort Processing V2
 *
 * Hotel-independent portfolio execution over the established GDI engine:
 *   HotelsFile / Hotels / HotelId → hotel specs from GDI configs →
 *   executeHotelPaidPilot → runCanonicalFullE2eResearch
 *
 * Does NOT hard-code Design Hotels pilot identifiers.
 * DesignHotelsLive* modes remain frozen for historical three-hotel regression.
 *
 * Safeguards preserved: dual spend ledgers, Exa-only, no Surfe/Findymail,
 * no Airtable apply, no publication, sequential hotels.
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
import {
  runCanonicalFullE2eResearch,
  CANONICAL_EXECUTION_MODE,
} from "./gdi-canonical-full-e2e-entry-v1.js";
import {
  loadOrCreatePaidBudgetLedger,
  remainingBudgetUsd,
} from "../research/gdi-authorized-paid-budget-ledger-v1.js";
import { resolveHotelIdentity } from "./gdi-portfolio-processing-certification-v1.js";
import { loadHotelGdiConfig } from "../research/commercial-demand-discovery-v4.js";
import { proposeHotelGdiConfig } from "./gdi-hotel-config-proposal-v2.js";
import { listFourteenWorkflowSegmentIds } from "../research/universal-contact-research-contract-v1.js";

export const GDI_UNIVERSAL_COHORT_PROCESSING_V2 =
  "gdi-universal-cohort-processing-v2";

export const UNIVERSAL_COHORT_FOUNDER_SPEND_PHRASE =
  "FOUNDER_GDI_UNIVERSAL_COHORT_PAID";

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

export function getUniversalCohortReportRoot(root = ROOT) {
  return path.join(root, "reports", "gdi-universal-cohort-processing-v2");
}

/**
 * Parse hotel IDs from HotelsFile (txt or json), comma list, or single id.
 */
export function parseHotelsInput({ hotelId, hotels, hotelsFile, root = ROOT } = {}) {
  const ids = [];
  if (hotelId) ids.push(text(hotelId));
  if (hotels) {
    for (const part of String(hotels).split(/[,;\s]+/)) {
      if (text(part)) ids.push(text(part));
    }
  }
  if (hotelsFile) {
    const abs = path.isAbsolute(hotelsFile)
      ? hotelsFile
      : path.resolve(root, hotelsFile);
    const raw = fs.readFileSync(abs, "utf8");
    if (/\.json$/i.test(abs)) {
      const parsed = JSON.parse(raw);
      const list = Array.isArray(parsed)
        ? parsed
        : parsed.hotelIds || parsed.hotels || [];
      for (const x of list) {
        const id = typeof x === "string" ? x : x.hotelId;
        if (text(id)) ids.push(text(id));
      }
    } else {
      for (const part of raw.split(/[\s,;]+/)) {
        if (text(part) && !part.startsWith("#")) ids.push(text(part));
      }
    }
  }
  const seen = new Set();
  const unique = [];
  const invalid = [];
  for (const id of ids) {
    if (!/^rec[a-zA-Z0-9]{14}$/.test(id)) {
      invalid.push(id);
      continue;
    }
    if (seen.has(id)) continue;
    seen.add(id);
    unique.push(id);
  }
  return { ok: invalid.length === 0 && unique.length > 0, hotelIds: unique, invalid };
}

function defaultPrioritySegments(cfg) {
  const notes = text(cfg?.commercialPriorities?.notes).toLowerCase();
  const classification = text(cfg?.capabilityProfile?.classification).toLowerCase();
  if (/leisure|destination|villa|resort/.test(classification) || /leisure/.test(notes)) {
    return [
      "travel_trade_group_intermediaries",
      "social_special_events",
      "corporate_celebrations_private_events",
      "sports_entertainment",
      "corporate_meetings_incentives",
      "government_public_sector",
      "preferred_lodging_partnerships",
    ];
  }
  if (/urban/.test(classification) || /meeting/.test(notes)) {
    return [
      "corporate_meetings_incentives",
      "associations_conferences_events",
      "corporate_accounts_scaling",
      "projects_infrastructure",
      "professional_event_planners",
      "government_public_sector",
      "social_special_events",
    ];
  }
  // Boutique default — small group / corporate travel oriented
  return [
    "corporate_meetings_incentives",
    "corporate_accounts_scaling",
    "associations_conferences_events",
    "professional_event_planners",
    "travel_trade_group_intermediaries",
    "social_special_events",
    "preferred_lodging_partnerships",
  ];
}

/**
 * Build paid-pilot hotel specs from canonical HPC ids + GDI configs.
 */
export async function buildHotelSpecsFromIds(hotelIds = [], opts = {}) {
  const perHotelCeilingUsd = Number(opts.perHotelCeilingUsd || 4);
  const specs = [];
  const blockers = [];
  for (const hotelId of hotelIds) {
    let cfg = loadHotelGdiConfig(hotelId);
    let configSource = cfg ? "existing_gdi_config" : null;
    if (!cfg && opts.autoProposeConfig !== false) {
      const proposal = await proposeHotelGdiConfig(hotelId, {
        skipHpc: opts.skipHpc === true,
      });
      if (proposal.ok && proposal.proposedConfig) {
        cfg = proposal.proposedConfig;
        configSource = "proposed_v2_runtime";
      } else {
        blockers.push({
          hotelId,
          code: "HOTEL_CONFIG_UNRESOLVED",
          unresolved: proposal.unresolved || [],
          hardBlockers: proposal.hardBlockers || [],
        });
        continue;
      }
    }
    if (!cfg) {
      blockers.push({ hotelId, code: "HOTEL_CONFIG_MISSING" });
      continue;
    }
    specs.push({
      hotelId,
      hotelName: cfg.displayName || hotelId,
      market:
        cfg.demandTerritory?.label ||
        [cfg.city, cfg.country].filter(Boolean).join(", ") ||
        null,
      authorizationId:
        opts.authorizationIdFor?.(hotelId) ||
        `FOUNDER_GDI_UNIVERSAL_COHORT_${hotelId}_USD_${String(perHotelCeilingUsd).replace(".", "_")}`,
      perHotelCeilingUsd,
      prioritySegments:
        opts.prioritySegments ||
        defaultPrioritySegments(cfg) ||
        listFourteenWorkflowSegmentIds().slice(0, 8),
      configSource,
    });
  }
  return { ok: blockers.length === 0 && specs.length > 0, specs, blockers };
}

export async function buildUniversalCohortPreflight(opts = {}) {
  const parsed = parseHotelsInput(opts);
  if (!parsed.ok) {
    return {
      ok: false,
      executeReady: false,
      contract: GDI_UNIVERSAL_COHORT_PROCESSING_V2,
      error: "INVALID_HOTEL_LIST",
      invalid: parsed.invalid,
      hotelIds: parsed.hotelIds,
      blockers: ["INVALID_OR_EMPTY_HOTEL_LIST"],
    };
  }
  const built = await buildHotelSpecsFromIds(parsed.hotelIds, opts);
  if (!built.ok) {
    return {
      ok: false,
      executeReady: false,
      contract: GDI_UNIVERSAL_COHORT_PROCESSING_V2,
      hotelIds: parsed.hotelIds,
      blockers: built.blockers,
      specs: built.specs,
    };
  }
  const aggregateCeilingUsd = Number(
    opts.aggregateCeilingUsd ||
      built.specs.reduce((s, h) => s + Number(h.perHotelCeilingUsd || 0), 0),
  );
  const plan = buildProviderCallPlan({
    hotels: built.specs,
    maxCandidatesPerHotel: opts.maxCandidates ?? 4,
    effort: opts.exaEffort || EXA_AGENT_EFFORT.MEDIUM,
    root: opts.root || ROOT,
  });
  plan.contract = GDI_UNIVERSAL_COHORT_PROCESSING_V2;
  plan.safety = {
    ...(plan.safety || {}),
    aggregateCeilingUsd,
    publication: false,
    airtableApply: false,
    surfe: false,
    findymail: false,
    weeklyAutomation: false,
    designHotelsHardcoded: false,
  };
  const caps = verifyDualSpendCaps({
    hotels: built.specs,
    aggregateCeilingUsd,
    effort: opts.exaEffort || EXA_AGENT_EFFORT.MEDIUM,
    root: opts.root || ROOT,
  });
  plan.caps = caps;
  if (!caps.ok) {
    plan.ok = false;
    plan.executeReady = false;
    plan.blockers = [...(plan.blockers || []), ...(caps.errors || [])];
  }
  return {
    ...plan,
    ok: plan.ok !== false && caps.ok,
    executeReady: plan.executeReady === true && caps.ok,
    contract: GDI_UNIVERSAL_COHORT_PROCESSING_V2,
    hotelIds: parsed.hotelIds,
    hotels: built.specs,
    aggregateCeilingUsd,
    founderSpendPhrase: UNIVERSAL_COHORT_FOUNDER_SPEND_PHRASE,
  };
}

export async function runUniversalCohortProcessing(opts = {}) {
  const reportRoot = opts.reportRoot || getUniversalCohortReportRoot();
  const portfolioRunId =
    text(opts.portfolioRunId) ||
    `gdi_univ_${new Date().toISOString().replace(/[-:TZ.]/g, "").slice(0, 14)}`;
  ensureDir(reportRoot);

  const plan = await buildUniversalCohortPreflight(opts);
  writeJson(path.join(reportRoot, "PROVIDER_CALL_PLAN.json"), plan);
  writeJson(path.join(reportRoot, "COHORT_MANIFEST.json"), {
    contract: GDI_UNIVERSAL_COHORT_PROCESSING_V2,
    portfolioRunId,
    hotelIds: plan.hotelIds,
    hotels: plan.hotels,
    aggregateCeilingUsd: plan.aggregateCeilingUsd,
    note: "Dynamic HotelsFile cohort — not Design Hotels hard-coded path",
  });

  if (opts.executePaid !== true) {
    return {
      ok: plan.ok,
      mode: "preflight",
      spentUsd: 0,
      plan,
      reportRoot,
      note: "Set executePaid:true only after founder preflight + spend confirmation",
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
  const ceiling = Number(plan.aggregateCeilingUsd || 0);
  for (const hotelSpec of plan.hotels) {
    if (aggregateSpend >= ceiling - 1e-9) {
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
  }

  const summary = {
    ok: hotels.every((h) => h.ok !== false || h.status === "BLOCKED_AGGREGATE_BUDGET"),
    contract: GDI_UNIVERSAL_COHORT_PROCESSING_V2,
    portfolioRunId,
    mode: "execute-paid",
    aggregateCeilingUsd: ceiling,
    aggregateSpentUsd: aggregateSpend,
    hotels,
    publicationAuthorized: false,
    airtableApply: false,
    finishedAt: nowIso(),
    reportRoot,
  };
  writeJson(path.join(reportRoot, "EXECUTION_SUMMARY.json"), summary);
  return summary;
}

/**
 * Zero-paid replay for an arbitrary hotel cohort (saved evidence only).
 */
export async function runUniversalCohortZeroPaidReplay(opts = {}) {
  const reportRoot = opts.reportRoot || getUniversalCohortReportRoot();
  const parsed = parseHotelsInput(opts);
  if (!parsed.ok) {
    return {
      ok: false,
      error: "INVALID_HOTEL_LIST",
      invalid: parsed.invalid,
      additionalSpendUsd: 0,
    };
  }
  const built = await buildHotelSpecsFromIds(parsed.hotelIds, opts);
  const hotels = [];
  let additionalSpend = 0;
  for (const hotelSpec of built.specs) {
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
      canonicalOk: result?.ok !== false,
      remainingUsd: remainingBudgetUsd(ledgerAfter.ledger),
    });
  }
  const summary = {
    ok: additionalSpend <= 1e-9 && hotels.every((h) => h.opportunityIdsStable !== false),
    mode: "zero-paid-replay",
    contract: GDI_UNIVERSAL_COHORT_PROCESSING_V2,
    noAdditionalPaidCalls: additionalSpend <= 1e-9,
    additionalSpendUsd: additionalSpend,
    hotels,
    finishedAt: nowIso(),
    reportRoot,
  };
  writeJson(path.join(reportRoot, "ZERO_PAID_REPLAY.json"), summary);
  return summary;
}

export default {
  GDI_UNIVERSAL_COHORT_PROCESSING_V2,
  UNIVERSAL_COHORT_FOUNDER_SPEND_PHRASE,
  parseHotelsInput,
  buildHotelSpecsFromIds,
  buildUniversalCohortPreflight,
  runUniversalCohortProcessing,
  runUniversalCohortZeroPaidReplay,
  getUniversalCohortReportRoot,
};
