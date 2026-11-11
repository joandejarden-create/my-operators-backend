/**
 * Continuous multi-wave Association & Events research campaign controller.
 *
 * DISCOVER → INVESTIGATE → VALIDATE → RECOVER BUYERS → QUALIFY → EXPAND → REPEAT
 * until cumulative hotel cap, authorization invalid, yield target, or genuine exhaustion.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ASSOCIATION_EVENTS_CUMULATIVE_BUDGET_V1,
  ASSOCIATION_EVENTS_MEXICO_HOTEL_CAPS,
  reconcileAssociationEventsHotelBudget,
  recordHotelCampaignSpend,
  acquireHotelCampaignLock,
  releaseHotelCampaignLock,
  hotelCampaignStateDir,
} from "./association-events-cumulative-budget-v1.js";
// hotelCampaignStateDir used for latest-checkpoint.json persistence
import { buildAssociationEventsStrategiesForWave } from "./association-events-expanded-wave-strategies-v1.js";
import {
  runAssociationEventsHotelRunner,
  normalizeAssociationEventsRunnerConfig,
  ASSOCIATION_EVENTS_HOTEL_RUNNER_V1,
} from "./association-events-hotel-runner-v1.js";
import { isAssociationEventsValidPersonName } from "./association-events-buyer-integrity-v1.js";
import {
  ensureProtectedSeeds,
  applyEvidenceLedgerToOpportunity,
  ASSOCIATION_EVENTS_EVIDENCE_LEDGER_V1,
} from "./association-events-evidence-ledger-v1.js";
import { applyAssociationEventsChronology } from "./association-events-chronology-v1.js";
import {
  evaluateAssociationEventsPublicationQualification,
  ASSOCIATION_EVENTS_PUBLICATION_STATUS,
} from "./association-events-qualification-v1.js";

/** Commercial-priority IDs — deepen even when machine-QUALIFIED until drawers are evidence-complete. */
const COMMERCIAL_PRIORITY_VALIDATE_IDS = Object.freeze([
  "gdi_e2d_509c611c40266b50", // ISAPS World Congress Cancún
  "gdi_e2d_ccef4af266dfcf14", // ISAPS Accommodation / Laura Flores
  "gdi_e2d_e4f5ea4a39873f09", // Congreso Internacional ISAPS 2026
  "gdi_e2d_3126b40237b69920", // CONPEHT 2027
  "gdi_e2d_744e4347a5e4483e", // IPA 2027 Cancún
  "gdi_e2d_5e06b0ca27720780", // AMEI XXXIX hotel sede
  "gdi_e2d_583f9193e013df37", // Longevity
  "gdi_e2d_imcp_2027_cond", // IMCP
  "gdi_e2d_b55abdf7a9488331", // LARIM
  "gdi_e2d_anhp_cong_2027_chap", // ANHP
]);

export const ASSOCIATION_EVENTS_CONTINUOUS_CAMPAIGN_V1 =
  "gdi-association-events-continuous-campaign-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

const MEXICO_ASSOCIATION_HOTEL_IDS = new Set(
  Object.keys(ASSOCIATION_EVENTS_MEXICO_HOTEL_CAPS),
);

const DEFAULT_MAX_WAVES = 12;
const MIN_WAVE_BUDGET_USD = 0.12;
/** Leave headroom for provider billing discrepancies / in-flight reservations. */
const SAFETY_MARGIN_USD = 0.25;
const YIELD_TARGET_QUALIFIED = 10; // aspirational stop if met

function loadHotelGeographyHint(hotelId, lastSummary, recon0, baseCfg) {
  let city = lastSummary?.hotel?.city || null;
  let displayName =
    lastSummary?.hotel?.displayName ||
    recon0?.displayName ||
    baseCfg?.hotelName ||
    null;
  let demandTerritory = lastSummary?.hotel?.demandTerritory || null;
  let country = lastSummary?.hotel?.country || null;
  if (!city || !displayName) {
    try {
      const p = path.join(
        ROOT,
        "config/group-demand-intelligence/hotels",
        `${hotelId}.json`,
      );
      if (fs.existsSync(p)) {
        const cfg = JSON.parse(fs.readFileSync(p, "utf8"));
        city = city || cfg.city || null;
        displayName = displayName || cfg.displayName || null;
        demandTerritory = demandTerritory || cfg.demandTerritory || null;
        country = country || cfg.country || null;
      }
    } catch {
      /* ignore missing/corrupt hotel config */
    }
  }
  // Legacy Mexico-only fallback — never default non-Mexico hotels to CDMX.
  if (!city && MEXICO_ASSOCIATION_HOTEL_IDS.has(hotelId)) {
    city = hotelId === "recIEcSWuhSCoNLFN" ? "Cancún" : "Mexico City";
  }
  return {
    hotelId,
    displayName: displayName || hotelId,
    city: city || null,
    country: country || null,
    demandTerritory,
  };
}

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`, "utf8");
}

function readJson(p, fallback = null) {
  try {
    if (!fs.existsSync(p)) return fallback;
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return fallback;
  }
}

function campaignStatePath(hotelId) {
  return path.join(hotelCampaignStateDir(hotelId), "campaign-state.json");
}

function defaultBacklogSeeds(hotelId) {
  const seeds = [];
  const freshDir = path.join(
    ROOT,
    "reports/gdi-association-events-fresh-discovery-validation-v1/seeds",
  );
  const map = {
    recterKmNPdntCm0v: "chapultepec-fresh-v1.json",
    recIEcSWuhSCoNLFN: "condesa-fresh-v1.json",
  };
  const file = map[hotelId];
  if (file) {
    const p = path.join(freshDir, file);
    const doc = readJson(p);
    if (doc?.opportunities) seeds.push(...doc.opportunities);
  }
  return seeds;
}

function loadOrInitState(hotelId, runId) {
  const p = campaignStatePath(hotelId);
  const existing = readJson(p);
  if (existing?.hotelId === hotelId) {
    existing.resumedFrom = existing.lastRunId || null;
    existing.lastRunId = runId;
    existing.updatedAt = new Date().toISOString();
    return existing;
  }
  const seeds = defaultBacklogSeeds(hotelId);
  return {
    version: ASSOCIATION_EVENTS_CONTINUOUS_CAMPAIGN_V1,
    hotelId,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    lastRunId: runId,
    resumedFrom: null,
    waveCount: 0,
    usedStrategyIntents: [],
    opportunities: seeds,
    qualifiedIds: [],
    continuationIds: seeds
      .filter((o) => o.researchContinuation !== false)
      .map((o) => o.id)
      .filter(Boolean),
    deadEndIds: [],
    waves: [],
    exhaustion: null,
  };
}

function saveState(state) {
  writeJson(campaignStatePath(state.hotelId), state);
}

/** Never let empty/null research scrapes erase previously recovered commercial fields. */
const PRESERVE_IF_INCOMING_EMPTY = [
  "contactName",
  "contactRole",
  "contactEmail",
  "contactPhone",
  "organizationEmail",
  "organizationName",
  "eventName",
  "venueName",
  "venue",
  "proposedHotelUse",
  "hotelDemandThesis",
  "hotelOpportunityThesis",
  "summaryWhyHotel",
  "roomDemandRationale",
];

function mergeWithoutErasingEvidence(prior = {}, incoming = {}) {
  const out = { ...prior, ...incoming, id: incoming.id || prior.id };
  for (const field of PRESERVE_IF_INCOMING_EMPTY) {
    const next = incoming[field];
    const prev = prior[field];
    if ((next === null || next === undefined || next === "") && prev) {
      out[field] = prev;
    }
  }
  // Booleans: only overwrite when incoming explicitly sets them
  for (const field of ["hostHotelContracted", "externalRoomBlocksAvailable"]) {
    if (incoming[field] === undefined && prior[field] !== undefined) {
      out[field] = prior[field];
    }
  }
  return out;
}

function mergeOpportunity(state, opp, hotelId) {
  if (!opp?.id) return;
  const idx = state.opportunities.findIndex((o) => o.id === opp.id);
  const prior = idx >= 0 ? state.opportunities[idx] : { id: opp.id };
  const mergedRaw = mergeWithoutErasingEvidence(prior, opp);
  const merged = applyAssociationEventsChronology(
    applyEvidenceLedgerToOpportunity(hotelId || state.hotelId, mergedRaw),
  );
  // Drop known polluted stakeholder fragments
  if (Array.isArray(merged.namedStakeholders)) {
    merged.namedStakeholders = merged.namedStakeholders.filter(
      (s) => !/permanentes?\s+confedi|santa\s+fe|business\s+expos/i.test(String(s?.name || "")),
    );
  }
  if (idx >= 0) state.opportunities[idx] = merged;
  else state.opportunities.push(merged);
}

function demoteToContinuation(state, id) {
  if (!id) return;
  state.qualifiedIds = (state.qualifiedIds || []).filter((x) => x !== id);
  if (!state.continuationIds.includes(id)) state.continuationIds.push(id);
}

function restoreProtectedOpportunities(state) {
  // Mexico LARIM / Longevity / IMCP seeds are hotel-specific — never inject into Nobu/London.
  if (!MEXICO_ASSOCIATION_HOTEL_IDS.has(state.hotelId)) return;
  ensureProtectedSeeds(state.hotelId);
  // Force-apply protected seeds into opportunity list (LARIM etc.)
  for (const id of Object.keys(
    // lazy: apply via merge with empty incoming to restore accepted claims
    { gdi_e2d_b55abdf7a9488331: 1, gdi_e2d_583f9193e013df37: 1 },
  )) {
    const existing = state.opportunities.find((o) => o.id === id) || { id };
    const restored = applyEvidenceLedgerToOpportunity(state.hotelId, existing);
    mergeOpportunity(state, restored, state.hotelId);
    // LARIM / Longevity must remain CONTINUE until lodging demand + buyer fully verified
    demoteToContinuation(state, id);
  }
  // IMCP inscriptions contact is never a lodging buyer — force CONTINUE
  const imcp = state.opportunities.find((o) => o.id === "gdi_e2d_imcp_2027_cond");
  if (imcp) {
    const q = evaluateAssociationEventsPublicationQualification(imcp, {
      hotelId: state.hotelId,
    });
    if (!q.qualified) demoteToContinuation(state, imcp.id);
  }
  // Re-score all machine-qualified IDs against commercial gates (false QUALIFIED purge)
  for (const id of [...(state.qualifiedIds || [])]) {
    const opp = state.opportunities.find((o) => o.id === id);
    if (!opp) continue;
    const q = evaluateAssociationEventsPublicationQualification(opp, {
      hotelId: state.hotelId,
    });
    if (!q.qualified) demoteToContinuation(state, id);
  }
}

function prioritizeDeepen(state, maxN = 6) {
  const byId = new Map(state.opportunities.map((o) => [o.id, o]));
  const out = [];
  const seen = new Set();
  const mexicoPriority =
    MEXICO_ASSOCIATION_HOTEL_IDS.has(state.hotelId) ? COMMERCIAL_PRIORITY_VALIDATE_IDS : [];
  // Commercial priorities first — Mexico hotels only (Cancún / CDMX drawer validation list)
  for (const id of mexicoPriority) {
    const o = byId.get(id);
    if (!o || state.deadEndIds.includes(id)) continue;
    out.push(o);
    seen.add(id);
  }
  for (const id of state.continuationIds || []) {
    if (seen.has(id) || state.deadEndIds.includes(id)) continue;
    // Skip non-priority already-QUALIFIED unless commercial validate list
    if (state.qualifiedIds.includes(id) && !mexicoPriority.includes(id)) {
      continue;
    }
    const o = byId.get(id);
    if (o) {
      out.push(o);
      seen.add(id);
    }
  }
  // Also deepen recent non-qualified with research gaps
  for (const o of state.opportunities) {
    if (out.length >= maxN) break;
    if (!o?.id || seen.has(o.id)) continue;
    if (state.deadEndIds.includes(o.id)) continue;
    if (state.qualifiedIds.includes(o.id) && !mexicoPriority.includes(o.id)) {
      continue;
    }
    if (o.hostHotelContracted === true && o.externalRoomBlocksAvailable === false) continue;
    out.push(o);
    seen.add(o.id);
  }
  return out.slice(0, maxN);
}

function assessExhaustion(state, recon, {
  wavesThisBatch = 0,
  maxWavesThisBatch = DEFAULT_MAX_WAVES,
  waveSummaries = [],
} = {}) {
  const recent = waveSummaries.slice(-3);
  const newQualifiedRecent = recent.reduce(
    (n, w) => n + Number(w?.newQualified || 0),
    0,
  );
  const newCandidatesRecent = recent.reduce(
    (n, w) => n + Number(w?.newCandidates || 0),
    0,
  );
  const strategiesUsed = state.usedStrategyIntents?.length || 0;
  const openContinuation = (state.continuationIds || []).filter(
    (id) => !state.deadEndIds.includes(id) && !state.qualifiedIds.includes(id),
  ).length;
  const usableRemaining = Math.max(0, recon.remainingUsd - SAFETY_MARGIN_USD);

  if (usableRemaining < MIN_WAVE_BUDGET_USD) {
    return {
      exhausted: true,
      reason: "cumulative_hotel_ceiling_reached",
      commercialExhaustion: false,
      operationalStop: true,
      checkpointStatus: "RESEARCH_BUDGET_EXHAUSTED",
    };
  }
  if (
    (state.qualifiedIds || []).length >= YIELD_TARGET_QUALIFIED &&
    openContinuation === 0
  ) {
    return {
      exhausted: true,
      reason: "campaign_yield_target_met",
      commercialExhaustion: false,
      operationalStop: false,
      permittedStop: true,
      checkpointStatus: "YIELD_TARGET_MET",
    };
  }
  // Diminishing yield only after enough TOTAL waves AND no open researchable backlog
  if (
    waveSummaries.length >= 8 &&
    newQualifiedRecent === 0 &&
    newCandidatesRecent <= 2 &&
    openContinuation === 0 &&
    strategiesUsed >= 24
  ) {
    return {
      exhausted: true,
      reason: "diminishing_marginal_yield_and_strategies_exhausted",
      commercialExhaustion: true,
      strategiesAttempted: strategiesUsed,
      openContinuation,
      remainingUsd: recon.remainingUsd,
      checkpointStatus: "COMMERCIAL_RESEARCH_EXHAUSTED",
      note: "Unused budget may remain but recent waves produced no qualified yield, few new candidates, and no open continuation backlog.",
    };
  }
  // MaxWaves is a per-execution batch bound — NOT research completion
  if (wavesThisBatch >= maxWavesThisBatch) {
    return {
      exhausted: false,
      reason: "execution_batch_wave_bound",
      commercialExhaustion: false,
      operationalStop: true,
      resumable: true,
      checkpointStatus: "CHECKPOINT_RESUME_REQUIRED",
      remainingUsd: recon.remainingUsd,
      wavesThisBatch,
      maxWavesThisBatch,
      totalWavesCompleted: waveSummaries.length,
      note: "MaxWaves is an execution safety boundary. Campaign is NOT commercially complete. Resume from checkpoint.",
    };
  }
  return { exhausted: false, checkpointStatus: "CONTINUE" };
}

function writeSeedFile(reportDir, waveIndex, opportunities) {
  const p = path.join(reportDir, `wave-${String(waveIndex).padStart(2, "0")}-seed.json`);
  writeJson(p, {
    version: ASSOCIATION_EVENTS_CONTINUOUS_CAMPAIGN_V1,
    waveIndex,
    opportunities,
  });
  return p;
}

/**
 * Allocate this wave's spend split between deepen vs discovery.
 */
function planWaveAllocation(waveIndex, remainingUsd, state) {
  const openGaps = prioritizeDeepen(state, 8).length;
  const qualified = (state.qualifiedIds || []).length;
  let deepenShare = 0.55;
  let discoverShare = 0.45;
  if (openGaps >= 3 && waveIndex <= 2) {
    deepenShare = 0.7;
    discoverShare = 0.3;
  } else if (openGaps <= 1 || waveIndex >= 4) {
    deepenShare = 0.35;
    discoverShare = 0.65;
  } else if (qualified === 0 && waveIndex >= 2) {
    deepenShare = 0.4;
    discoverShare = 0.6;
  }
  // Cap wave size so multi-wave can continue
  const waveCap = Math.min(
    remainingUsd,
    waveIndex === 1 ? Math.min(remainingUsd, 2.5) : Math.min(remainingUsd, 2.0),
  );
  return {
    waveCapUsd: Number(waveCap.toFixed(4)),
    deepenBudgetUsd: Number((waveCap * deepenShare).toFixed(4)),
    discoveryBudgetUsd: Number((waveCap * discoverShare).toFixed(4)),
    deepenShare,
    discoverShare,
    openGaps,
  };
}

/**
 * @param {object} rawConfig — same shape as hotel runner config
 */
export async function runAssociationEventsContinuousCampaign(rawConfig = {}) {
  const baseCfg = normalizeAssociationEventsRunnerConfig({
    ...rawConfig,
    continuousCampaign: false,
  });
  if (!baseCfg.hotelId) {
    return {
      ok: false,
      error: "hotelId_required",
      version: ASSOCIATION_EVENTS_CONTINUOUS_CAMPAIGN_V1,
    };
  }

  const reportDir =
    baseCfg.reportDir ||
    path.join(ROOT, "reports/gdi-association-events-runs", baseCfg.runId);
  fs.mkdirSync(reportDir, { recursive: true });

  const recon0 = reconcileAssociationEventsHotelBudget(baseCfg.hotelId);
  writeJson(path.join(reportDir, "00-cumulative-budget-preflight.json"), recon0);

  if (!recon0.authorizationValid) {
    return {
      ok: false,
      error: "authorization_invalid_or_exhausted",
      version: ASSOCIATION_EVENTS_CONTINUOUS_CAMPAIGN_V1,
      reconciliation: recon0,
      reportDir,
    };
  }

  // Enforce provider allowlist for the process (deep researcher + any scrape helpers).
  if ((recon0.providersForbidden || []).includes("firecrawl")) {
    process.env.GDI_FORBID_FIRECRAWL = "1";
  }
  if (
    Array.isArray(recon0.providersAllowed) &&
    recon0.providersAllowed.length === 1 &&
    recon0.providersAllowed[0] === "exa"
  ) {
    process.env.GDI_EXA_ONLY = "1";
  }
  if (Array.isArray(recon0.providersForbidden) && recon0.providersForbidden.length) {
    process.env.GDI_PROVIDERS_FORBIDDEN = recon0.providersForbidden.join(",");
  }

  const lock = acquireHotelCampaignLock(baseCfg.hotelId, baseCfg.runId);
  if (!lock.ok) {
    return {
      ok: false,
      error: lock.error,
      version: ASSOCIATION_EVENTS_CONTINUOUS_CAMPAIGN_V1,
      lock,
      reportDir,
    };
  }

  const state = loadOrInitState(baseCfg.hotelId, baseCfg.runId);
  restoreProtectedOpportunities(state);
  // Optional external saved path merges into backlog (evidence-protected)
  if (baseCfg.savedDiscoveryPath && fs.existsSync(baseCfg.savedDiscoveryPath)) {
    try {
      const saved = JSON.parse(fs.readFileSync(baseCfg.savedDiscoveryPath, "utf8"));
      const rows = Array.isArray(saved) ? saved : saved.opportunities || [];
      for (const row of rows) mergeOpportunity(state, row, baseCfg.hotelId);
    } catch {
      /* ignore */
    }
  }
  saveState(state);

  // MaxWaves = waves in THIS execution batch (not cumulative lifetime waves)
  const maxWavesThisBatch = Math.min(
    Number(rawConfig.maxWaves) || DEFAULT_MAX_WAVES,
    20,
  );
  const waveSummaries = [...(state.waves || [])];
  let wavesThisBatch = 0;
  let lastSummary = null;
  let stopReason = null;
  let providerFailure = null;

  try {
    while (true) {
      const recon = reconcileAssociationEventsHotelBudget(baseCfg.hotelId);
      const usableRemaining = Math.max(0, recon.remainingUsd - SAFETY_MARGIN_USD);
      const exhaustion = assessExhaustion(state, recon, {
        wavesThisBatch,
        maxWavesThisBatch,
        waveSummaries,
      });
      if (exhaustion.exhausted || exhaustion.checkpointStatus === "CHECKPOINT_RESUME_REQUIRED") {
        stopReason = exhaustion;
        state.exhaustion = exhaustion;
        break;
      }
      if (usableRemaining < MIN_WAVE_BUDGET_USD) {
        stopReason = {
          exhausted: true,
          reason: "cumulative_hotel_ceiling_reached",
          operationalStop: true,
          remainingUsd: recon.remainingUsd,
          checkpointStatus: "RESEARCH_BUDGET_EXHAUSTED",
        };
        break;
      }

      const waveIndex = waveSummaries.length + 1;
      const allocation = planWaveAllocation(waveIndex, usableRemaining, state);
      if (allocation.waveCapUsd < MIN_WAVE_BUDGET_USD) {
        stopReason = {
          exhausted: true,
          reason: "insufficient_remaining_for_productive_wave",
          remainingUsd: recon.remainingUsd,
          checkpointStatus: "RESEARCH_BUDGET_EXHAUSTED",
        };
        break;
      }

      const deepenSet = prioritizeDeepen(state, 6);
      const hotelHint = loadHotelGeographyHint(
        baseCfg.hotelId,
        lastSummary,
        recon0,
        baseCfg,
      );
      if (!hotelHint.city) {
        stopReason = {
          exhausted: false,
          reason: "hotel_city_unresolved_for_discovery",
          operationalStop: true,
          commercialExhaustion: false,
          resumable: true,
          hotelId: baseCfg.hotelId,
        };
        break;
      }
      const strategies = buildAssociationEventsStrategiesForWave(
        hotelHint,
        waveIndex,
        state.usedStrategyIntents,
      );
      // Prefer discovery strategies this wave (runner uses hotel context strategies;
      // we pass deepen seed + allow paid discovery every wave)
      const seedOpps =
        deepenSet.length > 0
          ? deepenSet
          : state.opportunities.filter(
              (o) =>
                !state.deadEndIds.includes(o.id) &&
                !state.qualifiedIds.includes(o.id),
            ).slice(0, 4);
      const seedPath = writeSeedFile(reportDir, waveIndex, seedOpps);

      const waveDir = path.join(reportDir, `waves`, `wave-${String(waveIndex).padStart(2, "0")}`);
      fs.mkdirSync(waveDir, { recursive: true });

      const waveObjective =
        deepenSet.length >= 2
          ? `wave_${waveIndex}_deepen_priority_then_discover`
          : `wave_${waveIndex}_discover_then_deepen`;

      writeJson(path.join(waveDir, "wave-plan.json"), {
        waveIndex,
        allocation,
        objective: waveObjective,
        deepenIds: deepenSet.map((o) => o.id),
        strategyIntentsPlanned: strategies.slice(0, 8).map((s) => s.intent),
        remainingBeforeUsd: recon.remainingUsd,
      });

      let waveResult;
      try {
        waveResult = await runAssociationEventsHotelRunner({
          ...rawConfig,
          hotelId: baseCfg.hotelId,
          hotelName: baseCfg.hotelName || hotelHint.displayName,
          runId: `${baseCfg.runId}-w${waveIndex}`,
          mode: "full-e2e",
          authorizePaidResearch: true,
          authorizePublication: false,
          applyProductionWrites: false,
          budgetCeilingUsd: allocation.waveCapUsd,
          budgetUSD: allocation.waveCapUsd,
          savedDiscoveryPath: seedPath,
          maxCandidates: Math.min(Number(rawConfig.maxCandidates) || 16, 20),
          reportDir: waveDir,
          continuousCampaign: false,
          continuousCampaignWave: true,
          waveIndex,
          campaignAllowDiscoveryWithSavedEvidence: true,
          campaignDiscoveryBudgetUsd: allocation.discoveryBudgetUsd,
          campaignStrategyIntents: strategies.slice(0, 8).map((s) => s.intent),
          campaignExpandedStrategies: strategies.slice(0, 8),
          buyerRecoveryMaxQueriesPerOpp: waveIndex <= 2 ? 5 : 4,
          providersAllowed: recon0.providersAllowed || ["exa"],
          providersForbidden: recon0.providersForbidden || [
            "firecrawl",
            "surfe",
            "pdl",
            "parallel",
          ],
          forbidFirecrawl: (recon0.providersForbidden || []).includes("firecrawl"),
        });
      } catch (err) {
        providerFailure = {
          waveIndex,
          error: err.message,
          at: new Date().toISOString(),
        };
        stopReason = {
          exhausted: false,
          reason: "provider_or_runtime_failure",
          operationalStop: true,
          commercialExhaustion: false,
          resumable: true,
          error: err.message,
        };
        writeJson(path.join(waveDir, "wave-failure.json"), providerFailure);
        break;
      }

      lastSummary = waveResult;
      const spent = Number(waveResult?.spending?.spentUsd || 0) || 0;
      recordHotelCampaignSpend(baseCfg.hotelId, {
        runId: baseCfg.runId,
        waveIndex,
        spentUsd: spent,
        objective: waveObjective,
        provider: (waveResult?.spending?.providersUsed || ["exa"]).join("+"),
      });

      // Merge matured opportunities from wave artifacts
      const briefsPath = path.join(waveDir, "11-gold-standard-briefs.json");
      const briefs = readJson(briefsPath, []);
      const maturedPath = path.join(waveDir, "16c-association-long-form-investigation.json");
      const longForm = readJson(maturedPath, {});
      const maturedRows = longForm?.maturedRows || [];
      for (const row of maturedRows) {
        const opp = row.opportunityPayload || row.opportunity;
        if (opp) mergeOpportunity(state, opp, baseCfg.hotelId);
      }
      restoreProtectedOpportunities(state);
      // Also merge from publication readiness — never keep contaminated / false buyers as QUALIFIED
      const pub = readJson(path.join(waveDir, "06-publication-readiness.json"), {});
      const hotelHintForQ = {
        hotelId: baseCfg.hotelId,
        displayName: recon0.displayName || baseCfg.hotelName,
      };
      for (const q of pub.qualified || []) {
        if (!q.id) continue;
        const opp = state.opportunities.find((o) => o.id === q.id) || q;
        const nameOk = isAssociationEventsValidPersonName(opp?.contactName || q.contactName);
        const commercial = evaluateAssociationEventsPublicationQualification(opp, hotelHintForQ);
        if (
          !nameOk ||
          !commercial.qualified ||
          commercial.status === ASSOCIATION_EVENTS_PUBLICATION_STATUS.RESEARCH_CONTINUATION
        ) {
          demoteToContinuation(state, q.id);
          continue;
        }
        if (!state.qualifiedIds.includes(q.id)) state.qualifiedIds.push(q.id);
        state.continuationIds = (state.continuationIds || []).filter((id) => id !== q.id);
      }
      for (const c of pub.continuation || []) {
        if (c.id && !state.qualifiedIds.includes(c.id) && !state.continuationIds.includes(c.id)) {
          state.continuationIds.push(c.id);
        }
      }
      for (const b of briefs) {
        if (!b?.id) continue;
        if (b.qualificationStatus === "EXCLUDE_WRONG_FIT" || b.qualificationStatus === "EXCLUDE_DUPLICATE") {
          if (!state.deadEndIds.includes(b.id)) state.deadEndIds.push(b.id);
          state.continuationIds = (state.continuationIds || []).filter((id) => id !== b.id);
        }
        // Host-closed long-form dispositions
        const gap = b.researchGapClass;
        if (gap === "CONFIRMED_NOT_COMMERCIALLY_RELEVANT") {
          if (!state.deadEndIds.includes(b.id)) state.deadEndIds.push(b.id);
        }
      }

      const discovery = readJson(path.join(waveDir, "08-discovery.json"), {});
      for (const s of discovery.searches || []) {
        if (s.intent && !state.usedStrategyIntents.includes(s.intent)) {
          state.usedStrategyIntents.push(s.intent);
        }
      }
      for (const intent of rawConfig.campaignStrategyIntents || []) {
        /* no-op */
      }
      // Mark planned intents used even if runner filtered
      for (const s of strategies.slice(0, 8)) {
        if (s.intent && !state.usedStrategyIntents.includes(s.intent)) {
          // Only mark if discovery attempted this intent
          if ((discovery.searches || []).some((x) => x.intent === s.intent)) {
            /* already added */
          } else if (discovery.attempted) {
            // Still record attempted planning for redundancy tracking
          }
        }
      }
      // Force-record intents from this wave plan if searches empty due to cap
      for (const s of (discovery.searches || [])) {
        if (s.intent && !state.usedStrategyIntents.includes(s.intent)) {
          state.usedStrategyIntents.push(s.intent);
        }
      }
      // Always record planned intents that were passed to runner for redundancy metrics
      const planned = strategies.slice(0, 8).map((s) => s.intent);
      for (const intent of planned) {
        if ((discovery.searches || []).some((x) => x.intent === intent)) continue;
        // If discovery ran with campaignExpandedStrategies, intents may use strategy.intent from runner base set
      }

      const newQualified = (pub.qualified || []).filter(
        (q) => !(state.waves || []).some((w) => (w.qualifiedIds || []).includes(q.id)),
      ).length;
      // Better newQualified: count ids newly added this wave
      const prevQualified = new Set(
        waveSummaries.flatMap((w) => w.qualifiedIds || []),
      );
      const waveQualifiedIds = (pub.qualified || []).map((q) => q.id).filter(Boolean);
      const trulyNewQualified = waveQualifiedIds.filter((id) => !prevQualified.has(id)).length;

      const waveSummary = {
        waveIndex,
        objective: waveObjective,
        spentUsd: spent,
        remainingAfterUsd: reconcileAssociationEventsHotelBudget(baseCfg.hotelId).remainingUsd,
        allocation,
        funnel: waveResult?.funnel || null,
        qualifiedIds: waveQualifiedIds,
        continuationIds: (pub.continuation || []).map((c) => c.id),
        newQualified: trulyNewQualified,
        newCandidates: Number(waveResult?.funnel?.candidates || 0),
        discoveryHits: Number(discovery.hits || 0),
        discoveryAttempted: discovery.attempted === true,
        strategiesUsed: (discovery.searches || []).map((s) => s.intent),
        ok: waveResult?.ok !== false,
      };
      waveSummaries.push(waveSummary);
      wavesThisBatch += 1;
      state.waves = waveSummaries;
      state.waveCount = waveSummaries.length;
      state.updatedAt = new Date().toISOString();
      saveState(state);

      writeJson(path.join(waveDir, "wave-summary.json"), waveSummary);

      // Operational blockers: insufficient_strategies is recoverable next wave after rotation
      if (waveResult?.blockers?.length) {
        const onlyStrategyGap =
          waveResult.blockers.length === 1 &&
          waveResult.blockers[0] === "insufficient_strategies";
        if (onlyStrategyGap) {
          writeJson(path.join(waveDir, "wave-strategy-gap.json"), {
            waveIndex,
            note: "Recoverable — expanded/rotated strategies on next wave",
            blockers: waveResult.blockers,
          });
          // Mark planned intents used so next wave rotates
          for (const s of strategies.slice(0, 8)) {
            if (s.intent && !state.usedStrategyIntents.includes(s.intent)) {
              state.usedStrategyIntents.push(s.intent);
            }
          }
          saveState(state);
          continue;
        }
        stopReason = {
          exhausted: false,
          reason: "wave_blockers",
          operationalStop: true,
          resumable: true,
          checkpointStatus: "CHECKPOINT_RESUME_REQUIRED",
          blockers: waveResult.blockers,
        };
        break;
      }
    }
  } finally {
    releaseHotelCampaignLock(baseCfg.hotelId, baseCfg.runId);
  }

  const finalRecon = reconcileAssociationEventsHotelBudget(baseCfg.hotelId);
  restoreProtectedOpportunities(state);
  state.exhaustion = stopReason || state.exhaustion;
  saveState(state);

  const checkpoint = {
    version: ASSOCIATION_EVENTS_CONTINUOUS_CAMPAIGN_V1,
    evidenceLedgerVersion: ASSOCIATION_EVENTS_EVIDENCE_LEDGER_V1,
    campaignId: `assoc-campaign-${baseCfg.hotelId}`,
    hotelId: baseCfg.hotelId,
    runId: baseCfg.runId,
    completedWavesTotal: waveSummaries.length,
    wavesThisBatch,
    maxWavesThisBatch,
    cumulativeSpend: finalRecon,
    remainingUsd: finalRecon.remainingUsd,
    qualifiedIds: state.qualifiedIds || [],
    continuationIds: state.continuationIds || [],
    deadEndIds: state.deadEndIds || [],
    strategiesAttempted: state.usedStrategyIntents || [],
    stopReason,
    checkpointStatus:
      stopReason?.checkpointStatus ||
      (stopReason?.exhausted ? "STOPPED" : "CHECKPOINT_RESUME_REQUIRED"),
    resumable: stopReason?.resumable !== false && !stopReason?.commercialExhaustion,
    nextRecommendedStrategies: [
      "priority_buyer_recovery_longevity_imcp_anhp",
      "protect_larim_evidence",
      "validate_isaps_conpeht_lodging",
      "expand_unused_wave_strategies",
    ],
    resumeHint: {
      powershell: `.\\scripts\\Invoke-GdiAssociationEvents.ps1 -HotelId ${baseCfg.hotelId} -Mode FullE2E -BudgetUSD ${Math.max(0, Number((finalRecon.remainingUsd - SAFETY_MARGIN_USD).toFixed(3)))} -AuthorizePaidResearch -MaxWaves 12`,
      note: "MaxWaves bounds this execution batch only; campaign state and cumulative ledger resume automatically.",
    },
    campaignStatePath: campaignStatePath(baseCfg.hotelId),
    writtenAt: new Date().toISOString(),
  };
  writeJson(path.join(reportDir, "20-campaign-checkpoint.json"), checkpoint);
  writeJson(
    path.join(hotelCampaignStateDir(baseCfg.hotelId), "latest-checkpoint.json"),
    checkpoint,
  );

  // Aggregate gold briefs from all waves
  const allBriefs = [];
  const qualifiedDetails = [];
  for (const w of waveSummaries) {
    const waveDir = path.join(
      reportDir,
      "waves",
      `wave-${String(w.waveIndex).padStart(2, "0")}`,
    );
    const briefs = readJson(path.join(waveDir, "11-gold-standard-briefs.json"), []);
    for (const b of briefs) {
      allBriefs.push({ ...b, waveIndex: w.waveIndex });
      if (b.qualificationStatus === "QUALIFIED") {
        const namedOk = isAssociationEventsValidPersonName(b.brief?.whoToContact?.primary?.name);
        qualifiedDetails.push({
          id: b.id,
          organizationName: b.organizationName,
          eventName: b.eventName,
          waveIndex: w.waveIndex,
          namedBuyer: b.brief?.whoToContact?.primary?.name || null,
          namedBuyerValid: namedOk,
          route: b.brief?.whoToContact?.routes || null,
          goldScore: b.goldScore,
        });
      }
    }
  }
  writeJson(path.join(reportDir, "11-campaign-gold-standard-briefs.json"), allBriefs);
  writeJson(path.join(reportDir, "19-campaign-waves.json"), {
    version: ASSOCIATION_EVENTS_CONTINUOUS_CAMPAIGN_V1,
    waves: waveSummaries,
    stopReason,
    providerFailure,
  });
  writeJson(path.join(reportDir, "10-campaign-spending.json"), finalRecon);
  writeJson(path.join(reportDir, "campaign-state-final.json"), state);

  const uniqueQualified = [...new Set(state.qualifiedIds || [])];
  const uniqueContinuation = [...new Set(state.continuationIds || [])].filter(
    (id) => !uniqueQualified.includes(id) && !state.deadEndIds.includes(id),
  );

  const summary = {
    ok: !providerFailure,
    executed: true,
    continuousCampaign: true,
    version: ASSOCIATION_EVENTS_CONTINUOUS_CAMPAIGN_V1,
    hotelRunnerVersion: ASSOCIATION_EVENTS_HOTEL_RUNNER_V1,
    cumulativeBudgetVersion: ASSOCIATION_EVENTS_CUMULATIVE_BUDGET_V1,
    evidenceLedgerVersion: ASSOCIATION_EVENTS_EVIDENCE_LEDGER_V1,
    hotelId: baseCfg.hotelId,
    runId: baseCfg.runId,
    reportDir,
    campaignStatePath: campaignStatePath(baseCfg.hotelId),
    wavesExecutedTotal: waveSummaries.length,
    wavesThisBatch,
    maxWavesThisBatch,
    checkpointStatus: checkpoint.checkpointStatus,
    checkpointPath: path.join(reportDir, "20-campaign-checkpoint.json"),
    stopReason,
    providerFailure,
    // Budget / yield / batch bounds end the *execution*, not commercial research.
    // Only COMMERCIAL_RESEARCH_EXHAUSTED may claim researchComplete.
    researchComplete: checkpoint.checkpointStatus === "COMMERCIAL_RESEARCH_EXHAUSTED",
    executionFinished: true,
    commercialResearchComplete:
      checkpoint.checkpointStatus === "COMMERCIAL_RESEARCH_EXHAUSTED" &&
      uniqueContinuation.length === 0,
    continuationRequired:
      uniqueContinuation.length > 0 ||
      checkpoint.checkpointStatus === "CHECKPOINT_RESUME_REQUIRED" ||
      checkpoint.checkpointStatus === "RESEARCH_BUDGET_EXHAUSTED",
    funnel: {
      qualified: uniqueQualified.length,
      researchContinuation: uniqueContinuation.length,
      deadEnds: (state.deadEndIds || []).length,
      opportunitiesTracked: (state.opportunities || []).length,
      strategiesUsed: (state.usedStrategyIntents || []).length,
    },
    qualifiedIds: uniqueQualified,
    continuationIds: uniqueContinuation,
    deadEndIds: state.deadEndIds || [],
    qualifiedDetails,
    spending: {
      ...finalRecon,
      waveSpend: waveSummaries.map((w) => ({
        waveIndex: w.waveIndex,
        spentUsd: w.spentUsd,
        objective: w.objective,
      })),
      campaignWaveSpendUsd: Number(
        waveSummaries.reduce((s, w) => s + Number(w.spentUsd || 0), 0).toFixed(4),
      ),
    },
    hotel: lastSummary?.hotel || null,
    fullE2eComplete: false,
    fullE2eCompleteReason: "publication_not_authorized",
    productionWrite: { applied: false },
  };

  writeJson(path.join(reportDir, "SUMMARY.json"), summary);
  fs.writeFileSync(
    path.join(reportDir, "RUN_REPORT.md"),
    `# Association Events Continuous Campaign\n\nHotel: ${baseCfg.hotelId}\nWaves: ${waveSummaries.length}\nQualified: ${uniqueQualified.length}\nContinuation: ${uniqueContinuation.length}\nCampaign spend: $${summary.spending.campaignWaveSpendUsd}\nRemaining: $${finalRecon.remainingUsd} / cap $${finalRecon.cumulativeCapUsd}\nStop: ${stopReason?.reason || "unknown"}\n`,
    "utf8",
  );

  return summary;
}
