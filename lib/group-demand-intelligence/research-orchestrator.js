/**
 * GDI research orchestrator — admin-triggered only.
 * Escalation: L1 → L2 methods → L3 web → L4 Apify/Ampfy → L5 Webhound ≤ $15 hard cap.
 * Does not auto-run on page load. Does not write ADP or Census.
 *
 * Pilot paths:
 * - Bethesda (PILOT_HOTEL_ID): seed + deepen + DMV expansion (legacy first-hotel path)
 * - Any onboarded hotel (config present): profile from ADP + seedCandidates / empty, then
 *   shared qualification + rad enrichment (no Bethesda seeds, no DMV expansion)
 */

import {
  buildBethesdaMarriottProfileFromExistingKnowledge,
  buildHotelGroupDemandProfile,
  PILOT_HOTEL_ID,
  loadHotelDemandConfig,
  isHotelOnboardedForGdi,
} from "./hotel-profile.js";
import { isPilotReferenceLogicEnabled } from "./pilot-path-policy.js";
import { buildBethesdaPilotOpportunities } from "./pilot-seed-opportunities.js";
import { applyBethesdaDeepenPass } from "./deepen-pass.js";
import { applyRadFeedbackEnrichmentPass } from "./rad-feedback-enrichment.js";
import { applyDmvExpansionPass } from "./dmv-expansion-pass.js";
import { applyQualificationPrecisionPass } from "./qualification-precision-pass.js";
import {
  createId,
  saveHotelProfile,
  saveOpportunities,
  saveResearchRun,
  loadOpportunities,
} from "./repository.js";
import { saveOpportunitiesCanonical } from "./opportunity-persistence.js";
import {
  createEmptyCostLedger,
  logEscalation,
  recordWebhoundSpend,
  recordProviderSpend,
  summarizeCostPerOpportunity,
  resolveAmpfyAdapterStatus,
  getWebhoundHardCapUsd,
} from "./cost-ledger.js";
import { listGdiResearchMethods } from "./research-methods.js";
import { filterSalespersonView, buildOpportunity } from "./opportunity-factory.js";
import { PRIORITY, RESEARCH_LEVELS_GDI } from "./claim-types.js";
import { isExternalResearchEnabled } from "../hotel-intelligence/research/policy.js";
import { GDI_PRODUCT_VERSION } from "./feature-flag.js";

/**
 * @param {{
 *   hotelId: string,
 *   trigger?: string,
 *   webhoundSessionId?: string|null,
 *   webhoundCostUsd?: number,
 *   webhoundCalls?: Array<{ sessionId: string, costUsd: number, question?: string, url?: string }>,
 *   seedCandidates?: object[],
 *   discoveryMode?: 'BETHESDA_LEGACY_SEEDS' | 'INDEPENDENT_CANDIDATES' | 'EMPTY',
 *   dryRun?: boolean
 * }} opts
 */
export async function runGroupDemandResearch(opts = {}) {
  const hotelId = String(opts.hotelId || "").trim();
  if (!hotelId) {
    const err = new Error("hotelId_required");
    err.code = "hotelId_required";
    throw err;
  }

  const runId = createId("gdi_run");
  const startedAt = new Date().toISOString();
  const ledger = createEmptyCostLedger();
  const methods = listGdiResearchMethods();
  const ampfyStatus = resolveAmpfyAdapterStatus();
  const demandConfig = loadHotelDemandConfig(hotelId);
  const pilotLogicEnabled = isPilotReferenceLogicEnabled(hotelId, {
    forceEnablePilotReferenceLogic: opts.enablePilotReferenceLogic === true,
    forceDisablePilotReferenceLogic: opts.enablePilotReferenceLogic === false,
  });
  const isBethesdaPilot = pilotLogicEnabled && hotelId === PILOT_HOTEL_ID;

  if (!isHotelOnboardedForGdi(hotelId) && !isBethesdaPilot) {
    const err = new Error("hotel_not_onboarded_for_gdi");
    err.code = "hotel_not_onboarded_for_gdi";
    err.message = `Hotel ${hotelId} is not onboarded. Add config/group-demand-intelligence/hotels/${hotelId}.json (or use pilot ${PILOT_HOTEL_ID} with pilot reference logic enabled).`;
    throw err;
  }

  logEscalation(ledger, {
    level: RESEARCH_LEVELS_GDI.L1_EXISTING_KNOWLEDGE,
    question: "Load hotel group demand profile from Dealality knowledge",
    result: "started",
  });

  const profile = isBethesdaPilot
    ? buildBethesdaMarriottProfileFromExistingKnowledge()
    : buildHotelGroupDemandProfile(hotelId);

  logEscalation(ledger, {
    level: RESEARCH_LEVELS_GDI.L1_EXISTING_KNOWLEDGE,
    question: "Hotel profile from ADP RO fixture + GDI demand config",
    result: "ok",
    sources: profile.researchNotes?.level1Sources || [],
    adpCanonicalReuse: profile.adpCanonicalReuse || null,
  });

  logEscalation(ledger, {
    level: RESEARCH_LEVELS_GDI.L2_CANONICAL_METHODS,
    question: "Apply GDI experimental research methods / seed qualification",
    methods: methods.map((m) => m.id),
    result: "ok",
  });

  logEscalation(ledger, {
    level: RESEARCH_LEVELS_GDI.L3_STANDARD_WEB,
    question: isBethesdaPilot
      ? "Official association / NIH / tournament / SAM.gov sources"
      : "Independent discovery candidates (no Bethesda seed path)",
    result: "ok",
    note: isBethesdaPilot
      ? "Pilot seed embeds Level-3 verified source URLs; live SerpAPI loop deferred to scale phase"
      : "Candidates supplied via seedCandidates / Webhound import — not Bethesda demand-type seeds",
  });

  logEscalation(ledger, {
    level: RESEARCH_LEVELS_GDI.L4_SUPPLEMENTAL,
    question: "Ampfy/Apify supplemental enrichment",
    result: ampfyStatus.status,
    ampfy: ampfyStatus,
    apify: "not_invoked_pilot_default",
  });

  let opportunities = [];
  let deepen = null;
  let dmvExpansionMeta = null;
  const discoveryMode =
    opts.discoveryMode ||
    (isBethesdaPilot ? "BETHESDA_LEGACY_SEEDS" : "INDEPENDENT_CANDIDATES");

  if (
    isBethesdaPilot &&
    discoveryMode === "BETHESDA_LEGACY_SEEDS" &&
    pilotLogicEnabled
  ) {
    opportunities = buildBethesdaPilotOpportunities();
    deepen = applyBethesdaDeepenPass(opportunities, {
      newOpportunities: opts.newOpportunities,
      webhoundEvidenceByOppId: opts.webhoundEvidenceByOppId || null,
      includeDiscovery: opts.includeDiscovery !== false,
    });
    opportunities = deepen.opportunities;
    ledger.incrementalRoi = deepen.roiLog || [];

    const dmvAllowed =
      opts.includeDmvExpansion !== false &&
      isPilotReferenceLogicEnabled(hotelId, {
        forDmvExpansion: true,
        forceEnablePilotReferenceLogic: opts.enablePilotReferenceLogic === true,
        forceDisablePilotReferenceLogic: opts.enablePilotReferenceLogic === false,
      });
    if (dmvAllowed) {
      const dmv = applyDmvExpansionPass(opportunities);
      opportunities = dmv.opportunities;
      dmvExpansionMeta = {
        addedIds: dmv.addedIds,
        rejectedCount: (dmv.rejectedExamples || []).length,
        researchNote: dmv.researchNote,
      };
      logEscalation(ledger, {
        level: RESEARCH_LEVELS_GDI.L3_STANDARD_WEB,
        question: "DMV expansion discovery across DC / NoVA / PG / Loudoun source pools",
        result: "ok",
        added: dmv.addedIds.length,
        rejectedExamples: dmv.rejectedExamples.length,
      });
    }
  } else {
    // Portable path — do NOT run Bethesda seeds or DMV expansion.
    const rawCandidates = Array.isArray(opts.seedCandidates) ? opts.seedCandidates : [];
    opportunities = rawCandidates.map((raw) =>
      buildOpportunity({
        ...raw,
        hotelId: raw.hotelId || hotelId,
      })
    );
    logEscalation(ledger, {
      level: RESEARCH_LEVELS_GDI.L3_STANDARD_WEB,
      question: "Ingest independent discovery candidates",
      result: "ok",
      candidateCount: opportunities.length,
      discoveryMode,
      note: "No Bethesda pilot seeds; no DMV expansion pass",
    });
  }

  const radEnrich = applyRadFeedbackEnrichmentPass(opportunities, hotelId);
  opportunities = radEnrich.opportunities;
  ledger.radFeedbackEnrichment = radEnrich.enrichment;
  if (dmvExpansionMeta) ledger.dmvExpansion = dmvExpansionMeta;

  if (opts.includeQualificationPrecision !== false) {
    const qp = applyQualificationPrecisionPass(opportunities);
    opportunities = qp.opportunities;
    ledger.qualificationPrecision = qp.enrichment;
  }

  const webhoundCalls = [];
  if (Array.isArray(opts.webhoundCalls) && opts.webhoundCalls.length) {
    for (const call of opts.webhoundCalls) {
      if (call?.sessionId) webhoundCalls.push(call);
    }
  } else if (opts.webhoundSessionId) {
    webhoundCalls.push({
      sessionId: opts.webhoundSessionId,
      costUsd: Number(opts.webhoundCostUsd || 0),
      question:
        opts.webhoundQuestion ||
        (isBethesdaPilot
          ? "Bethesda group demand deep research"
          : `${demandConfig?.displayName || hotelId} group demand deep research`),
      url: opts.webhoundUrl || null,
      opportunityIds: opts.webhoundOpportunityIds || [],
    });
  }

  let webhoundMeta = null;
  if (webhoundCalls.length) {
    if (!isExternalResearchEnabled() && opts.allowWebhoundWithoutFlag !== true) {
      logEscalation(ledger, {
        level: RESEARCH_LEVELS_GDI.L5_WEBHOUND,
        question: "Merge Webhound session(s)",
        result: "skipped_external_research_disabled",
        sessions: webhoundCalls.map((c) => c.sessionId),
      });
    } else {
      const recorded = [];
      for (const call of webhoundCalls) {
        const cost = Number(call.costUsd || 0);
        if (!(cost > 0)) {
          logEscalation(ledger, {
            level: RESEARCH_LEVELS_GDI.L5_WEBHOUND,
            question: call.question || "Webhound session linked; cost pending",
            result: "pending_cost",
            sessionId: call.sessionId,
          });
          recorded.push({
            sessionId: call.sessionId,
            costUsd: null,
            url: call.url || null,
            status: "pending_cost",
          });
          continue;
        }
        try {
          recordWebhoundSpend(ledger, {
            sessionId: call.sessionId,
            question:
              call.question ||
              opts.webhoundQuestion ||
              `${demandConfig?.displayName || hotelId} group demand deep research`,
            costUsd: cost,
            opportunityIds: call.opportunityIds || opts.webhoundOpportunityIds || [],
          });
          recorded.push({
            sessionId: call.sessionId,
            costUsd: cost,
            url: call.url || `https://webhound.ai/session/${call.sessionId}`,
          });
          logEscalation(ledger, {
            level: RESEARCH_LEVELS_GDI.L5_WEBHOUND,
            question:
              call.question ||
              opts.webhoundQuestion ||
              `${demandConfig?.displayName || hotelId} group demand deep research`,
            result: "recorded",
            sessionId: call.sessionId,
            costUsd: cost,
          });
        } catch (err) {
          logEscalation(ledger, {
            level: RESEARCH_LEVELS_GDI.L5_WEBHOUND,
            question: "Webhound spend",
            result: "blocked",
            sessionId: call.sessionId,
            error: err.code || err.message,
          });
          if (!opts.continueOnWebhoundCap) throw err;
        }
      }
      webhoundMeta = {
        sessions: recorded,
        costUsd: ledger.webhoundUsd,
        hardCapUsd: getWebhoundHardCapUsd(),
      };
      if (Array.isArray(opts.webhoundMergedOpportunityIds)) {
        opportunities = opportunities.map((o) =>
          opts.webhoundMergedOpportunityIds.includes(o.id)
            ? { ...o, webhoundUsed: true }
            : o
        );
      }
    }
  } else {
    logEscalation(ledger, {
      level: RESEARCH_LEVELS_GDI.L5_WEBHOUND,
      question: "Selective deep research",
      result: "not_requested_this_run",
      hardCapUsd: getWebhoundHardCapUsd(),
    });
  }

  if (opts.recordSerpEstimateUsd) {
    recordProviderSpend(ledger, "serpapi", opts.recordSerpEstimateUsd, {
      note: "estimated_or_metered",
    });
  }

  const active = filterSalespersonView(opportunities);
  const high = active.filter((o) => o.priority === PRIORITY.HIGH);
  const medium = active.filter((o) => o.priority === PRIORITY.MEDIUM);
  const watch = active.filter((o) => o.priority === PRIORITY.WATCHLIST);
  const disqualified = opportunities.filter((o) => o.priority === PRIORITY.DISQUALIFIED);

  const sourceUrls = new Set();
  for (const o of opportunities) {
    for (const e of o.evidence || []) {
      if (e.sourceUrl) sourceUrls.add(e.sourceUrl);
    }
  }

  const costSummary = summarizeCostPerOpportunity(
    ledger,
    active.length,
    high.length || 1
  );

  const completedAt = new Date().toISOString();
  const run = {
    id: runId,
    hotelId,
    status: "COMPLETED",
    productVersion: GDI_PRODUCT_VERSION,
    startedAt,
    completedAt,
    trigger: opts.trigger || "admin_run_research",
    discoveryMode,
    configuration: {
      demandConfig,
      webhoundHardCapUsd: getWebhoundHardCapUsd(),
      ampfyStatus,
      methodsVersion: methods[0] ? "gdi-research-methods-v1-experimental" : null,
    },
    providers: {
      dealality_existing_knowledge: true,
      gdi_research_methods: true,
      standard_web: true,
      ampfy: ampfyStatus.status,
      apify: "not_invoked_pilot_default",
      webhound: webhoundMeta,
    },
    researchMethods: methods.map((m) => m.id),
    metrics: {
      candidatesResearched: opportunities.length,
      opportunitiesQualified: active.length,
      highPriorityCount: high.length,
      mediumPriorityCount: medium.length,
      watchlistCount: watch.length,
      disqualifiedCount: disqualified.length,
      sourceCount: sourceUrls.size,
      duplicatesRemoved: 0,
      expiredRemoved: 0,
    },
    cost: {
      ...ledger,
      ...costSummary,
    },
    opportunityIds: opportunities.map((o) => o.id),
    errors: [],
    adpTouched: false,
    censusWritten: false,
    portability: {
      bethesdaSeedPathUsed: isBethesdaPilot && discoveryMode === "BETHESDA_LEGACY_SEEDS",
      dmvExpansionUsed: Boolean(dmvExpansionMeta),
      hotelConfigPath: `config/group-demand-intelligence/hotels/${hotelId}.json`,
    },
  };

  if (!opts.dryRun) {
    saveHotelProfile(hotelId, profile);
    await saveOpportunitiesCanonical(hotelId, {
      hotelId,
      runId,
      opportunities,
      salespersonVisibleIds: active.map((o) => o.id),
    });
    saveResearchRun(hotelId, run);
  }

  return {
    ok: true,
    dryRun: Boolean(opts.dryRun),
    run,
    profile,
    opportunities,
    summary: {
      qualified: active.length,
      high: high.length,
      medium: medium.length,
      watchlist: watch.length,
      disqualified: disqualified.length,
      researchCostUsd: ledger.totalUsd,
      webhoundUsd: ledger.webhoundUsd,
    },
  };
}

export async function getLatestOpportunities(hotelId) {
  const { loadOpportunitiesCanonical } = await import(
    "./opportunity-persistence.js"
  );
  return loadOpportunitiesCanonical(hotelId);
}
