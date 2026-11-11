/**
 * GDI 2.0 Phase 3A — Corporate Meetings & Training FullE2E runner.
 *
 * Composes Scaling / Association / ORS / adaptive controller / buyer gate /
 * demand / eligibility — does NOT duplicate those systems.
 *
 * Local checkpoints only. No Airtable, canonical, visibility, or publication writes.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { resolveHotelGdiProfileForResearch } from "../hotel-gdi-profile-resolution-v1.js";
import { assertPaidContactProvidersDisabled } from "./commercial-spine-restoration-v41.js";
import {
  assertPhase2ePublicProviderAuthorization,
  runBoundedPublicResearchCycle,
  selectAdaptiveNextAction,
} from "../adaptive-public-research-controller-v1.js";
import { assertAdaptiveExecutionDisabled } from "../adaptive-execution-controller-design-v1.js";
import { isSharedEligibilityEnforcementEnabled } from "../publication-enforcement-policy-design-v1.js";
import { classifyDemandMechanismEvidence } from "../demand-mechanism-research-v1.js";
import { evaluateBuyerEvidenceGate } from "../buyer-evidence-gate-v1.js";
import {
  runIndependentDemandDiscovery,
  partitionDiscoveryOrigins,
  DISCOVERY_ORIGIN,
} from "./independent-demand-discovery-v1.js";
import { getCmtIndependentDiscoverySegmentConfig } from "./corporate-projects-extended-stay-discovery-v1.js";
import { runCmtCpeBoundedDeepInvestigation } from "./cmt-cpe-deep-investigation-bridge-v1.js";
import { buildCmtCpeGoldStandardBrief } from "./cmt-cpe-gold-brief-adapters-v1.js";
import {
  CMT_FULL_E2E_STAGES,
  CMT_PILOT_LIMITS,
  CMT_RESEARCH_SEGMENT,
  CORPORATE_MEETINGS_TRAINING_V1,
} from "./corporate-meetings-training-constants-v1.js";
import {
  buildCmtDiscoveryPlan,
  classifyCmtDiscoverySignal,
  listCmtDemandCategories,
} from "./corporate-meetings-training-discovery-v1.js";
import { evaluateCorporateMeetingsTrainingQualification } from "./corporate-meetings-training-qualification-v1.js";
import { assessCmtHotelSpecificMatch } from "./corporate-meetings-training-hotel-match-v1.js";

export const CMT_FULL_E2E_V1 = "gdi-corporate-meetings-training-full-e2e-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`, "utf8");
}

function readJson(p) {
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function hashKey(s) {
  return createHash("sha256").update(String(s || "")).digest("hex").slice(0, 16);
}

function investigationId(hotelId, org, category, seed) {
  return `cmt_${hashKey([hotelId, org, category, seed].join("|"))}`;
}

function orgKey(name) {
  return text(name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 64);
}

/**
 * Durable local checkpoint path (never Airtable).
 */
export function cmtCheckpointPath(reportDir, hotelId) {
  return path.join(reportDir, "checkpoints", `${text(hotelId)}.json`);
}

export function loadCmtCheckpoint(reportDir, hotelId) {
  return readJson(cmtCheckpointPath(reportDir, hotelId));
}

export function saveCmtCheckpoint(reportDir, hotelId, state) {
  writeJson(cmtCheckpointPath(reportDir, hotelId), {
    ...state,
    updatedAt: new Date().toISOString(),
    version: CMT_FULL_E2E_V1,
  });
}

/**
 * Deduplicate candidates by org + category + activity headline.
 */
export function dedupeCmtCandidates(candidates = [], priorKeys = []) {
  const seen = new Set(priorKeys.map((k) => text(k)));
  const out = [];
  for (const c of candidates) {
    const key =
      text(c.dedupeKey) ||
      [orgKey(c.organizationName), c.cmtDemandCategory, orgKey(c.activityHeadline || c.eventName)]
        .filter(Boolean)
        .join("::");
    if (!key || seen.has(key)) {
      out.push({ ...c, duplicate: true, dedupeKey: key });
      continue;
    }
    seen.add(key);
    out.push({ ...c, duplicate: false, dedupeKey: key });
  }
  return out;
}

/**
 * Build shadow opportunity brief (local only — not customer-facing).
 */
export function buildCmtOpportunityBrief(opportunity = {}, qualification = {}, hotelMatch = {}) {
  return {
    version: CMT_FULL_E2E_V1,
    researchSegment: CMT_RESEARCH_SEGMENT,
    investigationId: opportunity.investigationId || null,
    organizationName: opportunity.organizationName || null,
    activityHeadline: opportunity.activityHeadline || opportunity.eventName || null,
    cmtDemandCategory: opportunity.cmtDemandCategory || null,
    qualificationStatus: qualification.status || null,
    demandEvidenceClass: qualification.demand?.evidenceClass || null,
    buyerEligible: qualification.buyerGate?.buyerEligible === true,
    hotelFitClass: hotelMatch.fitClass || null,
    sources: (opportunity.sources || []).slice(0, 8).map((s) => ({
      url: s.url || s,
      title: s.title || null,
    })),
    hotelFacingEligibleUnchanged: true,
    customerVisibilityUnchanged: true,
    doesNotAuthorizePublication: true,
    syntheticFixture: opportunity.syntheticFixture === true,
    note: "Shadow brief only. No customer publication authorized in Phase 3A.",
  };
}

/**
 * Run FullE2E lifecycle for one hotel against provided candidates
 * (fixtures / cached evidence / optional bounded public research).
 */
export async function runCorporateMeetingsTrainingFullE2e(opts = {}) {
  assertAdaptiveExecutionDisabled();
  const enforcement = isSharedEligibilityEnforcementEnabled();
  if (enforcement?.enabled === true) {
    throw new Error("Shared eligibility enforcement must remain disabled in Phase 3A");
  }
  const paidGuard = assertPaidContactProvidersDisabled({});
  if (!paidGuard.ok) throw new Error(paidGuard.error || "paid_providers_blocked");

  const limits = { ...CMT_PILOT_LIMITS, ...(opts.limits || {}) };
  const hotelId = text(opts.hotelId);
  if (!hotelId) throw new Error("hotelId required");

  const reportDir =
    opts.reportDir ||
    path.join(ROOT, "reports", "gdi-2-review-loop", "phase3a", "pilots", hotelId);
  fs.mkdirSync(reportDir, { recursive: true });

  const stagesCompleted = [];
  const stopReasons = [];
  const ledger = {
    externalRetrievals: 0,
    estimatedUsd: 0,
    attemptedUrls: [],
    requests: [],
    deniedPaidAttempts: 0,
    sourceSignalMemory: new Map(),
    attemptedNormalized: new Set(),
    discoveredNavByOrigin: new Map(),
    crossOpportunityUrlCache: [],
    authorizePaidResearch: false,
  };

  // Stage: hotel_profile (offline-safe: skip live HPC unless explicitly allowed)
  const profile = await resolveHotelGdiProfileForResearch(hotelId, {
    persistSynthesized: false,
    skipHpc: opts.allowLiveHpc !== true,
  });
  let hotel = profile?.hotel || null;
  if (!hotel) {
    const cfgPath = path.join(
      ROOT,
      "config",
      "group-demand-intelligence",
      "hotels",
      `${hotelId}.json`,
    );
    hotel = readJson(cfgPath) || { hotelId };
    stopReasons.push("hotel_profile_config_fallback_no_live_hpc");
  }
  stagesCompleted.push("hotel_profile");

  let checkpoint = loadCmtCheckpoint(reportDir, hotelId) || {
    hotelId,
    completedInvestigationIds: [],
    attemptedStrategyIds: [],
    priorDedupeKeys: [],
    candidatesProcessed: 0,
  };
  if (opts.resume === false) {
    checkpoint = {
      hotelId,
      completedInvestigationIds: [],
      attemptedStrategyIds: [],
      priorDedupeKeys: [],
      candidatesProcessed: 0,
      resetForFreshRun: true,
    };
  }

  // Stage: segment_discovery plan
  const categories = listCmtDemandCategories();
  const discoveryPlans = categories.map((c) =>
    buildCmtDiscoveryPlan(c.id, { attemptedStrategyIds: checkpoint.attemptedStrategyIds }),
  );
  stagesCompleted.push("segment_discovery");

  let independentDiscovery = null;
  let candidates = Array.isArray(opts.candidates) ? [...opts.candidates] : [];
  // Never invent candidates — empty is an honest result
  if (!candidates.length && Array.isArray(opts.cachedCandidates)) {
    candidates = [...opts.cachedCandidates];
  }

  // Phase 3B-R: optional shared independent discovery (same engine as CPE)
  if (opts.runIndependentDiscovery === true) {
    const remaining = Math.max(
      0,
      Number(limits.maxExternalRetrievalsTotal) - Number(ledger.externalRetrievals || 0),
    );
    independentDiscovery = await runIndependentDemandDiscovery({
      hotel,
      segmentConfig: getCmtIndependentDiscoverySegmentConfig(),
      ledger,
      limits: {
        maxExternalRetrievals: Math.min(remaining, Number(opts.independentDiscoveryBudget || 12)),
        maxCandidates: limits.maxCandidatesPerHotel,
        maxSearchQueries: opts.maxSearchQueries,
      },
      searchFn: opts.searchFn || null,
      authorizeSearchIndexDiscovery: opts.authorizeSearchIndexDiscovery === true,
      // Live Exa only when caller explicitly sets authorizeLiveMeteredSearch + liveSearchFn.
      // Default remains false (Phase 3B-R mocked / offline).
      authorizeLiveMeteredSearch: opts.authorizeLiveMeteredSearch === true,
      liveSearchFn: opts.liveSearchFn || null,
      budgetCeilingUsd: Number(opts.searchIndexBudgetCeilingUsd || 0),
      skipDirectPublicFetches: opts.skipDirectPublicFetches === true,
    });
    candidates = [
      ...(independentDiscovery.candidates || []),
      ...candidates.filter((c) => c.discoveryOrigin !== DISCOVERY_ORIGIN.INDEPENDENT_PUBLIC),
    ];
    stagesCompleted.push("independent_segment_discovery");
    if (independentDiscovery.limitationNotes?.length) {
      stopReasons.push(
        ...independentDiscovery.limitationNotes.map((n) => `limitation:${n.slice(0, 80)}`),
      );
    }
    if (independentDiscovery.stopReasons?.length) {
      stopReasons.push(...independentDiscovery.stopReasons);
    }
  } else {
    stagesCompleted.push("independent_segment_discovery");
    stopReasons.push("independent_discovery_not_requested_this_run");
  }

  candidates = dedupeCmtCandidates(candidates, checkpoint.priorDedupeKeys).filter(
    (c) => !c.duplicate,
  );
  candidates = candidates.slice(0, limits.maxCandidatesPerHotel);

  const results = [];
  let publicAuth = null;

  for (const raw of candidates) {
    if (results.length >= limits.maxCandidatesPerHotel) {
      stopReasons.push("max_candidates_per_hotel");
      break;
    }

    const category = raw.cmtDemandCategory || raw.demandCategory || null;
    const invId =
      raw.investigationId ||
      investigationId(hotelId, raw.organizationName, category, raw.activityHeadline || raw.sourceUrl);

    if (opts.resume !== false && checkpoint.completedInvestigationIds.includes(invId)) {
      results.push({
        investigationId: invId,
        skipped: true,
        reason: "already_completed_in_checkpoint",
      });
      continue;
    }

    // Candidate identity resolution
    const identity = {
      organizationName: text(raw.organizationName),
      orgKey: orgKey(raw.organizationName),
      investigationId: invId,
      researchSegment: CMT_RESEARCH_SEGMENT,
      syntheticFixture: raw.syntheticFixture === true,
    };
    stagesCompleted.push("candidate_identity_resolution");

    const discovery = classifyCmtDiscoverySignal({
      headline: raw.activityHeadline || raw.eventName,
      description: raw.whatIsHappening || raw.hotelDemandThesis,
      evidenceQuote: raw.evidenceQuote,
      category,
      indicators: raw.cmtIndicators || [],
      synthetic: raw.syntheticFixture === true,
    });

    let opportunity = {
      ...raw,
      id: raw.id || invId,
      investigationId: invId,
      hotelId,
      researchSegment: CMT_RESEARCH_SEGMENT,
      organizationName: identity.organizationName,
      cmtDemandCategory: category,
      discoveryOrigin:
        raw.discoveryOrigin ||
        (raw.syntheticFixture
          ? DISCOVERY_ORIGIN.SYNTHETIC_FIXTURE
          : raw.cachedEvidence
            ? DISCOVERY_ORIGIN.CACHED_INVENTORY
            : DISCOVERY_ORIGIN.SEEDED_CANDIDATE_LIST),
      primaryContact: raw.primaryContact || null,
      contactName: raw.contactName || raw.primaryContact?.name,
      contactRole: raw.contactRole || raw.primaryContact?.role || raw.primaryContact?.title,
      organizationContactEmail:
        raw.organizationContactEmail || raw.primaryContact?.email || raw.contactEmail,
      contactDataOrigin: raw.contactDataOrigin || "PUBLIC_SOURCE",
    };

    // Demand investigation (classifier — no fabrication)
    const demand = classifyDemandMechanismEvidence(opportunity, raw.extraction || {});
    stagesCompleted.push("demand_investigation");

    // Optional bounded public research (no-fee only)
    let publicCycle = null;
    if (
      opts.allowPublicResearch === true &&
      discovery.status === "CANDIDATE" &&
      !raw.syntheticFixture &&
      ledger.externalRetrievals < limits.maxExternalRetrievalsTotal
    ) {
      publicAuth = publicAuth || assertPhase2ePublicProviderAuthorization();
      if (!publicAuth.ok) {
        stopReasons.push("public_provider_authorization_failed");
      } else {
        const remaining = limits.maxExternalRetrievalsTotal - ledger.externalRetrievals;
        const next = selectAdaptiveNextAction(opportunity, {
          enforceBuyerEvidenceGate: true,
          useBuyerDiscoveryStrategies: true,
          attemptedStrategyIds: checkpoint.attemptedStrategyIds,
        });
        if (next?.continue === true && next.strategy) {
          try {
            publicCycle = await runBoundedPublicResearchCycle(opportunity, {
              ledger,
              limits: {
                maxExternalRetrievalsPilot: remaining,
                maxUrlsPerAction: Math.min(2, remaining),
                maxRetrievalsThisAction: Math.min(2, remaining),
                maxActionsPerOpportunity: 1,
                enforceBuyerEvidenceGate: true,
                useBuyerDiscoveryStrategies: true,
                authorizePaidResearch: false,
              },
              maxActionsThisCycle: 1,
            });
            if (publicCycle?.workingOpportunity) {
              opportunity = publicCycle.workingOpportunity;
            }
            if (next.strategy?.id) {
              checkpoint.attemptedStrategyIds.push(next.strategy.id);
            }
          } catch (err) {
            stopReasons.push(`public_research_error:${String(err?.message || err).slice(0, 120)}`);
            publicCycle = {
              error: String(err?.message || err),
              retrievalsUsed: 0,
              actions: [],
            };
          }
        } else {
          stopReasons.push(next?.reason || next?.decision || "adaptive_stop");
        }
      }
    }
    stagesCompleted.push("buyer_research");

    let deepInvestigation = null;
    if (opts.runDeepInvestigation === true && !raw.syntheticFixture) {
      deepInvestigation = await runCmtCpeBoundedDeepInvestigation({
        opportunity,
        hotel,
        corpus: opts.deepResearchCorpus || null,
        paidSearch: opts.paidSearch || opts.searchFn || null,
        budgetCeilingUsd: Number(opts.deepInvestigationBudgetCeilingUsd || 0),
        maxIterations: Number(opts.deepInvestigationMaxIterations || 3),
        runInvestigationLoop: opts.runInvestigationLoop === true,
      });
      if (deepInvestigation?.opportunity) {
        opportunity = deepInvestigation.opportunity;
      }
      if (deepInvestigation?.stopReason) {
        stopReasons.push(`deep:${deepInvestigation.stopReason}`);
      }
      stagesCompleted.push("deep_investigation_ors");
    }

    // Hotel matching
    const hotelMatch = assessCmtHotelSpecificMatch(opportunity, hotel, { hotelId });
    stagesCompleted.push("hotel_matching");

    // Shared qualification (shadow) — after optional deep investigation
    const qualification = evaluateCorporateMeetingsTrainingQualification(opportunity, hotel, {
      synthetic: raw.syntheticFixture === true,
      extraction: raw.extraction || {},
      segmentFamily: "generic",
    });
    stagesCompleted.push("shared_qualification");
    stagesCompleted.push("research_continuation_or_shadow_sales_ready");

    const brief = buildCmtOpportunityBrief(opportunity, qualification, hotelMatch);
    const goldBrief =
      opts.buildGoldBrief === true
        ? buildCmtCpeGoldStandardBrief({
            opportunity,
            hotel,
            qualification,
            hotelMatch,
            segment: "cmt",
          })
        : null;
    stagesCompleted.push("evidence_backed_opportunity_brief");

    const buyerGate = evaluateBuyerEvidenceGate(
      {
        name: opportunity.contactName || opportunity.primaryContact?.name,
        role: opportunity.contactRole || opportunity.primaryContact?.role,
        email: opportunity.organizationContactEmail || opportunity.primaryContact?.email,
        organization: opportunity.organizationName,
      },
      opportunity,
      {
        contactDataOrigin: opportunity.contactDataOrigin || "PUBLIC_SOURCE",
        affiliationVerified: Boolean(opportunity.organizationName),
      },
    );

    const row = {
      investigationId: invId,
      organizationName: identity.organizationName,
      cmtDemandCategory: category,
      discoveryOrigin: opportunity.discoveryOrigin,
      syntheticFixture: identity.syntheticFixture,
      cachedEvidence: raw.cachedEvidence === true,
      discovery,
      demand: {
        evidenceClass: demand.evidenceClass,
        reason: demand.reason,
        mechanismsDetected: demand.mechanismsDetected,
      },
      buyerGate: {
        buyerEligible: buyerGate.buyerEligible,
        completedStages: buyerGate.completedStages,
        notes: buyerGate.notes,
      },
      hotelMatch: {
        fitClass: hotelMatch.fitClass,
        commerciallyAccessible: hotelMatch.commerciallyAccessible,
        uncertaintyPreserved: hotelMatch.uncertaintyPreserved,
      },
      qualification,
      brief,
      goldBrief,
      deepInvestigation: deepInvestigation
        ? {
            ok: deepInvestigation.ok === true,
            stopReason: deepInvestigation.stopReason || null,
            commercialDisqualificationImplied:
              deepInvestigation.commercialDisqualificationImplied === true,
          }
        : null,
      publicResearch: publicCycle
        ? {
            retrievalsUsed: publicCycle.retrievalsUsed || 0,
            actions: (publicCycle.actions || []).map((a) => ({
              strategyId: a.strategyId,
              informationFound: a.informationFound === true,
            })),
          }
        : null,
      evidenceProvenance: (opportunity.sources || []).slice(0, 5),
    };

    results.push(row);
    checkpoint.completedInvestigationIds.push(invId);
    if (raw.dedupeKey || row.organizationName) {
      checkpoint.priorDedupeKeys.push(
        raw.dedupeKey ||
          [orgKey(row.organizationName), category, orgKey(raw.activityHeadline)].join("::"),
      );
    }
    checkpoint.candidatesProcessed += 1;
  }

  stagesCompleted.push("checkpoint_persist_local_only");
  saveCmtCheckpoint(reportDir, hotelId, checkpoint);

  const origins = partitionDiscoveryOrigins(results.filter((r) => !r.skipped));
  const summary = summarizeCmtRunResults(results, ledger, limits, independentDiscovery);

  const output = {
    version: CMT_FULL_E2E_V1,
    parentVersion: CORPORATE_MEETINGS_TRAINING_V1,
    researchSegment: CMT_RESEARCH_SEGMENT,
    hotelId,
    hotelDisplayName: hotel.displayName || hotel.name || null,
    stages: CMT_FULL_E2E_STAGES,
    stagesCompleted: [...new Set(stagesCompleted)],
    discoveryPlans: discoveryPlans.map((p) => ({
      categoryId: p.categoryId,
      ok: p.ok,
      verifiableIndicators: p.verifiableIndicators,
      buyerStrategyCount: (p.buyerStrategies || []).length,
    })),
    independentDiscovery: independentDiscovery
      ? {
          independentFreshCount: independentDiscovery.independentFreshCount,
          retrievalsUsed: independentDiscovery.retrievalsUsed,
          limitationNotes: independentDiscovery.limitationNotes,
          sourcePlan: independentDiscovery.sourcePlan,
          searchIndexPass: independentDiscovery.searchIndexPass,
          searchQuestions: independentDiscovery.searchQuestions,
          stagesCompleted: independentDiscovery.stagesCompleted,
          stopReasons: independentDiscovery.stopReasons,
        }
      : null,
    originPartition: {
      independentPublic: origins.independentPublic.length,
      cachedInventory: origins.cachedInventory.length,
      syntheticFixture: origins.syntheticFixture.length,
      seededCandidateList: origins.seededCandidateList.length,
    },
    limits,
    ledger: {
      externalRetrievals: ledger.externalRetrievals,
      estimatedUsd: ledger.estimatedUsd,
      attemptedUrlCount: (ledger.attemptedUrls || []).length,
      authorizePaidResearch: false,
      searchQueries: (ledger.searchQueries || []).length,
    },
    stopReasons: [...new Set(stopReasons)],
    results,
    summary,
    authorizations: {
      paidResearch: false,
      publication: false,
      productionWrites: false,
      airtableWrites: false,
      customerVisibilityMutation: false,
      searchIndexDiscovery: opts.authorizeSearchIndexDiscovery === true,
      liveMeteredSearch: false,
    },
    generatedAt: new Date().toISOString(),
  };

  writeJson(path.join(reportDir, "full-e2e-result.json"), output);
  return output;
}

export function summarizeCmtRunResults(results = [], ledger = {}, limits = {}, independentDiscovery = null) {
  const synth = results.filter((r) => !r.skipped && r.syntheticFixture === true);
  const cached = results.filter(
    (r) => !r.skipped && !r.syntheticFixture && r.cachedEvidence === true,
  );
  const fresh = results.filter(
    (r) => !r.skipped && !r.syntheticFixture && r.cachedEvidence !== true,
  );
  const independentPublic = results.filter(
    (r) =>
      !r.skipped &&
      !r.syntheticFixture &&
      r.discoveryOrigin === DISCOVERY_ORIGIN.INDEPENDENT_PUBLIC,
  );
  const countBucket = (bucket) => ({
    candidates: bucket.length,
    newDemandSignals: bucket.filter((r) => r.discovery?.status === "CANDIDATE").length,
    organizationsVerified: bucket.filter((r) => text(r.organizationName)).length,
    activitiesVerified: bucket.filter(
      (r) =>
        r.discovery?.status === "CANDIDATE" ||
        r.discovery?.status === "AMBIGUOUS_PRECONTRACTED_VENUE",
    ).length,
    hotelDemandMechanismsSupported: bucket.filter(
      (r) =>
        r.demand?.evidenceClass === "A_VERIFIED_HOTEL_OR_ACCOMMODATION_REQUIREMENT" ||
        r.demand?.evidenceClass === "B_EVIDENCE_GROUNDED_REALISTIC_HOTEL_DEMAND",
    ).length,
    namedBuyersIdentified: bucket.filter((r) =>
      (r.buyerGate?.completedStages || []).includes("NAMED_PERSON_IDENTIFIED"),
    ).length,
    buyerResponsibilitiesVerified: bucket.filter((r) =>
      (r.buyerGate?.completedStages || []).includes("COMMERCIAL_RELEVANCE_VERIFIED"),
    ).length,
    contactRoutesConfirmed: bucket.filter((r) =>
      (r.buyerGate?.completedStages || []).includes("CONTACT_ROUTE_VERIFIED"),
    ).length,
    hotelSpecificMatches: bucket.filter((r) => r.hotelMatch?.commerciallyAccessible).length,
    shadowSalesReady: bucket.filter((r) => r.qualification?.status === "SHADOW_SALES_READY")
      .length,
    researchContinuation: bucket.filter(
      (r) => r.qualification?.status === "RESEARCH_CONTINUATION",
    ).length,
    rejectedFalsePositives: bucket.filter(
      (r) => r.qualification?.status === "REJECTED_FALSE_POSITIVE",
    ).length,
  });

  const freshCounts = countBucket(fresh);
  const cachedCounts = countBucket(cached);
  const synthCounts = countBucket(synth);

  const independentCounts = countBucket(independentPublic);
  const counters = {
    candidatesProcessed: results.filter((r) => !r.skipped).length,
    syntheticFixtures: synth.length,
    cachedEvidenceCandidates: cached.length,
    freshResearchCandidates: fresh.length,
    independentPublicCandidates: independentPublic.length,
    independentDiscoveryEngineFresh: independentDiscovery?.independentFreshCount || 0,
    // Default commercial headline = fresh only (not cached, not synthetic)
    ...freshCounts,
    // Backward-compatible aliases
    realCandidates: fresh.length,
    syntheticNotCountedAsCommercial: synth.length,
    cachedNotCountedAsFreshDiscovery: cached.length,
    bySource: {
      fresh: freshCounts,
      independentPublic: independentCounts,
      cachedEvidence: cachedCounts,
      syntheticFixture: synthCounts,
    },
  };

  return {
    counters,
    requestCost: {
      externalRetrievals: ledger.externalRetrievals || 0,
      estimatedUsd: ledger.estimatedUsd || 0,
      ceiling: limits.maxExternalRetrievalsTotal,
    },
    note: "Commercial headline counters are fresh-research only. Synthetic and cached evidence are reported separately and never counted as independent discovery.",
  };
}
