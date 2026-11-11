/**
 * GDI 2.0 Phase 3B — Corporate Projects & Extended Stay FullE2E.
 *
 * Composes shared independent discovery + buyer gate + demand thesis + ORS-ready
 * adaptive research. Local checkpoints only. No publication / Airtable writes.
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
import { classifyCommercialDemandThesis } from "../commercial-demand-thesis-standard-v1.js";
import { evaluateBuyerEvidenceGate } from "../buyer-evidence-gate-v1.js";
import {
  runIndependentDemandDiscovery,
  partitionDiscoveryOrigins,
  DISCOVERY_ORIGIN,
} from "./independent-demand-discovery-v1.js";
import {
  CPE_FULL_E2E_STAGES,
  CPE_PILOT_LIMITS,
  CPE_RESEARCH_SEGMENT,
  CORPORATE_PROJECTS_EXTENDED_STAY_V1,
} from "./corporate-projects-extended-stay-constants-v1.js";
import {
  buildCpeDiscoveryPlan,
  classifyCpeDiscoverySignal,
  listCpeDemandCategories,
  getCpeIndependentDiscoverySegmentConfig,
} from "./corporate-projects-extended-stay-discovery-v1.js";
import { evaluateCorporateProjectsExtendedStayQualification } from "./corporate-projects-extended-stay-qualification-v1.js";
import { assessCpeHotelSpecificMatch } from "./corporate-projects-extended-stay-hotel-match-v1.js";
import { runCmtCpeBoundedDeepInvestigation } from "./cmt-cpe-deep-investigation-bridge-v1.js";
import { buildCmtCpeGoldStandardBrief } from "./cmt-cpe-gold-brief-adapters-v1.js";

export const CPE_FULL_E2E_V1 = "gdi-corporate-projects-extended-stay-full-e2e-v1";

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
  return `cpe_${hashKey([hotelId, org, category, seed].join("|"))}`;
}
function orgKey(name) {
  return text(name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 64);
}

export function cpeCheckpointPath(reportDir, hotelId) {
  return path.join(reportDir, "checkpoints", `${text(hotelId)}.json`);
}
export function loadCpeCheckpoint(reportDir, hotelId) {
  return readJson(cpeCheckpointPath(reportDir, hotelId));
}
export function saveCpeCheckpoint(reportDir, hotelId, state) {
  writeJson(cpeCheckpointPath(reportDir, hotelId), {
    ...state,
    updatedAt: new Date().toISOString(),
    version: CPE_FULL_E2E_V1,
  });
}

export function dedupeCpeCandidates(candidates = [], priorKeys = []) {
  const seen = new Set(priorKeys.map((k) => text(k)));
  const out = [];
  for (const c of candidates) {
    const key =
      text(c.dedupeKey) ||
      [orgKey(c.organizationName), c.cpeDemandCategory, orgKey(c.activityHeadline)]
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

export function buildCpeOpportunityBrief(opportunity = {}, qualification = {}, hotelMatch = {}) {
  return {
    version: CPE_FULL_E2E_V1,
    researchSegment: CPE_RESEARCH_SEGMENT,
    investigationId: opportunity.investigationId || null,
    organizationName: opportunity.organizationName || null,
    activityHeadline: opportunity.activityHeadline || null,
    cpeDemandCategory: opportunity.cpeDemandCategory || null,
    discoveryOrigin: opportunity.discoveryOrigin || null,
    qualificationStatus: qualification.status || null,
    thesisStandard: qualification.demand?.thesisStandard || null,
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
    note: "Shadow brief only. No customer publication authorized in Phase 3B.",
  };
}

export async function runCorporateProjectsExtendedStayFullE2e(opts = {}) {
  assertAdaptiveExecutionDisabled();
  if (isSharedEligibilityEnforcementEnabled()?.enabled === true) {
    throw new Error("Shared eligibility enforcement must remain disabled in Phase 3B");
  }
  const paidGuard = assertPaidContactProvidersDisabled({});
  if (!paidGuard.ok) throw new Error(paidGuard.error || "paid_providers_blocked");

  const limits = { ...CPE_PILOT_LIMITS, ...(opts.limits || {}) };
  const hotelId = text(opts.hotelId);
  if (!hotelId) throw new Error("hotelId required");

  const reportDir =
    opts.reportDir ||
    path.join(ROOT, "reports", "gdi-2-review-loop", "phase3b", "pilots", "cpe", hotelId);
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

  const profile = await resolveHotelGdiProfileForResearch(hotelId, {
    persistSynthesized: false,
    skipHpc: opts.allowLiveHpc !== true,
  });
  let hotel = profile?.hotel || null;
  if (!hotel) {
    hotel =
      readJson(
        path.join(ROOT, "config/group-demand-intelligence/hotels", `${hotelId}.json`),
      ) || { hotelId };
    stopReasons.push("hotel_profile_config_fallback_no_live_hpc");
  }
  hotel.hotelId = hotel.hotelId || hotelId;
  stagesCompleted.push("hotel_profile");

  let checkpoint = loadCpeCheckpoint(reportDir, hotelId) || {
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

  const categories = listCpeDemandCategories();
  const discoveryPlans = categories.map((c) =>
    buildCpeDiscoveryPlan(c.id, { attemptedStrategyIds: checkpoint.attemptedStrategyIds }),
  );

  // Independent discovery (optional) — never invent; never relabel fixtures as independent
  let independentDiscovery = null;
  let candidates = Array.isArray(opts.candidates) ? [...opts.candidates] : [];
  if (opts.runIndependentDiscovery === true) {
    const remaining = Math.max(
      0,
      Number(limits.maxExternalRetrievalsTotal) - Number(ledger.externalRetrievals || 0),
    );
    independentDiscovery = await runIndependentDemandDiscovery({
      hotel,
      segmentConfig: getCpeIndependentDiscoverySegmentConfig(),
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
      stopReasons.push(...independentDiscovery.limitationNotes.map((n) => `limitation:${n.slice(0, 80)}`));
    }
    if (independentDiscovery.stopReasons?.length) {
      stopReasons.push(...independentDiscovery.stopReasons);
    }
  } else {
    stagesCompleted.push("independent_segment_discovery");
    stopReasons.push("independent_discovery_not_requested_this_run");
  }

  candidates = dedupeCpeCandidates(candidates, checkpoint.priorDedupeKeys).filter((c) => !c.duplicate);
  candidates = candidates.slice(0, limits.maxCandidatesPerHotel);

  const results = [];
  for (const raw of candidates) {
    if (results.length >= limits.maxCandidatesPerHotel) {
      stopReasons.push("max_candidates_per_hotel");
      break;
    }
    const category = raw.cpeDemandCategory || raw.demandCategory || null;
    const invId =
      raw.investigationId ||
      investigationId(hotelId, raw.organizationName, category, raw.activityHeadline || raw.sourceUrl);

    if (opts.resume !== false && checkpoint.completedInvestigationIds.includes(invId)) {
      results.push({ investigationId: invId, skipped: true, reason: "already_completed_in_checkpoint" });
      continue;
    }

    stagesCompleted.push("candidate_identity_resolution");
    const discovery = classifyCpeDiscoverySignal({
      headline: raw.activityHeadline,
      description: raw.whatIsHappening || raw.hotelDemandThesis,
      evidenceQuote: raw.evidenceQuote,
      category,
      synthetic: raw.syntheticFixture === true,
    });

    let opportunity = {
      ...raw,
      id: raw.id || invId,
      investigationId: invId,
      hotelId,
      researchSegment: CPE_RESEARCH_SEGMENT,
      organizationName: text(raw.organizationName),
      cpeDemandCategory: category,
      discoveryOrigin:
        raw.discoveryOrigin ||
        (raw.syntheticFixture
          ? DISCOVERY_ORIGIN.SYNTHETIC_FIXTURE
          : raw.cachedEvidence
            ? DISCOVERY_ORIGIN.CACHED_INVENTORY
            : DISCOVERY_ORIGIN.SEEDED_CANDIDATE_LIST),
      contactName: raw.contactName || raw.primaryContact?.name,
      contactRole: raw.contactRole || raw.primaryContact?.role || raw.primaryContact?.title,
      organizationContactEmail:
        raw.organizationContactEmail || raw.primaryContact?.email || raw.contactEmail,
      contactDataOrigin: raw.contactDataOrigin || "PUBLIC_SOURCE",
    };

    const demand = classifyDemandMechanismEvidence(opportunity, raw.extraction || {});
    const demandThesis = classifyCommercialDemandThesis(opportunity, raw.extraction || {});
    stagesCompleted.push("demand_investigation");

    let publicCycle = null;
    if (
      opts.allowPublicResearch === true &&
      discovery.status === "CANDIDATE" &&
      !raw.syntheticFixture &&
      ledger.externalRetrievals < limits.maxExternalRetrievalsTotal
    ) {
      const publicAuth = assertPhase2ePublicProviderAuthorization();
      if (!publicAuth.ok) {
        stopReasons.push("public_provider_authorization_failed");
      } else {
        const remaining = limits.maxExternalRetrievalsTotal - ledger.externalRetrievals;
        const next = selectAdaptiveNextAction(opportunity, {
          enforceBuyerEvidenceGate: true,
          useBuyerDiscoveryStrategies: true,
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
            if (next.strategy?.id) checkpoint.attemptedStrategyIds.push(next.strategy.id);
          } catch (err) {
            stopReasons.push(`public_research_error:${String(err?.message || err).slice(0, 120)}`);
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

    const hotelMatch = assessCpeHotelSpecificMatch(opportunity, hotel, { hotelId });
    stagesCompleted.push("hotel_matching");

    const qualification = evaluateCorporateProjectsExtendedStayQualification(opportunity, hotel, {
      synthetic: raw.syntheticFixture === true,
      extraction: raw.extraction || {},
      segmentFamily: "generic",
    });
    stagesCompleted.push("shared_qualification");
    stagesCompleted.push("research_continuation_or_shadow_sales_ready");

    const brief = buildCpeOpportunityBrief(opportunity, qualification, hotelMatch);
    const goldBrief =
      opts.buildGoldBrief === true
        ? buildCmtCpeGoldStandardBrief({
            opportunity,
            hotel,
            qualification,
            hotelMatch,
            segment: "cpe",
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

    results.push({
      investigationId: invId,
      organizationName: opportunity.organizationName,
      cpeDemandCategory: category,
      discoveryOrigin: opportunity.discoveryOrigin,
      syntheticFixture: raw.syntheticFixture === true,
      cachedEvidence: raw.cachedEvidence === true,
      discovery,
      demand: {
        evidenceClass: demand.evidenceClass,
        thesisStandard: demandThesis.thesisStandard,
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
    });

    checkpoint.completedInvestigationIds.push(invId);
    checkpoint.priorDedupeKeys.push(
      raw.dedupeKey ||
        [orgKey(opportunity.organizationName), category, orgKey(raw.activityHeadline)].join("::"),
    );
    checkpoint.candidatesProcessed += 1;
  }

  stagesCompleted.push("checkpoint_persist_local_only");
  saveCpeCheckpoint(reportDir, hotelId, checkpoint);

  const origins = partitionDiscoveryOrigins(results.filter((r) => !r.skipped));
  const summary = summarizeCpeRunResults(results, ledger, limits, independentDiscovery);

  const output = {
    version: CPE_FULL_E2E_V1,
    parentVersion: CORPORATE_PROJECTS_EXTENDED_STAY_V1,
    researchSegment: CPE_RESEARCH_SEGMENT,
    hotelId,
    hotelDisplayName: hotel.displayName || hotel.name || null,
    stages: CPE_FULL_E2E_STAGES,
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
    stopReasons: [...new Set(stopReasons)].slice(0, 40),
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

export function summarizeCpeRunResults(results = [], ledger = {}, limits = {}, independentDiscovery = null) {
  const count = (bucket, pred) => bucket.filter(pred).length;
  const fresh = results.filter(
    (r) =>
      !r.skipped &&
      !r.syntheticFixture &&
      !r.cachedEvidence &&
      r.discoveryOrigin === DISCOVERY_ORIGIN.INDEPENDENT_PUBLIC,
  );
  const cached = results.filter((r) => !r.skipped && r.cachedEvidence === true);
  const synth = results.filter((r) => !r.skipped && r.syntheticFixture === true);

  const bucketStats = (bucket) => ({
    candidates: bucket.length,
    verifiedActivities: count(
      bucket,
      (r) => r.discovery?.status === "CANDIDATE",
    ),
    hotelDemandHypotheses: count(
      bucket,
      (r) =>
        r.demand?.thesisStandard === "CONFIRMED_DEMAND" ||
        r.demand?.thesisStandard === "RELIABLE_INFERRED_DEMAND",
    ),
    namedBuyers: count(bucket, (r) =>
      (r.buyerGate?.completedStages || []).includes("NAMED_PERSON_IDENTIFIED"),
    ),
    buyerResponsibilities: count(bucket, (r) =>
      (r.buyerGate?.completedStages || []).includes("COMMERCIAL_RELEVANCE_VERIFIED"),
    ),
    contactRoutes: count(bucket, (r) =>
      (r.buyerGate?.completedStages || []).includes("CONTACT_ROUTE_VERIFIED"),
    ),
    hotelMatches: count(bucket, (r) => r.hotelMatch?.commerciallyAccessible),
    shadowSalesReady: count(bucket, (r) => r.qualification?.status === "SHADOW_SALES_READY"),
    researchContinuation: count(
      bucket,
      (r) => r.qualification?.status === "RESEARCH_CONTINUATION",
    ),
    rejected: count(bucket, (r) => r.qualification?.status === "REJECTED_FALSE_POSITIVE"),
  });

  return {
    counters: {
      ...bucketStats(fresh),
      bySource: {
        independentPublic: bucketStats(fresh),
        cachedEvidence: bucketStats(cached),
        syntheticFixture: bucketStats(synth),
      },
      independentDiscoveryEngineFresh: independentDiscovery?.independentFreshCount || 0,
    },
    requestCost: {
      externalRetrievals: ledger.externalRetrievals || 0,
      estimatedUsd: ledger.estimatedUsd || 0,
      ceiling: limits.maxExternalRetrievalsTotal,
    },
    note: "Headline commercial counters are independent-public fresh only. Cached/synthetic never count as fresh discovery.",
  };
}
