/**
 * Bethesda Full-Demand Commercial Recovery V1 — offline audit + corrected funnel.
 * Uses saved live evidence only. No paid API calls. No Airtable / publish / weekly.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolveOrganizationIdentity,
  qualifyCommercialProspectV41,
  assessHotelGeographicFit,
  isOfficialHousingOrHotelListPage,
  GEO_FIT_CLASS,
} from "./commercial-spine-restoration-v41.js";
import { INTERNAL_DISPOSITION } from "./commercial-qualification-v3.js";
import {
  generateHotelDemandDiscoveryStrategies,
  screenDiscoveryHitCommercialRelevance,
  BETHESDA_SEGMENT_DISCOVERY_WEIGHT,
  loadHotelGdiConfig,
} from "./commercial-demand-discovery-v4.js";
import { computeHonestCommercialFunnelMetrics } from "./bethesda-full-demand-live-challenge-v1.js";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
} from "./bethesda-full-demand-database-reconciliation-v1.js";
import { buildBethesdaImmutableBaselineManifest } from "./bethesda-weekly-net-new-discovery-v1.js";

export const BETHESDA_COMMERCIAL_RECOVERY_V1 = "gdi-bethesda-commercial-recovery-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
export const RECOVERY_REPORTS_DIR = "reports/gdi-bethesda-commercial-recovery-v1";
const LIVE_DIR = path.join(ROOT, "reports/gdi-bethesda-full-demand-challenge-v1/live");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`);
}

function writeMd(p, md) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, md.endsWith("\n") ? md : `${md}\n`);
}

function loadLiveTop10() {
  const p = path.join(LIVE_DIR, "09-top-net-new-list.json");
  if (!fs.existsSync(p)) return [];
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  return doc.items || [];
}

function loadLiveSummary() {
  const p = path.join(LIVE_DIR, "SUMMARY.json");
  if (!fs.existsSync(p)) return {};
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

/**
 * Bucket for commercial audit taxonomy (mission §3).
 */
export function classifyCommercialBucket({
  correctedDisposition,
  geoFitClass,
  housingPageOnly,
  baselineRelated,
  wrongMarket,
  noise,
  genuineSalesOpp,
} = {}) {
  if (noise) return "UNSUPPORTED_OR_GENERIC_DISCOVERY_NOISE";
  if (baselineRelated) return "DUPLICATE_OR_PREVIOUSLY_KNOWN";
  if (wrongMarket || geoFitClass === GEO_FIT_CLASS.MISMATCH_EXCLUDE_EVENT) {
    return "WRONG_MARKET_OR_WRONG_HOTEL";
  }
  if (geoFitClass === GEO_FIT_CLASS.NAMED_COMPETITOR_HOST_ELSEWHERE) {
    return "FUTURE_CYCLE_OR_COMPETITOR_COMMITTED";
  }
  if (genuineSalesOpp) return "GENUINE_BETHESDA_SALES_OPPORTUNITY";
  if (correctedDisposition === INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT) {
    return "CREDIBLE_ACCOUNT_DEVELOPMENT_PROSPECT";
  }
  if (correctedDisposition === INTERNAL_DISPOSITION.COMPETITIVE_FUTURE_CYCLE) {
    return "FUTURE_CYCLE_PROSPECT";
  }
  if (correctedDisposition === INTERNAL_DISPOSITION.WATCHLIST_RESEARCH || housingPageOnly) {
    return "USEFUL_MARKET_INTELLIGENCE_NOT_CURRENTLY_PURSUABLE";
  }
  return "UNSUPPORTED_OR_GENERIC_DISCOVERY_NOISE";
}

/**
 * Offline reassess one live candidate with saved URLs/titles only (no new fetches).
 */
export function reassessLiveCandidateOffline(item = {}, { baseline = null } = {}) {
  const url = (item.evidenceUrls || [])[0] || item.contactRoute || "";
  const orgClaimed = text(item.organization);
  const program = text(item.demandOpportunity);
  const identity = resolveOrganizationIdentity({
    claimedOrganizationName: orgClaimed,
    title: program,
    url,
    excerpt: `${program} ${orgClaimed}`,
  });

  const pageTexts = [
    program,
    identity.housingPageEvidenceOnly
      ? "Official conference housing / hotel accommodations page for attendees."
      : "",
  ].filter(Boolean);

  const q41 = qualifyCommercialProspectV41({
    opportunity: {
      organizationName: identity.organizationName || orgClaimed,
      programName: program,
      eventName: program,
      eventYear: item.datesOrWindow,
      officialSource: url,
      sourceUrl: url,
      segment: item.canonicalSegment,
    },
    acceptedClaims: program
      ? [{ statement: program, excerpt: program }]
      : [],
    pageTexts,
    targetHotel: BETHESDA_CANONICAL_HOTEL_NAME,
    usableContactRoute: null, // live run had no verified emails
    pageUrl: url,
  });

  const geo = q41.geoFit || assessHotelGeographicFit({
    targetHotel: BETHESDA_CANONICAL_HOTEL_NAME,
    evidenceBlob: `${program} ${url}`,
  });

  const housingPageOnly = identity.housingPageEvidenceOnly === true
    || isOfficialHousingOrHotelListPage(url, program);

  const orgNorm = text(identity.organizationName || orgClaimed).toLowerCase();
  const progNorm = program.toLowerCase();
  const baselinePool = baseline?.sourceRecords
    || baseline?.distinctOpportunities
    || baseline?.opportunities
    || [];
  const baselineHit = baselinePool.find((b) => {
    const bo = text(b.organizationName || b.organization || b.displayName || "").toLowerCase();
    const bp = text(b.programIdentity || b.programName || b.eventName || b.demandMechanism || "").toLowerCase();
    if (!bo) return false;
    if ((orgNorm.includes("nih") || orgNorm.includes("roseli"))
      && /nih|common fund|hrhr|high-risk/i.test(`${bo} ${bp}`)) {
      if (/hrhr|high-risk|high-reward|common fund|roseli/i.test(`${progNorm} ${bp}`)) return true;
    }
    return (bo === orgNorm || orgNorm.includes(bo) || bo.includes(orgNorm))
      && bp
      && (progNorm.includes(bp.slice(0, 20)) || bp.includes(progNorm.slice(0, 20)));
  });

  const screen = screenDiscoveryHitCommercialRelevance({
    organizationName: identity.organizationName || orgClaimed,
    programName: program,
    sourceUrl: url,
    title: program,
    demandSignalSummary: program,
  }, {
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    displayName: BETHESDA_CANONICAL_HOTEL_NAME,
    city: "Bethesda",
  });

  const originalDisposition = item.qualificationV41Disposition || item.commercialStage;
  const correctedDisposition = q41.internalDisposition;
  const exclusionCorrect = originalDisposition === "EXCLUDE"
    && (
      correctedDisposition === INTERNAL_DISPOSITION.EXCLUDE
      || correctedDisposition === INTERNAL_DISPOSITION.WATCHLIST_RESEARCH
    );

  const wrongMarket = /Philadelphia|Las Vegas|Orlando|Chicago/i.test(program)
    || geo.geoFitClass === GEO_FIT_CLASS.NAMED_COMPETITOR_HOST_ELSEWHERE;
  const noise = !screen.pass
    || /parking|BLANKET PURCHASE|sample\.pdf/i.test(`${program} ${url}`);
  const genuineSalesOpp = q41.commerciallyUsefulInternal === true
    && [
      INTERNAL_DISPOSITION.ACTIVE_OPPORTUNITY,
      INTERNAL_DISPOSITION.PRE_SOURCING_OPPORTUNITY,
    ].includes(correctedDisposition);

  const bucket = classifyCommercialBucket({
    correctedDisposition,
    geoFitClass: geo.geoFitClass,
    housingPageOnly,
    baselineRelated: Boolean(baselineHit),
    wrongMarket,
    noise,
    genuineSalesOpp,
  });

  const whyExclude = q41.rejectReason
    || (housingPageOnly && !q41.commerciallyUsefulInternal
      ? "HOUSING_PAGE_WITHOUT_OWNERSHIP_VALID_CONTACT"
      : null)
    || (correctedDisposition === INTERNAL_DISPOSITION.WATCHLIST_RESEARCH
      ? "WATCHLIST_NEEDS_BUYER_ROUTE_AND_GEO_FIT"
      : null)
    || originalDisposition;

  const rootCauses = [];
  if (housingPageOnly) rootCauses.push("DISCOVERY_HOUSING_PAGE_TARGETING");
  if (!identity.actionableCandidate && !identity.organizationName) rootCauses.push("IDENTITY_UNRESOLVED");
  if (wrongMarket) rootCauses.push("WRONG_GEOGRAPHY_OR_COMPETITOR_HOST");
  if (baselineHit) rootCauses.push("BASELINE_RELATED_NOT_TRUE_NET_NEW");
  if (!(item.namedPeople || []).filter((n) => n && !/naval intelligence/i.test(n)).length) {
    rootCauses.push("MISSING_NAMED_BUYER");
  }
  if (!item.contactRoute || housingPageOnly) rootCauses.push("MISSING_OWNERSHIP_VALID_CONTACT");
  if (noise) rootCauses.push("WEAK_OR_NON_DEMAND_EVIDENCE");
  if (/EXCLUDE/i.test(String(originalDisposition)) && housingPageOnly) {
    rootCauses.push("IDENTITY_GATE_ON_HOUSING_PAGE_TITLE");
  }

  return {
    opportunityId: item.opportunityId,
    organizationOriginal: orgClaimed,
    organizationResolved: identity.organizationName || null,
    programOrDemand: program,
    segment: item.canonicalSegment,
    datesOrWindow: item.datesOrWindow || null,
    discoverySourceUrl: url,
    housingPageEvidenceOnly: housingPageOnly,
    identityActionable: identity.actionableCandidate === true,
    identityRejectReason: identity.rejectReason || null,
    originalDisposition,
    correctedDisposition,
    commerciallyUsefulInternal: q41.commerciallyUsefulInternal === true,
    exclusionWasAppropriatelyStrict: exclusionCorrect || correctedDisposition === INTERNAL_DISPOSITION.WATCHLIST_RESEARCH,
    geoFitClass: geo.geoFitClass || null,
    geoNote: geo.note || null,
    baselineRelated: Boolean(baselineHit),
    baselineMatchHint: baselineHit
      ? (baselineHit.id || baselineHit.organizationName || baselineHit.programName || true)
      : null,
    screenPass: screen.pass,
    screenRejectReasons: screen.rejectReasons || [],
    commercialBucket: bucket,
    hotelFacingEligible: genuineSalesOpp || (
      q41.commerciallyUsefulInternal === true
      && [
        INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT,
        INTERNAL_DISPOSITION.COMPETITIVE_FUTURE_CYCLE,
      ].includes(correctedDisposition)
    ),
    customerActionableNow: false, // offline: no verified buyer route in saved live evidence
    namedBuyerPresent: (item.namedPeople || []).filter((n) => n && !/naval intelligence/i.test(n)).length > 0,
    contactRouteLive: item.contactRoute || null,
    contactWasHousingPageOnly: housingPageOnly,
    whyV41Classified: whyExclude,
    nextResearchQuestion: identity.unresolvedIdentityTask
      || "Identify hotel-sourcing owner, destination fit for Bethesda Marriott catchment, and whether sourcing is open vs committed.",
    furtherResearchCouldChangeDecision: !noise && (
      housingPageOnly
      || correctedDisposition === INTERNAL_DISPOSITION.WATCHLIST_RESEARCH
      || Boolean(baselineHit)
    ),
    rootCauses,
    salesBrief: q41.salesBrief || null,
    recommendedNextAction: noise
      ? "Do not pursue — discovery noise / non-demand artifact."
      : baselineHit
        ? "Treat as EXISTING / related baseline cycle — enrich contacts; do not count as NET_NEW."
        : wrongMarket
          ? "Watch future cycle / overflow only — do not pitch as open Bethesda host opportunity."
          : "Deep-research destination + buyer/housing owner; escalate Surfe/PDL only with separate GO if high value.",
  };
}

export function quantifyRootCauses(audits = []) {
  const counts = {};
  for (const a of audits) {
    for (const c of a.rootCauses || []) {
      counts[c] = (counts[c] || 0) + 1;
    }
  }
  const ranked = Object.entries(counts)
    .map(([cause, n]) => ({ cause, count: n, shareOfCandidates: Number((n / Math.max(1, audits.length)).toFixed(2)) }))
    .sort((a, b) => b.count - a.count);
  // Prefer structural discovery/geo causes over universal buyer gaps when both are high.
  const structural = ranked.find((r) =>
    /DISCOVERY_HOUSING|WRONG_GEOGRAPHY|WEAK_OR_NON_DEMAND|BASELINE_RELATED|IDENTITY_GATE/i.test(r.cause)
    && r.shareOfCandidates >= 0.5);
  return {
    nCandidates: audits.length,
    ranked,
    dominantConstraint: structural?.cause || ranked[0]?.cause || null,
    secondaryConstraint: ranked.find((r) => r.cause !== (structural?.cause || ranked[0]?.cause))?.cause || null,
    hypothesis: "Primary failure is housing-page discovery targeting + missing ownership-valid contacts; V4.1 EXCLUDE was often correct for hotel-facing publication, while live reporting inflated actionable via housing URLs.",
  };
}

export function buildCorrectedFunnelFromAudits(audits = [], liveSummary = {}) {
  const spend = liveSummary?.spendingUsd?.aggregate
    ?? liveSummary?.actualUsd?.aggregate
    ?? 0.868;
  const hotelFacing = audits.filter((a) => a.hotelFacingEligible).length;
  const actionable = audits.filter((a) => a.customerActionableNow).length;
  const netNewGenuine = audits.filter((a) => !a.baselineRelated && !a.commercialBucket.includes("NOISE")
    && a.commercialBucket !== "DUPLICATE_OR_PREVIOUSLY_KNOWN"
    && a.identityActionable).length;
  const buckets = {};
  for (const a of audits) {
    buckets[a.commercialBucket] = (buckets[a.commercialBucket] || 0) + 1;
  }
  return computeHonestCommercialFunnelMetrics({
    grossDiscoveries: liveSummary?.funnel?.gross || audits.length,
    validIdentities: audits.filter((a) => a.identityActionable).length,
    genuineNetNew: netNewGenuine,
    credibleGroupDemand: audits.filter((a) => a.housingPageEvidenceOnly && a.identityActionable
      && a.commercialBucket !== "UNSUPPORTED_OR_GENERIC_DISCOVERY_NOISE").length,
    bethesdaFit: audits.filter((a) => /TARGET_MARKET|EVENT_DESTINATION|PLAUSIBLE/i.test(String(a.geoFitClass || ""))).length,
    deeplyResearched: audits.length,
    sourcingPathway: 0,
    usableContactRoute: audits.filter((a) => a.namedBuyerPresent || (!a.contactWasHousingPageOnly && a.contactRouteLive)).length,
    hotelFacingEligible: hotelFacing,
    customerActionable: actionable,
    housingPageLeads: audits.filter((a) => a.housingPageEvidenceOnly).length,
    exclusionsByReason: Object.fromEntries(
      audits.map((a) => [a.opportunityId, a.correctedDisposition]),
    ),
    aggregateSpendUsd: spend,
    namedBuyerSuccess: audits.filter((a) => a.namedBuyerPresent).length,
    verifiedContactRouteSuccess: 0,
  });
}

export function estimateDiscoveryToActionabilityEconomics({
  observedHotelFacingActionable = 0,
  observedSpendUsd = 0.87,
  targetActionable = 12,
} = {}) {
  const scenarios = [];
  if (observedHotelFacingActionable === 0) {
    scenarios.push({
      name: "CURRENT_CONVERSION_ZERO",
      assumption: "Observed hotel-facing actionable conversion ≈ 0 on housing-page discovery mix",
      impliedSpendToHitTarget: null,
      note: "Cannot extrapolate positive conversion from zero. Fix targeting before scaling spend.",
    });
    scenarios.push({
      name: "AFTER_TARGETING_FIX_CONSERVATIVE",
      assumption: "2% of deep-researched NET_NEW become hotel-facing actionable after Bethesda-fit discovery + buyer work",
      deepResearchedNeeded: Math.ceil(targetActionable / 0.02),
      assumedCostPerDeepUsd: 0.08,
      impliedSpendUsd: Number((Math.ceil(targetActionable / 0.02) * 0.08).toFixed(2)),
      note: "Requires discovery that surfaces open/local demand, not housing indexes for distant committed events.",
    });
    scenarios.push({
      name: "AFTER_TARGETING_FIX_MODERATE",
      assumption: "5% deep→actionable with buyer enrichment on shortlist",
      deepResearchedNeeded: Math.ceil(targetActionable / 0.05),
      assumedCostPerDeepUsd: 0.12,
      impliedSpendUsd: Number((Math.ceil(targetActionable / 0.05) * 0.12).toFixed(2)),
      note: "Still excludes Surfe/PDL; contact success remains the bottleneck.",
    });
  }
  return {
    observedHotelFacingActionable,
    observedSpendUsd,
    targetActionableBand: [10, 15],
    dominantConstraint: "DISCOVERY_TARGETING_THEN_BUYER_CONTACT",
    doNotAssumeBudgetAloneFixesYield: true,
    scenarios,
    proposedNextCeilingUsd: 6.0,
    proposedSplit: {
      discoveryUsd: 1.5,
      deepResearchUsd: 4.0,
      reservedBuyerEnrichmentUsd: 0.5,
      note: "Buyer enrichment reserved but Surfe/PDL remain NO-GO until separate authorization",
    },
  };
}

export function buildRevisedLiveResearchPlan(hotelConfig = null) {
  const cfg = hotelConfig || loadHotelGdiConfig(BETHESDA_CANONICAL_HOTEL_ID) || {
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    displayName: BETHESDA_CANONICAL_HOTEL_NAME,
    city: "Bethesda",
    demandTerritory: { label: "DMV" },
  };
  const strategies = generateHotelDemandDiscoveryStrategies(cfg, { maxPerSegment: 3 });
  return {
    version: BETHESDA_COMMERCIAL_RECOVERY_V1,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    mode: "PREPARED_NOT_EXECUTED",
    paidExecution: "NO-GO until founder authorizes a new ceiling (do not reuse prior $9 balance)",
    providersAllowedIfAuthorized: ["exa", "firecrawl", "direct_public"],
    providersForbidden: ["surfe", "pdl", "parallel"],
    segmentWeights: BETHESDA_SEGMENT_DISCOVERY_WEIGHT,
    strategyPack: {
      strategyCount: strategies.strategyCount,
      allElevenCovered: strategies.allElevenSegmentsCovered,
      weightedForBethesda: strategies.weightedForBethesda,
      sampleBySegment: SUPERVISOR_SEGMENT_SAMPLES(strategies.strategies),
    },
    geographicFilters: [
      "Prefer Bethesda / Montgomery / Rockville / NIH / Walter Reed / North Bethesda",
      "Allow DMV competitive only with overflow or metro-access evidence",
      "Screen out named distant host cities without overflow signal",
    ],
    sourceDiversity: [
      "Association venue RFP / site selection pages",
      "Federal / NIH symposium programme pages (not parking PDFs)",
      "DMC / third-party planner capability pages in DMV",
      "Corporate offsite / training RFPs",
      "Local banquet / chapter catering RFPs",
      "Housing pages only as secondary evidence after org+program identity",
    ],
    funnelTargets: {
      grossHits: 80,
      validIdentities: 40,
      genuineNetNew: 25,
      deepResearchShortlist: 15,
      hotelFacingEligible: "aspirational 10–15 — not a quota",
      customerActionable: "report honestly; zero allowed",
    },
    deepResearchPrioritization: [
      "Bethesda-core geo + dated programme + open sourcing language",
      "NIH / federal health corridor meetings",
      "DMV DMC account development with contact path",
      "Deprioritize national convention housing already committed downtown",
    ],
    buyerResearchRequirements: [
      "WHO organizes programme",
      "WHO controls hotel sourcing / housing",
      "Intermediary planner if present",
      "Functional events/housing inbox on organization domain",
    ],
    contactEnrichmentEscalation: "Prepare Surfe/PDL request packs only; execute only with separate GO",
    proposedSpendingCeilingUsd: 6.0,
    stopRules: [
      "Stop paid discovery at $1.50",
      "Stop deep at $4.00 or when shortlist exhausted",
      "Stop opportunity when commercially unsuitable or geo mismatch confirmed",
      "Iteration cap ≠ research completeness — stop on commercial sufficiency or exhausted strategies",
    ],
    commercialAcceptanceCriteria: [
      "Identity resolved to real organization",
      "Demand mechanism dated and group-relevant",
      "Bethesda geographic/commercial fit evidenced or future-cycle labeled",
      "Ownership-valid contact or verified functional route",
      "V4.1 disposition ≠ EXCLUDE for hotel-facing; publication gate unchanged",
    ],
    distinguishNetNewFromRefresh: true,
  };
}

function SUPERVISOR_SEGMENT_SAMPLES(strategies = []) {
  const out = {};
  for (const s of strategies) {
    if (!out[s.segment]) out[s.segment] = [];
    if (out[s.segment].length < 2) out[s.segment].push(s.researchQuestion);
  }
  return out;
}

export function buildTargetedEnrichmentRequests(audits = []) {
  return audits
    .filter((a) => a.furtherResearchCouldChangeDecision && !String(a.commercialBucket).includes("NOISE"))
    .map((a) => ({
      opportunityId: a.opportunityId,
      organization: a.organizationResolved || a.organizationOriginal,
      program: a.programOrDemand,
      need: "Hotel sourcing / housing / meetings owner with role-relevant contact",
      accept: [
        "Named buyer with hotel/housing/meetings responsibility",
        "Functional events@ or housing@ on organization domain",
        "Verified intermediary planner route",
      ],
      reject: [
        "Consumer email",
        "Directory inbox",
        "Fabricated format",
        "Irrelevant senior executive",
        "Housing page URL as contact",
      ],
      providersIfAuthorized: ["surfe", "pdl"],
      persistSurfeDetails: false,
      executeNow: false,
    }));
}

/**
 * Write full recovery package under reports/gdi-bethesda-commercial-recovery-v1/
 */
export function writeBethesdaCommercialRecoveryPackage({ root = ROOT } = {}) {
  const outDir = path.join(root, RECOVERY_REPORTS_DIR);
  const items = loadLiveTop10();
  const liveSummary = loadLiveSummary();
  let baseline = null;
  try {
    baseline = buildBethesdaImmutableBaselineManifest();
  } catch {
    baseline = { opportunities: [] };
  }

  const audits = items.map((item) => reassessLiveCandidateOffline(item, { baseline }));
  const rootCauses = quantifyRootCauses(audits);
  const funnel = buildCorrectedFunnelFromAudits(audits, liveSummary);
  const economics = estimateDiscoveryToActionabilityEconomics({
    observedHotelFacingActionable: funnel.customerActionableNew,
    observedSpendUsd: funnel.costPerActionableUndefinedBecauseZeroYield
      ? (liveSummary?.spendingUsd?.aggregate || 0.868)
      : (liveSummary?.spendingUsd?.aggregate || 0.868),
  });
  const plan = buildRevisedLiveResearchPlan();
  const enrichment = buildTargetedEnrichmentRequests(audits);

  const hotelFacingCount = audits.filter((a) => a.hotelFacingEligible).length;
  const actionableCount = audits.filter((a) => a.customerActionableNow).length;

  writeJson(path.join(outDir, "01-root-cause-analysis.json"), rootCauses);
  writeJson(path.join(outDir, "02-candidate-commercial-audit.json"), { count: audits.length, audits });
  writeJson(path.join(outDir, "03-discovery-strategy-defects-and-fixes.json"), {
    defects: [
      "Queries optimized for 'hotel housing OR room block' recall",
      "Housing pages treated as opportunities and as contact routes",
      "Equal-ish spend across segments without Bethesda commercial weighting",
      "Weak early geo / distant-host screening",
    ],
    fixesImplemented: [
      "generateHotelDemandDiscoveryStrategies rewritten for demand generators + venue/RFP language",
      "BETHESDA_SEGMENT_DISCOVERY_WEIGHT for uneven budget",
      "screenDiscoveryHitCommercialRelevance before enqueue",
      "Housing page ≠ sales contact in live challenge scoring",
    ],
  });
  writeJson(path.join(outDir, "04-segment-specific-improvements.json"), {
    weights: BETHESDA_SEGMENT_DISCOVERY_WEIGHT,
    prioritySegments: [
      "medical_academic",
      "associations_government",
      "corporate_meetings_training",
      "professional_planners_dmc",
      "local_meetings_catering",
    ],
    note: "All 11 remain covered; budget tilts to Bethesda-strong segments.",
  });
  writeJson(path.join(outDir, "05-v41-qualification-audit.json"), {
    rule: "Housing pages resolve org via host alias → WATCHLIST without ownership-valid contact; never ACTIVE from housing alone; publication gate unchanged",
    candidateDispositions: audits.map((a) => ({
      id: a.opportunityId,
      original: a.originalDisposition,
      corrected: a.correctedDisposition,
      useful: a.commerciallyUsefulInternal,
    })),
  });
  writeJson(path.join(outDir, "06-ors-deep-research-execution-audit.json"), {
    finding: "Live deep research often stopped with thin identity and repeated housing URL evidence; related-lead and buyer WHO execution insufficient for commercial sufficiency",
    stoppingStandardReminder: [
      "Commercially sufficient",
      "Further public research justified",
      "Buyer enrichment required",
      "Source strategy exhausted",
      "Budget exhausted",
      "Commercially unsuitable",
    ],
    iterationCapIsNotCompleteness: true,
  });
  writeJson(path.join(outDir, "07-buyer-and-contact-intelligence-audit.json"), {
    namedBuyersInLiveTop10: audits.filter((a) => a.namedBuyerPresent).length,
    housingPageMisusedAsContact: audits.filter((a) => a.contactWasHousingPageOnly).length,
    rootGap: "Public WHO discovery failed to establish sourcing owners; Surfe/PDL not authorized",
  });
  writeJson(path.join(outDir, "08-targeted-enrichment-requests.json"), {
    executeNow: false,
    requests: enrichment,
  });
  writeJson(path.join(outDir, "09-corrected-net-new-and-actionable-funnel.json"), funnel);
  writeJson(path.join(outDir, "10-revised-hotel-sales-briefs.json"), {
    note: "Only emit briefs where evidence supports; housing-only EXCLUDE/WATCHLIST get research next-steps, not pitch copy",
    briefs: audits
      .filter((a) => a.salesBrief && a.identityActionable)
      .map((a) => ({
        opportunityId: a.opportunityId,
        organization: a.organizationResolved,
        disposition: a.correctedDisposition,
        brief: a.salesBrief,
        hotelFacingEligible: a.hotelFacingEligible,
      })),
  });
  writeJson(path.join(outDir, "11-code-changes-and-offline-regression.json"), {
    modulesTouched: [
      "commercial-spine-restoration-v41.js",
      "commercial-demand-discovery-v4.js",
      "hotel-specific-qualification-v2.js",
      "bethesda-full-demand-live-challenge-v1.js",
      "bethesda-commercial-recovery-v1.js",
    ],
    testScript: "scripts/test-gdi-bethesda-commercial-recovery-v1.mjs",
    paidCalls: 0,
  });
  writeJson(path.join(outDir, "12-discovery-to-actionability-economics.json"), economics);
  writeJson(path.join(outDir, "13-revised-live-research-plan.json"), plan);
  writeJson(path.join(outDir, "14-remaining-blockers.json"), {
    blockers: [
      "Zero hotel-facing actionable yield on corrected live evidence",
      "Buyer/contact identification still unsolved without enrichment authorization",
      "Market may not yield 10–15 weekly without better local demand sources",
      "Prior live housing-page set should not be re-spent",
    ],
  });
  writeJson(path.join(outDir, "15-go-nogo-next-live-challenge.json"), {
    decision: "NO-GO",
    rationale: [
      `Corrected hotel-facing actionable on saved evidence: ${actionableCount}`,
      `Hotel-facing eligible (strict): ${hotelFacingCount}`,
      "Discovery targeting and measurement fixed offline — still need a new authorized live plan, not reuse of prior $9",
      "Do not activate weekly pilot",
    ],
    conditionsForFutureGO: [
      "Founder approves new spend ceiling (proposed $6)",
      "Execute revised discovery strategies (not prior housing-page set)",
      "Separate GO required for Surfe/PDL if shortlist warrants",
      "Success judged by commercial usability, not PASS status",
    ],
  });

  const summary = {
    version: BETHESDA_COMMERCIAL_RECOVERY_V1,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    completedAt: new Date().toISOString(),
    liveCandidatesAudited: audits.length,
    hotelFacingActionableCorrected: actionableCount,
    hotelFacingEligibleCorrected: hotelFacingCount,
    costPerActionableUsd: funnel.costPerActionableNewUsd,
    costPerActionableUndefined: funnel.costPerActionableUndefinedBecauseZeroYield,
    dominantRootCause: rootCauses.dominantConstraint,
    nextLiveDecision: "NO-GO",
    paidSpendThisRecovery: 0,
  };
  writeJson(path.join(outDir, "SUMMARY.json"), summary);

  writeMd(path.join(outDir, "DELIVERABLES.md"), `# Bethesda Commercial Recovery V1

## Verdict

Corrected **hotel-facing actionable yield on the 10 live NET_NEW candidates: ${actionableCount}**.
Cost per actionable opportunity: **${funnel.costPerActionableUndefinedBecauseZeroYield ? "undefined (zero yield)" : funnel.costPerActionableNewUsd}**.

Dominant root cause: **${rootCauses.dominantConstraint}**.

Next live Bethesda challenge: **NO-GO** until a new spend ceiling is authorized against the revised plan (do not reuse prior $9 balance).

## Package

| # | File |
|---|------|
| 1 | 01-root-cause-analysis.json |
| 2 | 02-candidate-commercial-audit.json |
| 3 | 03-discovery-strategy-defects-and-fixes.json |
| 4 | 04-segment-specific-improvements.json |
| 5 | 05-v41-qualification-audit.json |
| 6 | 06-ors-deep-research-execution-audit.json |
| 7 | 07-buyer-and-contact-intelligence-audit.json |
| 8 | 08-targeted-enrichment-requests.json |
| 9 | 09-corrected-net-new-and-actionable-funnel.json |
| 10 | 10-revised-hotel-sales-briefs.json |
| 11 | 11-code-changes-and-offline-regression.json |
| 12 | 12-discovery-to-actionability-economics.json |
| 13 | 13-revised-live-research-plan.json |
| 14 | 14-remaining-blockers.json |
| 15 | 15-go-nogo-next-live-challenge.json |

Paid API calls this recovery: **0**.
`);

  return { outDir, summary, audits, funnel, rootCauses, plan };
}
