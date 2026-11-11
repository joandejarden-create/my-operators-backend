/**
 * Bethesda Marriott Full-Demand Challenge V1 — offline inventory, plan, budget.
 * Target: 10–15 credible contactable opportunities across all 11 canonical segments.
 * No paid spend / Surfe / PDL / Airtable / publish / weekly in this package.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  SUPERVISOR_EXPECTED_SEGMENTS,
  canonicalizeSegmentId,
} from "./research-constants-v1.js";
import {
  RESEARCH_SEGMENT,
  RESEARCH_SEGMENT_LABEL,
} from "../workflow/hotel-segment-research-plan-v1.js";
import {
  generateHotelDemandDiscoveryStrategies,
  loadHotelGdiConfig,
} from "./commercial-demand-discovery-v4.js";
import { evaluateDeepResearchEligibility } from "./deep-research-eligibility-v1.js";
import { preflightLiveDeepResearchPricing } from "./deep-research-gdi-live-certification-v1.js";
import { assertPaidContactProvidersDisabled } from "./commercial-spine-restoration-v41.js";
import { evaluateProspectPublicationEligibility } from "../prospect-publication-eligibility-v1.js";
import {
  runBethesdaDatabaseReconciliationPreflight,
  assertBethesdaReconciliationGate,
  BETHESDA_DB_RECONCILIATION_V1,
} from "./bethesda-full-demand-database-reconciliation-v1.js";
import {
  BETHESDA_WEEKLY_NET_NEW_V1,
  WEEKLY_TARGET,
  runBethesdaWeeklyNetNewOfflinePackage,
  buildBethesdaImmutableBaselineManifest,
} from "./bethesda-weekly-net-new-discovery-v1.js";

export const BETHESDA_FULL_DEMAND_CHALLENGE_V1 = "gdi-bethesda-full-demand-challenge-v1";
export const BETHESDA_FULL_DEMAND_REPORTS_DIR = "reports/gdi-bethesda-full-demand-challenge-v1";
export const BETHESDA_CANONICAL_HOTEL_ID = "recLuxvwwxID7U2B8";
export const BETHESDA_CANONICAL_HOTEL_NAME = "Bethesda Marriott";

/** Proposed live ceilings — founder must GO before spend. Isolated ledger; no prior balances. */
export const BETHESDA_CHALLENGE_BUDGET_USD = Object.freeze({
  discoveryPhaseUsd: 1.5,
  deepResearchPhaseUsd: 7.5,
  aggregateCeilingUsd: 9.0,
  perOpportunityDeepResearchMaxUsd: 0.55,
  surfePdl: "SEPARATE_AUTHORIZATION_REQUIRED",
});

export {
  runBethesdaDatabaseReconciliationPreflight,
  assertBethesdaReconciliationGate,
  BETHESDA_DB_RECONCILIATION_V1,
};

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const HOTEL_DIR = path.join(ROOT, "data/group-demand-intelligence/hotels", BETHESDA_CANONICAL_HOTEL_ID);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function loadJson(p, fallback = null) {
  if (!fs.existsSync(p)) return fallback;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function mapDisplaySegmentToCanonical(opp = {}) {
  const raw = opp.researchSegment || opp.segmentId || opp.segment || "";
  const canon = canonicalizeSegmentId(raw);
  if (canon && SUPERVISOR_EXPECTED_SEGMENTS.includes(canon)) return canon;

  const blob = `${raw} ${opp.opportunityType || ""} ${opp.organizationName || ""} ${opp.hotelOpportunityThesis || ""}`.toLowerCase();
  if (/sport|soccer|tournament|weekend group/.test(blob)) return RESEARCH_SEGMENT.SPORTS;
  if (/wedding|social|private event|woman.?s club|gala/.test(blob)) return RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS;
  if (/dmc|planner|destination management|housing provider|maritz/.test(blob)) return RESEARCH_SEGMENT.PROFESSIONAL_PLANNERS_DMC;
  if (/local meetings|catering|banquet|lmc_/.test(blob)) return RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING;
  if (/scaling|emerging corporate|startup/.test(blob)) return RESEARCH_SEGMENT.SCALING_COMPANIES;
  if (/exhibitor|sponsor team|trade show booth/.test(blob)) return RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS;
  if (/entertainment|production|film|crew/.test(blob)) return RESEARCH_SEGMENT.ENTERTAINMENT_PRODUCTION;
  if (/tour|educational|specialist group|alumni|university|georgetown|maryland alumni/.test(blob)) {
    return RESEARCH_SEGMENT.TOURS_EDUCATIONAL_SPECIALIST;
  }
  if (/medical|nih|nci|scientific|healthcare|advocacy|amwa|cardiology|ophthalmology|dermatology|clinical/.test(blob)) {
    return RESEARCH_SEGMENT.MEDICAL_ACADEMIC;
  }
  if (/corporate|training|contractor|afcea|nice|cyber|marriott hq/.test(blob)) {
    return RESEARCH_SEGMENT.CORPORATE_MEETINGS_TRAINING;
  }
  if (/association|government|nado|asae|realtor|bar association|ntca|ptab/.test(blob)) {
    return RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT;
  }
  return RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT;
}

function hasUsableContactRoute(opp = {}) {
  const email = opp.contactEmail || opp.primaryContactEmail || opp.primaryContact?.email || null;
  const url = opp.contactUrl || opp.organizationContactUrl || opp.publicContactPath || null;
  const quality = String(opp.contactQuality || opp.primaryContact?.contactQuality || "").toUpperCase();
  if (email && !/gmail|yahoo|hotmail|outlook\.com/i.test(email)) return true;
  if (url && !/^https?:\/\/[^/]+\/?$/i.test(url)) return true;
  if (/NAMED_DECISION_MAKER|NAMED_EVENT|HOUSING_SOURCING|GENERAL_ORGANIZATION|GENERIC_INBOX/i.test(quality)
    && quality !== "NO_CONTACT") {
    return Boolean(email || url);
  }
  return false;
}

function scoreCandidateForChallenge(opp = {}, eligibility = null) {
  let score = 0;
  const reasons = [];
  const type = String(opp.opportunityType || "");
  if (/CLOSED_DISQUALIFIED/i.test(type)) {
    return { score: -100, reasons: ["disqualified"], reject: true };
  }
  if (/PRIMARY_PURSUIT/i.test(type)) { score += 8; reasons.push("primary_pursuit"); }
  else if (/OVERFLOW_HOUSING/i.test(type)) { score += 5; reasons.push("overflow_housing"); }
  else if (/FUTURE_CYCLE/i.test(type)) { score += 4; reasons.push("future_cycle"); }
  else if (/VENUE_PARTNERSHIP/i.test(type)) { score += 3; reasons.push("venue_partnership"); }

  const thesis = text(opp.hotelOpportunityThesis);
  if (thesis.length > 80) { score += 3; reasons.push("thesis_present"); }
  if (/nih|walter reed|bethesda|montgomery|rockville|dmv|metro/i.test(thesis + (opp.eventLocationSummary || ""))) {
    score += 3; reasons.push("geo_anchor_fit");
  }
  if (hasUsableContactRoute(opp)) { score += 4; reasons.push("contact_route"); }
  else { reasons.push("contact_gap"); }

  const cq = String(opp.contactQuality || opp.primaryContact?.contactQuality || "");
  if (/NAMED_DECISION_MAKER|HOUSING_SOURCING/i.test(cq)) { score += 3; reasons.push("strong_contact_quality"); }
  if (/NO_CONTACT/i.test(cq)) { score -= 1; }

  if ((opp.sources || []).length >= 2 || (opp.evidence || []).length >= 2) {
    score += 2; reasons.push("multi_source");
  }
  if (eligibility?.eligible) { score += 2; reasons.push("deep_research_eligible"); }
  if (eligibility?.expectedValueOfInvestigation === "MATERIAL") {
    score += 2; reasons.push("material_voi");
  }
  if (/watch|category watch|disqualified/i.test(`${opp.organizationName} ${opp.id}`)) {
    score -= 4; reasons.push("watch_or_disqualified_label");
  }

  return { score, reasons, reject: false };
}

/**
 * Resolve and verify Bethesda Marriott canonical identity.
 */
export function resolveBethesdaCanonicalHotel() {
  const profile = loadJson(path.join(HOTEL_DIR, "profile.json"));
  const config = loadHotelGdiConfig(BETHESDA_CANONICAL_HOTEL_ID);
  if (!profile?.hotelId) throw new Error("bethesda_profile_missing");
  if (profile.hotelId !== BETHESDA_CANONICAL_HOTEL_ID) {
    throw new Error(`bethesda_hotel_id_mismatch:${profile.hotelId}`);
  }
  const name = profile.identity?.hotelName || config?.displayName;
  if (!/Bethesda Marriott/i.test(String(name || ""))) {
    throw new Error(`bethesda_hotel_name_mismatch:${name}`);
  }
  return {
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    profileHotelId: profile.hotelId,
    profileHotelName: name,
    configHotelId: config?.hotelId || null,
    verified: true,
    note: "Canonical GDI hotel record verified — do not swap with other Marriott DMV properties",
  };
}

/**
 * Commercial profile from existing verified hotel data.
 */
export function buildBethesdaCommercialProfile() {
  const identity = resolveBethesdaCanonicalHotel();
  const profile = loadJson(path.join(HOTEL_DIR, "profile.json"));
  const config = loadHotelGdiConfig(BETHESDA_CANONICAL_HOTEL_ID) || {};

  return {
    version: BETHESDA_FULL_DEMAND_CHALLENGE_V1,
    identity,
    guestrooms: profile.guestrooms,
    meetingAndEventFacilities: profile.meetingCapability,
    locationAndAccess: {
      address: profile.identity?.address || null,
      city: profile.identity?.city || "Bethesda",
      state: profile.identity?.state || "Maryland",
      classification: profile.hotelCharacteristics?.classification || null,
      publicTransit: profile.hotelCharacteristics?.publicTransit || null,
      serviceLevel: profile.hotelCharacteristics?.serviceLevel || null,
      foodAndBeverage: profile.hotelCharacteristics?.foodAndBeverage || null,
      restaurant: profile.hotelCharacteristics?.restaurant || null,
    },
    demandGenerators: profile.demandAnchors || [],
    competitivePositioning: {
      brand: profile.identity?.brand || null,
      parent: profile.identity?.parentBrandCompany || null,
      declaredCompetitiveSet: profile.declaredCompetitiveSet || null,
      configCompetitiveContext: config.competitiveContext || null,
    },
    geographicCatchment: profile.demandTerritory || config.demandTerritory || null,
    suitableGroupTypes: profile.commercialPriorityConfiguration?.targetSegments
      || config.commercialPriorities?.targetSegments
      || [],
    commercialLimitations: [
      "Suburban full-service — not downtown DC host for mega-conventions",
      "Peak group block planning assumption ~100–300 rooms (not full 407 inventory)",
      "Overflow / stay-to-play / future-cycle often more realistic than primary host for large downtown medical meetings",
      "Parking / coach access fields UNKNOWN in profile — qualify with sales",
    ],
    relevantCustomerSegments: profile.commercialPriorityConfiguration?.demandAnchorFocus
      || config.commercialPriorities?.demandAnchorFocus
      || [],
    sourceBasis: {
      profilePath: `data/group-demand-intelligence/hotels/${BETHESDA_CANONICAL_HOTEL_ID}/profile.json`,
      configPath: `config/group-demand-intelligence/hotels/${BETHESDA_CANONICAL_HOTEL_ID}.json`,
      meetingSourceUrl: profile.meetingCapability?.sourceUrl || null,
      claimKindsPreserved: true,
    },
  };
}

/**
 * Reconcile existing opportunities into challenge candidate inventory.
 */
export function reconcileBethesdaCandidateInventory() {
  const raw = loadJson(path.join(HOTEL_DIR, "opportunities.json"), []);
  const opps = Array.isArray(raw) ? raw : (raw.opportunities || []);
  const researchCandidates = loadJson(path.join(HOTEL_DIR, "research-candidates.json"), null);
  const canonicalContacts = loadJson(path.join(HOTEL_DIR, "canonical-contacts.json"), null);
  const eventSeries = loadJson(path.join(HOTEL_DIR, "event-series-graph.json"), null);

  const byOrg = new Map();
  const inventory = [];
  const duplicates = [];

  for (const opp of opps) {
    const canonicalSegment = mapDisplaySegmentToCanonical(opp);
    const orgKey = text(opp.organizationName).toLowerCase();
    const eligibility = evaluateDeepResearchEligibility({
      ...opp,
      hotelId: BETHESDA_CANONICAL_HOTEL_ID,
      hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    }, { remainingBudgetUsd: 0.5, providersAvailable: true });
    const scored = scoreCandidateForChallenge(opp, eligibility);
    const pub = evaluateProspectPublicationEligibility(opp);

    const row = {
      opportunityId: opp.id,
      organizationName: opp.organizationName,
      namedProgramOrMechanism: opp.eventName || opp.programName || text(opp.hotelOpportunityThesis).slice(0, 120) || null,
      displaySegment: opp.segment || opp.researchSegment || null,
      canonicalSegment,
      opportunityType: opp.opportunityType || null,
      hotelFitThesis: text(opp.hotelOpportunityThesis).slice(0, 280) || null,
      evidenceStrength: {
        sourcesCount: (opp.sources || []).length,
        evidenceCount: (opp.evidence || []).length,
        officialSource: opp.officialSource || null,
      },
      commercialTiming: opp.eventDateDisplay || opp.eventStartDate || opp.roomDemandStatus || null,
      potentialRevenueType: opp.revenueTypes || opp.relevantRevenue || null,
      sourcingStatus: {
        room: opp.roomDemandStatus || null,
        venue: opp.venueSourcingStatus || null,
      },
      buyerIntelligence: {
        primaryContactName: opp.primaryContactName || opp.primaryContact?.name || null,
        contactRole: opp.contactRole || opp.primaryContactRole || opp.primaryContact?.title || null,
        claimKind: opp.primaryContact?.claimKind || null,
        whoRecordNamed: opp.whoRecord?.named ?? null,
      },
      contactReadiness: {
        hasUsableRoute: hasUsableContactRoute(opp),
        contactEmail: opp.contactEmail || opp.primaryContactEmail || opp.primaryContact?.email || null,
        contactUrl: opp.contactUrl || null,
        contactQuality: opp.contactQuality || opp.primaryContact?.contactQuality || null,
      },
      researchHistory: {
        investigatedUrls: (opp.investigatedUrls || []).length,
        deepResearchJobId: opp.deepResearchJobId || null,
      },
      publicationEligibility: pub?.eligibility || pub?.hotelFacing || null,
      challengeScore: scored.score,
      scoreReasons: scored.reasons,
      rejectFromChallenge: scored.reject,
      deepResearchEligibility: {
        decision: eligibility.decision,
        tier: eligibility.tier,
        eligible: eligibility.eligible,
        expectedValue: eligibility.expectedValueOfInvestigation,
      },
    };

    if (byOrg.has(orgKey)) {
      duplicates.push({
        organizationName: opp.organizationName,
        existingId: byOrg.get(orgKey),
        duplicateId: opp.id,
      });
    } else {
      byOrg.set(orgKey, opp.id);
    }
    inventory.push(row);
  }

  inventory.sort((a, b) => b.challengeScore - a.challengeScore);

  return {
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    totalOpportunities: inventory.length,
    uniqueOrganizations: byOrg.size,
    duplicateOrgPairs: duplicates,
    researchCandidatesFile: researchCandidates
      ? {
        present: true,
        count: Array.isArray(researchCandidates)
          ? researchCandidates.length
          : (researchCandidates.candidates?.length || null),
      }
      : { present: false },
    canonicalContactsPresent: Boolean(canonicalContacts),
    eventSeriesPresent: Boolean(eventSeries),
    withUsableContactRoute: inventory.filter((r) => r.contactReadiness.hasUsableRoute).length,
    disqualifiedOrClosed: inventory.filter((r) => r.rejectFromChallenge).length,
    inventory,
  };
}

/**
 * Eleven-segment demand map + coverage gaps.
 */
export function buildElevenSegmentDemandMap(reconciled = null) {
  const inv = reconciled || reconcileBethesdaCandidateInventory();
  const config = loadHotelGdiConfig(BETHESDA_CANONICAL_HOTEL_ID) || {};
  const profile = loadJson(path.join(HOTEL_DIR, "profile.json"), {});
  const strategies = generateHotelDemandDiscoveryStrategies({
    displayName: BETHESDA_CANONICAL_HOTEL_NAME,
    city: "Bethesda",
    country: "US",
    demandTerritory: profile.demandTerritory || config.demandTerritory,
    commercialPriorities: profile.commercialPriorityConfiguration || config.commercialPriorities,
    capabilityProfile: {
      totalGuestrooms: profile.guestrooms?.totalGuestrooms,
      meetingRooms: profile.meetingCapability?.meetingRooms,
      largestBallroomSqFt: profile.meetingCapability?.ballroomSqFt,
    },
  }, { maxPerSegment: 3 });

  const segmentStates = {};
  const stateDir = path.join(HOTEL_DIR, "segment-research-state");
  for (const seg of SUPERVISOR_EXPECTED_SEGMENTS) {
    const p = path.join(stateDir, `${seg}.json`);
    segmentStates[seg] = fs.existsSync(p)
      ? { present: true, path: `data/group-demand-intelligence/hotels/${BETHESDA_CANONICAL_HOTEL_ID}/segment-research-state/${seg}.json` }
      : { present: false };
  }

  const strategyList = Array.isArray(strategies?.strategies) ? strategies.strategies : [];
  const bySeg = {};
  for (const seg of SUPERVISOR_EXPECTED_SEGMENTS) {
    const rows = inv.inventory.filter((r) => r.canonicalSegment === seg && !r.rejectFromChallenge);
    const discoveryStrategies = strategyList
      .filter((s) => s.segment === seg)
      .map((s) => s.researchQuestion || s.query)
      .filter(Boolean);
    bySeg[seg] = {
      segmentId: seg,
      label: RESEARCH_SEGMENT_LABEL[seg] || seg,
      existingCandidates: rows.length,
      withContactRoute: rows.filter((r) => r.contactReadiness.hasUsableRoute).length,
      topCandidates: rows.slice(0, 5).map((r) => ({
        opportunityId: r.opportunityId,
        organizationName: r.organizationName,
        score: r.challengeScore,
        type: r.opportunityType,
        hasRoute: r.contactReadiness.hasUsableRoute,
      })),
      segmentResearchStateOnDisk: segmentStates[seg],
      productivityHypothesis: hypothesizeSegmentProductivity(seg, rows.length),
      researchQuestions: defaultResearchQuestionsForSegment(seg),
      evidenceGaps: defaultEvidenceGapsForSegment(seg, rows),
      discoveryStrategies: discoveryStrategies.length
        ? discoveryStrategies
        : defaultDiscoveryQueries(seg),
    };
  }

  const covered = SUPERVISOR_EXPECTED_SEGMENTS.filter((s) => bySeg[s].existingCandidates > 0);
  const thin = SUPERVISOR_EXPECTED_SEGMENTS.filter((s) => bySeg[s].existingCandidates === 0);

  return {
    expectedSegmentCount: 11,
    allElevenPresentInArchitecture: SUPERVISOR_EXPECTED_SEGMENTS.length === 11,
    segmentsWithExistingCandidates: covered.length,
    segmentsWithZeroExistingCandidates: thin,
    segments: bySeg,
    note: "Equal opportunity counts per segment are not required — yield follows strongest demand",
  };
}

function hypothesizeSegmentProductivity(seg, n) {
  if (seg === RESEARCH_SEGMENT.MEDICAL_ACADEMIC || seg === RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT) {
    return n >= 3 ? "HIGH — core Bethesda / NIH / association corridor" : "MEDIUM-HIGH — priority discovery";
  }
  if (seg === RESEARCH_SEGMENT.SPORTS || seg === RESEARCH_SEGMENT.CORPORATE_MEETINGS_TRAINING) {
    return n >= 1 ? "MEDIUM — selective DMV fit" : "MEDIUM — needs discovery";
  }
  if (seg === RESEARCH_SEGMENT.PROFESSIONAL_PLANNERS_DMC || seg === RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING) {
    return "MEDIUM — account-development / no-guestroom inclusive";
  }
  if (seg === RESEARCH_SEGMENT.ENTERTAINMENT_PRODUCTION || seg === RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS) {
    return n === 0 ? "LOW-MEDIUM — investigate applicability; may be thin for suburban Bethesda" : "MEDIUM";
  }
  if (seg === RESEARCH_SEGMENT.SCALING_COMPANIES) {
    return "MEDIUM — Montgomery / bio corridor employers";
  }
  if (seg === RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS) {
    return n >= 1 ? "MEDIUM — local social / private events" : "MEDIUM — local discovery";
  }
  if (seg === RESEARCH_SEGMENT.TOURS_EDUCATIONAL_SPECIALIST) {
    return "MEDIUM — university / specialist groups with metro access";
  }
  return "UNKNOWN";
}

function defaultResearchQuestionsForSegment(seg) {
  const hotel = BETHESDA_CANONICAL_HOTEL_NAME;
  return [
    `What organizations generate recurring group demand suitable for ${hotel} in this segment?`,
    "Who influences hotel sourcing / housing / venue selection?",
    "What is the next dated cycle or open sourcing window?",
    "Is Bethesda a primary, overflow, or account-development path — without inventing demand?",
  ];
}

function defaultEvidenceGapsForSegment(seg, rows) {
  const gaps = [];
  if (!rows.length) gaps.push("No existing GDI opportunities mapped — discovery required");
  if (rows.length && rows.every((r) => !r.contactReadiness.hasUsableRoute)) {
    gaps.push("Candidates lack usable contact routes");
  }
  if (rows.filter((r) => r.deepResearchEligibility.eligible).length < Math.min(2, rows.length || 2)) {
    gaps.push("Few deep-research-eligible candidates — may need discovery refresh");
  }
  return gaps;
}

function defaultDiscoveryQueries(seg) {
  const geo = "Bethesda OR Montgomery County OR NIH OR \"Walter Reed\" OR DMV OR \"Washington DC\" metro";
  const map = {
    [RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT]: [
      `${geo} association annual meeting OR advocacy day hotel housing OR room block`,
      `${geo} chapter conference host hotel OR overflow lodging`,
    ],
    [RESEARCH_SEGMENT.SPORTS]: [
      `${geo} youth soccer OR tournament stay-to-play hotel block`,
      `Maryland OR Northern Virginia tournament traveling teams lodging`,
    ],
    [RESEARCH_SEGMENT.CORPORATE_MEETINGS_TRAINING]: [
      `${geo} corporate offsite OR training program hotel meetings`,
      `government contractor Bethesda OR Rockville team meeting hotel`,
    ],
    [RESEARCH_SEGMENT.MEDICAL_ACADEMIC]: [
      `NIH OR NCI OR \"Walter Reed\" symposium OR conference lodging Bethesda`,
      `medical association legislative conference Washington housing overflow`,
    ],
    [RESEARCH_SEGMENT.PROFESSIONAL_PLANNERS_DMC]: [
      `DMV destination management company hotel supplier partnership`,
      `Washington DC housing bureau OR meeting planner hotel shortlist Bethesda`,
    ],
    [RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING]: [
      `Bethesda banquet OR board meeting OR awards dinner venue hotel`,
    ],
    [RESEARCH_SEGMENT.SCALING_COMPANIES]: [
      `Montgomery County biotech OR healthtech offsite hotel`,
    ],
    [RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS]: [
      `Bethesda hotel wedding OR social event OR private dining group`,
    ],
    [RESEARCH_SEGMENT.ENTERTAINMENT_PRODUCTION]: [
      `${geo} production crew housing OR talent hotel block`,
    ],
    [RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS]: [
      `Washington convention exhibitor team hotel overflow Bethesda`,
    ],
    [RESEARCH_SEGMENT.TOURS_EDUCATIONAL_SPECIALIST]: [
      `university alumni weekend OR educational group hotel Bethesda metro`,
    ],
  };
  return map[seg] || [`${geo} ${seg} hotel group demand`];
}

/**
 * Prioritized deep-research queue toward 10–15 final opportunities.
 */
export function buildChallengePrioritization(reconciled = null, segmentMap = null) {
  const inv = reconciled || reconcileBethesdaCandidateInventory();
  const eligible = inv.inventory.filter((r) => !r.rejectFromChallenge && r.challengeScore >= 6);
  const needsDiscoverySegments = (segmentMap || buildElevenSegmentDemandMap(inv))
    .segmentsWithZeroExistingCandidates || [];

  const deepResearchQueue = eligible.slice(0, 25).map((r, i) => ({
    priority: i + 1,
    opportunityId: r.opportunityId,
    organizationName: r.organizationName,
    canonicalSegment: r.canonicalSegment,
    score: r.challengeScore,
    hasContactRoute: r.contactReadiness.hasUsableRoute,
    opportunityType: r.opportunityType,
    eligibility: r.deepResearchEligibility,
    entrypoint: "runDeepResearchOnExistingGdiOpportunity → matureOpportunity",
    maxUsd: BETHESDA_CHALLENGE_BUDGET_USD.perOpportunityDeepResearchMaxUsd,
  }));

  const provisionalShortlistTarget = {
    min: 10,
    max: 15,
    doNotPad: true,
    note: "If fewer than 10 meet commercial standard after live research, report honestly with blockers",
  };

  return {
    recoveredExisting: inv.totalOpportunities,
    uniqueOrgs: inv.uniqueOrganizations,
    screenedIn: eligible.length,
    deepResearchQueueSize: deepResearchQueue.length,
    deepResearchQueue,
    discoveryRequiredForEmptySegments: needsDiscoverySegments,
    acceptanceCriteria: {
      credibleDemand: true,
      bethesdaSpecificFit: true,
      legitimateBuyingPath: "named person preferred; verified functional route acceptable",
      noInventedOverflowOrRfp: true,
      usableContactForHotelFacing: true,
      salespersonKnowsNextAction: true,
    },
    provisionalShortlistTarget,
    rankingDimensions: [
      "Credibility of demand",
      "Bethesda-specific commercial fit",
      "Potential revenue significance",
      "Sourcing accessibility",
      "Timing / pursuit window",
      "Buyer/contact readiness",
      "Evidence quality / independence",
      "Strength of recommended sales action",
    ],
  };
}

/**
 * Contact intelligence plan + gated enrichment posture.
 */
export function buildContactIntelligencePlan(prioritization = null) {
  const p = prioritization || buildChallengePrioritization();
  const gapQueue = p.deepResearchQueue.filter((r) => !r.hasContactRoute).slice(0, 12);
  return {
    publicWhoFirst: true,
    surfePdlDefault: "OFF",
    functionalRouteAcceptable: true,
    namedPersonPreferred: true,
    rejectedContactTypes: [
      "unsupported email",
      "directory publisher address",
      "speculative person",
      "unverified consumer email",
      "fixture identity",
    ],
    candidatesLikelyNeedingEnrichmentAfterPublicWho: gapQueue.map((r) => ({
      opportunityId: r.opportunityId,
      organizationName: r.organizationName,
      segment: r.canonicalSegment,
      status: "AWAITING_PUBLIC_WHO_THEN_SEPARATE_SURFE_GO",
    })),
    note: "Prepare targeted enrichment requests only after public WHO fails on high-score opportunities",
  };
}

/**
 * Live research authorization package (executePaidResearch=false until founder GO).
 */
export function buildBethesdaChallengeLiveAuthorizationPackage({
  pricing = null,
} = {}) {
  const pricingLive = pricing || preflightLiveDeepResearchPricing();
  const quotes = pricingLive.checks?.quotes || {};
  const exa = Number(quotes.exaLiteSearchUsd) || 0.004;
  const fc = Number(quotes.firecrawlScrapeUsd) || 0.005;

  const discoveryExa = 11 * 3; // 3 queries × 11 segments
  const discoveryScrapes = 40;
  const deepOpps = 22;
  const deepExaPerOpp = 3;
  const deepScrapesPerOpp = 6;

  const estimatedDiscoveryUsd = Number(
    ((discoveryExa * exa) + (discoveryScrapes * fc)).toFixed(2),
  );
  const estimatedDeepUsd = Number(
    ((deepOpps * deepExaPerOpp * exa) + (deepOpps * deepScrapesPerOpp * fc)).toFixed(2),
  );

  return {
    version: BETHESDA_FULL_DEMAND_CHALLENGE_V1,
    status: "PREPARED_AWAITING_FOUNDER_GO",
    executePaidResearch: false,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    preparedAt: new Date().toISOString(),
    entrypoint: "runDeepResearchOnExistingGdiOpportunity → matureOpportunity",
    discoveryAlsoUses: "commercial-demand-discovery-v4 strategies + segment supervisors",
    ceilingsUsd: { ...BETHESDA_CHALLENGE_BUDGET_USD },
    estimatedSpendUsd: {
      discovery: Math.min(estimatedDiscoveryUsd, BETHESDA_CHALLENGE_BUDGET_USD.discoveryPhaseUsd),
      deepResearch: Math.min(estimatedDeepUsd, BETHESDA_CHALLENGE_BUDGET_USD.deepResearchPhaseUsd),
      note: "Estimates from unit quotes × planned volume; stop early when commercially complete — do not burn unused ceiling",
    },
    unitCostsUsd: { exaLiteSearch: exa, firecrawlScrape: fc },
    allowedProviders: ["direct_public", "exa", "firecrawl"],
    forbiddenUnlessSeparateGo: ["surfe", "pdl", "parallel", "paid_contact_enrichment"],
    isolatedLedger: `${BETHESDA_FULL_DEMAND_REPORTS_DIR}/live-ledgers/bethesda-full-demand-challenge.json`,
    usesPriorResearchBalances: false,
    priorLedgersMustRemainUnchanged: [
      "reports/gdi-full-commercial-deep-research-validation-v1/live-ledgers/",
      "reports/gdi-commercial-depth-hardening-v1/live-ledgers/",
      "reports/gdi-deep-research-gdi-integration-v1/live-ledger.json",
      "reports/gdi-autonomous-longform-gdi-integration-v1/live-ledgers/",
    ],
    publication: "OFF",
    airtableWrites: false,
    weeklyActivation: false,
    GDI_WEEKLY_DEEP_RESEARCH_ENABLED: "0",
    pricingPreflight: pricingLive,
    successDefinition:
      "≈10–15 NET_NEW credible contactable opportunities per weekly cycle (aspirational) — not a shortlist from the baseline inventory",
    missionCorrection: {
      baselineIsStartingIntelligenceNotDeliverable: true,
      researchMemoryRecoveriesAreNotNetNew: true,
      twoOperatingStreams: ["NET_NEW_DISCOVERY", "EXISTING_OPPORTUNITY_INTELLIGENCE"],
      weeklyTarget: WEEKLY_TARGET,
    },
    aggregateCeilingIsProposedMaximumNotAuthorization: true,
    note: "Founder GO required before any paid Exa/Firecrawl. Surfe/PDL remain separately gated after public WHO. $9 ceiling is proposed max only.",
  };
}

/**
 * E2E readiness + blockers.
 */
export function assessChallengeExecutionReadiness(pack = {}) {
  const paidGuard = assertPaidContactProvidersDisabled({ authorizeSurfe: false, authorizePdl: false });
  const pricing = pack.liveAuthorization?.pricingPreflight || preflightLiveDeepResearchPricing();
  const reconGate = pack.reconciliationGate
    || assertBethesdaReconciliationGate(pack.databaseReconciliation || null, {
      allowPaidResearch: false,
      founderAuthorizedPaid: false,
    });
  const blockers = [];
  if (!pack.identity?.verified) blockers.push("hotel_identity_unverified");
  if ((pack.segmentMap?.segmentsWithExistingCandidates || 0) < 1) blockers.push("no_existing_candidates");
  if (pack.liveAuthorization?.executePaidResearch === true) {
    blockers.push("execute_flag_must_remain_false_until_founder_go");
  }
  if (!pricing.ok) blockers.push("provider_pricing_preflight_failed");
  if (!paidGuard.ok) blockers.push("paid_contact_guard_failed");
  if (!reconGate.ok) blockers.push("DATABASE_RECONCILIATION_GATE_FAILED");
  if (!pack.databaseReconciliation?.gate?.reconciliationPreflightComplete) {
    blockers.push("DATABASE_RECONCILIATION_PREFLIGHT_REQUIRED");
  }

  if (!pack.weeklyNetNew?.baseline?.immutable) {
    blockers.push("IMMUTABLE_BASELINE_MANIFEST_REQUIRED");
  }
  if (!pack.weeklyNetNew?.discoveryPlan?.allElevenSegmentsCovered) {
    blockers.push("ELEVEN_SEGMENT_FRESH_DISCOVERY_PLAN_REQUIRED");
  }
  if (!pack.weeklyNetNew?.dedupeGateCertification?.ok) {
    blockers.push("NET_NEW_DEDUPE_GATE_CERTIFICATION_REQUIRED");
  }

  blockers.push("FOUNDER_GO_REQUIRED_FOR_PAID_EXA_FIRECRAWL");
  blockers.push("SURFE_PDL_SEPARATE_AUTHORIZATION_FOR_NAMED_BUYER_ENRICHMENT");
  blockers.push("NO_AIRTABLE_OR_CUSTOMER_PUBLICATION_IN_THIS_PHASE");
  blockers.push("MANDATORY_DB_RECONCILIATION_BEFORE_PAID_RESEARCH");
  blockers.push("MANDATORY_FRESH_NET_NEW_DISCOVERY_EVERY_CYCLE");
  blockers.push("DO_NOT_SELECT_FINALS_FROM_BASELINE_SHORTLIST_ALONE");

  return {
    readyForOfflinePlanning: blockers.filter((b) => !/FOUNDER_GO|SURFE_PDL|NO_AIRTABLE|MANDATORY_|DO_NOT_SELECT|IMMUTABLE_|ELEVEN_|NET_NEW_DEDUPE/.test(b)).length === 0
      && reconGate.ok === true,
    readyForPaidLiveExecution: false,
    paidGuardOk: paidGuard.ok === true,
    pricingOk: pricing.ok === true,
    reconciliationGateOk: reconGate.ok === true,
    reconciliationGate: reconGate,
    weeklyNetNewReady: pack.weeklyNetNew?.dedupeGateCertification?.ok === true
      && pack.weeklyNetNew?.baseline?.immutable === true,
    blockers,
    requiredAuthorizations: [
      "Immutable Bethesda baseline manifest frozen before live cycle",
      "Mandatory database reconciliation preflight must pass",
      "Fresh discovery across all 11 segments beyond baseline (net-new gate)",
      `Founder GO for proposed aggregate ≤ $${BETHESDA_CHALLENGE_BUDGET_USD.aggregateCeilingUsd} Exa/Firecrawl/direct_public (ceiling ≠ authorization)`,
      "Separate GO for any Surfe/PDL named-buyer enrichment",
      "Explicit GO before Airtable writes or hotel-facing publication",
      "Live workflow must call assertBethesdaReconciliationGate + classifyFindingAgainstBethesdaBaseline before counting net-new",
    ],
  };
}

/**
 * Full offline challenge package (inventory + plan + auth). Stop before paid.
 * Mandatory database reconciliation is a hard preflight — not optional.
 */
export function runBethesdaFullDemandChallengeOfflinePackage() {
  const identity = resolveBethesdaCanonicalHotel();
  const profile = buildBethesdaCommercialProfile();
  const databaseReconciliation = runBethesdaDatabaseReconciliationPreflight();
  const reconciliationGate = assertBethesdaReconciliationGate(databaseReconciliation, {
    allowPaidResearch: false,
    founderAuthorizedPaid: false,
  });
  const reconciled = reconcileBethesdaCandidateInventory();
  const segmentMap = buildElevenSegmentDemandMap(reconciled);
  // Overlay segment yield from unified reconciliation (includes research memory)
  const unifiedBySeg = databaseReconciliation.bySegment || {};
  for (const seg of SUPERVISOR_EXPECTED_SEGMENTS) {
    if (segmentMap.segments?.[seg]) {
      segmentMap.segments[seg].unifiedInventoryCount = unifiedBySeg[seg] || 0;
      segmentMap.segments[seg].researchMemoryFillsGap =
        (segmentMap.segments[seg].existingCandidates || 0) === 0
        && (unifiedBySeg[seg] || 0) > 0;
    }
  }
  segmentMap.unifiedInventorySize = databaseReconciliation.counts.unifiedInventorySize;
  segmentMap.segmentsStillEmptyAfterReconciliation =
    databaseReconciliation.segmentsStillEmptyAfterReconciliation;
  segmentMap.note =
    "Equal opportunity counts per segment are not required — yield follows strongest demand. "
    + "Baseline inventory is starting intelligence; weekly finals require NET_NEW discovery.";

  const weeklyNetNew = runBethesdaWeeklyNetNewOfflinePackage();
  const baselineManifest = weeklyNetNew._baselineManifest
    || buildBethesdaImmutableBaselineManifest({ reconciliation: databaseReconciliation });

  const prioritization = buildChallengePrioritization(reconciled, segmentMap);
  const contactPlan = buildContactIntelligencePlan(prioritization);
  const liveAuthorization = buildBethesdaChallengeLiveAuthorizationPackage();
  liveAuthorization.mandatoryDatabaseReconciliation = {
    version: BETHESDA_DB_RECONCILIATION_V1,
    required: true,
    preflightComplete: databaseReconciliation.gate.reconciliationPreflightComplete === true,
    gateOk: reconciliationGate.ok === true,
    assertBeforePaid: "assertBethesdaReconciliationGate",
  };
  liveAuthorization.mandatoryWeeklyNetNewDiscovery = {
    version: BETHESDA_WEEKLY_NET_NEW_V1,
    required: true,
    immutableBaselineRequired: true,
    classifyEveryFinding: "classifyFindingAgainstBethesdaBaseline",
    freshDiscoveryAllElevenSegments: true,
    doNotSelectFinalsFromBaselineShortlist: true,
    researchMemoryRecoveriesAreNotNetNew: true,
    weeklyTarget: WEEKLY_TARGET,
    discoveryEntrypoint: weeklyNetNew.discoveryPlan.discoveryEntrypoint,
    deepResearchEntrypoint: weeklyNetNew.discoveryPlan.deepResearchEntrypoint,
  };
  liveAuthorization.executePaidResearch = false;
  liveAuthorization.aggregateCeilingIsProposedMaximumNotAuthorization = true;

  const readiness = assessChallengeExecutionReadiness({
    identity,
    segmentMap,
    liveAuthorization,
    databaseReconciliation,
    reconciliationGate,
    weeklyNetNew,
  });

  const funnelBaseline = {
    existingCandidatesRecovered: reconciled.totalOpportunities,
    uniqueOrganizations: baselineManifest.counts.distinctOrganizations,
    distinctBaselineOpportunities: baselineManifest.counts.distinctCanonicalOpportunities,
    sourceRecordsInBaseline: baselineManifest.counts.sourceRecords,
    duplicatesFlagged: reconciled.duplicateOrgPairs.length,
    researchCandidatesFileCount: reconciled.researchCandidatesFile.count,
    unifiedInventorySize: databaseReconciliation.counts.unifiedInventorySize,
    researchMemoryCandidatesRecovered:
      databaseReconciliation.counts.researchMemoryCandidatesRecovered,
    researchMemoryPreviouslyKnownNotNetNew:
      baselineManifest.counts.researchMemoryPreviouslyKnownRecords,
    newOpportunitiesFromMemory_NOT_MARKET_NET_NEW:
      databaseReconciliation.counts.newOpportunities,
    duplicatesLinked: databaseReconciliation.counts.duplicatesLinked,
    existingUpdatedProposed: databaseReconciliation.counts.existingUpdatedProposed,
    previouslyRejectedReassessed: databaseReconciliation.counts.previouslyRejectedReassessed,
    deferred: databaseReconciliation.counts.deferred,
    screenedNonDisqualified: reconciled.inventory.filter((r) => !r.rejectFromChallenge).length,
    deepResearchQueuedExistingRefresh: prioritization.deepResearchQueueSize,
    withUsableContactRoute: databaseReconciliation.counts.withUsableContactRoute,
    segmentsInvestigatedInPlan: 11,
    segmentsWithExistingYield: segmentMap.segmentsWithExistingCandidates,
    segmentsNeedingDiscovery: segmentMap.segmentsWithZeroExistingCandidates,
    segmentsStillEmptyAfterReconciliation:
      databaseReconciliation.segmentsStillEmptyAfterReconciliation,
    weeklyNetNewActionable: null,
    weeklyReactivated: null,
    weeklyEnriched: null,
    finalSelectedNetNew: null,
    note: "finalSelectedNetNew remains null until live discovery — must NOT be filled from baseline shortlist",
  };

  return {
    version: BETHESDA_FULL_DEMAND_CHALLENGE_V1,
    preparedAt: new Date().toISOString(),
    mission: {
      primary: "≈10–15 NET_NEW commercially actionable opportunities per weekly cycle (aspirational)",
      secondary: "Refresh / reactivate / enrich existing baseline opportunities when material changes occur",
      baselineRole: "starting_intelligence_not_deliverable",
      weeklyTarget: WEEKLY_TARGET,
    },
    identity,
    commercialProfile: profile,
    databaseReconciliation: {
      version: databaseReconciliation.version,
      counts: databaseReconciliation.counts,
      bySegment: databaseReconciliation.bySegment,
      segmentsStillEmptyAfterReconciliation:
        databaseReconciliation.segmentsStillEmptyAfterReconciliation,
      safety: databaseReconciliation.safety,
      gate: databaseReconciliation.gate,
      changeLedgerSize: databaseReconciliation.changeLedger.length,
      note: "Reconciliation NEW_OPPORTUNITY for research-memory rows ≠ market NET_NEW_OPPORTUNITY",
    },
    reconciliationGate,
    weeklyNetNewDiscovery: {
      version: weeklyNetNew.version,
      baseline: weeklyNetNew.baseline,
      discoveryPlan: {
        allElevenSegmentsCovered: weeklyNetNew.discoveryPlan.allElevenSegmentsCovered,
        strategyCount: weeklyNetNew.discoveryPlan.strategyCount,
        productiveSegmentsExtraEffort: weeklyNetNew.discoveryPlan.productiveSegmentsExtraEffort,
        mustSearchBeyondBaseline: weeklyNetNew.discoveryPlan.mustSearchBeyondBaseline,
        discoveryEntrypoint: weeklyNetNew.discoveryPlan.discoveryEntrypoint,
        deepResearchEntrypoint: weeklyNetNew.discoveryPlan.deepResearchEntrypoint,
      },
      funnel: weeklyNetNew.funnel,
      weeklyOutputs: weeklyNetNew.weeklyOutputs,
      dedupeGateCertification: weeklyNetNew.dedupeGateCertification,
      goNoGo: weeklyNetNew.goNoGo,
    },
    elevenSegmentDemandMap: segmentMap,
    reconciledCandidateInventory: {
      ...reconciled,
      inventoryPreview: reconciled.inventory.slice(0, 20),
      inventory: reconciled.inventory,
      insufficientAlone: true,
      note: "Baseline inventory is necessary starting intelligence — weekly finals must come from NET_NEW discovery yield",
    },
    discoveryAndDeepResearchPlan: {
      phase0_mandatory: "Freeze immutable baseline + DB reconciliation preflight",
      phase1_mandatory: "Fresh discovery across all 11 segments beyond baseline (V4 strategies)",
      phase2: "Classify every hit: NET_NEW / REACTIVATED / ENRICHED / UNCHANGED / DUPLICATE / EXCLUDED",
      phase3: "Screen large net-new funnel; deep-research eligible via ORS matureOpportunity",
      phase4: "Existing-stream: gap-only refresh / reactivate when material changes",
      phase5: "Buyer/WHO + gated Surfe only for unresolved high-value buyers",
      phase6: "Report A net-new / B reactivated / C enriched / D excluded — do not pad A from baseline",
      changeLedgerRequired: true,
      netNewDedupeGateRequired: true,
      prioritizationExistingRefresh: prioritization,
      plannedDiscoveryVolume: weeklyNetNew.funnel.plannedVolume,
    },
    contactIntelligencePlan: contactPlan,
    liveAuthorizationPackage: liveAuthorization,
    executionReadiness: readiness,
    funnelBaseline,
    goNoGo: {
      decision: "GO_FOR_OFFLINE_BASELINE_NET_NEW_PLAN_ONLY",
      paidLiveResearch: "NO_GO_UNTIL_FOUNDER_AUTHORIZES",
      surfePdl: "NO_GO",
      airtablePublicationWeekly: "NO_GO",
      databaseReconciliation: reconciliationGate.ok ? "PREFLIGHT_PASS" : "PREFLIGHT_FAIL",
      weeklyNetNewDedupeGate: weeklyNetNew.dedupeGateCertification.ok ? "PASS" : "FAIL",
      aggregateCeilingUsd: BETHESDA_CHALLENGE_BUDGET_USD.aggregateCeilingUsd,
      aggregateCeilingIsAuthorization: false,
      note: "Baseline corrected; net-new discovery integrated offline. Stop before paid execution.",
    },
    _baselineManifestForWrite: baselineManifest,
    _weeklyNetNewFullForWrite: weeklyNetNew,
  };
}
