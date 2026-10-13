/**
 * P0 — Demand Campaign → Child Decomposition orchestrator.
 * Wires visible campaigns into existing Ten Bases decomposers + admission + packet + gates.
 * Does NOT run broad hotel discovery. Jev advisory-only after deterministic completion.
 */

import crypto from "node:crypto";
import { routeCampaignToBaseOfDemand, GDI_BASE_OF_DEMAND } from "./base-router.js";
import { getCampaignEvidencePack } from "./campaign-evidence-packs.js";
import { upsertDemandCampaigns } from "./store.js";
import {
  decomposePublishedEventDemand,
  decomposeIntlOrgMeeting,
  mineEventParticipants,
  expandMedicalDemandEcosystem,
  decomposeProjectWorkforceDemand,
  decomposeSportsEntertainmentDemand,
} from "../ten-bases-of-demand-v1/decomposers.js";
import { scoreChildAccount } from "../ten-bases-of-demand-v1/child-account-gate.js";
import {
  evaluateCompleteDemandPacket,
  PACKET_QUALITY,
  highPotentialPartial,
  isQualifiedForExpensiveCompletion,
} from "../complete-demand-packet-v8/packet-schema.js";
import { completeDemandPacket, jevAdvisePacket } from "../complete-demand-packet-v8/packet-completion.js";
import { successfulPacketPatternMatch } from "../complete-demand-packet-v8/success-calibration.js";
import { isGdiCustomerOpportunityReady } from "../customer-readiness-gate-v1.js";
import { isValidFutureWatch } from "../future-watch/is-valid-future-watch-v1.js";
import { applyLiveCommercialQuality } from "../live-commercial-quality-v1.js";
import { promoteQualifiedGdiOpportunity } from "../promote-qualified-opportunity.js";
import { invalidateGdiHotelReadCache } from "../read-cache.js";
import { loadOpportunitiesCanonical } from "../opportunity-persistence.js";
import {
  buildDeterministicActiveAdvice,
  JEV_LOOP,
} from "../jev-active-research-v2/jev-active-advisor.js";
import { RESEARCH_PRIORITY } from "../jev-active-research-v2/score.js";
import {
  classifySecondGenContamination,
  YOTEL_SECOND_GEN_ENGINE_ID,
} from "./yotel-second-generation-p0.js";
import { loadHotelDemandConfig } from "../hotel-profile.js";

const B = GDI_BASE_OF_DEMAND;

export const CAMPAIGN_DECOMP_STATUS = Object.freeze({
  NOT_STARTED: "NOT_STARTED",
  RUNNING: "RUNNING",
  COMPLETE: "COMPLETE",
  PARTIAL: "PARTIAL",
  PUBLIC_DATA_CEILING: "PUBLIC_DATA_CEILING",
  ERROR: "ERROR",
});

function slug(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}

function normalizeEntityKey(name = "") {
  return String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

function hotelContext(campaign) {
  const hotelId = String(campaign.hotelId || "");
  const hotelKey = String(campaign.hotelKey || "").toUpperCase();
  // Prefer campaign-stamped geography; fall back to GDI hotel config — never assume YOTEL.
  const cfg = loadHotelDemandConfig(hotelId) || null;
  const territory = cfg?.demandTerritory || {};
  const capability = cfg?.capabilityProfile || {};
  const market =
    campaign.market ||
    territory.label ||
    cfg?.city ||
    capability.classification ||
    "destination market";
  const destinationMarket =
    campaign.geography ||
    cfg?.city ||
    (territory.label ? String(territory.label).split("/")[0].trim() : null) ||
    market;
  const geoTokens = [];
  for (const k of territory.classificationKeywords?.TERRITORY_CORE || []) {
    if (k) geoTokens.push(String(k));
  }
  if (cfg?.city) geoTokens.push(cfg.city);
  if (!geoTokens.length && destinationMarket) geoTokens.push(destinationMarket);
  return {
    hotelId: campaign.hotelId,
    hotelKey: campaign.hotelKey || hotelKey || "HOTEL",
    displayName: cfg?.displayName || campaign.hotelDisplayName || null,
    label: cfg?.displayName || null,
    market,
    destinationMarket,
    geoTokens,
    defaultFitScore: 58,
  };
}

/** Reject decomposer text-mining junk (role-word chains, nextAction fragments). */
const NOISE_ORG_RE =
  /\b(sponsor|exhibitor|speaker|partner|delegation|university|production|vendor|pavilion|ngo|visible|decompose|spectators?|consultanc|ampaign|keep generator|listing)\b/i;

function isNoiseOrganization(name = "", opts = {}) {
  const n = String(name || "").trim();
  // Short brand codes (EY, HP) are valid when second-gen / evidence-pack stamped
  if (n.length < 2) return true;
  if (n.length < 4) {
    if (opts.allowShortNamedBrand === true && /^[A-Z][A-Za-z0-9.&]{1,3}$/.test(n)) return false;
    return true;
  }
  if (
    NOISE_ORG_RE.test(n) &&
    !/\b(Clarion|Palexpo|WHO|SETAC|Geneva Health|Art Gen|Watches|CHI|ECOSOC|OCHA|ITU|Microsoft|Google|Cisco|Lenovo|TikTok|PixVerse|Ministry|University of Geneva|Access Partnership|\bEY\b)\b/i.test(
      n
    )
  ) {
    return true;
  }
  // Role-word salad without a real proper noun company
  const roleHits = (n.match(/\b(sponsor|exhibitor|speaker|partner|delegation|vendor|university|production)\b/gi) || [])
    .length;
  if (roleHits >= 2) return true;
  return false;
}

function campaignAsGenerator(campaign, baseOfDemand) {
  // Fact/verification only — do NOT inject role cue words (causes junk named-entity mining).
  const evidenceBlob = [
    campaign.name,
    campaign.organizationName,
    campaign.organizer,
    campaign.fact,
    campaign.verificationNote,
  ].join(" ");
  return {
    id: campaign.demandGeneratorId || campaign.campaignId,
    title: campaign.name || campaign.title,
    organizationName: campaign.organizationName,
    organization: campaign.organizationName,
    organizer: campaign.organizer,
    officialSource: campaign.officialSource,
    source: campaign.officialSource,
    eventStartDate: campaign.eventStartDate,
    eventEndDate: campaign.eventEndDate,
    eventYear: campaign.eventYear,
    eventSeriesId: campaign.eventSeriesId,
    eventCycleId: campaign.eventCycleId,
    demandEngine: campaign.demandEngine,
    venue: campaign.venue,
    market: campaign.market,
    marketRelevant: true,
    fact: campaign.fact,
    snippet: evidenceBlob,
    summaryWhat: campaign.fact || campaign.verificationNote || "",
    baseOfDemand,
  };
}

function runDecomposer(base, generator, hotel, opts) {
  switch (base) {
    case B.PUBLISHED_EVENT_DECOMPOSITION:
      return decomposePublishedEventDemand(generator, hotel, opts);
    case B.INTERNATIONAL_ORG_RECURRING_GROUPS:
      return decomposeIntlOrgMeeting(generator, hotel, opts);
    case B.PARTICIPANT_EXHIBITOR_SPONSOR_MINING:
      return mineEventParticipants(generator, hotel, opts);
    case B.PHARMA_MEDICAL_ECOSYSTEM:
      return expandMedicalDemandEcosystem(generator, hotel, opts);
    case B.PROJECT_WORKFORCE_DEMAND:
      return decomposeProjectWorkforceDemand(generator, hotel, opts);
    case B.SPORTS_ENTERTAINMENT_PRODUCTION:
      return decomposeSportsEntertainmentDemand(generator, hotel, opts);
    default:
      return { children: [], leads: [], intelligence: [], childCount: 0, leadCount: 0 };
  }
}

function admitSeed(seed, campaign) {
  const named = Boolean(seed.organizationName && seed.organizationName.length >= 4);
  const evidence = Boolean(seed.evidenceUrl || campaign.officialSource);
  const travel = Boolean(seed.travelingGroup || seed.travelingEntityEvidence);
  const future =
    Boolean(campaign.eventStartDate) ||
    Boolean(campaign.eventYear && Number(campaign.eventYear) >= 2026);
  const market = Boolean(campaign.marketRelevant !== false);

  const contam = classifySecondGenContamination(seed);
  if (contam.blocked && seed.forceClass !== "SIGNAL_ONLY") {
    if (contam.failureType === "VENUE_AS_ACCOUNT" || contam.failureType === "GENERIC_ORG_SHELL") {
      return {
        class: "REJECTED",
        reason: contam.failureType,
        contaminationFailureType: contam.failureType,
      };
    }
    if (contam.failureType === "ORGANIZER_AS_ACCOUNT_WITHOUT_HOUSING_ROLE") {
      return {
        class: "SIGNAL_ONLY",
        reason: contam.failureType,
        contaminationFailureType: contam.failureType,
      };
    }
    if (contam.failureType === "EVENT_AS_ACCOUNT") {
      return {
        class: "SIGNAL_ONLY",
        reason: contam.failureType,
        contaminationFailureType: contam.failureType,
      };
    }
  }

  if (seed.forceClass === "SIGNAL_ONLY") {
    return {
      class: "SIGNAL_ONLY",
      reason: seed.contaminationFailureType || "force_signal_local_or_secretariat",
      contaminationFailureType: seed.contaminationFailureType || null,
    };
  }
  if (seed.forceClass === "REJECTED") {
    return {
      class: "REJECTED",
      reason: seed.contaminationFailureType || "force_rejected",
      contaminationFailureType: seed.contaminationFailureType || null,
    };
  }

  // Second-gen: require traveling entity proof for RESEARCH_LEAD
  if (seed.secondGeneration === true && seed.travelingEntityProven !== true) {
    return {
      class: "SIGNAL_ONLY",
      reason: "NO_TRAVELING_ENTITY",
      contaminationFailureType: "NO_TRAVELING_ENTITY",
    };
  }

  if (!(named && evidence && travel && future && market)) {
    return {
      class: "REJECTED",
      reason: "admission_failed",
      named,
      evidence,
      travel,
      future,
      market,
    };
  }
  return {
    class: "RESEARCH_LEAD",
    reason: seed.secondGeneration
      ? "second_gen_named_participation_travel_future_market"
      : "named_participation_future_market",
  };
}

function childOpportunityId(campaign, org, role) {
  return `gdi_opp_${slug(campaign.campaignId)}_${slug(org)}_${slug(role)}`.slice(0, 96);
}

function hotelThesisForCampaign(campaign, seed) {
  const hotel = hotelContext(campaign);
  const role = String(seed.role || "").replace(/_/g, " ");
  const venue = campaign.venue || hotel.destinationMarket || hotel.market || "destination";
  const hotelLabel =
    campaign.hotelDisplayName ||
    hotel.displayName ||
    hotel.label ||
    campaign.hotelKey ||
    "Subject hotel";
  return `${seed.organizationName} is a named ${role} under ${campaign.name} (${venue}). ${hotelLabel} is a plausible lodging option for traveling teams when territory and housing access are evidenced — room counts not invented.`;
}

function whyHotelLineForCampaign(campaign) {
  const hotel = hotelContext(campaign);
  const hotelLabel =
    campaign.hotelDisplayName ||
    hotel.displayName ||
    hotel.label ||
    campaign.hotelKey ||
    "Subject hotel";
  const market = hotel.destinationMarket || hotel.market || "destination corridor";
  return `${hotelLabel} is a plausible ${market} lodging base for traveling sponsor, delegation, and production teams when housing access is evidenced — overflow only; no invented blocks.`;
}

function buildChildOpportunity(campaign, seed, admission, baseOfDemand) {
  const id = childOpportunityId(campaign, seed.organizationName, seed.role);
  const title = `${seed.organizationName} — ${campaign.name} (${String(seed.role).replace(/_/g, " ")})`;
  const thesis = hotelThesisForCampaign(campaign, seed);
  const accountId = `acct_${slug(seed.organizationName)}`;
  return {
    id,
    opportunityId: id,
    hotelId: campaign.hotelId,
    title,
    organizationName: seed.organizationName,
    accountId,
    opportunityType: "FUTURE_CYCLE",
    opportunityQualification: "WATCH",
    priority: "WATCHLIST",
    customerFacingState: "FUTURE_WATCH",
    customerVisible: false,
    segment: seed.participantType || seed.role || "Campaign account",
    demandFamily: "CAMPAIGN_CHILD",
    demandType: seed.participantType || seed.role,
    eventStartDate: campaign.eventStartDate,
    eventEndDate: campaign.eventEndDate,
    eventYear: campaign.eventYear,
    eventSeriesId: campaign.eventSeriesId,
    eventCycleId: campaign.eventCycleId,
    demandGeneratorId: campaign.demandGeneratorId,
    parentGeneratorId: campaign.demandGeneratorId,
    parentEventSeriesId: campaign.eventSeriesId,
    parentEventCycleId: campaign.eventCycleId,
    parentCampaignId: campaign.campaignId,
    baseOfDemand,
    childEntityId: `child_${slug(campaign.campaignId)}_${slug(seed.organizationName)}`,
    childEntityName: seed.organizationName,
    childEntityType: seed.participantType || seed.role,
    participationRole: seed.role,
    participationEvidence: true,
    venue: campaign.venue,
    eventLocationSummary: campaign.geography || campaign.venue,
    officialSource: campaign.officialSource,
    discoverySource: seed.evidenceUrl || campaign.officialSource,
    sourceEvidence: seed.evidenceUrl || campaign.officialSource,
    sources: [
      { url: campaign.officialSource, title: campaign.name },
      ...(seed.evidenceUrl && seed.evidenceUrl !== campaign.officialSource
        ? [{ url: seed.evidenceUrl, title: "Participation evidence" }]
        : []),
    ],
    relationshipToEvent: seed.role,
    buyerEntity: seed.buyerEntity,
    organizer: seed.buyerEntity,
    primaryContactRole: seed.buyerRole,
    publicContactPath: seed.publicContactPath || campaign.officialSource,
    organizationContactUrl: seed.publicContactPath || campaign.officialSource,
    officialContactPath: seed.publicContactPath || campaign.officialSource,
    contactPathClass: seed.contactPathClass || null,
    buyerPathCommercialRelevance: seed.buyerPathCommercialRelevance || null,
    contactResearchAttempted: true,
    contactResearchState: "ATTEMPTED",
    researchMethodsAttempted: [
      "campaign_decomposition_p0",
      "evidence_pack",
      "buyer_role_path",
      ...(seed.secondGeneration ? [YOTEL_SECOND_GEN_ENGINE_ID] : []),
    ],
    whoPathClass: "ORG_PATH",
    preferredWhoRoles: [seed.buyerRole || "Events / Partnerships"],
    travelingEntityType: seed.travelingEntityType || null,
    travelingEntityEvidence: seed.travelingEntityEvidence || seed.travelingGroup || null,
    travelingEntityConfidence: seed.travelingEntityConfidence || null,
    travelingEntityProven: seed.travelingEntityProven === true,
    groupMotionType: seed.travelingEntityType || seed.groupMotionType || null,
    groupMotionEvidence: seed.travelingEntityEvidence || seed.travelingGroup || null,
    groupMotionConfidence: seed.travelingEntityConfidence || null,
    futureDecisionType: campaign.eventStartDate ? "CONFIRMED_EVENT_DATE" : "CYCLE_YEAR",
    futureDecisionEvidence: campaign.eventStartDate || String(campaign.eventYear || ""),
    futureDecisionDateOrWindow: campaign.eventStartDate || String(campaign.eventYear || ""),
    futureDecisionConfidence: campaign.eventStartDate ? "HIGH" : "MEDIUM",
    lodgingEvidence: seed.lodgingNote,
    roomDemandStatus: seed.lodgingState === "UNKNOWN" ? "UNKNOWN" : "ESTIMATED_LIKELY",
    roomDemandConfidence: "UNKNOWN",
    housingStatus: seed.lodgingState,
    hotelMotionClass: seed.hotelMotionClass || null,
    hotelOpportunityThesis: thesis,
    hotelDemandThesis: thesis,
    summaryWhat: `${seed.organizationName} under ${campaign.name} — ${seed.travelingGroup || seed.travelingEntityEvidence || "participation"}.`,
    summaryWhyHotel: whyHotelLineForCampaign(campaign),
    summaryWhyMatters:
      "Account-level child of a demand campaign — not the mega-event itself.",
    whyNow: seed.secondGeneration
      ? `${campaign.name} cycle ${campaign.eventStartDate || campaign.eventYear}: published future/current cycle with named ${seed.role || "participant"} ${seed.organizationName}${seed.travelingEntityProven ? ` (${seed.travelingEntityType || "traveling team"})` : ""} — engage ${seed.buyerRole || "events"} path while lodging controllers are still open.`
      : `${campaign.name} cycle ${campaign.eventStartDate || campaign.eventYear}: published future/current cycle — confirm lodging path and buyer function before outreach.`,
    recommendedAction:
      seed.recommendedAction ||
      (seed.secondGeneration
        ? `Contact ${seed.buyerEntity || seed.organizationName} via ${seed.buyerRole || "events / field ops"} regarding ${campaign.name} team lodging near ${campaign.venue || "Geneva / Palexpo"}.`
        : "Confirm participation / housing path via a public function contact; do not invent room blocks."),
    recommendedNextStep: seed.secondGeneration
      ? "Pursue relevant buyer-function path; do not invent room blocks."
      : "Complete lodging + relevant buyer-function path before customer-ready promotion.",
    fitExplanation: thesis,
    hotelFitScore: 60,
    funnelStage: admission.class,
    childAdmissionClass: admission.class,
    childAdmissionReason: admission.reason,
    contaminationFailureType: admission.contaminationFailureType || seed.contaminationFailureType || null,
    gdiCampaignDecompP0: true,
    secondGeneration: seed.secondGeneration === true,
    secondGenEngineId: seed.secondGenEngineId || null,
    sourceFamily: seed.sourceFamily || null,
    publicDataCeiling: seed.publicDataCeiling === true,
    isTestData: false,
    claimKindNotes: {
      rooms: "NOT_INVENTED",
      lodging: seed.lodgingState,
    },
  };
}

function findExistingChild(existingOpps, campaign, orgName) {
  const key = normalizeEntityKey(orgName);
  const genId = campaign.demandGeneratorId;
  const campId = campaign.campaignId;
  return (existingOpps || []).find((o) => {
    if (o.parentCampaignId === campId || o.demandGeneratorId === genId || o.parentGeneratorId === genId) {
      return normalizeEntityKey(o.organizationName || o.childEntityName) === key;
    }
    // AI for Good continuity: match by org under campaign series
    if (
      campId === "ycamp_ai_for_good_2027" &&
      String(o.id || "").includes("aifg_2027") &&
      normalizeEntityKey(o.organizationName) === key
    ) {
      return true;
    }
    return false;
  });
}

function mapDecomposerChildToSeed(child, campaign) {
  if (!child.namedEntity && !child.organization) return null;
  if (child.generatorOnly && /unnamed/i.test(child.organization || "")) return null;
  const org = child.namedEntity || child.organization;
  if (/unnamed|\(unnamed\)/i.test(org)) return null;
  if (normalizeEntityKey(org) === normalizeEntityKey(campaign.organizationName) && child.role === "ORGANIZING_TEAM") {
    // Prefer evidence-pack organizer rows when present
  }
  return {
    organizationName: org,
    role: child.role || child.participantType || "PARTICIPANT",
    participantType: child.participantType || child.role || "PARTICIPANT",
    travelingGroup: child.hotelMotionHypothesis || `${child.role} traveling group`,
    buyerEntity: child.buyerEntity || `${org} events / partnerships (role path)`,
    buyerRole: child.buyerRoleHint || child.primaryContactRole || "Events / Partnerships",
    publicContactPath: child.publicContactPath || campaign.officialSource,
    lodgingState: child.accommodationSignal ? "WEAK" : "UNKNOWN",
    lodgingNote: child.hotelMotionHypothesis || "No published lodging block",
    evidenceUrl: child.officialSource || child.evidenceSource || campaign.officialSource,
  };
}

/**
 * Decompose one demand campaign into parent-linked children.
 */
export async function runDemandCampaignDecomposition(campaign = {}, opts = {}) {
  const nowDate = opts.nowDate || new Date().toISOString().slice(0, 10);
  const runId = opts.runId || `gdi_camp_decomp_${crypto.randomBytes(3).toString("hex")}`;
  const hotel = hotelContext(campaign);
  const routing = routeCampaignToBaseOfDemand(campaign);
  const existingOpps = opts.existingOpps || [];

  const result = {
    campaignId: campaign.campaignId,
    hotelId: campaign.hotelId,
    demandGeneratorId: campaign.demandGeneratorId,
    eventSeriesId: campaign.eventSeriesId,
    eventCycleId: campaign.eventCycleId,
    baseOfDemand: routing.baseOfDemand,
    secondaryBases: routing.secondaryBases,
    routingReason: routing.reason,
    status: CAMPAIGN_DECOMP_STATUS.RUNNING,
    children: [],
    researchLeads: [],
    reused: [],
    created: [],
    rejected: [],
    signalOnly: [],
    packets: [],
    jevLog: [],
    readiness: [],
    errors: [],
  };

  try {
    upsertDemandCampaigns(campaign.hotelId, [
      {
        campaignId: campaign.campaignId,
        childDecompositionState: CAMPAIGN_DECOMP_STATUS.RUNNING,
        researchStatus: "DECOMPOSITION_RUNNING",
      },
    ]);

    const bases = [routing.baseOfDemand, ...(routing.secondaryBases || [])].filter(
      (b, i, a) => a.indexOf(b) === i
    );
    const generator = campaignAsGenerator(campaign, routing.baseOfDemand);
    const decompChildren = [];
    for (const base of bases) {
      const decomp = runDecomposer(base, generator, hotel, { maxPerGenerator: 8 });
      for (const c of decomp.children || []) {
        decompChildren.push({ ...c, baseOfDemand: base });
      }
    }

    /** @type {Array} */
    let seeds = [
      ...getCampaignEvidencePack(campaign.campaignId, {
        hotelId: campaign.hotelId,
        campaign,
      }),
    ];

    // Continuity: AI for Good — pull existing canonical children as seeds
    // (second-gen pack preferred on dedupe; skip noise / venue shells)
    if (campaign.campaignId === "ycamp_ai_for_good_2027") {
      const existingAifg = (existingOpps || []).filter(
        (o) =>
          String(o.id || "").includes("aifg_2027") ||
          o.parentCampaignId === "ycamp_ai_for_good_2027"
      );
      for (const o of existingAifg) {
        if (isNoiseOrganization(o.organizationName)) continue;
        const contam = classifySecondGenContamination({
          organizationName: o.organizationName,
          role: o.participationRole || o.relationshipToEvent || "PARTNER",
          lodgingState: o.housingStatus,
          lodgingNote: o.lodgingEvidence,
          buyerRole: o.primaryContactRole,
        });
        if (contam.failureType === "VENUE_AS_ACCOUNT" || contam.failureType === "GENERIC_ORG_SHELL") {
          continue;
        }
        seeds.push({
          organizationName: o.organizationName,
          role: o.participationRole || o.relationshipToEvent || "PARTNER",
          participantType: o.childEntityType || o.demandType || "PARTNER",
          travelingGroup: o.travelingEntityEvidence || o.summaryWhat || "Partner / sponsor traveling team",
          travelingEntityType: o.travelingEntityType || null,
          travelingEntityEvidence: o.travelingEntityEvidence || null,
          travelingEntityConfidence: o.travelingEntityConfidence || null,
          travelingEntityProven: o.travelingEntityProven === true,
          buyerEntity: o.buyerEntity || o.primaryContactRole || "Events / Partnerships",
          buyerRole: o.primaryContactRole || "Events",
          publicContactPath: o.officialSource || campaign.officialSource,
          lodgingState: o.housingStatus || "UNKNOWN",
          lodgingNote: o.lodgingEvidence || "From prior canary",
          evidenceUrl: o.discoverySource || o.officialSource || campaign.officialSource,
          contactPathClass: o.contactPathClass || null,
          forceClass: contam.blocked ? "SIGNAL_ONLY" : undefined,
          contaminationFailureType: contam.failureType || null,
          _reuseOpportunityId: o.id,
          _existingOpp: o,
        });
      }
    }

    // Decomposer children: only admit if they match evidence-pack orgs OR
    // are the campaign's own named organizer (strict anti-noise).
    const packKeys = new Set(
      getCampaignEvidencePack(campaign.campaignId, {
        hotelId: campaign.hotelId,
        campaign,
      }).map((s) => normalizeEntityKey(s.organizationName))
    );
    const organizerKey = normalizeEntityKey(campaign.organizationName || campaign.organizer);
    for (const c of decompChildren) {
      const seed = mapDecomposerChildToSeed(c, campaign);
      if (!seed) continue;
      if (isNoiseOrganization(seed.organizationName)) continue;
      const key = normalizeEntityKey(seed.organizationName);
      const allowed =
        packKeys.has(key) ||
        key === organizerKey ||
        key.includes(organizerKey) ||
        organizerKey.includes(key);
      if (!allowed) continue;
      const gate = scoreChildAccount(
        {
          organization: seed.organizationName,
          namedEntity: seed.organizationName,
          role: seed.role,
          participantType: seed.participantType,
          participationEvidence: true,
          evidenceSource: seed.evidenceUrl,
          plausibleTravelingGroup: true,
          marketRelevant: true,
          accommodationSignal: seed.lodgingState !== "UNKNOWN",
          futureTiming: campaign.eventYear || campaign.eventStartDate,
        },
        generator,
        hotel
      );
      if (!gate.admitAsLead && !gate.named) continue;
      seeds.push({ ...seed, _fromDecomposer: true, _gate: gate });
    }

    // Dedupe seeds by normalized org
    const seen = new Set();
    seeds = seeds.filter((s) => {
      const k = normalizeEntityKey(s.organizationName);
      if (!k || seen.has(k)) return false;
      seen.add(k);
      return true;
    });

    for (const seed of seeds) {
      if (
        isNoiseOrganization(seed.organizationName, {
          allowShortNamedBrand: seed.secondGeneration === true,
        }) &&
        !seed._reuseOpportunityId
      ) {
        result.rejected.push({
          organization: seed.organizationName,
          reason: "noise_organization",
        });
        continue;
      }
      const admission = admitSeed(seed, campaign);
      const parentLink = {
        parentGeneratorId: campaign.demandGeneratorId,
        parentEventSeriesId: campaign.eventSeriesId,
        parentEventCycleId: campaign.eventCycleId,
        hotelId: campaign.hotelId,
        baseOfDemand: routing.baseOfDemand,
        childEntityName: seed.organizationName,
        participationRole: seed.role,
        sourceUrl: seed.evidenceUrl || campaign.officialSource,
        discoveryTimestamp: new Date().toISOString(),
      };

      result.children.push({
        ...parentLink,
        childEntityId: `child_${slug(campaign.campaignId)}_${slug(seed.organizationName)}`,
        childEntityType: seed.participantType || seed.role || "PARTICIPANT",
        admissionClass: admission.class,
        lodgingState: seed.lodgingState,
        evidenceUrl: seed.evidenceUrl || campaign.officialSource,
      });

      if (admission.class === "REJECTED" || admission.class === "SIGNAL_ONLY") {
        if (admission.class === "REJECTED") {
          result.rejected.push({ organization: seed.organizationName, reason: admission.reason });
        } else {
          result.signalOnly.push({ organization: seed.organizationName, reason: admission.reason });
        }
        // Demote previously persisted venue/organizer shells so they cannot linger as leads
        const existingContam =
          seed._existingOpp || findExistingChild(existingOpps, campaign, seed.organizationName);
        if (
          existingContam &&
          opts.persist !== false &&
          /VENUE_AS_ACCOUNT|ORGANIZER_AS_ACCOUNT|EVENT_AS_ACCOUNT|GENERIC_ORG_SHELL|NO_TRAVELING_ENTITY/i.test(
            admission.reason || ""
          )
        ) {
          try {
            await promoteQualifiedGdiOpportunity({
              candidate: {
                ...existingContam,
                customerVisible: false,
                priority: "WATCHLIST",
                customerFacingState: "FUTURE_WATCH",
                childAdmissionClass: admission.class,
                childAdmissionReason: admission.reason,
                contaminationFailureType: admission.contaminationFailureType || admission.reason,
                secondGeneration: seed.secondGeneration === true,
                secondGenEngineId: seed.secondGenEngineId || YOTEL_SECOND_GEN_ENGINE_ID,
                qualificationFailureReason: admission.reason,
              },
              existingOpps,
              hotelId: campaign.hotelId,
              runId,
              dryRun: false,
              forceUpdateId: existingContam.id,
              materialUpdateOnly: true,
            });
          } catch (err) {
            result.errors.push({
              organization: seed.organizationName,
              stage: "contamination_demote",
              message: err?.message || String(err),
            });
          }
        }
        continue;
      }

      const existing = seed._existingOpp || findExistingChild(existingOpps, campaign, seed.organizationName);
      let opp = existing
        ? {
            ...existing,
            ...buildChildOpportunity(campaign, seed, admission, routing.baseOfDemand),
            id: existing.id,
            opportunityId: existing.id,
            _airtableRecordId: existing._airtableRecordId,
            reused: true,
          }
        : buildChildOpportunity(campaign, seed, admission, routing.baseOfDemand);

      // Deterministic completion before Jev
      let packetEval = evaluateCompleteDemandPacket(opp, {
        defaultFitScore: hotel.defaultFitScore,
        geoOk: true,
      });
      const successMatch = successfulPacketPatternMatch(packetEval, {
        allowHighPotentialPartial: true,
      });
      if (
        isQualifiedForExpensiveCompletion(packetEval.quality) ||
        highPotentialPartial(packetEval)
      ) {
        try {
          const budget = { queriesLeft: opts.maxCompletionSteps ?? 1, queriesRun: 0 };
          const completed = await completeDemandPacket(opp, hotel, budget, { nowDate });
          if (completed?.record) {
            opp = { ...opp, ...completed.record };
          }
          if (completed?.buyer) {
            opp.buyerEntity = completed.buyer.buyerEntity || opp.buyerEntity;
            opp.publicContactPath =
              completed.buyer.publicContactPath || opp.publicContactPath;
          }
          if (completed?.thesis?.whyRelevant) {
            opp.hotelOpportunityThesis =
              completed.record?.hotelOpportunityThesis || opp.hotelOpportunityThesis;
          }
          packetEval = evaluateCompleteDemandPacket(opp, {
            defaultFitScore: hotel.defaultFitScore,
            geoOk: true,
          });
        } catch (err) {
          result.errors.push({
            organization: seed.organizationName,
            stage: "deterministic_completion",
            message: err?.message || String(err),
          });
        }
      }

      // Conditional Jev (advisory only — after deterministic pass)
      let jevIssued = false;
      let jevResolved = false;
      let jevClassChange = false;
      if (
        opts.enableJev !== false &&
        admission.class === "RESEARCH_LEAD" &&
        packetEval.quality !== PACKET_QUALITY.COMPLETE_STRONG &&
        (packetEval.missingPillars || []).length > 0
      ) {
        const advice =
          jevAdvisePacket(packetEval, {
            admitted: true,
            successMatch: successMatch.match,
            depth: 0,
          }) || {};
        const det = buildDeterministicActiveAdvice({
          organization: seed.organizationName,
          title: opp.title,
          researchPriority: RESEARCH_PRIORITY.P1_MEDIUM,
          currentBlockers: (packetEval.missingPillars || []).map((p) =>
            /LODGING|HOTEL/i.test(p) ? "LODGING" : /BUYER|CONTACT/i.test(p) ? "WHO" : "TIMING"
          ),
          completionPotentialScore: 6,
          language: "en",
        });
        jevIssued = Boolean(advice.issued || det.jevDecision === "RESEARCH_NOW");
        result.jevLog.push({
          organization: seed.organizationName,
          opportunityId: opp.id,
          recommendation: advice.action || det.jevDecision,
          sourceFamily: det.jevSourceFamily,
          question: det.jevResearchQuestion,
          loop: advice.action?.includes("STOP")
            ? JEV_LOOP.STOP_LOW_INFORMATION_GAIN
            : JEV_LOOP.CONTINUE,
          blockerResolved: false,
          classificationChanged: false,
          wroteFacts: false,
          promoted: false,
          costUsd: 0,
        });
        // Jev cannot write facts — no classification change from Jev
        jevResolved = false;
        jevClassChange = false;
      }

      // Re-evaluate packet after completion
      packetEval = evaluateCompleteDemandPacket(opp);
      opp.packetQuality = packetEval.quality;
      opp.packetMissingPillars = packetEval.missingPillars || [];

      const cq = applyLiveCommercialQuality(opp, { nowDate });
      // Diagnostic probe (pre-hold) vs strict persisted gate (post-fields, no override).
      const readyProbe = isGdiCustomerOpportunityReady(
        {
          ...cq,
          customerVisible: true,
          customerActiveEligible: true,
          customerSurfaceDisposition: "KEEP_ACTIVE",
        },
        { nowDate }
      );
      const readyStrict = isGdiCustomerOpportunityReady(cq, { nowDate });
      const watch = isValidFutureWatch(cq, { nowDate });
      const terminal =
        (readyProbe.failed || [])[0] || (packetEval.missingPillars || [])[0] || "";
      result.readiness.push({
        opportunityId: opp.id,
        organization: seed.organizationName,
        packetQuality: packetEval.quality,
        readyOk: readyStrict.ok,
        readyProbeOk: readyProbe.ok,
        readyFailed: readyStrict.failed || [],
        readyProbeFailed: readyProbe.failed || [],
        watchOk: watch.ok,
        watchClass: watch.class,
        terminalBlocker: terminal,
        secondaryBlockers: [
          ...(readyProbe.failed || []).slice(1),
          ...(packetEval.missingPillars || []).slice(1),
        ],
        blockerKind: readyProbe.ok
          ? "NONE"
          : (readyProbe.failed || []).some((f) => /surface|summary|who_research/i.test(f))
            ? "MAPPING_OR_GATE"
            : "EVIDENCE_MISSING",
        jevIssued,
        jevResolved,
        jevClassChange,
      });

      result.packets.push({
        opportunityId: opp.id,
        organization: seed.organizationName,
        quality: packetEval.quality,
        missing: (packetEval.missingPillars || []).join("|"),
      });

      if (opts.persist !== false) {
        const promo = await promoteQualifiedGdiOpportunity({
          candidate: cq,
          existingOpps,
          hotelId: campaign.hotelId,
          runId,
          discoveryRunId: runId,
          method: "campaign_decomposition_p0",
          playbook: routing.baseOfDemand,
          source: seed.evidenceUrl || campaign.officialSource,
          dryRun: false,
          forceUpdateId: existing?.id || null,
          materialUpdateOnly: Boolean(existing),
        });
        if (existing || seed._reuseOpportunityId) {
          result.reused.push({
            opportunityId: promo.opportunity?.id || existing?.id,
            organization: seed.organizationName,
            action: promo.action,
          });
        } else {
          result.created.push({
            opportunityId: promo.opportunity?.id || opp.id,
            organization: seed.organizationName,
            action: promo.action,
            recordId: promo.recordId || "",
          });
        }
        if (promo.opportunity) {
          existingOpps.push(promo.opportunity);
          result.researchLeads.push(promo.opportunity);
        } else {
          result.researchLeads.push(cq);
        }
      } else {
        result.researchLeads.push(cq);
      }
    }

    const leadCount = result.researchLeads.length;
    // Persist strict ready only (no customerVisible force). Probe kept for diagnostics.
    const readyCount = result.readiness.filter((r) => r.readyOk).length;
    const watchCount = result.readiness.filter((r) => r.watchOk).length;
    const completeCount = result.packets.filter((p) =>
      /COMPLETE_/.test(p.quality)
    ).length;

    let status = CAMPAIGN_DECOMP_STATUS.COMPLETE;
    if (leadCount === 0 && result.signalOnly.length > 0) {
      status = CAMPAIGN_DECOMP_STATUS.PUBLIC_DATA_CEILING;
    } else if (leadCount === 0) {
      status = CAMPAIGN_DECOMP_STATUS.PUBLIC_DATA_CEILING;
    } else if (completeCount === 0) {
      status = CAMPAIGN_DECOMP_STATUS.PARTIAL;
    }

    result.status = status;
    result.counts = {
      childrenDiscovered: result.children.length,
      researchLeads: leadCount,
      completePackets: completeCount,
      ready: readyCount,
      watch: watchCount,
      rejected: result.rejected.length,
      unresolved: result.signalOnly.length,
      reused: result.reused.length,
      created: result.created.length,
    };

    if (opts.persist !== false) {
      upsertDemandCampaigns(campaign.hotelId, [
        {
          campaignId: campaign.campaignId,
          childDecompositionState: status,
          researchStatus:
            status === CAMPAIGN_DECOMP_STATUS.PUBLIC_DATA_CEILING
              ? "PUBLIC_DATA_CEILING"
              : "DECOMPOSITION_RUN",
          childEntitiesDiscovered: result.counts.childrenDiscovered,
          researchLeads: result.counts.researchLeads,
          candidateOpportunities: result.counts.researchLeads,
          customerReady: result.counts.ready,
          validFutureWatch: result.counts.watch,
          rejected: result.counts.rejected,
          unresolved: result.counts.unresolved,
          // Preserve linked Watch/Ready opportunityIds; append new research-lead children.
          opportunityIds: [
            ...new Set([
              ...(Array.isArray(campaign.opportunityIds) ? campaign.opportunityIds : []),
              ...result.researchLeads.map((o) => o.id).filter(Boolean),
            ]),
          ],
          latestResearchDate: nowDate,
          nextAction:
            status === CAMPAIGN_DECOMP_STATUS.PUBLIC_DATA_CEILING
              ? "Public participant lists not yet available — monitor official sources"
              : "Continue packet completion on research leads; do not treat as ready until gates pass",
          decompBaseOfDemand: routing.baseOfDemand,
          decompRoutingReason: routing.reason,
          decompRunId: runId,
        },
      ]);
    }

    return result;
  } catch (err) {
    result.status = CAMPAIGN_DECOMP_STATUS.ERROR;
    result.errors.push({ stage: "orchestrator", message: err?.message || String(err) });
    if (opts.persist !== false) {
      upsertDemandCampaigns(campaign.hotelId, [
        {
          campaignId: campaign.campaignId,
          childDecompositionState: CAMPAIGN_DECOMP_STATUS.ERROR,
          researchStatus: "DECOMPOSITION_ERROR",
        },
      ]);
    }
    return result;
  }
}

/**
 * Run decomposition for all visible campaigns of a hotel (controlled list optional).
 */
export async function runHotelDemandCampaignDecompositions(hotelId, opts = {}) {
  const { loadDemandCampaigns, listVisibleDemandCampaigns } = await import("./store.js");
  const nowDate = opts.nowDate || "2026-10-04";
  invalidateGdiHotelReadCache(hotelId);
  const canon = await loadOpportunitiesCanonical(hotelId);
  let existingOpps = [...(canon.opportunities || [])];

  const visible = listVisibleDemandCampaigns(hotelId, { nowDate });
  let campaigns = visible.campaigns || [];
  if (Array.isArray(opts.campaignIds) && opts.campaignIds.length) {
    campaigns = campaigns.filter((c) => opts.campaignIds.includes(c.campaignId));
  }

  const results = [];
  for (const campaign of campaigns) {
    const r = await runDemandCampaignDecomposition(campaign, {
      ...opts,
      nowDate,
      existingOpps,
      persist: opts.persist !== false,
    });
    results.push(r);
    // Keep bag fresh for dedupe
    for (const lead of r.researchLeads) {
      if (!existingOpps.find((o) => o.id === lead.id)) existingOpps.push(lead);
    }
    invalidateGdiHotelReadCache(hotelId);
    const refreshed = await loadOpportunitiesCanonical(hotelId);
    existingOpps = [...(refreshed.opportunities || [])];
  }

  return {
    hotelId,
    campaignCount: campaigns.length,
    results,
    stored: loadDemandCampaigns(hotelId),
  };
}
