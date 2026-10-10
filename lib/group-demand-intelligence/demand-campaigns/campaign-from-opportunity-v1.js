/**
 * Shared hotel-agnostic Demand Campaign builder from validated Watch/Ready opportunities.
 * Links existing opportunities — does not invent lodging/buyer facts.
 */

import {
  computeDemandGeneratorId,
  computeSeriesId,
} from "../demand-generators/entities.js";
import {
  classifyHotelHostedCampaignAdmission,
  isHotelHostedProduct,
} from "../external-demand-invariant-v1.js";

const NOW = new Date().toISOString().slice(0, 10);

function slug(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}

/**
 * Campaign admission gate — CAMPAIGN_ADMIT | WATCH_ONLY_SIGNAL | SIGNAL_ONLY | REJECT
 */
export function classifyCampaignAdmission(signal = {}, opts = {}) {
  const nowDate = opts.nowDate || NOW;
  const title = String(signal.name || signal.title || "").trim();
  const org = String(signal.organizationName || signal.organizer || "").trim();
  const source = String(signal.officialSource || signal.sourceUrl || "").trim();
  const start = String(signal.eventStartDate || "").slice(0, 10);
  const end = String(signal.eventEndDate || start).slice(0, 10);
  const year = Number(signal.eventYear) || (start ? Number(start.slice(0, 4)) : 0);
  const marketRelevant = signal.marketRelevant !== false;
  const reasons = [];

  if (!title || title.length < 4) {
    return { class: "REJECT", reasons: ["unnamed"] };
  }
  if (!org) reasons.push("weak_org");
  if (!source || !/^https?:\/\//i.test(source)) {
    return { class: "REJECT", reasons: ["no_credible_source"] };
  }
  if (end && end < nowDate) {
    return { class: "REJECT", reasons: ["past_only"] };
  }
  if (!start && !year) {
    return { class: "SIGNAL_ONLY", reasons: ["no_future_cycle"] };
  }
  if (year && year < 2026) {
    return { class: "REJECT", reasons: ["past_year"] };
  }
  if (marketRelevant === false || signal.wrongDestination === true) {
    return { class: "REJECT", reasons: ["wrong_destination"] };
  }
  if (signal.closedPlacement === true || signal.fullyPlaced === true) {
    return { class: "REJECT", reasons: ["closed_or_placed"] };
  }

  // External-demand invariant: hotel-hosted product without independent external
  // named demand entity must not be admitted as an expensive campaign.
  const hostedBlock = classifyHotelHostedCampaignAdmission(signal);
  if (hostedBlock.class === "REJECT") {
    return { class: "REJECT", reasons: hostedBlock.reasons };
  }
  if (isHotelHostedProduct(signal) && signal.decompositionPotential !== true) {
    return {
      class: "SIGNAL_ONLY",
      reasons: ["hotel_hosted_signal_not_campaign", ...reasons],
    };
  }

  // Closed official lodging lists that exclude the target hotel are not
  // high-value campaigns for that hotel (destination match alone is insufficient).
  if (
    signal.targetHotelOnOfficialList === false ||
    signal.sonVidaOnList === false ||
    (String(signal.selectionStatus || "").toUpperCase() === "LIST_PUBLISHED" &&
      signal.hotelFit === "WEAK_FIT" &&
      signal.forceAdmit !== true)
  ) {
    return {
      class: "SIGNAL_ONLY",
      reasons: ["closed_or_absent_target_hotel_path", ...reasons],
    };
  }

  const blob = `${title} ${org} ${signal.fact || ""}`.toLowerCase();
  const decompPotential =
    signal.decompositionPotential === true ||
    /congreso|congress|feria|feira|expo|symposium|simposio|jornadas|xornadas|asamblea|asemblea|forum|foro|convenci|trade show|exhibitor|expositor|delegaci/i.test(
      blob
    );

  // Require plausible downstream path: external entity + travel/buyer/decision potential.
  // Destination/event noise without decomp path must not consume research budget.
  if (!decompPotential && !signal.linkExistingOpportunity) {
    return {
      class: "WATCH_ONLY_SIGNAL",
      reasons: ["low_decomposition_potential", ...reasons],
    };
  }
  if (
    decompPotential &&
    !signal.forceAdmit &&
    signal.plausibleBuyerOrController !== true &&
    signal.plausibleTravelMotion !== true &&
    signal.plausibleFutureDecision !== true &&
    !/exhibitor|sponsor|delegaci|particip|housing|alojamiento|room block|official hotel/i.test(
      `${blob} ${signal.lodgingSignal || ""} ${signal.notes || ""}`
    )
  ) {
    // Soft: still admit congress-style with lodging signal language; otherwise signal-only
    if (!/alojamiento|hotel oficial|housing|room block|hospedaje/i.test(String(signal.lodgingSignal || signal.officialSource || ""))) {
      return {
        class: "SIGNAL_ONLY",
        reasons: ["no_plausible_buyer_travel_or_decision_path", ...reasons],
      };
    }
  }

  if (!org) {
    return { class: "WATCH_ONLY_SIGNAL", reasons: reasons.length ? reasons : ["org_thin"] };
  }

  return {
    class: "CAMPAIGN_ADMIT",
    reasons: ["future_cycle", "credible_source", "market_relevant", "decomp_potential", ...reasons],
  };
}

/**
 * Build a demand campaign record from a validated opportunity (Watch/Ready).
 */
export function buildCampaignFromOpportunity(opp = {}, hotelProfile = {}, opts = {}) {
  const hotelId = hotelProfile.hotelId || opp.hotelId;
  const hotelKey = hotelProfile.hotelKey || "HOTEL";
  const market =
    hotelProfile.demandTerritory?.label ||
    hotelProfile.market ||
    opp.market ||
    hotelProfile.city ||
    "";
  const country =
    hotelProfile.country ||
    hotelProfile.demandTerritory?.country ||
    hotelProfile.capabilityProfile?.country ||
    "";
  const name = opp.displayTitle || opp.title || opp.name;
  const organizationName = opp.organizationName || opp.organizer || name;
  const officialSource =
    opp.officialSource ||
    opp.sourceUrl ||
    opp.primarySourceUrl ||
    opp.discoverySource ||
    "";
  const eventStartDate = opp.eventStartDate || null;
  const eventEndDate = opp.eventEndDate || null;
  const eventYear =
    opp.eventYear ||
    (eventStartDate ? Number(String(eventStartDate).slice(0, 4)) : null);
  const demandGeneratorId = computeDemandGeneratorId({
    organizationName,
    website: officialSource,
  });
  const eventSeriesId = computeSeriesId({
    demandGeneratorId,
    programName: name,
  });
  const prefix =
    String(hotelKey || "gdi")
      .toLowerCase()
      .replace(/[^a-z0-9]/g, "")
      .slice(0, 6) || "gdi";
  const campaignId =
    opts.campaignId ||
    `${prefix}camp_${slug(name)}_${eventYear || "cycle"}`.slice(0, 72);

  const lodgingHint = String(
    `${opp.lodgingEvidence || ""} ${opp.watchCardHotelSelectionStatus || ""} ${officialSource}`
  ).toLowerCase();
  const lodgingState = /alojamiento|hotel oficial|bloque|housing|room block|hospedaje/.test(
    lodgingHint
  )
    ? "WEAK"
    : "UNKNOWN";

  const evidenceSeeds = [
    {
      organizationName,
      role: "ORGANIZER",
      participantType: "ORGANIZER",
      travelingGroup: "Organizing / technical secretariat / operations team",
      buyerEntity: `${organizationName} — secretaría técnica / eventos`,
      buyerRole: "Technical Secretariat / Events",
      publicContactPath: officialSource,
      lodgingState,
      lodgingNote:
        lodgingState === "WEAK"
          ? "Public lodging/housing language evidenced on source path — blocks not invented"
          : "Lodging motion unconfirmed on campaign seed — complete via official-list research",
      evidenceUrl: officialSource,
      sourceLanguage: opts.sourceLanguage || detectLangHint(name, officialSource),
      linkedOpportunityId: opp.id || null,
    },
  ];

  const admission = classifyCampaignAdmission(
    {
      name,
      organizationName,
      officialSource,
      eventStartDate,
      eventEndDate,
      eventYear,
      marketRelevant: true,
      linkExistingOpportunity: true,
      decompositionPotential: true,
    },
    { nowDate: opts.nowDate || NOW }
  );

  return {
    campaignId,
    hotelId,
    hotelKey,
    kind: "DEMAND_CAMPAIGN",
    name,
    title: name,
    organizationName,
    demandGeneratorId,
    opportunityIds: [opp.id].filter(Boolean),
    eventSeriesId,
    eventCycleId: `cycle:${eventSeriesId}|${eventYear || "future"}`,
    demandEngine: opp.demandEngine || inferEngine(name, organizationName),
    venue: opp.venue || opp.primaryVenue || null,
    market,
    country,
    geography: hotelProfile.city || market,
    organizer: organizationName,
    officialSource,
    eventStartDate,
    eventEndDate,
    eventYear,
    cycleStatus: "CURRENT_FUTURE",
    currentCycle: true,
    historical: false,
    archived: false,
    superseded: false,
    active: true,
    status: "ACTIVE_GENERATOR",
    marketRelevant: true,
    marketFit: "PLAUSIBLE",
    hotelFit: "PLAUSIBLE_TERRITORY_OVERFLOW",
    researchStatus: "OPPORTUNITY_LINKED",
    childDecompositionState: "CHILD_DECOMPOSITION_NOT_YET_RUN",
    nextAction: "Run official-list / participant / exhibitor decomposition",
    verifiedAt: opts.nowDate || NOW,
    verificationNote: opts.verificationNote || "Linked from validated customer Watch",
    fact: `${name} · ${eventStartDate || eventYear || "future cycle"} · ${organizationName}`,
    inference: "",
    unknown: "child_accounts|buyer_paths|lodging_blocks",
    sourceLanguage: opts.sourceLanguage || detectLangHint(name, officialSource),
    queryLanguage: opts.queryLanguage || null,
    baseOfDemand: opts.baseOfDemand || null,
    decompositionStrategy: "OFFICIAL_LIST_THEN_ORGANIZER_SEED",
    evidenceSeeds,
    admissionClass: admission.class,
    admissionReasons: admission.reasons,
    linkedPursuitId: opp.pursuitId || null,
    createdFrom: "VALIDATED_WATCH_LINK",
  };
}

function detectLangHint(name, url) {
  const blob = `${name} ${url}`.toLowerCase();
  if (/aloxamento|xornadas|encontro|asemblea|feira|galicia|coru/.test(blob)) return "gl";
  if (
    /congreso|feria|jornadas|alojamiento|filosof|santo domingo|biocultura|autoamericas|cielo/.test(
      blob
    )
  ) {
    return "es";
  }
  return "en";
}

function inferEngine(name, org) {
  const blob = `${name} ${org}`.toLowerCase();
  if (/auto|motorshow|expo/.test(blob)) return "ASSOCIATION_NGO";
  if (/filosof|academic|symposium|iaps|universidad|universidade/.test(blob)) {
    return "ASSOCIATION_NGO";
  }
  if (/laboral|cielo|empleo/.test(blob)) return "ASSOCIATION_NGO";
  if (/biocultura|organic|feria/.test(blob)) return "ASSOCIATION_NGO";
  return "ASSOCIATION_NGO";
}

/**
 * Build campaigns for all FUTURE_WATCH / Ready opportunities that pass admission.
 */
export function buildCampaignsFromHotelOpportunities(opps = [], hotelProfile = {}, opts = {}) {
  const nowDate = opts.nowDate || NOW;
  const out = [];
  const admissions = [];
  for (const opp of opps) {
    const facing = String(opp.customerFacingState || "").toUpperCase();
    const isWatch = facing === "FUTURE_WATCH" || /WATCH/i.test(facing);
    const isReady = facing === "READY" || facing === "ACTIONABLE_NOW";
    if (!isWatch && !isReady) {
      admissions.push({
        opportunityId: opp.id,
        title: opp.title,
        class: "REJECT",
        reasons: ["not_customer_watch_or_ready"],
      });
      continue;
    }
    const camp = buildCampaignFromOpportunity(opp, hotelProfile, { nowDate, ...opts });
    admissions.push({
      hotelKey: hotelProfile.hotelKey || "",
      opportunityId: opp.id,
      title: camp.name,
      campaignId: camp.campaignId,
      class: camp.admissionClass,
      reasons: camp.admissionReasons,
      sourceLanguage: camp.sourceLanguage,
    });
    if (camp.admissionClass === "CAMPAIGN_ADMIT") out.push(camp);
  }
  return { campaigns: out, admissions };
}
