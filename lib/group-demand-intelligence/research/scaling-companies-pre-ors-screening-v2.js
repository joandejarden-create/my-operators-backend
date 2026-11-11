/**
 * Scaling Companies Discovery V2 — pre-ORS screening & ranking.
 *
 * Separates entity fitness, expansion evidence, and hotel-demand relevance
 * BEFORE expensive ORS. Does not call paid providers. Does not publish.
 *
 * Dispositions are INTERNAL only (not customer-facing labels).
 */

import {
  classifyScalingCompanyEntityFitness,
  isScalingCompanyOpportunity,
  SCALING_ENTITY_CLASS,
} from "../workflow/scaling-companies-research-v1.js";
import {
  DESTINATION_PROGRAM_ACTIVITY_RE,
  detectScalingHotelDemandMode,
  isDestinationCorporateDemandHotel,
  SCALING_HOTEL_DEMAND_MODE,
} from "./scaling-destination-demand-discovery-v1.js";

export const SCALING_PRE_ORS_SCREENING_V2 = "gdi-scaling-companies-pre-ors-screening-v2";

export const PRE_ORS_DISPOSITION = Object.freeze({
  SCALING_RESEARCH_ELIGIBLE: "SCALING_RESEARCH_ELIGIBLE",
  NEEDS_IDENTITY_RESEARCH: "NEEDS_IDENTITY_RESEARCH",
  NEEDS_EXPANSION_EVIDENCE: "NEEDS_EXPANSION_EVIDENCE",
  BETTER_FIT_OTHER_SEGMENT: "BETTER_FIT_OTHER_SEGMENT",
  DUPLICATE_EXISTING_OPPORTUNITY: "DUPLICATE_EXISTING_OPPORTUNITY",
  OUT_OF_MARKET: "OUT_OF_MARKET",
  EXCLUDE: "EXCLUDE",
});

const EXPANSION_SIGNAL_RE =
  /\b(new office|office opening|opened (?:an? )?office|opens? (?:an? )?office|new offices?|headquarters|hq relocation|relocating|relocation|expands? (?:to|into|its)|expansion|opening a (?:new )?(?:location|facility|plant|warehouse|campus)|new facility|new plant|new warehouse|new campus|market entry|entering .{0,40}market|acquires?|acquisition|integration|regional (?:team|hub|office)|business unit|ampliaci[oó]n|nueva sede|nueva oficina|apertura de oficinas?|expansi[oó]n de operaciones|crecimiento empresarial|inversi[oó]n empresarial|ampliaci[oó]n de instalaciones|nuevas contrataciones|centro de operaciones|expansi[oó]n regional|instalaci[oó]n de nuevos equipos|proyectos corporativos)\b/i;

const WEAK_MENTION_ONLY_RE =
  /\b(crunchbase|pitchbook|startup directory|best of|top \d+|awards? dinner|tourism board|resort package|all[- ]inclusive promo)\b/i;

const HOTEL_DEMAND_MECHANISM_RE =
  /\b(hotel|lodging|rooms?|meeting(?:s)?|offsite|training|onboarding|kickoff|retreat|project team|visiting (?:staff|team|leadership)|extended stay|capacitación|hospedaje|sal[oó]n de eventos|viaje de negocios|comisi[oó]n de servicio|equipo de proyecto|incentive|viaje de incentivo|convenci[oó]n|reconocimiento|todo incluido|all.?inclusive)\b/i;

const DATE_HINT_RE =
  /\b(20\d{2}|january|february|march|april|may|june|july|august|september|october|november|december|enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre|q[1-4]\s*20\d{2})\b/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function lower(v) {
  return text(v).toLowerCase();
}

function orgKey(name) {
  return lower(name)
    .replace(/[^a-z0-9áéíóúñü]+/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function candidateBlob(c = {}) {
  return [
    c.organizationName,
    c.company,
    c.title,
    c.summaryWhat,
    c.whatIsHappening,
    c.localPresence,
    c.localConnection,
    c.datedGrowthEvidence,
    c.activityHeadline,
    c.proposedHotelUse,
    c.excerpt,
    c.note,
    c.url,
    c.officialSource,
    ...(Array.isArray(c.sources) ? c.sources.map((s) => `${s?.title || ""} ${s?.url || ""}`) : []),
  ]
    .map(text)
    .filter(Boolean)
    .join(" ");
}

/**
 * Multilingual expansion-oriented discovery query intents (plan only).
 * Augments — does not replace — buildScalingCompanyResearchQueries.
 */
export function buildScalingDiscoveryV2QueryIntents(hotelConfig = {}) {
  const city = text(
    hotelConfig.city ||
      hotelConfig.demandTerritory?.label ||
      hotelConfig.researchCity ||
      "market",
  );
  const localePrimary = lower(
    hotelConfig.locale?.primary || hotelConfig.capabilityProfile?.localePrimary || "",
  );
  const country = lower(hotelConfig.country || hotelConfig.demandTerritory?.country || "");
  const spanish =
    localePrimary === "es" ||
    country === "mexico" ||
    country === "méxico" ||
    country === "mx";

  const intents = [
    {
      intent: "office_opening_en",
      query: `"${city}" ("new office" OR "opens office" OR "office opening" OR "opened an office" OR "expanding its office") (company OR corporate) -hotel -resort -dmc`,
    },
    {
      intent: "facility_expansion_en",
      query: `"${city}" ("new facility" OR "new plant" OR "distribution center" OR "operations center" OR campus) (opens OR opening OR expands) (company OR manufacturer OR logistics)`,
    },
    {
      intent: "hiring_location_en",
      query: `"${city}" (hiring OR "now hiring" OR "join our team") ("new office" OR "expanding team" OR "regional hub") (company) -tourism -resort`,
    },
  ];

  if (spanish) {
    intents.push(
      {
        intent: "apertura_oficinas_es",
        language: "es",
        query: `"${city}" ("apertura de oficinas" OR "nueva oficina" OR "nueva sede" OR "abre oficinas" OR "inaugura oficinas") (empresa OR corporativo) -hotel -resort -dmc`,
      },
      {
        intent: "expansion_operaciones_es",
        language: "es",
        query: `"${city}" ("expansión de operaciones" OR "ampliación de instalaciones" OR "crecimiento empresarial" OR "inversión empresarial" OR "centro de operaciones" OR "expansión regional") (empresa OR compañía)`,
      },
      {
        intent: "contrataciones_capacitacion_es",
        language: "es",
        query: `"${city}" ("nuevas contrataciones" OR "capacitación empresarial" OR "proyectos corporativos" OR "instalación de nuevos equipos") (empresa OR corporativo) (oficina OR sede OR planta)`,
      },
    );
  }

  return intents;
}

export function assessExpansionEvidence(candidate = {}, hotel = {}) {
  const blob = candidateBlob(candidate);
  const hasExpansion = EXPANSION_SIGNAL_RE.test(blob);
  const hasProgram = DESTINATION_PROGRAM_ACTIVITY_RE.test(blob);
  const destinationMode = isDestinationCorporateDemandHotel(hotel);
  // Destination hotels: documented corporate programs count as activity evidence
  // (company need not be opening a local office).
  const hasSignal = hasExpansion || (destinationMode && hasProgram);
  const dated =
    Boolean(text(candidate.growthEvidenceDate || candidate.evidenceDate)) ||
    DATE_HINT_RE.test(blob) ||
    Boolean(text(candidate.datedGrowthEvidence));
  const weakOnly = WEAK_MENTION_ONLY_RE.test(blob) && !hasSignal;
  return {
    ok: hasSignal && !weakOnly,
    dated: dated === true,
    hasExpansionSignal: hasExpansion,
    hasProgramActivitySignal: hasProgram,
    destinationProgramAccepted: destinationMode && hasProgram,
    weakMentionOnly: weakOnly,
    evidenceExcerpt: text(candidate.datedGrowthEvidence || candidate.summaryWhat || candidate.excerpt).slice(
      0,
      240,
    ),
  };
}

export function assessHotelDemandRelevance(candidate = {}, hotel = {}) {
  const blob = candidateBlob(candidate);
  const explicitUse = text(
    candidate.proposedHotelUse ||
      candidate.hotelUseCase ||
      candidate.plausibleTravelOrMeetingUse ||
      candidate.potentialHotelBusiness,
  );
  const mechanismFromBlob = HOTEL_DEMAND_MECHANISM_RE.test(blob);
  const programMechanism = DESTINATION_PROGRAM_ACTIVITY_RE.test(blob);
  const destinationMode = isDestinationCorporateDemandHotel(hotel);
  const local =
    text(candidate.localConnection || candidate.localPresence) ||
    (hotel?.city && blob.toLowerCase().includes(String(hotel.city).toLowerCase())
      ? `Mention of ${hotel.city}`
      : null);
  // Destination mode: program activity + destination mention is enough mechanism
  // without requiring a local office footprint.
  const ok =
    Boolean(explicitUse) ||
    (mechanismFromBlob && Boolean(local)) ||
    (destinationMode && programMechanism && Boolean(local));
  return {
    ok,
    hasExplicitHotelUse: Boolean(explicitUse),
    mechanismInEvidence: mechanismFromBlob || programMechanism,
    localConnection: local,
    demandMode: detectScalingHotelDemandMode(hotel),
    rationale: explicitUse
      ? explicitUse.slice(0, 200)
      : destinationMode && programMechanism
        ? "Destination corporate program evidence implies guestrooms / hosted meetings at the destination"
        : ok
          ? "Local expansion evidence implies visiting teams / meetings / training lodging demand"
          : "No clear hotel-demand mechanism yet",
  };
}

export function assessMarketRelevance(candidate = {}, hotel = {}) {
  const blob = lower(candidateBlob(candidate));
  const city = lower(hotel?.city || "");
  const territoryTerms = [
    city,
    lower(hotel?.demandTerritory?.label || ""),
    ...((hotel?.demandTerritory?.includes || []).map((s) => lower(String(s).split(/\s+where\b/)[0]))),
    ...((hotel?.commercialPriorities?.demandAnchorFocus || []).map((s) => lower(s))),
  ].filter((t) => t && t.length >= 3);

  const hit = territoryTerms.some((t) => t.length >= 4 && blob.includes(t));
  if (hit) return { ok: true, reason: "territory_term_match" };
  if (!city) return { ok: true, reason: "hotel_city_unspecified_defer" };
  // Destination mode: program may name destination synonyms not yet in includes[]
  if (
    isDestinationCorporateDemandHotel(hotel) &&
    DESTINATION_PROGRAM_ACTIVITY_RE.test(blob) &&
    /canc[uú]n|riviera maya|hotel zone|destination|todo incluido|all.?inclusive/.test(blob)
  ) {
    return { ok: true, reason: "destination_program_geography_match" };
  }
  // Soft fail — may still need identity research before hard OUT_OF_MARKET
  return { ok: false, reason: "no_territory_term_in_candidate_blob" };
}

/**
 * Screen one discovery/matured candidate before ORS.
 */
export function screenScalingCandidatePreOrs(candidate = {}, hotel = {}, opts = {}) {
  const existingKeys = new Set(
    (opts.existingOrgKeys || []).map((k) => orgKey(k)).filter(Boolean),
  );
  const org = text(candidate.organizationName || candidate.company || candidate.companyName);
  const key = orgKey(org);

  if (key && existingKeys.has(key)) {
    return {
      version: SCALING_PRE_ORS_SCREENING_V2,
      disposition: PRE_ORS_DISPOSITION.DUPLICATE_EXISTING_OPPORTUNITY,
      organizationName: org || null,
      orgKey: key,
      entityFitness: null,
      expansion: null,
      hotelDemand: null,
      market: null,
      orsEligible: false,
      rankScore: 0,
      reasons: ["matches_existing_scaling_inventory"],
    };
  }

  const entityFitness = classifyScalingCompanyEntityFitness(candidate);
  if (!entityFitness.ok) {
    // UNKNOWN is research incompleteness — continue identity research; do not
    // auto-disqualify as BETTER_FIT_OTHER_SEGMENT (founder research-first rule).
    if (entityFitness.entityClass === SCALING_ENTITY_CLASS.UNKNOWN) {
      return {
        version: SCALING_PRE_ORS_SCREENING_V2,
        disposition: PRE_ORS_DISPOSITION.NEEDS_IDENTITY_RESEARCH,
        organizationName: org || null,
        orgKey: key || null,
        entityFitness,
        expansion: assessExpansionEvidence(candidate, hotel),
        hotelDemand: assessHotelDemandRelevance(candidate, hotel),
        market: assessMarketRelevance(candidate, hotel),
        orsEligible: false,
        rankScore: 2,
        alternateSegment: null,
        reasons: ["entity_class_unknown_continue_identity_research"],
        note: "Preserve candidate for research continuation — UNKNOWN ≠ wrong segment.",
      };
    }
    return {
      version: SCALING_PRE_ORS_SCREENING_V2,
      disposition: PRE_ORS_DISPOSITION.BETTER_FIT_OTHER_SEGMENT,
      organizationName: org || null,
      orgKey: key || null,
      entityFitness,
      expansion: assessExpansionEvidence(candidate, hotel),
      hotelDemand: assessHotelDemandRelevance(candidate, hotel),
      market: assessMarketRelevance(candidate, hotel),
      orsEligible: false,
      rankScore: 0,
      alternateSegment: entityFitness.alternateSegment,
      reasons: entityFitness.reasons || ["entity_not_scaling_company"],
    };
  }

  if (!org || org.length < 2) {
    return {
      version: SCALING_PRE_ORS_SCREENING_V2,
      disposition: PRE_ORS_DISPOSITION.NEEDS_IDENTITY_RESEARCH,
      organizationName: org || null,
      orgKey: key || null,
      entityFitness,
      expansion: assessExpansionEvidence(candidate, hotel),
      hotelDemand: assessHotelDemandRelevance(candidate, hotel),
      market: assessMarketRelevance(candidate, hotel),
      orsEligible: false,
      rankScore: 1,
      reasons: ["organization_identity_unresolved"],
    };
  }

  const market = assessMarketRelevance(candidate, hotel);
  const expansion = assessExpansionEvidence(candidate, hotel);
  const hotelDemand = assessHotelDemandRelevance(candidate, hotel);

  if (!market.ok && opts.requireMarketMatch === true) {
    return {
      version: SCALING_PRE_ORS_SCREENING_V2,
      disposition: PRE_ORS_DISPOSITION.OUT_OF_MARKET,
      organizationName: org,
      orgKey: key,
      entityFitness,
      expansion,
      hotelDemand,
      market,
      orsEligible: false,
      rankScore: 0,
      reasons: [market.reason],
    };
  }

  if (!expansion.ok) {
    return {
      version: SCALING_PRE_ORS_SCREENING_V2,
      disposition: PRE_ORS_DISPOSITION.NEEDS_EXPANSION_EVIDENCE,
      organizationName: org,
      orgKey: key,
      entityFitness,
      expansion,
      hotelDemand,
      market,
      orsEligible: false,
      rankScore: 2,
      reasons: expansion.weakMentionOnly
        ? ["weak_directory_or_promo_mention_only"]
        : ["no_credible_expansion_signal"],
    };
  }

  if (!hotelDemand.ok) {
    return {
      version: SCALING_PRE_ORS_SCREENING_V2,
      disposition: PRE_ORS_DISPOSITION.NEEDS_EXPANSION_EVIDENCE,
      organizationName: org,
      orgKey: key,
      entityFitness,
      expansion,
      hotelDemand,
      market,
      orsEligible: false,
      rankScore: 3,
      reasons: ["expansion_present_but_hotel_demand_mechanism_unclear"],
      note: "May still become eligible after light identity research — not yet ORS-ready.",
    };
  }

  // Explainable rank: expansion dated + demand explicit + market match + identity
  let rankScore = 50;
  if (expansion.dated) rankScore += 15;
  if (hotelDemand.hasExplicitHotelUse) rankScore += 15;
  if (market.ok) rankScore += 10;
  if (candidate.contactEmail || candidate.primaryContact?.email) rankScore += 5;
  if (candidate.officialSource || candidate.url) rankScore += 5;

  return {
    version: SCALING_PRE_ORS_SCREENING_V2,
    disposition: PRE_ORS_DISPOSITION.SCALING_RESEARCH_ELIGIBLE,
    organizationName: org,
    orgKey: key,
    entityFitness,
    expansion,
    hotelDemand,
    market,
    orsEligible: true,
    rankScore,
    reasons: ["entity_fit", "expansion_evidence", "hotel_demand_relevance"],
  };
}

/**
 * Rank ORS-eligible screens (stable, explainable — not false precision).
 */
export function rankScalingPreOrsCandidates(screens = [], { max = 8 } = {}) {
  const eligible = (screens || []).filter(
    (s) => s?.disposition === PRE_ORS_DISPOSITION.SCALING_RESEARCH_ELIGIBLE,
  );
  const sorted = [...eligible].sort((a, b) => {
    const ds = (b.rankScore || 0) - (a.rankScore || 0);
    if (ds !== 0) return ds;
    return String(a.organizationName || "").localeCompare(String(b.organizationName || ""));
  });
  return {
    version: SCALING_PRE_ORS_SCREENING_V2,
    selected: sorted.slice(0, Math.max(0, max)),
    deferred: sorted.slice(Math.max(0, max)),
    rankingNote:
      "Rank prefers dated expansion + explicit hotel-use hypothesis + territory match. Not a precision score.",
  };
}

/**
 * Batch screen discovery/matured rows for a hotel.
 */
export function screenScalingDiscoveryBatch({
  candidates = [],
  hotel = {},
  existingOpportunities = [],
  maxOrs = 8,
  requireMarketMatch = false,
} = {}) {
  const existingOrgKeys = (existingOpportunities || [])
    .filter((o) => isScalingCompanyOpportunity(o) || o.researchSegment === "scaling_companies")
    .map((o) => o.organizationName || o.company)
    .filter(Boolean);

  const screens = (candidates || []).map((c) =>
    screenScalingCandidatePreOrs(c, hotel, { existingOrgKeys, requireMarketMatch }),
  );

  const byDisposition = {};
  for (const d of Object.values(PRE_ORS_DISPOSITION)) byDisposition[d] = 0;
  for (const s of screens) {
    byDisposition[s.disposition] = (byDisposition[s.disposition] || 0) + 1;
  }

  const ranking = rankScalingPreOrsCandidates(screens, { max: maxOrs });

  return {
    version: SCALING_PRE_ORS_SCREENING_V2,
    hotelId: hotel.hotelId || null,
    inputCount: candidates.length,
    screens,
    byDisposition,
    ranking,
    orsQueue: ranking.selected,
    metrics: {
      raw: candidates.length,
      entityFit: screens.filter((s) => s.entityFitness?.ok === true).length,
      documentedExpansion: screens.filter((s) => s.expansion?.ok === true).length,
      hotelDemandRelevant: screens.filter((s) => s.hotelDemand?.ok === true).length,
      orsEligible: screens.filter((s) => s.orsEligible === true).length,
      orsQueued: ranking.selected.length,
      redirectedOtherSegment: byDisposition[PRE_ORS_DISPOSITION.BETTER_FIT_OTHER_SEGMENT] || 0,
      duplicates: byDisposition[PRE_ORS_DISPOSITION.DUPLICATE_EXISTING_OPPORTUNITY] || 0,
      excludedOrOutOfMarket:
        (byDisposition[PRE_ORS_DISPOSITION.EXCLUDE] || 0) +
        (byDisposition[PRE_ORS_DISPOSITION.OUT_OF_MARKET] || 0),
    },
  };
}
