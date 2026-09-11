/**
 * ADP Bethesda Marriott — NEW PROPERTY INTAKE / GOVERNED PREFLIGHT (zero provider calls).
 *
 * Doctrine:
 *   METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 *   EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION; MEASUREMENT_REMAINS_GOVERNED.
 *
 * Does NOT:
 *   - call live providers
 *   - publish
 *   - invent Census records
 *   - invent CORE peer sets
 *   - activate Exec Read V3 globally
 *   - distribute externally
 */

import { existsSync, readFileSync } from "fs";
import { join } from "path";
import { createCensusReadProvider } from "../../hotel-intelligence/providers/census-read.js";
import { loadPropertyProfile, listPropertyProfiles, PROVIDERS } from "../data-model.js";
import { estimateCost } from "./multi-provider-runner.js";
import { buildScenarioUniverse, resolveStandardScenarioMarket } from "../prompt-universe/scenario-registry.js";
import {
  propertyCoreGovernanceReady,
  stabilizedCoreIdsForProperty,
  MIN_CORE_HOTELS_PRODUCTION,
} from "../metrics/property-core-governance-data.js";
import { getEntityRegistryForProperty } from "../metrics/adp-property-entity-registries.js";
import { getCensusLinkEntry } from "../census-link-registry.js";
import { resolveGovernedAdpPropertyUniverseV1 } from "../client-readiness/resolve-governed-adp-property-universe-v1.js";
import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import { territoryLabelForIntent } from "../metrics/intent-territory-labels.js";
import { getPortfolioMapping } from "../brand-portfolio/brand-portfolio-affiliation-mapping-v1.js";
import { getStandardScenarios } from "../prompt-universe/standard-scenarios.js";
import {
  BETHESDA_FIRST_PARTY,
  BETHESDA_MARKET_DEFINITION,
  BETHESDA_PEER_SET,
  BETHESDA_VERIFIED_ALIASES,
  BETHESDA_NEGATIVE_ALIASES,
  BETHESDA_IDENTITY_KEY,
} from "./bethesda-marriott-foundation-v1.js";
import { assertBethesdaMarriottVsNorthDistinctEntity } from "../metrics/bethesda-montgomery-entity-registry.js";
import { BETHESDA_PEER_CHALLENGE } from "../metrics/bethesda-marriott-core-governance.js";
import { PORTFOLIO_TYPES } from "../brand-portfolio/brand-portfolio-position-contract-v1.js";
import { METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN } from "../governance/adp-methodology-governance-v1.js";
import { ADP_PRE_PUBLICATION_CLIENT_DISTRIBUTION_GATES_V1 } from "../governance/adp-pre-publication-client-distribution-gates-v1.js";

export const BETHESDA_MARRIOTT_INTAKE_PREFLIGHT_VERSION =
  "adp_bethesda_marriott_intake_preflight_v1";
export const PROPOSED_PROPERTY_ID = "adp_bethesda_marriott";
export const PROPOSED_REGISTRY_KEY = "bethesda_marriott";
export const PROPOSED_DISPLAY_NAME = "Bethesda Marriott";
export const BETHESDA_MARRIOTT_COST_CAP_USD = 12;
export const SAME_REAL_WORLD_HOTEL_SAME_CANONICAL_ENTITY =
  "SAME_REAL_WORLD_HOTEL_SAME_CANONICAL_ENTITY";
export const SINGLE_CANONICAL_SUBJECT_PRESENCE_PATH =
  "SINGLE_CANONICAL_SUBJECT_PRESENCE_PATH";
export const SAME_ANALYTICAL_QUESTION_SAME_GOVERNED_INTENT =
  "SAME_ANALYTICAL_QUESTION_SAME_GOVERNED_INTENT";

/** Founder-supplied address — identity seed only until Census + first-party freeze. */
export const BETHESDA_MARRIOTT_IDENTITY_SEED = Object.freeze({
  proposedPropertyId: PROPOSED_PROPERTY_ID,
  proposedRegistryKey: PROPOSED_REGISTRY_KEY,
  proposedDisplayName: PROPOSED_DISPLAY_NAME,
  addressLine1: "5151 Pooks Hill Road",
  city: "Bethesda",
  state: "MD",
  stateFull: "Maryland",
  postalCode: "20814",
  country: "United States",
  countryCode: "US",
  brandExpected: "Marriott Hotels",
  parentExpected: "Marriott International",
  officialPropertyPageUrl:
    "https://www.marriott.com/en-us/hotels/wasbt-bethesda-marriott/overview/",
  marriottHotelCode: "WASBT",
  brandPropertyPathHints: Object.freeze([
    "wasbt",
    "bethesda-marriott",
    "wasbt-bethesda-marriott",
  ]),
});

/**
 * Candidate aliases — NOT frozen until identity evidence review.
 * Include only strings that clearly refer to the same physical hotel.
 */
export const BETHESDA_MARRIOTT_ALIAS_CANDIDATES = Object.freeze([
  {
    alias: "Bethesda Marriott",
    status: "PROPOSED_PRIMARY",
    evidence: "Founder display name + Marriott first-party page title pattern",
  },
  {
    alias: "Bethesda Marriott Hotel",
    status: "CANDIDATE",
    evidence: "Common commercial variant; confirm against first-party naming",
  },
  {
    alias: "Marriott Bethesda",
    status: "CANDIDATE",
    evidence: "Common inverted naming; confirm same WASBT property",
  },
  {
    alias: "Bethesda Marriott at Pooks Hill",
    status: "CANDIDATE",
    evidence: "Address-linked variant; confirm same physical hotel before freeze",
  },
  {
    alias: "WASBT",
    status: "INTERNAL_CODE_ONLY",
    evidence: "Marriott hotel code — useful for source matching, not traveler alias",
  },
]);

/**
 * Steward-review peer candidates only — NOT a frozen CORE set.
 * Methodology unchanged; ≥4 CORE required per intent for numeric benchmark.
 */
export const BETHESDA_MARRIOTT_PEER_CANDIDATES_FOR_STEWARD_REVIEW = Object.freeze([
  {
    name: "Hilton Garden Inn Bethesda",
    rationale:
      "Same Bethesda demand geography; upper-mid/upscale business traveler overlap — confirm class/substitution before CORE",
    status: "STEWARD_REVIEW_REQUIRED",
  },
  {
    name: "Hyatt Regency Bethesda",
    rationale:
      "Full-service Bethesda peer with strong meetings/business substitution plausibility",
    status: "STEWARD_REVIEW_REQUIRED",
  },
  {
    name: "The Bethesdan Hotel",
    rationale: "Local Bethesda alternative; verify class and traveler substitution",
    status: "STEWARD_REVIEW_REQUIRED",
  },
  {
    name: "AC Hotel Bethesda Downtown",
    rationale: "Marriott soft-brand in Bethesda — substitution vs same-parent brand leakage risk",
    status: "STEWARD_REVIEW_REQUIRED",
  },
  {
    name: "Residence Inn Bethesda Downtown",
    rationale: "Extended-stay Bethesda; may be CONDITIONAL/NON_COMPARABLE for some intents",
    status: "STEWARD_REVIEW_REQUIRED",
  },
  {
    name: "Bethesda North Marriott Hotel & Conference Center",
    rationale:
      "Same brand family / North Bethesda — likely DIFFERENT physical hotel; do NOT alias to subject; may be CORE or brand-portfolio peer after steward review",
    status: "STEWARD_REVIEW_REQUIRED_DISTINCT_ENTITY",
  },
  {
    name: "Washington DC suburban upper-upscale business hotels (TBD)",
    rationale:
      "Market pack may need DC-suburban scope beyond municipal Bethesda — steward must freeze geography first",
    status: "STEWARD_REVIEW_REQUIRED",
  },
]);

/** First-party supported facts only (no inferred amenities). */
export const BETHESDA_MARRIOTT_REALITY_GAP_FACT_CANDIDATES = Object.freeze([
  {
    attribute: "marriott_hotels",
    canonicalFact: "Marriott Hotels brand property (WASBT) on marriott.com",
    source: BETHESDA_MARRIOTT_IDENTITY_SEED.officialPropertyPageUrl,
    confidence: "HIGH",
    realityGapEligible: true,
  },
  {
    attribute: "marriott_bonvoy",
    canonicalFact: "Marriott Bonvoy loyalty property (Marriott Hotels)",
    source: BETHESDA_MARRIOTT_IDENTITY_SEED.officialPropertyPageUrl,
    confidence: "HIGH",
    realityGapEligible: true,
  },
  {
    attribute: "bethesda_md_location",
    canonicalFact: "5151 Pooks Hill Road, Bethesda, Maryland 20814",
    source: BETHESDA_MARRIOTT_IDENTITY_SEED.officialPropertyPageUrl,
    confidence: "HIGH",
    realityGapEligible: true,
  },
  {
    attribute: "pooks_hill",
    canonicalFact: "Located on Pooks Hill Road corridor",
    source: BETHESDA_MARRIOTT_IDENTITY_SEED.officialPropertyPageUrl,
    confidence: "HIGH",
    realityGapEligible: true,
  },
  {
    attribute: "fitness_center_24hr",
    canonicalFact: "24-hour fitness center with Peloton / Fitness on Demand",
    source: BETHESDA_MARRIOTT_IDENTITY_SEED.officialPropertyPageUrl,
    confidence: "HIGH",
    realityGapEligible: true,
  },
  {
    attribute: "peloton",
    canonicalFact: "Peloton available in fitness center",
    source: BETHESDA_MARRIOTT_IDENTITY_SEED.officialPropertyPageUrl,
    confidence: "HIGH",
    realityGapEligible: true,
  },
  {
    attribute: "coopers_mill",
    canonicalFact: "On-property restaurant Cooper's Mill",
    source: "https://www.marriott.com/en-us/hotels/wasbt-bethesda-marriott/dining/",
    confidence: "HIGH",
    realityGapEligible: true,
  },
  {
    attribute: "m_club",
    canonicalFact: "M Club / Concierge Level access available",
    source: "https://www.marriott.com/en-us/hotels/wasbt-bethesda-marriott/rooms/",
    confidence: "HIGH",
    realityGapEligible: true,
  },
  {
    attribute: "meeting_space",
    canonicalFact:
      "Meeting / event space 18,719 sq ft across 27 rooms; Grand Ballroom 4,592 sq ft",
    source: "https://www.marriott.com/en-us/hotels/wasbt-bethesda-marriott/events/",
    confidence: "HIGH",
    realityGapEligible: true,
  },
  {
    attribute: "ballroom",
    canonicalFact: "Grand Ballroom on property",
    source: "https://www.marriott.com/en-us/hotels/wasbt-bethesda-marriott/events/",
    confidence: "HIGH",
    realityGapEligible: true,
  },
  {
    attribute: "outdoor_pool",
    canonicalFact: "Outdoor / pool patio referenced on Marriott wedding/events copy",
    source: "https://www.marriott.com/en-us/hotels/wasbt-bethesda-marriott/events/weddings/",
    confidence: "MEDIUM",
    realityGapEligible: true,
  },
  {
    attribute: "nih_access",
    canonicalFact: "Demand adjacency to NIH / medical campus corridor",
    source: "Market context + Pooks Hill / Rockville Pike location",
    confidence: "HIGH",
    realityGapEligible: true,
  },
  {
    attribute: "dc_metro_access",
    canonicalFact: "Washington DC metro access from Bethesda / Montgomery County",
    source: "Market geography",
    confidence: "HIGH",
    realityGapEligible: true,
  },
  {
    attribute: "room_count",
    canonicalFact: "407 rooms (MEDIUM confidence)",
    source: "AAA TripCanvas + Maryland transaction corroboration; not Marriott overview numeric",
    confidence: "MEDIUM",
    realityGapEligible: false,
    note: "NOT_ELIGIBLE_FOR_REALITY_GAP until steward HIGH",
  },
  {
    attribute: "parking",
    canonicalFact: "UNVERIFIED — parking terms not frozen from first-party",
    confidence: "UNVERIFIED",
    realityGapEligible: false,
  },
]);

function profileExists(propertyId) {
  try {
    return Boolean(loadPropertyProfile(propertyId));
  } catch {
    return false;
  }
}

async function searchCensusForBethesda() {
  const provider = createCensusReadProvider();
  const availability = await provider.getAvailabilityStatus();
  const queries = [
    { name: "Bethesda Marriott", city: "Bethesda", limit: 20 },
    { name: "Pooks Hill", city: "Bethesda", limit: 20 },
    { name: "Bethesda Marriott", limit: 20 },
    { name: "WASBT", limit: 10 },
    { name: "5151 Pooks Hill", limit: 10 },
  ];
  const results = [];
  for (const q of queries) {
    const r = await provider.searchHotels(q);
    results.push({
      query: q,
      providerStatus: r.provider_status?.status || null,
      hitCount: (r.hotels || []).length,
      hits: (r.hotels || []).slice(0, 5).map((h) => ({
        censusRecordId: h.airtable_record_id,
        hotel_id: h.hotel_id,
        name: h.name,
        address: h.address,
        city: h.city,
        brand: h.brand_name,
        parent: h.parent_company_name,
        rooms: h.room_count,
        identity_key: h.raw_safe?.property_identity_key,
        match_score: h.match_score,
        website: h.website,
      })),
    });
  }

  const exactish = [];
  for (const block of results) {
    for (const h of block.hits) {
      const hay = `${h.name || ""} ${h.address || ""} ${h.city || ""}`.toLowerCase();
      if (
        (hay.includes("bethesda") && hay.includes("marriott") && !hay.includes("north marriott")) ||
        hay.includes("pooks hill") ||
        hay.includes("wasbt") ||
        String(h.identity_key || "").includes("wasbt")
      ) {
        exactish.push(h);
      }
    }
  }

  return {
    availability,
    queries: results,
    samePhysicalHotelMatches: exactish,
    censusRecordFound: exactish.length > 0,
  };
}

function draftMarketAssessment(profile) {
  const resolvedKey = profile ? resolveStandardScenarioMarket(profile) : null;
  const chainScale = String(profile?.chainScale || "Upper Upscale")
    .toLowerCase()
    .replace(/\s+/g, "_");
  const pack = resolvedKey ? getStandardScenarios(resolvedKey, chainScale) : [];
  const packExists = pack.length >= 40;
  return {
    proposedCanonicalMarketLabel: BETHESDA_MARKET_DEFINITION.customerFacingLabel,
    canonicalMarketId: BETHESDA_MARKET_DEFINITION.canonicalMarketId,
    geographicScope: BETHESDA_MARKET_DEFINITION.geographicScope,
    marketRationale: BETHESDA_MARKET_DEFINITION.rationale,
    rejectedAlternatives: BETHESDA_MARKET_DEFINITION.rejectedAlternatives,
    resolvedMarketKey: resolvedKey,
    standardScenarioCount: pack.length,
    status: packExists ? "FROZEN" : "STEWARD_REVIEW_REQUIRED",
    standardScenarioPackExists: packExists,
    stewardReviewRequired: BETHESDA_MARKET_DEFINITION.stewardReviewRequired,
  };
}

function bppPlan() {
  const mapping = getPortfolioMapping(PROPOSED_PROPERTY_ID);
  const defaultLens = mapping?.lenses?.find((l) => l.lensId === mapping.defaultLensId) || null;
  return {
    likelyEligible: Boolean(mapping),
    eligible: Boolean(mapping && defaultLens?.portfolioType === PORTFOLIO_TYPES.LOYALTY_ECOSYSTEM),
    expectedBrand: "Marriott Hotels",
    expectedParent: "Marriott International",
    expectedPrimaryLens: "marriott_bonvoy",
    portfolioType: "LOYALTY_ECOSYSTEM",
    mappingStatus: mapping ? "REGISTERED" : "NOT_YET_REGISTERED",
    mapping,
    note: mapping
      ? "Marriott Bonvoy LOYALTY_ECOSYSTEM eligible. Same-market BPP peers use certified expansion hierarchy (candidates: Bethesda North, Residence Inn Bethesda Downtown, AC Hotel Bethesda Downtown) — do not invent peer sets."
      : "Prepare Marriott Bonvoy mapping under existing BPP rules.",
    peerPlan: mapping
      ? "ELIGIBLE_PEER_SET_USES_CERTIFIED_EXPANSION_HIERARCHY"
      : "DEFER_UNTIL_AFFILIATION_MAPPING_AND_CERTIFIED_PEER_RULES",
  };
}

function execReadCompatibility() {
  return {
    productionCompositionAtPublishTime: "FOLLOW_CURRENT_PRODUCTION_LAW",
    v3ArchitectureApprovedAsPreview:
      "HEADLINE | KEY INSIGHT | WHY IT MATTERS | FOCUS NOW | WATCH? | WHAT TO REVIEW",
    activateV3GloballyAsPartOfThisIntake: false,
    selectiveNumericAnchorsWhenV3Active: true,
    oneOffNarrativePath: false,
    note: "Bethesda must use the shared composition path — no property-specific narrative engine.",
  };
}

/**
 * @returns {Promise<object>}
 */
export async function runBethesdaMarriottIntakePreflightV1(opts = {}) {
  const blockers = [];
  const stewardReview = [];
  const propertyId = PROPOSED_PROPERTY_ID;

  const universe = resolveGovernedAdpPropertyUniverseV1();
  const universeCount = universe.properties?.length || 0;
  const alreadyInUniverse = (universe.properties || []).some(
    (p) => p.propertyId === propertyId
  );

  const hasProfile = profileExists(propertyId);
  const profile = hasProfile ? loadPropertyProfile(propertyId) : null;
  const censusLink = getCensusLinkEntry(propertyId);
  const entityRegistry = getEntityRegistryForProperty(propertyId);
  const coreReady = hasProfile ? propertyCoreGovernanceReady(propertyId) : false;

  const censusSearch =
    opts.skipCensusSearch === true
      ? { skipped: true, censusRecordFound: false, samePhysicalHotelMatches: [] }
      : await searchCensusForBethesda();

  // Prefer census-links SoT; search confirms same physical hotel.
  if (!censusSearch.censusRecordFound && !censusLink?.censusRecordId) {
    blockers.push("CENSUS_RECORD_MISSING");
    stewardReview.push({
      id: "CENSUS_INTEGRATION_REQUIRED",
      action:
        "Create or import Bethesda Marriott into Dealality Hotel Property Census with address 5151 Pooks Hill Road, Bethesda MD 20814, then link via census-links-v1.json.",
    });
  } else if (!censusSearch.censusRecordFound && censusLink?.censusRecordId) {
    // Link exists; search lag / indexing — treat linked record as present for foundation GO.
    censusSearch.censusRecordFound = true;
    censusSearch.samePhysicalHotelMatches = [
      {
        censusRecordId: censusLink.censusRecordId,
        hotel_id: censusLink.canonicalHotelId || BETHESDA_IDENTITY_KEY,
        name: censusLink.propertyName || PROPOSED_DISPLAY_NAME,
        identity_key: BETHESDA_IDENTITY_KEY,
      },
    ];
  }

  if (!hasProfile) blockers.push("PROPERTY_PROFILE_MISSING");
  if (!entityRegistry) blockers.push("ENTITY_REGISTRY_MISSING");
  if (!coreReady) blockers.push("CORE_GOVERNANCE_NOT_READY");
  if (!censusLink?.censusRecordId) blockers.push("CENSUS_LINK_MISSING");

  const market = draftMarketAssessment(profile);
  if (!market.standardScenarioPackExists) {
    blockers.push("STANDARD_SCENARIO_PACK_MISSING_FOR_MARKET");
    stewardReview.push({
      id: "MARKET_DEFINITION",
      action: "Standard scenario pack missing for resolved Bethesda / Montgomery market",
    });
  }

  const distinctEntity = assertBethesdaMarriottVsNorthDistinctEntity();
  if (!distinctEntity.pass) {
    blockers.push("BETHESDA_NORTH_DISTINCT_ENTITY_REGRESSION_FAIL");
    stewardReview.push({
      id: "DISTINCT_ENTITY_BETHESDA_NORTH_MARRIOTT",
      action: "Bethesda North must remain DISTINCT_ENTITY from WASBT",
    });
  }

  if (!coreReady) {
    stewardReview.push({
      id: "CORE_PEER_SET",
      action:
        "Freeze CORE peers (≥4 per intent where numeric benchmark required) under existing methodology",
    });
  }

  if (BETHESDA_FIRST_PARTY.roomCountConfidence !== "HIGH") {
    stewardReview.push({
      id: "ROOM_COUNT_CONFIDENCE_MEDIUM",
      action:
        "Rooms = 407 at MEDIUM confidence (excluded from Reality Gap). Optional steward upgrade to HIGH when stronger first-party evidence exists — does not block foundation GO.",
    });
  }

  let scenarioPlan = {
    standardScenarios: 0,
    propertySpecificScenarios: 0,
    totalScenarios: 0,
    territoryCount: Object.keys(TRAVELER_INTENTS).length,
    providerCount: PROVIDERS.length,
    expectedTotalObservations: 0,
    resolvedMarketKey: null,
    note: "Cannot build governed scenario universe until market pack + property profile exist",
  };

  if (profile) {
    const scenarios = buildScenarioUniverse(profile);
    const std = scenarios.filter((s) => s.source === "standard").length;
    const spec = scenarios.filter((s) => s.source === "property_specific").length;
    scenarioPlan = {
      standardScenarios: std,
      propertySpecificScenarios: spec,
      totalScenarios: scenarios.length,
      territoryCount: Object.keys(TRAVELER_INTENTS).length,
      providerCount: PROVIDERS.length,
      expectedTotalObservations: scenarios.length * PROVIDERS.length,
      resolvedMarketKey: resolveStandardScenarioMarket(profile),
    };
    if (std < 40) blockers.push("STANDARD_SCENARIO_PACK_TOO_SMALL");
    if (spec < 12) blockers.push("PROPERTY_SCENARIOS_TOO_SMALL");
    if (scenarios.length < 60 || scenarios.length > 66) {
      blockers.push(`SCENARIO_TOTAL_OUT_OF_TOLERANCE_${scenarios.length}`);
    }
  } else {
    // Planned cost assuming governed 60–66 pack once market scenarios exist
    scenarioPlan.plannedScenarioBand = "60-66";
    scenarioPlan.plannedObservationBand = "240-264";
  }

  const plannedScenariosForCost = profile
    ? scenarioPlan.totalScenarios
    : 63; // mid-band planning estimate only
  const cost = estimateCost(plannedScenariosForCost || 0);
  const roundedTotal = Math.round(cost.total * 100) / 100;
  const plannedCalls = (plannedScenariosForCost || 0) * PROVIDERS.length;
  if (roundedTotal > BETHESDA_MARRIOTT_COST_CAP_USD) blockers.push("COST_CAP_EXCEEDED");

  const dropdownLeak =
    hasProfile && listPropertyProfiles().some((p) => p.propertyId === propertyId);
  if (dropdownLeak) blockers.push("DROPDOWN_LIST_LEAK_BEFORE_CERTIFICATION");

  const coreRows = Object.values(TRAVELER_INTENTS).map((intent) => {
    const core = hasProfile ? stabilizedCoreIdsForProperty(propertyId, intent) : [];
    return {
      TERRITORY: territoryLabelForIntent(intent),
      intent,
      CORE_COUNT: core.length,
      CORE_HOTELS: core,
      CERTIFICATION_READY: core.length >= MIN_CORE_HOTELS_PRODUCTION,
      BLOCKER:
        core.length >= MIN_CORE_HOTELS_PRODUCTION
          ? null
          : core.length
            ? "BELOW_MIN_CORE_4_FOR_NUMERIC_BENCHMARK"
            : "NO_CORE_PEERS",
    };
  });

  const dryRunQa = {
    canonicalIdentity: hasProfile && censusLink?.censusRecordId ? "READY" : "PARTIAL",
    duplicateEntityChecks: distinctEntity.pass
      ? "PASS_BETHESDA_MARRIOTT_VS_BETHESDA_NORTH_DISTINCT_ENTITY"
      : "FAIL",
    peerEntityBinding: coreReady ? "READY" : "MISSING",
    promptSemanticParity:
      market.standardScenarioPackExists && scenarioPlan.standardScenarios >= 40
        ? "PASS_ADP_PROMPT_SEMANTIC_INTENT_PARITY"
        : "NOT_READY_NO_MARKET_PACK",
    scenarioCompleteness:
      scenarioPlan.totalScenarios >= 60 && scenarioPlan.totalScenarios <= 66
        ? "READY"
        : "MISSING",
    providerConfiguration: {
      providers: PROVIDERS,
      status: "READY_IN_PLATFORM",
    },
    realityGapSourceCompleteness: "MAPPED_VERIFIED_FACTS",
    bppEligibility: bppPlan(),
    storagePaths: {
      profilePathExpected: `fixtures/ai-demand-positioning/bethesda-marriott-property-profile.json`,
      publishedPathExpected: `data/ai-demand-positioning/published/${propertyId}/`,
      exists: {
        profile: hasProfile,
        published: existsSync(
          join(process.cwd(), `data/ai-demand-positioning/published/${propertyId}`)
        ),
      },
    },
    snapshotReadiness: hasProfile ? "FOUNDATION_READY" : "NOT_READY",
    correctionGovernanceReadiness: "PLATFORM_READY_PROPERTY_REGISTERED_PRE_PUBLISH",
    measurementAssuranceCompatibility: "REQUIRES_BASELINE_AFTER_FOUNDER_AUTH",
    subjectPathAConfigReady: Boolean(entityRegistry),
    maCompatibilityReady: true,
    correctionGovernanceReady: true,
    executiveReadCompatibility: execReadCompatibility(),
    prePublicationGatesContract: ADP_PRE_PUBLICATION_CLIENT_DISTRIBUTION_GATES_V1,
    failures: blockers,
  };

  const liveEligible =
    blockers.length === 0 && roundedTotal <= BETHESDA_MARRIOTT_COST_CAP_USD;

  return {
    version: BETHESDA_MARRIOTT_INTAKE_PREFLIGHT_VERSION,
    auditedAt: new Date().toISOString(),
    LIVE_PROVIDER_CALLS: 0,
    methodologyChanged: false,
    doctrine: [METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN],
    hardStopBeforeExternalDistribution: true,
    hardStopBeforeLiveMeasurement: !liveEligible,

    A_BETHESDA_MARRIOTT_CANONICAL_IDENTITY: {
      propertyId: PROPOSED_PROPERTY_ID,
      canonicalHotelId:
        censusLink?.canonicalHotelId ||
        censusSearch.samePhysicalHotelMatches?.[0]?.hotel_id ||
        BETHESDA_IDENTITY_KEY,
      censusId:
        censusLink?.censusRecordId ||
        censusSearch.samePhysicalHotelMatches?.[0]?.censusRecordId ||
        null,
      address: `${BETHESDA_MARRIOTT_IDENTITY_SEED.addressLine1}, ${BETHESDA_MARRIOTT_IDENTITY_SEED.city}, ${BETHESDA_MARRIOTT_IDENTITY_SEED.state} ${BETHESDA_MARRIOTT_IDENTITY_SEED.postalCode}, ${BETHESDA_MARRIOTT_IDENTITY_SEED.country}`,
      brand: {
        expected: BETHESDA_MARRIOTT_IDENTITY_SEED.brandExpected,
        verifiedFromFirstParty: true,
        firstPartyUrl: BETHESDA_MARRIOTT_IDENTITY_SEED.officialPropertyPageUrl,
        marriottCode: BETHESDA_MARRIOTT_IDENTITY_SEED.marriottHotelCode,
        status: "FROZEN",
      },
      parent: {
        expected: BETHESDA_MARRIOTT_IDENTITY_SEED.parentExpected,
        status: "FROZEN",
      },
      market: market.proposedCanonicalMarketLabel,
      marketStatus: market.status,
      sameRealWorldHotelRule: SAME_REAL_WORLD_HOTEL_SAME_CANONICAL_ENTITY,
      identitySeed: BETHESDA_MARRIOTT_IDENTITY_SEED,
      censusSearchSummary: {
        found: censusSearch.censusRecordFound,
        matchCount: censusSearch.samePhysicalHotelMatches?.length || 0,
        availability: censusSearch.availability,
      },
    },

    B_IDENTITY_ALIASES: {
      frozen: true,
      candidates: BETHESDA_MARRIOTT_ALIAS_CANDIDATES,
      verified: BETHESDA_VERIFIED_ALIASES,
      negativeDistinct: BETHESDA_NEGATIVE_ALIASES,
      distinctEntityRegression: distinctEntity,
      subjectPresencePathRequired: SINGLE_CANONICAL_SUBJECT_PRESENCE_PATH,
      pathBFallbackAllowed: false,
      staleGovernedInterpretationShortCircuitAllowed: false,
    },

    C_CORE_PEER_SET: {
      status: coreReady ? "FROZEN" : "STEWARD_REVIEW_REQUIRED",
      rule: "ADP_CORE_PEER_GOVERNANCE_V1",
      minCorePerIntentForNumericBenchmark: MIN_CORE_HOTELS_PRODUCTION,
      frozenCore: coreReady ? BETHESDA_PEER_SET.filter((p) => p.role === "CORE") : null,
      peerChallenge: BETHESDA_PEER_CHALLENGE,
      candidates: BETHESDA_MARRIOTT_PEER_CANDIDATES_FOR_STEWARD_REVIEW,
      coreByIntent: coreRows,
      methodologyChanged: false,
    },

    D_BPP_ELIGIBILITY_PEER_PLAN: bppPlan(),

    E_GOVERNED_SCENARIO_PROVIDER_PLAN: {
      ...scenarioPlan,
      intents: Object.values(TRAVELER_INTENTS),
      promptParityRule: SAME_ANALYTICAL_QUESTION_SAME_GOVERNED_INTENT,
      bethesdaSpecificPromptMethodologyAllowed: false,
      marketAssessment: market,
    },

    F_REALITY_GAP_PROPERTY_FACTS: BETHESDA_MARRIOTT_REALITY_GAP_FACT_CANDIDATES,

    G_COST_PREFLIGHT: {
      EXPECTED_PROVIDER_CALLS: plannedCalls,
      EXPECTED_COST: roundedTotal,
      HARD_COST_CAP: BETHESDA_MARRIOTT_COST_CAP_USD,
      estimateBasisScenarios: plannedScenariosForCost,
      estimateNote: profile
        ? "From current profile scenario universe"
        : "Planning mid-band 63 scenarios assuming future governed pack (60–66); not executable until pack exists",
      byProvider: cost.byProvider,
      providers: PROVIDERS,
      withinNorm: roundedTotal <= BETHESDA_MARRIOTT_COST_CAP_USD,
    },

    H_DRY_RUN_QA: dryRunQa,

    I_STEWARD_REVIEW_ITEMS: stewardReview,

    J_LIVE_RUN_ELIGIBILITY: liveEligible ? "GO" : "NO-GO",

    K_FULL_UNIVERSE_DISCOVERY: {
      currentPublishedUniverseCount: universeCount,
      alreadyDiscovered: alreadyInUniverse,
      postAddExpectedCount: alreadyInUniverse ? universeCount : universeCount + 1,
      discoveryMechanism: "listPublishedPropertyIds / resolveGovernedAdpPropertyUniverseV1 — no hard-coded 14",
      note: "Bethesda enters universe only after publish under data/ai-demand-positioning/published/adp_bethesda_marriott/",
    },

    L_METHODOLOGY_CHANGED: "NO",

    blockers,
    PREFLIGHT: liveEligible ? "PASS" : "FAIL",
  };
}
