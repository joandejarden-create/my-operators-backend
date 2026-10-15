/**
 * Hotel segment research plan V1.
 *
 * Generates a per-hotel commercial-segment research plan from location,
 * positioning, inventory, catchment, and target business — then scores
 * research coverage from existing opportunities (not config lists alone).
 *
 * Association research alone is never treated as full hotel coverage.
 * Equal prospect counts across segments are not required.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { DEMAND_FAMILY } from "../demand-signal-types.js";
import { OPPORTUNITY_TYPE } from "../claim-types.js";
import { isCustomerFacingOpportunity } from "../customer-visibility.js";
import { evaluateProspectPublicationEligibility } from "../prospect-publication-eligibility-v1.js";
import {
  assessScalingCompaniesApplicability,
  isScalingCompanyOpportunity,
} from "./scaling-companies-research-v1.js";

export const HOTEL_SEGMENT_RESEARCH_PLAN_VERSION = "gdi-hotel-segment-research-plan-v1";

/** Commercial research segments (sales-facing, not capability paths). */
export const RESEARCH_SEGMENT = Object.freeze({
  ASSOCIATIONS_GOVERNMENT: "associations_government",
  SPORTS: "sports",
  CORPORATE_MEETINGS_TRAINING: "corporate_meetings_training",
  SCALING_COMPANIES: "scaling_companies",
  MEDICAL_ACADEMIC: "medical_academic",
  WEDDINGS_SOCIAL_PRIVATE_EVENTS: "weddings_social_private_events",
  ENTERTAINMENT_PRODUCTION: "entertainment_production",
  EXHIBITOR_SPONSOR_TEAMS: "exhibitor_sponsor_teams",
  TOURS_EDUCATIONAL_SPECIALIST: "tours_educational_specialist",
});

export const RESEARCH_SEGMENT_LABEL = Object.freeze({
  [RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT]: "Associations / government",
  [RESEARCH_SEGMENT.SPORTS]: "Sports",
  [RESEARCH_SEGMENT.CORPORATE_MEETINGS_TRAINING]:
    "Corporate meetings, training, and project teams",
  [RESEARCH_SEGMENT.SCALING_COMPANIES]:
    "Scaling Companies / Emerging Corporate Accounts",
  [RESEARCH_SEGMENT.MEDICAL_ACADEMIC]: "Medical / academic",
  [RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS]:
    "Weddings, social groups, and nearby private-event venues",
  [RESEARCH_SEGMENT.ENTERTAINMENT_PRODUCTION]: "Entertainment / production crews",
  [RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS]: "Exhibitor / sponsor teams",
  [RESEARCH_SEGMENT.TOURS_EDUCATIONAL_SPECIALIST]:
    "Tours / educational / specialist groups",
});

/** Applicability of a segment to the hotel. */
export const APPLICABILITY = Object.freeze({
  APPLICABLE: "applicable",
  NOT_APPLICABLE: "not_applicable",
});

/** Actual research coverage (evidence-backed — not config listing). */
export const RESEARCH_COVERAGE = Object.freeze({
  RESEARCHED: "researched",
  PARTIALLY_RESEARCHED: "partially_researched",
  BLOCKED: "blocked",
  NOT_YET_RESEARCHED: "not_yet_researched",
  NOT_APPLICABLE: "not_applicable",
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function lower(v) {
  return text(v).toLowerCase();
}

export function loadHotelConfig(hotelId) {
  const p = path.join(
    ROOT,
    "config/group-demand-intelligence/hotels",
    `${String(hotelId || "").trim()}.json`
  );
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function rooms(cfg) {
  return Number(cfg?.capabilityProfile?.totalGuestrooms || 0) || 0;
}

function meetingRooms(cfg) {
  return (
    Number(
      cfg?.capabilityProfile?.meetingRoomCount ??
        cfg?.capabilityProfile?.meetingRoomsIndoor ??
        0
    ) || 0
  );
}

function meetingSqFt(cfg) {
  return Number(cfg?.capabilityProfile?.totalMeetingSpaceSqFt || 0) || 0;
}

function territoryBlob(cfg) {
  return lower(
    [
      cfg?.displayName,
      cfg?.city,
      cfg?.country,
      cfg?.demandTerritory?.label,
      ...(cfg?.demandTerritory?.includes || []),
      cfg?.capabilityProfile?.classification,
      cfg?.capabilityProfile?.serviceLevel,
      cfg?.commercialPriorities?.notes,
      ...(cfg?.commercialPriorities?.demandAnchorFocus || []),
      ...(cfg?.commercialPriorities?.targetSegments || []),
    ].join(" ")
  );
}

function isUrbanMeetingsHotel(cfg) {
  const blob = territoryBlob(cfg);
  return (
    meetingRooms(cfg) >= 8 ||
    meetingSqFt(cfg) >= 10000 ||
    /downtown|urban|meetings|convention|business|historic/.test(blob)
  );
}

function isConventionAdjacent(cfg) {
  const blob = territoryBlob(cfg);
  return /convention|harbor|bayfront|waterfront|gaslamp|seaport/.test(blob);
}

function isCapitalAdvocacyMarket(cfg) {
  const blob = territoryBlob(cfg);
  return /washington|district of columbia|capitol|connecticut avenue|dupont|k street/.test(
    blob
  );
}

function isWaterfrontOrDestination(cfg) {
  const blob = territoryBlob(cfg);
  return /bay|harbor|waterfront|resort|beach|marina|destination/.test(blob);
}

/**
 * Assess segment applicability from hotel profile — not from empty targetSegments.
 */
export function assessSegmentApplicability(hotelConfig = {}) {
  const cfg = hotelConfig || {};
  const blob = territoryBlob(cfg);
  const r = rooms(cfg);
  const mr = meetingRooms(cfg);
  const urban = isUrbanMeetingsHotel(cfg);
  const convention = isConventionAdjacent(cfg);
  const capital = isCapitalAdvocacyMarket(cfg);
  const waterfront = isWaterfrontOrDestination(cfg);
  const weekend = cfg?.commercialPriorities?.weekendFocus === true;
  const explicit = (cfg?.commercialPriorities?.targetSegments || []).map(lower);

  const out = {};

  // Associations / government
  {
    const applicable =
      urban ||
      capital ||
      convention ||
      explicit.some((s) => /assoc|govern|ngo|nonprofit/.test(s)) ||
      mr >= 5;
    out[RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT] = applicable
      ? {
          applicability: APPLICABILITY.APPLICABLE,
          rationale: capital
            ? "Downtown capital / advocacy market with meeting inventory supports association and government programs."
            : convention
              ? "Convention-adjacent meetings hotel can capture association/government lodging and overflow."
              : "Meeting inventory and urban/business positioning support association and government demand.",
        }
      : {
          applicability: APPLICABILITY.NOT_APPLICABLE,
          rationale:
            "Insufficient meetings inventory or urban/government catchment to prioritize association/government research.",
        };
  }

  // Sports
  {
    const sportsExplicit = explicit.some((s) => /sport|tournament|team/.test(s));
    const youthSportsHub = /san diego|mission valley|carlsbad|anaheim|orlando/.test(
      blob
    );
    // Downtown luxury/historic DC: sports travel exists but is secondary vs advocacy meetings.
    const applicable = sportsExplicit || youthSportsHub || r >= 200 || weekend;
    out[RESEARCH_SEGMENT.SPORTS] = applicable
      ? {
          applicability: APPLICABILITY.APPLICABLE,
          rationale: youthSportsHub
            ? "Market hosts recurring youth/amateur tournaments; visiting teams and housing agencies are in catchment when venue geography is verified."
            : capital
              ? "Applicable but secondary: investigate visiting teams, tournament organizers, colleges, camps, and officials only when travel lodging (not local participation) is evidenced."
              : "Room inventory supports sports travel blocks when visiting teams or organizers need lodging near verified venues.",
          sportsResearchNotes: [
            "Investigate visiting teams, tournament organizers, housing agencies, colleges, camps, officials, and event crews.",
            "Verify venue geography and lodging mechanisms.",
            "Distinguish local participation from actual travel demand.",
            "Stay-to-play does not prove an available hotel allocation.",
          ],
        }
      : {
          applicability: APPLICABILITY.NOT_APPLICABLE,
          rationale:
            "No sports catchment or inventory profile that supports prioritized sports lodging research.",
        };
  }

  // Corporate meetings / training / project teams
  {
    const applicable =
      urban ||
      mr >= 5 ||
      explicit.some((s) => /corporate|training|project|meeting/.test(s));
    out[RESEARCH_SEGMENT.CORPORATE_MEETINGS_TRAINING] = applicable
      ? {
          applicability: APPLICABILITY.APPLICABLE,
          rationale:
            "Meeting rooms and urban/business positioning support corporate meetings, training cohorts, and project-team lodging.",
        }
      : {
          applicability: APPLICABILITY.NOT_APPLICABLE,
          rationale: "Limited meetings product for corporate/training research priority.",
        };
  }

  // Scaling companies / emerging corporate accounts
  {
    const sc = assessScalingCompaniesApplicability(cfg);
    out[RESEARCH_SEGMENT.SCALING_COMPANIES] = sc.applicable
      ? {
          applicability: APPLICABILITY.APPLICABLE,
          rationale: sc.rationale,
        }
      : {
          applicability: APPLICABILITY.NOT_APPLICABLE,
          rationale: sc.rationale,
        };
  }

  // Medical / academic
  {
    const applicable =
      capital ||
      convention ||
      urban ||
      explicit.some((s) => /medical|health|pharma|universit|academic|scientific/.test(s));
    out[RESEARCH_SEGMENT.MEDICAL_ACADEMIC] = applicable
      ? {
          applicability: APPLICABILITY.APPLICABLE,
          rationale: capital
            ? "DC advocacy and specialty-society cycles create medical/academic travel lodging near downtown hotels."
            : convention
              ? "Convention-center medical and life-science congresses create exhibitor and attendee lodging demand."
              : "Urban meetings hotel can serve medical/academic programs when travel cohorts are evidenced.",
        }
      : {
          applicability: APPLICABILITY.NOT_APPLICABLE,
          rationale: "No medical/academic catchment priority for this property profile.",
        };
  }

  // Weddings / social / private-event venues
  {
    const applicable =
      mr >= 3 ||
      weekend ||
      waterfront ||
      /historic|social|wedding|autograph|ballroom/.test(blob) ||
      explicit.some((s) => /social|wedding|weekend|private/.test(s));
    out[RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS] = applicable
      ? {
          applicability: APPLICABILITY.APPLICABLE,
          rationale: waterfront
            ? "Waterfront / destination positioning plus meeting inventory supports social events and nearby venue-referral partnerships."
            : "Historic or meetings hotel with social/ballroom capacity — nearby private-event venues and on-property social demand are in scope.",
        }
      : {
          applicability: APPLICABILITY.NOT_APPLICABLE,
          rationale: "Social/PE path not prioritized for this inventory profile.",
        };
  }

  // Entertainment / production
  {
    const applicable =
      waterfront ||
      /film|studio|production|entertainment|harbor|midway|broadway|theater district/.test(
        blob
      ) ||
      explicit.some((s) => /entertainment|production|film|crew/.test(s));
    out[RESEARCH_SEGMENT.ENTERTAINMENT_PRODUCTION] = applicable
      ? {
          applicability: APPLICABILITY.APPLICABLE,
          rationale: waterfront
            ? "Waterfront / downtown entertainment corridors can generate production-crew and touring-show lodging when named productions travel."
            : "Market entertainment inventory warrants production/touring-crew checks when travel lodging is evidenced.",
        }
      : capital
        ? {
            applicability: APPLICABILITY.APPLICABLE,
            rationale:
              "Capital markets host occasional productions and touring companies; applicable at lower priority than advocacy meetings.",
          }
        : {
            applicability: APPLICABILITY.NOT_APPLICABLE,
            rationale:
              "No entertainment/production catchment prioritized for this property.",
          };
  }

  // Exhibitor / sponsor teams
  {
    const applicable =
      convention ||
      capital ||
      explicit.some((s) => /exhibit|sponsor|trade.?show/.test(s));
    out[RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS] = applicable
      ? {
          applicability: APPLICABILITY.APPLICABLE,
          rationale: convention
            ? "Convention-adjacent hotel can pursue exhibitor/sponsor team lodging independent of official housing blocks."
            : "Advocacy / large meetings market supports exhibitor and sponsor satellite lodging research.",
        }
      : {
          applicability: APPLICABILITY.NOT_APPLICABLE,
          rationale: "No convention or large-meeting catchment for exhibitor-team priority.",
        };
  }

  // Tours / educational / specialist
  {
    const applicable =
      capital ||
      waterfront ||
      /museum|tour|education|school|university/.test(blob) ||
      explicit.some((s) => /tour|education|school|specialist/.test(s));
    out[RESEARCH_SEGMENT.TOURS_EDUCATIONAL_SPECIALIST] = applicable
      ? {
          applicability: APPLICABILITY.APPLICABLE,
          rationale: capital
            ? "Downtown DC attracts educational tours and specialist groups; applicable when named organizers and overnight needs are evidenced."
            : waterfront
              ? "Destination/waterfront markets attract educational and specialist tour series when travel lodging is verified."
              : "Educational/specialist tour demand may exist; research only with named organizers and travel evidence.",
        }
      : {
          applicability: APPLICABILITY.NOT_APPLICABLE,
          rationale: "Tours/educational specialist demand not prioritized for this profile.",
        };
  }

  return out;
}

/**
 * Classify an opportunity into a research segment (best effort).
 * Exhibitor/sponsor titles take precedence over generic Association labels.
 */
export function classifyOpportunitySegment(opp = {}) {
  const blob = lower(
    [
      opp.segment,
      opp.demandFamily,
      opp.opportunityType,
      opp.demandSignalType,
      opp.title,
      opp.organizationName,
      opp.company,
      opp.summaryWhat,
      opp.hotelOpportunityThesis,
    ].join(" ")
  );

  if (
    opp.peVenueId ||
    opp.demandFamily === DEMAND_FAMILY.PRIVATE_EVENTS ||
    opp.opportunityType === OPPORTUNITY_TYPE.VENUE_PARTNERSHIP ||
    opp.opportunityType === "VENUE_REFERRAL_PARTNERSHIP" ||
    /private.?event|venue.?partnership|wedding|social group/.test(blob)
  ) {
    return RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS;
  }

  if (
    isScalingCompanyOpportunity(opp) ||
    opp.opportunityType === OPPORTUNITY_TYPE.CORPORATE_ACCOUNT_RELATIONSHIP ||
    opp.detailLayout === "CORPORATE_ACCOUNT_RELATIONSHIP" ||
    /scaling compan|emerging corporate|corporate account relationship/.test(blob)
  ) {
    return RESEARCH_SEGMENT.SCALING_COMPANIES;
  }

  if (
    /\b(exhibit|exhibitor|sponsor|booth|trade.?show)\b/.test(blob) ||
    /exhibiting team/.test(blob)
  ) {
    return RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS;
  }

  if (
    /\b(sport|tournament|volleyball|baseball|softball|soccer|cheer|athletic|perfect game|stay.?to.?play)\b/.test(
      blob
    ) ||
    /youth sports|team sports|team events/.test(blob)
  ) {
    return RESEARCH_SEGMENT.SPORTS;
  }

  if (
    /\b(film|production|crew|touring|entertainment|broadcast|concert)\b/.test(blob)
  ) {
    return RESEARCH_SEGMENT.ENTERTAINMENT_PRODUCTION;
  }

  if (
    /\b(tour|school group|student travel|educational travel|dmc|incentive)\b/.test(
      blob
    )
  ) {
    return RESEARCH_SEGMENT.TOURS_EDUCATIONAL_SPECIALIST;
  }

  if (
    /\b(medical|pharma|clinical|hospital|cancer|urolog|dermatolog|osteopath|translational|healthcare|nursing|physician)\b/.test(
      blob
    ) ||
    /\b(university|academic|college|ascb|embo)\b/.test(blob)
  ) {
    return RESEARCH_SEGMENT.MEDICAL_ACADEMIC;
  }

  if (
    /\b(corporate|training|train.?the.?trainer|project team|workshop|leadership academy|retreat)\b/.test(
      blob
    ) ||
    /corporate/.test(lower(opp.segment))
  ) {
    return RESEARCH_SEGMENT.CORPORATE_MEETINGS_TRAINING;
  }

  if (
    /\b(association|government|legislative|advocacy|congress|convention|nonprofit|ngo|federal)\b/.test(
      blob
    ) ||
    /association|government|conference|summit|convention/.test(lower(opp.segment))
  ) {
    return RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT;
  }

  // Default: treat unlabeled group lodging as association/government family
  // only when it looks like a dated occasion — otherwise leave unclassified noise out.
  if (
    opp.opportunityType === "GROUP_LODGING_OCCASION" ||
    opp.opportunityType === "OVERFLOW_HOUSING" ||
    opp.opportunityType === "PRIMARY_PURSUIT"
  ) {
    return RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT;
  }

  return null;
}

function isNoiseOrNonProspect(opp = {}) {
  const id = text(opp.id || opp.opportunityId);
  const title = text(opp.title || opp.opportunityName);
  // Synthetic scrape / non-opportunity debris — not segment research evidence
  if (/^gdi_v5_/i.test(id)) return true;
  if (
    /airline crew hotel contracts|flight attendant|affordable housing|dtap solicitation|procurement transparency|conrad washington/i.test(
      title
    )
  ) {
    return true;
  }
  if (opp.isTestData === true) return true;
  // customerVisible:false is incomplete research, not noise — score as partial
  return false;
}

function isCredibleProspect(opp = {}, hotelId = "") {
  if (isNoiseOrNonProspect(opp)) return false;
  const facing = isCustomerFacingOpportunity(
    { ...opp, hotelId: opp.hotelId || hotelId },
    { hotelId: opp.hotelId || hotelId }
  );
  if (facing) return true;
  // PE linked rows count even before reviewed stamp if venue IDs exist
  if (opp.peVenueId && opp.hotelVenueFitId) return true;
  // Substantive packed prospects with fit + source
  const elig = evaluateProspectPublicationEligibility(opp);
  if (elig.suitability?.ok && elig.route?.ok) return true;
  // Dated GROUP_LODGING with org + thesis
  if (
    (opp.opportunityType === "GROUP_LODGING_OCCASION" ||
      opp.opportunityType === "OVERFLOW_HOUSING" ||
      opp.opportunityType === "VENUE_PARTNERSHIP") &&
    text(opp.organizationName || opp.company) &&
    text(
      opp.hotelOpportunityThesis ||
        opp.fitExplanation ||
        opp.summaryWhyHotel ||
        opp.summaryWhyMatters
    )
  ) {
    return true;
  }
  return false;
}

function hasTravelCohortEvidence(opp = {}) {
  return Boolean(
    text(
      opp.relevantTravelers ||
        opp.travelEvidenceNote ||
        opp.summaryWhat ||
        opp.whoTravels ||
        opp.travelingCohort
    ) ||
      opp.travelEvidence === true ||
      text(opp.timing || opp.eventStartDate)
  );
}

function hasHotelFitThesis(opp = {}) {
  return Boolean(
    text(
      opp.fitExplanation ||
        opp.summaryWhyHotel ||
        opp.hotelFitRationale ||
        opp.hotelOpportunityThesis ||
        opp.hotelWinThesis ||
        opp.summaryWhyMatters
    ) || opp.hotelFitScore != null
  );
}

function hasContactRoute(opp = {}) {
  const elig = evaluateProspectPublicationEligibility(opp);
  return elig.route?.ok === true;
}

function nextActionsForSegment(segmentId, applicability, coverage, samples) {
  if (applicability !== APPLICABILITY.APPLICABLE) return [];
  const base = [
    "Reuse existing intelligence before new discovery.",
    "Research named accounts and traveling cohorts (who → why → where/when → hotel business → fit → contact → next action).",
  ];
  if (segmentId === RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS) {
    return [
      ...base,
      "Route private-event findings through canonical venue → signal → hotel-fit → GDI link (persistVenuePartnershipPackages / apply_pe_canonical).",
    ];
  }
  if (segmentId === RESEARCH_SEGMENT.SPORTS) {
    return [
      ...base,
      "Investigate visiting teams, tournament organizers, housing agencies, colleges, camps, officials, and event crews.",
      "Verify venue geography and lodging mechanisms; do not treat stay-to-play as an available allocation.",
      "Reject local-participation-only calendars without travel demand.",
    ];
  }
  if (segmentId === RESEARCH_SEGMENT.SCALING_COMPANIES) {
    return [
      ...base,
      "Require local connection + dated growth evidence + plausible hotel use + hotel fit — funding/hiring-page/directory alone is insufficient.",
      "Build standing CORPORATE_ACCOUNT_RELATIONSHIP cards (no invented event dates); dedupe one account → one opportunity.",
      "Select contact by use case (workplace/People Ops/EA/travel/events) — founder is not automatically the buyer.",
      "Reuse Demand Generator + hotel-fit model; refresh when expansion, hiring, office presence, or contacts change.",
    ];
  }
  if (coverage === RESEARCH_COVERAGE.RESEARCHED) {
    return [
      "Maintain coverage: refresh contacts and hotel-fit theses on existing named accounts; expand only where gaps remain.",
    ];
  }
  if (coverage === RESEARCH_COVERAGE.PARTIALLY_RESEARCHED) {
    return [
      ...base,
      samples.length
        ? `Strengthen existing candidates (${samples
            .slice(0, 3)
            .map((s) => s.title || s.id)
            .join("; ")}) — add travel cohort, hotel-fit thesis, and usable contact route.`
        : "Upgrade calendar-only rows into named-account prospects with fit and contact.",
    ];
  }
  if (coverage === RESEARCH_COVERAGE.BLOCKED) {
    return [
      "Blocked pending authorization/budget or missing prerequisite evidence — do not invent paid research.",
    ];
  }
  return [
    ...base,
    "Segment is applicable but not yet researched with credible hotel-specific prospects.",
  ];
}

/**
 * Build segment research plan + evidence-backed coverage for one hotel.
 *
 * @param {object} opts
 * @param {string} opts.hotelId
 * @param {object} [opts.hotelConfig]
 * @param {object[]} [opts.opportunities]
 * @param {object} [opts.artifacts] optional prior research markers
 *   { peCanonicalPersisted?: boolean, peVenueCount?: number, paidBudgetApproved?: boolean }
 */
export function buildHotelSegmentResearchPlan(opts = {}) {
  const hotelId = text(opts.hotelId);
  const cfg = opts.hotelConfig || loadHotelConfig(hotelId) || {};
  const opportunities = Array.isArray(opts.opportunities) ? opts.opportunities : [];
  const artifacts = opts.artifacts || {};

  const applicabilityMap = assessSegmentApplicability(cfg);
  const configListed = (cfg.commercialPriorities?.targetSegments || []).map(text);

  const bySegment = {};
  for (const id of Object.values(RESEARCH_SEGMENT)) {
    bySegment[id] = {
      opportunities: [],
      credible: [],
      partial: [],
      noise: [],
    };
  }

  for (const opp of opportunities) {
    const seg = classifyOpportunitySegment(opp);
    if (!seg || !bySegment[seg]) continue;
    bySegment[seg].opportunities.push(opp);
    if (isNoiseOrNonProspect(opp)) {
      bySegment[seg].noise.push(opp);
      continue;
    }
    if (isCredibleProspect(opp, hotelId)) {
      bySegment[seg].credible.push(opp);
    } else {
      bySegment[seg].partial.push(opp);
    }
  }

  // PE canonical persist artifact can elevate PE coverage even if list was stale
  if (artifacts.peCanonicalPersisted && artifacts.peVenueCount > 0) {
    // ensure PE segment has credit without inventing opportunity rows
  }

  const segments = [];
  for (const segmentId of Object.values(RESEARCH_SEGMENT)) {
    const app = applicabilityMap[segmentId];
    const bucket = bySegment[segmentId];
    const credible = bucket.credible;
    const partial = bucket.partial;
    const noise = bucket.noise;

    let researchCoverage = RESEARCH_COVERAGE.NOT_YET_RESEARCHED;
    let coverageRationale = "";

    if (app.applicability === APPLICABILITY.NOT_APPLICABLE) {
      researchCoverage = RESEARCH_COVERAGE.NOT_APPLICABLE;
      coverageRationale = app.rationale;
    } else if (
      segmentId === RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS &&
      artifacts.peCanonicalPersisted &&
      Number(artifacts.peVenueCount || 0) >= 1 &&
      credible.length === 0
    ) {
      researchCoverage = RESEARCH_COVERAGE.RESEARCHED;
      coverageRationale = `Canonical PE persist completed (${artifacts.peVenueCount} venue package(s)); hotel-specific venue partnerships linked.`;
    } else if (credible.length >= 2) {
      const withFit = credible.filter(hasHotelFitThesis).length;
      const withCohort = credible.filter(hasTravelCohortEvidence).length;
      const withContact = credible.filter(hasContactRoute).length;
      if (withFit >= 1 && (withCohort >= 1 || withContact >= 1)) {
        researchCoverage = RESEARCH_COVERAGE.RESEARCHED;
        coverageRationale = `${credible.length} credible hotel-specific prospects with fit and travel/contact evidence (not calendar-only).`;
      } else {
        researchCoverage = RESEARCH_COVERAGE.PARTIALLY_RESEARCHED;
        coverageRationale = `${credible.length} candidate rows but incomplete travel-cohort / fit / contact depth.`;
      }
    } else if (credible.length === 1) {
      const only = credible[0];
      if (hasHotelFitThesis(only) && (hasTravelCohortEvidence(only) || hasContactRoute(only))) {
        researchCoverage = RESEARCH_COVERAGE.RESEARCHED;
        coverageRationale =
          "At least one credible named prospect with hotel-fit thesis and travel/contact evidence.";
      } else {
        researchCoverage = RESEARCH_COVERAGE.PARTIALLY_RESEARCHED;
        coverageRationale =
          "Single candidate present but missing full who→why→where/when→fit→contact chain.";
      }
    } else if (partial.length > 0) {
      researchCoverage = RESEARCH_COVERAGE.PARTIALLY_RESEARCHED;
      coverageRationale = `${partial.length} weak/calendar-style row(s) without credible hotel-specific prospect depth; ${noise.length} noise row(s) excluded.`;
    } else if (
      artifacts.paidBudgetApproved === false &&
      segmentId !== RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT &&
      segmentId !== RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS
    ) {
      // Not blocked — unpaid reuse/native still allowed; mark not yet researched
      researchCoverage = RESEARCH_COVERAGE.NOT_YET_RESEARCHED;
      coverageRationale =
        "Applicable; no credible prospects yet. Paid discovery not authorized — reuse existing evidence and unpaid retrieval only.";
    } else {
      researchCoverage = RESEARCH_COVERAGE.NOT_YET_RESEARCHED;
      coverageRationale =
        "Applicable segment with no credible researched prospects in the opportunity store.";
    }

    // Elevate PE when credible PE cards exist
    if (
      segmentId === RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS &&
      credible.some((o) => o.peVenueId)
    ) {
      researchCoverage = RESEARCH_COVERAGE.RESEARCHED;
      coverageRationale = `${credible.filter((o) => o.peVenueId).length} linked venue-partnership prospect(s) via canonical PE graph.`;
    }

    const sampleRows = [...credible, ...partial].slice(0, 8).map((o) => ({
      id: o.id || o.opportunityId,
      title: o.title || o.opportunityName || null,
      organizationName: o.organizationName || o.company || null,
      opportunityType: o.opportunityType || null,
      customerFacing: isCustomerFacingOpportunity(
        { ...o, hotelId: o.hotelId || hotelId },
        { hotelId }
      ),
      hasFit: hasHotelFitThesis(o),
      hasTravelCohort: hasTravelCohortEvidence(o),
      hasContact: hasContactRoute(o),
      peVenueId: o.peVenueId || null,
    }));

    segments.push({
      segmentId,
      label: RESEARCH_SEGMENT_LABEL[segmentId],
      applicability: app.applicability,
      applicabilityRationale: app.rationale,
      researchCoverage,
      coverageRationale,
      sportsResearchNotes: app.sportsResearchNotes || null,
      counts: {
        classified: bucket.opportunities.length,
        credible: credible.length,
        partial: partial.length,
        noiseExcluded: noise.length,
      },
      sampleOpportunities: sampleRows,
      nextActions: nextActionsForSegment(
        segmentId,
        app.applicability,
        researchCoverage,
        sampleRows
      ),
      listedInConfigOnly: false,
    });
  }

  const applicable = segments.filter(
    (s) => s.applicability === APPLICABILITY.APPLICABLE
  );
  const researchedApplicable = applicable.filter(
    (s) => s.researchCoverage === RESEARCH_COVERAGE.RESEARCHED
  );
  const partialApplicable = applicable.filter(
    (s) => s.researchCoverage === RESEARCH_COVERAGE.PARTIALLY_RESEARCHED
  );
  const notYetApplicable = applicable.filter(
    (s) => s.researchCoverage === RESEARCH_COVERAGE.NOT_YET_RESEARCHED
  );

  const assocOnlyTrap =
    researchedApplicable.length === 1 &&
    researchedApplicable[0].segmentId === RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT &&
    applicable.length > 1;

  const peSeg = segments.find(
    (s) => s.segmentId === RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS
  );

  return {
    version: HOTEL_SEGMENT_RESEARCH_PLAN_VERSION,
    generatedAt: new Date().toISOString(),
    hotelId,
    hotelDisplayName: cfg.displayName || null,
    hotelProfile: {
      rooms: rooms(cfg),
      meetingRooms: meetingRooms(cfg),
      meetingSqFt: meetingSqFt(cfg),
      classification: cfg.capabilityProfile?.classification || null,
      territory: cfg.demandTerritory?.label || null,
      configTargetSegments: configListed,
      configTargetSegmentsEmpty: configListed.length === 0,
      note: configListed.length
        ? "Config lists preferred segments; coverage below is scored from opportunity evidence, not the list alone."
        : "Config targetSegments empty — applicability inferred from location, positioning, inventory, and catchment (not association-default).",
    },
    summary: {
      applicableCount: applicable.length,
      researchedCount: researchedApplicable.length,
      partiallyResearchedCount: partialApplicable.length,
      notYetResearchedCount: notYetApplicable.length,
      notApplicableCount: segments.filter(
        (s) => s.applicability === APPLICABILITY.NOT_APPLICABLE
      ).length,
      associationOnlyIsNotFullCoverage: true,
      associationOnlyTrap: assocOnlyTrap,
      associationOnlyTrapNote: assocOnlyTrap
        ? "Only associations/government are researched among applicable segments — do not treat as full hotel coverage."
        : peSeg?.researchCoverage === RESEARCH_COVERAGE.RESEARCHED &&
            researchedApplicable.some(
              (s) => s.segmentId === RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT
            )
          ? "Associations and private-event venues researched; other applicable segments still open."
          : null,
      fullCoverageClaimAllowed: false,
      fullCoverageNote:
        "Full hotel coverage requires applicable segments to be researched or explicitly not_applicable — association volume alone never qualifies.",
    },
    segments,
    invariants: [
      "Do not silently treat association research as full hotel coverage.",
      "Do not force equal prospect counts across segments.",
      "Config listing ≠ researched.",
      "Stay-to-play ≠ available hotel allocation.",
      "Private-event findings route through canonical venue/signal/hotel-fit persist.",
      "No additional paid research without approved budget.",
    ],
  };
}

export function segmentResearchPlanToCsv(plan) {
  const header = [
    "hotelId",
    "segmentId",
    "label",
    "applicability",
    "applicabilityRationale",
    "researchCoverage",
    "coverageRationale",
    "credibleCount",
    "partialCount",
    "noiseExcluded",
    "sampleIds",
  ];
  const esc = (v) => `"${String(v ?? "").replace(/"/g, '""')}"`;
  const lines = [header.join(",")];
  for (const s of plan.segments || []) {
    lines.push(
      [
        plan.hotelId,
        s.segmentId,
        esc(s.label),
        s.applicability,
        esc(s.applicabilityRationale),
        s.researchCoverage,
        esc(s.coverageRationale),
        s.counts?.credible ?? 0,
        s.counts?.partial ?? 0,
        s.counts?.noiseExcluded ?? 0,
        esc((s.sampleOpportunities || []).map((o) => o.id).join("|")),
      ].join(",")
    );
  }
  return lines.join("\n") + "\n";
}

export function segmentResearchPlanToMarkdown(plan) {
  const lines = [
    `# Segment research plan — ${plan.hotelDisplayName || plan.hotelId}`,
    "",
    `- Generated: ${plan.generatedAt}`,
    `- Version: ${plan.version}`,
    `- Rooms: ${plan.hotelProfile?.rooms} · Meeting rooms: ${plan.hotelProfile?.meetingRooms} · Territory: ${plan.hotelProfile?.territory}`,
    `- Config targetSegments: ${
      plan.hotelProfile?.configTargetSegmentsEmpty
        ? "(empty — applicability inferred)"
        : (plan.hotelProfile?.configTargetSegments || []).join("; ")
    }`,
    "",
    "## Coverage summary",
    "",
    `| Applicable | Researched | Partially | Not yet | Not applicable |`,
    `|---|---|---|---|---|`,
    `| ${plan.summary.applicableCount} | ${plan.summary.researchedCount} | ${plan.summary.partiallyResearchedCount} | ${plan.summary.notYetResearchedCount} | ${plan.summary.notApplicableCount} |`,
    "",
    plan.summary.associationOnlyTrapNote
      ? `> **Warning:** ${plan.summary.associationOnlyTrapNote}`
      : `> Association-only lists are never full coverage. ${plan.summary.fullCoverageNote}`,
    "",
    "## Segments",
    "",
    `| Segment | Applicable? | Research coverage | Credible | Partial | Notes |`,
    `|---|---|---|---|---|---|`,
  ];
  for (const s of plan.segments || []) {
    lines.push(
      `| ${s.label} | ${s.applicability} | **${s.researchCoverage}** | ${s.counts.credible} | ${s.counts.partial} | ${text(s.coverageRationale).slice(0, 140)} |`
    );
  }
  lines.push("", "## Next actions by applicable gap", "");
  for (const s of plan.segments || []) {
    if (s.applicability !== APPLICABILITY.APPLICABLE) continue;
    if (s.researchCoverage === RESEARCH_COVERAGE.RESEARCHED) continue;
    lines.push(`### ${s.label} — ${s.researchCoverage}`, "");
    for (const a of s.nextActions || []) lines.push(`- ${a}`);
    lines.push("");
  }
  return lines.join("\n") + "\n";
}
