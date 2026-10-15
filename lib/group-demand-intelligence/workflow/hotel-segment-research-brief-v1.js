/**
 * Hotel × demand-segment research brief V1.
 *
 * One focused brief per applicable segment — not a mixed “find hotel
 * opportunities” request. Research segment stays separate from hotel use case.
 *
 * Briefs are generated from hotel profile + segment plan + known gaps.
 * They do not auto-submit paid providers.
 */

import {
  RESEARCH_SEGMENT,
  RESEARCH_SEGMENT_LABEL,
  APPLICABILITY,
  loadHotelConfig,
  buildHotelSegmentResearchPlan,
} from "./hotel-segment-research-plan-v1.js";
import {
  DEMAND_USE_CASE,
  DEMAND_USE_CASE_LABEL,
} from "./shared-demand-model-v1.js";
import {
  LMC_SUBSEGMENT,
  LMC_SUBSEGMENT_LABEL,
  buildLocalMeetingsCateringCoverageSnapshot,
  buildLocalMeetingsCateringResearchQueries,
} from "./local-meetings-catering-research-v1.js";

export const HOTEL_SEGMENT_RESEARCH_BRIEF_VERSION =
  "gdi-hotel-segment-research-brief-v1";

/** Default use cases by research segment (orthogonal; may be overridden). */
export const DEFAULT_USE_CASES_BY_SEGMENT = Object.freeze({
  [RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT]: [
    DEMAND_USE_CASE.GROUP_LODGING,
    DEMAND_USE_CASE.ROOMS_PLUS_MEETINGS,
  ],
  [RESEARCH_SEGMENT.SPORTS]: [DEMAND_USE_CASE.GROUP_LODGING],
  [RESEARCH_SEGMENT.CORPORATE_MEETINGS_TRAINING]: [
    DEMAND_USE_CASE.ROOMS_PLUS_MEETINGS,
    DEMAND_USE_CASE.MEETINGS_AND_CATERING,
  ],
  [RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING]: [
    DEMAND_USE_CASE.MEETINGS_AND_CATERING,
  ],
  [RESEARCH_SEGMENT.SCALING_COMPANIES]: [
    DEMAND_USE_CASE.ROOMS_PLUS_MEETINGS,
    DEMAND_USE_CASE.MEETINGS_AND_CATERING,
    DEMAND_USE_CASE.GROUP_LODGING,
  ],
  [RESEARCH_SEGMENT.MEDICAL_ACADEMIC]: [
    DEMAND_USE_CASE.GROUP_LODGING,
    DEMAND_USE_CASE.ROOMS_PLUS_MEETINGS,
  ],
  [RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS]: [
    DEMAND_USE_CASE.VENUE_REFERRAL_PARTNERSHIP,
    DEMAND_USE_CASE.GROUP_LODGING,
  ],
  [RESEARCH_SEGMENT.ENTERTAINMENT_PRODUCTION]: [
    DEMAND_USE_CASE.GROUP_LODGING,
  ],
  [RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS]: [
    DEMAND_USE_CASE.GROUP_LODGING,
  ],
  [RESEARCH_SEGMENT.TOURS_EDUCATIONAL_SPECIALIST]: [
    DEMAND_USE_CASE.GROUP_LODGING,
  ],
});

const SEGMENT_BUYER_ROLES = Object.freeze({
  [RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT]: [
    "meetings_housing_staff",
    "contracted_planner",
    "chapter_admin",
  ],
  [RESEARCH_SEGMENT.SPORTS]: [
    "team_housing_organizer",
    "tournament_director",
    "housing_agency",
  ],
  [RESEARCH_SEGMENT.CORPORATE_MEETINGS_TRAINING]: [
    "travel_workplace_ops",
    "people_ops_training",
    "executive_assistant",
  ],
  [RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING]: [
    "events_catering_buyer",
    "chapter_admin",
    "program_coordinator",
    "office_operations",
    "executive_support",
    "training_coordinator",
    "fundraising_coordinator",
    "professional_event_planner",
  ],
  [RESEARCH_SEGMENT.SCALING_COMPANIES]: [
    "people_ops_training",
    "travel_workplace_ops",
    "executive_assistant",
    "office_manager",
  ],
  [RESEARCH_SEGMENT.MEDICAL_ACADEMIC]: [
    "meetings_housing_staff",
    "contracted_planner",
  ],
  [RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS]: [
    "private_events_manager",
    "venue_sales",
  ],
  [RESEARCH_SEGMENT.ENTERTAINMENT_PRODUCTION]: [
    "production_coordinator",
    "location_manager",
  ],
  [RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS]: [
    "exhibit_manager",
    "travel_coordinator",
  ],
  [RESEARCH_SEGMENT.TOURS_EDUCATIONAL_SPECIALIST]: [
    "tour_operator",
    "group_travel_coordinator",
  ],
});

const SEGMENT_SOURCES = Object.freeze({
  [RESEARCH_SEGMENT.SCALING_COMPANIES]: [
    "company career / about / locations pages",
    "dated expansion / funding / hiring announcements with local ops",
    "local office / coworking / lease notices",
    "public professional profiles for workplace/ops contacts",
  ],
  [RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING]: [
    "chapter event calendars and awards dinners",
    "private dining / club function notices",
    "organization contact / events pages",
    "corporate training / board / workshop notices",
    "nonprofit fundraiser / gala calendars",
    "professional planner portfolios (public business pages only)",
    "public organizational event-coordinator listings",
  ],
  [RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS]: [
    "venue official sites and event menus",
    "management-agency listings",
    "wedding / social calendars (activity evidence only)",
  ],
  [RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT]: [
    "association annual meeting / housing pages",
    "government / NGO program calendars with travel lodging",
  ],
  [RESEARCH_SEGMENT.SPORTS]: [
    "tournament housing pages",
    "visiting-team / organizer notices",
    "verified venue geography",
  ],
});

const SEGMENT_QUALIFICATION = Object.freeze({
  [RESEARCH_SEGMENT.SCALING_COMPANIES]: [
    "Verified company identity",
    "Relevant local presence (HQ outside market OK if local ops verified)",
    "Dated growth/expansion evidence",
    "Supported activity creating plausible hotel use",
    "Independent hotel fit",
    "Usable relevant contact route before publication",
    "Funding, hiring pages, directories, or proximity alone are insufficient",
  ],
  [RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING]: [
    "Correct account identity and local connection",
    "Evidence of relevant event activity or recurring organizing role",
    "Potential need for hotel meeting space, catering, or both",
    "Why this hotel's verified facilities and positioning could fit",
    "Timing if known, or clearly labeled standing relationship",
    "Relevant usable contact route (named person preferred; org fallback OK)",
    "Concrete next sales question",
    "Guestrooms / housing control / public RFP / growth evidence NOT required",
    "guestroomStatus=NOT_REQUIRED when rooms unnecessary — do not penalize",
    "Do not invent dates, attendance, budgets, kosher capability, or buying authority",
    "Existing named host ≠ open dated opportunity",
    "Directory listing alone is discovery lead, not qualification",
  ],
  [RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS]: [
    "Actual venue identity",
    "Activity evidence",
    "Independent hotel fit (complementary / competing / referral / unsuitable)",
    "Relevant contact",
    "Supported relationship proposition",
    "Nearby venue marketing alone ≠ traveling guests",
    "On-site lodging is not a blanket rejection",
    "Do not invent preferred-partner vacancies or confirmed overflow",
  ],
  [RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT]: [
    "Travel demand evidenced (local participation alone insufficient)",
    "Hotel fit for lodging and/or meetings",
    "Usable contact route",
  ],
  [RESEARCH_SEGMENT.SPORTS]: [
    "Visiting travel demand (not local participation only)",
    "Verified venue geography",
    "Stay-to-play / housing provider ≠ available room allocation",
  ],
});

const SEGMENT_REJECTION = Object.freeze({
  [RESEARCH_SEGMENT.SCALING_COMPANIES]: [
    "Wrong market with no verified local operations",
    "Funding-only or directory-only hits",
    "No plausible hotel use beyond proximity",
    "Identity collision with already-published standing account (enrich instead)",
  ],
  [RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING]: [
    "Venue-as-buyer without catering buyer evidence",
    "Preferred-lodging outside-venue partnership (route to Private Events)",
    "Directory-only listings",
    "No local connection or facility fit",
    "Private family / children's details / religion inference",
    "Subsegment not applicable for this hotel's inventory",
    "Planner treated as confirmed buyer without authority evidence",
  ],
  [RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS]: [
    "Marketing-only venue with no activity evidence",
    "Duplicate venue identity collision",
    "Unsuitable competing relationship without complementary thesis",
  ],
  [RESEARCH_SEGMENT.SPORTS]: [
    "Local participation calendars without travel lodging",
    "Stay-to-play claims without allocation evidence",
  ],
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function hotelIdentityBlock(cfg = {}) {
  return {
    hotelId: cfg.hotelId || null,
    displayName: cfg.displayName || null,
    city: cfg.city || null,
    country: cfg.country || null,
    geo: cfg.geo || null,
    aliases: cfg.aliases || null,
    capabilityProfile: {
      totalGuestrooms: cfg.capabilityProfile?.totalGuestrooms ?? null,
      meetingRoomCount:
        cfg.capabilityProfile?.meetingRoomCount ??
        cfg.capabilityProfile?.meetingRoomsIndoor ??
        null,
      totalMeetingSpaceSqFt:
        cfg.capabilityProfile?.totalMeetingSpaceSqFt ?? null,
      classification: cfg.capabilityProfile?.classification ?? null,
      serviceLevel: cfg.capabilityProfile?.serviceLevel ?? null,
    },
    demandTerritory: cfg.demandTerritory || null,
    commercialNotes: cfg.commercialPriorities?.notes || null,
  };
}

function catchmentFromConfig(cfg = {}) {
  const t = cfg.demandTerritory || {};
  return {
    label: t.label || null,
    includes: t.includes || [],
    evaluationQuestion: t.evaluationQuestion || null,
  };
}

/**
 * Build one focused research brief for hotel × segment.
 */
export function buildHotelSegmentResearchBrief({
  hotelId,
  segmentId,
  hotelConfig = null,
  segmentPlanRow = null,
  existingOrganizationNames = [],
  existingEvidenceNotes = [],
  specificGaps = [],
  costAllocationUsd = null,
  effortBound = null,
  briefVersion = HOTEL_SEGMENT_RESEARCH_BRIEF_VERSION,
} = {}) {
  const cfg = hotelConfig || loadHotelConfig(hotelId) || {};
  const useCases = DEFAULT_USE_CASES_BY_SEGMENT[segmentId] || [
    DEMAND_USE_CASE.GROUP_LODGING,
  ];
  const applicability =
    segmentPlanRow?.applicability ||
    APPLICABILITY.APPLICABLE;
  const coverage = segmentPlanRow?.researchCoverage || null;

  const objective =
    segmentId === RESEARCH_SEGMENT.SCALING_COMPANIES
      ? `Discover and qualify scaling / emerging corporate accounts with verified local presence and dated growth evidence that could use ${cfg.displayName || hotelId} for meetings, lodging, or catering — without inventing volumes or buying authority.`
      : segmentId === RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING
        ? `Find named local meetings/catering buyers and professional planners with evidenced functions that fit ${cfg.displayName || hotelId} verified facility inventory across applicable LMC subsegments. Guestrooms not required. Preferred lodging with outside venues stays on Private Events.`
        : segmentId === RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS
          ? `Assess actual nearby venues for complementary lodging/referral relationships with ${cfg.displayName || hotelId}. Venue marketing alone is insufficient.`
          : `Research ${RESEARCH_SEGMENT_LABEL[segmentId] || segmentId} demand applicable to ${cfg.displayName || hotelId} with travel/use evidence, hotel fit, and usable contact.`;

  const avoid = [
    ...existingOrganizationNames.map((n) => `Already assessed/published: ${n}`),
    ...existingEvidenceNotes,
  ].filter(Boolean);

  const lmcCoverage =
    segmentId === RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING
      ? buildLocalMeetingsCateringCoverageSnapshot(cfg)
      : null;
  const lmcQueries =
    segmentId === RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING
      ? buildLocalMeetingsCateringResearchQueries(cfg, {
          subsegmentIds: segmentPlanRow?.applicableLmcSubsegmentIds || null,
        })
      : null;

  return {
    version: briefVersion,
    briefId: `brief_${hotelId}_${segmentId}_v1`,
    hotel: hotelIdentityBlock(cfg),
    segment: {
      segmentId,
      label: RESEARCH_SEGMENT_LABEL[segmentId] || segmentId,
      applicability,
      coverageStatus: coverage,
      note: "Research segment is orthogonal to hotel use case",
      ...(lmcCoverage
        ? {
            lmcSubsegments: Object.values(LMC_SUBSEGMENT).map((id) => ({
              id,
              label: LMC_SUBSEGMENT_LABEL[id],
              applicable: Boolean(lmcCoverage.subsegments.find((s) => s.subsegmentId === id)?.applicable),
            })),
            applicableLmcSubsegmentIds: lmcCoverage.applicableSubsegmentIds,
            lmcConstraints: lmcCoverage.constraints,
            pePathNote: lmcCoverage.pePathNote,
          }
        : {}),
    },
    useCases: useCases.map((u) => ({
      id: u,
      label: DEMAND_USE_CASE_LABEL[u] || u,
    })),
    objective,
    geographicCatchment: catchmentFromConfig(cfg),
    sourcesAndCategories: SEGMENT_SOURCES[segmentId] || [
      "Official organization / venue pages",
      "Credible public corroboration",
      "Existing canonical GDI intelligence first",
    ],
    relevantBuyerRoles: SEGMENT_BUYER_ROLES[segmentId] || [
      "relevant_functional_contact",
    ],
    qualificationRequirements:
      SEGMENT_QUALIFICATION[segmentId] ||
      SEGMENT_QUALIFICATION[RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT],
    rejectionRequirements:
      SEGMENT_REJECTION[segmentId] || [
        "Wrong market",
        "Unsupported commercial thesis",
        "Duplicate of published standing account without new occasion evidence",
      ],
    existingEvidenceToAvoidDuplicating: avoid,
    specificGaps: specificGaps.length
      ? specificGaps
      : segmentPlanRow?.nextActions || [],
    ...(lmcQueries ? { researchQueries: lmcQueries } : {}),
    ...(lmcCoverage ? { lmcCoverageSnapshot: lmcCoverage } : {}),
    structuredOutput: {
      candidates: [
        "organizationOrVenueIdentity",
        "lmcSubsegment",
        "relationshipGrain",
        "localPresenceEvidence",
        "datedActivityOrRecurringOrganizingEvidence",
        "proposedHotelUseCase",
        "hotelFitRationaleTiedToVerifiedFacilities",
        "constraintUnknowns",
        "factsVsInference",
        "contactRouteNamedOrFallback",
        "firstSalesQuestion",
        "disposition",
        "peCrossReferenceIfRelated",
      ],
      note: "Separate verified facts from hotel-use hypothesis. Do not invent room nights, budgets, kosher capability, or buying authority. Combine related use cases on one card.",
    },
    boundedEffort: {
      costAllocationUsd,
      maxPaidTasks: effortBound?.maxPaidTasks ?? 1,
      stopCriteria:
        effortBound?.stopCriteria ||
        "Stop after declared max paid tasks or when declared scope is filled; do not force minimum yield or equal subsegment quotas; do not spend to exhaust cap.",
      modeDefault: "reuse_prior_then_bounded_fresh",
      budgetNote:
        segmentId === RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING
          ? "Include LMC subsegment expansion within the existing three-hotel consolidated paid-research proposal — do not silently increase budget or strip other applicable segments."
          : null,
    },
    briefVersion,
    generatedAt: new Date().toISOString(),
  };
}

/**
 * Build briefs for all applicable segments (or a selected subset).
 * Does not allocate equal quotas — callers select which briefs to execute.
 */
export function buildHotelSegmentResearchBriefs({
  hotelId,
  hotelConfig = null,
  opportunities = [],
  segmentIds = null,
  existingOrganizationNamesBySegment = {},
  costAllocationBySegment = {},
  artifacts = {},
} = {}) {
  const cfg = hotelConfig || loadHotelConfig(hotelId) || {};
  const plan = buildHotelSegmentResearchPlan({
    hotelId,
    hotelConfig: cfg,
    opportunities,
    artifacts,
  });
  const wanted = segmentIds
    ? new Set(segmentIds)
    : null;

  const briefs = [];
  for (const row of plan.segments || []) {
    if (row.applicability !== APPLICABILITY.APPLICABLE) continue;
    if (wanted && !wanted.has(row.segmentId)) continue;
    briefs.push(
      buildHotelSegmentResearchBrief({
        hotelId,
        segmentId: row.segmentId,
        hotelConfig: cfg,
        segmentPlanRow: row,
        existingOrganizationNames:
          existingOrganizationNamesBySegment[row.segmentId] ||
          (row.sampleRows || [])
            .map((s) => s.organizationName)
            .filter(Boolean),
        specificGaps: row.nextActions || [],
        costAllocationUsd: costAllocationBySegment[row.segmentId] ?? null,
      })
    );
  }

  return {
    version: HOTEL_SEGMENT_RESEARCH_BRIEF_VERSION,
    hotelId,
    generatedAt: new Date().toISOString(),
    planVersion: plan.version,
    applicableSegmentCount: briefs.length,
    note: "Separate logical briefs do not require tiny provider requests — batch by pricing/quality/shared-source reuse. Do not run all segments every time.",
    briefs,
    planSummary: {
      segments: (plan.segments || []).map((s) => ({
        segmentId: s.segmentId,
        applicability: s.applicability,
        researchCoverage: s.researchCoverage,
        credibleCount: s.counts?.credible ?? null,
      })),
    },
  };
}

/**
 * Select continuation-priority briefs (scaling + contact) without PE expansion.
 */
export function selectContinuationPriorityBriefs(briefPack, opts = {}) {
  const priority = opts.prioritySegmentIds || [
    RESEARCH_SEGMENT.SCALING_COMPANIES,
    RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING,
  ];
  const avoidPe = opts.avoidPeExpansion !== false;
  return {
    ...briefPack,
    selectionNote:
      "Continuation priority: scaling discovery + buyer/contact; avoid additional PE unless critical.",
    selectedBriefs: (briefPack.briefs || []).filter((b) => {
      if (
        avoidPe &&
        b.segment?.segmentId ===
          RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS
      ) {
        return false;
      }
      return priority.includes(b.segment?.segmentId);
    }),
  };
}

export function briefToPromptFocus(brief) {
  if (!brief) return null;
  const hotel = brief.hotel?.displayName || brief.hotel?.hotelId;
  const seg = brief.segment?.label || brief.segment?.segmentId;
  const avoid = (brief.existingEvidenceToAvoidDuplicating || [])
    .slice(0, 8)
    .join("; ");
  const gaps = (brief.specificGaps || []).slice(0, 5).join("; ");
  return [
    `BRIEF ${brief.briefId} (${brief.briefVersion})`,
    `Hotel: ${hotel}`,
    `Segment: ${seg} (use cases: ${(brief.useCases || []).map((u) => u.label).join(", ")})`,
    `Objective: ${brief.objective}`,
    `Catchment: ${brief.geographicCatchment?.label || "hotel demand territory"}`,
    avoid ? `Avoid duplicating: ${avoid}` : null,
    gaps ? `Gaps: ${gaps}` : null,
    `Stop: ${brief.boundedEffort?.stopCriteria}`,
  ]
    .filter(Boolean)
    .join("\n");
}

export { text };
