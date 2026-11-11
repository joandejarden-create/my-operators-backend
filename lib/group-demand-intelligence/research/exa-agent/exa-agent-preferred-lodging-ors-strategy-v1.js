/**
 * Preferred Lodging Partnership — dedicated Exa Agent ORS strategy executor.
 * Does NOT route through Scaling Companies or Association Events families.
 */
import {
  buildAuthoritativeHotelResearchContext,
  formatHotelResearchContextBlock,
} from "../authoritative-hotel-research-context-v1.js";
import { buildUniversalContactResearchPromptBlock } from "../universal-contact-research-contract-v1.js";
import {
  EXA_PREFERRED_LODGING_DISCOVERY_V1,
  EXA_PREFERRED_LODGING_DEEP_V1,
  buildPreferredLodgingExaAgentCreatePayload,
} from "../../preferred-lodging-partnership/preferred-lodging-exa-payload-compat-v1.js";
import { EXA_PREFERRED_LODGING_RESEARCH_STRATEGIES } from "./gdi-segment-aware-exa-agent-routing-v1.js";
import { firstSafeDisplayText } from "../safe-display-text-v1.js";

export const EXA_PREFERRED_LODGING_ORS_STRATEGY_V1 =
  "gdi-exa-preferred-lodging-ors-strategy-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function buildPreferredLodgingDiscoveryContextBlock({
  hotel = {},
  exclusions = [],
  limit = 12,
  additionalQueryDirectives = [],
} = {}) {
  const ctx = buildAuthoritativeHotelResearchContext(hotel, {
    contextAddress: hotel.address || null,
  });
  const extra = (Array.isArray(additionalQueryDirectives)
    ? additionalQueryDirectives
    : []
  )
    .map((d) => text(d))
    .filter(Boolean);
  return [
    formatHotelResearchContextBlock(ctx),
    "",
    `Hotel ID: ${hotel.hotelId || hotel.id || "(unknown)"}`,
    `Candidate limit: ${limit}`,
    `Segment: Preferred Lodging Partnerships (dedicated family — not Scaling, not Association Events).`,
    `Investigate independent event venues that may generate overnight guest / preferred-hotel referral demand.`,
    exclusions.length
      ? `Exclude / de-duplicate known venues: ${exclusions.slice(0, 40).join("; ")}`
      : "No exclusion list supplied.",
    ...extra,
  ].join("\n");
}

export function buildPreferredLodgingDeepResearchContextBlock({
  hotel = {},
  opportunity = {},
  segmentId = null,
  segmentLabel = null,
} = {}) {
  const ctx = buildAuthoritativeHotelResearchContext(hotel, {
    contextAddress: hotel.address || null,
  });
  const venueName = firstSafeDisplayText(
    opportunity.organizationName,
    opportunity.venueName,
    opportunity.title,
    opportunity.displayTitle,
  );
  const resolvedSegmentId =
    segmentId ||
    opportunity.researchSegment ||
    opportunity.segmentId ||
    "preferred_lodging_partnerships";
  const resolvedSegmentLabel =
    segmentLabel || opportunity.segmentLabel || "Preferred Lodging Partnerships";
  return [
    formatHotelResearchContextBlock(ctx),
    "",
    `Focus venue: ${venueName || "(unnamed venue)"}`,
    `Opportunity ID: ${opportunity.id || opportunity.opportunityId || "(unset)"}`,
    `Website / seed: ${(opportunity.seedUrls || [])[0] || opportunity.website || "(unset)"}`,
    `Demand segment ID: ${resolvedSegmentId}`,
    `Agent family: Preferred Lodging Partnerships — venue preferred-hotel / overflow lodging thesis only.`,
    `Do NOT use Scaling Companies growth framing or Association Events conference housing framing.`,
    `Investigate material uncertainties for commercial qualification and named venue lodging decision-makers.`,
    buildUniversalContactResearchPromptBlock({
      segmentId: resolvedSegmentId,
      segmentLabel: resolvedSegmentLabel,
    }),
  ].join("\n");
}

export function validatePreferredLodgingDiscoveryStructured(structured = {}) {
  const issues = [];
  if (!structured || typeof structured !== "object") {
    return {
      ok: false,
      issues: [{ severity: "P0", code: "missing_structured" }],
      acceptedCandidates: [],
    };
  }
  if (structured.contractId !== EXA_PREFERRED_LODGING_DISCOVERY_V1) {
    issues.push({
      severity: "P0",
      code: "wrong_contract",
      message: structured.contractId,
    });
  }
  const candidates = Array.isArray(structured.candidates)
    ? structured.candidates
    : [];
  const acceptedCandidates = [];
  for (let i = 0; i < candidates.length; i += 1) {
    const c = candidates[i];
    const path = `candidates[${i}]`;
    let reject = false;
    if (!text(c?.venueName)) {
      issues.push({ severity: "P0", code: "venue_name_missing", path });
      reject = true;
    }
    if (!Array.isArray(c?.supportingSources) || !c.supportingSources.length) {
      issues.push({ severity: "P0", code: "sources_missing", path });
      reject = true;
    }
    if (!reject) acceptedCandidates.push(c);
  }
  const p0 = issues.filter((x) => x.severity === "P0");
  return {
    ok: p0.length === 0,
    issues,
    acceptedCandidates,
    candidateCount: candidates.length,
  };
}

export function validatePreferredLodgingDeepResearchStructured(
  structured = {},
) {
  const issues = [];
  if (!structured || typeof structured !== "object") {
    return { ok: false, issues: [{ severity: "P0", code: "missing_structured" }] };
  }
  if (structured.contractId !== EXA_PREFERRED_LODGING_DEEP_V1) {
    issues.push({
      severity: "P0",
      code: "wrong_contract",
      message: structured.contractId,
    });
  }
  if (!text(structured.venueIdentity)) {
    issues.push({ severity: "P0", code: "venue_identity_missing" });
  }
  if (
    !Array.isArray(structured.supportingSources) ||
    !structured.supportingSources.length
  ) {
    issues.push({ severity: "P0", code: "sources_missing" });
  }
  for (const s of structured.supportingSources || []) {
    if (!text(s?.claim) || !text(s?.url)) {
      issues.push({ severity: "P0", code: "claim_url_incomplete" });
    }
  }
  const p0 = issues.filter((x) => x.severity === "P0");
  return { ok: p0.length === 0, issues, structured };
}

/**
 * Map Preferred Lodging deep structured output onto opportunity fields.
 */
export function applyPreferredLodgingDeepResearchToOpportunity(
  opportunity = {},
  structured = {},
  hotel = {},
) {
  const next = { ...opportunity };
  const sources = Array.isArray(structured.supportingSources)
    ? structured.supportingSources
    : [];
  const acceptedClaims = sources.map((s, i) => ({
    evidenceId: `pref_deep_${i}`,
    url: s.url,
    statement: text(s.claim).slice(0, 400),
    accepted: true,
    provenance: "preferred_lodging_exa_deep",
  }));
  const namedStakeholders = (structured.venueContactCandidates || [])
    .filter((c) => text(c?.name))
    .map((c) => ({
      name: text(c.name),
      title: text(c.role),
      role: text(c.role),
      organization: text(c.organization || structured.venueIdentity),
      whyRelevant: text(c.whyRelevant),
      sourceUrl: c.sourceUrl || null,
      linkedinUrl: c.linkedinUrl || null,
      contactRelevanceLabel: c.relevanceClass || null,
      commercialClass: c.buyerVersusIntermediary || null,
      decisionMakerClass:
        c.relevanceClass === "CONTRACTING_AUTHORITY_VERIFIED"
          ? "VERIFIED_DECISION_MAKER"
          : c.relevanceClass === "CONTACT_COMMERCIALLY_RELEVANT"
            ? "POTENTIAL_DECISION_MAKER"
            : "RESEARCH_CANDIDATE",
    }));

  const hints = structured.cibMappingHints || {};
  if (text(hints.whatIsHappening)) next.whatIsHappening = text(hints.whatIsHappening);
  if (text(hints.whyThisHotel)) {
    next.hotelOpportunityThesis = text(hints.whyThisHotel);
    next.whyMyHotel = text(hints.whyThisHotel);
  } else if (text(structured.findings?.hotelSpecificFit)) {
    next.hotelOpportunityThesis = text(structured.findings.hotelSpecificFit);
  }
  if (text(hints.recommendedNextStep)) {
    next.recommendedNextSalesAction = text(hints.recommendedNextStep);
  } else if (text(structured.findings?.recommendedSalesAction)) {
    next.recommendedNextSalesAction = text(
      structured.findings.recommendedSalesAction,
    );
  }
  if (text(structured.findings?.onsiteLodgingStatus)) {
    next.onSiteLodgingStatus = structured.findings.onsiteLodgingStatus;
  }
  if (namedStakeholders.length) {
    next.namedStakeholders = [
      ...(Array.isArray(next.namedStakeholders) ? next.namedStakeholders : []),
      ...namedStakeholders,
    ];
    if (!text(next.primaryContact?.name) && namedStakeholders[0]) {
      next.primaryContact = {
        name: namedStakeholders[0].name,
        title: namedStakeholders[0].title,
        role: namedStakeholders[0].role,
        sourceUrl: namedStakeholders[0].sourceUrl,
      };
      next.contactName = namedStakeholders[0].name;
      next.contactRole = namedStakeholders[0].role;
    }
    if (
      !text(next.secondaryContact?.name) &&
      namedStakeholders[1] &&
      text(namedStakeholders[1].name).toLowerCase() !==
        text(namedStakeholders[0].name).toLowerCase()
    ) {
      next.secondaryContact = {
        name: namedStakeholders[1].name,
        title: namedStakeholders[1].title,
        role: namedStakeholders[1].role,
        sourceUrl: namedStakeholders[1].sourceUrl,
      };
    }
  }
  next.researchSegment = next.researchSegment || "preferred_lodging_partnerships";
  next.demandType = next.demandType || "preferred_lodging_partnerships";
  next.exaAgentDeepResearch = {
    ...(next.exaAgentDeepResearch || {}),
    contractId: EXA_PREFERRED_LODGING_DEEP_V1,
    agentFamily: "PREFERRED_LODGING",
    structured,
    hotelId: hotel.hotelId || next.hotelId || null,
    at: new Date().toISOString(),
  };
  next.unresolvedQuestions = [
    ...new Set([
      ...(next.unresolvedQuestions || []),
      ...(structured.remainingUncertainties || []),
      ...(structured.suggestedOrsFollowUps || []),
    ]),
  ];
  next.preferredLodgingDeepDisposition = structured.researchDisposition || null;
  return {
    opportunity: next,
    acceptedClaims,
    namedStakeholders,
  };
}

/**
 * Execute Preferred Lodging Exa Agent via shared provider adapter + dedicated contracts.
 */
export async function executeExaPreferredLodgingOrsStrategy({
  strategyId,
  hotel = {},
  opportunity = {},
  candidate = null,
  exclusions = [],
  authorizeExaAgentLive = false,
  mockStructured = null,
  mockStatus = "completed",
  effort = "medium",
  maxCostDollars = null,
  forceMockOnly = false,
  limit = 12,
  pollIntervalMs = 4000,
  maxPolls = 90,
  additionalQueryDirectives = [],
  segmentId = null,
  segmentLabel = null,
} = {}) {
  const focus = opportunity || candidate || {};
  const { runExaAgentResearch, quoteExaAgentRun } = await import(
    "./exa-agent-provider-adapter-v1.js"
  );

  const mode =
    strategyId ===
    EXA_PREFERRED_LODGING_RESEARCH_STRATEGIES.EXA_AGENT_PREFERRED_LODGING_DEEP_RESEARCH
      ? "deep_research"
      : "discovery";

  const contextBlock =
    mode === "discovery"
      ? buildPreferredLodgingDiscoveryContextBlock({
          hotel,
          exclusions,
          limit,
          additionalQueryDirectives,
        })
      : buildPreferredLodgingDeepResearchContextBlock({
          hotel,
          opportunity: focus,
          segmentId:
            segmentId ||
            focus.researchSegment ||
            focus.segmentId ||
            "preferred_lodging_partnerships",
          segmentLabel: segmentLabel || null,
        });

  const preview = buildPreferredLodgingExaAgentCreatePayload({
    mode,
    contextBlock,
    effort,
    maxCostDollars,
    hotel,
    venue: focus,
    exclusion: exclusions,
  });

  const quote = quoteExaAgentRun({ effort, maxCostDollars });
  const run = await runExaAgentResearch({
    mode,
    contextBlock,
    effort,
    maxCostDollars,
    authorizeExaAgentLive,
    mockStructured,
    mockStatus,
    forceMockOnly: forceMockOnly === true || mockStructured != null,
    createPayloadOverride: preview.body,
    contractIdOverride: preview.meta.contractId,
    pollIntervalMs,
    maxPolls,
  });

  if (!run.ok) {
    return {
      version: EXA_PREFERRED_LODGING_ORS_STRATEGY_V1,
      strategyId:
        strategyId ||
        (mode === "discovery"
          ? EXA_PREFERRED_LODGING_RESEARCH_STRATEGIES.EXA_AGENT_PREFERRED_LODGING_DISCOVERY
          : EXA_PREFERRED_LODGING_RESEARCH_STRATEGIES.EXA_AGENT_PREFERRED_LODGING_DEEP_RESEARCH),
      mode,
      quote,
      run,
      validation: null,
      opportunity: focus,
      requestPayloadMeta: {
        contractId: preview.meta.contractId,
        queryChars: String(preview.body?.query || "").length,
        hasOutputSchema: Boolean(preview.body?.outputSchema),
        agentFamily: "PREFERRED_LODGING",
      },
      continueResearch: true,
    };
  }

  if (mode === "discovery") {
    const validation = validatePreferredLodgingDiscoveryStructured(
      run.structured,
    );
    return {
      version: EXA_PREFERRED_LODGING_ORS_STRATEGY_V1,
      strategyId:
        EXA_PREFERRED_LODGING_RESEARCH_STRATEGIES.EXA_AGENT_PREFERRED_LODGING_DISCOVERY,
      mode,
      quote,
      run,
      validation,
      discoveryCandidates: validation.acceptedCandidates || [],
      opportunity: focus,
      requestPayloadMeta: {
        contractId: preview.meta.contractId,
        queryChars: String(preview.body?.query || "").length,
        hasOutputSchema: Boolean(preview.body?.outputSchema),
        agentFamily: "PREFERRED_LODGING",
      },
      continueResearch: true,
    };
  }

  const validation = validatePreferredLodgingDeepResearchStructured(
    run.structured,
  );
  let nextOpp = focus;
  if (validation.ok) {
    const applied = applyPreferredLodgingDeepResearchToOpportunity(
      focus,
      run.structured,
      hotel,
    );
    nextOpp = applied.opportunity;
    nextOpp = {
      ...nextOpp,
      evidence: [
        ...(nextOpp.evidence || []),
        ...applied.acceptedClaims.map((c) => ({
          evidenceId: c.evidenceId,
          url: c.url,
          statement: c.statement,
          accepted: true,
          provenance: c.provenance,
        })),
      ],
      acceptedClaims: [
        ...(nextOpp.acceptedClaims || []),
        ...applied.acceptedClaims,
      ],
    };
  }

  return {
    version: EXA_PREFERRED_LODGING_ORS_STRATEGY_V1,
    strategyId:
      EXA_PREFERRED_LODGING_RESEARCH_STRATEGIES.EXA_AGENT_PREFERRED_LODGING_DEEP_RESEARCH,
    mode,
    quote,
    run,
    validation,
    deepResearch: run.structured,
    opportunity: nextOpp,
    requestPayloadMeta: {
      contractId: preview.meta.contractId,
      queryChars: String(preview.body?.query || "").length,
      hasOutputSchema: Boolean(preview.body?.outputSchema),
      agentFamily: "PREFERRED_LODGING",
    },
    continueResearch: Boolean(run.structured?.suggestedOrsFollowUps?.length),
  };
}
