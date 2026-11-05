/**
 * Map validated Exa Agent deep-research dossier → fields consumable by
 * applyScalingCommercialIntelligenceBrief / opportunity maturation.
 *
 * Exa output is never the customer drawer. Dealality regenerates CIB.
 */
import { createHash } from "node:crypto";
import { applyScalingCommercialIntelligenceBrief } from "../../scaling-commercial-intelligence-brief-v1.js";
import {
  assessBuyerOrgAndAccessibility,
  applyBuyerOrgAccessibilityToOpportunity,
} from "../commercial-accessibility-and-org-route-v1.js";
import {
  mapDossierEvidenceToOpportunityFields,
  certifyClaimSourceLinkage,
} from "./exa-agent-source-evidence-v1.js";
import {
  evaluateContactIntelligenceFromDossier,
  applyContactIntelligenceToOpportunity,
} from "../scaling-contact-intelligence-v1.js";

export const EXA_AGENT_TO_CIB_MAPPING_V1 = "gdi-scaling-exa-agent-to-cib-mapping-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Merge validated deep dossier into opportunity research fields (non-destructive
 * to stronger named contacts / verified flags unless dossier is stronger).
 */
export function mapDeepResearchDossierToOpportunityFields(
  opportunity = {},
  dossier = {},
  opts = {},
) {
  let next = { ...opportunity };
  const org =
    text(dossier.companyIdentity?.legalOrTradingName) ||
    text(next.organizationName) ||
    "Company";

  next.organizationName = org;
  next.exaAgentDeepResearch = {
    version: EXA_AGENT_TO_CIB_MAPPING_V1,
    at: new Date().toISOString(),
    researchDisposition: dossier.researchDisposition || null,
    contractIngest: true,
  };

  if (text(dossier.activityAndChronology?.summary)) {
    next.whatIsHappening = text(dossier.activityAndChronology.summary);
    next.activityHeadline = text(dossier.activityAndChronology.summary).slice(0, 180);
  }
  if (dossier.activityAndChronology?.activityDate) {
    next.growthEvidenceDate = dossier.activityAndChronology.activityDate;
    next.originalActivityDate = dossier.activityAndChronology.activityDate;
  }
  if (dossier.activityAndChronology?.geography) {
    next.activityLocation = dossier.activityAndChronology.geography;
    next.localConnection = [
      text(next.localConnection),
      `Activity geography: ${dossier.activityAndChronology.geography}`,
    ]
      .filter(Boolean)
      .join(" ");
  }

  const demand = text(dossier.hotelDemandHypotheses?.primaryMechanism);
  if (demand) {
    next.potentialHotelBusiness = demand;
    next.proposedHotelUse = demand;
  }

  if (text(dossier.hotelSpecificFit?.whyThisHotel)) {
    // Prefer property-specific over city_only when provided
    if (
      dossier.hotelSpecificFit.specificity === "property_specific" ||
      !text(next.whyThisHotel) ||
      opts.preferAgentFit === true
    ) {
      next.whyThisHotel = text(dossier.hotelSpecificFit.whyThisHotel);
      next.hotelFitRationale = next.whyThisHotel;
    }
  }

  if (text(dossier.commercialTiming?.rationale)) {
    next.whyEstablishContactNow = text(dossier.commercialTiming.rationale);
    next.whyNow = next.whyEstablishContactNow;
  }

  // Claim→source→drawer evidence mapping (deduped; provider URLs only)
  const evidenceMapped = mapDossierEvidenceToOpportunityFields(next, dossier, {
    hotelName: opts.hotelName || opts.hotelDisplayName,
    retrievedAt: opts.retrievedAt,
    verificationDate: opts.verificationDate,
  });
  next = evidenceMapped.opportunity;
  next.exaAgentEvidenceCertification = certifyClaimSourceLinkage(
    evidenceMapped.evidencePack,
  );

  // Primary + secondary CONTACT_RELEVANT people (Stage D); never invent channels
  const contactPack = evaluateContactIntelligenceFromDossier(dossier, {
    hotelName: opts.hotelName || opts.hotelDisplayName,
    companyName: org,
    verificationDate: opts.verificationDate,
  });
  next = applyContactIntelligenceToOpportunity(next, contactPack);
  // Do not auto-flip hasVerifiedNamedRelevantContact for influencers —
  // applyContactIntelligenceToOpportunity only sets it for CONTRACTING_AUTHORITY_VERIFIED

  const route = (dossier.organizationalContactRoutes || []).find(
    (r) => r.isTargetCompanyRoute && r.url,
  );
  if (route?.url && !next.organizationRouteUrl) {
    next.organizationRouteUrl = route.url;
    next.contactUrl = next.contactUrl || route.url;
  }

  next.unknowns = [
    ...(Array.isArray(next.unknowns) ? next.unknowns : []),
    ...(dossier.materialUncertainties || []),
  ].filter(Boolean);

  next.unresolvedResearchTasks = dossier.unresolvedResearchTasks || [];
  next.exaAgentClaimToSourceMap =
    next.exaAgentClaimToSourceMap || dossier.claimToSourceMap || [];
  next.exaAgentResearchDisposition = dossier.researchDisposition || null;

  if (text(dossier.recommendedCommercialAction?.nextAction)) {
    next.notes = [
      ...(next.notes || []),
      `exa_agent_recommended_action:${dossier.recommendedCommercialAction.nextAction}`,
    ];
  }

  return next;
}

/**
 * Stable opportunity id for scaling deep results (hotel-generic).
 * Prevents unstaged Sales Ready rows when mapper omits id (YOTEL V23 TMC defect).
 */
export function ensureScalingOpportunityId(opportunity = {}, hotel = {}) {
  const next = { ...opportunity };
  if (text(next.id) || text(next.opportunityId)) {
    if (!text(next.id) && text(next.opportunityId)) next.id = text(next.opportunityId);
    return next;
  }
  const hotelId = text(hotel.hotelId || hotel.id || next.hotelId) || "hotel";
  const org = text(next.organizationName || next.companyName) || "org";
  const title = text(next.title || next.programmeName || next.whatIsHappening || org);
  next.id = `gdi_sc_${createHash("sha256")
    .update(`${hotelId}|${org}|${title}`)
    .digest("hex")
    .slice(0, 12)}`;
  next.opportunityId = next.id;
  return next;
}

/**
 * Full shadow path: dossier → opportunity fields → Scaling CIB → corporate org-route.
 */
export function applyValidatedDeepResearchToScalingBrief(
  opportunity = {},
  dossier = {},
  hotel = {},
  opts = {},
) {
  let next = mapDeepResearchDossierToOpportunityFields(opportunity, dossier, {
    ...opts,
    hotelName: hotel.displayName || hotel.name || opts.hotelName,
    hotelDisplayName: hotel.displayName || hotel.name || opts.hotelDisplayName,
  });
  next = ensureScalingOpportunityId(next, hotel);
  next = applyScalingCommercialIntelligenceBrief(next, hotel, opts);
  // Preserve claim-linked evidence on CIB sources (CIB mapper may thin fields)
  if (next.commercialIntelligenceBrief?.evidenceAndUncertainty && next.sources?.length) {
    next.commercialIntelligenceBrief.evidenceAndUncertainty.sources = (
      next.sources || []
    )
      .map((s) => ({
        url: s.url,
        title: s.title || null,
        publisher: s.publisher || null,
        publishedDate: s.publishedDate || s.publicationDate || null,
        establishes: s.establishes || null,
        evidenceGroup: s.evidenceGroup || null,
        evidenceGroupLabel: s.evidenceGroupLabel || null,
        sourceId: s.sourceId || null,
      }))
      .filter((s) => s.url)
      .slice(0, 20);
  }
  const assessment = assessBuyerOrgAndAccessibility(next, {
    hotelName: hotel.displayName || hotel.name,
  });
  next = applyBuyerOrgAccessibilityToOpportunity(next, assessment);
  return {
    opportunity: next,
    assessment,
    mappingVersion: EXA_AGENT_TO_CIB_MAPPING_V1,
  };
}
