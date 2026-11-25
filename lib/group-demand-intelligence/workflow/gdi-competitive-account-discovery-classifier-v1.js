/**
 * GDI Competitive Account Discovery classifier V1 — Phase 3A PR-A.
 *
 * Deterministic classification into:
 *   HISTORICAL_COMPETITIVE_INTEL | EMERGING_COMPETITIVE_OPPORTUNITY | FQO_CANDIDATE
 *
 * Reuses competitor-commitment mapping. Never invents sourcing dates or
 * incumbent contracts. FQO_CANDIDATE does not mean FQO PASS / persistence auth.
 *
 * Auth: FOUNDER_GO AUTHORIZE_GDI_CAD_PHASE_3A_PR_A
 */
import {
  extractCompetitorHotelFromResearchEvidence,
  isEventVenueOnlyMention,
  COMPETITOR_COMMITMENT_SCOPE,
} from "../commercial-consistency/gdi-research-competitor-commitment-mapping-v1.js";
import { looksLikePersonName } from "../secondary-named-contact-v1.js";
import {
  GDI_COMPETITIVE_ACCOUNT_DISCOVERY_CONTRACT_V1,
  CAD_CLASS,
  CAD_ATTAINABILITY,
  CAD_INCUMBENT_RELATIONSHIP,
  CAD_SOURCING_WINDOW_STATE,
  CAD_CONFIDENCE,
  buildCadProgramKey,
  emptyCompetitiveAccountDiscovery,
  sanitizeSourcingWindow,
  validateCompetitiveAccountDiscovery,
  collectCadEvidenceUrls,
  cadText as text,
} from "./gdi-competitive-account-discovery-contract-v1.js";

export const GDI_COMPETITIVE_ACCOUNT_DISCOVERY_CLASSIFIER_V1 =
  "gdi-competitive-account-discovery-classifier-v1";

function upper(v) {
  return text(v).toUpperCase();
}

function collectOppEvidenceUrls(opp = {}) {
  const urls = [];
  for (const u of [
    ...(Array.isArray(opp.sourceUrls) ? opp.sourceUrls : []),
    ...(Array.isArray(opp.seedUrls) ? opp.seedUrls : []),
    ...(Array.isArray(opp.additionalEvidenceUrls)
      ? opp.additionalEvidenceUrls
      : []),
  ]) {
    urls.push(u);
  }
  for (const e of Array.isArray(opp.evidence) ? opp.evidence : []) {
    urls.push(e?.url || e?.sourceUrl);
  }
  for (const s of Array.isArray(opp.sources) ? opp.sources : []) {
    urls.push(typeof s === "string" ? s : s?.url || s?.sourceUrl);
  }
  urls.push(opp.sourceUrl, opp.lodgingEvidenceUrl, opp.locationEvidenceUrl);
  urls.push(opp.primaryContact?.sourceUrl);
  return collectCadEvidenceUrls(urls);
}

function researchViewFromOpportunity(opp = {}) {
  return {
    competitorHotel:
      opp.competitiveContext?.incumbentHotel ||
      opp.incumbentHotel ||
      opp.competitorHotel ||
      opp.likelyCompetitor ||
      null,
    hostHotel: opp.hostHotel || opp.historicalHostHotel || null,
    venue: opp.venue || opp.eventVenue || null,
    eventVenue: opp.eventVenue || opp.venue || null,
    hotelSourcingDecisionEvidence:
      opp.hotelSourcingDecisionEvidence ||
      opp.competitiveContext?.evidenceSummary ||
      "",
    competitorBookingClaim: opp.competitorBookingClaim || "",
    lodgingEvidence:
      opp.lodgingEvidence ||
      text(opp.whyThisHotel || "") +
        " " +
        text(opp.whatIsHappening || "") +
        " " +
        text(opp.potentialHotelBusiness || ""),
    lodgingEvidenceUrl: opp.lodgingEvidenceUrl || null,
    sourceUrl: opp.sourceUrl || null,
    sources: opp.sources || opp.sourceUrls || [],
    overflowVerified:
      opp.overflowVerified === true ||
      /OVERFLOW/i.test(
        text(
          opp.competitiveContext?.pathway ||
            opp.opportunityPathway ||
            opp.sourcingPathway ||
            "",
        ),
      ),
    targetHotelSourcingStatus: opp.targetHotelSourcingStatus || null,
  };
}

function hasFutureOrOverflowPursuit(opp = {}, research = {}) {
  const pathway = upper(
    opp.competitiveContext?.pathway ||
      opp.opportunityPathway ||
      opp.sourcingPathway ||
      opp.competitiveStatusLabel ||
      "",
  );
  if (/OVERFLOW|FUTURE_CYCLE|NEXT_CYCLE|OPEN_HOUSING|OPEN_SOURCING|RECURRING/.test(pathway)) {
    return { ok: true, basis: "PATHWAY_LABEL", pathway };
  }
  if (opp.futureDemandPathway === true || research.overflowVerified === true) {
    return { ok: true, basis: "FUTURE_OR_OVERFLOW_FLAG" };
  }
  const blob = `${text(opp.whyThisHotel)} ${text(opp.whatIsHappening)} ${text(opp.recommendedNextStep)}`;
  if (/\b(overflow|next cycle|future cycle|recurring|sourcing window)\b/i.test(blob)) {
    return { ok: true, basis: "NARRATIVE_RECURRENCE_OR_OVERFLOW" };
  }
  const start = Date.parse(opp.eventStartDate || opp.tileEventStartDate || "");
  if (Number.isFinite(start) && start >= Date.now() - 7 * 86400000) {
    return { ok: true, basis: "FUTURE_DATED_EVENT" };
  }
  return { ok: false, basis: null };
}

function resolveAttainability(opp = {}, pursuit = {}, incumbentRelationship) {
  const pathway = upper(
    opp.competitiveContext?.pathway ||
      opp.opportunityPathway ||
      opp.sourcingPathway ||
      "",
  );
  if (/UNATTAINABLE|NO_PATH|LOCKED_EXCLUSIVE/i.test(pathway)) {
    return {
      attainability: CAD_ATTAINABILITY.UNATTAINABLE,
      confidence: CAD_CONFIDENCE.MEDIUM,
    };
  }
  if (/OVERFLOW/i.test(pathway) || pursuit.basis === "FUTURE_OR_OVERFLOW_FLAG") {
    return {
      attainability: CAD_ATTAINABILITY.OVERFLOW_ONLY,
      confidence: CAD_CONFIDENCE.MEDIUM,
    };
  }
  if (
    pursuit.ok &&
    (incumbentRelationship === CAD_INCUMBENT_RELATIONSHIP.VERIFIED ||
      incumbentRelationship === CAD_INCUMBENT_RELATIONSHIP.INFERRED)
  ) {
    if (/OPEN|BID|RFP|AVAILABLE/i.test(pathway + " " + text(opp.recommendedNextStep))) {
      return {
        attainability: CAD_ATTAINABILITY.WINNABLE,
        confidence: CAD_CONFIDENCE.LOW,
      };
    }
    return {
      attainability: CAD_ATTAINABILITY.OVERFLOW_ONLY,
      confidence: CAD_CONFIDENCE.LOW,
    };
  }
  if (pursuit.ok) {
    return {
      attainability: CAD_ATTAINABILITY.MONITOR,
      confidence: CAD_CONFIDENCE.LOW,
    };
  }
  return {
    attainability: CAD_ATTAINABILITY.MONITOR,
    confidence: CAD_CONFIDENCE.UNKNOWN,
  };
}

function resolveSourcingWindow(opp = {}, pursuit = {}) {
  const evidenceUrls = collectOppEvidenceUrls(opp);
  const startRaw = text(opp.eventStartDate || opp.tileEventStartDate || "") || null;
  const endRaw = text(opp.eventEndDate || opp.tileEventEndDate || "") || null;

  if (/OPEN_SOURCING|ENTERING_WINDOW|CURRENT_FUTURE/i.test(
    text(opp.futureCycleEvidenceState || opp.commercialTemporalDisposition || ""),
  )) {
    return sanitizeSourcingWindow({
      state: CAD_SOURCING_WINDOW_STATE.CONFIRMED,
      start: startRaw,
      end: endRaw,
      basis: "EXPLICIT_SOURCING_STATE",
      evidenceUrls,
    });
  }

  if (pursuit.ok && (startRaw || pursuit.basis === "NARRATIVE_RECURRENCE_OR_OVERFLOW")) {
    return sanitizeSourcingWindow({
      state: CAD_SOURCING_WINDOW_STATE.INFERRED,
      start: startRaw,
      end: endRaw,
      basis: pursuit.basis || "INFERRED_FROM_PURSUIT",
      evidenceUrls,
    });
  }

  return sanitizeSourcingWindow({
    state: CAD_SOURCING_WINDOW_STATE.UNKNOWN,
    start: null,
    end: null,
    basis: "INSUFFICIENT_SOURCING_EVIDENCE",
    evidenceUrls: [],
  });
}

function extractBuyer(opp = {}) {
  const name = text(opp.primaryContact?.name || opp.primaryContactName || "");
  if (!name || !looksLikePersonName(name)) return null;
  return {
    name,
    role: text(opp.primaryContact?.role || opp.primaryContact?.title) || null,
    email: text(opp.primaryContact?.email) || null,
    phone: text(opp.primaryContact?.phone) || null,
    linkedinUrl: text(opp.primaryContact?.linkedinUrl) || null,
    organizationContactRouteLabel:
      text(opp.primaryContact?.organizationContactRouteLabel) || null,
  };
}

function extractIntermediary(opp = {}) {
  const label = text(
    opp.intermediaryName ||
      opp.housingBureau ||
      opp.primaryContact?.organizationContactRouteLabel ||
      "",
  );
  if (!label) return null;
  if (looksLikePersonName(label) && !/housing|bureau|onpeak|maritz|fht/i.test(label)) {
    return null;
  }
  return {
    label,
    kind: /onpeak|housing|bureau|maritz|fht|spargo/i.test(label)
      ? "HOUSING_INTERMEDIARY"
      : "ORGANIZATIONAL_ROUTE",
    email: text(opp.intermediaryEmail) || null,
  };
}

/**
 * Classify one opportunity / research row into a CAD payload.
 * Pure function — no I/O, no persistence, no FQO pass claim.
 */
export function classifyCompetitiveAccountDiscovery(opportunity = {}, opts = {}) {
  const opp = opportunity && typeof opportunity === "object" ? opportunity : {};
  const research = {
    ...researchViewFromOpportunity(opp),
    ...(opts.research || {}),
  };

  const evidenceUrls = collectOppEvidenceUrls(opp);
  const venue = text(research.venue || research.eventVenue);
  let hotelName =
    text(research.competitorHotel) ||
    extractCompetitorHotelFromResearchEvidence(research) ||
    null;

  const venueOnly =
    Boolean(hotelName) &&
    isEventVenueOnlyMention(research, hotelName);

  if (venueOnly) {
    hotelName = null;
  }

  const hasBookingLanguage = /room block|hotel block|negotiated block|host hotel|official housing|housing provider|published (room )?rates/i.test(
    `${text(research.lodgingEvidence)} ${text(research.hotelSourcingDecisionEvidence)} ${text(research.competitorBookingClaim)}`,
  );

  let relationship = CAD_INCUMBENT_RELATIONSHIP.UNKNOWN;
  if (hotelName && evidenceUrls.length && hasBookingLanguage && !venueOnly) {
    relationship = CAD_INCUMBENT_RELATIONSHIP.VERIFIED;
  } else if (hotelName && !venueOnly && (evidenceUrls.length || hasBookingLanguage)) {
    relationship = CAD_INCUMBENT_RELATIONSHIP.INFERRED;
  } else if (hotelName && !venueOnly) {
    relationship = CAD_INCUMBENT_RELATIONSHIP.INFERRED;
  }

  const pursuit = hasFutureOrOverflowPursuit(opp, research);
  const { attainability, confidence: attainabilityConfidence } =
    resolveAttainability(opp, pursuit, relationship);
  const sourcingWindow = resolveSourcingWindow(opp, pursuit);

  const programKey = buildCadProgramKey({
    organizationName: opp.organizationName || opp.organization,
    programName: opp.programmeName || opp.programName,
    title: opp.title || opp.displayTitle,
    eventStartDate: opp.eventStartDate || opp.tileEventStartDate,
    cycleKey: opts.cycleKey,
  });

  const historicalDemand = [];
  if (hotelName || venue) {
    const yearMatch = String(opp.eventStartDate || opp.title || "").match(/\b(20\d{2})\b/);
    historicalDemand.push({
      year: yearMatch ? Number(yearMatch[1]) : null,
      hostHotel: hotelName,
      location: text(opp.city || opp.market || opp.location) || null,
      evidenceUrls,
      roomBlockEvidence: hasBookingLanguage,
      confidence:
        relationship === CAD_INCUMBENT_RELATIONSHIP.VERIFIED
          ? CAD_CONFIDENCE.HIGH
          : relationship === CAD_INCUMBENT_RELATIONSHIP.INFERRED
            ? CAD_CONFIDENCE.MEDIUM
            : CAD_CONFIDENCE.LOW,
    });
  }

  const risks = [];
  if (venueOnly) {
    risks.push("HISTORICAL_VENUE_IS_NOT_INCUMBENT_CONTRACT");
  }
  if (hotelName && !pursuit.ok) {
    risks.push("COMPETITOR_RELATIONSHIP_WITHOUT_AVAILABLE_DEMAND");
  }
  if (!extractBuyer(opp)) {
    risks.push("NAMED_BUYER_UNVERIFIED");
  }
  if (sourcingWindow.state === CAD_SOURCING_WINDOW_STATE.UNKNOWN) {
    risks.push("SOURCING_WINDOW_UNKNOWN");
  }

  let cadClass = null;
  const competitiveOk =
    Boolean(hotelName) &&
    !venueOnly &&
    (relationship === CAD_INCUMBENT_RELATIONSHIP.VERIFIED ||
      relationship === CAD_INCUMBENT_RELATIONSHIP.INFERRED);

  if (!competitiveOk) {
    cadClass = null;
    risks.push("INSUFFICIENT_COMPETITIVE_EVIDENCE_FOR_CAD");
  } else if (!pursuit.ok) {
    cadClass = CAD_CLASS.HISTORICAL_COMPETITIVE_INTEL;
  } else if (
    attainability === CAD_ATTAINABILITY.UNATTAINABLE ||
    attainability === CAD_ATTAINABILITY.MONITOR
  ) {
    // Credible pathway but not yet a strong hotel pursuit package
    cadClass =
      attainability === CAD_ATTAINABILITY.UNATTAINABLE
        ? CAD_CLASS.HISTORICAL_COMPETITIVE_INTEL
        : CAD_CLASS.EMERGING_COMPETITIVE_OPPORTUNITY;
  } else if (
    attainability === CAD_ATTAINABILITY.WINNABLE ||
    attainability === CAD_ATTAINABILITY.OVERFLOW_ONLY
  ) {
    // Enough to enter FQO evaluation — does NOT authorize PASS or persistence
    cadClass = CAD_CLASS.FQO_CANDIDATE;
    risks.push("FQO_CANDIDATE_REQUIRES_SIX_GATE_EVALUATION");
  } else {
    cadClass = CAD_CLASS.EMERGING_COMPETITIVE_OPPORTUNITY;
  }

  // Incomplete hotel qualification despite pursuit → Emerging (not FQO_CANDIDATE)
  const hotelAction = text(
    opp.recommendedNextStep || opp.recommendedAction || opp.whyThisHotel || "",
  );
  if (
    cadClass === CAD_CLASS.FQO_CANDIDATE &&
    (!hotelAction ||
      /\bno verified (current )?(host|overflow|role)\b/i.test(hotelAction))
  ) {
    cadClass = CAD_CLASS.EMERGING_COMPETITIVE_OPPORTUNITY;
    risks.push("HOTEL_QUALIFICATION_INCOMPLETE_DOWNGRADED_FROM_FQO_CANDIDATE");
  }

  const payload = emptyCompetitiveAccountDiscovery({
    organizationId: opp.organizationId || null,
    demandGeneratorId: opp.demandGeneratorId || opp.dgGeneratorId || null,
    programKey,
    historicalDemand,
    incumbent: {
      hotelName,
      relationship: venueOnly
        ? CAD_INCUMBENT_RELATIONSHIP.UNKNOWN
        : relationship,
      commitmentScope: hasBookingLanguage
        ? COMPETITOR_COMMITMENT_SCOPE.PRIMARY_ROOM_BLOCK
        : null,
      evidenceUrls,
      venueOnlyRejected: venueOnly,
    },
    sourcingWindow,
    attainability,
    attainabilityConfidence,
    buyer: extractBuyer(opp),
    intermediary: extractIntermediary(opp),
    cadClass,
    recommendedAction: hotelAction || null,
    risks,
    notes: [
      `classifier=${GDI_COMPETITIVE_ACCOUNT_DISCOVERY_CLASSIFIER_V1}`,
      `fqoCandidateDoesNotMeanFqoPass=true`,
      pursuit.basis ? `pursuitBasis=${pursuit.basis}` : null,
    ].filter(Boolean),
    classifiedAt: new Date().toISOString(),
  });

  const validated = validateCompetitiveAccountDiscovery(payload);
  return {
    contractVersion: GDI_COMPETITIVE_ACCOUNT_DISCOVERY_CONTRACT_V1,
    classifierVersion: GDI_COMPETITIVE_ACCOUNT_DISCOVERY_CLASSIFIER_V1,
    ok: validated.ok,
    errors: validated.errors,
    cad: validated.value,
    // Explicit non-claims
    fqoPass: false,
    persistenceAuthorized: false,
    publicationAuthorized: false,
    qualificationTransferred: false,
  };
}

/**
 * Attach CAD classification onto a copy of the opportunity (Layer-1 SoT field).
 * Does not mutate the input object.
 */
export function attachCompetitiveAccountDiscovery(opportunity = {}, opts = {}) {
  const result = classifyCompetitiveAccountDiscovery(opportunity, opts);
  return {
    ...opportunity,
    competitiveAccountDiscovery: result.cad,
    __cadClassification: {
      ok: result.ok,
      errors: result.errors,
      fqoPass: false,
      persistenceAuthorized: false,
      publicationAuthorized: false,
      qualificationTransferred: false,
      classifierVersion: result.classifierVersion,
    },
  };
}

export {
  CAD_CLASS,
  CAD_ATTAINABILITY,
  CAD_INCUMBENT_RELATIONSHIP,
  CAD_SOURCING_WINDOW_STATE,
  buildCadProgramKey,
  validateCompetitiveAccountDiscovery,
};
