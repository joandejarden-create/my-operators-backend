/**
 * GDI Universal Research-to-Drawer Intelligence Standard V2
 *
 * Two layers for every customer-facing drawer section that carries research:
 *   Layer 1 — Sales Brief (concise, immediately visible)
 *   Layer 2 — Research Detail (expandable, evidence-preserving, non-duplicative)
 *
 * Shared across all fourteen PowerShell workflows. Zero paid.
 * Does not invent volume, RFPs, or unsourced claims.
 */
export const RESEARCH_TO_DRAWER_INTELLIGENCE_V2 =
  "gdi-research-to-drawer-intelligence-v2";

function text(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.replace(/\s+/g, " ").trim();
  if (typeof v === "number" || typeof v === "boolean") {
    return String(v).replace(/\s+/g, " ").trim();
  }
  if (Array.isArray(v)) {
    return v
      .map((x) => text(x))
      .filter(Boolean)
      .join(" · ")
      .replace(/\s+/g, " ")
      .trim();
  }
  if (typeof v === "object") {
    // Never serialize raw objects as "[object Object]" into customer copy.
    const nested =
      v.claim ||
      v.text ||
      v.body ||
      v.summary ||
      v.thesis ||
      v.label ||
      v.title ||
      v.note ||
      v.value ||
      null;
    if (nested != null && nested !== v) return text(nested);
    return "";
  }
  return String(v)
    .replace(/\s+/g, " ")
    .trim()
    .replace(/^\[object Object\]$/i, "");
}

function firstText(...vals) {
  for (const v of vals) {
    const t = text(v);
    if (t && !/^\[object Object\]$/i.test(t)) return t;
  }
  return "";
}

function listOf(...vals) {
  const out = [];
  for (const v of vals) {
    if (Array.isArray(v)) {
      for (const x of v) {
        const t = text(
          typeof x === "string" ? x : x?.claim || x?.text || x?.url || x,
        );
        if (t && !/^\[object Object\]$/i.test(t) && !out.includes(t)) out.push(t);
      }
    } else {
      const t = text(v);
      if (t && !/^\[object Object\]$/i.test(t) && !out.includes(t)) out.push(t);
    }
  }
  return out;
}

function resolveHotelDisplayName(opportunity = {}, deep = null) {
  return (
    firstText(
      opportunity.hotelDisplayName,
      opportunity.hotelName,
      opportunity.propertyName,
      deep?.hotelDisplayName,
      deep?.hotelName,
      opportunity.commercialIntelligenceBrief?.hotelDisplayName,
    ) || "this hotel"
  );
}

function briefFromLong(s, max = 420) {
  const t = text(s);
  if (!t) return "";
  if (t.length <= max) return t;
  const cut = t.slice(0, max);
  const sp = cut.lastIndexOf(". ");
  if (sp > max * 0.45) return cut.slice(0, sp + 1).trim();
  return `${cut.trim()}…`;
}

function detailWithoutBrief(brief, detail) {
  const b = text(brief).toLowerCase();
  const d = text(detail);
  if (!d) return "";
  if (!b) return d;
  if (d.toLowerCase() === b) return "";
  if (d.toLowerCase().startsWith(b) && d.length > b.length + 40) {
    return text(d.slice(brief.length));
  }
  // If detail is mostly a repeat, suppress.
  if (b.length > 80 && d.toLowerCase().includes(b.slice(0, Math.min(120, b.length)))) {
    const stripped = d.replace(new RegExp(text(brief).replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i"), "").trim();
    return text(stripped).length > 60 ? text(stripped) : "";
  }
  return d;
}

/**
 * Build V2 pack from opportunity + optional deep/ORS research artifact.
 */
export function buildResearchToDrawerIntelligenceV2(opportunity = {}, deepArtifact = null) {
  const o = opportunity || {};
  const deep = deepArtifact?.candidate || deepArtifact?.run?.structured || deepArtifact || null;
  const cib = o.commercialIntelligenceBrief || o.salesBrief || {};
  const hotelName = resolveHotelDisplayName(o, deep);

  const demandFull = firstText(
    deep?.hotelDemandThesis,
    deep?.evidenceOfHospitalityDemand,
    deep?.demandMechanismSummary,
    deep?.potentialGuestroomDemand,
    cib.potentialBusinessForThisHotel,
    cib.potentialHotelBusiness,
    o.proposedHotelUse,
    o.hotelOpportunityThesis,
    o.whyHotelDemand,
  );
  const demandAssumptions = listOf(
    deep?.uncertaintyRegister,
    deep?.peakRoomsStatus === "not_published"
      ? "Peak room requirement is not published — do not invent volume."
      : null,
    Array.isArray(o.uncertainties) ? o.uncertainties : null,
  );
  const demandBrief = briefFromLong(
    firstText(
      demandFull,
      `Potential lodging, meetings or catering demand near ${hotelName} remains a hypothesis until sourcing and buyer authority are confirmed.`,
    ),
    380,
  );
  const demandDetail = detailWithoutBrief(
    demandBrief,
    [
      demandFull,
      deep?.potentialMeetingDemand
        ? `Meetings / F&B: ${text(deep.potentialMeetingDemand)}`
        : "",
      deep?.potentialCateringDemand
        ? `Catering: ${text(deep.potentialCateringDemand)}`
        : "",
      deep?.likelyTravelerProfiles
        ? `Likely travelers: ${text(deep.likelyTravelerProfiles)}`
        : "",
      deep?.recurrenceEvidence || deep?.recurrenceStatus
        ? `Recurrence: ${text(deep.recurrenceEvidence || deep.recurrenceStatus)}`
        : "",
      demandAssumptions.length
        ? `Assumptions / gaps: ${demandAssumptions.slice(0, 6).join(" · ")}`
        : "",
    ]
      .filter(Boolean)
      .join(" "),
  );

  const fitFull = firstText(
    deep?.hotelFitThesis,
    deep?.preliminaryHotelFit,
    deep?.hotelMarketRelevance,
    cib.whyThisHotel,
    o.hotelFitRationale,
    o.whyThisHotel,
  );
  // Never inject another hotel's geography (e.g. Marylebone / Portman Square).
  // Prefer researched advantages only; omit when absent rather than fabricating fit.
  const advantages = listOf(
    deep?.advantages,
    cib.whyThisHotelAdvantages,
    o.whyThisHotelAdvantages,
  );
  const limitations = listOf(
    deep?.limitations,
    cib.whyThisHotelLimitations,
    o.whyThisHotelLimitations,
    deep?.knownCompetitiveConstraints,
    "Programme existence or nearby venue activity is not proof of an open room block at this hotel.",
  );
  const fitBrief = briefFromLong(fitFull || advantages[0] || "", 320);
  const fitDetail = detailWithoutBrief(
    fitBrief,
    [fitFull, deep?.sourcingAccessibility, deep?.sourcingCycleEvidence]
      .filter(Boolean)
      .join(" "),
  );

  const contacts = [];
  const pushContact = (c) => {
    if (!c) return;
    const name = text(c.name || c.fullName);
    if (!name) return;
    contacts.push({
      name,
      title: text(c.title || c.role || c.contactRole) || null,
      organization: text(c.organization || c.organizationName || o.organizationName) || null,
      whyRelevant: text(c.whyRelevant || c.commercialRelevance) || null,
      publicUrl: text(c.publicUrl || c.sourceUrl || c.url) || null,
      contractingAuthorityVerified: c.contractingAuthorityVerified === true,
    });
  };
  pushContact(o.primaryContact);
  pushContact(o.secondaryContact);
  for (const c of deep?.organizerContactCandidates || deep?.namedContacts || []) {
    pushContact(c);
  }
  if (!contacts.length && text(o.contactName)) {
    pushContact({
      name: o.contactName,
      title: o.contactRole,
      organization: o.organizationName,
    });
  }

  const sourcingStatus = firstText(
    deep?.hotelSourcingStatus,
    o.hotelSourcingStatus,
    o.hotelSourcingStatusLabel,
    o.venueSourcingStatusLabel,
  );
  const competitive = {
    sourcingStatus: sourcingStatus || "hotel_sourcing_unconfirmed",
    sourcingLabel: firstText(
      o.hotelSourcingStatusLabel,
      sourcingStatus === "hotel_sourcing_open"
        ? "Hotel Sourcing Open"
        : sourcingStatus === "venue_confirmed"
          ? "Venue Confirmed"
          : sourcingStatus === "historical_venue_only"
            ? "Historical Venue Only"
            : "Hotel Sourcing Unconfirmed",
    ),
    incumbentVenue: firstText(deep?.incumbentVenue, deep?.verifiedVenue, o.verifiedVenue),
    historicalVenues: listOf(deep?.historicalHostHotels, deep?.historicalEditions),
    constraints: listOf(deep?.knownCompetitiveConstraints),
    note: firstText(
      deep?.sourcingCycleEvidence,
      deep?.sourcingAccessibility,
      "Do not treat unknown hotel sourcing as verified open sourcing.",
    ),
  };

  const whatHappening = firstText(
    cib.whatIsHappening,
    o.whatIsHappening,
    o.summaryWhat,
    deep?.eventIdentity || deep?.verifiedEventName,
    deep?.eventActivitySummary,
  );

  return {
    version: RESEARCH_TO_DRAWER_INTELLIGENCE_V2,
    generatedAt: new Date().toISOString(),
    doesNotAffectProduction: true,
    opportunityId: o.id || null,
    salesBrief: {
      whatIsHappening: briefFromLong(whatHappening, 360) || null,
      potentialBusinessForThisHotel: demandBrief || null,
      whyThisHotel: fitBrief || null,
      whyThisHotelAdvantages: advantages.slice(0, 4),
      whyThisHotelLimitations: limitations.slice(0, 4),
      whyActNow: briefFromLong(
        firstText(cib.whyActNow, o.whyNow, o.summaryWhyMatters, deep?.nextMonitoringTrigger),
        280,
      ) || null,
      recommendedNextStep: briefFromLong(
        firstText(
          cib.recommendedNextStep,
          o.recommendedNextStep,
          o.recommendedAction,
          deep?.suggestedRelationshipDevelopmentAction,
        ),
        280,
      ) || null,
    },
    researchDetail: {
      potentialBusinessForThisHotel: demandDetail || null,
      demandAssumptions: demandAssumptions.slice(0, 8),
      whyThisHotel: fitDetail || null,
      competitiveIntelligence: competitive,
      buyerResearch: contacts.slice(0, 4),
      programmeDetails: firstText(
        deep?.verifiedEventName,
        deep?.eventIdentity,
        deep?.verifiedDates,
        o.programmeName,
        o.eventDisplayName,
      ) || null,
      qualificationQuestions: listOf(
        deep?.suggestedDeepResearchQuestions,
        cib.qualificationQuestions,
        o.qualificationQuestions,
      ).slice(0, 6),
      evidenceUrls: listOf(
        (deep?.supportingSources || []).map((s) => s?.url),
        deep?.evidenceUrls,
        (o.sources || []).map((s) => (typeof s === "string" ? s : s?.url)),
      ).slice(0, 12),
    },
    preservation: {
      originalCibPreserved: true,
      deepArtifactUsed: Boolean(deepArtifact),
      inventedVolume: false,
    },
  };
}

/**
 * Apply V2 pack onto an opportunity (non-destructive overlay).
 */
export function applyResearchToDrawerIntelligenceV2(opportunity = {}, pack = null) {
  if (!opportunity || typeof opportunity !== "object") return opportunity;
  const v2 = pack || opportunity.researchToDrawerV2;
  if (!v2 || v2.version !== RESEARCH_TO_DRAWER_INTELLIGENCE_V2) return opportunity;
  const sb = v2.salesBrief || {};
  const cib = { ...(opportunity.commercialIntelligenceBrief || {}) };
  if (sb.potentialBusinessForThisHotel) {
    cib.potentialBusinessForThisHotel = sb.potentialBusinessForThisHotel;
  }
  if (sb.whyThisHotel) cib.whyThisHotel = sb.whyThisHotel;
  if (sb.whyThisHotelAdvantages?.length) {
    cib.whyThisHotelAdvantages = sb.whyThisHotelAdvantages;
  }
  if (sb.whyThisHotelLimitations?.length) {
    cib.whyThisHotelLimitations = sb.whyThisHotelLimitations;
  }
  if (sb.whyActNow) cib.whyActNow = sb.whyActNow;
  if (sb.recommendedNextStep) cib.recommendedNextStep = sb.recommendedNextStep;
  if (sb.whatIsHappening) {
    cib.whatIsHappening = sb.whatIsHappening;
  }
  return {
    ...opportunity,
    researchToDrawerV2: v2,
    commercialIntelligenceBrief: cib,
    proposedHotelUse: sb.potentialBusinessForThisHotel || opportunity.proposedHotelUse,
    whyThisHotel: sb.whyThisHotel || opportunity.whyThisHotel,
    whyThisHotelAdvantages:
      sb.whyThisHotelAdvantages || opportunity.whyThisHotelAdvantages,
    whyThisHotelLimitations:
      sb.whyThisHotelLimitations || opportunity.whyThisHotelLimitations,
  };
}
