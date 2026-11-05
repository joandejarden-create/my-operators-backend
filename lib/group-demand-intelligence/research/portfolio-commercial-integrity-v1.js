/**
 * Shared GDI commercial integrity corrections (run-046+).
 *
 * Cross-hotel contamination quarantine, honest evidence confidence,
 * contact attribution gates, and gold-status consistency — applied in
 * shared code paths (not one-off per-opportunity patches).
 */
import {
  scrubAssociationEventsContactFields,
  isAssociationEventsValidPersonName,
  scrubAssociationEventsPhone,
  assessAssociationEventsBuyerCandidate,
  buildAssociationEventsOrgRouteSalesAction,
  isAssociationEventsBuyerIntegritySegment,
} from "./association-events-buyer-integrity-v1.js";
import {
  DOS_GRADE,
  GOLD_BRIEF_STATUS,
} from "../commercial-sales-quality-v1.js";

export const PORTFOLIO_COMMERCIAL_INTEGRITY_V1 =
  "gdi-portfolio-commercial-integrity-v1";

const FOREIGN_HOTEL_RE =
  /\b(Bethesda Marriott|Waterstone Resort|Wyndham San Diego|NIH\b|National Institutes of Health|Boca Raton Marina(?!\s*\/)|AC Hotel A Coru[nñ]a)\b/i;

const CHAPULTEPEC_MARKERS_RE =
  /\b(Grand Fiesta Americana Chapultepec|Chapultepec|Reforma|Polanco|Anzures|Ciudad de M[eé]xico|Mexico City|CDMX|Fiesta Rewards|Grupo Posadas)\b/i;

const GENERIC_FACILITIES_RE =
  /Seven meeting rooms and executive facilities|Ballroom capacity up to\s*~?220/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function collectSourceUrls(opp = {}) {
  const urls = new Set();
  for (const s of opp.sources || []) {
    if (s?.url) urls.add(String(s.url));
  }
  for (const s of opp.evidenceUrls || []) {
    if (typeof s === "string" && /^https?:/i.test(s)) urls.add(s);
    else if (s?.url) urls.add(String(s.url));
  }
  return [...urls];
}

function sourceCount(opp = {}) {
  return collectSourceUrls(opp).length;
}

function attachedSourcesForDisplay(opp = {}) {
  return Array.isArray(opp.sources) ? opp.sources.filter((s) => s?.url).length : 0;
}

/** Speaker-directory / unrelated person pages are weak lodging evidence. */
function weakLodgingEvidenceRatio(urls = []) {
  if (!urls.length) return 1;
  const weak = urls.filter((u) =>
    /\/speakers?\b|\/people\/|linkedin\.com\/in\/|directorio|meet-the-team|\/nosotros\/?/i.test(
      u,
    ),
  ).length;
  return weak / urls.length;
}

/**
 * Detect foreign-hotel thesis content that does not belong to `hotel`.
 */
export function detectCrossHotelContamination(opportunity = {}, hotel = {}) {
  const hotelId = text(hotel.hotelId || opportunity.hotelId);
  const hotelName = text(hotel.displayName || hotel.name || "");
  const fields = {
    recommendedAction: opportunity.recommendedAction,
    whyThisHotel: opportunity.whyThisHotel,
    hotelOpportunityThesis: opportunity.hotelOpportunityThesis,
    salesBriefAction:
      typeof opportunity.salesBrief?.recommendedSalesApproach === "string"
        ? opportunity.salesBrief.recommendedSalesApproach
        : opportunity.salesBrief?.recommendedSalesApproach?.nextAction,
    cibAction:
      typeof opportunity.commercialIntelligenceBrief?.recommendedSalesApproach ===
      "string"
        ? opportunity.commercialIntelligenceBrief.recommendedSalesApproach
        : opportunity.commercialIntelligenceBrief?.recommendedSalesApproach
            ?.nextAction,
    goldWhy: opportunity.associationEventsGoldBrief?.whyThisHotel,
    goldAction:
      typeof opportunity.associationEventsGoldBrief?.recommendedSalesApproach ===
      "string"
        ? opportunity.associationEventsGoldBrief.recommendedSalesApproach
        : null,
  };
  const hits = [];
  for (const [field, value] of Object.entries(fields)) {
    const blob = text(value);
    if (!blob) continue;
    if (!FOREIGN_HOTEL_RE.test(blob)) continue;
    // Allowed only if the foreign name is the current hotel itself
    if (hotelName && blob.includes(hotelName)) continue;
    // Waterstone local inventory: "Waterstone Resort" is the current hotel, not foreign.
    if (
      hotelId === "recgMYovrrZDJMqzX" ||
      /Waterstone Resort/i.test(hotelName)
    ) {
      const otherForeign =
        /\b(Bethesda Marriott|Wyndham San Diego|NIH\b|National Institutes of Health|AC Hotel A Coru[nñ]a)\b/i.test(
          blob,
        );
      if (!otherForeign) continue;
    }
    // Chapultepec campaigns: Bethesda/Waterstone/NIH are always foreign
    if (
      hotelId === "recterKmNPdntCm0v" ||
      /Chapultepec/i.test(hotelName)
    ) {
      hits.push({ field, excerpt: blob.slice(0, 160) });
    } else if (!new RegExp(hotelName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(blob)) {
      hits.push({ field, excerpt: blob.slice(0, 160) });
    }
  }
  return {
    contaminated: hits.length > 0,
    hits,
    version: PORTFOLIO_COMMERCIAL_INTEGRITY_V1,
  };
}

/**
 * Honest evidence confidence from source/verification facts — never inflate to 95 with empty evidence.
 */
export function recalibrateEvidenceConfidence(opportunity = {}) {
  const urls = collectSourceUrls(opportunity);
  const sources = urls.length;
  const displaySources = attachedSourcesForDisplay(opportunity);
  const weakRatio = weakLodgingEvidenceRatio(urls);
  const verifiedHints = [
    opportunity.eventStartDate,
    opportunity.eventCity || opportunity.eventLocationSummary,
    opportunity.organizationName,
    opportunity.contactUrl || opportunity.organizationContactEmail,
  ].filter((v) => text(v)).length;
  const speakerPageTitle = /^Speakers\b/i.test(text(opportunity.title));
  const genericFit =
    !text(opportunity.whyThisHotel) || GENERIC_FACILITIES_RE.test(opportunity.whyThisHotel);

  const prior = Number(opportunity.evidenceConfidence);
  let score = 15;
  if (sources >= 1) score += 20;
  if (sources >= 3) score += 15;
  if (sources >= 6) score += 15;
  if (verifiedHints >= 2) score += 15;
  if (verifiedHints >= 4) score += 10;
  if (text(opportunity.whyThisHotel) && !GENERIC_FACILITIES_RE.test(opportunity.whyThisHotel)) {
    score += 10;
  }
  // Cap hard when no sources
  if (sources === 0) {
    score = Math.min(score, 35);
  }
  // Drawer-visible sources empty → never present as high-confidence verified
  if (displaySources === 0) {
    score = Math.min(score, 40);
  }
  // Speaker directories / LinkedIn person pages are not lodging proof
  if (weakRatio >= 0.5 || speakerPageTitle) {
    score = Math.min(score, 45);
  }
  if (speakerPageTitle && genericFit) {
    score = Math.min(score, 35);
  }
  // Never allow prior inflated 90+ to survive with zero display sources
  if (displaySources === 0 && Number.isFinite(prior) && prior >= 80) {
    score = Math.min(score, 30);
  }

  // run-052 Issue 4 (RC-08): source volume must not compensate for critical commercial gaps.
  const goldQual =
    opportunity.associationEventsGoldBrief?.qualification ||
    opportunity.salesBrief?.qualification ||
    null;
  const researchContinuation =
    goldQual?.status === "RESEARCH_CONTINUATION" ||
    goldQual?.qualified === false ||
    opportunity.commercialResearchCompletionState === "RESEARCH_CONTINUATION" ||
    /RESEARCH_CONTINUATION/i.test(text(goldQual?.status));
  const missingBuyer =
    opportunity.hasVerifiedNamedRelevantContact === false ||
    (!text(opportunity.contactName) &&
      !text(opportunity.primaryContact?.name) &&
      !opportunity.associationEventsGoldBrief?.whoToContact?.primary);
  const pastChronology =
    /PAST_COMPLETED|PAST\b/i.test(text(goldQual?.chronologyStatus)) ||
    /PAST_COMPLETED/i.test(text(opportunity.associationEventsChronology?.status));
  const caps = [];
  if (missingBuyer) {
    score = Math.min(score, 55);
    caps.push("missing_verified_buyer");
  }
  if (researchContinuation || goldQual?.qualified === false) {
    score = Math.min(score, 60);
    caps.push("research_continuation");
  }
  if (pastChronology) {
    score = Math.min(score, 50);
    caps.push("past_chronology");
  }
  if (genericFit) {
    score = Math.min(score, 55);
    caps.push("generic_or_missing_hotel_fit");
  }

  score = Math.max(0, Math.min(100, Math.round(score)));

  const capNote = caps.length ? ` Capped for: ${caps.join(", ")}.` : "";
  const explanation =
    displaySources === 0
      ? `Evidence confidence ${score}/100: ${sources} URL(s) in provenance, but 0 attached drawer source links; ${verifiedHints} identity field(s). Not verified research completeness.${capNote}`
      : `Evidence confidence ${score}/100 from ${displaySources} attached source URL(s) (${sources} incl. provenance) and ${verifiedHints} structured identity field(s). Confidence is not hotel fit, availability, or win probability.${capNote}`;

  return {
    evidenceConfidence: score,
    evidenceConfidenceExplanation: explanation,
    sourceUrlCount: sources,
    attachedSourceUrlCount: displaySources,
    weakLodgingEvidenceRatio: weakRatio,
    verifiedFieldHints: verifiedHints,
    priorConfidence: Number.isFinite(prior) ? prior : null,
    downgraded: Number.isFinite(prior) && prior - score >= 20,
    confidenceCaps: caps,
  };
}

/**
 * run-052 Issue 4 (RC-04): displayed sales posture must follow current authoritative
 * commercial qualification, not a stale import-era QUALIFIED stamp.
 */
export function reconcileOpportunityQualificationDisplay(opportunity = {}) {
  const next = { ...opportunity };
  const goldQual =
    next.associationEventsGoldBrief?.qualification ||
    next.salesBrief?.qualification ||
    null;
  const goldStatus = text(goldQual?.status);
  const top = text(next.opportunityQualification);
  const continuation =
    goldQual &&
    (goldQual.qualified === false ||
      goldStatus === "RESEARCH_CONTINUATION" ||
      /^HOLD_/i.test(goldStatus));

  if (continuation) {
    // Demote sales-ready stamps; preserve honest CLOSED / WATCH / EXCLUDE.
    const demoteTop =
      !top ||
      /^(QUALIFIED|STRONG|MODERATE|ACTIONABLE|VERIFIED_OPEN)$/i.test(top);
    if (demoteTop) {
      next._priorOpportunityQualification = top || null;
      next.opportunityQualification = goldStatus || "RESEARCH_CONTINUATION";
      next.opportunityQualificationLabel =
        goldStatus === "RESEARCH_CONTINUATION"
          ? "Research continuation"
          : goldStatus.replace(/^HOLD_/, "Hold — ").replace(/_/g, " ");
      next.qualificationReconciledFromGold = true;
    }
    // Tile maturity must not advertise QUALIFIED while commercial state is continuation.
    if (/^QUALIFIED$/i.test(text(next.gdiMaturityState))) {
      next._priorGdiMaturityState = next.gdiMaturityState;
      next.gdiMaturityState = "CANDIDATE";
      next.maturityReconciledFromGold = true;
    }
  }

  // Align nested buyer gates with verified-contact flag after scrub.
  if (next.hasVerifiedNamedRelevantContact === false) {
    for (const briefKey of ["associationEventsGoldBrief", "salesBrief"]) {
      const brief = next[briefKey];
      if (!brief?.qualification?.gates) continue;
      next[briefKey] = {
        ...brief,
        qualification: {
          ...brief.qualification,
          gates: {
            ...brief.qualification.gates,
            oneNamedRelevantBuyer: false,
          },
          namedBuyers: Array.isArray(brief.qualification.namedBuyers)
            ? brief.qualification.namedBuyers.filter((b) => {
                const n = text(b?.name || b);
                return n && !brief.qualification?._clearedRejectedBuyer;
              })
            : [],
        },
      };
    }
  }

  return next;
}

/**
 * Align goldBriefStatus with actual evidence — do not keep COMPLETE_AND_ACTIONABLE on empty research.
 */
export function reconcileGoldBriefHonesty(opportunity = {}, evidence = {}) {
  const sources = evidence.sourceUrlCount ?? sourceCount(opportunity);
  const attached = evidence.attachedSourceUrlCount ?? attachedSourcesForDisplay(opportunity);
  const weakRatio = evidence.weakLodgingEvidenceRatio ?? 0;
  const speakerPageTitle = /^Speakers\b/i.test(text(opportunity.title));
  const genericFit =
    !text(opportunity.whyThisHotel) || GENERIC_FACILITIES_RE.test(text(opportunity.whyThisHotel));
  const current = text(opportunity.goldBriefStatus);
  let next = current || null;
  let reason = null;

  if (
    (attached === 0 || sources === 0) &&
    current === GOLD_BRIEF_STATUS.COMPLETE_AND_ACTIONABLE
  ) {
    next = GOLD_BRIEF_STATUS.INSUFFICIENT_COMMERCIAL_EVIDENCE;
    reason = "complete_flag_without_attached_sources";
  } else if (
    speakerPageTitle &&
    (current === GOLD_BRIEF_STATUS.COMPLETE_AND_ACTIONABLE ||
      current === GOLD_BRIEF_STATUS.ACTIONABLE_WITH_DISCLOSED_GAPS)
  ) {
    next = GOLD_BRIEF_STATUS.RESEARCH_CONTINUATION;
    reason = "speaker_page_not_commercial_complete";
  } else if (
    weakRatio >= 0.5 &&
    genericFit &&
    current === GOLD_BRIEF_STATUS.COMPLETE_AND_ACTIONABLE
  ) {
    next = GOLD_BRIEF_STATUS.RESEARCH_CONTINUATION;
    reason = "weak_lodging_urls_generic_fit";
  } else if (
    attached === 0 &&
    current === GOLD_BRIEF_STATUS.ACTIONABLE_WITH_DISCLOSED_GAPS &&
    !text(opportunity.organizationName)
  ) {
    next = GOLD_BRIEF_STATUS.INSUFFICIENT_COMMERCIAL_EVIDENCE;
    reason = "actionable_without_identity_or_sources";
  } else if (
    attached < 2 &&
    current === GOLD_BRIEF_STATUS.COMPLETE_AND_ACTIONABLE &&
    genericFit
  ) {
    next = GOLD_BRIEF_STATUS.RESEARCH_CONTINUATION;
    reason = "complete_with_generic_fit_thin_sources";
  }

  // DOS honesty: speaker-page / empty drawer sources → RESEARCH_FIRST
  let dosGrade = opportunity.dosGrade || null;
  let dosReason = null;
  if (
    dosGrade === DOS_GRADE.ASSIGN_FOR_DISCOVERY &&
    (speakerPageTitle ||
      attached === 0 ||
      /Needs Analysis/i.test(text(opportunity.contactName)))
  ) {
    dosGrade = DOS_GRADE.RESEARCH_FIRST;
    dosReason = speakerPageTitle
      ? "speaker_page_needs_event_identity_research"
      : "discovery_grade_without_attached_sources";
  }

  return {
    goldBriefStatus: next || current || GOLD_BRIEF_STATUS.INSUFFICIENT_COMMERCIAL_EVIDENCE,
    goldBriefComplete: false,
    goldReason: reason,
    dosGrade,
    dosReason,
  };
}

/**
 * Tag contacts with relationship evidence; demote unverified page-heading people.
 */
export function remediateContactAttribution(opportunity = {}) {
  let next = scrubAssociationEventsContactFields({ ...opportunity });
  const eventBlob = `${next.title || ""} ${next.organizationName || ""} ${next.whatIsHappening || ""}`;
  const assessment = assessAssociationEventsBuyerCandidate(
    {
      name: next.contactName,
      title: next.contactRole || next.primaryContact?.title,
    },
    { eventBlob },
  );

  const contactAttribution = {
    version: PORTFOLIO_COMMERCIAL_INTEGRITY_V1,
    name: next.contactName || null,
    role: next.contactRole || next.primaryContact?.title || null,
    organization: next.organizationName || null,
    relationshipToOpportunity: assessment.ok
      ? "credible_commercial_entry_or_buyer_candidate"
      : assessment.reason || "unverified_or_invalid",
    relationshipEvidenceUrl:
      next.contactUrl ||
      next.sources?.[0]?.url ||
      next.associationEventsGoldBrief?.whoToContact?.routes?.url ||
      null,
    isDecisionMaker: assessment.ok && /director|presidente|ceo|gerente|sourcing|meetings/i.test(
      text(next.contactRole || next.primaryContact?.title),
    ),
    isCredibleEntryPoint: assessment.ok,
    isUnverifiedLead: Boolean(next.contactName) && !assessment.ok,
  };

  if (next.contactName && !assessment.ok) {
    // Keep name only as unverified lead — do not present as verified buyer
    next.contactAttributionClass = "UNVERIFIED_LEAD";
    next.hasVerifiedNamedRelevantContact = false;
    if (/speaker_only|invalid_person|media_contact/i.test(assessment.reason || "")) {
      next._rejectedContactName = next.contactName;
      next.contactName = null;
      contactAttribution.name = null;
      contactAttribution.relationshipToOpportunity = assessment.reason;
    }
  } else if (assessment.ok) {
    next.contactAttributionClass = "CREDIBLE_ENTRY_OR_BUYER_CANDIDATE";
  } else if (
    next.organizationContactEmail ||
    next.contactUrl ||
    next.functionalContactEmail
  ) {
    next.contactAttributionClass = "FUNCTIONAL_ORGANIZATION_ROUTE";
    next.isFunctionalOrganizationRoute = true;
  }

  // Scrub polluted phones again (incl. Mexico scrapes like 52-0882881)
  for (const key of ["contactPhone", "companyPhone"]) {
    if (next[key] && !scrubAssociationEventsPhone(next[key])) {
      next[`_rejected_${key}`] = next[key];
      next[key] = null;
    }
  }

  next.contactAttribution = contactAttribution;

  // Association/Events only: scrub "Contact <person>" when buyer flag is false.
  // Catering / government / relocation keep evidenced event-routing contacts and
  // their segment-specific next steps (never rewrite into congress/housing copy).
  const applyEventsContactRewrite = isAssociationEventsBuyerIntegritySegment(next);
  const rewriteContactAction = (raw, fieldTag) => {
    if (!applyEventsContactRewrite) return raw;
    const action = text(raw);
    const actionPerson = (action.match(/^Contact\s+([^:(]+)/i) || [])[1];
    if (!actionPerson) return raw;
    const personName = actionPerson.trim();
    const roleHint =
      text(next.contactRole) ||
      text(next._rejectedContactRole) ||
      text(next.personEvidence?.role) ||
      text(next.personEvidence?._rejectedBuyerRole) ||
      (action.match(/\(([^)]+)\)/) || [])[1] ||
      "";
    const buyerOk = assessAssociationEventsBuyerCandidate(
      { name: personName, title: roleHint },
      {
        eventBlob: `${next.title || ""} ${next.organizationName || ""}`,
        organizerName: next.organizationName || "",
        organizationName: next.organizationName || "",
      },
    );
    const mustRewrite =
      next.hasVerifiedNamedRelevantContact === false ||
      !buyerOk.ok ||
      !isAssociationEventsValidPersonName(personName);
    if (!mustRewrite) return raw;
    next[`_scrubbed${fieldTag}Person`] = personName;
    next[`_scrubbed${fieldTag}Reason`] =
      next.hasVerifiedNamedRelevantContact === false
        ? "unverified_named_contact_not_actionable"
        : buyerOk.reason || "invalid_person_identity";
    return buildAssociationEventsOrgRouteSalesAction(next);
  };

  next.recommendedAction = rewriteContactAction(
    next.recommendedAction,
    "RecommendedAction",
  );
  next.salesActionRecommended = rewriteContactAction(
    next.salesActionRecommended,
    "SalesActionRecommended",
  );
  next.suggestedAction = rewriteContactAction(next.suggestedAction, "SuggestedAction");

  // Gold / salesBrief Contact approaches must not outrank the scrub.
  for (const briefKey of [
    "associationEventsGoldBrief",
    "salesBrief",
    "commercialIntelligenceBrief",
  ]) {
    const brief = next[briefKey];
    if (!brief || typeof brief !== "object") continue;
    const approach = brief.recommendedSalesApproach;
    const approachText =
      typeof approach === "string"
        ? approach
        : text(approach?.nextAction || approach?.openingApproach);
    if (applyEventsContactRewrite && /^Contact\s+/i.test(text(approachText))) {
      const cleaned = buildAssociationEventsOrgRouteSalesAction(next);
      next[briefKey] = {
        ...brief,
        recommendedSalesApproach:
          typeof approach === "object" && approach
            ? { ...approach, nextAction: cleaned, openingApproach: cleaned }
            : cleaned,
      };
    }
    if (
      applyEventsContactRewrite &&
      next.hasVerifiedNamedRelevantContact === false &&
      brief.whoToContact?.primary
    ) {
      // Display vs buyer eligibility: keep the named person for the shared
      // Who to Contact card; demote commercial-buyer flags only. Nulling the
      // person hid verified public identities from Events drawers.
      const priorPrimary = brief.whoToContact.primary;
      const keepPerson =
        priorPrimary &&
        typeof priorPrimary === "object" &&
        String(priorPrimary.name || "").trim();
      next[briefKey] = {
        ...next[briefKey],
        whoToContact: {
          ...next[briefKey].whoToContact,
          primary: keepPerson
            ? {
                ...priorPrimary,
                isNamedCommerciallyRelevant: false,
                buyerEligible: false,
                displayEligible: true,
              }
            : null,
          _demotedUnverifiedPrimaryBuyer: true,
          _clearedUnverifiedPrimary: !keepPerson,
        },
      };
    }
  }

  return next;
}

/**
 * Quarantine foreign-hotel sales thesis fields; keep evidence URLs.
 */
export function quarantineCrossHotelContent(opportunity = {}, hotel = {}, detection = null) {
  const det = detection || detectCrossHotelContamination(opportunity, hotel);
  if (!det.contaminated) return { ...opportunity, crossHotelContamination: null };

  const next = { ...opportunity };
  const hotelIdForName = hotel.hotelId || opportunity.hotelId || null;
  const hotelName =
    text(hotel.displayName || hotel.name) ||
    HOTEL_DISPLAY_NAME_BY_ID[hotelIdForName] ||
    text(opportunity.hotelDisplayName || opportunity.hotelName) ||
    "this hotel";

  const namesCurrentHotel = (blob) => {
    const b = text(blob);
    if (!b) return false;
    if (hotelName && b.toLowerCase().includes(hotelName.toLowerCase().slice(0, 28))) {
      return true;
    }
    if (
      hotelIdForName === "recgMYovrrZDJMqzX" ||
      /Waterstone Resort/i.test(hotelName)
    ) {
      // Possessive / short forms ("Waterstone's sales", "Boca Raton Marina")
      // are current-hotel copy — not foreign contamination.
      return (
        (/\bWaterstone(?:\s+Resort|['\u2019]s)?\b/i.test(b) ||
          /\bBoca Raton Marina\b/i.test(b)) &&
        !/\b(Bethesda Marriott|Wyndham San Diego|NIH\b|National Institutes of Health|AC Hotel A Coru[nñ]a|Grand Fiesta Americana Chapultepec|Nobu Hotel London)\b/i.test(
          b,
        )
      );
    }
    return false;
  };
  const isForeignHotelCopy = (value) => {
    const blob = text(value);
    if (!blob || !FOREIGN_HOTEL_RE.test(blob)) return false;
    // Current-hotel mentions in FOREIGN_HOTEL_RE (e.g. Waterstone on Waterstone) are not foreign.
    if (namesCurrentHotel(blob) && !/\b(Bethesda Marriott|Wyndham San Diego|NIH\b|National Institutes of Health|AC Hotel A Coru[nñ]a)\b/i.test(blob)) {
      return false;
    }
    return true;
  };
  const rewriteForeignHotelMentions = (value) => {
    const blob = text(value);
    if (!blob) return value;
    // Prefer targeted rewrite when the only contamination is a wrong hotel name
    // inside otherwise usable Waterstone/current-hotel sales copy.
    let rewritten = blob
      .replace(/\bBethesda Marriott\b/gi, hotelName)
      .replace(/\bNobu Hotel London(?: Portman Square)?\b/gi, hotelName)
      .replace(/\bWyndham San Diego Bayside\b/gi, hotelName)
      .replace(/\bGrand Fiesta Americana Chapultepec\b/gi, hotelName)
      .replace(
        /position NIH\s*\/\s*Metro\s*\/\s*parking advantages/gi,
        hotelIdForName === "recgMYovrrZDJMqzX" || /Waterstone/i.test(hotelName)
          ? "position Boca Raton marina access, meeting capacity, and South Palm Beach market proximity"
          : `position advantages relevant to ${hotelName}`,
      )
      .replace(
        /Bethesda Marriott capacity and DMV positioning may be relevant/gi,
        `${hotelName} capacity and local market positioning may be relevant`,
      );
    if (rewritten !== blob && !isForeignHotelCopy(rewritten)) {
      return rewritten;
    }
    if (!isForeignHotelCopy(value)) return value;
    return null;
  };

  next.recommendedAction = rewriteForeignHotelMentions(next.recommendedAction);
  next.whyThisHotel = rewriteForeignHotelMentions(next.whyThisHotel);
  next.hotelOpportunityThesis = rewriteForeignHotelMentions(
    next.hotelOpportunityThesis,
  );

  if (next.salesBrief && typeof next.salesBrief === "object") {
    const sb = { ...next.salesBrief };
    sb.whyThisHotel = rewriteForeignHotelMentions(sb.whyThisHotel);
    sb.companyAndLocalActivity = rewriteForeignHotelMentions(
      sb.companyAndLocalActivity,
    );
    sb.potentialBusinessForThisHotel = rewriteForeignHotelMentions(
      sb.potentialBusinessForThisHotel,
    );
    if (isForeignHotelCopy(sb.whyThisHotel)) {
      sb.whyThisHotel = `${hotelName} — prior brief referenced another hotel; re-research property-specific fit before sales use.`;
    }
    if (
      isForeignHotelCopy(
        typeof sb.recommendedSalesApproach === "string"
          ? sb.recommendedSalesApproach
          : sb.recommendedSalesApproach?.nextAction,
      )
    ) {
      sb.recommendedSalesApproach =
        "Re-qualify outreach for this property only — prior recommendation referenced a different hotel.";
    }
    next.salesBrief = sb;
  }

  if (next.commercialIntelligenceBrief && typeof next.commercialIntelligenceBrief === "object") {
    const cib = { ...next.commercialIntelligenceBrief };
    cib.whyThisHotel = rewriteForeignHotelMentions(cib.whyThisHotel);
    cib.companyAndLocalActivity = rewriteForeignHotelMentions(
      cib.companyAndLocalActivity,
    );
    if (isForeignHotelCopy(cib.whyThisHotel)) {
      cib.whyThisHotel = `${hotelName} — prior CIB referenced another hotel; do not use that thesis.`;
    }
    next.commercialIntelligenceBrief = cib;
  }

  if (!next.recommendedAction) {
    next.recommendedAction =
      `Confirm activity relevance to ${hotelName}, then identify the lodging/meeting owner via official organization channels. Do not use recommendations written for another hotel.`;
  }
  if (!next.whyThisHotel || GENERIC_FACILITIES_RE.test(next.whyThisHotel)) {
    if (CHAPULTEPEC_MARKERS_RE.test(text(next.eventCity) + text(next.eventLocationSummary) + text(next.title))) {
      next.whyThisHotel = `${hotelName} sits on the Reforma/Chapultepec corridor. Realistic roles are mid-size meetings, speaker/VIP overflow, or executive lodging when geography and timing align — not assumed as citywide congress HQ.`;
    }
  }

  next.crossHotelContamination = {
    version: PORTFOLIO_COMMERCIAL_INTEGRITY_V1,
    quarantined: true,
    hits: det.hits,
  };
  return next;
}

/**
 * Full integrity pass for one opportunity against its hotel profile.
 */
const HOTEL_DISPLAY_NAME_BY_ID = Object.freeze({
  recgMYovrrZDJMqzX: "Waterstone Resort & Marina, Curio Collection by Hilton",
  recterKmNPdntCm0v: "Grand Fiesta Americana Chapultepec",
  recIEcSWuhSCoNLFN: "Fiesta Americana Condesa Cancún All Inclusive",
  recC2kbCaS48jFjFF: "Nobu Hotel London Portman Square",
});

/**
 * Repair stale cross-hotel quarantine copy that named the wrong property
 * (e.g. Chapultepec default left on a Waterstone opportunity).
 */
function repairWrongHotelQuarantineCopy(opportunity = {}, hotelCtx = {}) {
  const hotelName = text(hotelCtx.displayName);
  const hotelId = hotelCtx.hotelId || opportunity.hotelId || null;
  if (!hotelName && !hotelId) return opportunity;
  const quarantineBoilerplateRe =
    /\s*[—-]\s*prior (?:CIB|brief) referenced another hotel/i;
  const next = { ...opportunity };
  const fix = (value) => {
    const blob = text(value);
    if (!blob || !quarantineBoilerplateRe.test(blob)) return value;
    const label =
      hotelName || HOTEL_DISPLAY_NAME_BY_ID[hotelId] || "This hotel";
    return `${label} sits in its verified market with guestroom and meeting inventory relevant to qualified demand — re-qualify property-specific fit before using a thesis written for another hotel.`;
  };
  next.whyThisHotel = fix(next.whyThisHotel);
  next.summaryWhyHotel = fix(next.summaryWhyHotel);
  next.hotelOpportunityThesis = fix(next.hotelOpportunityThesis);
  if (next.commercialIntelligenceBrief && typeof next.commercialIntelligenceBrief === "object") {
    next.commercialIntelligenceBrief = {
      ...next.commercialIntelligenceBrief,
      whyThisHotel: fix(next.commercialIntelligenceBrief.whyThisHotel),
    };
  }
  if (next.salesBrief && typeof next.salesBrief === "object") {
    next.salesBrief = {
      ...next.salesBrief,
      whyThisHotel: fix(next.salesBrief.whyThisHotel),
    };
  }
  return next;
}

/**
 * Prefer a non-quarantine top-level recommendedAction over stale CIB/salesBrief
 * quarantine boilerplate so Events drawers do not re-promote Re-qualify copy.
 */
export function preferNonQuarantineRecommendedAction(opportunity = {}) {
  const next = { ...opportunity };
  const top = text(next.recommendedAction);
  const isBoilerplate = (v) =>
    /Re-qualify outreach for this property only|prior recommendation referenced a different hotel/i.test(
      text(v),
    );
  if (!top || isBoilerplate(top)) return next;

  if (
    next.commercialIntelligenceBrief &&
    typeof next.commercialIntelligenceBrief === "object" &&
    isBoilerplate(next.commercialIntelligenceBrief.recommendedSalesApproach)
  ) {
    next.commercialIntelligenceBrief = {
      ...next.commercialIntelligenceBrief,
      recommendedSalesApproach: top,
    };
  }
  if (next.salesBrief && typeof next.salesBrief === "object") {
    const approach = next.salesBrief.recommendedSalesApproach;
    if (typeof approach === "string" && isBoilerplate(approach)) {
      next.salesBrief = {
        ...next.salesBrief,
        recommendedSalesApproach: top,
      };
    } else if (
      approach &&
      typeof approach === "object" &&
      isBoilerplate(approach.nextAction)
    ) {
      next.salesBrief = {
        ...next.salesBrief,
        recommendedSalesApproach: {
          ...approach,
          nextAction: top,
        },
      };
    }
  }
  if (
    next.associationEventsGoldBrief &&
    typeof next.associationEventsGoldBrief === "object" &&
    isBoilerplate(next.associationEventsGoldBrief.recommendedSalesApproach)
  ) {
    next.associationEventsGoldBrief = {
      ...next.associationEventsGoldBrief,
      recommendedSalesApproach: top,
    };
  }
  return next;
}

export function applyPortfolioCommercialIntegrity(opportunity = {}, hotel = {}) {
  const hotelId = hotel.hotelId || opportunity.hotelId || null;
  const hotelCtx = {
    hotelId,
    displayName:
      hotel.displayName ||
      hotel.name ||
      opportunity.hotelDisplayName ||
      opportunity.hotelName ||
      HOTEL_DISPLAY_NAME_BY_ID[hotelId] ||
      null,
  };

  let next = { ...opportunity, hotelId: opportunity.hotelId || hotelCtx.hotelId };
  next = preferNonQuarantineRecommendedAction(next);
  next = repairWrongHotelQuarantineCopy(next, hotelCtx);
  // Promote evidenceUrls into sources when drawer sources are empty (display honesty)
  if (
    (!Array.isArray(next.sources) || next.sources.length === 0) &&
    Array.isArray(next.evidenceUrls) &&
    next.evidenceUrls.length
  ) {
    next.sources = next.evidenceUrls
      .map((s) =>
        typeof s === "string"
          ? { url: s, title: null }
          : s?.url
            ? { url: s.url, title: s.title || null }
            : null,
      )
      .filter(Boolean);
  }
  const cross = detectCrossHotelContamination(next, hotelCtx);
  next = quarantineCrossHotelContent(next, hotelCtx, cross);
  next = remediateContactAttribution(next);
  // Consistency: verified-buyer flag requires an actionable named contact slot
  // (not merely an ineligible leadership scrape still nested under whoToContact).
  const actionableNamed = (person) => {
    if (!person) return false;
    const name = text(person.name || person.personName || person.fullName || person);
    if (!name || typeof person === "string") {
      return Boolean(name) && isAssociationEventsValidPersonName(name);
    }
    if (person.isRelevantCommercialEntryPoint === false) return false;
    if (person.isDirectBuyer === false && person.isRelevantCommercialEntryPoint !== true) {
      return false;
    }
    if (/^INELIGIBLE/i.test(text(person.contactEligibilityClass))) return false;
    return isAssociationEventsValidPersonName(name);
  };
  const namedSurvives =
    actionableNamed({ name: next.contactName, title: next.contactRole }) ||
    actionableNamed(next.primaryContact) ||
    actionableNamed(next.associationEventsGoldBrief?.whoToContact?.primary) ||
    actionableNamed(next.salesBrief?.whoToContact?.primary);
  if (next.hasVerifiedNamedRelevantContact === true && !namedSurvives) {
    next.hasVerifiedNamedRelevantContact = false;
    next._clearedOrphanVerifiedBuyerFlag = true;
    // Clear ineligible gold/sales primaries so they cannot re-assert verification.
    for (const briefKey of ["associationEventsGoldBrief", "salesBrief"]) {
      const brief = next[briefKey];
      if (!brief?.whoToContact?.primary) continue;
      if (actionableNamed(brief.whoToContact.primary)) continue;
      next[briefKey] = {
        ...brief,
        whoToContact: {
          ...brief.whoToContact,
          primary: null,
          _clearedIneligiblePrimary: true,
          _priorIneligiblePrimary: brief.whoToContact.primary,
        },
      };
    }
  }
  // Presentation contract: verified buyer must expose a top-level contactName
  // when the actionable name currently lives only on primaryContact / gold primary.
  if (next.hasVerifiedNamedRelevantContact === true && !text(next.contactName)) {
    const syncFrom =
      (actionableNamed(next.primaryContact) && next.primaryContact) ||
      (actionableNamed(next.associationEventsGoldBrief?.whoToContact?.primary) &&
        next.associationEventsGoldBrief.whoToContact.primary) ||
      (actionableNamed(next.salesBrief?.whoToContact?.primary) &&
        next.salesBrief.whoToContact.primary) ||
      null;
    if (syncFrom) {
      next.contactName = text(syncFrom.name || syncFrom.personName || syncFrom.fullName);
      if (!text(next.contactRole)) {
        next.contactRole = text(syncFrom.title || syncFrom.role || syncFrom.jobTitle) || null;
      }
      next._syncedContactNameFromPrimary = true;
    }
  }
  next = reconcileOpportunityQualificationDisplay(next);

  const evidence = recalibrateEvidenceConfidence(next);
  next.evidenceConfidence = evidence.evidenceConfidence;
  next.evidenceConfidenceExplanation = evidence.evidenceConfidenceExplanation;

  const gold = reconcileGoldBriefHonesty(next, evidence);
  next.goldBriefStatus = gold.goldBriefStatus;
  next.goldBriefComplete = gold.goldBriefComplete === true;
  if (gold.dosGrade) next.dosGrade = gold.dosGrade;
  next.integrityRemediation = {
    version: PORTFOLIO_COMMERCIAL_INTEGRITY_V1,
    at: new Date().toISOString(),
    crossHotel: cross,
    evidence,
    gold,
    contactClass: next.contactAttributionClass || null,
  };

  // Empty shells
  if (!text(next.title) && !text(next.organizationName) && !text(next.companyName)) {
    next.dosGrade = DOS_GRADE.DO_NOT_PURSUE;
    next.goldBriefStatus = GOLD_BRIEF_STATUS.INSUFFICIENT_COMMERCIAL_EVIDENCE;
    next.run044LifecycleDisposition = "REJECTED_OR_IRRELEVANT";
    next.integrityRemediation.emptyShell = true;
  }

  next.updatedAt = new Date().toISOString();
  return scrubAssociationEventsContactFields(next);
}

/**
 * Non-mutating regression helper: returns contamination result only.
 */
export function assertNoCrossHotelRecommendation(opportunity = {}, hotel = {}) {
  const det = detectCrossHotelContamination(opportunity, hotel);
  return {
    ok: !det.contaminated,
    ...det,
  };
}
