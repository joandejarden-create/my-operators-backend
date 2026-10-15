/**
 * Hotel-relevance research (mandatory coordinator output).
 *
 * Produces a canonical, hotel-specific research brief BEFORE final qualification
 * and card generation. Reuses hotel config, independent fit, contacts, and
 * existing opportunity evidence — not a separate pipeline or hand overlay.
 *
 * Rules:
 * - Attach sources to factual claims; label commercial inference explicitly.
 * - Do not infer open buying from unnamed hotel / TBA block / housing mechanism.
 * - Unknown room volume + unconfirmed sourcing alone are acceptable.
 * - Missing core answers → recover evidence → record precise gaps → INTERNAL.
 */

import { loadHotelConfig } from "./independent-hotel-fit-v1.js";
import { isGenericThesis } from "./drawer-content-contract-v1.js";
import {
  stripEvidenceClaimPrefixes,
  detectClaimKindFromProse,
  proseDedupeKey,
} from "./customer-facing-prose-v1.js";
import {
  assessHotelFitForOpportunity,
  applyHotelFitAssessmentToOpportunity,
} from "./hotel-fit-assessment-v1.js";

export const HOTEL_RELEVANCE_RESEARCH_VERSION = "gdi-hotel-relevance-research-v1";

export const HOTEL_RELEVANCE_DISPOSITION = Object.freeze({
  READY_FOR_CARD: "READY_FOR_CARD",
  INTERNAL_GAP: "INTERNAL_GAP",
  INSUFFICIENT: "INSUFFICIENT",
});

const CLAIM = Object.freeze({
  FACT: "FACT",
  INFERENCE: "INFERENCE",
  MIXED: "MIXED",
  UNKNOWN: "UNKNOWN",
});

function text(v) {
  return stripEvidenceClaimPrefixes(String(v || "").replace(/\s+/g, " ").trim());
}

function claimKindOf(raw, fallback = CLAIM.MIXED) {
  return detectClaimKindFromProse(raw) || fallback;
}

function asCustomerProse(raw, uncertaintySuffix = "") {
  const body = text(raw);
  if (!body) return "";
  if (!uncertaintySuffix) return body;
  if (body.toLowerCase().includes(uncertaintySuffix.toLowerCase().slice(0, 40))) {
    return body;
  }
  return `${body} ${uncertaintySuffix}`.trim();
}

/** Compatibility exports used by reviewed-prospect importer. */
export function stripClaimLabel(raw = "") {
  return stripEvidenceClaimPrefixes(raw);
}

export function toCustomerProse(raw = "") {
  return stripEvidenceClaimPrefixes(raw);
}

function hasSubstance(v, min = 40) {
  const t = text(v);
  return t.length >= min && !isGenericThesis(t);
}

function sourceList(opp = {}) {
  const urls = [];
  if (text(opp.officialSource)) urls.push(text(opp.officialSource));
  for (const s of opp.sources || []) {
    const u = text(s?.url || s);
    if (u) urls.push(u);
  }
  if (text(opp.primaryContact?.sourceUrl)) urls.push(text(opp.primaryContact.sourceUrl));
  if (text(opp.discoverySource)) urls.push(text(opp.discoverySource));
  return [...new Set(urls)];
}

function hotelIdentityBits(cfg = {}) {
  const name =
    text(cfg.displayName) ||
    text(cfg.name) ||
    text(cfg.aliases?.note)?.split(".")[0] ||
    "this hotel";
  const rooms = cfg.capabilityProfile?.totalGuestrooms ?? null;
  const soft = text(cfg.capabilityProfile?.softBrand);
  const territory = text(cfg.demandTerritory?.label);
  const city = text(cfg.city) || text(cfg.locale?.city);
  const includes = Array.isArray(cfg.demandTerritory?.includes)
    ? cfg.demandTerritory.includes
    : [];
  return { name, rooms, soft, territory, city, includes };
}

function classifyTerritoryHint(opp = {}, cfg = {}) {
  const blob = [
    opp.destinationStatus,
    opp.eventLocationSummary,
    opp.eventCity,
    opp.city,
    opp.venueCity,
    opp.title,
    opp.summaryWhat,
  ]
    .map(text)
    .join(" ")
    .toLowerCase();
  const kw = cfg.demandTerritory?.classificationKeywords || {};
  for (const [klass, words] of Object.entries(kw)) {
    for (const w of words || []) {
      if (blob.includes(String(w).toLowerCase())) return klass;
    }
  }
  if (/washington|district of columbia|\bdc\b/.test(blob)) {
    return "TERRITORY_COMPETITIVE";
  }
  return null;
}

function detectOpenBuyingInference(textBlob = "") {
  const t = text(textBlob);
  if (!t) return false;
  // Forbidden: treating unnamed / TBA as open RFP or confirmed buy
  if (
    /\b(open\s+rfp|confirmed\s+buying|lodging\s+demand\s+path\s+open|pursue\s+as\s+open\s+demand)\b/i.test(
      t
    )
  ) {
    return true;
  }
  if (
    /\b(hotel\s+not\s+named|hotel\s+tba|block\s+tba|sourcing\s+unconfirmed|venue\s+tba)\b/i.test(
      t
    ) &&
    /\b(open\s+demand|confirmed\s+block|host\s+hotel\s+locked)\b/i.test(t)
  ) {
    return true;
  }
  return false;
}

function recoverField(opp, keys) {
  for (const k of keys) {
    const v = opp?.[k];
    if (hasSubstance(v, 24)) return text(v);
  }
  return "";
}

/**
 * Synthesize hotel-relevance research for one candidate.
 */
export function synthesizeHotelRelevanceResearch(candidate = {}, opts = {}) {
  const hotelId = String(opts.hotelId || candidate.hotelId || "").trim();
  const cfg = opts.hotelConfig || loadHotelConfig(hotelId) || {};
  const fit = opts.fitAssessment || null;
  const contact = opts.contact || candidate.primaryContact || null;
  const hotel = hotelIdentityBits(cfg);
  const sources = sourceList(candidate);
  const verifiedAt =
    text(opts.verificationDate) ||
    text(candidate.lastVerifiedAt)?.slice(0, 10) ||
    new Date().toISOString().slice(0, 10);

  const account = text(candidate.organizationName || candidate.company || candidate.account);
  const occasion = text(candidate.title || candidate.opportunityName || candidate.occasion);
  const timing =
    text(candidate.timingNote) ||
    [candidate.eventStartDate, candidate.eventEndDate].filter(Boolean).join(" – ") ||
    text(candidate.eventTiming);
  const location =
    text(candidate.eventLocationSummary) ||
    text(candidate.destinationStatus) ||
    [candidate.eventCity, candidate.venueCity, candidate.city].filter(Boolean).join(", ");

  const gaps = [];
  const warnings = [];

  // 1) Account + occasion + cohort
  let cohort = recoverField(candidate, [
    "travelingCohort",
    "relevantTravelers",
    "cohortDescription",
  ]);
  let accountOccasion = "";
  if (account && occasion) {
    accountOccasion = `${account} — ${occasion}`;
    if (timing) accountOccasion += `. Timing: ${timing}`;
    if (location) accountOccasion += `. Location: ${location}`;
  } else if (occasion) {
    accountOccasion = occasion;
  }
  if (!account) gaps.push({ field: "account", reason: "organization_missing" });
  if (!occasion) gaps.push({ field: "occasion", reason: "title_missing" });
  let cohortClaim = CLAIM.MIXED;
  if (!cohort) {
    // Light synthesis from occasion type — claimKind INFERENCE (not prose prefix)
    if (/advocacy|legislative|capitol\s*hill/i.test(occasion + " " + account)) {
      cohort =
        "Advocacy / legislative attendees typically require overnight downtown lodging around Hill meetings when the program spans multiple days.";
      cohortClaim = CLAIM.INFERENCE;
    } else if (/conference|annual\s+meeting|summit|symposium/i.test(occasion)) {
      cohort =
        "Conference / meeting attendees and speakers are the plausible traveling cohort when the occasion is multi-day and not staff-only.";
      cohortClaim = CLAIM.INFERENCE;
    } else if (/tournament|cup|sports|soccer/i.test(occasion + " " + account)) {
      cohort =
        "Traveling teams / families are the plausible lodging cohort for stay-to-play sports demand when housing rules require overnight stays.";
      cohortClaim = CLAIM.INFERENCE;
    } else if (/private\s*event|venue\s*referral|evening\s*event/i.test(occasion)) {
      cohort =
        "Event guests needing overnight rooms near a venue without on-site lodging.";
      cohortClaim = CLAIM.INFERENCE;
    } else {
      gaps.push({
        field: "travelingCohort",
        reason: "no_cohort_evidence_and_no_defensible_inference",
      });
    }
  } else {
    cohortClaim = claimKindOf(candidate.travelingCohort || cohort, CLAIM.MIXED);
    cohort = text(cohort);
  }

  // 2) Hotel business mechanism — distinct from traveling cohort
  let mechanism = recoverField(candidate, [
    "whyHotelDemand",
    "hotelDemandThesis",
  ]);
  // Do not reuse summaryWhat / cohort as mechanism (causes duplicate drawer paragraphs)
  {
    const mechKey = proseDedupeKey(mechanism);
    const cohortKey = proseDedupeKey(cohort);
    if (
      mechKey &&
      cohortKey &&
      (mechKey === cohortKey ||
        mechKey.startsWith(cohortKey.slice(0, 64)) ||
        cohortKey.startsWith(mechKey.slice(0, 64)))
    ) {
      mechanism = "";
    }
  }
  let mechanismClaim = CLAIM.MIXED;
  const sourcingUnconfirmed = /unconfirm|unknown|tba|not\s*named|not\s*publicly\s*locked/i.test(
    `${candidate.venueSourcingStatus || ""} ${candidate.venueSourcingStatusLabel || ""} ${mechanism} ${accountOccasion}`
  );
  const uncertaintyNote =
    "Hotel/host is not confirmed — do not treat as open buying.";
  if (!hasSubstance(mechanism, 40)) {
    if (sourcingUnconfirmed && location) {
      mechanism = `${occasion || "This occasion"} is dated in ${location} with hotel sourcing unconfirmed — a plausible group-lodging conversation for ${hotel.name}, not proof of an open room block or RFP.`;
      mechanismClaim = CLAIM.INFERENCE;
    } else if (location) {
      mechanism = `Traveling attendees for ${occasion || "this occasion"} in ${location} can create overnight demand near the meeting footprint; confirm housing control before pursuit intensity increases.`;
      mechanismClaim = CLAIM.INFERENCE;
    } else {
      gaps.push({
        field: "hotelBusinessMechanism",
        reason: "no_mechanism_without_location_or_housing_evidence",
      });
    }
  } else {
    mechanismClaim = claimKindOf(candidate.whyHotelDemand || mechanism, CLAIM.MIXED);
    mechanism = text(mechanism);
    if (sourcingUnconfirmed) {
      mechanism = asCustomerProse(mechanism, uncertaintyNote);
      mechanismClaim = CLAIM.MIXED;
    }
  }

  // 3) Property-specific fit (prefer hotel-fit assessment when stretch)
  let fitText = recoverField(candidate, [
    "hotelOpportunityThesis",
    "fitExplanation",
    "fitRationale",
    "summaryWhyHotel",
  ]);
  let fitClaim = CLAIM.MIXED;
  const territoryClass = classifyTerritoryHint(candidate, cfg);
  const territoryLabel =
    cfg.demandTerritory?.displayLabels?.[territoryClass] || hotel.territory || hotel.city;
  if (!hasSubstance(fitText, 40) || !new RegExp(hotel.name.split(",")[0].slice(0, 12), "i").test(fitText) && !/\b(mayflower|bethesda|marriott|wyndham|renaissance|waterstone|autograph|connecticut|harbor|bayside|downtown|overflow)\b/i.test(fitText)) {
    const roomBit =
      hotel.rooms != null
        ? `~${hotel.rooms} guestrooms`
        : "configured group-meeting inventory";
    const brandBit = hotel.soft ? `${hotel.soft} positioning` : "full-service positioning";
    if (territoryClass === "OUTSIDE_REALISTIC_TERRITORY") {
      fitText = `Geography reads outside ${hotel.name}'s realistic demand territory (${territoryLabel || "see hotel config"}). Do not promote as core fit without new location evidence.`;
      fitClaim = CLAIM.INFERENCE;
      gaps.push({
        field: "propertySpecificFit",
        reason: "outside_realistic_territory",
      });
    } else if (territoryClass === "TERRITORY_STRETCH" || /arlington|alexandria|stretch/i.test(location + " " + fitText)) {
      fitText = `No supported primary-host advantage for ${hotel.name} (${roomBit}, ${brandBit}) versus the occasion geography (${location || "unconfirmed"}). Exploratory / stretch — pursue only as overflow or downtown preference if planners signal openness.`;
      fitClaim = CLAIM.INFERENCE;
    } else {
      fitText = `${hotel.name} (${roomBit}, ${brandBit}${hotel.territory ? `, ${hotel.territory}` : ""}) is a plausible lodging option if the meeting footprint stays ${territoryLabel || "in-territory"}. Confirm venue geography before primary-host pursuit.`;
      fitClaim = CLAIM.INFERENCE;
    }
  } else {
    fitClaim = claimKindOf(candidate.fitExplanation || fitText, CLAIM.MIXED);
    fitText = text(fitText);
  }
  if (fit?.outcome === "OUT_OF_TERRITORY" || fit?.fit?.outcome === "OUT_OF_TERRITORY") {
    fitText = `Independent fit marked out of territory — ${text(fit?.rationale || fit?.fit?.rationale) || "see independent hotel fit"}.`;
    fitClaim = CLAIM.FACT;
    gaps.push({ field: "propertySpecificFit", reason: "independent_fit_out_of_territory" });
  }

  // 4) Limitations — distinct purpose from "why this hotel"
  let limitations = recoverField(candidate, [
    "competitiveContextNote",
    "materialLimitations",
    "limitations",
  ]);
  const venueHistory = recoverField(candidate, ["venueHistoryNote", "venueHistory"]);
  if (!hasSubstance(limitations, 24)) {
    const bits = [];
    if (sourcingUnconfirmed) bits.push("Host hotel / housing list not publicly locked.");
    if (!candidate.estimatedPeakRooms && !candidate.peakRooms) {
      bits.push("Peak rooms not published (acceptable unknown).");
    }
    if (territoryClass === "TERRITORY_STRETCH") {
      bits.push("Geography is stretch for this property — overflow posture only.");
    }
    if (/grand hyatt|crystal gateway|hilton national mall|omni shoreham/i.test(`${candidate.summaryWhat} ${candidate.venueHistoryNote} ${accountOccasion}`)) {
      bits.push("A primary host may already be named — pitch overflow/preferred only.");
    }
    if (bits.length) {
      limitations = bits.join(" ");
    } else {
      limitations =
        "Not fully researched — competing hotels and prior hosts not exhaustively assessed this pass.";
      warnings.push("limitations_thin");
    }
  }
  limitations = text(limitations)
    // Collapse prior-run duplication of venue-history appendages
    .replace(
      /\s*(?:Venue history note|Prior host pattern):\s*[^.]*\.\s*(?:(?:Venue history note|Prior host pattern):\s*[^.]*\.)+/gi,
      (m) => {
        const first = m.match(/(?:Venue history note|Prior host pattern):\s*[^.]*\./i);
        return first ? ` ${first[0]}` : m;
      }
    )
    .replace(/\s{2,}/g, " ")
    .trim();
  // Keep venue history only when it adds distinct facts
  {
    const limKey = proseDedupeKey(limitations);
    const histKey = proseDedupeKey(venueHistory);
    const histAlready =
      !histKey ||
      limKey === histKey ||
      limKey.includes(histKey.slice(0, 40)) ||
      /venue history note:|prior host pattern:/i.test(limitations);
    if (venueHistory && histKey && !histAlready) {
      limitations = `${limitations} Prior host pattern: ${text(venueHistory)}`.trim();
    }
  }

  // 5) Timing rationale
  let timingRationale = recoverField(candidate, ["whyNow", "summaryWhyNow", "cardWhyNowLine"]);
  let timingClaim = CLAIM.MIXED;
  if (!hasSubstance(timingRationale, 24)) {
    if (timing && sourcingUnconfirmed) {
      timingRationale = `Dates/city are public (${timing}) while hotel selection remains unconfirmed — the practical window to ask about housing process is before a host announcement. Do not treat as open demand.`;
      timingClaim = CLAIM.INFERENCE;
    } else if (timing) {
      timingRationale = `Timing evidence: ${timing}. Confirm whether lodging contracting is still open before intensifying outreach.`;
      timingClaim = CLAIM.MIXED;
    } else {
      gaps.push({ field: "timingRationale", reason: "no_timing_evidence" });
    }
  } else {
    timingClaim = claimKindOf(candidate.whyNow || timingRationale, CLAIM.MIXED);
    timingRationale = text(timingRationale);
  }

  // 6) Contact route
  const contactName = text(contact?.name || candidate.primaryContactName);
  const contactEmail = text(contact?.email || candidate.primaryContact?.email || candidate.functionalContactEmail);
  const contactPhone = text(contact?.phone || candidate.primaryContact?.phone);
  const contactUrl = text(
    candidate.publicContactPath ||
      candidate.organizationContactUrl ||
      contact?.sourceUrl
  );
  const contactRelevance = text(
    contact?.relevanceEvidence ||
      contact?.whyThisContact ||
      candidate.contactRelevance ||
      candidate.whyThisContact
  );
  const hasRoute = Boolean(
    contactEmail ||
      contactPhone ||
      (contactUrl && !/^https?:\/\/[^/]+\/?$/i.test(contactUrl))
  );
  if (!hasRoute) {
    gaps.push({ field: "contactRoute", reason: "no_usable_contact_route" });
  } else if (contactName && /\s/.test(contactName) && !contactRelevance) {
    warnings.push("contact_relevance_thin");
  }
  const contactRoute = {
    person: contactName || null,
    role: text(contact?.role || contact?.title || candidate.primaryContactRole) || null,
    method: contactEmail || contactUrl || null,
    phone: contactPhone || null,
    responsibility:
      text(candidate.bookingResponsibilityLabel || candidate.bookingResponsibilityClass) ||
      "Potential relevant contact — not proven booking authority",
    relevance: contactRelevance || null,
    claimKind: contactEmail || contactPhone ? CLAIM.FACT : CLAIM.MIXED,
  };

  // 7) Next sales action
  let nextAction = recoverField(candidate, [
    "recommendedAction",
    "recommendedNextStep",
    "recommendedFirstAction",
  ]);
  const specificQuestion = text(candidate.specificQuestion);
  if (
    !hasSubstance(nextAction, 24) ||
    /^ask whether hotel selection\s*\/\s*lodging plans are open/i.test(nextAction)
  ) {
    if (contactName && contactEmail) {
      nextAction = `Ask ${contactName} whether lodging/housing for ${occasion || "this occasion"} is open and whether ${hotel.name} can be considered for primary, overflow, or preferred listing.`;
    } else if (contactEmail || contactUrl) {
      nextAction = `Using the published contact route, ask whether lodging plans for ${occasion || "this occasion"} are open and whether ${hotel.name} belongs on the hotel list (primary/overflow/preferred).`;
    } else {
      gaps.push({
        field: "nextSalesAction",
        reason: "no_specific_action_without_contact_route",
      });
      nextAction = "";
    }
  }

  // Open-buying guard
  const openBuyingInferred = detectOpenBuyingInference(
    [mechanism, fitText, timingRationale, nextAction].join(" ")
  );
  if (openBuyingInferred) {
    gaps.push({
      field: "openBuyingGuard",
      reason: "text_implies_open_buying_without_host_confirmation",
    });
  }

  const coreMissing = gaps
    .map((g) => g.field)
    .filter((f) =>
      [
        "account",
        "occasion",
        "hotelBusinessMechanism",
        "propertySpecificFit",
        "timingRationale",
        "contactRoute",
        "nextSalesAction",
        "openBuyingGuard",
      ].includes(f)
    );

  let disposition = HOTEL_RELEVANCE_DISPOSITION.READY_FOR_CARD;
  if (coreMissing.length >= 3 || gaps.some((g) => g.field === "openBuyingGuard")) {
    disposition = HOTEL_RELEVANCE_DISPOSITION.INSUFFICIENT;
  } else if (coreMissing.length > 0 || gaps.some((g) => g.field === "propertySpecificFit" && g.reason === "independent_fit_out_of_territory")) {
    disposition = HOTEL_RELEVANCE_DISPOSITION.INTERNAL_GAP;
  }
  // Contact gap alone → internal enrichment, not discard of research
  if (
    disposition === HOTEL_RELEVANCE_DISPOSITION.READY_FOR_CARD &&
    gaps.some((g) => g.field === "contactRoute")
  ) {
    disposition = HOTEL_RELEVANCE_DISPOSITION.INTERNAL_GAP;
  }

  const research = {
    version: HOTEL_RELEVANCE_RESEARCH_VERSION,
    hotelId,
    opportunityId: candidate.id || candidate.opportunityId || null,
    cardId: candidate.prospectReviewCardId || candidate.cardId || null,
    researchedAt: new Date().toISOString(),
    verificationDate: verifiedAt,
    disposition,
    completeness: {
      ok: disposition === HOTEL_RELEVANCE_DISPOSITION.READY_FOR_CARD,
      missingCore: coreMissing,
      warnings,
      gaps,
    },
    accountOccasion: {
      text: accountOccasion,
      account,
      occasion,
      claimKind: sources.length ? CLAIM.MIXED : CLAIM.UNKNOWN,
      sources: sources.slice(0, 6),
    },
    travelingCohort: {
      text: text(cohort),
      claimKind: cohortClaim,
    },
    hotelBusinessMechanism: {
      text: text(mechanism),
      claimKind: mechanismClaim,
    },
    propertySpecificFit: {
      text: text(fitText),
      claimKind: fitClaim,
      territoryClass,
      territoryLabel,
      hotelName: hotel.name,
      totalGuestrooms: hotel.rooms,
    },
    limitations: {
      text: text(limitations),
      claimKind: CLAIM.MIXED,
    },
    timingRationale: {
      text: text(timingRationale),
      claimKind: timingClaim,
      timingRaw: timing || null,
    },
    contactRoute,
    nextSalesAction: {
      text: nextAction,
      specificQuestion: specificQuestion || null,
    },
    openBuyingInferred: openBuyingInferred === true,
    sources: sources.map((url) => ({
      url,
      verificationDate: verifiedAt,
      claimKind: CLAIM.FACT,
    })),
    independentFitOutcome:
      fit?.outcome || fit?.fit?.outcome || null,
  };

  return research;
}

/**
 * Map research brief onto opportunity fields used by cards/drawers/importer.
 */
export function applyHotelRelevanceToOpportunity(opportunity = {}, research = {}) {
  if (!research || research.version !== HOTEL_RELEVANCE_RESEARCH_VERSION) {
    return opportunity;
  }
  const next = { ...opportunity };
  next.hotelRelevanceResearch = research;
  next.hotelRelevanceResearchVersion = HOTEL_RELEVANCE_RESEARCH_VERSION;
  next.hotelRelevanceDisposition = research.disposition;
  next.hotelRelevanceResearchedAt = research.researchedAt;
  next.researchVersion = HOTEL_RELEVANCE_RESEARCH_VERSION;
  if (research.verificationDate) {
    next.lastVerifiedAt = `${research.verificationDate}T00:00:00.000Z`;
  }

  if (hasSubstance(research.travelingCohort?.text)) {
    next.travelingCohort = text(research.travelingCohort.text);
    next.relevantTravelers = text(research.travelingCohort.text);
  }
  if (hasSubstance(research.hotelBusinessMechanism?.text)) {
    const mech = text(research.hotelBusinessMechanism.text);
    next.whyHotelDemand = mech;
    // Keep account/occasion summary distinct from demand mechanism when possible
    if (!hasSubstance(next.summaryWhat, 40) || proseDedupeKey(next.summaryWhat) === proseDedupeKey(mech)) {
      next.summaryWhat =
        text(research.accountOccasion?.text) || mech;
    }
  }
  if (hasSubstance(research.propertySpecificFit?.text)) {
    const fit = text(research.propertySpecificFit.text);
    next.hotelOpportunityThesis = fit;
    next.summaryWhyHotel = fit;
    next.fitExplanation = fit;
    next.fitRationale = fit;
  }
  if (hasSubstance(research.limitations?.text)) {
    next.competitiveContextNote = text(research.limitations.text);
  }
  if (hasSubstance(research.timingRationale?.text)) {
    next.whyNow = text(research.timingRationale.text);
    next.summaryWhyNow = text(research.timingRationale.text);
  }
  if (hasSubstance(research.nextSalesAction?.text)) {
    next.recommendedAction = text(research.nextSalesAction.text);
    next.recommendedNextStep = text(research.nextSalesAction.text);
  }
  if (research.nextSalesAction?.specificQuestion) {
    next.specificQuestion = research.nextSalesAction.specificQuestion;
  }
  if (research.accountOccasion?.text) {
    const fitSlice = text(research.propertySpecificFit?.text).slice(0, 220);
    const whyMatters = `Why ${research.propertySpecificFit?.hotelName || "this hotel"}: ${fitSlice}`;
    if (
      !hasSubstance(next.summaryWhyMatters, 40) ||
      proseDedupeKey(next.summaryWhyMatters) === proseDedupeKey(next.travelingCohort) ||
      proseDedupeKey(next.summaryWhyMatters) === proseDedupeKey(next.whyHotelDemand)
    ) {
      next.summaryWhyMatters = whyMatters;
    }
  }
  if (Array.isArray(research.sources) && research.sources.length) {
    const existing = Array.isArray(next.sources) ? next.sources : [];
    const merged = [...existing, ...research.sources].filter(
      (s, i, arr) => s?.url && arr.findIndex((x) => x.url === s.url) === i
    );
    next.sources = merged;
  }
  // Facing gate hint — qualification still owns hotelFacing
  if (research.disposition === HOTEL_RELEVANCE_DISPOSITION.INTERNAL_GAP) {
    next.prospectInternalOnly = next.prospectInternalOnly || true;
    next.hotelRelevanceInternalReason = (research.completeness?.gaps || [])
      .map((g) => g.reason)
      .join("; ");
  }
  if (research.disposition === HOTEL_RELEVANCE_DISPOSITION.INSUFFICIENT) {
    next.prospectHotelFacing = false;
    next.prospectInternalOnly = true;
    next.hotelRelevanceInternalReason = (research.completeness?.gaps || [])
      .map((g) => `${g.field}:${g.reason}`)
      .join("; ");
  }

  // Persist structured hotel-fit assessment during research (no manual overlay required)
  const fitAssessment = assessHotelFitForOpportunity(next, {
    hotelId: research.hotelId || next.hotelId,
  });
  Object.assign(next, applyHotelFitAssessmentToOpportunity(next, fitAssessment));
  next.hotelRelevanceResearch = {
    ...research,
    hotelFitAssessment: fitAssessment,
  };

  next.drawerContentContractVersion = "gdi-drawer-content-contract-v3";
  return next;
}

/**
 * Batch synthesize for a hotel run.
 */
export function synthesizeHotelRelevanceResearchBatch(
  opportunities = [],
  hotelId,
  opts = {}
) {
  const cfg = opts.hotelConfig || loadHotelConfig(hotelId);
  const fitById = new Map(
    (opts.fitAssessments || []).map((f) => [
      f.opportunityId || f.candidateId || f.id,
      f,
    ])
  );
  const contactById = new Map(
    (opts.contactResults || []).map((c) => [c.opportunityId, c])
  );

  const results = [];
  for (const opp of opportunities || []) {
    const contactResult = contactById.get(opp.id);
    const contact =
      contactResult?.after ||
      contactResult?.updatedOpportunityPatch?.primaryContact ||
      opp.primaryContact;
    const research = synthesizeHotelRelevanceResearch(opp, {
      hotelId,
      hotelConfig: cfg,
      fitAssessment: fitById.get(opp.id),
      contact,
      verificationDate: opts.verificationDate,
    });
    const patched = applyHotelRelevanceToOpportunity(opp, research);
    results.push({
      opportunityId: opp.id,
      cardId: opp.prospectReviewCardId || null,
      disposition: research.disposition,
      research,
      opportunity: patched,
    });
  }

  const counts = {
    total: results.length,
    readyForCard: results.filter(
      (r) => r.disposition === HOTEL_RELEVANCE_DISPOSITION.READY_FOR_CARD
    ).length,
    internalGap: results.filter(
      (r) => r.disposition === HOTEL_RELEVANCE_DISPOSITION.INTERNAL_GAP
    ).length,
    insufficient: results.filter(
      (r) => r.disposition === HOTEL_RELEVANCE_DISPOSITION.INSUFFICIENT
    ).length,
  };

  return {
    version: HOTEL_RELEVANCE_RESEARCH_VERSION,
    hotelId,
    generatedAt: new Date().toISOString(),
    counts,
    results,
  };
}

/**
 * Build sales-review overlay cards from research results (reproducible path).
 */
export function researchBatchToSalesReviewOverlay(batch = {}, opts = {}) {
  const hotelKey = opts.hotelKey || "hotel";
  const cards = [];
  for (const row of batch.results || []) {
    const r = row.research;
    if (!r) continue;
    if (r.disposition === HOTEL_RELEVANCE_DISPOSITION.INSUFFICIENT) continue;
    const cardId =
      row.cardId ||
      r.cardId ||
      row.opportunity?.prospectReviewCardId ||
      `${hotelKey}-${text(r.accountOccasion?.account)
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .slice(0, 32)}`;
    // Preserve existing segment type (association / corporate / sports / PE).
    // Only refine to OVERFLOW when geography/fit text clearly indicates overflow.
    const existingType = text(row.opportunity?.opportunityType);
    const overflowHint = /overflow|stretch|arlington|alexandria|secondary\s+lodging/i.test(
      `${r.propertySpecificFit?.text} ${r.limitations?.text}`
    );
    cards.push({
      hotelKey,
      cardId,
      forceEditorial: true,
      editorialVersion: HOTEL_RELEVANCE_RESEARCH_VERSION,
      fromHotelRelevanceResearch: true,
      ...(overflowHint
        ? { opportunityType: "OVERFLOW_HOUSING" }
        : existingType
          ? { opportunityType: existingType }
          : {}),
      travelers: r.travelingCohort?.text || null,
      whatWeKnow: r.hotelBusinessMechanism?.text || null,
      whyFits:
        row.opportunity?.hotelFitAssessment?.summary ||
        r.propertySpecificFit?.text ||
        null,
      hotelFitAssessment:
        row.opportunity?.hotelFitAssessment?.summary ||
        r.propertySpecificFit?.text ||
        null,
      hotelFitAssessmentStructured:
        row.opportunity?.hotelFitAssessment || r.hotelFitAssessment || null,
      whyMatters: r.propertySpecificFit?.text || null,
      whyNow: r.timingRationale?.text || null,
      limitations: r.limitations?.text || null,
      nextAction: r.nextSalesAction?.text || null,
      specificQuestion: r.nextSalesAction?.specificQuestion || null,
      contactRelevance: r.contactRoute?.relevance || null,
      destinationStatus: text(row.opportunity?.destinationStatus) || null,
      eventLocationSummary: text(row.opportunity?.eventLocationSummary) || null,
      sourceUrls: (r.sources || []).map((s) => s.url).filter(Boolean),
      hotelRelevanceDisposition: r.disposition,
      hotelRelevanceResearch: r,
    });
  }
  return {
    asOf: new Date().toISOString(),
    editorialVersion: HOTEL_RELEVANCE_RESEARCH_VERSION,
    note: "Generated from coordinator hotel-relevance research stage — not a hand overlay.",
    hotelId: batch.hotelId,
    counts: batch.counts,
    cards,
  };
}

export default {
  HOTEL_RELEVANCE_RESEARCH_VERSION,
  HOTEL_RELEVANCE_DISPOSITION,
  synthesizeHotelRelevanceResearch,
  applyHotelRelevanceToOpportunity,
  synthesizeHotelRelevanceResearchBatch,
  researchBatchToSalesReviewOverlay,
};
