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
  computeHotelFitScore,
  HOTEL_FIT_COMPONENT_LABELS,
  DEFAULT_HOTEL_FIT_WEIGHTS,
} from "../scoring.js";

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
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Strip internal evidence labels from customer-facing prose.
 * Classification stays on structured claimKind metadata.
 */
export function stripClaimLabel(raw = "") {
  return text(raw)
    .replace(/^(FACT|INFERENCE|HYPOTHESIS|MIXED|UNKNOWN|FACT\/FIT-GATE)\s*:\s*/i, "")
    .replace(/\s*\(\s*hotel\/host not confirmed[^)]*\)\.?\s*$/i, "")
    .trim();
}

export function toCustomerProse(raw = "") {
  const body = stripClaimLabel(raw);
  if (!body) return "";
  // Prefer a natural sentence; keep uncertainty without META: prefixes.
  return body.replace(/\s{2,}/g, " ").trim();
}

function detectClaimKind(raw = "") {
  const t = text(raw);
  if (/^FACT\s*:/i.test(t)) return CLAIM.FACT;
  if (/^INFERENCE\s*:/i.test(t)) return CLAIM.INFERENCE;
  if (/^MIXED\s*:/i.test(t)) return CLAIM.MIXED;
  if (/^UNKNOWN\s*:/i.test(t)) return CLAIM.UNKNOWN;
  return null;
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
  let cohortClaimKind = detectClaimKind(cohort) || CLAIM.MIXED;
  if (!cohort) {
    // Light synthesis from occasion type — claimKind INFERENCE (label not in prose)
    cohortClaimKind = CLAIM.INFERENCE;
    if (/advocacy|legislative|capitol\s*hill/i.test(occasion + " " + account)) {
      cohort =
        "Advocacy / legislative attendees typically require overnight downtown lodging around Hill meetings when the program spans multiple days.";
    } else if (/conference|annual\s+meeting|summit|symposium/i.test(occasion)) {
      cohort =
        "Conference / meeting attendees and speakers are the plausible traveling cohort when the occasion is multi-day and not staff-only.";
    } else if (/tournament|cup|sports|soccer/i.test(occasion + " " + account)) {
      cohort =
        "Traveling teams / families are the plausible lodging cohort for stay-to-play sports demand when housing rules require overnight stays.";
    } else if (/private\s*event|venue\s*referral|evening\s*event/i.test(occasion)) {
      cohort =
        "Event guests needing overnight rooms near a venue without on-site lodging.";
    } else {
      gaps.push({
        field: "travelingCohort",
        reason: "no_cohort_evidence_and_no_defensible_inference",
      });
    }
  } else {
    cohort = toCustomerProse(cohort);
  }

  // 2) Hotel business mechanism
  let mechanism = recoverField(candidate, [
    "whyHotelDemand",
    "hotelDemandThesis",
  ]);
  const sourcingUnconfirmed = /unconfirm|unknown|tba|not\s*named|not\s*publicly\s*locked/i.test(
    `${candidate.venueSourcingStatus || ""} ${candidate.venueSourcingStatusLabel || ""} ${mechanism} ${accountOccasion}`
  );
  let mechanismClaimKind = detectClaimKind(mechanism) || CLAIM.MIXED;
  if (!hasSubstance(mechanism, 40)) {
    mechanismClaimKind = CLAIM.INFERENCE;
    if (sourcingUnconfirmed && location) {
      mechanism = `${occasion || "This occasion"} is dated in ${location} with hotel sourcing unconfirmed — a plausible group-lodging conversation for ${hotel.name}, not proof of an open room block or RFP.`;
    } else if (location) {
      mechanism = `Traveling attendees for ${occasion || "this occasion"} in ${location} can create overnight demand near the meeting footprint; confirm housing control before pursuit intensity increases.`;
    } else {
      gaps.push({
        field: "hotelBusinessMechanism",
        reason: "no_mechanism_without_location_or_housing_evidence",
      });
    }
  } else if (sourcingUnconfirmed) {
    // Keep uncertainty in metadata + natural prose — never leak "MIXED:" into drawers.
    mechanismClaimKind = CLAIM.MIXED;
    const clean = toCustomerProse(mechanism);
    if (clean && !/hotel\/host not confirmed|do not treat as open buying/i.test(clean)) {
      mechanism = `${clean} Hotel/host is not confirmed — do not treat as open buying.`;
    } else {
      mechanism = clean || mechanism;
    }
  } else {
    mechanism = toCustomerProse(mechanism) || mechanism;
  }

  // 3) Property-specific fit
  let fitText = recoverField(candidate, [
    "hotelOpportunityThesis",
    "fitExplanation",
    "fitRationale",
    "summaryWhyHotel",
  ]);
  const territoryClass = classifyTerritoryHint(candidate, cfg);
  const territoryLabel =
    cfg.demandTerritory?.displayLabels?.[territoryClass] || hotel.territory || hotel.city;
  let fitClaimKind = detectClaimKind(fitText) || CLAIM.MIXED;
  if (
    !hasSubstance(fitText, 40) ||
    (!new RegExp(hotel.name.split(",")[0].slice(0, 12), "i").test(fitText) &&
      !/\b(mayflower|bethesda|marriott|wyndham|renaissance|waterstone|autograph|connecticut|harbor|bayside|downtown|overflow)\b/i.test(
        fitText
      ))
  ) {
    fitClaimKind = CLAIM.INFERENCE;
    const roomBit =
      hotel.rooms != null
        ? `~${hotel.rooms} guestrooms`
        : "configured group-meeting inventory";
    const brandBit = hotel.soft ? `${hotel.soft} positioning` : "full-service positioning";
    if (territoryClass === "OUTSIDE_REALISTIC_TERRITORY") {
      fitText = `Geography reads outside ${hotel.name}'s realistic demand territory (${territoryLabel || "see hotel config"}). Do not promote as core fit without new location evidence.`;
      gaps.push({
        field: "propertySpecificFit",
        reason: "outside_realistic_territory",
      });
    } else if (
      territoryClass === "TERRITORY_STRETCH" ||
      /arlington|bethesda|alexandria|stretch/i.test(location + " " + fitText)
    ) {
      fitText = `${hotel.name} (${roomBit}, ${brandBit}) is a stretch vs the occasion geography (${location || "unconfirmed"}). Pursue only as overflow/downtown preference — not as automatic primary host.`;
    } else {
      fitText = `${hotel.name} (${roomBit}, ${brandBit}${hotel.territory ? `, ${hotel.territory}` : ""}) is a plausible lodging option if the meeting footprint stays ${territoryLabel || "in-territory"}. Confirm venue geography before primary-host pursuit.`;
    }
  } else {
    fitText = toCustomerProse(fitText);
  }
  if (fit?.outcome === "OUT_OF_TERRITORY" || fit?.fit?.outcome === "OUT_OF_TERRITORY") {
    fitClaimKind = CLAIM.FACT;
    fitText = `Independent fit marked out of territory — ${toCustomerProse(fit?.rationale || fit?.fit?.rationale) || "see independent hotel fit"}.`;
    gaps.push({ field: "propertySpecificFit", reason: "independent_fit_out_of_territory" });
  }

  // 4) Limitations
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
  if (venueHistory && !limitations.includes(venueHistory.slice(0, 40))) {
    limitations = `${limitations} Venue history note: ${venueHistory}`.trim();
  }

  // 5) Timing rationale
  let timingRationale = recoverField(candidate, ["whyNow", "summaryWhyNow", "cardWhyNowLine"]);
  let timingClaimKind = detectClaimKind(timingRationale) || CLAIM.MIXED;
  if (!hasSubstance(timingRationale, 24)) {
    timingClaimKind = CLAIM.INFERENCE;
    if (timing && sourcingUnconfirmed) {
      timingRationale = `Dates/city are public (${timing}) while hotel selection remains unconfirmed — the practical window to ask about housing process is before a host announcement. Do not treat as open demand.`;
    } else if (timing) {
      timingRationale = `Timing evidence: ${timing}. Confirm whether lodging contracting is still open before intensifying outreach.`;
    } else {
      gaps.push({ field: "timingRationale", reason: "no_timing_evidence" });
    }
  } else {
    timingRationale = toCustomerProse(timingRationale);
  }
  if (hasSubstance(limitations, 8)) limitations = toCustomerProse(limitations);

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
      text: toCustomerProse(cohort) || cohort,
      claimKind: cohortClaimKind,
    },
    hotelBusinessMechanism: {
      text: toCustomerProse(mechanism) || mechanism,
      claimKind: mechanismClaimKind,
    },
    propertySpecificFit: {
      text: toCustomerProse(fitText) || fitText,
      claimKind: fitClaimKind,
      territoryClass,
      territoryLabel,
      hotelName: hotel.name,
      totalGuestrooms: hotel.rooms,
    },
    limitations: {
      text: toCustomerProse(limitations) || limitations,
      claimKind: CLAIM.MIXED,
    },
    timingRationale: {
      text: toCustomerProse(timingRationale) || timingRationale,
      claimKind: timingClaimKind,
      timingRaw: timing || null,
    },
    contactRoute,
    nextSalesAction: {
      text: toCustomerProse(nextAction) || nextAction,
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

  research.hotelFitAssessment = buildHotelFitAssessmentFromResearch(research, {
    candidate,
    hotel,
    contactRoute,
    sourcingUnconfirmed,
    territoryClass,
  });

  return research;
}

/**
 * Dimension-level hotel-fit assessment using the supported scoring model.
 * Emits numeric scores only when every required component has a defensible input.
 */
export function buildHotelFitAssessmentFromResearch(research = {}, opts = {}) {
  const candidate = opts.candidate || {};
  const hotel = opts.hotel || {};
  const contactRoute = opts.contactRoute || research.contactRoute || {};
  const territoryClass =
    opts.territoryClass || research.propertySpecificFit?.territoryClass || null;
  const sourcingUnconfirmed = opts.sourcingUnconfirmed === true;
  const location =
    text(candidate.eventLocationSummary) ||
    text(candidate.destinationStatus) ||
    [candidate.eventCity, candidate.venueCity].filter(Boolean).join(", ");
  const stretch =
    territoryClass === "TERRITORY_STRETCH" ||
    /stretch|overflow only|arlington|crystal gateway/i.test(
      `${research.propertySpecificFit?.text || ""} ${location}`
    );
  const hostPlaced =
    /primary (hotel|host|lodging).*(locked|placed|contracted)|crystal gateway/i.test(
      `${candidate.destinationStatus || ""} ${candidate.eventLocationSummary || ""} ${research.limitations?.text || ""}`
    );
  const rooms = hotel.rooms ?? research.propertySpecificFit?.totalGuestrooms ?? null;
  const peakRaw = candidate.peakRooms ?? candidate.estimatedPeakRooms;
  const peakNum = Number(String(peakRaw || "").replace(/[^\d.].*$/, ""));
  const hasPeak = Number.isFinite(peakNum) && peakNum > 0;

  const dimensions = {
    physicalFit: {
      label: HOTEL_FIT_COMPONENT_LABELS.physicalFit,
      score: hasPeak && rooms != null
        ? peakNum <= rooms * 0.45
          ? 78
          : peakNum <= rooms
            ? 62
            : 40
        : null,
      assessment: hasPeak && rooms != null
        ? `Published/estimated peak (~${peakRaw}) vs ~${rooms} guestrooms — ${
            peakNum <= rooms ? "within property scale" : "above typical single-hotel block scale"
          }.`
        : rooms != null
          ? `Property scale (~${rooms} guestrooms) is known; peak-room need is unpublished — physical fit stays open.`
          : "Peak rooms and property inventory are not both evidenced — physical fit not scored.",
    },
    geographyFit: {
      label: HOTEL_FIT_COMPONENT_LABELS.geographyFit,
      score:
        territoryClass === "OUTSIDE_REALISTIC_TERRITORY"
          ? 15
          : stretch
            ? 38
            : territoryClass === "TERRITORY_COMPETITIVE"
              ? 62
              : territoryClass === "TERRITORY_NEARBY" || territoryClass === "TERRITORY_CORE"
                ? 82
                : location
                  ? 55
                  : null,
      assessment: stretch
        ? `Occasion geography (${location || "unconfirmed"}) is a stretch for ${hotel.name || "this hotel"} (${research.propertySpecificFit?.territoryLabel || "configured territory"}). Travel practicality is weaker than an in-footprint host.`
        : location
          ? `Occasion geography reads as ${location}; territory class ${territoryClass || "unclassified"} for ${hotel.name || "this hotel"}.`
          : "Occasion geography is not confirmed — demand-territory fit not scored.",
    },
    timing: {
      label: HOTEL_FIT_COMPONENT_LABELS.timing,
      score: sourcingUnconfirmed ? 68 : hostPlaced ? 35 : 55,
      assessment: sourcingUnconfirmed
        ? "Hotel/host not locked — still a window to ask about lodging process before a host announcement."
        : hostPlaced
          ? "A primary host appears placed — timing favors overflow/preferred inquiry only, not a primary bid."
          : "Timing evidence is partial; confirm whether lodging contracting is still open.",
    },
    commercialValue: {
      label: HOTEL_FIT_COMPONENT_LABELS.commercialValue,
      score: hasPeak ? (peakNum >= 40 ? 70 : 52) : null,
      assessment: hasPeak
        ? `Commercial scale signal from peak rooms (~${peakRaw}); not proof of an open block.`
        : "Commercial scale not established from peak rooms — keep as qualitative until volume is known.",
    },
    historicalFit: {
      label: HOTEL_FIT_COMPONENT_LABELS.historicalFit,
      score: /arlington|crystal/i.test(location + " " + (research.limitations?.text || ""))
        ? 32
        : /historically|prior host|past host/i.test(research.limitations?.text || "")
          ? 58
          : null,
      assessment: /arlington|crystal/i.test(location + " " + (research.limitations?.text || ""))
        ? "Recent/host pattern points to Arlington / Crystal City — not a demonstrated Mayflower/downtown host history for this occasion."
        : text(research.limitations?.text)
          ? `Historical/host pattern note: ${toCustomerProse(research.limitations.text).slice(0, 180)}`
          : "Prior host pattern not captured this pass — historical fit not scored.",
    },
    competitiveAccessibility: {
      label: HOTEL_FIT_COMPONENT_LABELS.competitiveAccessibility,
      score: hostPlaced ? 28 : sourcingUnconfirmed ? 60 : 45,
      assessment: hostPlaced
        ? "Primary lodging appears awarded elsewhere — competitive access is limited to overflow/preferred paths."
        : sourcingUnconfirmed
          ? "No public host lock — competitive access is still plausible but unproven."
          : "Competitive accessibility not strongly evidenced.",
    },
    contactability: {
      label: HOTEL_FIT_COMPONENT_LABELS.contactability,
      score:
        contactRoute?.method || contactRoute?.phone
          ? contactRoute?.person && /\s/.test(String(contactRoute.person))
            ? 82
            : 64
          : 20,
      assessment:
        contactRoute?.method || contactRoute?.phone
          ? `Contact route available${contactRoute.person ? ` (${contactRoute.person})` : ""} — kept distinct from physical/geography fit.`
          : "No usable contact route — contactability weak; do not treat physical fit as outreach-ready.",
    },
  };

  const scoredInputs = {};
  let allScored = true;
  for (const [key, dim] of Object.entries(dimensions)) {
    if (dim.score == null || !Number.isFinite(Number(dim.score))) {
      allScored = false;
    } else {
      scoredInputs[key] = dim.score;
    }
  }

  let numeric = null;
  if (allScored) {
    numeric = computeHotelFitScore(scoredInputs, DEFAULT_HOTEL_FIT_WEIGHTS);
  }

  const stretchPosture = stretch || hostPlaced;
  return {
    version: "gdi-hotel-fit-v1",
    scoringVersion: numeric?.scoringVersion || "gdi-hotel-fit-v1",
    numericScore: numeric?.hotelFitScore ?? null,
    scored: Boolean(numeric),
    weights: { ...DEFAULT_HOTEL_FIT_WEIGHTS },
    componentLabels: { ...HOTEL_FIT_COMPONENT_LABELS },
    components: numeric?.components || null,
    dimensions,
    posture: stretchPosture ? "EXPLORATORY_STRETCH" : "COMPETE_IF_OPEN",
    summary: stretchPosture
      ? `No supported primary-host advantage versus the occasion geography/host pattern. Treat as Exploratory/stretch: ${toCustomerProse(research.propertySpecificFit?.text || "").slice(0, 200)}`
      : toCustomerProse(research.propertySpecificFit?.text || "") ||
        "Hotel-fit dimensions assessed from available evidence.",
  };
}

/**
 * Map research brief onto opportunity fields used by cards/drawers/importer.
 * Keeps section-owned prose distinct (no duplicated paragraphs across fields).
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

  const cohort = toCustomerProse(research.travelingCohort?.text);
  const mechanism = toCustomerProse(research.hotelBusinessMechanism?.text);
  const fit = toCustomerProse(research.propertySpecificFit?.text);
  const limits = toCustomerProse(research.limitations?.text);
  const timing = toCustomerProse(research.timingRationale?.text);
  const action = toCustomerProse(research.nextSalesAction?.text);
  const accountOccasion = toCustomerProse(research.accountOccasion?.text);

  // Account / occasion — never overwrite with hotel-business mechanism
  if (accountOccasion) {
    next.summaryWhat = accountOccasion;
  }
  // Who travels
  if (cohort) {
    next.travelingCohort = cohort;
    next.relevantTravelers = cohort;
  }
  // Why hotel business (demand mechanism) — distinct from cohort and fit
  if (mechanism) {
    next.whyHotelDemand = mechanism;
  }
  // Why this hotel — single thesis field; do not mirror into summaryWhyMatters
  if (fit) {
    next.hotelOpportunityThesis = fit;
    next.summaryWhyHotel = fit;
    next.fitExplanation = fit;
    next.fitRationale = fit;
  }
  // Limitations only (venue history belongs here, not in why-hotel)
  if (limits) {
    next.competitiveContextNote = limits;
  }
  if (timing) {
    next.whyNow = timing;
    next.summaryWhyNow = timing;
  }
  if (action) {
    next.recommendedAction = action;
    next.recommendedNextStep = action;
  }
  if (research.nextSalesAction?.specificQuestion) {
    next.specificQuestion = research.nextSalesAction.specificQuestion;
  }
  // summaryWhyMatters: short "why investigate" — not a copy of the thesis
  if (timing && (!next.summaryWhyMatters || /credible reviewed prospect/i.test(next.summaryWhyMatters))) {
    next.summaryWhyMatters = timing.slice(0, 220);
  }

  const assessment =
    research.hotelFitAssessment ||
    buildHotelFitAssessmentFromResearch(research, { candidate: opportunity });
  next.hotelFitAssessment = assessment;
  next.hotelFitComponentLabels = assessment.componentLabels || HOTEL_FIT_COMPONENT_LABELS;
  if (assessment.scored && assessment.numericScore != null) {
    next.hotelFitScore = assessment.numericScore;
    next.hotelFitComponents = assessment.components;
    next.hotelFitScoreNote = `Scored with ${assessment.scoringVersion}; inputs: ${Object.entries(
      assessment.dimensions || {}
    )
      .map(([k, d]) => `${k}=${d.score}`)
      .join(", ")}.`;
  } else {
    next.hotelFitScore = null;
    next.hotelFitComponents = null;
    next.hotelFitScoreNote = null;
    next.hotelFitDimensionAssessments = Object.fromEntries(
      Object.entries(assessment.dimensions || {}).map(([k, d]) => [
        k,
        { label: d.label, assessment: d.assessment, score: d.score },
      ])
    );
  }
  if (assessment.posture === "EXPLORATORY_STRETCH") {
    // Do not invent Priority when evidence shows stretch / no advantage
    next.prospectReviewSection = "EXPLORATORY";
    next.prospectCustomerLabel = "Exploratory account";
    next.prospectCommercialLabel = "Exploratory account";
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
    const stretchPosture =
      r.hotelFitAssessment?.posture === "EXPLORATORY_STRETCH" || overflowHint;
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
      ...(stretchPosture ? { section: "EXPLORATORY" } : {}),
      // Account/occasion prose — not the hotel-business mechanism
      whatWeKnow:
        r.accountOccasion?.text ||
        text(row.opportunity?.summaryWhat) ||
        null,
      travelers: r.travelingCohort?.text || null,
      whyHotelDemand: r.hotelBusinessMechanism?.text || null,
      whyFits: r.propertySpecificFit?.text || null,
      hotelFitAssessment: r.hotelFitAssessment || null,
      hotelFitDimensionAssessments: r.hotelFitAssessment?.dimensions || null,
      // whyMatters = timing / investigate-now — not a duplicate of whyFits
      whyMatters: r.timingRationale?.text || null,
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
