/**
 * Global GDI opportunity summary enrichment (hotel-agnostic).
 * One builder for all hotels / discovery lanes.
 * Does not invent facts; distinguishes known vs unknown.
 */

export const SUMMARY_QUALITY = Object.freeze({
  STRONG: "STRONG",
  ADEQUATE: "ADEQUATE",
  THIN: "THIN",
  INVALID: "INVALID",
});

export const SUMMARY_INSUFFICIENT = "INSUFFICIENT_FOR_CUSTOMER_SUMMARY";

function norm(s) {
  return String(s || "")
    .replace(/\s+/g, " ")
    .trim();
}

function lower(s) {
  return norm(s).toLowerCase();
}

/**
 * True when summary is empty or essentially duplicates the title.
 */
export function isTitleDuplicateSummary(summary, title) {
  const a = lower(summary);
  const b = lower(title);
  if (!a) return true;
  if (!b) return false;
  if (a === b) return true;
  const strip = (t) =>
    t
      .replace(/\s*[—\-]\s*(exhibitor|vendor|sponsor)\s*block\s*$/i, "")
      .replace(/[^\w\s]/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  return strip(a) === strip(b);
}

function formatDates(opp = {}) {
  if (opp.eventDateDisplay && !/^\d{4}-\d{2}-\d{2}/.test(opp.eventDateDisplay)) {
    return opp.eventDateDisplay;
  }
  const start = opp.eventStartDate || null;
  const end = opp.eventEndDate || null;
  if (start && end && start !== end) return `${start} – ${end}`;
  if (start) return start;
  if (opp.eventYear) return String(opp.eventYear);
  return null;
}

function lodgingFacts(opp = {}) {
  const le = opp.lodgingEvidence || opp.lodging || null;
  const thesis = `${opp.hotelOpportunityThesis || ""} ${opp.summaryWhyHotel || ""} ${opp.venueStatus || ""}`;
  const mentioned =
    (le && typeof le === "object" && (le.roomBlockMentioned || le.housingPageFound)) ||
    /room block|housing|hotel TBA|not[_ ]?announced|overflow/i.test(thesis);
  const overflow =
    (le && typeof le === "object" && le.overflowMentioned) ||
    /overflow/i.test(`${opp.opportunityType || ""} ${thesis}`);
  const open = le && typeof le === "object" ? le.housingOpen : null;
  return { mentioned: Boolean(mentioned), overflow: Boolean(overflow), open };
}

function teamFacts(opp = {}) {
  if (opp.teamSupported === true) return { supported: true };
  const te = opp.teamEvidence || opp.hiddenDemand?.teamEvidence || null;
  if (!te) return { supported: false };
  if (te === true || te === "present") return { supported: true };
  if (typeof te === "object" && (te.teamSupported || te.multiPerson || te.outOfMarket)) {
    return { supported: true };
  }
  return { supported: false };
}

function hasUnsupportedFabrication(summary, opp = {}) {
  const s = lower(summary);
  // Hard invalid if claims confirmed lodging while lodging evidence is empty
  const lodging = lodgingFacts(opp);
  if (
    /\b(confirmed|secured|booked)\b.*\b(room block|hotel block|housing)\b/i.test(summary) &&
    !lodging.mentioned &&
    !lodging.overflow
  ) {
    return true;
  }
  if (/\b\d{2,4}\s+rooms?\b/i.test(summary) && !(opp.estimatedPeakRooms || opp.peakRooms)) {
    return true;
  }
  void s;
  return false;
}

/**
 * Score an existing or proposed summary against the quality contract.
 */
export function evaluateGdiSummaryQuality(opp = {}, summaryOverride = null) {
  const title = norm(opp.title || opp.opportunityName);
  const summary = norm(summaryOverride != null ? summaryOverride : opp.summaryWhat);
  const org = norm(opp.organizationName || opp.company);

  if (!summary) {
    return {
      quality: SUMMARY_QUALITY.THIN,
      reasons: ["empty_summary"],
      dimensions: {},
    };
  }
  if (hasUnsupportedFabrication(summary, opp)) {
    return {
      quality: SUMMARY_QUALITY.INVALID,
      reasons: ["unsupported_lodging_or_rooms_claim"],
      dimensions: {},
    };
  }
  if (isTitleDuplicateSummary(summary, title)) {
    return {
      quality: SUMMARY_QUALITY.THIN,
      reasons: ["title_duplicate"],
      dimensions: { titleDuplicate: true },
    };
  }
  if (/^future opportunity$/i.test(summary) || /^watch$/i.test(summary)) {
    return {
      quality: SUMMARY_QUALITY.THIN,
      reasons: ["generic_boilerplate"],
      dimensions: {},
    };
  }

  const dims = {
    hasOrg: Boolean(org) && summary.toLowerCase().includes(org.split(/\s+/)[0].toLowerCase()),
    hasTrigger: /\b(meeting|conference|summit|symposium|tournament|exhibitor|housing|overflow|festival|forum|session|advocacy)\b/i.test(
      summary
    ),
    hasTiming: /\b(20\d{2}|january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)\b/i.test(
      summary
    ),
    hasLodgingContext: /\b(hotel|lodging|housing|room block|overflow|TBA|not yet|arrangements|accommodation)\b/i.test(
      summary
    ),
    hasUnknown: /\b(not yet|TBA|unknown|unconfirmed|have not|has not|monitor|pending)\b/i.test(
      summary
    ),
    specific: summary.length >= 60 && summary.split(/[.!?]/).filter(Boolean).length >= 1,
  };

  const score = Object.values(dims).filter(Boolean).length;
  if (score >= 4 && dims.hasLodgingContext && dims.specific) {
    return { quality: SUMMARY_QUALITY.STRONG, reasons: ["rich_commercial_context"], dimensions: dims };
  }
  if (score >= 3 && dims.specific) {
    return { quality: SUMMARY_QUALITY.ADEQUATE, reasons: ["adequate_context"], dimensions: dims };
  }
  if (score >= 2) {
    return { quality: SUMMARY_QUALITY.ADEQUATE, reasons: ["partial_context"], dimensions: dims };
  }
  return { quality: SUMMARY_QUALITY.THIN, reasons: ["insufficient_context"], dimensions: dims };
}

/**
 * Build a customer opportunity summary from supported canonical facts only.
 * @returns {{ summaryWhat, summaryQuality, summaryInsufficient, reasons, changed }}
 */
export function buildGdiOpportunitySummary(opp = {}, opts = {}) {
  const existing = norm(opp.summaryWhat);
  const title = norm(opp.title || opp.opportunityName);
  const existingEval = evaluateGdiSummaryQuality(opp, existing);

  // Preserve STRONG existing summaries unless force
  if (
    !opts.force &&
    existingEval.quality === SUMMARY_QUALITY.STRONG &&
    !isTitleDuplicateSummary(existing, title)
  ) {
    return {
      summaryWhat: existing,
      summaryQuality: SUMMARY_QUALITY.STRONG,
      summaryInsufficient: false,
      reasons: ["kept_strong_existing"],
      changed: false,
    };
  }

  const org = norm(opp.organizationName || opp.company) || null;
  const eventHint =
    norm(opp.eventName) ||
    title.replace(/\s*[—\-]\s*(EXHIBITOR|VENDOR|SPONSOR)\s*BLOCK\s*$/i, "") ||
    null;
  const dates = formatDates(opp);
  const dest =
    norm(opp.destinationStatus) ||
    norm(opp.eventLocation) ||
    norm(opp.location) ||
    null;
  const venue = norm(opp.venueStatus);
  const lodging = lodgingFacts(opp);
  const team = teamFacts(opp);

  const facts = {
    hasOrg: Boolean(org),
    hasEvent: Boolean(eventHint),
    hasTiming: Boolean(dates) || Boolean(opp.eventYear),
    hasGeo: Boolean(dest) && !/^UNKNOWN$/i.test(dest),
    hasLodgingSignal: lodging.mentioned || lodging.overflow,
    hasTeam: team.supported,
    hasVenueOpen:
      venue &&
      !/^UNKNOWN$/i.test(venue) &&
      /TBA|not[_ ]?announced|not (yet )?(named|listed)|hotel/i.test(venue),
  };

  const factCount = Object.values(facts).filter(Boolean).length;
  if (factCount < 2 || (!facts.hasOrg && !facts.hasEvent)) {
    return {
      summaryWhat: existing && !isTitleDuplicateSummary(existing, title) ? existing : null,
      summaryQuality: SUMMARY_QUALITY.THIN,
      summaryInsufficient: true,
      reasons: ["insufficient_canonical_facts", SUMMARY_INSUFFICIENT],
      changed: false,
    };
  }

  const sentences = [];

  // Sentence 1: who + what + when/where
  {
    const who = org || "The organization";
    const what = eventHint || "this program";
    const when = dates ? ` (${dates})` : opp.eventYear ? ` (${opp.eventYear})` : "";
    const where =
      dest && !/^UNKNOWN$/i.test(dest) && !lower(what).includes(lower(dest).slice(0, 12))
        ? ` in ${dest}`
        : "";
    if (lodging.overflow || /OVERFLOW/i.test(String(opp.opportunityType || ""))) {
      sentences.push(
        `${who} is tied to ${what}${when}${where}, creating a potential overflow / housing opportunity.`
      );
    } else if (/EXHIBITOR|VENDOR BLOCK/i.test(title)) {
      sentences.push(
        `${who} is linked to ${what}${when}${where} as a participating exhibitor / vendor organization.`
      );
    } else {
      sentences.push(`${who} is associated with ${what}${when}${where}.`);
    }
  }

  // Sentence 2: lodging / team interpretation
  if (team.supported && (lodging.mentioned || lodging.overflow)) {
    sentences.push(
      "Public evidence supports a multi-person travel party with a lodging or housing signal; hotel arrangements still need confirmation."
    );
  } else if (team.supported) {
    sentences.push(
      "Team participation is evidenced publicly, but lodging arrangements have not yet been confirmed."
    );
  } else if (lodging.open === true) {
    sentences.push("Housing registration appears open; confirm Midtown / local partner acceptance.");
  } else if (lodging.mentioned || lodging.overflow || facts.hasVenueOpen) {
    sentences.push(
      "Housing or host-hotel details are incomplete or not yet publicly locked."
    );
  } else if (facts.hasTiming) {
    sentences.push(
      "Commercial lodging details remain to be confirmed from public sources."
    );
  }

  // Sentence 3: open issue (optional, keep short)
  if (
    sentences.length < 3 &&
    venue &&
    /TBA|not[_ ]?announced|not (yet )?/i.test(venue) &&
    !/incomplete or not yet/i.test(sentences.join(" "))
  ) {
    sentences.push("Venue or host hotel naming remains an open commercial question.");
  }

  let built = sentences.slice(0, 3).join(" ").replace(/\s+/g, " ").trim();
  // Never emit title duplicate
  if (isTitleDuplicateSummary(built, title)) {
    built = `${org || "Organization"}: ${built}`;
  }

  const evalBuilt = evaluateGdiSummaryQuality({ ...opp, summaryWhat: built }, built);
  if (
    evalBuilt.quality === SUMMARY_QUALITY.THIN ||
    evalBuilt.quality === SUMMARY_QUALITY.INVALID
  ) {
    // Prefer existing ADEQUATE+ over thin rebuild
    if (
      existingEval.quality === SUMMARY_QUALITY.ADEQUATE ||
      existingEval.quality === SUMMARY_QUALITY.STRONG
    ) {
      return {
        summaryWhat: existing,
        summaryQuality: existingEval.quality,
        summaryInsufficient: false,
        reasons: ["kept_existing_over_thin_rebuild"],
        changed: false,
      };
    }
    return {
      summaryWhat: null,
      summaryQuality: SUMMARY_QUALITY.THIN,
      summaryInsufficient: true,
      reasons: ["rebuild_still_thin", SUMMARY_INSUFFICIENT],
      changed: false,
    };
  }

  return {
    summaryWhat: built,
    summaryQuality: evalBuilt.quality,
    summaryInsufficient: false,
    reasons: ["built_from_canonical_facts"],
    changed: built !== existing,
  };
}

/**
 * Apply summary enrichment onto an opportunity (immutable-style return).
 */
export function applyGdiSummaryEnrichment(opp = {}, opts = {}) {
  const result = buildGdiOpportunitySummary(opp, opts);
  const next = { ...opp };
  if (result.summaryWhat) {
    next.summaryWhat = result.summaryWhat;
  }
  next.summaryQuality = result.summaryQuality;
  next.summaryInsufficient = result.summaryInsufficient === true;
  next.summaryEnrichmentAt = new Date().toISOString();
  next.summaryEnrichmentReasons = result.reasons;
  if (result.summaryInsufficient) {
    next.summaryHoldReason = SUMMARY_INSUFFICIENT;
  }
  return { opportunity: next, result };
}
