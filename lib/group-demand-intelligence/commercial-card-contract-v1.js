/**
 * Commercial card information contract for GDI opportunity tiles.
 * Card fields derive from the same canonical opportunity as the drawer.
 * Never duplicate the title as the description.
 */

import { BOOKING_WINDOW_LABEL, OPPORTUNITY_TYPE_LABEL } from "./claim-types.js";
import { DEMAND_FAMILY_LABEL } from "./demand-signal-types.js";
import { classifyEntityTruth } from "./entity-truth-gate-v1.js";

function norm(s) {
  return String(s || "")
    .replace(/\s+/g, " ")
    .trim();
}

function isDuplicateOfTitle(text, title) {
  const a = norm(text).toLowerCase();
  const b = norm(title).toLowerCase();
  if (!a || !b) return false;
  if (a === b) return true;
  // Title with block suffix vs bare summary
  const strip = (t) =>
    t.replace(/\s*[—\-]\s*(exhibitor|vendor|sponsor)\s*block\s*$/i, "").trim();
  return strip(a) === strip(b);
}

function formatDateRange(opp = {}) {
  if (opp.eventDateDisplay && !/^20\d{2}-\d{2}-\d{2}/.test(opp.eventDateDisplay)) {
    return opp.eventDateDisplay;
  }
  const start = opp.eventStartDate || null;
  const end = opp.eventEndDate || null;
  if (start && end && start !== end) return `${start} – ${end}`;
  if (start) return start;
  if (opp.eventYear) return String(opp.eventYear);
  return null;
}

function commercialMotionLabel(opp = {}) {
  if (opp.commercialMotionLabel) return opp.commercialMotionLabel;
  if (opp.commercialMotion) return String(opp.commercialMotion).replace(/_/g, " ");
  const type = opp.opportunityType || opp.demandType || null;
  if (type && OPPORTUNITY_TYPE_LABEL[type]) return OPPORTUNITY_TYPE_LABEL[type];
  if (type) return String(type).replace(/_/g, " ");
  if (/exhibitor/i.test(opp.title || "") || opp.boothNumber) return "Exhibitor Team";
  if (/overflow/i.test(String(opp.opportunityType || ""))) return "Overflow Housing";
  return null;
}

function demandFamilyLabel(opp = {}) {
  if (opp.demandFamilyLabel) return opp.demandFamilyLabel;
  if (opp.demandFamily && DEMAND_FAMILY_LABEL[opp.demandFamily]) {
    return DEMAND_FAMILY_LABEL[opp.demandFamily];
  }
  if (opp.segment) return String(opp.segment);
  return null;
}

function hasTeamEvidence(opp = {}) {
  if (opp.teamSupported === true) return true;
  const te = opp.teamEvidence || opp.hiddenDemand?.teamEvidence || null;
  if (!te) return false;
  if (te === true || te === "present") return true;
  if (typeof te === "object" && (te.teamSupported || te.multiPerson || te.outOfMarket)) {
    return true;
  }
  return false;
}

function lodgingSignal(opp = {}) {
  const le = opp.lodgingEvidence || opp.lodging || opp.hiddenDemand?.lodgingEvidence || null;
  if (!le || typeof le !== "object") {
    return {
      mentioned: false,
      open: null,
      overflow: false,
      status: null,
    };
  }
  return {
    mentioned: Boolean(le.roomBlockMentioned || le.housingPageFound),
    open: le.housingOpen,
    overflow: Boolean(le.overflowMentioned),
    status: le.status || null,
  };
}

/**
 * Build a short commercial summary from supported claims only.
 */
export function buildCommercialCardSummary(opp = {}) {
  const title = norm(opp.title);
  const existing = norm(opp.commercialSummary || opp.summaryWhat || "");
  if (existing && !isDuplicateOfTitle(existing, title)) {
    return existing;
  }

  const org =
    norm(opp.organizationName) ||
    classifyEntityTruth(opp).displayName ||
    "Organization";
  const dates = formatDateRange(opp);
  const eventHint = norm(opp.eventName) || title.replace(/\s*[—\-]\s*(EXHIBITOR|VENDOR|SPONSOR)\s*BLOCK\s*$/i, "");
  const lodging = lodgingSignal(opp);
  const team = hasTeamEvidence(opp);
  const venue = norm(opp.venueStatus || opp.destinationStatus || "");

  const parts = [];
  if (dates) {
    parts.push(`${org} is linked to ${eventHint} (${dates})`);
  } else {
    parts.push(`${org} is linked to ${eventHint}`);
  }

  if (team && (lodging.mentioned || lodging.overflow)) {
    parts.push(
      "Public evidence indicates a multi-person travel party with a lodging/housing signal, creating a potential hotel opportunity"
    );
  } else if (team) {
    parts.push("Team lodging has not yet been publicly confirmed");
  } else if (lodging.mentioned || lodging.overflow) {
    parts.push("Housing or overflow language appears publicly; team size is not yet confirmed");
  } else if (/not (yet )?(named|announced)|TBA|hotel TBA/i.test(venue)) {
    parts.push("Host hotel is not yet publicly named");
  } else {
    parts.push("Commercial lodging details remain to be confirmed from public sources");
  }

  return `${parts[0]}. ${parts[1]}.`;
}

/**
 * Compact why-this-hotel line (hotel-specific when available).
 */
export function buildCardHotelFitLine(opp = {}) {
  const fit = norm(
    opp.cardHotelFitLine ||
      opp.summaryWhyHotel ||
      opp.fitExplanation ||
      opp.hotelFitReason ||
      ""
  );
  if (!fit) return null;
  // Prefer a short clause
  const first = fit.split(/(?<=\.)\s+/)[0] || fit;
  return first.length > 140 ? `${first.slice(0, 137)}…` : first;
}

/**
 * Timing reason — never bare "Future opportunity".
 */
export function buildCardWhyNowLine(opp = {}) {
  let why = norm(opp.whyNow || "");
  if (!why || /^future opportunity$/i.test(why)) {
    const lodging = lodgingSignal(opp);
    if (lodging.open === true) why = "Housing registration appears open";
    else if (lodging.mentioned) why = "Housing or room-block language is public; confirm Midtown acceptance";
    else if (opp.bookingWindowStatus === "CONTACT_NOW") why = "Contact window is open based on current cycle timing";
    else if (opp.bookingWindowStatus === "QUALIFY_NOW" || opp.bookingWindowStatus === "RESEARCH_FURTHER") {
      why = "Qualify now — cycle timing is actionable while lodging remains unsettled";
    } else if (formatDateRange(opp)) {
      why = `Upcoming cycle (${formatDateRange(opp)}) — confirm lodging and decision path`;
    } else {
      why = "Monitor for lodging or team announcements";
    }
  }
  // Strip duplicated CONTACT NOW: prefix noise for card
  why = why.replace(/^(CONTACT NOW|QUALIFY NOW|WATCH)\s*:\s*/i, "");
  return why.length > 160 ? `${why.slice(0, 157)}…` : why;
}

/**
 * Contact display model for cards (never "No primary contact" when a path exists).
 */
export function buildCardContactDisplay(opp = {}) {
  const pathClass =
    opp.contactPathClass ||
    opp.addressability ||
    opp.cardContactPathClass ||
    null;
  const c = opp.primaryContact && typeof opp.primaryContact === "object" ? opp.primaryContact : null;
  const name = c && c.name && c.name !== "UNKNOWN" ? norm(c.name) : "";
  const role = c ? norm(c.role || c.title || "") : "";
  const email = c ? norm(c.email || "") : "";
  const functional = norm(opp.functionalContactEmail || opp.functionalContact || "");

  const looksLikePerson =
    name &&
    name.split(/\s+/).length >= 2 &&
    !/^(booth|press|event|survey|learning futures|boutique design|interested|powered)/i.test(
      name
    );

  if (looksLikePerson && email) {
    return {
      pathClass: "NAMED_DIRECT",
      name,
      role: role || null,
      email,
      line: name,
      detail: [role, email].filter(Boolean).join(" · "),
      empty: false,
    };
  }
  if (looksLikePerson) {
    return {
      pathClass: "NAMED_PARTIAL",
      name,
      role: role || null,
      email: null,
      line: name,
      detail: [role, "Contact details not publicly identified"].filter(Boolean).join(" · "),
      empty: false,
    };
  }
  if (functional || pathClass === "FUNCTIONAL" || pathClass === "FUNCTIONAL_PATH") {
    return {
      pathClass: "FUNCTIONAL",
      name: role || "Exhibitor / events operations",
      role: role || null,
      email: functional || email || null,
      line: role || "Functional contact path",
      detail: functional || email || "Official functional inbox available",
      empty: false,
    };
  }
  if (
    pathClass === "ORG_PATH" ||
    pathClass === "COMPANY_PATH" ||
    opp.organizationContactUrl ||
    opp.officialContactPath
  ) {
    return {
      pathClass: "ORG_PATH",
      name: norm(opp.organizationName) || "Organization",
      role: "Events / meetings team",
      email: null,
      line: `${norm(opp.organizationName) || "Organization"} events team`,
      detail: "Official contact path available",
      empty: false,
    };
  }
  return {
    pathClass: "NO_CONTACT_AFTER_RESEARCH",
    name: null,
    role: null,
    email: null,
    line: "Named stakeholder not publicly identified yet",
    detail: null,
    empty: true,
  };
}

/**
 * Enrich opportunity with card-contract fields (canonical, shared with drawer).
 */
export function applyCommercialCardContract(opp = {}) {
  const motion = commercialMotionLabel(opp);
  const family = demandFamilyLabel(opp);
  const summary = buildCommercialCardSummary(opp);
  const hotelFitLine = buildCardHotelFitLine(opp);
  const whyNowLine = buildCardWhyNowLine(opp);
  const contact = buildCardContactDisplay(opp);
  const entity = classifyEntityTruth(opp);

  const score =
    typeof opp.hotelFitScore === "number" && Number.isFinite(opp.hotelFitScore)
      ? Math.round(opp.hotelFitScore)
      : null;
  let fitBadge = null;
  if (score != null && score >= 75) fitBadge = `FIT ${score}`;
  else if (score != null && score >= 60) fitBadge = "Strong Fit";
  else if (hotelFitLine) fitBadge = null; // prefer reason line over weak score

  return {
    ...opp,
    entityClass: opp.entityClass || entity.entityClass,
    commercialMotion: opp.commercialMotion || motion,
    commercialMotionLabel: opp.commercialMotionLabel || motion,
    demandFamilyLabel: opp.demandFamilyLabel || family,
    commercialSummary: summary,
    summaryWhat: isDuplicateOfTitle(opp.summaryWhat, opp.title) ? summary : opp.summaryWhat || summary,
    cardHotelFitLine: hotelFitLine,
    cardWhyNowLine: whyNowLine,
    whyNow: opp.whyNow || whyNowLine,
    cardContact: contact,
    contactPathClass: opp.contactPathClass || contact.pathClass,
    cardFitBadge: fitBadge,
    bookingWindowLabel:
      opp.bookingWindowLabel ||
      BOOKING_WINDOW_LABEL[opp.bookingWindowStatus] ||
      null,
  };
}

/**
 * Whether a card meets the Bethesda information bar (pre-drawer prioritization).
 */
export function scoreCardCompleteness(opp = {}) {
  const enriched = applyCommercialCardContract(opp);
  const checks = {
    identity: Boolean(enriched.title),
    dates: Boolean(formatDateRange(enriched)),
    organization: Boolean(enriched.organizationName),
    demandFamily: Boolean(enriched.demandFamilyLabel || enriched.segment),
    commercialMotion: Boolean(enriched.commercialMotionLabel || enriched.commercialMotion),
    commercialSummary:
      Boolean(enriched.commercialSummary) &&
      !isDuplicateOfTitle(enriched.commercialSummary, enriched.title),
    whyHotel: Boolean(enriched.cardHotelFitLine),
    whyNow: Boolean(enriched.cardWhyNowLine) && !/^future opportunity$/i.test(enriched.cardWhyNowLine),
    contact: Boolean(enriched.cardContact && !enriched.cardContact.empty),
    qualification: Boolean(
      enriched.opportunityQualificationLabel ||
        enriched.opportunityQualification ||
        enriched.bookingWindowStatus
    ),
  };
  const missing = Object.entries(checks)
    .filter(([, ok]) => !ok)
    .map(([k]) => k);
  return {
    complete: missing.length === 0,
    checks,
    missing,
    score: Object.values(checks).filter(Boolean).length,
    total: Object.keys(checks).length,
  };
}
