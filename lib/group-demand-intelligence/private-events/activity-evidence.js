/**
 * Multi-signal private-event activity evidence (Private Events V1.4).
 * Exact annual volume remains strong when present; not required for STRONG.
 */

import {
  ACTIVITY_EVIDENCE_TYPE,
  EVENT_ACTIVITY_EVIDENCE_STATUS,
  PARTNER_STATUS,
} from "./constants.js";

const DIRECTORY_HOST_RE =
  /theknot|weddingwire|zola|herecomestheguide|eventective|peerspace|gigsalad|yelp\.|tripadvisor|facebook\.|instagram\.|linkedin\.|maps\.google|google\.com\/maps|bing\.com/i;

const VOLUME_RE =
  /\b(?:host(?:s|ed|ing)?|holds?|books?|accommodates?)\s+(?:over\s+|more\s+than\s+|approximately\s+|about\s+|nearly\s+)?(\d{1,3})\+?\s+(?:weddings?|private\s+events?|events?)\s+(?:per\s+year|a\s+year|annually|\/\s*year)\b/i;

const WEDDING_PAGE_RE =
  /\b(?:weddings?|bridal)\b.{0,40}\b(?:venue|package|packages|celebration|celebrations|reception)\b|\b(?:host\s+your\s+wedding|wedding\s+at\s+(?:our|the)|say\s+['']?i\s+do)\b/i;

const PRIVATE_EVENT_PAGE_RE =
  /\b(?:private\s+events?|special\s+events?|corporate\s+events?|event\s+rentals?|facility\s+rental|venue\s+rental)\b/i;

const PACKAGE_RE =
  /\b(?:wedding\s+package|event\s+package|ceremony\s+package|reception\s+package|packages?\s+(?:include|start))\b/i;

const BOOKING_RE =
  /\b(?:book\s+(?:a|your)\s+(?:tour|event|wedding)|request\s+(?:a\s+)?(?:tour|proposal|quote)|inquire\s+(?:now|today)|inquiry\s+form|check\s+availability|schedule\s+(?:a\s+)?(?:tour|site\s+visit))\b/i;

const GALLERY_RE =
  /\b(?:wedding\s+galler(?:y|ies)|real\s+weddings?|event\s+galler(?:y|ies)|photo\s+galler(?:y|ies))\b/i;

const CALENDAR_RE =
  /\b(?:event\s+calendar|upcoming\s+events?|wedding\s+dates?\s+available)\b/i;

const EXPO_RE =
  /\b(?:wedding\s+(?:expo|show|fair)|bridal\s+(?:expo|show|fair)|events?\s+expo)\b/i;

const TESTIMONIAL_RE =
  /\b(?:testimonial|what\s+(?:our\s+)?(?:couples?|clients?|guests?)\s+say|reviews?\s+from\s+couples?)\b/i;

const PLANNER_RE =
  /\b(?:wedding\s+planner|event\s+planner|planned\s+(?:by|with)|planner\s+recommendation)\b/i;

const VENDOR_RE =
  /\b(?:preferred\s+vendors?|vendor\s+list|caterer|florist|dj\s+recommended)\b/i;

const YEAR_RE = /\b(20(?:2[4-9]|3[0-5]))\b/;

function clean(s) {
  return String(s || "").trim();
}

function hostOf(url) {
  try {
    return new URL(url).hostname.toLowerCase();
  } catch {
    return "";
  }
}

function pathKey(url) {
  try {
    const u = new URL(url);
    return `${u.hostname.toLowerCase()}${u.pathname.replace(/\/+$/, "").toLowerCase()}`;
  } catch {
    return clean(url).toLowerCase();
  }
}

export function isDirectoryUrl(url) {
  return DIRECTORY_HOST_RE.test(String(url || ""));
}

export function isOfficialVenueUrl(url, venue = {}) {
  const host = hostOf(url);
  if (!host || isDirectoryUrl(url)) return false;
  const domain = clean(venue.officialDomain || venue.website)
    .replace(/^https?:\/\//i, "")
    .replace(/^www\./i, "")
    .split("/")[0]
    .toLowerCase();
  if (domain && (host === domain || host.endsWith(`.${domain}`))) return true;
  // Tourism / city / parks official hosts
  if (/\.(gov|edu)$/i.test(host) || /montgomeryparks|bethesda\.org|montgomerycountymd/i.test(host)) {
    return true;
  }
  return false;
}

/**
 * Deduplicate evidence items that repeat the same underlying page/content.
 */
export function dedupeActivityEvidence(items = []) {
  const seen = new Set();
  const out = [];
  let removed = 0;
  for (const raw of items || []) {
    if (!raw || typeof raw !== "object") continue;
    const url = clean(raw.sourceUrl);
    const type = clean(raw.evidenceType) || ACTIVITY_EVIDENCE_TYPE.OTHER;
    const key = `${type}|${pathKey(url) || clean(raw.evidenceText).slice(0, 80).toLowerCase()}`;
    if (seen.has(key)) {
      removed += 1;
      continue;
    }
    seen.add(key);
    out.push({
      evidenceType: type,
      sourceUrl: url || null,
      sourceAuthority: raw.sourceAuthority || null,
      sourceDate: raw.sourceDate || raw.eventDate || null,
      evidenceText: clean(raw.evidenceText).slice(0, 400) || null,
      eventDate: raw.eventDate || null,
      confidence: raw.confidence || null,
      official: raw.official === true,
    });
  }
  return { items: out, duplicatesRemoved: removed };
}

/**
 * Infer evidence items from page text + URL context.
 */
export function extractActivityEvidenceFromPage({
  url,
  text,
  venue = {},
  title = "",
  snippet = "",
} = {}) {
  const blob = [title, snippet, text].filter(Boolean).join(" \n ");
  if (!blob || blob.length < 40) return [];
  const official = isOfficialVenueUrl(url, venue);
  const directory = isDirectoryUrl(url);
  const authority = official
    ? "OFFICIAL_VENUE"
    : directory
      ? "THIRD_PARTY_DIRECTORY"
      : "SECONDARY";
  const items = [];
  const push = (evidenceType, evidenceText, extra = {}) => {
    items.push({
      evidenceType,
      sourceUrl: url || null,
      sourceAuthority: authority,
      evidenceText: clean(evidenceText).slice(0, 280),
      official,
      confidence: official ? "HIGH" : directory ? "LOW" : "MODERATE",
      ...extra,
    });
  };

  const vol = blob.match(VOLUME_RE);
  if (vol) {
    push(
      ACTIVITY_EVIDENCE_TYPE.ANNUAL_VOLUME_STATEMENT,
      vol[0],
      { annualCountHint: Number(vol[1]), sourceDate: null }
    );
  }
  if (WEDDING_PAGE_RE.test(blob) && (/wedding/i.test(url || "") || /wedding/i.test(blob.slice(0, 500)))) {
    push(ACTIVITY_EVIDENCE_TYPE.OFFICIAL_WEDDING_PAGE, "Wedding venue marketing language");
  }
  if (PRIVATE_EVENT_PAGE_RE.test(blob)) {
    push(
      ACTIVITY_EVIDENCE_TYPE.OFFICIAL_PRIVATE_EVENT_PAGE,
      "Private / special events marketing language"
    );
  }
  if (PACKAGE_RE.test(blob)) {
    push(ACTIVITY_EVIDENCE_TYPE.EVENT_PACKAGE, "Event/wedding package language");
  }
  if (BOOKING_RE.test(blob)) {
    push(ACTIVITY_EVIDENCE_TYPE.PUBLIC_BOOKING_LANGUAGE, "Public booking / inquiry language");
  }
  if (GALLERY_RE.test(blob)) {
    push(ACTIVITY_EVIDENCE_TYPE.GALLERY, "Wedding/event gallery reference");
  }
  if (CALENDAR_RE.test(blob)) {
    push(ACTIVITY_EVIDENCE_TYPE.EVENT_CALENDAR, "Event calendar / upcoming events");
  }
  if (EXPO_RE.test(blob)) {
    push(ACTIVITY_EVIDENCE_TYPE.EVENT_EXPO, "Wedding/events expo hosting");
  }
  if (TESTIMONIAL_RE.test(blob)) {
    push(ACTIVITY_EVIDENCE_TYPE.TESTIMONIAL, "Couple/client testimonials");
  }
  if (PLANNER_RE.test(blob) && !official) {
    push(ACTIVITY_EVIDENCE_TYPE.PLANNER_REFERENCE, "Planner reference");
  }
  if (VENDOR_RE.test(blob)) {
    push(ACTIVITY_EVIDENCE_TYPE.VENDOR_REFERENCE, "Vendor / preferred vendor reference");
  }

  // Dated wedding/event posts
  const years = [...blob.matchAll(new RegExp(YEAR_RE.source, "g"))].map((m) => m[1]);
  const uniqueYears = [...new Set(years)];
  if (
    uniqueYears.length >= 1 &&
    /\bwedding|private\s+event|reception\b/i.test(blob) &&
    (official || /real\s+wedding|hosted|celebrated/i.test(blob))
  ) {
    for (const y of uniqueYears.slice(0, 3)) {
      push(ACTIVITY_EVIDENCE_TYPE.DATED_EVENT_POST, `Dated event evidence (${y})`, {
        sourceDate: `${y}`,
        eventDate: `${y}`,
      });
    }
  }
  if (/\b(202[5-9]|203[0-5])\b/.test(blob) && /\bbook(?:ing)?|available|reserve|inquire\b/i.test(blob)) {
    push(ACTIVITY_EVIDENCE_TYPE.FUTURE_EVENT, "Future booking / availability language");
  }

  return items;
}

/**
 * Classify repeated private-event activity from structured evidence + venue fields.
 */
export function classifyEventActivityEvidence(venue = {}, evidenceItems = []) {
  const { items, duplicatesRemoved } = dedupeActivityEvidence([
    ...(venue.activityEvidence || []),
    ...(evidenceItems || []),
  ]);

  const annual = Number(venue.estimatedAnnualPrivateEvents || 0);
  const volumeStatus = clean(venue.annualEventVolumeStatus).toUpperCase();
  const volumeEvidence = items.filter(
    (e) => e.evidenceType === ACTIVITY_EVIDENCE_TYPE.ANNUAL_VOLUME_STATEMENT
  );
  // Stored annual count from prior research counts as CONFIRMED_VOLUME when
  // status is CONFIRMED/ESTIMATED, or when a numeric annual ≥12 is already on the venue
  // (legacy V1 fixtures) — still distinct from multi-signal STRONG without a number.
  const explicitVolume =
    volumeEvidence.length > 0 ||
    (annual > 0 &&
      (volumeStatus === "CONFIRMED" ||
        volumeStatus === "ESTIMATED" ||
        (annual >= 12 && (!volumeStatus || volumeStatus === "UNKNOWN"))));

  if (explicitVolume) {
    const count =
      annual ||
      volumeEvidence.find((e) => e.annualCountHint)?.annualCountHint ||
      null;
    return {
      eventActivityEvidenceStatus: EVENT_ACTIVITY_EVIDENCE_STATUS.CONFIRMED_VOLUME,
      estimatedAnnualPrivateEvents: count,
      activityEvidence: items,
      duplicatesRemoved,
      decisionReason:
        count != null
          ? `Credible source states ~${count} events/year`
          : "Credible annual volume statement present",
      officialCount: items.filter((e) => e.official).length,
      datedIndependentCount: countDatedIndependent(items),
      thirdPartyCount: items.filter((e) => !e.official).length,
    };
  }

  const officialTypes = new Set(
    items.filter((e) => e.official).map((e) => e.evidenceType)
  );
  const datedOfficial = items.filter(
    (e) =>
      e.official &&
      (e.evidenceType === ACTIVITY_EVIDENCE_TYPE.DATED_EVENT_POST ||
        e.evidenceType === ACTIVITY_EVIDENCE_TYPE.FUTURE_EVENT ||
        e.evidenceType === ACTIVITY_EVIDENCE_TYPE.EVENT_EXPO) &&
      e.sourceDate
  );
  const datedYears = new Set(datedOfficial.map((e) => String(e.sourceDate).slice(0, 4)));
  const plannerRefs = items.filter(
    (e) =>
      e.evidenceType === ACTIVITY_EVIDENCE_TYPE.PLANNER_REFERENCE ||
      e.evidenceType === ACTIVITY_EVIDENCE_TYPE.VENDOR_REFERENCE
  );
  const independentPlanner = plannerRefs.filter((e) => !e.official);

  const strongSignals = [];
  if (officialTypes.has(ACTIVITY_EVIDENCE_TYPE.OFFICIAL_WEDDING_PAGE)) {
    strongSignals.push("OFFICIAL_WEDDING_PAGE");
  }
  if (officialTypes.has(ACTIVITY_EVIDENCE_TYPE.OFFICIAL_PRIVATE_EVENT_PAGE)) {
    strongSignals.push("OFFICIAL_PRIVATE_EVENT_PAGE");
  }
  if (officialTypes.has(ACTIVITY_EVIDENCE_TYPE.EVENT_PACKAGE)) {
    strongSignals.push("EVENT_PACKAGE");
  }
  if (officialTypes.has(ACTIVITY_EVIDENCE_TYPE.EVENT_CALENDAR)) {
    strongSignals.push("EVENT_CALENDAR");
  }
  if (officialTypes.has(ACTIVITY_EVIDENCE_TYPE.GALLERY)) {
    strongSignals.push("GALLERY");
  }
  if (officialTypes.has(ACTIVITY_EVIDENCE_TYPE.PUBLIC_BOOKING_LANGUAGE)) {
    strongSignals.push("PUBLIC_BOOKING_LANGUAGE");
  }
  if (datedYears.size >= 2) strongSignals.push("MULTI_YEAR_DATED");
  if (datedOfficial.length >= 2) strongSignals.push("MULTI_DATED_OFFICIAL");
  if (independentPlanner.length >= 2) strongSignals.push("MULTI_PLANNER_REF");
  if (officialTypes.has(ACTIVITY_EVIDENCE_TYPE.EVENT_EXPO)) {
    strongSignals.push("EVENT_EXPO");
  }
  if (officialTypes.has(ACTIVITY_EVIDENCE_TYPE.TESTIMONIAL)) {
    strongSignals.push("TESTIMONIAL");
  }

  // Directory-only never STRONG
  const onlyDirectory =
    items.length > 0 && items.every((e) => isDirectoryUrl(e.sourceUrl));

  const uniqueOfficialTypeCount = officialTypes.size;
  const isStrong =
    !onlyDirectory &&
    ((uniqueOfficialTypeCount >= 3 &&
      (officialTypes.has(ACTIVITY_EVIDENCE_TYPE.OFFICIAL_WEDDING_PAGE) ||
        officialTypes.has(ACTIVITY_EVIDENCE_TYPE.OFFICIAL_PRIVATE_EVENT_PAGE))) ||
      (uniqueOfficialTypeCount >= 2 &&
        datedYears.size >= 2) ||
      (uniqueOfficialTypeCount >= 2 &&
        independentPlanner.length >= 2 &&
        (officialTypes.has(ACTIVITY_EVIDENCE_TYPE.OFFICIAL_WEDDING_PAGE) ||
          officialTypes.has(ACTIVITY_EVIDENCE_TYPE.OFFICIAL_PRIVATE_EVENT_PAGE))) ||
      (uniqueOfficialTypeCount >= 2 &&
        strongSignals.includes("EVENT_EXPO") &&
        strongSignals.includes("PUBLIC_BOOKING_LANGUAGE")));

  if (isStrong) {
    return {
      eventActivityEvidenceStatus:
        EVENT_ACTIVITY_EVIDENCE_STATUS.STRONG_REPEATED_ACTIVITY,
      estimatedAnnualPrivateEvents: annual > 0 ? annual : null,
      activityEvidence: items,
      duplicatesRemoved,
      decisionReason: `Multiple independent public signals (${strongSignals.join(", ")})`,
      strongSignals,
      officialCount: items.filter((e) => e.official).length,
      datedIndependentCount: countDatedIndependent(items),
      thirdPartyCount: items.filter((e) => !e.official).length,
    };
  }

  const marketsEvents =
    venue.weddingsAdvertised ||
    venue.privateEventsAdvertised ||
    officialTypes.has(ACTIVITY_EVIDENCE_TYPE.OFFICIAL_WEDDING_PAGE) ||
    officialTypes.has(ACTIVITY_EVIDENCE_TYPE.OFFICIAL_PRIVATE_EVENT_PAGE);

  if (marketsEvents && uniqueOfficialTypeCount >= 1) {
    return {
      eventActivityEvidenceStatus: EVENT_ACTIVITY_EVIDENCE_STATUS.MODERATE_ACTIVITY,
      estimatedAnnualPrivateEvents: annual > 0 ? annual : null,
      activityEvidence: items,
      duplicatesRemoved,
      decisionReason:
        "Venue markets weddings/private events but repeated frequency not strongly evidenced",
      strongSignals,
      officialCount: items.filter((e) => e.official).length,
      datedIndependentCount: countDatedIndependent(items),
      thirdPartyCount: items.filter((e) => !e.official).length,
    };
  }

  if (items.length > 0 || marketsEvents) {
    return {
      eventActivityEvidenceStatus: EVENT_ACTIVITY_EVIDENCE_STATUS.LIMITED_ACTIVITY,
      estimatedAnnualPrivateEvents: annual > 0 ? annual : null,
      activityEvidence: items,
      duplicatesRemoved,
      decisionReason: "One-off or thin private-event mention only",
      officialCount: items.filter((e) => e.official).length,
      datedIndependentCount: countDatedIndependent(items),
      thirdPartyCount: items.filter((e) => !e.official).length,
    };
  }

  return {
    eventActivityEvidenceStatus: EVENT_ACTIVITY_EVIDENCE_STATUS.UNKNOWN,
    estimatedAnnualPrivateEvents: annual > 0 ? annual : null,
    activityEvidence: items,
    duplicatesRemoved,
    decisionReason: "Insufficient public activity evidence",
    officialCount: 0,
    datedIndependentCount: 0,
    thirdPartyCount: 0,
  };
}

function countDatedIndependent(items) {
  const years = new Set();
  for (const e of items) {
    if (
      e.sourceDate &&
      (e.evidenceType === ACTIVITY_EVIDENCE_TYPE.DATED_EVENT_POST ||
        e.evidenceType === ACTIVITY_EVIDENCE_TYPE.FUTURE_EVENT ||
        e.evidenceType === ACTIVITY_EVIDENCE_TYPE.EVENT_EXPO)
    ) {
      years.add(String(e.sourceDate).slice(0, 4));
    }
  }
  return years.size;
}

/**
 * Normalize partner research outcome. Conservative language.
 */
export function classifyPartnerStatus(venue = {}) {
  if (venue.exclusiveHotelRelationship === true) {
    return {
      partnerStatus: PARTNER_STATUS.EXCLUSIVE_PARTNER_FOUND,
      note: "Explicit exclusive/sole hotel language found",
    };
  }
  const raw = clean(venue.partnerStatus || venue.hotelPartnerStatus).toUpperCase();
  if (raw === "EXCLUSIVE_PARTNER_FOUND" || raw === "EXCLUSIVE") {
    return {
      partnerStatus: PARTNER_STATUS.EXCLUSIVE_PARTNER_FOUND,
      note: "Exclusive partner status from research",
    };
  }
  if (
    raw === "PREFERRED_PARTNER_FOUND" ||
    venue.preferredHotelListed === true
  ) {
    return {
      partnerStatus: PARTNER_STATUS.PREFERRED_PARTNER_FOUND,
      note: "Preferred hotel listed publicly (not exclusive)",
    };
  }
  if (
    raw === "NONEXCLUSIVE_PARTNERS_FOUND" ||
    (Array.isArray(venue.hotelPartners) && venue.hotelPartners.length > 0) ||
    (Array.isArray(venue.knownHotelPartners) && venue.knownHotelPartners.length > 0)
  ) {
    return {
      partnerStatus: PARTNER_STATUS.NONEXCLUSIVE_PARTNERS_FOUND,
      note: "Non-exclusive hotel partners mentioned publicly",
    };
  }
  if (
    raw === "NO_PUBLIC_PARTNER_FOUND" ||
    raw === "NO_PARTNER_FOUND" ||
    venue.partnerResearchComplete === true
  ) {
    return {
      partnerStatus: PARTNER_STATUS.NO_PUBLIC_PARTNER_FOUND,
      note: "Bounded public research found no hotel partner (not proof none exists)",
    };
  }
  if (raw === "UNKNOWN" || !raw) {
    return {
      partnerStatus: PARTNER_STATUS.UNKNOWN,
      note: "Partner status not resolved after available evidence",
    };
  }
  return { partnerStatus: PARTNER_STATUS.UNKNOWN, note: `Unrecognized: ${raw}` };
}

export function activitySupportsTruePartnership(status) {
  return (
    status === EVENT_ACTIVITY_EVIDENCE_STATUS.CONFIRMED_VOLUME ||
    status === EVENT_ACTIVITY_EVIDENCE_STATUS.STRONG_REPEATED_ACTIVITY
  );
}
