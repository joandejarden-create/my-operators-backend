/**
 * Early-signal candidate classification (deterministic, no Airtable writes).
 */
import { MAP_INTEL } from "../api/lib/market-alerts-intelligence-map.js";
import { assessMarketAlertRelevance } from "./market-alerts-relevance.js";
import { computeMarketAlertIntelligence } from "./market-alerts-intelligence.js";
import { inferRegionGroup } from "../api/lib/market-alerts-rss-airtable.js";
import { REGION_GEO_GROUPS } from "./market-alerts-geo-keywords.js";
import { isEarlyLifecycleTiming } from "./market-alerts-signal-timing.js";
import { EARLY_SIGNAL_FAMILY_LABELS } from "./market-alerts-early-signal-queries.js";
import { detectStaleEarlySignal } from "./market-alerts-early-signal-stale.js";
import { isHotelToNonHotelChangeOfUse } from "./market-alerts-change-of-use.js";
import { hasKnownBrandToken } from "./market-alerts-entity-extract.js";
import { PROJECT_DIRECTION_VALUES } from "./market-alerts-project-direction.js";
import { isUsableEntityName, inferPublisherTokens } from "./market-alerts-qualification-gate.js";

const HOTEL_EVIDENCE_RE =
  /\b(hotel|resort|lodge|motel|hostel|inn|aparthotel|lodging|hospitality development|tourism development|\d+[-\s]?(?:key|room)s?)\b/i;

const GENERIC_RE_RE =
  /\b(industrial (?:parcel|land|site)|warehouse (?:parcel|land)|office park|retail plaza|vacant land|commercial land|agriculture|farmland)\b/i;

const OFF_TOPIC_RE =
  /\b(fifa|world cup|miss universe|celebrity (?:news|wedding|gossip)|cruise (?:ship|line|guide)|best hotels? to (?:book|stay)|hotel packing list|consumer hotel list)\b/i;

/** Civic / non-hospitality leakage common in broad planning queries. */
const CIVIC_NOISE_RE =
  /\b(data center|cricket stadium|bus service|transit line|parking lot sale|affordable housing(?!.{0,40}hotel)|apartment (?:building|tower)(?!.{0,40}hotel)|mini-?golf(?!.{0,30}hotel)|lodging tax|convention center commission|master plan\b(?![^]{0,50}hotel)|rezoning for (?:apartments|retail|industrial)|stadium (?:project|development))\b/i;

const DEED_DISPUTE_RE =
  /\b(forged (?:property )?deeds?|faking deeds|deed fraud|suspected of faking)\b/i;

const LATE_STAGE_EVENTS = new Set([
  "Brand Signing",
  "Management Agreement",
  "Operator Appointment",
  "Sale",
  "Acquisition",
  "Hotel For Sale",
  "Reflag",
  "Conversion",
]);

function extractLocationHint(text) {
  const t = String(text || "");
  const phrases = REGION_GEO_GROUPS.flatMap((g) => g.phrases || [])
    .filter((p) => String(p).length >= 4)
    .sort((a, b) => b.length - a.length);
  for (const phrase of phrases) {
    const re = new RegExp(`\\b${phrase.replace(/[.*+?^${}()|[\]\\]/g, "\\$&").replace(/\s+/g, "\\s+")}\\b`, "i");
    if (re.test(t)) {
      return phrase.replace(/\b\w/g, (c) => c.toUpperCase());
    }
  }
  return null;
}

/**
 * Production insert gate — stricter than pilot classification.
 * @param {ReturnType<typeof classifyEarlySignalCandidate>} classified
 * @returns {{ ok: boolean, reason: string|null }}
 */
export function assessEarlySignalProductionReady(classified = {}) {
  if (classified.rejection) {
    return { ok: false, reason: classified.rejection };
  }
  if (!classified.validHospitality || !classified.eventType) {
    return { ok: false, reason: "weak context/entity" };
  }
  if (!isEarlyLifecycleTiming(classified.signalTiming)) {
    return { ok: false, reason: "already decided" };
  }
  const text = `${classified.title || ""} ${classified.summary || ""}`;
  if (!hasHospitalityEvidence(text)) {
    return { ok: false, reason: "non-hotel" };
  }
  const hotelProject = classified.hotelProject || classified.intelligence?.meta?.entities?.hotelProject;
  if (hotelProject) {
    const publishers = inferPublisherTokens(classified.title || "", classified.source || "");
    if (!isUsableEntityName(hotelProject, { publishers })) {
      return { ok: false, reason: "weak context/entity" };
    }
  }
  return { ok: true, reason: null };
}

function sourceDomain(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return null;
  }
}

function hasHospitalityEvidence(text) {
  return HOTEL_EVIDENCE_RE.test(text) || hasKnownBrandToken(text);
}

function hasTitleHospitalityEvidence(title) {
  const t = String(title || "");
  return (
    /\b(hotel|resort|lodging|hospitality|aparthotel|hotel development|resort development)\b/i.test(t) ||
    hasKnownBrandToken(t)
  );
}

/**
 * @param {{ title?: string, summary?: string, source?: string, link?: string, pubDate?: string|null, family?: string, queryId?: string, cala?: boolean, requireTitleHospitality?: boolean }} item
 */
export function classifyEarlySignalCandidate(item = {}) {
  const title = item.title || "";
  const summary = item.summary || "";
  const text = `${title} ${summary}`;
  const relevance = assessMarketAlertRelevance(item);
  const stale = detectStaleEarlySignal({ title, summary, pubDate: item.pubDate });

  const intel = computeMarketAlertIntelligence({ title, summary, sourceName: item.source });
  const eventType = intel.meta.event?.eventType || null;
  const timing = intel.meta.signalTiming || null;
  const projectDirection = intel.meta.projectDirection || null;
  const entities = intel.meta.entities || {};
  const audience = intel.meta.audience || {};
  const region = inferRegionGroup(item);

  let rejection = null;
  if (stale.stale) {
    rejection = "stale";
  } else if (OFF_TOPIC_RE.test(text)) {
    rejection = "off-topic";
  } else if (CIVIC_NOISE_RE.test(text) && !hasHospitalityEvidence(text)) {
    rejection = "off-topic";
  } else if (CIVIC_NOISE_RE.test(title) && !/\b(hotel|resort|lodging|hospitality)\b/i.test(title)) {
    rejection = "off-topic";
  } else if ((item.cala || item.requireTitleHospitality) && !hasTitleHospitalityEvidence(title)) {
    rejection = "off-topic";
  } else if (DEED_DISPUTE_RE.test(text) && !/\b(planned|proposed|development)\s+(hotel|resort)\b/i.test(text)) {
    rejection = "other";
  } else if (isHotelToNonHotelChangeOfUse(text)) {
    rejection = "non-hotel";
  } else if (!relevance.keep) {
    if (/industrial|commercial land|vacant land|warehouse for sale|real estate market/i.test(relevance.reason || "")) {
      rejection = "generic real estate";
    } else if (
      relevance.reason?.startsWith("noise:") ||
      relevance.reason === "google_news_no_hotel_in_title" ||
      relevance.reason === "weak_hotel_signal"
    ) {
      rejection = relevance.reason === "google_news_no_hotel_in_title" ? "off-topic" : "non-hotel";
    } else {
      rejection = "other";
    }
  } else if (GENERIC_RE_RE.test(text) && !hasHospitalityEvidence(text)) {
    rejection = "generic real estate";
  } else if (item.family === "landSite" && !hasHospitalityEvidence(text)) {
    rejection = "generic real estate";
  } else if (!hasHospitalityEvidence(text) && !eventType) {
    rejection = "non-hotel";
  } else if (!eventType) {
    rejection = "weak context/entity";
  } else if (
    LATE_STAGE_EVENTS.has(eventType) &&
    (timing === "Decision Announced" || timing === "Post-Decision")
  ) {
    rejection = "already decided";
  }

  const validHospitality = !!(
    eventType &&
    hasHospitalityEvidence(text) &&
    rejection !== "non-hotel" &&
    rejection !== "generic real estate" &&
    rejection !== "stale" &&
    rejection !== "off-topic"
  );
  const earlyWr =
    validHospitality &&
    intel.meta.treatment === "REVIEW" &&
    isEarlyLifecycleTiming(timing) &&
    !rejection;

  return {
    title,
    summary,
    source: item.source || "",
    link: item.link || "",
    pubDate: item.pubDate || null,
    family: item.family || null,
    familyLabel: EARLY_SIGNAL_FAMILY_LABELS[item.family] || item.family || null,
    queryId: item.queryId || null,
    region,
    location: extractLocationHint(text),
    domain: sourceDomain(item.link),
    eventType,
    signalTiming: timing,
    projectDirection,
    projectLabel: intel.meta.projectLabel || null,
    hotelProject: entities.hotelProject || null,
    ownerDeveloper: entities.ownerDeveloper || null,
    brandInvolved: entities.brandInvolved || null,
    operatorInvolved: entities.operatorInvolved || null,
    rooms: entities.rooms ?? null,
    treatment: rejection && !validHospitality ? "REJECTED" : intel.meta.treatment,
    audience,
    whyOwner: intel.fields?.[MAP_INTEL.whyItMattersOwner] || null,
    whyBrand: intel.fields?.[MAP_INTEL.whyItMattersBrand] || null,
    whyOperator: intel.fields?.[MAP_INTEL.whyItMattersOperator] || null,
    actionOwner: intel.fields?.[MAP_INTEL.recommendedActionOwner] || null,
    intelligence: intel,
    validHospitality,
    earlyWr,
    rejection,
    relevanceReason: relevance.reason || null,
    staleReason: stale.reason || null,
  };
}

export function summarizeEarlySignalPilot(classified = []) {
  const byFamily = {};
  const byTiming = {
    "Pre-Decision": 0,
    "Decision Forming": 0,
    "Decision Announced": 0,
    "Post-Decision": 0,
  };
  const byDirection = {
    Advancing: 0,
    "Under Review": 0,
    Challenged: 0,
    Delayed: 0,
    "Rejected / Blocked": 0,
    Unknown: 0,
  };
  const timingXDirection = {};
  const byRegion = {};
  const rejectionReasons = {
    "non-hotel": 0,
    "generic real estate": 0,
    duplicate: 0,
    "already decided": 0,
    "weak context/entity": 0,
    stale: 0,
    "off-topic": 0,
    other: 0,
  };

  let review = 0;
  let standard = 0;
  let rejected = 0;
  let valid = 0;
  let ownerWr = 0;
  let brandWr = 0;
  let operatorWr = 0;

  for (const row of classified) {
    const fam = row.family || "unknown";
    byFamily[fam] = byFamily[fam] || {
      raw: 0,
      valid: 0,
      review: 0,
      standard: 0,
      rejected: 0,
      preDecision: 0,
      decisionForming: 0,
      rejectionCounts: {},
    };
    byFamily[fam].raw += 1;
    byRegion[row.region || "Unknown"] = (byRegion[row.region || "Unknown"] || 0) + 1;

    if (row.signalTiming && byTiming[row.signalTiming] != null) {
      byTiming[row.signalTiming] += 1;
    }
    const dir = PROJECT_DIRECTION_VALUES.includes(row.projectDirection)
      ? row.projectDirection
      : "Unknown";
    byDirection[dir] += 1;
    if (row.signalTiming) {
      const cross = `${row.signalTiming} + ${dir}`;
      timingXDirection[cross] = (timingXDirection[cross] || 0) + 1;
    }

    if (row.signalTiming === "Pre-Decision") byFamily[fam].preDecision += 1;
    if (row.signalTiming === "Decision Forming") byFamily[fam].decisionForming += 1;

    if (row.rejection && !row.validHospitality) {
      rejected += 1;
      byFamily[fam].rejected += 1;
      const key = rejectionReasons[row.rejection] != null ? row.rejection : "other";
      rejectionReasons[key] += 1;
      byFamily[fam].rejectionCounts[key] = (byFamily[fam].rejectionCounts[key] || 0) + 1;
      continue;
    }

    if (row.validHospitality) {
      valid += 1;
      byFamily[fam].valid += 1;
    }

    if (row.treatment === "REVIEW") {
      review += 1;
      byFamily[fam].review += 1;
    } else {
      standard += 1;
      byFamily[fam].standard += 1;
    }

    if (row.audience?.owner?.worthReviewing) ownerWr += 1;
    if (row.audience?.brand?.worthReviewing) brandWr += 1;
    if (row.audience?.operator?.worthReviewing) operatorWr += 1;

    if (row.rejection === "already decided") {
      rejectionReasons["already decided"] += 1;
    }
  }

  for (const fam of Object.keys(byFamily)) {
    const counts = byFamily[fam].rejectionCounts || {};
    const top = Object.entries(counts).sort((a, b) => b[1] - a[1])[0];
    byFamily[fam].topRejectionReason = top ? top[0] : null;
  }

  return {
    classified: classified.length,
    validHospitalitySignals: valid,
    review,
    standard,
    rejected,
    byFamily,
    byTiming,
    byDirection,
    timingXDirection,
    byRegion,
    audience: { ownerWr, brandWr, operatorWr },
    rejectionReasons,
  };
}
