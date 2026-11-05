/**
 * Scaling Companies — activity chronology & recency classification V1.
 *
 * Distinguishes original corporate event dates from research/verification dates.
 * Shared across all hotels. Does not invent continuity.
 */

export const SCALING_ACTIVITY_CHRONOLOGY_V1 =
  "gdi-scaling-activity-chronology-v1";

export const SCALING_ACTIVITY_STATUS = Object.freeze({
  CURRENT_SUPPORTED_EXPANSION: "CURRENT_SUPPORTED_EXPANSION",
  UPCOMING_SUPPORTED_ACTIVITY: "UPCOMING_SUPPORTED_ACTIVITY",
  CONTINUING_ESTABLISHED_OPERATIONS: "CONTINUING_ESTABLISHED_OPERATIONS",
  HISTORICAL_ACCOUNT_DEVELOPMENT: "HISTORICAL_ACCOUNT_DEVELOPMENT",
  STALE_OR_INSUFFICIENT_EVIDENCE: "STALE_OR_INSUFFICIENT_EVIDENCE",
  UNRESOLVED_CHRONOLOGY: "UNRESOLVED_CHRONOLOGY",
});

const MONTHS = {
  january: 1,
  jan: 1,
  february: 2,
  feb: 2,
  march: 3,
  mar: 3,
  april: 4,
  apr: 4,
  may: 5,
  june: 6,
  jun: 6,
  july: 7,
  jul: 7,
  august: 8,
  aug: 8,
  september: 9,
  sep: 9,
  sept: 9,
  october: 10,
  oct: 10,
  november: 11,
  nov: 11,
  december: 12,
  dec: 12,
};

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function parseIsoDate(s) {
  const m = text(s).match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!m) return null;
  const d = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
  return Number.isNaN(d.getTime()) ? null : d;
}

function toIso(d) {
  if (!(d instanceof Date) || Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

/**
 * Extract the earliest plausible corporate event / publication date from prose.
 * Prefers explicit Month DD, YYYY and ISO dates. Ignores "evidence dated YYYY-MM-DD"
 * research-verification phrases.
 */
export function extractActivityDatesFromText(input = "") {
  const s = text(input);
  if (!s) return [];
  const cleaned = s
    .replace(/evidence dated\s+\d{4}-\d{2}-\d{2}/gi, " ")
    .replace(/verified\s+\d{4}-\d{2}-\d{2}/gi, " ")
    .replace(/as of\s+research[^.]{0,40}/gi, " ");
  const out = [];
  const monthRe =
    /\b(January|February|March|April|May|June|July|August|September|October|November|December|Jan\.?|Feb\.?|Mar\.?|Apr\.?|Jun\.?|Jul\.?|Aug\.?|Sep\.?|Sept\.?|Oct\.?|Nov\.?|Dec\.?)\s+(\d{1,2}),?\s+(\d{4})\b/gi;
  let m;
  while ((m = monthRe.exec(cleaned))) {
    const mo = MONTHS[m[1].toLowerCase().replace(/\./g, "")];
    const day = Number(m[2]);
    const year = Number(m[3]);
    if (mo && day >= 1 && day <= 31 && year >= 1990 && year <= 2100) {
      out.push({
        iso: `${year}-${String(mo).padStart(2, "0")}-${String(day).padStart(2, "0")}`,
        raw: m[0],
        kind: "publication_or_event",
      });
    }
  }
  const isoRe = /\b(20\d{2})-(\d{2})-(\d{2})\b/g;
  while ((m = isoRe.exec(cleaned))) {
    out.push({
      iso: `${m[1]}-${m[2]}-${m[3]}`,
      raw: m[0],
      kind: "iso",
    });
  }
  // Year-only when clearly an event year ("in 2022", "permanent residence in 2022")
  const yearOnly = cleaned.match(
    /\b(?:in|by|during|for)\s+(20(?:1\d|2\d))\b/gi,
  );
  for (const y of yearOnly || []) {
    const yr = y.match(/20\d{2}/)?.[0];
    if (yr) {
      out.push({
        iso: `${yr}-01-01`,
        raw: y,
        kind: "year_only",
        precision: "year",
      });
    }
  }
  // Dedupe by iso
  const seen = new Set();
  return out.filter((d) => {
    if (!d.iso || seen.has(d.iso + d.kind)) return false;
    seen.add(d.iso + d.kind);
    return Boolean(parseIsoDate(d.iso));
  });
}

function monthsBetween(fromIso, toIso) {
  const a = parseIsoDate(fromIso);
  const b = parseIsoDate(toIso) || new Date();
  if (!a) return null;
  return (
    (b.getUTCFullYear() - a.getUTCFullYear()) * 12 +
    (b.getUTCMonth() - a.getUTCMonth())
  );
}

function sourceBlob(opportunity = {}) {
  const parts = [
    opportunity.whatIsHappening,
    opportunity.localConnection,
    opportunity.localPresence,
    opportunity.datedGrowthEvidence,
    opportunity.summaryWhat,
    opportunity.activityHeadline,
    opportunity.growthHeadline,
    opportunity.sourceTitle,
  ];
  for (const s of opportunity.sources || []) {
    parts.push(s.title, s.excerpt, s.snippet, s.publisherDate, s.publishedAt);
  }
  for (const e of opportunity.evidence || []) {
    parts.push(e.statement, e.quote, e.excerpt, e.publishedAt, e.date);
  }
  if (opportunity.sourceFetchSnippet) parts.push(opportunity.sourceFetchSnippet);
  if (opportunity.growthSourceBodySnippet) parts.push(opportunity.growthSourceBodySnippet);
  return parts.filter(Boolean).join(" \n ");
}

/**
 * Classify activity chronology for one Scaling opportunity.
 */
export function classifyScalingActivityChronology(
  opportunity = {},
  opts = {},
) {
  const nowIso = opts.asOfIso || toIso(new Date());
  const researchVerifiedAt = text(
    opportunity.lastResearchedAt ||
      opportunity.evidenceFreshnessAt ||
      opportunity.researchVerifiedAt ||
      "",
  ).slice(0, 10);

  const blob = sourceBlob(opportunity);
  const extracted = extractActivityDatesFromText(blob);
  const explicitGrowth = text(opportunity.growthEvidenceDate);
  // Reject research-verification masquerading as growth date
  const growthLooksLikeResearch =
    explicitGrowth &&
    researchVerifiedAt &&
    explicitGrowth === researchVerifiedAt;

  let originalEventDate = null;
  let originalEventPrecision = "day";
  let sourcePublicationDate = null;

  if (explicitGrowth && !growthLooksLikeResearch && parseIsoDate(explicitGrowth)) {
    // Reject same-day research batch contamination (Oct 26–27 2026 release window)
    if (!/^2026-10-2[67]$/.test(explicitGrowth.slice(0, 10))) {
      originalEventDate = explicitGrowth.slice(0, 10);
    }
  }

  const dayPrecision = extracted.filter((d) => d.kind !== "year_only");
  // Drop dates that equal the research verification day — not corporate events
  const usableDay = dayPrecision.filter((d) => {
    if (researchVerifiedAt && d.iso === researchVerifiedAt) return false;
    if (/^2026-10-2[67]$/.test(d.iso)) return false;
    return true;
  });
  if (!originalEventDate && usableDay.length) {
    // Prefer earliest day-precision date in blob (announcement date)
    usableDay.sort((a, b) => a.iso.localeCompare(b.iso));
    originalEventDate = usableDay[0].iso;
    sourcePublicationDate = usableDay[0].iso;
  } else if (!originalEventDate && extracted.length) {
    const usable = extracted.filter(
      (d) =>
        !(researchVerifiedAt && d.iso === researchVerifiedAt) &&
        !/^2026-10-2[67]$/.test(d.iso),
    );
    if (usable.length) {
      usable.sort((a, b) => a.iso.localeCompare(b.iso));
      originalEventDate = usable[0].iso;
      originalEventPrecision = usable[0].precision || "year";
    }
  }

  if (!sourcePublicationDate && dayPrecision.length) {
    sourcePublicationDate = dayPrecision[0].iso;
  }

  const continuityHints = {
    careersListsCity: /careers|global offices/i.test(blob) &&
      opts.hotelCity &&
      new RegExp(String(opts.hotelCity), "i").test(blob),
    currentOperationsClaimed: /currently operate|now open|opened its|office in/i.test(
      blob,
    ),
    continuityEvidenceFlag: opportunity.continuityEvidenceEstablished === true,
    continuityNote: text(opportunity.continuityEvidenceNote),
  };

  const hasContinuity =
    continuityHints.continuityEvidenceFlag ||
    Boolean(continuityHints.continuityNote) ||
    (opts.continuityEvidence === true);

  const ageMonths = originalEventDate
    ? monthsBetween(originalEventDate, nowIso)
    : null;

  let status = SCALING_ACTIVITY_STATUS.UNRESOLVED_CHRONOLOGY;
  let customerFacingAsCurrentExpansion = false;
  let reason = "No reliable original event/publication date extracted.";

  if (originalEventDate && ageMonths != null) {
    if (ageMonths <= 18) {
      status = SCALING_ACTIVITY_STATUS.CURRENT_SUPPORTED_EXPANSION;
      customerFacingAsCurrentExpansion = true;
      reason = `Original activity dated ${originalEventDate} is within 18 months of ${nowIso}.`;
    } else if (ageMonths <= 30 && /intend|plan|will open|upcoming|202[5-9]/i.test(blob)) {
      status = SCALING_ACTIVITY_STATUS.UPCOMING_SUPPORTED_ACTIVITY;
      customerFacingAsCurrentExpansion = true;
      reason = `Planned/upcoming language with dated signal ${originalEventDate}.`;
    } else if (hasContinuity) {
      status = SCALING_ACTIVITY_STATUS.CONTINUING_ESTABLISHED_OPERATIONS;
      customerFacingAsCurrentExpansion = false;
      reason = `Original announcement ${originalEventDate} is historical; continuity evidence supports ongoing local operations (account development, not a new expansion trigger).`;
    } else if (ageMonths > 24) {
      status = SCALING_ACTIVITY_STATUS.HISTORICAL_ACCOUNT_DEVELOPMENT;
      customerFacingAsCurrentExpansion = false;
      reason = `Original activity dated ${originalEventDate} is >24 months old without verified continuity — do not market as newly expanding.`;
    } else {
      status = SCALING_ACTIVITY_STATUS.STALE_OR_INSUFFICIENT_EVIDENCE;
      customerFacingAsCurrentExpansion = false;
      reason = `Activity date ${originalEventDate} needs continuity research before current-expansion framing.`;
    }
  }

  return {
    version: SCALING_ACTIVITY_CHRONOLOGY_V1,
    opportunityId: opportunity.id || opportunity.opportunityId || null,
    organizationName: opportunity.organizationName || null,
    originalEventDate,
    originalEventPrecision,
    sourcePublicationDate,
    researchVerificationDate: researchVerifiedAt || null,
    researchDateMustNotBeUsedAsActivityDate: true,
    extractedDates: extracted.slice(0, 12),
    ageMonths,
    activityStatus: status,
    customerFacingAsCurrentExpansion,
    hasContinuityEvidence: hasContinuity,
    continuityHints,
    reason,
    // Canonical field to persist — never the research verification date
    recommendedGrowthEvidenceDate:
      originalEventDate && originalEventPrecision === "day"
        ? originalEventDate
        : originalEventDate && originalEventPrecision === "year"
          ? String(originalEventDate).slice(0, 4)
          : null,
  };
}

/**
 * Patch opportunity chronology fields without inventing activity.
 */
export function applyScalingActivityChronology(opportunity = {}, opts = {}) {
  const chrono = classifyScalingActivityChronology(opportunity, opts);
  const next = {
    ...opportunity,
    activityChronology: chrono,
    activityStatus: chrono.activityStatus,
    growthEvidenceDate:
      chrono.recommendedGrowthEvidenceDate ||
      (opportunity.growthEvidenceDate &&
      opportunity.growthEvidenceDate !== chrono.researchVerificationDate
        ? opportunity.growthEvidenceDate
        : chrono.recommendedGrowthEvidenceDate),
    originalActivityDate: chrono.originalEventDate,
    sourcePublicationDate: chrono.sourcePublicationDate,
    researchVerificationDate: chrono.researchVerificationDate,
  };
  return next;
}
