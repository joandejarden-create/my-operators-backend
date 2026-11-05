/**
 * Shared loader + mapper for paid multisegment candidate-dispositions artifacts.
 *
 * Paid MS writers persist `{ generatedAt, rows: [...] }`.
 * Several coordinator stages historically read only `dispositions` / raw arrays,
 * which silently produced an empty maturation pipeline after successful paid MS
 * (Charlotte 23→0 Eligibility/Ready handoff failure).
 *
 * Classification: REUSABLE_PRODUCT_LOGIC
 */

import fs from "node:fs";

export const CANDIDATE_DISPOSITIONS_LOAD_VERSION =
  "gdi-candidate-dispositions-load-v1";

/** Research-layer dispositions that may advance to Eligibility / Ready. */
export const RESEARCH_LAYER_PUBLISHABLE = new Set([
  "qualified_publishable",
]);

/**
 * Quarantined / rejected / deferred research-layer rows must not be promoted
 * to HOTEL_FACING or Ready solely because package prose looks complete.
 */
export function isResearchLayerPublishableDisposition(disposition) {
  const d = String(disposition || "")
    .trim()
    .toLowerCase();
  if (!d) return true; // unknown / non-disposition candidates: leave to eligibility
  return RESEARCH_LAYER_PUBLISHABLE.has(d);
}

/**
 * Normalize disposition document shapes into a row array.
 * Accepts: Array | { rows } | { dispositions } | { raw: { dispositions } }
 */
export function loadCandidateDispositionRows(docOrRows) {
  if (Array.isArray(docOrRows)) return docOrRows.filter(Boolean);
  if (!docOrRows || typeof docOrRows !== "object") return [];
  if (Array.isArray(docOrRows.rows)) return docOrRows.rows.filter(Boolean);
  if (Array.isArray(docOrRows.dispositions)) {
    return docOrRows.dispositions.filter(Boolean);
  }
  if (Array.isArray(docOrRows.raw?.dispositions)) {
    return docOrRows.raw.dispositions.filter(Boolean);
  }
  if (Array.isArray(docOrRows.raw?.rows)) {
    return docOrRows.raw.rows.filter(Boolean);
  }
  return [];
}

/**
 * Read candidate-dispositions.json from disk (missing → []).
 */
export function readCandidateDispositionFile(filePath) {
  if (!filePath || !fs.existsSync(filePath)) {
    return { ok: false, path: filePath || null, rows: [], error: "missing" };
  }
  try {
    const doc = JSON.parse(fs.readFileSync(filePath, "utf8"));
    const rows = loadCandidateDispositionRows(doc);
    return {
      ok: true,
      path: filePath,
      rows,
      generatedAt: doc?.generatedAt || null,
      count: rows.length,
      version: CANDIDATE_DISPOSITIONS_LOAD_VERSION,
    };
  } catch (err) {
    return {
      ok: false,
      path: filePath,
      rows: [],
      error: err?.message || String(err),
    };
  }
}

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

const MONTHS = Object.freeze({
  january: "01",
  february: "02",
  march: "03",
  april: "04",
  may: "05",
  june: "06",
  july: "07",
  august: "08",
  september: "09",
  october: "10",
  november: "11",
  december: "12",
});

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function isFutureOrTodayIso(iso, today = todayIsoDate()) {
  return Boolean(iso && iso >= today);
}

function toIsoFromMonthDayYear(monthName, day, year) {
  const mm = MONTHS[String(monthName || "").toLowerCase()];
  if (!mm || !year) return null;
  const dd = String(day || "01").padStart(2, "0");
  return `${year}-${mm}-${dd}`;
}

/**
 * Collect ISO date candidates from package evidence.
 * Prefer *future* lodging/event dates over announcement/publication dates.
 * `growthEvidenceDate` is often the announcement day — do not use it as the
 * commercial eventStartDate when a later date exists in source prose.
 */
export function extractDispositionTiming(pkg = {}, row = {}, opts = {}) {
  const today = opts.today || todayIsoDate();
  const blob = [
    pkg.datedGrowthEvidence,
    pkg.localConnection,
    pkg.eventActivityEvidence,
    pkg.proposedHotelUse,
    row.raw?.datedGrowthEvidence,
    row.raw?.localConnection,
  ]
    .map(text)
    .filter(Boolean)
    .join(" ");

  const candidates = [];

  const push = (iso, source, score) => {
    if (!iso || !/^20\d{2}(-\d{2}-\d{2})?$/.test(iso)) return;
    const normalized = iso.length === 4 ? `${iso}-12-31` : iso;
    candidates.push({ iso: normalized, source, score });
  };

  // Explicit event fields (high trust)
  for (const [val, source, score] of [
    [pkg.eventStartDate, "pkg.eventStartDate", 100],
    [row.raw?.eventStartDate, "raw.eventStartDate", 100],
    [pkg.eventDate, "pkg.eventDate", 90],
  ]) {
    const t = text(val);
    if (/^20\d{2}-\d{2}-\d{2}/.test(t)) push(t.slice(0, 10), source, score);
  }

  // Month-name dates in prose (prefer future; boost lodging/meeting context)
  // Also: "March 19 and 21, 2027" / "March 19–21, 2027"
  const monthRe =
    /\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})(?:\s*(?:[–\-]|&|and)\s*(?:\d{1,2}))?,?\s+(20\d{2})\b/gi;
  let m;
  while ((m = monthRe.exec(blob))) {
    const iso = toIsoFromMonthDayYear(m[1], m[2], m[3]);
    const ctx = blob.slice(Math.max(0, m.index - 40), m.index + m[0].length + 40);
    let score = 50;
    if (/meeting|conference|tournament|program date|packages?|season|performance|show|lodging|accommodations?/i.test(ctx)) {
      score += 25;
    }
    if (/announced|announcement|posted|job listing|opened|signed lease|appointment|staffing|interns/i.test(ctx)) {
      score -= 20;
    }
    if (isFutureOrTodayIso(iso, today)) score += 40;
    else score -= 50;
    push(iso, "prose_month_day_year", score);
  }

  // ISO dates in prose
  const isoRe = /\b(20\d{2}-\d{2}-\d{2})\b/g;
  let im;
  while ((im = isoRe.exec(blob))) {
    const iso = im[1];
    const ctx = blob.slice(Math.max(0, im.index - 40), im.index + 50);
    let score = 40;
    if (/meeting|conference|tournament|program|packages?|lodging/i.test(ctx)) score += 20;
    if (/announced|announcement|posted|opened/i.test(ctx)) score -= 20;
    if (isFutureOrTodayIso(iso, today)) score += 40;
    else score -= 50;
    push(iso, "prose_iso", score);
  }

  // growthEvidenceDate last — often announcement date, only if still future
  const growth =
    text(pkg.growthEvidenceDate) || text(row.raw?.growthEvidenceDate) || "";
  if (/^20\d{2}-\d{2}-\d{2}/.test(growth)) {
    const iso = growth.slice(0, 10);
    push(
      iso,
      "growthEvidenceDate",
      isFutureOrTodayIso(iso, today) ? 30 : -100
    );
  }

  if (!candidates.length) return null;

  candidates.sort((a, b) => b.score - a.score || b.iso.localeCompare(a.iso));
  const best = candidates[0];
  // Refuse past announcement-only dates when no future commercial date exists
  if (best.score < 0) return null;
  return best.iso;
}

/**
 * Map a paid-MS disposition row (+ nested package) onto opportunity-shaped
 * fields eligibility / Ready / maturation expect.
 *
 * Does not invent lodging demand or named buyers. Preserves package evidence.
 */
export function mapDispositionRowToOpportunityCandidate(row = {}, opts = {}) {
  const pkg = row.package || row.raw || {};
  const hotelId = opts.hotelId || row.hotelId || pkg.hotelId || null;
  const organizationName =
    text(pkg.organizationName) ||
    text(row.organizationName) ||
    text(pkg.company) ||
    "";
  const extractedTiming = extractDispositionTiming(pkg, row);
  const announcementDate =
    text(pkg.growthEvidenceDate) || text(row.raw?.growthEvidenceDate) || null;
  const sourceUrls = []
    .concat(pkg.sourceUrls || [])
    .concat(pkg.sourceUrl ? [pkg.sourceUrl] : [])
    .map(text)
    .filter(Boolean);
  const uniqueSources = [...new Set(sourceUrls)];

  const contactRole = text(pkg.contactRole) || null;
  // Source-backed functional desk / role text — never invent a named person.
  const buyerRole =
    text(pkg.buyerRole) ||
    text(pkg.primaryContactRole) ||
    (contactRole && !/not a verified|unknown|n\/a/i.test(contactRole)
      ? contactRole
      : null);

  const segment = row.segment || pkg.segment || pkg.researchSegment || null;
  const openEndedSegment =
    /scaling_companies|corporate_meetings_training|professional_planners_dmc|local_meetings_catering|sports|entertainment_production|exhibitor_sponsor_teams/i.test(
      String(segment || "")
    );
  // Never stamp a past announcement date as the commercial event date —
  // year-in-title parsing would PAST_CLOSED the card (CONSILIO 2024 regression).
  const commercialEventStart =
    extractedTiming && isFutureOrTodayIso(extractedTiming)
      ? extractedTiming
      : null;

  const opportunityType =
    text(pkg.opportunityType) ||
    (/scaling_companies|corporate_meetings_training/i.test(String(segment || ""))
      ? "CORPORATE_ACCOUNT_RELATIONSHIP"
      : /professional_planners_dmc|weddings_social|local_meetings_catering/i.test(
            String(segment || "")
          )
        ? "VENUE_PARTNERSHIP"
        : /sports|entertainment_production|exhibitor_sponsor_teams|tours_educational/i.test(
              String(segment || "")
            )
          ? "PREFERRED_LODGING"
          : null);

  const title =
    text(pkg.title) ||
    text(pkg.opportunityTitle) ||
    (organizationName
      ? `${organizationName}${
          commercialEventStart
            ? ` ${String(commercialEventStart).slice(0, 4)}`
            : ""
        } lodging opportunity`
      : "");

  return {
    id:
      row.id ||
      row.opportunityId ||
      pkg.id ||
      `disp_${hotelId || "hotel"}_${String(organizationName || "org")
        .toLowerCase()
        .replace(/[^\w]+/g, "_")
        .slice(0, 48)}`,
    hotelId,
    organizationName: organizationName || null,
    company: organizationName || null,
    title: title || null,
    displayTitle: title || organizationName || null,
    timing: commercialEventStart || null,
    eventStartDate: commercialEventStart || null,
    eventEndDate: commercialEventStart || null,
    eventDateDisplay: commercialEventStart || null,
    announcementDate: announcementDate || null,
    opportunityType: opportunityType || undefined,
    demandType:
      opportunityType === "CORPORATE_ACCOUNT_RELATIONSHIP"
        ? "corporate offsite recurring pattern"
        : opportunityType === "VENUE_PARTNERSHIP"
          ? "preferred lodging partnership"
          : undefined,
    hotelFitRationale: text(pkg.hotelFitRationale) || null,
    proposedHotelUse: text(pkg.proposedHotelUse) || null,
    localConnection: text(pkg.localConnection) || null,
    datedGrowthEvidence: text(pkg.datedGrowthEvidence) || null,
    lodgingOrReferralRationale:
      text(pkg.lodgingOrReferralRationale) ||
      text(pkg.proposedHotelUse) ||
      null,
    hotelOpportunityThesis: text(pkg.hotelFitRationale) || null,
    whyThisHotel: text(pkg.hotelFitRationale) || null,
    eventActivityEvidence: text(pkg.eventActivityEvidence) || null,
    establishedFacts: [
      pkg.datedGrowthEvidence,
      pkg.localConnection,
      pkg.proposedHotelUse,
      pkg.hotelFitRationale,
    ]
      .map(text)
      .filter(Boolean),
    contactEmail: text(pkg.contactEmail) || null,
    functionalContactEmail: text(pkg.contactEmail) || null,
    organizationContactEmail: text(pkg.contactEmail) || null,
    contactPhone: text(pkg.contactPhone) || null,
    companyPhone: text(pkg.contactPhone) || null,
    contactUrl: text(pkg.contactUrl) || null,
    organizationContactUrl: text(pkg.contactUrl) || null,
    publicContactPath: text(pkg.contactUrl) || null,
    contactName: text(pkg.contactName) || null,
    contactRole,
    buyerRole,
    primaryContactRole: buyerRole,
    primaryContact: {
      name: text(pkg.contactName) || undefined,
      email: text(pkg.contactEmail) || undefined,
      phone: text(pkg.contactPhone) || undefined,
      role: buyerRole || undefined,
    },
    contact: {
      businessEmail: text(pkg.contactEmail) || undefined,
      businessPhone: text(pkg.contactPhone) || undefined,
      contactFormUrl: text(pkg.contactUrl) || undefined,
      name: text(pkg.contactName) || undefined,
      role: buyerRole || undefined,
    },
    // Functional org route from package = who-research already attempted
    whoHowResearchState: "RESEARCHED_FUNCTIONAL",
    contactResearchState: "RESEARCHED",
    sources: uniqueSources.map((url) => ({ url })),
    sourceUrls: uniqueSources,
    officialDomain: text(pkg.officialDomain) || null,
    officialSource: uniqueSources[0] || null,
    recommendedAction: text(pkg.recommendedAction) || null,
    materialLimitations: pkg.materialLimitations || [],
    researchSegment: segment || null,
    segment: segment || null,
    researchLayerDisposition: row.disposition || null,
    disposition: row.disposition || null,
    taskId: row.taskId || null,
    package: pkg,
    travelEvidenceNote:
      text(pkg.proposedHotelUse) || text(pkg.datedGrowthEvidence) || null,
    // Source-backed why-now from package evidence (not invented urgency).
    whyNow:
      text(pkg.whyNow) ||
      text(pkg.datedGrowthEvidence) ||
      text(pkg.localConnection) ||
      null,
    summaryWhyHotel: text(pkg.hotelFitRationale) || null,
    futureDemand: commercialEventStart
      ? {
          supported: true,
          dateWindow: commercialEventStart,
          sourceUrl: uniqueSources[0] || null,
        }
      : openEndedSegment && text(pkg.datedGrowthEvidence)
        ? {
            supported: true,
            dateWindow: text(pkg.datedGrowthEvidence).slice(0, 120),
            sourceUrl: uniqueSources[0] || null,
          }
        : pkg.futureDemand || row.raw?.futureDemand || undefined,
    _fromDisposition: true,
    _dispositionLoadVersion: CANDIDATE_DISPOSITIONS_LOAD_VERSION,
  };
}

/**
 * Map all publishable (or all) disposition rows to opportunity candidates.
 */
export function mapDispositionRowsToOpportunityCandidates(
  rows = [],
  opts = {}
) {
  const list = Array.isArray(rows) ? rows : [];
  const onlyPublishable = opts.onlyPublishable !== false;
  return list
    .filter((r) => {
      if (!onlyPublishable) return true;
      const d = String(r.disposition || "").toLowerCase();
      return (
        d === "qualified_publishable" ||
        d === "contact_resolved" ||
        d === "publishable"
      );
    })
    .map((r) => mapDispositionRowToOpportunityCandidate(r, opts));
}

/**
 * True when paid MS produced disposition rows but a downstream stage treated
 * the pipeline as empty / completed without ingesting them.
 */
export function isEmptyPipelineAfterPaidDispositions({
  dispositionRowCount = 0,
  maturedCount = null,
  eligibilityEvaluatedCount = null,
} = {}) {
  if (Number(dispositionRowCount) <= 0) return false;
  if (maturedCount != null && Number(maturedCount) === 0) return true;
  if (
    eligibilityEvaluatedCount != null &&
    Number(eligibilityEvaluatedCount) === 0
  ) {
    return true;
  }
  return false;
}

export default {
  CANDIDATE_DISPOSITIONS_LOAD_VERSION,
  loadCandidateDispositionRows,
  readCandidateDispositionFile,
  extractDispositionTiming,
  mapDispositionRowToOpportunityCandidate,
  mapDispositionRowsToOpportunityCandidates,
  isEmptyPipelineAfterPaidDispositions,
};
