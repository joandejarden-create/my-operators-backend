/**
 * HPC historical facts: Year Built (via Opening Date) + renovation events.
 * Schema has no dedicated Year Built — construction year is Opening Date year
 * with explicit provenance. Renovation uses Renovation / Conversion * fields.
 */

export const HE_HPC_HISTORICAL_FACTS_V1 = "he-hpc-historical-facts-v1";

const SEC_CMBS_URL =
  "https://www.sec.gov/Archives/edgar/data/1013611/000153949724000828/n4133-x6ts.htm";
const CMBS_NEWS_URL = "https://cmbs.news/property/bethesda-marriott-bethesda-md/";
const PATCH_2015_URL =
  "https://patch.com/maryland/bethesda-chevychase/open-door-unexpected-bethesda-marriott-unveils-contemporary-guestroom-design";
const CVENT_URL =
  "https://www.cvent.com/venues/es-ES/bethesda/hotel/bethesda-marriott/venue-759e1222-3f0f-4739-9375-e9cb7962c6a4";

export const BETHESDA_YEAR_BUILT_EVIDENCE = Object.freeze({
  hotelId: "recLuxvwwxID7U2B8",
  yearBuilt: 1979,
  disposition: "VERIFIED_MAPPED_VIA_OPENING_DATE",
  hpcField: "Opening Date",
  hpcValue: "1979-01-01",
  note:
    "HPC has no Year Built field. Verified construction year 1979 is stored as Opening Date and must hydrate hotel.yearBuilt in all HE DTOs.",
  sources: [
    {
      role: "primary_sec_cmbs",
      year: 1979,
      claim: "Built 1979",
      url: SEC_CMBS_URL,
      verification: "VERIFIED",
    },
    {
      role: "corroborating_cmbs_news",
      year: 1979,
      claim: "Built 1979",
      url: CMBS_NEWS_URL,
      verification: "VERIFIED",
    },
  ],
});

export const BETHESDA_RENOVATION_EVENTS = Object.freeze([
  {
    eventId: "beth_reno_2015_major",
    year: 2015,
    scope: "PROPERTY_WIDE_MAJOR_RENOVATION",
    role: "primary_last_renovated_display",
    claim: "Renovated 2015 (SEC CMBS / Travel Weekly / local press)",
    url: SEC_CMBS_URL,
    corroboratingUrls: [PATCH_2015_URL, CMBS_NEWS_URL],
    verification: "VERIFIED",
    displayPriority: 1,
  },
  {
    eventId: "beth_unveil_2017_guestroom",
    year: 2017,
    scope: "GUESTROOM_PUBLIC_SPACE_UNVEIL_PRESS",
    role: "secondary_milestone",
    claim: "2017 guestroom / public-space unveil press; Cvent lists Renovated 2017",
    url: CVENT_URL,
    corroboratingUrls: [PATCH_2015_URL],
    verification: "VERIFIED_AS_MILESTONE_NOT_REPLACEMENT",
    displayPriority: 2,
    note: "Do not overwrite 2015 major renovation with 2017 solely from Cvent.",
  },
]);

export function extractYearFromDateLike(raw) {
  if (raw == null || raw === "") return null;
  if (typeof raw === "number" && Number.isFinite(raw)) {
    const y = Math.round(raw);
    return y >= 1800 && y <= 2100 ? y : null;
  }
  const m = String(raw).match(/(18|19|20)\d{2}/);
  if (!m) return null;
  const y = Number(m[0]);
  return Number.isFinite(y) && y >= 1800 && y <= 2100 ? y : null;
}

/**
 * Resolve Year Built for HE display from HPC fields.
 */
export function resolveYearBuiltFromHpcFields(fields = {}) {
  const dedicated = extractYearFromDateLike(fields["Year Built"]);
  if (dedicated != null) {
    return {
      yearBuilt: dedicated,
      sourceField: "Year Built",
      class: "VERIFIED_HPC",
      openingDate: fields["Opening Date"] || null,
    };
  }
  const fromOpening = extractYearFromDateLike(fields["Opening Date"]);
  if (fromOpening != null) {
    return {
      yearBuilt: fromOpening,
      sourceField: "Opening Date",
      class: "VERIFIED_DERIVED_FROM_OPENING_DATE",
      openingDate: fields["Opening Date"] || null,
      sourceUrl: fields["Opening Date Source URL"] || null,
      note: "Construction year derived from HPC Opening Date (no Year Built column).",
    };
  }
  return {
    yearBuilt: null,
    sourceField: null,
    class: "EMPTY",
    openingDate: null,
  };
}

/**
 * Resolve Last Renovated display + event list from HPC + known evidence pack.
 */
export function resolveLastRenovatedFromHpcFields(fields = {}, opts = {}) {
  const events =
    Array.isArray(opts.events) && opts.events.length
      ? opts.events
      : opts.hotelId === "recLuxvwwxID7U2B8" || opts.includeBethesdaPack === true
        ? BETHESDA_RENOVATION_EVENTS
        : [];

  const hpcDate = fields["Renovation / Conversion Date"] || null;
  const hpcYear = extractYearFromDateLike(hpcDate);
  const hpcStatus = fields["Renovation / Conversion Status"] || null;
  const hpcUrl = fields["Renovation / Conversion Source URL"] || null;

  let displayYear = hpcYear;
  let className = hpcYear != null ? "VERIFIED_HPC" : "EMPTY";
  let note = hpcStatus || null;

  if (displayYear == null && events.length) {
    const primary = [...events].sort(
      (a, b) => (a.displayPriority || 99) - (b.displayPriority || 99),
    )[0];
    displayYear = primary?.year ?? null;
    className = displayYear != null ? "VERIFIED_FROM_EVIDENCE_PACK" : "EMPTY";
    note =
      primary?.scope === "PROPERTY_WIDE_MAJOR_RENOVATION"
        ? `Major renovation ${primary.year}; additional milestones retained in evidence`
        : primary?.claim || null;
  }

  return {
    lastRenovated: displayYear,
    yearRenovated: displayYear,
    sourceField: hpcYear != null ? "Renovation / Conversion Date" : null,
    class: className,
    status: hpcStatus,
    sourceUrl: hpcUrl,
    note,
    renovationEvents: events,
  };
}

/**
 * Authorized null-fill patch for Bethesda renovation (dry-run safe).
 */
export function buildBethesdaRenovationNullFillPatch() {
  return {
    recordId: "recLuxvwwxID7U2B8",
    patch: {
      "Renovation / Conversion Date": "2015-01-01",
      "Renovation / Conversion Status":
        "Major renovation 2015 (SEC/CMBS). Guestroom unveil press 2017 retained as separate milestone — not a replacement year.",
      "Renovation / Conversion Source URL": SEC_CMBS_URL,
    },
    evidence: BETHESDA_RENOVATION_EVENTS,
    yearBuiltEvidence: BETHESDA_YEAR_BUILT_EVIDENCE,
  };
}

export default {
  HE_HPC_HISTORICAL_FACTS_V1,
  resolveYearBuiltFromHpcFields,
  resolveLastRenovatedFromHpcFields,
  buildBethesdaRenovationNullFillPatch,
  extractYearFromDateLike,
};
