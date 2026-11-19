/**
 * Brand Explorer — HPC brand footprint summary V1 (read-only).
 *
 * Designated authority for verified open/pipeline hotel + keys rollups.
 * Hotel Census remains transitional until brand-level HPC rollup is certified.
 * Never writes Airtable. Never invents counts. Company-reported figures stay separate.
 *
 * Contract: BE_HPC_FOOTPRINT_SUMMARY_V1
 */

import {
  BE_FOOTPRINT_METRIC_CONTRACT,
  BE_HPC_TABLE,
  BE_HOTEL_CENSUS_TABLE,
  BE_IDENTITY_CONTRACT,
  aggregateOpenPipelineCounts,
  buildCoverageLimitations,
  companyReportedMustNotReplaceHpc,
  filterHpcRowsForBrandFootprint,
  normalizeExactKey,
} from "./be-airtable-integration-sot-v1.js";

export const BE_HPC_FOOTPRINT_SUMMARY_V1 = "BE_HPC_FOOTPRINT_SUMMARY_V1";

export const BE_HPC_FOOTPRINT_CERT_STATES = Object.freeze({
  CERTIFIED: "HPC_BRAND_ROLLUP_CERTIFIED",
  TRANSITIONAL_CENSUS: "TRANSITIONAL_HOTEL_CENSUS",
  INSUFFICIENT_MATCH: "INSUFFICIENT_HPC_MATCH",
  LIVE_UNAVAILABLE: "HPC_LIVE_LOOKUP_UNAVAILABLE",
  BLOCKED_COMPANY_OVERRIDE: "BLOCKED_COMPANY_REPORTED_OVERRIDE",
});

export const BE_HPC_FIELDS = Object.freeze({
  currentBrand: "Current Brand",
  brandFamily: "Brand Family",
  affiliationStatus: "Affiliation Status",
  radarStatus: "Radar Status",
  roomsKeys: "Rooms / Keys",
  country: "Country",
  propertyName: "Property Name",
});

/**
 * Representative Phase 2 pilots — diverse affiliation / operating models.
 * Brand Basics record ids are canonical; never invent new identities.
 */
export const BE_PHASE2_PILOT_BRANDS = Object.freeze([
  Object.freeze({
    slug: "autograph-collection",
    recordId: "recEJCTDj1zrsjPM6",
    name: "Autograph Collection",
    parentCompany: "Marriott International",
    affiliationModel: "soft_brand",
    operatingModel: "franchise_soft_brand",
    hpcMatchers: Object.freeze(["Autograph Collection", "Autograph"]),
  }),
  Object.freeze({
    slug: "ascend",
    recordId: "reclkgOzvAcBheUSo",
    name: "Ascend Hotel Collection",
    parentCompany: "Choice Hotels",
    affiliationModel: "soft_brand_collection",
    operatingModel: "franchise_membership",
    hpcMatchers: Object.freeze(["Ascend Hotel Collection", "Ascend"]),
  }),
  Object.freeze({
    slug: "curio-collection",
    recordId: "receQkxgjlezsc1xg",
    name: "Curio Collection by Hilton",
    parentCompany: "Hilton",
    affiliationModel: "soft_brand",
    operatingModel: "franchise_soft_brand",
    hpcMatchers: Object.freeze(["Curio Collection", "Curio Collection by Hilton", "Curio"]),
  }),
  Object.freeze({
    slug: "tribute-portfolio",
    recordId: "recCvV0PuZOi8c3hC",
    name: "Tribute Portfolio",
    parentCompany: "Marriott International",
    affiliationModel: "soft_brand",
    operatingModel: "franchise_soft_brand",
    hpcMatchers: Object.freeze(["Tribute Portfolio"]),
  }),
  Object.freeze({
    slug: "mama-shelter",
    recordId: "recXCZCK05XXYX7Q8",
    name: "Mama Shelter",
    parentCompany: "Accor",
    affiliationModel: "lifestyle_brand",
    operatingModel: "managed_operated",
    hpcMatchers: Object.freeze(["Mama Shelter"]),
  }),
  Object.freeze({
    slug: "preferred-hotels-and-resorts",
    recordId: "recwl5JOYxlChuCAr",
    name: "Preferred Hotels & Resorts",
    parentCompany: "Preferred Hotels & Resorts",
    affiliationModel: "independent_affiliation",
    operatingModel: "membership_affiliation",
    hpcMatchers: Object.freeze(["Preferred Hotels & Resorts", "Preferred Hotels and Resorts"]),
  }),
]);

function airtableToken() {
  return (
    process.env.AIRTABLE_API_KEY ||
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    ""
  ).trim();
}

function platformBaseId() {
  return (process.env.AIRTABLE_BASE_ID_ALT || "").trim();
}

export function hpcFootprintLiveConfigured() {
  return Boolean(airtableToken() && platformBaseId());
}

function escapeFormula(s) {
  return String(s || "").replace(/'/g, "\\'");
}

function mapHpcRecord(rec) {
  const f = rec?.fields || {};
  return {
    id: rec.id,
    recordId: rec.id,
    currentBrand: f[BE_HPC_FIELDS.currentBrand] || "",
    brandFamily: f[BE_HPC_FIELDS.brandFamily] || "",
    affiliationStatus: f[BE_HPC_FIELDS.affiliationStatus] || "",
    radarStatus: f[BE_HPC_FIELDS.radarStatus] || "",
    roomsKeys: f[BE_HPC_FIELDS.roomsKeys] ?? null,
    country: f[BE_HPC_FIELDS.country] || "",
    propertyName: f[BE_HPC_FIELDS.propertyName] || "",
  };
}

/**
 * Pure: build footprint summary from already-fetched HPC rows + identity.
 * Does not call Airtable.
 */
export function buildHpcFootprintSummaryFromRows({
  brandBasicsRecordId,
  brandName,
  brandMatchers = [],
  hpcRows = [],
  companyReported = null,
  preferCompanyReported = false,
  usingTransitional = false,
  liveFetchOk = true,
  aggregatedAt = null,
} = {}) {
  const warnings = [];
  const matchers = [...new Set((brandMatchers || []).map((m) => String(m || "").trim()).filter(Boolean))];

  if (!brandBasicsRecordId || !/^rec[a-zA-Z0-9]{10,}$/.test(String(brandBasicsRecordId))) {
    return {
      contract: BE_HPC_FOOTPRINT_SUMMARY_V1,
      available: false,
      certified: false,
      certState: BE_HPC_FOOTPRINT_CERT_STATES.INSUFFICIENT_MATCH,
      fallbackRecommended: true,
      designatedAuthority: BE_FOOTPRINT_METRIC_CONTRACT.designatedAuthority,
      warnings: ["IDENTITY_REQUIRED: Brand Basics record id is the canonical identity"],
      metrics: null,
      companyReported: companyReported || null,
      writes: 0,
    };
  }

  const overrideGuard = companyReportedMustNotReplaceHpc({
    hpcMetrics: { totalOpenHotels: 1 },
    companyReported,
    preferCompanyReported,
  });
  if (!overrideGuard.ok) {
    return {
      contract: BE_HPC_FOOTPRINT_SUMMARY_V1,
      available: false,
      certified: false,
      certState: BE_HPC_FOOTPRINT_CERT_STATES.BLOCKED_COMPANY_OVERRIDE,
      fallbackRecommended: true,
      designatedAuthority: BE_FOOTPRINT_METRIC_CONTRACT.designatedAuthority,
      warnings: [overrideGuard.error],
      metrics: null,
      companyReported: companyReported || null,
      identity: {
        brandBasicsRecordId,
        brandName: brandName || null,
        primaryKey: BE_IDENTITY_CONTRACT.primaryKey,
      },
      writes: 0,
    };
  }

  if (!liveFetchOk) {
    return {
      contract: BE_HPC_FOOTPRINT_SUMMARY_V1,
      available: false,
      certified: false,
      certState: BE_HPC_FOOTPRINT_CERT_STATES.LIVE_UNAVAILABLE,
      fallbackRecommended: true,
      designatedAuthority: BE_FOOTPRINT_METRIC_CONTRACT.designatedAuthority,
      transitionalAuthority: BE_FOOTPRINT_METRIC_CONTRACT.transitionalAuthority,
      warnings: ["HPC_LIVE_LOOKUP_UNAVAILABLE: do not invent footprint counts"],
      metrics: null,
      companyReported: companyReported || null,
      identity: {
        brandBasicsRecordId,
        brandName: brandName || null,
        primaryKey: BE_IDENTITY_CONTRACT.primaryKey,
      },
      writes: 0,
    };
  }

  const { matched, excluded } = filterHpcRowsForBrandFootprint(hpcRows, {
    brandMatchers: matchers,
    statuses: BE_FOOTPRINT_METRIC_CONTRACT.qualifyingHpcStatuses,
  });
  const metrics = aggregateOpenPipelineCounts(matched);
  const recordsMatched = matched.length;
  const certified =
    !usingTransitional &&
    recordsMatched > 0 &&
    (metrics.totalOpenHotels > 0 || metrics.totalPipelineHotels > 0);

  let certState = BE_HPC_FOOTPRINT_CERT_STATES.INSUFFICIENT_MATCH;
  if (usingTransitional) certState = BE_HPC_FOOTPRINT_CERT_STATES.TRANSITIONAL_CENSUS;
  else if (certified) certState = BE_HPC_FOOTPRINT_CERT_STATES.CERTIFIED;

  if (excluded.nonQualifyingStatus) {
    warnings.push(`Excluded ${excluded.nonQualifyingStatus} non-Open/Pipeline HPC row(s) from totals`);
  }
  if (excluded.noBrandMatch) {
    warnings.push(`Excluded ${excluded.noBrandMatch} HPC row(s) that did not match brand Current Brand matchers`);
  }
  if (!recordsMatched) {
    warnings.push("No qualifying HPC Open/Pipeline rows matched — do not display fabricated counts");
  }

  const coverageLimitations = buildCoverageLimitations({
    usingTransitional,
    unmatchedAliases: [],
    excludedStatuses: excluded.nonQualifyingStatus
      ? [...BE_FOOTPRINT_METRIC_CONTRACT.statusBuckets.other]
      : [],
    recordsMatched,
  });

  const ts = aggregatedAt || new Date().toISOString();

  return {
    contract: BE_HPC_FOOTPRINT_SUMMARY_V1,
    available: recordsMatched > 0 || usingTransitional,
    certified,
    certState,
    fallbackRecommended: !certified,
    designatedAuthority: BE_FOOTPRINT_METRIC_CONTRACT.designatedAuthority,
    transitionalAuthority: BE_FOOTPRINT_METRIC_CONTRACT.transitionalAuthority,
    identity: {
      brandBasicsRecordId,
      brandName: brandName || null,
      primaryKey: BE_IDENTITY_CONTRACT.primaryKey,
    },
    source: {
      table: usingTransitional ? BE_HOTEL_CENSUS_TABLE : BE_HPC_TABLE,
      base: "AIRTABLE_BASE_ID_ALT",
      aggregatedAt: ts,
      readOnly: true,
    },
    match: {
      canonicalBrandName: brandName || null,
      affiliationMatchers: matchers,
      matcherNormalization: "exact_trimmed_case_insensitive",
    },
    metrics,
    census: {
      recordsMatched,
      excluded,
    },
    companyReported: companyReported
      ? {
          ...companyReported,
          attribution: "company_reported_separate",
          note: "Must not replace HPC / certified inventory totals",
        }
      : null,
    coverageLimitations,
    warnings,
    adpGdi: {
      adpBrandLevel: "RESERVED_READ_ONLY",
      gdiBrandLevel: "RESERVED_READ_ONLY",
      speculativeMetrics: false,
    },
    writes: 0,
  };
}

/**
 * Read-only live HPC fetch for a brand's Current Brand matchers.
 * Caps pages to keep pilot validation bounded. Never writes.
 */
export async function fetchHpcRowsForBrandMatchers(brandMatchers = [], { maxPages = 8, pageSize = 100 } = {}) {
  if (!hpcFootprintLiveConfigured()) {
    return { ok: false, error: "HPC_CONFIG_MISSING", rows: [] };
  }
  const matchers = [...new Set((brandMatchers || []).map((m) => String(m || "").trim()).filter(Boolean))];
  if (!matchers.length) {
    return { ok: false, error: "NO_MATCHERS", rows: [] };
  }

  const orParts = matchers.map((m) => `{${BE_HPC_FIELDS.currentBrand}}='${escapeFormula(m)}'`);
  const filterByFormula = `OR(${orParts.join(",")})`;
  const token = airtableToken();
  const baseId = platformBaseId();
  const rows = [];
  let offset = null;
  let pages = 0;

  try {
    do {
      const q = new URLSearchParams({
        pageSize: String(pageSize),
        filterByFormula,
      });
      if (offset) q.set("offset", offset);
      const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BE_HPC_TABLE)}?${q}`;
      const res = await fetch(url, {
        headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) {
        return {
          ok: false,
          error: `Airtable ${res.status}: ${json?.error?.message || "request_failed"}`,
          rows: [],
        };
      }
      for (const rec of json.records || []) rows.push(mapHpcRecord(rec));
      offset = json.offset || null;
      pages += 1;
    } while (offset && pages < maxPages);

    return {
      ok: true,
      rows,
      truncated: Boolean(offset),
      pages,
      filterByFormula,
    };
  } catch (err) {
    return { ok: false, error: err?.message || String(err), rows: [] };
  }
}

/**
 * Build footprint for one pilot / brand. Live fetch only when allowLive=true and configured.
 */
export async function buildBrandHpcFootprintSummary({
  brandBasicsRecordId,
  brandName,
  brandMatchers = [],
  companyReported = null,
  preferCompanyReported = false,
  allowLive = false,
  prefetchedRows = null,
} = {}) {
  if (Array.isArray(prefetchedRows)) {
    return buildHpcFootprintSummaryFromRows({
      brandBasicsRecordId,
      brandName,
      brandMatchers,
      hpcRows: prefetchedRows,
      companyReported,
      preferCompanyReported,
      liveFetchOk: true,
    });
  }

  if (!allowLive) {
    return buildHpcFootprintSummaryFromRows({
      brandBasicsRecordId,
      brandName,
      brandMatchers,
      hpcRows: [],
      companyReported,
      preferCompanyReported,
      liveFetchOk: false,
    });
  }

  const fetchResult = await fetchHpcRowsForBrandMatchers(brandMatchers);
  if (!fetchResult.ok) {
    const offline = buildHpcFootprintSummaryFromRows({
      brandBasicsRecordId,
      brandName,
      brandMatchers,
      hpcRows: [],
      companyReported,
      preferCompanyReported,
      liveFetchOk: false,
    });
    offline.warnings = [...(offline.warnings || []), fetchResult.error].filter(Boolean);
    return offline;
  }

  const summary = buildHpcFootprintSummaryFromRows({
    brandBasicsRecordId,
    brandName,
    brandMatchers,
    hpcRows: fetchResult.rows,
    companyReported,
    preferCompanyReported,
    liveFetchOk: true,
  });
  if (fetchResult.truncated) {
    summary.warnings = [...(summary.warnings || []), "HPC_FETCH_TRUNCATED: raise maxPages for full certification"];
    summary.fallbackRecommended = true;
    if (summary.certState === BE_HPC_FOOTPRINT_CERT_STATES.CERTIFIED) {
      summary.certified = false;
      summary.certState = BE_HPC_FOOTPRINT_CERT_STATES.INSUFFICIENT_MATCH;
    }
  }
  summary.live = { pages: fetchResult.pages, truncated: fetchResult.truncated };
  return summary;
}

export function getPhase2PilotBySlug(slug) {
  const key = normalizeExactKey(slug);
  return BE_PHASE2_PILOT_BRANDS.find((b) => normalizeExactKey(b.slug) === key) || null;
}

export function summarizePhase2Pilots() {
  return {
    contract: BE_HPC_FOOTPRINT_SUMMARY_V1,
    pilotCount: BE_PHASE2_PILOT_BRANDS.length,
    pilots: BE_PHASE2_PILOT_BRANDS.map((p) => ({
      slug: p.slug,
      recordId: p.recordId,
      name: p.name,
      affiliationModel: p.affiliationModel,
      operatingModel: p.operatingModel,
    })),
    designatedAuthority: BE_FOOTPRINT_METRIC_CONTRACT.designatedAuthority,
    writesAuthorized: false,
    paidResearchAuthorized: false,
    autoPublishAuthorized: false,
  };
}
