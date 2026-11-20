/**
 * Brand Explorer — HPC brand footprint summary V1 (read-only).
 *
 * Designated authority for verified open/pipeline hotel + keys rollups.
 * Hotel Census remains transitional until brand-level HPC rollup is certified.
 * Never writes Airtable. Never invents counts. Company-reported figures stay separate.
 *
 * Contract: BE_HPC_FOOTPRINT_SUMMARY_V1
 *
 * Certification rule (founder): synthetic fixtures must NEVER return
 * HPC_BRAND_ROLLUP_CERTIFIED. Matching rows alone are insufficient.
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
  mapCountryToFootprintRegion,
  normalizeExactKey,
} from "./be-airtable-integration-sot-v1.js";

export const BE_HPC_FOOTPRINT_SUMMARY_V1 = "BE_HPC_FOOTPRINT_SUMMARY_V1";

/** Where the row payload came from. */
export const BE_HPC_DATA_ORIGINS = Object.freeze({
  SYNTHETIC_FIXTURE: "synthetic_fixture",
  LIVE_AIRTABLE: "live_airtable",
  TRANSITIONAL_CENSUS: "transitional_census",
  UNAVAILABLE: "unavailable",
});

/**
 * Certification / inventory states.
 * CERTIFIED is reserved for independently verified live rollups with full gates.
 */
export const BE_HPC_FOOTPRINT_CERT_STATES = Object.freeze({
  SYNTHETIC_FIXTURE_VALIDATION: "SYNTHETIC_FIXTURE_VALIDATION",
  LIVE_UNVERIFIED_INVENTORY: "LIVE_UNVERIFIED_INVENTORY",
  LIVE_PARTIAL_COVERAGE: "LIVE_PARTIAL_COVERAGE",
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
  propertyType: "Property Type",
  continent: "Continent",
  market: "Market",
});

/** Customer-facing source note — HPC observed ≠ worldwide official brand scale. */
export const SOURCE_NOTE_HPC_OBSERVED =
  "HPC-observed Dealality inventory (Radar Status Open/Pipeline). Coverage may be partial — not the same as worldwide published brand scale.";
export const SOURCE_NOTE_OFFICIAL_REPORTED =
  "Official / company-reported brand footprint (Brand Setup). Shown separately from HPC-observed inventory.";
export const SOURCE_NOTE_HPC_UNAVAILABLE =
  "HPC inventory unavailable — footprint metrics are not shown as zero.";
export const SOURCE_NOTE_HPC_INCOMPLETE_KEYS =
  "Some qualifying HPC properties are missing Rooms / Keys — room totals are incomplete, not a verified zero.";

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
    propertyType: f[BE_HPC_FIELDS.propertyType] || "",
    continent: f[BE_HPC_FIELDS.continent] || "",
    market: f[BE_HPC_FIELDS.market] || "",
  };
}

function parseRoomsKeys(row) {
  const raw = row?.roomsKeys ?? row?.["Rooms / Keys"] ?? row?.rooms;
  if (raw == null || raw === "") return { value: null, missing: true };
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) return { value: null, missing: true };
  if (n === 0) return { value: 0, missing: false };
  return { value: n, missing: false };
}

function bumpBreakdown(
  map,
  label,
  {
    openHotels = 0,
    openKeys = 0,
    pipelineHotels = 0,
    pipelineKeys = 0,
    openKeysKnown = false,
    openKeysMissing = false,
    pipelineKeysKnown = false,
    pipelineKeysMissing = false,
  } = {},
) {
  const key = String(label || "Unknown").trim() || "Unknown";
  if (!map[key]) {
    map[key] = {
      label: key,
      hotels: 0,
      keys: 0,
      pipelineHotels: 0,
      pipelineKeys: 0,
      openWithKeys: 0,
      openMissingKeys: 0,
      pipelineWithKeys: 0,
      pipelineMissingKeys: 0,
    };
  }
  map[key].hotels += openHotels;
  map[key].keys += openKeys;
  map[key].pipelineHotels += pipelineHotels;
  map[key].pipelineKeys += pipelineKeys;
  if (openKeysKnown) map[key].openWithKeys += 1;
  if (openKeysMissing) map[key].openMissingKeys += 1;
  if (pipelineKeysKnown) map[key].pipelineWithKeys += 1;
  if (pipelineKeysMissing) map[key].pipelineMissingKeys += 1;
}

function breakdownMapToRows(map) {
  return Object.values(map)
    .filter((r) => r.hotels + r.pipelineHotels > 0)
    .map((r) => ({
      ...r,
      keysIncomplete: (r.openMissingKeys || 0) + (r.pipelineMissingKeys || 0) > 0,
      // Display keys are known-room sums only — never treat missing as zero inventory.
      displayOpenKeys: r.openMissingKeys > 0 && r.openWithKeys === 0 ? null : r.keys,
      displayPipelineKeys:
        r.pipelineMissingKeys > 0 && r.pipelineWithKeys === 0 ? null : r.pipelineKeys,
    }))
    .sort((a, b) => {
      const aUnk = /^(unknown|unclassified)$/i.test(a.label);
      const bUnk = /^(unknown|unclassified)$/i.test(b.label);
      if (aUnk && !bUnk) return 1;
      if (bUnk && !aUnk) return -1;
      return b.hotels + b.pipelineHotels - (a.hotels + a.pipelineHotels) || b.keys - a.keys;
    });
}

/**
 * Resolve Current Brand matchers for a Brand Basics identity.
 * Prefer registered pilot matchers; otherwise use the Brand Name.
 */
export function resolveHpcMatchersForBrand({ recordId, brandName, extraMatchers = [] } = {}) {
  const pilot = BE_PHASE2_PILOT_BRANDS.find((p) => p.recordId === recordId);
  const fromPilot = pilot ? [...pilot.hpcMatchers] : [];
  const fromName = brandName ? [String(brandName).trim()] : [];
  const extras = (extraMatchers || []).map((m) => String(m || "").trim()).filter(Boolean);
  return [...new Set([...fromPilot, ...fromName, ...extras].filter(Boolean))];
}

/**
 * Extract official/company-reported totals from Brand Setup footprint payload.
 * Never treat these as HPC inventory.
 */
export function extractOfficialReportedFootprint(footprint = {}) {
  const openHotels = Number(footprint.totalExistingHotels);
  const openRooms = Number(footprint.totalExistingRooms);
  const pipelineHotels = Number(
    footprint.totalPipelineHotels ??
      (Number(footprint.totalNewBuildHotels) || 0) + (Number(footprint.totalConversionHotels) || 0),
  );
  const pipelineRooms = Number(
    footprint.totalPipelineRooms ??
      (Number(footprint.totalNewBuildRooms) || 0) + (Number(footprint.totalConversionRooms) || 0),
  );
  const hasAny =
    (Number.isFinite(openHotels) && openHotels > 0) ||
    (Number.isFinite(openRooms) && openRooms > 0) ||
    (Number.isFinite(pipelineHotels) && pipelineHotels > 0) ||
    (Number.isFinite(pipelineRooms) && pipelineRooms > 0);
  if (!hasAny) return null;
  return {
    totalOpenHotels: Number.isFinite(openHotels) ? openHotels : null,
    totalOpenRooms: Number.isFinite(openRooms) ? openRooms : null,
    totalPipelineHotels: Number.isFinite(pipelineHotels) ? pipelineHotels : null,
    totalPipelineRooms: Number.isFinite(pipelineRooms) ? pipelineRooms : null,
    regionalDistribution: footprint.regionalDistribution || null,
    figuresAsOf: footprint.formValues?.figuresAsOf || footprint.verification?.figuresAsOf || null,
    attribution: "company_reported_brand_setup",
  };
}

/**
 * Build country / region / property-type breakdowns + rooms coverage from matched rows.
 */
export function buildHpcPortfolioBreakdowns(matchedRows = []) {
  const byCountry = {};
  const byPropertyType = {};
  const regionFromRows = {};
  let openWithKeys = 0;
  let openMissingKeys = 0;
  let pipelineWithKeys = 0;
  let pipelineMissingKeys = 0;

  for (const row of matchedRows || []) {
    const status = normalizeExactKey(row.radarStatus || row["Radar Status"] || row.status);
    const rooms = parseRoomsKeys(row);
    const keysAdd = rooms.missing ? 0 : rooms.value || 0;
    const isOpen = status === "open";
    const isPipe = status === "pipeline";
    if (!isOpen && !isPipe) continue;

    if (isOpen) {
      if (rooms.missing) openMissingKeys += 1;
      else openWithKeys += 1;
    } else if (isPipe) {
      if (rooms.missing) pipelineMissingKeys += 1;
      else pipelineWithKeys += 1;
    }

    const delta = {
      openHotels: isOpen ? 1 : 0,
      openKeys: isOpen ? keysAdd : 0,
      pipelineHotels: isPipe ? 1 : 0,
      pipelineKeys: isPipe ? keysAdd : 0,
      openKeysKnown: isOpen && !rooms.missing,
      openKeysMissing: isOpen && rooms.missing,
      pipelineKeysKnown: isPipe && !rooms.missing,
      pipelineKeysMissing: isPipe && rooms.missing,
    };
    bumpBreakdown(byCountry, row.country || row.Country || "Unknown", delta);
    bumpBreakdown(byPropertyType, row.propertyType || row["Property Type"] || "Unknown", delta);

    const code = mapCountryToFootprintRegion(
      row.country || row.Country || row.market || row.Market || row.continent || "",
    );
    // Never silently drop qualifying properties from region view.
    const regionLabel = code ? code.toUpperCase() : "Unclassified";
    bumpBreakdown(regionFromRows, regionLabel, delta);
  }

  return {
    country: breakdownMapToRows(byCountry),
    propertyType: breakdownMapToRows(byPropertyType),
    region: breakdownMapToRows(regionFromRows),
    roomsCoverage: {
      openWithKeys,
      openMissingKeys,
      pipelineWithKeys,
      pipelineMissingKeys,
      openKeysIncomplete: openMissingKeys > 0,
      pipelineKeysIncomplete: pipelineMissingKeys > 0,
      allOpenKeysMissing: openWithKeys === 0 && openMissingKeys > 0,
      allPipelineKeysMissing: pipelineWithKeys === 0 && pipelineMissingKeys > 0,
    },
  };
}

/**
 * Display metrics that never coerce incomplete rooms into a verified zero.
 */
export function toHpcDisplayMetrics(summary) {
  if (!summary || !summary.metrics || summary.available !== true) {
    return {
      status: summary?.certState === BE_HPC_FOOTPRINT_CERT_STATES.LIVE_UNAVAILABLE ? "airtable_unavailable" : "not_available",
      totalOpenHotels: null,
      totalOpenKeys: null,
      totalPipelineHotels: null,
      totalPipelineKeys: null,
    };
  }
  const matched = summary.census?.recordsMatched || 0;
  if (matched <= 0) {
    return {
      status: "insufficient_match",
      totalOpenHotels: null,
      totalOpenKeys: null,
      totalPipelineHotels: null,
      totalPipelineKeys: null,
    };
  }
  const m = summary.metrics;
  const rc = summary.roomsCoverage || summary.breakdowns?.roomsCoverage || {};
  return {
    status: "hpc_observed",
    totalOpenHotels: m.totalOpenHotels,
    totalOpenKeys: rc.allOpenKeysMissing ? null : m.totalOpenKeys,
    totalPipelineHotels: m.totalPipelineHotels,
    totalPipelineKeys: rc.allPipelineKeysMissing ? null : m.totalPipelineKeys,
    openKeysIncomplete: Boolean(rc.openKeysIncomplete),
    pipelineKeysIncomplete: Boolean(rc.pipelineKeysIncomplete),
    openWithKeys: rc.openWithKeys || 0,
    openMissingKeys: rc.openMissingKeys || 0,
    pipelineWithKeys: rc.pipelineWithKeys || 0,
    pipelineMissingKeys: rc.pipelineMissingKeys || 0,
  };
}

/**
 * Customer-facing coverage copy — no certState / Radar Status / internal jargon.
 */
export function buildCustomerFacingHpcCoverageCopy(summary) {
  const m = summary?.metrics || {};
  const rc = summary?.roomsCoverage || {};
  const open = Number(m.totalOpenHotels) || 0;
  const pipe = Number(m.totalPipelineHotels) || 0;
  const known = Number(rc.openWithKeys) || 0;
  const missing = Number(rc.openMissingKeys) || 0;
  const propertyWord = open === 1 ? "property" : "properties";
  const parts = [];
  if (open > 0) {
    parts.push(
      `Dealality has identified ${open} affiliated ${propertyWord} in its hotel database.`,
    );
  } else if (pipe > 0) {
    parts.push(
      `Dealality has identified ${pipe} pipeline ${pipe === 1 ? "property" : "properties"} in its hotel database.`,
    );
  } else {
    parts.push("Dealality has not yet identified affiliated properties for this brand in its hotel database.");
  }
  if (open > 0 && missing > 0 && known > 0) {
    parts.push(
      `Room counts are available for ${known} of these ${propertyWord}.`,
    );
  } else if (open > 0 && missing > 0 && known === 0) {
    parts.push("Room counts are not yet available for these properties.");
  } else if (open > 0 && known > 0 && missing === 0) {
    parts.push("Room counts are available for all identified properties.");
  }
  parts.push(
    "This represents observed coverage rather than the brand's complete worldwide portfolio.",
  );
  return parts.join(" ");
}

/**
 * Evaluate whether a rollup may be independently certified.
 * Matching HPC rows alone is never sufficient.
 */
export function evaluateHpcCertificationGates({
  dataOrigin,
  brandBasicsRecordId,
  brandMatchers = [],
  recordsMatched = 0,
  paginationComplete = false,
  statusFilteringApplied = false,
  deduplicationApplied = false,
  coverageDocumented = false,
  usingTransitional = false,
  independentCertification = false,
} = {}) {
  const identityMappingProven = Boolean(
    brandBasicsRecordId &&
      /^rec[a-zA-Z0-9]{10,}$/.test(String(brandBasicsRecordId)) &&
      Array.isArray(brandMatchers) &&
      brandMatchers.filter((m) => String(m || "").trim()).length > 0,
  );
  const liveSource = dataOrigin === BE_HPC_DATA_ORIGINS.LIVE_AIRTABLE;
  const notSynthetic = dataOrigin !== BE_HPC_DATA_ORIGINS.SYNTHETIC_FIXTURE;
  const gates = {
    identityMappingProven,
    paginationComplete: Boolean(paginationComplete),
    statusFilteringApplied: Boolean(statusFilteringApplied),
    deduplicationApplied: Boolean(deduplicationApplied),
    coverageDocumented: Boolean(coverageDocumented),
    liveSource,
    notSynthetic,
    hasQualifyingRows: recordsMatched > 0,
    notTransitional: !usingTransitional,
    independentCertification: Boolean(independentCertification),
  };
  const allRequired =
    gates.identityMappingProven &&
    gates.paginationComplete &&
    gates.statusFilteringApplied &&
    gates.deduplicationApplied &&
    gates.coverageDocumented &&
    gates.liveSource &&
    gates.notSynthetic &&
    gates.hasQualifyingRows &&
    gates.notTransitional &&
    gates.independentCertification;
  return { gates, certified: allRequired };
}

function resolveCertState({
  dataOrigin,
  usingTransitional,
  paginationComplete,
  recordsMatched,
  certified,
  liveFetchOk,
}) {
  if (!liveFetchOk && dataOrigin === BE_HPC_DATA_ORIGINS.UNAVAILABLE) {
    return BE_HPC_FOOTPRINT_CERT_STATES.LIVE_UNAVAILABLE;
  }
  if (dataOrigin === BE_HPC_DATA_ORIGINS.SYNTHETIC_FIXTURE) {
    return BE_HPC_FOOTPRINT_CERT_STATES.SYNTHETIC_FIXTURE_VALIDATION;
  }
  if (usingTransitional || dataOrigin === BE_HPC_DATA_ORIGINS.TRANSITIONAL_CENSUS) {
    return BE_HPC_FOOTPRINT_CERT_STATES.TRANSITIONAL_CENSUS;
  }
  if (certified) {
    return BE_HPC_FOOTPRINT_CERT_STATES.CERTIFIED;
  }
  if (dataOrigin === BE_HPC_DATA_ORIGINS.LIVE_AIRTABLE && !paginationComplete) {
    return BE_HPC_FOOTPRINT_CERT_STATES.LIVE_PARTIAL_COVERAGE;
  }
  if (dataOrigin === BE_HPC_DATA_ORIGINS.LIVE_AIRTABLE && recordsMatched > 0) {
    return BE_HPC_FOOTPRINT_CERT_STATES.LIVE_UNVERIFIED_INVENTORY;
  }
  if (dataOrigin === BE_HPC_DATA_ORIGINS.LIVE_AIRTABLE) {
    return BE_HPC_FOOTPRINT_CERT_STATES.INSUFFICIENT_MATCH;
  }
  return BE_HPC_FOOTPRINT_CERT_STATES.INSUFFICIENT_MATCH;
}

/**
 * Pure: build footprint summary from already-fetched HPC rows + identity.
 * Does not call Airtable.
 *
 * @param {object} opts
 * @param {'synthetic_fixture'|'live_airtable'|'transitional_census'|'unavailable'} [opts.dataOrigin]
 * @param {boolean} [opts.paginationComplete] — must be true for certification (full pages drained)
 * @param {boolean} [opts.independentCertification] — explicit independent cert decision; default false
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
  dataOrigin = null,
  paginationComplete = false,
  independentCertification = false,
  coverageNotes = [],
} = {}) {
  const warnings = [];
  const matchers = [...new Set((brandMatchers || []).map((m) => String(m || "").trim()).filter(Boolean))];

  let origin = dataOrigin;
  if (!origin) {
    if (!liveFetchOk) origin = BE_HPC_DATA_ORIGINS.UNAVAILABLE;
    else if (usingTransitional) origin = BE_HPC_DATA_ORIGINS.TRANSITIONAL_CENSUS;
    else origin = BE_HPC_DATA_ORIGINS.LIVE_AIRTABLE;
  }

  if (!brandBasicsRecordId || !/^rec[a-zA-Z0-9]{10,}$/.test(String(brandBasicsRecordId))) {
    return {
      contract: BE_HPC_FOOTPRINT_SUMMARY_V1,
      available: false,
      certified: false,
      dataOrigin: origin,
      certState: BE_HPC_FOOTPRINT_CERT_STATES.INSUFFICIENT_MATCH,
      fallbackRecommended: true,
      designatedAuthority: BE_FOOTPRINT_METRIC_CONTRACT.designatedAuthority,
      certificationGates: evaluateHpcCertificationGates({
        dataOrigin: origin,
        brandBasicsRecordId,
        brandMatchers: matchers,
        recordsMatched: 0,
        paginationComplete,
        independentCertification,
      }).gates,
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
      dataOrigin: origin,
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

  if (!liveFetchOk || origin === BE_HPC_DATA_ORIGINS.UNAVAILABLE) {
    return {
      contract: BE_HPC_FOOTPRINT_SUMMARY_V1,
      available: false,
      certified: false,
      dataOrigin: BE_HPC_DATA_ORIGINS.UNAVAILABLE,
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
  const aggregated = aggregateOpenPipelineCounts(matched);
  const metrics = {
    totalOpenHotels: aggregated.totalOpenHotels,
    totalOpenKeys: aggregated.totalOpenKeys,
    totalPipelineHotels: aggregated.totalPipelineHotels,
    totalPipelineKeys: aggregated.totalPipelineKeys,
  };
  const portfolio = buildHpcPortfolioBreakdowns(matched);
  const recordsMatched = matched.length;

  if (excluded.nonQualifyingStatus) {
    warnings.push(`Excluded ${excluded.nonQualifyingStatus} non-Open/Pipeline HPC row(s) from totals`);
  }
  if (excluded.noBrandMatch) {
    warnings.push(`Excluded ${excluded.noBrandMatch} HPC row(s) that did not match brand Current Brand matchers`);
  }
  if (!recordsMatched) {
    warnings.push("No qualifying HPC Open/Pipeline rows matched — do not display fabricated counts");
  }
  if (origin === BE_HPC_DATA_ORIGINS.SYNTHETIC_FIXTURE) {
    warnings.push("SYNTHETIC_FIXTURE: counts are for pipeline/unit validation only — not certifiable inventory");
  }
  if (portfolio.roomsCoverage.openKeysIncomplete || portfolio.roomsCoverage.pipelineKeysIncomplete) {
    warnings.push(SOURCE_NOTE_HPC_INCOMPLETE_KEYS);
  }

  const coverageLimitations = buildCoverageLimitations({
    usingTransitional,
    unmatchedAliases: [],
    excludedStatuses: excluded.nonQualifyingStatus
      ? [...BE_FOOTPRINT_METRIC_CONTRACT.statusBuckets.other]
      : [],
    recordsMatched,
  });
  if (Array.isArray(coverageNotes) && coverageNotes.length) {
    coverageLimitations.push(...coverageNotes);
  }
  if (origin === BE_HPC_DATA_ORIGINS.SYNTHETIC_FIXTURE) {
    coverageLimitations.push("Synthetic fixture origin — not Dealality inventory");
  }
  if (!paginationComplete && origin === BE_HPC_DATA_ORIGINS.LIVE_AIRTABLE) {
    coverageLimitations.push("Pagination incomplete — rollup may under-count");
  }
  coverageLimitations.push(
    "HPC-observed totals reflect qualifying Airtable HPC rows only — not automatic worldwide brand disclosure.",
  );
  if (portfolio.roomsCoverage.openMissingKeys || portfolio.roomsCoverage.pipelineMissingKeys) {
    coverageLimitations.push(
      `${portfolio.roomsCoverage.openMissingKeys + portfolio.roomsCoverage.pipelineMissingKeys} qualifying HPC row(s) missing Rooms / Keys`,
    );
  }

  const coverageDocumented = coverageLimitations.length > 0 || recordsMatched > 0;
  const { gates, certified } = evaluateHpcCertificationGates({
    dataOrigin: origin,
    brandBasicsRecordId,
    brandMatchers: matchers,
    recordsMatched,
    paginationComplete,
    statusFilteringApplied: true,
    deduplicationApplied: true,
    coverageDocumented,
    usingTransitional,
    independentCertification,
  });

  // Hard rule: synthetic never certified regardless of independentCertification flag
  const certifiedFinal =
    certified && origin !== BE_HPC_DATA_ORIGINS.SYNTHETIC_FIXTURE && independentCertification;

  const certState = resolveCertState({
    dataOrigin: origin,
    usingTransitional,
    paginationComplete,
    recordsMatched,
    certified: certifiedFinal,
    liveFetchOk: true,
  });

  const ts = aggregatedAt || new Date().toISOString();

  const summary = {
    contract: BE_HPC_FOOTPRINT_SUMMARY_V1,
    available: recordsMatched > 0 || usingTransitional || origin === BE_HPC_DATA_ORIGINS.SYNTHETIC_FIXTURE,
    certified: certifiedFinal,
    dataOrigin: origin,
    certState,
    fallbackRecommended: !certifiedFinal,
    designatedAuthority: BE_FOOTPRINT_METRIC_CONTRACT.designatedAuthority,
    transitionalAuthority: BE_FOOTPRINT_METRIC_CONTRACT.transitionalAuthority,
    certificationGates: gates,
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
      dataOrigin: origin,
    },
    match: {
      canonicalBrandName: brandName || null,
      affiliationMatchers: matchers,
      matcherNormalization: "exact_trimmed_case_insensitive",
    },
    metrics,
    byRegion: aggregated.byRegion,
    breakdowns: {
      country: portfolio.country,
      propertyType: portfolio.propertyType,
      region: portfolio.region,
    },
    roomsCoverage: portfolio.roomsCoverage,
    census: {
      recordsMatched,
      excluded,
      paginationComplete: Boolean(paginationComplete),
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
  summary.displayMetrics = toHpcDisplayMetrics(summary);
  return summary;
}

/**
 * Read-only live HPC fetch for a brand's Current Brand matchers.
 * Caps pages to keep pilot validation bounded. Never writes.
 */
export async function fetchHpcRowsForBrandMatchers(brandMatchers = [], { maxPages = 8, pageSize = 100 } = {}) {
  if (!hpcFootprintLiveConfigured()) {
    return { ok: false, error: "HPC_CONFIG_MISSING", rows: [], truncated: false, pages: 0 };
  }
  const matchers = [...new Set((brandMatchers || []).map((m) => String(m || "").trim()).filter(Boolean))];
  if (!matchers.length) {
    return { ok: false, error: "NO_MATCHERS", rows: [], truncated: false, pages: 0 };
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
          truncated: false,
          pages,
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
    return { ok: false, error: err?.message || String(err), rows: [], truncated: false, pages };
  }
}

/**
 * Build footprint for one pilot / brand. Live fetch only when allowLive=true and configured.
 * Live fetches default to LIVE_UNVERIFIED unless independentCertification=true and all gates pass.
 */
export async function buildBrandHpcFootprintSummary({
  brandBasicsRecordId,
  brandName,
  brandMatchers = [],
  companyReported = null,
  preferCompanyReported = false,
  allowLive = false,
  prefetchedRows = null,
  dataOrigin = null,
  paginationComplete = false,
  independentCertification = false,
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
      dataOrigin: dataOrigin || BE_HPC_DATA_ORIGINS.LIVE_AIRTABLE,
      paginationComplete,
      independentCertification,
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
      dataOrigin: BE_HPC_DATA_ORIGINS.UNAVAILABLE,
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
      dataOrigin: BE_HPC_DATA_ORIGINS.UNAVAILABLE,
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
    dataOrigin: BE_HPC_DATA_ORIGINS.LIVE_AIRTABLE,
    paginationComplete: !fetchResult.truncated,
    independentCertification,
    coverageNotes: fetchResult.truncated
      ? ["HPC_FETCH_TRUNCATED: raise maxPages before independent certification"]
      : [`Live HPC pages fetched: ${fetchResult.pages}`],
  });
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
    certificationNote:
      "HPC_BRAND_ROLLUP_CERTIFIED requires live Airtable origin + identity + complete pagination + status filter + dedupe + documented coverage + independentCertification=true. Synthetic fixtures never certify.",
  };
}
