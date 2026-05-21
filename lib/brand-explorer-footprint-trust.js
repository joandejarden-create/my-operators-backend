/**
 * Brand Explorer footprint display trust (Node / audit). Keep in sync with
 * public/js/brand-explorer-census-metrics.js for browser parity.
 */

export const SOURCE_NOTE_CENSUS = "Based on current Dealality census records.";
export const SOURCE_NOTE_MVP = "Based on brand setup footprint data.";
export const SOURCE_NOTE_MVP_VERIFIED =
  "Based on verified brand setup footprint data.";
export const SOURCE_NOTE_MVP_ESTIMATED =
  "Based on estimated brand setup footprint data.";
export const VERIFIED_EMPTY_MESSAGE = "Portfolio data being verified.";

function hasVal(v) {
  return v != null && v !== "";
}

function hasNum(v) {
  const n = Number(v);
  return Number.isFinite(n) && n > 0;
}

function normText(v) {
  return String(v == null ? "" : v)
    .trim()
    .toLowerCase();
}

export function useCensusSummary(brand) {
  const cs = brand && brand.censusSummary;
  return !!(cs && cs.available === true && cs.fallbackRecommended === false);
}

export function isCensusFallbackRecommended(brand) {
  const cs = brand && brand.censusSummary;
  if (!cs) return false;
  return cs.fallbackRecommended === true;
}

export function footprintHasMetricValues(fp) {
  if (!fp || typeof fp !== "object") return false;
  if (hasNum(fp.totalExistingHotels) || hasNum(fp.totalExistingRooms)) return true;
  const rd = fp.regionalDistribution;
  if (rd && typeof rd === "object") {
    for (const k of Object.keys(rd)) {
      const o = rd[k] || {};
      if (hasNum(o.hotels) || hasNum(o.rooms)) return true;
    }
  }
  return false;
}

function verificationLooksUnverified(text) {
  const t = normText(text);
  if (!t) return false;
  return /unverified|draft|placeholder|demo|sample|pending|tbd|not verified|under review/.test(
    t
  );
}

function verificationLooksVerified(text) {
  const t = normText(text);
  if (!t) return false;
  if (verificationLooksUnverified(t)) return false;
  return /verified|audited|confirmed|published|franchise disclosure|fdd|annual report|census|brand setup/.test(
    t
  );
}

function dataSourceLooksUnverified(text) {
  const t = normText(text);
  if (!t) return false;
  return /placeholder|demo|sample|draft|unverified|tbd|mock/.test(t);
}

function dataSourceLooksCurated(text) {
  const t = normText(text);
  if (!t) return false;
  if (dataSourceLooksUnverified(t)) return false;
  return /brand setup|airtable|fdd|franchise|annual|investor|curated|operations|published|ye20\d{2}/.test(
    t
  );
}

function isGenericHeroVerification(text) {
  return normText(text) === "verified by brand";
}

function isGenericHeroDataSource(text) {
  return normText(text) === "live airtable / brand setup data";
}

function explicitFootprintStatus(fp) {
  const status = fp && fp.verification && fp.verification.status;
  return status ? String(status).trim() : "";
}

/**
 * Phase 1E: explicit Footprint Data Status on brand.footprint.verification
 */
function resolveExplicitFootprintTrust(fp) {
  const status = explicitFootprintStatus(fp);
  if (!status) return null;

  const hasMetrics = footprintHasMetricValues(fp);

  if (status === "Verified") {
    return {
      sourceUsed: "mvp-footprint",
      isCensusBacked: false,
      isMvpFallback: true,
      isUnverifiedFallback: false,
      displaySourceLabel: SOURCE_NOTE_MVP_VERIFIED,
      showVerifiedMetrics: hasMetrics,
      mvpTrustTier: "verified",
    };
  }

  if (status === "Estimated") {
    return {
      sourceUsed: "mvp-footprint",
      isCensusBacked: false,
      isMvpFallback: true,
      isUnverifiedFallback: false,
      displaySourceLabel: SOURCE_NOTE_MVP_ESTIMATED,
      showVerifiedMetrics: hasMetrics,
      mvpTrustTier: "estimated",
    };
  }

  if (status === "Placeholder" || status === "Needs Review") {
    return {
      sourceUsed: "unverified",
      isCensusBacked: false,
      isMvpFallback: false,
      isUnverifiedFallback: true,
      displaySourceLabel: VERIFIED_EMPTY_MESSAGE,
      showVerifiedMetrics: false,
      mvpTrustTier: "unverified",
    };
  }

  return null;
}

function censusFallbackWithZeroOpenHotels(brand) {
  if (!isCensusFallbackRecommended(brand)) return false;
  const open = brand.censusSummary?.metrics?.totalOpenHotels;
  return open == null || Number(open) === 0;
}

/** Phase 1D legacy trust when explicit Footprint Data Status is absent. */
function isMvpFootprintVerifiedLegacy(brand, fp) {
  if (!footprintHasMetricValues(fp)) return false;

  const ver = fp && fp.verification;
  if (ver && hasVal(ver.figuresAsOf)) return true;

  const fv = (fp && fp.formValues) || {};
  const blockLegacyFiguresAsOf = censusFallbackWithZeroOpenHotels(brand);
  if (!blockLegacyFiguresAsOf && hasVal(fv.figuresAsOf)) return true;

  if (isCensusFallbackRecommended(brand)) return false;

  if (hasVal(brand.explorerHeroVerification)) {
    if (isGenericHeroVerification(brand.explorerHeroVerification)) return false;
    if (verificationLooksUnverified(brand.explorerHeroVerification)) return false;
    if (verificationLooksVerified(brand.explorerHeroVerification)) return true;
    return true;
  }

  if (hasVal(brand.explorerHeroDataSource)) {
    if (isGenericHeroDataSource(brand.explorerHeroDataSource)) return false;
    if (dataSourceLooksUnverified(brand.explorerHeroDataSource)) return false;
    if (dataSourceLooksCurated(brand.explorerHeroDataSource)) return true;
    return true;
  }

  return false;
}

/**
 * @param {object} brand Brand library / explorer brand payload
 */
export function footprintTrustModel(brand) {
  const fp = (brand && brand.footprint) || {};

  if (useCensusSummary(brand)) {
    return {
      sourceUsed: "census",
      isCensusBacked: true,
      isMvpFallback: false,
      isUnverifiedFallback: false,
      displaySourceLabel: SOURCE_NOTE_CENSUS,
      showVerifiedMetrics: true,
      mvpTrustTier: null,
    };
  }

  const explicit = resolveExplicitFootprintTrust(fp);
  if (explicit) return explicit;

  const mvpVerified = isMvpFootprintVerifiedLegacy(brand, fp);
  if (mvpVerified) {
    return {
      sourceUsed: "mvp-footprint",
      isCensusBacked: false,
      isMvpFallback: true,
      isUnverifiedFallback: false,
      displaySourceLabel: SOURCE_NOTE_MVP,
      showVerifiedMetrics: true,
      mvpTrustTier: "legacy",
    };
  }

  return {
    sourceUsed: "unverified",
    isCensusBacked: false,
    isMvpFallback: false,
    isUnverifiedFallback: true,
    displaySourceLabel: VERIFIED_EMPTY_MESSAGE,
    showVerifiedMetrics: false,
    mvpTrustTier: "unverified",
  };
}

/**
 * QA CSV / ops recommendation label.
 */
export function displaySourceRecommendation(brand) {
  const trust = footprintTrustModel(brand);
  if (trust.sourceUsed === "census") return "Census";
  if (trust.sourceUsed === "mvp-footprint") {
    if (trust.mvpTrustTier === "estimated") return "Estimated MVP";
    return "Verified MVP";
  }
  return "Unverified / Do Not Display";
}

/**
 * Build brand-shaped object for audit trust (census + footprint fields).
 */
export function brandFootprintTrustInput({
  name,
  parentCompany,
  explorerHeroVerification,
  explorerHeroDataSource,
  footprint,
  censusSummary,
}) {
  return {
    name,
    parentCompany,
    explorerHeroVerification,
    explorerHeroDataSource,
    footprint,
    censusSummary,
  };
}
