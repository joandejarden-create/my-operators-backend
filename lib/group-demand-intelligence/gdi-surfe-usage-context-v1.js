/**
 * Surfe Usage V1.2 — hotel / opportunity / request-origin / environment context.
 * Resolves human-readable names from canonical GDI sources. Never invents names
 * from technical IDs. Never stores Surfe contact PII.
 */
import { listGdiSelectableHotels } from "./repository.js";

export const GDI_SURFE_USAGE_TRACKING_V1_2 = "gdi-surfe-usage-tracking-v1.2";

export const USAGE_CLASS = Object.freeze({
  FIXTURE_TEST: "FIXTURE_TEST",
  DEVELOPMENT: "DEVELOPMENT",
  STAGING: "STAGING",
  PRODUCTION: "PRODUCTION",
  UNKNOWN: "UNKNOWN",
});

export const DATA_ENVIRONMENT = Object.freeze({
  development: "development",
  staging: "staging",
  production: "production",
  test: "test",
});

const FIXTURE_HOTEL_IDS = new Set([
  "hotel_a",
  "hotel_demo",
  "hotel_parallel",
  "hotel_b",
  "hotel_test",
]);

const FIXTURE_OPPORTUNITY_PREFIXES = ["opp_p_", "opp_1", "opp_test", "opp_fixture"];
const FIXTURE_PORTFOLIO_IDS = new Set(["port_1", "port_test", "portfolio_test"]);

function text(v) {
  return String(v == null ? "" : v).trim();
}

function unknown() {
  return "UNKNOWN";
}

/** Build hotel lookup map from canonical GDI selectable hotels. */
export function buildHotelContextMap() {
  const map = new Map();
  try {
    for (const h of listGdiSelectableHotels() || []) {
      const id = text(h.hotelId || h.id);
      if (!id) continue;
      map.set(id, {
        hotelId: id,
        hotelName: text(h.displayName || h.hotelName || h.name) || null,
        market: text(h.market || h.city || h.locationLine) || null,
        country: text(h.country) || null,
        city: text(h.city) || null,
        censusRecordId: h.censusRecordId || null,
      });
    }
  } catch {
    /* optional at boot */
  }
  return map;
}

export function resolveOpportunityHotelContext(hotelId, hotelMap = null) {
  const id = text(hotelId) || null;
  if (!id) {
    return {
      hotelId: null,
      hotelName: unknown(),
      market: unknown(),
      country: unknown(),
      resolution: "missing",
    };
  }
  const map = hotelMap || buildHotelContextMap();
  const hit = map.get(id);
  if (!hit || !hit.hotelName) {
    return {
      hotelId: id,
      hotelName: unknown(),
      market: unknown(),
      country: unknown(),
      resolution: "unresolved",
    };
  }
  return {
    hotelId: id,
    hotelName: hit.hotelName,
    market: hit.market || unknown(),
    country: hit.country || unknown(),
    city: hit.city || unknown(),
    resolution: "resolved",
  };
}

/**
 * Extract display fields from a loaded GDI opportunity object.
 * Does not invent titles from IDs.
 */
export function resolveOpportunityContext(opportunity = {}, opportunityId = null) {
  const id =
    text(opportunityId) ||
    text(opportunity?.opportunityId || opportunity?.id) ||
    null;
  if (!id && !opportunity) {
    return {
      opportunityId: null,
      opportunityTitle: unknown(),
      opportunityOrganizationName: unknown(),
      drawerHref: null,
      resolution: "missing",
    };
  }
  const title =
    text(
      opportunity?.title ||
        opportunity?.displayTitle ||
        opportunity?.opportunityName ||
        opportunity?.name,
    ) || null;
  const org =
    text(
      opportunity?.organizationName ||
        opportunity?.companyName ||
        opportunity?.eventName ||
        opportunity?.associationName ||
        opportunity?.projectName ||
        opportunity?.employerName,
    ) || null;
  const hotelId = text(opportunity?.hotelId) || null;
  const drawerHref =
    hotelId && id
      ? `/app#/group-demand-intelligence?hotelId=${encodeURIComponent(hotelId)}&opportunityId=${encodeURIComponent(id)}`
      : null;

  if (!title && !org) {
    return {
      opportunityId: id,
      opportunityTitle: unknown(),
      opportunityOrganizationName: unknown(),
      drawerHref,
      resolution: id ? "unresolved" : "missing",
    };
  }
  return {
    opportunityId: id,
    opportunityTitle: title || org || unknown(),
    opportunityOrganizationName: org || unknown(),
    drawerHref,
    resolution: "resolved",
  };
}

/**
 * Request origin is the authenticated customer context initiating the lookup —
 * NOT the opportunity hotel by default.
 */
export function resolveRequestOriginContext({
  dealalityUser = null,
  req = null,
  opportunity = null,
  body = null,
} = {}) {
  const user = dealalityUser || req?.dealalityUser || null;
  const companyIds = Array.isArray(user?.companyIds)
    ? user.companyIds.map(text).filter(Boolean)
    : [];
  const requestingPortfolioId =
    text(
      body?.requestingPortfolioId ||
        user?.companyProfileId ||
        companyIds[0] ||
        opportunity?.requestingPortfolioId,
    ) || null;

  const requestingOrganizationName =
    text(
      body?.requestingOrganizationName ||
        user?.companyName ||
        user?.organizationName,
    ) || null;

  // Explicit request-origin hotel/office — never fall back to opportunity hotel.
  const requestingHotelOrOfficeId =
    text(
      body?.requestingHotelOrOfficeId ||
        body?.requestingOfficeId ||
        user?.activeHotelId ||
        user?.officeId,
    ) || null;

  const requestingOfficeMarket =
    text(
      body?.requestingOfficeMarket ||
        user?.officeMarket ||
        user?.officeCity ||
        user?.market,
    ) || null;

  const requestingOfficeCity =
    text(body?.requestingOfficeCity || user?.officeCity) || null;

  return {
    requestingPortfolioId: requestingPortfolioId || unknown(),
    requestingHotelOrOfficeId: requestingHotelOrOfficeId || unknown(),
    requestingOrganizationName: requestingOrganizationName || unknown(),
    requestingOfficeMarket: requestingOfficeMarket || unknown(),
    requestingOfficeCity: requestingOfficeCity || unknown(),
    requestingUserHash: null, // filled by ledger hashUserId
    originResolution:
      requestingOrganizationName || requestingPortfolioId
        ? requestingHotelOrOfficeId
          ? "resolved"
          : "partial"
        : "unknown",
  };
}

export function resolveDataEnvironment(env = process.env) {
  const forced = text(env.GDI_SURFE_USAGE_ENVIRONMENT).toLowerCase();
  if (
    forced === DATA_ENVIRONMENT.production ||
    forced === DATA_ENVIRONMENT.staging ||
    forced === DATA_ENVIRONMENT.development ||
    forced === DATA_ENVIRONMENT.test
  ) {
    return forced;
  }
  if (text(env.DEALALITY_INTEGRATED_STAGING) === "1") {
    return DATA_ENVIRONMENT.staging;
  }
  const cwd = text(env.DEALALITY_CWD || process.cwd()).replace(/\\/g, "/");
  if (cwd.includes("ws-integrated-staging")) {
    return DATA_ENVIRONMENT.staging;
  }
  if (
    text(env.RAILWAY_ENVIRONMENT).toLowerCase() === "production" ||
    (text(env.NODE_ENV).toLowerCase() === "production" &&
      text(env.DEALALITY_FORCE_PRODUCTION_USAGE) === "1")
  ) {
    return DATA_ENVIRONMENT.production;
  }
  if (text(env.NODE_ENV).toLowerCase() === "test") {
    return DATA_ENVIRONMENT.test;
  }
  return DATA_ENVIRONMENT.development;
}

export function classifyUsageClass(entry = {}, env = process.env) {
  if (entry.usageClass && USAGE_CLASS[entry.usageClass]) {
    return entry.usageClass;
  }
  const hotelId = text(entry.hotelId);
  const opportunityId = text(entry.opportunityId);
  const portfolioId = text(entry.portfolioId);
  const clientRequestId = text(entry.clientRequestId);

  if (
    FIXTURE_HOTEL_IDS.has(hotelId) ||
    FIXTURE_PORTFOLIO_IDS.has(portfolioId) ||
    FIXTURE_OPPORTUNITY_PREFIXES.some(
      (p) => opportunityId === p || opportunityId.startsWith(p),
    ) ||
    clientRequestId.startsWith("cr_parallel_") ||
    clientRequestId.startsWith("cr_unique_") ||
    clientRequestId.startsWith("cr_legitimate_")
  ) {
    return USAGE_CLASS.FIXTURE_TEST;
  }

  const dataEnv = text(entry.dataEnvironment) || resolveDataEnvironment(env);
  if (dataEnv === DATA_ENVIRONMENT.test) return USAGE_CLASS.FIXTURE_TEST;
  if (dataEnv === DATA_ENVIRONMENT.development) return USAGE_CLASS.DEVELOPMENT;
  if (dataEnv === DATA_ENVIRONMENT.staging) return USAGE_CLASS.STAGING;
  if (dataEnv === DATA_ENVIRONMENT.production) return USAGE_CLASS.PRODUCTION;
  return USAGE_CLASS.UNKNOWN;
}

/**
 * Compose enrichment payload for ledger append / summary projection.
 */
export function enrichSurfeUsageContext({
  hotelId = null,
  opportunityId = null,
  opportunity = null,
  dealalityUser = null,
  req = null,
  body = null,
  hotelMap = null,
  env = process.env,
} = {}) {
  const hotel = resolveOpportunityHotelContext(hotelId, hotelMap);
  const opp = resolveOpportunityContext(opportunity, opportunityId);
  const origin = resolveRequestOriginContext({
    dealalityUser,
    req,
    opportunity,
    body,
  });
  const dataEnvironment = resolveDataEnvironment(env);
  const usageClass = classifyUsageClass(
    {
      hotelId,
      opportunityId,
      portfolioId: opportunity?.portfolioId || body?.portfolioId,
      clientRequestId: body?.clientRequestId,
      dataEnvironment,
    },
    env,
  );

  return {
    schemaVersion: GDI_SURFE_USAGE_TRACKING_V1_2,
    // Opportunity hotel
    hotelId: hotel.hotelId,
    hotelName: hotel.hotelName,
    hotelMarket: hotel.market,
    hotelCountry: hotel.country,
    hotelResolution: hotel.resolution,
    // Opportunity
    opportunityId: opp.opportunityId,
    opportunityTitle: opp.opportunityTitle,
    opportunityOrganizationName: opp.opportunityOrganizationName,
    opportunityDrawerHref: opp.drawerHref,
    opportunityResolution: opp.resolution,
    // Request origin (separate from opportunity hotel)
    requestingPortfolioId: origin.requestingPortfolioId,
    requestingHotelOrOfficeId: origin.requestingHotelOrOfficeId,
    requestingOrganizationName: origin.requestingOrganizationName,
    requestingOfficeMarket: origin.requestingOfficeMarket,
    requestingOfficeCity: origin.requestingOfficeCity,
    originResolution: origin.originResolution,
    // Environment
    dataEnvironment,
    usageClass,
  };
}

export function isCustomerAdoptionEligible(entry = {}) {
  return (
    classifyUsageClass(entry) === USAGE_CLASS.PRODUCTION &&
    text(entry.hotelResolution) === "resolved"
  );
}
