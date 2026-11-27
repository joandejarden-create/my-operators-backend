/**
 * Admin API — Surfe Usage V1.2 (GDI on-demand reveal accounting).
 * Admin-only. Never returns emails, phones, raw Surfe payloads, or API keys.
 */
import {
  summarizeSurfeLiveAccounting,
  readSurfeLiveAccounting,
  GDI_SURFE_USAGE_TRACKING_V1,
  GDI_SURFE_USAGE_TRACKING_V1_1,
  GDI_SURFE_USAGE_TRACKING_V1_2,
} from "../lib/group-demand-intelligence/gdi-surfe-live-gate-v1.js";
import {
  buildHotelContextMap,
  classifyUsageClass,
} from "../lib/group-demand-intelligence/gdi-surfe-usage-context-v1.js";
import {
  planSurfeUsageAirtableSync,
  PROPOSED_GDI_SURFE_USAGE_EVENTS_TABLE,
} from "../lib/group-demand-intelligence/gdi-surfe-usage-airtable-sync-v1.js";
import { getSurfeCredits } from "../lib/surfe/client.js";

function hotelNameMap() {
  const map = {};
  try {
    for (const [id, h] of buildHotelContextMap()) {
      if (!id) continue;
      map[id] = h.hotelName || id;
    }
  } catch {
    /* optional */
  }
  return map;
}

function parseQuery(req) {
  const q = req.query || {};
  return {
    hotelId: q.hotelId ? String(q.hotelId) : null,
    portfolioId: q.portfolioId ? String(q.portfolioId) : null,
    opportunityId: q.opportunityId ? String(q.opportunityId) : null,
    requestingOrganization: q.requestingOrganization
      ? String(q.requestingOrganization)
      : null,
    environment: q.environment ? String(q.environment) : null,
    usageClass: q.usageClass ? String(q.usageClass) : null,
    status: q.status ? String(q.status) : null,
    from: q.from ? String(q.from) : null,
    to: q.to ? String(q.to) : null,
    hideFixtures: q.hideFixtures === "1" || q.hideFixtures === "true",
    latestLimit: q.latestLimit ? Number(q.latestLimit) : 50,
  };
}

/**
 * GET /api/admin/surfe-usage/summary
 */
export async function getAdminSurfeUsageSummary(req, res) {
  const filters = parseQuery(req);
  const summary = summarizeSurfeLiveAccounting(
    { ...filters, hotelNames: hotelNameMap() },
    process.env,
  );
  let remainingCredits = null;
  try {
    const credits = await getSurfeCredits();
    if (credits.ok) {
      remainingCredits = {
        totalEmail: Number(credits.payload?.totalEmail ?? 0),
        totalMobile: Number(credits.payload?.totalMobile ?? 0),
        source: "surfe_get_credits",
      };
    }
  } catch {
    remainingCredits = null;
  }

  const knownCostLookups =
    (summary.totals?.successfulLookups || 0) +
    (summary.totals?.partialLookups || 0);
  const avgLookupCostUsd =
    knownCostLookups > 0 && summary.totals?.totalCostEstimateUsd != null
      ? Math.round(
          (summary.totals.totalCostEstimateUsd / knownCostLookups) * 10000,
        ) / 10000
      : null;

  return res.status(200).json({
    ok: true,
    version: GDI_SURFE_USAGE_TRACKING_V1_2,
    trackingCompatVersion: GDI_SURFE_USAGE_TRACKING_V1_1,
    legacyCompatVersion: GDI_SURFE_USAGE_TRACKING_V1,
    remainingCredits,
    ...summary,
    reporting: {
      avgLookupCostUsd,
      emailVsPhone: {
        emailFound: summary.totals?.emailFoundCount || 0,
        phoneFound: summary.totals?.phoneFoundCount || 0,
      },
      usageClassBreakdown: {
        FIXTURE_TEST: summary.totals?.fixtureTestCount || 0,
        DEVELOPMENT: summary.totals?.developmentCount || 0,
        STAGING: summary.totals?.stagingCount || 0,
        PRODUCTION: summary.totals?.productionCount || 0,
      },
      adoptionEligibleNote:
        "Only PRODUCTION class with resolved hotel names counts as customer adoption.",
    },
    airtable: {
      proposedTable: PROPOSED_GDI_SURFE_USAGE_EVENTS_TABLE,
      syncHealth: summary.airtableSyncHealth || null,
    },
    includesPersonalContactDetails: false,
    includesRawSurfePayloads: false,
  });
}

/**
 * GET /api/admin/surfe-usage/events
 */
export async function getAdminSurfeUsageEvents(req, res) {
  const filters = parseQuery(req);
  const limit = Math.min(Math.max(Number(req.query?.limit) || 100, 1), 500);
  const { doc } = readSurfeLiveAccounting(process.env);
  const hotelNames = hotelNameMap();
  let entries = Array.isArray(doc.entries) ? doc.entries.slice() : [];
  entries = entries.filter((e) => {
    if (!e || e.surfeAttempted === false) return false;
    const usageClass = classifyUsageClass(e);
    if (filters.hideFixtures && usageClass === "FIXTURE_TEST") return false;
    if (filters.hotelId && e.hotelId !== filters.hotelId) return false;
    if (filters.portfolioId && e.portfolioId !== filters.portfolioId) return false;
    if (filters.opportunityId && e.opportunityId !== filters.opportunityId) {
      return false;
    }
    if (filters.requestingOrganization) {
      const org = String(
        e.requestingOrganizationName || e.portfolioId || "",
      ).toLowerCase();
      if (!org.includes(String(filters.requestingOrganization).toLowerCase())) {
        return false;
      }
    }
    if (filters.environment) {
      const de = String(e.dataEnvironment || "").toLowerCase();
      const uc = String(usageClass).toLowerCase();
      const ef = String(filters.environment).toLowerCase();
      if (ef === "test" || ef === "fixture_test") {
        if (uc !== "fixture_test" && de !== "test") return false;
      } else if (de !== ef && uc !== ef) return false;
    }
    if (filters.usageClass && usageClass !== filters.usageClass) return false;
    if (filters.status) {
      if (String(e.status || "").toLowerCase() !== String(filters.status).toLowerCase()) {
        return false;
      }
    }
    const t = Date.parse(e.at || "");
    if (filters.from && Number.isFinite(t) && t < Date.parse(filters.from)) {
      return false;
    }
    if (filters.to && Number.isFinite(t) && t > Date.parse(filters.to)) {
      return false;
    }
    return true;
  });
  entries.sort((a, b) => String(b.at).localeCompare(String(a.at)));
  const page = entries.slice(0, limit).map((e) => {
    const usageClass = classifyUsageClass(e);
    return {
      lookupEventId: e.lookupEventId,
      clientRequestId: e.clientRequestId || null,
      at: e.at,
      hotelId: e.hotelId,
      hotelName:
        (e.hotelName && e.hotelName !== "UNKNOWN" ? e.hotelName : null) ||
        hotelNames[e.hotelId] ||
        "UNKNOWN",
      hotelMarket: e.hotelMarket || null,
      hotelCountry: e.hotelCountry || null,
      portfolioId: e.portfolioId || null,
      opportunityId: e.opportunityId,
      opportunityTitle:
        e.opportunityTitle && e.opportunityTitle !== "UNKNOWN"
          ? e.opportunityTitle
          : "UNKNOWN",
      opportunityOrganizationName:
        e.opportunityOrganizationName && e.opportunityOrganizationName !== "UNKNOWN"
          ? e.opportunityOrganizationName
          : null,
      opportunityDrawerHref: e.opportunityDrawerHref || null,
      requestingOrganizationName:
        e.requestingOrganizationName &&
        e.requestingOrganizationName !== "UNKNOWN"
          ? e.requestingOrganizationName
          : "UNKNOWN",
      requestingHotelOrOfficeId:
        e.requestingHotelOrOfficeId &&
        e.requestingHotelOrOfficeId !== "UNKNOWN"
          ? e.requestingHotelOrOfficeId
          : "UNKNOWN",
      requestingOfficeMarket:
        e.requestingOfficeMarket && e.requestingOfficeMarket !== "UNKNOWN"
          ? e.requestingOfficeMarket
          : "UNKNOWN",
      lookupType: e.lookupType || e.channel,
      status: e.status,
      emailFound: e.emailFound === true || e.emailReturned === true,
      phoneFound: e.phoneFound === true || e.phoneReturned === true,
      creditsCharged: e.creditsCharged ?? null,
      creditCertainty: e.creditCertainty || null,
      creditsConfirmed: e.creditsConfirmed ?? null,
      creditsEstimated: e.creditsEstimated ?? null,
      costEstimateUsd: e.costEstimateUsd ?? null,
      costCertainty: e.costCertainty || null,
      costEstimateSource: e.costEstimateSource || null,
      attributionWarning: e.attributionWarning || null,
      providerLatencyMs: e.providerLatencyMs ?? null,
      errorCategory: e.errorCategory || e.error || null,
      resolvedVia: e.resolvedVia || null,
      surface: e.surface || null,
      dataEnvironment: e.dataEnvironment || null,
      usageClass,
      airtableSyncStatus: e.airtableSyncStatus || "pending",
    };
  });

  return res.status(200).json({
    ok: true,
    version: GDI_SURFE_USAGE_TRACKING_V1_2,
    count: page.length,
    events: page,
    includesPersonalContactDetails: false,
    includesRawSurfePayloads: false,
  });
}

/**
 * GET /api/admin/surfe-usage/airtable-plan — dry-run sync plan (no writes).
 */
export async function getAdminSurfeUsageAirtablePlan(req, res) {
  const { doc } = readSurfeLiveAccounting(process.env);
  const includeFixtures = req.query?.includeFixtures === "1";
  const plan = planSurfeUsageAirtableSync(doc.entries || [], {
    includeFixtures,
  });
  return res.status(200).json({
    ok: true,
    ...plan,
    note: "No Airtable writes. Table creation requires founder/IC schema approval.",
  });
}

/**
 * GET /api/admin/surfe-usage/fullenrich-benchmark-eligibility
 * Admin-only readiness view. Never triggers paid enrichment or returns provider PII.
 */
export async function getAdminFullEnrichBenchmarkEligibility(req, res) {
  const { buildFullEnrichBenchmarkEligibilityReport } = await import(
    "../lib/group-demand-intelligence/gdi-fullenrich-benchmark-eligibility-v1.js"
  );
  const report = buildFullEnrichBenchmarkEligibilityReport({
    env: process.env,
  });
  return res.status(200).json({
    ok: true,
    adminOnly: true,
    customerFacing: false,
    paidLookupsTriggered: false,
    ...report,
  });
}
