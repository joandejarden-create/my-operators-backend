/**
 * Admin API — Surfe Usage (GDI on-demand reveal accounting).
 * Admin-only. Never returns emails, phones, raw Surfe payloads, or API keys.
 */
import {
  summarizeSurfeLiveAccounting,
  readSurfeLiveAccounting,
  GDI_SURFE_USAGE_TRACKING_V1,
} from "../lib/group-demand-intelligence/gdi-surfe-live-gate-v1.js";
import { listGdiSelectableHotels } from "../lib/group-demand-intelligence/repository.js";
import { getSurfeCredits } from "../lib/surfe/client.js";

function hotelNameMap() {
  const map = {};
  try {
    for (const h of listGdiSelectableHotels() || []) {
      const id = h.hotelId || h.id;
      if (!id) continue;
      map[id] = h.displayName || h.hotelName || h.name || id;
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
    from: q.from ? String(q.from) : null,
    to: q.to ? String(q.to) : null,
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

  return res.status(200).json({
    ok: true,
    version: GDI_SURFE_USAGE_TRACKING_V1,
    remainingCredits,
    ...summary,
    // Explicit contract
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
    if (filters.hotelId && e.hotelId !== filters.hotelId) return false;
    if (filters.portfolioId && e.portfolioId !== filters.portfolioId) return false;
    const t = Date.parse(e.at || "");
    if (filters.from && Number.isFinite(t) && t < Date.parse(filters.from)) return false;
    if (filters.to && Number.isFinite(t) && t > Date.parse(filters.to)) return false;
    return true;
  });
  entries.sort((a, b) => String(b.at).localeCompare(String(a.at)));
  const page = entries.slice(0, limit).map((e) => ({
    lookupEventId: e.lookupEventId,
    at: e.at,
    hotelId: e.hotelId,
    hotelName: hotelNames[e.hotelId] || e.hotelId,
    portfolioId: e.portfolioId || null,
    opportunityId: e.opportunityId,
    lookupType: e.lookupType || e.channel,
    status: e.status,
    emailFound: e.emailFound === true || e.emailReturned === true,
    phoneFound: e.phoneFound === true || e.phoneReturned === true,
    creditsCharged: e.creditsCharged ?? null,
    costEstimateUsd: e.costEstimateUsd ?? null,
    costEstimateSource: e.costEstimateSource || null,
    providerLatencyMs: e.providerLatencyMs ?? null,
    errorCategory: e.errorCategory || e.error || null,
    resolvedVia: e.resolvedVia || null,
    surface: e.surface || null,
  }));

  return res.status(200).json({
    ok: true,
    version: GDI_SURFE_USAGE_TRACKING_V1,
    count: page.length,
    totalMatching: entries.length,
    events: page,
    includesPersonalContactDetails: false,
    includesRawSurfePayloads: false,
    historicalNote:
      doc.historicalNote ||
      "Only post-instrumentation events are listed; pre-tracking history is not fabricated.",
  });
}
