/**
 * Brand AI Visibility authenticated read API.
 * No provider calls. No Airtable writes. Authorization on every brandId.
 */

import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import { loadBrandViewerEntitlements, fetchBrandBasicsMetaForIds } from "../lib/ai-visibility/load-brand-entitlements.js";
import { demoBrandPortfolioEntitlementOverride } from "../lib/dealality/demo-brand-portfolio-context.js";
import { buildFixtureEntitlementGraph } from "../lib/ai-visibility/entitlements.js";
import { resolvePeerSetMembership, peerSetBrandNamesById, PEER_SET_ID_V2 } from "../lib/ai-visibility/peer-sets.js";
import {
  getBrandPortfolioPayload,
  getBrandOverviewPayload,
  getBrandTrendPayload,
  getBrandQuestionsPayload,
  getBrandCompetitorsPayload,
  getBrandSourcesPayload,
  getBrandEvidencePayload,
  parseGeographyQuery,
} from "../lib/ai-visibility/brand-read-service.js";
import { getBrandExecutiveSummaryPayload } from "../lib/ai-visibility/brand-executive-summary.js";
import {
  DEFAULT_AI_VISIBILITY_PROVIDER,
  resolveProviderId,
} from "../lib/ai-visibility/provider-dimension.js";
import { toClientAccessError } from "../lib/ai-visibility/access-reason-codes.js";
import { getBrandBenchmarkPayload } from "../lib/ai-visibility/competitive-moat/brand-benchmark-read-service.js";
import { CUSTOMER_PAYLOAD_ALLOWLIST } from "../lib/ai-visibility/competitive-moat/customer-payload.js";

/**
 * Hotel Decision Visibility public route retired in Phase 3A.4.
 * Proprietary HDV intelligence is merged into executive-summary + brand overview.
 * Internal service: lib/ai-visibility/hotel-decision-visibility.js (kept).
 */

function getStore(req) {
  if (req.aiVisibilityStore) return req.aiVisibilityStore;
  // Prefer federated four-provider baseline (wave1 + gemini/perplexity/claude)
  // over legacy Phase 2E recovery so Brand UI sees measured multi-provider data.
  return createBrandAiVisibilityReadStore({
    rootDir: process.env.AI_VISIBILITY_STORE_ROOT || null,
  });
}

/**
 * Explicit provider from query. Omitted → openai (backward compatible).
 * Never remaps an explicit Gemini/Perplexity request to OpenAI.
 */
function providerFromQuery(req) {
  const raw = req.query.provider;
  if (raw == null || String(raw).trim() === "") return DEFAULT_AI_VISIBILITY_PROVIDER;
  return resolveProviderId(raw);
}

function languageFromQuery(req) {
  const raw = req.query.language;
  if (raw == null || String(raw).trim() === "") return null;
  return String(raw).trim();
}

function deny(res, access) {
  const client = toClientAccessError(access.reasonCode);
  return res.status(access.reasonCode === "VIEWER_REQUIRED" ? 401 : 403).json({
    ok: false,
    success: false,
    ...client,
    reasonCode: access.reasonCode,
  });
}

async function withEntitlements(req) {
  const demoOverride = demoBrandPortfolioEntitlementOverride(req.dealalityUser);
  if (demoOverride) {
    const membership = resolvePeerSetMembership({
      peerSetId: PEER_SET_ID_V2,
      commercialRegion: parseGeographyQuery(req.query).commercialRegion,
    });
    // Peer cohort names (Autograph, Curio, …) — not only the showcase portfolio map.
    const peerNames = peerSetBrandNamesById(PEER_SET_ID_V2);
    const brandIdsForBasics = [
      ...new Set([
        ...(demoOverride.entitledBrandIds || []),
        ...(membership.entityIds || []),
      ]),
    ];
    let brandBasicsById = {};
    let brandWebsiteNames = {};
    let ownedDomainCoverage = null;
    try {
      const brandMeta = await fetchBrandBasicsMetaForIds(brandIdsForBasics);
      brandBasicsById = brandMeta.brandBasicsById || {};
      brandWebsiteNames = brandMeta.brandNamesById || {};
      ownedDomainCoverage = brandMeta.ownedDomainCoverage || null;
    } catch (err) {
      if (process.env.NODE_ENV !== "production") {
        console.warn(
          "[ai-visibility] demo portfolio brand basics enrichment failed:",
          err.message
        );
      }
    }
    return {
      ok: true,
      entitlementGraph: buildFixtureEntitlementGraph({
        entitledBrandIds: demoOverride.entitledBrandIds,
        peerBrandIds: membership.entityIds || [],
        source: demoOverride.source,
      }),
      brandNamesById: {
        ...peerNames,
        ...brandWebsiteNames,
        ...(demoOverride.brandNamesById || {}),
      },
      brandBasicsById,
      ownedDomainCoverage,
      source: demoOverride.source,
      demoBrandPortfolioKey: demoOverride.demoBrandPortfolioKey,
      AIRTABLE_WRITES: 0,
    };
  }

  return loadBrandViewerEntitlements(req.dealalityUser, {
    entitlementGraph: req.aiVisibilityEntitlementGraph || undefined,
    brandNamesById: req.aiVisibilityBrandNamesById || undefined,
    commercialRegion: parseGeographyQuery(req.query).commercialRegion,
  });
}

export async function getBrandPortfolio(req, res) {
  try {
    const ent = await withEntitlements(req);
    const payload = await getBrandPortfolioPayload({
      dealalityUser: req.dealalityUser,
      entitlementGraph: ent.entitlementGraph,
      brandNamesById: ent.brandNamesById,
      store: getStore(req),
      provider: providerFromQuery(req),
      geography: req.query.geography || req.query.region,
      language: languageFromQuery(req),
    });
    return res.status(200).json({
      success: true,
      ...payload,
      demoBrandPortfolioKey: ent.demoBrandPortfolioKey || req.dealalityUser?.demoBrandPortfolioKey || null,
      AIRTABLE_WRITES: 0,
      LIVE_PROVIDER_CALLS: 0,
      OPPORTUNITY_WRITES: 0,
    });
  } catch (err) {
    console.error("[ai-visibility-brand] portfolio:", err.message);
    return res.status(500).json({
      ok: false,
      success: false,
      error: "server_error",
      message: "Failed to load AI Visibility portfolio.",
    });
  }
}

/**
 * Company-scoped Executive Summary (portfolio briefing).
 * Must be registered before /:brandId routes.
 */
export async function getBrandExecutiveSummary(req, res) {
  try {
    const ent = await withEntitlements(req);
    if (typeof console !== "undefined" && console.info) {
      console.info("[ai-visibility-brand] executive-summary entitlement", {
        demoBrandPortfolioKey:
          ent.demoBrandPortfolioKey || req.dealalityUser?.demoBrandPortfolioKey || null,
        source: ent.source || null,
        activeWorkspace: req.dealalityUser?.activeWorkspace || null,
        entitledGuess: ent.entitlementGraph?.entitledBrandIds?.length ??
          ent.entitlementGraph?.brands?.length ??
          null,
        demoHeader: req.headers?.["x-dealality-demo-brand-portfolio"] || null,
        workspaceHeader: req.headers?.["x-dealality-active-workspace"] || null,
      });
    }
    const payload = await getBrandExecutiveSummaryPayload({
      dealalityUser: req.dealalityUser,
      entitlementGraph: ent.entitlementGraph,
      brandNamesById: ent.brandNamesById,
      brandBasicsById: ent.brandBasicsById || {},
      store: getStore(req),
      provider: providerFromQuery(req),
      geography: req.query.geography || req.query.region,
      language: languageFromQuery(req),
    });
    const entitledBrands = Array.isArray(payload?.portfolioOverview?.brands)
      ? payload.portfolioOverview.brands.map((b) => b.brandName || b.brandId)
      : [];
    if (typeof console !== "undefined" && console.info) {
      console.info("[ai-visibility-brand] executive-summary result", {
        entitledCount: entitledBrands.length,
        brands: entitledBrands,
        portfolioAiPresence: payload?.currentPosition?.portfolioAiPresence?.display || null,
      });
    }
    return res.status(200).json({
      success: true,
      ...payload,
      entitlementSource: ent.source || null,
      demoBrandPortfolioKey:
        ent.demoBrandPortfolioKey || req.dealalityUser?.demoBrandPortfolioKey || null,
      entitledCount: Array.isArray(payload?.portfolioOverview?.brands)
        ? payload.portfolioOverview.brands.length
        : null,
      AIRTABLE_WRITES: 0,
      LIVE_PROVIDER_CALLS: 0,
      OPPORTUNITY_WRITES: 0,
    });
  } catch (err) {
    console.error("[ai-visibility-brand] executive-summary:", err.message);
    return res.status(500).json({
      ok: false,
      success: false,
      error: "server_error",
      message: "Failed to load Brand AI Visibility executive summary.",
    });
  }
}

export async function getBrandOverview(req, res) {
  try {
    const brandId = String(req.params.brandId || "").trim();
    const ent = await withEntitlements(req);
    const payload = await getBrandOverviewPayload({
      dealalityUser: req.dealalityUser,
      entitlementGraph: ent.entitlementGraph,
      brandNamesById: ent.brandNamesById,
      brandBasicsById: ent.brandBasicsById || {},
      store: getStore(req),
      brandId,
      provider: providerFromQuery(req),
      geography: req.query.geography || req.query.region,
      language: languageFromQuery(req),
    });
    if (payload.ok === false && payload.allowed === false) return deny(res, payload);
    return res.status(200).json({
      success: true,
      ...payload,
      AIRTABLE_WRITES: 0,
      LIVE_PROVIDER_CALLS: 0,
      OPPORTUNITY_WRITES: 0,
    });
  } catch (err) {
    console.error("[ai-visibility-brand] overview:", err.message);
    return res.status(500).json({
      ok: false,
      success: false,
      error: "server_error",
      message: "Failed to load brand AI Visibility overview.",
    });
  }
}

export async function getBrandTrend(req, res) {
  try {
    const brandId = String(req.params.brandId || "").trim();
    const ent = await withEntitlements(req);
    const payload = await getBrandTrendPayload({
      dealalityUser: req.dealalityUser,
      entitlementGraph: ent.entitlementGraph,
      store: getStore(req),
      brandId,
      provider: providerFromQuery(req),
      geography: req.query.geography || req.query.region,
      language: languageFromQuery(req),
      range: req.query.range,
    });
    if (payload.ok === false && payload.allowed === false) return deny(res, payload);
    return res.status(200).json({
      success: true,
      ...payload,
      AIRTABLE_WRITES: 0,
      LIVE_PROVIDER_CALLS: 0,
    });
  } catch (err) {
    console.error("[ai-visibility-brand] trend:", err.message);
    return res.status(500).json({
      ok: false,
      success: false,
      error: "server_error",
      message: "Failed to load presence trend.",
    });
  }
}

export async function getBrandQuestions(req, res) {
  try {
    const brandId = String(req.params.brandId || "").trim();
    const ent = await withEntitlements(req);
    const payload = await getBrandQuestionsPayload({
      dealalityUser: req.dealalityUser,
      entitlementGraph: ent.entitlementGraph,
      brandNamesById: ent.brandNamesById,
      store: getStore(req),
      brandId,
      provider: providerFromQuery(req),
      geography: req.query.geography || req.query.region,
      language: languageFromQuery(req),
      filter: String(req.query.filter || "all").toLowerCase(),
      intentTerritory: req.query.intent || req.query.intentTerritory || undefined,
      limit: req.query.limit != null ? Number(req.query.limit) : 50,
      offset: req.query.offset != null ? Number(req.query.offset) : 0,
      watchlistMode: req.query.watchlistMode || req.query.mode || null,
      groupBy: req.query.groupBy || null,
    });
    if (payload.ok === false && payload.allowed === false) return deny(res, payload);
    return res.status(200).json({
      success: true,
      ...payload,
      AIRTABLE_WRITES: 0,
      LIVE_PROVIDER_CALLS: 0,
    });
  } catch (err) {
    console.error("[ai-visibility-brand] questions:", err.message);
    return res.status(500).json({
      ok: false,
      success: false,
      error: "server_error",
      message: "Failed to load owner questions.",
    });
  }
}

export async function getBrandCompetitors(req, res) {
  try {
    const brandId = String(req.params.brandId || "").trim();
    const ent = await withEntitlements(req);
    const payload = await getBrandCompetitorsPayload({
      dealalityUser: req.dealalityUser,
      entitlementGraph: ent.entitlementGraph,
      brandNamesById: ent.brandNamesById,
      store: getStore(req),
      brandId,
      provider: providerFromQuery(req),
      geography: req.query.geography || req.query.region,
      language: languageFromQuery(req),
    });
    if (payload.ok === false && payload.allowed === false) return deny(res, payload);
    return res.status(200).json({
      success: true,
      ...payload,
      AIRTABLE_WRITES: 0,
      LIVE_PROVIDER_CALLS: 0,
    });
  } catch (err) {
    console.error("[ai-visibility-brand] competitors:", err.message);
    return res.status(500).json({
      ok: false,
      success: false,
      error: "server_error",
      message: "Failed to load competitive context.",
    });
  }
}

export async function getBrandSources(req, res) {
  try {
    const brandId = String(req.params.brandId || "").trim();
    const ent = await withEntitlements(req);
    const payload = await getBrandSourcesPayload({
      dealalityUser: req.dealalityUser,
      entitlementGraph: ent.entitlementGraph,
      store: getStore(req),
      brandId,
      provider: providerFromQuery(req),
      geography: req.query.geography || req.query.region,
      language: languageFromQuery(req),
    });
    if (payload.ok === false && payload.allowed === false) return deny(res, payload);
    return res.status(200).json({
      success: true,
      ...payload,
      AIRTABLE_WRITES: 0,
      LIVE_PROVIDER_CALLS: 0,
    });
  } catch (err) {
    console.error("[ai-visibility-brand] sources:", err.message);
    return res.status(500).json({
      ok: false,
      success: false,
      error: "server_error",
      message: "Failed to load sources.",
    });
  }
}

export async function getBrandEvidence(req, res) {
  try {
    const brandId = String(req.params.brandId || "").trim();
    const ent = await withEntitlements(req);
    const payload = await getBrandEvidencePayload({
      dealalityUser: req.dealalityUser,
      entitlementGraph: ent.entitlementGraph,
      store: getStore(req),
      brandId,
      evidenceId: req.query.evidenceId || undefined,
      provider: providerFromQuery(req),
      language: languageFromQuery(req),
      geography: req.query.geography || req.query.region,
    });
    if (payload.ok === false && payload.allowed === false) return deny(res, payload);
    return res.status(200).json({
      success: true,
      ...payload,
      AIRTABLE_WRITES: 0,
      LIVE_PROVIDER_CALLS: 0,
    });
  } catch (err) {
    console.error("[ai-visibility-brand] evidence:", err.message);
    return res.status(500).json({
      ok: false,
      success: false,
      error: "server_error",
      message: "Failed to load evidence.",
    });
  }
}

export async function getAiVisibilityBrandBenchmark(req, res) {
  try {
    const brandId = String(req.params.brandId || "").trim();
    const ent = await withEntitlements(req);
    const entitled = ent.entitlementGraph?.entitledBrandIds || [];
    if (!entitled.includes(brandId)) {
      return deny(res, { reasonCode: "SUBJECT_NOT_ENTITLED", allowed: false });
    }

    const result = getBrandBenchmarkPayload({ brandId, internalAdmin: false });
    if (!result.ok) {
      return res.status(result.error === "subject_not_in_pilot" ? 404 : 400).json({
        ok: false,
        success: false,
        error: result.error,
        message: result.error === "subject_not_in_pilot"
          ? "Benchmark pilot data not available for this brand."
          : "Failed to load benchmark.",
      });
    }

    return res.status(200).json({
      success: true,
      ok: true,
      ...result,
      CUSTOMER_ALLOWLIST: [...CUSTOMER_PAYLOAD_ALLOWLIST],
      AIRTABLE_WRITES: 0,
      LIVE_PROVIDER_CALLS: 0,
      PROVIDER_CALLS: 0,
    });
  } catch (err) {
    console.error("[ai-visibility-brand] benchmark:", err.message);
    return res.status(500).json({
      ok: false,
      success: false,
      error: "server_error",
      message: "Failed to load benchmark.",
    });
  }
}

export async function getAiVisibilityBrandBenchmarkDiagnostics(req, res) {
  try {
    const brandId = String(req.params.brandId || "").trim();
    const result = getBrandBenchmarkPayload({ brandId, internalAdmin: true });
    if (!result.ok) {
      return res.status(404).json({
        ok: false,
        success: false,
        error: result.error,
      });
    }
    return res.status(200).json({
      success: true,
      ok: true,
      accessClass: "INTERNAL_ADMIN",
      ...result,
      PROVIDER_CALLS: 0,
    });
  } catch (err) {
    console.error("[ai-visibility-brand] benchmark-diagnostics:", err.message);
    return res.status(500).json({
      ok: false,
      success: false,
      error: "server_error",
      message: "Failed to load benchmark diagnostics.",
    });
  }
}
