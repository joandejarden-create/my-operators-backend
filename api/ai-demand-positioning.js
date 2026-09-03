/**
 * AI Demand Positioning — API Routes.
 * Owner-facing endpoints for property AI demand intelligence.
 * Production reads pre-computed published snapshots (file or Airtable).
 */

import { loadPropertyProfile, listPropertyProfiles } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { estimateCost } from "../lib/ai-demand-positioning/execution/multi-provider-runner.js";
import {
  getPublishedOwnerReport,
  getPublishedEvidenceResponse,
  getAdpPublishedReadSourceStatus,
} from "../lib/ai-demand-positioning/published-read-service.js";
import { buildBrandPortfolioPositionPayload } from "../lib/ai-demand-positioning/brand-portfolio/build-brand-portfolio-position-payload-v1.js";
import {
  BPP_CUSTOMER_PUBLICATION_VERSION,
  BPP_ASSET_CACHE_TOKEN,
  BPP_KPI_CONTRACT_VERSION,
  BPP_METRICS_VERSION,
  BPP_CUSTOMER_PUBLISHED_PACK,
  BPP_CUSTOMER_PUBLISHED_PACK_CANDIDATES,
  resolveCanonicalBppPropertyId,
  buildBppReportCacheKey,
} from "../lib/ai-demand-positioning/brand-portfolio/bpp-publication-meta-v1.js";
import { filterPropertiesForOwnerApp } from "../lib/ai-demand-positioning/share/adp-owner-app-property-access-v1.js";
import { attachBppRowLevelPriorComparisons } from "../lib/ai-demand-positioning/longitudinal/attach-row-level-prior-comparisons-v1.js";
import fs from "fs";
import path from "path";

/** @deprecated use BPP_CUSTOMER_PUBLICATION_VERSION */
export const BPP_PUBLICATION_VERSION = BPP_CUSTOMER_PUBLICATION_VERSION;

function setAdpNoStoreHeaders(res) {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, private");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
  res.setHeader("Vary", "Authorization, Cookie");
}

function readCustomerPublishedPack() {
  for (const rel of BPP_CUSTOMER_PUBLISHED_PACK_CANDIDATES) {
    const p = path.join(process.cwd(), rel);
    if (!fs.existsSync(p)) continue;
    try {
      const pack = JSON.parse(fs.readFileSync(p, "utf8"));
      if (!pack?.customerPublished) continue;
      return { ...pack, _bppPackPath: rel };
    } catch (err) {
      if (process.env.NODE_ENV !== "production") {
        console.error(`[ADP BPP] customer published pack read failed (${rel}):`, err.message);
      }
    }
  }
  void BPP_CUSTOMER_PUBLISHED_PACK;
  return null;
}

/**
 * PUBLISHED_BPP_PAYLOAD_PRECEDENCE_INTEGRITY
 * certified customer-published > controlled local candidate > derived awaiting
 */
export function loadCustomerPublishedBrandPortfolio(propertyId) {
  const pack = readCustomerPublishedPack();
  if (!pack) return null;
  const canonicalId = resolveCanonicalBppPropertyId(propertyId);
  const payload =
    pack.payloads?.[canonicalId] ||
    pack.payloads?.[propertyId] ||
    null;
  if (!payload) return null;
  const publicationVersion = pack.publicationVersion || BPP_CUSTOMER_PUBLICATION_VERSION;
  const withRowMovement = attachBppRowLevelPriorComparisons(canonicalId || propertyId, payload);
  return {
    ...withRowMovement,
    publicationVersion,
    customerPublished: true,
    status: withRowMovement.status || payload.status || "READY",
    assuranceStatus: withRowMovement.assuranceStatus || payload.assuranceStatus || "CUSTOMER_READY",
    showAnalyticalScaffolding: withRowMovement.showAnalyticalScaffolding !== false,
    _bppPackVersion: publicationVersion,
    _bppPayloadHash: pack.payloadHash || null,
    _bppResolvedPropertyId: canonicalId,
    _bppAssetCacheToken: BPP_ASSET_CACHE_TOKEN,
    measurement: {
      ...(payload.measurement || {}),
      publicationVersion,
      kpiContractVersion: BPP_KPI_CONTRACT_VERSION,
      metricsVersion: BPP_METRICS_VERSION,
      customerPublished: true,
    },
  };
}

function internalBppPreviewAllowed() {
  return (
    process.env.ADP_BRAND_PORTFOLIO_ALLOW_INTERNAL_PREVIEW === "1" ||
    process.env.ADP_BRAND_PORTFOLIO_ALLOW_INTERNAL_PREVIEW === "true"
  );
}

function loadLocalBrandPortfolioReady(propertyId) {
  // Env gate only — customers cannot enable via query string.
  const enabled =
    process.env.ADP_BRAND_PORTFOLIO_LOCAL_READY === "1" ||
    process.env.ADP_BRAND_PORTFOLIO_LOCAL_READY === "true";
  if (!enabled || !internalBppPreviewAllowed()) return null;
  try {
    const p = path.join(
      process.cwd(),
      "reports/ai-demand-positioning/ADP_BRAND_PORTFOLIO_LOCAL_READY_PAYLOADS_V1.json"
    );
    if (!fs.existsSync(p)) return null;
    const pack = JSON.parse(fs.readFileSync(p, "utf8"));
    const canonicalId = resolveCanonicalBppPropertyId(propertyId);
    return pack?.payloads?.[canonicalId] || pack?.payloads?.[propertyId] || null;
  } catch {
    return null;
  }
}

/** Founder/internal Period-2 local preview only — never customer-activatable. */
function loadPeriod2LocalBrandPortfolio(propertyId) {
  if (!internalBppPreviewAllowed()) return null;
  const enabled =
    process.env.ADP_BRAND_PORTFOLIO_PERIOD_2_LOCAL === "1" ||
    process.env.ADP_BRAND_PORTFOLIO_PERIOD_2_LOCAL === "true";
  if (!enabled) return null;
  try {
    const p = path.join(
      process.cwd(),
      "reports/ai-demand-positioning/ADP_BRAND_PORTFOLIO_PERIOD_2_LOCAL_READY_PAYLOADS_V1.json"
    );
    if (!fs.existsSync(p)) return null;
    const pack = JSON.parse(fs.readFileSync(p, "utf8"));
    if (pack?.customerPublished === true) return null;
    const canonicalId = resolveCanonicalBppPropertyId(propertyId);
    const payload = pack?.payloads?.[canonicalId] || pack?.payloads?.[propertyId] || null;
    if (!payload) return null;
    return {
      ...payload,
      customerPublished: false,
      controlledPeriod2LocalPreview: true,
      periodId: pack.periodId || payload.periodId || null,
      priorPeriodId: pack.priorPeriodId || payload.priorPeriodId || null,
    };
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.error("[ADP BPP] Period-2 local pack read failed:", err.message);
    }
    return null;
  }
}

export function resolveBrandPortfolioPosition(propertyId, profile, req) {
  // Published customer pack always wins for normal share/current URLs.
  // Internal preview query flags are ignored unless founder enables
  // ADP_BRAND_PORTFOLIO_ALLOW_INTERNAL_PREVIEW=1 (CUSTOMER_CANNOT_ENABLE_INTERNAL_REPORT_STATE).
  const allowInternal = internalBppPreviewAllowed();
  if (allowInternal && req?.query?.bppPeriod2Local === "1") {
    const period2 = loadPeriod2LocalBrandPortfolio(propertyId);
    if (period2) return period2;
  }
  const published = loadCustomerPublishedBrandPortfolio(propertyId);
  if (published) {
    return published;
  }
  if (allowInternal && req?.query?.bppLocalReady === "1") {
    const local = loadLocalBrandPortfolioReady(propertyId);
    if (local) return local;
  }
  const localEnv = loadLocalBrandPortfolioReady(propertyId);
  if (localEnv) return localEnv;
  return buildBrandPortfolioPositionPayload(profile);
}

export function getAiDemandPositioningPublicationMeta(req, res) {
  setAdpNoStoreHeaders(res);
  const pack = readCustomerPublishedPack();
  let propertyIds = pack?.payloads ? Object.keys(pack.payloads) : [];
  if (req.adpShare?.propertyId) {
    propertyIds = propertyIds.filter((id) => id === req.adpShare.propertyId);
  }
  return res.json({
    ok: true,
    product: "ai_demand_positioning",
    bpp: {
      customerPublished: !!pack?.customerPublished,
      publicationVersion: pack?.publicationVersion || BPP_CUSTOMER_PUBLICATION_VERSION,
      payloadHash: pack?.payloadHash || null,
      assetCacheToken: BPP_ASSET_CACHE_TOKEN,
      kpiContractVersion: BPP_KPI_CONTRACT_VERSION,
      metricsVersion: BPP_METRICS_VERSION,
      propertyIds,
      propertyCount: propertyIds.length,
    },
    reportCachePolicy: "no-store",
    gateHints: [
      "CUSTOMER_REPORT_PUBLICATION_CACHE_INVALIDATION_INTEGRITY",
      "PUBLISHED_BPP_PAYLOAD_PRECEDENCE_INTEGRITY",
      "BPP_PUBLISHED_PROPERTY_ID_RESOLUTION_INTEGRITY",
      "CUSTOMER_PUBLISHED_READY_STATE_DELIVERY_INTEGRITY",
      "SIGNED_SHARE_CAPABILITY_AUTHORIZATION",
    ],
  });
}

export function getAiDemandPositioningProperties(req, res) {
  setAdpNoStoreHeaders(res);
  let properties = listPropertyProfiles();
  if (req.adpShare?.propertyId) {
    properties = properties.filter((p) => p.propertyId === req.adpShare.propertyId);
  } else if (req.adpShareAuth?.mode === "MEMBERSTACK") {
    properties = filterPropertiesForOwnerApp(req, properties);
  }
  res.json({ ok: true, properties });
}

/** ADP read health — auth-gated; internal config only for Memberstack sessions. */
export function getAiDemandPositioningReadHealth(req, res) {
  setAdpNoStoreHeaders(res);
  const authMode = req.adpShareAuth?.mode;
  if (authMode !== "MEMBERSTACK") {
    return res.json({ ok: true, product: "ai_demand_positioning", healthy: true });
  }
  const status = getAdpPublishedReadSourceStatus();
  return res.json({
    ok: true,
    product: "ai_demand_positioning",
    ...status,
  });
}

export async function getAiDemandPositioningReport(req, res) {
  try {
    setAdpNoStoreHeaders(res);
    const rawId = req.params.propertyId;
    const propertyId = resolveCanonicalBppPropertyId(rawId) || rawId;
    const profile = loadPropertyProfile(propertyId);
    if (!profile) {
      return res.status(404).json({ ok: false, error: "property_not_found" });
    }

    const result = await getPublishedOwnerReport(propertyId);
    if (!result.ok) {
      const status = result.error === "property_not_found" ? 404 : 404;
      return res.status(status).json(result);
    }

    const brandPortfolioPosition = resolveBrandPortfolioPosition(propertyId, profile, req);
    // Strip any stale BPP embedded in Core published snapshot before overlay
    const { brandPortfolioPosition: _staleBpp, ...corePayload } = result.payload || {};

    const publicationVersion =
      brandPortfolioPosition?.publicationVersion ||
      brandPortfolioPosition?.measurement?.publicationVersion ||
      BPP_CUSTOMER_PUBLICATION_VERSION;
    const payloadHash = brandPortfolioPosition?._bppPayloadHash || null;
    const reportCacheKey = buildBppReportCacheKey({
      propertyId,
      publicationVersion,
      payloadHash,
    });

    const payload = {
      ...corePayload,
      ok: true,
      propertyId,
      property: {
        ...(corePayload.property || {}),
        propertyId,
      },
      // PUBLISHED_BPP_PAYLOAD_PRECEDENCE — always last write wins
      brandPortfolioPosition,
      _adpReadSource: result.readSource || {
        requested: result.source === "airtable" ? "airtable" : "filesystem",
        active: result.source === "airtable" ? "airtable" : "filesystem",
      },
      _bppPublicationVersion: publicationVersion,
      _bppAssetCacheToken: BPP_ASSET_CACHE_TOKEN,
      _bppPayloadHash: payloadHash,
      _reportCacheKey: reportCacheKey,
      _reportCachePolicy: "no-store",
      _requestedPropertyId: rawId,
      _resolvedPropertyId: propertyId,
    };

    res.setHeader("X-ADP-BPP-Publication-Version", publicationVersion);
    res.setHeader("X-ADP-Report-Cache-Key", reportCacheKey);
    return res.json(payload);
  } catch (err) {
    console.error("[AI Demand Positioning] report error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

export async function getAiDemandPositioningEvidence(req, res) {
  try {
    setAdpNoStoreHeaders(res);
    const propertyId = resolveCanonicalBppPropertyId(req.params.propertyId) || req.params.propertyId;
    const { intent, type, mode, competitor, competitorId, scope, provider, limit, offset } = req.query;
    const profile = loadPropertyProfile(propertyId);
    if (!profile) return res.status(404).json({ ok: false, error: "property_not_found" });

    const result = await getPublishedEvidenceResponse(propertyId, {
      intent,
      type: type || (mode === "positive" ? "present" : mode === "missing" ? "missing" : type),
      mode,
      competitor,
      competitorId,
      scope,
      provider,
      limit,
      offset,
    });
    if (!result.ok) {
      return res.status(404).json(result);
    }

    return res.json(result);
  } catch (err) {
    console.error("[AI Demand Positioning] evidence error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

export function getAiDemandPositioningCostEstimate(req, res) {
  try {
    setAdpNoStoreHeaders(res);
    const propertyId = resolveCanonicalBppPropertyId(req.params.propertyId) || req.params.propertyId;
    const profile = loadPropertyProfile(propertyId);
    if (!profile) return res.status(404).json({ ok: false, error: "property_not_found" });

    const scenarios = buildScenarioUniverse(profile);
    const estimate = estimateCost(scenarios.length);
    return res.json({ ok: true, propertyId, ...estimate });
  } catch (err) {
    return res.status(500).json({ ok: false, error: "internal_error" });
  }
}
