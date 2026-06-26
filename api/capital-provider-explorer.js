/**
 * Capital Provider Explorer API — live Airtable (Capital Setup tables).
 *
 * GET /api/capital-provider-explorer
 * GET /api/capital-provider-explorer?id=<recordId>
 *
 * Legacy (existing UI):
 * GET /api/capital-provider-explorer/providers
 * GET /api/capital-provider-explorer/provider?id=
 */

import {
  loadProviderList,
  loadProviderDetail,
  toLegacyListItem,
  toLegacyDetailProfile,
  collectFilterOptionsFromCards,
} from "./lib/capital-provider-explorer-airtable.js";

function parseBearerIsAdmin(req) {
  const u = req.dealalityUser;
  return !!(u && u.isAdmin);
}

function parseBearerIsInternal(req) {
  const u = req.dealalityUser;
  if (!u) return false;
  if (u.isAdmin) return true;
  if (u.isDemo) return true;
  return false;
}

function listQueryFromReq(req) {
  const q = req.query || {};
  return {
    institutionType: q.institutionType,
    region: q.region,
    geography: q.geography,
    loanProduct: q.loanProduct,
    dealType: q.dealType,
    assetType: q.assetType,
    projectStage: q.projectStage,
    brandPreference: q.brandPreference,
    operatorPreference: q.operatorPreference,
    contactPathway: q.contactPathway,
    sourceConfidence: q.sourceConfidence,
    search: q.search,
  };
}

function logDev(event, payload) {
  if (process.env.NODE_ENV === "production") return;
  console.info(JSON.stringify({ scope: "capital_provider_explorer", event, ...payload }));
}

function airtableUnavailable(res) {
  return res.status(503).json({
    ok: false,
    error: "capital_provider_airtable_unavailable",
    message: "Capital provider data is temporarily unavailable.",
  });
}

function handleAirtableError(res, err, context) {
  console.error(`[capital-provider-explorer] ${context} failed:`, err);
  if (err.code === "capital_provider_airtable_unavailable") {
    return airtableUnavailable(res);
  }
  return res.status(500).json({
    ok: false,
    error: "capital_provider_load_failed",
    message: "Capital provider data is temporarily unavailable.",
  });
}

/** Unified endpoint — list or detail based on ?id= */
export async function handleCapitalProviderExplorer(req, res) {
  const providerId = String(req.query.id || req.query.providerId || "").trim();
  if (providerId) {
    return getCapitalProviderDetailApi(req, res);
  }
  return listCapitalProvidersApi(req, res);
}

/** GET /api/capital-provider-explorer (list) */
export async function listCapitalProvidersApi(req, res) {
  try {
    const isAdmin = parseBearerIsAdmin(req);
    const isInternal = parseBearerIsInternal(req);
    const providers = await loadProviderList({
      isAdmin,
      isInternal,
      query: listQueryFromReq(req),
    });

    logDev("list", { count: providers.length, isAdmin, isInternal });

    return res.json({
      ok: true,
      source: "airtable",
      count: providers.length,
      providers,
    });
  } catch (err) {
    return handleAirtableError(res, err, "list");
  }
}

/** GET /api/capital-provider-explorer?id= (detail) */
export async function getCapitalProviderDetailApi(req, res) {
  try {
    const id = String(req.query.id || req.query.providerId || req.params?.id || "").trim();
    if (!id) {
      return res.status(400).json({
        ok: false,
        error: "missing_provider_id",
        message: "Capital provider ID required.",
      });
    }

    const isAdmin = parseBearerIsAdmin(req);
    const isInternal = parseBearerIsInternal(req);
    const detail = await loadProviderDetail(id, { isAdmin, isInternal });

    if (!detail) {
      return res.status(404).json({
        ok: false,
        error: "not_found",
        message: "Capital provider not found.",
      });
    }

    logDev("detail", { id, isAdmin, isInternal });

    return res.json({
      ok: true,
      source: "airtable",
      provider: detail.provider,
      criteria: detail.criteria,
      requiredDocuments: detail.requiredDocuments,
      sourceReferences: detail.sourceReferences,
    });
  } catch (err) {
    return handleAirtableError(res, err, "detail");
  }
}

/** Legacy list — maps to existing Explorer UI shape */
export async function listCapitalProviders(req, res) {
  try {
    const isAdmin = parseBearerIsAdmin(req);
    const isInternal = parseBearerIsInternal(req);
    const cards = await loadProviderList({ isAdmin, isInternal, query: {} });
    const providers = cards.map(toLegacyListItem);

    logDev("list_legacy", { count: providers.length, isAdmin });

    return res.json({
      success: true,
      providers,
      filterOptions: collectFilterOptionsFromCards(cards),
      meta: {
        dataSource: "airtable",
        total: providers.length,
        isAdmin,
      },
    });
  } catch (err) {
    console.error("[capital-provider-explorer] list legacy failed:", err);
    if (err.code === "capital_provider_airtable_unavailable") {
      return res.status(503).json({
        success: false,
        error: "capital_provider_airtable_unavailable",
        message: "Capital provider data is temporarily unavailable.",
      });
    }
    return res.status(500).json({
      success: false,
      error: "Failed to load capital providers",
      message: "Capital provider data is temporarily unavailable.",
    });
  }
}

/** Legacy detail — maps to existing Explorer UI shape */
export async function getCapitalProviderById(req, res) {
  try {
    const id = String(req.query.id || req.query.providerId || req.params?.id || "").trim();
    if (!id) {
      return res.status(400).json({ success: false, error: "Capital provider ID required" });
    }

    const isAdmin = parseBearerIsAdmin(req);
    const isInternal = parseBearerIsInternal(req);
    const detail = await loadProviderDetail(id, { isAdmin, isInternal });

    if (!detail) {
      return res.status(404).json({ success: false, error: "Capital provider not found" });
    }

    const provider = toLegacyDetailProfile(detail);
    const payload = {
      success: true,
      provider,
      meta: {
        dataSource: "airtable",
        visibility: provider.visibility,
        canViewInternal: isInternal,
      },
    };

    logDev("detail_legacy", { id, isInternal });

    return res.json(payload);
  } catch (err) {
    console.error("[capital-provider-explorer] detail legacy failed:", err);
    if (err.code === "capital_provider_airtable_unavailable") {
      return res.status(503).json({
        success: false,
        error: "capital_provider_airtable_unavailable",
        message: "Capital provider data is temporarily unavailable.",
      });
    }
    return res.status(500).json({
      success: false,
      error: "Failed to load capital provider",
      message: "Capital provider data is temporarily unavailable.",
    });
  }
}
