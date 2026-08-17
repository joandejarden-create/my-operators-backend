/**
 * Founder/demo Brand portfolio context (showcase companies).
 *
 * Production clients never gain cross-company portfolio switching.
 * Validated header only; no Airtable writes.
 */

import {
  loadShowcaseCompaniesConfig,
  listShowcaseCompanyKeys,
  getShowcaseCompany,
  getShowcasePortfolioBrandIds,
} from "../ai-visibility/brand-ai-showcase-companies.js";
import {
  canUseDemoFounderNavOverrides,
  isDemoStakeholderConstellation,
  MAP_DEMO_STAKEHOLDER_COMPANIES,
} from "./demo-stakeholder-workspace.js";
import { WORKSPACE_BRAND } from "../company-workspace-access.js";

export const DEMO_BRAND_PORTFOLIO_HEADER = "x-dealality-demo-brand-portfolio";
export const DEMO_BRAND_PORTFOLIO_STORAGE_KEY = "dealality_demo_brand_portfolio";
export const DEMO_BRAND_PORTFOLIO_CONTEXT_VERSION = "demo_brand_portfolio_context_v1";

/** Prefer Marriott as default demo portfolio when Brand-Side is active. */
export const DEFAULT_DEMO_BRAND_PORTFOLIO_KEY = "marriott";

function companyIdsOf(user) {
  const ids = [...(user?.companyIds || [])].filter(Boolean);
  if (user?.companyId && !ids.includes(user.companyId)) ids.unshift(user.companyId);
  return ids;
}

/**
 * Who may use the demo Brand Portfolio switcher (UI + header honor).
 * Demo constellation or founder/admin nav overrides — never production Brand clients alone.
 */
export function canUseDemoBrandPortfolioSwitch(user) {
  if (!user) return false;
  if (canUseDemoFounderNavOverrides(user)) return true;
  if (isDemoStakeholderConstellation(user)) return true;
  if (user.isDemo || user.canAccessDemoWorkspace || user.flags?.isDemo) {
    const ids = new Set(companyIdsOf(user));
    const brandDemoId = MAP_DEMO_STAKEHOLDER_COMPANIES.Brand.companyId;
    if (ids.has(brandDemoId)) return true;
  }
  return false;
}

/**
 * Active context where portfolio override is meaningful (Brand-Side or admin preview).
 */
export function isDemoBrandPortfolioContextActive(user) {
  if (!canUseDemoBrandPortfolioSwitch(user)) return false;
  const active = String(user.activeWorkspace || "").trim();
  if (active === WORKSPACE_BRAND || active === "Brand") return true;
  // Admin / All Workspaces nav preview while Brand feature access is on
  if (user.isAdmin || user.flags?.isAdmin) {
    return !!(user.canAccessBrandWorkspace || user.isBrand);
  }
  return false;
}

/**
 * @param {string|null|undefined} raw
 * @returns {string|null} normalized companyKey or null
 */
export function normalizeDemoBrandPortfolioKey(raw) {
  const key = String(raw || "")
    .trim()
    .toLowerCase();
  if (!key) return null;
  const allowed = new Set(listShowcaseCompanyKeys());
  if (!allowed.has(key)) return null;
  return key;
}

/**
 * Read header from request (no query-param impersonation).
 */
export function readDemoBrandPortfolioHeader(req) {
  const headers = req?.headers || {};
  const raw =
    headers[DEMO_BRAND_PORTFOLIO_HEADER] ||
    headers["X-Dealality-Demo-Brand-Portfolio"] ||
    headers["x-dealality-demo-brand-portfolio"];
  const value = Array.isArray(raw) ? raw[0] : raw;
  return normalizeDemoBrandPortfolioKey(value);
}

/**
 * List portfolios for /api/me + UI (governed showcase only).
 */
export function listDemoBrandPortfolioOptions() {
  const cfg = loadShowcaseCompaniesConfig();
  return (cfg.companies || []).map((c) => ({
    companyKey: c.companyKey,
    label: c.canonicalCompanyName || c.companyKey,
    showcasePortfolioId: c.showcasePortfolioId || null,
    brandCount: Array.isArray(c.brandIds) ? c.brandIds.length : 0,
    brandIds: [...(c.brandIds || [])],
    brands: (c.brands || []).map((b) => ({
      brandId: b.brandId,
      brandName: b.brandName,
    })),
  }));
}

/**
 * Resolve governed showcase portfolio for a validated key.
 * @param {string} companyKey
 */
export function resolveDemoBrandPortfolio(companyKey) {
  const key = normalizeDemoBrandPortfolioKey(companyKey);
  if (!key) {
    return {
      ok: false,
      reasonCode: "UNKNOWN_DEMO_BRAND_PORTFOLIO",
      companyKey: companyKey || null,
    };
  }
  const company = getShowcaseCompany(key);
  if (!company.ok) {
    return {
      ok: false,
      reasonCode: "UNKNOWN_DEMO_BRAND_PORTFOLIO",
      companyKey: key,
    };
  }
  const ids = getShowcasePortfolioBrandIds(key);
  if (!ids.ok) {
    return {
      ok: false,
      reasonCode: "UNKNOWN_DEMO_BRAND_PORTFOLIO",
      companyKey: key,
    };
  }
  const brandIds = [...(ids.brandIds || [])].filter(
    (id) => typeof id === "string" && id.startsWith("rec")
  );
  if (!brandIds.length) {
    return {
      ok: false,
      reasonCode: "EMPTY_DEMO_BRAND_PORTFOLIO",
      companyKey: key,
    };
  }
  const brandNamesById = {};
  for (const b of company.brands || []) {
    if (b.brandId) brandNamesById[b.brandId] = b.brandName || null;
  }
  return {
    ok: true,
    companyKey: key,
    canonicalCompanyName: company.canonicalCompanyName,
    showcasePortfolioId: company.showcasePortfolioId || null,
    brandIds,
    brandNamesById,
    brands: (company.brands || []).map((b) => ({
      brandId: b.brandId,
      brandName: b.brandName,
    })),
    AUTHORIZATION_BYPASS: false,
    AIRTABLE_WRITES: 0,
    source: "showcase_config",
    configId: company.configId,
    configVersion: company.configVersion,
  };
}

/**
 * Apply demo portfolio onto dealalityUser when header is valid + context active.
 * Invalid keys / unauthorized requesters → no mutation (ignore).
 *
 * @param {object} user
 * @param {string|null|undefined} requestedKey
 * @returns {object} user
 */
export function applyDemoBrandPortfolioContext(user, requestedKey) {
  if (!user || typeof user !== "object") return user;

  user.demoBrandPortfolioOptions = listDemoBrandPortfolioOptions();
  user.demoBrandPortfolioSwitchAvailable = canUseDemoBrandPortfolioSwitch(user);

  const rawKey = String(requestedKey || "").trim();
  let key = normalizeDemoBrandPortfolioKey(requestedKey);
  /**
   * Founder/demo Brand-Side coherence: when the showcase switch is authorized and
   * Brand workspace is active, *omitted* header defaults to Marriott (nav + API match).
   * Explicit unknown keys still reject. Production Brand clients never enter this path.
   */
  let usedDefault = false;
  if (
    !key &&
    !rawKey &&
    canUseDemoBrandPortfolioSwitch(user) &&
    isDemoBrandPortfolioContextActive(user)
  ) {
    key = DEFAULT_DEMO_BRAND_PORTFOLIO_KEY;
    usedDefault = true;
  }
  if (!key) {
    user.demoBrandPortfolioKey = null;
    user.demoBrandPortfolio = null;
    if (rawKey) {
      user.demoBrandPortfolioRejected = "UNKNOWN_DEMO_BRAND_PORTFOLIO";
    }
    return user;
  }

  if (!canUseDemoBrandPortfolioSwitch(user)) {
    user.demoBrandPortfolioKey = null;
    user.demoBrandPortfolio = null;
    user.demoBrandPortfolioRejected = "NOT_AUTHORIZED_FOR_DEMO_PORTFOLIO";
    return user;
  }

  if (!isDemoBrandPortfolioContextActive(user)) {
    // Header present but Owner/Operator context — ignore (no escalation).
    user.demoBrandPortfolioKey = null;
    user.demoBrandPortfolio = null;
    user.demoBrandPortfolioIgnored = "WORKSPACE_NOT_BRAND_CONTEXT";
    return user;
  }

  const resolved = resolveDemoBrandPortfolio(key);
  if (!resolved.ok) {
    user.demoBrandPortfolioKey = null;
    user.demoBrandPortfolio = null;
    user.demoBrandPortfolioRejected = resolved.reasonCode;
    return user;
  }

  user.demoBrandPortfolioKey = resolved.companyKey;
  user.demoBrandPortfolio = resolved;
  if (usedDefault) {
    user.demoBrandPortfolioDefaulted = DEFAULT_DEMO_BRAND_PORTFOLIO_KEY;
  }
  return user;
}

/**
 * Build entitlement override for Brand AI Visibility reads.
 * @param {object} dealalityUser
 * @returns {null|{ entitledBrandIds: string[], brandNamesById: object, source: string, demoBrandPortfolioKey: string }}
 */
export function demoBrandPortfolioEntitlementOverride(dealalityUser) {
  const portfolio = dealalityUser?.demoBrandPortfolio;
  if (!portfolio?.ok || !portfolio.brandIds?.length) return null;
  if (!canUseDemoBrandPortfolioSwitch(dealalityUser)) return null;
  if (!isDemoBrandPortfolioContextActive(dealalityUser)) return null;
  return {
    entitledBrandIds: [...portfolio.brandIds],
    brandNamesById: { ...(portfolio.brandNamesById || {}) },
    source: "demo_showcase_portfolio",
    demoBrandPortfolioKey: portfolio.companyKey,
    canonicalCompanyName: portfolio.canonicalCompanyName,
    AIRTABLE_WRITES: 0,
    AUTHORIZATION_BYPASS: false,
  };
}
