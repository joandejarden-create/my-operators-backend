/**
 * Demo Dealality stakeholder workspace switching.
 *
 * Extends the shell workspace switcher for the governed demo constellation:
 *   Dealality Owner Demo + Dealality Brand Demo (+ Operator WA on Owner Demo)
 *
 * Rules:
 * - Changes authenticated demo *context* (active workspace + company subject).
 * - Does not grant arbitrary company/brand IDs.
 * - Does not elevate production Owner/Operator write gates beyond Airtable truth.
 * - Brand AI Visibility still uses resolveEntitledBrands on the active company.
 * - Non-demo production users are unchanged.
 */

import {
  DEMO_PREVIEW_WORKSPACES,
  WORKSPACE_BRAND,
  WORKSPACE_OPERATOR,
  WORKSPACE_OWNER,
} from "../company-workspace-access.js";
import { resolveWorkspaceOptions } from "./resolve-workspace-options.js";

export const DEMO_ACTIVE_WORKSPACE_HEADER = "x-dealality-active-workspace";

/** Governed demo Company Profile record IDs (overridable via env). */
export const MAP_DEMO_STAKEHOLDER_COMPANIES = Object.freeze({
  Owner: {
    workspace: WORKSPACE_OWNER,
    companyId:
      process.env.DEALALITY_DEMO_OWNER_COMPANY_ID || "recr0XXnseXNlIlxk",
    companyName: "Dealality Owner Demo",
  },
  Operator: {
    workspace: WORKSPACE_OPERATOR,
    // Operator WA currently lives on Owner Demo until a dedicated Operator Demo CP exists.
    companyId:
      process.env.DEALALITY_DEMO_OPERATOR_COMPANY_ID ||
      process.env.DEALALITY_DEMO_OWNER_COMPANY_ID ||
      "recr0XXnseXNlIlxk",
    companyName: "Dealality Owner Demo",
  },
  Brand: {
    workspace: WORKSPACE_BRAND,
    companyId:
      process.env.DEALALITY_DEMO_BRAND_COMPANY_ID || "reciQEtqmxz6ZroVc",
    companyName: "Dealality Brand Demo",
  },
});

/**
 * Governed Brand Basics portfolio for Dealality Brand Demo (Phase 3A.2 validation set).
 * All IDs are Active Brand Basics in the upper-upscale peer monitoring universe.
 * Demo-only — does not imply client ownership.
 */
export const DEMO_BRAND_PORTFOLIO = Object.freeze([
  {
    brandId: "reclkgOzvAcBheUSo",
    brandName: "Ascend Hotel Collection",
    note: "Choice Active — Dealality Brand Demo portfolio (Phase 3A.2)",
  },
  {
    brandId: "recEJCTDj1zrsjPM6",
    brandName: "Autograph Collection",
    note: "Marriott Active — Dealality Brand Demo portfolio (Phase 3A.2)",
  },
  {
    brandId: "receQkxgjlezsc1xg",
    brandName: "Curio Collection by Hilton",
    note: "Hilton Active — Dealality Brand Demo portfolio (Phase 3A.2)",
  },
  {
    brandId: "recCvV0PuZOi8c3hC",
    brandName: "Tribute Portfolio",
    note: "Marriott Active — Dealality Brand Demo portfolio (Phase 3A.2)",
  },
  {
    brandId: "recegXrqaPiSLGCIe",
    brandName: "Hotel Indigo",
    note: "IHG Active — Dealality Brand Demo portfolio (Phase 3A.2)",
  },
  {
    brandId: "rec02zPClpWUTCyXM",
    brandName: "Design Hotels",
    note: "Marriott Active — Dealality Brand Demo portfolio (Phase 3A.2)",
  },
  {
    brandId: "recIPuBC50fv13zRR",
    brandName: "Westin",
    note: "Marriott Active — Dealality Brand Demo portfolio (Phase 3A.2)",
  },
]);

/** Prior Phase 3A demo Choice-only set (kept for rollback documentation). */
export const DEMO_BRAND_PORTFOLIO_PHASE3A_PRIOR = Object.freeze([
  { brandId: "reclkgOzvAcBheUSo", brandName: "Ascend Hotel Collection" },
  { brandId: "recOzH5iAE1xEjyD0", brandName: "Comfort Inn & Suites" },
  { brandId: "recmKqo7M7mLZgRqQ", brandName: "Radisson RED by Choice" },
]);

const ALLOWED_ACTIVE = new Set(DEMO_PREVIEW_WORKSPACES);

function normalizeWorkspace(raw) {
  const s = String(raw || "").trim();
  if (ALLOWED_ACTIVE.has(s)) return s;
  const lower = s.toLowerCase();
  if (lower === "owner") return WORKSPACE_OWNER;
  if (lower === "operator") return WORKSPACE_OPERATOR;
  if (lower === "brand") return WORKSPACE_BRAND;
  return "";
}

function companyIdsOf(user) {
  const ids = [...(user?.companyIds || [])].filter(Boolean);
  if (user?.companyId && !ids.includes(user.companyId)) ids.unshift(user.companyId);
  return ids;
}

/**
 * @param {object|null|undefined} user
 * @returns {boolean}
 */
export function isDemoStakeholderConstellation(user) {
  if (!user) return false;
  if (user.isDemo || user.canAccessDemoWorkspace || user.flags?.isDemo) return true;
  const ids = new Set(companyIdsOf(user));
  const ownerId = MAP_DEMO_STAKEHOLDER_COMPANIES.Owner.companyId;
  const brandId = MAP_DEMO_STAKEHOLDER_COMPANIES.Brand.companyId;
  return ids.has(ownerId) && ids.has(brandId);
}

/**
 * Workspaces the demo account may intentionally switch among.
 *
 * Always returns the full governed demo preview set (Owner, Operator, Brand)
 * for demo/constellation users. Company links select the *subject* company
 * when switching; they must not shrink the switcher to Owner+Operator only
 * when Brand Demo is temporarily unlinked or isDemo short-circuits constellation.
 *
 * @param {object|null|undefined} user
 * @returns {string[]}
 */
export function getDemoStakeholderWorkspaces(user) {
  if (!user) return [];
  const isDemoUser = !!(
    user.isDemo ||
    user.canAccessDemoWorkspace ||
    user.flags?.isDemo
  );
  if (!isDemoStakeholderConstellation(user) && !isDemoUser) {
    // Founder/admin nav QA uses the same governed preview workspaces.
    // Does not elevate production clients (canUseDemoFounderNavOverrides is false for them).
    if (canUseDemoFounderNavOverrides(user)) {
      return [...DEMO_PREVIEW_WORKSPACES];
    }
    return [];
  }
  // Full preview constellation — do not omit Brand when company link is partial.
  return [...DEMO_PREVIEW_WORKSPACES];
}

/**
 * Whether Admin / All Workspaces nav overrides are available for founder QA.
 * Nav preview only — does not elevate production write gates or entitlements.
 * Limited to: platform admin, or Owner+Brand demo constellation (not every Demo row).
 * @param {object|null|undefined} user
 */
export function canUseDemoFounderNavOverrides(user) {
  if (!user) return false;
  if (user.isAdmin || user.flags?.isAdmin) return true;
  const ids = new Set(companyIdsOf(user));
  const ownerId = MAP_DEMO_STAKEHOLDER_COMPANIES.Owner.companyId;
  const brandId = MAP_DEMO_STAKEHOLDER_COMPANIES.Brand.companyId;
  return ids.has(ownerId) && ids.has(brandId);
}

/**
 * Read active workspace from request headers (no query-param impersonation).
 * @param {import('express').Request | { headers?: Record<string, string|string[]> }} req
 */
export function readActiveWorkspaceHeader(req) {
  const headers = req?.headers || {};
  const raw =
    headers[DEMO_ACTIVE_WORKSPACE_HEADER] ||
    headers["X-Dealality-Active-Workspace"] ||
    headers["x-dealality-active-workspace"];
  const value = Array.isArray(raw) ? raw[0] : raw;
  return normalizeWorkspace(value);
}

/**
 * Apply demo stakeholder active workspace onto dealalityUser (mutates + returns).
 * Production non-constellation users are returned unchanged.
 *
 * @param {object} user
 * @param {string} requestedWorkspace
 * @returns {object}
 */
export function applyDemoStakeholderActiveWorkspace(user, requestedWorkspace) {
  if (!user || typeof user !== "object") return user;

  const allowed = getDemoStakeholderWorkspaces(user);
  if (!allowed.length) {
    return user;
  }

  // Snapshot Airtable/production gates once (never overwrite with demo-elevated values).
  if (!user._demoStakeholderSnapshot) {
    user._demoStakeholderSnapshot = {
      companyId: user.companyId || null,
      companyName: user.companyName || null,
      canAccessOwnerWorkspace: !!user.canAccessOwnerWorkspace,
      canAccessOperatorWorkspace: !!user.canAccessOperatorWorkspace,
      canAccessBrandWorkspace: !!user.canAccessBrandWorkspace,
      isOwner: !!user.isOwner,
      isOperator: !!user.isOperator,
      isBrand: !!user.isBrand,
      workspaceAccess: Array.isArray(user.workspaceAccess)
        ? [...user.workspaceAccess]
        : [],
    };
  }
  const snap = user._demoStakeholderSnapshot;

  let active = normalizeWorkspace(requestedWorkspace);
  if (!active || !allowed.includes(active)) {
    active = allowed.includes(WORKSPACE_OWNER)
      ? WORKSPACE_OWNER
      : allowed[0];
  }

  const map = MAP_DEMO_STAKEHOLDER_COMPANIES[active];
  const ids = new Set(companyIdsOf(user));

  // Restore Airtable production gates — never elevate Owner/Operator write access.
  user.canAccessOwnerWorkspace = snap.canAccessOwnerWorkspace;
  user.canAccessOperatorWorkspace = snap.canAccessOperatorWorkspace;
  user.canAccessBrandWorkspace = snap.canAccessBrandWorkspace;
  user.isOwner = snap.isOwner;
  user.isOperator = snap.isOperator;
  user.isBrand = snap.isBrand;
  user.workspaceAccess = [...snap.workspaceAccess];

  user.activeWorkspace = active;
  user.demoStakeholderMode = true;
  user.demoStakeholderWorkspaces = allowed;
  user.isDemoStakeholder = true;

  // Demo Mode badge / preview list (does not invent Admin).
  // Set isDemo BEFORE canonical resolve so constellation detection cannot miss.
  if (!user.isDemo) user.isDemo = true;
  if (!user.canAccessDemoWorkspace) user.canAccessDemoWorkspace = true;
  if (user.flags && typeof user.flags === "object") {
    user.flags = { ...user.flags, isDemo: true };
  } else {
    user.flags = { ...(user.flags || {}), isDemo: true };
  }
  user.demoPreviewWorkspaces = allowed;
  user.canonicalWorkspaceOptions = resolveWorkspaceOptions(user);

  if (map && ids.has(map.companyId)) {
    user.companyId = map.companyId;
    user.companyName = map.companyName;
  } else {
    user.companyId = snap.companyId;
    user.companyName = snap.companyName;
  }

  // Stakeholder-selected feature context:
  // - Brand-Side may grant Brand workspace feature access (Brand AI Visibility read path)
  //   without changing Owner/Operator Airtable write gates.
  // - Non-Brand sides force Brand feature access off so Owner/Operator previews
  //   do not expose Brand AI Visibility via API.
  if (active === WORKSPACE_BRAND) {
    user.canAccessBrandWorkspace = true;
    user.isBrand = true;
  } else {
    user.canAccessBrandWorkspace = false;
    user.isBrand = false;
  }

  if (active === WORKSPACE_OPERATOR) {
    user.canAccessOperatorWorkspace = true;
    user.isOperator = true;
  } else {
    user.canAccessOperatorWorkspace = snap.canAccessOperatorWorkspace;
    user.isOperator = snap.isOperator;
  }

  return user;
}

/**
 * Attach demo stakeholder fields for /api/me without requiring an active header.
 * @param {object} dealalityPayload
 * @param {{ companyIds?: string[], requestedWorkspace?: string }} opts
 */
export function enrichDealalityMeForDemoStakeholder(dealalityPayload, opts = {}) {
  const companyIds = opts.companyIds || dealalityPayload.companyIds || [];
  const probe = {
    ...dealalityPayload,
    companyIds,
    companyId: dealalityPayload.companyProfileId || dealalityPayload.companyId,
    isDemo: dealalityPayload.isDemo,
    canAccessDemoWorkspace: dealalityPayload.canAccessDemoWorkspace,
    flags: dealalityPayload.flags,
    isAdmin: dealalityPayload.isAdmin,
  };

  if (!isDemoStakeholderConstellation(probe) && !probe.isDemo) {
    return {
      ...dealalityPayload,
      founderNavOverridesAvailable: canUseDemoFounderNavOverrides(probe),
      canonicalWorkspaceOptions: resolveWorkspaceOptions(probe),
    };
  }

  const allowed = getDemoStakeholderWorkspaces(probe);
  const requested = normalizeWorkspace(opts.requestedWorkspace);
  const applied = applyDemoStakeholderActiveWorkspace(
    {
      ...probe,
      canAccessOwnerWorkspace: dealalityPayload.canAccessOwnerWorkspace,
      canAccessOperatorWorkspace: dealalityPayload.canAccessOperatorWorkspace,
      canAccessBrandWorkspace: dealalityPayload.canAccessBrandWorkspace,
      isOwner: dealalityPayload.isOwner,
      isOperator: dealalityPayload.isOperator,
      isBrand: dealalityPayload.isBrand,
      workspaceAccess: dealalityPayload.workspaceAccess,
      companyName: dealalityPayload.companyName,
    },
    requested
  );

  const enriched = {
    ...dealalityPayload,
    companyIds,
    companyProfileId: applied.companyId || dealalityPayload.companyProfileId,
    companyName: applied.companyName || dealalityPayload.companyName,
    activeWorkspace: applied.activeWorkspace || null,
    demoStakeholderMode: true,
    demoStakeholderWorkspaces: allowed,
    demoPreviewWorkspaces: allowed,
    founderNavOverridesAvailable: canUseDemoFounderNavOverrides(probe),
    isDemo: true,
    canAccessDemoWorkspace: true,
    flags: {
      ...(dealalityPayload.flags || {}),
      isDemo: true,
      isOwner: !!applied.isOwner,
      isOperator: !!applied.isOperator,
      isBrand: !!applied.isBrand,
    },
    isOwner: !!applied.isOwner,
    isOperator: !!applied.isOperator,
    isBrand: !!applied.isBrand,
    canAccessOwnerWorkspace: !!applied.canAccessOwnerWorkspace,
    canAccessOperatorWorkspace: !!applied.canAccessOperatorWorkspace,
    canAccessBrandWorkspace: !!applied.canAccessBrandWorkspace,
  };
  enriched.canonicalWorkspaceOptions = resolveWorkspaceOptions(enriched);
  return enriched;
}

export function getDemoBrandPortfolioIds() {
  return DEMO_BRAND_PORTFOLIO.map((b) => b.brandId);
}
