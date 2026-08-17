/**
 * Canonical workspace switcher options — single source of truth.
 *
 * Server computes this for /api/me (and middleware enrichment).
 * Frontend must RENDER these options; it must not rebuild a competing list
 * from workspaceAccess / localStorage / portfolio context.
 *
 * Production: only Airtable-authorized workspaces.
 * Demo/founder constellation: always Owner + Operator + Brand (never shrink).
 * Admin / All Workspaces: separate founder nav overrides (not workspace sides).
 *
 * Intentionally does NOT import demo-stakeholder-workspace.js (avoids circular deps).
 * Demo company IDs stay aligned with MAP_DEMO_STAKEHOLDER_COMPANIES.
 */

import {
  DEMO_PREVIEW_WORKSPACES,
  WORKSPACE_BRAND,
  WORKSPACE_OPERATOR,
  WORKSPACE_OWNER,
} from "../company-workspace-access.js";

export const RESOLVE_WORKSPACE_OPTIONS_VERSION = "dealality_resolve_workspace_options_v1";

export const CANONICAL_WORKSPACE_ORDER = Object.freeze([
  WORKSPACE_OWNER,
  WORKSPACE_OPERATOR,
  WORKSPACE_BRAND,
]);

/** Keep in sync with MAP_DEMO_STAKEHOLDER_COMPANIES in demo-stakeholder-workspace.js */
const DEMO_OWNER_COMPANY_ID =
  process.env.DEALALITY_DEMO_OWNER_COMPANY_ID || "recr0XXnseXNlIlxk";
const DEMO_BRAND_COMPANY_ID =
  process.env.DEALALITY_DEMO_BRAND_COMPANY_ID || "reciQEtqmxz6ZroVc";

function companyIdsOf(viewer) {
  const ids = [...(viewer?.companyIds || [])].filter(Boolean);
  if (viewer?.companyId && !ids.includes(viewer.companyId)) ids.unshift(viewer.companyId);
  if (viewer?.companyProfileId && !ids.includes(viewer.companyProfileId)) {
    ids.push(viewer.companyProfileId);
  }
  return ids;
}

/**
 * Production-authorized workspace sides from access context (no demo expansion).
 * @param {object|null|undefined} viewer
 * @returns {string[]}
 */
export function resolveProductionWorkspaceOptions(viewer) {
  if (!viewer) return [];
  const allowed = new Set();
  const access = Array.isArray(viewer.workspaceAccess) ? viewer.workspaceAccess : [];
  for (const ws of CANONICAL_WORKSPACE_ORDER) {
    if (access.includes(ws)) allowed.add(ws);
  }
  const isOwnerOperator = !!(
    viewer.isOwnerOperator ||
    viewer.flags?.isOwnerOperator
  );
  if (isOwnerOperator) {
    allowed.add(WORKSPACE_OWNER);
    allowed.add(WORKSPACE_OPERATOR);
  }
  if (viewer.canAccessOwnerWorkspace) allowed.add(WORKSPACE_OWNER);
  if (viewer.canAccessOperatorWorkspace) allowed.add(WORKSPACE_OPERATOR);
  if (viewer.canAccessBrandWorkspace) allowed.add(WORKSPACE_BRAND);

  if (!allowed.size) {
    const legacy = String(
      viewer.legacyRole || viewer.role || viewer.primaryRole || ""
    )
      .toLowerCase()
      .replace(/_/g, "-");
    if (legacy === "owner" || legacy === "owner-operator") allowed.add(WORKSPACE_OWNER);
    if (legacy === "operator") allowed.add(WORKSPACE_OPERATOR);
    if (legacy === "brand") allowed.add(WORKSPACE_BRAND);
  }

  return CANONICAL_WORKSPACE_ORDER.filter((ws) => allowed.has(ws));
}

/**
 * Whether this viewer is entitled to the durable founder/demo constellation.
 * @param {object|null|undefined} viewer
 */
export function isDemoFounderWorkspaceConstellation(viewer) {
  if (!viewer) return false;
  if (
    viewer.isDemo ||
    viewer.canAccessDemoWorkspace ||
    viewer.flags?.isDemo ||
    viewer.demoStakeholderMode
  ) {
    return true;
  }
  const ids = new Set(companyIdsOf(viewer));
  return ids.has(DEMO_OWNER_COMPANY_ID) && ids.has(DEMO_BRAND_COMPANY_ID);
}

function resolveFounderNavOverridesAvailable(viewer) {
  if (!viewer) return false;
  if (viewer.founderNavOverridesAvailable === true) return true;
  if (viewer.isAdmin || viewer.flags?.isAdmin) return true;
  const ids = new Set(companyIdsOf(viewer));
  return ids.has(DEMO_OWNER_COMPANY_ID) && ids.has(DEMO_BRAND_COMPANY_ID);
}

/**
 * Canonical workspace option set for the app shell switcher.
 *
 * @param {object|null|undefined} viewer — dealality user /me.dealality shape
 */
export function resolveWorkspaceOptions(viewer) {
  const founderNavOverridesAvailable = resolveFounderNavOverridesAvailable(viewer);
  const demoConstellation = isDemoFounderWorkspaceConstellation(viewer);

  if (demoConstellation) {
    // Always the full governed preview — never company-link–gated, never
    // partial lists from a prior applyDemoStakeholder snapshot.
    const finalList = CANONICAL_WORKSPACE_ORDER.filter((ws) =>
      DEMO_PREVIEW_WORKSPACES.includes(ws)
    );

    return {
      version: RESOLVE_WORKSPACE_OPTIONS_VERSION,
      workspaces: finalList,
      founderNavOverridesAvailable,
      source: "demo_founder_constellation",
      DEMO_WORKSPACE_CONSTELLATION_EXPECTED: true,
      DEMO_OWNER_SIDE_AVAILABLE: finalList.includes(WORKSPACE_OWNER),
      DEMO_BRAND_SIDE_AVAILABLE: finalList.includes(WORKSPACE_BRAND),
      DEMO_OPERATOR_SIDE_AVAILABLE: finalList.includes(WORKSPACE_OPERATOR),
      DEMO_ADMIN_AVAILABLE: founderNavOverridesAvailable,
      DEMO_ALL_WORKSPACES_AVAILABLE: founderNavOverridesAvailable,
    };
  }

  const production = resolveProductionWorkspaceOptions(viewer);
  return {
    version: RESOLVE_WORKSPACE_OPTIONS_VERSION,
    workspaces: production,
    founderNavOverridesAvailable,
    source: "production_workspace_access",
    DEMO_WORKSPACE_CONSTELLATION_EXPECTED: false,
    DEMO_OWNER_SIDE_AVAILABLE: false,
    DEMO_BRAND_SIDE_AVAILABLE: false,
    DEMO_OPERATOR_SIDE_AVAILABLE: false,
    DEMO_ADMIN_AVAILABLE:
      founderNavOverridesAvailable && !!(viewer?.isAdmin || viewer?.flags?.isAdmin),
    DEMO_ALL_WORKSPACES_AVAILABLE:
      founderNavOverridesAvailable && !!(viewer?.isAdmin || viewer?.flags?.isAdmin),
  };
}

/**
 * Assert demo constellation integrity (dev/test helper).
 * @param {ReturnType<typeof resolveWorkspaceOptions>} options
 * @returns {{ ok: boolean, code: string|null }}
 */
export function assertDemoWorkspaceConstellation(options) {
  if (!options?.DEMO_WORKSPACE_CONSTELLATION_EXPECTED) {
    return { ok: true, code: null };
  }
  const ws = options.workspaces || [];
  if (
    !ws.includes(WORKSPACE_BRAND) ||
    !ws.includes(WORKSPACE_OWNER) ||
    !ws.includes(WORKSPACE_OPERATOR)
  ) {
    return { ok: false, code: "DEMO_WORKSPACE_CONSTELLATION_INVALID" };
  }
  return { ok: true, code: null };
}

/**
 * Shell render boundary — same preference as public/app.js buildSwitchableWorkspaces:
 * render canonicalWorkspaceOptions; never rebuild from workspaceAccess when canonical exists.
 *
 * @param {object|null|undefined} dealality
 * @returns {string[]}
 */
export function shellRenderWorkspaceOptions(dealality) {
  if (!dealality) return [];
  const canonical = dealality.canonicalWorkspaceOptions;
  if (canonical && Array.isArray(canonical.workspaces) && canonical.workspaces.length) {
    return CANONICAL_WORKSPACE_ORDER.filter((ws) =>
      canonical.workspaces.includes(ws)
    );
  }
  // Prefer re-resolve over inventing a frontend demo list.
  return resolveWorkspaceOptions(dealality).workspaces;
}

/**
 * Pick active workspace from storage without shrinking the allowed list.
 * @param {string[]} allowed
 * @param {string|null|undefined} stored
 * @param {string|null|undefined} serverActive
 */
export function pickActiveWorkspaceFromStorage(allowed, stored, serverActive) {
  const list = Array.isArray(allowed) ? allowed : [];
  if (stored && list.includes(stored)) return stored;
  if (serverActive && list.includes(serverActive)) return serverActive;
  return list[0] || WORKSPACE_OWNER;
}
