/**
 * Resolve MarketAlerts audience for API requests.
 *
 * Feature access is independent of personalization:
 * filters are always available; owner|brand|operator only customize copy.
 * Unmapped stakeholders (advisor, lender, admin, demo, unsigned) use audience=all.
 */
import { verifyMemberstackToken } from "./memberstack/verify-token.js";
import { resolveDealalityUser } from "./dealality/resolve-user.js";
import { marketAlertsAudienceFromFlags } from "./market-alerts-intelligence.js";

function bearerToken(req) {
  const raw = req.headers?.authorization || req.headers?.Authorization;
  if (!raw || typeof raw !== "string") return null;
  const m = raw.match(/^Bearer\s+(\S+)$/i);
  return m ? m[1] : null;
}

/**
 * Real Dealality stakeholder types found in company-type / ecosystem-role / workspace flags.
 * specializedAudience: owner | brand | operator | all
 */
export const MARKET_ALERTS_STAKEHOLDER_MATRIX = Object.freeze([
  {
    stakeholder: "Hotel Owner",
    companyType: "Hotel Owner",
    specializedAudience: "owner",
    filtersEnabled: true,
  },
  {
    stakeholder: "Hotel Owner - Operator",
    companyType: "Hotel Owner - Operator",
    specializedAudience: "owner",
    filtersEnabled: true,
  },
  {
    stakeholder: "Hotel Brands (Franchise)",
    companyType: "Hotel Brands (Franchise)",
    specializedAudience: "brand",
    filtersEnabled: true,
  },
  {
    stakeholder: "Hotel Management Company",
    companyType: "Hotel Management Company",
    specializedAudience: "operator",
    filtersEnabled: true,
  },
  {
    stakeholder: "Brand + Operator (Both)",
    ecosystemRole: "Both",
    flags: { isBrand: true, isOperator: true },
    specializedAudience: "brand",
    filtersEnabled: true,
  },
  {
    stakeholder: "Advisor / Consultant",
    companyType: "Hospitality Consultants",
    ecosystemRole: "Advisor",
    specializedAudience: "all",
    filtersEnabled: true,
  },
  {
    stakeholder: "Lender / Legal-Advisory",
    ecosystemRole: "Lender",
    specializedAudience: "all",
    filtersEnabled: true,
  },
  {
    stakeholder: "Other",
    companyType: "Other",
    specializedAudience: "all",
    filtersEnabled: true,
  },
  {
    stakeholder: "Admin",
    flags: { isAdmin: true },
    specializedAudience: "all",
    filtersEnabled: true,
  },
  {
    stakeholder: "Demo",
    flags: { isDemo: true },
    specializedAudience: "all",
    filtersEnabled: true,
  },
  {
    stakeholder: "Unsigned",
    flags: null,
    specializedAudience: "all",
    filtersEnabled: true,
  },
]);

/**
 * @param {string|null|undefined} raw
 * @returns {'owner'|'brand'|'operator'|'all'|null}
 */
export function normalizeMarketAlertsAudienceParam(raw) {
  const v = String(raw || "")
    .trim()
    .toLowerCase();
  if (v === "owner" || v === "brand" || v === "operator" || v === "all") return v;
  return null;
}

/**
 * @param {string|null|undefined} legacyRole
 * @returns {'owner'|'brand'|'operator'|null}
 */
export function audienceFromLegacyRole(legacyRole) {
  const r = String(legacyRole || "")
    .trim()
    .toLowerCase();
  if (r === "owner" || r === "owner-operator") return "owner";
  if (r === "brand") return "brand";
  if (r === "operator") return "operator";
  return null;
}

/**
 * Specialized audience or generic fallback. Never returns null.
 * @param {{ isOwner?: boolean, isBrand?: boolean, isOperator?: boolean, isOwnerOperator?: boolean, isAdmin?: boolean }|null|undefined} flags
 * @param {string|null|undefined} [legacyRole]
 * @returns {'owner'|'brand'|'operator'|'all'}
 */
export function canonicalMarketAlertsAudience(flags, legacyRole) {
  const fromFlags = marketAlertsAudienceFromFlags(flags);
  if (fromFlags) return fromFlags;
  const fromLegacy = audienceFromLegacyRole(legacyRole);
  if (fromLegacy) return fromLegacy;
  return "all";
}

export function marketAlertsFiltersEnabled(_stakeholder) {
  return true;
}

/**
 * @param {import('express').Request} req
 * @returns {Promise<{ audience: 'owner'|'brand'|'operator'|'all', source: string }>}
 */
export async function resolveMarketAlertsAudience(req) {
  const override = normalizeMarketAlertsAudienceParam(req.query?.audience);
  if (override) {
    return { audience: override, source: "query" };
  }

  const token = bearerToken(req);
  if (!token) {
    return { audience: "all", source: "none" };
  }

  try {
    const verified = await verifyMemberstackToken(token);
    const memberstackId = verified?.id || verified?.memberId || null;
    const email = verified?.email || verified?.auth?.email || null;
    if (!memberstackId && !email) {
      return { audience: "all", source: "token_no_identity" };
    }

    const resolved = await resolveDealalityUser({
      memberstackId: memberstackId || "",
      email,
    });
    if (!resolved?.found) {
      return { audience: "all", source: "user_not_found" };
    }

    const flags = {
      isOwner: !!resolved.isOwner,
      isBrand: !!resolved.isBrand,
      isOperator: !!resolved.isOperator,
      isOwnerOperator: !!resolved.isOwnerOperator,
      isAdmin: !!resolved.isAdmin,
    };
    return {
      audience: canonicalMarketAlertsAudience(flags, resolved.legacyRole),
      source: "server_flags",
    };
  } catch (err) {
    console.warn(
      "[market-alerts] audience resolve failed:",
      err?.message || err
    );
  }

  return { audience: "all", source: "resolve_failed" };
}
