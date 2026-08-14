/**
 * Resolve MarketAlerts audience for API requests.
 * Product path: Memberstack JWT → Users → company flags → owner|brand|operator.
 * Testing: ?audience=owner|brand|operator optional override.
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
 * @param {string|null|undefined} raw
 * @returns {'owner'|'brand'|'operator'|null}
 */
export function normalizeMarketAlertsAudienceParam(raw) {
  const v = String(raw || "")
    .trim()
    .toLowerCase();
  if (v === "owner" || v === "brand" || v === "operator") return v;
  return null;
}

/**
 * Map legacy role string to market-alerts audience (admin/unknown → null).
 * @param {string|null|undefined} legacyRole
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
 * @param {import('express').Request} req
 * @returns {Promise<{ audience: 'owner'|'brand'|'operator'|null, source: string }>}
 */
export async function resolveMarketAlertsAudience(req) {
  const override = normalizeMarketAlertsAudienceParam(req.query?.audience);
  if (override) {
    return { audience: override, source: "query" };
  }

  const token = bearerToken(req);
  if (!token) {
    return { audience: null, source: "none" };
  }

  try {
    const verified = await verifyMemberstackToken(token);
    const memberstackId = verified?.id || verified?.memberId || null;
    const email = verified?.email || verified?.auth?.email || null;
    if (!memberstackId && !email) {
      return { audience: null, source: "token_no_identity" };
    }

    const resolved = await resolveDealalityUser({
      memberstackId: memberstackId || "",
      email,
    });
    if (!resolved?.found) {
      return { audience: null, source: "user_not_found" };
    }

    const flags = {
      isOwner: !!resolved.isOwner,
      isBrand: !!resolved.isBrand,
      isOperator: !!resolved.isOperator,
      isOwnerOperator: !!resolved.isOwnerOperator,
      isAdmin: !!resolved.isAdmin,
    };
    const fromFlags = marketAlertsAudienceFromFlags(flags);
    if (fromFlags) return { audience: fromFlags, source: "server_flags" };

    return {
      audience: audienceFromLegacyRole(resolved.legacyRole),
      source: "server_legacy",
    };
  } catch (err) {
    console.warn(
      "[market-alerts] audience resolve failed:",
      err?.message || err
    );
  }

  return { audience: null, source: "resolve_failed" };
}
