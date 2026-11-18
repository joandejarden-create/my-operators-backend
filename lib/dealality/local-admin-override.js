/**
 * LOCAL-ONLY platform-admin override (development).
 *
 * Doctrine:
 *   DEALALITY_LOCAL_ADMIN_OVERRIDE_ENABLED — explicit opt-in (default off)
 *   DEALALITY_LOCAL_ADMIN_OVERRIDE_EMAIL — exact verified identity
 *
 * Activates only when ALL are true:
 *   - NODE_ENV !== production
 *   - DEALALITY_LOCAL_ADMIN_OVERRIDE_ENABLED=1
 *   - request host is localhost / 127.0.0.1 / ::1
 *   - Memberstack identity verified (JWKS/REST/member_lookup — never DEV bypass)
 *   - verified Memberstack email === configured override email (exact)
 *
 * Never trusts client headers, query params, or unverified claims.
 * Never elevates other demo/customer accounts.
 * Production always disabled.
 */

import {
  isLocalDevHost,
  isLocalDevRuntime,
  normalizeAdminEmail,
  requestHostname,
} from "./local-dealality-admin-access.js";

export const DEALALITY_LOCAL_ADMIN_OVERRIDE_DOCTRINE =
  "DEALALITY_LOCAL_ADMIN_OVERRIDE_V1";

export const DEFAULT_LOCAL_ADMIN_OVERRIDE_EMAIL = "dealalitydemo@dealality.com";

/**
 * Verified Memberstack only — never DEV_AUTH_BYPASS.
 * Kept local to avoid circular import with governed-platform-admin-emails.
 */
function isVerifiedJwtIdentityForLocalOverride(req = {}) {
  const memberstackId = String(req.memberstackMemberId || "").trim();
  if (!memberstackId || memberstackId === "dev_bypass") return false;
  const via = String(req.memberstackVerifiedVia || "").toLowerCase();
  if (!via || via === "dev_bypass") return false;
  if (via.includes("bypass") || via.includes("unverified")) return false;
  if (via.includes("jwks") || via.includes("rest") || via.includes("member_lookup")) {
    return true;
  }
  // Accept other non-bypass verifiedVia values from verifyMemberstackToken.
  return true;
}

/**
 * @param {NodeJS.ProcessEnv} [env]
 * @returns {boolean}
 */
export function isLocalAdminOverrideFlagEnabled(env = process.env) {
  return String(env.DEALALITY_LOCAL_ADMIN_OVERRIDE_ENABLED || "").trim() === "1";
}

/**
 * @param {NodeJS.ProcessEnv} [env]
 * @returns {string}
 */
export function configuredLocalAdminOverrideEmail(env = process.env) {
  const raw = String(env.DEALALITY_LOCAL_ADMIN_OVERRIDE_EMAIL || "").trim();
  return normalizeAdminEmail(raw || DEFAULT_LOCAL_ADMIN_OVERRIDE_EMAIL);
}

/**
 * True when this verified request may receive local platform-admin elevation.
 *
 * @param {object} req
 * @param {NodeJS.ProcessEnv} [env]
 * @returns {boolean}
 */
export function shouldApplyLocalAdminOverride(req = {}, env = process.env) {
  if (!isLocalDevRuntime(env)) return false;
  if (!isLocalAdminOverrideFlagEnabled(env)) return false;
  if (!isLocalDevHost(requestHostname(req))) return false;
  // Explicitly exclude DEV_AUTH_BYPASS — founder requires verified JWT, not bypass.
  if (!isVerifiedJwtIdentityForLocalOverride(req)) return false;

  const verifiedEmail = normalizeAdminEmail(req.memberstackEmail);
  if (!verifiedEmail) return false;

  return verifiedEmail === configuredLocalAdminOverrideEmail(env);
}
