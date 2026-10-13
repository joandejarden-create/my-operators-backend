/**
 * Canonical LOCAL Dealality QA admin access.
 *
 * Doctrine:
 *   ADP_LOCAL_ADMIN_AUTH_SINGLE_SOURCE
 *   ADP_LOCAL_ADMIN_NAV_PAGE_API_AUTH_PARITY
 *
 * Grants ADP admin-tool access only when:
 *   - caller is authenticated (caller must pass resolved user)
 *   - runtime is local/dev (never production NODE_ENV)
 *   - request host is localhost / 127.0.0.1 (or DEALALITY_LOCAL_QA_ADMIN=1)
 *   - identity is an explicit approved local QA email
 *
 * Does NOT invent a second "demo user" concept — Demo Mode remains the product
 * identity surface (`isDemo` / demo stakeholder). This helper is the local
 * admin-tools elevation for approved QA emails only.
 *
 * Production must never grant from email alone.
 */

export const ADP_LOCAL_ADMIN_AUTH_SINGLE_SOURCE = "ADP_LOCAL_ADMIN_AUTH_SINGLE_SOURCE";
export const ADP_LOCAL_ADMIN_NAV_PAGE_API_AUTH_PARITY =
  "ADP_LOCAL_ADMIN_NAV_PAGE_API_AUTH_PARITY";

/** Explicit approved local QA identities (lowercase). */
export const APPROVED_LOCAL_DEALALITY_QA_ADMIN_EMAILS = Object.freeze([
  "dealalitydemo@dealality.com",
]);

export function normalizeAdminEmail(email) {
  return String(email || "")
    .trim()
    .toLowerCase();
}

export function requestHostname(req) {
  const host = String(req?.hostname || req?.headers?.host || "")
    .toLowerCase()
    .trim();
  return host.replace(/:\d+$/, "");
}

export function isLocalDevHost(hostname) {
  const h = String(hostname || "")
    .toLowerCase()
    .replace(/:\d+$/, "")
    .trim();
  return h === "localhost" || h === "127.0.0.1" || h === "::1" || h === "[::1]";
}

/**
 * Local/dev runtime — production NODE_ENV always false unless explicit QA flag
 * is set (still requires non-production for safety).
 */
export function isLocalDevRuntime(env = process.env) {
  const nodeEnv = String(env.NODE_ENV || "").toLowerCase();
  if (nodeEnv === "production") return false;
  return true;
}

/**
 * Local request gate: non-production runtime AND (localhost host OR explicit flag).
 */
export function isLocalDevRequest(req = {}, env = process.env) {
  if (!isLocalDevRuntime(env)) return false;
  if (String(env.DEALALITY_LOCAL_QA_ADMIN || "") === "1") return true;
  return isLocalDevHost(requestHostname(req));
}

export function isApprovedLocalDealalityQaIdentity(user, req = {}) {
  const email = normalizeAdminEmail(user?.email || req?.memberstackEmail || req?.memberstackAuth?.email);
  if (!email) return false;
  return APPROVED_LOCAL_DEALALITY_QA_ADMIN_EMAILS.includes(email);
}

/**
 * True only for authenticated approved local QA admin on a local/dev request.
 *
 * @param {object|null|undefined} user - resolved Dealality user (must include email)
 * @param {object} [req]
 * @param {NodeJS.ProcessEnv} [env]
 */
export function hasLocalDealalityAdminAccess(user, req = {}, env = process.env) {
  if (!user || typeof user !== "object") return false;
  if (!isLocalDevRequest(req, env)) return false;
  if (!isApprovedLocalDealalityQaIdentity(user, req)) return false;
  return true;
}
