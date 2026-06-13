/**
 * Partner Intelligence — access gate.
 * Requires memberstackAuth + requireDealalityUser before this middleware.
 *
 * Default: any authenticated Dealality user (solo builder / internal tooling).
 * Set PARTNER_INTELLIGENCE_ADMIN_ONLY=1 to restrict to admin roles in production.
 */
import { PARTNER_INTELLIGENCE_FLAGS } from "../api/lib/partner-intelligence-field-map.js";

function parseAdminRoles() {
  const raw =
    process.env.PARTNER_INTELLIGENCE_ADMIN_ROLES || "Platform Admin,Dealality Admin";
  return raw
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
}

function adminOnlyMode() {
  return process.env.PARTNER_INTELLIGENCE_ADMIN_ONLY === "1";
}

function roleTokens(user) {
  const parts = [
    user?.role,
    user?.primaryRole,
    user?.legacyRole,
    user?.roleRaw,
  ];
  if (Array.isArray(user?.workspaceAccess)) parts.push(...user.workspaceAccess);
  return parts
    .filter(Boolean)
    .flatMap((p) => String(p).split(/[,;|/]/))
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
}

function userHasAdminRole(user) {
  if (user?.isAdmin) return true;
  const adminRoles = parseAdminRoles();
  const tokens = roleTokens(user);
  return adminRoles.some((r) => tokens.some((t) => t === r || t.includes(r)));
}

export function requirePartnerIntelligenceAccess(req, res, next) {
  const user = req.dealalityUser;
  if (!user) {
    return res.status(401).json({
      ok: false,
      success: false,
      error: "authentication_required",
      message: "Dealality user context required.",
    });
  }

  if (!adminOnlyMode()) {
    return next();
  }

  if (userHasAdminRole(user)) {
    return next();
  }

  return res.status(403).json({
    ok: false,
    success: false,
    error: "partner_intelligence_forbidden",
    message:
      "Partner Intelligence is restricted to admin roles (PARTNER_INTELLIGENCE_ADMIN_ONLY=1).",
  });
}

export function isDevPartnerIntelligenceLogging() {
  return process.env.NODE_ENV !== "production";
}

export { PARTNER_INTELLIGENCE_FLAGS };
