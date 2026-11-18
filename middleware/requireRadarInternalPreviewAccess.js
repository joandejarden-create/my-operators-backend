/**
 * Gate for GET /api/brand-presence/internal-preview-hotels.
 *
 * Used after optionalDealalityAuth so:
 *   - Absent Bearer + X-Dealality-Owner-App on localhost → DEV_AUTH_BYPASS admin
 *   - Invalid/expired Bearer + owner-app on localhost → same bypass (JWT failure fallback)
 *   - Valid Bearer with Airtable isAdmin / local QA admin → allow
 *   - Valid Bearer with ADP owner-app admin email elevation → allow
 *   - Share / customer / non-admin → 401/403
 *
 * Does not authorize from query flags. Does not use hotel ID allowlists.
 */

import { WORKSPACE_ADMIN } from "../lib/company-workspace-access.js";
import { hasLocalDealalityAdminAccess } from "../lib/dealality/local-dealality-admin-access.js";
import { isAuthorizedForRadarInternalPreviewHotels } from "../lib/hotel-intelligence/radar-internal-preview-hotels-v1.js";
import { elevateAdpOwnerAppAdminFlags } from "./optionalDealalityAuth.js";

export function requireRadarInternalPreviewAccess(req, res, next) {
  // Parity with ADP owner-app: founder/demo may be Airtable role=owner, isAdmin=false.
  elevateAdpOwnerAppAdminFlags(req);

  const u = req.dealalityUser;
  if (!u || typeof u !== "object") {
    return res.status(401).json({
      ok: false,
      success: false,
      error: "authentication_required",
      message:
        "Admin or local Dealality admin access required for internal-preview hotels.",
    });
  }

  if (isAuthorizedForRadarInternalPreviewHotels(u, req)) {
    return next();
  }

  const workspaces = Array.isArray(u.workspaceAccess) ? u.workspaceAccess : [];
  if (workspaces.includes(WORKSPACE_ADMIN)) {
    return next();
  }

  if (hasLocalDealalityAdminAccess(u, req)) {
    return next();
  }

  // Localhost owner-app demo QA (dealalitydemo@…) must reach HPC Open|Pipeline even
  // when Airtable isAdmin=false and the first embed fetch races elevation.
  const ownerApp =
    String(req.headers?.["x-dealality-owner-app"] || req.headers?.["X-Dealality-Owner-App"] || "").trim() ===
    "1";
  const host = String(req.hostname || req.headers?.host || "").toLowerCase();
  const localHost =
    process.env.NODE_ENV !== "production" &&
    (host.startsWith("localhost") || host.startsWith("127.0.0.1") || host.startsWith("[::1]"));
  if (localHost && ownerApp && u.isDemo === true) {
    return next();
  }

  return res.status(403).json({
    ok: false,
    success: false,
    error: "forbidden_admin",
    message: "Admin or local Dealality admin access required for internal-preview hotels.",
  });
}
