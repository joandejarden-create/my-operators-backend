/**
 * Gate for GET /api/brand-presence/internal-preview-hotels.
 *
 * Used after optionalDealalityAuth so a verified Memberstack session resolves
 * to req.dealalityUser (Airtable WA + flags), then:
 *
 *   ALLOW — Radar app workspace principal (Owner/Operator/Brand/Admin),
 *           platform admin, local QA admin email, ADP admin email allowlist,
 *           or governed DEV bypass + owner-app header
 *   DENY  — missing session, or no Radar workspace entitlement
 *
 * Policy source of truth: evaluateRadarInternalPreviewAccess /
 * RADAR_INTERNAL_PREVIEW_ACCESS_POLICY in radar-internal-preview-hotels-v1.js.
 *
 * Client headers alone never authorize. Query flags never authorize.
 * Hotel ID allowlists are not used.
 */

import {
  evaluateRadarInternalPreviewAccess,
  isAuthorizedForRadarInternalPreviewHotels,
} from "../lib/hotel-intelligence/radar-internal-preview-hotels-v1.js";
import { elevateAdpOwnerAppAdminFlags } from "./optionalDealalityAuth.js";

function nonProductionDecision(req) {
  if (String(process.env.NODE_ENV || "").toLowerCase() === "production") {
    return undefined;
  }
  return evaluateRadarInternalPreviewAccess(req.dealalityUser, req);
}

export function requireRadarInternalPreviewAccess(req, res, next) {
  // Parity with ADP owner-app: founder/demo may be Airtable role=owner, isAdmin=false.
  elevateAdpOwnerAppAdminFlags(req);

  const u = req.dealalityUser;
  if (!u || typeof u !== "object") {
    const decision = nonProductionDecision(req);
    return res.status(401).json({
      ok: false,
      success: false,
      error: "authentication_required",
      message:
        "Sign in with a Dealality account that can open Opportunity Radar.",
      ...(decision ? { decision: { reason: decision.reason, policy: decision.policy, checks: decision.checks } } : {}),
    });
  }

  if (isAuthorizedForRadarInternalPreviewHotels(u, req)) {
    return next();
  }

  const decision = nonProductionDecision(req);
  return res.status(403).json({
    ok: false,
    success: false,
    error: "forbidden_radar_workspace",
    message:
      "Authenticated Radar workspace access (Owner, Operator, Brand, or Admin) is required for HPC Open/Pipeline inventory.",
    ...(decision
      ? { decision: { reason: decision.reason, policy: decision.policy, checks: decision.checks } }
      : {}),
  });
}
