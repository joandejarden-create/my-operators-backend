/**
 * Founder/admin gate for GDI Executive Summary STAGED preview.
 * Ordinary authenticated hotel customers must not see unpublished research.
 */

import { WORKSPACE_ADMIN } from "../../company-workspace-access.js";
import { hasLocalDealalityAdminAccess } from "../../dealality/local-dealality-admin-access.js";

export const GDI_EXEC_SUMMARY_PREVIEW_AUTH_V1 =
  "gdi-executive-summary-preview-auth-v1";

/**
 * @param {object} opts
 * @param {object|null} [opts.dealalityUser]
 * @param {boolean} [opts.gdiPilotUnauthRead]
 * @param {object} [opts.req] — optional; used only for local admin access helper
 * @returns {{ ok: boolean, status: number, error: string, message: string }}
 */
export function assertExecutiveSummaryFounderPreviewAuth(opts = {}) {
  if (opts.gdiPilotUnauthRead === true || !opts.dealalityUser) {
    return {
      ok: false,
      status: 401,
      error: "authentication_required",
      message:
        "Founder Executive Summary preview requires authenticated founder/admin access.",
    };
  }

  const u = opts.dealalityUser;
  if (u.isAdmin === true || u.flags?.isAdmin === true) {
    return { ok: true, status: 200, error: null, message: null };
  }

  const workspaces = Array.isArray(u.workspaceAccess) ? u.workspaceAccess : [];
  if (workspaces.includes(WORKSPACE_ADMIN)) {
    return { ok: true, status: 200, error: null, message: null };
  }

  if (opts.req && hasLocalDealalityAdminAccess(u, opts.req)) {
    return { ok: true, status: 200, error: null, message: null };
  }

  return {
    ok: false,
    status: 403,
    error: "forbidden_founder_preview",
    message:
      "STAGED Executive Summary preview is restricted to founder/admin. Ordinary hotel users cannot access unpublished research.",
  };
}

/**
 * Cross-hotel preview refusal — preview hotelId must match authorized hotel scope when provided.
 */
export function assertExecutiveSummaryHotelScope(opts = {}) {
  const hotelId = String(opts.hotelId || "").trim();
  const allowed = opts.allowedHotelIds;
  if (!hotelId) {
    return {
      ok: false,
      status: 400,
      error: "hotelId_required",
      message: "hotelId required",
    };
  }
  if (!Array.isArray(allowed) || allowed.length === 0) {
    // No explicit hotel ACL on user — founder/admin path may browse
    return { ok: true, status: 200, error: null, message: null };
  }
  if (allowed.map(String).includes(hotelId)) {
    return { ok: true, status: 200, error: null, message: null };
  }
  return {
    ok: false,
    status: 403,
    error: "forbidden_cross_hotel_preview",
    message: "Cross-hotel STAGED preview is not authorized for this user.",
  };
}
