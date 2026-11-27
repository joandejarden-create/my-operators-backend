/**
 * GDI Executive Summary auth:
 * - Customer CURRENT reads: same as gdiPilotReadAuth (pilot unauth allowed).
 * - ?preview=staged|founder: full Memberstack (+ local DEV_AUTH_BYPASS) +
 *   Dealality user resolution — never pilot-unauth alone.
 * Founder/admin gate remains in api/gdi-executive-summary.js.
 */

import { memberstackAuth } from "./memberstackAuth.js";
import { requireDealalityUser } from "./requireDealalityUser.js";
import { gdiPilotReadAuth } from "./gdiPilotReadAuth.js";

function wantsFounderPreview(req) {
  const preview = String(req.query?.preview || "")
    .trim()
    .toLowerCase();
  return preview === "staged" || preview === "founder";
}

export async function gdiExecutiveSummaryAuth(req, res, next) {
  if (!wantsFounderPreview(req)) {
    return gdiPilotReadAuth(req, res, next);
  }

  // Clear any prior pilot-unauth flag if present.
  delete req.gdiPilotUnauthRead;

  return memberstackAuth(req, res, (err) => {
    if (err) return next(err);
    return requireDealalityUser(req, res, (err2) => {
      if (err2) return next(err2);
      // Explicit: founder preview is never a pilot-unauth read.
      req.gdiPilotUnauthRead = false;
      return next();
    });
  });
}
