/**
 * Portfolio Demand Intelligence read auth — verified Memberstack only.
 * Does NOT allow pilot unauthenticated reads (unlike gdiPilotReadAuth).
 */

import { memberstackAuth } from "./memberstackAuth.js";
import { requireDealalityUser } from "./requireDealalityUser.js";

export async function gdiPortfolioReadAuth(req, res, next) {
  const auth = String(req.headers.authorization || "");
  if (!auth.toLowerCase().startsWith("bearer ")) {
    return res.status(401).json({
      ok: false,
      error: "authentication_required",
      code: "UNAUTHENTICATED",
      message:
        "Portfolio Demand Intelligence requires a verified Dealality / Memberstack session.",
    });
  }
  return memberstackAuth(req, res, (err) => {
    if (err) return next(err);
    return requireDealalityUser(req, res, next);
  });
}

export default { gdiPortfolioReadAuth };
