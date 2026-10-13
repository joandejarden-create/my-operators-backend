/**
 * GDI read auth: Memberstack when present; otherwise allow unauthenticated
 * reads when GROUP_DEMAND_INTELLIGENCE_V1 or _PILOT_READ is on.
 * Writes / research / share-issue stay on full admin auth separately.
 */

import { memberstackAuth } from "./memberstackAuth.js";
import { requireDealalityUser } from "./requireDealalityUser.js";
import { isGroupDemandIntelligencePilotReadAllowed } from "../lib/group-demand-intelligence/feature-flag.js";

export async function gdiPilotReadAuth(req, res, next) {
  const auth = String(req.headers.authorization || "");
  if (auth.toLowerCase().startsWith("bearer ")) {
    return memberstackAuth(req, res, (err) => {
      if (err) return next(err);
      return requireDealalityUser(req, res, next);
    });
  }

  if (isGroupDemandIntelligencePilotReadAllowed()) {
    req.gdiPilotUnauthRead = true;
    req.user = req.user || null;
    return next();
  }

  return res.status(401).json({
    ok: false,
    error: "authentication_required",
    message:
      "Sign in required, or set GROUP_DEMAND_INTELLIGENCE_PILOT_READ=1 for local pilot read.",
  });
}
