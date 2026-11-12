/**
 * Need Times / Priorities writes: Memberstack user, or local portal write mode.
 * Hotel isolation is still enforced inside assertNeedDatesHotelAccess.
 */

import { memberstackAuth } from "./memberstackAuth.js";
import { requireDealalityUser } from "./requireDealalityUser.js";

function localWriteEnabled(env = process.env) {
  const raw = String(env.GDI_NEED_DATES_PORTAL_LOCAL_WRITE || "")
    .trim()
    .toLowerCase();
  return raw === "1" || raw === "true";
}

export async function gdiNeedDatesWriteAuth(req, res, next) {
  const auth = String(req.headers.authorization || "");
  if (auth.toLowerCase().startsWith("bearer ")) {
    return memberstackAuth(req, res, (err) => {
      if (err) return next(err);
      return requireDealalityUser(req, res, next);
    });
  }

  if (localWriteEnabled()) {
    req.gdiNeedDatesLocalWrite = true;
    return next();
  }

  return res.status(401).json({
    ok: false,
    error: "authentication_required",
    message:
      "Sign in required to update hotel need times and priorities (or set GDI_NEED_DATES_PORTAL_LOCAL_WRITE=1 for local certification).",
  });
}
