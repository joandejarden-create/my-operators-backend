/**
 * Load Airtable Users row → req.dealalityUser (403 if missing or inactive).
 */

import { resolveDealalityUser } from "../lib/dealality/resolve-user.js";

export async function requireDealalityUser(req, res, next) {
  if (!req.memberstackMemberId) {
    return res.status(401).json({
      ok: false,
      success: false,
      error: "authentication_required",
      message: "Memberstack identity missing; run memberstackAuth first.",
    });
  }

  try {
    const result = await resolveDealalityUser({
      memberstackId: req.memberstackMemberId,
      email: req.memberstackEmail,
    });

    if (!result.found) {
      if (result.reason === "user_inactive") {
        return res.status(403).json({
          ok: false,
          success: false,
          error: "user_inactive",
          message: "This account is inactive.",
          status: result.status || null,
        });
      }
      return res.status(403).json({
        ok: false,
        success: false,
        error: "user_not_found",
        message: "No Users row matched this Memberstack account in Airtable.",
        memberstackId: req.memberstackMemberId,
        hint: "Add slug or Unique Webflow ID on Users, or match by email.",
      });
    }

    req.dealalityUser = {
      email: result.email,
      memberstackId: result.memberstackId || req.memberstackMemberId,
      role: result.role,
      roleRaw: result.roleRaw,
      companyId: result.companyId,
      companyIds: result.companyIds || [],
      companyName: result.companyName,
      status: result.status,
      isAdmin: result.isAdmin,
      isOwner: result.isOwner,
      isBrand: result.isBrand,
      isOperator: result.isOperator,
      userRecordId: result.userRecordId,
    };
    return next();
  } catch (err) {
    console.error("[requireDealalityUser] lookup error:", (err && err.message) || err);
    return res.status(500).json({
      ok: false,
      success: false,
      error: "airtable_error",
      message: "Failed to load user from Airtable.",
    });
  }
}
