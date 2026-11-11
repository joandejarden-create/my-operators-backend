/**
 * Server-side ADP founder/admin review capability.
 * URL params (adpReview=1) are hints only — never sufficient authorization.
 */

import { resolveOwnerAppPropertyAccess } from "../share/adp-owner-app-property-access-v1.js";
import { canAccessAdpMonthlyReviewAdmin } from "../../../middleware/requireAdpMonthlyReviewAdminAccess.js";

export const ADP_REVIEW_CAPABILITY_V1 = "ADP_REVIEW_CAPABILITY_V1";

/**
 * @param {import("express").Request} req
 * @returns {{
 *   ok: boolean,
 *   reviewAllowed: boolean,
 *   publishActionsAllowed: boolean,
 *   reason: string|null,
 *   actor: { id: string|null, email: string|null, isAdmin: boolean }|null,
 *   defaultReportVersion: "staged"|"published",
 *   contractVersion: string
 * }}
 */
export function resolveAdpReviewCapability(req) {
  if (req?.adpShareAuth?.mode === "SHARE_CAPABILITY" || req?.adpShare?.propertyId) {
    return {
      ok: true,
      reviewAllowed: false,
      publishActionsAllowed: false,
      reason: "share_capability_customer_surface",
      actor: null,
      defaultReportVersion: "published",
      contractVersion: ADP_REVIEW_CAPABILITY_V1,
    };
  }

  const u = req?.dealalityUser || {};
  const id =
    req?.memberstackMemberId ||
    u.memberstackId ||
    u.id ||
    (req?.memberstackVerifiedVia === "dev_bypass" ? "dev_bypass_reviewer" : null);
  const email = req?.memberstackEmail || u.email || null;

  if (!id && process.env.NODE_ENV === "production") {
    return {
      ok: true,
      reviewAllowed: false,
      publishActionsAllowed: false,
      reason: "authentication_required",
      actor: null,
      defaultReportVersion: "published",
      contractVersion: ADP_REVIEW_CAPABILITY_V1,
    };
  }

  const access = resolveOwnerAppPropertyAccess(req || {});
  const adpAdmin =
    !!u &&
    typeof u === "object" &&
    canAccessAdpMonthlyReviewAdmin(
      {
        ...u,
        email: email || u.email,
        isAdmin: u.isAdmin === true || access.isAdmin === true,
        flags: u.flags || { isAdmin: access.isAdmin === true },
      },
      req || {}
    );
  const reviewAllowed = access.isAdmin === true || adpAdmin === true;

  return {
    ok: true,
    reviewAllowed,
    publishActionsAllowed: reviewAllowed,
    reason: reviewAllowed ? null : "not_platform_admin",
    actor: {
      id: id ? String(id) : null,
      email: email || null,
      isAdmin: reviewAllowed,
    },
    defaultReportVersion: reviewAllowed ? "staged" : "published",
    contractVersion: ADP_REVIEW_CAPABILITY_V1,
  };
}

export function requireAdpReviewCapability(req, res) {
  const cap = resolveAdpReviewCapability(req);
  if (!cap.reviewAllowed) {
    const status = cap.reason === "authentication_required" ? 401 : 403;
    res.status(status).json({
      ok: false,
      error: "adp_review_not_authorized",
      reason: cap.reason,
      message:
        "ADP staged review requires a platform admin Memberstack session. URL parameters alone are not authorization.",
      contractVersion: ADP_REVIEW_CAPABILITY_V1,
    });
    return null;
  }
  return cap;
}
