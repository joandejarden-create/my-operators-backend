/**
 * Hotel-scoped authorization for on-demand contact enrichment (Surfe reveal).
 *
 * Reuses Dealality hotel/operator membership fields already resolved onto
 * req.dealalityUser by memberstackAuth + requireDealalityUser.
 *
 * Never authorize from:
 *   - client-supplied hotel IDs alone
 *   - client authorizeLiveProvider / useMock flags alone
 *   - req.memberstackMemberId without a resolved dealalityUser
 *   - share / anonymous / pilot-unauth surfaces
 */

export const GDI_CONTACT_REVEAL_AUTHORIZATION_V1 =
  "gdi-contact-reveal-authorization-v1";

export const CONTACT_REVEAL_AUTH_DENY = Object.freeze({
  SIGN_IN_REQUIRED: "SIGN_IN_REQUIRED",
  MEMBERSTACK_ID_INSUFFICIENT: "MEMBERSTACK_ID_INSUFFICIENT",
  INSUFFICIENT_PERMISSIONS: "INSUFFICIENT_PERMISSIONS",
  HOTEL_ACCESS_DENIED: "HOTEL_ACCESS_DENIED",
  SHARE_VIEWER_BLOCKED: "SHARE_VIEWER_BLOCKED",
  INVALID_HOTEL_ID: "INVALID_HOTEL_ID",
  FEATURE_NOT_PERMITTED: "FEATURE_NOT_PERMITTED",
});

function text(v) {
  return String(v == null ? "" : v).trim();
}

/**
 * Hotel IDs the server-resolved Dealality user may act on.
 * Same bags as need-dates / sales-inventory scopes (never client query).
 */
export function collectDealalityUserHotelScopeIds(user = {}) {
  const bags = [
    user.allowedHotelIds,
    user.gdiHotelIds,
    user.hotelIds,
    user.authorizedHotelIds,
    user.gdiAllowedHotelIds,
    user.gdiSalesInventoryHotelIds,
    user.salesInventoryHotelIds,
    user.gdiSalesTeamInventoryHotelIds,
  ];
  const out = [];
  for (const bag of bags) {
    if (!Array.isArray(bag)) continue;
    for (const id of bag) {
      const s = text(id);
      if (s && !out.includes(s)) out.push(s);
    }
  }
  const primary = text(user.primaryHotelId);
  if (primary && !out.includes(primary)) out.push(primary);
  return out;
}

export function isFounderOrAdminDealalityUser(user = {}) {
  if (!user || typeof user !== "object") return false;
  if (user.isAdmin === true || user.isOwner === true) return true;
  const role = String(user.role || user.primaryRole || "").toLowerCase();
  if (role === "admin" || role === "founder" || role === "owner") return true;
  if (user.flags?.platformAdmin === true || user.flags?.gdiFounder === true) {
    return true;
  }
  return false;
}

/**
 * Contact-enrichment feature gate — hotel membership alone is not enough if the
 * account is explicitly denied; admin/founder always permitted; otherwise require
 * operator/owner workspace membership or an explicit privilege flag.
 */
export function hasContactEnrichmentFeaturePermission(user = {}) {
  if (!user || typeof user !== "object") return false;
  if (isFounderOrAdminDealalityUser(user)) return true;
  if (user.flags?.gdiContactEnrichment === true) return true;
  if (user.flags?.gdiContactReveal === true) return true;
  const privileges = Array.isArray(user.privileges) ? user.privileges : [];
  if (
    privileges.includes("GDI_CONTACT_ENRICHMENT") ||
    privileges.includes("GDI_CONTACT_REVEAL")
  ) {
    return true;
  }
  // Operator / owner-operator product users with hotel scope may enrich.
  if (
    user.isOperator === true ||
    user.isOwnerOperator === true ||
    user.canAccessOperatorWorkspace === true ||
    user.canAccessOwnerWorkspace === true
  ) {
    return true;
  }
  const workspaces = Array.isArray(user.workspaceAccess)
    ? user.workspaceAccess.map((w) => String(w || "").toLowerCase())
    : [];
  if (
    workspaces.some((w) =>
      ["owner", "operator", "admin", "gdi"].includes(w),
    )
  ) {
    return true;
  }
  return false;
}

function isShareOrExternalSurface(req = {}, opts = {}) {
  return (
    opts.shareSurface === true ||
    Boolean(req?.gdiShareCapability) ||
    Boolean(req?.gdiSignedShare) ||
    req?.gdiExternalShare === true ||
    req?.gdiPilotUnauthRead === true
  );
}

/**
 * Fail-closed authorization for contact reveal.
 * @returns {{ ok: true, hotelId, actor, via } | { ok: false, code, status, message, reason? }}
 */
export function assertGdiContactRevealAuthorization(req = {}, opts = {}) {
  const hotelId = text(opts.hotelId || req?.params?.hotelId);
  if (!hotelId || !/^rec[a-zA-Z0-9]+$/.test(hotelId)) {
    return {
      ok: false,
      code: CONTACT_REVEAL_AUTH_DENY.INVALID_HOTEL_ID,
      status: 400,
      message: "A valid hotel id is required for contact lookup.",
      reason: "invalid_hotel_id",
    };
  }

  if (isShareOrExternalSurface(req, opts)) {
    return {
      ok: false,
      code: CONTACT_REVEAL_AUTH_DENY.SHARE_VIEWER_BLOCKED,
      status: 403,
      message:
        "Contact enrichment is not available on share or anonymous pilot-read surfaces. Open Group & Demand Intelligence from the signed-in Dealality app.",
      reason: "share_or_pilot_unauth",
    };
  }

  const user = req?.dealalityUser || null;
  // memberstackMemberId alone is never enough — require server-resolved user.
  if (!user) {
    if (req?.memberstackMemberId) {
      return {
        ok: false,
        code: CONTACT_REVEAL_AUTH_DENY.MEMBERSTACK_ID_INSUFFICIENT,
        status: 403,
        message:
          "Contact enrichment requires a verified Dealality user profile, not Memberstack identity alone.",
        reason: "memberstack_id_without_dealality_user",
      };
    }
    return {
      ok: false,
      code: CONTACT_REVEAL_AUTH_DENY.SIGN_IN_REQUIRED,
      status: 401,
      message:
        "Sign in to Dealality to look up email and phone. Open Group & Demand Intelligence from the Dealality app after signing in.",
      reason: "unauthenticated",
      signInHref: "/app#/group-demand-intelligence",
    };
  }

  if (!hasContactEnrichmentFeaturePermission(user)) {
    return {
      ok: false,
      code: CONTACT_REVEAL_AUTH_DENY.FEATURE_NOT_PERMITTED,
      status: 403,
      message:
        "Your account does not have permission to use contact enrichment.",
      reason: "feature_not_permitted",
    };
  }

  if (isFounderOrAdminDealalityUser(user)) {
    return {
      ok: true,
      hotelId,
      actor: user.email || user.userRecordId || user.memberstackId || "admin",
      via: "dealalityUser.admin",
      hotelScope: collectDealalityUserHotelScopeIds(user),
    };
  }

  const scoped = collectDealalityUserHotelScopeIds(user);
  // Fail closed: empty hotel binding is not "all hotels".
  if (!scoped.length) {
    return {
      ok: false,
      code: CONTACT_REVEAL_AUTH_DENY.HOTEL_ACCESS_DENIED,
      status: 403,
      message:
        "Your account is not linked to any hotel for contact enrichment.",
      reason: "empty_hotel_scope",
    };
  }
  if (!scoped.includes(hotelId)) {
    return {
      ok: false,
      code: CONTACT_REVEAL_AUTH_DENY.HOTEL_ACCESS_DENIED,
      status: 403,
      message:
        "Your account does not have permission to look up contact details for this hotel.",
      reason: "hotel_not_in_user_scope",
      userHotelScope: scoped,
    };
  }

  return {
    ok: true,
    hotelId,
    actor: user.email || user.userRecordId || user.memberstackId || "user",
    via: "dealalityUser.hotel_scope",
    hotelScope: scoped,
  };
}

/**
 * Isolated non-production mock only.
 * Production customer hosts never honor mock / test injection.
 * Requires prior authorization (caller must already have auth.ok).
 */
export function resolveContactRevealMockAllowance(req = {}, body = {}, env = process.env) {
  const railwayEnv = text(env.RAILWAY_ENVIRONMENT).toLowerCase();
  const isProductionCustomerHost =
    text(env.NODE_ENV).toLowerCase() === "production" ||
    railwayEnv === "production" ||
    (Boolean(env.RAILWAY_PROJECT_ID) && railwayEnv !== "" && railwayEnv !== "development");

  // Explicit worktree / local preview override for isolated testing.
  const worktree = text(env.DEALALITY_ALLOW_WORKTREE_SERVER) === "1";
  if (isProductionCustomerHost && !worktree) {
    return {
      allowMock: false,
      reason: "production_customer_host",
      isProductionCustomerHost: true,
    };
  }

  const envMock = text(env.GDI_CONTACT_REVEAL_MOCK) === "1";
  const bodyMock = body.useMock === true || body.useMock === "1";
  // Never honor client-only mock without isolated env flag.
  if (!(worktree || envMock)) {
    return {
      allowMock: false,
      reason: "mock_env_not_enabled",
      isProductionCustomerHost: false,
    };
  }
  if (!(bodyMock || envMock)) {
    return {
      allowMock: false,
      reason: "mock_not_requested",
      isProductionCustomerHost: false,
    };
  }
  return {
    allowMock: true,
    reason: worktree ? "worktree_isolated_mock" : "local_mock_env",
    isProductionCustomerHost: false,
  };
}

export default {
  GDI_CONTACT_REVEAL_AUTHORIZATION_V1,
  CONTACT_REVEAL_AUTH_DENY,
  collectDealalityUserHotelScopeIds,
  isFounderOrAdminDealalityUser,
  hasContactEnrichmentFeaturePermission,
  assertGdiContactRevealAuthorization,
  resolveContactRevealMockAllowance,
};
