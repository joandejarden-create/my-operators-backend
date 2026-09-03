/**

 * Express middleware — ADP share capability or Memberstack session.

 * Fail closed when enforcement is on.

 *

 * Precedence: valid signed share capability OR authenticated owner app session

 * with property-level authorization — never require share when Memberstack is valid.

 */

import {

  extractShareCapabilityFromRequest,

  shareEnforcementEnabled,

  verifyShareCapability,

} from "../lib/ai-demand-positioning/share/adp-signed-share-capability-v1.js";

import {

  ownerAppCanAccessProperty,

  resolveOwnerAppPropertyAccess,

} from "../lib/ai-demand-positioning/share/adp-owner-app-property-access-v1.js";

import { resolveCanonicalBppPropertyId } from "../lib/ai-demand-positioning/brand-portfolio/bpp-publication-meta-v1.js";
import { resolveAdpSharePropertyDisplay } from "../lib/ai-demand-positioning/share/adp-share-property-display-v1.js";



function hasMemberstackSession(req) {

  return Boolean(req.memberstackMemberId || req.dealalityUser || req.user);

}



function denyOwnerProperty(res, check) {

  return res.status(check.status || 403).json({

    ok: false,

    error: check.error || "owner_property_access_denied",

    code: check.code || "OWNER_PROPERTY_ACCESS_DENIED",

    message: check.message || "You do not have access to this property report.",

  });

}



/**

 * @param {{ requiredSurface?: string, allowMemberstack?: boolean }} opts

 */

export function requireAdpShareCapability(opts = {}) {

  const requiredSurface = opts.requiredSurface || null;

  const allowMemberstack = opts.allowMemberstack !== false;



  return function adpShareCapabilityMiddleware(req, res, next) {

    if (!shareEnforcementEnabled()) {

      req.adpShareAuth = { mode: "ENFORCEMENT_DISABLED" };

      return next();

    }



    const rawId = req.params?.propertyId || req.query?.propertyId || null;

    const expectedPropertyId = rawId ? resolveCanonicalBppPropertyId(rawId) || rawId : null;



    const token = extractShareCapabilityFromRequest(req);

    if (token) {

      const verified = verifyShareCapability(token, {

        expectedPropertyId: expectedPropertyId || null,

        requiredSurface,

      });

      if (!verified.ok) {

        return res.status(verified.code === "SHARE_PROPERTY_SCOPE" ? 403 : 401).json({

          ok: false,

          error: verified.error,

          code: verified.code,

        });

      }

      req.adpShare = verified.claims;

      req.adpShareAuth = { mode: "SHARE_CAPABILITY", tokenId: verified.claims.tid };

      if (!req.params.propertyId && verified.claims.propertyId) {

        req.adpShareBoundPropertyId = verified.claims.propertyId;

      }

      return next();

    }



    if (allowMemberstack && hasMemberstackSession(req)) {

      req.adpShareAuth = { mode: "MEMBERSTACK" };

      req.adpOwnerPropertyAccess = resolveOwnerAppPropertyAccess(req);

      if (expectedPropertyId) {

        const check = ownerAppCanAccessProperty(req, expectedPropertyId);

        if (!check.ok) return denyOwnerProperty(res, check);

      }

      return next();

    }



    return res.status(401).json({

      ok: false,

      error: allowMemberstack ? "authentication_required" : "share_capability_required",

      code: allowMemberstack ? "ADP_AUTH_REQUIRED" : "SHARE_MISSING",

      message: allowMemberstack

        ? "Sign in to Dealality or use a signed share link to view this report."

        : "A signed share link is required to view this report.",

    });

  };

}



/** Resolve endpoint: validates token and returns bound property (no propertyId in URL). */

export function getAdpShareResolve(req, res) {

  if (!shareEnforcementEnabled()) {

    return res.json({ ok: true, enforcement: false });

  }

  const token = extractShareCapabilityFromRequest(req);

  if (!token) {

    return res.status(401).json({

      ok: false,

      error: "share_capability_required",

      code: "SHARE_MISSING",

    });

  }

  const verified = verifyShareCapability(token, { requiredSurface: "report" });

  if (!verified.ok) {

    return res.status(401).json({ ok: false, error: verified.error, code: verified.code });

  }

  const property = resolveAdpSharePropertyDisplay(verified.claims.propertyId);
  return res.json({
    ok: true,
    propertyId: verified.claims.propertyId,
    property,
    tokenId: verified.claims.tid,
    surfaces: verified.claims.surfaces,
    reportScope: verified.claims.reportScope,
    EXTERNAL_SHARE_DOES_NOT_ENUMERATE_OTHER_ENTITIES: true,
  });
}


