/**
 * Express middleware — Brand AI share capability or Memberstack session.
 * Supports BAI_BRAND_SHARE and BAI_PARENT_COMPANY_SHARE.
 * Fail closed. JWT must not be parsed as a BAI share token.
 */
import { requireDealalityUser } from "./requireDealalityUser.js";
import { requireBrandAiVisibilityAccess } from "./requireBrandAiVisibilityAccess.js";
import {
  BAI_BRAND_SHARE,
  BAI_PARENT_COMPANY_SHARE,
  BAI_PARENT_SHARE_BRAND_SCOPE_AUTHORIZATION,
  BAI_PARENT_SHARE_DOES_NOT_ENUMERATE_OTHER_PARENTS,
  EXTERNAL_BAI_SHARE_PARENT_SCOPED_SELECTOR,
  SHARE_CAPABILITY_CANNOT_BYPASS_PUBLICATION_STATE,
  baiShareEnforcementEnabled,
  buildBaiShareEntitlement,
  buildBaiShareViewer,
  extractBaiShareCapabilityFromRequest,
  resolveBaiShareBrandDisplay,
  verifyBaiShareCapability,
} from "../lib/ai-visibility/share/bai-signed-share-capability-v1.js";

function hasMemberstackSession(req) {
  return Boolean(req.memberstackMemberId || req.dealalityUser || req.user);
}

function attachBaiShareContext(req, verified) {
  const claims = verified.claims;
  const kind = claims.kind || BAI_BRAND_SHARE;
  const allowedBrandIds =
    verified.allowedBrandIds ||
    claims.allowedBrandIds ||
    (claims.brandId ? [claims.brandId] : []);

  if (kind === BAI_PARENT_COMPANY_SHARE) {
    const bound = buildBaiShareEntitlement(allowedBrandIds);
    req.baiShare = claims;
    req.baiShareAuth = {
      mode: "SHARE_CAPABILITY",
      kind: BAI_PARENT_COMPANY_SHARE,
      tokenId: claims.tid,
      parentCompanyId: claims.parentCompanyId,
      gate: BAI_PARENT_SHARE_BRAND_SCOPE_AUTHORIZATION,
    };
    req.dealalityUser = buildBaiShareViewer({
      parentCompanyId: claims.parentCompanyId,
      parentCompanyName: claims.parentCompanyName,
      allowedBrandIds,
    });
    req.aiVisibilityEntitlementGraph = bound.entitlementGraph;
    req.aiVisibilityBrandNamesById = bound.brandNamesById;
    return;
  }

  const bound = buildBaiShareEntitlement(claims.brandId);
  req.baiShare = claims;
  req.baiShareAuth = {
    mode: "SHARE_CAPABILITY",
    kind: BAI_BRAND_SHARE,
    tokenId: claims.tid,
  };
  req.dealalityUser = buildBaiShareViewer(claims.brandId);
  req.aiVisibilityEntitlementGraph = bound.entitlementGraph;
  req.aiVisibilityBrandNamesById = bound.brandNamesById;
}

export function requireBaiShareCapability(opts = {}) {
  const requiredSurface = opts.requiredSurface || null;
  const allowMemberstack = opts.allowMemberstack !== false;

  return function baiShareCapabilityMiddleware(req, res, next) {
    if (!baiShareEnforcementEnabled()) {
      req.baiShareAuth = { mode: "ENFORCEMENT_DISABLED" };
      return next();
    }

    const rawId = req.params?.brandId || req.query?.brandId || null;
    const expectedBrandId = rawId ? String(rawId).trim() : null;
    const token = extractBaiShareCapabilityFromRequest(req);

    if (token) {
      const verified = verifyBaiShareCapability(token, {
        expectedBrandId: expectedBrandId || null,
        requiredSurface,
      });
      if (!verified.ok) {
        return res.status(verified.code === "SHARE_BRAND_SCOPE" ? 403 : 401).json({
          ok: false,
          success: false,
          error: verified.error,
          code: verified.code,
          gate: verified.gate || null,
        });
      }
      attachBaiShareContext(req, verified);
      return next();
    }

    if (!allowMemberstack) {
      return res.status(401).json({
        ok: false,
        success: false,
        error: "share_capability_required",
        code: "SHARE_MISSING",
        message: "A signed share link is required to view this report.",
      });
    }

    if (!hasMemberstackSession(req)) {
      return res.status(401).json({
        ok: false,
        success: false,
        error: "authentication_required",
        code: "BAI_AUTH_REQUIRED",
        message: "Sign in to Dealality or use a signed share link to view this report.",
      });
    }

    req.baiShareAuth = { mode: "MEMBERSTACK" };
    return requireDealalityUser(req, res, () => requireBrandAiVisibilityAccess(req, res, next));
  };
}

export function getBaiShareResolve(req, res) {
  if (!baiShareEnforcementEnabled()) {
    return res.json({ ok: true, enforcement: false });
  }
  const token = extractBaiShareCapabilityFromRequest(req);
  if (!token) {
    return res.status(401).json({
      ok: false,
      success: false,
      error: "share_capability_required",
      code: "SHARE_MISSING",
    });
  }
  const verified = verifyBaiShareCapability(token, { requiredSurface: "report" });
  if (!verified.ok) {
    return res.status(401).json({
      ok: false,
      success: false,
      error: verified.error,
      code: verified.code,
      gate: verified.gate || null,
    });
  }

  const claims = verified.claims;
  if (claims.kind === BAI_PARENT_COMPANY_SHARE) {
    const allowedBrandIds = verified.allowedBrandIds || [];
    const brands = allowedBrandIds.map((id) => resolveBaiShareBrandDisplay(id));
    const defaultBrandId = claims.defaultBrandId || allowedBrandIds[0] || null;
    return res.json({
      ok: true,
      kind: BAI_PARENT_COMPANY_SHARE,
      parentCompanyId: claims.parentCompanyId,
      parentCompanyName: claims.parentCompanyName,
      allowedBrandIds,
      brands,
      defaultBrandId,
      brandId: defaultBrandId,
      brand: defaultBrandId ? resolveBaiShareBrandDisplay(defaultBrandId) : null,
      brandName: defaultBrandId
        ? resolveBaiShareBrandDisplay(defaultBrandId).brandName
        : null,
      tokenId: claims.tid,
      surfaces: claims.surfaces,
      reportScope: claims.reportScope,
      EXTERNAL_BAI_SHARE_PARENT_SCOPED_SELECTOR: true,
      BAI_PARENT_SHARE_DOES_NOT_ENUMERATE_OTHER_PARENTS: true,
      SHARE_CAPABILITY_CANNOT_BYPASS_PUBLICATION_STATE: true,
      gate: EXTERNAL_BAI_SHARE_PARENT_SCOPED_SELECTOR,
    });
  }

  const brand = resolveBaiShareBrandDisplay(claims.brandId);
  return res.json({
    ok: true,
    kind: BAI_BRAND_SHARE,
    brandId: claims.brandId,
    brand,
    brandName: brand.brandName,
    allowedBrandIds: [claims.brandId],
    brands: [brand],
    defaultBrandId: claims.brandId,
    tokenId: claims.tid,
    surfaces: claims.surfaces,
    reportScope: claims.reportScope,
    EXTERNAL_SHARE_DOES_NOT_ENUMERATE_OTHER_ENTITIES: true,
  });
}
