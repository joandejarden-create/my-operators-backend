/**
 * PDI-2 HTTP handlers — read-only portfolio Demand Intelligence.
 *
 * Fail closed when live steward registry / inventory are not configured.
 * Fixtures are NEVER loaded on the deployed HTTP path.
 * Tests inject `req.portfolioReadonlyApi`.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createPortfolioReadonlyApi } from "../lib/group-demand-intelligence/portfolio-demand-intelligence/gdi-portfolio-readonly-api-v1.js";
import { PORTFOLIO_AUTH_DENY } from "../lib/group-demand-intelligence/portfolio-demand-intelligence/gdi-portfolio-operator-authorization-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

/** Production / runtime steward config (not fixtures/). */
const LIVE_AUTH_PATH = path.join(
  ROOT,
  "data/group-demand-intelligence/portfolio-demand-intelligence/operator-auth-registry.json",
);
const LIVE_INVENTORY_PATH = path.join(
  ROOT,
  "data/group-demand-intelligence/portfolio-demand-intelligence/portfolio-inventory.json",
);

let cachedLiveApi = null;
let cachedLiveApiError = null;

function loadJsonSafe(p) {
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

/**
 * Build live API from server-controlled data paths only.
 * Returns { ok:false } when not configured — never falls back to fixtures.
 */
export function getLivePortfolioReadonlyApi() {
  if (cachedLiveApi) return { ok: true, api: cachedLiveApi };
  if (cachedLiveApiError) return cachedLiveApiError;

  const authRegistry = loadJsonSafe(LIVE_AUTH_PATH);
  const inventory = loadJsonSafe(LIVE_INVENTORY_PATH);
  if (!authRegistry?.sessions?.length || !inventory) {
    cachedLiveApiError = {
      ok: false,
      status: 503,
      error: {
        code: PORTFOLIO_AUTH_DENY.REGISTRY_NOT_CONFIGURED,
        message:
          "Portfolio Demand Intelligence live registry/inventory is not configured. Fixtures are offline-test only.",
      },
      airtableWrites: 0,
      productionWrites: 0,
      publicationWrites: 0,
      readOnly: true,
    };
    return cachedLiveApiError;
  }
  cachedLiveApi = createPortfolioReadonlyApi({
    authRegistry,
    memberships: inventory.memberships || [],
    hotelConfigsById: inventory.hotelConfigsById || inventory.hotelConfigs || {},
    markets: inventory.markets || [],
    marketOpportunities: inventory.marketOpportunities || [],
  });
  return { ok: true, api: cachedLiveApi };
}

export function resetPortfolioReadonlyApiCache() {
  cachedLiveApi = null;
  cachedLiveApiError = null;
}

export function getPortfolioReadonlyApiForRequest(req) {
  // Tests / harness may inject a fully constructed API.
  if (req?.portfolioReadonlyApi) return { ok: true, api: req.portfolioReadonlyApi };
  return getLivePortfolioReadonlyApi();
}

/**
 * Trusted identity only — ignore spoofable headers / query identity.
 */
export function callerFromVerifiedRequest(req) {
  const headerMember = req.headers?.["x-dealality-member-id"];
  const headerSession = req.headers?.["x-dealality-session-id"];
  const querySession = req.query?.sessionId;
  const clientSuppliedIdentityAttempt = Boolean(
    headerMember || headerSession || querySession,
  );

  const verifiedMemberstackMemberId =
    req.dealalityUser?.memberstackId || req.memberstackMemberId || null;

  const serverVerified = Boolean(
    req.dealalityUser &&
      verifiedMemberstackMemberId &&
      !req.gdiPilotUnauthRead,
  );

  return {
    serverVerified,
    verifiedMemberstackMemberId: serverVerified
      ? String(verifiedMemberstackMemberId)
      : null,
    requestedOperatorId: req.query?.operatorId || null,
    clientSuppliedIdentityAttempt,
    // Captured only so fuzzy rejection tests can assert — never used to grant.
    companyName: req.query?.companyName || req.body?.companyName || null,
    operatorDisplayName:
      req.query?.operatorDisplayName || req.body?.operatorDisplayName || null,
  };
}

function send(res, result) {
  const status = result.status || (result.ok ? 200 : 400);
  return res.status(status).json(result);
}

function resolveApiOrFail(req, res) {
  const resolved = getPortfolioReadonlyApiForRequest(req);
  if (!resolved.ok) {
    send(res, resolved);
    return null;
  }
  return resolved.api;
}

export function getGdiPortfolioMarkets(req, res) {
  const api = resolveApiOrFail(req, res);
  if (!api) return;
  return send(
    res,
    api.listOperatorMarkets(callerFromVerifiedRequest(req), {
      requestedOperatorId: req.query?.operatorId,
    }),
  );
}

export function getGdiPortfolioHotels(req, res) {
  const api = resolveApiOrFail(req, res);
  if (!api) return;
  return send(
    res,
    api.listPortfolioHotels(callerFromVerifiedRequest(req), {
      requestedOperatorId: req.query?.operatorId,
      marketId: req.query?.marketId || req.params?.marketId,
    }),
  );
}

/**
 * Customer-published is the default.
 * customerSurface=0 / internalResearch requires steward privilege — enforced in API.
 */
export function getGdiPortfolioMarketOpportunities(req, res) {
  const api = resolveApiOrFail(req, res);
  if (!api) return;
  const q = req.query || {};
  const customerSurfaceRaw = q.customerSurface;
  const wantsInternal =
    q.internalResearch === "1" ||
    q.internalResearch === "true" ||
    customerSurfaceRaw === "0" ||
    customerSurfaceRaw === "false";

  return send(
    res,
    api.getPortfolioMarketOpportunity(callerFromVerifiedRequest(req), {
      requestedOperatorId: q.operatorId,
      marketId: q.marketId || req.params?.marketId,
      marketOpportunityId: q.marketOpportunityId,
      hotelId: q.hotelId,
      // Default customer; only privileged callers may set internal.
      customerSurface: wantsInternal ? false : true,
      internalResearch: wantsInternal,
    }),
  );
}

export { PORTFOLIO_AUTH_DENY };
