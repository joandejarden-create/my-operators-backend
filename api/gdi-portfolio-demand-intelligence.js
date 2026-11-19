/**
 * PDI-2 HTTP handlers — read-only portfolio Demand Intelligence.
 * Mounted under /api/group-demand-intelligence/portfolio/*
 * Default corpus: offline steward fixtures (no Airtable production reads required).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createPortfolioReadonlyApi } from "../lib/group-demand-intelligence/portfolio-demand-intelligence/gdi-portfolio-readonly-api-v1.js";
import { PORTFOLIO_AUTH_DENY } from "../lib/group-demand-intelligence/portfolio-demand-intelligence/gdi-portfolio-operator-authorization-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const DEFAULT_FIXTURE = path.join(
  ROOT,
  "fixtures/gdi-portfolio-demand-intelligence-v1/aimbridge-cala-guadalajara.json",
);
const DEFAULT_AUTH = path.join(
  ROOT,
  "fixtures/gdi-portfolio-demand-intelligence-v1/pdi-2-operator-auth-registry.json",
);

let cachedApi = null;

function loadJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function getPortfolioReadonlyApiForRequest(req) {
  if (req?.portfolioReadonlyApi) return req.portfolioReadonlyApi;
  if (cachedApi) return cachedApi;
  const fixture = loadJson(DEFAULT_FIXTURE);
  const authRegistry = fs.existsSync(DEFAULT_AUTH)
    ? loadJson(DEFAULT_AUTH)
    : { sessions: [] };
  cachedApi = createPortfolioReadonlyApi({
    authRegistry,
    memberships: fixture.memberships,
    hotelConfigsById: fixture.hotelConfigs,
    markets: fixture.markets,
    marketOpportunities: [
      {
        ...fixture.marketOpportunitySeed,
        marketId: "market_guadalajara_mx_v1",
        published: fixture.marketOpportunitySeed.published === true,
        customerFacing: fixture.marketOpportunitySeed.customerFacing === true,
        publicationStatus:
          fixture.marketOpportunitySeed.publicationStatus || "not_published",
      },
      ...(Array.isArray(fixture.additionalMarketOpportunities)
        ? fixture.additionalMarketOpportunities
        : []),
    ],
  });
  return cachedApi;
}

/** Test helper — reset singleton between suites. */
export function resetPortfolioReadonlyApiCache() {
  cachedApi = null;
}

function callerFromReq(req) {
  return {
    authenticated: Boolean(req.dealalityUser || req.memberstackMemberId),
    dealalityUser: req.dealalityUser || null,
    memberstackMemberId:
      req.memberstackMemberId ||
      req.dealalityUser?.memberstackId ||
      req.headers?.["x-dealality-member-id"] ||
      null,
    sessionId:
      req.headers?.["x-dealality-session-id"] ||
      req.query?.sessionId ||
      null,
    operatorId: req.query?.operatorId || req.body?.operatorId || null,
    operatorDisplayName:
      req.query?.operatorDisplayName || req.body?.operatorDisplayName || null,
    companyName: req.query?.companyName || req.body?.companyName || null,
  };
}

function send(res, result) {
  const status = result.status || (result.ok ? 200 : 400);
  return res.status(status).json(result);
}

/**
 * GET /api/group-demand-intelligence/portfolio/markets
 */
export function getGdiPortfolioMarkets(req, res) {
  const api = getPortfolioReadonlyApiForRequest(req);
  return send(
    res,
    api.listOperatorMarkets(callerFromReq(req), {
      operatorId: req.query?.operatorId,
    }),
  );
}

/**
 * GET /api/group-demand-intelligence/portfolio/hotels?marketId=
 */
export function getGdiPortfolioHotels(req, res) {
  const api = getPortfolioReadonlyApiForRequest(req);
  return send(
    res,
    api.listPortfolioHotels(callerFromReq(req), {
      operatorId: req.query?.operatorId,
      marketId: req.query?.marketId || req.params?.marketId,
    }),
  );
}

/**
 * GET /api/group-demand-intelligence/portfolio/market-opportunities?marketId=
 * Optional: marketOpportunityId, hotelId, customerSurface=1
 */
export function getGdiPortfolioMarketOpportunities(req, res) {
  const api = getPortfolioReadonlyApiForRequest(req);
  return send(
    res,
    api.getPortfolioMarketOpportunity(callerFromReq(req), {
      operatorId: req.query?.operatorId,
      marketId: req.query?.marketId || req.params?.marketId,
      marketOpportunityId: req.query?.marketOpportunityId,
      hotelId: req.query?.hotelId,
      customerSurface:
        req.query?.customerSurface === "1" ||
        req.query?.customerSurface === "true",
    }),
  );
}

export { PORTFOLIO_AUTH_DENY };
