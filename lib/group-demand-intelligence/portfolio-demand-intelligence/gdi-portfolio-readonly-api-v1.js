/**
 * PDI-2 — Read-only portfolio Demand Intelligence API.
 *
 * Customer surface is the DEFAULT. Internal research requires verified
 * GDI_INTERNAL_RESEARCH_READ privilege — never a client-controlled flag.
 *
 * Market opportunities without matching marketId are excluded.
 * Hotel publication is evaluated independently per hotel.
 */

import { buildPortfolioMembershipIndex } from "./gdi-portfolio-membership-contract-v1.js";
import {
  evaluatePortfolioMarketOpportunityAcrossHotels,
  buildSharedPublicMarketPacket,
  PORTFOLIO_HOTEL_FIT_CLASS,
} from "./gdi-portfolio-hotel-opportunity-fit-contract-v1.js";
import {
  buildPortfolioOperatorAuthIndex,
  PORTFOLIO_AUTH_DENY,
  rejectFuzzyOperatorAuthorizationAttempt,
} from "./gdi-portfolio-operator-authorization-v1.js";

export const GDI_PORTFOLIO_READONLY_API_V1 = "gdi-portfolio-readonly-api-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * Market-level publish flags (necessary but not sufficient for a hotel).
 */
export function isMarketOpportunityCustomerPublished(opp = {}) {
  if (opp.customerFacing === false || opp.customerReady === false) return false;
  if (opp.publicationStatus === "not_published" || opp.published === false) {
    return false;
  }
  if (opp.disposition === "EXCLUDE" || opp.excluded === true) return false;
  if (opp.commercialEligibilityStatus === "RESEARCH_ONLY") return false;
  if (opp.published === true || opp.customerFacing === true) return true;
  if (
    opp.publicationStatus === "published" ||
    opp.publicationStatus === "PUBLISHED"
  ) {
    return true;
  }
  return false;
}

/**
 * Hotel-level publication — independent of other hotels.
 * Market-level publish alone does NOT authorize Hotel B.
 */
export function isHotelPublicationEligible(opp = {}, hotelId) {
  const hid = text(hotelId);
  if (!hid) return false;
  if (!isMarketOpportunityCustomerPublished(opp)) return false;

  if (Array.isArray(opp.publishedHotelIds)) {
    return opp.publishedHotelIds.map(text).includes(hid);
  }
  if (opp.hotelPublication && typeof opp.hotelPublication === "object") {
    const row = opp.hotelPublication[hid];
    if (!row || typeof row !== "object") return false;
    if (row.published === false || row.customerFacing === false) return false;
    return row.published === true || row.customerFacing === true;
  }
  // No hotel-level authorization present → fail closed for customer visibility
  return false;
}

function drawerLink({
  hotelId,
  hotelOpportunityId,
  marketOpportunityId,
  publicationEligible,
}) {
  if (!publicationEligible) {
    return {
      hotelId,
      hotelOpportunityId,
      marketOpportunityId,
      detailPath: null,
      gdiUiHash: null,
      withheld: "UNPUBLISHED_HOTEL_DRAWER",
    };
  }
  return {
    hotelId,
    hotelOpportunityId,
    marketOpportunityId,
    detailPath:
      hotelId && hotelOpportunityId
        ? `/api/group-demand-intelligence/hotels/${encodeURIComponent(hotelId)}/opportunities/${encodeURIComponent(hotelOpportunityId)}`
        : null,
    gdiUiHash:
      hotelId && hotelOpportunityId
        ? `#/group-demand-intelligence?hotelId=${encodeURIComponent(hotelId)}&opportunityId=${encodeURIComponent(hotelOpportunityId)}`
        : null,
  };
}

function deny(status, error) {
  return {
    ok: false,
    status,
    error,
    airtableWrites: 0,
    productionWrites: 0,
    publicationWrites: 0,
    paidCalls: 0,
    readOnly: true,
  };
}

/**
 * Create a read-only portfolio API bound to steward registries + opportunity corpus.
 */
export function createPortfolioReadonlyApi(deps = {}) {
  const authIndex = buildPortfolioOperatorAuthIndex(deps.authRegistry || {});
  const membershipIndex = buildPortfolioMembershipIndex(deps.memberships || []);
  const hotelConfigsById = deps.hotelConfigsById || {};
  const marketsById = new Map(
    (deps.markets || []).map((m) => [text(m.marketId), m]),
  );
  const opportunities = Array.isArray(deps.marketOpportunities)
    ? deps.marketOpportunities
    : [];

  function authorize(caller, { requestedOperatorId, marketId } = {}) {
    const auth = authIndex.resolveCaller({
      ...caller,
      requestedOperatorId:
        requestedOperatorId || caller?.requestedOperatorId || null,
    });
    if (!auth.ok) return auth;
    if (marketId) {
      const marketAuth = authIndex.assertOperatorMarketAccess(auth, marketId);
      if (!marketAuth.ok) return marketAuth;
      const hotels = membershipIndex.listHotelsForOperatorMarket(
        auth.operatorId,
        marketId,
      );
      if (!hotels.length) {
        return {
          ok: false,
          code: PORTFOLIO_AUTH_DENY.MARKET_NOT_AUTHORIZED,
          operatorId: auth.operatorId,
          marketId: text(marketId),
          reason: "NO_ACTIVE_MEMBERSHIP_IN_MARKET",
        };
      }
    }
    return auth;
  }

  function resolveAccessMode(caller, auth, query = {}) {
    // Client flags never unlock internal research.
    const wantsInternal =
      query.internalResearch === true ||
      query.customerSurface === false ||
      query.customerSurface === 0 ||
      query.customerSurface === "0";

    if (wantsInternal) {
      if (!auth.canReadInternalResearch) {
        return {
          ok: false,
          code: PORTFOLIO_AUTH_DENY.INTERNAL_RESEARCH_NOT_AUTHORIZED,
          message:
            "Internal research portfolio access requires steward privilege GDI_INTERNAL_RESEARCH_READ. Customer surface is the default.",
        };
      }
      return { ok: true, mode: "INTERNAL_RESEARCH" };
    }
    // Default: customer-published only
    return { ok: true, mode: "CUSTOMER" };
  }

  function listOperatorMarkets(caller = {}, query = {}) {
    const auth = authorize(caller, {
      requestedOperatorId: query.requestedOperatorId,
    });
    if (!auth.ok) {
      return deny(
        auth.code === PORTFOLIO_AUTH_DENY.UNAUTHENTICATED ||
          auth.code === PORTFOLIO_AUTH_DENY.REGISTRY_NOT_CONFIGURED
          ? 401
          : 403,
        auth,
      );
    }
    const access = resolveAccessMode(caller, auth, query);
    if (!access.ok) return deny(403, access);

    const marketIds = new Set();
    for (const m of membershipIndex.memberships) {
      if (
        m.authorized &&
        m.operatorId === auth.operatorId &&
        auth.allowedMarketIds.includes(m.marketId)
      ) {
        marketIds.add(m.marketId);
      }
    }
    const markets = [...marketIds].map((id) => {
      const meta = marketsById.get(id) || { marketId: id };
      return {
        marketId: id,
        marketLabel: meta.marketLabel || id,
        country: meta.country || null,
        hotelCount: membershipIndex.listHotelsForOperatorMarket(
          auth.operatorId,
          id,
        ).length,
      };
    });
    return {
      ok: true,
      status: 200,
      operatorId: auth.operatorId,
      accessMode: access.mode,
      markets,
      airtableWrites: 0,
      productionWrites: 0,
      publicationWrites: 0,
      readOnly: true,
    };
  }

  function listPortfolioHotels(caller = {}, query = {}) {
    const marketId = text(query.marketId);
    if (!marketId) {
      return deny(400, {
        code: PORTFOLIO_AUTH_DENY.MARKET_ID_REQUIRED,
      });
    }
    const auth = authorize(caller, {
      requestedOperatorId: query.requestedOperatorId,
      marketId,
    });
    if (!auth.ok) {
      return deny(
        auth.code === PORTFOLIO_AUTH_DENY.UNAUTHENTICATED ? 401 : 403,
        auth,
      );
    }
    const access = resolveAccessMode(caller, auth, query);
    if (!access.ok) return deny(403, access);

    const hotels = membershipIndex
      .listHotelsForOperatorMarket(auth.operatorId, marketId)
      .map((m) => ({
        hotelId: m.hotelId,
        hotelDisplayName: m.hotelDisplayName,
        marketId: m.marketId,
        provenanceKind: m.provenanceKind,
        validity: m.validity,
      }));
    return {
      ok: true,
      status: 200,
      operatorId: auth.operatorId,
      marketId,
      accessMode: access.mode,
      hotels,
      airtableWrites: 0,
      productionWrites: 0,
      publicationWrites: 0,
      readOnly: true,
    };
  }

  function getPortfolioMarketOpportunity(caller = {}, query = {}) {
    const marketId = text(query.marketId);
    if (!marketId) {
      return deny(400, { code: PORTFOLIO_AUTH_DENY.MARKET_ID_REQUIRED });
    }
    const marketOpportunityId = text(query.marketOpportunityId);
    const auth = authorize(caller, {
      requestedOperatorId: query.requestedOperatorId,
      marketId,
    });
    if (!auth.ok) {
      return deny(
        auth.code === PORTFOLIO_AUTH_DENY.UNAUTHENTICATED ? 401 : 403,
        auth,
      );
    }
    const access = resolveAccessMode(caller, auth, query);
    if (!access.ok) return deny(403, access);

    const customerMode = access.mode === "CUSTOMER";
    const market = marketsById.get(marketId);
    if (!market) {
      return deny(404, { code: "MARKET_NOT_FOUND", marketId });
    }

    // Require explicit matching marketId — never include missing/mismatched.
    let corpus = opportunities.filter((o) => {
      const oid = text(o.marketId || o.marketOpportunityMarketId);
      return oid === marketId;
    });
    if (marketOpportunityId) {
      corpus = corpus.filter(
        (o) =>
          text(o.marketOpportunityId || o.id || o.opportunityId) ===
          marketOpportunityId,
      );
    }
    if (customerMode) {
      corpus = corpus.filter(isMarketOpportunityCustomerPublished);
    }

    const memberships = membershipIndex.listHotelsForOperatorMarket(
      auth.operatorId,
      marketId,
    );

    const requestedHotelId = text(query.hotelId);
    if (
      requestedHotelId &&
      !membershipIndex.isHotelAuthorized(
        auth.operatorId,
        marketId,
        requestedHotelId,
      )
    ) {
      return deny(403, {
        code: PORTFOLIO_AUTH_DENY.HOTEL_NOT_IN_PORTFOLIO,
        hotelId: requestedHotelId,
        marketId,
        operatorId: auth.operatorId,
      });
    }

    const results = [];
    for (const seed of corpus) {
      const evalResult = evaluatePortfolioMarketOpportunityAcrossHotels({
        sourceOpportunity: seed,
        memberships: requestedHotelId
          ? memberships.filter((m) => m.hotelId === requestedHotelId)
          : memberships,
        hotelConfigsById,
        marketHints: market.marketHints || {},
      });
      const shared = buildSharedPublicMarketPacket(seed);

      let hotelRows = (evalResult.hotelAssessments || [])
        .filter((a) => a.ok)
        .map((a) => {
          const publicationEligible = isHotelPublicationEligible(
            seed,
            a.hotelId,
          );
          return {
            hotelId: a.hotelId,
            fitClass: a.fitClass,
            confidence: a.confidence,
            marketOpportunityId: a.marketOpportunityId,
            hotelOpportunityId: a.hotelOpportunityId,
            missingAttributes: a.missingAttributes || [],
            recommendedPursuit: a.recommendedPursuit,
            limitations: a.limitations || [],
            publicationEligible,
            drawer: drawerLink({
              hotelId: a.hotelId,
              hotelOpportunityId: a.hotelOpportunityId,
              marketOpportunityId: a.marketOpportunityId,
              publicationEligible: customerMode
                ? publicationEligible
                : true,
            }),
            crossHotelFinalState: a.crossHotelFinalState,
          };
        });

      // Customer mode: only hotels with hotel-level publication eligibility
      if (customerMode) {
        hotelRows = hotelRows.filter((h) => h.publicationEligible);
        if (!hotelRows.length) continue;
      }

      results.push({
        marketOpportunityId:
          evalResult.marketOpportunityId || shared.marketOpportunityId,
        marketOpportunityIdentityStable:
          evalResult.marketOpportunityIdentityStable,
        duplicateMarketResearchObjects:
          evalResult.duplicateMarketResearchObjects,
        sharedMarketPacket: {
          marketOpportunityId: shared.marketOpportunityId,
          title: shared.title || seed.title || null,
          summaryWhat: shared.summaryWhat || seed.summaryWhat || null,
          eventName: shared.eventName || seed.eventName || null,
          eventStartDate: seed.eventStartDate || null,
          sources: shared.sources || seed.sources || [],
          layer: "SHARED_PUBLIC_MARKET_INTELLIGENCE",
          confidentialKeysStripped: shared.confidentialKeysStripped || [],
        },
        hotelAssessments: hotelRows,
      });
    }

    return {
      ok: true,
      status: 200,
      operatorId: auth.operatorId,
      marketId,
      accessMode: access.mode,
      customerSurfaceDefault: true,
      results,
      fitClasses: PORTFOLIO_HOTEL_FIT_CLASS,
      airtableWrites: 0,
      productionWrites: 0,
      paidCalls: 0,
      readOnly: true,
      publicationWrites: 0,
    };
  }

  return {
    version: GDI_PORTFOLIO_READONLY_API_V1,
    authorize,
    listOperatorMarkets,
    listPortfolioHotels,
    getPortfolioMarketOpportunity,
    rejectFuzzyOperatorAuthorizationAttempt,
    membershipIndex,
    authIndex,
    airtableWrites: 0,
    productionWrites: 0,
  };
}

export default {
  GDI_PORTFOLIO_READONLY_API_V1,
  createPortfolioReadonlyApi,
  isMarketOpportunityCustomerPublished,
  isHotelPublicationEligible,
  PORTFOLIO_AUTH_DENY,
};
