/**
 * PDI-2 — Read-only portfolio Demand Intelligence API (offline-capable).
 *
 * Reuses PDI-1 membership + fit contracts and MOG market identity.
 * No Airtable writes. No paid research. No customer publication.
 */

import {
  buildPortfolioMembershipIndex,
} from "./gdi-portfolio-membership-contract-v1.js";
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

function isPublishedForCustomer(opp = {}) {
  if (opp.customerFacing === false || opp.customerReady === false) return false;
  if (opp.publicationStatus === "not_published" || opp.published === false) {
    return false;
  }
  if (opp.disposition === "EXCLUDE" || opp.excluded === true) return false;
  if (opp.commercialEligibilityStatus === "RESEARCH_ONLY") return false;
  // Explicit publish flags
  if (opp.published === true || opp.customerFacing === true) return true;
  if (opp.publicationStatus === "published" || opp.publicationStatus === "PUBLISHED") {
    return true;
  }
  // Default for portfolio customer surface: unpublished unless flagged
  return false;
}

function drawerLink({ hotelId, hotelOpportunityId, marketOpportunityId }) {
  return {
    hotelId,
    hotelOpportunityId,
    marketOpportunityId,
    detailPath: hotelId && hotelOpportunityId
      ? `/api/group-demand-intelligence/hotels/${encodeURIComponent(hotelId)}/opportunities/${encodeURIComponent(hotelOpportunityId)}`
      : null,
    gdiUiHash:
      hotelId && hotelOpportunityId
        ? `#/group-demand-intelligence?hotelId=${encodeURIComponent(hotelId)}&opportunityId=${encodeURIComponent(hotelOpportunityId)}`
        : null,
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

  function authorize(caller, { operatorId, marketId } = {}) {
    const auth = authIndex.resolveCaller({
      ...caller,
      operatorId: operatorId || caller?.operatorId,
    });
    if (!auth.ok) return auth;
    if (operatorId && text(operatorId) !== auth.operatorId) {
      return {
        ok: false,
        code: PORTFOLIO_AUTH_DENY.CROSS_OPERATOR_DENIED,
        operatorId: auth.operatorId,
        requestedOperatorId: text(operatorId),
      };
    }
    if (marketId) {
      const marketAuth = authIndex.assertOperatorMarketAccess(auth, marketId);
      if (!marketAuth.ok) return marketAuth;
      // Membership must also include at least one hotel in market
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

  /**
   * GET portfolio markets for an authorized operator.
   */
  function listOperatorMarkets(caller = {}, query = {}) {
    const auth = authorize(caller, { operatorId: query.operatorId });
    if (!auth.ok) {
      return {
        ok: false,
        status: auth.code === PORTFOLIO_AUTH_DENY.UNAUTHENTICATED ? 401 : 403,
        error: auth,
        airtableWrites: 0,
        productionWrites: 0,
      };
    }
    const marketIds = new Set();
    for (const m of membershipIndex.memberships) {
      if (m.authorized && m.operatorId === auth.operatorId) {
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
      markets,
      airtableWrites: 0,
      productionWrites: 0,
      readOnly: true,
    };
  }

  /**
   * GET portfolio hotels for operator × market.
   */
  function listPortfolioHotels(caller = {}, query = {}) {
    const marketId = text(query.marketId);
    const auth = authorize(caller, {
      operatorId: query.operatorId,
      marketId,
    });
    if (!auth.ok) {
      return {
        ok: false,
        status: auth.code === PORTFOLIO_AUTH_DENY.UNAUTHENTICATED ? 401 : 403,
        error: auth,
        airtableWrites: 0,
        productionWrites: 0,
      };
    }
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
      hotels,
      airtableWrites: 0,
      productionWrites: 0,
      readOnly: true,
    };
  }

  /**
   * Evaluate / retrieve shared market opportunities with per-hotel fit.
   */
  function getPortfolioMarketOpportunity(caller = {}, query = {}) {
    const marketId = text(query.marketId);
    const marketOpportunityId = text(query.marketOpportunityId);
    const customerSurface = query.customerSurface === true;
    const auth = authorize(caller, {
      operatorId: query.operatorId,
      marketId,
    });
    if (!auth.ok) {
      return {
        ok: false,
        status: auth.code === PORTFOLIO_AUTH_DENY.UNAUTHENTICATED ? 401 : 403,
        error: auth,
        airtableWrites: 0,
        productionWrites: 0,
      };
    }

    const market = marketsById.get(marketId);
    if (!market) {
      return {
        ok: false,
        status: 404,
        error: { code: "MARKET_NOT_FOUND", marketId },
        airtableWrites: 0,
        productionWrites: 0,
      };
    }

    let corpus = opportunities.filter((o) => {
      const oid = text(o.marketId || o.marketOpportunityMarketId);
      return !oid || oid === marketId;
    });
    if (marketOpportunityId) {
      corpus = corpus.filter(
        (o) =>
          text(o.marketOpportunityId || o.id || o.opportunityId) ===
          marketOpportunityId,
      );
    }
    if (customerSurface) {
      corpus = corpus.filter(isPublishedForCustomer);
    }

    const memberships = membershipIndex.listHotelsForOperatorMarket(
      auth.operatorId,
      marketId,
    );

    // Optional: caller asks to include a hotel not in membership → deny that hotel
    const requestedHotelId = text(query.hotelId);
    if (
      requestedHotelId &&
      !membershipIndex.isHotelAuthorized(
        auth.operatorId,
        marketId,
        requestedHotelId,
      )
    ) {
      return {
        ok: false,
        status: 403,
        error: {
          code: PORTFOLIO_AUTH_DENY.HOTEL_NOT_IN_PORTFOLIO,
          hotelId: requestedHotelId,
          marketId,
          operatorId: auth.operatorId,
        },
        airtableWrites: 0,
        productionWrites: 0,
      };
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
      // Confidentiality: shared packet must not carry hotel-private keys
      const hotelRows = (evalResult.hotelAssessments || [])
        .filter((a) => a.ok)
        .map((a) => ({
          hotelId: a.hotelId,
          fitClass: a.fitClass,
          confidence: a.confidence,
          marketOpportunityId: a.marketOpportunityId,
          hotelOpportunityId: a.hotelOpportunityId,
          missingAttributes: a.missingAttributes || [],
          recommendedPursuit: a.recommendedPursuit,
          limitations: a.limitations || [],
          drawer: drawerLink({
            hotelId: a.hotelId,
            hotelOpportunityId: a.hotelOpportunityId,
            marketOpportunityId: a.marketOpportunityId,
          }),
          publicationEligible: isPublishedForCustomer({
            ...seed,
            hotelId: a.hotelId,
            // hotel-level override if present on seed
            published: seed.published,
            customerFacing: seed.customerFacing,
            publicationStatus: seed.publicationStatus,
          }),
          crossHotelFinalState: a.crossHotelFinalState,
        }));

      results.push({
        marketOpportunityId: evalResult.marketOpportunityId || shared.marketOpportunityId,
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
        unpublishedFiltered: customerSurface && !isPublishedForCustomer(seed),
      });
    }

    return {
      ok: true,
      status: 200,
      operatorId: auth.operatorId,
      marketId,
      customerSurface,
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
  PORTFOLIO_AUTH_DENY,
};
