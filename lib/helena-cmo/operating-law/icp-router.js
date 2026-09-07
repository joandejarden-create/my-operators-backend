import { ICP_LABELS } from './constants.js';

function hasLiveDealmakingTrigger(signals = {}) {
  return Boolean(
    signals.liveDealmakingTrigger ||
      signals.brandSelection ||
      signals.operatorSelection ||
      signals.reflag ||
      signals.hmaReview ||
      signals.newBuildSelection ||
      signals.repositioning,
  );
}

function hasAdpRelevance(signals = {}) {
  return Boolean(
    signals.adpRelevance ||
      signals.operatingHotel ||
      signals.openingHotel ||
      signals.portfolioHotels ||
      signals.aiVisibilityInterest ||
      signals.requestAdp,
  );
}

/**
 * Deterministic ICP router. Returns multiple eligible routes when applicable.
 * Does not permanently assign one product to one client.
 */
export function routeIcp({ clientType, signals = {} } = {}) {
  const type = String(clientType || '').toLowerCase();
  const routes = [];

  if (type === 'advisor' || type === 'lawyer' || type === 'consultant') {
    routes.push({
      icp: 'ICP-A1',
      ...ICP_LABELS['ICP-A1'],
      primaryEconomicBuyer: false,
      path: 'owner opt-in / amplifier',
      rankHint: signals.qualifiedOwnerRelationship ? 1 : 2,
      explanation: 'Advisor/lawyer/consultant is P1 channel amplifier — not automatic economic buyer',
    });
  }

  if (type === 'brand') {
    routes.push({
      icp: 'ICP-B1',
      ...ICP_LABELS['ICP-B1'],
      roleDistinction: 'BRAND_AS_CUSTOMER',
      notSameAs: 'BRAND_AS_PARTICIPANT_IN_OWNER_WORKFLOW',
      rankHint: 1,
      explanation: 'Brand active secondary commercial / ADP opportunity',
      enterpriseLikely: Boolean(signals.portfolio || signals.enterprise || (signals.hotelCount || 0) > 2),
    });
  }

  if (type === 'operator') {
    routes.push({
      icp: 'ICP-OP1',
      ...ICP_LABELS['ICP-OP1'],
      roleDistinction: 'OPERATOR_AS_CUSTOMER',
      notSameAs: 'OPERATOR_AS_PARTICIPANT_IN_OWNER_WORKFLOW',
      rankHint: 1,
      explanation: 'Operator active secondary commercial / ADP opportunity',
      enterpriseLikely: Boolean(signals.portfolio || signals.enterprise || (signals.hotelCount || 0) > 2),
    });
  }

  if (type === 'owner' || type === 'developer' || type === 'ownership_group') {
    if (hasLiveDealmakingTrigger(signals)) {
      routes.push({
        icp: 'ICP-O1',
        ...ICP_LABELS['ICP-O1'],
        rankHint: 1,
        explanation: 'Live hotel dealmaking trigger → owner decision workflow',
        manufactureUrgencyForbidden: true,
      });
    }
    if (hasAdpRelevance(signals) || signals.requestAdp || !hasLiveDealmakingTrigger(signals)) {
      // O2 when ADP relevance OR explicit ADP request; also eligible alongside O1
      if (hasAdpRelevance(signals) || signals.requestAdp) {
        routes.push({
          icp: 'ICP-O2',
          ...ICP_LABELS['ICP-O2'],
          rankHint: hasLiveDealmakingTrigger(signals) ? 2 : 1,
          explanation:
            'Owner/ownership ADP opportunity — pre-existing AI visibility gap NOT required; diagnostic may establish',
          requirePreExistingAiVisibilityGap: false,
        });
      }
    }
  }

  routes.sort((a, b) => (a.rankHint || 99) - (b.rankHint || 99));
  return {
    clientType: type,
    routes,
    multipleEligible: routes.length > 1,
    permanentSingleProductAssignment: false,
  };
}
