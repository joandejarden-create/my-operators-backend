import { POSITIONING, MARKETPLACE } from './constants.js';
import {
  classifyClaimText,
  gateAdpRoiClaim,
  gateLifecycleClaim,
  gateMarketplaceIdentityClaim,
} from './claims.js';

export function messagingOrder() {
  return [...POSITIONING.messagingOrder];
}

export function workingExpression() {
  return POSITIONING.workingExpressionNotTagline;
}

export function cardTriad() {
  return [...POSITIONING.cardTriad];
}

/**
 * Urgency must come from real stakes — not manufactured scarcity.
 */
export function gateUrgency({ text = '', hasRealTrigger = false, manufacturedScarcity = false } = {}) {
  if (manufacturedScarcity) {
    return {
      allowed: false,
      action: 'BLOCK',
      reason: 'Manufactured scarcity / fake countdown forbidden',
    };
  }
  if (/limited\s+spots|act\s+now\s+or\s+lose|only\s+\d+\s+left/i.test(text) && !hasRealTrigger) {
    return {
      allowed: false,
      action: 'BLOCK',
      reason: 'Unsupported urgency without real trigger',
    };
  }
  return {
    allowed: true,
    action: 'ALLOW',
    reason: 'Urgency gate passed — prefer real consequence framing',
    messagingOrder: messagingOrder(),
  };
}

export function evaluateCompanyHeadline(text = '') {
  const claim = classifyClaimText(text);
  const lifecycle = gateLifecycleClaim(text);
  const marketplace = gateMarketplaceIdentityClaim(text);
  const adpRoi = gateAdpRoiClaim(text);

  const blocked =
    claim.action === 'BLOCK' ||
    claim.action === 'BLOCK_OR_REWRITE' ||
    !lifecycle.allowed ||
    !marketplace.allowed ||
    !adpRoi.allowed;

  return {
    blocked,
    claim,
    lifecycle,
    marketplace,
    adpRoi,
    positioningTerritory: POSITIONING.territory,
    marketplaceRole: MARKETPLACE.role,
    publicTaglineStatus: POSITIONING.publicTaglineCategory,
  };
}

export function answerIsDealalityAMarketplace() {
  return {
    action: 'EXPLAIN_LAYER',
    leadWithMarketplace: false,
    statement:
      'Dealality is not simply a hotel marketplace. Marketplace / matching / participation is an important underlying platform layer that supports owner-driven hotel dealmaking. Owner control and confidentiality override marketplace liquidity.',
    locks: ['D1', 'D2'],
  };
}

export function answerDoesDealalityManageFullLifecycle() {
  return {
    action: 'EXPLAIN_AMBITION_VS_TODAY',
    statement:
      'Dealality is building toward supporting more of the hotel lifecycle over time. Current external claims must describe what Dealality can actually do today — do not claim full lifecycle management as a current capability.',
    locks: ['lifecycle claim law'],
  };
}
