import { PRODUCT_MATURITY } from './constants.js';

export function normalizeMaturity(status) {
  const s = String(status || '').toUpperCase().trim();
  if (!PRODUCT_MATURITY.includes(s)) {
    throw new Error(`Invalid product maturity: ${status}`);
  }
  return s;
}

/**
 * Marketing allowance by maturity. Technical existence ≠ marketing readiness.
 */
export function marketingAllowance(maturity) {
  const m = normalizeMaturity(maturity);
  switch (m) {
    case 'LIVE_SELLABLE':
      return {
        mayMarketNormally: true,
        mayMarketAsPilot: true,
        mayDemonstrate: true,
        mayClaimBroadlyAvailable: true,
        publicClaimAllowed: true,
      };
    case 'PILOT_READY':
      return {
        mayMarketNormally: false,
        mayMarketAsPilot: true,
        mayDemonstrate: true,
        mayClaimBroadlyAvailable: false,
        publicClaimAllowed: true,
        requiredFraming: 'pilot / controlled commercial offer',
      };
    case 'DEMO_READY':
      return {
        mayMarketNormally: false,
        mayMarketAsPilot: false,
        mayDemonstrate: true,
        mayClaimBroadlyAvailable: false,
        publicClaimAllowed: false,
        requiredFraming: 'demo only — not broadly available for purchase',
      };
    case 'IN_DEVELOPMENT':
      return {
        mayMarketNormally: false,
        mayMarketAsPilot: false,
        mayDemonstrate: false,
        mayClaimBroadlyAvailable: false,
        publicClaimAllowed: false,
        requiredFraming: 'internal/future unless Joan-approved controlled preview',
      };
    case 'CONCEPT':
      return {
        mayMarketNormally: false,
        mayMarketAsPilot: false,
        mayDemonstrate: false,
        mayClaimBroadlyAvailable: false,
        publicClaimAllowed: false,
        requiredFraming: 'must not be marketed as existing functionality',
      };
    case 'RETIRED':
      return {
        mayMarketNormally: false,
        mayMarketAsPilot: false,
        mayDemonstrate: false,
        mayClaimBroadlyAvailable: false,
        publicClaimAllowed: false,
        requiredFraming: 'must not be promoted as current',
      };
    default:
      throw new Error(`Unhandled maturity: ${m}`);
  }
}

/**
 * Gate a proposed marketing claim against product maturity.
 * @returns {{ allowed: boolean, action: 'ALLOW'|'REWRITE'|'BLOCK', reason: string }}
 */
export function gateProductClaim({ maturity, claimAsAvailableForPurchase = false, claimAsPilot = false } = {}) {
  const allowance = marketingAllowance(maturity);
  if (claimAsAvailableForPurchase && !allowance.mayClaimBroadlyAvailable && !allowance.mayMarketAsPilot) {
    return {
      allowed: false,
      action: 'BLOCK',
      reason: `Product maturity ${normalizeMaturity(maturity)} forbids purchase/availability claims`,
      allowance,
    };
  }
  if (claimAsAvailableForPurchase && allowance.mayMarketAsPilot && !allowance.mayClaimBroadlyAvailable) {
    return {
      allowed: false,
      action: 'REWRITE',
      reason: `Rewrite as pilot/controlled offer — maturity is ${normalizeMaturity(maturity)}`,
      allowance,
    };
  }
  if (claimAsPilot && !allowance.mayMarketAsPilot && !allowance.mayMarketNormally) {
    return {
      allowed: false,
      action: 'BLOCK',
      reason: `Maturity ${normalizeMaturity(maturity)} does not allow pilot marketing`,
      allowance,
    };
  }
  return { allowed: true, action: 'ALLOW', reason: 'Maturity gate passed', allowance };
}
