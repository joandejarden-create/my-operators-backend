import { resolveAuthorityConflict } from './authority.js';
import { gateProductClaim } from './product-maturity.js';

/**
 * Founder lock vs product reality: respect both — do not market as live; flag gap.
 */
export function resolveFounderVsProductReality({
  founderSupportsProduct = true,
  productMaturity = 'DEMO_READY',
  claimAsAvailableForPurchase = true,
} = {}) {
  const maturityGate = gateProductClaim({
    maturity: productMaturity,
    claimAsAvailableForPurchase,
    claimAsPilot: false,
  });

  if (founderSupportsProduct && !maturityGate.allowed) {
    return {
      status: 'GAP_FLAGGED',
      action: 'RESPECT_STRATEGY_AND_MATURITY',
      doNotMarketAsLive: true,
      doNotIgnoreFounderLock: true,
      doNotIgnoreProductReality: true,
      maturityGate,
      explanation:
        'Strategy may support the product, but maturity forbids live marketing claims — flag gap; do not silently reconcile',
    };
  }

  return {
    status: 'ALIGNED',
    action: 'PROCEED_UNDER_GATES',
    maturityGate,
  };
}

export function escalateConflict(a, b) {
  return resolveAuthorityConflict({ a, b });
}
