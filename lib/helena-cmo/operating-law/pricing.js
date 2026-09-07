import { ADP_MARKET_TEST, PRICING_CLASSES } from './constants.js';

export function normalizePricingClass(c) {
  const v = String(c || '').toUpperCase();
  if (!PRICING_CLASSES.includes(v)) throw new Error(`Invalid pricing class: ${c}`);
  return v;
}

/**
 * Resolve ADP pricing authority for a commercial situation.
 */
export function resolveAdpPricing({
  hotelCount = null,
  clientType = 'owner',
  enterprise = false,
  portfolio = false,
  discountRequested = false,
  freeRequested = false,
  whiteLabel = false,
  annualContract = false,
  unusualServiceIntensity = false,
} = {}) {
  const type = String(clientType || '').toLowerCase();
  const n = hotelCount == null ? null : Number(hotelCount);

  if (freeRequested || discountRequested || whiteLabel || annualContract || unusualServiceIntensity) {
    return {
      pricingClass: 'CUSTOM_FOUNDER_REVIEW',
      mayQuoteMarketTestDefault: false,
      quote: null,
      reason: 'Non-standard commercial terms require Joan review',
    };
  }

  if (enterprise || portfolio || type === 'brand' || (type === 'operator' && (n == null || n > 2))) {
    return {
      pricingClass: 'ENTERPRISE_TBD',
      mayQuoteMarketTestDefault: false,
      quote: null,
      reason: 'Brand/operator/enterprise/portfolio ADP pricing is case-by-case — do not extrapolate $3,500/2 hotels',
      forbiddenExtrapolation: true,
    };
  }

  if (type === 'operator' && n != null && n > 2) {
    return {
      pricingClass: 'ENTERPRISE_TBD',
      mayQuoteMarketTestDefault: false,
      quote: null,
      reason: 'Operator multi-hotel ADP is enterprise/custom — do not multiply $1,750 × N',
      forbidden: { multiplyPerHotel: true, formula: '1750 * hotelCount' },
    };
  }

  if (n === 1) {
    return {
      pricingClass: 'CUSTOM_FOUNDER_REVIEW',
      mayQuoteMarketTestDefault: false,
      quote: null,
      opportunityQualified: true,
      reason:
        'One-hotel opportunity may be commercially valid (D4) but $1,750 is NOT an approved standalone one-hotel SKU (D5)',
      doNotQuoteAsApproved: 1750,
    };
  }

  if (n != null && n >= 3) {
    return {
      pricingClass: 'CUSTOM_FOUNDER_REVIEW',
      mayQuoteMarketTestDefault: false,
      quote: null,
      reason: '3+ hotels require founder review — do not mechanically scale market-test package',
    };
  }

  if (n === 2 || n == null) {
    // n==null treated as "reasonably comparable two-hotel" only when explicitly comparable; safer: require 2
    if (n === 2) {
      return {
        pricingClass: 'MARKET_TEST_DEFAULT',
        mayQuoteMarketTestDefault: true,
        quote: {
          monthlyUsd: ADP_MARKET_TEST.monthlyUsd,
          hotels: ADP_MARKET_TEST.hotels,
          pilotMonths: ADP_MARKET_TEST.pilotMonths,
          totalUsd: ADP_MARKET_TEST.totalUsd,
          labels: [...ADP_MARKET_TEST.labels],
        },
        reason: 'Comparable two-hotel owner/hotel pilot may use current market-test default with correct label',
        approvedStandard: false,
      };
    }
  }

  return {
    pricingClass: 'CUSTOM_FOUNDER_REVIEW',
    mayQuoteMarketTestDefault: false,
    quote: null,
    reason: 'Insufficient configuration match — founder review required',
  };
}

export function blockMechanicalScale({ hotelCount, proposedMonthly } = {}) {
  const n = Number(hotelCount);
  const proposed = Number(proposedMonthly);
  if (n > 2 && proposed === ADP_MARKET_TEST.perHotelArithmeticUsd * n) {
    return {
      blocked: true,
      reason: `Forbidden: mechanical ${ADP_MARKET_TEST.perHotelArithmeticUsd} × ${n}`,
    };
  }
  return { blocked: false };
}
