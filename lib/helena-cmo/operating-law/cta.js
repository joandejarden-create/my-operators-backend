import { CTA_FAMILIES, CTA_RETIRED } from './constants.js';
import { marketingAllowance, normalizeMaturity } from './product-maturity.js';

export function normalizeCta(cta) {
  return String(cta || '').trim();
}

export function isRetiredCta(cta) {
  const n = normalizeCta(cta).toUpperCase();
  return CTA_RETIRED.some((r) => r.toUpperCase() === n);
}

/**
 * CTA must reflect intent and not exceed product maturity.
 */
export function gateCta({ cta, intent = null, productMaturity = 'PILOT_READY' } = {}) {
  const value = normalizeCta(cta);
  if (!value) {
    return { allowed: false, action: 'BLOCK', reason: 'CTA required' };
  }
  if (isRetiredCta(value)) {
    return {
      allowed: false,
      action: 'BLOCK',
      reason: 'REQUEST BETA ACCESS is RETIRED — do not reuse unless founder reactivates',
    };
  }

  const allowedValues = Object.values(CTA_FAMILIES);
  const matched = allowedValues.find((a) => a.toUpperCase() === value.toUpperCase());
  if (!matched) {
    return {
      allowed: false,
      action: 'REWRITE',
      reason: `CTA must be one of: ${allowedValues.join(' | ')}`,
      allowedFamilies: allowedValues,
    };
  }

  const maturity = normalizeMaturity(productMaturity);
  const allowance = marketingAllowance(maturity);

  if (matched === CTA_FAMILIES.DISCUSS_A_PILOT && !allowance.mayMarketAsPilot && !allowance.mayMarketNormally) {
    return {
      allowed: false,
      action: 'BLOCK',
      reason: `DISCUSS A PILOT exceeds maturity ${maturity}`,
    };
  }
  if (matched === CTA_FAMILIES.REQUEST_A_DEMO && !allowance.mayDemonstrate && !allowance.mayMarketNormally) {
    return {
      allowed: false,
      action: 'BLOCK',
      reason: `REQUEST A DEMO exceeds maturity ${maturity}`,
    };
  }

  let intentOk = true;
  if (intent === 'educational' && matched !== CTA_FAMILIES.SEE_HOW_IT_WORKS) {
    intentOk = false;
  }

  return {
    allowed: true,
    action: 'ALLOW',
    cta: matched,
    intentHint: intent,
    intentOk,
    reason: 'CTA family allowed under maturity',
  };
}

export function recommendCta({ funnelStage = 'interest', productMaturity = 'PILOT_READY' } = {}) {
  const stage = String(funnelStage).toLowerCase();
  if (stage === 'education' || stage === 'educational') {
    return gateCta({ cta: CTA_FAMILIES.SEE_HOW_IT_WORKS, intent: 'educational', productMaturity });
  }
  if (stage === 'pilot' || stage === 'commercial_offer') {
    return gateCta({ cta: CTA_FAMILIES.DISCUSS_A_PILOT, intent: 'pilot', productMaturity });
  }
  return gateCta({ cta: CTA_FAMILIES.REQUEST_A_DEMO, intent: 'commercial_interest', productMaturity });
}
