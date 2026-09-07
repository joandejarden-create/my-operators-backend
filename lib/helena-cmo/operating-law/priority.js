import { LAW_META } from './constants.js';
import { routeGtm } from './gtm-router.js';
import { gateProductClaim, normalizeMaturity } from './product-maturity.js';
import { classifyClaimText } from './claims.js';

/**
 * Explainable priority judgment — not fake precision.
 * Autonomous recurring prioritization remains OFF.
 */
export function evaluatePriority(input = {}) {
  if (LAW_META.autonomousPrioritizationEnabled) {
    // Hard default false — never auto-run ranking cadence from this helper alone.
  }

  const gates = {
    founderStrategyAligned: input.founderStrategyAligned !== false,
    icpRelevant: Boolean(input.icp || input.clientType),
    productMaturityOk: false,
    claimSafe: true,
    commercialReady: input.commercialReady !== false,
    deliveryCapacityOk: input.deliveryCapacityOk !== false,
    approvalFeasible: input.approvalFeasible !== false,
  };

  const maturity = input.productMaturity ? normalizeMaturity(input.productMaturity) : 'PILOT_READY';
  const maturityGate = gateProductClaim({
    maturity,
    claimAsAvailableForPurchase: Boolean(input.claimAsAvailableForPurchase),
    claimAsPilot: Boolean(input.claimAsPilot ?? true),
  });
  gates.productMaturityOk = maturityGate.allowed || maturityGate.action === 'REWRITE';

  if (input.claimText) {
    const c = classifyClaimText(input.claimText);
    gates.claimSafe = c.claimClass !== 'RED';
  }

  const failedGates = Object.entries(gates)
    .filter(([, v]) => !v)
    .map(([k]) => k);

  if (failedGates.length) {
    return {
      band: 'PARK',
      eligible: false,
      failedGates,
      explanation: `Ineligible — failed gates: ${failedGates.join(', ')}`,
      autonomous: false,
      gtm: input.skipGtm ? null : routeGtm(input),
    };
  }

  const factors = {
    strategicClientImportance: score01(input.strategicClientImportance, 1),
    realUrgency: score01(input.realUrgency, input.signals?.liveDealmakingTrigger ? 2 : 1),
    relationshipWarmth: score01(input.relationshipWarmth, 1),
    abilityWillingnessToPay: score01(input.abilityWillingnessToPay, 1),
    commercialPotential: score01(input.commercialPotential, 1),
    learningValue: score01(input.learningValue, 1),
    proofCreation: score01(input.proofCreation, 1),
    adoptionLikelihood: score01(input.adoptionLikelihood, 1),
    deliveryEffortInverse: score01(input.deliveryEffortInverse, 1),
    channelFit: score01(input.channelFit, 1),
  };

  const sum = Object.values(factors).reduce((a, b) => a + b, 0);
  let band = 'MEDIUM';
  if (sum >= 16) band = 'TOP PRIORITY';
  else if (sum >= 12) band = 'HIGH';
  else if (sum >= 8) band = 'MEDIUM';
  else band = 'PARK';

  if (input.founderOverrideBand) {
    band = input.founderOverrideBand;
  }

  return {
    band,
    eligible: true,
    factors,
    assistScore: sum,
    explanation: `Eligible judgment band ${band} (assist score ${sum}/20). Not autonomous law.`,
    autonomous: false,
    founderOverride: Boolean(input.founderOverrideBand),
    gtm: input.skipGtm ? null : routeGtm(input),
  };
}

function score01(v, fallback = 1) {
  if (v == null) return fallback;
  const n = Number(v);
  if (Number.isNaN(n)) return fallback;
  return Math.max(0, Math.min(2, n));
}
