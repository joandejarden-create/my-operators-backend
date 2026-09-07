import { gateCta } from './cta.js';
import { normalizeClaimClass, classifyClaimText } from './claims.js';
import { normalizeMaturity } from './product-maturity.js';
import { normalizePricingClass } from './pricing.js';
import { canPerformExternalAction } from './approvals.js';

const REQUIRED = [
  'icp',
  'gtmTrack',
  'product',
  'productMaturity',
  'claimClass',
  'evidence',
  'confidence',
  'cta',
  'approvalRequired',
  'provenance',
];

/**
 * Every public-facing PREPARE output must carry metadata contract.
 */
export function buildPrepareEnvelope(input = {}) {
  const claim =
    input.claimClass != null
      ? { claimClass: normalizeClaimClass(input.claimClass) }
      : classifyClaimText(input.claimText || input.body || '');

  const maturity = normalizeMaturity(input.productMaturity || 'PILOT_READY');
  const ctaGate = gateCta({
    cta: input.cta || 'SEE HOW IT WORKS',
    productMaturity: maturity,
  });

  const pricingClass = input.pricingClass ? normalizePricingClass(input.pricingClass) : null;

  const envelope = {
    schemaVersion: 'helena-cmo-prepare-contract-v1',
    mode: 'PREPARE',
    icp: input.icp || null,
    gtmTrack: input.gtmTrack || null,
    product: input.product || null,
    productMaturity: maturity,
    claimClass: claim.claimClass,
    evidence: input.evidence || null,
    confidence: input.confidence || 'MEDIUM',
    cta: ctaGate.allowed ? ctaGate.cta : input.cta || null,
    ctaGate,
    pricingClass,
    approvalRequired: true,
    approvalStatus: input.approvalStatus || 'DRAFT',
    provenance: input.provenance || input.source || null,
    body: input.body || null,
    executeAllowed: false,
    publishAllowed: false,
  };

  const missing = REQUIRED.filter((k) => {
    if (k === 'pricingClass') return false; // optional unless pricing present
    if (k === 'gtmTrack' && envelope.icp === 'ICP-A1') return false;
    return envelope[k] == null || envelope[k] === '';
  });

  const external = canPerformExternalAction({
    action: 'PUBLISH',
    approvalStatus: envelope.approvalStatus,
    joanExplicitApproval: false,
  });

  return {
    valid: missing.length === 0 && ctaGate.allowed,
    missing,
    envelope,
    publish: external,
    prepareAllowed: true,
  };
}

export function assertPrepareCannotPublish(prepareResult) {
  if (prepareResult?.publish?.allowed) {
    throw new Error('PREPARE must not authorize PUBLISH under Operating Law v1 defaults');
  }
  return true;
}
