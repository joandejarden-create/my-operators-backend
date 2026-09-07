/**
 * Helena CMO Operating Law v1 — public API
 */
export * from './constants.js';
export * from './authority.js';
export * from './epistemic.js';
export * from './product-maturity.js';
export * from './claims.js';
export * from './icp-router.js';
export * from './gtm-router.js';
export * from './pricing.js';
export * from './messaging.js';
export * from './cta.js';
export * from './priority.js';
export * from './approvals.js';
export * from './conflicts.js';
export * from './prepare-contract.js';

import { LAW_META, FOUNDER_LOCKS, ALLOWED_ACTIONS, PROHIBITED_ACTIONS } from './constants.js';
import { routeIcp } from './icp-router.js';
import { routeGtm } from './gtm-router.js';
import { resolveAdpPricing, blockMechanicalScale } from './pricing.js';
import { evaluateCompanyHeadline, answerIsDealalityAMarketplace, answerDoesDealalityManageFullLifecycle, gateUrgency } from './messaging.js';
import { gateCta } from './cta.js';
import { gateProductClaim } from './product-maturity.js';
import { gateAdpRoiClaim } from './claims.js';
import { canPerformExternalAction, canHelenaSelfApprove, approvalSemantics } from './approvals.js';
import { buildPrepareEnvelope, assertPrepareCannotPublish } from './prepare-contract.js';
import { evaluatePriority } from './priority.js';
import { resolveFounderVsProductReality } from './conflicts.js';

export function getOperatingLawMeta() {
  return { ...LAW_META, founderLocks: { ...FOUNDER_LOCKS }, allowedActions: [...ALLOWED_ACTIONS], prohibitedActions: [...PROHIBITED_ACTIONS] };
}

export function helenaMay(action) {
  const a = String(action || '').toUpperCase();
  if (PROHIBITED_ACTIONS.includes(a)) return { allowed: false, reason: 'Prohibited by Operating Law v1' };
  if (ALLOWED_ACTIONS.includes(a)) return { allowed: true, reason: 'Allowed CMO mode' };
  return canPerformExternalAction({ action: a });
}

export const OperatingLaw = Object.freeze({
  meta: getOperatingLawMeta,
  routeIcp,
  routeGtm,
  resolveAdpPricing,
  blockMechanicalScale,
  evaluateCompanyHeadline,
  answerIsDealalityAMarketplace,
  answerDoesDealalityManageFullLifecycle,
  gateUrgency,
  gateCta,
  gateProductClaim,
  gateAdpRoiClaim,
  canPerformExternalAction,
  canHelenaSelfApprove,
  approvalSemantics,
  buildPrepareEnvelope,
  assertPrepareCannotPublish,
  evaluatePriority,
  resolveFounderVsProductReality,
  helenaMay,
});

export default OperatingLaw;
