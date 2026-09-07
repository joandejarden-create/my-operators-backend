import { APPROVAL_STATUSES, LAW_META, PROHIBITED_ACTIONS } from './constants.js';

export function normalizeApprovalStatus(status) {
  const s = String(status || '').toUpperCase();
  if (!APPROVAL_STATUSES.includes(s)) throw new Error(`Invalid approval status: ${status}`);
  return s;
}

/**
 * Helena cannot self-approve. APPROVED ≠ PUBLISHED/MERGED/DEPLOYED.
 */
export function canHelenaSelfApprove() {
  return false;
}

export function canPerformExternalAction({ action, approvalStatus = 'DRAFT', joanExplicitApproval = false } = {}) {
  const a = String(action || '').toUpperCase();
  if (PROHIBITED_ACTIONS.includes(a) || ['PUBLISH', 'POST', 'SCHEDULE', 'SEND', 'LAUNCH', 'EXECUTE'].includes(a)) {
    if (!LAW_META.executeEnabled) {
      return {
        allowed: false,
        action: 'BLOCK',
        reason: `EXECUTE/external action disabled — ${a} blocked`,
        executeEnabled: false,
        recurringHelenaEnabled: LAW_META.recurringHelenaEnabled,
      };
    }
  }

  if (['PUBLISH', 'POST', 'SCHEDULE', 'SEND', 'LAUNCH'].includes(a)) {
    if (!joanExplicitApproval) {
      return {
        allowed: false,
        action: 'BLOCK',
        reason: 'Explicit Joan approval required — ambiguous approval does not count',
      };
    }
    const st = normalizeApprovalStatus(approvalStatus);
    if (st !== 'APPROVED' && st !== 'APPROVED_WITH_CHANGES') {
      return {
        allowed: false,
        action: 'BLOCK',
        reason: `Approval status ${st} does not authorize external action`,
      };
    }
    // Even with approval, EXECUTE remains off in v1 defaults — PREPARE only unless unlocked
    if (!LAW_META.executeEnabled) {
      return {
        allowed: false,
        action: 'BLOCK',
        reason: 'Joan approval noted but EXECUTE remains disabled in Operating Law v1 defaults',
        approvedDoesNotMeanPublished: true,
      };
    }
  }

  if (a === 'PREPARE') {
    return { allowed: true, action: 'ALLOW', reason: 'PREPARE is allowed' };
  }

  return { allowed: true, action: 'ALLOW', reason: 'Action allowed under current gates' };
}

export function approvalSemantics() {
  return {
    APPROVED_NE_PUBLISHED: true,
    APPROVED_NE_MERGED: true,
    APPROVED_NE_DEPLOYED: true,
    helenaSelfApprove: false,
    joanOnlyFounderApproval: true,
  };
}
