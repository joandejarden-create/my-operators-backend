import { AUTHORITY_RANK } from './constants.js';

export function authorityRankIndex(level) {
  const i = AUTHORITY_RANK.indexOf(String(level || '').toUpperCase());
  if (i < 0) throw new Error(`Unknown authority level: ${level}`);
  return i;
}

/**
 * Lower index = higher authority. Lower may not override higher.
 */
export function resolveAuthorityConflict({ a, b }) {
  const ia = authorityRankIndex(a.level);
  const ib = authorityRankIndex(b.level);
  if (ia === ib) {
    return {
      status: 'EQUAL_AUTHORITY_CONFLICT',
      action: 'FLAG_ESCALATE',
      winner: null,
      doNotSilentlyReconcile: true,
      a,
      b,
    };
  }
  const winner = ia < ib ? a : b;
  const loser = ia < ib ? b : a;
  return {
    status: 'RESOLVED_BY_HIERARCHY',
    action: 'USE_HIGHER_AUTHORITY_LOG_CONFLICT',
    winner,
    loser,
    doNotSilentlyReconcile: true,
  };
}

export function assertHigherAuthorityWins(higher, lower) {
  if (authorityRankIndex(higher.level) > authorityRankIndex(lower.level)) {
    throw new Error('Lower authority cannot override higher authority');
  }
  return true;
}
