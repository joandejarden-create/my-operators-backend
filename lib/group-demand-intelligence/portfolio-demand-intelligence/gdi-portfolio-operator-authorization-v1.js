/**
 * PDI-2 — Operator authorization for portfolio reads.
 *
 * Authorization is explicit steward mapping only.
 * Never treat company-name text, notes, or fixture display labels as proof of access.
 */

export const GDI_PORTFOLIO_OPERATOR_AUTHORIZATION_V1 =
  "gdi-portfolio-operator-authorization-v1";

export const PORTFOLIO_AUTH_DENY = Object.freeze({
  UNAUTHENTICATED: "UNAUTHENTICATED",
  OPERATOR_NOT_MAPPED: "OPERATOR_NOT_MAPPED",
  OPERATOR_MISMATCH: "OPERATOR_MISMATCH",
  MARKET_NOT_AUTHORIZED: "MARKET_NOT_AUTHORIZED",
  HOTEL_NOT_IN_PORTFOLIO: "HOTEL_NOT_IN_PORTFOLIO",
  CROSS_OPERATOR_DENIED: "CROSS_OPERATOR_DENIED",
  FUZZY_NAME_MATCH_REJECTED: "FUZZY_NAME_MATCH_REJECTED",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * Build an authorization index from explicit steward session→operator maps.
 * @param {{ sessions?: object[], operatorMarketAllowlist?: object[] }} registry
 */
export function buildPortfolioOperatorAuthIndex(registry = {}) {
  const sessions = Array.isArray(registry.sessions) ? registry.sessions : [];
  const byMember = new Map();
  const bySession = new Map();
  for (const row of sessions) {
    const operatorId = text(row.operatorId);
    const memberstackMemberId = text(row.memberstackMemberId);
    const sessionId = text(row.sessionId);
    if (!operatorId) continue;
    const entry = {
      operatorId,
      memberstackMemberId: memberstackMemberId || null,
      sessionId: sessionId || null,
      email: text(row.email) || null,
      provenanceKind: text(row.provenanceKind) || "STEWARD_AUTHORIZED_SESSION_MAP",
      allowedMarketIds: Array.isArray(row.allowedMarketIds)
        ? row.allowedMarketIds.map(text).filter(Boolean)
        : null,
    };
    if (memberstackMemberId) byMember.set(memberstackMemberId, entry);
    if (sessionId) bySession.set(sessionId, entry);
  }

  return {
    version: GDI_PORTFOLIO_OPERATOR_AUTHORIZATION_V1,
    sessionCount: sessions.length,
    resolveCaller(caller = {}) {
      const memberId = text(
        caller.memberstackMemberId || caller.dealalityUser?.memberstackId,
      );
      const sessionId = text(caller.sessionId);
      if (!memberId && !sessionId && !caller.dealalityUser && !caller.authenticated) {
        return {
          ok: false,
          code: PORTFOLIO_AUTH_DENY.UNAUTHENTICATED,
          operatorId: null,
        };
      }
      // Reject company-name-only authorization attempts
      if (
        !memberId &&
        !sessionId &&
        text(caller.operatorDisplayName || caller.companyName) &&
        !text(caller.operatorId)
      ) {
        return {
          ok: false,
          code: PORTFOLIO_AUTH_DENY.FUZZY_NAME_MATCH_REJECTED,
          operatorId: null,
        };
      }
      const mapped =
        (memberId && byMember.get(memberId)) ||
        (sessionId && bySession.get(sessionId)) ||
        null;
      if (!mapped) {
        // Explicit operatorId on caller still requires registry mapping
        return {
          ok: false,
          code: PORTFOLIO_AUTH_DENY.OPERATOR_NOT_MAPPED,
          operatorId: text(caller.operatorId) || null,
        };
      }
      if (
        text(caller.operatorId) &&
        text(caller.operatorId) !== mapped.operatorId
      ) {
        return {
          ok: false,
          code: PORTFOLIO_AUTH_DENY.OPERATOR_MISMATCH,
          operatorId: mapped.operatorId,
          requestedOperatorId: text(caller.operatorId),
        };
      }
      return {
        ok: true,
        operatorId: mapped.operatorId,
        memberstackMemberId: mapped.memberstackMemberId,
        email: mapped.email,
        allowedMarketIds: mapped.allowedMarketIds,
        provenanceKind: mapped.provenanceKind,
      };
    },
    assertOperatorMarketAccess(auth, marketId) {
      if (!auth?.ok) return auth;
      const mid = text(marketId);
      if (!mid) {
        return {
          ok: false,
          code: PORTFOLIO_AUTH_DENY.MARKET_NOT_AUTHORIZED,
          operatorId: auth.operatorId,
        };
      }
      if (
        Array.isArray(auth.allowedMarketIds) &&
        auth.allowedMarketIds.length > 0 &&
        !auth.allowedMarketIds.includes(mid)
      ) {
        return {
          ok: false,
          code: PORTFOLIO_AUTH_DENY.MARKET_NOT_AUTHORIZED,
          operatorId: auth.operatorId,
          marketId: mid,
        };
      }
      return { ok: true, operatorId: auth.operatorId, marketId: mid };
    },
  };
}

/**
 * Reject company-name / notes authorization (fail closed).
 */
export function rejectFuzzyOperatorAuthorizationAttempt({
  companyName,
  noteText,
} = {}) {
  return {
    ok: false,
    code: PORTFOLIO_AUTH_DENY.FUZZY_NAME_MATCH_REJECTED,
    companyName: text(companyName) || null,
    noteText: text(noteText) || null,
    message:
      "Operator authorization requires steward session/member mapping — not company-name or notes matching.",
  };
}

export default {
  GDI_PORTFOLIO_OPERATOR_AUTHORIZATION_V1,
  PORTFOLIO_AUTH_DENY,
  buildPortfolioOperatorAuthIndex,
  rejectFuzzyOperatorAuthorizationAttempt,
};
