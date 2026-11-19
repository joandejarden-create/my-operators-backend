/**
 * PDI-2 — Operator authorization for portfolio reads.
 *
 * Server-verified Memberstack identity only.
 * Never authorize from client headers, query sessionId, company-name, or notes.
 *
 * Market allowlist semantics (fail closed):
 *   - missing / null / undefined → deny all markets (MARKET_ALLOWLIST_REQUIRED)
 *   - [] empty array → deny all markets
 *   - non-empty → only listed marketIds
 */

export const GDI_PORTFOLIO_OPERATOR_AUTHORIZATION_V1 =
  "gdi-portfolio-operator-authorization-v1";

export const PORTFOLIO_AUTH_DENY = Object.freeze({
  UNAUTHENTICATED: "UNAUTHENTICATED",
  OPERATOR_NOT_MAPPED: "OPERATOR_NOT_MAPPED",
  OPERATOR_MISMATCH: "OPERATOR_MISMATCH",
  MARKET_NOT_AUTHORIZED: "MARKET_NOT_AUTHORIZED",
  MARKET_ALLOWLIST_REQUIRED: "MARKET_ALLOWLIST_REQUIRED",
  MARKET_ID_REQUIRED: "MARKET_ID_REQUIRED",
  HOTEL_NOT_IN_PORTFOLIO: "HOTEL_NOT_IN_PORTFOLIO",
  CROSS_OPERATOR_DENIED: "CROSS_OPERATOR_DENIED",
  FUZZY_NAME_MATCH_REJECTED: "FUZZY_NAME_MATCH_REJECTED",
  CLIENT_SUPPLIED_IDENTITY_REJECTED: "CLIENT_SUPPLIED_IDENTITY_REJECTED",
  INTERNAL_RESEARCH_NOT_AUTHORIZED: "INTERNAL_RESEARCH_NOT_AUTHORIZED",
  REGISTRY_NOT_CONFIGURED: "REGISTRY_NOT_CONFIGURED",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * Normalize allowlist: null/undefined → not configured; [] → empty deny-all.
 */
export function normalizeMarketAllowlist(raw) {
  if (raw == null) {
    return { configured: false, ids: [] };
  }
  if (!Array.isArray(raw)) {
    return { configured: false, ids: [] };
  }
  return {
    configured: true,
    ids: raw.map(text).filter(Boolean),
  };
}

/**
 * Build authorization index from steward-controlled registry only.
 * @param {{ sessions?: object[] }} registry — mapped by verified memberstackMemberId
 */
export function buildPortfolioOperatorAuthIndex(registry = {}) {
  const sessions = Array.isArray(registry.sessions) ? registry.sessions : [];
  const byMember = new Map();

  for (const row of sessions) {
    const operatorId = text(row.operatorId);
    const memberstackMemberId = text(row.memberstackMemberId);
    if (!operatorId || !memberstackMemberId) continue;
    const allow = normalizeMarketAllowlist(row.allowedMarketIds);
    const privileges = Array.isArray(row.privileges)
      ? row.privileges.map(text).filter(Boolean)
      : [];
    byMember.set(memberstackMemberId, {
      operatorId,
      memberstackMemberId,
      email: text(row.email) || null,
      provenanceKind:
        text(row.provenanceKind) || "STEWARD_AUTHORIZED_MEMBER_MAP",
      marketAllowlistConfigured: allow.configured,
      allowedMarketIds: allow.ids,
      privileges,
    });
  }

  return {
    version: GDI_PORTFOLIO_OPERATOR_AUTHORIZATION_V1,
    sessionCount: sessions.length,
    memberCount: byMember.size,
    isConfigured() {
      return byMember.size > 0;
    },

    /**
     * @param {object} caller — must carry serverVerified: true and
     *   verifiedMemberstackMemberId from trusted middleware only.
     */
    resolveCaller(caller = {}) {
      if (caller.clientSuppliedIdentityAttempt === true) {
        return {
          ok: false,
          code: PORTFOLIO_AUTH_DENY.CLIENT_SUPPLIED_IDENTITY_REJECTED,
          operatorId: null,
        };
      }
      if (
        text(caller.companyName) ||
        text(caller.operatorDisplayName) ||
        text(caller.noteText)
      ) {
        // Presence of name/notes identity claims is never accepted as auth.
        if (!caller.serverVerified) {
          return {
            ok: false,
            code: PORTFOLIO_AUTH_DENY.FUZZY_NAME_MATCH_REJECTED,
            operatorId: null,
          };
        }
      }

      if (!caller.serverVerified) {
        return {
          ok: false,
          code: PORTFOLIO_AUTH_DENY.UNAUTHENTICATED,
          operatorId: null,
        };
      }

      const memberId = text(caller.verifiedMemberstackMemberId);
      if (!memberId) {
        return {
          ok: false,
          code: PORTFOLIO_AUTH_DENY.UNAUTHENTICATED,
          operatorId: null,
        };
      }

      if (byMember.size === 0) {
        return {
          ok: false,
          code: PORTFOLIO_AUTH_DENY.REGISTRY_NOT_CONFIGURED,
          operatorId: null,
        };
      }

      const mapped = byMember.get(memberId) || null;
      if (!mapped) {
        return {
          ok: false,
          code: PORTFOLIO_AUTH_DENY.OPERATOR_NOT_MAPPED,
          operatorId: null,
        };
      }

      // Client-requested operatorId may only narrow / must match — never grant.
      const requestedOperatorId = text(caller.requestedOperatorId);
      if (requestedOperatorId && requestedOperatorId !== mapped.operatorId) {
        return {
          ok: false,
          code: PORTFOLIO_AUTH_DENY.CROSS_OPERATOR_DENIED,
          operatorId: mapped.operatorId,
          requestedOperatorId,
        };
      }

      if (!mapped.marketAllowlistConfigured) {
        return {
          ok: false,
          code: PORTFOLIO_AUTH_DENY.MARKET_ALLOWLIST_REQUIRED,
          operatorId: mapped.operatorId,
          message:
            "Steward market allowlist must be explicitly configured; missing allowlist denies all markets.",
        };
      }

      return {
        ok: true,
        operatorId: mapped.operatorId,
        memberstackMemberId: mapped.memberstackMemberId,
        email: mapped.email,
        allowedMarketIds: mapped.allowedMarketIds,
        marketAllowlistConfigured: true,
        privileges: mapped.privileges,
        provenanceKind: mapped.provenanceKind,
        canReadInternalResearch: mapped.privileges.includes(
          "GDI_INTERNAL_RESEARCH_READ",
        ),
      };
    },

    assertOperatorMarketAccess(auth, marketId) {
      if (!auth?.ok) return auth;
      const mid = text(marketId);
      if (!mid) {
        return {
          ok: false,
          code: PORTFOLIO_AUTH_DENY.MARKET_ID_REQUIRED,
          operatorId: auth.operatorId,
        };
      }
      if (!auth.marketAllowlistConfigured) {
        return {
          ok: false,
          code: PORTFOLIO_AUTH_DENY.MARKET_ALLOWLIST_REQUIRED,
          operatorId: auth.operatorId,
          marketId: mid,
        };
      }
      // Empty allowlist → deny all
      if (!auth.allowedMarketIds.length || !auth.allowedMarketIds.includes(mid)) {
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
      "Operator authorization requires steward Memberstack mapping — not company-name or notes matching.",
  };
}

export default {
  GDI_PORTFOLIO_OPERATOR_AUTHORIZATION_V1,
  PORTFOLIO_AUTH_DENY,
  normalizeMarketAllowlist,
  buildPortfolioOperatorAuthIndex,
  rejectFuzzyOperatorAuthorizationAttempt,
};
