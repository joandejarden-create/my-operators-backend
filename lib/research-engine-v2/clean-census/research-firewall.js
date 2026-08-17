/**
 * Hard research firewall — legacy census values MUST NOT enter independent research.
 */

export class ResearchFirewallError extends Error {
  constructor(message) {
    super(message);
    this.name = "ResearchFirewallError";
  }
}

/**
 * @param {{ phase: 'independent_research'|'frozen'|'legacy_reconciliation'|'legacy_only_challenge' }} opts
 */
export function createResearchFirewall(opts = {}) {
  const phase = opts.phase || "independent_research";
  /** @type {object|null} */
  let frozenSnapshot = null;
  let freezeTimestamp = null;
  let legacyLoadedAt = null;

  const state = {
    phase,
    legacyAccessAttempts: 0,
    blockedAttempts: [],
  };

  function assertIndependentPhase(action) {
    if (state.phase === "independent_research" || state.phase === "frozen") {
      if (action === "read_legacy") {
        state.legacyAccessAttempts++;
        state.blockedAttempts.push({
          at: new Date().toISOString(),
          action,
          phase: state.phase,
        });
        throw new ResearchFirewallError(
          `FIREWALL: legacy census access blocked during phase=${state.phase}. Freeze independent output before reconciliation.`
        );
      }
    }
  }

  return {
    getPhase: () => state.phase,
    getAudit: () => ({
      ...state,
      freezeTimestamp,
      legacyLoadedAt,
      frozenRecordCount: frozenSnapshot?.records?.length ?? null,
    }),

    /**
     * Independent research helpers must call this before any query generation.
     * @param {object} context
     */
    assertNoLegacyInContext(context = {}) {
      const banned = [
        "legacyHotels",
        "legacyCensus",
        "censusHotels",
        "currentCensus",
        "strRows",
        "legacyNames",
        "legacyValues",
      ];
      for (const key of banned) {
        if (context[key] != null) {
          state.blockedAttempts.push({ at: new Date().toISOString(), key, phase: state.phase });
          throw new ResearchFirewallError(
            `FIREWALL: context key "${key}" is legacy-contaminated and cannot be used in independent research`
          );
        }
      }
      // Detect accidental seed lists that look like census dumps
      if (Array.isArray(context.seedHotelNames) && context.seedHotelNames.length) {
        throw new ResearchFirewallError(
          "FIREWALL: seedHotelNames forbidden — discover from official directories only"
        );
      }
    },

    /** @param {() => any} fn */
    withLegacyBlocked(fn) {
      assertIndependentPhase("read_legacy");
      return fn();
    },

    requestLegacyCensus(loaderFn) {
      if (state.phase !== "legacy_reconciliation" && state.phase !== "legacy_only_challenge") {
        assertIndependentPhase("read_legacy");
      }
      if (!frozenSnapshot) {
        throw new ResearchFirewallError("FIREWALL: cannot load legacy until independent universe is frozen");
      }
      legacyLoadedAt = new Date().toISOString();
      return loaderFn();
    },

    /**
     * @param {object} snapshot - immutable independent universe
     */
    freezeIndependentUniverse(snapshot) {
      if (state.phase !== "independent_research") {
        throw new ResearchFirewallError(`Cannot freeze from phase=${state.phase}`);
      }
      freezeTimestamp = new Date().toISOString();
      frozenSnapshot = Object.freeze({
        ...snapshot,
        frozenAt: freezeTimestamp,
        frozen: true,
      });
      // Deep-freeze records array shallowly
      Object.freeze(frozenSnapshot.records);
      state.phase = "frozen";
      return frozenSnapshot;
    },

    getFrozenUniverse() {
      if (!frozenSnapshot) throw new ResearchFirewallError("No frozen independent universe");
      return frozenSnapshot;
    },

    beginLegacyReconciliation() {
      if (state.phase !== "frozen") {
        throw new ResearchFirewallError("Must freeze independent universe before legacy reconciliation");
      }
      state.phase = "legacy_reconciliation";
    },

    beginLegacyOnlyChallenge() {
      if (state.phase !== "legacy_reconciliation" && state.phase !== "legacy_only_challenge") {
        throw new ResearchFirewallError("Legacy-only challenge requires reconciliation phase");
      }
      state.phase = "legacy_only_challenge";
    },
  };
}
