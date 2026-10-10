/**
 * True end-to-end hotel onboarding state model V1.
 *
 * LAW: ADP READY does NOT terminate hotel onboarding.
 * COMPLETE requires HI + ADP terminal + GDI terminal (current cycle processed).
 */

export const HOTEL_E2E_ONBOARDING_STATE_VERSION = "hotel-e2e-onboarding-state-v1";

export const HI_STATUS = Object.freeze({
  COMPLETE: "COMPLETE",
  INCOMPLETE: "INCOMPLETE",
  ERROR: "ERROR",
});

export const ADP_STATUS = Object.freeze({
  READY: "READY",
  BASELINE_PUBLISHED: "BASELINE_PUBLISHED",
  MEASUREMENT_CERTIFIED: "MEASUREMENT_CERTIFIED",
  NOT_READY: "NOT_READY",
  UNKNOWN: "UNKNOWN",
});

/** Terminal GDI outcomes — onboarding may complete with any of these. */
export const GDI_TERMINAL_STATUS = Object.freeze({
  PROCESSED_CUSTOMER_READY: "PROCESSED_CUSTOMER_READY",
  PROCESSED_WATCH_ONLY: "PROCESSED_WATCH_ONLY",
  PROCESSED_NO_QUALIFIED_DEMAND: "PROCESSED_NO_QUALIFIED_DEMAND",
  PROCESSED_PUBLIC_DATA_CEILING: "PROCESSED_PUBLIC_DATA_CEILING",
});

/** Non-terminal — onboarding must NOT complete. */
export const GDI_INCOMPLETE_STATUS = Object.freeze({
  GDI_NOT_STARTED: "GDI_NOT_STARTED",
  DISCOVERY_REQUIRED: "DISCOVERY_REQUIRED",
  STALE_PRIOR_CYCLE: "STALE_PRIOR_CYCLE",
  DRY_RUN_ONLY: "DRY_RUN_ONLY",
  BLOCKED_WITHOUT_RESEARCH: "BLOCKED_WITHOUT_RESEARCH",
  INCOMPLETE: "INCOMPLETE",
});

export const GDI_REJECTION_REASON = Object.freeze({
  NO_LODGING_SIGNAL: "NO_LODGING_SIGNAL",
  PLACED_NO_OVERFLOW: "PLACED_NO_OVERFLOW",
  OUT_OF_MARKET: "OUT_OF_MARKET",
  FIT_TOO_LOW: "FIT_TOO_LOW",
  TIMING_UNKNOWN: "TIMING_UNKNOWN",
  STALE_CYCLE: "STALE_CYCLE",
  CONTACT_GAP: "CONTACT_GAP",
  INSUFFICIENT_EVIDENCE: "INSUFFICIENT_EVIDENCE",
  FUTURE_UNCONFIRMED: "FUTURE_UNCONFIRMED",
  OTHER: "OTHER",
});

const ADP_TERMINAL = new Set([
  ADP_STATUS.READY,
  ADP_STATUS.BASELINE_PUBLISHED,
  ADP_STATUS.MEASUREMENT_CERTIFIED,
]);

const GDI_TERMINAL = new Set(Object.values(GDI_TERMINAL_STATUS));

/**
 * @param {string|null|undefined} status
 */
export function isAdpTerminal(status) {
  return ADP_TERMINAL.has(String(status || "").toUpperCase()) ||
    ADP_TERMINAL.has(String(status || ""));
}

/**
 * @param {string|null|undefined} status
 */
export function isGdiTerminal(status) {
  return GDI_TERMINAL.has(String(status || ""));
}

/**
 * @param {{ hiStatus: string, adpStatus: string, gdiStatus: string }} s
 */
export function isHotelOnboardingComplete(s = {}) {
  const hiOk =
    String(s.hiStatus || "").toUpperCase() === HI_STATUS.COMPLETE ||
    String(s.hiStatus || "").toUpperCase() === "HI_COMPLETE";
  const adpOk = isAdpTerminal(s.adpStatus);
  const gdiOk = isGdiTerminal(s.gdiStatus);
  return Boolean(hiOk && adpOk && gdiOk);
}

/**
 * Classify terminal GDI status from a CURRENT cycle summary.
 * Never assign PUBLIC_DATA_CEILING without a proven current cycle.
 *
 * @param {{
 *   currentCycleRan: boolean,
 *   dryRunOnly?: boolean,
 *   discoveryCandidates?: number,
 *   qualifiedForFit?: number,
 *   customerReady?: number,
 *   futureWatch?: number,
 *   rejected?: number,
 *   queries?: number,
 *   sourceFamiliesAttempted?: boolean,
 *   lodgingQualificationAttempted?: boolean,
 *   timingValidationAttempted?: boolean,
 *   whoContactAttempted?: boolean,
 *   followUpResearchAttempted?: boolean,
 * }} cycle
 */
export function classifyGdiTerminalStatus(cycle = {}) {
  if (!cycle.currentCycleRan) {
    return {
      status: GDI_INCOMPLETE_STATUS.DISCOVERY_REQUIRED,
      terminal: false,
      reason: "no_current_gdi_cycle",
    };
  }
  if (cycle.dryRunOnly) {
    return {
      status: GDI_INCOMPLETE_STATUS.DRY_RUN_ONLY,
      terminal: false,
      reason: "dry_run_only_not_processed",
    };
  }

  const candidates = Number(cycle.discoveryCandidates || 0);
  const queries = Number(cycle.queries || 0);
  const customerReady = Number(cycle.customerReady || 0);
  const futureWatch = Number(cycle.futureWatch || 0);
  const qualified = Number(cycle.qualifiedForFit || 0);

  if (queries <= 0 && candidates <= 0) {
    return {
      status: GDI_INCOMPLETE_STATUS.BLOCKED_WITHOUT_RESEARCH,
      terminal: false,
      reason: "cycle_claimed_but_no_queries_or_candidates",
    };
  }

  if (customerReady > 0) {
    return {
      status: GDI_TERMINAL_STATUS.PROCESSED_CUSTOMER_READY,
      terminal: true,
      reason: "customer_ready_gt_0",
    };
  }

  if (futureWatch > 0 && qualified >= 0) {
    const ceilingProven =
      cycle.sourceFamiliesAttempted !== false &&
      cycle.lodgingQualificationAttempted !== false &&
      cycle.timingValidationAttempted !== false &&
      (cycle.followUpResearchAttempted !== false || candidates > 0);

    if (ceilingProven && candidates > 0 && customerReady === 0) {
      // Watch-only is preferred when watches exist; ceiling only when
      // evidence shows public-data exhaustion rather than mere watch backlog.
      if (cycle.publicDataCeilingClaimed === true) {
        return {
          status: GDI_TERMINAL_STATUS.PROCESSED_PUBLIC_DATA_CEILING,
          terminal: true,
          reason: "current_cycle_proved_ceiling_with_watch",
        };
      }
      return {
        status: GDI_TERMINAL_STATUS.PROCESSED_WATCH_ONLY,
        terminal: true,
        reason: "watch_gt_0_customer_ready_0",
      };
    }
    return {
      status: GDI_TERMINAL_STATUS.PROCESSED_WATCH_ONLY,
      terminal: true,
      reason: "watch_gt_0_customer_ready_0",
    };
  }

  if (candidates === 0 && queries > 0) {
    return {
      status: GDI_TERMINAL_STATUS.PROCESSED_NO_QUALIFIED_DEMAND,
      terminal: true,
      reason: "discovery_ran_zero_candidates",
    };
  }

  if (candidates > 0 && customerReady === 0 && futureWatch === 0) {
    const ceilingProven =
      cycle.sourceFamiliesAttempted !== false &&
      cycle.lodgingQualificationAttempted !== false &&
      cycle.timingValidationAttempted !== false;
    if (ceilingProven && cycle.publicDataCeilingClaimed === true) {
      return {
        status: GDI_TERMINAL_STATUS.PROCESSED_PUBLIC_DATA_CEILING,
        terminal: true,
        reason: "current_cycle_proved_ceiling",
      };
    }
    return {
      status: GDI_TERMINAL_STATUS.PROCESSED_NO_QUALIFIED_DEMAND,
      terminal: true,
      reason: "candidates_but_none_qualified_customer_or_watch",
    };
  }

  return {
    status: GDI_TERMINAL_STATUS.PROCESSED_NO_QUALIFIED_DEMAND,
    terminal: true,
    reason: "processed_zero_ready",
  };
}

/**
 * Map free-text / hygiene hold reasons into canonical rejection buckets.
 * @param {string} raw
 */
export function mapGdiRejectionReason(raw) {
  const s = String(raw || "").toLowerCase();
  if (!s) return GDI_REJECTION_REASON.OTHER;
  if (/lodging|room.?demand|housing.?evidence|no.?lodging/.test(s)) {
    return GDI_REJECTION_REASON.NO_LODGING_SIGNAL;
  }
  if (/placed|fully.?placed|no.?overflow|commercial_not_open/.test(s)) {
    return GDI_REJECTION_REASON.PLACED_NO_OVERFLOW;
  }
  if (/out.?of.?market|outside.?territory|geography|geo_/.test(s)) {
    return GDI_REJECTION_REASON.OUT_OF_MARKET;
  }
  if (/fit|surface_eligibility|boutique|scale|capability/.test(s)) {
    return GDI_REJECTION_REASON.FIT_TOO_LOW;
  }
  if (/timing|date.?unknown|event.?date|no.?date/.test(s)) {
    return GDI_REJECTION_REASON.TIMING_UNKNOWN;
  }
  if (/stale|past.?event|expired/.test(s)) {
    return GDI_REJECTION_REASON.STALE_CYCLE;
  }
  if (/contact|who|surface_eligibility.*who|no.?contact/.test(s)) {
    return GDI_REJECTION_REASON.CONTACT_GAP;
  }
  if (/insufficient|evidence|invalid|unverif/.test(s)) {
    return GDI_REJECTION_REASON.INSUFFICIENT_EVIDENCE;
  }
  if (/future|unconfirm|valid_watch|hold_watch/.test(s)) {
    return GDI_REJECTION_REASON.FUTURE_UNCONFIRMED;
  }
  return GDI_REJECTION_REASON.OTHER;
}

/**
 * Count rejection reasons from hygiene rows / qualification objects.
 * @param {Array<object>} rows
 */
export function tallyRejectionReasons(rows = []) {
  const tallies = Object.fromEntries(
    Object.values(GDI_REJECTION_REASON).map((k) => [k, 0])
  );
  for (const row of rows) {
    const raw =
      row.holdReason ||
      row.rejectionReason ||
      row.reason ||
      row.actionabilityV3 ||
      row.state ||
      row.opportunityQualification ||
      "";
    const key = mapGdiRejectionReason(raw);
    tallies[key] += 1;
  }
  return tallies;
}

/**
 * Evaluate end-to-end onboarding for one hotel.
 * @param {{
 *   hotelId: string,
 *   hotelName?: string,
 *   hiStatus: string,
 *   adpStatus: string,
 *   adpAttributeCount?: number,
 *   gdiCycle: object,
 * }} input
 */
export function evaluateHotelE2eOnboarding(input = {}) {
  const gdiClass = classifyGdiTerminalStatus(input.gdiCycle || {});
  const hiStatus =
    String(input.hiStatus || "").toUpperCase() === "HI_COMPLETE"
      ? HI_STATUS.COMPLETE
      : String(input.hiStatus || HI_STATUS.INCOMPLETE);
  const adpStatus = String(input.adpStatus || ADP_STATUS.UNKNOWN);
  const complete = isHotelOnboardingComplete({
    hiStatus,
    adpStatus,
    gdiStatus: gdiClass.status,
  });

  return {
    version: HOTEL_E2E_ONBOARDING_STATE_VERSION,
    hotelId: input.hotelId || null,
    hotelName: input.hotelName || null,
    hiStatus,
    adpStatus,
    adpAttributeCount: input.adpAttributeCount ?? null,
    gdiStatus: gdiClass.status,
    gdiTerminal: gdiClass.terminal,
    gdiClassifyReason: gdiClass.reason,
    gdiResearchRequired: !gdiClass.terminal,
    hotelOnboardingComplete: complete,
    // Explicit anti-pattern guard
    adpReadyDoesNotCompleteOnboarding: true,
    law: "HI_COMPLETE && ADP_TERMINAL && GDI_TERMINAL",
  };
}

/**
 * Founder-facing verdict label from terminal GDI + ADP ready.
 * @param {{ adpStatus: string, gdiStatus: string }} s
 */
export function founderAdpGdiVerdict(s = {}) {
  if (!isAdpTerminal(s.adpStatus)) return "ADP NOT READY";
  switch (s.gdiStatus) {
    case GDI_TERMINAL_STATUS.PROCESSED_CUSTOMER_READY:
      return "ADP READY — GDI CUSTOMER READY";
    case GDI_TERMINAL_STATUS.PROCESSED_WATCH_ONLY:
      return "ADP READY — GDI WATCH ONLY";
    case GDI_TERMINAL_STATUS.PROCESSED_NO_QUALIFIED_DEMAND:
      return "ADP READY — GDI NO QUALIFIED CURRENT DEMAND";
    case GDI_TERMINAL_STATUS.PROCESSED_PUBLIC_DATA_CEILING:
      return "ADP READY — GDI VERIFIED PUBLIC DATA CEILING";
    default:
      return "ADP READY — GDI INCOMPLETE (RESEARCH REQUIRED)";
  }
}

/**
 * Flags for HI orchestrator return payloads.
 *
 * LAW: never set adpGdiReady=true as an e2e-complete signal from HI alone.
 * hotelOnboardingComplete requires HI + ADP terminal + GDI terminal.
 *
 * @param {{
 *   hiComplete?: boolean,
 *   adpStatus?: string,
 *   hotelId?: string,
 *   hotelName?: string,
 *   adpAttributeCount?: number|null,
 *   gdiCycle?: object|null,
 *   gdiEligible?: boolean,
 * }} input
 */
export function buildE2eOnboardingFlags(input = {}) {
  const hiComplete = Boolean(input.hiComplete);
  const adpStatus = String(input.adpStatus || ADP_STATUS.UNKNOWN);
  const gdiCycle =
    input.gdiCycle && typeof input.gdiCycle === "object"
      ? input.gdiCycle
      : { currentCycleRan: false };

  const e2e = evaluateHotelE2eOnboarding({
    hotelId: input.hotelId || null,
    hotelName: input.hotelName || null,
    hiStatus: hiComplete ? HI_STATUS.COMPLETE : HI_STATUS.INCOMPLETE,
    adpStatus,
    adpAttributeCount: input.adpAttributeCount ?? null,
    gdiCycle,
  });

  const gdiResearchRequired =
    e2e.gdiResearchRequired === true ||
    !e2e.gdiTerminal ||
    // Every new hotel needs GDI unless a current cycle already covers it.
    gdiCycle.currentCycleRan !== true;

  return {
    ...e2e,
    // HI gate only — GDI may initialize after HI complete; NOT e2e complete.
    gdiEligible: input.gdiEligible !== false && hiComplete,
    // DEPRECATED as e2e-complete signal — always false for onboarding complete.
    adpGdiReady: false,
    gdiResearchRequired,
    hotelOnboardingComplete: e2e.hotelOnboardingComplete,
    founderVerdict: founderAdpGdiVerdict(e2e),
    gdiCurrentCycleRequirement: true,
  };
}
