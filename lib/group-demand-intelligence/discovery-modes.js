/**
 * GDI discovery research modes + cadence.
 * Three distinct modes — do not run open-universe every weekly cycle.
 *
 * REUSABLE — hotel-agnostic. No org/event hardcodes.
 */

export const DISCOVERY_MODE = Object.freeze({
  KNOWN_TARGET_MONITORING: "KNOWN_TARGET_MONITORING",
  OPEN_UNIVERSE_DISCOVERY: "OPEN_UNIVERSE_DISCOVERY",
  EVENT_SERIES_EXPANSION: "EVENT_SERIES_EXPANSION",
});

export const DISCOVERY_CADENCE = Object.freeze({
  [DISCOVERY_MODE.KNOWN_TARGET_MONITORING]: {
    cadence: "WEEKLY",
    purpose: "Recurring monitoring of validated Research Targets",
    broadWebDiscovery: false,
  },
  [DISCOVERY_MODE.OPEN_UNIVERSE_DISCOVERY]: {
    cadence: "PERIODIC",
    suggestedIntervalDays: 28,
    purpose: "Discover unknown organizations / event series / programs from hotel profile themes",
    broadWebDiscovery: true,
    note: "Do not run broad web discovery every week — control cost",
  },
  [DISCOVERY_MODE.EVENT_SERIES_EXPANSION]: {
    cadence: "MONTHLY_BOUNDED",
    suggestedIntervalDays: 30,
    purpose: "Known organization/event series → future cycles → destination/venue/housing status",
    broadWebDiscovery: false,
  },
});

/**
 * Promotion path for open-universe hits — never auto-create customer opportunities.
 */
export const OPEN_UNIVERSE_PROMOTION_FLOW = Object.freeze([
  "DISCOVER_CANDIDATE_TARGET",
  "VALIDATE_TARGET",
  "CREATE_RESEARCH_TARGET",
  "RESEARCH_EVENT_OR_PROGRAM",
  "QUALIFY_SIGNAL",
  "CANONICAL_OPPORTUNITY_IF_WARRANTED",
]);

/**
 * @param {string} mode
 * @param {{ lastRunAt?: string|Date|null, now?: Date }} [opts]
 */
export function shouldRunDiscoveryMode(mode, { lastRunAt = null, now = new Date() } = {}) {
  const cfg = DISCOVERY_CADENCE[mode];
  if (!cfg) {
    return { run: false, reason: "UNKNOWN_MODE" };
  }
  if (cfg.cadence === "WEEKLY") {
    return { run: true, reason: "WEEKLY_ALLOWED", mode, cadence: cfg.cadence };
  }
  if (!lastRunAt) {
    return { run: true, reason: "NO_PRIOR_RUN", mode, cadence: cfg.cadence };
  }
  const last = new Date(lastRunAt).getTime();
  if (!Number.isFinite(last)) {
    return { run: true, reason: "INVALID_PRIOR", mode, cadence: cfg.cadence };
  }
  const days = (now.getTime() - last) / (24 * 60 * 60 * 1000);
  const minDays = cfg.suggestedIntervalDays || 28;
  if (days >= minDays) {
    return { run: true, reason: "INTERVAL_ELAPSED", mode, cadence: cfg.cadence, daysSince: days };
  }
  return {
    run: false,
    reason: "CADENCE_HOLD",
    mode,
    cadence: cfg.cadence,
    daysSince: days,
    minDays,
  };
}

export function listDiscoveryModes() {
  return Object.values(DISCOVERY_MODE).map((mode) => ({
    mode,
    ...DISCOVERY_CADENCE[mode],
  }));
}
