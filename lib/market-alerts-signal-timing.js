/**
 * Deterministic Signal Timing for Market Alerts (internal, not user-facing).
 * Values: Pre-Decision | Decision Forming | Decision Announced | Post-Decision
 */

export const SIGNAL_TIMING_VALUES = [
  "Pre-Decision",
  "Decision Forming",
  "Decision Announced",
  "Post-Decision",
];

const PRE_DECISION_EVENTS = new Set([
  "Site Acquisition",
  "Planning Application",
  "Development Proposal",
  "Adaptive Reuse Proposal",
]);

const DECISION_FORMING_EVENTS = new Set([
  "Planning Approval",
  "New Development",
  "Financing",
  "JV",
  "Construction Start",
]);

const DECISION_ANNOUNCED_EVENTS = new Set([
  "Brand Signing",
  "Management Agreement",
  "Operator Appointment",
  "Operator Change",
  "Reflag",
  "Conversion",
  "Acquisition",
  "Sale",
  "Portfolio Acquisition",
  "Brand Exit",
  "Operator Exit",
]);

const POST_DECISION_EVENTS = new Set(["Hotel For Sale", "Refinancing", "Major Renovation"]);

const OPENING_RE =
  /\b((?:hotel|resort|inn|suites)\s+(?:opens?|opened|debuts?)|(?:opens?|opened|debuts?|now open)\s+(?:as |its )?(?:a )?(?:new )?(?:hotel|resort)|celebrates? (?:its )?opening)\b/i;

const CONSTRUCTION_FINANCING_RE =
  /\b(credit approval|construction (?:loan|financing|debt)|development (?:financing|loan|equity|capital|funding|facility)|project (?:loan|financing)|debt package)\b/i;

const SUBMITTED_NOT_APPROVED_RE =
  /\b(submitted|filed|lodged|application|seeks? (?:planning|zoning|approval|permission))\b/i;

const APPROVED_RE =
  /\b(approved|approval|permission granted|entitlement(?:s)? (?:approved|secured)|receives? approval)\b/i;

/**
 * @param {{ eventType?: string|null, title?: string, summary?: string, entities?: object }} input
 * @returns {"Pre-Decision"|"Decision Forming"|"Decision Announced"|"Post-Decision"|null}
 */
export function inferSignalTiming(input = {}) {
  const eventType = input.eventType || null;
  const text = `${input.title || ""} ${input.summary || ""}`.trim();

  if (OPENING_RE.test(text) && eventType !== "Construction Start") {
    return "Post-Decision";
  }

  if (eventType === "Planning Application" || (SUBMITTED_NOT_APPROVED_RE.test(text) && !APPROVED_RE.test(text) && /\b(hotel|resort)\b/i.test(text))) {
    if (eventType === "Planning Application" || eventType === "Adaptive Reuse Proposal" || eventType === "Development Proposal") {
      return "Pre-Decision";
    }
  }

  if (eventType === "Financing" && CONSTRUCTION_FINANCING_RE.test(text)) {
    return "Decision Forming";
  }

  if (eventType === "Refinancing") return "Post-Decision";

  if (eventType === "Construction Start") return "Decision Forming";

  if (PRE_DECISION_EVENTS.has(eventType)) return "Pre-Decision";
  if (DECISION_FORMING_EVENTS.has(eventType)) return "Decision Forming";
  if (DECISION_ANNOUNCED_EVENTS.has(eventType)) return "Decision Announced";
  if (POST_DECISION_EVENTS.has(eventType)) return "Post-Decision";

  if (!eventType) return null;
  return "Decision Forming";
}

export function isEarlyLifecycleTiming(timing) {
  return timing === "Pre-Decision" || timing === "Decision Forming";
}
