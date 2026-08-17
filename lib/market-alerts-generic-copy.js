/**
 * Neutral copy for generic / all Market Alerts audience.
 * Do not reuse Owner / Brand / Operator template language.
 */

const OPEN_DEV_SIGNALS = new Set([
  "Potential Development Opportunity",
  "Potential Management Opportunity",
  "New Development Opportunity",
  "Potential Conversion Opportunity",
  "Brand White-Space Signal",
  "Reflag Opportunity",
  "Turnaround / Repositioning Opportunity",
]);

/**
 * @param {{
 *   actionable?: boolean,
 *   worthReviewing?: boolean,
 *   eventType?: string|null,
 *   signalTypes?: Array<string|null|undefined>,
 * }} input
 */
export function buildGenericAudienceCopy(input = {}) {
  const actionable = !!input.actionable;
  const worthReviewing = !!input.worthReviewing;
  const eventType = input.eventType || null;
  const signalTypes = (input.signalTypes || []).filter(Boolean);

  if (actionable) {
    const hasOpenDev = signalTypes.some((s) => OPEN_DEV_SIGNALS.has(s));
    const isSale = eventType === "Hotel For Sale";
    return {
      signalType: isSale && !hasOpenDev ? "Hotel Transaction Opportunity" : "Potential Hotel Development Opportunity",
      whyItMatters:
        "A hotel project is progressing and key commercial decisions remain publicly unresolved.",
      recommendedAction: "Review the project and monitor the next development milestone.",
    };
  }

  if (worthReviewing) {
    return {
      signalType: "Market Intelligence",
      whyItMatters:
        "This is important market intelligence. No currently open action window is established.",
      recommendedAction: "Review the announcement for competitive and market context.",
    };
  }

  return {
    signalType: null,
    whyItMatters: null,
    recommendedAction: null,
  };
}
