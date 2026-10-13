/**
 * Contact confidence classification for Market Alerts (Phase A).
 */

export const CONTACT_CONFIDENCE = Object.freeze({
  HIGH: "HIGH",
  MEDIUM: "MEDIUM",
  LOW: "LOW",
});

/**
 * @param {{
 *   namedInArticle?: boolean,
 *   titleCompanyConfirmed?: boolean,
 *   hasVerifiedWorkEmail?: boolean,
 *   hasBusinessContact?: boolean,
 *   strongCompanyRoleMatch?: boolean,
 *   currentEmploymentConfirmed?: boolean,
 *   inferredIdentity?: boolean,
 *   weakRoleMatch?: boolean,
 *   ambiguousMatch?: boolean,
 * }} signals
 */
export function classifyContactConfidence(signals = {}) {
  if (signals.ambiguousMatch || signals.inferredIdentity || signals.weakRoleMatch) {
    return CONTACT_CONFIDENCE.LOW;
  }

  const high =
    signals.namedInArticle === true &&
    signals.titleCompanyConfirmed === true &&
    (signals.hasVerifiedWorkEmail === true || signals.hasBusinessContact === true);
  if (high) return CONTACT_CONFIDENCE.HIGH;

  const medium =
    signals.strongCompanyRoleMatch === true &&
    signals.currentEmploymentConfirmed === true &&
    (signals.hasVerifiedWorkEmail === true || signals.hasBusinessContact === true || signals.titleCompanyConfirmed === true);
  if (medium) return CONTACT_CONFIDENCE.MEDIUM;

  if (signals.namedInArticle && signals.titleCompanyConfirmed) {
    return CONTACT_CONFIDENCE.MEDIUM;
  }

  if (signals.strongCompanyRoleMatch) {
    return CONTACT_CONFIDENCE.MEDIUM;
  }

  return CONTACT_CONFIDENCE.LOW;
}

/** UI should hide LOW by default unless explicitly requested. */
export function shouldDisplayContactByDefault(confidence) {
  return confidence === CONTACT_CONFIDENCE.HIGH || confidence === CONTACT_CONFIDENCE.MEDIUM;
}
