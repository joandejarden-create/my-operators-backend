/**
 * Hotel Explorer Guest Sentiment — reversible founder feature flag.
 *
 * Default: DISABLED (HE_GUEST_SENTIMENT_ENABLED=false).
 * Enabling the env flag never overrides publication-rights gates in the model/API.
 */

export const HE_GUEST_SENTIMENT_FEATURE_FLAG_V1 =
  "he-guest-sentiment-feature-flag-v1";

export const HE_GUEST_SENTIMENT_TAB_ID = "sentiment";

/** Certification / closeout vocabulary when the customer tab is off. */
export const HE_GUEST_SENTIMENT_DISABLED_BY_FOUNDER = "DISABLED_BY_FOUNDER";

/**
 * @param {NodeJS.ProcessEnv | Record<string, string | undefined>} [env]
 * @returns {boolean}
 */
export function isHeGuestSentimentEnabled(env = process.env) {
  const raw = String(env?.HE_GUEST_SENTIMENT_ENABLED ?? "false")
    .trim()
    .toLowerCase();
  return raw === "1" || raw === "true" || raw === "yes" || raw === "on";
}

/**
 * @param {NodeJS.ProcessEnv | Record<string, string | undefined>} [env]
 */
export function getHeGuestSentimentFeatureState(env = process.env) {
  const enabled = isHeGuestSentimentEnabled(env);
  return {
    version: HE_GUEST_SENTIMENT_FEATURE_FLAG_V1,
    envKey: "HE_GUEST_SENTIMENT_ENABLED",
    enabled,
    customerTabVisible: enabled,
    status: enabled
      ? "CONFIG_ENABLED_RIGHTS_GATED"
      : HE_GUEST_SENTIMENT_DISABLED_BY_FOUNDER,
    // Flag never authorizes licensed publication by itself.
    overridesPublicationRights: false,
    paidProviders: false,
    scrapingAuthorized: false,
    scheduledCollectionAuthorized: false,
  };
}

/**
 * Safe customer API body when the founder flag is off — no platform payloads.
 * @param {string} hotelId
 */
export function buildGuestSentimentDisabledByFounderResponse(hotelId) {
  return {
    success: true,
    available: false,
    visible: false,
    disabledByFounder: true,
    status: HE_GUEST_SENTIMENT_DISABLED_BY_FOUNDER,
    certificationStatus: HE_GUEST_SENTIMENT_DISABLED_BY_FOUNDER,
    reason: "guest_sentiment_disabled_by_founder",
    version: HE_GUEST_SENTIMENT_FEATURE_FLAG_V1,
    hotelId: String(hotelId || "").trim() || null,
    hotelName: null,
    platforms: [],
    platformOverview: [],
    overviewPlatformCount: 0,
    themes: [],
    categoryPerformance: null,
    executiveInsights: [],
    reviewThemes: {
      available: false,
      reason: HE_GUEST_SENTIMENT_DISABLED_BY_FOUNDER,
      items: [],
    },
    insights: null,
    trends: { available: false, reason: HE_GUEST_SENTIMENT_DISABLED_BY_FOUNDER },
    limitations: [
      "Guest Sentiment is temporarily disabled by founder configuration pending licensing / redistribution rights review.",
    ],
    rightsChip: null,
    canonicalSource: null,
    observedAt: null,
    stagedPacketPath: null,
    applyAuthorized: false,
    paidProviders: false,
    scrapingAuthorized: false,
  };
}
