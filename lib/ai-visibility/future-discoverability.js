/**
 * Discoverability / Referral / Business Impact UI envelopes (Phase 3C.1).
 * Replaces OpenAI-only placeholder with provider-neutral foundation.
 * No synthetic zeros.
 */

import { AVAILABILITY } from "./availability-states.js";
import {
  buildDiscoverabilityExecutiveBlock,
  buildDiscoverabilityDetailBlock,
} from "./discoverability-read-service.js";

export const DISCOVERABILITY_UI_VERSION = "ai_visibility_discoverability_ui_v1";

/** @deprecated Use discoverabilityBusinessImpact — kept for backward compat */
export const OPENAI_DISCOVERABILITY_STATUS = Object.freeze({
  CRAWL_DATA_CONNECTED: false,
  SERVER_LOG_DATA_CONNECTED: false,
  ANALYTICS_CONNECTED: false,
  CRM_ACTION_DATA_CONNECTED: false,
  SYNTHETIC_VALUES_DISPLAYED: false,
  PROVIDER_NEUTRAL: true,
});

export const DISCOVERABILITY_COMING_LATER_NOTE =
  "Public crawl baseline not yet run. Referral and business impact require governed analytics or log connections — never shown as zero when disconnected.";

/**
 * Executive Summary — Discoverability & Business Impact block.
 */
export function buildDiscoverabilityExecutivePlaceholder(opts = {}) {
  const block = buildDiscoverabilityExecutiveBlock(opts);
  return {
    status: opts.publicChecks ? AVAILABILITY.OBSERVED : AVAILABILITY.FUTURE_READY,
    ...block,
    comingLaterNote: DISCOVERABILITY_COMING_LATER_NOTE,
    ...OPENAI_DISCOVERABILITY_STATUS,
  };
}

/** @deprecated alias */
export function buildOpenAiDiscoverabilityExecutivePlaceholder(opts = {}) {
  return buildDiscoverabilityExecutivePlaceholder(opts);
}

/**
 * Detailed View — Discoverability / Referral / Business Impact.
 */
export function buildDiscoverabilityDetailPlaceholder(opts = {}) {
  const block = buildDiscoverabilityDetailBlock(opts);
  return {
    status: opts.publicChecks ? AVAILABILITY.OBSERVED : AVAILABILITY.FUTURE_READY,
    ...block,
    comingLaterNote: DISCOVERABILITY_COMING_LATER_NOTE,
    ...OPENAI_DISCOVERABILITY_STATUS,
  };
}

/** @deprecated alias */
export function buildOpenAiDiscoverabilityDetailPlaceholder(opts = {}) {
  return buildDiscoverabilityDetailPlaceholder(opts);
}
