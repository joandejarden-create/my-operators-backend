/**
 * GDI SVG / icon-state presentation contract V1.
 *
 * Single meaning table for customer-facing opportunity icons.
 * Auth PACKAGE preview and external share must map the same approved fields
 * through the same states — no route-specific coloring logic.
 *
 * Contract: GDI_SVG_ICON_STATE_CONTRACT_V1
 */
import {
  REVENUE_STREAM_ICON_STATE,
  mapRevenueStreamClassToIconStates,
} from "../opportunity-revenue-stream-icons-v1.js";
import { HOTEL_REVENUE_STREAM_CLASS } from "../multi-revenue-qualification-v1.js";

export const GDI_SVG_ICON_STATE_CONTRACT_V1 = "GDI_SVG_ICON_STATE_CONTRACT_V1";
export const SVG_ICON_STATE_CONTRACT_VERSION = "gdi-svg-icon-state-contract-v1";

/** Customer-visible visual states for revenue-stream SVGs. */
export const SVG_VISUAL_STATE = Object.freeze({
  ACTIVE: REVENUE_STREAM_ICON_STATE.ACTIVE, // gold / confirmed applicable
  MUTED: REVENUE_STREAM_ICON_STATE.MUTED, // gray / not indicated for this row
});

/**
 * Semantic meanings used in founder/QA documentation.
 * Visual rendering collapses to ACTIVE vs MUTED for revenue-stream icons.
 */
export const SVG_SEMANTIC_MEANING = Object.freeze({
  ENABLED: "ACTIVE — stream applicable per approved evidence",
  DISABLED: "MUTED — stream not applicable or not indicated",
  AVAILABLE: "ACTIVE — capacity/stream available in approved thesis",
  CONFIRMED: "ACTIVE — approved publication confirms the stream",
  POTENTIAL: "ACTIVE only when approved class/thesis supports it; never invent",
  UNKNOWN: "MUTED — revenue stream unconfirmed",
  NOT_APPLICABLE: "MUTED — stream not part of this opportunity",
});

export const SUPPORTED_REVENUE_STREAM_ICON_FIXTURES = Object.freeze([
  {
    id: "GUESTROOMS_ONLY",
    class: HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY,
    expected: { guestrooms: "active", eventFnb: "muted" },
  },
  {
    id: "EVENT_FNB_ONLY",
    class: HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY,
    expected: { guestrooms: "muted", eventFnb: "active" },
  },
  {
    id: "COMBINED",
    class: HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB,
    expected: { guestrooms: "active", eventFnb: "active" },
  },
  {
    id: "UNCONFIRMED",
    class: HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED,
    expected: { guestrooms: "muted", eventFnb: "muted" },
  },
]);

export function resolveCanonicalRevenueStreamIconStates(revenueStreamClass) {
  return mapRevenueStreamClassToIconStates(revenueStreamClass);
}

export function describeSvgIconStateContract() {
  return {
    version: SVG_ICON_STATE_CONTRACT_VERSION,
    contractId: GDI_SVG_ICON_STATE_CONTRACT_V1,
    visualStates: SVG_VISUAL_STATE,
    semanticMeanings: SVG_SEMANTIC_MEANING,
    fixtures: SUPPORTED_REVENUE_STREAM_ICON_FIXTURES,
    owningModules: [
      "lib/group-demand-intelligence/opportunity-revenue-stream-icons-v1.js",
      "lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js",
      "public/js/group-demand-intelligence/dealality-gdi-ui.js#resolveRevenueStreamIconState",
    ],
    rule: "Prefer canonicalCustomerPresentation.icons when present; never invent catering gold without event thesis.",
  };
}
