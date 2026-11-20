/**
 * Conditional Y1 review for YOTEL Geneva Lake airport attributes.
 * Confirms evidence posture — does NOT enable customer-facing RG definitions.
 */

import { ATTRIBUTE_DEFINITIONS } from "../intelligence/reality-gap.js";
import { hasAttributeRecognitionKeywords } from "../execution/attribute-recognition-keywords-v1.js";
import { loadPropertyProfile } from "../data-model.js";

export const ADP_YOTEL_AIRPORT_ATTRIBUTE_REVIEW_V1 =
  "ADP_YOTEL_AIRPORT_ATTRIBUTE_REVIEW_V1";

const YOTEL_ID = "adp_yotel_geneva_lake";
const CANDIDATES = ["airport_shuttle", "park_and_fly"];

/**
 * Evidence-only review. No ATTRIBUTE_DEFINITIONS mutation.
 */
export function reviewYotelAirportAttributes(profile = null) {
  const propertyProfile = profile || loadPropertyProfile(YOTEL_ID);
  if (!propertyProfile || propertyProfile.propertyId !== YOTEL_ID) {
    return {
      ok: false,
      error: "yotel_profile_required",
      contractVersion: ADP_YOTEL_AIRPORT_ATTRIBUTE_REVIEW_V1,
    };
  }

  const selected = propertyProfile.attributes || [];
  const positioning = [
    propertyProfile.positioning?.primary,
    ...(propertyProfile.positioning?.differentiators || []),
  ]
    .filter(Boolean)
    .join(" | ");

  const rows = CANDIDATES.map((key) => {
    const inSelection = selected.includes(key);
    const defined = Boolean(ATTRIBUTE_DEFINITIONS[key]);
    const recognition = hasAttributeRecognitionKeywords(key);
    const text = positioning.toLowerCase();
    const shuttleMention =
      /shuttle/.test(text) || /airport/.test(text) || /geneva airport/.test(text);
    const parkFlyMention = /park/.test(text) && /fly/.test(text);

    let factualSupport = "INSUFFICIENT";
    let strategicMeaning = null;
    if (key === "airport_shuttle") {
      factualSupport = shuttleMention ? "PROFILE_POSITIONING_HINT" : "NOT_CORROBORATED_IN_PROFILE_FIELDS";
      strategicMeaning =
        "Intended as airport-accessible transfer / shuttle convenience for La Côte / Geneva Airport demand — not downtown CBD positioning";
    }
    if (key === "park_and_fly") {
      factualSupport = parkFlyMention ? "PROFILE_POSITIONING_HINT" : "NOT_CORROBORATED_IN_PROFILE_FIELDS";
      strategicMeaning =
        "Intended as park-and-fly / airport-adjacent parking convenience for travelers using Geneva Airport";
    }

    return {
      attribute: key,
      inStrategicSelection: inSelection,
      definitionPresent: defined,
      recognitionRulePresent: recognition,
      factualSupport,
      strategicMeaning,
      customerFacingEnablement: "BLOCKED_PENDING_FOUNDER_Y1",
      historicalImmutable: true,
      recommendedNextStep:
        factualSupport === "PROFILE_POSITIONING_HINT"
          ? "Steward-confirm first-party service page evidence, then propose DEFINITIONS + recognition keywords for NEW periods only"
          : "Do not fabricate shuttle/parking capability — gather first-party evidence before DEFINE",
    };
  });

  return {
    ok: true,
    contractVersion: ADP_YOTEL_AIRPORT_ATTRIBUTE_REVIEW_V1,
    propertyId: YOTEL_ID,
    rows,
    autoEnabledInCustomerRealityGap: false,
    note: "Y1 conditional — analysis only; no definition/parser enablement in PR-2",
  };
}
