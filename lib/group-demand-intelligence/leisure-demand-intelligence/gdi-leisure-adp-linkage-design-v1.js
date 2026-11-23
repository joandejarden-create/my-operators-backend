/**
 * Optional future ADP ↔ LDI linkage — DESIGN ONLY.
 * ADP is not required to operate Leisure Demand Intelligence.
 */

export const GDI_LEISURE_ADP_LINKAGE_DESIGN_V1 =
  "gdi-leisure-adp-linkage-design-v1";

export function designLeisureAdpLinkage() {
  return {
    schema: GDI_LEISURE_ADP_LINKAGE_DESIGN_V1,
    requiredForLdi: false,
    autoCustomerAdpRecommendations: false,
    proposedLinks: [
      {
        from: "verified_leisure_event_or_attraction",
        to: "adp_ai_hotel_discovery_positioning_candidate",
        condition:
          "Only when independent ADP evidence validates a positioning opportunity; never auto-emit customer ADP copy from LDI alone.",
      },
      {
        from: "campaign_driven_destination_demand",
        to: "adp_seasonality_or_intent_signal_watch",
        condition: "Internal watchlist only until ADP Full E2E evidence gate passes.",
      },
    ],
    thisPhase: {
      adpAudits: "NONE",
      adpResearch: "NONE",
      customerFacingAdp: "NONE",
    },
  };
}
