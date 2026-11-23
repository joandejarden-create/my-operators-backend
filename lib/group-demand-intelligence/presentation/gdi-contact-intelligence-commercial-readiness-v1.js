/**
 * Commercial readiness mapping for Contact Intelligence V2.2.
 * Preserves Surfe-first on-demand reveal. No Findymail/PDL/FullEnrich activation.
 */

export const GDI_CONTACT_INTELLIGENCE_COMMERCIAL_READINESS_V1 =
  "gdi-contact-intelligence-commercial-readiness-v1";

export function getContactIntelligenceCommercialReadiness() {
  return {
    version: GDI_CONTACT_INTELLIGENCE_COMMERCIAL_READINESS_V1,
    liveCustomerPublication: "CUSTOMER_BLOCKED",
    surfeFirstPreserved: true,
    findymailActivation: "OFF",
    pdlActivation: "OFF",
    fullEnrichActivation: "OFF",
    surfaces: {
      gdiOpportunityDrawer: {
        support:
          "Verified public channels via existing primaryContact* + channel hierarchy display labels; Surfe reveal remains on-demand for missing direct email/phone",
        directVsOrgRoute: "Distinguished by verification statuses / routeType",
        status: "DESIGNED_NOT_CUSTOMER_PUBLISHED",
      },
      verifiedContactAvailability: {
        support:
          "Drawer can show VERIFIED_DIRECT vs ORGANIZATION_ROUTE vs research-required tiers without inventing channels",
        status: "READY_WHEN_PERSISTENCE_AUTHORIZED",
      },
      surfeOnDemandReveal: {
        support: "Unchanged Surfe-first waterfall; public recovery runs before paid reveal",
        status: "PRODUCTION_BEHAVIOR_PRESERVED",
      },
      futureAuthorizedProviders: {
        support:
          "Provider results stay ephemeral corroboration unless founder authorizes storage; never promoted to PUBLIC_SOURCE",
        status: "HOLD",
      },
      hotelSalesWorkflows: {
        support:
          "Hotel-scoped opportunity association; confidential sales feedback remains FS-only / sales-owned",
        status: "DESIGNED",
      },
      opportunityReassignmentAcrossHotels: {
        support:
          "personIdentityKey + durable person index links multiple hotelIds/opportunityIds without duplicating person records",
        status: "FIXTURE_VALIDATED",
      },
    },
    blockers: [
      "LIVE_WRITES_DISABLED",
      "Optional typed personIdentityKey / rights fields pending founder schema approval",
      "Customer display permission must be explicit per channel",
    ],
  };
}

export default {
  GDI_CONTACT_INTELLIGENCE_COMMERCIAL_READINESS_V1,
  getContactIntelligenceCommercialReadiness,
};
