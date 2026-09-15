/**
 * GDI research-method stubs that plug into canonical Dealality research-methods patterns.
 * Readiness: EXPERIMENTAL — not auto-promoted to production HI playbooks.
 */

export const GDI_METHOD_PACK_VERSION = "gdi-research-methods-v1-experimental";

export const GDI_RESEARCH_METHODS = Object.freeze([
  {
    id: "GDI-ASSOC-EVENT-01",
    name: "Association future meeting discovery",
    readiness: "EXPERIMENTAL",
    level: "L2_CANONICAL_DEALALITY_RESEARCH_METHODS",
    queries: [
      "{org} annual meeting {year} venue",
      "{org} save the date {year}",
      "{org} call for exhibitors {year}",
    ],
  },
  {
    id: "GDI-MED-SCI-01",
    name: "Medical / scientific conference discovery",
    readiness: "EXPERIMENTAL",
    level: "L2_CANONICAL_DEALALITY_RESEARCH_METHODS",
    queries: [
      "{topic} conference {year} Bethesda OR NIH OR \"Washington DC\"",
      "{society} annual meeting destination",
    ],
  },
  {
    id: "GDI-HIST-MEETING-01",
    name: "Historical meeting timeline reconstruction",
    readiness: "EXPERIMENTAL",
    level: "L2_CANONICAL_DEALALITY_RESEARCH_METHODS",
    queries: [
      "{event} {year} hotel venue",
      "{event} past locations",
    ],
  },
  {
    id: "GDI-GOV-EVENT-01",
    name: "Public government / contractor event discovery",
    readiness: "EXPERIMENTAL",
    level: "L3_STANDARD_APPROVED_WEB_RESEARCH",
    queries: [
      "site:sam.gov industry day {agency}",
      "{agency} conference {year} registration",
    ],
  },
  {
    id: "GDI-WEEKEND-01",
    name: "Weekend group / sports / university demand",
    readiness: "EXPERIMENTAL",
    level: "L3_STANDARD_APPROVED_WEB_RESEARCH",
    queries: [
      "{market} youth tournament {year} hotel block",
      "{university} alumni weekend {year}",
    ],
  },
  {
    id: "GDI-CONTACT-01",
    name: "Meeting planner / director of meetings contact research",
    readiness: "EXPERIMENTAL",
    level: "L2_CANONICAL_DEALALITY_RESEARCH_METHODS",
    queries: [
      "{org} \"director of meetings\" OR \"director of events\"",
      "{event} meeting planner contact",
    ],
    note: "Opportunity-linked only — not a parallel contact database. Run AFTER venue/sourcing + hotel-opportunity qualification.",
  },
  {
    id: "GDI-VENUE-HOUSING-01",
    name: "Verify event venue and housing status",
    readiness: "EXPERIMENTAL",
    level: "L2_CANONICAL_DEALALITY_RESEARCH_METHODS",
    methodConcept: "verify_event_venue_and_housing_status",
    queries: [
      "{event} host hotel OR \"official hotel\" OR accommodations OR lodging OR \"room block\"",
      "{event} housing OR \"where to stay\" OR registration OR \"conference venue\"",
      "{event} \"destination\" OR \"future meeting\" OR prospectus",
    ],
    note: "Mandatory before High Priority. Distinguishes primary host vs overflow/housing. Do not rely on event-title pages alone.",
  },
  {
    id: "GDI-ANCHOR-01",
    name: "Demand-anchor proximity research",
    readiness: "EXPERIMENTAL",
    level: "L1_EXISTING_DEALALITY_KNOWLEDGE",
    queries: [],
  },
]);

export function listGdiResearchMethods() {
  return GDI_RESEARCH_METHODS.map((m) => ({ ...m }));
}
