/**
 * Canonical twelve demand-segment registry V1
 *
 * ONE shared ORS/Exa/CIB/drawer engine · TWELVE specialized contracts.
 * Preserves legacy researchSegment keys via compatibility aliases.
 * Does not migrate historical records; routing is mechanism-first.
 *
 * Segments 1–8 retain prior certification. Segments 9–12 added for
 * catering-only / banquet / intermediary / extended-stay demand.
 */

export const EIGHT_DEMAND_SEGMENT_REGISTRY_V1 =
  "gdi-eight-demand-segment-registry-v1";
export const TWELVE_DEMAND_SEGMENT_REGISTRY_V1 =
  "gdi-twelve-demand-segment-registry-v1";

/** Shared PowerShell / Node execution modes. */
export const GDI_SEGMENT_EXECUTION_MODE = Object.freeze({
  PREVIEW: "Preview",
  FULL_E2E: "FullE2E",
  CERTIFY_ONLY: "CertifyOnly",
  WEEKLY_INCREMENTAL: "WeeklyIncremental",
});

/**
 * Canonical customer demand segments (product labels 1–12).
 * researchAxis = primary RESEARCH_SEGMENT / compatibility key used by runners.
 */
export const DEMAND_SEGMENT = Object.freeze({
  ASSOCIATIONS_CONFERENCES_EVENTS: Object.freeze({
    id: "associations_conferences_events",
    number: 1,
    label: "Associations, Conferences & Events",
    shortLabel: "Events",
    researchAxis: "associations_government",
    legacyAliases: Object.freeze([
      "associations_government",
      "association_events",
      "association_conference",
      "events",
    ]),
    powershell: "Invoke-GdiAssociationEvents.ps1",
    overviewMode: "events_structured_grid_v1",
    discoveryContract: "gdi-exa-events-discovery-v1",
    deepResearchContract: "gdi-exa-events-deep-research-v1",
    hotelRunner:
      "lib/group-demand-intelligence/research/association-events-hotel-runner-v1.js",
    certifyScript: "scripts/gdi-association-events-certify-local-preview-v1.mjs",
    mockFullE2eScript: "scripts/gdi-events-full-integration-zero-paid-v1.mjs",
    isolatedLedgerSlot: "association_events",
    status: "PRODUCTION_CERTIFIED",
  }),
  CORPORATE_ACCOUNTS_SCALING: Object.freeze({
    id: "corporate_accounts_scaling",
    number: 2,
    label: "Corporate Accounts & Scaling Companies",
    shortLabel: "Scaling",
    researchAxis: "scaling_companies",
    legacyAliases: Object.freeze(["scaling_companies", "scaling", "corporate_accounts"]),
    powershell: "Invoke-GdiScalingCompanies.ps1",
    overviewMode: "scaling_narrative_overview_v1",
    discoveryContract: "gdi-exa-scaling-discovery-v2",
    deepResearchContract: "gdi-exa-scaling-deep-research-v1",
    hotelRunner:
      "lib/group-demand-intelligence/research/scaling-companies-hotel-runner-v1.js",
    certifyScript: "scripts/gdi-scaling-certify-local-preview-v1.mjs",
    mockFullE2eScript: "scripts/gdi-scaling-mock-provider-full-e2e-v1.mjs",
    isolatedLedgerSlot: "scaling_companies",
    status: "PRODUCTION_CERTIFIED",
  }),
  CORPORATE_MEETINGS_INCENTIVES: Object.freeze({
    id: "corporate_meetings_incentives",
    number: 3,
    label: "Corporate Meetings & Incentive Travel",
    shortLabel: "Meetings & Incentives",
    researchAxis: "corporate_meetings_incentives",
    legacyAliases: Object.freeze([
      "corporate_meetings_training",
      "corporate_meetings",
      "incentives",
      "incentive_travel",
      "mice",
      "mice_incentive_travel",
      "mice_incentive",
      "incentive",
      "corporate_incentive",
      "meetings_incentives",
    ]),
    powershell: "Invoke-GdiMeetingsIncentives.ps1",
    overviewMode: "meetings_incentives_overview_v1",
    discoveryContract: "gdi-exa-meetings-incentives-discovery-v1",
    deepResearchContract: "gdi-exa-meetings-incentives-deep-research-v1",
    hotelRunner:
      "lib/group-demand-intelligence/demand-segments/segment-hotel-runner-stub-v1.js",
    certifyScript: "scripts/gdi-segment-certify-local-preview-v1.mjs",
    mockFullE2eScript: "scripts/gdi-segment-mock-provider-full-e2e-v1.mjs",
    isolatedLedgerSlot: "corporate_meetings_incentives",
    status: "CONTRACT_READY",
  }),
  GOVERNMENT_PUBLIC_SECTOR: Object.freeze({
    id: "government_public_sector",
    number: 4,
    label: "Government & Public Sector",
    shortLabel: "Government",
    researchAxis: "government_public_sector",
    legacyAliases: Object.freeze(["government", "public_sector"]),
    powershell: "Invoke-GdiGovernmentPublicSector.ps1",
    overviewMode: "government_overview_v1",
    discoveryContract: "gdi-exa-government-discovery-v1",
    deepResearchContract: "gdi-exa-government-deep-research-v1",
    hotelRunner:
      "lib/group-demand-intelligence/demand-segments/segment-hotel-runner-stub-v1.js",
    certifyScript: "scripts/gdi-segment-certify-local-preview-v1.mjs",
    mockFullE2eScript: "scripts/gdi-segment-mock-provider-full-e2e-v1.mjs",
    isolatedLedgerSlot: "government_public_sector",
    status: "CONTRACT_READY",
  }),
  SPORTS_ENTERTAINMENT: Object.freeze({
    id: "sports_entertainment",
    number: 5,
    label: "Sports & Entertainment",
    shortLabel: "Sports & Entertainment",
    researchAxis: "sports",
    legacyAliases: Object.freeze([
      "sports",
      "entertainment_production",
      "sports_entertainment",
    ]),
    powershell: "Invoke-GdiSportsEntertainment.ps1",
    overviewMode: "sports_entertainment_overview_v1",
    discoveryContract: "gdi-exa-sports-entertainment-discovery-v1",
    deepResearchContract: "gdi-exa-sports-entertainment-deep-research-v1",
    hotelRunner:
      "lib/group-demand-intelligence/demand-segments/segment-hotel-runner-stub-v1.js",
    certifyScript: "scripts/gdi-segment-certify-local-preview-v1.mjs",
    mockFullE2eScript: "scripts/gdi-segment-mock-provider-full-e2e-v1.mjs",
    isolatedLedgerSlot: "sports_entertainment",
    status: "CONTRACT_READY",
  }),
  EDUCATION_SCIENTIFIC: Object.freeze({
    id: "education_scientific",
    number: 6,
    label: "Education, Academic & Scientific Groups",
    shortLabel: "Education & Scientific",
    researchAxis: "education_scientific",
    legacyAliases: Object.freeze([
      "medical_academic",
      "tours_educational_specialist",
      "education",
      "academic",
    ]),
    powershell: "Invoke-GdiEducationScientific.ps1",
    overviewMode: "education_scientific_overview_v1",
    discoveryContract: "gdi-exa-education-scientific-discovery-v1",
    deepResearchContract: "gdi-exa-education-scientific-deep-research-v1",
    hotelRunner:
      "lib/group-demand-intelligence/demand-segments/segment-hotel-runner-stub-v1.js",
    certifyScript: "scripts/gdi-segment-certify-local-preview-v1.mjs",
    mockFullE2eScript: "scripts/gdi-segment-mock-provider-full-e2e-v1.mjs",
    isolatedLedgerSlot: "education_scientific",
    status: "CONTRACT_READY",
  }),
  PROJECTS_INFRASTRUCTURE: Object.freeze({
    id: "projects_infrastructure",
    number: 7,
    label: "Projects, Construction & Infrastructure",
    shortLabel: "Projects & Infrastructure",
    researchAxis: "projects_infrastructure",
    legacyAliases: Object.freeze([
      "projects",
      "infrastructure",
      "construction",
      "project_travel",
    ]),
    powershell: "Invoke-GdiProjectsInfrastructure.ps1",
    overviewMode: "projects_infrastructure_overview_v1",
    discoveryContract: "gdi-exa-projects-infrastructure-discovery-v1",
    deepResearchContract: "gdi-exa-projects-infrastructure-deep-research-v1",
    hotelRunner:
      "lib/group-demand-intelligence/demand-segments/segment-hotel-runner-stub-v1.js",
    certifyScript: "scripts/gdi-segment-certify-local-preview-v1.mjs",
    mockFullE2eScript: "scripts/gdi-segment-mock-provider-full-e2e-v1.mjs",
    isolatedLedgerSlot: "projects_infrastructure",
    status: "CONTRACT_READY",
  }),
  SOCIAL_SPECIAL_EVENTS: Object.freeze({
    id: "social_special_events",
    number: 8,
    label: "Social Groups & Special Events",
    shortLabel: "Social & Special Events",
    researchAxis: "weddings_social_private_events",
    legacyAliases: Object.freeze([
      "weddings_social_private_events",
      "social",
      "weddings",
      "private_events",
    ]),
    powershell: "Invoke-GdiSocialSpecialEvents.ps1",
    overviewMode: "social_special_events_overview_v1",
    discoveryContract: "gdi-exa-social-special-events-discovery-v1",
    deepResearchContract: "gdi-exa-social-special-events-deep-research-v1",
    hotelRunner:
      "lib/group-demand-intelligence/demand-segments/segment-hotel-runner-stub-v1.js",
    certifyScript: "scripts/gdi-segment-certify-local-preview-v1.mjs",
    mockFullE2eScript: "scripts/gdi-segment-mock-provider-full-e2e-v1.mjs",
    isolatedLedgerSlot: "social_special_events",
    status: "CONTRACT_READY",
  }),
  LOCAL_CATERING_DAY_MEETINGS: Object.freeze({
    id: "local_catering_day_meetings",
    number: 9,
    label: "Local Catering & Day Meetings",
    shortLabel: "Local Catering & Day Meetings",
    researchAxis: "local_meetings_catering",
    legacyAliases: Object.freeze([
      "local_meetings_catering",
      "local_catering",
      "day_meetings",
      "catering_only",
    ]),
    powershell: "Invoke-GdiLocalCateringDayMeetings.ps1",
    overviewMode: "local_catering_day_meetings_overview_v1",
    discoveryContract: "gdi-exa-local-catering-day-meetings-discovery-v1",
    deepResearchContract: "gdi-exa-local-catering-day-meetings-deep-research-v1",
    hotelRunner:
      "lib/group-demand-intelligence/demand-segments/segment-hotel-runner-stub-v1.js",
    certifyScript: "scripts/gdi-segment-certify-local-preview-v1.mjs",
    mockFullE2eScript: "scripts/gdi-segment-mock-provider-full-e2e-v1.mjs",
    isolatedLedgerSlot: "local_catering_day_meetings",
    guestroomRequired: false,
    status: "CONTRACT_READY",
  }),
  CORPORATE_CELEBRATIONS_PRIVATE_EVENTS: Object.freeze({
    id: "corporate_celebrations_private_events",
    number: 10,
    label: "Corporate Celebrations & Private Events",
    shortLabel: "Corporate Celebrations",
    researchAxis: "corporate_celebrations_private_events",
    legacyAliases: Object.freeze([
      "corporate_celebrations",
      "corporate_private_events",
      "holiday_parties",
      "product_launches",
    ]),
    powershell: "Invoke-GdiCorporateCelebrations.ps1",
    overviewMode: "corporate_celebrations_overview_v1",
    discoveryContract: "gdi-exa-corporate-celebrations-discovery-v1",
    deepResearchContract: "gdi-exa-corporate-celebrations-deep-research-v1",
    hotelRunner:
      "lib/group-demand-intelligence/demand-segments/segment-hotel-runner-stub-v1.js",
    certifyScript: "scripts/gdi-segment-certify-local-preview-v1.mjs",
    mockFullE2eScript: "scripts/gdi-segment-mock-provider-full-e2e-v1.mjs",
    isolatedLedgerSlot: "corporate_celebrations_private_events",
    guestroomRequired: false,
    status: "CONTRACT_READY",
  }),
  TRAVEL_TRADE_GROUP_INTERMEDIARIES: Object.freeze({
    id: "travel_trade_group_intermediaries",
    number: 11,
    label: "Travel Trade & Group Intermediaries",
    shortLabel: "Travel Trade & Intermediaries",
    researchAxis: "professional_planners_dmc",
    legacyAliases: Object.freeze([
      "professional_planners_dmc",
      "travel_trade",
      "dmc",
      "group_intermediaries",
      "tour_operators",
    ]),
    powershell: "Invoke-GdiTravelTradeIntermediaries.ps1",
    overviewMode: "travel_trade_intermediaries_overview_v1",
    discoveryContract: "gdi-exa-travel-trade-intermediaries-discovery-v1",
    deepResearchContract: "gdi-exa-travel-trade-intermediaries-deep-research-v1",
    hotelRunner:
      "lib/group-demand-intelligence/demand-segments/segment-hotel-runner-stub-v1.js",
    certifyScript: "scripts/gdi-segment-certify-local-preview-v1.mjs",
    mockFullE2eScript: "scripts/gdi-segment-mock-provider-full-e2e-v1.mjs",
    isolatedLedgerSlot: "travel_trade_group_intermediaries",
    guestroomRequired: false,
    status: "CONTRACT_READY",
  }),
  RELOCATION_EXTENDED_STAY: Object.freeze({
    id: "relocation_extended_stay",
    number: 12,
    label: "Relocation & Extended Stay",
    shortLabel: "Relocation & Extended Stay",
    researchAxis: "relocation_extended_stay",
    legacyAliases: Object.freeze([
      "relocation",
      "extended_stay",
      "corporate_mobility",
      "expatriate_assignments",
    ]),
    powershell: "Invoke-GdiRelocationExtendedStay.ps1",
    overviewMode: "relocation_extended_stay_overview_v1",
    discoveryContract: "gdi-exa-relocation-extended-stay-discovery-v1",
    deepResearchContract: "gdi-exa-relocation-extended-stay-deep-research-v1",
    hotelRunner:
      "lib/group-demand-intelligence/demand-segments/segment-hotel-runner-stub-v1.js",
    certifyScript: "scripts/gdi-segment-certify-local-preview-v1.mjs",
    mockFullE2eScript: "scripts/gdi-segment-mock-provider-full-e2e-v1.mjs",
    isolatedLedgerSlot: "relocation_extended_stay",
    guestroomRequired: false,
    status: "CONTRACT_READY",
  }),
});

export const DEMAND_SEGMENTS_ORDERED = Object.freeze(
  Object.values(DEMAND_SEGMENT).sort((a, b) => a.number - b.number),
);

/** First eight segments only — prior certification surface. */
export const EIGHT_DEMAND_SEGMENTS_ORDERED = Object.freeze(
  DEMAND_SEGMENTS_ORDERED.filter((s) => s.number <= 8),
);

const BY_ID = new Map(DEMAND_SEGMENTS_ORDERED.map((s) => [s.id, s]));
const BY_AXIS = new Map();
const BY_ALIAS = new Map();
for (const s of DEMAND_SEGMENTS_ORDERED) {
  BY_AXIS.set(s.researchAxis, s);
  BY_ALIAS.set(s.id, s);
  BY_ALIAS.set(s.researchAxis, s);
  for (const a of s.legacyAliases) BY_ALIAS.set(String(a).toLowerCase(), s);
}

export function getDemandSegmentById(id) {
  return BY_ID.get(String(id || "").trim()) || null;
}

export function getDemandSegmentByNumber(n) {
  const num = Number(n);
  return DEMAND_SEGMENTS_ORDERED.find((s) => s.number === num) || null;
}

/** Resolve any legacy researchSegment / alias → canonical segment def. */
export function resolveDemandSegment(key) {
  const raw = String(key || "").trim().toLowerCase();
  if (!raw) return null;
  const k = raw.replace(/[^a-z0-9]+/g, "_").replace(/^_|_$/g, "");
  if (!k) return null;
  if (BY_ALIAS.get(k) || BY_AXIS.get(k)) {
    return BY_ALIAS.get(k) || BY_AXIS.get(k);
  }
  // Soft match: "MICE / Incentive Travel", "Meetings & Incentives", etc.
  if (/\bmice\b|\bincentive/.test(raw) && /travel|meeting|corporate|programme|program/.test(raw)) {
    return BY_ID.get("corporate_meetings_incentives") || null;
  }
  if (/\bassoc|\bconference|\bcongress|\bevents?\b/.test(raw) && !/\bscaling\b/.test(raw)) {
    return BY_ID.get("associations_conferences_events") || null;
  }
  return null;
}

export function listDemandSegmentPowershellEntryPoints() {
  return DEMAND_SEGMENTS_ORDERED.map((s) => ({
    number: s.number,
    id: s.id,
    label: s.label,
    script: `scripts/${s.powershell}`,
    status: s.status,
  }));
}

export function sharedExecutionModes() {
  return Object.values(GDI_SEGMENT_EXECUTION_MODE);
}
