/**
 * Fourteen-workflow Apify coverage matrix V1.
 * Classifications: PRIMARY_APIFY_DISCOVERY | SUPPLEMENTAL_APIFY_DISCOVERY | EXA_PRIMARY
 */

import { listApifyActorsForSegment, APIFY_ACTOR_REGISTRY } from "./apify-actor-registry-v1.js";
import {
  NEED_DATE_RESEARCH_STRATEGY,
  NEED_DATE_STRATEGY_META,
} from "../../need-date-demand/need-date-demand-contract-v1.js";

export const APIFY_COVERAGE_CLASS = Object.freeze({
  PRIMARY_APIFY_DISCOVERY: "PRIMARY_APIFY_DISCOVERY",
  SUPPLEMENTAL_APIFY_DISCOVERY: "SUPPLEMENTAL_APIFY_DISCOVERY",
  EXA_PRIMARY: "EXA_PRIMARY",
});

/** Segment id used in actor registry demandSegments */
const STRATEGY_TO_SEGMENT = Object.freeze({
  [NEED_DATE_RESEARCH_STRATEGY.ASSOCIATION_EVENTS]: "associations_conferences_events",
  [NEED_DATE_RESEARCH_STRATEGY.SCALING_COMPANIES]: "corporate_accounts_scaling",
  [NEED_DATE_RESEARCH_STRATEGY.MEETINGS_INCENTIVES]: "corporate_meetings_incentives",
  [NEED_DATE_RESEARCH_STRATEGY.GOVERNMENT_PUBLIC_SECTOR]: "government_public_sector",
  [NEED_DATE_RESEARCH_STRATEGY.SPORTS_ENTERTAINMENT]: "sports_entertainment",
  [NEED_DATE_RESEARCH_STRATEGY.EDUCATION_SCIENTIFIC]: "education_scientific",
  [NEED_DATE_RESEARCH_STRATEGY.PROJECTS_INFRASTRUCTURE]: "projects_infrastructure",
  [NEED_DATE_RESEARCH_STRATEGY.SOCIAL_SPECIAL_EVENTS]: "social_special_events",
  [NEED_DATE_RESEARCH_STRATEGY.LOCAL_CATERING_DAY_MEETINGS]: "local_catering_day_meetings",
  [NEED_DATE_RESEARCH_STRATEGY.CORPORATE_CELEBRATIONS]: "corporate_celebrations_private_events",
  [NEED_DATE_RESEARCH_STRATEGY.TRAVEL_TRADE]: "travel_trade_group_intermediaries",
  [NEED_DATE_RESEARCH_STRATEGY.RELOCATION_EXTENDED_STAY]: "relocation_extended_stay",
  [NEED_DATE_RESEARCH_STRATEGY.PREFERRED_LODGING]: "preferred_lodging_partnerships",
  [NEED_DATE_RESEARCH_STRATEGY.PROFESSIONAL_EVENT_PLANNERS]: "professional_event_planners",
});

const COVERAGE_ROWS = Object.freeze({
  [NEED_DATE_RESEARCH_STRATEGY.ASSOCIATION_EVENTS]: {
    classification: APIFY_COVERAGE_CLASS.SUPPLEMENTAL_APIFY_DISCOVERY,
    exaCoverage: "Strong — Exa Agent + Deep Research for association programmes and calendars",
    underrepresented: "Structured Eventbrite/Meetup/Luma rows; public Facebook community events",
    expectedIncremental: "Dated public events + organizer names for ORS follow-up",
    structuredDates: "Often yes on event platforms; association sites vary",
    buyerIdentities: "Organizer fields when Actor enrich enabled; still need ORS buyer verify",
    dupRisk: "HIGH vs Exa event discovery; competing-hotel calendars",
    priority: "P2",
  },
  [NEED_DATE_RESEARCH_STRATEGY.SCALING_COMPANIES]: {
    classification: APIFY_COVERAGE_CLASS.EXA_PRIMARY,
    exaCoverage: "Primary path — company growth, offices, programmes",
    underrepresented: "Local HQ / office listings via Maps (relationship only)",
    expectedIncremental: "Local employer relationship prospects — not dated room blocks",
    structuredDates: "Rare from Apify",
    buyerIdentities: "Business listing contacts only — weak",
    dupRisk: "MEDIUM — Maps businesses vs Exa company entities",
    priority: "P4",
  },
  [NEED_DATE_RESEARCH_STRATEGY.MEETINGS_INCENTIVES]: {
    classification: APIFY_COVERAGE_CLASS.SUPPLEMENTAL_APIFY_DISCOVERY,
    exaCoverage: "Primary for corporate meeting evidence and buyer roles",
    underrepresented: "Public business/networking events on Eventbrite/Meetup",
    expectedIncremental: "Public meeting-format events near Boca Raton",
    structuredDates: "Yes when event-platform sourced",
    buyerIdentities: "Organizer enrichment optional",
    dupRisk: "MEDIUM",
    priority: "P2",
  },
  [NEED_DATE_RESEARCH_STRATEGY.GOVERNMENT_PUBLIC_SECTOR]: {
    classification: APIFY_COVERAGE_CLASS.EXA_PRIMARY,
    exaCoverage: "Primary — agency RFPs, calendars, programme pages",
    underrepresented: "Website Content Crawler for known .gov programme URLs only",
    expectedIncremental: "Extraction assist after Exa finds URLs",
    structuredDates: "Depends on target page",
    buyerIdentities: "Usually not from Apify",
    dupRisk: "LOW volume; HIGH irrelevance if unconstrained crawl",
    priority: "P4",
  },
  [NEED_DATE_RESEARCH_STRATEGY.SPORTS_ENTERTAINMENT]: {
    classification: APIFY_COVERAGE_CLASS.PRIMARY_APIFY_DISCOVERY,
    exaCoverage: "Good narrative search; weaker structured schedules",
    underrepresented: "ESPN schedules; Facebook sports/community events",
    expectedIncremental: "Future competitions for South Florida geo filter + ORS lodging thesis",
    structuredDates: "Yes for schedule Actors",
    buyerIdentities: "Weak — teams/leagues need ORS",
    dupRisk: "HIGH national schedule noise; geo filter mandatory",
    priority: "P2",
  },
  [NEED_DATE_RESEARCH_STRATEGY.EDUCATION_SCIENTIFIC]: {
    classification: APIFY_COVERAGE_CLASS.SUPPLEMENTAL_APIFY_DISCOVERY,
    exaCoverage: "Primary for university/scientific programmes",
    underrepresented: "Public Eventbrite academic/community events; targeted crawl",
    expectedIncremental: "Public programme events near campus corridors",
    structuredDates: "Sometimes",
    buyerIdentities: "Organizer when present",
    dupRisk: "MEDIUM",
    priority: "P3",
  },
  [NEED_DATE_RESEARCH_STRATEGY.PROJECTS_INFRASTRUCTURE]: {
    classification: APIFY_COVERAGE_CLASS.EXA_PRIMARY,
    exaCoverage: "Primary — project announcements, contractor demand",
    underrepresented: "Minimal Apify fit",
    expectedIncremental: "Little — avoid forced Actors",
    structuredDates: "No",
    buyerIdentities: "No",
    dupRisk: "N/A",
    priority: "P5",
  },
  [NEED_DATE_RESEARCH_STRATEGY.SOCIAL_SPECIAL_EVENTS]: {
    classification: APIFY_COVERAGE_CLASS.PRIMARY_APIFY_DISCOVERY,
    exaCoverage: "Previously under-executed (wedding gap); Exa still needed for depth",
    underrepresented: "The Knot / WeddingWire / Maps wedding planners & venues",
    expectedIncremental: "Planner/venue relationship inventory + public social events",
    structuredDates: "Directories: no; Eventbrite/FB social: sometimes",
    buyerIdentities: "Vendor/planner businesses — not couples",
    dupRisk: "HIGH listing duplication across Knot/WW/Maps",
    priority: "P1",
  },
  [NEED_DATE_RESEARCH_STRATEGY.LOCAL_CATERING_DAY_MEETINGS]: {
    classification: APIFY_COVERAGE_CLASS.SUPPLEMENTAL_APIFY_DISCOVERY,
    exaCoverage: "Local org discovery via Exa",
    underrepresented: "Maps offices/venues; Meetup day events",
    expectedIncremental: "Local generators without rooms + public day meetings",
    structuredDates: "Events yes; Maps no",
    buyerIdentities: "Business contacts weak",
    dupRisk: "MEDIUM",
    priority: "P2",
  },
  [NEED_DATE_RESEARCH_STRATEGY.CORPORATE_CELEBRATIONS]: {
    classification: APIFY_COVERAGE_CLASS.SUPPLEMENTAL_APIFY_DISCOVERY,
    exaCoverage: "Company celebration evidence via Exa",
    underrepresented: "Public celebration/gala Eventbrite + FB events",
    expectedIncremental: "Dated public galas/fundraisers (not private parties)",
    structuredDates: "Yes when public",
    buyerIdentities: "Organizer",
    dupRisk: "MEDIUM — many social/fundraising noise",
    priority: "P3",
  },
  [NEED_DATE_RESEARCH_STRATEGY.TRAVEL_TRADE]: {
    classification: APIFY_COVERAGE_CLASS.SUPPLEMENTAL_APIFY_DISCOVERY,
    exaCoverage: "Primary for DMC/tour ops intelligence",
    underrepresented: "Maps travel agencies / group intermediaries",
    expectedIncremental: "Local intermediary relationship prospects",
    structuredDates: "No",
    buyerIdentities: "Business listing only",
    dupRisk: "MEDIUM",
    priority: "P3",
  },
  [NEED_DATE_RESEARCH_STRATEGY.RELOCATION_EXTENDED_STAY]: {
    classification: APIFY_COVERAGE_CLASS.EXA_PRIMARY,
    exaCoverage: "Primary — relocation programmes, corp housing",
    underrepresented: "Minimal Apify differentiation",
    expectedIncremental: "Low",
    structuredDates: "No",
    buyerIdentities: "No",
    dupRisk: "N/A",
    priority: "P5",
  },
  [NEED_DATE_RESEARCH_STRATEGY.PREFERRED_LODGING]: {
    classification: APIFY_COVERAGE_CLASS.PRIMARY_APIFY_DISCOVERY,
    exaCoverage: "Partnership thesis via Exa after prospect identified",
    underrepresented: "Venues without guestrooms; wedding venues; conference facilities via Maps",
    expectedIncremental: "Accommodation partnership prospects (relationship track)",
    structuredDates: "No — separate from need-date confirmed business",
    buyerIdentities: "Venue/business contacts",
    dupRisk: "HIGH vs social wedding listings",
    priority: "P1",
  },
  [NEED_DATE_RESEARCH_STRATEGY.PROFESSIONAL_EVENT_PLANNERS]: {
    classification: APIFY_COVERAGE_CLASS.PRIMARY_APIFY_DISCOVERY,
    exaCoverage: "Background/qualification via Exa/ORS",
    underrepresented: "Structured planner directories (Knot, WW, Maps)",
    expectedIncremental: "Planner inventory for Waterstone outreach + Research Continuation",
    structuredDates: "No",
    buyerIdentities: "Planner businesses — YES for relationship track",
    dupRisk: "HIGH cross-directory",
    priority: "P1",
  },
});

export function buildFourteenWorkflowApifyCoverageMatrix() {
  const workflows = Object.values(NEED_DATE_RESEARCH_STRATEGY).map((strategyId) => {
    const meta = NEED_DATE_STRATEGY_META[strategyId] || {};
    const segmentId = STRATEGY_TO_SEGMENT[strategyId];
    const row = COVERAGE_ROWS[strategyId] || {
      classification: APIFY_COVERAGE_CLASS.EXA_PRIMARY,
      exaCoverage: "Default Exa",
      underrepresented: "None mapped",
      expectedIncremental: "Low",
      structuredDates: "Unknown",
      buyerIdentities: "Unknown",
      dupRisk: "Unknown",
      priority: "P5",
    };
    const actors = listApifyActorsForSegment(segmentId);
    const allMatching = APIFY_ACTOR_REGISTRY.filter((a) =>
      (a.demandSegments || []).includes(segmentId),
    );
    return {
      strategyId,
      workflowNumber: meta.number,
      powershell: meta.powershell,
      segmentId,
      classification: row.classification,
      exaCoverage: row.exaCoverage,
      potentialApifySources: allMatching.map((a) => a.actorId),
      enabledActors: actors.map((a) => a.actorId),
      underrepresented: row.underrepresented,
      expectedIncremental: row.expectedIncremental,
      structuredDates: row.structuredDates,
      buyerIdentities: row.buyerIdentities,
      dupRisk: row.dupRisk,
      recommendedPriority: row.priority,
    };
  });

  workflows.sort((a, b) => a.workflowNumber - b.workflowNumber);

  const counts = {
    PRIMARY_APIFY_DISCOVERY: workflows.filter(
      (w) => w.classification === APIFY_COVERAGE_CLASS.PRIMARY_APIFY_DISCOVERY,
    ).length,
    SUPPLEMENTAL_APIFY_DISCOVERY: workflows.filter(
      (w) => w.classification === APIFY_COVERAGE_CLASS.SUPPLEMENTAL_APIFY_DISCOVERY,
    ).length,
    EXA_PRIMARY: workflows.filter(
      (w) => w.classification === APIFY_COVERAGE_CLASS.EXA_PRIMARY,
    ).length,
  };

  return {
    version: "gdi-apify-fourteen-workflow-coverage-v1",
    workflowCount: workflows.length,
    counts,
    workflows,
  };
}
