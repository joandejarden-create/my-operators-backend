/**
 * Full E2E → Airtable write eligibility (D1 amendments).
 * Fail-closed for hotel-facing GDO qualification.
 */

import { computeDemandGeneratorId } from "../demand-generators/entities.js";
import { computeVenueId } from "../private-events/venue-entity.js";
import { DEMAND_THESIS_STANDARD_ALIAS as DEMAND_THESIS_STANDARD } from "../demand-mechanism-research-v1.js";
import { GDI_AIRTABLE_TABLES } from "./gdi-full-e2e-airtable-persistence-contract-v1.js";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

export const FULL_E2E_AIRTABLE_DENY_LIST = Object.freeze([
  GDI_AIRTABLE_TABLES.GDI_HOTEL_COMMERCIAL_PROFILES.name,
  GDI_AIRTABLE_TABLES.GDI_HOTEL_NEED_PERIODS.name,
  GDI_AIRTABLE_TABLES.GDI_NEED_DATE_CAMPAIGN_RUNS.name,
  GDI_AIRTABLE_TABLES.GDI_NEED_PERIOD_OPPORTUNITY_MATCHES.name,
  "GDI Need Period Change History",
  "GDI Commercial Profile Change History",
  "Decisions",
  "Decision Events",
  "Hotel Commercial Profiles",
  "Hotel Event Spaces",
  "Hotel Demand Nodes",
  "Hotel Seasonality & Need Periods",
  "Hotel Intelligence Evidence",
  "AI Demand Positioning - Published Reports",
  "ADP Monitoring Periods",
  "ADP Observation Prompt Ledger",
]);

export function resolveCanonicalPersistenceIdentities(candidate = {}, geo = {}) {
  const organizationName = text(
    candidate.organizationName || candidate.company || candidate.venueName,
  );
  const opportunityId = text(candidate.opportunityId || candidate.id) || null;
  const demandGeneratorId = computeDemandGeneratorId({
    organizationName,
    officialDomain: candidate.officialDomain || null,
    website: candidate.website || candidate.sourceUrl || null,
  });
  const venueId = computeVenueId({
    venueName: organizationName,
    city: geo.city || candidate.city || null,
    region: geo.region || candidate.region || null,
    website: candidate.website || candidate.sourceUrl || null,
    officialDomain: candidate.officialDomain || null,
  });
  return { organizationName, opportunityId, demandGeneratorId, venueId };
}

function extractDemandThesis(candidate = {}) {
  const raw =
    candidate.demandThesis ||
    candidate.demandThesisStandard ||
    candidate.thesisStandard ||
    candidate.demandConfidence ||
    candidate.qualification?.demandThesis ||
    candidate.commercial?.demandThesis ||
    "";
  return upper(raw);
}

function hasCredibleCommercialEntryPoint(candidate = {}) {
  if (Number(candidate.salesReadyContacts || 0) > 0) return true;
  if (candidate.verifiedNamedContact === true) return true;
  if (candidate.hasCredibleCommercialEntryPoint === true) return true;
  const contact =
    candidate.primaryContact ||
    candidate.verifiedContact ||
    candidate.contact ||
    null;
  if (contact && (contact.name || contact.fullName) && (contact.email || contact.role || contact.title)) {
    return true;
  }
  return false;
}

/**
 * Evaluate which Airtable tables may receive a write for this research object.
 */
export function evaluateFullE2eAirtableWriteEligibility(input = {}) {
  const candidate = input.candidate || {};
  const kind = upper(input.kind || candidate.kind);
  const disposition = upper(candidate.disposition || candidate.evaluationLabel);
  const demandThesis = extractDemandThesis(candidate);
  const reasons = [];
  const allowedTables = [];

  if (disposition === "EXCLUDE" || disposition === "REJECTED") {
    return {
      ok: true,
      allowedTables: [],
      reasons: ["EXCLUDED_OR_REJECTED"],
      commercialQualificationStatus: "RESEARCH_ONLY",
      demandThesis: demandThesis || null,
    };
  }

  const confirmedOrReliable =
    demandThesis === DEMAND_THESIS_STANDARD.CONFIRMED_DEMAND ||
    demandThesis === "CONFIRMED" ||
    demandThesis === DEMAND_THESIS_STANDARD.RELIABLE_INFERRED_DEMAND ||
    demandThesis === "RELIABLE_INFERRED";

  const entryPoint = hasCredibleCommercialEntryPoint(candidate);
  const watchlist =
    disposition === "WATCHLIST_RESEARCH" ||
    disposition === "NEW_UNIQUE" ||
    candidate.hotelQualified === true;

  // Organization / generator intelligence — reusable, not hotel-facing by itself
  if (
    kind === "HOTEL_OPPORTUNITY" ||
    kind === "ORGANIZATION" ||
    kind === "GENERATOR" ||
    kind === "COMPANY" ||
    kind === "PLANNER"
  ) {
    allowedTables.push(GDI_AIRTABLE_TABLES.DEMAND_GENERATORS.name);
    reasons.push("CREDIBLE_ORG_INTELLIGENCE_ELIGIBLE");
  }

  // Venue identity — not automatically a lodging GDO
  if (kind === "VENUE") {
    allowedTables.push(GDI_AIRTABLE_TABLES.PRIVATE_EVENT_VENUES.name);
    reasons.push("VENUE_IDENTITY_ELIGIBLE");
    if (candidate.hotelQualified === true && confirmedOrReliable && entryPoint) {
      allowedTables.push(GDI_AIRTABLE_TABLES.HOTEL_VENUE_FIT.name);
      allowedTables.push(GDI_AIRTABLE_TABLES.GROUP_DEMAND_OPPORTUNITIES.name);
      reasons.push("HOTEL_QUALIFIED_VENUE_OPPORTUNITY");
    } else {
      reasons.push("VENUE_NOT_AUTO_LODGING_OPPORTUNITY");
    }
  }

  // Hotel-facing GDO — strict commercial eligibility
  if (kind === "HOTEL_OPPORTUNITY" || (watchlist && kind !== "VENUE")) {
    if (confirmedOrReliable && entryPoint) {
      allowedTables.push(GDI_AIRTABLE_TABLES.GROUP_DEMAND_OPPORTUNITIES.name);
      if (candidate.hotelQualified === true) {
        allowedTables.push(GDI_AIRTABLE_TABLES.HOTEL_DEMAND_GENERATOR_FIT.name);
      }
      reasons.push("GDO_COMMERCIAL_ELIGIBILITY_MET");
    } else {
      reasons.push("GDO_DEFERRED_WATCHLIST_BELOW_ELIGIBILITY");
      if (!confirmedOrReliable) {
        reasons.push("MISSING_CONFIRMED_OR_RELIABLE_INFERRED_DEMAND");
      }
      if (!entryPoint) {
        reasons.push("MISSING_CREDIBLE_COMMERCIAL_ENTRY_POINT");
      }
    }
  }

  // Programs / signals — only when explicitly evidence-backed
  if (candidate.recurringProgramVerified === true || candidate.programId) {
    allowedTables.push(GDI_AIRTABLE_TABLES.DEMAND_PROGRAMS.name);
    reasons.push("VERIFIED_RECURRING_PROGRAM");
  }
  if (candidate.datedSignalVerified === true || candidate.signalId) {
    allowedTables.push(GDI_AIRTABLE_TABLES.DEMAND_GENERATOR_SIGNALS.name);
    reasons.push("EVIDENCE_BACKED_SIGNAL");
  }
  if (candidate.privateEventSignalVerified === true) {
    allowedTables.push(GDI_AIRTABLE_TABLES.PRIVATE_EVENT_SIGNALS.name);
    reasons.push("EVIDENCE_BACKED_PE_SIGNAL");
  }

  // Research Targets — D3 Option A: explicit monitoring commitment only
  if (candidate.explicitMonitoringCommitment === true) {
    allowedTables.push(GDI_AIRTABLE_TABLES.GDI_RESEARCH_TARGETS.name);
    reasons.push("EXPLICIT_MONITORING_COMMITMENT");
  } else {
    reasons.push("RESEARCH_TARGET_NOT_AUTO_CREATED");
  }

  // Optional run summary
  if (input.includeResearchRunSummary === true) {
    allowedTables.push(GDI_AIRTABLE_TABLES.GDI_RESEARCH_RUNS.name);
    reasons.push("OPTIONAL_RESEARCH_RUN_SUMMARY");
  }

  const unique = [...new Set(allowedTables)].filter(
    (t) => !FULL_E2E_AIRTABLE_DENY_LIST.includes(t),
  );

  let commercialQualificationStatus = "RESEARCH_ONLY";
  if (unique.includes(GDI_AIRTABLE_TABLES.GROUP_DEMAND_OPPORTUNITIES.name)) {
    commercialQualificationStatus = "CUSTOMER_ELIGIBLE_CANDIDATE";
  } else if (unique.includes(GDI_AIRTABLE_TABLES.DEMAND_GENERATORS.name)) {
    commercialQualificationStatus = "GLOBAL_ORG_ONLY";
  } else if (unique.includes(GDI_AIRTABLE_TABLES.PRIVATE_EVENT_VENUES.name)) {
    commercialQualificationStatus = "VENUE_IDENTITY_ONLY";
  }

  return {
    ok: true,
    allowedTables: unique,
    reasons,
    commercialQualificationStatus,
    demandThesis: demandThesis || null,
    confirmedOrReliable,
    hasCredibleCommercialEntryPoint: entryPoint,
  };
}

export function isTableDeniedForFullE2eDiscovery(tableName) {
  return FULL_E2E_AIRTABLE_DENY_LIST.includes(text(tableName));
}

export default {
  FULL_E2E_AIRTABLE_DENY_LIST,
  evaluateFullE2eAirtableWriteEligibility,
  resolveCanonicalPersistenceIdentities,
  isTableDeniedForFullE2eDiscovery,
};
