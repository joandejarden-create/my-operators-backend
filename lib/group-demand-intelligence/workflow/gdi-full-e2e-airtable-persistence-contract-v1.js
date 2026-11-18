/**
 * Canonical Full E2E → EXISTING Airtable table routing (AI Demand Positioning).
 * Reuses Demand Generators / Opportunities / PE / Research Coverage tables.
 * Does NOT invent new competing tables. Production writes fail-closed.
 */

export const GDI_FULL_E2E_AIRTABLE_PERSISTENCE_CONTRACT_V1 =
  "gdi-full-e2e-airtable-persistence-contract-v1";

export const GDI_AIRTABLE_BASE = Object.freeze({
  name: "AI Demand Positioning",
  baseId: "appa2cE7FTRmIbB32",
});

/** Existing GDI tables — IDs from live schema inventory (reports/gdi-airtable-complete-data-model-audit-v1). */
export const GDI_AIRTABLE_TABLES = Object.freeze({
  GROUP_DEMAND_OPPORTUNITIES: Object.freeze({
    name: "Group Demand Opportunities",
    tableId: "tblRuReslJMwsfRQj",
    adapter: "airtable-opportunity-store / gdi-airtable-sync-v1",
    identityField: "opportunityId",
  }),
  DEMAND_GENERATORS: Object.freeze({
    name: "Demand Generators",
    tableId: "tblykUVOjGVkawvdD",
    adapter: "demand-generators/airtable-stores.upsertDemandGenerator",
    identityField: "Demand Generator ID",
  }),
  DEMAND_PROGRAMS: Object.freeze({
    name: "Demand Programs",
    tableId: "tblxm7wqfFNG37Z2m",
    adapter: "demand-generators/airtable-stores.upsertDemandProgram",
    identityField: "Program ID",
  }),
  DEMAND_GENERATOR_SIGNALS: Object.freeze({
    name: "Demand Generator Signals",
    tableId: "tblZ0cOM78uYMCTXx",
    adapter: "demand-generators/airtable-stores.upsertSignal",
    identityField: "Demand Generator Signal ID",
  }),
  HOTEL_DEMAND_GENERATOR_FIT: Object.freeze({
    name: "Hotel Demand Generator Fit",
    tableId: "tblqamXbIk9n6gXfV",
    adapter: "demand-generators/airtable-stores.upsertHotelFit",
    identityField: "Hotel Generator Fit ID",
  }),
  PRIVATE_EVENT_VENUES: Object.freeze({
    name: "Private Event Venues",
    tableId: "tblE5p4HjVHfcMDna",
    adapter: "private-events/airtable-venue-store",
    identityField: "Venue ID",
  }),
  PRIVATE_EVENT_SIGNALS: Object.freeze({
    name: "Private Event Signals",
    tableId: "tbl5rSZxKv218GuOP",
    adapter: "private-events/airtable-signal-store",
    identityField: "Event Signal ID",
  }),
  HOTEL_VENUE_FIT: Object.freeze({
    name: "Hotel Venue Fit",
    tableId: "tbluXqX7ecocCrdE1",
    adapter: "private-events/airtable-fit-store",
    identityField: "Hotel Venue Fit ID",
  }),
  GDI_RESEARCH_TARGETS: Object.freeze({
    name: "GDI Research Targets",
    tableId: "tblVyuEf5vjooWDKX",
    adapter: "research-coverage/airtable-stores",
    identityField: "Target ID",
  }),
  GDI_RESEARCH_RUNS: Object.freeze({
    name: "GDI Research Runs",
    tableId: "tblzNMUIo2T6onaKH",
    adapter: "research-coverage/airtable-stores",
    identityField: "Run ID",
  }),
  GDI_RESEARCH_TARGET_RUNS: Object.freeze({
    name: "GDI Research Target Runs",
    tableId: "tblSA0cFVplNfWMjp",
    adapter: "research-coverage/airtable-stores",
    identityField: "Target Run ID",
  }),
  GDI_HOTEL_COMMERCIAL_PROFILES: Object.freeze({
    name: "GDI Hotel Commercial Profiles",
    tableId: "tbl8KSNUCRbIzEv15",
    adapter: "need-date-airtable-store-v3",
    identityField: "Profile ID",
  }),
  GDI_HOTEL_NEED_PERIODS: Object.freeze({
    name: "GDI Hotel Need Periods",
    tableId: "tbl50kj5XkGs22C6j",
    adapter: "need-date-airtable-store-v3",
    identityField: "Need Period ID",
  }),
  GDI_NEED_DATE_CAMPAIGN_RUNS: Object.freeze({
    name: "GDI Need-Date Campaign Runs",
    tableId: "tblghbxp4reaQWx2b",
    adapter: "need-date campaign writers",
    identityField: "Campaign ID",
  }),
  GDI_NEED_PERIOD_OPPORTUNITY_MATCHES: Object.freeze({
    name: "GDI Need Period Opportunity Matches",
    tableId: "tblOw5r7r95eQ1dPq",
    adapter: "need-date match writers",
    /** Canonical upsert identity (D7). Need Period ID is a linked dimension, not unique. */
    identityField: "Match Key",
  }),
});

/** Founder-approved D1 destinations vs deny-list for generic Full E2E discovery. */
export const FULL_E2E_APPROVED_WRITE_TABLES = Object.freeze([
  "Demand Generators",
  "Demand Programs",
  "Demand Generator Signals",
  "Group Demand Opportunities",
  "Private Event Venues",
  "Private Event Signals",
  "Hotel Venue Fit",
  "Hotel Demand Generator Fit",
  "GDI Research Runs",
  "GDI Research Targets",
]);

export const WRITE_OUTCOME = Object.freeze({
  CREATED: "CREATED",
  UPDATED: "UPDATED",
  UNCHANGED: "UNCHANGED",
  DUPLICATE_LINKED: "DUPLICATE_LINKED",
  DEFERRED: "DEFERRED",
  FAILED: "FAILED",
});

export const PERSISTENCE_CHECKPOINT = Object.freeze({
  AFTER_GLOBAL_IDENTITY: "AFTER_GLOBAL_IDENTITY",
  AFTER_SCREENING: "AFTER_SCREENING",
  AFTER_HOTEL_QUALIFICATION: "AFTER_HOTEL_QUALIFICATION",
  AFTER_DEEP_RESEARCH: "AFTER_DEEP_RESEARCH",
  AFTER_CONTACT_UPDATE: "AFTER_CONTACT_UPDATE",
  RUN_SUMMARY: "RUN_SUMMARY",
});

/** Explicit non-Airtable destinations (required when Airtable is wrong layer). */
export const NON_AIRTABLE_DESTINATIONS = Object.freeze({
  GLOBAL_INTELLIGENCE_FS: "data/group-demand-intelligence/global-intelligence/",
  HOTEL_OPPORTUNITIES_FS: "data/group-demand-intelligence/hotels/{hotelId}/opportunities.json",
  EXECUTION_LEDGER_FS:
    "data/group-demand-intelligence/execution-observability/{hotelId}/{runId}.json",
  RESEARCH_STAGE_FS: "data/group-demand-intelligence/research-stage/",
  PUBLICATION_PACKAGE:
    "lib/group-demand-intelligence/bundled-publication-packages/{hotelId}/",
  HOTEL_SALES_FEEDBACK: "data/group-demand-intelligence/hotels/{hotelId}/feedback.json",
  RAW_AUDIT_BOUNDED: "reports/ + execution ledger provider rows (bounded)",
});

export const AIRTABLE_PRODUCTION_WRITE_GUARD = Object.freeze({
  blocked: true,
  code: "AIRTABLE_PRODUCTION_WRITES_NOT_AUTHORIZED",
  note:
    "Canonical Full E2E may dry-run map and plan writes; live Airtable mutations require separate founder approval.",
});

const VENUE_SEGMENTS = new Set([
  "preferred_lodging_partnerships",
  "social_special_events",
  "local_catering_day_meetings",
]);
const PLANNER_SEGMENTS = new Set([
  "professional_event_planners",
  "travel_trade_group_intermediaries",
]);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Route one research object to existing Airtable table(s) and/or non-AT storage.
 */
export function routeResearchObjectToAirtable(input = {}) {
  const kind = text(input.kind || input.objectKind).toUpperCase();
  const segmentId = text(input.segmentId);
  const hotelQualified = input.hotelQualified === true;
  const credibleGlobal = input.credibleGlobal !== false;
  const unsupported = input.unsupported === true || input.disposition === "EXCLUDE";
  const outOfMarket = input.outOfMarket === true;

  if (unsupported && !credibleGlobal) {
    return {
      ok: true,
      airtable: [],
      nonAirtable: [NON_AIRTABLE_DESTINATIONS.RAW_AUDIT_BOUNDED],
      reason: "UNSUPPORTED_OR_SPECULATIVE_AUDIT_ONLY",
      checkpoint: PERSISTENCE_CHECKPOINT.AFTER_SCREENING,
    };
  }

  if (kind === "RAW_DISCOVERY" || kind === "SCREEN_ONLY") {
    return {
      ok: true,
      airtable: [],
      nonAirtable: [
        NON_AIRTABLE_DESTINATIONS.RAW_AUDIT_BOUNDED,
        NON_AIRTABLE_DESTINATIONS.GLOBAL_INTELLIGENCE_FS,
      ],
      reason: "RAW_NOT_AUTO_WRITTEN_TO_AIRTABLE",
      checkpoint: PERSISTENCE_CHECKPOINT.AFTER_SCREENING,
    };
  }

  if (kind === "ORGANIZATION" || kind === "COMPANY" || kind === "GENERATOR") {
    return {
      ok: true,
      airtable: [
        {
          table: GDI_AIRTABLE_TABLES.DEMAND_GENERATORS,
          role: "ORGANIZATION_IDENTITY",
          required: credibleGlobal,
        },
        outOfMarket || !hotelQualified
          ? null
          : {
              table: GDI_AIRTABLE_TABLES.HOTEL_DEMAND_GENERATOR_FIT,
              role: "HOTEL_SPECIFIC_FIT",
              required: hotelQualified,
            },
      ].filter(Boolean),
      nonAirtable: [NON_AIRTABLE_DESTINATIONS.GLOBAL_INTELLIGENCE_FS],
      reason: "ORG_TO_DEMAND_GENERATORS",
      checkpoint: PERSISTENCE_CHECKPOINT.AFTER_GLOBAL_IDENTITY,
    };
  }

  if (kind === "PROGRAM" || kind === "RECURRING_DEMAND") {
    return {
      ok: true,
      airtable: [
        { table: GDI_AIRTABLE_TABLES.DEMAND_PROGRAMS, role: "PROGRAM_IDENTITY" },
      ],
      nonAirtable: [NON_AIRTABLE_DESTINATIONS.GLOBAL_INTELLIGENCE_FS],
      reason: "PROGRAM_TO_DEMAND_PROGRAMS",
      checkpoint: PERSISTENCE_CHECKPOINT.AFTER_GLOBAL_IDENTITY,
    };
  }

  if (kind === "SIGNAL" || kind === "OCCURRENCE") {
    return {
      ok: true,
      airtable: [
        {
          table: GDI_AIRTABLE_TABLES.DEMAND_GENERATOR_SIGNALS,
          role: "DATED_SIGNAL",
        },
      ],
      nonAirtable: [],
      reason: "SIGNAL_TO_DEMAND_GENERATOR_SIGNALS",
      checkpoint: PERSISTENCE_CHECKPOINT.AFTER_SCREENING,
    };
  }

  if (
    kind === "VENUE" ||
    kind === "PRIVATE_EVENT_VENUE" ||
    (VENUE_SEGMENTS.has(segmentId) && kind === "PARTNERSHIP")
  ) {
    return {
      ok: true,
      airtable: [
        { table: GDI_AIRTABLE_TABLES.PRIVATE_EVENT_VENUES, role: "VENUE_IDENTITY" },
        hotelQualified
          ? {
              table: GDI_AIRTABLE_TABLES.HOTEL_VENUE_FIT,
              role: "HOTEL_VENUE_FIT",
            }
          : null,
      ].filter(Boolean),
      nonAirtable: [NON_AIRTABLE_DESTINATIONS.GLOBAL_INTELLIGENCE_FS],
      reason: "VENUE_TO_PRIVATE_EVENT_TABLES",
      checkpoint: PERSISTENCE_CHECKPOINT.AFTER_GLOBAL_IDENTITY,
    };
  }

  if (kind === "PLANNER" || PLANNER_SEGMENTS.has(segmentId)) {
    return {
      ok: true,
      airtable: [
        {
          table: GDI_AIRTABLE_TABLES.DEMAND_GENERATORS,
          role: "PLANNER_ORG_IDENTITY",
        },
        {
          table: GDI_AIRTABLE_TABLES.GDI_RESEARCH_TARGETS,
          role: "MONITORING_TARGET",
        },
        hotelQualified
          ? {
              table: GDI_AIRTABLE_TABLES.HOTEL_DEMAND_GENERATOR_FIT,
              role: "HOTEL_PARTNERSHIP_FIT",
            }
          : null,
      ].filter(Boolean),
      nonAirtable: [],
      reason: "PLANNER_TO_GENERATOR_AND_TARGET",
      checkpoint: PERSISTENCE_CHECKPOINT.AFTER_GLOBAL_IDENTITY,
    };
  }

  if (kind === "HOTEL_OPPORTUNITY" || kind === "QUALIFIED_OPPORTUNITY") {
    if (!hotelQualified) {
      return {
        ok: true,
        airtable: [],
        nonAirtable: [
          NON_AIRTABLE_DESTINATIONS.GLOBAL_INTELLIGENCE_FS,
          NON_AIRTABLE_DESTINATIONS.HOTEL_OPPORTUNITIES_FS,
        ],
        reason: "NOT_HOTEL_QUALIFIED_NO_OPPORTUNITY_ROW",
        checkpoint: PERSISTENCE_CHECKPOINT.AFTER_HOTEL_QUALIFICATION,
      };
    }
    return {
      ok: true,
      airtable: [
        {
          table: GDI_AIRTABLE_TABLES.DEMAND_GENERATORS,
          role: "ORGANIZATION_IDENTITY",
          required: true,
        },
        {
          table: GDI_AIRTABLE_TABLES.GROUP_DEMAND_OPPORTUNITIES,
          role: "HOTEL_QUALIFIED_OPPORTUNITY",
        },
      ],
      nonAirtable: [NON_AIRTABLE_DESTINATIONS.HOTEL_OPPORTUNITIES_FS],
      reason: "QUALIFIED_TO_GENERATOR_AND_GROUP_DEMAND_OPPORTUNITIES",
      checkpoint: PERSISTENCE_CHECKPOINT.AFTER_HOTEL_QUALIFICATION,
    };
  }

  if (kind === "RESEARCH_RUN") {
    return {
      ok: true,
      airtable: [
        { table: GDI_AIRTABLE_TABLES.GDI_RESEARCH_RUNS, role: "RUN_SUMMARY" },
      ],
      nonAirtable: [NON_AIRTABLE_DESTINATIONS.EXECUTION_LEDGER_FS],
      reason: "RUN_TO_RESEARCH_RUNS_PLUS_LEDGER",
      checkpoint: PERSISTENCE_CHECKPOINT.RUN_SUMMARY,
    };
  }

  if (kind === "RESEARCH_TARGET" || kind === "TARGET_RUN") {
    return {
      ok: true,
      airtable: [
        {
          table:
            kind === "TARGET_RUN"
              ? GDI_AIRTABLE_TABLES.GDI_RESEARCH_TARGET_RUNS
              : GDI_AIRTABLE_TABLES.GDI_RESEARCH_TARGETS,
          role: kind,
        },
      ],
      nonAirtable: [NON_AIRTABLE_DESTINATIONS.EXECUTION_LEDGER_FS],
      reason: "TARGET_COVERAGE_TABLES",
      checkpoint: PERSISTENCE_CHECKPOINT.RUN_SUMMARY,
    };
  }

  return {
    ok: false,
    airtable: [],
    nonAirtable: [NON_AIRTABLE_DESTINATIONS.RAW_AUDIT_BOUNDED],
    reason: "UNKNOWN_OBJECT_KIND",
    checkpoint: null,
  };
}

/**
 * Inventory of existing routing — for reports / founder review.
 */
export function listCanonicalAirtableRoutingInventory() {
  return {
    version: GDI_FULL_E2E_AIRTABLE_PERSISTENCE_CONTRACT_V1,
    base: GDI_AIRTABLE_BASE,
    tables: Object.values(GDI_AIRTABLE_TABLES),
    nonAirtableDestinations: NON_AIRTABLE_DESTINATIONS,
    writeOutcomes: WRITE_OUTCOME,
    checkpoints: PERSISTENCE_CHECKPOINT,
    publicationBoundary: {
      airtablePersistenceDoesNotPublish: true,
      customerPublishedSoT: NON_AIRTABLE_DESTINATIONS.PUBLICATION_PACKAGE,
      hotelSalesNeverSyncedToGlobal: true,
    },
    doNotCreate: [
      "GDI Global Demand Intelligence (prefer Demand Generators + Signals)",
      "GDI Hotel Opportunity Matches (prefer Hotel Demand Generator Fit / Hotel Venue Fit / Opportunities)",
      "Any second competing Airtable base",
    ],
    mutationGuard: AIRTABLE_PRODUCTION_WRITE_GUARD,
  };
}

export default {
  GDI_FULL_E2E_AIRTABLE_PERSISTENCE_CONTRACT_V1,
  GDI_AIRTABLE_BASE,
  GDI_AIRTABLE_TABLES,
  WRITE_OUTCOME,
  PERSISTENCE_CHECKPOINT,
  NON_AIRTABLE_DESTINATIONS,
  AIRTABLE_PRODUCTION_WRITE_GUARD,
  routeResearchObjectToAirtable,
  listCanonicalAirtableRoutingInventory,
};
