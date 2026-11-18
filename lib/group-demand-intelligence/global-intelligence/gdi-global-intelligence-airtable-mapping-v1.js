/**
 * Airtable mapping proposal for global intelligence — dry-run / schema only.
 * Production mutations are blocked until founder authorization.
 */

export const GDI_GLOBAL_INTELLIGENCE_AIRTABLE_MAPPING_V1 =
  "gdi-global-intelligence-airtable-mapping-v1";

export const AIRTABLE_MUTATION_BLOCKED = Object.freeze({
  blocked: true,
  code: "AIRTABLE_PRODUCTION_WRITES_NOT_AUTHORIZED",
  note: "Local filesystem store is canonical until founder-approved activation.",
});

/**
 * Prefer EXISTING Demand Generators / Programs / Signals / Fit tables.
 * Repository JSON remains SoT for evidence blobs; Airtable is operational index.
 * Do not create competing "GDI Global Demand Intelligence" tables unless founder
 * explicitly rejects reuse of Demand Generators.
 */
export function proposeGlobalIntelligenceAirtableSchema() {
  return {
    version: GDI_GLOBAL_INTELLIGENCE_AIRTABLE_MAPPING_V1,
    mutationGuard: AIRTABLE_MUTATION_BLOCKED,
    ownership: {
      authoritativeStore: "repository:data/group-demand-intelligence/global-intelligence/",
      airtableRole: "OPERATIONAL_INDEX_AND_SEARCH",
      hotelOpportunitySoT: "existing hotel publication packages + hotel opportunities.json",
      hotelSalesActivitySoT: "hotel-private feedback / CRM — NEVER synced to global table",
    },
    preferExistingTables: [
      {
        name: "Demand Generators",
        tableId: "tblykUVOjGVkawvdD",
        purpose: "Organization / company identity for independently sourced demand",
      },
      {
        name: "Demand Programs",
        tableId: "tblxm7wqfFNG37Z2m",
        purpose: "Recurring demand programs",
      },
      {
        name: "Demand Generator Signals",
        tableId: "tblZ0cOM78uYMCTXx",
        purpose: "Dated occurrences / evidence signals",
      },
      {
        name: "Hotel Demand Generator Fit",
        tableId: "tblqamXbIk9n6gXfV",
        purpose: "Hotel-specific evaluation (not global rejection)",
      },
    ],
    proposedTables: [
      {
        name: "(DEPRECATED PROPOSAL) GDI Global Demand Intelligence",
        purpose:
          "Do not create — map to Demand Generators + Signals instead (founder directive).",
        deprecated: true,
      },
      {
        name: "(DEPRECATED PROPOSAL) GDI Hotel Opportunity Matches",
        purpose:
          "Do not create — map to Hotel Demand Generator Fit / Hotel Venue Fit / Group Demand Opportunities.",
        deprecated: true,
      },
    ],
    explicitlyExcludedFromAirtable: [
      "hotelSalesFeedback",
      "rates",
      "outreachHistory",
      "CRM notes",
      "share tokens",
      "negotiations",
    ],
    upsertLogic: {
      organizationKey: "Demand Generator ID",
      hotelFitKey: "Hotel Generator Fit ID",
      opportunityKey: "opportunityId",
      idempotent: true,
      readBackVerification: "required before any production apply",
    },
    activationSteps: [
      "1. Founder approves reuse of existing Demand Generators / Opportunities / PE / Research tables (no new competing tables).",
      "2. Review Bethesda reconciliation + dry-run write set.",
      "3. Confirm adapter permissions on base appa2cE7FTRmIbB32.",
      "4. Wire production apply path with read-back certification (still gated).",
      "5. Separate founder approval for first production write batch / selective backfill.",
    ],
  };
}

/**
 * Map a local global record to EXISTING Demand Generators fields — never mutates.
 * Full evidence blobs stay on the filesystem SoT.
 */
export function mapGlobalRecordToAirtableFields(record = {}) {
  const evidence = record.evidence || [];
  return {
    mutationGuard: AIRTABLE_MUTATION_BLOCKED,
    table: "Demand Generators",
    tableId: "tblykUVOjGVkawvdD",
    fields: {
      "Demand Generator ID":
        record.organizationKey ||
        record.globalDemandId ||
        null,
      "Organization Name": record.organizationName,
      "Primary Markets": record.marketLocation?.city || null,
      "Research Reason": record.segmentId || record.demandType || null,
      "Source URLs": evidence
        .map((e) => e?.url)
        .filter(Boolean)
        .join("\n") || null,
      "Generator Status": record.state === "VERIFIED" ? "VERIFIED" : "DISCOVERED",
      "Last Research Result": record.discoveryRunId || record.providerRunId || null,
    },
    filesystemSoT: {
      globalDemandId: record.globalDemandId,
      marketKey: record.marketLocation?.key || null,
      evidenceUrlCount: evidence.length,
      eventOrProjectTitle: record.eventOrProjectTitle || null,
      discoveryRunId: record.discoveryRunId || null,
    },
  };
}

/**
 * Dry-run adapter — returns planned upserts; refuses live writes.
 */
export function dryRunUpsertGlobalIntelligenceToAirtable(records = [], opts = {}) {
  if (opts.apply === true || opts.liveWrite === true) {
    return {
      ok: false,
      ...AIRTABLE_MUTATION_BLOCKED,
    };
  }
  return {
    ok: true,
    dryRun: true,
    version: GDI_GLOBAL_INTELLIGENCE_AIRTABLE_MAPPING_V1,
    plannedUpserts: records.map((r) => mapGlobalRecordToAirtableFields(r)),
    mutationGuard: AIRTABLE_MUTATION_BLOCKED,
  };
}

export default {
  GDI_GLOBAL_INTELLIGENCE_AIRTABLE_MAPPING_V1,
  AIRTABLE_MUTATION_BLOCKED,
  proposeGlobalIntelligenceAirtableSchema,
  mapGlobalRecordToAirtableFields,
  dryRunUpsertGlobalIntelligenceToAirtable,
};
