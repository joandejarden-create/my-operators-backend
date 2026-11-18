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
 * Prefer smallest extension: two logical tables (or views) linked by Global Demand ID.
 * Repository JSON remains SoT for evidence blobs; Airtable is operational index.
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
    proposedTables: [
      {
        name: "GDI Global Demand Intelligence",
        purpose: "Searchable index of independently sourced demand facts",
        fields: [
          { name: "Global Demand ID", type: "singleLineText", primary: true },
          { name: "Organization Name", type: "singleLineText" },
          { name: "Organization Key", type: "singleLineText" },
          { name: "Event Or Project Title", type: "singleLineText" },
          { name: "Demand Type", type: "singleSelect" },
          { name: "Segment ID", type: "singleLineText" },
          { name: "Market City", type: "singleLineText" },
          { name: "Market Region", type: "singleLineText" },
          { name: "Market Country", type: "singleLineText" },
          { name: "Market Key", type: "singleLineText" },
          { name: "State", type: "singleSelect" },
          { name: "Evidence URL Count", type: "number" },
          { name: "Primary Evidence URL", type: "url" },
          { name: "Discovery Run ID", type: "singleLineText" },
          { name: "Provider Run ID", type: "singleLineText" },
          { name: "First Discovered At", type: "dateTime" },
          { name: "Last Updated At", type: "dateTime" },
          { name: "Repo Path", type: "singleLineText" },
        ],
      },
      {
        name: "GDI Hotel Opportunity Matches",
        purpose: "Hotel-specific evaluation linked to global demand (no sales CRM)",
        fields: [
          { name: "Match Key", type: "singleLineText", primary: true },
          { name: "Hotel ID", type: "singleLineText" },
          {
            name: "Global Demand",
            type: "multipleRecordLinks",
            linkedTable: "GDI Global Demand Intelligence",
          },
          { name: "Candidate ID", type: "singleLineText" },
          { name: "Segment ID", type: "singleLineText" },
          { name: "Geographic Suitability", type: "singleSelect" },
          { name: "Match Reason", type: "singleSelect" },
          { name: "Research Status", type: "singleSelect" },
          { name: "Publication Status", type: "singleSelect" },
          { name: "Recommended Action", type: "singleSelect" },
          { name: "Updated At", type: "dateTime" },
        ],
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
      globalKey: "Global Demand ID",
      matchKey: "Hotel ID + Global Demand ID",
      idempotent: true,
      readBackVerification: "required before any production apply",
    },
    activationSteps: [
      "1. Founder approves schema + base target (prefer existing GDI/platform base — no new competing base).",
      "2. Document fields in docs/gdi-global-intelligence-airtable-fields.md.",
      "3. Create tables / options via ensure-* dry-run then apply.",
      "4. Wire dry-run upsert adapter with read-back fixtures.",
      "5. Separate founder approval for first production write batch.",
    ],
  };
}

/**
 * Map a local global record to Airtable fields — never mutates.
 */
export function mapGlobalRecordToAirtableFields(record = {}) {
  const evidence = record.evidence || [];
  return {
    mutationGuard: AIRTABLE_MUTATION_BLOCKED,
    table: "GDI Global Demand Intelligence",
    fields: {
      "Global Demand ID": record.globalDemandId,
      "Organization Name": record.organizationName,
      "Organization Key": record.organizationKey,
      "Event Or Project Title": record.eventOrProjectTitle,
      "Demand Type": record.demandType,
      "Segment ID": record.segmentId,
      "Market City": record.marketLocation?.city || null,
      "Market Region": record.marketLocation?.region || null,
      "Market Country": record.marketLocation?.country || null,
      "Market Key": record.marketLocation?.key || null,
      State: record.state,
      "Evidence URL Count": evidence.length,
      "Primary Evidence URL": evidence[0]?.url || null,
      "Discovery Run ID": record.discoveryRunId,
      "Provider Run ID": record.providerRunId,
      "First Discovered At": record.firstDiscoveredAt,
      "Last Updated At": record.lastUpdatedAt,
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
