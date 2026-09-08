/**
 * Airtable table contracts for AI Demand Leak Audit (Phase 2A).
 * Re-exports canonical schema from airtable-schema.js.
 */

export {
  LEAK_AUDIT_AIRTABLE_SCHEMA_VERSION,
  AIRTABLE_TABLE_CONTRACTS,
  LEAK_AUDIT_TABLE_NAMES,
  LEAK_AUDIT_TABLE_FIELDS,
  LEAK_AUDIT_COLLECTIONS,
  resolveLeakAuditTableName,
  listRequiredLeakAuditTables,
} from "./airtable-schema.js";
