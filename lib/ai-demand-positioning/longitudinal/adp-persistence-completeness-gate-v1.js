/**
 * ADP_PERSISTENCE_COMPLETENESS_GATE_V1
 *
 * Before research-cycle finalization, verify every required history table has
 * intended records for the certified property+period. Detects partial writes.
 */

import { HISTORY_TABLES } from "./airtable-history-schema-final-v1.js";
import {
  ADP_E2E_PERSISTENCE_TABLE_CONTRACT_V1,
  requiredHistoryTablesForFinalization,
  assertPersistenceDistinctFromPublication,
  PERSISTENCE_LIFECYCLE,
  PUBLICATION_LIFECYCLE,
} from "./adp-e2e-persistence-table-contract-v1.js";

export const ADP_PERSISTENCE_COMPLETENESS_GATE_V1 = "ADP_PERSISTENCE_COMPLETENESS_GATE_V1";

/**
 * @param {object} opts
 * @param {object} opts.manifestCounts - counts by table from buildHistoricalWriteManifest
 * @param {object} opts.storedCounts - actual stored/readback counts by table
 * @param {string[]} [opts.requiredTables]
 * @param {boolean} [opts.requirePromptLedger]
 * @param {boolean} [opts.requireEvidence]
 * @param {boolean} [opts.correctionMode]
 */
export function assessPersistenceCompleteness({
  manifestCounts = {},
  storedCounts = {},
  requiredTables = requiredHistoryTablesForFinalization(),
  requirePromptLedger = true,
  requireEvidence = true,
  correctionMode = false,
  persistenceState = PERSISTENCE_LIFECYCLE.VERIFYING,
  publicationState = PUBLICATION_LIFECYCLE.NOT_PUBLISHED,
  claimedComplete = false,
} = {}) {
  const defects = [];
  const tableResults = {};

  for (const table of requiredTables) {
    if (table === HISTORY_TABLES.REPORT_CORRECTIONS && !correctionMode) {
      tableResults[table] = { status: "OPTIONAL_SKIP", expected: 0, stored: 0 };
      continue;
    }
    if (table === HISTORY_TABLES.PROMPT_LEDGER && !requirePromptLedger) {
      tableResults[table] = { status: "OPTIONAL_SKIP", expected: 0, stored: 0 };
      continue;
    }
    if (table === HISTORY_TABLES.EVIDENCE_INDEX && !requireEvidence) {
      tableResults[table] = { status: "OPTIONAL_SKIP", expected: 0, stored: 0 };
      continue;
    }

    const expected = Number(manifestCounts[table] || 0);
    // Prefer storedCounts; if only manifest provided for dry-run, treat expected as plan
    const actualStored =
      storedCounts[table] != null ? Number(storedCounts[table]) : null;

    if (expected <= 0 && (table === HISTORY_TABLES.EVIDENCE_INDEX || table === HISTORY_TABLES.PROMPT_LEDGER)) {
      tableResults[table] = {
        status: "MISSING_FROM_MANIFEST",
        expected,
        stored: actualStored,
      };
      defects.push({
        code: "REQUIRED_TABLE_EMPTY_IN_MANIFEST",
        table,
        detail: "Certified cycle requires evidence/prompt ledger rows — pass period observations",
      });
      continue;
    }

    if (expected <= 0) {
      tableResults[table] = { status: "EMPTY_MANIFEST", expected, stored: actualStored };
      defects.push({ code: "REQUIRED_TABLE_EMPTY_IN_MANIFEST", table });
      continue;
    }

    if (actualStored != null && actualStored < expected) {
      tableResults[table] = {
        status: "PARTIAL",
        expected,
        stored: actualStored,
      };
      defects.push({
        code: "PARTIAL_HISTORY_WRITE",
        table,
        detail: `stored ${actualStored} < expected ${expected}`,
      });
      continue;
    }

    tableResults[table] = {
      status: actualStored == null ? "PLANNED" : "COMPLETE",
      expected,
      stored: actualStored,
    };
  }

  const pubCheck = assertPersistenceDistinctFromPublication({
    persistenceState,
    publicationState,
    claimedComplete,
  });
  const hardDefects = [
    ...defects,
    ...pubCheck.defects.filter((d) => d.severity !== "INFO"),
  ];

  const complete =
    hardDefects.length === 0 &&
    Object.values(tableResults).every(
      (t) =>
        t.status === "COMPLETE" ||
        t.status === "PLANNED" ||
        t.status === "OPTIONAL_SKIP"
    );

  return {
    gate: ADP_PERSISTENCE_COMPLETENESS_GATE_V1,
    contract: ADP_E2E_PERSISTENCE_TABLE_CONTRACT_V1,
    ok: hardDefects.length === 0,
    complete: claimedComplete ? complete && hardDefects.length === 0 : complete,
    tableResults,
    defects: [...hardDefects, ...pubCheck.defects.filter((d) => d.severity === "INFO")],
    recoverable:
      hardDefects.every((d) =>
        ["PARTIAL_HISTORY_WRITE", "REQUIRED_TABLE_EMPTY_IN_MANIFEST"].includes(d.code)
      ) && hardDefects.length > 0,
  };
}

/**
 * Count records in a mock store or plan by table for property+period.
 */
export function countRecordsByTable(records, { propertyId, periodId } = {}) {
  const counts = Object.create(null);
  for (const r of records || []) {
    const f = r.fields || {};
    if (propertyId && f["Property ID"] && f["Property ID"] !== propertyId) continue;
    if (periodId && f["Period ID"] && f["Period ID"] !== periodId) continue;
    counts[r.table] = (counts[r.table] || 0) + 1;
  }
  return counts;
}
