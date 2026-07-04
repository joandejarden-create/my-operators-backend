/**
 * Read-only Airtable base record count audit (Metadata API + minimal pagination).
 */

import Airtable from "airtable";
import { metaFetch } from "./str-census-import/airtable-meta.mjs";

const PROTECTED_TABLE_PATTERNS = [
  /^hotel census$/i,
  /brand setup/i,
  /brand alias/i,
  /brand basics/i,
];

const STAGING_CLEANUP_HINT_PATTERNS = [
  /independent hotel source candidates/i,
  /independent hotel source evidence/i,
  /verified independent hotel census/i,
  /\braw\b/i,
  /staging/i,
  /import batch/i,
  /archive/i,
  /^temp\b/i,
  /test|e2e|qa/i,
];

function pickMinimalCountField(fields) {
  if (!fields?.length) return null;
  const byName = fields.find((f) => /^name$/i.test(f.name));
  if (byName) return byName.name;
  const textTypes = new Set([
    "singleLineText",
    "email",
    "url",
    "multilineText",
    "phoneNumber",
  ]);
  const text = fields.find((f) => textTypes.has(f.type));
  return text?.name || fields[0].name;
}

/**
 * @param {import('airtable').Base} base
 * @param {string} tableName
 * @param {string|null} countField
 */
export async function countTableRecordsPaginated(base, tableName, countField) {
  let count = 0;
  let pages = 0;
  const selectOpts = { pageSize: 100 };
  if (countField) selectOpts.fields = [countField];

  await new Promise((resolve, reject) => {
    base(tableName)
      .select(selectOpts)
      .eachPage(
        (records, fetchNextPage) => {
          count += records.length;
          pages++;
          fetchNextPage();
        },
        (err) => (err ? reject(err) : resolve())
      );
  });

  return {
    recordCount: count,
    countExact: true,
    countMethod: "paginated_select_minimal_fields",
    pagesFetched: pages,
    countFieldUsed: countField || "(record id only)",
  };
}

export async function fetchBaseTablesMetadata(baseId, token) {
  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) {
    return {
      metadataAvailable: false,
      metadataError: `Metadata API ${res.status}: ${JSON.stringify(json)}`,
      tables: [],
    };
  }
  return {
    metadataAvailable: true,
    metadataError: null,
    tables: json.tables || [],
  };
}

export function isProtectedTable(tableName) {
  return PROTECTED_TABLE_PATTERNS.some((re) => re.test(tableName));
}

export function suggestCleanupHint(tableName, recordCount) {
  if (isProtectedTable(tableName)) {
    return { safeCleanupCandidate: false, cleanupHint: "protected_do_not_modify" };
  }
  const staging = STAGING_CLEANUP_HINT_PATTERNS.some((re) => re.test(tableName));
  if (staging && recordCount >= 500) {
    return {
      safeCleanupCandidate: true,
      cleanupHint: "staging_or_workflow_table_review_archive_export_first",
    };
  }
  if (recordCount >= 10000) {
    return {
      safeCleanupCandidate: true,
      cleanupHint: "high_volume_table_review_before_any_delete",
    };
  }
  if (recordCount >= 1000) {
    return {
      safeCleanupCandidate: false,
      cleanupHint: "moderate_volume_monitor",
    };
  }
  return { safeCleanupCandidate: false, cleanupHint: "" };
}

/**
 * @param {object} opts
 */
export async function auditBaseRecordCounts(opts) {
  const token = opts.apiKey;
  const baseId = opts.baseId;
  const label = opts.baseLabel || baseId;

  const meta = await fetchBaseTablesMetadata(baseId, token);
  const base = new Airtable({ apiKey: token }).base(baseId);

  const tableResults = [];

  for (const table of meta.tables) {
    const countField = pickMinimalCountField(table.fields || []);
    let countResult;
    try {
      countResult = await countTableRecordsPaginated(
        base,
        table.name,
        countField
      );
    } catch (err) {
      countResult = {
        recordCount: null,
        countExact: false,
        countMethod: "error",
        pagesFetched: 0,
        countFieldUsed: countField,
        error: err.message || String(err),
      };
    }

    const recordCount = countResult.recordCount ?? 0;
    const hints = suggestCleanupHint(table.name, recordCount);

    tableResults.push({
      baseId,
      baseLabel: label,
      tableName: table.name,
      tableId: table.id,
      recordCount,
      countExact: countResult.countExact,
      countMethod: countResult.countMethod,
      countFieldUsed: countResult.countFieldUsed,
      pagesFetched: countResult.pagesFetched,
      metadataRecordCountAvailable: false,
      protectedTable: isProtectedTable(table.name),
      safeCleanupCandidate: hints.safeCleanupCandidate,
      cleanupHint: hints.cleanupHint,
      error: countResult.error || null,
    });

    if (opts.onTableComplete) {
      opts.onTableComplete(table.name, recordCount);
    }
  }

  tableResults.sort((a, b) => (b.recordCount || 0) - (a.recordCount || 0));

  const totalRecords = tableResults.reduce(
    (sum, t) => sum + (Number.isFinite(t.recordCount) ? t.recordCount : 0),
    0
  );

  const over1000 = tableResults.filter((t) => (t.recordCount || 0) > 1000);
  const over10000 = tableResults.filter((t) => (t.recordCount || 0) > 10000);
  const cleanupCandidates = tableResults.filter((t) => t.safeCleanupCandidate);

  return {
    baseId,
    baseLabel: label,
    metadataAvailable: meta.metadataAvailable,
    metadataError: meta.metadataError,
    tableCount: tableResults.length,
    totalRecords,
    totalRecordsExact: tableResults.every((t) => t.countExact && !t.error),
    tablesOver1000: over1000.length,
    tablesOver10000: over10000.length,
    tables: tableResults,
    tablesOver1000List: over1000.map((t) => ({
      tableName: t.tableName,
      recordCount: t.recordCount,
    })),
    tablesOver10000List: over10000.map((t) => ({
      tableName: t.tableName,
      recordCount: t.recordCount,
    })),
    safeCleanupCandidates: cleanupCandidates.map((t) => ({
      tableName: t.tableName,
      recordCount: t.recordCount,
      cleanupHint: t.cleanupHint,
    })),
    airtableWrites: false,
    sensitiveFieldsLoaded: false,
    strFieldsUsed: false,
  };
}

export function resolveBaseConfigs(argvBase, argvBaseId, argvLabel) {
  const token = process.env.AIRTABLE_API_KEY;
  if (!token) throw new Error("Missing AIRTABLE_API_KEY");

  if (argvBaseId) {
    return [
      {
        baseId: argvBaseId,
        label: argvLabel || "explicit_base_id",
      },
    ];
  }

  const primary = process.env.AIRTABLE_BASE_ID;
  const alt = process.env.AIRTABLE_BASE_ID_ALT;
  const mode = (argvBase || (primary && alt ? "all" : "alt")).toLowerCase();

  if (mode === "all") {
    const configs = [];
    if (alt) configs.push({ baseId: alt, label: "AIRTABLE_BASE_ID_ALT" });
    if (primary && primary !== alt) {
      configs.push({ baseId: primary, label: "AIRTABLE_BASE_ID" });
    }
    if (!configs.length) throw new Error("Set AIRTABLE_BASE_ID and/or AIRTABLE_BASE_ID_ALT");
    return configs;
  }

  if (mode === "primary") {
    if (!primary) throw new Error("Missing AIRTABLE_BASE_ID");
    return [{ baseId: primary, label: "AIRTABLE_BASE_ID" }];
  }

  const id = alt || primary;
  if (!id) throw new Error("Missing AIRTABLE_BASE_ID_ALT or AIRTABLE_BASE_ID");
  return [{ baseId: id, label: alt ? "AIRTABLE_BASE_ID_ALT" : "AIRTABLE_BASE_ID" }];
}

/** @param {string} token */
export async function listAccessibleBases(token) {
  const bases = [];
  let offset;
  do {
    const url = offset
      ? `https://api.airtable.com/v0/meta/bases?offset=${encodeURIComponent(offset)}`
      : "https://api.airtable.com/v0/meta/bases";
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const json = await res.json();
    if (!res.ok) {
      throw new Error(`list bases failed ${res.status}: ${JSON.stringify(json)}`);
    }
    bases.push(...(json.bases || []));
    offset = json.offset;
  } while (offset);
  return bases;
}

export function slugifyReportLabel(label) {
  return String(label || "audit")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}
