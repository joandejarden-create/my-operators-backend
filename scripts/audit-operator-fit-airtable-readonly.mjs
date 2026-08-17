#!/usr/bin/env node
/**
 * AUDIT UTILITY (read-only) — Operator Fit Engine current-state Airtable inspection.
 *
 * - Uses existing .env conventions (AIRTABLE_BASE_ID, AIRTABLE_API_KEY).
 * - Fetches table/field metadata + limited Active Operator Setup records.
 * - Computes field-completeness stats; redacts free-text narratives.
 * - Performs NO write / update / delete / upsert operations.
 * - Not connected to production routes.
 *
 * Usage:
 *   node scripts/audit-operator-fit-airtable-readonly.mjs
 *   node scripts/audit-operator-fit-airtable-readonly.mjs --out reports/operator-fit-airtable-readonly.json
 */
import "dotenv/config";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const args = process.argv.slice(2);
const outFlag = args.indexOf("--out");
const outPath =
  outFlag >= 0 && args[outFlag + 1]
    ? join(root, args[outFlag + 1])
    : join(root, "reports", "operator-fit-airtable-readonly.json");

const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;

const OPERATOR_TABLES = [
  process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master",
  "Operator Setup - Profile & Positioning",
  "Operator Setup - Platform & Markets",
  "Operator Setup - Commercial Fit & Terms",
  "Operator Setup - Governance, Delivery & Diligence",
  "Operator Setup - Case Studies",
  "Operator Setup - Engagement & Reporting",
  "Operator Setup - Explorer Materials",
  "Operator Deal Requests",
];

const DEAL_TABLES = [
  process.env.AIRTABLE_TABLE_DEALS || "Deals",
  "Location & Property",
  "Market - Performance - Deal & Capital Structure",
  "Strategic Intent - Operational - Key Challenges",
];

/** Fields used by scoreOperatorMatchForDeal / company alignment (prefill + Airtable names). */
const SCORING_FIELD_HINTS = [
  "Active Countries",
  "Active Markets / Cities",
  "Market Presence Type",
  "chainScalesSupported",
  "Service Models Supported",
  "Management Structures Supported",
  "Offered Services",
  "bf_selected_deal_structures",
  "bf_not_ideal_for",
  "New-Build Opening Experience",
  "Pre-Opening Support Capability",
  "Conversion / Reflag Experience",
  "Owner Reporting Level",
  "Governance Cadence",
  "Revenue Management Capability",
  "Sales Platform",
  "F&B Capability Level",
  "brands",
  "Brand Families Operated",
  "feeStructureSummary",
  "lessIdealSituations",
  "company_name",
  "submission_status",
  "Data Confidence Level",
  "Source Type",
  "Last Updated Date",
];

const REDACT_TYPES = new Set(["multilineText", "richText", "singleLineText", "email", "phoneNumber", "url"]);

function authHeaders() {
  return { Authorization: "Bearer " + apiKey };
}

async function fetchJson(url) {
  const res = await fetch(url, { headers: authHeaders() });
  const data = await res.json();
  if (!res.ok) {
    const msg = data?.error?.message || data?.error || res.statusText;
    throw new Error(`${res.status} ${url}: ${msg}`);
  }
  return data;
}

async function fetchMetaTables() {
  return fetchJson(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`);
}

async function fetchActiveMasters(limit = 45) {
  const table = encodeURIComponent(
    process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master"
  );
  const statusField = process.env.AIRTABLE_OPERATOR_SETUP_SUBMISSION_STATUS_FIELD || "submission_status";
  const activeValues = String(
    process.env.AIRTABLE_OPERATOR_SETUP_ACTIVE_STATUS_VALUES || "Active"
  )
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  const formula =
    activeValues.length === 1
      ? `{${statusField}}='${activeValues[0].replace(/'/g, "\\'")}'`
      : `OR(${activeValues.map((v) => `{${statusField}}='${v.replace(/'/g, "\\'")}'`).join(",")})`;
  const params = new URLSearchParams({
    filterByFormula: formula,
    pageSize: String(Math.min(limit, 100)),
  });
  const data = await fetchJson(
    `https://api.airtable.com/v0/${baseId}/${table}?${params.toString()}`
  );
  return data.records || [];
}

async function fetchLinkedRows(tableName, masterIds, linkFieldCandidates = ["Operator", "operator", "Master"]) {
  const table = encodeURIComponent(tableName);
  const records = [];
  let offset = null;
  let pages = 0;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    const data = await fetchJson(
      `https://api.airtable.com/v0/${baseId}/${table}?${params.toString()}`
    );
    records.push(...(data.records || []));
    offset = data.offset || null;
    pages += 1;
    if (pages >= 5) break; // safety: audit cap
  } while (offset);

  const idSet = new Set(masterIds);
  return records.filter((r) => {
    const f = r.fields || {};
    for (const key of linkFieldCandidates) {
      const v = f[key];
      if (Array.isArray(v) && v.some((id) => idSet.has(id))) return true;
    }
    return false;
  });
}

function isPopulated(val) {
  if (val == null) return false;
  if (typeof val === "string") return val.trim().length > 0;
  if (typeof val === "number") return Number.isFinite(val);
  if (typeof val === "boolean") return true;
  if (Array.isArray(val)) return val.length > 0;
  if (typeof val === "object") return Object.keys(val).length > 0;
  return Boolean(val);
}

function redactValue(fieldMeta, value) {
  if (value == null) return null;
  if (Array.isArray(value)) {
    if (value.every((x) => typeof x === "string" && x.startsWith("rec"))) {
      return { type: "linkedRecords", count: value.length };
    }
    if (fieldMeta?.type === "multipleSelects" || fieldMeta?.type === "singleSelect") {
      return value;
    }
    return { type: "array", count: value.length };
  }
  if (REDACT_TYPES.has(fieldMeta?.type) || typeof value === "string") {
    const s = String(value);
    if (s.length <= 40 && !/@/.test(s) && !s.startsWith("http")) return s;
    return { type: "redactedText", length: s.length };
  }
  return value;
}

function completenessForFields(records, fieldNames) {
  const n = records.length || 1;
  return fieldNames.map((name) => {
    const populated = records.filter((r) => isPopulated(r.fields?.[name])).length;
    return {
      field: name,
      operatorsPopulated: `${populated}/${records.length}`,
      completenessPct: Math.round((populated / n) * 1000) / 10,
    };
  });
}

function summarizeTableMeta(table) {
  return {
    id: table.id,
    name: table.name,
    primaryFieldId: table.primaryFieldId,
    fieldCount: (table.fields || []).length,
    fields: (table.fields || []).map((f) => ({
      id: f.id,
      name: f.name,
      type: f.type,
      options:
        f.options?.choices?.map((c) => c.name) ||
        (f.options?.linkedTableId ? { linkedTableId: f.options.linkedTableId } : undefined) ||
        (f.options?.formula ? { formula: "[present]" } : undefined),
    })),
  };
}

async function main() {
  if (!baseId || !apiKey) {
    throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required (read-only audit)");
  }

  console.log("[audit-operator-fit] READ-ONLY Airtable inspection starting…");
  const meta = await fetchMetaTables();
  const allTables = meta.tables || [];
  const wanted = new Set([...OPERATOR_TABLES, ...DEAL_TABLES]);
  const relevantMeta = allTables
    .filter((t) => wanted.has(t.name) || /operator|brand setup|deal/i.test(t.name))
    .map(summarizeTableMeta);

  const masters = await fetchActiveMasters(45);
  const masterIds = masters.map((r) => r.id);
  console.log(`[audit-operator-fit] Active masters fetched: ${masters.length}`);

  const profileTable = "Operator Setup - Profile & Positioning";
  const platformTable = "Operator Setup - Platform & Markets";
  const commercialTable = "Operator Setup - Commercial Fit & Terms";
  const governanceTable = "Operator Setup - Governance, Delivery & Diligence";

  const [profiles, platforms, commercials, governances] = await Promise.all([
    fetchLinkedRows(profileTable, masterIds).catch((e) => {
      console.warn("profile fetch skipped:", e.message);
      return [];
    }),
    fetchLinkedRows(platformTable, masterIds).catch((e) => {
      console.warn("platform fetch skipped:", e.message);
      return [];
    }),
    fetchLinkedRows(commercialTable, masterIds).catch((e) => {
      console.warn("commercial fetch skipped:", e.message);
      return [];
    }),
    fetchLinkedRows(governanceTable, masterIds).catch((e) => {
      console.warn("governance fetch skipped:", e.message);
      return [];
    }),
  ]);

  const byTable = {
    master: masters,
    profile: profiles,
    platform: platforms,
    commercial: commercials,
    governance: governances,
  };

  const completeness = {
    master: completenessForFields(
      masters,
      ["company_name", "submission_status", "Data Confidence Level", "Source Type", "Last Updated Date"].filter(
        (n) => masters.some((r) => n in (r.fields || {})) || true
      )
    ),
    platform: completenessForFields(platforms, [
      "Active Countries",
      "Active Markets / Cities",
      "Market Presence Type",
    ]),
    profile: completenessForFields(profiles, [
      "chainScalesSupported",
      "Service Models Supported",
      "brands",
      "Brand Families Operated",
    ]),
    commercial: completenessForFields(commercials, [
      "Management Structures Supported",
      "New-Build Opening Experience",
      "Pre-Opening Support Capability",
      "Conversion / Reflag Experience",
      "bf_selected_deal_structures",
      "bf_not_ideal_for",
    ]),
    governance: completenessForFields(governances, [
      "Offered Services",
      "Owner Reporting Level",
      "Governance Cadence",
      "Revenue Management Capability",
      "Sales Platform",
      "F&B Capability Level",
    ]),
  };

  // Merge representative redacted sample (first 2 masters only)
  const samples = masters.slice(0, 2).map((m) => {
    const id = m.id;
    const pick = (rows) => rows.find((r) => {
      const link = r.fields?.Operator || r.fields?.operator || r.fields?.Master;
      return Array.isArray(link) && link.includes(id);
    });
    const p = pick(profiles);
    const pl = pick(platforms);
    const c = pick(commercials);
    const g = pick(governances);
    const tableMeta = (name) => relevantMeta.find((t) => t.name === name);
    const redactRow = (row, tableName) => {
      if (!row) return null;
      const metaT = tableMeta(tableName);
      const out = {};
      for (const [k, v] of Object.entries(row.fields || {})) {
        const fm = metaT?.fields?.find((f) => f.name === k);
        out[k] = redactValue(fm, v);
      }
      return { id: row.id, fields: out };
    };
    return {
      masterId: id,
      companyName: String(m.fields?.company_name || "[redacted]"),
      master: redactRow(m, process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master"),
      profile: redactRow(p, profileTable),
      platform: redactRow(pl, platformTable),
      commercial: redactRow(c, commercialTable),
      governance: redactRow(g, governanceTable),
    };
  });

  const scoringHintsPresence = SCORING_FIELD_HINTS.map((name) => {
    const tablesChecked = [];
    let populated = 0;
    let total = 0;
    for (const [key, rows] of Object.entries(byTable)) {
      if (!rows.length) continue;
      const hits = rows.filter((r) => Object.prototype.hasOwnProperty.call(r.fields || {}, name));
      if (hits.length) {
        tablesChecked.push(key);
        total += rows.length;
        populated += rows.filter((r) => isPopulated(r.fields?.[name])).length;
      }
    }
    return {
      field: name,
      foundOnTables: tablesChecked,
      populated,
      totalChecked: total,
      completenessPct: total ? Math.round((populated / total) * 1000) / 10 : null,
    };
  });

  const report = {
    auditUtility: "audit-operator-fit-airtable-readonly",
    mode: "read-only",
    generatedAt: new Date().toISOString(),
    baseIdPresent: Boolean(baseId),
    activeOperatorCount: masters.length,
    linkedRowCounts: {
      profile: profiles.length,
      platform: platforms.length,
      commercial: commercials.length,
      governance: governances.length,
    },
    relevantTables: relevantMeta.map((t) => ({
      name: t.name,
      id: t.id,
      fieldCount: t.fieldCount,
      scoringRelatedFields: t.fields
        .filter((f) =>
          SCORING_FIELD_HINTS.some(
            (h) => h.toLowerCase() === f.name.toLowerCase() || f.name.toLowerCase().includes(h.toLowerCase().slice(0, 12))
          )
        )
        .map((f) => ({ name: f.name, type: f.type })),
    })),
    fieldCatalog: relevantMeta,
    completeness,
    scoringHintsPresence,
    redactedSamples: samples,
    notes: [
      "No Airtable writes were performed.",
      "Free-text values longer than 40 chars and contact-like strings are redacted.",
      "Linked record IDs are replaced with counts.",
      "Completeness is based on Active Master records and their linked child rows only.",
    ],
  };

  mkdirSync(dirname(outPath), { recursive: true });
  writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");
  console.log("[audit-operator-fit] Wrote", outPath);
  console.log(
    JSON.stringify(
      {
        activeOperatorCount: report.activeOperatorCount,
        linkedRowCounts: report.linkedRowCounts,
        completenessSummary: Object.fromEntries(
          Object.entries(completeness).map(([k, rows]) => [
            k,
            rows.map((r) => `${r.field}:${r.completenessPct}%`),
          ])
        ),
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error("[audit-operator-fit] FAILED:", err.message || err);
  process.exit(1);
});
