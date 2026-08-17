#!/usr/bin/env node
/**
 * Read-only: export live Airtable schema for Deals workflow tables and diff vs repo references.
 *
 * Usage:
 *   node scripts/export-and-diff-airtable-deals-schema.mjs
 *
 * Requires:
 *   AIRTABLE_API_KEY (or AIRTABLE_PAT) with schema.bases:read on AIRTABLE_BASE_ID
 *   AIRTABLE_BASE_ID
 *
 * Outputs:
 *   reports/airtable-deals-schema-live.json
 *   reports/airtable-deals-schema-diff.md
 *
 * Does NOT read or write deal records. Does NOT modify Airtable schema.
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  buildExpectedDealsSchemaRegistry,
  DEAL_SCHEMA_AUDIT_TABLE_KEYS,
  matchSpecToLive,
} from "../lib/deal-schema-audit/expected-deals-schema-registry.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const LIVE_JSON = join(ROOT, "reports", "airtable-deals-schema-live.json");
const DIFF_MD = join(ROOT, "reports", "airtable-deals-schema-diff.md");

async function metaFetch(baseId, token, path = "/tables") {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${path}`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

function mdEscape(s) {
  return String(s || "").replace(/\|/g, "\\|");
}

function matchPhase5bRow(row, liveNames) {
  const liveSet = new Set(liveNames);
  if (liveSet.has(row.field)) return { live: true, matchedAs: row.field, kind: "exact" };
  for (const alias of row.aliases || []) {
    if (liveSet.has(alias)) return { live: true, matchedAs: alias, kind: "alias" };
  }
  const norm = (s) => String(s).trim().toLowerCase();
  const hit = liveNames.find((n) => norm(n) === norm(row.field));
  if (hit) return { live: true, matchedAs: hit, kind: "exact" };
  for (const alias of row.aliases || []) {
    const ahit = liveNames.find((n) => norm(n) === norm(alias));
    if (ahit) return { live: true, matchedAs: ahit, kind: "alias" };
  }
  return { live: false, matchedAs: null, kind: "missing" };
}

function buildDiffSection(tableKey, config, liveTable, phase5bRows = []) {
  const fieldSpecs = (config.fieldSpecs || []).filter((s) => !s.deprecated);
  const tableName = config.tableName;

  if (!liveTable) {
    const optional = config.optionalTable ? " (optional — not in audited base)" : "";
    return {
      tableKey,
      tableName,
      found: false,
      optionalTable: Boolean(config.optionalTable),
      liveFieldCount: 0,
      expectedCount: fieldSpecs.length,
      exactMatches: [],
      aliasMatches: [],
      missing: fieldSpecs.map((s) => ({
        expected: s.name,
        classification: s.classification || "Confirmed Missing",
        notes: s.notes || config.notes || "",
      })),
      undocumented: [],
      phase5b: phase5bRows.map((row) => ({ ...row, live: false, matchedAs: null, kind: "missing" })),
      lines: [
        `### ${tableName}${optional}`,
        "",
        config.notes ? `> ${config.notes}` : "",
        config.notes ? "" : null,
        "**Status:** Table not found in live base",
        "",
        `**Expected sources:** ${config.sources.join(", ")}`,
        "",
      ].filter(Boolean),
    };
  }

  const liveFields = (liveTable.fields || []).map((f) => ({
    name: f.name,
    id: f.id,
    type: f.type,
  }));
  const liveNames = liveFields.map((f) => f.name);

  const exactMatches = [];
  const aliasMatches = [];
  const missing = [];

  for (const spec of fieldSpecs) {
    const m = matchSpecToLive(spec, liveNames);
    if (m.kind === "exact") {
      exactMatches.push({ expected: spec.name, live: m.live });
    } else if (m.kind === "alias") {
      aliasMatches.push({
        expected: spec.name,
        alias: m.matchedAlias,
        live: m.live,
        notes: spec.notes || "",
      });
    } else {
      missing.push({
        expected: spec.name,
        classification: spec.classification || "Confirmed Missing",
        notes: spec.notes || "",
        aliases: spec.aliases || [],
      });
    }
  }

  const coveredLive = new Set();
  for (const spec of fieldSpecs) {
    const m = matchSpecToLive(spec, liveNames);
    if (m.live) coveredLive.add(m.live);
  }
  const undocumented = liveNames.filter((n) => !coveredLive.has(n));

  const phase5bStatus = phase5bRows.map((row) => {
    const m = matchPhase5bRow(row, liveNames);
    return { ...row, live: m.live, matchedAs: m.matchedAs, kind: m.kind };
  });

  const lines = [];
  lines.push(`### ${tableName}`, "");
  lines.push(`| Property | Value |`);
  lines.push(`|----------|-------|`);
  lines.push(`| Live table ID | \`${liveTable.id}\` |`);
  lines.push(`| Live fields | ${liveFields.length} |`);
  lines.push(`| Repo-expected specs | ${fieldSpecs.length} |`);
  lines.push(`| Exact matches | ${exactMatches.length} |`);
  lines.push(`| Alias matches | ${aliasMatches.length} |`);
  lines.push(`| Missing from live | ${missing.length} |`);
  lines.push(`| Live but not in registry | ${undocumented.length} |`);
  lines.push(`| Sources | ${config.sources.join(", ")} |`);
  lines.push("");

  if (aliasMatches.length) {
    lines.push("**Alias matches (this table)**", "");
    lines.push("| Expected (registry) | Live column | Notes |");
    lines.push("|---------------------|-------------|-------|");
    for (const a of aliasMatches) {
      lines.push(
        `| \`${mdEscape(a.expected)}\` | \`${mdEscape(a.live)}\` | ${mdEscape(a.notes || a.alias)} |`
      );
    }
    lines.push("");
  }

  if (missing.length) {
    lines.push("**Expected missing from live (this table)**", "");
    lines.push("| Field | Classification | Notes |");
    lines.push("|-------|----------------|-------|");
    for (const m of missing) {
      lines.push(
        `| \`${mdEscape(m.expected)}\` | ${m.classification} | ${mdEscape(m.notes || "—")} |`
      );
    }
    lines.push("");
  }

  if (phase5bStatus.length) {
    lines.push("**Phase 5B fields (deal scope)**", "");
    lines.push("| Field | Priority | Live | Matched as |");
    lines.push("|-------|----------|------|------------|");
    for (const row of phase5bStatus) {
      const liveCol = row.live ? `\`${mdEscape(row.matchedAs)}\`` : "**No**";
      lines.push(
        `| \`${mdEscape(row.field)}\` | ${row.priority} | ${row.live ? "Yes" : "No"} | ${liveCol} |`
      );
    }
    lines.push("");
  }

  if (undocumented.length) {
    lines.push("<details><summary>Live fields not in repo registry (sample)</summary>", "");
    const sample = undocumented.slice(0, 40);
    for (const f of sample) {
      const meta = liveFields.find((x) => x.name === f);
      lines.push(`- \`${mdEscape(f)}\`${meta?.type ? ` (${meta.type})` : ""}`);
    }
    if (undocumented.length > sample.length) {
      lines.push(`- … and ${undocumented.length - sample.length} more`);
    }
    lines.push("", "</details>", "");
  }

  return {
    tableKey,
    tableName,
    found: true,
    optionalTable: Boolean(config.optionalTable),
    tableId: liveTable.id,
    liveFieldCount: liveFields.length,
    expectedCount: fieldSpecs.length,
    exactMatches,
    aliasMatches,
    missing,
    undocumented,
    phase5b: phase5bStatus,
    liveFields,
    lines,
  };
}

function classifyPriority(missingRows) {
  return missingRows.filter((row) => {
    const c = row.classification || "Confirmed Missing";
    if (c.includes("Proposed") || c.includes("Not Yet Implemented")) return false;
    return (
      c === "Confirmed Missing" ||
      c === "Needs Manual Verification" ||
      c === "Expected In Code But Not Live"
    );
  });
}

async function main() {
  const token =
    process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;

  if (!token) {
    console.error("Missing AIRTABLE_API_KEY, AIRTABLE_PAT, or AIRTABLE_TOKEN");
    process.exit(1);
  }
  if (!baseId) {
    console.error("Missing AIRTABLE_BASE_ID");
    process.exit(1);
  }

  console.log("[audit] Fetching Meta API schema (read-only)…");
  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) {
    console.error("Meta API failed:", res.status, json.error?.message || JSON.stringify(json));
    process.exit(1);
  }

  const liveTables = json.tables || [];
  const tableByName = new Map(liveTables.map((t) => [t.name, t]));

  const { tables: expected, phase5b } = buildExpectedDealsSchemaRegistry();
  const exportPayload = {
    generatedAt: new Date().toISOString(),
    baseId,
    metaApiStatus: res.status,
    tableCount: liveTables.length,
    tables: {},
    diffs: {},
  };

  const sections = [];
  const summary = {
    tablesFound: [],
    tablesNotFound: [],
    optionalTablesNotFound: [],
    totalExactMatches: 0,
    totalAliasMatches: 0,
    totalMissingExpected: 0,
    totalUndocumentedLive: 0,
    phase5bConfirmed: 0,
    phase5bMissing: 0,
  };

  const allExact = [];
  const allAlias = [];
  const allMissing = [];
  const allUndocumented = [];
  const allPhase5b = [];

  for (const key of DEAL_SCHEMA_AUDIT_TABLE_KEYS) {
    const config = expected[key];
    const liveTable = tableByName.get(config.tableName) || null;
    const p5b = phase5b.get(config.tableName) || [];
    const section = buildDiffSection(key, config, liveTable, p5b);

    exportPayload.tables[key] = liveTable
      ? {
          name: liveTable.name,
          id: liveTable.id,
          fields: (liveTable.fields || []).map((f) => ({
            id: f.id,
            name: f.name,
            type: f.type,
          })),
        }
      : null;

    exportPayload.diffs[key] = {
      tableName: config.tableName,
      found: section.found,
      optionalTable: section.optionalTable,
      exactMatches: section.exactMatches,
      aliasMatches: section.aliasMatches,
      missingExpected: section.missing,
      undocumentedLiveCount: section.undocumented?.length ?? 0,
      phase5b: section.phase5b,
      expectedSources: config.sources,
    };

    if (section.found) summary.tablesFound.push(config.tableName);
    else if (section.optionalTable) summary.optionalTablesNotFound.push(config.tableName);
    else summary.tablesNotFound.push(config.tableName);

    summary.totalExactMatches += section.exactMatches.length;
    summary.totalAliasMatches += section.aliasMatches.length;
    summary.totalMissingExpected += section.missing.length;
    summary.totalUndocumentedLive += section.undocumented?.length ?? 0;

    for (const row of section.phase5b || []) {
      if (row.live) summary.phase5bConfirmed += 1;
      else summary.phase5bMissing += 1;
      allPhase5b.push({ table: config.tableName, ...row });
    }

    for (const e of section.exactMatches) allExact.push({ table: config.tableName, ...e });
    for (const a of section.aliasMatches) allAlias.push({ table: config.tableName, ...a });
    for (const m of section.missing) allMissing.push({ table: config.tableName, ...m });
    for (const u of section.undocumented || []) {
      allUndocumented.push({ table: config.tableName, field: u });
    }

    sections.push(section);
  }

  exportPayload.summary = summary;
  exportPayload.expectedRegistry = Object.fromEntries(
    Object.entries(expected).map(([k, v]) => [
      k,
      {
        tableName: v.tableName,
        fieldSpecCount: (v.fieldSpecs || []).filter((s) => !s.deprecated).length,
        optionalTable: Boolean(v.optionalTable),
        sources: v.sources,
      },
    ])
  );

  mkdirSync(join(ROOT, "reports"), { recursive: true });
  writeFileSync(LIVE_JSON, JSON.stringify(exportPayload, null, 2));

  const highPriority = classifyPriority(allMissing);
  const docUpdates = [
    "Document alias mappings in airtable-deals-fields.md (Deal Status/Status, Property Name/Project Name, Location & Property, SI *Other Text columns).",
    "Note Lease Structure links via Deal_ID on child table — Deals row may lack Lease Structure link field.",
    "Expand Deal Activity Log and Deal Room Documents field lists from audit registry.",
    "Mark Phase 5B P1/P2 fields not live as proposed until columns are created.",
  ];
  if (summary.optionalTablesNotFound.length) {
    docUpdates.push(
      `Note optional table(s) absent in audited base: ${summary.optionalTablesNotFound.join(", ")}.`
    );
  }

  const airtableActions = [
    "**No automated changes** — suggestions only.",
    "Consider creating Phase 5B P1 fields (`Preferred Operator Management Structure`, `Operator Structure Intent`, `Brand Affiliation Path`, `F&B Complexity Level`) after product approval.",
    "If `Brand Internal Notes` on BDR is still needed, add column or confirm `Next Follow-up Notes (Internal)` covers the use case.",
    "If `Ownership Type Other Text` / `Zoning Status Other Text` are required for Location form Other paths, add columns or map to existing free-text fields.",
    "Proposal Submissions table: create only if proposal history snapshots are still planned for this base.",
  ];

  const md = [
    "# Airtable Deals Schema Diff Report",
    "",
    `Generated: ${exportPayload.generatedAt}`,
    "",
    `Base: \`${baseId}\``,
    "",
    "> Read-only Meta API export. No Airtable data or schema was modified.",
    "",
    "## Summary",
    "",
    "| Metric | Count |",
    "|--------|-------|",
    `| Tables found live | ${summary.tablesFound.length} / ${DEAL_SCHEMA_AUDIT_TABLE_KEYS.length} |`,
    `| Tables not found (required) | ${summary.tablesNotFound.length} |`,
    `| Tables not found (optional) | ${summary.optionalTablesNotFound.length} |`,
    `| Exact field matches (all tables) | ${summary.totalExactMatches} |`,
    `| Alias field matches (all tables) | ${summary.totalAliasMatches} |`,
    `| Expected fields missing from live | ${summary.totalMissingExpected} |`,
    `| Live fields not in repo registry | ${summary.totalUndocumentedLive} |`,
    `| Phase 5B deal fields confirmed live | ${summary.phase5bConfirmed} |`,
    `| Phase 5B deal fields missing live | ${summary.phase5bMissing} |`,
    "",
    "## Tables Found",
    "",
    ...(summary.tablesFound.length ? summary.tablesFound.map((t) => `- ${t}`) : ["- _(none)_"]),
    "",
    "## Tables Not Found",
    "",
    ...(summary.tablesNotFound.length
      ? summary.tablesNotFound.map((t) => `- **${t}** (required in registry)`)
      : ["- _(none — all required tables present)_"]),
    ...(summary.optionalTablesNotFound.length
      ? [
          "",
          "**Optional (not in audited base):**",
          ...summary.optionalTablesNotFound.map((t) => `- ${t}`),
        ]
      : []),
    "",
    "## Exact Field Matches",
    "",
    `Total: **${summary.totalExactMatches}** across scoped tables. See per-table sections for detail.`,
    "",
    "<details><summary>All exact matches (by table)</summary>",
    "",
    "| Table | Expected | Live |",
    "|-------|----------|------|",
    ...allExact.slice(0, 200).map((e) => `| ${mdEscape(e.table)} | \`${mdEscape(e.expected)}\` | \`${mdEscape(e.live)}\` |`),
    ...(allExact.length > 200 ? [`| … | … | _${allExact.length - 200} more_ |`] : []),
    "",
    "</details>",
    "",
    "## Alias Field Matches",
    "",
    ...(allAlias.length
      ? [
          "| Table | Expected (registry) | Live column | Notes |",
          "|-------|---------------------|-------------|-------|",
          ...allAlias.map(
            (a) =>
              `| ${mdEscape(a.table)} | \`${mdEscape(a.expected)}\` | \`${mdEscape(a.live)}\` | ${mdEscape(a.notes || a.alias || "—")} |`
          ),
        ]
      : ["- _(none)_"]),
    "",
    "## Expected Fields Missing From Live",
    "",
    ...(allMissing.length
      ? [
          "| Table | Field | Classification | Notes |",
          "|-------|-------|----------------|-------|",
          ...allMissing.map(
            (m) =>
              `| ${mdEscape(m.table)} | \`${mdEscape(m.expected)}\` | ${m.classification} | ${mdEscape(m.notes || "—")} |`
          ),
        ]
      : ["- _(none — all expected fields matched exact or alias)_"]),
    "",
    "## Live Fields Not In Registry",
    "",
    `Total undocumented live columns: **${summary.totalUndocumentedLive}** (rollups, formulas, legacy DELETE fields, and UI-only columns are expected).`,
    "",
    "<details><summary>Sample undocumented live fields</summary>",
    "",
    ...allUndocumented.slice(0, 60).map((u) => `- **${mdEscape(u.table)}** — \`${mdEscape(u.field)}\``),
    ...(allUndocumented.length > 60
      ? [`- … and ${allUndocumented.length - 60} more`]
      : []),
    "",
    "</details>",
    "",
    "## Phase 5B Status",
    "",
    "| Table | Field | Priority | Live | Matched as |",
    "|-------|-------|----------|------|------------|",
    ...allPhase5b.map((row) => {
      const liveCol = row.live ? `\`${mdEscape(row.matchedAs)}\`` : "**No**";
      return `| ${mdEscape(row.table)} | \`${mdEscape(row.field)}\` | ${row.priority} | ${row.live ? "Yes" : "No"} | ${liveCol} |`;
    }),
    "",
    "## High-Priority Gaps",
    "",
    ...(highPriority.length
      ? highPriority.map(
          (m) =>
            `- **${m.table}** — \`${m.expected}\` (${m.classification})${m.notes ? `: ${m.notes}` : ""}`
        )
      : ["- No confirmed-missing fields beyond proposed/optional columns."]),
    "",
    "## Recommended Documentation Updates",
    "",
    ...docUpdates.map((d) => `- ${d}`),
    "",
    "## Recommended Airtable Actions, If Any",
    "",
    ...airtableActions.map((a) => `- ${a}`),
    "",
    "## Per-table detail",
    "",
    ...sections.flatMap((s) => s.lines),
    "",
    "## How to use this report",
    "",
    "1. **Alias matches** — registry expects canonical name; live uses alias. Update code maps or docs, not Airtable, unless alias is wrong.",
    "2. **Missing with Proposed / Not Yet Implemented** — do not write from production code until columns exist.",
    "3. **Undocumented live** — often rollups/formulas; add to registry only when code reads/writes the column.",
    "4. Re-run after schema changes: `npm run audit-airtable-deals-schema`.",
    "",
    "## Artifacts",
    "",
    `- Live export: \`reports/airtable-deals-schema-live.json\``,
    `- Registry: \`lib/deal-schema-audit/expected-deals-schema-registry.js\``,
    `- Script: \`scripts/export-and-diff-airtable-deals-schema.mjs\``,
    "",
  ].join("\n");

  writeFileSync(DIFF_MD, md);

  console.log("Wrote", LIVE_JSON);
  console.log("Wrote", DIFF_MD);
  console.log(
    `Tables found: ${summary.tablesFound.length}/${DEAL_SCHEMA_AUDIT_TABLE_KEYS.length}; ` +
      `exact: ${summary.totalExactMatches}; alias: ${summary.totalAliasMatches}; ` +
      `missing: ${summary.totalMissingExpected}; ` +
      `Phase 5B confirmed: ${summary.phase5bConfirmed}/${summary.phase5bConfirmed + summary.phase5bMissing}`
  );

  if (summary.tablesNotFound.length) {
    console.warn("Required tables not found:", summary.tablesNotFound.join(", "));
  }
  if (summary.optionalTablesNotFound.length) {
    console.log("Optional tables not found:", summary.optionalTablesNotFound.join(", "));
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
