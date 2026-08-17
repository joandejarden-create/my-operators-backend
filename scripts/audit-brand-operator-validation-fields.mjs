#!/usr/bin/env node
/**
 * Read-only: export live Airtable schema for Brand/Operator validation governance audit.
 *
 * Usage:
 *   node scripts/audit-brand-operator-validation-fields.mjs
 *   npm run audit-brand-operator-validation-fields
 *
 * Requires:
 *   AIRTABLE_API_KEY (or AIRTABLE_PAT) with schema.bases:read
 *   AIRTABLE_BASE_ID — Brand/Operator/Partner Intelligence / Company Profile / Deal Brand Cache
 *   AIRTABLE_BASE_ID_ALT — optional; Brand Alias Mapping + Hotel Census (skipped if unset)
 *
 * Outputs:
 *   reports/brand-operator-validation-schema-live.json
 *   reports/brand-operator-validation-schema-diff.md
 *
 * Does NOT read or write records. Does NOT modify Airtable schema.
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  buildExpectedBrandOperatorValidationRegistry,
  BRAND_OPERATOR_AUDIT_TABLE_KEYS,
  CORE_GOVERNANCE_FIELD_SPECS,
  ALIAS_EQUIVALENT_FIELD_SPECS,
  matchSpecToLive,
  resolveLiveTable,
} from "../lib/brand-operator-validation-audit/expected-brand-operator-validation-registry.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const LIVE_JSON = join(ROOT, "reports", "brand-operator-validation-schema-live.json");
const DIFF_MD = join(ROOT, "reports", "brand-operator-validation-schema-diff.md");

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

function specsForTable(config, registry) {
  const specs = [];
  if (config.checkP1ProfileGovernance) specs.push(...registry.p1ProfileGovernance);
  if (config.checkCoreGovernance) specs.push(...registry.coreGovernance);
  if (config.checkAliasEquivalents) specs.push(...registry.aliasEquivalents);
  if (config.fieldSpecs?.length) specs.push(...config.fieldSpecs);
  const seen = new Set();
  return specs.filter((s) => {
    const k = `${s.tier || "x"}:${s.name}`;
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

function buildTableSection(tableKey, config, liveTable, matchedName, registry) {
  const fieldSpecs = specsForTable(config, registry);
  const tableName = config.tableName;

  if (!liveTable) {
    const optional = config.optionalTable ? " (optional)" : "";
    const legacy = config.legacy ? " (legacy)" : "";
    return {
      tableKey,
      tableName,
      matchedName: null,
      found: false,
      optionalTable: Boolean(config.optionalTable),
      legacy: Boolean(config.legacy),
      category: config.category,
      base: config.base,
      setupPriority: config.setupPriority,
      liveFieldCount: 0,
      coreMissing: [],
      coreExact: [],
      coreAlias: [],
      tableSpecificExact: [],
      tableSpecificAlias: [],
      tableSpecificMissing: [],
      aliasEquivalentsFound: [],
      lines: [
        `### ${tableName}${optional}${legacy}`,
        "",
        config.notes ? `> ${config.notes}` : "",
        config.notes ? "" : null,
        "**Status:** Table not found in audited base",
        "",
        config.tableAliases?.length
          ? `**Name aliases checked:** ${config.tableAliases.map((a) => `\`${a}\``).join(", ")}`
          : null,
        `**Base:** \`${config.base === "alt" ? "AIRTABLE_BASE_ID_ALT" : "AIRTABLE_BASE_ID"}\``,
        `**Sources:** ${config.sources.join(", ")}`,
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

  const coreExact = [];
  const coreAlias = [];
  const coreMissing = [];
  const tableSpecificExact = [];
  const tableSpecificAlias = [];
  const tableSpecificMissing = [];
  const aliasEquivalentsFound = [];

  const coreSet = new Set([
    ...registry.coreGovernance.map((s) => s.name),
    ...registry.p1ProfileGovernance.map((s) => s.name),
  ]);
  const aliasSet = new Set(registry.aliasEquivalents.map((s) => s.name));
  const tableSpecificSet = new Set((config.fieldSpecs || []).map((s) => s.name));

  for (const spec of fieldSpecs) {
    const m = matchSpecToLive(spec, liveNames);
    const row = { expected: spec.name, live: m.live, classification: spec.classification, notes: spec.notes };
    if (m.kind === "exact") {
      if (coreSet.has(spec.name)) coreExact.push(row);
      else if (aliasSet.has(spec.name)) aliasEquivalentsFound.push({ ...row, kind: "exact" });
      else if (tableSpecificSet.has(spec.name)) tableSpecificExact.push(row);
      else tableSpecificExact.push(row);
    } else if (m.kind === "alias") {
      const aliasRow = { ...row, alias: m.matchedAlias };
      if (coreSet.has(spec.name)) coreAlias.push(aliasRow);
      else if (aliasSet.has(spec.name)) aliasEquivalentsFound.push({ ...aliasRow, kind: "alias" });
      else if (tableSpecificSet.has(spec.name)) tableSpecificAlias.push(aliasRow);
      else tableSpecificAlias.push(aliasRow);
    } else if (coreSet.has(spec.name)) {
      coreMissing.push({
        expected: spec.name,
        classification: spec.classification || "Recommended Governance",
        notes: spec.notes || "",
        aliases: spec.aliases || [],
      });
    } else if (tableSpecificSet.has(spec.name)) {
      tableSpecificMissing.push({
        expected: spec.name,
        classification: spec.classification || "Table-specific",
        notes: spec.notes || "",
      });
    }
  }

  const covered = new Set();
  for (const spec of fieldSpecs) {
    const m = matchSpecToLive(spec, liveNames);
    if (m.live) covered.add(m.live);
  }
  const undocumented = liveNames.filter((n) => !covered.has(n));

  const lines = [];
  lines.push(`### ${tableName}`);
  if (matchedName && matchedName !== tableName) {
    lines.push(`> Matched live table as \`${matchedName}\``);
    lines.push("");
  }
  lines.push("| Property | Value |");
  lines.push("|----------|-------|");
  lines.push(`| Live table ID | \`${liveTable.id}\` |`);
  lines.push(`| Category | ${config.category} |`);
  lines.push(`| Setup priority | ${config.setupPriority} |`);
  lines.push(`| Live fields | ${liveFields.length} |`);
  lines.push(`| Core governance exact | ${coreExact.length} / ${registry.coreGovernance.length} |`);
  lines.push(`| Core governance alias | ${coreAlias.length} |`);
  lines.push(`| Core governance missing | ${coreMissing.length} |`);
  lines.push(`| Sources | ${config.sources.join(", ")} |`);
  lines.push("");

  if (coreAlias.length || tableSpecificAlias.length) {
    lines.push("**Alias matches**", "");
    lines.push("| Expected | Live column | Notes |");
    lines.push("|----------|-------------|-------|");
    for (const a of [...coreAlias, ...tableSpecificAlias]) {
      lines.push(
        `| \`${mdEscape(a.expected)}\` | \`${mdEscape(a.live)}\` | ${mdEscape(a.notes || a.alias || "—")} |`
      );
    }
    lines.push("");
  }

  if (coreMissing.length) {
    lines.push("**Core governance missing**", "");
    lines.push("| Field | Classification | Notes |");
    lines.push("|-------|----------------|-------|");
    for (const m of coreMissing) {
      lines.push(`| \`${mdEscape(m.expected)}\` | ${m.classification} | ${mdEscape(m.notes || "—")} |`);
    }
    lines.push("");
  }

  if (aliasEquivalentsFound.length) {
    lines.push("**Equivalent / workflow fields present**", "");
    for (const a of aliasEquivalentsFound) {
      lines.push(`- \`${mdEscape(a.expected)}\` → live \`${mdEscape(a.live)}\` (${a.kind})`);
    }
    lines.push("");
  }

  if (undocumented.length) {
    lines.push("<details><summary>Live fields not checked by governance registry (sample)</summary>", "");
    for (const f of undocumented.slice(0, 30)) {
      const meta = liveFields.find((x) => x.name === f);
      lines.push(`- \`${mdEscape(f)}\`${meta?.type ? ` (${meta.type})` : ""}`);
    }
    if (undocumented.length > 30) lines.push(`- … and ${undocumented.length - 30} more`);
    lines.push("", "</details>", "");
  }

  return {
    tableKey,
    tableName,
    matchedName,
    found: true,
    optionalTable: Boolean(config.optionalTable),
    legacy: Boolean(config.legacy),
    category: config.category,
    base: config.base,
    setupPriority: config.setupPriority,
    tableId: liveTable.id,
    liveFieldCount: liveFields.length,
    coreExact,
    coreAlias,
    coreMissing,
    tableSpecificExact,
    tableSpecificAlias,
    tableSpecificMissing,
    aliasEquivalentsFound,
    undocumented,
    liveFields,
    lines,
  };
}

function scanGovernanceAcrossTables(allSections) {
  const found = [];
  for (const s of allSections) {
    if (!s.found) continue;
    for (const row of [...s.coreExact, ...s.coreAlias, ...s.tableSpecificExact, ...s.tableSpecificAlias, ...s.aliasEquivalentsFound]) {
      found.push({
        table: s.tableName,
        category: s.category,
        expected: row.expected,
        live: row.live,
        kind: row.alias ? "alias" : row.kind || "exact",
      });
    }
  }
  return found;
}

function highPriorityGaps(sections) {
  const gaps = [];
  for (const s of sections) {
    if (!s.found || s.legacy) continue;
    if (s.setupPriority > 2) continue;
    for (const m of s.coreMissing || []) {
      gaps.push({
        table: s.tableName,
        field: m.expected,
        priority: s.setupPriority,
        category: s.category,
        notes: m.notes,
      });
    }
  }
  gaps.sort((a, b) => a.priority - b.priority || a.table.localeCompare(b.table));
  return gaps;
}

function setupOrder(sections) {
  return [...sections]
    .filter((s) => s.found && !s.legacy && s.category !== "legacy")
    .sort((a, b) => a.setupPriority - b.setupPriority || a.tableName.localeCompare(b.tableName))
    .map((s) => ({
      table: s.tableName,
      priority: s.setupPriority,
      category: s.category,
      coreMissing: (s.coreMissing || []).length,
      corePresent: (s.coreExact || []).length + (s.coreAlias || []).length,
    }));
}

async function fetchBaseSchema(baseId, token, label) {
  if (!baseId) return { label, baseId: null, skipped: true, tables: [], error: null };
  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) {
    return {
      label,
      baseId,
      skipped: false,
      tables: [],
      error: json.error?.message || `Meta API ${res.status}`,
    };
  }
  return { label, baseId, skipped: false, tables: json.tables || [], error: null };
}

async function main() {
  const token =
    process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || process.env.AIRTABLE_TOKEN;
  const primaryBaseId = process.env.AIRTABLE_BASE_ID;
  const altBaseId = process.env.AIRTABLE_BASE_ID_ALT || null;

  if (!token) {
    console.error("Missing AIRTABLE_API_KEY, AIRTABLE_PAT, or AIRTABLE_TOKEN");
    process.exit(1);
  }
  if (!primaryBaseId) {
    console.error("Missing AIRTABLE_BASE_ID");
    process.exit(1);
  }

  console.log("[audit] Fetching primary base schema (read-only)…");
  const primary = await fetchBaseSchema(primaryBaseId, token, "primary");
  if (primary.error) {
    console.error("Primary Meta API failed:", primary.error);
    process.exit(1);
  }

  let alt = { label: "alt", baseId: altBaseId, skipped: true, tables: [], error: null };
  if (altBaseId) {
    console.log("[audit] Fetching ALT base schema (read-only)…");
    alt = await fetchBaseSchema(altBaseId, token, "alt");
    if (alt.error) {
      console.warn("[audit] ALT base Meta API failed (continuing):", alt.error);
      alt.tables = [];
    }
  } else {
    console.log("[audit] AIRTABLE_BASE_ID_ALT not set — skipping Brand Alias Mapping + Hotel Census");
  }

  const primaryByName = new Map(primary.tables.map((t) => [t.name, t]));
  const altByName = new Map(alt.tables.map((t) => [t.name, t]));

  const registry = buildExpectedBrandOperatorValidationRegistry();
  const sections = [];
  const exportPayload = {
    generatedAt: new Date().toISOString(),
    bases: {
      primary: { id: primaryBaseId, tableCount: primary.tables.length },
      alt: altBaseId
        ? { id: altBaseId, tableCount: alt.tables.length, skipped: alt.skipped, error: alt.error }
        : { skipped: true, reason: "AIRTABLE_BASE_ID_ALT not set" },
    },
    tables: {},
    diffs: {},
    summary: {},
  };

  const summary = {
    tablesFound: [],
    tablesNotFound: [],
    optionalTablesNotFound: [],
    legacyTablesFound: [],
    legacyTablesNotFound: [],
    totalCoreExact: 0,
    totalCoreAlias: 0,
    totalCoreMissing: 0,
  };

  for (const key of BRAND_OPERATOR_AUDIT_TABLE_KEYS) {
    const config = registry.tables[key];
    if (!config) continue;

    const tableMap = config.base === "alt" ? altByName : primaryByName;
    const { table: liveTable, matchedName } = resolveLiveTable(tableMap, config);
    const section = buildTableSection(key, config, liveTable, matchedName, registry);
    sections.push(section);

    exportPayload.tables[key] = liveTable
      ? {
          name: liveTable.name,
          matchedName,
          id: liveTable.id,
          base: config.base,
          fields: (liveTable.fields || []).map((f) => ({ id: f.id, name: f.name, type: f.type })),
        }
      : null;

    exportPayload.diffs[key] = {
      tableName: config.tableName,
      matchedName,
      found: section.found,
      optionalTable: section.optionalTable,
      legacy: section.legacy,
      category: section.category,
      base: config.base,
      setupPriority: config.setupPriority,
      coreExact: section.coreExact,
      coreAlias: section.coreAlias,
      coreMissing: section.coreMissing,
      tableSpecificExact: section.tableSpecificExact,
      tableSpecificAlias: section.tableSpecificAlias,
      tableSpecificMissing: section.tableSpecificMissing,
      aliasEquivalentsFound: section.aliasEquivalentsFound,
      undocumentedLiveCount: section.undocumented?.length ?? 0,
      expectedSources: config.sources,
    };

    if (section.found) {
      summary.tablesFound.push(section.matchedName || section.tableName);
      if (section.legacy) summary.legacyTablesFound.push(section.tableName);
    } else if (section.legacy) {
      summary.legacyTablesNotFound.push(section.tableName);
    } else if (section.optionalTable) {
      summary.optionalTablesNotFound.push(section.tableName);
    } else {
      summary.tablesNotFound.push(section.tableName);
    }

    summary.totalCoreExact += section.coreExact?.length ?? 0;
    summary.totalCoreAlias += section.coreAlias?.length ?? 0;
    summary.totalCoreMissing += section.coreMissing?.length ?? 0;
  }

  const governanceFound = scanGovernanceAcrossTables(sections);
  const gaps = highPriorityGaps(sections);
  const fieldSetupOrder = setupOrder(sections);

  exportPayload.summary = { ...summary, highPriorityGapCount: gaps.length, governanceFieldHits: governanceFound.length };
  exportPayload.registryMeta = {
    coreGovernanceFieldCount: CORE_GOVERNANCE_FIELD_SPECS.length,
    aliasEquivalentCount: ALIAS_EQUIVALENT_FIELD_SPECS.length,
    tableKeysAudited: BRAND_OPERATOR_AUDIT_TABLE_KEYS.length,
  };

  mkdirSync(join(ROOT, "reports"), { recursive: true });
  writeFileSync(LIVE_JSON, JSON.stringify(exportPayload, null, 2));

  const brandSections = sections.filter((s) => s.category === "brand");
  const operatorSections = sections.filter((s) => s.category === "operator");
  const piSections = sections.filter((s) => s.category === "partner-intelligence");
  const sharedSections = sections.filter((s) => s.category === "shared");
  const legacySections = sections.filter((s) => s.category === "legacy");

  const allCoreMissing = sections.flatMap((s) =>
    (s.coreMissing || []).map((m) => ({ table: s.tableName, ...m }))
  );

  const allAliasFound = sections.flatMap((s) =>
    (s.aliasEquivalentsFound || []).map((a) => ({
      table: s.tableName,
      expected: a.expected,
      live: a.live,
      kind: a.kind,
    }))
  );

  const airtableActions = [
    "**No automated changes from this script** — suggestions for human review only.",
    "After founder approval, run future `setup-brand-validation-fields.mjs` / `setup-operator-validation-fields.mjs` with `--dry-run` first.",
    "Enable Partner Intelligence tables via `scripts/ensure-partner-intelligence-tables.mjs --dry-run` before expecting PI governance columns live.",
    "Do not bulk-add governance columns to every Brand Setup child table — start with Brand Basics + Brand Explorer Presentation + Operator Master.",
  ];

  const docUpdates = [
    "Update brand-operator-validation-fields-plan.md Current State Summary with live audit counts.",
    "Mark Partner Intelligence tables live vs proposed based on this report.",
    "Document Operator Master partial governance fields confirmed live.",
    "Add Company Profile column inventory if undocumented live fields are material.",
    "Note legacy 3rd Party Operator table presence/absence for retirement planning.",
  ];

  const md = [
    "# Brand / Operator Validation Schema Audit",
    "",
    `Generated: ${exportPayload.generatedAt}`,
    "",
    `Primary base: \`${primaryBaseId}\``,
    altBaseId ? `ALT base: \`${altBaseId}\`` : "ALT base: _(not configured — Brand Alias + Hotel Census skipped)_",
    "",
    "> Read-only Meta API export. No Airtable records read. No schema or data modified.",
    "",
    "## Summary",
    "",
    "| Metric | Count |",
    "|--------|-------|",
    `| Tables found live | ${summary.tablesFound.length} |`,
    `| Required tables not found | ${summary.tablesNotFound.length} |`,
    `| Optional tables not found | ${summary.optionalTablesNotFound.length} |`,
    `| Legacy operator tables found | ${summary.legacyTablesFound.length} |`,
    `| Core governance exact matches (all tables) | ${summary.totalCoreExact} |`,
    `| Core governance alias matches (all tables) | ${summary.totalCoreAlias} |`,
    `| Core governance missing checks (all tables) | ${summary.totalCoreMissing} |`,
    `| Governance / equivalent field hits | ${governanceFound.length} |`,
    `| High-priority setup gaps (P1–P2 tables) | ${gaps.length} |`,
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
    "",
    "## Optional Tables Not Found",
    "",
    ...(summary.optionalTablesNotFound.length
      ? summary.optionalTablesNotFound.map((t) => `- ${t}`)
      : ["- _(none)_"]),
    "",
    "## Existing Governance Fields Found",
    "",
    ...(governanceFound.length
      ? [
          "| Table | Category | Expected | Live column | Match |",
          "|-------|----------|----------|-------------|-------|",
          ...governanceFound.slice(0, 120).map(
            (g) =>
              `| ${mdEscape(g.table)} | ${g.category} | \`${mdEscape(g.expected)}\` | \`${mdEscape(g.live)}\` | ${g.kind} |`
          ),
          ...(governanceFound.length > 120
            ? [`| … | … | … | … | _${governanceFound.length - 120} more in JSON_ |`]
            : []),
        ]
      : ["- _(no governance or equivalent fields matched)_"]),
    "",
    "## Recommended Governance Fields Missing",
    "",
    ...(allCoreMissing.length
      ? [
          "| Table | Field | Classification | Notes |",
          "|-------|-------|----------------|-------|",
          ...allCoreMissing.slice(0, 150).map(
            (m) =>
              `| ${mdEscape(m.table)} | \`${mdEscape(m.expected)}\` | ${m.classification} | ${mdEscape(m.notes || "—")} |`
          ),
          ...(allCoreMissing.length > 150
            ? [`| … | … | … | _${allCoreMissing.length - 150} more_ |`]
            : []),
        ]
      : ["- _(none — all core governance fields matched on audited tables)_"]),
    "",
    "## Alias / Equivalent Fields Found",
    "",
    ...(allAliasFound.length
      ? allAliasFound.map((a) => `- **${mdEscape(a.table)}** — \`${mdEscape(a.expected)}\` → \`${mdEscape(a.live)}\` (${a.kind})`)
      : ["- _(none)_"]),
    "",
    "## Brand Tables — Detail",
    "",
    ...brandSections.flatMap((s) => s.lines),
    "",
    "## Operator Tables — Detail",
    "",
    ...operatorSections.flatMap((s) => s.lines),
    "",
    "## Partner Intelligence Tables — Detail",
    "",
    ...piSections.flatMap((s) => s.lines),
    "",
    "## Company Profile / Shared Tables — Detail",
    "",
    ...sharedSections.flatMap((s) => s.lines),
    "",
    "## Legacy Tables Found",
    "",
    ...(summary.legacyTablesFound.length
      ? summary.legacyTablesFound.map((t) => `- **${t}** — still live; plan retirement or exclude from governance rollout`)
      : ["- _(no legacy 3rd Party Operator tables found in primary base)_"]),
    "",
    ...(summary.legacyTablesNotFound.length
      ? [
          "",
          "**Legacy tables not found (expected if new-base only):**",
          ...summary.legacyTablesNotFound.map((t) => `- ${t}`),
        ]
      : []),
    "",
    "## High-Priority Gaps",
    "",
    ...(gaps.length
      ? gaps.map(
          (g) =>
            `- **${g.table}** (P${g.priority}) — missing \`${g.field}\`${g.notes ? `: ${g.notes}` : ""}`
        )
      : ["- No P1–P2 table gaps beyond optional/legacy scope."]),
    "",
    "## Recommended Field Setup Order",
    "",
    "| Priority | Table | Category | Core governance present | Core missing |",
    "|----------|-------|----------|-------------------------|--------------|",
    ...fieldSetupOrder.slice(0, 25).map(
      (r) =>
        `| ${r.priority} | ${mdEscape(r.table)} | ${r.category} | ${r.corePresent} | ${r.coreMissing} |`
    ),
    "",
    "## Recommended Airtable Actions, If Any",
    "",
    ...airtableActions.map((a) => `- ${a}`),
    "",
    "## Recommended Documentation Updates",
    "",
    ...docUpdates.map((d) => `- ${d}`),
    "",
    "## Open Questions",
    "",
    "- Should governance columns live on **profile root only** (Brand Basics / Operator Master / Presentation rows) or also on every Setup child table?",
    "- Are Partner Intelligence tables intended to be created on primary base before Brand/Operator Setup columns?",
    "- Should `Company Profile` carry minimal `Company Validated` fields or inherit from linked brand/operator profiles?",
    "- Confirm legacy `3rd Party Operator - *` retirement timeline if any tables remain live.",
    "- Reconcile `Notes` alias matches — generic Notes columns are not full Internal Notes governance.",
    "",
    "## How to use this report",
    "",
    "1. Review **High-Priority Gaps** for P1 tables (Brand Basics, Brand Explorer Presentation, Operator Master).",
    "2. Treat **Alias / Equivalent** hits as partial governance — not a substitute for Validation Status + Usage Permission.",
    "3. **Partner Intelligence** absence means extraction/publish workflow is not schema-ready.",
    "4. Re-run after schema changes: `npm run audit-brand-operator-validation-fields`.",
    "",
    "## Artifacts",
    "",
    "- Live export: `reports/brand-operator-validation-schema-live.json`",
    "- Registry: `lib/brand-operator-validation-audit/expected-brand-operator-validation-registry.js`",
    "- Script: `scripts/audit-brand-operator-validation-fields.mjs`",
    "- Plan: `docs/data-intelligence/brand-operator-validation-fields-plan.md`",
    "",
  ].join("\n");

  writeFileSync(DIFF_MD, md);

  console.log("Wrote", LIVE_JSON);
  console.log("Wrote", DIFF_MD);
  console.log(
    `Tables found: ${summary.tablesFound.length}; ` +
      `core exact: ${summary.totalCoreExact}; core alias: ${summary.totalCoreAlias}; ` +
      `core missing checks: ${summary.totalCoreMissing}; high-priority gaps: ${gaps.length}`
  );

  if (summary.tablesNotFound.length) {
    console.warn("Required tables not found:", summary.tablesNotFound.join(", "));
  }
  if (summary.optionalTablesNotFound.length) {
    console.log("Optional tables not found:", summary.optionalTablesNotFound.join(", "));
  }
  if (summary.legacyTablesFound.length) {
    console.log("Legacy tables still live:", summary.legacyTablesFound.join(", "));
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
