#!/usr/bin/env node
/**
 * Read-only audit: confirm each Operator Master has linked rows in Setup tables.
 * Matches links in JS (Operator array includes masterId) — not FIND/ARRAYJOIN
 * (ARRAYJOIN on linked fields returns primary display names, not rec IDs).
 *
 *   node scripts/audit-operator-setup-linked-rows.mjs
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { OPERATOR_FACTORY_QUEUE } from "../lib/partner-intelligence/operator-explorer-factory-queue.js";
import { OPERATOR_QUALITY_BASELINE_OPERATORS } from "../lib/partner-intelligence/operator-explorer-quality-baseline.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const ONE_TO_ONE = [
  "Operator Setup - Profile & Positioning",
  "Operator Setup - Platform & Markets",
  "Operator Setup - Commercial Fit & Terms",
  "Operator Setup - Governance, Delivery & Diligence",
];

const MULTI = [
  "Operator Setup - Operating Platform",
  "Operator Setup - Brand Relationships",
  "Operator Setup - Engagement & Reporting",
  "Operator Setup - Infrastructure Platform",
  "Operator Setup - Leadership Platform",
  "Operator Setup - Leadership Team Members",
  "Operator Setup - Case Studies",
  "Operator Setup - Diligence QA",
];

function shortName(tableName) {
  return tableName.replace(/^Operator Setup - /, "");
}

function getBase() {
  const key = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Missing AIRTABLE_API_KEY/PAT or AIRTABLE_BASE_ID");
  return new Airtable({ apiKey: key }).base(baseId);
}

/** @returns {Promise<Map<string, string[]>>} masterId -> linked record ids */
async function indexLinksByMaster(base, tableName) {
  /** @type {Map<string, string[]>} */
  const map = new Map();
  try {
    const rows = await base(tableName).select({ fields: ["Operator"], pageSize: 100 }).all();
    for (const r of rows) {
      const ops = r.fields?.Operator;
      if (!Array.isArray(ops)) continue;
      for (const masterId of ops) {
        if (!masterId) continue;
        if (!map.has(masterId)) map.set(masterId, []);
        map.get(masterId).push(r.id);
      }
    }
    return { map, error: null, rowCount: rows.length };
  } catch (err) {
    return { map, error: String(err?.message || err).slice(0, 200), rowCount: 0 };
  }
}

function collectOperators() {
  const list = [
    ...OPERATOR_QUALITY_BASELINE_OPERATORS,
    ...OPERATOR_FACTORY_QUEUE.filter((o) => o.recordId),
  ];
  const seen = new Set();
  const out = [];
  for (const o of list) {
    if (!o?.recordId || seen.has(o.recordId)) continue;
    seen.add(o.recordId);
    out.push({
      slug: o.slug,
      recordId: o.recordId,
      companyName: o.companyName || o.slug,
    });
  }
  return out;
}

async function main() {
  const base = getBase();
  const operators = collectOperators();
  const allTables = [...ONE_TO_ONE, ...MULTI];

  /** @type {Record<string, { map: Map<string, string[]>, error: string|null, rowCount: number }>} */
  const indexes = {};
  for (const t of allTables) {
    process.stderr.write(`Indexing ${t}...\n`);
    indexes[t] = await indexLinksByMaster(base, t);
  }

  const report = [];
  for (const op of operators) {
    const oneToOne = {};
    const multiRow = {};
    const missingOneToOne = [];

    for (const t of ONE_TO_ONE) {
      const short = shortName(t);
      const idx = indexes[t];
      if (idx.error) {
        oneToOne[short] = { error: idx.error, count: 0 };
        missingOneToOne.push(`${short} (error)`);
        continue;
      }
      const ids = idx.map.get(op.recordId) || [];
      oneToOne[short] = { count: ids.length, id: ids[0] || null, ids };
      if (ids.length < 1) missingOneToOne.push(short);
    }

    for (const t of MULTI) {
      const short = shortName(t);
      const idx = indexes[t];
      if (idx.error) {
        multiRow[short] = { error: idx.error, count: 0 };
        continue;
      }
      const ids = idx.map.get(op.recordId) || [];
      multiRow[short] = { count: ids.length, ids };
    }

    report.push({
      slug: op.slug,
      companyName: op.companyName,
      recordId: op.recordId,
      oneToOneComplete: missingOneToOne.length === 0,
      missingOneToOne,
      oneToOne,
      multiRow,
    });
  }

  const incomplete = report.filter((r) => !r.oneToOneComplete);
  const out = {
    generatedAt: new Date().toISOString(),
    checked: report.length,
    tableIndexMeta: Object.fromEntries(
      allTables.map((t) => [
        shortName(t),
        { rowCount: indexes[t].rowCount, error: indexes[t].error },
      ])
    ),
    oneToOneTables: ONE_TO_ONE.map(shortName),
    multiRowTables: MULTI.map(shortName),
    allOneToOneComplete: incomplete.length === 0,
    incompleteCount: incomplete.length,
    incomplete: incomplete.map((r) => ({
      slug: r.slug,
      company: r.companyName,
      master: r.recordId,
      missing: r.missingOneToOne,
    })),
    operators: report,
  };

  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, "operator-setup-linked-rows-audit.json");
  const mdPath = path.join(reportsDir, "operator-setup-linked-rows-audit.md");
  fs.writeFileSync(jsonPath, JSON.stringify(out, null, 2));

  const lines = [
    "# Operator Setup linked rows audit",
    "",
    `Generated: ${out.generatedAt}`,
    `Operators checked: **${out.checked}**`,
    `All 1:1 linked tabs present: **${out.allOneToOneComplete ? "YES" : "NO"}** (${out.incompleteCount} incomplete)`,
    "",
    "## 1:1 required tabs",
    "",
    "| Operator | Master | Profile | Platform | Commercial | Governance | OK? |",
    "|---|---|---:|---:|---:|---:|---|",
  ];
  for (const r of report) {
    const c = (name) => r.oneToOne[name]?.count ?? "err";
    lines.push(
      `| ${r.companyName} | \`${r.recordId}\` | ${c("Profile & Positioning")} | ${c("Platform & Markets")} | ${c("Commercial Fit & Terms")} | ${c("Governance, Delivery & Diligence")} | ${r.oneToOneComplete ? "yes" : "NO"} |`
    );
  }
  lines.push("", "## Multi-row child tables (counts; optional for scaffold)", "");
  lines.push(
    "| Operator | Operating | Brands | Engagement | Infra | Lead Platform | Lead Members | Case Studies | Diligence |"
  );
  lines.push("|---|---:|---:|---:|---:|---:|---:|---:|---:|");
  for (const r of report) {
    const m = (name) => r.multiRow[name]?.count ?? "err";
    lines.push(
      `| ${r.companyName} | ${m("Operating Platform")} | ${m("Brand Relationships")} | ${m("Engagement & Reporting")} | ${m("Infrastructure Platform")} | ${m("Leadership Platform")} | ${m("Leadership Team Members")} | ${m("Case Studies")} | ${m("Diligence QA")} |`
    );
  }
  if (incomplete.length) {
    lines.push("", "## Incomplete 1:1", "");
    for (const r of incomplete) {
      lines.push(
        `- **${r.companyName}** (\`${r.recordId}\`): missing ${r.missingOneToOne.join(", ")}`
      );
    }
  }
  fs.writeFileSync(mdPath, lines.join("\n"));

  console.log(`Wrote ${jsonPath}`);
  console.log(`Wrote ${mdPath}`);
  console.log(
    JSON.stringify(
      {
        checked: out.checked,
        allOneToOneComplete: out.allOneToOneComplete,
        incompleteCount: out.incompleteCount,
        incomplete: out.incomplete,
        tableIndexMeta: out.tableIndexMeta,
        summary: report.map((r) => ({
          company: r.companyName,
          master: r.recordId,
          oneToOneOk: r.oneToOneComplete,
          oneToOne: Object.fromEntries(
            Object.entries(r.oneToOne).map(([k, v]) => [k, v.count ?? v.error])
          ),
          multiRow: Object.fromEntries(
            Object.entries(r.multiRow).map(([k, v]) => [k, v.count ?? v.error])
          ),
        })),
      },
      null,
      2
    )
  );
  if (!out.allOneToOneComplete) process.exitCode = 2;
}

main().catch((err) => {
  console.error(err?.stack || err?.message || err);
  process.exit(1);
});
