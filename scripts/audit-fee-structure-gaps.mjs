/**
 * Read-only snapshot of Brand Setup - Fee Structure completeness.
 *   node scripts/audit-fee-structure-gaps.mjs
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { isBrandStatusActive } from "../lib/brand-status-active.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const TABLE_BASICS = "Brand Setup - Brand Basics";
const TABLE_FEE = "Brand Setup - Fee Structure";
const LINK = "Brand Setup - Fee Structure";

/** Core fee columns used for completeness (from FEE_FORM_TO_AIRTABLE). */
const CORE_COLS = [
  "Min - Typical Application Fee",
  "Max - Typical Application Fee",
  "Basis - Typical Application Fee",
  "Min - Typical Royalty Fee Range",
  "Max - Typical Royalty Fee Range",
  "Basis - Typical Royalty Fee Range",
  "Min - Typical Marketing Fee Range",
  "Max - Typical Marketing Fee Range",
  "Basis - Typical Marketing Fee Range",
  "Min - Typical Tech",
  "Max - Typical Tech",
  "Basis - Typical Tech",
  "Min - Typical Loyalty Program Fee",
  "Max - Typical Loyalty Program Fee",
  "Basis - Typical Loyalty Program Fee",
  "Min - Typical Reservation / Distribution Fee",
  "Max - Typical Reservation / Distribution Fee",
  "Basis - Typical Reservation / Distribution Fee",
  "Min - Typical Training Fee",
  "Max - Typical Training Fee",
  "Basis - Typical Training Fee",
  "Typical Incentives Offered",
  "Typical Termination Fee Structure (if any)",
  "Who Can Exercise Termination Right After Failed Test?",
];

function empty(v) {
  return v === undefined || v === null || v === "" || (Array.isArray(v) && !v.length);
}

async function main() {
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID
  );
  const metaRes = await fetch(
    `https://api.airtable.com/v0/meta/bases/${process.env.AIRTABLE_BASE_ID}/tables`,
    { headers: { Authorization: `Bearer ${process.env.AIRTABLE_API_KEY}` } }
  );
  const meta = await metaRes.json();
  const feeTable = meta.tables.find((t) => t.name === TABLE_FEE);
  const metaNames = new Set((feeTable?.fields || []).map((f) => f.name));
  const missingInMeta = CORE_COLS.filter((c) => !metaNames.has(c));

  const basics = [];
  await base(TABLE_BASICS)
    .select({ fields: ["Brand Name", "Brand Status", "Parent Company", LINK] })
    .eachPage((p, n) => {
      basics.push(...p);
      n();
    });

  const byStatus = { Active: [], Live: [], other: [] };
  let linked = 0;
  let sumPct = 0;
  let n = 0;
  const activeRows = [];

  for (const r of basics) {
    const name = r.fields["Brand Name"];
    const status = r.fields["Brand Status"] || "(blank)";
    const dealId = r.fields[LINK]?.[0];
    const active = isBrandStatusActive(status);
    if (!dealId) {
      if (active) activeRows.push({ name, status, linked: false, pct: 0, filled: 0, empty: CORE_COLS.length });
      continue;
    }
    linked++;
    const fee = await base(TABLE_FEE).find(dealId);
    let filled = 0;
    const emptyCols = [];
    for (const c of CORE_COLS) {
      if (!metaNames.has(c)) continue;
      if (empty(fee.fields[c])) emptyCols.push(c);
      else filled++;
    }
    const measurable = CORE_COLS.filter((c) => metaNames.has(c)).length;
    const pct = measurable ? Math.round((100 * filled) / measurable) : 0;
    sumPct += pct;
    n++;
    const row = {
      name,
      status,
      parent: r.fields["Parent Company"] || null,
      linked: true,
      pct,
      filled,
      empty: emptyCols.length,
      emptyCols: emptyCols.slice(0, 8),
    };
    if (active) activeRows.push(row);
  }

  activeRows.sort((a, b) => a.name.localeCompare(b.name));
  const report = {
    generatedAt: new Date().toISOString(),
    coreColumnCount: CORE_COLS.length,
    missingInMeta,
    summary: {
      basics: basics.length,
      withFeeLink: linked,
      avgCompletenessAllLinked: n ? Math.round(sumPct / n) : 0,
      activeLive: activeRows.length,
      activeLiveAvgPct: activeRows.length
        ? Math.round(activeRows.reduce((s, r) => s + r.pct, 0) / activeRows.length)
        : 0,
      activeFullyEmpty: activeRows.filter((r) => r.filled === 0).map((r) => r.name),
      activeFullyComplete: activeRows.filter((r) => r.empty === 0).map((r) => r.name),
    },
    activeLive: activeRows,
    existingTooling: [
      "scripts/apply-choice-fee-structure-batch.mjs",
      "scripts/lib/choice-fee-structure-profiles.mjs",
      "scripts/apply-kimpton-fdd-economics.mjs",
      "lib/partner-intelligence/build-kimpton-fee-structure-from-fdd.js",
      "scripts/apply-curio-fdd-economics.mjs (presentation-focused)",
    ],
  };

  const out = path.join(__dirname, "..", "reports", "fee-structure-gap-audit.json");
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, JSON.stringify(report, null, 2));

  const md = [];
  md.push(`# Brand Setup - Fee Structure gap audit`);
  md.push("");
  md.push(`Generated: ${report.generatedAt}`);
  md.push("");
  md.push(`| Metric | Value |`);
  md.push(`|--------|-------|`);
  md.push(`| Brand Basics | ${report.summary.basics} |`);
  md.push(`| With Fee Structure link | ${report.summary.withFeeLink} |`);
  md.push(`| Avg completeness (all linked, core cols) | ${report.summary.avgCompletenessAllLinked}% |`);
  md.push(`| Active/Live | ${report.summary.activeLive} |`);
  md.push(`| Active/Live avg | ${report.summary.activeLiveAvgPct}% |`);
  md.push(`| Active fully empty | ${report.summary.activeFullyEmpty.length} |`);
  md.push(`| Active fully complete (core) | ${report.summary.activeFullyComplete.length} |`);
  if (missingInMeta.length) {
    md.push("");
    md.push(`## Core columns missing in Meta`);
    for (const c of missingInMeta) md.push(`- ${c}`);
  }
  md.push("");
  md.push(`## Active/Live`);
  md.push("");
  md.push(`| Brand | % | Filled | Empty sample |`);
  md.push(`|-------|---|--------|--------------|`);
  for (const r of activeRows) {
    md.push(
      `| ${r.name} | ${r.pct}% | ${r.filled} | ${(r.emptyCols || []).slice(0, 3).join("; ") || "—"} |`
    );
  }
  const mdPath = path.join(__dirname, "..", "reports", "fee-structure-gap-audit.md");
  fs.writeFileSync(mdPath, md.join("\n") + "\n");
  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Wrote ${out}`);
  console.log(`Wrote ${mdPath}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
