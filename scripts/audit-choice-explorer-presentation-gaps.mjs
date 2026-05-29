/**
 * Audit CHI brands for missing Brand Explorer presentation slot keys / row counts.
 *
 *   node scripts/audit-choice-explorer-presentation-gaps.mjs
 *   node scripts/audit-choice-explorer-presentation-gaps.mjs --brand "Ascend Hotel Collection"
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";
import { resolveProfileForAirtableName } from "./lib/choice-chi-brand-resolve.mjs";
import {
  buildCompletePresentationRows,
  slotKeyCounts,
} from "./lib/choice-explorer-complete-rows.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS = "Brand Setup - Brand Basics";
const OUT = path.join(ROOT, "docs", "choice-explorer-presentation-gap-audit.md");

function parseArgs(argv) {
  const i = argv.indexOf("--brand");
  return { brandFilter: i >= 0 ? String(argv[i + 1] || "").trim() : "" };
}

async function listChiBrands(base) {
  const rows = await base(BASICS).select({ maxRecords: 500 }).all();
  return rows
    .filter((r) => String(r.get("Parent Company") || "").includes("Choice Hotels International"))
    .map((r) => String(r.get("Brand Name") || "").trim())
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
}

function countMapFromRecords(records) {
  /** @type {Map<string, number>} */
  const counts = new Map();
  for (const rec of records) {
    const sk = String(rec.get("Slot Key") || "").trim();
    if (!sk) continue;
    counts.set(sk, (counts.get(sk) || 0) + 1);
  }
  return counts;
}

async function main() {
  const { brandFilter } = parseArgs(process.argv);
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID
  );
  let brands = await listChiBrands(base);
  if (brandFilter) brands = brands.filter((b) => b === brandFilter);

  const lines = [
    "# Choice Explorer presentation gap audit",
    "",
    `Generated: ${new Date().toISOString().slice(0, 10)}`,
    "",
    "Compares live Airtable rows to expected rows from `buildCompletePresentationRows()` (includes default `footprint.openings` when profile has none).",
    "",
  ];

  let totalGaps = 0;

  for (const brandName of brands) {
    const esc = brandName.replace(/"/g, '\\"');
    /** @type {import('airtable').Record<any>[]} */
    const existing = [];
    const seen = new Set();
    const push = (records) => {
      for (const r of records) {
        if (seen.has(r.id)) continue;
        seen.add(r.id);
        existing.push(r);
      }
    };
    try {
      push(
        await base(TABLE)
          .select({ filterByFormula: `{Brand} = "${esc}"`, maxRecords: 500 })
          .all()
      );
    } catch {
      /* schema */
    }
    try {
      push(
        await base(TABLE)
          .select({
            filterByFormula: `AND({Brand Name} = "${esc}", {Active} != FALSE())`,
            maxRecords: 500,
          })
          .all()
      );
    } catch {
      /* optional column */
    }
    const have = countMapFromRecords(existing);
    const expected = slotKeyCounts(buildCompletePresentationRows(brandName));
    const profile = resolveProfileForAirtableName(brandName);

    const missing = [];
    const short = [];
    for (const [sk, need] of expected) {
      const got = have.get(sk) || 0;
      if (got === 0) missing.push(sk);
      else if (got < need) short.push(`${sk} (${got}/${need})`);
    }

    const gapCount = missing.length + short.length;
    totalGaps += gapCount;

    lines.push(`## ${brandName}`);
    lines.push("");
    lines.push(`- Profile: \`${profile.name}\` (${profile.slug})`);
    lines.push(`- Existing rows: ${existing.length} · Expected unique slot keys: ${expected.size}`);
    lines.push(`- Missing slot keys: **${missing.length}** · Short counts: **${short.length}**`);
    if (missing.length) {
      lines.push("");
      lines.push("**Missing:**");
      for (const sk of missing.sort()) lines.push(`- \`${sk}\``);
    }
    if (short.length) {
      lines.push("");
      lines.push("**Need more rows:**");
      for (const s of short.sort()) lines.push(`- \`${s}\``);
    }
    lines.push(`- Status: **${gapCount ? "gaps remain" : "complete"}**`);
    lines.push("");
    console.log(
      `${gapCount ? "GAP " : "OK  "} ${brandName} | existing=${existing.length} missing=${missing.length} short=${short.length}`
    );
  }

  lines.push(`## Summary`);
  lines.push("");
  lines.push(`Total brands: ${brands.length} · Brands with gaps: ${brands.filter(() => false).length}`);
  lines.push(`Total missing/short slot groups: ${totalGaps}`);

  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, lines.join("\n"), "utf8");
  console.log(`\nWrote ${OUT}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
