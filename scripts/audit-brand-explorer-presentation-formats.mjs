/**
 * Audit Brand Explorer Presentation rows for inconsistent answer formats per Slot Key.
 *
 * Flags slot keys where different brands use different "shape" of Body/Title
 * (e.g. canonical level "High" vs compound "Moderate to High" vs long narrative).
 *
 * Usage:
 *   node scripts/audit-brand-explorer-presentation-formats.mjs
 *   node scripts/audit-brand-explorer-presentation-formats.mjs --fixtures-only
 *   node scripts/audit-brand-explorer-presentation-formats.mjs --out reports/my-audit.md
 *
 * Env (live Airtable): AIRTABLE_API_KEY, AIRTABLE_BASE_ID
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const TABLE = "Brand Setup - Brand Explorer Presentation";
const FIXTURES_DIR = path.join(ROOT, "fixtures");

/** Documented or de-facto expected format per slot (prefix match supported). */
const SLOT_EXPECTED = [
  {
    match: (sk) => sk.startsWith("operations.flexibility."),
    expected: "flex_level",
    label: "Single canonical level (Minimal … Very high) or 1–6 — not narrative paragraphs",
  },
  {
    match: (sk) => sk === "footprint.portfolio_mix",
    expected: "portfolio_mix",
    label: "Title = category; Body = level label (e.g. High) — or Body Category|Level",
  },
  {
    match: (sk) => sk.startsWith("footprint.region."),
    expected: "region_card",
    label: "Body line 1 = status badge; blank line; narrative",
  },
  {
    match: (sk) => sk === "footprint.momentum",
    expected: "momentum_row",
    label: "Title = headline; Body: date, blank, description, optional URL",
  },
  {
    match: (sk) => sk === "standards.requirement",
    expected: "standards_structured",
    label: "Title = area; Body with labeled lines (Typical consideration:, Owner planning:, …)",
  },
  {
    match: (sk) => sk.startsWith("operations.model.") || sk.startsWith("operations.compliance."),
    expected: "short_line",
    label: "One short line or short paragraph (not a 1–6 scale)",
  },
  {
    match: (sk) => sk === "operations.operator_compat.tags",
    expected: "tag_list",
    label: "Tags: one per line or comma/semicolon separated",
  },
  {
    match: (sk) => sk.startsWith("economics.kpi."),
    expected: "fee_or_term_line",
    label: "Typical fee/term shorthand (e.g. 5% · 20 yr) — not essay",
  },
  {
    match: (sk) => sk.startsWith("overview.bestAt.") || sk.startsWith("overview.scenario."),
    expected: "card_copy",
    label: "Card title + short body paragraph",
  },
  {
    match: (sk) => sk.startsWith("valueOwners.scenario.") || sk.startsWith("valueOwners.lifecycle."),
    expected: "card_copy",
    label: "Title + body line(s)",
  },
];

function parseArgs() {
  const flags = new Set(process.argv.slice(2).filter((a) => a.startsWith("--") && !a.includes("=")));
  let out = path.join(ROOT, "reports", "brand-explorer-presentation-format-audit.md");
  for (let i = 2; i < process.argv.length; i++) {
    if (process.argv[i] === "--out" && process.argv[i + 1]) out = path.resolve(process.argv[++i]);
    if (process.argv[i] === "--fixtures-only") flags.add("fixtures-only");
  }
  return { fixturesOnly: flags.has("fixtures-only"), out };
}

function expectedForSlot(slotKey) {
  for (const r of SLOT_EXPECTED) {
    if (r.match(slotKey)) return r;
  }
  return null;
}

const CANONICAL_LEVELS = new Set([
  "minimal",
  "low",
  "moderate",
  "medium",
  "high",
  "very high",
]);

function classifyFlexLevel(text) {
  const s = String(text || "")
    .trim()
    .replace(/[–—]/g, "-");
  if (!s) return "empty";
  if (/^([1-6])(?:\s*\/\s*6)?$/.test(s)) return "numeric_scale";
  const lower = s.toLowerCase();
  if (CANONICAL_LEVELS.has(lower)) return "canonical_level";
  if (/^(minimal|low|moderate|medium|high|very\s*high)$/i.test(s)) return "canonical_level";
  if (/\bto\b|\b[-–]\b/.test(lower) && /\b(minimal|low|moderate|medium|high)\b/.test(lower))
    return "compound_level";
  if (s.length <= 40 && !/[.!?]/.test(s.slice(0, -1))) return "short_phrase";
  if (s.length > 120 || (s.match(/[.!?]/g) || []).length >= 2) return "narrative";
  return "mixed_phrase";
}

function classifyBody(slotKey, body, title) {
  const primary = String(body || "").trim() || String(title || "").trim();
  if (!primary) return "empty";

  const exp = expectedForSlot(slotKey);
  if (exp?.expected === "flex_level") return classifyFlexLevel(primary);

  if (exp?.expected === "standards_structured") {
    if (/^(Typical consideration|Owner planning|Applies to|Typical status|Notes to confirm):/im.test(body))
      return "structured_labels";
    if (body.length > 200) return "narrative";
    return "unstructured_text";
  }

  if (exp?.expected === "portfolio_mix") {
    if (/^\s*[^|]+\|\s*[^|]+\s*$/.test(primary.split("\n")[0])) return "pipe_pair";
    if (primary.length < 60 && !primary.includes("\n\n")) return "title_body_pair";
    return "narrative";
  }

  if (exp?.expected === "tag_list") {
    const lines = primary.split(/\n/).filter(Boolean);
    if (lines.length >= 2 && lines.every((l) => l.length < 80)) return "line_list";
    if (primary.includes(",") && primary.length < 200) return "comma_list";
    return "mixed_phrase";
  }

  if (exp?.expected === "momentum_row") {
    const paras = primary.split(/\n\n+/);
    if (paras.length >= 2) return "multi_paragraph";
    return "short_block";
  }

  if (primary.includes("\n\n")) return "multi_paragraph";
  if (/^https?:\/\//im.test(primary)) return "url_only";
  if (primary.length <= 48 && !primary.includes("\n")) return "short_line";
  if (primary.length <= 120) return "medium_line";
  return "narrative";
}

function preview(text, max = 72) {
  const one = String(text || "").replace(/\s+/g, " ").trim();
  return one.length <= max ? one : one.slice(0, max - 1) + "…";
}

async function loadFromAirtable() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("Missing AIRTABLE_BASE_ID or AIRTABLE_API_KEY");
  const base = new Airtable({ apiKey }).base(baseId);
  const rows = [];
  await base(TABLE)
    .select({
      fields: ["Slot Key", "Brand Name", "Title", "Body", "Active"],
      pageSize: 100,
    })
    .eachPage((page, next) => {
      for (const rec of page) {
        const active = rec.get("Active");
        if (active === false || String(active).toLowerCase() === "no") continue;
        const brand = String(rec.get("Brand Name") || "").trim() || "(no brand name)";
        const slotKey = String(rec.get("Slot Key") || "").trim();
        if (!slotKey) continue;
        rows.push({
          brand,
          slotKey,
          title: String(rec.get("Title") || ""),
          body: String(rec.get("Body") || ""),
          source: "airtable",
        });
      }
      next();
    });
  return rows;
}

function loadFromFixtures() {
  const rows = [];
  if (!fs.existsSync(FIXTURES_DIR)) return rows;
  const files = fs
    .readdirSync(FIXTURES_DIR)
    .filter((f) => f.startsWith("brand-explorer-presentation-") && f.endsWith(".json"));
  for (const file of files) {
    let data;
    try {
      data = JSON.parse(fs.readFileSync(path.join(FIXTURES_DIR, file), "utf8"));
    } catch {
      continue;
    }
    const brand =
      data.brandName ||
      data.brand ||
      file.replace(/^brand-explorer-presentation-/, "").replace(/\.json$/, "").replace(/-full$/, "");
    const recs = data.records || data.rows || [];
    for (const r of recs) {
      const slotKey = String(r.slotKey || r["Slot Key"] || "").trim();
      if (!slotKey) continue;
      rows.push({
        brand: String(brand),
        slotKey,
        title: String(r.title ?? r.Title ?? ""),
        body: String(r.body ?? r.Body ?? ""),
        source: `fixture:${file}`,
      });
    }
  }
  return rows;
}

function buildAudit(rows) {
  /** slotKey -> Map format -> { brands: Set, samples: [] } */
  const bySlot = new Map();

  for (const row of rows) {
    const fmt = classifyBody(row.slotKey, row.body, row.title);
    if (!bySlot.has(row.slotKey)) bySlot.set(row.slotKey, new Map());
    const slotMap = bySlot.get(row.slotKey);
    if (!slotMap.has(fmt)) slotMap.set(fmt, { brands: new Set(), samples: [] });
    const bucket = slotMap.get(fmt);
    bucket.brands.add(row.brand);
    if (bucket.samples.length < 4) {
      bucket.samples.push({
        brand: row.brand,
        format: fmt,
        preview: preview(row.body || row.title),
        source: row.source,
      });
    }
  }

  const issues = [];
  for (const [slotKey, formatMap] of [...bySlot.entries()].sort()) {
    const formats = [...formatMap.keys()].filter((f) => f !== "empty");
    if (formats.length < 2) continue;
    const exp = expectedForSlot(slotKey);
    const brandsByFormat = {};
    for (const [fmt, bucket] of formatMap) {
      if (fmt === "empty") continue;
      brandsByFormat[fmt] = [...bucket.brands].sort();
    }
    issues.push({
      slotKey,
      formats,
      brandsByFormat,
      samples: [...formatMap.values()].flatMap((b) => b.samples).slice(0, 8),
      expected: exp,
      severity:
        exp?.expected === "flex_level" &&
        formats.some((f) => f === "canonical_level" || f === "numeric_scale") &&
        formats.some((f) => f === "narrative" || f === "compound_level")
          ? "high"
          : formats.length >= 3
            ? "medium"
            : "low",
    });
  }

  issues.sort((a, b) => {
    const sev = { high: 0, medium: 1, low: 2 };
    return (sev[a.severity] ?? 9) - (sev[b.severity] ?? 9) || a.slotKey.localeCompare(b.slotKey);
  });

  return { bySlot, issues, brandCount: new Set(rows.map((r) => r.brand)).size, rowCount: rows.length };
}

function renderMarkdown(audit, sources) {
  const lines = [];
  lines.push("# Brand Explorer Presentation — format consistency audit");
  lines.push("");
  lines.push(`Generated: ${new Date().toISOString()}`);
  lines.push(`Sources: ${sources.join(", ")}`);
  lines.push(`Rows: ${audit.rowCount} · Brands: ${audit.brandCount} · Slot keys: ${audit.bySlot.size}`);
  lines.push("");
  lines.push("## Summary");
  lines.push("");
  const high = audit.issues.filter((i) => i.severity === "high");
  const med = audit.issues.filter((i) => i.severity === "medium");
  lines.push(
    `- **${audit.issues.length}** slot keys with **2+ answer formats** across brands (${high.length} high-priority, ${med.length} medium).`
  );
  lines.push(
    "- **High-priority** usually means documented canonical levels (e.g. flexibility indicators) mixed with narratives or compound phrases like `Moderate to High`."
  );
  lines.push("");
  lines.push("### Format type legend");
  lines.push("");
  lines.push("| Type | Meaning |");
  lines.push("|------|---------|");
  lines.push("| `canonical_level` | Single label: Minimal, Low, Moderate, Medium, High, Very high |");
  lines.push("| `numeric_scale` | `1`–`6` or `4/6` |");
  lines.push("| `compound_level` | e.g. Moderate to High, Low–Moderate |");
  lines.push("| `narrative` | Long prose or multiple sentences |");
  lines.push("| `short_line` / `medium_line` | Brief copy |");
  lines.push("| `structured_labels` | standards.requirement labeled lines |");
  lines.push("");
  if (high.length) {
    lines.push("## High-priority inconsistencies");
    lines.push("");
    for (const issue of high) {
      lines.push(`### \`${issue.slotKey}\``);
      if (issue.expected) {
        lines.push(`**Expected:** ${issue.expected.label}`);
      }
      lines.push("");
      for (const fmt of issue.formats) {
        const brands = issue.brandsByFormat[fmt] || [];
        lines.push(`- **${fmt}** (${brands.length}): ${brands.slice(0, 12).join("; ")}${brands.length > 12 ? ` … +${brands.length - 12}` : ""}`);
      }
      lines.push("");
      lines.push("Samples:");
      for (const s of issue.samples) {
        lines.push(`- ${s.brand} [${s.format}]: “${s.preview}”`);
      }
      lines.push("");
    }
  }
  lines.push("## All slot keys with mixed formats");
  lines.push("");
  if (!audit.issues.length) {
    lines.push("_No cross-brand format conflicts detected._");
  } else {
    lines.push("| Slot key | Formats | Expected rule |");
    lines.push("|----------|---------|---------------|");
    for (const issue of audit.issues) {
      lines.push(
        `| \`${issue.slotKey}\` | ${issue.formats.join(", ")} | ${issue.expected?.expected || "—"} |`
      );
    }
  }
  lines.push("");
  lines.push("## Recommended normalization");
  lines.push("");
  lines.push("1. **`operations.flexibility.*`** — Use **one canonical word** per row (`High`, `Moderate`, …) or **`1`–`6`**. Avoid `Moderate to High` and long narratives on these six slots; put prose in `operations.standards_philosophy` or conversion copy slots.");
  lines.push("2. **Choice full-bundle templates** — Many `*-full.json` fixtures use narrative bodies for flexibility; align with Radisson Blu / docs example (`High`, `Very high`, etc.) when pushing to Airtable.");
  lines.push("3. **`operations.flexibility.operational_rigidity`** — Often `Moderate to High` in CHI bundles vs single word elsewhere; pick one convention.");
  lines.push("4. Run this script after bulk applies: `node scripts/audit-brand-explorer-presentation-formats.mjs`");
  lines.push("");
  return lines.join("\n");
}

async function main() {
  const { fixturesOnly, out } = parseArgs();
  const sources = [];
  let rows = [];

  if (!fixturesOnly) {
    try {
      rows = rows.concat(await loadFromAirtable());
      sources.push("Airtable");
    } catch (e) {
      console.warn("Airtable load skipped:", e.message);
    }
  }
  const fixtureRows = loadFromFixtures();
  if (fixtureRows.length) {
    rows = rows.concat(fixtureRows);
    sources.push(`fixtures (${fixtureRows.length} rows)`);
  }
  if (!rows.length) {
    console.error("No rows to audit.");
    process.exit(1);
  }

  const audit = buildAudit(rows);
  const md = renderMarkdown(audit, sources);
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, md, "utf8");

  console.log(`Wrote ${out}`);
  console.log(`Issues: ${audit.issues.length} slot keys with mixed formats`);
  const flexIssues = audit.issues.filter((i) => i.slotKey.startsWith("operations.flexibility."));
  console.log(`Flexibility slots with mixed formats: ${flexIssues.length}`);
  for (const i of flexIssues) {
    console.log(`  ${i.slotKey}: ${i.formats.join(", ")}`);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
