/**
 * Extraction smoke test for PoC-downloaded FDDs only.
 * Tries existing Curio/Kimpton parsers + lightweight Item presence scans.
 * Writes only to reports/fdd-intelligence/.
 */
import fs from "fs";
import path from "path";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import { decodeFddPlainTextBuffer } from "../lib/partner-intelligence/decode-fdd-plain-text.mjs";
import { parseCurioFddEconomics } from "../lib/partner-intelligence/parse-curio-fdd-economics.mjs";
import { parseKimptonFddEconomics } from "../lib/partner-intelligence/parse-kimpton-fdd-economics.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const RAW = path.join(ROOT, "data/fdd-test/raw");
const OUT = path.join(ROOT, "reports/fdd-intelligence");
const PY = path.join(ROOT, "scripts/lib/extract-pdf-text.py");

function walk(dir, acc = []) {
  if (!fs.existsSync(dir)) return acc;
  for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, ent.name);
    if (ent.isDirectory()) walk(p, acc);
    else acc.push(p);
  }
  return acc;
}

function extractText(pdfPath) {
  const r = spawnSync("python", [PY, pdfPath], {
    encoding: "buffer",
    maxBuffer: 80 * 1024 * 1024,
    env: { ...process.env, PYTHONIOENCODING: "utf-8" },
  });
  if (r.status !== 0) {
    return { ok: false, error: (r.stderr || Buffer.from("")).toString("utf8").slice(0, 500) };
  }
  return { ok: true, text: decodeFddPlainTextBuffer(r.stdout) };
}

function smokeScan(text) {
  const has = (re) => re.test(text);
  return {
    item5: has(/ITEM\s*5\b/i),
    item6: has(/ITEM\s*6\b/i),
    item7: has(/ITEM\s*7\b/i),
    item11: has(/ITEM\s*11\b/i),
    item12: has(/ITEM\s*12\b/i),
    item17: has(/ITEM\s*17\b/i),
    item19: has(/ITEM\s*19\b|FINANCIAL PERFORMANCE REPRESENTATION/i),
    item20: has(/ITEM\s*20\b/i),
    royaltyMention: has(/royalty/i),
    marketingMention: has(/marketing|program fee|advertising fund|brand fund/i),
    reservationMention: has(/reservation|distribution fee|GDS/i),
    technologyMention: has(/technology fee|tech fee|PMS|systems fee/i),
    loyaltyMention: has(/loyalty|honors|rewards|one rewards/i),
    franchiseTermMention: has(/initial term|years.*license|term of (this|the) agreement/i),
  };
}

function loadExistingFixture(brand) {
  if (/curio/i.test(brand)) {
    const p = path.join(ROOT, "fixtures/curio-fdd-economics.json");
    return fs.existsSync(p) ? JSON.parse(fs.readFileSync(p, "utf8")) : null;
  }
  if (/kimpton/i.test(brand)) {
    const p = path.join(ROOT, "fixtures/kimpton-fdd-economics.json");
    return fs.existsSync(p) ? JSON.parse(fs.readFileSync(p, "utf8")) : null;
  }
  return null;
}

function compareCurio(existing, parsed) {
  const e = existing?.parsed || {};
  const rows = [];
  const pairs = [
    ["royaltyRoomPct", e.item6?.royaltyRoomPct, parsed.item6?.royaltyRoomPct],
    ["programFeePct", e.item6?.programFeePct, parsed.item6?.programFeePct],
    ["applicationFeeNewDevBase", e.item5?.applicationFeeNewDevBase, parsed.item5?.applicationFeeNewDevBase],
    ["totalInvestmentMin", e.item7?.totalInvestmentMin, parsed.item7?.totalInvestmentMin],
    ["termNewDevelopmentYears", e.item17?.termNewDevelopmentYears, parsed.item17?.termNewDevelopmentYears],
  ];
  for (const [k, a, b] of pairs) {
    rows.push({
      field: k,
      existing: a ?? null,
      newValue: b ?? null,
      match: a != null && b != null ? a === b : null,
      difference: a != null && b != null && a !== b ? { existing: a, new: b } : null,
    });
  }
  return rows;
}

function compareKimpton(existing, parsed) {
  const e = existing?.parsed || {};
  const pairs = [
    ["royaltyRoomPct", e.item6?.royaltyRoomPct, parsed.item6?.royaltyRoomPct],
    ["servicesContributionPct", e.item6?.servicesContributionPct, parsed.item6?.servicesContributionPct],
    ["applicationFeeMinimum", e.item5?.applicationFeeMinimum, parsed.item5?.applicationFeeMinimum],
    ["totalInvestmentMin", e.item7?.totalInvestmentMin, parsed.item7?.totalInvestmentMin],
    ["termNewDevelopmentYears", e.item17?.termNewDevelopmentYears, parsed.item17?.termNewDevelopmentYears],
  ];
  return pairs.map(([k, a, b]) => ({
    field: k,
    existing: a ?? null,
    newValue: b ?? null,
    match: a != null && b != null ? a === b : null,
    difference: a != null && b != null && a !== b ? { existing: a, new: b } : null,
  }));
}

async function main() {
  const metas = walk(RAW).filter((p) => path.basename(p) === "metadata.json");
  const results = [];
  for (const metaPath of metas) {
    const meta = JSON.parse(fs.readFileSync(metaPath, "utf8"));
    const pdfPath = meta.local_path;
    if (!pdfPath || !fs.existsSync(pdfPath)) {
      results.push({ brand: meta.brand, status: "missing_pdf", metaPath });
      continue;
    }
    const extracted = extractText(pdfPath);
    if (!extracted.ok) {
      results.push({ brand: meta.brand, status: "extract_failed", error: extracted.error, meta });
      continue;
    }
    const text = extracted.text;
    const scan = smokeScan(text);
    const entry = {
      brand: meta.brand,
      status: "ok",
      meta,
      smoke: scan,
      parserReuse: null,
      comparison: null,
    };
    try {
      if (/curio/i.test(meta.brand)) {
        const parsed = parseCurioFddEconomics(text);
        entry.parserReuse = { parser: "parseCurioFddEconomics", parsed };
        entry.comparison = compareCurio(loadExistingFixture(meta.brand), parsed);
      } else if (/kimpton/i.test(meta.brand)) {
        const parsed = parseKimptonFddEconomics(text);
        entry.parserReuse = { parser: "parseKimptonFddEconomics", parsed };
        entry.comparison = compareKimpton(loadExistingFixture(meta.brand), parsed);
      } else {
        entry.parserReuse = {
          parser: "none_brand_specific",
          note: "No generalized parser; smoke scan only",
        };
      }
    } catch (e) {
      entry.parserReuse = { error: e.message };
    }
    results.push(entry);
  }
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(
    path.join(OUT, "fdd-poc-extraction-smoke.json"),
    JSON.stringify({ generatedAt: new Date().toISOString(), results }, null, 2)
  );
  console.log(JSON.stringify({ count: results.length, brands: results.map((r) => r.brand) }, null, 2));
}

main();
