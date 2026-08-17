/**
 * Normalize operations.flexibility.* rows to canonical level vocabulary.
 * Moves former narrative bodies into operations.standards_philosophy when needed.
 *
 *   node scripts/normalize-flexibility-presentation.mjs --dry-run
 *   node scripts/normalize-flexibility-presentation.mjs --fixtures
 *   node scripts/normalize-flexibility-presentation.mjs --airtable
 *   node scripts/normalize-flexibility-presentation.mjs --brand-name "Comfort Inn & Suites" --airtable
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";
import {
  FLEXIBILITY_SLOT_KEYS,
  flexEditorialSupplement,
  inferFlexSegmentForBrand,
  normalizeFlexSlotBody,
} from "../lib/brand-explorer-flexibility-levels.mjs";
import { TIER1_BRANDS } from "./lib/choice-tier1-explorer-profiles.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const TABLE = "Brand Setup - Brand Explorer Presentation";
const FIXTURES = path.join(ROOT, "fixtures");

const SEGMENT_BY_BRAND = new Map(TIER1_BRANDS.map((p) => [p.name.toLowerCase(), p.segment]));
const PROFILE_BY_BRAND = new Map(TIER1_BRANDS.map((p) => [p.name.toLowerCase(), p]));

/** Radisson-family fixtures outside Tier 1 generator */
const SEGMENT_OVERRIDES = new Map([
  ["radisson", "upscale"],
  ["radisson (choice)", "upscale"],
  ["radisson blu (choice)", "upscale"],
  ["radisson blu", "upscale"],
  ["kimpton hotels", "softCollection"],
  ["kimpton", "softCollection"],
]);

function parseArgs() {
  const argv = process.argv.slice(2);
  const flags = new Set(argv.filter((a) => a.startsWith("--")));
  let brandName = "";
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--brand-name" && argv[i + 1]) brandName = argv[++i];
  }
  return {
    dryRun: flags.has("--dry-run"),
    fixtures: flags.has("--fixtures") || flags.has("--all") || !flags.has("--airtable-only"),
    airtable: flags.has("--airtable") || flags.has("--all"),
    brandName: brandName.trim(),
  };
}

function segmentForBrand(brandName) {
  const key = String(brandName || "").trim().toLowerCase();
  if (SEGMENT_OVERRIDES.has(key)) return SEGMENT_OVERRIDES.get(key);
  if (SEGMENT_BY_BRAND.has(key)) {
    const s = SEGMENT_BY_BRAND.get(key);
    if (s === "economy" || s === "extendedStay" || s === "upscale" || s === "softCollection") return s;
    return "midscale";
  }
  return inferFlexSegmentForBrand(brandName);
}

function profileForBrand(brandName) {
  const key = String(brandName || "").trim().toLowerCase();
  const p = PROFILE_BY_BRAND.get(key);
  if (p) return p;
  return {
    name: brandName,
    segment: segmentForBrand(brandName),
    developmentModel: "Conversion and new construction—confirm in FDD.",
  };
}

function normalizeRows(rows, brandName) {
  const segment = segmentForBrand(brandName);
  const profile = profileForBrand(brandName);
  const changes = [];
  const bySlot = new Map();
  for (const r of rows) {
    const sk = String(r.slotKey || r["Slot Key"] || "").trim();
    if (!sk) continue;
    if (!bySlot.has(sk)) bySlot.set(sk, r);
  }

  let philosophy = bySlot.get("operations.standards_philosophy");
  const narrativesToAppend = [];

  for (const slotKey of FLEXIBILITY_SLOT_KEYS) {
    const row = bySlot.get(slotKey);
    if (!row) continue;
    const body = String(row.body ?? row.Body ?? "");
    const { level, wasNarrative, prior } = normalizeFlexSlotBody(slotKey, body, segment);
    if (prior === level) continue;
    if (wasNarrative && prior) {
      const label = slotKey.split(".").pop().replace(/_/g, " ");
      narrativesToAppend.push(`${label} (prior indicator copy): ${prior}`);
    }
    changes.push({ slotKey, from: prior, to: level });
    if (row.body !== undefined) row.body = level;
    if (row.Body !== undefined) row.Body = level;
  }

  const supplement = flexEditorialSupplement(profile);
  const philosophyBody = String(philosophy?.body ?? philosophy?.Body ?? "");
  // Avoid re-appending when prior normalize already wrote the detail block.
  const needsPhilosophyBlock =
    !philosophy ||
    (!philosophyBody.includes("Flexibility indicators on") &&
      !philosophyBody.includes("Design flexibility (detail):"));

  if (philosophy) {
    let body = String(philosophy.body ?? philosophy.Body ?? "").trim();
    if (narrativesToAppend.length) {
      body = `${body}\n\n${narrativesToAppend.join("\n\n")}`.trim();
    }
    if (needsPhilosophyBlock) {
      body = `${body}\n\n${supplement}`.trim();
    }
    const oldBody = String(philosophy.body ?? philosophy.Body ?? "");
    if (body !== oldBody) {
      changes.push({
        slotKey: "operations.standards_philosophy",
        from: "(append editorial)",
        to: body.slice(0, 80) + "…",
      });
      if (philosophy.body !== undefined) philosophy.body = body;
      if (philosophy.Body !== undefined) philosophy.Body = body;
    }
  }

  return { changes, segment };
}

function normalizeFixtureFile(filePath, dryRun) {
  const data = JSON.parse(fs.readFileSync(filePath, "utf8"));
  const brand =
    data.targetBrandBasicsName || data.brandNameFallback || data.brandName || path.basename(filePath);
  const rows = data.records || data.rows || [];
  const { changes, segment } = normalizeRows(rows, brand);
  if (changes.length && !dryRun) {
    fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, "utf8");
  }
  return { file: path.relative(ROOT, filePath), brand, segment, changes };
}

async function normalizeAirtableBrand(base, brandName, dryRun) {
  const esc = brandName.replace(/"/g, '\\"');
  const recs = await base(TABLE)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 500 })
    .all();
  const rows = recs.map((r) => ({
    id: r.id,
    slotKey: r.get("Slot Key"),
    body: r.get("Body"),
  }));
  const { changes, segment } = normalizeRows(rows, brandName);
  const updates = [];
  for (const c of changes) {
    if (c.slotKey === "operations.standards_philosophy") {
      const row = rows.find((x) => x.slotKey === c.slotKey);
      if (row) updates.push({ id: row.id, fields: { Body: row.body } });
      continue;
    }
    const row = rows.find((x) => x.slotKey === c.slotKey);
    if (row) updates.push({ id: row.id, fields: { Body: row.body } });
  }
  if (!dryRun && updates.length) {
    for (let i = 0; i < updates.length; i += 10) {
      await base(TABLE).update(updates.slice(i, i + 10));
    }
  }
  return { brand: brandName, segment, changes, updateCount: updates.length };
}

async function main() {
  const { dryRun, fixtures, airtable, brandName } = parseArgs();
  console.log(dryRun ? "DRY RUN\n" : "");

  if (fixtures) {
    const files = fs
      .readdirSync(FIXTURES)
      .filter((f) => f.startsWith("brand-explorer-presentation-") && f.endsWith(".json"));
    let total = 0;
    for (const f of files) {
      const full = path.join(FIXTURES, f);
      let data;
      try {
        data = JSON.parse(fs.readFileSync(full, "utf8"));
      } catch {
        continue;
      }
      const rows = data.records || data.rows || [];
      if (!rows.some((r) => FLEXIBILITY_SLOT_KEYS.includes(r.slotKey || r["Slot Key"]))) continue;
      const result = normalizeFixtureFile(full, dryRun);
      if (result.changes.length) {
        total += result.changes.length;
        console.log(`${result.file} (${result.brand}, ${result.segment})`);
        for (const c of result.changes) {
          if (c.slotKey.startsWith("operations.flexibility.")) {
            console.log(`  ${c.slotKey}: "${c.from}" → "${c.to}"`);
          }
        }
      }
    }
    console.log(`\nFixture flex changes: ${total}`);
  }

  if (airtable) {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) throw new Error("Missing AIRTABLE_* env");
    const base = new Airtable({ apiKey }).base(baseId);
    const brandSet = new Set();
    if (brandName) {
      brandSet.add(brandName);
    } else {
      await base(TABLE)
        .select({ fields: ["Brand Name", "Slot Key"] })
        .eachPage((page, next) => {
          for (const r of page) {
            if (!FLEXIBILITY_SLOT_KEYS.includes(String(r.get("Slot Key") || "").trim())) continue;
            const b = String(r.get("Brand Name") || "").trim();
            if (b) brandSet.add(b);
          }
          next();
        });
    }
    const brands = [...brandSet].sort();
    let n = 0;
    for (const b of brands) {
      const result = await normalizeAirtableBrand(base, b, dryRun);
      if (result.changes.length) {
        n++;
        console.log(`\nAirtable: ${result.brand} (${result.segment}) — ${result.updateCount} row(s)`);
        for (const c of result.changes.filter((x) => x.slotKey.startsWith("operations.flexibility."))) {
          console.log(`  ${c.slotKey}: "${c.from}" → "${c.to}"`);
        }
      }
    }
    console.log(`\nAirtable brands with flex changes: ${n} / ${brands.length}`);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
