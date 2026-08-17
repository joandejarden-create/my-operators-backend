/**
 * Extract Kimpton FDD Items 5/6/7 economics → Brand Setup (Fee Structure + Deal Terms)
 * and Brand Explorer economics.* presentation slots.
 *
 * Display copy does NOT cite "Kimpton FDD 2024 … Item 5/6/7" on every line.
 * Parsed evidence is stored in fixtures/kimpton-fdd-economics.json for audit.
 *
 *   node scripts/apply-kimpton-fdd-economics.mjs
 *   node scripts/apply-kimpton-fdd-economics.mjs --apply
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { spawnSync } from "child_process";
import "../load-env.js";
import Airtable from "airtable";
import { PILOT_BRANDS } from "../api/lib/partner-intelligence-explorer-field-registry.js";
import { parseKimptonFddEconomics } from "../lib/partner-intelligence/parse-kimpton-fdd-economics.mjs";
import {
  buildKimptonDealTermsPatch,
  buildKimptonFeeStructurePatch,
} from "../lib/partner-intelligence/build-kimpton-fee-structure-from-fdd.js";
import { applyKimptonEconomicsPresentationSlots } from "../lib/partner-intelligence/build-kimpton-economics-presentation-slots.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const PILOT = PILOT_BRANDS.kimptonHotels;
const BASICS_ID = PILOT.recordId || "recCKuXCmGvxHPfb3";

const FDD_REL =
  "IHG Hotels & Resorts/fdd/Kimpton FDD 2024 (MN state filing).pdf";
const PLAIN_CACHE = path.join(ROOT, "reports", "kimpton-fdd-plain.txt");
const ECON_JSON = path.join(ROOT, "fixtures", "kimpton-fdd-economics.json");
const ECON_PRESENTATION = path.join(ROOT, "fixtures", "brand-explorer-presentation-kimpton-economics.json");
const SETUP_FIXTURE = path.join(ROOT, "fixtures", "kimpton-brand-setup.json");
const TEMPLATE = path.join(ROOT, "fixtures", "brand-explorer-presentation-kimpton-full.json");
const PY = path.join(ROOT, "scripts", "lib", "extract-pdf-text.py");

const LINKED = [
  { linkField: "Brand Setup - Fee Structure", table: "Brand Setup - Fee Structure" },
  { linkField: "Brand Setup - Deal Terms", table: "Brand Setup - Deal Terms" },
];

function decodeFddBuffer(buf) {
  if (buf.length >= 2 && buf[0] === 0xff && buf[1] === 0xfe) {
    return buf.toString("utf16le");
  }
  return buf.toString("utf8");
}

function loadFddText() {
  const refRoot = process.env.BRAND_REFERENCE_MATERIAL_ROOT || process.env.PI_REFERENCE_ROOT;
  if (refRoot) {
    const abs = path.join(refRoot, FDD_REL);
    if (fs.existsSync(abs)) {
      const r = spawnSync("python", [PY, abs], {
        encoding: "buffer",
        maxBuffer: 50 * 1024 * 1024,
        env: { ...process.env, PYTHONIOENCODING: "utf-8" },
      });
      if (r.status === 0 && r.stdout?.length) {
        const text = decodeFddBuffer(r.stdout);
        fs.mkdirSync(path.dirname(PLAIN_CACHE), { recursive: true });
        fs.writeFileSync(PLAIN_CACHE, text, "utf8");
        return text;
      }
    }
  }
  if (fs.existsSync(PLAIN_CACHE)) {
    const text = decodeFddBuffer(fs.readFileSync(PLAIN_CACHE));
    if (!PLAIN_CACHE.endsWith(".utf8") && text.includes("Royalty 6%")) {
      fs.writeFileSync(PLAIN_CACHE, text, "utf8");
    }
    return text;
  }
  throw new Error(
    `Kimpton FDD text not found. Set BRAND_REFERENCE_MATERIAL_ROOT or refresh ${PLAIN_CACHE}`
  );
}

function extractUnknownFieldName(err) {
  const msg = String(err?.message || err || "");
  const m =
    msg.match(/Unknown field name:\s*['"](.+?)['"]/i) ||
    msg.match(/INVALID_VALUE_FOR_COLUMN[^"]*"([^"]+)"/i);
  return m ? m[1] : null;
}

async function writeWithFieldPruning(base, table, recordId, fields) {
  let payload = { ...fields };
  const removed = [];
  for (let attempt = 0; attempt < 80; attempt++) {
    if (!Object.keys(payload).length) return { removed };
    try {
      await base(table).update(recordId, payload, { typecast: true });
      return { removed };
    } catch (err) {
      const bad = extractUnknownFieldName(err);
      if (bad && Object.prototype.hasOwnProperty.call(payload, bad)) {
        delete payload[bad];
        removed.push(bad);
        continue;
      }
      throw err;
    }
  }
  throw new Error(`writeWithFieldPruning exceeded retries for ${table}`);
}

async function patchChildTable(base, basicsId, { linkField, table }, fields) {
  const basics = await base("Brand Setup - Brand Basics").find(basicsId);
  const childId = Array.isArray(basics.get(linkField)) ? basics.get(linkField)[0] : null;
  if (!childId) throw new Error(`No linked ${table} on basics ${basicsId}`);
  const payload = { ...fields, "Brand Name": PILOT.brandName };
  const { removed } = await writeWithFieldPruning(base, table, childId, payload);
  console.log(`  ${table}: updated ${childId} (${Object.keys(fields).length} fields)`);
  if (removed.length) console.warn(`    dropped fields:`, removed.join(", "));
  return childId;
}

function mergeSetupFixture(feePatch, dealPatch) {
  const spec = JSON.parse(fs.readFileSync(SETUP_FIXTURE, "utf8"));
  spec.childTables = spec.childTables || {};
  spec.childTables["Brand Setup - Fee Structure"] = {
    ...(spec.childTables["Brand Setup - Fee Structure"] || {}),
    ...feePatch,
  };
  spec.childTables["Brand Setup - Deal Terms"] = {
    ...(spec.childTables["Brand Setup - Deal Terms"] || {}),
    ...dealPatch,
  };
  fs.writeFileSync(SETUP_FIXTURE, JSON.stringify(spec, null, 2) + "\n");
  console.log("Updated", SETUP_FIXTURE);
}

async function main() {
  const text = loadFddText();
  const econ = parseKimptonFddEconomics(text);
  const feePatch = buildKimptonFeeStructurePatch(econ);
  const dealPatch = buildKimptonDealTermsPatch(econ);

  fs.writeFileSync(
    ECON_JSON,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        brandName: PILOT.brandName,
        basicsRecordId: BASICS_ID,
        sourceDocument: FDD_REL,
        parsed: econ,
        feeStructurePatch: feePatch,
        dealTermsPatch: dealPatch,
      },
      null,
      2
    )
  );
  console.log("Wrote", ECON_JSON);

  const template = JSON.parse(fs.readFileSync(TEMPLATE, "utf8"));
  const rows = applyKimptonEconomicsPresentationSlots(template.rows, econ, {
    brandName: PILOT.brandName,
  });
  const economicsOnly = rows.filter((r) => String(r.slotKey || "").startsWith("economics."));
  const out = {
    targetBrandBasicsName: PILOT.brandName,
    brandNameFallback: PILOT.brandName,
    instructions: "Economics slots from Kimpton FDD parse — no inline source citations",
    rows: economicsOnly,
  };
  fs.writeFileSync(ECON_PRESENTATION, JSON.stringify(out, null, 2));
  console.log("Wrote", ECON_PRESENTATION, "| economics rows:", economicsOnly.length);

  mergeSetupFixture(feePatch, dealPatch);

  console.log("\nParsed highlights:");
  if (econ.item6.royaltyRoomPct != null) {
    console.log(
      "  Royalty:",
      `${(econ.item6.royaltyRoomPct * 100).toFixed(0)}% GRR + ${((econ.item6.royaltyFoodBeveragePct || 0) * 100).toFixed(0)}% F&B`
    );
  }
  if (econ.item6.servicesContributionPct != null) {
    console.log("  Services contribution:", `${(econ.item6.servicesContributionPct * 100).toFixed(0)}%`);
  }
  if (econ.item6.technologyPerRoomMonthly != null) {
    console.log("  Tech:", `$${econ.item6.technologyPerRoomMonthly}/room/mo`);
  }
  if (econ.item7.perRoomMin != null && econ.item7.perRoomMax != null) {
    console.log(
      "  Item 7 (200-room):",
      `$${econ.item7.perRoomMin.toLocaleString()}–$${econ.item7.perRoomMax.toLocaleString()}/key`
    );
  }

  if (!APPLY) {
    console.log("\nDry run. Pass --apply to write Airtable + presentation slots.");
    return;
  }

  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const base = new Airtable({ apiKey: key }).base(baseId);
  console.log("\n=== Brand Setup (Fee Structure + Deal Terms) ===");
  for (const link of LINKED) {
    const fields = link.table.includes("Fee") ? feePatch : dealPatch;
    await patchChildTable(base, BASICS_ID, link, fields);
  }

  console.log("\n=== Brand Explorer economics.* ===");
  const pres = spawnSync(
    "node",
    [
      path.join(ROOT, "scripts", "apply-brand-explorer-presentation-fixture.mjs"),
      "--brand-record-id",
      BASICS_ID,
      "--fixture",
      ECON_PRESENTATION,
      "--replace-slot-prefix",
      "economics.",
    ],
    { stdio: "inherit", cwd: ROOT, env: process.env }
  );
  process.exit(pres.status || 0);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
