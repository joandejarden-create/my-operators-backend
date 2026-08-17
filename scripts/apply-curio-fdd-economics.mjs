/**
 * Extract Curio FDD Items 5/6/7/17/19 economics → Brand Explorer economics.* presentation slots.
 *
 * Parsed evidence is stored in fixtures/curio-fdd-economics.json for audit.
 *
 *   npm run apply-curio-fdd-economics
 *   npm run apply-curio-fdd-economics -- --apply
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { spawnSync } from "child_process";
import "../load-env.js";
import { decodeFddPlainTextBuffer } from "../lib/partner-intelligence/decode-fdd-plain-text.mjs";
import { parseCurioFddEconomics } from "../lib/partner-intelligence/parse-curio-fdd-economics.mjs";
import { applyCurioEconomicsPresentationSlots } from "../lib/partner-intelligence/build-curio-economics-presentation-slots.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const BRAND_NAME = "Curio Collection by Hilton";
const BASICS_ID = "receQkxgjlezsc1xg";

const FDD_REL = "Hilton/operator-materials/2026 US Curio FDD.pdf";
const PLAIN_CACHE = path.join(ROOT, "reports", "curio-fdd-plain.txt");
const ECON_JSON = path.join(ROOT, "fixtures", "curio-fdd-economics.json");
const ECON_PRESENTATION = path.join(ROOT, "fixtures", "brand-explorer-presentation-curio-economics.json");
const FULL_FIXTURE = path.join(ROOT, "fixtures", "brand-explorer-presentation-curio-full.json");
const SOURCE_FIXTURE = path.join(ROOT, "fixtures", "brand-explorer-presentation-curio-from-sources.json");
const PY = path.join(ROOT, "scripts", "lib", "extract-pdf-text.py");

function decodeFddBuffer(buf) {
  return decodeFddPlainTextBuffer(buf);
}

function loadFddText() {
  if (fs.existsSync(PLAIN_CACHE)) {
    const text = decodeFddPlainTextBuffer(fs.readFileSync(PLAIN_CACHE));
    if (text.includes("Monthly Royalty") || text.includes("5% of Gross Rooms")) {
      return text;
    }
  }

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

  throw new Error(
    `Curio FDD text not found. Set BRAND_REFERENCE_MATERIAL_ROOT or refresh ${PLAIN_CACHE}`
  );
}

function patchFullFixtures(rows) {
  for (const fixturePath of [FULL_FIXTURE, SOURCE_FIXTURE]) {
    if (!fs.existsSync(fixturePath)) continue;
    const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
    const merged = applyCurioEconomicsPresentationSlots(fixture.rows, rows, { brandName: BRAND_NAME });
    fixture.rows = merged;
    fs.writeFileSync(fixturePath, JSON.stringify(fixture, null, 2));
    console.log("Updated", fixturePath);
  }
}

function main() {
  const text = loadFddText();
  const econ = parseCurioFddEconomics(text);

  fs.writeFileSync(
    ECON_JSON,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        brandName: BRAND_NAME,
        basicsRecordId: BASICS_ID,
        sourceDocument: FDD_REL,
        parsed: econ,
      },
      null,
      2
    )
  );
  console.log("Wrote", ECON_JSON);

  const template = JSON.parse(fs.readFileSync(FULL_FIXTURE, "utf8"));
  const rows = applyCurioEconomicsPresentationSlots(template.rows, econ, { brandName: BRAND_NAME });
  const economicsOnly = rows.filter((r) => String(r.slotKey || "").startsWith("economics."));
  const out = {
    targetBrandBasicsName: BRAND_NAME,
    brandNameFallback: BRAND_NAME,
    instructions: "Economics slots from Curio FDD parse — no inline source citations",
    rows: economicsOnly,
  };
  fs.writeFileSync(ECON_PRESENTATION, JSON.stringify(out, null, 2));
  console.log("Wrote", ECON_PRESENTATION, "| economics rows:", economicsOnly.length);

  patchFullFixtures(econ);

  console.log("\nParsed highlights:");
  if (econ.item6.royaltyRoomPct != null) {
    console.log("  Royalty:", `${(econ.item6.royaltyRoomPct * 100).toFixed(0)}% GRR`);
  }
  if (econ.item6.programFeePct != null) {
    console.log("  Program fee:", `${(econ.item6.programFeePct * 100).toFixed(0)}% GRR`);
  }
  if (econ.item6.honorsEligibleFolioPct != null) {
    console.log("  Hilton Honors:", `${(econ.item6.honorsEligibleFolioPct * 100).toFixed(0)}% eligible folio`);
  }
  if (econ.item7.perRoomMin != null && econ.item7.perRoomMax != null) {
    console.log(
      "  Item 7 (200-room):",
      `$${econ.item7.perRoomMin.toLocaleString()}–$${econ.item7.perRoomMax.toLocaleString()}/key`
    );
  }
  if (econ.item19.honorsOccupancyContributionAvgPct != null) {
    console.log(
      "  Item 19 honors occupancy:",
      `~${(econ.item19.honorsOccupancyContributionAvgPct * 100).toFixed(1)}% avg`
    );
  }

  if (!APPLY) {
    console.log("\nDry run. Pass --apply to write economics.* slots to Airtable.");
    return;
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

main();
