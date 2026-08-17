/**
 * Patch Curio Brand Explorer materials with real CALA Curio hotels (replaces Kimpton template carryover).
 *
 * WARNING: Never use --replace-slot-prefix "materials." — that deletes materials.file PDFs and
 * materials.gallery Image attachments. Use the narrow prefixes below only.
 *
 *   node scripts/apply-curio-cala-materials.mjs --dry-run
 *   node scripts/apply-curio-cala-materials.mjs --apply
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { spawnSync } from "child_process";
import { getCurioCalaMaterialRows, overlayCurioCalaMaterials } from "../lib/curio-brand-explorer-cala-materials.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BRAND_NAME = "Curio Collection by Hilton";
const SOURCE_FIXTURE = path.join(ROOT, "fixtures", "brand-explorer-presentation-curio-from-sources.json");
const FULL_FIXTURE = path.join(ROOT, "fixtures", "brand-explorer-presentation-curio-full.json");
const PATCH_FIXTURE = path.join(ROOT, "fixtures", "brand-explorer-presentation-curio-cala-materials.json");

const APPLY = process.argv.includes("--apply");

function main() {
  for (const p of [SOURCE_FIXTURE, FULL_FIXTURE]) {
    if (!fs.existsSync(p)) continue;
    const fixture = JSON.parse(fs.readFileSync(p, "utf8"));
    const rows = overlayCurioCalaMaterials(fixture.rows);
    fs.writeFileSync(p, JSON.stringify({ ...fixture, rows }, null, 2));
    console.log("Patched", p);
  }

  const patchOut = {
    targetBrandBasicsName: BRAND_NAME,
    brandNameFallback: BRAND_NAME,
    instructions: `CALA Curio gallery + case studies. Apply: node scripts/apply-curio-cala-materials.mjs --apply`,
    rows: getCurioCalaMaterialRows(),
  };
  fs.writeFileSync(PATCH_FIXTURE, JSON.stringify(patchOut, null, 2));
  console.log("Wrote", PATCH_FIXTURE, `(${patchOut.rows.length} rows)`);

  if (!APPLY) {
    console.log("\nDry run. Re-run with --apply to push to Airtable.");
    return;
  }

  const applyScript = path.join(ROOT, "scripts", "apply-brand-explorer-presentation-fixture.mjs");
  /** Text-only slots — never materials.file (PDF attachments) or bare materials. prefix */
  for (const prefix of [
    "materials.gallery.",
    "materials.caseStudy",
    "footprint.openings",
    "footprint.momentum",
  ]) {
    console.log("\nApplying prefix:", prefix);
    const res = spawnSync(
      "node",
      [
        applyScript,
        "--brand-name",
        BRAND_NAME,
        "--fixture",
        PATCH_FIXTURE,
        "--replace-slot-prefix",
        prefix,
      ],
      { stdio: "inherit", cwd: ROOT, env: process.env }
    );
    if (res.status !== 0) process.exit(res.status || 1);
  }

  console.log("\nApplying footprint.region.cala + footprint.momentum_label…");
  const res2 = spawnSync(
    "node",
    [
      applyScript,
      "--brand-name",
      BRAND_NAME,
      "--fixture",
      PATCH_FIXTURE,
      "--slot-keys",
      "footprint.region.cala,footprint.momentum_label",
      "--only-missing",
    ],
    { stdio: "inherit", cwd: ROOT, env: process.env }
  );
  if (res2.status !== 0) {
    spawnSync(
      "node",
      [
        applyScript,
        "--brand-name",
        BRAND_NAME,
        "--fixture",
        PATCH_FIXTURE,
        "--replace-slot-prefix",
        "footprint.region.cala",
      ],
      { stdio: "inherit", cwd: ROOT, env: process.env }
    );
    spawnSync(
      "node",
      [
        applyScript,
        "--brand-name",
        BRAND_NAME,
        "--fixture",
        PATCH_FIXTURE,
        "--replace-slot-prefix",
        "footprint.momentum_label",
      ],
      { stdio: "inherit", cwd: ROOT, env: process.env }
    );
  }

  console.log("\nDone. Review Brand Explorer materials tab for Curio Collection by Hilton.");
}

main();
