/**
 * Move IHG FDD PDFs from brands/{Brand}/ to fdd/ (one-time layout fix).
 *   node scripts/move-ihg-fdds-to-fdd-folder.mjs
 *   node scripts/move-ihg-fdds-to-fdd-folder.mjs --apply
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { resolveReferenceRoot } from "../lib/partner-intelligence/reference-material-paths.js";

const APPLY = process.argv.includes("--apply");
const COMPANY = "IHG Hotels & Resorts";
const root = path.join(resolveReferenceRoot(), COMPANY);
const brandsDir = path.join(root, "brands");
const fddDir = path.join(root, "fdd");

if (!fs.existsSync(brandsDir)) {
  console.log("No brands folder — nothing to move.");
  process.exit(0);
}

const moves = [];
for (const brandFolder of fs.readdirSync(brandsDir, { withFileTypes: true })) {
  if (!brandFolder.isDirectory()) continue;
  const dir = path.join(brandsDir, brandFolder.name);
  for (const file of fs.readdirSync(dir)) {
    if (!/ FDD .*\.pdf$/i.test(file) && !/fdd/i.test(file)) continue;
    if (!file.toLowerCase().endsWith(".pdf")) continue;
    moves.push({
      from: path.join(dir, file),
      to: path.join(fddDir, file),
    });
  }
}

console.log("FDD files to move:", moves.length);
for (const m of moves) {
  console.log(APPLY ? "MOVE" : "WOULD", path.relative(root, m.from), "->", path.relative(root, m.to));
  if (!APPLY) continue;
  fs.mkdirSync(fddDir, { recursive: true });
  if (fs.existsSync(m.to)) {
    console.warn("  SKIP exists", m.to);
    continue;
  }
  fs.renameSync(m.from, m.to);
}

if (!APPLY && moves.length) console.log("\nAdd --apply to move files.");
