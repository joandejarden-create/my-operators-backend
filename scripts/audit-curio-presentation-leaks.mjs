/**
 * Audit Curio presentation fixture for Kimpton/IHG/Choice template leaks.
 *
 *   node scripts/audit-curio-presentation-leaks.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { auditCurioForbiddenCopy } from "../lib/curio-brand-explorer-presentation-overrides.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const FIXTURE = path.join(ROOT, "fixtures", "brand-explorer-presentation-curio-full.json");

const fixture = JSON.parse(fs.readFileSync(FIXTURE, "utf8"));
const violations = auditCurioForbiddenCopy(fixture.rows);

if (!violations.length) {
  console.log("No Kimpton/IHG leak patterns in", FIXTURE);
  process.exit(0);
}

console.log("Leaks found:", violations.length);
for (const v of violations) console.log(" -", v);
process.exit(1);
