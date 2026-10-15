#!/usr/bin/env node
/**
 * Fast GDI quality gate for development.
 *   npm run gdi:quality:quick
 */
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const failures = [];

function run(label, cmd, args) {
  console.log(`\n=== ${label} ===`);
  const r = spawnSync(cmd, args, {
    cwd: ROOT,
    stdio: "inherit",
    shell: true,
    env: process.env,
  });
  if (r.status !== 0) failures.push(label);
}

run("integrity-fixtures", "node", ["scripts/test-gdi-quality-integrity-v1.mjs"]);
run("hotel-fit-presentation", "node", ["scripts/test-gdi-hotel-fit-presentation-v1.mjs"]);
run("buyer-contact-contract", "node", ["scripts/test-gdi-buyer-contact-enrichment-v1.mjs"]);
run("sanitize-v2", "node", ["scripts/test-gdi-customer-presentation-sanitize-v2.mjs"]);
run("drawer-fit-display", "node", ["scripts/test-gdi-drawer-hotel-fit-display-v1.mjs"]);
run("history-eligibility", "node", ["scripts/test-gdi-hotel-activity-history-eligibility-v1.mjs"]);
// Scoped prettier: only files owned by the quality/sanitize stack (avoid
// failing the gate on pre-existing unrelated formatting drift).
run("prettier-check-gdi", "npx", [
  "prettier",
  "--config",
  "config/gdi-quality/.prettierrc.json",
  "--ignore-path",
  "config/gdi-quality/.prettierignore",
  "--check",
  "lib/group-demand-intelligence/customer-presentation-sanitize-v1.js",
  "scripts/test-gdi-customer-presentation-sanitize-v2.mjs",
  "scripts/gdi-repair-provisional-withheld-narrative-v1.mjs",
  "scripts/gdi-quality-quick.mjs",
]);

if (failures.length) {
  console.error("\nFAIL gdi:quality:quick —", failures.join(", "));
  process.exit(1);
}
console.log("\nPASS gdi:quality:quick");
