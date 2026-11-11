/**
 * Dry-run inventory of tracked operational paths.
 * Refuses to move or untrack data.
 */
import { execFileSync } from "node:child_process";
import {
  MIGRATION_PLAN_V1,
  classifyTrackedPath,
  isOperationalClass,
} from "../lib/dealality/operational-data-boundary-v1.js";

if (process.argv.includes("--apply")) {
  console.error(JSON.stringify({
    ok: false,
    error: "migration_not_approved",
    plan: MIGRATION_PLAN_V1.id,
  }));
  process.exit(2);
}

const files = execFileSync("git", ["ls-files"], { encoding: "utf8" })
  .split(/\r?\n/)
  .filter(Boolean);
const counts = {};
for (const file of files) {
  const kind = classifyTrackedPath(file);
  counts[kind] = (counts[kind] || 0) + 1;
}
console.log(JSON.stringify({
  ok: true,
  mode: "dry-run",
  plan: MIGRATION_PLAN_V1.status,
  operationalTracked: Object.entries(counts)
    .filter(([kind]) => isOperationalClass(kind))
    .reduce((sum, [, n]) => sum + n, 0),
  counts,
}, null, 2));
