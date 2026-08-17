/**
 * Validate master to-do seed data (no Airtable calls).
 *
 *   node scripts/test-dealality-master-todo-seed.mjs
 */
import {
  MASTER_TODO_SEED,
  validateMasterTodoSeed,
} from "../lib/dealality-master-todo/master-todo-seed.js";
import { spawnSync } from "child_process";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function main() {
  const errors = validateMasterTodoSeed();
  if (errors.length) {
    console.error("Seed validation failed:");
    for (const e of errors) console.error(`  - ${e}`);
    process.exit(1);
  }
  console.log(`OK: ${MASTER_TODO_SEED.length} seed tasks validated.`);

  const dryRun = spawnSync(
    process.execPath,
    ["scripts/upsert-dealality-master-todo.mjs", "--dry-run"],
    { cwd: ROOT, encoding: "utf8" }
  );
  if (dryRun.status !== 0) {
    console.error("Upsert dry-run failed:");
    console.error(dryRun.stdout);
    console.error(dryRun.stderr);
    process.exit(dryRun.status || 1);
  }
  console.log("OK: upsert dry-run completed without Airtable writes.");
  console.log(dryRun.stdout.trim());
}

main();
