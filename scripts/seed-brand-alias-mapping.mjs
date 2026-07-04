/**
 * Idempotent seed for Brand Alias Mapping (Platform base only).
 *
 * Upsert key: Canonical Brand Name + Alias / Source Brand Name + Parent Company (trim-exact).
 * Rows with Active: false update existing rows to inactive (records are not deleted).
 *
 * Usage:
 *   node scripts/seed-brand-alias-mapping.mjs
 *   node scripts/seed-brand-alias-mapping.mjs --dry-run
 *   node scripts/seed-brand-alias-mapping.mjs --fixture=fixtures/brand-alias-mapping-choice-radisson-seed.json
 *   node scripts/seed-brand-alias-mapping.mjs --fixture=fixtures/brand-alias-mapping-phase1-seed.json --fixture=fixtures/brand-alias-mapping-choice-radisson-seed.json
 *
 * Env: AIRTABLE_API_KEY (data.records:write), AIRTABLE_BASE_ID_ALT
 */
import "../load-env.js";
import { readFileSync, writeFileSync, mkdtempSync, rmSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { tmpdir } from "os";
import { upsertAliasRowsFromFixture } from "./lib/brand-alias-seed.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DEFAULT_FIXTURE = join(__dirname, "..", "fixtures", "brand-alias-mapping-phase1-seed.json");

function parseFixtureArgs(argv) {
  const paths = argv
    .filter((a) => a.startsWith("--fixture="))
    .map((a) => a.slice("--fixture=".length));
  if (argv.includes("--choice-radisson")) {
    paths.push("fixtures/brand-alias-mapping-choice-radisson-seed.json");
  }
  if (!paths.length) paths.push(DEFAULT_FIXTURE);
  return paths.map((p) => (p.startsWith("/") || /^[A-Za-z]:/.test(p) ? p : join(__dirname, "..", p)));
}

function mergeFixtures(paths) {
  const allRows = [];
  for (const p of paths) {
    const fixture = JSON.parse(readFileSync(p, "utf8"));
    for (const row of fixture.rows || []) {
      allRows.push(row);
    }
  }
  return allRows;
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const fixturePaths = parseFixtureArgs(process.argv.slice(2));
  const rows = mergeFixtures(fixturePaths);

  const tmpDir = mkdtempSync(join(tmpdir(), "alias-seed-"));
  const tmpPath = join(tmpDir, "merged.json");
  writeFileSync(tmpPath, JSON.stringify({ rows }, null, 2), "utf8");

  try {
    const stats = await upsertAliasRowsFromFixture({ fixturePath: tmpPath, dryRun });
    console.log(
      JSON.stringify(
        {
          ...stats,
          fixtures: fixturePaths.map((p) => p.replace(/.*[\\/]fixtures[\\/]/, "fixtures/")),
        },
        null,
        2
      )
    );
  } finally {
    try {
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {
      /* ignore */
    }
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
