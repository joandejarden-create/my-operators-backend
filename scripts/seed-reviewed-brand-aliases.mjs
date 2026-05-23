/**
 * Upsert human-approved alias proposals into Brand Alias Mapping only.
 *
 * Usage:
 *   node scripts/seed-reviewed-brand-aliases.mjs
 *   node scripts/seed-reviewed-brand-aliases.mjs --dry-run
 *   node scripts/seed-reviewed-brand-aliases.mjs --file=reports/census-backed-brand-alias-proposals-reviewed.json
 *
 * Input JSON: rows with Approved: true are upserted.
 *   Active: false deactivates existing row (does not delete).
 *   Approved: false rows are skipped.
 *
 * Does not modify Hotel Census, Radar, or Brand Footprint.
 */
import "../load-env.js";
import { readFileSync, existsSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { writeFileSync, mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { upsertAliasRowsFromFixture } from "./lib/brand-alias-seed.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DEFAULT_PATHS = [
  join(__dirname, "..", "reports", "all-brand-alias-proposals-reviewed.json"),
  join(__dirname, "..", "reports", "census-backed-brand-alias-proposals-reviewed.json"),
  join(__dirname, "..", "reports", "proposed-brand-aliases-reviewed.json"),
];

function resolveReviewedPath(argv) {
  const arg = argv.find((a) => a.startsWith("--file="));
  if (arg) {
    const p = arg.slice("--file=".length);
    return p.startsWith("/") || /^[A-Za-z]:/.test(p) ? p : join(__dirname, "..", p);
  }
  for (const p of DEFAULT_PATHS) {
    if (existsSync(p)) return p;
  }
  return DEFAULT_PATHS[0];
}

function stripReviewMetadata(row) {
  const out = { ...row };
  const drop = [
    "Approved",
    "approved",
    "_qa",
    "Proposal Reason",
    "Requires Human Review",
    "censusEvidence",
    "existingAliasStatus",
    "_example",
  ];
  for (const k of drop) {
    delete out[k];
  }
  return out;
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const reviewedPath = resolveReviewedPath(process.argv.slice(2));

  let reviewed;
  try {
    reviewed = JSON.parse(readFileSync(reviewedPath, "utf8"));
  } catch {
    throw new Error(
      `Missing or invalid ${reviewedPath}. Copy proposals JSON, set Approved: true on rows to seed.`
    );
  }

  const allRows = reviewed.rows || [];
  const approved = allRows.filter((r) => r.Approved === true || r.approved === true);
  const skippedNotApproved = allRows.length - approved.length;

  if (!approved.length) {
    console.log(
      JSON.stringify({
        created: 0,
        updated: 0,
        deactivated: 0,
        skipped: 0,
        skippedNotApproved,
        message: "No Approved rows",
        reviewedFile: reviewedPath,
      })
    );
    return;
  }

  const forSeed = approved.map(stripReviewMetadata);

  const tmpDir = mkdtempSync(join(tmpdir(), "alias-reviewed-"));
  const tmpPath = join(tmpDir, "fixture.json");
  writeFileSync(tmpPath, JSON.stringify({ rows: forSeed }, null, 2), "utf8");

  try {
    const stats = await upsertAliasRowsFromFixture({
      fixturePath: tmpPath,
      dryRun,
    });
    console.log(
      JSON.stringify(
        {
          ...stats,
          approvedInputRows: approved.length,
          skippedNotApproved,
          reviewedFile: reviewedPath.replace(/.*[\\/]reports[\\/]/, "reports/"),
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
