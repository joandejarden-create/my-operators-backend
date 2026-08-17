#!/usr/bin/env node
/**
 * Review excluded Colombia demand anchor rows after Google verification.
 *   node scripts/review-colombia-excluded-demand-anchors.mjs
 *   node scripts/review-colombia-excluded-demand-anchors.mjs --report fixtures/demand-anchors-colombia-google-verification-report.json
 *   node scripts/review-colombia-excluded-demand-anchors.mjs --rebuild --verify
 */
import "../load-env.js";
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { spawnSync } from "child_process";
import { COLOMBIA_GOOGLE_PLACE_REVIEW_CORRECTIONS } from "../lib/radar-buildout/colombia-google-place-review-corrections.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const args = process.argv.slice(2);

function getArg(name, fallback = "") {
  const idx = args.indexOf(name);
  return idx >= 0 ? args[idx + 1] : fallback;
}

function run(cmd, cmdArgs) {
  const res = spawnSync(cmd, cmdArgs, { cwd: root, stdio: "inherit", shell: true });
  if (res.status !== 0) process.exit(res.status || 1);
}

function recommendAction(row) {
  const status = String(row.verificationStatus || "");
  const confidence = String(row.matchConfidence || "");
  const notes = String(row.verificationNotes || "");
  if (status === "No Match") {
    return "fix candidate name or city; retry with alternate query; or remove as weak/minor";
  }
  if (status === "Ambiguous Match") {
    return "fix candidate name; pin alternate query; or manually verify official source";
  }
  if (confidence === "Medium") {
    return "manually verify official source; allow-medium only after review";
  }
  if (confidence === "Low" || notes.includes("cityCountryMatch=no")) {
    return "fix candidate city or coordinates; verify official source";
  }
  if (notes.includes("coordCorrection=yes")) {
    return "manually verify official source; candidate name likely correct";
  }
  return "review candidate name/city and retry verification";
}

const reportPath =
  getArg("--report") || "fixtures/demand-anchors-colombia-google-verification-report.json";

if (args.includes("--rebuild")) {
  run("npm", ["run", "build:colombia-fixtures"]);
}
if (args.includes("--verify")) {
  run("npm", ["run", "verify:colombia-google"]);
}

console.log("=== Colombia excluded-row review ===");
console.log("Report:", reportPath);

let report;
try {
  report = JSON.parse(readFileSync(join(root, reportPath), "utf8"));
} catch (err) {
  console.error("Cannot read verification report:", err.message);
  process.exit(1);
}

const reportRows = report.records || report.rows || [];
const byStatus = {};
for (const row of reportRows) {
  const key = `${row.verificationStatus}/${row.matchConfidence}`;
  byStatus[key] = (byStatus[key] || 0) + 1;
}

const stillExcluded = reportRows.filter(
  (row) => !(row.verificationStatus === "Verified" && row.matchConfidence === "High")
);

const excludedDetail = stillExcluded.map((row) => ({
  candidateName: row.candidateName,
  candidateCity: row.candidateCity,
  googleName: row.googleName || "",
  verificationStatus: row.verificationStatus,
  matchConfidence: row.matchConfidence,
  verificationNotes: row.verificationNotes,
  googleMapsUri: row.googleMapsUri || "",
  recommendedAction: recommendAction(row),
}));

const summary = {
  generatedAt: new Date().toISOString(),
  reportPath,
  correctionsAvailable: Object.keys(COLOMBIA_GOOGLE_PLACE_REVIEW_CORRECTIONS).length,
  verification: report.verification || {
    candidateCount: report.totalCandidates,
    verifiedHighConfidence: report.verifiedRecords,
    excludedRecords: report.excludedRecords,
    cacheHits: report.cacheHits,
    cacheMisses: report.cacheMisses,
    apiRequestsMade: report.apiRequestsMade,
  },
  byStatus,
  excludedCount: excludedDetail.length,
  excludedRecords: excludedDetail,
};

const summaryPath = "fixtures/demand-anchors-colombia-google-review-pass-summary.json";
writeFileSync(join(root, summaryPath), JSON.stringify(summary, null, 2) + "\n");
writeFileSync(
  join(root, "public/fixtures/demand-anchors-colombia-google-review-pass-summary.json"),
  JSON.stringify(summary, null, 2) + "\n"
);

const v = summary.verification;
console.log("\nVerification summary:");
console.log("Candidates:", v.candidateCount);
console.log("High confidence:", v.verifiedHighConfidence);
console.log("Excluded:", v.excludedRecords ?? excludedDetail.length);
console.log("Cache hits/misses/API:", v.cacheHits, "/", v.cacheMisses, "/", v.apiRequestsMade);
console.log("By status:", byStatus);
console.log("Summary written:", summaryPath);

if (excludedDetail.length) {
  console.log("\nExcluded records:");
  for (const row of excludedDetail) {
    console.log(
      `- ${row.candidateName} (${row.candidateCity}) → ${row.googleName || "(no match)"} ` +
        `[${row.verificationStatus}/${row.matchConfidence}]`
    );
    console.log(`  Action: ${row.recommendedAction}`);
    if (row.verificationNotes) console.log(`  Notes: ${row.verificationNotes}`);
  }
}
