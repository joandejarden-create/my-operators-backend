#!/usr/bin/env node
/**
 * Review excluded Mexico Cancún demand anchors after Google verification.
 */
import "../load-env.js";
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

function getArg(name, fallback = "") {
  const idx = process.argv.indexOf(name);
  return idx >= 0 ? process.argv[idx + 1] : fallback;
}

function recommendAction(row) {
  const status = String(row.verificationStatus || "");
  const confidence = String(row.matchConfidence || "");
  const notes = String(row.verificationNotes || "");
  const name = String(row.candidateName || "");
  const pointType = String(row.candidatePointType || "");

  if (status === "No Match") {
    if (/beach|corridor|zone|waterfront/i.test(name + pointType)) {
      return "replace broad zone with centroid and manual verification; add Quintana Roo, Mexico to query";
    }
    return "fix name; add Quintana Roo, Mexico; retry with alternate query; or remove weak/minor record";
  }
  if (status === "Ambiguous Match") {
    return "use official project name; fix candidate name; pin alternate query";
  }
  if (confidence === "Medium") {
    return "manually verify official source; allow-medium only after review";
  }
  if (/business|restaurant|shop|store/i.test(row.googleName || "")) {
    return "beach/corridor incorrectly matched to specific business — fix name or use manual verification";
  }
  if (notes.includes("cityCountryMatch=no")) {
    return "add Quintana Roo, Mexico to candidate city/query";
  }
  if (/beach|corridor|growth|waterfront/i.test(name + pointType)) {
    return "replace broad zone with centroid and manual verification using official tourism/project source";
  }
  return "fix name; verify official source; remove if minor";
}

const reportPath =
  getArg("--report") ||
  "fixtures/demand-anchors-mexico-cancun-google-verification-report.json";

let report;
try {
  report = JSON.parse(readFileSync(join(root, reportPath), "utf8"));
} catch (err) {
  console.error("Cannot read report:", err.message);
  process.exit(1);
}

const rows = report.records || report.rows || [];
const excluded = rows.filter(
  (row) => !(row.verificationStatus === "Verified" && row.matchConfidence === "High")
);

const excludedDetail = excluded.map((row) => ({
  candidateName: row.candidateName,
  candidateCity: row.candidateCity,
  submarket: row.sourceRow?.submarket || "",
  pointType: row.candidatePointType || "",
  candidateLatitude: row.candidateLatitude,
  candidateLongitude: row.candidateLongitude,
  googleName: row.googleName || "",
  googleLatitude: row.googleLatitude ?? null,
  googleLongitude: row.googleLongitude ?? null,
  verificationStatus: row.verificationStatus,
  matchConfidence: row.matchConfidence,
  verificationNotes: row.verificationNotes,
  recommendedAction: recommendAction(row),
}));

const summary = {
  generatedAt: new Date().toISOString(),
  reportPath,
  verification: report.verification || {},
  excludedCount: excludedDetail.length,
  excludedRecords: excludedDetail,
};

const summaryPath = "fixtures/demand-anchors-mexico-cancun-google-review-pass-summary.json";
writeFileSync(join(root, summaryPath), JSON.stringify(summary, null, 2) + "\n");
writeFileSync(
  join(root, "public/fixtures/demand-anchors-mexico-cancun-google-review-pass-summary.json"),
  JSON.stringify(summary, null, 2) + "\n"
);

console.log("Excluded:", excludedDetail.length, "/", rows.length);
for (const row of excludedDetail) {
  console.log(
    `- ${row.candidateName} (${row.submarket || row.candidateCity}) → ${row.googleName || "(no match)"} ` +
      `[${row.verificationStatus}/${row.matchConfidence}]`
  );
  console.log(`  Action: ${row.recommendedAction}`);
}
console.log("Summary:", summaryPath);
