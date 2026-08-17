#!/usr/bin/env node
/**
 * Review excluded Peru Lima / Cusco demand anchors after Google verification.
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
    if (/beach|corridor|zone|waterfront|growth|valley|district/i.test(name + pointType)) {
      return "manually verify broad zone using official/public source; add Lima or Cusco, Peru to query";
    }
    return "fix official name; add city/region context; retry alternate query; or remove weak/minor record";
  }
  if (status === "Ambiguous Match") {
    return "use official project name; fix candidate name; pin alternate query";
  }
  if (confidence === "Medium") {
    return "manually verify official source; allow-medium only after review";
  }
  if (/business|restaurant|shop|store|hotel/i.test(row.googleName || "")) {
    return "district/corridor incorrectly matched to specific business — fix name or use manual verification";
  }
  if (notes.includes("cityCountryMatch=no")) {
    return "add Lima, Peru or Cusco, Peru to candidate city/query";
  }
  if (/beach|corridor|growth|waterfront|valley|heritage/i.test(name + pointType)) {
    return "manually verify broad zone using official tourism/heritage source";
  }
  return "fix name; verify official source; remove if minor";
}

const reportPath =
  getArg("--report") ||
  "fixtures/demand-anchors-peru-lima-cusco-google-verification-report.json";

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

const summaryPath = "fixtures/demand-anchors-peru-lima-cusco-google-review-pass-summary.json";
writeFileSync(join(root, summaryPath), JSON.stringify(summary, null, 2) + "\n");
writeFileSync(
  join(root, "public/fixtures/demand-anchors-peru-lima-cusco-google-review-pass-summary.json"),
  JSON.stringify(summary, null, 2) + "\n"
);

console.log("Excluded:", excludedDetail.length, "/", rows.length);
for (const row of excludedDetail) {
  console.log(
    `- ${row.candidateName} (${row.submarket || row.candidateCity}) → ${row.googleName || "(no match)"} ` +
      `[${row.verificationStatus}/${row.matchConfidence}] → ${row.recommendedAction}`
  );
}
