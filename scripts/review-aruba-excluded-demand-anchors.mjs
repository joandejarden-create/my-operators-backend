#!/usr/bin/env node
/**
 * Review excluded Aruba demand anchors after Google verification.
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
    if (/beach|corridor|district|waterfront|growth|zone/i.test(name + pointType)) {
      return "manually verify broad beach/resort zone using official/public source; add Aruba district to query";
    }
    return "fix official name; add Aruba context; retry alternate query; or remove weak/minor record";
  }
  if (status === "Ambiguous Match") {
    return "use official project name; fix candidate name; pin alternate query";
  }
  if (confidence === "Medium") {
    return "manually verify official source; allow-medium only after review";
  }
  if (/business|restaurant|shop|store|hotel/i.test(row.googleName || "")) {
    return "beach/corridor incorrectly matched to specific business — fix name or use manual verification";
  }
  if (notes.includes("cityCountryMatch=no")) {
    return "add Oranjestad, Aruba or district to candidate city/query";
  }
  if (/beach|corridor|growth|waterfront|resort/i.test(name + pointType)) {
    return "manually verify broad beach/resort zone using official tourism source";
  }
  return "fix name; verify official source; remove if minor";
}

const reportPath =
  getArg("--report") ||
  "fixtures/demand-anchors-aruba-google-verification-report.json";

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

const out = {
  reviewedAt: new Date().toISOString(),
  country: "Aruba",
  market: "Aruba Countrywide",
  reportPath,
  summary: {
    totalCandidates: rows.length,
    verifiedHigh: rows.filter(
      (r) => r.verificationStatus === "Verified" && r.matchConfidence === "High"
    ).length,
    excluded: excluded.length,
  },
  excluded: excludedDetail,
};

const outPath =
  getArg("--output") || "fixtures/demand-anchors-aruba-google-review-pass-summary.json";
writeFileSync(join(root, outPath), JSON.stringify(out, null, 2) + "\n");

console.log("Aruba excluded review:", out.summary);
console.log("Written:", outPath);
if (excludedDetail.length) {
  console.log("\nExcluded candidates:");
  for (const row of excludedDetail) {
    console.log(`- ${row.candidateName} (${row.verificationStatus}/${row.matchConfidence}): ${row.recommendedAction}`);
  }
}
