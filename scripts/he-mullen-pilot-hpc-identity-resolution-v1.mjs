/**
 * Mullen 8-hotel HPC identity resolution — READ ONLY.
 *
 *   node scripts/he-mullen-pilot-hpc-identity-resolution-v1.mjs
 *
 * Never applies Airtable writes. Outputs reports under reports/hotel-explorer/hpc-population/.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { resolveMullenPilotIdentities } from "../lib/hotel-intelligence/hpc-population/mullen-pilot-identity-resolution-v1.js";
import {
  MULLEN_PILOT_BASE_ID_CANONICAL,
  MULLEN_PILOT_HOTELS,
} from "../lib/hotel-intelligence/hpc-population/mullen-pilot-cohort-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.join(__dirname, "../reports/hotel-explorer/hpc-population");

const baseId = (process.env.AIRTABLE_BASE_ID_ALT || "").trim();
if (!baseId) {
  console.error("Missing AIRTABLE_BASE_ID_ALT");
  process.exit(1);
}
if (baseId !== MULLEN_PILOT_BASE_ID_CANONICAL) {
  console.error(
    `Refuse: AIRTABLE_BASE_ID_ALT=${baseId} ≠ canonical ${MULLEN_PILOT_BASE_ID_CANONICAL}`
  );
  process.exit(1);
}
if (baseId === "appvtnDurnMSjINP6" || process.env.AIRTABLE_BASE_ID === baseId) {
  console.warn(
    "[warn] Primary AIRTABLE_BASE_ID points at legacy base; HPC searches use AIRTABLE_BASE_ID_ALT only."
  );
}

fs.mkdirSync(OUT_DIR, { recursive: true });

console.log(
  JSON.stringify(
    {
      phase: "PHASE_1_IDENTITY_RESOLUTION",
      hotels: MULLEN_PILOT_HOTELS.length,
      baseId,
      applyAuthorized: false,
    },
    null,
    2
  )
);

const report = await resolveMullenPilotIdentities();

const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
const jsonPath = path.join(OUT_DIR, `HE-MULLEN-PILOT-IDENTITY-RESOLUTION-V1.json`);
const mdPath = path.join(OUT_DIR, `HE-MULLEN-PILOT-IDENTITY-RESOLUTION-V1.md`);
const stampedJson = path.join(OUT_DIR, `HE-MULLEN-PILOT-IDENTITY-RESOLUTION-V1-${stamp}.json`);

fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
fs.writeFileSync(stampedJson, `${JSON.stringify(report, null, 2)}\n`);

const rows = report.results
  .map(
    (r) =>
      `| ${r.hotelName} | ${r.identityOutcome} | ${r.hpcRecordId || "—"} | ${
        r.matchScore ?? "—"
      } | ${r.founderPreviewUrl ? "yes" : "no"} |`
  )
  .join("\n");

const md = `# Mullen Pilot — HPC Identity Resolution (v1)

**Status:** READ-ONLY · \`applyAuthorized: false\` · Airtable writes: **0**  
**Generated:** ${report.completedAt}  
**Base:** \`${report.canonicalBaseId}\` · HPC \`${report.hpcTableId}\`

## Summary

| Outcome | Count |
| --- | ---: |
| MATCHED_HPC | ${report.summary.MATCHED_HPC} |
| MATCHED_EXISTING_ALIAS | ${report.summary.MATCHED_EXISTING_ALIAS} |
| MATCHED_HOTEL_CENSUS | ${report.summary.MATCHED_HOTEL_CENSUS} |
| NEW_HPC_CANDIDATE | ${report.summary.NEW_HPC_CANDIDATE} |
| AMBIGUOUS_MATCH | ${report.summary.AMBIGUOUS_MATCH} |
| IDENTITY_UNRESOLVED | ${report.summary.IDENTITY_UNRESOLVED} |

## Per-hotel results

| Hotel | Identity outcome | HPC ID | Score | Preview link |
| --- | --- | --- | --- | --- |
${rows}

## Gate

${report.nextPhaseGate}

Machine report: \`${path.basename(jsonPath)}\`
`;

fs.writeFileSync(mdPath, md);

console.log(JSON.stringify({ summary: report.summary, jsonPath, mdPath }, null, 2));
for (const r of report.results) {
  console.log(
    `- ${r.hotelName}: ${r.identityOutcome} hpc=${r.hpcRecordId || "—"} score=${r.matchScore ?? "—"}`
  );
}
