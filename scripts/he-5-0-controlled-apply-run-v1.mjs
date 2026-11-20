#!/usr/bin/env node
/**
 * HE 5.0 controlled apply under standing founder authorization.
 *
 * Default: dry-run classification + live revalidation (no writes).
 * Apply:   node scripts/he-5-0-controlled-apply-run-v1.mjs --apply
 *
 * Does not write legacy Hotel Census. Does not deploy. Does not change schema.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import { runHe50ControlledApply } from "../lib/hotel-intelligence/he-5-0-full-e2e/controlled-apply-executor-v1.js";
import { HE50_GLOBAL_USD_CAP } from "../lib/hotel-intelligence/he-5-0-full-e2e/contract.js";

const require = createRequire(import.meta.url);
try {
  require("dotenv").config({ path: "C:/Dev/dealality/my-operators-backend/.env" });
} catch {
  /* optional */
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const outDir = path.join(root, "reports/hotel-explorer/he-5-0-pilot");
const apply = process.argv.includes("--apply");

const ledgerPath = path.join(outDir, "HE50-RESEARCH-SPEND-LEDGER.json");
let budgetSnap = {
  confirmedActualUsd: 0,
  reservedUnreconciledUsd: 0,
  remainingAuthorizedUsd: HE50_GLOBAL_USD_CAP,
};
if (fs.existsSync(ledgerPath)) {
  try {
    const raw = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
    budgetSnap = {
      confirmedActualUsd: Number(raw.confirmedActualUsd) || 0,
      reservedUnreconciledUsd: Number(raw.reservedUnreconciledUsd) || 0,
      remainingAuthorizedUsd:
        Number(raw.remainingAuthorizedUsd) ??
        HE50_GLOBAL_USD_CAP - (Number(raw.confirmedActualUsd) || 0),
      globalUsdCap: raw.globalUsdCap || HE50_GLOBAL_USD_CAP,
    };
  } catch {
    /* keep defaults */
  }
}

console.log(
  JSON.stringify(
    {
      mode: apply ? "APPLY" : "DRY_RUN",
      budget: {
        confirmedActualUsd: budgetSnap.confirmedActualUsd,
        reservedUnreconciledUsd: budgetSnap.reservedUnreconciledUsd,
        remainingAuthorizedUsd: budgetSnap.remainingAuthorizedUsd,
        note: "Ledger not reset — free-first; paid remaining of $50 unused",
      },
    },
    null,
    2
  )
);

if (budgetSnap.confirmedActualUsd > HE50_GLOBAL_USD_CAP) {
  console.error("BLOCKED — paid spend already exceeds $50 ceiling");
  process.exit(2);
}

const report = await runHe50ControlledApply({ apply, outDir });

const md = `# HE 5.0 Controlled Apply — Standing Authorization

**Mode:** ${apply ? "APPLY" : "DRY_RUN"}  
**Status:** ${report.reportStatus}  
**Completed:** ${report.completedAt}

## Budget (not reset)

| Metric | USD |
| --- | ---: |
| Cap | ${HE50_GLOBAL_USD_CAP.toFixed(2)} |
| Confirmed paid | ${Number(budgetSnap.confirmedActualUsd).toFixed(2)} |
| Reserved | ${Number(budgetSnap.reservedUnreconciledUsd).toFixed(2)} |
| Remaining | ${Number(budgetSnap.remainingAuthorizedUsd).toFixed(2)} |

## Mutation outcomes

| Mutation | Class | Applied | Notes |
| --- | --- | --- | --- |
${report.results
  .map((r) => {
    const note =
      r.quarantineReason ||
      r.opResults
        ?.map((o) => o.reason || o.error || (o.recordId ? `id=${o.recordId}` : ""))
        .filter(Boolean)
        .join("; ") ||
      "";
    return `| ${r.mutationId} | ${r.approvalClass} | ${r.applied ? "YES" : "no"} | ${note} |`;
  })
  .join("\n")}

## Counts

- Applied: **${report.counts.applied}**
- Quarantined / blocked / failed: **${report.counts.quarantined}**
- HPC created: **${report.counts.createdHpc}**
- HPC updated: **${report.counts.updatedHpc}**
- Evidence created: **${report.counts.evidenceCreated}**
- Legacy Hotel Census access: **${report.legacyHotelCensusAccess}**

## Persistence stages

Distinguish RESEARCH_VERIFIED / CANONICAL_PERSISTED / AIRTABLE_READBACK_VALIDATED / HOTEL_EXPLORER_UI_VALIDATED in JSON opResults.

Browser UI validation remains a follow-on check after apply.
`;

fs.writeFileSync(
  path.join(
    outDir,
    apply ? "HE50-CONTROLLED-APPLY-RESULT-V1.md" : "HE50-CONTROLLED-APPLY-DRY-RUN-V1.md"
  ),
  md
);

console.log(JSON.stringify({ reportStatus: report.reportStatus, counts: report.counts }, null, 2));
if (apply && report.counts.applied === 0 && report.counts.quarantined > 0) {
  process.exitCode = 0; // partial is still success for the engine
}
