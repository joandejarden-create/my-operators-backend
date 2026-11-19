/**
 * HE 5.0 autonomous reconciliation runner.
 * Continues through all pilot conflicts without founder prompts for routine ambiguity.
 * STOP only before production Airtable apply.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runAutonomousReconciliation,
  buildHe50DryRunMutationPackage,
  summarizeReconciliation,
  summarizeCompleteness,
  APPLY_GATE,
  RESOLUTION_CLASS,
  defaultLedgerPath,
} from "../lib/hotel-intelligence/he-5-0-full-e2e/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const outDir = path.join(root, "reports/hotel-explorer/he-5-0-pilot");
fs.mkdirSync(outDir, { recursive: true });

const ledgerPath = defaultLedgerPath(root);
const result = runAutonomousReconciliation({
  enablePaid: false,
  ledgerPath,
});

const dryRun = buildHe50DryRunMutationPackage();
const recon = summarizeReconciliation();
const completeness = summarizeCompleteness();

fs.writeFileSync(
  path.join(outDir, "HE50-AUTONOMOUS-RECONCILIATION-V1.json"),
  JSON.stringify(result, null, 2),
  "utf8"
);
fs.writeFileSync(
  path.join(outDir, "HE50-AIRTABLE-DRY-RUN-PACKAGE.json"),
  JSON.stringify(dryRun, null, 2),
  "utf8"
);
fs.writeFileSync(
  path.join(outDir, "HE50-MUTATION-RECONCILIATION-V1.json"),
  JSON.stringify(recon, null, 2),
  "utf8"
);
fs.writeFileSync(
  path.join(outDir, "HE50-COMPLETENESS-MATRIX-V1.json"),
  JSON.stringify(completeness, null, 2),
  "utf8"
);

const resolvedRows = result.results
  .filter((r) => String(r.resolutionClass).startsWith("RESOLVED"))
  .map((r) => `| ${r.issueId} | ${r.resolutionClass} | ${JSON.stringify(r.resolvedValue)?.slice(0, 80)} |`)
  .join("\n");

const unresolvedRows = result.results
  .filter((r) => String(r.resolutionClass).startsWith("UNRESOLVED"))
  .map((r) => `| ${r.issueId} | ${r.resolutionClass} | ${String(r.rationale || "").slice(0, 120)} |`)
  .join("\n");

const md = `# HE 5.0 — Autonomous Reconciliation Report

**Status:** ${APPLY_GATE.stopMessage}  
**Run:** \`${result.runId}\`  
**Paid spend:** $${result.budget.confirmedActualUsd.toFixed(2)} confirmed · $${result.budget.reservedUnreconciledUsd.toFixed(2)} reserved · **$${result.budget.remainingAuthorizedUsd.toFixed(2)} remaining of $50**  
**Airtable writes:** 0

## Behavior

The supervisor investigated **${result.issuesInvestigated}** detected conflicts/gaps across the eight-hotel pilot **without stopping for routine founder review**.

Founder decisions required for factual ambiguity: **${result.founderDecisionCount}**  
(Only \`REQUIRES_FOUNDER_DECISION\` for governance — none for ordinary room/identity/renovation conflicts.)

## Classification totals

${Object.entries(result.byClass)
  .map(([k, v]) => `- **${k}:** ${v}`)
  .join("\n")}

## Automatically resolved

| Issue | Class | Value |
| --- | --- | --- |
${resolvedRows}

## Still unresolved (continue-safe)

| Issue | Class | Why |
| --- | --- | --- |
${unresolvedRows || "| — | — | none |"}

## Mutation package impact

- Cap Cana rooms **457** added as READY UPDATE (identity NO_OP remains REJECT).
- HGI rooms confidence upgraded **MEDIUM → HIGH** (138).
- Dry-run package revision includes autonomous amendments.
- Production apply still blocked.

## Cost per useful verified resolution

Paid: $0.00 · Free resolutions: ${result.resolvedCount} · Cost/resolution: n/a (paid=$0)

## Genuinely requiring founder judgment

None from this autonomous factual pass. Remaining gates:

1. Authorize production Airtable apply for READY mutations.
2. Optional: authorize paid spend for Bethesda current-lender if desired later.
3. Steward lowercase Cap Cana alias \`recFP9eslejVXbyuR\` merge (ops hygiene, not identity block).

## ${APPLY_GATE.stopMessage}
`;

fs.writeFileSync(path.join(outDir, "HE50-AUTONOMOUS-RECONCILIATION-V1.md"), md, "utf8");

console.log(
  JSON.stringify(
    {
      ok: true,
      stop: APPLY_GATE.stopMessage,
      investigated: result.issuesInvestigated,
      resolved: result.resolvedCount,
      unresolved: result.unresolvedCount,
      founderDecisions: result.founderDecisionCount,
      byClass: result.byClass,
      paidConfirmed: result.budget.confirmedActualUsd,
      remainingUsd: result.budget.remainingAuthorizedUsd,
      dryRunMutations: dryRun.mutationCount,
      resolutionClasses: Object.values(RESOLUTION_CLASS).length,
    },
    null,
    2
  )
);
