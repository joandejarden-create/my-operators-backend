#!/usr/bin/env node
/**
 * Bethesda 29 targeted commercial research — zero-paid enrichment only.
 * Stops before any paid provider calls.
 *
 *   node scripts/gdi-bethesda-29-targeted-commercial-research-v1.mjs
 *   node scripts/gdi-bethesda-29-targeted-commercial-research-v1.mjs --dry-run
 */
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildBethesda29TargetedCommercialResearch,
  stageBethesda29TargetedCommercialResearch,
  PROPOSED_PAID_BATCH_V1,
} from "../lib/group-demand-intelligence/research/bethesda-29-targeted-commercial-research-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const dryRun = process.argv.includes("--dry-run");

if (dryRun) {
  const doc = buildBethesda29TargetedCommercialResearch({ root: ROOT });
  console.log(
    JSON.stringify(
      {
        ok: true,
        mode: "dry-run",
        counts: doc.counts,
        scorecards: doc.scorecards,
        proposedPaidBatch: doc.proposedPaidBatch,
        storeUntouched: doc.opportunityStore.untouched,
        stop: "AWAITING_FOUNDER_PAID_APPROVAL",
      },
      null,
      2
    )
  );
  process.exit(0);
}

const result = stageBethesda29TargetedCommercialResearch({
  root: ROOT,
  writeFixture: true,
  writeReports: true,
});

console.log(
  JSON.stringify(
    {
      ok: result.ok,
      mode: "stage-zero-paid",
      counts: result.doc.counts,
      scorecards: result.doc.scorecards.map((s) => ({
        id: s.id,
        org: s.organizationName,
        before: s.beforeMaturity,
        after: s.afterMaturity,
        named: s.namedContact,
        ad: s.accountDevelopment,
      })),
      proposedPaidBatch: PROPOSED_PAID_BATCH_V1,
      persistOk: result.persist?.ok === true,
      storeUntouched: result.doc.opportunityStore.untouched,
      stop: "AWAITING_FOUNDER_PAID_APPROVAL",
      reports: "reports/gdi-bethesda-29-targeted-commercial-research-v1/",
    },
    null,
    2
  )
);
process.exit(result.ok ? 0 : 1);
