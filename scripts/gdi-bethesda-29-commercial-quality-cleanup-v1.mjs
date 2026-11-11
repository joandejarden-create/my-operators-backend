#!/usr/bin/env node
/**
 * Bethesda 29 commercial quality cleanup + category restoration.
 * Research-stage only. No opportunities.json writes. No paid research.
 *
 * Usage:
 *   node scripts/gdi-bethesda-29-commercial-quality-cleanup-v1.mjs
 *   node scripts/gdi-bethesda-29-commercial-quality-cleanup-v1.mjs --dry-run
 */
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildBethesda29CommercialQualityCleanup,
  stageBethesda29CommercialQualityCleanup,
} from "../lib/group-demand-intelligence/research/bethesda-29-commercial-quality-cleanup-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const dryRun = process.argv.includes("--dry-run");

if (dryRun) {
  const doc = buildBethesda29CommercialQualityCleanup({ root: ROOT });
  console.log(
    JSON.stringify(
      {
        ok: doc.counts.otherUnclassifiedAfter === 0,
        mode: "dry-run",
        counts: doc.counts,
        beforeFilterDistribution: doc.beforeFilterDistribution,
        afterFilterDistribution: doc.afterFilterDistribution,
        proposedPaidResearchAuthorized: doc.proposedPaidResearch.authorized,
        storeUntouched: doc.opportunityStore.untouched,
      },
      null,
      2
    )
  );
  process.exit(doc.counts.otherUnclassifiedAfter === 0 ? 0 : 1);
}

const result = stageBethesda29CommercialQualityCleanup({
  root: ROOT,
  writeFixture: true,
  writeReports: true,
});

console.log(
  JSON.stringify(
    {
      ok: result.ok,
      mode: "stage",
      counts: result.doc.counts,
      beforeFilterDistribution: result.doc.beforeFilterDistribution,
      afterFilterDistribution: result.doc.afterFilterDistribution,
      persistOk: result.persist?.ok === true,
      storeUntouched: result.doc.opportunityStore.untouched,
      stagedPath: result.stagedPath,
      reports: "reports/gdi-bethesda-29-commercial-quality-cleanup-v1/",
    },
    null,
    2
  )
);
process.exit(result.ok ? 0 : 1);
