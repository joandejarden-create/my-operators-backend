#!/usr/bin/env node
/**
 * Bethesda 29 historical reconstruction (zero-paid, research-stage only).
 *
 *   node scripts/gdi-bethesda-29-historical-reconstruction-v1.mjs
 *   node scripts/gdi-bethesda-29-historical-reconstruction-v1.mjs --stage
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildBethesda29HistoricalReconstruction,
  stageBethesda29HistoricalReconstruction,
} from "../lib/group-demand-intelligence/research/bethesda-29-historical-reconstruction-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const stage = process.argv.includes("--stage");

const doc = buildBethesda29HistoricalReconstruction({ root: ROOT });
const outDir = path.join(
  ROOT,
  "reports",
  "gdi-bethesda-29-historical-reconstruction-v1"
);
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(
  path.join(outDir, "00-reconstruction-summary.json"),
  `${JSON.stringify(
    {
      version: doc.version,
      generatedAt: doc.generatedAt,
      safety: doc.safety,
      counts: doc.counts,
      dispositions: doc.dispositions,
      opportunityStore: doc.opportunityStore,
      groups: doc.groups,
      perRecord: doc.opportunities.map((o) => ({
        id: o.id,
        organizationName: o.organizationName,
        researchCategory: o.researchCategory,
        historicalRecoveryDisposition: o.historicalRecoveryDisposition,
        reconstructionStatus: o.reconstructionStatus,
        bodyStatus: o.bodyStatus,
        commercialMaturity: o.commercialMaturity,
        schemaOk: o.schemaValidation?.scalingCanonical?.ok === true,
        missingResearchRequirements: o.missingResearchRequirements,
        contactClass: o.contactRecovery?.contactClass || null,
      })),
    },
    null,
    2
  )}\n`
);

let staged = null;
if (stage) {
  staged = stageBethesda29HistoricalReconstruction({
    root: ROOT,
    writeFixture: true,
    writeMemoryCheckpoints: true,
  });
  fs.writeFileSync(
    path.join(outDir, "01-stage-result.json"),
    `${JSON.stringify(
      {
        ok: staged.ok,
        stagedPath: staged.stagedPath,
        hotelStoreUntouched: staged.hotelStoreUntouched,
        opportunityStoreSha256: staged.opportunityStoreSha256,
        persistCode: staged.persist?.code,
        memoryCount: staged.memoryWrites?.length || 0,
        fixture: staged.fixtureWrite?.path || null,
      },
      null,
      2
    )}\n`
  );
}

console.log(
  JSON.stringify(
    {
      ok: true,
      staged: Boolean(stage && staged?.ok),
      counts: doc.counts,
      dispositions: doc.dispositions,
      opportunityStoreSha256: doc.opportunityStore.sha256,
      reportDir: path.relative(ROOT, outDir).replace(/\\/g, "/"),
    },
    null,
    2
  )
);
