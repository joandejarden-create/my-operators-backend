#!/usr/bin/env node
/**
 * Bethesda live Full E2E + Airtable validation planner (fail-closed).
 * Generates report pack. Never launches paid Exa or production Airtable writes.
 */
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runOfflineValidationSuite,
  writeValidationReportPack,
  GDI_BETHESDA_LIVE_FULL_E2E_AIRTABLE_VALIDATION_V1,
} from "../lib/group-demand-intelligence/workflow/gdi-bethesda-live-full-e2e-airtable-validation-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-bethesda-live-full-e2e-airtable-validation-v1",
);

const suite = await runOfflineValidationSuite({ repoRoot: ROOT });
writeValidationReportPack(OUT, suite);

const ok =
  suite.persistenceAuth.paidDoesNotUnlockWrites === true &&
  suite.idempotency.repeatCreatesZero === true &&
  suite.weeklyChange.descriptionRewriteNonMaterial === true &&
  suite.sharedIntelligenceReuse?.ok === true &&
  suite.paidResearchExecuted === false &&
  suite.productionAirtableWrites === false;

process.stdout.write(
  `${JSON.stringify(
    {
      ok,
      version: GDI_BETHESDA_LIVE_FULL_E2E_AIRTABLE_VALIDATION_V1,
      outDir: OUT,
      paidResearchExecuted: suite.paidResearchExecuted,
      productionAirtableWrites: suite.productionAirtableWrites,
      stop: suite.stop,
    },
    null,
    2,
  )}\n`,
);

process.exit(ok ? 0 : 1);
