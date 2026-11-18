#!/usr/bin/env node
/**
 * Validates Bethesda live 14-segment persistence pilot artifacts (no Airtable writes).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getCanonicalFourteenSegmentIds } from "../lib/group-demand-intelligence/workflow/full-e2e-fourteen-segment-traversal-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-bethesda-live-14-segment-persistence-pilot-v1",
);

const REQUIRED = [
  "FOUNDER_EXECUTIVE_SUMMARY.md",
  "FOURTEEN_SEGMENT_EXECUTION_MATRIX.md",
  "NEW_SEGMENT_DISCOVERY_FINDINGS.md",
  "REAL_DISCOVERY_EVIDENCE_REGISTER.json",
  "OPPORTUNITY_QUALITY_AND_DRAWER_REVIEW.md",
  "COMMERCIAL_QUALIFICATION_RESULTS.md",
  "BETHESDA_86_AIRTABLE_RECONCILIATION.md",
  "RESEARCH_PERSISTENCE_DRY_RUN.json",
  "WEEKLY_CHANGE_VALIDATION.md",
  "CROSS_HOTEL_REUSE_ASSESSMENT.md",
  "EXACT_PRODUCTION_PILOT_WRITE_MANIFEST.json",
  "TEST_RESULTS.md",
  "FOUNDER_NEXT_APPROVAL.md",
  "PILOT_ACCEPTANCE.json",
];

function fail(msg) {
  console.error(`FAIL: ${msg}`);
  process.exitCode = 1;
}

function main() {
  for (const name of REQUIRED) {
    const p = path.join(OUT, name);
    if (!fs.existsSync(p)) fail(`missing ${name}`);
  }
  if (process.exitCode) return;

  const acceptance = JSON.parse(
    fs.readFileSync(path.join(OUT, "PILOT_ACCEPTANCE.json"), "utf8"),
  );
  const matrix = JSON.parse(
    fs.readFileSync(path.join(OUT, "SEGMENT_REGISTRY.json"), "utf8"),
  );
  const dry = JSON.parse(
    fs.readFileSync(path.join(OUT, "RESEARCH_PERSISTENCE_DRY_RUN.json"), "utf8"),
  );
  const manifest = JSON.parse(
    fs.readFileSync(
      path.join(OUT, "EXACT_PRODUCTION_PILOT_WRITE_MANIFEST.json"),
      "utf8",
    ),
  );
  const evidence = JSON.parse(
    fs.readFileSync(
      path.join(OUT, "REAL_DISCOVERY_EVIDENCE_REGISTER.json"),
      "utf8",
    ),
  );

  const ids = getCanonicalFourteenSegmentIds();
  if (ids.length !== 14) fail(`canonical segment count ${ids.length}`);
  if ((matrix.segmentIds || []).length !== 14) fail("matrix segmentIds != 14");
  if ((matrix.matrix || []).length !== 14) fail("execution matrix != 14");
  if (!matrix.matrix.every((m) => m.executorInvoked === true)) {
    fail("not all 14 executors invoked");
  }
  if (acceptance?.acceptance?.all14Accounted !== true) fail("acceptance all14");
  if (acceptance?.acceptance?.bethesda86AirtableIntact !== true) {
    fail("86 airtable not intact");
  }
  if (dry.applied === true || dry.productionWritesAuthorized === true) {
    fail("dry-run must not authorize/apply writes");
  }
  if (
    dry.gdoWrites !== 0 ||
    dry.publicationWrites !== 0 ||
    dry.rockCreekWrites !== 0
  ) {
    fail("non-research write counters must be 0");
  }
  if (manifest.productionWritesAuthorized === true) {
    fail("manifest must remain unauthorized");
  }
  if ((manifest.plannedCreateCount || 0) > 1) fail("CREATE > 1");
  if ((manifest.plannedUpdateCount || 0) > 1) fail("UPDATE > 1");
  if ((manifest.entries || []).length > 2) fail("manifest entries > 2");
  if (evidence.paidProvidersInvoked === true) fail("paid providers invoked");
  if (evidence.exaInvoked === true || evidence.firecrawlInvoked === true) {
    fail("exa/firecrawl invoked");
  }
  if (acceptance?.productionWrites !== 0) fail("productionWrites != 0");

  if (process.exitCode) return;
  console.log(
    JSON.stringify(
      {
        ok: true,
        requiredArtifacts: REQUIRED.length,
        segments: 14,
        plannedWrites: (manifest.entries || []).length,
        productionWrites: 0,
      },
      null,
      2,
    ),
  );
}

main();
