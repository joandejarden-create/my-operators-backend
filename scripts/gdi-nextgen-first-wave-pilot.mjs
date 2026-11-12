#!/usr/bin/env node
/**
 * Zero-paid four-hotel first-wave pilot.
 * No Apify/Exa live spend. Cache/fixture only.
 */

import path from "node:path";
import fs from "node:fs";
import { fileURLToPath } from "node:url";
import {
  runFirstWavePilot,
  NEXTGEN_PILOT_HOTELS,
} from "../lib/group-demand-intelligence/nextgen-discovery/hunter-orchestrator-v1.js";
import { buildCapabilityGapMatrixV1 } from "../lib/group-demand-intelligence/nextgen-discovery/capability-gap-matrix-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORT_ROOT = path.join(
  ROOT,
  "reports/gdi-nextgen-discovery-architecture-v1/pilot"
);

async function main() {
  fs.mkdirSync(REPORT_ROOT, { recursive: true });
  const gap = buildCapabilityGapMatrixV1();
  fs.writeFileSync(
    path.join(
      ROOT,
      "reports/gdi-nextgen-discovery-architecture-v1/capability-gap-matrix.json"
    ),
    JSON.stringify(gap, null, 2),
    "utf8"
  );

  const summary = await runFirstWavePilot({
    hotels: NEXTGEN_PILOT_HOTELS,
    reportRoot: REPORT_ROOT,
    rootDir: ROOT,
  });

  console.log(
    JSON.stringify(
      {
        ok: true,
        hotels: summary.hotels.map((h) => ({
          hotelId: h.hotelId,
          hotelName: h.hotelName,
          hunters: h.selection.selectedHunterIds,
          totals: h.totals,
        })),
        portfolioTotals: summary.portfolioTotals,
        budgetUsdSpent: summary.budgetUsdSpent,
        reportRoot: REPORT_ROOT,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
