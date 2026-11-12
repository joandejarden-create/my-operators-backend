#!/usr/bin/env node
/**
 * Zero-paid Hunter→Exa orchestration demo + bounded paid pilot preparation.
 *
 * Does NOT execute paid Apify/Exa calls.
 *
 * Usage:
 *   node scripts/gdi-nextgen-hunter-exa-orchestration-demo.mjs
 *   node scripts/gdi-nextgen-hunter-exa-orchestration-demo.mjs --hotel-id recLuxvwwxID7U2B8
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runHunterToExaOrchestration,
  prepareBoundedPaidResearchPilots,
} from "../lib/group-demand-intelligence/nextgen-discovery/orchestration/hunter-to-exa-orchestration-v1.js";
import {
  FIRST_WAVE_HUNTERS,
  SPECIALIST_HUNTER,
} from "../lib/group-demand-intelligence/nextgen-discovery/specialist-hunter-contract-v1.js";
import { resolveHunterConfig } from "../lib/group-demand-intelligence/nextgen-discovery/shared/hunter-config-registry-v1.js";
import { getHunterRunner } from "../lib/group-demand-intelligence/nextgen-discovery/hunters/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BETHESDA = "recLuxvwwxID7U2B8";

function parseArgv(argv) {
  let hotelId = BETHESDA;
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === "--hotel-id" && argv[i + 1]) {
      hotelId = argv[i + 1];
      i += 1;
    }
  }
  return { hotelId };
}

async function main() {
  const { hotelId } = parseArgv(process.argv.slice(2));
  const reportRoot = path.join(
    ROOT,
    "reports/gdi-nextgen-hunter-exa-orchestration-v1"
  );
  fs.mkdirSync(reportRoot, { recursive: true });

  // 1) Genuine zero-paid path — Government Procurement (NIH SBPO SAM.gov)
  const govOrch = await runHunterToExaOrchestration({
    hunterId: SPECIALIST_HUNTER.GOVERNMENT_PROCUREMENT,
    hotelId,
    hotelName: "Bethesda Marriott",
    territory: "Bethesda MD / DMV",
    city: "Bethesda",
    country: "US",
    commercialPriorities: {
      targetSegments: ["government", "public_sector"],
      weekdayFocus: true,
      guestroomPriority: true,
    },
    dryRun: true,
    authorizePaid: false,
    budgetUsd: 0,
    attemptLivePublic: true,
    reportDir: path.join(reportRoot, "zero-paid-gov"),
    rootDir: ROOT,
  });

  // 2) Independence: disable sports; government still runs
  const sportsDisabled = resolveHunterConfig(SPECIALIST_HUNTER.SPORTS_HOUSING, {
    hotelId,
    hotelOverrides: { [SPECIALIST_HUNTER.SPORTS_HOUSING]: { enabled: false } },
  });
  const govStillEnabled = resolveHunterConfig(
    SPECIALIST_HUNTER.GOVERNMENT_PROCUREMENT,
    {
      hotelId,
      hotelOverrides: { [SPECIALIST_HUNTER.SPORTS_HOUSING]: { enabled: false } },
    }
  );
  const sportsRun = await getHunterRunner(SPECIALIST_HUNTER.SPORTS_HOUSING).run({
    hotelId,
    hotelName: "Bethesda Marriott",
    dryRun: true,
    budgetUsd: 0,
    rootDir: ROOT,
    reportDir: path.join(reportRoot, "independence-sports-disabled-attempt"),
  });
  // Force disabled discover via config in orchestration path
  const sportsOrchDisabled = await runHunterToExaOrchestration({
    hunterId: SPECIALIST_HUNTER.SPORTS_HOUSING,
    hotelId,
    hotelName: "Bethesda Marriott",
    dryRun: true,
    preferHistoricalOnly: true,
    reportDir: path.join(reportRoot, "independence-check"),
    rootDir: ROOT,
  });

  // 3) Bounded paid pilots — quotes only, no execution
  const paidPilots = prepareBoundedPaidResearchPilots({
    hunterIds: FIRST_WAVE_HUNTERS.slice(),
    hotelId,
    budgetUsdPerHunter: 3,
    discoveriesByHunter: {
      [SPECIALIST_HUNTER.GOVERNMENT_PROCUREMENT]: govOrch.incremental || [],
    },
  });
  fs.writeFileSync(
    path.join(reportRoot, "bounded-paid-pilots-prepared.json"),
    JSON.stringify(paidPilots, null, 2),
    "utf8"
  );

  const summary = {
    schema: "gdi-nextgen-hunter-exa-orchestration-demo-summary-v1",
    hotelId,
    paidAuthorized: false,
    budgetUsdSpent: 0,
    zeroPaidGov: {
      evidenceClass: govOrch.adapterMeta?.discoveryEvidenceClass,
      mode: govOrch.adapterMeta?.mode,
      incremental: govOrch.measurement?.counts?.incrementalUniqueCandidates,
      orsRouted: govOrch.qualificationHandoff?.summary?.routedToOrs,
      salesReady: govOrch.orsQualification?.salesReady,
      researchLevels: govOrch.researchDepth?.summary,
      skipPaid: govOrch.researchDepth?.summary?.skipPaid,
      reportPath: govOrch.reportPath,
    },
    independence: {
      sportsConfigDisabled: sportsDisabled.enabled === false,
      governmentStillEnabled: govStillEnabled.enabled === true,
      sportsRunnerStillIndependent: Boolean(sportsRun?.hunterId),
      sportsOrchRanSeparately: sportsOrchDisabled.hunterId === "sports_housing",
      note: "Disabling or updating sports does not remove government runner or shared ORS policy.",
    },
    paidPilotsPrepared: {
      execute: false,
      hunterCount: paidPilots.pilots.length,
      totalPlannedCeilingUsd: paidPilots.totalPlannedCeilingUsd,
      path: path.join(reportRoot, "bounded-paid-pilots-prepared.json"),
    },
    successMetric:
      "Incremental commercially credible opportunities — not discovery volume or API calls.",
  };

  fs.writeFileSync(
    path.join(reportRoot, "00-demo-summary.json"),
    JSON.stringify(summary, null, 2),
    "utf8"
  );

  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(JSON.stringify({ ok: false, error: String(err?.stack || err) }));
  process.exit(1);
});
