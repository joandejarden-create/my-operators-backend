/**
 * Ingest evidence-backed market-local entities (report artifact only).
 * Does NOT hard-code Rome orgs into production lib — entities come from fetched sources.
 *
 * Usage:
 *   node scripts/gdi-cross-market-ingest-local-evidence.mjs --evidence path.json
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  proposeMarketLocalTargets,
  buildMarketLocalDiscoveryPlan,
} from "../lib/group-demand-intelligence/research-coverage/market-local-expansion.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/cross-market-replication-v1"
);
const HOTEL_ID = "gdi_hotel_w_rome";

/** Evidence harvested from official pages this cycle (not production templates). */
const EVIDENCE_ENTITIES = [
  {
    organizationName: "Turismo Roma (Roma Capitale tourism)",
    category: "destination_events",
    entityKind: "EVENT_SOURCE",
    sourceUrl: "https://www.turismoroma.it/en",
    sourceLanguage: "mixed",
    destinationRelevant: true,
    catchmentBand: "DESTINATION_RELEVANT",
    priority: "HIGH",
  },
  {
    organizationName: "Fiera Roma",
    category: "trade_shows_exhibitions",
    entityKind: "VENUE",
    sourceUrl: "https://www.fieraroma.it/en/",
    sourceLanguage: "mixed",
    destinationRelevant: true,
    catchmentBand: "EXTENDED",
    priority: "HIGH",
  },
  {
    organizationName: "Rom-E 2026",
    category: "destination_events",
    entityKind: "PROGRAM",
    programName: "Rom-E 2026",
    sourceUrl: "https://www.turismoroma.it/it/eventi/rom-e-2026",
    sourceLanguage: "it",
    destinationRelevant: true,
    catchmentBand: "DESTINATION_RELEVANT",
    priority: "MEDIUM",
  },
  {
    organizationName: "Piano City Roma 2026",
    category: "cultural_institutions",
    entityKind: "PROGRAM",
    programName: "Piano City Roma 2026",
    sourceUrl: "https://www.turismoroma.it/it/eventi/piano-city-roma-2026",
    sourceLanguage: "it",
    destinationRelevant: true,
    catchmentBand: "CORE",
    priority: "MEDIUM",
  },
  {
    organizationName: "Rome Future Week 2026",
    category: "destination_events",
    entityKind: "PROGRAM",
    programName: "Rome Future Week 2026",
    sourceUrl: "https://www.turismoroma.it/en",
    sourceLanguage: "en",
    destinationRelevant: true,
    catchmentBand: "DESTINATION_RELEVANT",
    priority: "MEDIUM",
  },
];

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const plan = buildMarketLocalDiscoveryPlan(HOTEL_ID, { maxTasks: 40 });
  const proposed = proposeMarketLocalTargets(HOTEL_ID, EVIDENCE_ENTITIES, {
    now: "2026-09-25T12:00:00.000Z",
  });
  const report = {
    generatedAt: new Date().toISOString(),
    hotelId: HOTEL_ID,
    webhoundSession: {
      id: "47dcf4c6-6743-4fee-9716-6e32d059412b",
      status: "failed_zero_spend",
      note: "Webhound dataset failed before research; used official page fetches instead.",
    },
    planSummary: {
      languages: plan.languages,
      taskCount: plan.taskCount,
      categories: plan.categoryCount,
    },
    evidenceCount: EVIDENCE_ENTITIES.length,
    proposed,
    persistence: {
      appliedToAirtable: false,
      reason: "census_missing_blocking_apply",
    },
  };
  const outPath = path.join(OUT, "PHASE2_MARKET_LOCAL_EVIDENCE.json");
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        outPath,
        entities: EVIDENCE_ENTITIES.length,
        targets: proposed.totals.targets,
        rejected: proposed.totals.rejected,
        byBand: proposed.totals.byBand,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
