#!/usr/bin/env node
/**
 * Repair HI completeness after V2 batch — re-run seasonality (and demand if needed)
 * for hotels left NOT_RESEARCHED / HI_INCOMPLETE.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { researchSeasonalityDepthV2 } from "../lib/hotel-intelligence/research/seasonality-depth-v2.js";
import { researchDemandDepthV2 } from "../lib/hotel-intelligence/research/demand-depth-v2.js";
import { isHotelIntelligenceComplete } from "../lib/hotel-intelligence/onboarding/hi-completeness-gate.js";
import { loadDomainStatusLedger } from "../lib/hotel-intelligence/onboarding/domain-status-store.js";
import { upsertDomainStatus } from "../lib/hotel-intelligence/onboarding/domain-status-store.js";
import { HI_DOMAIN, HI_DOMAIN_STATUS } from "../lib/hotel-intelligence/onboarding/domain-status-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.join(__dirname, "..", "reports", "hotel-intelligence", "evidence-depth-v2");

const ALL = [
  "recLuxvwwxID7U2B8",
  "recgMYovrrZDJMqzX",
  "recG66DQJKP2c0UNh",
  "recGkME49yYuxQl0u",
  "rec8hHupaSwiWI3r7",
  "recIwaP1etgx2g9nA",
  "recsn3BUKJ9PNfeZW",
  "recRXmrakhSAuctwz",
  "recN76iEE6yAaPh8H",
  "recESHsNsWUFYZrxR",
  "recCEpdskZeUBvQwG",
  "recD17Kxn6BcJjGFh",
  "recUOyzOXn2Zdp98I",
  "recjDsNzu93CFfe87",
  "rec9Tp0WBb2uk6w3u",
  "rece0or38cxo3Fymb",
  "rec35fExUxCClpOP6",
  "rec2PVBDavppGpenm",
  "recKRJjcPnb4tVDDS",
];

async function main() {
  const mode = process.argv.includes("--apply") ? "apply" : "dry-run";
  const results = [];
  for (const id of ALL) {
    const gate = await isHotelIntelligenceComplete(id);
    const ledger = loadDomainStatusLedger(id);
    const seasonStatus = ledger.domains?.SEASONALITY_NEED_PERIODS?.domainStatus;
    const demandStatus = ledger.domains?.DEMAND_NODES?.domainStatus;
    const needsSeason =
      !gate.complete ||
      seasonStatus === HI_DOMAIN_STATUS.NOT_RESEARCHED ||
      seasonStatus === HI_DOMAIN_STATUS.ERROR;
    const needsDemand =
      demandStatus === HI_DOMAIN_STATUS.NOT_RESEARCHED ||
      demandStatus === HI_DOMAIN_STATUS.ERROR;

    if (!needsSeason && !needsDemand) {
      results.push({ id, hotel: gate.hotelName, ok: true, skipped: true });
      continue;
    }

    const row = { id, hotel: gate.hotelName, beforeComplete: gate.complete };
    if (needsDemand) {
      process.stderr.write(`repair demand ${id}\n`);
      row.demand = await researchDemandDepthV2(id, { mode, maxQueries: 2, maxFetches: 3, maxJev: 1 });
    }
    if (needsSeason) {
      process.stderr.write(`repair seasonality ${id}\n`);
      row.seasonality = await researchSeasonalityDepthV2(id, {
        mode,
        maxQueries: 2,
        maxFetches: 2,
        maxJev: 1,
        skipIfPopulated: false,
      });
      // Hard safety: if still NOT_RESEARCHED after attempt, force ceiling
      if (
        mode === "apply" &&
        row.seasonality?.domainStatus === HI_DOMAIN_STATUS.NOT_RESEARCHED
      ) {
        upsertDomainStatus(id, HI_DOMAIN.SEASONALITY_NEED_PERIODS, {
          domainStatus: HI_DOMAIN_STATUS.PUBLIC_DATA_CEILING,
          researchDepth: "PUBLIC_DATA_CEILING",
          lastResearchedAt: new Date().toISOString(),
          researchRunId: `hi_seasonality_repair_${Date.now()}`,
          evidenceCount: 0,
          rowCount: 0,
          notes: "repair_force_ceiling_after_depth_v2_attempt",
          blocker: HI_DOMAIN_STATUS.PUBLIC_DATA_CEILING,
        });
        row.seasonality.domainStatus = HI_DOMAIN_STATUS.PUBLIC_DATA_CEILING;
        row.forcedCeiling = true;
      }
    }
    const after = await isHotelIntelligenceComplete(id);
    row.afterComplete = after.complete;
    row.afterOverall = after.overallStatus;
    row.seasonAfter = after.domains?.SEASONALITY_NEED_PERIODS?.domainStatus;
    row.demandAfter = after.domains?.DEMAND_NODES?.domainStatus;
    results.push(row);
    console.log(
      JSON.stringify(
        {
          hotel: row.hotel,
          id,
          afterComplete: row.afterComplete,
          seasonAfter: row.seasonAfter,
          demandAfter: row.demandAfter,
          forcedCeiling: row.forcedCeiling || false,
        },
        null,
        2
      )
    );
  }
  const out = path.join(OUT_DIR, `REPAIR_${mode}_${Date.now()}.json`);
  fs.writeFileSync(out, JSON.stringify({ mode, results }, null, 2));
  console.log("Wrote", out);
  const stillBad = results.filter((r) => r.afterComplete === false);
  if (stillBad.length) {
    console.error("STILL_INCOMPLETE", stillBad.map((r) => r.id));
    process.exit(2);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
