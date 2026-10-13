#!/usr/bin/env node
/**
 * HI Evidence Depth V2 — full 19-hotel batch (event → demand → seasonality).
 * Radisson must already PASS before invoking --batch-remaining.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { researchEventSpaceDepthV2 } from "../lib/hotel-intelligence/research/event-space-depth-v2.js";
import { researchDemandDepthV2 } from "../lib/hotel-intelligence/research/demand-depth-v2.js";
import { researchSeasonalityDepthV2 } from "../lib/hotel-intelligence/research/seasonality-depth-v2.js";
import { isHotelIntelligenceComplete } from "../lib/hotel-intelligence/onboarding/hi-completeness-gate.js";
import { loadDomainStatusLedger } from "../lib/hotel-intelligence/onboarding/domain-status-store.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(REPO_ROOT, "reports", "hotel-intelligence", "evidence-depth-v2");
const RADISSON = "recUOyzOXn2Zdp98I";

const ALL_HOTELS = [
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

function parseArgs(argv) {
  const out = {
    mode: "dry-run",
    skipRadisson: true,
    domains: ["event", "demand", "seasonality"],
    eventEmptyOnly: true,
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--mode") out.mode = argv[++i];
    else if (a === "--include-radisson") out.skipRadisson = false;
    else if (a === "--domains") out.domains = String(argv[++i]).split(",");
    else if (a === "--all-event") out.eventEmptyOnly = false;
  }
  return out;
}

function baselineStatus(id, domain) {
  const p = path.join(OUT_DIR, "BASELINE_FORENSIC.json");
  if (!fs.existsSync(p)) return null;
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const h = (j.hotels || []).find((x) => x.hpcHotelId === id);
  return h?.domains?.[domain]?.domainStatus || null;
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const args = parseArgs(process.argv);
  const radissonGate = path.join(OUT_DIR, "RADISSON_POSITIVE_CONTROL.json");
  if (fs.existsSync(radissonGate)) {
    const g = JSON.parse(fs.readFileSync(radissonGate, "utf8"));
    if (!g.ok && args.mode === "apply") {
      console.error("Radisson positive control not PASS — refuse batch apply");
      process.exit(3);
    }
  }

  const hotels = ALL_HOTELS.filter((id) => !(args.skipRadisson && id === RADISSON));
  const results = [];

  for (const id of hotels) {
    const row = { hpcHotelId: id, domains: {} };
    const beforeLedger = loadDomainStatusLedger(id);
    row.hotelName = beforeLedger.hotelName || null;

    // EVENT
    if (args.domains.includes("event")) {
      const before = baselineStatus(id, "EVENT_SPACES") || beforeLedger.domains?.EVENT_SPACES?.domainStatus;
      const shouldRun =
        !args.eventEmptyOnly ||
        before === "RESEARCHED_EMPTY" ||
        before === "NOT_RESEARCHED";
      if (shouldRun) {
        process.stderr.write(`\n[event] ${id} (${before})\n`);
        try {
          const r = await researchEventSpaceDepthV2(id, {
            mode: args.mode,
            forceSecondary: true,
            maxQueries: 3,
            maxFetches: 6,
            maxJev: 1,
          });
          row.domains.event = {
            before,
            after: r.domainStatus,
            populated: r.populated,
            jev: r.jev?.action,
            jevMaterial: r.ledger?.jevMaterial || 0,
            discovered: {
              totalMeetingSpaceSqFt: r.discovered?.commercial?.totalMeetingSpaceSqFt,
              meetingRoomCount: r.discovered?.commercial?.meetingRoomCount,
              spaces: r.discovered?.eventSpaces?.length || 0,
            },
            cvent: (r.sourcesChecked || []).some((s) => s.family === "CVENT" && s.ok),
            ledger: r.ledger,
            conflicts: r.conflicts?.length || 0,
          };
        } catch (err) {
          row.domains.event = { before, error: err.message || String(err) };
        }
      } else {
        row.domains.event = { before, skipped: true, reason: "already_resolved_not_empty" };
      }
    }

    // DEMAND
    if (args.domains.includes("demand")) {
      const before = baselineStatus(id, "DEMAND_NODES") || beforeLedger.domains?.DEMAND_NODES?.domainStatus;
      if (before === "RESEARCHED_EMPTY" || before === "NOT_RESEARCHED") {
        process.stderr.write(`[demand] ${id} (${before})\n`);
        try {
          const r = await researchDemandDepthV2(id, {
            mode: args.mode,
            maxQueries: 2,
            maxFetches: 3,
            maxJev: 1,
          });
          row.domains.demand = {
            before,
            after: r.domainStatus,
            populated: r.populated,
            skipped: r.skipped || false,
            jev: r.jev?.action,
            jevMaterial: r.ledger?.jevMaterial || 0,
            nodes: r.discovered?.demandNodes?.length || 0,
            ledger: r.ledger,
          };
        } catch (err) {
          row.domains.demand = { before, error: err.message || String(err) };
        }
      } else {
        row.domains.demand = { before, skipped: true };
      }
    }

    // SEASONALITY
    if (args.domains.includes("seasonality")) {
      const before =
        baselineStatus(id, "SEASONALITY_NEED_PERIODS") ||
        beforeLedger.domains?.SEASONALITY_NEED_PERIODS?.domainStatus;
      if (before === "RESEARCHED_EMPTY" || before === "NOT_RESEARCHED") {
        process.stderr.write(`[seasonality] ${id} (${before})\n`);
        try {
          const r = await researchSeasonalityDepthV2(id, {
            mode: args.mode,
            maxQueries: 2,
            maxFetches: 2,
            maxJev: 1,
          });
          row.domains.seasonality = {
            before,
            after: r.domainStatus,
            populated: r.populated,
            skipped: r.skipped || false,
            jev: r.jev?.action,
            jevMaterial: r.ledger?.jevMaterial || 0,
            periods: r.discovered?.seasonality?.length || 0,
            note: r.note,
            ledger: r.ledger,
          };
        } catch (err) {
          row.domains.seasonality = { before, error: err.message || String(err) };
        }
      } else {
        row.domains.seasonality = { before, skipped: true };
      }
    }

    try {
      const gate = await isHotelIntelligenceComplete(id);
      row.afterOverall = gate.overallStatus;
      row.hiComplete = gate.complete;
      row.hotelName = gate.hotelName || row.hotelName;
    } catch (err) {
      row.gateError = err.message || String(err);
    }

    results.push(row);
    console.log(
      JSON.stringify(
        {
          hotel: row.hotelName,
          id,
          event: row.domains.event?.after || row.domains.event?.skipped,
          demand: row.domains.demand?.after || row.domains.demand?.skipped,
          seasonality: row.domains.seasonality?.after || row.domains.seasonality?.skipped,
          hiComplete: row.hiComplete,
        },
        null,
        2
      )
    );
  }

  const outPath = path.join(
    OUT_DIR,
    `BATCH_${args.mode}_${new Date().toISOString().replace(/[:.]/g, "-")}.json`
  );
  fs.writeFileSync(outPath, JSON.stringify({ args, results }, null, 2));
  console.log(`Wrote ${outPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
