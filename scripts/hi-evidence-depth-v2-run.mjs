#!/usr/bin/env node
/**
 * HI Evidence Depth V2 — runner
 * Usage:
 *   node scripts/hi-evidence-depth-v2-run.mjs --hotel recUOyzOXn2Zdp98I --mode dry-run
 *   node scripts/hi-evidence-depth-v2-run.mjs --hotel recUOyzOXn2Zdp98I --mode apply
 *   node scripts/hi-evidence-depth-v2-run.mjs --batch-remaining --mode apply
 *   node scripts/hi-evidence-depth-v2-run.mjs --all --mode apply
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { researchEventSpaceDepthV2 } from "../lib/hotel-intelligence/research/event-space-depth-v2.js";
import { isHotelIntelligenceComplete } from "../lib/hotel-intelligence/onboarding/hi-completeness-gate.js";

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
  const out = { mode: "dry-run", hotel: null, batchRemaining: false, all: false, forceSecondary: true };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--mode") out.mode = argv[++i];
    else if (a === "--hotel") out.hotel = argv[++i];
    else if (a === "--batch-remaining") out.batchRemaining = true;
    else if (a === "--all") out.all = true;
    else if (a === "--no-force-secondary") out.forceSecondary = false;
  }
  return out;
}

function eventEmptyFromBaseline(id) {
  const p = path.join(OUT_DIR, "BASELINE_FORENSIC.json");
  if (!fs.existsSync(p)) return true;
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const h = (j.hotels || []).find((x) => x.hpcHotelId === id);
  return h?.domains?.EVENT_SPACES?.domainStatus === "RESEARCHED_EMPTY";
}

async function runOne(hpcHotelId, opts) {
  const before = await isHotelIntelligenceComplete(hpcHotelId);
  const beforeEvent = before.domains?.EVENT_SPACES?.domainStatus;
  const result = await researchEventSpaceDepthV2(hpcHotelId, {
    mode: opts.mode,
    forceSecondary: opts.forceSecondary,
    maxQueries: 3,
    maxFetches: 5,
    maxJev: 1,
    researchRunId: `hi_depth_v2_${opts.mode}_${Date.now()}`,
  });
  const after = await isHotelIntelligenceComplete(hpcHotelId);
  return {
    hpcHotelId,
    hotelName: result.hotelName || before.hotelName,
    beforeEvent,
    afterEvent: after.domains?.EVENT_SPACES?.domainStatus,
    populated: result.populated,
    domainStatus: result.domainStatus,
    researchDepth: result.researchDepth,
    conflicts: result.conflicts,
    jev: result.jev,
    ledger: result.ledger,
    discovered: {
      totalMeetingSpaceSqFt: result.discovered?.commercial?.totalMeetingSpaceSqFt,
      meetingRoomCount: result.discovered?.commercial?.meetingRoomCount,
      largestMeetingSpaceSqFt: result.discovered?.commercial?.largestMeetingSpaceSqFt,
      largestEventCapacity: result.discovered?.commercial?.largestEventCapacity,
      eventSpaceCount: result.discovered?.eventSpaces?.length || 0,
      eventSpaceNames: (result.discovered?.eventSpaces || []).map((s) => s.spaceName),
    },
    sourcesChecked: result.sourcesChecked,
    applyResult: result.applyResult,
    adpSync: result.adpSync,
    cventDiscovered: (result.sourcesChecked || []).some(
      (s) => s.family === "CVENT" && s.ok
    ),
    positiveControl:
      hpcHotelId === RADISSON
        ? {
            pass:
              result.populated === true &&
              (result.discovered?.commercial?.totalMeetingSpaceSqFt != null ||
                (result.discovered?.eventSpaces || []).length > 0) &&
              (result.sourcesChecked || []).some((s) => /cvent\.com\/venues/i.test(s.url || "")),
            cventUrl: (result.sourcesChecked || []).find((s) =>
              /cvent\.com\/venues/i.test(s.url || "")
            )?.url,
          }
        : null,
  };
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const args = parseArgs(process.argv);
  let targets = [];
  if (args.hotel) targets = [args.hotel];
  else if (args.batchRemaining) {
    targets = ALL_HOTELS.filter((id) => id !== RADISSON).filter(
      (id) => eventEmptyFromBaseline(id) || args.forceSecondary
    );
    // Prefer RESEARCHED_EMPTY event hotels first, then others needing deepen
    const emptyFirst = targets.filter((id) => eventEmptyFromBaseline(id));
    const rest = targets.filter((id) => !eventEmptyFromBaseline(id));
    targets = [...emptyFirst, ...rest];
  } else if (args.all) targets = ALL_HOTELS;
  else {
    console.error("Specify --hotel <id> | --batch-remaining | --all");
    process.exit(2);
  }

  const results = [];
  for (const id of targets) {
    process.stderr.write(`\n=== ${args.mode} ${id} ===\n`);
    try {
      const r = await runOne(id, args);
      results.push(r);
      console.log(
        JSON.stringify(
          {
            hotel: r.hotelName,
            id: r.hpcHotelId,
            beforeEvent: r.beforeEvent,
            afterEvent: r.afterEvent,
            populated: r.populated,
            cvent: r.cventDiscovered,
            positiveControl: r.positiveControl,
            discovered: r.discovered,
            jevAction: r.jev?.action,
          },
          null,
          2
        )
      );
      if (id === RADISSON && args.mode === "apply" && r.positiveControl && !r.positiveControl.pass) {
        console.error("RADISSON POSITIVE CONTROL FAILED — STOP");
        const failPath = path.join(OUT_DIR, "RADISSON_POSITIVE_CONTROL.json");
        fs.writeFileSync(failPath, JSON.stringify({ ok: false, ...r }, null, 2));
        process.exit(3);
      }
    } catch (err) {
      results.push({ hpcHotelId: id, error: err.message || String(err) });
      console.error(id, err);
      if (id === RADISSON) process.exit(3);
    }
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outName =
    targets.length === 1 && targets[0] === RADISSON
      ? `RADISSON_${args.mode}.json`
      : `BATCH_${args.mode}_${stamp}.json`;
  fs.writeFileSync(path.join(OUT_DIR, outName), JSON.stringify({ args, results }, null, 2));
  if (targets.length === 1 && targets[0] === RADISSON) {
    fs.writeFileSync(
      path.join(OUT_DIR, "RADISSON_POSITIVE_CONTROL.json"),
      JSON.stringify(
        {
          ok: results[0]?.positiveControl?.pass === true || (args.mode === "dry-run" && results[0]?.populated),
          mode: args.mode,
          ...results[0],
        },
        null,
        2
      )
    );
  }
  console.log(`Wrote ${path.join(OUT_DIR, outName)}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
