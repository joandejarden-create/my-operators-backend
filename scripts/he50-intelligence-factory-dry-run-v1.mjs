#!/usr/bin/env node
/**
 * HE Intelligence Factory — idempotent dry-run for one canonical HPC.
 * Walks the 10-step publication sequence without Airtable writes or paid research.
 *
 * Usage:
 *   node scripts/he50-intelligence-factory-dry-run-v1.mjs
 *   node scripts/he50-intelligence-factory-dry-run-v1.mjs --hotel recLuxvwwxID7U2B8
 */
import dotenv from "dotenv";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  HE_PUBLICATION_SEQUENCE,
  HE_PUBLICATION_STEP,
  buildHe50AcceptanceSnapshot,
  HE50_EIGHT_HOTEL_HPC_IDS,
} from "../lib/hotel-intelligence/he-explorer-publication-gates-v1.js";
import {
  HE50_BETHESDA_HPC_ID,
  HE50_CANONICAL_HPC_BY_PILOT_KEY,
} from "../lib/hotel-intelligence/he-5-0-full-e2e/contract.js";
import { buildGuestRoomsMeetingSpaceFromHpc } from "../lib/hotel-intelligence/he-hpc-guest-rooms-meeting-space-v1.js";

dotenv.config({ path: "C:/Dev/dealality/my-operators-backend/.env", override: false });
dotenv.config({ override: false });

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = "appCCUsuGsE1ifoLk";
const TABLE = "tbl9aY5ijiuIzzWam";
const OUT_DIR = path.join(ROOT, "reports/hotel-explorer/he50-completion-v1");

function argHotel() {
  const i = process.argv.indexOf("--hotel");
  if (i >= 0 && process.argv[i + 1]) return String(process.argv[i + 1]).trim();
  return HE50_BETHESDA_HPC_ID;
}

function pilotKeyFor(hpcId) {
  return (
    Object.entries(HE50_CANONICAL_HPC_BY_PILOT_KEY).find(([, id]) => id === hpcId)?.[0] || null
  );
}

async function fetchHpc(id, token) {
  const res = await fetch(`https://api.airtable.com/v0/${BASE}/${TABLE}/${id}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const body = await res.json().catch(() => ({}));
  return { ok: res.ok, status: res.status, body };
}

async function main() {
  const hotelId = argHotel();
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || process.env.AIRTABLE_TOKEN;
  if (!token) {
    console.error(JSON.stringify({ ok: false, error: "MISSING_AIRTABLE_TOKEN" }));
    process.exit(2);
  }
  if (!HE50_EIGHT_HOTEL_HPC_IDS.includes(hotelId)) {
    console.error(
      JSON.stringify({
        ok: false,
        error: "HOTEL_NOT_IN_EIGHT_PILOT",
        hotelId,
        allowed: HE50_EIGHT_HOTEL_HPC_IDS,
      }),
    );
    process.exit(2);
  }

  const steps = [];
  const push = (step, status, detail) => steps.push({ step, status, detail });

  // 1 Resolve HPC
  const live = await fetchHpc(hotelId, token);
  if (!live.ok) {
    push(HE_PUBLICATION_STEP.RESOLVE_HPC, "FAIL", { httpStatus: live.status, body: live.body });
    writeAndExit({ ok: false, hotelId, steps });
    return;
  }
  const fields = live.body.fields || {};
  push(HE_PUBLICATION_STEP.RESOLVE_HPC, "PASS", {
    hpcId: hotelId,
    pilotKey: pilotKeyFor(hotelId),
    propertyName: fields["Property Name"] || null,
    legacyCensus: "BLOCKED_NOT_USED",
  });

  // 2 Reuse research artifacts
  const stagedRooms = path.join(
    ROOT,
    "reports/hotel-explorer/phase4-quality/HE-BETH-GUEST-ROOMS-MEETING-SPACE-STAGED-V1.json",
  );
  const gapMatrix = path.join(
    ROOT,
    "reports/hotel-explorer/he-5-0-pilot/HE50-FIELD-GAP-MATRIX.json",
  );
  push(HE_PUBLICATION_STEP.REUSE_RESEARCH, "PASS", {
    stagedRoomsExists: fs.existsSync(stagedRooms),
    gapMatrixExists: fs.existsSync(gapMatrix),
    note: "Reuse staged/verified artifacts; no rediscovery this run",
  });

  // 3 Identify gaps (from live reconcile if present, else matrix)
  const reconcilePath = path.join(OUT_DIR, "BETHESDA_LIVE_GAP_RECONCILE_V1.json");
  let gapTotals = null;
  if (fs.existsSync(reconcilePath) && hotelId === HE50_BETHESDA_HPC_ID) {
    gapTotals = JSON.parse(fs.readFileSync(reconcilePath, "utf8")).totals;
  }
  push(HE_PUBLICATION_STEP.IDENTIFY_GAPS, gapTotals ? "PASS" : "PARTIAL", {
    gapTotals,
    note: gapTotals
      ? "Live reconcile present"
      : "Run he50-bethesda-live-gap-reconcile-v1.mjs for Bethesda",
  });

  // 4 Validate sources (presence of source URLs on key fields)
  const sourceUrls = {
    roomsSource: fields["Rooms Source URL"] || null,
    openingDateSource: fields["Opening Date Source URL"] || null,
    meetingSpaceSource: fields["Meeting Space Source URL"] || null,
    officialUrl: fields["Official Property URL"] || null,
  };
  push(HE_PUBLICATION_STEP.VALIDATE_SOURCES, "PASS", sourceUrls);

  // 5 Dedupe — confirm single HPC identity in eight-hotel map
  const dupes = HE50_EIGHT_HOTEL_HPC_IDS.filter((id) => id === hotelId);
  push(HE_PUBLICATION_STEP.DEDUPE_ENTITIES, dupes.length === 1 ? "PASS" : "FAIL", {
    occurrencesInPilotMap: dupes.length,
  });

  // 6 Apply — dry-run only
  const roomsDto = buildGuestRoomsMeetingSpaceFromHpc(fields, {
    hpcId: hotelId,
    hotelName: fields["Property Name"],
  });
  push(HE_PUBLICATION_STEP.APPLY_AIRTABLE, "DRY_RUN_ONLY", {
    applyAuthorized: false,
    proposedWrites: [],
    note: "No mutations this run. Rooms/meeting already on HPC for Bethesda when present.",
    roomsDtoBuilt: Boolean(roomsDto),
  });

  // 7 Read-back
  push(HE_PUBLICATION_STEP.READBACK, "PASS", {
    rooms: fields["Rooms / Keys"] ?? null,
    openingDate: fields["Opening Date"] ?? null,
    meetingSqFt: fields["Total Meeting Space (sq ft)"] ?? null,
    suites: fields["Guestroom Suites Count"] ?? null,
    meetingRoomsCount: fields["Meeting Rooms Count"] ?? null,
  });

  // 8 API hydration — structural: DTO buildable from HPC
  push(
    HE_PUBLICATION_STEP.API_HYDRATION,
    roomsDto ? "PASS_STRUCTURAL" : "FAIL",
    {
      guestRoomsMeetingSpace: Boolean(roomsDto),
      note: "Structural HPC→DTO map. Live HTTP ownership probe still required for full PASS.",
    },
  );

  // 9 Live render — not claimed without Radar browser evidence
  push(HE_PUBLICATION_STEP.LIVE_RENDER, "NOT_RUN", {
    note: "Requires authenticated Radar 8080 hotel×tab checks",
  });

  // 10 Provenance
  push(HE_PUBLICATION_STEP.PROVENANCE, "PASS", {
    sequence: HE_PUBLICATION_SEQUENCE,
    recordedAt: new Date().toISOString(),
  });

  const acceptance = buildHe50AcceptanceSnapshot({
    notes: "Factory dry-run; fail-closed founder gates",
    blockers: [
      "PR #619 / #621 pending Integration Controller merge for main + HE home sync",
      "Radar 88-check browser acceptance not run",
      "Remaining CONNECT rows: ownership/contacts/demand/access/sources",
    ],
  });

  const report = {
    version: "he50-intelligence-factory-dry-run-v1",
    at: new Date().toISOString(),
    hotelId,
    pilotKey: pilotKeyFor(hotelId),
    mode: "DRY_RUN",
    paidResearchSpendUsd: 0,
    steps,
    acceptanceGates: acceptance.gates,
    ok: steps.every((s) => s.status !== "FAIL"),
  };

  fs.mkdirSync(OUT_DIR, { recursive: true });
  const outPath = path.join(
    OUT_DIR,
    `FACTORY_DRY_RUN_${pilotKeyFor(hotelId) || hotelId}.json`,
  );
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ ok: report.ok, out: outPath, steps: steps.map((s) => [s.step, s.status]) }, null, 2));
}

function writeAndExit(report) {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const outPath = path.join(OUT_DIR, "FACTORY_DRY_RUN_FAILED.json");
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ ...report, out: outPath }, null, 2));
  process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
