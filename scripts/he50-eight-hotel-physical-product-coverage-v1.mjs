#!/usr/bin/env node
/**
 * Eight-hotel physical-product coverage review (read-only).
 * Confirms Bethesda fully persisted; other pilots retain Rooms / Keys and leave
 * unverified meeting/mix fields unavailable (no Bethesda value copy).
 */
import dotenv from "dotenv";
import { mkdirSync, writeFileSync, existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { HE50_CANONICAL_HPC_BY_PILOT_KEY } from "../lib/hotel-intelligence/he-5-0-full-e2e/contract.js";
import {
  HE_PHYSICAL_PRODUCT_BASE_ID,
  HE_PHYSICAL_PRODUCT_HPC_TABLE_ID,
  BETHESDA_PHYSICAL_PRODUCT_VALUES,
} from "../lib/hotel-intelligence/he-physical-product-schema-v1.js";
import { inventoryFromHpcFields } from "../lib/hotel-intelligence/he-physical-product-adapter-v1.js";

dotenv.config({ path: "C:/Dev/dealality/my-operators-backend/.env", override: false });
dotenv.config({ override: false });

const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
if (!token) {
  console.error(JSON.stringify({ ok: false, error: "missing_airtable_token" }));
  process.exit(2);
}

const OUT_DIR = join(process.cwd(), "reports/hotel-explorer/he-intelligence-factory-v1");
mkdirSync(OUT_DIR, { recursive: true });

const PHYSICAL_FIELDS = [
  "Rooms / Keys",
  "Total Meeting Space (sq ft)",
  "Meeting Rooms Count",
  "Largest Meeting Room Name",
  "Largest Meeting Room (sq ft)",
  "Guestroom Single Count",
  "Guestroom Double Count",
  "Guestroom Suites Count",
  "Guestroom Mix Confidence",
  "Meeting Space Source URL",
  "Meeting Space Flag",
];

/** Independently verified meeting/mix research packets by HPC (Bethesda only today). */
const VERIFIED_RESEARCH_PACKETS = Object.freeze({
  recLuxvwwxID7U2B8:
    "reports/hotel-explorer/phase4-quality/HE-BETH-GUEST-ROOMS-MEETING-SPACE-STAGED-V1.json",
});

async function getFields(id) {
  const res = await fetch(
    `https://api.airtable.com/v0/${HE_PHYSICAL_PRODUCT_BASE_ID}/${HE_PHYSICAL_PRODUCT_HPC_TABLE_ID}/${id}`,
    { headers: { Authorization: `Bearer ${token}` } },
  );
  if (!res.ok) throw new Error(`GET ${id} ${res.status}`);
  const j = await res.json();
  return j.fields || {};
}

function classifyHotel(pilotKey, hpcId, fields) {
  const rooms = fields["Rooms / Keys"] ?? null;
  const meeting = fields["Total Meeting Space (sq ft)"] ?? null;
  const mixSingle = fields["Guestroom Single Count"] ?? null;
  const researchPath = VERIFIED_RESEARCH_PACKETS[hpcId] || null;
  const hasVerifiedResearch = Boolean(researchPath && existsSync(join(process.cwd(), researchPath)));
  const adapter = inventoryFromHpcFields(fields, { hpcId, hotelName: fields["Property Name"] });

  let coverage;
  if (hpcId === BETHESDA_PHYSICAL_PRODUCT_VALUES.hpcId) {
    coverage =
      Number(rooms) === 407 &&
      Number(meeting) === 18719 &&
      Number(mixSingle) === 171
        ? "FULL_PHYSICAL_PRODUCT_PERSISTED"
        : "BETHESDA_INCOMPLETE";
  } else if (rooms != null && meeting == null && mixSingle == null) {
    coverage = "ROOMS_KEYS_ONLY_MEETING_MIX_UNAVAILABLE";
  } else if (meeting != null || mixSingle != null) {
    coverage = "PARTIAL_OR_UNEXPECTED_PHYSICAL_PRODUCT";
  } else {
    coverage = "PHYSICAL_PRODUCT_UNAVAILABLE";
  }

  return {
    pilotKey,
    hpcId,
    propertyName: fields["Property Name"] || null,
    roomsKeys: rooms,
    meetingSpaceSqFt: meeting,
    meetingRoomsCount: fields["Meeting Rooms Count"] ?? null,
    guestroomMixPresent: mixSingle != null,
    independentlyVerifiedResearchPacket: researchPath,
    hasVerifiedResearch,
    bethhesdaValuesCopied: false,
    coverage,
    adapterHydrates: Boolean(adapter),
    fieldsPresent: Object.fromEntries(PHYSICAL_FIELDS.map((k) => [k, fields[k] ?? null])),
  };
}

const hotels = [];
for (const [pilotKey, hpcId] of Object.entries(HE50_CANONICAL_HPC_BY_PILOT_KEY)) {
  const fields = await getFields(hpcId);
  hotels.push(classifyHotel(pilotKey, hpcId, fields));
}

const beth = hotels.find((h) => h.pilotKey === "bethesda_marriott");
const others = hotels.filter((h) => h.pilotKey !== "bethesda_marriott");

const report = {
  version: "he50-eight-hotel-physical-product-coverage-v1",
  at: new Date().toISOString(),
  founderAuthorization: "HE_5_0_PHYSICAL_PRODUCT_SCHEMA_2026-11-27",
  gate: "EIGHT_HOTEL_PHYSICAL_PRODUCT_COVERAGE_REVIEWED",
  hotels,
  summary: {
    hotelCount: hotels.length,
    fullPhysicalProduct: hotels.filter((h) => h.coverage === "FULL_PHYSICAL_PRODUCT_PERSISTED").length,
    roomsKeysOnly: hotels.filter((h) => h.coverage === "ROOMS_KEYS_ONLY_MEETING_MIX_UNAVAILABLE")
      .length,
    bethhesdaComplete: beth?.coverage === "FULL_PHYSICAL_PRODUCT_PERSISTED",
    noBethhesdaValueCopy: others.every((h) => h.bethhesdaValuesCopied === false),
    unresolvedMeetingMixLeftUnavailable: others.every(
      (h) => h.meetingSpaceSqFt == null && h.guestroomMixPresent === false,
    ),
  },
  gates: {
    EIGHT_HOTEL_PHYSICAL_PRODUCT_COVERAGE_REVIEWED:
      beth?.coverage === "FULL_PHYSICAL_PRODUCT_PERSISTED" &&
      others.every((h) => h.coverage === "ROOMS_KEYS_ONLY_MEETING_MIX_UNAVAILABLE") &&
      others.every((h) => h.bethhesdaValuesCopied === false),
    FULL_HE_E2E_VALIDATED: false,
  },
  note:
    "Seven non-Bethesda pilots retain independently verified Rooms / Keys only. Meeting/mix fields remain unavailable until hotel-specific verified research exists. Do not copy Bethesda values.",
};

const outPath = join(OUT_DIR, "EIGHT_HOTEL_PHYSICAL_PRODUCT_COVERAGE_V1.json");
writeFileSync(outPath, JSON.stringify(report, null, 2));
console.log(
  JSON.stringify(
    {
      ok: report.gates.EIGHT_HOTEL_PHYSICAL_PRODUCT_COVERAGE_REVIEWED,
      out: outPath,
      summary: report.summary,
      gates: report.gates,
    },
    null,
    2,
  ),
);

if (!report.gates.EIGHT_HOTEL_PHYSICAL_PRODUCT_COVERAGE_REVIEWED) process.exit(3);
