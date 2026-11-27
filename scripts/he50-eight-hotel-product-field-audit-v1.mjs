#!/usr/bin/env node
/**
 * Eight-hotel product-field reuse audit (read-only).
 * Confirms which HPC product fields already exist before any new research.
 */
import dotenv from "dotenv";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { HE50_CANONICAL_HPC_BY_PILOT_KEY } from "../lib/hotel-intelligence/he-5-0-full-e2e/contract.js";
import { buildGuestRoomsMeetingSpaceFromHpc } from "../lib/hotel-intelligence/he-hpc-guest-rooms-meeting-space-v1.js";

dotenv.config({ path: "C:/Dev/dealality/my-operators-backend/.env", override: false });
dotenv.config({ override: false });

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = "appCCUsuGsE1ifoLk";
const TABLE = "tbl9aY5ijiuIzzWam";
const OUT = path.join(
  ROOT,
  "reports/hotel-explorer/he50-completion-v1/EIGHT_HOTEL_PRODUCT_FIELD_AUDIT_V1.json",
);

const PRODUCT_FIELDS = [
  "Property Name",
  "Rooms / Keys",
  "Opening Date",
  "Total Meeting Space (sq ft)",
  "Meeting Rooms Count",
  "Guestroom Single Count",
  "Guestroom Double Count",
  "Guestroom Suites Count",
  "Largest Meeting Room Name",
  "Largest Meeting Room (sq ft)",
  "Meeting Space Source URL",
  "Hotel Headline",
  "Hotel Description",
  "Market",
  "Submarket",
  "Operator / Management Company",
  "Phone",
  "Official Property URL",
];

async function fetchHpc(id, token) {
  const res = await fetch(`https://api.airtable.com/v0/${BASE}/${TABLE}/${id}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const body = await res.json().catch(() => ({}));
  return { ok: res.ok, status: res.status, body };
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || process.env.AIRTABLE_TOKEN;
  if (!token) {
    console.error(JSON.stringify({ ok: false, error: "MISSING_AIRTABLE_TOKEN" }));
    process.exit(2);
  }

  const hotels = [];
  for (const [pilotKey, hpcId] of Object.entries(HE50_CANONICAL_HPC_BY_PILOT_KEY)) {
    const r = await fetchHpc(hpcId, token);
    const fields = r.body?.fields || {};
    const pick = {};
    for (const k of PRODUCT_FIELDS) pick[k] = fields[k] ?? null;
    const dto = r.ok ? buildGuestRoomsMeetingSpaceFromHpc(fields, { hpcId, hotelName: pick["Property Name"] }) : null;
    const present = PRODUCT_FIELDS.filter((k) => pick[k] != null && pick[k] !== "");
    hotels.push({
      pilotKey,
      hpcId,
      exists: r.ok,
      httpStatus: r.status,
      propertyName: pick["Property Name"],
      presentFieldCount: present.length,
      presentFields: present,
      missingFields: PRODUCT_FIELDS.filter((k) => pick[k] == null || pick[k] === ""),
      roomsDtoHydratable: Boolean(dto),
      snapshot: pick,
    });
    await new Promise((x) => setTimeout(x, 100));
  }

  const report = {
    version: "he50-eight-hotel-product-field-audit-v1",
    at: new Date().toISOString(),
    mode: "READ_ONLY",
    paidResearchSpendUsd: 0,
    hotelCount: hotels.length,
    allExist: hotels.every((h) => h.exists),
    hydratableCount: hotels.filter((h) => h.roomsDtoHydratable).length,
    hotels,
    nextActions: [
      "Reuse present HPC fields — do not re-research",
      "CONNECT remaining ownership/contacts/demand/access via HI graph where staged",
      "Do not fabricate missing ownership or direct contacts",
      "Keep cumulative paid research ≤ $50",
    ],
  };

  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: report.allExist,
        out: OUT,
        hydratableCount: report.hydratableCount,
        hotels: hotels.map((h) => ({
          pilotKey: h.pilotKey,
          present: h.presentFieldCount,
          roomsDto: h.roomsDtoHydratable,
        })),
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
