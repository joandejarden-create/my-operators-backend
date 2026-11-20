#!/usr/bin/env node
/**
 * Live read-back of the eight HE50 pilot HPC records from canonical production.
 * Fail-closed if any expected ID is missing — never CREATE duplicates.
 */
import dotenv from "dotenv";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { EIGHT_HOTEL_COMPLETENESS } from "../lib/hotel-intelligence/he-5-0-full-e2e/completeness-matrix-v1.js";

dotenv.config({ path: "C:/Dev/dealality/my-operators-backend/.env", override: false });
dotenv.config({ override: false });

const BASE = "appCCUsuGsE1ifoLk";
const TABLE = "tbl9aY5ijiuIzzWam";
const OUT = join(process.cwd(), "reports/hotel-explorer/he50-canonical-intelligence-recovery-v1");

export const HE50_CANONICAL_HPC_IDENTITIES = Object.freeze([
  { hotel: "Bethesda Marriott", pilotKey: "bethesda_marriott", hpcId: "recLuxvwwxID7U2B8" },
  { hotel: "AC Hotel Newtown Square", pilotKey: "ac_hotel_newtown_square", hpcId: "recF7DQBPDW5aUDoC" },
  { hotel: "Carton House Fairmont", pilotKey: "carton_house_fairmont", hpcId: "recYf7HZWmCdWPaSd" },
  { hotel: "Hilton Garden Inn Newtown Square", pilotKey: "hgi_newtown_square_radnor", hpcId: "recgaBAwa24wsl3xF" },
  { hotel: "Secrets Papagayo", pilotKey: "secrets_papagayo", hpcId: "recFQ0WvGLbfK5Gpa" },
  { hotel: "Secrets St. Lucia", pilotKey: "secrets_st_lucia", hpcId: "recwZJuMcx6kA7f7I" },
  { hotel: "Secrets Cap Cana", pilotKey: "secrets_cap_cana", hpcId: "recd7XAM6tLjzQhaI" },
  { hotel: "JW Marriott Costa Elena", pilotKey: "jw_marriott_costa_elena", hpcId: "recq9KWr8UE7z2549" },
]);

const FIELDS = [
  "Property Name",
  "Canonical Property Name",
  "Current Brand",
  "Brand Family",
  "Rooms / Keys",
  "Rooms Confidence",
  "Radar Status",
  "Country",
  "City",
  "State / Region",
  "Market",
  "Submarket",
  "Address",
  "Opening Date",
  "Property Type",
  "Identity Confidence",
  "Production Use Status",
];

async function fetchRecord(id, token) {
  const url = `https://api.airtable.com/v0/${BASE}/${TABLE}/${id}`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const body = await res.json().catch(() => ({}));
  return { status: res.status, ok: res.ok, body };
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || process.env.AIRTABLE_TOKEN;
  if (!token) {
    console.error(JSON.stringify({ ok: false, error: "MISSING_AIRTABLE_TOKEN" }));
    process.exit(2);
  }
  mkdirSync(OUT, { recursive: true });
  const rows = [];
  for (const h of HE50_CANONICAL_HPC_IDENTITIES) {
    const r = await fetchRecord(h.hpcId, token);
    const f = r.body?.fields || {};
    const pick = {};
    for (const k of FIELDS) pick[k] = f[k] ?? null;
    rows.push({
      ...h,
      httpStatus: r.status,
      exists: r.ok === true,
      fields: pick,
      error: r.ok ? null : r.body?.error || r.body,
    });
    await new Promise((x) => setTimeout(x, 120));
  }
  const missing = rows.filter((x) => !x.exists);
  const staleCompletenessMatrixNullIds = EIGHT_HOTEL_COMPLETENESS.filter(
    (h) => !h.hpcId || /CREATE READY/i.test(String(h.readinessNote || "")),
  ).map((h) => h.pilotKey);
  const report = {
    ok: missing.length === 0,
    gate: "he50-eight-hotel-hpc-readback-v1",
    baseId: BASE,
    tableId: TABLE,
    expectedCount: 8,
    foundCount: rows.filter((x) => x.exists).length,
    missingIds: missing.map((m) => m.hpcId),
    staleCompletenessMatrixNullIds,
    matrixSyncedWithLiveHpc: staleCompletenessMatrixNullIds.length === 0,
    rootCauseOfCreateReadyMisclassification:
      "Historical: completeness-matrix-v1.js retained hpcId:null and CREATE READY readiness notes after HE50 controlled apply already created five HPC records. PR #482 founder report used that stale matrix, not live HPC read-back. Recovery branch syncs matrix + allowlist + free-research packets to the live IDs.",
    hotels: rows,
    certifications: {
      EIGHT_HPC_IDENTITIES_READBACK_VALIDATED: missing.length === 0,
      MATRIX_NO_LONGER_CREATE_READY:
        staleCompletenessMatrixNullIds.length === 0,
    },
  };
  writeFileSync(join(OUT, "EIGHT_HPC_READBACK.json"), JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
  if (!report.ok) process.exit(1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
