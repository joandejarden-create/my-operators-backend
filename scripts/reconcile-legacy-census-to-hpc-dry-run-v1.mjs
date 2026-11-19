#!/usr/bin/env node
/**
 * Read-only legacy Hotel Census → HPC reconciliation dry-run (sample + fixtures).
 * Requires DEALALITY_LEGACY_HOTEL_CENSUS_MIGRATION_READ=1 for legacy reads.
 * Never writes Airtable.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import {
  assertLegacyHotelCensusAccess,
  LEGACY_HOTEL_CENSUS_TABLE_ID,
  LEGACY_HOTEL_CENSUS_TABLE_NAME,
  migrationReadAllowed,
} from "../lib/hotel-intelligence/hpc-population/legacy-hotel-census-access-guard-v1.js";
import { performLiveHpcIdentityCollisionCheck } from "../lib/hotel-census/hpc-live-identity-collision-v1.js";
import { HOTEL_PROPERTY_CENSUS_TABLE } from "../lib/hotel-census/platform-base.js";

const require = createRequire(import.meta.url);
try {
  require("dotenv").config({ path: "C:/Dev/dealality/my-operators-backend/.env" });
} catch {
  /* optional */
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const outDir = path.join(root, "reports/hotel-explorer/global-census-retirement");
fs.mkdirSync(outDir, { recursive: true });

const FIXTURES = [
  {
    legacyId: "recyX1wLsHBJYRPw7",
    name: "Secrets Papagayo Costa Rica",
    brand: "Secrets",
    country: "Costa Rica",
    expected: "PROPOSE_HPC_CREATE_OR_MATCH",
  },
  {
    legacyId: null,
    name: "Bethesda Marriott",
    brand: "Marriott",
    country: "United States",
    city: "Bethesda",
    knownHpcId: "recLuxvwwxID7U2B8",
    expected: "MATCHED_HPC",
  },
];

const rows = [];

for (const fx of FIXTURES) {
  const collision = await performLiveHpcIdentityCollisionCheck({
    hotelName: fx.name,
    aliases: [],
    city: fx.city || null,
    country: fx.country,
    brand: fx.brand,
    knownHpcId: fx.knownHpcId || null,
  });

  let matchStatus = "UNRESOLVED";
  let canonicalHpcId = null;
  let fieldActions = [];

  if (fx.knownHpcId && collision?.hpcRecordId === fx.knownHpcId) {
    matchStatus = "MATCHED_HPC";
    canonicalHpcId = fx.knownHpcId;
    fieldActions = ["PRESERVE_HPC", "NO_LEGACY_OVERWRITE"];
  } else if (collision?.outcome === "HPC_EXISTING_RECORD_VERIFIED" && collision.hpcRecordId) {
    matchStatus = "MATCHED_HPC";
    canonicalHpcId = collision.hpcRecordId;
    fieldActions = ["PRESERVE_HPC", "COMPARE_ROOMS_BRAND_GEO_DRY_RUN_ONLY"];
  } else if (collision?.outcome === "HPC_NEW_RECORD_PROPOSED_VERIFIED_ABSENT") {
    matchStatus = "NEW_HPC_CANDIDATE";
    fieldActions = [
      "PROPOSE_CREATE_AFTER_FOUNDER_APPROVAL",
      fx.legacyId
        ? `LEGACY_PROVENANCE_ONLY:${fx.legacyId}`
        : "NO_LEGACY_ID",
    ];
  } else if (collision?.outcome === "HPC_IDENTITY_COLLISION_REQUIRES_REVIEW") {
    matchStatus = "AMBIGUOUS";
    fieldActions = ["AUTONOMOUS_RECONCILE", "DO_NOT_CREATE_DUPLICATE"];
  }

  rows.push({
    LEGACY_ID: fx.legacyId,
    CANONICAL_HPC_ID: canonicalHpcId,
    MATCH_STATUS: matchStatus,
    FIELD_ACTIONS: fieldActions,
    EVIDENCE: {
      hotelName: fx.name,
      hpcCollisionOutcome: collision?.outcome || null,
      candidates: (collision?.candidates || []).slice(0, 5).map((c) => ({
        id: c.id,
        name: c.name,
        score: c.score,
      })),
    },
  });
}

// Optional: sample legacy census count when migration read allowed
let legacySample = { attempted: false, note: "migration read not enabled" };
if (migrationReadAllowed({ allowLegacyHotelCensusRead: process.env.DEALALITY_LEGACY_HOTEL_CENSUS_MIGRATION_READ === "1" })) {
  try {
    assertLegacyHotelCensusAccess(LEGACY_HOTEL_CENSUS_TABLE_ID, {
      method: "GET",
      workflow: "reconcile-legacy-census-to-hpc-dry-run",
      allowLegacyHotelCensusRead: true,
    });
    const token =
      process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || process.env.AIRTABLE_TOKEN;
    const baseId = process.env.AIRTABLE_BASE_ID_ALT;
    if (token && baseId) {
      const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(
        LEGACY_HOTEL_CENSUS_TABLE_NAME
      )}?maxRecords=1&pageSize=1`;
      const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
      const json = await res.json().catch(() => ({}));
      legacySample = {
        attempted: true,
        ok: res.ok,
        note: res.ok
          ? "Legacy table reachable for migration inventory (not product runtime)"
          : json?.error?.message || `HTTP ${res.status}`,
      };
    }
  } catch (err) {
    legacySample = { attempted: true, ok: false, note: err.message };
  }
}

const report = {
  version: "reconcile-legacy-census-to-hpc-dry-run-v1",
  generatedAt: new Date().toISOString(),
  applyAuthorized: false,
  airtableWrites: 0,
  hpcTable: HOTEL_PROPERTY_CENSUS_TABLE,
  matrix: rows,
  legacySample,
  certificationNote:
    "HPC_DATA_RECONCILED remains false until full-table matrix + founder-approved field actions complete.",
};

fs.writeFileSync(
  path.join(outDir, "LEGACY-TO-HPC-RECONCILIATION-MATRIX-DRY-RUN-V1.json"),
  JSON.stringify(report, null, 2)
);

const md = `# Legacy → HPC reconciliation matrix (dry-run v1)

Generated: ${report.generatedAt}  
**applyAuthorized: false** — no Airtable writes.

| LEGACY_ID | CANONICAL_HPC_ID | MATCH_STATUS | FIELD_ACTIONS |
| --- | --- | --- | --- |
${rows
  .map(
    (r) =>
      `| ${r.LEGACY_ID || "—"} | ${r.CANONICAL_HPC_ID || "—"} | ${r.MATCH_STATUS} | ${(r.FIELD_ACTIONS || []).join("; ")} |`
  )
  .join("\n")}

## Evidence

See JSON sibling for candidate scores. Secrets Papagayo remains HPC CREATE candidate with legacy provenance only when collision confirms absence.
`;

fs.writeFileSync(
  path.join(outDir, "LEGACY-TO-HPC-RECONCILIATION-MATRIX-DRY-RUN-V1.md"),
  md
);

console.log(JSON.stringify({ ok: true, rows: rows.length, outDir }, null, 2));
