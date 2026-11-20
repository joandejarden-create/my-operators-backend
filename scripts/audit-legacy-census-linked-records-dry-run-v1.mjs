#!/usr/bin/env node
/**
 * Linked-record migration dry-run: map LEGACY_RECORD_ID → CANONICAL_HPC_RECORD_ID
 * for HI Graph / known product references. No Airtable writes.
 *
 * Prefers full reconciliation matrix when present; falls back to inventory overlays.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import {
  LEGACY_HOTEL_CENSUS_TABLE_ID,
  migrationReadAllowed,
} from "../lib/hotel-intelligence/hpc-population/legacy-hotel-census-access-guard-v1.js";
import { HOTEL_PROPERTY_CENSUS_TABLE_ID } from "../lib/hotel-census/platform-base.js";

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

const matrixPath = path.join(outDir, "LEGACY-TO-HPC-RECONCILIATION-MATRIX-FULL-V1.json");
const dryMatrixPath = path.join(outDir, "LEGACY-TO-HPC-RECONCILIATION-MATRIX-DRY-RUN-V1.json");

function loadMap() {
  if (fs.existsSync(matrixPath)) {
    const full = JSON.parse(fs.readFileSync(matrixPath, "utf8"));
    return {
      source: "FULL_MATRIX",
      map: full.legacyToHpcMap || {},
      rows: full.rows || [],
    };
  }
  if (fs.existsSync(dryMatrixPath)) {
    const dry = JSON.parse(fs.readFileSync(dryMatrixPath, "utf8"));
    const map = {};
    for (const row of dry.rows || []) {
      if (row.LEGACY_ID && row.CANONICAL_HPC_ID) map[row.LEGACY_ID] = row.CANONICAL_HPC_ID;
    }
    // HE 5.0 Papagayo overlay
    map.recyX1wLsHBJYRPw7 = map.recyX1wLsHBJYRPw7 || "recFQ0WvGLbfK5Gpa";
    return { source: "DRY_RUN_MATRIX_PLUS_OVERLAY", map, rows: dry.rows || [] };
  }
  return {
    source: "OVERLAY_ONLY",
    map: {
      recyX1wLsHBJYRPw7: "recFQ0WvGLbfK5Gpa",
    },
    rows: [],
  };
}

async function probeHiGraphEvidenceLinks(legacyIds, token, baseId) {
  // HI Graph Evidence table — sample formula search for legacy id strings in text fields.
  // Read-only. Does not mutate.
  const table = process.env.AIRTABLE_HI_GRAPH_EVIDENCE_TABLE || "HI Graph Evidence";
  const findings = [];
  const sample = legacyIds.slice(0, 25);
  for (const legacyId of sample) {
    const formula = `OR(FIND('${legacyId}', {Evidence ID}&'')>0, FIND('${legacyId}', {Notes}&'')>0, FIND('${legacyId}', {Source URL}&'')>0)`;
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}?maxRecords=5&filterByFormula=${encodeURIComponent(formula)}`;
    try {
      const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) {
        findings.push({ legacyId, ok: false, error: json?.error?.message || res.status });
        continue;
      }
      findings.push({
        legacyId,
        ok: true,
        hitCount: (json.records || []).length,
        recordIds: (json.records || []).map((r) => r.id),
      });
    } catch (err) {
      findings.push({ legacyId, ok: false, error: err.message });
    }
  }
  return findings;
}

const { source, map, rows } = loadMap();
const mappedCount = Object.keys(map).length;
const unmatched = (rows || [])
  .filter((r) => r.legacyId && !r.canonicalHpcId)
  .map((r) => ({
    legacyId: r.legacyId,
    disposition: r.disposition,
    propertyName: r.propertyName || null,
  }));

const integrity = {
  orphanRisk: unmatched.length,
  mappedPairs: mappedCount,
  duplicateHpcTargets: (() => {
    const rev = {};
    for (const [leg, hpc] of Object.entries(map)) {
      if (!rev[hpc]) rev[hpc] = [];
      rev[hpc].push(leg);
    }
    return Object.entries(rev)
      .filter(([, legs]) => legs.length > 1)
      .map(([hpc, legs]) => ({ hpcId: hpc, legacyIds: legs }));
  })(),
  notes: [
    "Name-only UI display is insufficient — use explicit LEGACY→HPC map.",
    "AIRTABLE_LINKS_MIGRATED remains false until founder-authorized apply + read-back.",
    "No production link rewrite in this dry-run.",
  ],
};

let hiProbe = { attempted: false };
const token =
  process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || process.env.AIRTABLE_TOKEN;
const baseId = process.env.AIRTABLE_BASE_ID_ALT;
if (token && baseId && migrationReadAllowed({ allowLegacyHotelCensusRead: process.env.DEALALITY_LEGACY_HOTEL_CENSUS_MIGRATION_READ === "1" })) {
  // HI Graph probe does not require legacy table access; skip assert unless we touch Census.
  hiProbe = {
    attempted: true,
    findings: await probeHiGraphEvidenceLinks(Object.keys(map), token, baseId),
  };
} else if (token && baseId) {
  hiProbe = {
    attempted: true,
    findings: await probeHiGraphEvidenceLinks(Object.keys(map), token, baseId),
  };
}

const report = {
  contract: "DEALALITY_LEGACY_CENSUS_LINKED_RECORD_DRY_RUN_V1",
  generatedAt: new Date().toISOString(),
  applyAuthorized: false,
  airtableLinksMigrated: false,
  legacyTableId: LEGACY_HOTEL_CENSUS_TABLE_ID,
  hpcTableId: HOTEL_PROPERTY_CENSUS_TABLE_ID,
  mapSource: source,
  legacyToHpcMap: map,
  integrity,
  unmatchedSample: unmatched.slice(0, 100),
  hiGraphEvidenceProbe: hiProbe,
  proposedApplySteps: [
    "1. Founder authorize link migration apply (separate task).",
    "2. For each mapped pair, rewrite HI / org / product links from legacy id → HPC id.",
    "3. Idempotency key: linkmig:{table}:{record}:{field}:{legacyId}:{hpcId}",
    "4. Read-back verify no dangling legacy hotel refs in migrated tables.",
    "5. Only then set AIRTABLE_LINKS_MIGRATED=true.",
  ],
};

fs.writeFileSync(
  path.join(outDir, "LEGACY-CENSUS-LINKED-RECORD-MIGRATION-DRY-RUN-V1.json"),
  JSON.stringify(report, null, 2)
);

const md = [];
md.push("# Legacy Census linked-record migration dry-run v1");
md.push("");
md.push(`Generated: ${report.generatedAt}`);
md.push("**applyAuthorized: false** — `AIRTABLE_LINKS_MIGRATED` remains **false**.");
md.push("");
md.push(`Map source: ${source}`);
md.push(`Mapped pairs: **${mappedCount}**`);
md.push(`Unmatched (orphan risk sample): **${unmatched.length}**`);
md.push(`Duplicate HPC targets: **${integrity.duplicateHpcTargets.length}**`);
md.push("");
md.push("## Referential integrity checklist");
md.push("");
md.push("- [ ] No orphaned links after apply");
md.push("- [ ] No duplicated relationships");
md.push("- [ ] Contact / ownership mappings remapped via explicit ids");
md.push("- [ ] GDI opportunities not duplicated by hotel-id rewrite");
md.push("- [ ] ADP associations use HPC ids");
md.push("- [ ] Evidence provenance retained (legacy id kept as historical attribute)");
md.push("");
md.push("See JSON for full `legacyToHpcMap` and proposed apply steps.");

fs.writeFileSync(
  path.join(outDir, "LEGACY-CENSUS-LINKED-RECORD-MIGRATION-DRY-RUN-V1.md"),
  md.join("\n")
);

console.log(
  JSON.stringify(
    {
      mappedPairs: mappedCount,
      unmatched: unmatched.length,
      duplicateHpcTargets: integrity.duplicateHpcTargets.length,
      airtableLinksMigrated: false,
    },
    null,
    2
  )
);
console.log("Wrote LEGACY-CENSUS-LINKED-RECORD-MIGRATION-DRY-RUN-V1.*");
