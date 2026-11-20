#!/usr/bin/env node
/**
 * Build dry-run HPC mutation package from full reconciliation matrix.
 * No Airtable writes. applyAuthorized=false.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import { RECON_DISPOSITION } from "../lib/hotel-census/dealality-legacy-hpc-reconciliation-v1.js";
import { HPC_CLI_TABLE_ID } from "../lib/hotel-census/dealality-census-cli-migration-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const outDir = path.join(root, "reports/hotel-explorer/global-census-retirement");
const matrixPath = path.join(outDir, "LEGACY-TO-HPC-RECONCILIATION-MATRIX-FULL-V1.json");
const dryPath = path.join(outDir, "LEGACY-TO-HPC-RECONCILIATION-MATRIX-DRY-RUN-V1.json");

function idempotencyKey(parts) {
  return createHash("sha256").update(parts.join("|")).digest("hex").slice(0, 24);
}

function loadRows() {
  if (fs.existsSync(matrixPath)) {
    const full = JSON.parse(fs.readFileSync(matrixPath, "utf8"));
    return { source: "FULL", rows: full.rows || [] };
  }
  if (fs.existsSync(dryPath)) {
    const dry = JSON.parse(fs.readFileSync(dryPath, "utf8"));
    // Normalize dry-run fixture shape
    const rows = (dry.rows || []).map((r) => ({
      legacyId: r.LEGACY_ID,
      canonicalHpcId: r.CANONICAL_HPC_ID || null,
      disposition:
        r.MATCH_STATUS === "MATCHED_HPC"
          ? RECON_DISPOSITION.MATCHED_HPC_VERIFIED
          : r.MATCH_STATUS === "NEW_HPC_CANDIDATE"
            ? RECON_DISPOSITION.HPC_CREATE_CANDIDATE
            : r.MATCH_STATUS === "AMBIGUOUS"
              ? RECON_DISPOSITION.AMBIGUOUS_MATCH
              : RECON_DISPOSITION.REQUIRES_EVIDENCE,
      propertyName: r.EVIDENCE?.hotelName || null,
      proposeCreate:
        r.MATCH_STATUS === "NEW_HPC_CANDIDATE"
          ? {
              "Property Name": r.EVIDENCE?.hotelName,
              _legacyProvenanceId: r.LEGACY_ID,
            }
          : null,
      evidence: r.EVIDENCE || {},
    }));
    // Papagayo already created in HE 5.0 — convert CREATE to matched when known
    for (const row of rows) {
      if (row.legacyId === "recyX1wLsHBJYRPw7") {
        row.canonicalHpcId = "recFQ0WvGLbfK5Gpa";
        row.disposition = RECON_DISPOSITION.MATCHED_HPC_VERIFIED;
        row.proposeCreate = null;
      }
    }
    return { source: "DRY_FIXTURE", rows };
  }
  return { source: "EMPTY", rows: [] };
}

const { source, rows } = loadRows();
const mutations = [];

for (const row of rows) {
  if (row.disposition === RECON_DISPOSITION.HPC_CREATE_CANDIDATE && row.proposeCreate) {
    const fields = { ...row.proposeCreate };
    delete fields._legacyProvenanceId;
    mutations.push({
      op: "HPC_CREATE",
      legacyRecordId: row.legacyId,
      canonicalHpcRecordId: null,
      targetTableId: HPC_CLI_TABLE_ID,
      targetField: null,
      currentValue: null,
      proposedValue: fields,
      evidence: row.evidence || {},
      verificationClassification: "PROPOSE_CREATE_DRY_RUN",
      duplicateCheck: "LIVE_COLLISION_REQUIRED_BEFORE_APPLY",
      idempotencyKey: idempotencyKey(["CREATE", row.legacyId, fields["Property Name"] || ""]),
      expectedDownstreamEffect: "New HPC hotel available to Scout/Radar/HE after apply",
      applyAuthorized: false,
    });
    continue;
  }

  if (
    (row.disposition === RECON_DISPOSITION.MATCHED_HPC_VERIFIED ||
      row.disposition === RECON_DISPOSITION.MATCHED_HPC_ALIAS) &&
    row.canonicalHpcId
  ) {
    // Null-fill proposals only when legacy has rooms and we flag compare — dry package lists identity preserve.
    mutations.push({
      op: "HPC_PRESERVE",
      legacyRecordId: row.legacyId,
      canonicalHpcRecordId: row.canonicalHpcId,
      targetTableId: HPC_CLI_TABLE_ID,
      targetField: null,
      currentValue: "HPC_RECORD",
      proposedValue: null,
      evidence: row.evidence || {},
      verificationClassification: "PRESERVE_HPC_NO_LEGACY_OVERWRITE",
      duplicateCheck: "MATCHED",
      idempotencyKey: idempotencyKey(["PRESERVE", row.legacyId, row.canonicalHpcId]),
      expectedDownstreamEffect: "No write; identity mapping only",
      applyAuthorized: false,
    });
  }
}

const report = {
  contract: "DEALALITY_LEGACY_TO_HPC_MUTATION_PACKAGE_V1",
  generatedAt: new Date().toISOString(),
  source,
  applyAuthorized: false,
  productionApply: false,
  counts: {
    total: mutations.length,
    creates: mutations.filter((m) => m.op === "HPC_CREATE").length,
    preserves: mutations.filter((m) => m.op === "HPC_PRESERVE").length,
    nullFills: mutations.filter((m) => m.op === "HPC_NULL_FILL").length,
  },
  mutations,
};

fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(
  path.join(outDir, "LEGACY-TO-HPC-MUTATION-PACKAGE-DRY-RUN-V1.json"),
  JSON.stringify(report, null, 2)
);

const md = [];
md.push("# Legacy → HPC mutation package (dry-run v1)");
md.push("");
md.push(`Generated: ${report.generatedAt}`);
md.push(`Source matrix: ${source}`);
md.push("**applyAuthorized: false** — no production Airtable mutation under this task.");
md.push("");
md.push("| Op | Count |");
md.push("| --- | ---: |");
md.push(`| HPC_CREATE proposals | ${report.counts.creates} |`);
md.push(`| HPC_PRESERVE (no write) | ${report.counts.preserves} |`);
md.push(`| HPC_NULL_FILL | ${report.counts.nullFills} |`);
md.push("");
md.push("Each mutation includes legacy id, HPC id (when known), evidence, duplicate check, and idempotency key.");

fs.writeFileSync(path.join(outDir, "LEGACY-TO-HPC-MUTATION-PACKAGE-DRY-RUN-V1.md"), md.join("\n"));
console.log(JSON.stringify(report.counts, null, 2));
console.log("Wrote LEGACY-TO-HPC-MUTATION-PACKAGE-DRY-RUN-V1.*");
