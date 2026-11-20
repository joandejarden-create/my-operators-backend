#!/usr/bin/env node
/**
 * Quality-gated migration dry-run (V1/V2/V3 → agreements/milestones).
 * No Airtable writes. No HPC creates.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildQualityGatedMigration } from "../lib/hotel-intelligence/sec-edgar/contract-timing-v4-migration-quality-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-contract-timing-v4/MIGRATION_QUALITY_AUDIT.json");
const OUT_LEGACY = path.join(ROOT, "reports/sec-edgar-contract-timing-v4/MIGRATION_DRY_RUN.json");

function loadAll() {
  const rows = [];
  const v3 = path.join(ROOT, "reports/sec-edgar-contract-timing-v3/V3_FINDINGS_NEW.json");
  if (fs.existsSync(v3)) {
    for (const f of JSON.parse(fs.readFileSync(v3, "utf8")).findings || []) {
      rows.push({ ...f, wave: "V3" });
    }
  }
  const v2 = path.join(ROOT, "reports/sec-edgar-contract-timing-v2/V2_FINDINGS_INCREMENTAL.json");
  if (fs.existsSync(v2)) {
    for (const f of JSON.parse(fs.readFileSync(v2, "utf8")).combined || []) {
      rows.push({ ...f, wave: "V2" });
    }
  }
  const v1 = path.join(ROOT, "reports/sec-edgar-contract-timing-cmbs-v1/VERIFIED_FINDINGS.json");
  if (fs.existsSync(v1)) {
    const j = JSON.parse(fs.readFileSync(v1, "utf8"));
    for (const f of [...(j.verified || []), ...(j.probable || [])]) {
      rows.push({ ...f, wave: "V1" });
    }
  }
  return rows;
}

const findings = loadAll();
const audit = buildQualityGatedMigration(findings);

const report = {
  module: "V4_CONTRACT_MIGRATION_QUALITY_AUDIT",
  at: new Date().toISOString(),
  mode: "DRY_RUN_QUALITY_GATED",
  noAirtableWrites: true,
  noHpcCreates: true,
  noFullMigrationApply: true,
  publicationState: "CUSTOMER_BLOCKED",
  ...audit,
  proposedAirtableOps: {
    verifiedContractCreates: audit.agreementQualityCounts.VERIFIED || 0,
    researchOnlyContractCreates: audit.agreementQualityCounts.RESEARCH_ONLY || 0,
    conflictedQuarantine: audit.agreementQualityCounts.CONFLICTED || 0,
    rejectedExcluded: audit.findingQualityCounts.REJECTED || 0,
    verifiedMilestoneCreates: audit.milestones.filter((m) => m.quality === "VERIFIED").length,
    note: "Full 481 migration blocked. Research Mode pilot (10–20 VERIFIED) only after schema auth + founder persist auth.",
  },
  investigationNotes: [
    "Maple Grove RV Resort — REJECTED (non_hotel_asset / manufactured housing collateral)",
    "Bellagio Hotel and Casino city=Clark — CONFLICTED (county-as-city; multi-city Las Vegas also present)",
    "Agreement type FRANCHISE default without franchise wording → RESEARCH_ONLY or CONFLICTED",
    "Stable agreement keys are not verified legal agreement identities",
  ],
};

fs.writeFileSync(OUT, JSON.stringify(report, null, 2));
fs.writeFileSync(
  OUT_LEGACY,
  JSON.stringify(
    {
      module: "V4_CONTRACT_MIGRATION_DRY_RUN",
      at: report.at,
      mode: "DRY_RUN_QUALITY_GATED",
      noAirtableWrites: true,
      noHpcCreates: true,
      publicationState: "CUSTOMER_BLOCKED",
      input: report.input,
      unique: report.unique,
      findingQualityCounts: report.findingQualityCounts,
      agreementQualityCounts: report.agreementQualityCounts,
      proposedAirtableOps: report.proposedAirtableOps,
      samples: report.samples,
      investigationNotes: report.investigationNotes,
      note: report.note,
    },
    null,
    2,
  ),
);

console.log(
  JSON.stringify(
    {
      out: OUT,
      findingQualityCounts: report.findingQualityCounts,
      agreementQualityCounts: report.agreementQualityCounts,
      unique: report.unique,
    },
    null,
    2,
  ),
);
