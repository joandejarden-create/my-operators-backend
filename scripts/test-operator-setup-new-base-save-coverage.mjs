#!/usr/bin/env node
/**
 * Dry-run: which Phase B fields would persist to new-base tables.
 * Usage: node scripts/test-operator-setup-new-base-save-coverage.mjs [--apply]
 * --apply performs a real write (only when explicitly passed).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import {
  buildNewBaseTablePayloads,
  createOrUpdateOperatorMaster,
  writeOperatorSetupToNewBase,
} from "../api/lib/operator-setup-new-base-writer.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const PHASE_B_PATH = path.join(ROOT, "api/lib/operator-setup-new-base-phase-b-fields.json");

const SAMPLE_BODY = {
  recordId: "",
  companyName: "Phase B Coverage Test Operator",
  companyDescription: "Dry-run sample operator for Phase B save coverage.",
  website: "https://example.com",
  headquarters: "Atlanta, GA",
  primaryServiceModel: "Third-Party Management",
  yearEstablished: 1990,
  yearsInBusiness: 35,
  chainScale: "Upscale, Upper Upscale",
  regions: ["North America", "Caribbean & Latin America"],
  specificMarkets: "US Southeast, Mexico",
  totalProperties: "20",
  totalRooms: "4500",
  brands: [],
  dataConfidenceLevel: "Medium",
  sourceType: ["Operator-provided"],
  lastUpdatedDate: "2026-05-25",
  brandsPortfolioDetail: JSON.stringify([{ brand: "Sample", keys: 100 }]),
  submission_status: "Active",
};

function loadJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function masterFieldsFromBody(body) {
  const fields = { company_name: String(body.companyName || "").trim() };
  const admin = [
    ["dataConfidenceLevel", "Data Confidence Level"],
    ["sourceType", "Source Type"],
    ["lastUpdatedDate", "Last Updated Date"],
  ];
  for (const [k, col] of admin) {
    const v = body[k];
    if (v != null && v !== "" && !(Array.isArray(v) && !v.length)) fields[col] = v;
  }
  return fields;
}

async function main() {
  const apply = process.argv.includes("--apply");
  const phaseB = loadJson(PHASE_B_PATH);
  const phaseBForms = new Set([
    ...(phaseB.rows || []).map((r) => r.form_name),
    ...(phaseB.masterWriterHardcoded || []).map((r) => r.form_name),
  ]);

  const payloads = buildNewBaseTablePayloads({
    body: SAMPLE_BODY,
    derived: { numberOfBrands: 0 },
    linkedBrandRecordIds: [],
    caseStudiesDetail: null,
    ownerDiligenceQa: null,
  });

  const masterPreview = masterFieldsFromBody(SAMPLE_BODY);

  console.log("=== Phase B new-base save coverage (dry-run) ===\n");
  console.log("Mode:", apply ? "APPLY (live write)" : "DRY-RUN only\n");

  console.log("Master (createOrUpdateOperatorMaster):");
  for (const [k, v] of Object.entries(masterPreview)) {
    console.log(`  ${k}:`, JSON.stringify(v));
  }

  for (const [table, fields] of Object.entries(payloads.oneToOne || {})) {
    const phaseRows = Object.entries(fields).filter(([col]) =>
      (phaseB.rows || []).some((r) => r.table_name === table && r.airtable_field_name === col)
    );
    if (!phaseRows.length && !Object.keys(fields).length) continue;
    console.log(`\n${table}:`);
    for (const [col, val] of Object.entries(fields)) {
      const hit = (phaseB.rows || []).find((r) => r.table_name === table && r.airtable_field_name === col);
      const tag = hit ? "[Phase B]" : "";
      console.log(`  ${tag} ${col}:`, JSON.stringify(val));
    }
  }

  const missing = [];
  for (const r of phaseB.rows || []) {
    const tablePayload = payloads.oneToOne[r.table_name] || {};
    let val = tablePayload[r.airtable_field_name];
    if (r.form_name === "regions" && r.airtable_field_name === "specificMarkets") {
      val = tablePayload.specificMarkets;
    }
    if (val == null || val === "") missing.push(`${r.table_name} :: ${r.airtable_field_name} (form ${r.form_name})`);
  }
  for (const r of phaseB.masterWriterHardcoded || []) {
    if (!masterPreview[r.airtable_field_name] && r.airtable_field_name !== "company_name") {
      missing.push(`Master :: ${r.airtable_field_name}`);
    }
  }

  console.log("\n--- Phase B form keys in sample ---");
  for (const f of phaseBForms) {
    const present = SAMPLE_BODY[f] != null && SAMPLE_BODY[f] !== "";
    console.log(`  ${f}: ${present ? "present" : "absent"}`);
  }

  if (missing.length) {
    console.log("\nWould NOT persist from sample (empty after coerce):");
    for (const m of missing) console.log("  -", m);
  } else {
    console.log("\nAll Phase B fields would persist from sample payload.");
  }

  if (apply) {
    if (!process.env.AIRTABLE_BASE_ID || !process.env.AIRTABLE_API_KEY) {
      console.error("Set AIRTABLE_BASE_ID and AIRTABLE_API_KEY for --apply");
      process.exit(1);
    }
    console.log("\nApplying write (test operator)...");
    const res = await writeOperatorSetupToNewBase({
      body: SAMPLE_BODY,
      existingRecordId: "",
      isDraft: true,
      correlationId: "phase-b-coverage-test",
    });
    console.log("Written master id:", res.recordId);
  } else {
    console.log("\nNo Airtable write (dry-run). Pass --apply only when instructed.");
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
