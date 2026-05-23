#!/usr/bin/env node
/**
 * Remove demo disclaimers from CALA fixture field values (and re-normalize).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  sanitizeDemoIntakeCopy,
  sanitizeDemoIntakeFields,
  sanitizeDemoIntakeDeep,
} from "../lib/demo-intake-copy-sanitize.js";
import { normalizeDealSetupFields } from "../lib/deal-setup-form-value-normalize.js";

const DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "fixtures", "sample-deals");

const FILES = [
  "proyecto-reforma-urban-conversion.example.json",
  "playa-dorada-resort-repositioning.example.json",
  "cartagena-walled-city-collection.example.json",
  "merida-centro-select-service.example.json",
  "san-juan-bay-turnaround.example.json",
  "panama-city-mixed-use-hotel-component.example.json",
  "aeropuerto-cancun-select-service.example.json",
  "cusco-heritage-palace-hotel.example.json",
  "colonial-city-lifestyle-conversion.example.json",
  "riviera-maya-wellness-resort-repositioning.example.json",
  "andean-business-hotel-reflag.example.json",
  "cascadas-lifestyle-hotel-component.example.json",
];

function sanitizeEmail(email) {
  const s = String(email || "");
  return s.replace(/@dealality\.sample$/i, "@hospitalitygroup.com");
}

for (const file of FILES) {
  const filePath = path.join(DIR, file);
  let record = JSON.parse(fs.readFileSync(filePath, "utf8"));
  record = /** @type {typeof record} */ (sanitizeDemoIntakeDeep(record));

  for (const layer of ["referenceProperty", "fictionalDeal"]) {
    const block = record[layer];
    if (!block?.fields) continue;
    block.fields = normalizeDealSetupFields(sanitizeDemoIntakeFields(block.fields)).fields;
  }

  if (record.fictionalDeal?.fields?.["Email Address"]) {
    record.fictionalDeal.fields["Email Address"] = sanitizeEmail(
      record.fictionalDeal.fields["Email Address"]
    );
  }

  record.disclaimer = "";

  if (record.airtableRows) {
    for (const row of record.airtableRows) {
      if (row.field === "Deal Status") row.value = process.env.CALA_SAMPLE_DEALS_STATUS || "In Review";
    }
  }

  if (record.meta?.createdFor) {
    record.meta.createdFor = "CALA opportunity set for brand and operator review workflows";
  }

  fs.writeFileSync(filePath, JSON.stringify(record, null, 2) + "\n", "utf8");
  console.log("Sanitized", file);
}

console.log("Done.");
