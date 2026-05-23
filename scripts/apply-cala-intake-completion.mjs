#!/usr/bin/env node
/**
 * Enrich CALA sample-deal fixtures with completed intake fields + normalize.
 * Usage: node scripts/apply-cala-intake-completion.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { applyCalaIntakeCompletion } from "../lib/cala-sample-intake-completion.js";
import { normalizeDealSetupFields } from "../lib/deal-setup-form-value-normalize.js";
import { sanitizeDemoIntakeCopy, sanitizeDemoIntakeFields } from "../lib/demo-intake-copy-sanitize.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DIR = path.join(__dirname, "..", "fixtures", "sample-deals");

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

/** @param {object} record */
function cfgFromRecord(record) {
  const f = record.fictionalDeal?.fields || {};
  return {
    projectName: record.fictionalDeal?.projectName || f["Project Name"],
    projectType: f["Project Type"],
    stage: f["Stage of Development"],
    openingDate: f["Expected Opening or Rebranding Date"],
    country: f.Country,
    market: record.meta?.market,
    cityState: f["City & State"],
    keys: f["Total Number of Rooms/Keys"],
    fbCount: f["Number of F&B Outlets"],
    currentlyBranded: f["Is the hotel currently branded?"],
    currentlyManaged: f["Is the hotel currently managed by a third-party operator?"],
    operatorPlan: f["Plan to Self-Manage or Hire Third Party?"],
    operatorCurrent: f["Operator Name Current"],
    workedWithPreferred: f["Have you worked with any of your preferred brands/operators before?"],
    hotelType: f["Hotel Type"],
    serviceModel: f["Hotel Service Model"],
    buildingType: f["Building Type"],
    submarket: f["Hotel Submarket & Location"],
    siteRestrictions: f["Site/Development Restrictions?"],
    pipStatus: f["PIP / CapEx Status"],
    revpar: f["Estimated or Actual RevPAR"],
    broker: f["Working with Broker/Advisor?"],
    priorities: f["Top Priorities for Project"],
    concerns: f["Top Concerns for this Project"],
    dealBreakers: f["Top 3 Deal Breakers"],
    mustHaves: f["Must-haves From Brand or Operator"],
    targetGuest: f["Target Guest Segment"],
    intentionalRevparGap: !f["Estimated or Actual RevPAR"],
    intentionalGaps: record.intentionalGaps || [],
    currentBrand: f["Current Brand Affiliation"],
    siteSize: f["Total Site Size"],
    siteSizeUnit: f["Total Site Size Unit"],
    priorFranchise:
      f[
        "Has there ever been a franchise, branded management, affiliation or similar agreement pertaining to the proposed hotel or site?"
      ],
  };
}

for (const file of FILES) {
  const filePath = path.join(DIR, file);
  const record = JSON.parse(fs.readFileSync(filePath, "utf8"));
  const cfg = cfgFromRecord(record);
  for (const layer of ["referenceProperty", "fictionalDeal"]) {
    const block = record[layer];
    if (!block?.fields) continue;
    applyCalaIntakeCompletion(block.fields, cfg);
    const { fields } = normalizeDealSetupFields(block.fields);
    for (const k of [
      "Contact Source",
      "Main Contact Title",
      "Secondary Contact",
      "Best Time or Method to Reach",
      "What makes this opportunity stand out to a brand or operator?",
      "Additional Notes or Unique Project Aspects",
      "Anything else you'd like to add?",
      "Would you like to meet consultants?",
      "Other Projects Nearing Contract Expiration?",
      "Broker/Advisor Company and Contract Details",
    ]) {
      delete fields[k];
    }
    block.fields = sanitizeDemoIntakeFields(fields);
  }

  if (record.fictionalDeal?.ownerEntity) {
    record.fictionalDeal.ownerEntity = sanitizeDemoIntakeCopy(record.fictionalDeal.ownerEntity);
  }
  fs.writeFileSync(filePath, JSON.stringify(record, null, 2) + "\n", "utf8");
  console.log("Enriched", file);
}

console.log("Done —", FILES.length, "fixtures");
