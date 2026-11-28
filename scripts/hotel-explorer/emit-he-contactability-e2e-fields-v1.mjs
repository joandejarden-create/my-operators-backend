#!/usr/bin/env node
/**
 * Emit Contactability Metrics V1 fields into the permanent HE E2E / factory report.
 * Does not call Findymail or Surfe. Uses in-memory people payload or empty baseline.
 */
import fs from "node:fs";
import path from "node:path";
import {
  computeContactabilityMetrics,
  formatContactabilityFactoryFields,
  HE_CONTACTABILITY_METRICS_V1,
} from "../../lib/hotel-intelligence/contact-intelligence/he-contactability-metrics-v1.js";

function arg(name, fallback = null) {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : fallback;
}

const hotelId = arg("hotelId", "recLuxvwwxID7U2B8");
const peoplePath = arg("people");
const outDir = arg(
  "outDir",
  path.join(process.cwd(), "reports", "hotel-explorer"),
);

let people = [];
if (peoplePath && fs.existsSync(peoplePath)) {
  const raw = JSON.parse(fs.readFileSync(peoplePath, "utf8"));
  people = Array.isArray(raw) ? raw : raw.people || raw.contacts || [];
}

const metrics = computeContactabilityMetrics(people);
const factoryFields = formatContactabilityFactoryFields(metrics);
const report = {
  version: HE_CONTACTABILITY_METRICS_V1,
  generatedAt: new Date().toISOString(),
  hotelId,
  metrics,
  factoryFields,
  rules: [
    "Verified named person without durable email/phone is NOT directly contactable",
    "Surfe/ephemeral channels never count toward contactability",
    "Findymail only with explicit authorization; Surfe only on explicit user click",
  ],
};

fs.mkdirSync(outDir, { recursive: true });
const outPath = path.join(outDir, `HE-CONTACTABILITY-METRICS-${hotelId}.json`);
fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
process.stdout.write(`${outPath}\n`);
