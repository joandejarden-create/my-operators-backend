#!/usr/bin/env node
/**
 * Write Mexico Cancún excluded record classification report.
 */
import { writeFileSync, mkdirSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildExcludedClassificationReport } from "../lib/radar-buildout/mexico-cancun-excluded-record-classification.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const output = join(root, "data/mexico-cancun-excluded-record-classification.json");

const report = buildExcludedClassificationReport();
const outDir = dirname(output);
if (!existsSync(outDir)) mkdirSync(outDir, { recursive: true });
writeFileSync(output, JSON.stringify(report, null, 2) + "\n");

console.log("Excluded classification written:", output);
console.log("Requery POI:", report.summary.requery_poi);
console.log("Manual corridor:", report.summary.manual_corridor);
console.log("Remove/defer:", report.summary.remove_defer);
console.log("Delta included:", report.summary.deltaIncluded);
