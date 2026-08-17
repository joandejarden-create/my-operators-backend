#!/usr/bin/env node
/**
 * Retry CDX for failed regions and merge into existing Hyatt directory extract.
 */
import { readFileSync, existsSync } from "node:fs";
import {
  harvestHyattCalaFromWaybackCdx,
  mergeHyattDirectoryRows,
  writeHyattDirectoryExtract,
} from "../lib/hyatt-brand-directory-extract.js";

const PATH = "reports/hyatt-cala-directory-extract.json";
const regions = (
  process.argv.find((a) => a.startsWith("--regions="))?.split("=")[1] ||
  "brazil,panama,jamaica,barbados,saint-lucia,grenada"
).split(",");

if (!existsSync(PATH)) throw new Error(`Missing ${PATH}`);
const existing = JSON.parse(readFileSync(PATH, "utf8"));

console.log("Retry CDX regions:", regions.join(", "));
const cdx = await harvestHyattCalaFromWaybackCdx({
  regions,
  delayMs: 1200,
  limitPerRegion: 500,
  fetchTimeoutMs: 45000,
  onProgress: (msg) => console.log(" ", msg),
});

const merged = mergeHyattDirectoryRows(existing.propertyRows || [], cdx.propertyRows || []);
writeHyattDirectoryExtract(PATH, {
  ...existing,
  generatedAt: new Date().toISOString(),
  cdxRetryMeta: { regions, locCount: cdx.locCount, unique: cdx.propertyRows.length, fetchLog: cdx.fetchLog },
  propertyIdConflicts: [
    ...(existing.propertyIdConflicts || []),
    ...(merged.propertyIdConflicts || []),
  ],
  uniqueProperties: merged.propertyRows.length,
  propertyRows: merged.propertyRows,
});

console.log("Was", existing.uniqueProperties, "→ now", merged.propertyRows.length);
