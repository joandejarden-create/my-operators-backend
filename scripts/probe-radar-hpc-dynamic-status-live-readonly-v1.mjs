#!/usr/bin/env node
/**
 * Read-only live probe for HE-001 dynamic HPC Radar Status retrieval.
 * Loads credentials from workspace/.env or primary checkout .env.
 * No Airtable writes.
 *
 * Usage: node scripts/probe-radar-hpc-dynamic-status-live-readonly-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  clearRadarHpcStatusCache,
  HPC_SELECT_FIELDS,
  loadAuthorizedInternalPreviewHotels,
} from "../lib/hotel-intelligence/radar-internal-preview-hotels-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function loadEnv(file) {
  const out = {};
  if (!fs.existsSync(file)) return out;
  for (const line of fs.readFileSync(file, "utf8").split(/\r?\n/)) {
    const t = line.trim();
    if (!t || t.startsWith("#")) continue;
    const i = t.indexOf("=");
    if (i < 0) continue;
    const k = t.slice(0, i).trim();
    let v = t.slice(i + 1).trim();
    if (
      (v.startsWith('"') && v.endsWith('"')) ||
      (v.startsWith("'") && v.endsWith("'"))
    ) {
      v = v.slice(1, -1);
    }
    out[k] = v;
  }
  return out;
}

const env = {
  ...loadEnv(path.join("C:/Dev/dealality/my-operators-backend", ".env")),
  ...loadEnv(path.join(ROOT, ".env")),
};
for (const [k, v] of Object.entries(env)) {
  if (process.env[k] == null || process.env[k] === "") process.env[k] = v;
}

clearRadarHpcStatusCache();

const loaded = await loadAuthorizedInternalPreviewHotels({
  dealalityUser: { isAdmin: true },
  req: { headers: { "x-dealality-owner-app": "1" } },
  bypassCache: true,
});

const ids = (loaded.hotels || []).map((h) => h.id).sort();
const report = {
  ok: Boolean(loaded.ok) && !(loaded.errors || []).length,
  selectFieldCount: HPC_SELECT_FIELDS.length,
  hotelCount: (loaded.hotels || []).length,
  openCount: loaded.diagnostics?.openCount ?? null,
  pipelineCount: loaded.diagnostics?.pipelineCount ?? null,
  paginationComplete: loaded.diagnostics?.paginationComplete ?? null,
  pageCount: loaded.diagnostics?.pageCount ?? null,
  lastSuccessfulFetchAt: loaded.diagnostics?.lastSuccessfulFetchAt ?? null,
  errors: loaded.errors || [],
  hotelIds: ids,
  sample: (loaded.hotels || []).slice(0, 5).map((h) => ({
    id: h.id,
    name: h.name,
    status: h.status,
    lat: h.lat,
    lng: h.lng,
  })),
};

console.log(JSON.stringify(report, null, 2));
process.exit(report.ok ? 0 : 1);
