/**
 * Sync ADP property-profile loader for Hotel Explorer Overview enrichment.
 * Read-only fixture access — no Airtable writes.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../..");
const PROFILES_DIR = path.join(REPO_ROOT, "fixtures", "ai-demand-positioning");

function readJsonSafe(filePath) {
  if (!fs.existsSync(filePath)) return null;
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return null;
  }
}

export function loadOverviewPropertyProfile(adpPropertyId) {
  const id = String(adpPropertyId || "").trim();
  if (!id) return null;
  if (!fs.existsSync(PROFILES_DIR)) return null;
  const files = fs
    .readdirSync(PROFILES_DIR)
    .filter((f) => f.endsWith("-property-profile.json"));
  for (const file of files) {
    const profile = readJsonSafe(path.join(PROFILES_DIR, file));
    if (profile?.propertyId === id) {
      return { file, profile };
    }
  }
  return null;
}

export function resolveAdpPropertyIdForCensusRecord(censusRecordId) {
  const id = String(censusRecordId || "").trim();
  if (!id) return null;
  const links = readJsonSafe(path.join(PROFILES_DIR, "census-links-v1.json"));
  for (const [adpId, link] of Object.entries(links?.links || {})) {
    if (link?.censusRecordId === id) return adpId;
  }
  return null;
}
