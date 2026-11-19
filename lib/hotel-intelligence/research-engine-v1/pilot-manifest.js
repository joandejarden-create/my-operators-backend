/**
 * Frozen HE-INTEL-001 pilot manifest loader.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const PILOT_MANIFEST_REL =
  "fixtures/hotel-explorer/HE-INTEL-001-pilot-16-manifest-frozen-v1.json";

const HPC_RE = /^rec[A-Za-z0-9]{14}$/;

export function loadPilotManifest(opts = {}) {
  const filePath =
    opts.path ||
    path.join(ROOT, PILOT_MANIFEST_REL);
  const raw = JSON.parse(fs.readFileSync(filePath, "utf8"));
  const hotels = Array.isArray(raw.hotels) ? raw.hotels : [];
  const byHpc = new Map();
  const distinct = [];
  for (const h of hotels) {
    const id = String(h.hpcId || "").trim();
    if (!HPC_RE.test(id)) continue;
    if (!byHpc.has(id)) {
      byHpc.set(id, []);
      distinct.push(id);
    }
    byHpc.get(id).push(h);
  }
  return {
    ok: true,
    manifestId: raw.manifestId || null,
    status: raw.status || null,
    founderPropertyCount: Number(raw.founderPropertyCount || hotels.length),
    hpcDistinctRecordCount: Number(raw.hpcDistinctRecordCount || distinct.length),
    hyattDualHouseSingleRecord: Boolean(raw.hyattDualHouseSingleRecord),
    goldenDemoExcluded: new Set(raw.goldenDemoExcluded || ["recUNycnMwOVFX0hc"]),
    hotels,
    distinctHpcIds: distinct,
    byHpc,
    path: filePath,
  };
}

export function resolvePilotHotel(hpcId, manifest = null) {
  const m = manifest || loadPilotManifest();
  const id = String(hpcId || "").trim();
  if (!HPC_RE.test(id)) {
    return { ok: false, reason: "INPUT_INVALID", hpcId: id };
  }
  if (m.goldenDemoExcluded.has(id)) {
    return { ok: false, reason: "GOLDEN_DEMO_ISOLATION", hpcId: id };
  }
  const entries = m.byHpc.get(id);
  if (!entries?.length) {
    return { ok: false, reason: "NOT_IN_PILOT_MANIFEST", hpcId: id };
  }
  return {
    ok: true,
    canonicalHpcId: id,
    entries,
    primary: entries[0],
    founderExperienceCount: entries.length,
    dualHouse: entries.length > 1,
  };
}

export function listCanonicalPilotHpcIds(manifest = null) {
  const m = manifest || loadPilotManifest();
  return [...m.distinctHpcIds];
}
