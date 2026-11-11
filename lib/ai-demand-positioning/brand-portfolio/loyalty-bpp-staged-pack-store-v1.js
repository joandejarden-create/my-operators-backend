/**
 * Staged LOYALTY_BPP packs — separate from customer-published Core peer-reuse packs.
 * Never writes config/client-share/bpp-customer-published-v1.json.
 */

import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
  readdirSync,
} from "fs";
import { join } from "path";
import { createHash } from "crypto";

export const LOYALTY_BPP_STAGED_PACK_STORE_V1 = "loyalty_bpp_staged_pack_store_v1";

function rootDir(cwd = process.cwd()) {
  return join(cwd, "data/ai-demand-positioning/bpp-loyalty-staged");
}

function propertyDir(propertyId, cwd = process.cwd()) {
  return join(rootDir(cwd), String(propertyId || "").trim());
}

function sha(obj) {
  return createHash("sha256").update(JSON.stringify(obj)).digest("hex");
}

export function saveLoyaltyBppStagedPack({
  propertyId,
  periodId,
  payload,
  certification = null,
  pipeline = null,
  cwd = process.cwd(),
} = {}) {
  const id = String(propertyId || "").trim();
  if (!id.startsWith("adp_") || !payload || typeof payload !== "object") {
    return { ok: false, error: "invalid_loyalty_bpp_pack_input" };
  }

  const dir = propertyDir(id, cwd);
  mkdirSync(dir, { recursive: true });

  const versionId = `loy_bpp_${Date.now().toString(36)}_${sha(payload).slice(0, 10)}`;
  const packFile = `pack-${versionId}.json`;
  const body = {
    storeVersion: LOYALTY_BPP_STAGED_PACK_STORE_V1,
    propertyId: id,
    periodId: periodId || payload.periodId || payload.measurement?.periodId || null,
    versionId,
    stagedAt: new Date().toISOString(),
    customerPublished: false,
    publicationAuthorized: false,
    measurementFamily: "LOYALTY_BPP",
    certification,
    pipeline,
    payload,
  };
  const artifactHash = sha(body);
  writeFileSync(join(dir, packFile), JSON.stringify(body, null, 2) + "\n", "utf8");

  const prior = loadLoyaltyBppStagedManifest(id, cwd);
  const manifest = {
    storeVersion: LOYALTY_BPP_STAGED_PACK_STORE_V1,
    propertyId: id,
    latestVersionId: versionId,
    periodId: body.periodId,
    packFile,
    artifactHash,
    certification,
    stagedAt: body.stagedAt,
    previousVersionId: prior?.latestVersionId || null,
    customerPublished: false,
  };
  writeFileSync(
    join(dir, "manifest.json"),
    JSON.stringify(manifest, null, 2) + "\n",
    "utf8"
  );

  return {
    ok: true,
    propertyId: id,
    versionId,
    artifactHash,
    path: join(dir, packFile),
    manifest,
  };
}

export function loadLoyaltyBppStagedManifest(propertyId, cwd = process.cwd()) {
  const p = join(propertyDir(propertyId, cwd), "manifest.json");
  if (!existsSync(p)) return null;
  try {
    return JSON.parse(readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

/**
 * Load latest staged Loyalty BPP payload (unwrapped), or null.
 */
export function loadLoyaltyBppStagedPack(propertyId, cwd = process.cwd()) {
  const manifest = loadLoyaltyBppStagedManifest(propertyId, cwd);
  if (!manifest?.packFile) return null;
  const packPath = join(propertyDir(propertyId, cwd), manifest.packFile);
  if (!existsSync(packPath)) return null;
  try {
    const doc = JSON.parse(readFileSync(packPath, "utf8"));
    const payload = doc.payload && typeof doc.payload === "object" ? doc.payload : doc;
    return {
      ok: true,
      source: "loyalty_bpp_staged",
      versionId: manifest.latestVersionId,
      artifactHash: manifest.artifactHash,
      periodId: manifest.periodId,
      certification: manifest.certification || doc.certification || null,
      pipeline: doc.pipeline || null,
      payload: {
        ...payload,
        customerPublished: false,
        _bppLoyaltyStagedVersionId: manifest.latestVersionId,
        _bppLoyaltyArtifactHash: manifest.artifactHash,
        _bppPackSource: "loyalty_bpp_staged",
      },
      manifest,
    };
  } catch {
    return null;
  }
}

export function listLoyaltyBppStagedPropertyIds(cwd = process.cwd()) {
  const root = rootDir(cwd);
  if (!existsSync(root)) return [];
  return readdirSync(root, { withFileTypes: true })
    .filter((d) => d.isDirectory() && d.name.startsWith("adp_"))
    .map((d) => d.name);
}
