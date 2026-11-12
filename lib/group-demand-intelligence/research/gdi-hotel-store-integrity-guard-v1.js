/**
 * GDI hotel store integrity guard V1.
 *
 * Prevents silent loss of richer local operational hotel stores
 * (e.g. Bethesda 59 → 54 after git checkout replaced local with committed snapshot).
 *
 * Does not write opportunities.json unless explicitly authorized.
 */

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const GDI_HOTEL_STORE_INTEGRITY_GUARD_V1 =
  "gdi-hotel-store-integrity-guard-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

/** Bethesda Scaling Companies IDs lost in the 59→54 regression (verified 2026-11-12). */
export const BETHESDA_PROTECTED_SCALING_IDS = Object.freeze([
  "gdi_sc_b36b66547864e8ce", // Matthews
  "gdi_sc_a5768a99d4e89875", // IMC Construction
  "gdi_sc_e7dd46732dd5bb7a", // BioHub Maryland
  "gdi_sc_5c5d41eb670e6c7b", // Miles & Stockbridge
  "gdi_sc_9d14aee7ff250b11", // REGENXBIO
]);

export const BETHESDA_HOTEL_ID = "recLuxvwwxID7U2B8";
export const BETHESDA_EXPECTED_CANONICAL_COUNT = 59;
export const BETHESDA_EXPECTED_HISTORICAL_COUNT = 29;
export const BETHESDA_EXPECTED_UNION_COUNT = 88;

export const VERIFIED_59_ROW_SHA256 =
  "fd438549ff030ffc6f219469a526ec9b88ad8a704871fa01784d5650468226ad";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

export function hotelOpportunitiesPath(hotelId, { root = ROOT } = {}) {
  return path.join(
    root,
    "data/group-demand-intelligence/hotels",
    String(hotelId),
    "opportunities.json"
  );
}

export function sha256File(filePath) {
  return crypto.createHash("sha256").update(fs.readFileSync(filePath)).digest("hex");
}

export function loadHotelOpportunityDoc(hotelId, { root = ROOT } = {}) {
  const p = hotelOpportunitiesPath(hotelId, { root });
  if (!fs.existsSync(p)) return { path: p, exists: false, opportunities: [] };
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  const opportunities = Array.isArray(doc)
    ? doc
    : Array.isArray(doc.opportunities)
      ? doc.opportunities
      : [];
  return {
    path: p,
    exists: true,
    doc,
    opportunities,
    ids: opportunities.map((r) => text(r.id || r.opportunityId)).filter(Boolean),
    sha256: sha256File(p),
    count: opportunities.length,
  };
}

export function assertNoProtectedIdLoss({
  hotelId,
  nextOpportunities = [],
  protectedIds = [],
} = {}) {
  const nextIds = new Set(
    (nextOpportunities || []).map((r) => text(r.id || r.opportunityId)).filter(Boolean)
  );
  const missing = (protectedIds || []).filter((id) => !nextIds.has(text(id)));
  if (missing.length) {
    return {
      ok: false,
      code: "PROTECTED_ID_LOSS_REFUSED",
      hotelId,
      missing,
    };
  }
  return { ok: true, code: "PROTECTED_IDS_PRESENT", hotelId };
}

export function assertCountNotDropped({
  hotelId,
  priorCount,
  nextCount,
  allowDrop = false,
} = {}) {
  if (allowDrop) return { ok: true, code: "DROP_EXPLICITLY_ALLOWED" };
  if (Number(nextCount) < Number(priorCount)) {
    return {
      ok: false,
      code: "OPPORTUNITY_COUNT_DROP_REFUSED",
      hotelId,
      priorCount,
      nextCount,
      dropped: Number(priorCount) - Number(nextCount),
    };
  }
  return { ok: true, code: "COUNT_OK", priorCount, nextCount };
}

/**
 * Backup a hotel opportunities.json before any authorized mutation.
 */
export function backupHotelOpportunityStore(
  hotelId,
  { root = ROOT, label = "pre-mutation" } = {}
) {
  const loaded = loadHotelOpportunityDoc(hotelId, { root });
  if (!loaded.exists) {
    return { ok: false, code: "STORE_ABSENT", path: loaded.path };
  }
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const backupDir = path.join(
    root,
    "data/group-demand-intelligence/hotel-store-backups",
    String(hotelId)
  );
  fs.mkdirSync(backupDir, { recursive: true });
  const backupPath = path.join(
    backupDir,
    `${label}-${stamp}-${loaded.sha256.slice(0, 12)}.json`
  );
  fs.copyFileSync(loaded.path, backupPath);
  return {
    ok: true,
    code: "BACKED_UP",
    hotelId,
    sourcePath: loaded.path,
    backupPath,
    sha256: loaded.sha256,
    count: loaded.count,
  };
}

/**
 * Dry-run merge: add missing protected bodies into current store without writing.
 */
export function dryRunMergeProtectedBodies({
  hotelId = BETHESDA_HOTEL_ID,
  recoveredBodies = [],
  protectedIds = BETHESDA_PROTECTED_SCALING_IDS,
  root = ROOT,
} = {}) {
  const loaded = loadHotelOpportunityDoc(hotelId, { root });
  if (!loaded.exists) {
    return { ok: false, code: "STORE_ABSENT", path: loaded.path };
  }
  const byId = new Map(
    loaded.opportunities.map((r) => [text(r.id || r.opportunityId), r])
  );
  const added = [];
  const skippedPresent = [];
  for (const body of recoveredBodies) {
    const id = text(body?.id || body?.opportunityId);
    if (!id) continue;
    if (!protectedIds.includes(id)) {
      return {
        ok: false,
        code: "BODY_NOT_ON_PROTECTED_ALLOWLIST",
        id,
      };
    }
    if (byId.has(id)) {
      skippedPresent.push(id);
      continue;
    }
    byId.set(id, body);
    added.push(id);
  }
  const missingStill = protectedIds.filter((id) => !byId.has(id));
  const opportunities = [...byId.values()];
  const countGate = assertCountNotDropped({
    hotelId,
    priorCount: loaded.count,
    nextCount: opportunities.length,
  });
  const idGate = assertNoProtectedIdLoss({
    hotelId,
    nextOpportunities: opportunities,
    protectedIds,
  });
  return {
    ok: countGate.ok && idGate.ok && missingStill.length === 0,
    code:
      missingStill.length === 0
        ? "DRY_RUN_MERGE_READY"
        : "DRY_RUN_MERGE_INCOMPLETE_MISSING_BODIES",
    hotelId,
    priorCount: loaded.count,
    priorSha256: loaded.sha256,
    proposedCount: opportunities.length,
    added,
    skippedPresent,
    missingStill,
    countGate,
    idGate,
    expectedCanonical: BETHESDA_EXPECTED_CANONICAL_COUNT,
    expectedUnion: BETHESDA_EXPECTED_UNION_COUNT,
    writeAuthorized: false,
    opportunities, // caller may persist only with explicit founder approval
  };
}

/**
 * Detect if a candidate/replacement store would drop IDs present locally.
 */
export function compareLocalVsCandidateStore({
  hotelId,
  candidateOpportunities = [],
  protectedIds = BETHESDA_PROTECTED_SCALING_IDS,
  root = ROOT,
} = {}) {
  const local = loadHotelOpportunityDoc(hotelId, { root });
  const candidateIds = new Set(
    (candidateOpportunities || [])
      .map((r) => text(r.id || r.opportunityId))
      .filter(Boolean)
  );
  const localOnly = (local.ids || []).filter((id) => !candidateIds.has(id));
  const protectedLost = protectedIds.filter((id) => local.ids.includes(id) && !candidateIds.has(id));
  const richerLocal =
    local.count > (candidateOpportunities || []).length || protectedLost.length > 0;
  return {
    hotelId,
    localCount: local.count,
    candidateCount: (candidateOpportunities || []).length,
    localOnlyIds: localOnly,
    protectedLost,
    richerLocal,
    refuseReplace: richerLocal,
    code: richerLocal ? "REFUSE_REPLACE_RICHER_LOCAL" : "CANDIDATE_OK",
  };
}

export function listTrackedHotelOpportunityRelPaths({ root = ROOT } = {}) {
  const hotelsDir = path.join(root, "data/group-demand-intelligence/hotels");
  if (!fs.existsSync(hotelsDir)) return [];
  return fs
    .readdirSync(hotelsDir, { withFileTypes: true })
    .filter((d) => d.isDirectory())
    .map((d) => path.join("data/group-demand-intelligence/hotels", d.name, "opportunities.json"))
    .filter((rel) => fs.existsSync(path.join(root, rel)));
}
