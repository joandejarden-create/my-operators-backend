/**
 * GDI Research-Stage Persistence V1
 *
 * Non-destructive persistence for research-memory, segment state, and local-shadow
 * bags. Separates research-stage storage from approved published inventory.
 *
 * Contract: GDI_RESEARCH_STAGE_PERSISTENCE_V1
 *
 * Does not write opportunities.json, Airtable, or customer publication.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";

export const GDI_RESEARCH_STAGE_PERSISTENCE_V1 =
  "gdi-research-stage-persistence-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

export const RESEARCH_STAGE_ROOT_REL =
  "data/group-demand-intelligence/research-stage";

export const BODY_STATUS = Object.freeze({
  ORIGINAL_BODY_AVAILABLE: "ORIGINAL_BODY_AVAILABLE",
  HISTORICAL_ATTESTATION_ONLY: "HISTORICAL_ATTESTATION_ONLY",
  SOURCE_EVIDENCE_AVAILABLE: "SOURCE_EVIDENCE_AVAILABLE",
  MISSING_ORIGINAL_RESEARCH_BODY: "MISSING_ORIGINAL_RESEARCH_BODY",
  FURTHER_RESEARCH_REQUIRED: "FURTHER_RESEARCH_REQUIRED",
});

export const RECOVERY_DISPOSITION = Object.freeze({
  ATTESTED_BODY_MISSING: "ATTESTED_BODY_MISSING",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  VENUE_INTELLIGENCE: "VENUE_INTELLIGENCE",
  OVERFLOW_WATCH: "OVERFLOW_WATCH",
  PARTNERSHIP_WATCH: "PARTNERSHIP_WATCH",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

export function researchStageHotelDir(hotelId, { root = ROOT } = {}) {
  return path.join(root, RESEARCH_STAGE_ROOT_REL, String(hotelId || "").trim());
}

export function researchStageMemoryDir(hotelId, { root = ROOT } = {}) {
  return path.join(researchStageHotelDir(hotelId, { root }), "research-memory");
}

export function researchStageCheckpointDir(hotelId, { root = ROOT } = {}) {
  return path.join(researchStageHotelDir(hotelId, { root }), "checkpoints");
}

export function researchStageAttestationBagPath(hotelId, { root = ROOT } = {}) {
  return path.join(
    researchStageHotelDir(hotelId, { root }),
    "historical-attestation-bag-v1.json"
  );
}

/**
 * Atomic write: temp file then rename. Windows EPERM/EEXIST → overwrite + cleanup.
 */
export function writeJsonAtomic(targetPath, obj) {
  fs.mkdirSync(path.dirname(targetPath), { recursive: true });
  const body = `${JSON.stringify(obj, null, 2)}\n`;
  const tmp = `${targetPath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, body);
  try {
    fs.renameSync(tmp, targetPath);
  } catch (err) {
    if (err?.code === "EPERM" || err?.code === "EEXIST" || err?.code === "EACCES") {
      try {
        fs.writeFileSync(targetPath, body);
      } finally {
        try {
          fs.rmSync(tmp, { force: true });
        } catch {
          /* ignore */
        }
      }
    } else {
      try {
        fs.rmSync(tmp, { force: true });
      } catch {
        /* ignore */
      }
      throw err;
    }
  }
  return { path: targetPath, bytes: Buffer.byteLength(body), sha256: sha256(body) };
}

/**
 * Snapshot an existing file into hotel checkpoints before mutation.
 * Returns null when source does not exist.
 */
export function checkpointExistingFile(filePath, hotelId, { root = ROOT, label = "pre-write" } = {}) {
  if (!filePath || !fs.existsSync(filePath)) return null;
  const raw = fs.readFileSync(filePath);
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const base = path.basename(filePath);
  const dest = path.join(
    researchStageCheckpointDir(hotelId, { root }),
    `${stamp}_${label}_${base}`
  );
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  fs.writeFileSync(dest, raw);
  return {
    checkpointPath: dest,
    sourcePath: filePath,
    sha256: sha256(raw),
    bytes: raw.length,
  };
}

function loadJsonSafe(p) {
  if (!p || !fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function asOppList(doc) {
  if (!doc) return [];
  if (Array.isArray(doc)) return doc;
  if (Array.isArray(doc.opportunities)) return doc.opportunities;
  if (Array.isArray(doc.items)) return doc.items;
  return [];
}

function oppId(o = {}) {
  return text(o.id || o.opportunityId || o.canonicalOpportunityId);
}

/**
 * Merge research-stage / local-shadow opportunity bags by stable ID.
 * Never drops prior IDs unless allowDropIds is explicitly provided.
 */
export function mergeOpportunityBagsById({
  existing = [],
  incoming = [],
  allowDropIds = null,
  protectedIds = [],
  mode = "merge",
} = {}) {
  const priorIds = new Set(
    existing.map((o) => oppId(o)).filter(Boolean)
  );
  const byId = new Map();

  if (mode === "replace") {
    // Explicit replace: only incoming IDs survive — gated by protectedIds.
    for (const o of incoming) {
      const id = oppId(o);
      if (!id) continue;
      byId.set(id, { ...o, id });
    }
  } else {
    for (const o of existing) {
      const id = oppId(o);
      if (!id) continue;
      byId.set(id, o);
    }
    for (const o of incoming) {
      const id = oppId(o);
      if (!id) continue;
      const prev = byId.get(id);
      byId.set(id, prev ? { ...prev, ...o, id } : { ...o, id });
    }
  }

  const nextIds = new Set(byId.keys());
  const dropped = [...priorIds].filter((id) => !nextIds.has(id));
  const protectedDropped = dropped.filter((id) =>
    (protectedIds || []).map(String).includes(String(id))
  );
  const allow = allowDropIds == null ? null : new Set([...allowDropIds].map(String));

  if (protectedDropped.length) {
    return {
      ok: false,
      code: "PROTECTED_RESEARCH_IDS_WOULD_BE_DROPPED",
      protectedDropped,
      dropped,
      opportunities: existing,
      mode,
    };
  }
  if (mode === "replace" && allow && dropped.some((id) => !allow.has(String(id)))) {
    return {
      ok: false,
      code: "UNEXPECTED_ID_DROP_WITHOUT_ALLOWLIST",
      dropped: dropped.filter((id) => !allow.has(String(id))),
      opportunities: existing,
      mode,
    };
  }
  if (mode === "replace" && !allow && dropped.length && !(protectedIds || []).length) {
    // Replace without protectedIds and without allowlist still keeps prior via merge fallback.
    for (const o of existing) {
      const id = oppId(o);
      if (id && !byId.has(id)) byId.set(id, o);
    }
  }

  return {
    ok: true,
    code: mode === "replace" ? "REPLACED" : "MERGED",
    dropped: mode === "replace" ? dropped : [],
    added: [...nextIds].filter((id) => !priorIds.has(id)),
    opportunities: [...byId.values()],
    mode,
  };
}

/**
 * Persist a local-shadow / research-stage opportunity bag with checkpoint + merge.
 * Refuses destructive full replacement that would erase prior research IDs.
 */
export function persistResearchStageOpportunityBag({
  hotelId,
  targetPath,
  incomingDoc,
  protectedIds = [],
  root = ROOT,
  label = "shadow-merge",
  mode = "merge",
} = {}) {
  if (!hotelId) throw new Error("hotelId required");
  if (!targetPath) throw new Error("targetPath required");

  const existingDoc = loadJsonSafe(targetPath);
  const existing = asOppList(existingDoc);
  const incoming = asOppList(incomingDoc);
  const checkpoint = checkpointExistingFile(targetPath, hotelId, { root, label });

  // Default is merge. Explicit replace simulates the historical destructive
  // writeJson(shadowPath, { opportunities: thisRunOnly }) failure mode and
  // is rejected when protected research IDs would disappear.
  const merged = mergeOpportunityBagsById({
    existing,
    incoming,
    protectedIds,
    mode,
  });
  if (!merged.ok) {
    return {
      ok: false,
      ...merged,
      checkpoint,
      targetPath,
      hotelStoreWrites: false,
      publication: false,
    };
  }

  const nextDoc = {
    ...(existingDoc && typeof existingDoc === "object" && !Array.isArray(existingDoc)
      ? existingDoc
      : {}),
    ...(incomingDoc && typeof incomingDoc === "object" && !Array.isArray(incomingDoc)
      ? incomingDoc
      : {}),
    version: incomingDoc?.version || existingDoc?.version || GDI_RESEARCH_STAGE_PERSISTENCE_V1,
    hotelId,
    updatedAt: new Date().toISOString(),
    persistence: GDI_RESEARCH_STAGE_PERSISTENCE_V1,
    doesNotAffectProduction: true,
    publication: "NOT_AUTHORIZED",
    customerFacing: false,
    opportunities: merged.opportunities,
    _mergeMeta: {
      priorCount: existing.length,
      incomingCount: incoming.length,
      mergedCount: merged.opportunities.length,
      added: merged.added,
      checkpointPath: checkpoint?.checkpointPath || null,
    },
  };

  const written = writeJsonAtomic(targetPath, nextDoc);
  return {
    ok: true,
    code: "PERSISTED_MERGE",
    checkpoint,
    written,
    targetPath,
    added: merged.added,
    count: merged.opportunities.length,
    hotelStoreWrites: false,
    publication: false,
  };
}

/**
 * Union prior segment candidate IDs with corpus assignment — never FS-only replace.
 */
export function mergeSegmentCandidateIds({
  priorCandidateIds = [],
  priorCompletedIds = [],
  priorUnresolvedIds = [],
  corpusPrimaryIds = [],
  corpusSecondaryIds = [],
} = {}) {
  const candidateIds = [
    ...new Set([
      ...corpusPrimaryIds.map(String),
      ...priorCandidateIds.map(String),
      ...priorUnresolvedIds.map(String),
      ...priorCompletedIds.map(String),
    ]),
  ].filter(Boolean);

  const secondaryAssociationIds = [
    ...new Set(corpusSecondaryIds.map(String).filter(Boolean)),
  ];

  return {
    candidateIds,
    secondaryAssociationIds,
    preservedFromPrior: priorCandidateIds.filter(
      (id) => id && !corpusPrimaryIds.map(String).includes(String(id))
    ),
    corpusOnly: corpusPrimaryIds.filter(
      (id) => id && !priorCandidateIds.map(String).includes(String(id))
    ),
  };
}

/**
 * Atomic segment-state save (replaces non-atomic writeFileSync).
 */
export function saveSegmentResearchStateAtomic(dir, state) {
  fs.mkdirSync(dir, { recursive: true });
  const seg = String(state.segmentId || state.segment || "unknown")
    .toLowerCase()
    .replace(/[^\w.-]+/g, "_");
  const file = path.join(dir, `${seg}.json`);
  const next = { ...state, updatedAt: new Date().toISOString() };
  writeJsonAtomic(file, next);
  return file;
}

/**
 * Reject accidental deletion of historical research-memory JSON files.
 */
export function assertResearchMemoryNotErased({
  beforeIds = [],
  afterIds = [],
  protectedIds = [],
} = {}) {
  const after = new Set(afterIds.map(String));
  const missingProtected = protectedIds
    .map(String)
    .filter((id) => beforeIds.map(String).includes(id) && !after.has(id));
  if (missingProtected.length) {
    return {
      ok: false,
      code: "HISTORICAL_RESEARCH_MEMORY_ERASURE_REJECTED",
      missingProtected,
    };
  }
  const missingAny = beforeIds.map(String).filter((id) => !after.has(id));
  return {
    ok: missingAny.length === 0,
    code: missingAny.length ? "RESEARCH_IDS_MISSING_AFTER_SYNC" : "OK",
    missingAny,
  };
}

/**
 * Restore research-memory files from the newest matching checkpoint when present.
 */
export function restoreResearchMemoryFromCheckpoint({
  hotelId,
  opportunityId,
  root = ROOT,
} = {}) {
  const memDir = researchStageMemoryDir(hotelId, { root });
  const target = path.join(
    memDir,
    `${String(opportunityId || "x").replace(/[^\w.-]+/g, "_")}.json`
  );
  if (fs.existsSync(target)) {
    return { ok: true, code: "ALREADY_PRESENT", path: target };
  }
  const cpDir = researchStageCheckpointDir(hotelId, { root });
  if (!fs.existsSync(cpDir)) {
    return { ok: false, code: "NO_CHECKPOINT_DIR" };
  }
  const needle = `${String(opportunityId || "").replace(/[^\w.-]+/g, "_")}.json`;
  const matches = fs
    .readdirSync(cpDir)
    .filter((f) => f.endsWith(needle) || f.includes(needle))
    .sort()
    .reverse();
  if (!matches.length) return { ok: false, code: "NO_CHECKPOINT_MATCH" };
  const src = path.join(cpDir, matches[0]);
  fs.mkdirSync(memDir, { recursive: true });
  fs.copyFileSync(src, target);
  return { ok: true, code: "RESTORED_FROM_CHECKPOINT", path: target, from: src };
}
