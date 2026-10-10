/**
 * Packet 2.8B-2 — Organization-level OwnerPortfolio store.
 * Profiles keyed by owner_entity_id — not hotel cohort arrays.
 *
 * Startup recovery V2:
 * - Idempotent skip when profile+graph unchanged (ignore written_at)
 * - Windows-safe sibling temp write + replace (no blind overwrite)
 * - Once-per-process materialization singleton
 * - Fail-safe: keep existing valid fixture if rewrite fails
 */

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { compileGsfOwnerPortfolioFromEvidence, GSF_OWNER_ENTITY_ID, GSF_SLUG } from "./compilers/gsf-from-evidence.js";
import {
  compileCambridgeOwnerPortfolioFromEvidence,
  DOVETAIL_ENTITY_ID,
} from "./compilers/cambridge-from-evidence.js";
import {
  compileSheratonHnfOwnerPortfolioFromEvidence,
  compileVocoAllianceOwnerPortfolioFromEvidence,
  HNF_ENTITY_ID,
  ALLIANCE_ENTITY_ID,
} from "./compilers/mexico-from-evidence.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../../..");
const FIXTURE_DIR = path.join(ROOT, "fixtures/hotel-intelligence/owner-portfolio");

const cache = new Map();

/** @type {string[]|null} */
let goldenOwnerPortfoliosMaterializedIds = null;
/** In-flight sync guard — prevents re-entrant concurrent materialization in one process. */
let goldenOwnerPortfoliosMaterializing = false;
let goldenMaterializationInitCount = 0;
let goldenMaterializationDegraded = false;

function ensureFixtureDir() {
  if (!fs.existsSync(FIXTURE_DIR)) fs.mkdirSync(FIXTURE_DIR, { recursive: true });
}

function fixturePath(ownerEntityId) {
  const safe = String(ownerEntityId || "").replace(/[^a-zA-Z0-9_-]/g, "_");
  return path.join(FIXTURE_DIR, `${safe}.json`);
}

/** Strip volatile timestamps so identical commercial content hashes equal. */
function stripVolatile(value) {
  if (Array.isArray(value)) return value.map(stripVolatile);
  if (!value || typeof value !== "object") return value;
  const out = {};
  for (const [k, v] of Object.entries(value)) {
    if (k === "compiled_at" || k === "written_at" || k === "generated_at") continue;
    out[k] = stripVolatile(v);
  }
  return out;
}

function stableCanonicalBody(profile, graph) {
  return JSON.stringify({
    profile: stripVolatile(profile || null),
    graph: stripVolatile(graph || null),
  });
}

function hashText(text) {
  return crypto.createHash("sha256").update(String(text || ""), "utf8").digest("hex").slice(0, 16);
}

function readExistingPayload(targetPath) {
  if (!fs.existsSync(targetPath)) return null;
  try {
    const raw = fs.readFileSync(targetPath, "utf8");
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || !parsed.profile?.owner_entity_id) {
      return { ok: false, raw, parsed: null, error: "invalid_fixture_shape" };
    }
    return { ok: true, raw, parsed };
  } catch (err) {
    return { ok: false, raw: null, parsed: null, error: err?.message || String(err) };
  }
}

/**
 * Windows-safe write: sibling temp → fsync → rename; fallback overwrite with retry.
 * Prefer not to unlink+rename unless rename fails with EEXIST/EPERM/EACCES.
 */
function safeWriteJsonFile(targetPath, contents) {
  ensureFixtureDir();
  const dir = path.dirname(targetPath);
  const base = path.basename(targetPath);
  const tmp = path.join(
    dir,
    `.${base}.${process.pid}.${Date.now()}.${crypto.randomBytes(3).toString("hex")}.tmp`
  );
  fs.writeFileSync(tmp, contents, "utf8");
  try {
    const fd = fs.openSync(tmp, "r+");
    try {
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
  } catch {
    /* fsync best-effort */
  }

  try {
    fs.renameSync(tmp, targetPath);
    return { method: "rename" };
  } catch (renameErr) {
    const code = renameErr?.code;
    if (code === "EEXIST" || code === "EPERM" || code === "EACCES" || process.platform === "win32") {
      let lastErr = renameErr;
      for (let i = 0; i < 8; i++) {
        try {
          fs.writeFileSync(targetPath, contents, "utf8");
          try {
            fs.unlinkSync(tmp);
          } catch {
            /* best-effort */
          }
          return { method: "overwrite_retry", attempts: i + 1 };
        } catch (err) {
          lastErr = err;
          const until = Date.now() + 50 * (i + 1);
          while (Date.now() < until) {
            /* brief backoff */
          }
        }
      }
      try {
        fs.unlinkSync(tmp);
      } catch {
        /* best-effort */
      }
      throw lastErr;
    }
    try {
      fs.unlinkSync(tmp);
    } catch {
      /* best-effort */
    }
    throw renameErr;
  }
}

function logPortfolioWriteDiag(diag) {
  console.warn(
    "[owner-portfolio-store]",
    JSON.stringify({
      event: "owner_portfolio_write",
      pid: process.pid,
      ...diag,
    })
  );
}

/**
 * Persist owner portfolio. Skips rewrite when canonical profile+graph unchanged.
 * On write failure with a valid existing fixture: degrade (keep existing), do not throw.
 *
 * @returns {{ payload: object, skipped: boolean, wrote: boolean, degraded?: boolean }}
 */
export function writeOwnerPortfolioProfile(profile, graph = null, opts = {}) {
  if (!profile?.owner_entity_id) throw new Error("owner_entity_id_required");
  ensureFixtureDir();
  const ownerId = profile.owner_entity_id;
  const targetPath = fixturePath(ownerId);
  const nextBody = stableCanonicalBody(profile, graph);
  const nextHash = hashText(nextBody);
  const existing = readExistingPayload(targetPath);
  const targetExists = Boolean(existing);
  const targetSize = existing?.raw ? Buffer.byteLength(existing.raw, "utf8") : 0;
  const existingHash = existing?.ok
    ? hashText(stableCanonicalBody(existing.parsed.profile, existing.parsed.graph))
    : null;

  if (existing?.ok && existingHash === nextHash) {
    const payload = {
      profile: existing.parsed.profile,
      graph: existing.parsed.graph ?? null,
      written_at: existing.parsed.written_at || null,
    };
    cache.set(ownerId, payload);
    if (opts.diagnostics) {
      logPortfolioWriteDiag({
        owner_entity_id: ownerId,
        target_path: targetPath,
        target_exists: true,
        target_size: targetSize,
        existing_hash: existingHash,
        next_hash: nextHash,
        write_skipped: true,
        write_attempted: false,
      init_count: goldenMaterializationInitCount,
    });
    }
    return { payload, skipped: true, wrote: false, unchanged: true };
  }

  const payload = {
    profile,
    graph: graph || null,
    written_at: new Date().toISOString(),
  };
  const serialized = `${JSON.stringify(payload, null, 2)}\n`;

  try {
    const writeMeta = safeWriteJsonFile(targetPath, serialized);
    cache.set(ownerId, payload);
    if (opts.diagnostics) {
      logPortfolioWriteDiag({
        owner_entity_id: ownerId,
        target_path: targetPath,
        target_exists: targetExists,
        target_size: targetSize,
        existing_hash: existingHash,
        next_hash: nextHash,
        write_skipped: false,
        write_attempted: true,
        write_method: writeMeta.method,
        init_count: goldenMaterializationInitCount,
      });
    }
    return { payload, skipped: false, wrote: true };
  } catch (err) {
    // Fail-safe: keep valid existing fixture; only hard-fail when none exists
    const fallback = readExistingPayload(targetPath);
    const diag = {
      owner_entity_id: ownerId,
      target_path: targetPath,
      target_exists: Boolean(fallback),
      target_size: fallback?.raw ? Buffer.byteLength(fallback.raw, "utf8") : 0,
      existing_hash: existingHash,
      next_hash: nextHash,
      write_skipped: false,
      write_attempted: true,
      init_count: goldenMaterializationInitCount,
      error_code: err?.code || null,
      errno: err?.errno ?? null,
      syscall: err?.syscall || null,
      error_message: err?.message || String(err),
    };
    if (fallback?.ok) {
      goldenMaterializationDegraded = true;
      cache.set(ownerId, fallback.parsed);
      logPortfolioWriteDiag({
        ...diag,
        degraded: true,
        used_existing_fixture: true,
      });
      return {
        payload: fallback.parsed,
        skipped: false,
        wrote: false,
        degraded: true,
        error: err,
      };
    }
    logPortfolioWriteDiag({ ...diag, degraded: false, used_existing_fixture: false });
    throw err;
  }
}

export function getOwnerPortfolioRecord(ownerEntityId) {
  const id = String(ownerEntityId || "").trim();
  if (!id) return null;
  if (cache.has(id)) return cache.get(id);
  const file = fixturePath(id);
  const existing = readExistingPayload(file);
  if (existing?.ok) {
    cache.set(id, existing.parsed);
    return existing.parsed;
  }
  return null;
}

export function getOwnerPortfolioProfile(ownerEntityId) {
  return getOwnerPortfolioRecord(ownerEntityId)?.profile || null;
}

export function getOwnerControlGraph(ownerEntityId) {
  return getOwnerPortfolioRecord(ownerEntityId)?.graph || null;
}

function materializeGoldenOwnerPortfoliosOnce() {
  goldenMaterializationInitCount += 1;
  const out = [];
  const compilers = [
    compileGsfOwnerPortfolioFromEvidence,
    compileCambridgeOwnerPortfolioFromEvidence,
    compileSheratonHnfOwnerPortfolioFromEvidence,
    compileVocoAllianceOwnerPortfolioFromEvidence,
  ];
  for (const compile of compilers) {
    const compiled = compile();
    writeOwnerPortfolioProfile(compiled.profile, compiled.graph, { diagnostics: true });
    out.push(compiled.profile.owner_entity_id);
  }
  return out;
}

/** Compile + persist golden owners from existing evidence (idempotent, once per process). */
export function ensureGoldenOwnerPortfoliosMaterialized() {
  if (goldenOwnerPortfoliosMaterializedIds) {
    return goldenOwnerPortfoliosMaterializedIds;
  }
  if (goldenOwnerPortfoliosMaterializing) {
    // Re-entrant call during first materialization — return disk ids, do not nest writes
    return listCachedOrDiskOwnerIds();
  }
  goldenOwnerPortfoliosMaterializing = true;
  try {
    const ids = materializeGoldenOwnerPortfoliosOnce();
    goldenOwnerPortfoliosMaterializedIds = ids;
    return ids;
  } finally {
    goldenOwnerPortfoliosMaterializing = false;
  }
}

function listCachedOrDiskOwnerIds() {
  ensureFixtureDir();
  try {
    return fs
      .readdirSync(FIXTURE_DIR)
      .filter((f) => f.endsWith(".json") && !f.startsWith("."))
      .map((f) => f.replace(/\.json$/, ""));
  } catch {
    return [];
  }
}

/** Test / recovery helpers */
export function __resetGoldenOwnerPortfolioMaterializationForTests() {
  goldenOwnerPortfoliosMaterializedIds = null;
  goldenOwnerPortfoliosMaterializing = false;
  goldenMaterializationInitCount = 0;
  goldenMaterializationDegraded = false;
  cache.clear();
}

export function getGoldenOwnerPortfolioMaterializationState() {
  return {
    initCount: goldenMaterializationInitCount,
    materialized: Boolean(goldenOwnerPortfoliosMaterializedIds),
    degraded: goldenMaterializationDegraded,
  };
}

const HOTEL_TO_OWNER = Object.freeze({
  recUNycnMwOVFX0hc: GSF_OWNER_ENTITY_ID,
  recIwaP1etgx2g9nA: DOVETAIL_ENTITY_ID,
  recsYJb2R1jarPpK3: HNF_ENTITY_ID,
  recTYaiA4S6fR6ixx: ALLIANCE_ENTITY_ID,
});

const SLUG_TO_OWNER = Object.freeze({
  [GSF_SLUG]: GSF_OWNER_ENTITY_ID,
  "grupo-hotelero-santa-fe": GSF_OWNER_ENTITY_ID,
  "dovetail-hospitality": DOVETAIL_ENTITY_ID,
  dovetail: DOVETAIL_ENTITY_ID,
  "inmobiliaria-hnf": HNF_ENTITY_ID,
  hnf: HNF_ENTITY_ID,
  "alliance-hotel-management": ALLIANCE_ENTITY_ID,
  alliance: ALLIANCE_ENTITY_ID,
});

export function resolveOwnerEntityId(ref) {
  const raw = String(ref || "").trim();
  if (!raw) return null;
  if (HOTEL_TO_OWNER[raw]) return HOTEL_TO_OWNER[raw];
  if (SLUG_TO_OWNER[raw]) return SLUG_TO_OWNER[raw];
  if (raw.startsWith("dle_") || raw.startsWith("ent_")) return raw;
  return SLUG_TO_OWNER[raw.toLowerCase()] || raw;
}

export function resolveOwnerForHotel(hotelId) {
  const ownerId = HOTEL_TO_OWNER[String(hotelId || "").trim()];
  if (!ownerId) return null;
  ensureGoldenOwnerPortfoliosMaterialized();
  return getOwnerPortfolioProfile(ownerId);
}

export function listMaterializedOwnerIds() {
  ensureFixtureDir();
  ensureGoldenOwnerPortfoliosMaterialized();
  return listCachedOrDiskOwnerIds();
}

export { HOTEL_TO_OWNER, SLUG_TO_OWNER, FIXTURE_DIR };
