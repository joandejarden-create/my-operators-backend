/**
 * AVA durable runtime storage root resolution.
 *
 * Single volume layout (recommended mount: /app/ava-data):
 *   {root}/audits/   — meta, versions, decision/approval events
 *   {root}/pdfs/     — versioned + current PDFs, checksums
 *   {root}/backups/  — snapshots
 *
 * Modes (AVA_STORAGE_MODE):
 *   off      — legacy repo paths only (ephemeral on Railway without volume)
 *   local    — write to AVA_STORAGE_ROOT or default data/ava-data (dev/test)
 *   required — durable root must be present + writable; writes fail closed
 *
 * When mode is local|required, writes NEVER silently fall back to ephemeral
 * application filesystem paths.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  AVA_PRODUCTION_ARCHIVE_ROOT,
  AVA_PRODUCTION_STORE_ROOT,
} from "./certified-baseline-paths-v1.js";

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

export const AVA_DURABLE_RUNTIME_STORAGE_V1 = "AVA_DURABLE_RUNTIME_STORAGE_V1";
export const DEFAULT_AVA_STORAGE_RELATIVE = "data/ava-data";
export const RECOMMENDED_AVA_VOLUME_MOUNT = "/app/ava-data";

export const AVA_STORAGE_MODE = Object.freeze({
  OFF: "off",
  LOCAL: "local",
  REQUIRED: "required",
});

export function resolveAvaStorageMode(env = process.env) {
  const raw = String(env.AVA_STORAGE_MODE || "").trim().toLowerCase();
  if (
    raw === AVA_STORAGE_MODE.OFF ||
    raw === AVA_STORAGE_MODE.LOCAL ||
    raw === AVA_STORAGE_MODE.REQUIRED
  ) {
    return raw;
  }
  // Explicit root implies durable local unless mode forced off.
  if (String(env.AVA_STORAGE_ROOT || "").trim()) {
    const required = String(env.AVA_STORAGE_REQUIRED || "").trim().toLowerCase();
    if (required === "1" || required === "true" || required === "yes" || required === "on") {
      return AVA_STORAGE_MODE.REQUIRED;
    }
    return AVA_STORAGE_MODE.LOCAL;
  }
  return AVA_STORAGE_MODE.OFF;
}

export function resolveAvaStorageRoot(env = process.env) {
  const override = String(env.AVA_STORAGE_ROOT || "").trim();
  if (override) return path.resolve(override);
  return path.join(REPO_ROOT, DEFAULT_AVA_STORAGE_RELATIVE);
}

export function avaStorageLayout(root) {
  const base = path.resolve(root);
  return {
    root: base,
    audits: path.join(base, "audits"),
    pdfs: path.join(base, "pdfs"),
    backups: path.join(base, "backups"),
  };
}

export function isAvaDurableStorageEnabled(env = process.env) {
  return resolveAvaStorageMode(env) !== AVA_STORAGE_MODE.OFF;
}

export function isAvaDurableStorageRequired(env = process.env) {
  return resolveAvaStorageMode(env) === AVA_STORAGE_MODE.REQUIRED;
}

/**
 * Ensure durable root exists and is writable when durable mode is on.
 * Fail closed for writes when required/local cannot access the root.
 */
export function ensureAvaDurableStorage(env = process.env) {
  const mode = resolveAvaStorageMode(env);
  const root = resolveAvaStorageRoot(env);
  const layout = avaStorageLayout(root);

  if (mode === AVA_STORAGE_MODE.OFF) {
    return {
      ok: true,
      mode,
      enabled: false,
      required: false,
      writable: false,
      root: null,
      layout: null,
      legacyStoreRoot: AVA_PRODUCTION_STORE_ROOT,
      legacyPdfRoot: AVA_PRODUCTION_ARCHIVE_ROOT,
    };
  }

  try {
    fs.mkdirSync(layout.audits, { recursive: true });
    fs.mkdirSync(layout.pdfs, { recursive: true });
    fs.mkdirSync(layout.backups, { recursive: true });
    fs.accessSync(layout.root, fs.constants.R_OK | fs.constants.W_OK);
    // Probe write
    const probe = path.join(layout.root, ".ava-write-probe");
    fs.writeFileSync(probe, String(Date.now()), "utf8");
    fs.unlinkSync(probe);
    return {
      ok: true,
      mode,
      enabled: true,
      required: mode === AVA_STORAGE_MODE.REQUIRED,
      writable: true,
      root: layout.root,
      layout,
      legacyStoreRoot: AVA_PRODUCTION_STORE_ROOT,
      legacyPdfRoot: AVA_PRODUCTION_ARCHIVE_ROOT,
      contract: AVA_DURABLE_RUNTIME_STORAGE_V1,
    };
  } catch (err) {
    const message = `AVA durable storage unavailable at ${layout.root}: ${
      err?.message || err
    }. Set a writable AVA_STORAGE_ROOT (Railway Volume mount ${RECOMMENDED_AVA_VOLUME_MOUNT}) or set AVA_STORAGE_MODE=off for legacy ephemeral paths.`;
    if (mode === AVA_STORAGE_MODE.REQUIRED || mode === AVA_STORAGE_MODE.LOCAL) {
      return {
        ok: false,
        mode,
        enabled: true,
        required: mode === AVA_STORAGE_MODE.REQUIRED,
        writable: false,
        root: layout.root,
        layout,
        error: "AVA_STORAGE_UNAVAILABLE",
        message,
      };
    }
    return {
      ok: false,
      mode,
      enabled: false,
      required: false,
      writable: false,
      root: layout.root,
      layout,
      error: "AVA_STORAGE_UNAVAILABLE",
      message,
    };
  }
}

export function assertAvaDurableWritable(env = process.env) {
  const state = ensureAvaDurableStorage(env);
  if (!state.enabled) {
    return state;
  }
  if (!state.ok || !state.writable) {
    const e = new Error(state.message || "AVA_STORAGE_UNAVAILABLE");
    e.code = "AVA_STORAGE_UNAVAILABLE";
    e.details = state;
    throw e;
  }
  return state;
}

/** Resolve absolute path for a storage-relative PDF reference. */
export function resolveAvaPdfAbsolute(pdfRelativePath, env = process.env) {
  const rel = String(pdfRelativePath || "").replace(/\\/g, "/");
  if (!rel) return null;
  if (path.isAbsolute(rel) && fs.existsSync(rel)) return rel;

  // Always probe the durable volume for pdfs/… references (read-only OK even when
  // AVA_STORAGE_MODE=off) so Admin can open staged drafts without silent legacy fallback.
  const durableRoot = resolveAvaStorageRoot(env);
  if (rel.startsWith("pdfs/")) {
    const durableAbs = path.join(durableRoot, rel);
    if (fs.existsSync(durableAbs)) return durableAbs;
  }
  const durableUnderPdfs = path.join(
    durableRoot,
    "pdfs",
    rel.replace(/^pdfs\//, "")
  );
  if (fs.existsSync(durableUnderPdfs)) return durableUnderPdfs;

  if (isAvaDurableStorageEnabled(env)) {
    const { layout } = ensureAvaDurableStorage(env);
    if (layout) {
      if (rel.startsWith("pdfs/")) {
        const abs = path.join(layout.root, rel);
        if (fs.existsSync(abs)) return abs;
      }
      const underPdfs = path.join(layout.pdfs, rel.replace(/^pdfs\//, ""));
      if (fs.existsSync(underPdfs)) return underPdfs;
    }
  }

  const fromRepo = path.join(REPO_ROOT, rel);
  if (fs.existsSync(fromRepo)) return fromRepo;
  return fromRepo;
}

export function repoRoot() {
  return REPO_ROOT;
}
