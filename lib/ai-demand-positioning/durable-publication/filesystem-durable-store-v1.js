/**
 * Filesystem-backed ADP durable published-report store.
 *
 * Suitable for:
 *  - Railway persistent volume mount (production, after founder auth)
 *  - Local / isolated test roots (ADP_DURABLE_PUBLISHED_ROOT)
 *
 * Layout under root:
 *   registry/published-current-registry-v1.json
 *   registry/history/<propertyId>/<iso>_<periodId>.json
 *   snapshots/<propertyId>/report-<periodId>.json
 *   snapshots/<propertyId>/evidence-<periodId>.json
 *   snapshots/<propertyId>/manifest-<periodId>.json
 *   current/<propertyId>/manifest.json   (active pointer mirror)
 *   locks/<propertyId>.lock
 *   staging/<txnId>/...
 */

import { createHash } from "crypto";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
  renameSync,
  unlinkSync,
  readdirSync,
  openSync,
  closeSync,
  constants,
} from "fs";
import { join, dirname } from "path";
import {
  ADP_DURABLE_PUBLICATION_STORAGE_V1,
  ensureAdpDurablePublishedRoot,
  resolveAdpDurablePublishedRoot,
} from "./resolve-durable-root-v1.js";

export const ADP_DURABLE_STORE_SCHEMA = "ADP_FILESYSTEM_DURABLE_PUBLISHED_STORE_V1";
export const ADP_DURABLE_INTEGRITY_FAILURE = "ADP_DURABLE_INTEGRITY_FAILURE";
export const ADP_DURABLE_SNAPSHOT_MISSING = "ADP_DURABLE_SNAPSHOT_MISSING";
export const ADP_DURABLE_REGISTRY_MISSING = "ADP_DURABLE_REGISTRY_MISSING";
export const ADP_DURABLE_LOCK_HELD = "ADP_DURABLE_LOCK_HELD";
export const ADP_DURABLE_IMMUTABLE_CONFLICT = "ADP_DURABLE_IMMUTABLE_CONFLICT";
export const ADP_DURABLE_POINTER_MISMATCH = "ADP_DURABLE_POINTER_MISMATCH";

function sha256Buffer(buf) {
  return createHash("sha256").update(buf).digest("hex");
}

function sha256File(path) {
  return sha256Buffer(readFileSync(path));
}

function readJson(path) {
  if (!existsSync(path)) return null;
  return JSON.parse(readFileSync(path, "utf8"));
}

function atomicWriteFile(path, bufOrString) {
  mkdirSync(dirname(path), { recursive: true });
  const payload = Buffer.isBuffer(bufOrString) ? bufOrString : Buffer.from(String(bufOrString), "utf8");
  const tmp = `${path}.${process.pid}.${Date.now()}.tmp`;
  writeFileSync(tmp, payload);
  try {
    renameSync(tmp, path);
  } catch {
    writeFileSync(path, payload);
    try {
      unlinkSync(tmp);
    } catch {
      /* best-effort */
    }
  }
  return sha256Buffer(payload);
}

function atomicWriteJson(path, data) {
  return atomicWriteFile(path, `${JSON.stringify(data, null, 2)}\n`);
}

export function createFilesystemDurablePublishedStore(options = {}) {
  const env = options.env || process.env;
  const ensured = ensureAdpDurablePublishedRoot(env);
  const root = options.root || ensured.root || resolveAdpDurablePublishedRoot(env);

  const paths = {
    root,
    registryFile: join(root, "registry", "published-current-registry-v1.json"),
    historyDir: join(root, "registry", "history"),
    snapshotsDir: join(root, "snapshots"),
    currentDir: join(root, "current"),
    locksDir: join(root, "locks"),
    stagingDir: join(root, "staging"),
  };

  function propertySnapshotDir(propertyId) {
    return join(paths.snapshotsDir, propertyId);
  }

  function reportPath(propertyId, periodId) {
    return join(propertySnapshotDir(propertyId), `report-${periodId}.json`);
  }

  function evidencePath(propertyId, periodId) {
    return join(propertySnapshotDir(propertyId), `evidence-${periodId}.json`);
  }

  function periodManifestPath(propertyId, periodId) {
    return join(propertySnapshotDir(propertyId), `manifest-${periodId}.json`);
  }

  function currentManifestPath(propertyId) {
    return join(paths.currentDir, propertyId, "manifest.json");
  }

  function lockPath(propertyId) {
    return join(paths.locksDir, `${propertyId}.lock`);
  }

  function emptyRegistry() {
    return {
      schema: "ADP_PUBLISHED_CURRENT_REGISTRY_V1",
      contract: ADP_DURABLE_PUBLICATION_STORAGE_V1,
      store: ADP_DURABLE_STORE_SCHEMA,
      updatedAt: null,
      notes: [
        "Durable published-current registry. Lives outside deploy-replaced application filesystem when ADP_DURABLE_PUBLISHED_ROOT is a Railway volume.",
      ],
      properties: [],
    };
  }

  function readRegistry() {
    const parsed = readJson(paths.registryFile);
    if (!parsed) {
      if (options.allowMissingRegistry === false) {
        const err = new Error(`${ADP_DURABLE_REGISTRY_MISSING}: ${paths.registryFile}`);
        err.code = ADP_DURABLE_REGISTRY_MISSING;
        throw err;
      }
      return emptyRegistry();
    }
    return {
      ...emptyRegistry(),
      ...parsed,
      properties: Array.isArray(parsed.properties) ? parsed.properties : [],
    };
  }

  function getCurrentEntry(propertyId) {
    const id = String(propertyId || "").trim();
    return (readRegistry().properties || []).find((p) => p.propertyId === id) || null;
  }

  function listPublicationHistory(propertyId) {
    const id = String(propertyId || "").trim();
    const dir = join(paths.historyDir, id);
    const entries = [];
    if (existsSync(dir)) {
      for (const name of readdirSync(dir).sort()) {
        if (!name.endsWith(".json")) continue;
        const row = readJson(join(dir, name));
        if (row) entries.push({ file: name, ...row });
      }
    }
    const current = getCurrentEntry(id);
    return { propertyId: id, current, history: entries };
  }

  function writeImmutableSnapshotBytes({
    propertyId,
    periodId,
    reportBytes,
    evidenceBytes = null,
    manifest = null,
    expectedReportSha256 = null,
  }) {
    const id = String(propertyId || "").trim();
    const pid = String(periodId || "").trim();
    if (!id || !pid) throw new Error("propertyId and periodId required");
    const reportBuf = Buffer.isBuffer(reportBytes)
      ? reportBytes
      : Buffer.from(String(reportBytes), "utf8");
    const evidenceBuf =
      evidenceBytes == null
        ? Buffer.from(
            `${JSON.stringify({ propertyId: id, periodId: pid, entries: [] }, null, 2)}\n`,
            "utf8"
          )
        : Buffer.isBuffer(evidenceBytes)
          ? evidenceBytes
          : Buffer.from(String(evidenceBytes), "utf8");

    const reportSha = sha256Buffer(reportBuf);
    const evidenceSha = sha256Buffer(evidenceBuf);
    if (expectedReportSha256 && expectedReportSha256 !== reportSha) {
      const err = new Error(
        `${ADP_DURABLE_INTEGRITY_FAILURE}: source report sha mismatch expected=${expectedReportSha256} actual=${reportSha}`
      );
      err.code = ADP_DURABLE_INTEGRITY_FAILURE;
      throw err;
    }

    const rPath = reportPath(id, pid);
    const ePath = evidencePath(id, pid);
    const mPath = periodManifestPath(id, pid);

    if (existsSync(rPath)) {
      const existing = sha256File(rPath);
      if (existing !== reportSha) {
        const err = new Error(
          `${ADP_DURABLE_IMMUTABLE_CONFLICT}: refusing to overwrite ${rPath} (existing=${existing} new=${reportSha})`
        );
        err.code = ADP_DURABLE_IMMUTABLE_CONFLICT;
        throw err;
      }
    } else {
      atomicWriteFile(rPath, reportBuf);
    }

    if (existsSync(ePath)) {
      const existingEv = sha256File(ePath);
      if (existingEv !== evidenceSha) {
        const err = new Error(
          `${ADP_DURABLE_IMMUTABLE_CONFLICT}: refusing to overwrite evidence ${ePath}`
        );
        err.code = ADP_DURABLE_IMMUTABLE_CONFLICT;
        throw err;
      }
    } else {
      atomicWriteFile(ePath, evidenceBuf);
    }

    const periodManifest = {
      ...(manifest || {}),
      propertyId: id,
      latestPeriodId: pid,
      reportFile: `report-${pid}.json`,
      evidenceFile: `evidence-${pid}.json`,
      reportSha256: reportSha,
      evidenceSha256: evidenceSha,
      durableStore: ADP_DURABLE_STORE_SCHEMA,
      writtenAt: new Date().toISOString(),
    };
    atomicWriteJson(mPath, periodManifest);

    const verifyReport = sha256File(rPath);
    if (verifyReport !== reportSha) {
      const err = new Error(`${ADP_DURABLE_INTEGRITY_FAILURE}: report verify failed`);
      err.code = ADP_DURABLE_INTEGRITY_FAILURE;
      throw err;
    }

    return {
      ok: true,
      propertyId: id,
      periodId: pid,
      reportPath: rPath,
      evidencePath: ePath,
      periodManifestPath: mPath,
      reportSha256: reportSha,
      evidenceSha256: evidenceSha,
      periodManifest,
    };
  }

  function writeImmutableSnapshot({ propertyId, periodId, report, evidence, manifest }) {
    const reportBody = `${JSON.stringify(report, null, 2)}\n`;
    const evidenceBody = `${JSON.stringify(
      evidence ?? { propertyId, periodId, entries: [] },
      null,
      2
    )}\n`;
    return writeImmutableSnapshotBytes({
      propertyId,
      periodId,
      reportBytes: reportBody,
      evidenceBytes: evidenceBody,
      manifest,
    });
  }

  /**
   * Migration import: preserve exact source file bytes (SHA-stable).
   */
  function importAuthorizedPublishedSnapshot({
    propertyId,
    periodId,
    reportFilePath,
    evidenceFilePath = null,
    manifest = null,
    expectedReportSha256 = null,
    sourceStagedVersionId = null,
    considerationRate = null,
  }) {
    const reportBytes = readFileSync(reportFilePath);
    const evidenceBytes = evidenceFilePath && existsSync(evidenceFilePath)
      ? readFileSync(evidenceFilePath)
      : null;
    const written = writeImmutableSnapshotBytes({
      propertyId,
      periodId,
      reportBytes,
      evidenceBytes,
      manifest,
      expectedReportSha256,
    });
    const activated = activatePublishedCurrent({
      propertyId,
      periodId,
      propertyName: manifest?.propertyName || null,
      reportFile: `report-${periodId}.json`,
      evidenceFile: `evidence-${periodId}.json`,
      reportSha256: written.reportSha256,
      evidenceSha256: written.evidenceSha256,
      reportBytes: reportBytes.length,
      latestPublishedAt: manifest?.latestPublishedAt || new Date().toISOString(),
      sourceStagedVersionId: sourceStagedVersionId || manifest?.sourceStagedVersionId || null,
      considerationRate,
      publishStatus: manifest?.publishStatus || "Live",
      provenance: "importAuthorizedPublishedSnapshot",
    });
    return { ok: true, written, activated };
  }

  function activatePublishedCurrent(entry, { previousEntry = null } = {}) {
    const id = String(entry?.propertyId || "").trim();
    const periodId = String(entry?.periodId || "").trim();
    if (!id || !periodId) throw new Error("activatePublishedCurrent requires propertyId+periodId");

    // Snapshot must already exist and match sha
    const rPath = reportPath(id, periodId);
    if (!existsSync(rPath)) {
      const err = new Error(`${ADP_DURABLE_SNAPSHOT_MISSING}: ${rPath}`);
      err.code = ADP_DURABLE_SNAPSHOT_MISSING;
      throw err;
    }
    if (entry.reportSha256) {
      const actual = sha256File(rPath);
      if (actual !== entry.reportSha256) {
        const err = new Error(`${ADP_DURABLE_INTEGRITY_FAILURE}: pointer activation sha mismatch`);
        err.code = ADP_DURABLE_INTEGRITY_FAILURE;
        throw err;
      }
    }

    const registry = readRegistry();
    const prev =
      previousEntry ||
      (registry.properties || []).find((p) => p.propertyId === id) ||
      null;

    if (prev?.periodId && prev.periodId !== periodId) {
      const histDir = join(paths.historyDir, id);
      mkdirSync(histDir, { recursive: true });
      const stamp = new Date().toISOString().replace(/[:.]/g, "-");
      atomicWriteJson(join(histDir, `${stamp}_${prev.periodId}.json`), {
        ...prev,
        supersededAt: new Date().toISOString(),
        supersededByPeriodId: periodId,
      });
    }

    const nextEntry = {
      propertyId: id,
      periodId,
      reportFile: entry.reportFile || `report-${periodId}.json`,
      evidenceFile: entry.evidenceFile || `evidence-${periodId}.json`,
      reportSha256: entry.reportSha256 || sha256File(rPath),
      evidenceSha256: entry.evidenceSha256 ?? null,
      reportBytes: entry.reportBytes ?? null,
      latestPublishedAt: entry.latestPublishedAt || new Date().toISOString(),
      sourceStagedVersionId: entry.sourceStagedVersionId || null,
      considerationRate: entry.considerationRate ?? null,
      publishStatus: entry.publishStatus || "Live",
      provenance: entry.provenance || "durable_publication_transaction",
    };

    const properties = [...(registry.properties || [])];
    const idx = properties.findIndex((p) => p.propertyId === id);
    if (idx >= 0) properties[idx] = nextEntry;
    else properties.push(nextEntry);
    properties.sort((a, b) => a.propertyId.localeCompare(b.propertyId));

    const nextRegistry = {
      ...registry,
      schema: "ADP_PUBLISHED_CURRENT_REGISTRY_V1",
      contract: ADP_DURABLE_PUBLICATION_STORAGE_V1,
      store: ADP_DURABLE_STORE_SCHEMA,
      updatedAt: new Date().toISOString(),
      properties,
    };
    atomicWriteJson(paths.registryFile, nextRegistry);

    const currentManifest = {
      propertyId: id,
      propertyName: entry.propertyName || null,
      latestPeriodId: periodId,
      latestPublishedAt: nextEntry.latestPublishedAt,
      reportFile: nextEntry.reportFile,
      evidenceFile: nextEntry.evidenceFile,
      reportSha256: nextEntry.reportSha256,
      evidenceSha256: nextEntry.evidenceSha256,
      publishStatus: nextEntry.publishStatus,
      sourceStagedVersionId: nextEntry.sourceStagedVersionId,
      considerationRate: nextEntry.considerationRate,
      durableStore: ADP_DURABLE_STORE_SCHEMA,
    };
    atomicWriteJson(currentManifestPath(id), currentManifest);

    // Read-back verification
    const loaded = getCurrentEntry(id);
    if (!loaded || loaded.periodId !== periodId || loaded.reportSha256 !== nextEntry.reportSha256) {
      const err = new Error(`${ADP_DURABLE_POINTER_MISMATCH}: registry read-back failed for ${id}`);
      err.code = ADP_DURABLE_POINTER_MISMATCH;
      throw err;
    }

    return { ok: true, previous: prev, current: nextEntry, registryPath: paths.registryFile };
  }

  function readPublishedReport(propertyId) {
    const entry = getCurrentEntry(propertyId);
    if (!entry) return null;
    const rPath = reportPath(propertyId, entry.periodId);
    if (!existsSync(rPath)) {
      const err = new Error(`${ADP_DURABLE_SNAPSHOT_MISSING}: ${rPath}`);
      err.code = ADP_DURABLE_SNAPSHOT_MISSING;
      err.expectedPeriodId = entry.periodId;
      throw err;
    }
    if (entry.reportSha256) {
      const actual = sha256File(rPath);
      if (actual !== entry.reportSha256) {
        const err = new Error(`${ADP_DURABLE_INTEGRITY_FAILURE}: ${propertyId} report sha mismatch`);
        err.code = ADP_DURABLE_INTEGRITY_FAILURE;
        err.expectedSha256 = entry.reportSha256;
        err.actualSha256 = actual;
        throw err;
      }
    }
    const report = readJson(rPath);
    if (report?.payload && typeof report.payload === "object") return { entry, report, payload: report.payload };
    return { entry, report, payload: report };
  }

  function readPublishedManifest(propertyId) {
    const entry = getCurrentEntry(propertyId);
    if (!entry) return null;
    const cur = readJson(currentManifestPath(propertyId));
    if (cur?.latestPeriodId && cur.latestPeriodId !== entry.periodId) {
      const err = new Error(
        `${ADP_DURABLE_POINTER_MISMATCH}: current manifest ${cur.latestPeriodId} != registry ${entry.periodId}`
      );
      err.code = ADP_DURABLE_POINTER_MISMATCH;
      throw err;
    }
    return cur || {
      propertyId,
      latestPeriodId: entry.periodId,
      reportFile: entry.reportFile,
      evidenceFile: entry.evidenceFile,
      reportSha256: entry.reportSha256,
      publishStatus: entry.publishStatus,
    };
  }

  function readPreviousPublishedVersion(propertyId) {
    const hist = listPublicationHistory(propertyId);
    const last = hist.history[hist.history.length - 1] || null;
    if (!last?.periodId) return null;
    const rPath = reportPath(propertyId, last.periodId);
    if (!existsSync(rPath)) return { entry: last, report: null, missingSnapshot: true };
    return { entry: last, report: readJson(rPath), missingSnapshot: false };
  }

  function acquirePublicationLock(propertyId, { staleMs = 120_000 } = {}) {
    const id = String(propertyId || "").trim();
    mkdirSync(paths.locksDir, { recursive: true });
    const lp = lockPath(id);
    if (existsSync(lp)) {
      try {
        const existing = readJson(lp);
        const age = Date.now() - Date.parse(existing?.acquiredAt || 0);
        if (Number.isFinite(age) && age < staleMs) {
          const err = new Error(`${ADP_DURABLE_LOCK_HELD}: ${id}`);
          err.code = ADP_DURABLE_LOCK_HELD;
          err.lock = existing;
          throw err;
        }
      } catch (err) {
        if (err.code === ADP_DURABLE_LOCK_HELD) throw err;
      }
    }
    const lock = {
      propertyId: id,
      pid: process.pid,
      acquiredAt: new Date().toISOString(),
      schema: "ADP_DURABLE_PUBLICATION_LOCK_V1",
    };
    // Exclusive create when possible
    try {
      const fd = openSync(lp, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY);
      writeFileSync(fd, `${JSON.stringify(lock, null, 2)}\n`);
      closeSync(fd);
    } catch {
      // Fallback after stale check already passed
      atomicWriteJson(lp, lock);
    }
    return {
      ok: true,
      lock,
      release() {
        try {
          if (existsSync(lp)) unlinkSync(lp);
        } catch {
          /* best-effort */
        }
      },
    };
  }

  function beginStaging(txnId) {
    const dir = join(paths.stagingDir, txnId);
    mkdirSync(dir, { recursive: true });
    return dir;
  }

  function recoverInterruptedPublication(propertyId) {
    // If snapshot exists for a staged period but pointer not activated, leave previous current.
    const current = getCurrentEntry(propertyId);
    const stagingRoot = paths.stagingDir;
    const leftover = existsSync(stagingRoot)
      ? readdirSync(stagingRoot).filter((n) => n.includes(propertyId) || n.startsWith("txn_"))
      : [];
    return {
      ok: true,
      propertyId,
      currentPublishedPeriodId: current?.periodId || null,
      previousStillAvailable: Boolean(current?.periodId),
      leftoverStagingDirs: leftover,
      action: "NO_POINTER_ACTIVATION_ON_INTERRUPT — previous published remains current",
    };
  }

  return {
    schema: ADP_DURABLE_STORE_SCHEMA,
    contract: ADP_DURABLE_PUBLICATION_STORAGE_V1,
    root,
    paths,
    readRegistry,
    getCurrentEntry,
    listPublicationHistory,
    writeImmutableSnapshot,
    writeImmutableSnapshotBytes,
    importAuthorizedPublishedSnapshot,
    activatePublishedCurrent,
    readPublishedReport,
    readPublishedManifest,
    readPreviousPublishedVersion,
    acquirePublicationLock,
    beginStaging,
    recoverInterruptedPublication,
    reportPath,
    evidencePath,
    sha256File,
  };
}

export function getDefaultDurablePublishedStore(env = process.env) {
  return createFilesystemDurablePublishedStore({ env });
}
