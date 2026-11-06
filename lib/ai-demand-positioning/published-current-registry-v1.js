/**
 * ADP Published Current Registry V1
 *
 * Authority (ADP_DURABLE_PUBLISHED_MODE=prefer|required):
 *   1. Railway / filesystem durable published-current registry is authoritative
 *      for customer-published current period.
 *   2. Static deploy-pack registry (`config/.../adp-published-current-registry-v1.json`)
 *      must NOT override a newer explicitly authorized durable publication.
 *   3. Static registry is a controlled fallback only when durable authority is
 *      absent and the mode permits fallback (prefer/off/warm — not required).
 *   4. Genuine unauthorized FS rollback vs authoritative current still fails closed.
 *   5. Never resolve conflicts merely by selecting the newest timestamp.
 *
 * Contract: ADP_PRODUCTION_PUBLICATION_PERSISTENCE_P0_V1
 * Authority: ADP_PUBLICATION_AUTHORITY_DURABLE_PREFER_V1
 */

import { createHash } from "crypto";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  unlinkSync,
  writeFileSync,
} from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  resolveAdpDurablePublishedMode,
  shouldReadFromDurableStore,
  ADP_DURABLE_MODE,
} from "./durable-publication/resolve-durable-root-v1.js";

const REPO_ROOT = join(dirname(fileURLToPath(import.meta.url)), "../..");

export const ADP_PUBLISHED_CURRENT_REGISTRY_SCHEMA = "ADP_PUBLISHED_CURRENT_REGISTRY_V1";
export const ADP_PUBLISHED_SNAPSHOT_INTEGRITY_FAILURE = "PUBLISHED_SNAPSHOT_INTEGRITY_FAILURE";
export const ADP_PRODUCTION_PUBLISHED_DATA_MISSING = "PRODUCTION_PUBLISHED_DATA_MISSING";
export const ADP_PUBLISHED_REGISTRY_DIVERGENCE = "PUBLISHED_REGISTRY_FS_DIVERGENCE";
export const ADP_PUBLISHED_SILENT_ROLLBACK_BLOCKED = "PUBLISHED_SILENT_ROLLBACK_BLOCKED";
export const ADP_PUBLICATION_AUTHORITY_DURABLE_PREFER_V1 =
  "ADP_PUBLICATION_AUTHORITY_DURABLE_PREFER_V1";

export function resolvePublishedCurrentRegistryPath(env = process.env) {
  const override = String(env.ADP_PUBLISHED_CURRENT_REGISTRY_PATH || "").trim();
  if (override) return override;
  return join(REPO_ROOT, "config/ai-demand-positioning/adp-published-current-registry-v1.json");
}

/** Static deploy-pack registry path only (never the durable volume file). */
export function resolveStaticPublishedCurrentRegistryPath(env = process.env) {
  return resolvePublishedCurrentRegistryPath(env);
}

export function isPublishedCurrentRegistryEnforceEnabled(env = process.env) {
  const raw = String(env.ADP_PUBLISHED_CURRENT_REGISTRY_ENFORCE ?? "1").trim().toLowerCase();
  return !(raw === "0" || raw === "false" || raw === "off" || raw === "no");
}

export function sha256File(filePath) {
  return createHash("sha256").update(readFileSync(filePath)).digest("hex");
}

export function sha256Buffer(buf) {
  return createHash("sha256").update(buf).digest("hex");
}

function emptyRegistry() {
  return {
    schema: ADP_PUBLISHED_CURRENT_REGISTRY_SCHEMA,
    contract: "ADP_PRODUCTION_PUBLICATION_PERSISTENCE_P0_V1",
    updatedAt: null,
    notes: [],
    properties: [],
  };
}

export function loadPublishedCurrentRegistry(env = process.env) {
  const path = resolvePublishedCurrentRegistryPath(env);
  if (!existsSync(path)) return emptyRegistry();
  try {
    const parsed = JSON.parse(readFileSync(path, "utf8"));
    if (!parsed || typeof parsed !== "object") return emptyRegistry();
    const properties = Array.isArray(parsed.properties) ? parsed.properties : [];
    return {
      schema: parsed.schema || ADP_PUBLISHED_CURRENT_REGISTRY_SCHEMA,
      contract: parsed.contract || "ADP_PRODUCTION_PUBLICATION_PERSISTENCE_P0_V1",
      updatedAt: parsed.updatedAt || null,
      notes: Array.isArray(parsed.notes) ? parsed.notes : [],
      properties,
      _path: path,
    };
  } catch (err) {
    const e = new Error(
      `Failed to parse published-current registry at ${path}: ${err?.message || err}`
    );
    e.code = ADP_PUBLISHED_SNAPSHOT_INTEGRITY_FAILURE;
    throw e;
  }
}

export function getStaticPublishedCurrentEntry(propertyId, env = process.env) {
  const id = String(propertyId || "").trim();
  if (!id) return null;
  const registry = loadPublishedCurrentRegistry(env);
  return (
    (registry.properties || []).find((row) => String(row?.propertyId || "").trim() === id) || null
  );
}

/**
 * Read durable volume/store published-current entry when MODE allows durable reads.
 * Returns null when durable is off, missing, or unreadable (never throws for prefer).
 */
export function getDurablePublishedCurrentEntry(propertyId, env = process.env) {
  const id = String(propertyId || "").trim();
  if (!id) return null;
  if (!shouldReadFromDurableStore(env)) return null;
  try {
    const store = createLightweightDurableRegistryReader({ env });
    return store.getCurrentEntry(id) || null;
  } catch {
    return null;
  }
}

/**
 * Lightweight durable registry reader — reads volume registry JSON without
 * importing the full durable store (avoids circular ESM init).
 */
function createLightweightDurableRegistryReader({ env = process.env, root = null } = {}) {
  const override = String(env.ADP_DURABLE_PUBLISHED_ROOT || "").trim();
  const durableRoot =
    root || override || join(REPO_ROOT, "data/adp-durable-published");
  const registryFile = join(
    durableRoot,
    "registry",
    "published-current-registry-v1.json"
  );
  return {
    getCurrentEntry(propertyId) {
      const pid = String(propertyId || "").trim();
      if (!pid || !existsSync(registryFile)) return null;
      try {
        const parsed = JSON.parse(readFileSync(registryFile, "utf8"));
        return (
          (parsed.properties || []).find(
            (row) => String(row?.propertyId || "").trim() === pid
          ) || null
        );
      } catch {
        return null;
      }
    },
  };
}

/**
 * Resolve which published-current entry is authoritative for integrity + Admin.
 * Never picks "newest timestamp" across competing registries.
 */
export function resolveAuthoritativePublishedCurrent(propertyId, env = process.env) {
  const id = String(propertyId || "").trim();
  const mode = resolveAdpDurablePublishedMode(env);
  const staticEntry = id ? getStaticPublishedCurrentEntry(id, env) : null;
  const durableEntry =
    id && shouldReadFromDurableStore(env)
      ? getDurablePublishedCurrentEntry(id, env)
      : null;

  if (durableEntry?.periodId) {
    return {
      contract: ADP_PUBLICATION_AUTHORITY_DURABLE_PREFER_V1,
      propertyId: id,
      mode,
      entry: durableEntry,
      source: "durable_published_current_registry",
      staticEntry,
      durableEntry,
      staticLagging:
        !!staticEntry?.periodId &&
        String(staticEntry.periodId) !== String(durableEntry.periodId),
      staticShaLagging:
        !!staticEntry?.reportSha256 &&
        !!durableEntry?.reportSha256 &&
        String(staticEntry.reportSha256) !== String(durableEntry.reportSha256),
    };
  }

  if (mode === ADP_DURABLE_MODE.REQUIRED) {
    return {
      contract: ADP_PUBLICATION_AUTHORITY_DURABLE_PREFER_V1,
      propertyId: id,
      mode,
      entry: null,
      source: "durable_required_but_missing",
      staticEntry,
      durableEntry: null,
      staticLagging: false,
      staticShaLagging: false,
    };
  }

  return {
    contract: ADP_PUBLICATION_AUTHORITY_DURABLE_PREFER_V1,
    propertyId: id,
    mode,
    entry: staticEntry,
    source: staticEntry
      ? "static_deploy_pack_registry_fallback"
      : "no_published_current_entry",
    staticEntry,
    durableEntry: null,
    staticLagging: false,
    staticShaLagging: false,
  };
}

/**
 * Authoritative published-current entry (durable wins under prefer|required).
 */
export function getPublishedCurrentEntry(propertyId, env = process.env) {
  return resolveAuthoritativePublishedCurrent(propertyId, env).entry;
}

function atomicWriteJson(path, data) {
  mkdirSync(dirname(path), { recursive: true });
  const payload = `${JSON.stringify(data, null, 2)}\n`;
  const tmp = `${path}.${process.pid}.tmp`;
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
}

/**
 * Record (or replace) the current published pointer for a property.
 * Previous entries remain recoverable via prior registry versions in git /
 * historical report files that are not deleted by this write.
 */
export function recordPublishedCurrent(entry, env = process.env) {
  const propertyId = String(entry?.propertyId || "").trim();
  const periodId = String(entry?.periodId || "").trim();
  if (!propertyId || !periodId) {
    throw new Error("recordPublishedCurrent requires propertyId and periodId");
  }
  if (env.ADP_PUBLISHED_CURRENT_REGISTRY_WRITE === "0") {
    return { ok: true, skipped: true, reason: "registry_write_disabled" };
  }

  const path = resolvePublishedCurrentRegistryPath(env);
  const registry = loadPublishedCurrentRegistry(env);
  const nextEntry = {
    propertyId,
    periodId,
    reportFile: entry.reportFile || `report-${periodId}.json`,
    evidenceFile: entry.evidenceFile || `evidence-${periodId}.json`,
    reportSha256: entry.reportSha256 || null,
    evidenceSha256: entry.evidenceSha256 ?? null,
    reportBytes: entry.reportBytes ?? null,
    latestPublishedAt: entry.latestPublishedAt || new Date().toISOString(),
    sourceStagedVersionId: entry.sourceStagedVersionId || null,
    considerationRate: entry.considerationRate ?? null,
    publishStatus: entry.publishStatus || "Live",
    provenance: entry.provenance || "savePublishedSnapshotBundle",
    pdfArtifactSha256: entry.pdfArtifactSha256 ?? null,
    publicationManifestSha256: entry.publicationManifestSha256 ?? null,
    methodologyFingerprintSha256: entry.methodologyFingerprintSha256 ?? null,
  };

  const properties = [...(registry.properties || [])];
  const idx = properties.findIndex((row) => String(row?.propertyId || "").trim() === propertyId);
  const previous = idx >= 0 ? properties[idx] : null;
  if (idx >= 0) properties[idx] = nextEntry;
  else properties.push(nextEntry);
  properties.sort((a, b) => String(a.propertyId).localeCompare(String(b.propertyId)));

  const next = {
    schema: ADP_PUBLISHED_CURRENT_REGISTRY_SCHEMA,
    contract: "ADP_PRODUCTION_PUBLICATION_PERSISTENCE_P0_V1",
    updatedAt: new Date().toISOString(),
    notes: registry.notes?.length
      ? registry.notes
      : [
          "Canonical current_published period ledger. Deploy packs MUST include matching report/evidence bytes.",
        ],
    properties,
  };
  atomicWriteJson(path, next);
  return { ok: true, path, previous, current: nextEntry };
}

/**
 * Assert filesystem published snapshot matches the *authoritative* registry entry.
 * Under prefer|required, durable volume registry wins over stale deploy-pack static.
 * Never silently serves an older FS period when authoritative current points elsewhere.
 * Never resolves by picking the newest timestamp across competing authorities.
 */
export function assertPublishedFsMatchesRegistry({
  propertyId,
  manifest = null,
  reportFilePath = null,
  evidenceFilePath = null,
  env = process.env,
} = {}) {
  if (!isPublishedCurrentRegistryEnforceEnabled(env)) {
    return { ok: true, skipped: true, reason: "enforcement_disabled" };
  }

  const id = String(propertyId || "").trim();
  const authority = resolveAuthoritativePublishedCurrent(id, env);
  const entry = authority.entry;

  if (authority.source === "durable_required_but_missing") {
    const err = new Error(
      `${ADP_PRODUCTION_PUBLISHED_DATA_MISSING}: ${id} MODE=required but durable published-current entry is missing`
    );
    err.code = ADP_PRODUCTION_PUBLISHED_DATA_MISSING;
    err.propertyId = id;
    err.authoritySource = authority.source;
    throw err;
  }

  if (!entry) {
    return {
      ok: true,
      skipped: true,
      reason: "no_registry_entry",
      authoritySource: authority.source,
      mode: authority.mode,
    };
  }

  const fsPeriod = String(manifest?.latestPeriodId || "").trim();
  const expectedPeriod = String(entry.periodId || "").trim();

  if (!fsPeriod) {
    const err = new Error(
      `${ADP_PRODUCTION_PUBLISHED_DATA_MISSING}: ${id} authoritative registry (${authority.source}) expects ${expectedPeriod} but published manifest is missing`
    );
    err.code = ADP_PRODUCTION_PUBLISHED_DATA_MISSING;
    err.propertyId = id;
    err.expectedPeriodId = expectedPeriod;
    err.authoritySource = authority.source;
    throw err;
  }

  if (fsPeriod !== expectedPeriod) {
    const err = new Error(
      `${ADP_PUBLISHED_SILENT_ROLLBACK_BLOCKED}: ${id} filesystem latestPeriodId=${fsPeriod} but authoritative current_published=${expectedPeriod} (source=${authority.source}). Refusing to serve older/divergent published data without explicit recovery.`
    );
    err.code = ADP_PUBLISHED_SILENT_ROLLBACK_BLOCKED;
    err.propertyId = id;
    err.filesystemPeriodId = fsPeriod;
    err.expectedPeriodId = expectedPeriod;
    err.authoritySource = authority.source;
    err.staticPeriodId = authority.staticEntry?.periodId || null;
    err.durablePeriodId = authority.durableEntry?.periodId || null;
    throw err;
  }

  if (reportFilePath) {
    if (!existsSync(reportFilePath)) {
      const err = new Error(
        `${ADP_PRODUCTION_PUBLISHED_DATA_MISSING}: ${id} registry period ${expectedPeriod} report file missing at ${reportFilePath}`
      );
      err.code = ADP_PRODUCTION_PUBLISHED_DATA_MISSING;
      err.propertyId = id;
      err.expectedPeriodId = expectedPeriod;
      err.reportFilePath = reportFilePath;
      throw err;
    }
    if (entry.reportSha256) {
      const actual = sha256File(reportFilePath);
      if (actual !== entry.reportSha256) {
        const err = new Error(
          `${ADP_PUBLISHED_REGISTRY_DIVERGENCE}: ${id} report sha256 mismatch for ${expectedPeriod}`
        );
        err.code = ADP_PUBLISHED_REGISTRY_DIVERGENCE;
        err.propertyId = id;
        err.expectedPeriodId = expectedPeriod;
        err.expectedSha256 = entry.reportSha256;
        err.actualSha256 = actual;
        throw err;
      }
    }
  }

  if (evidenceFilePath && entry.evidenceSha256 && existsSync(evidenceFilePath)) {
    const actualEv = sha256File(evidenceFilePath);
    if (actualEv !== entry.evidenceSha256) {
      const err = new Error(
        `${ADP_PUBLISHED_REGISTRY_DIVERGENCE}: ${id} evidence sha256 mismatch for ${expectedPeriod}`
      );
      err.code = ADP_PUBLISHED_REGISTRY_DIVERGENCE;
      err.propertyId = id;
      err.expectedPeriodId = expectedPeriod;
      err.expectedSha256 = entry.evidenceSha256;
      err.actualSha256 = actualEv;
      throw err;
    }
  }

  return {
    ok: true,
    propertyId: id,
    periodId: expectedPeriod,
    reportSha256: entry.reportSha256 || null,
    authoritySource: authority.source,
    mode: authority.mode,
    staticLagging: authority.staticLagging === true,
  };
}

/**
 * Verify every registry entry against a published root directory.
 * Used by deploy packing asserts and startup health.
 */
export function verifyRegistryAgainstPublishedRoot(publishedRoot, env = process.env) {
  const registry = loadPublishedCurrentRegistry(env);
  const results = [];
  let ok = true;
  for (const entry of registry.properties || []) {
    const propertyId = String(entry.propertyId || "").trim();
    const dir = join(publishedRoot, propertyId);
    const manifestPath = join(dir, "manifest.json");
    const reportPath = join(dir, entry.reportFile || `report-${entry.periodId}.json`);
    const evidencePath = join(dir, entry.evidenceFile || `evidence-${entry.periodId}.json`);
    const row = {
      propertyId,
      periodId: entry.periodId,
      ok: false,
      code: null,
      detail: null,
    };
    try {
      if (!existsSync(manifestPath)) {
        throw Object.assign(new Error("manifest_missing"), {
          code: ADP_PRODUCTION_PUBLISHED_DATA_MISSING,
        });
      }
      const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
      assertPublishedFsMatchesRegistry({
        propertyId,
        manifest,
        reportFilePath: reportPath,
        evidenceFilePath: evidencePath,
        env: { ...env, ADP_PUBLISHED_CURRENT_REGISTRY_ENFORCE: "1" },
      });
      row.ok = true;
      row.code = "MATCH";
    } catch (err) {
      ok = false;
      row.ok = false;
      row.code = err.code || ADP_PUBLISHED_SNAPSHOT_INTEGRITY_FAILURE;
      row.detail = err.message;
    }
    results.push(row);
  }
  return {
    ok,
    registryPath: resolvePublishedCurrentRegistryPath(env),
    publishedRoot,
    entryCount: results.length,
    failureCount: results.filter((r) => !r.ok).length,
    results,
  };
}

export function createPublishedIntegrityError(code, message, extra = {}) {
  const err = new Error(message);
  err.code = code;
  Object.assign(err, extra);
  return err;
}
