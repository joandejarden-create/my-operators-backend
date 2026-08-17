/**
 * AI Visibility storage abstraction — filesystem development implementation.
 * Business logic must depend on this interface, not on paths.
 *
 * Phase 1–2E: local JSON under data/ai-visibility/runtime (gitignored).
 * Not permanent production storage — replaceable with DB/object store later.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";
import { resolveAiVisibilityStoreRoot } from "./resolve-store-root.js";
import {
  isMultiSlotBatchSummary,
  multiSlotSummaryMatchesStoreFilter,
} from "../multi-slot-geography.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_ROOT = path.resolve(__dirname, "..", "..", "..", "data", "ai-visibility", "runtime");

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function writeJson(filePath, value) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2), "utf8");
}

function readJson(filePath) {
  if (!fs.existsSync(filePath)) return null;
  const raw = fs.readFileSync(filePath, "utf8");
  return JSON.parse(raw);
}

function newId(prefix) {
  return `${prefix}_${randomUUID().replace(/-/g, "").slice(0, 16)}`;
}

function listJsonDir(dir) {
  if (!fs.existsSync(dir)) return [];
  return fs
    .readdirSync(dir)
    .filter((f) => f.endsWith(".json"))
    .map((f) => readJson(path.join(dir, f)))
    .filter(Boolean);
}

/**
 * Per-directory JSON listing cache. Invalidates when directory mtime changes.
 * Cuts repeated full-directory scans during Exec/Detail reads.
 */
function createCachedListJsonDir() {
  /** @type {Map<string, { mtimeMs: number, rows: object[] }>} */
  const cache = new Map();
  return function listJsonDirCached(dir) {
    if (!fs.existsSync(dir)) return [];
    let mtimeMs = 0;
    try {
      mtimeMs = fs.statSync(dir).mtimeMs;
    } catch {
      return listJsonDir(dir);
    }
    const hit = cache.get(dir);
    if (hit && hit.mtimeMs === mtimeMs) return hit.rows;
    const rows = listJsonDir(dir);
    cache.set(dir, { mtimeMs, rows });
    return rows;
  };
}

/**
 * @param {{ rootDir?: string }} [options]
 */
export function createFileStore(options = {}) {
  const resolved = resolveAiVisibilityStoreRoot(options);
  const rootDir = resolved.rootDir || DEFAULT_ROOT;
  const paths = {
    runs: path.join(rootDir, "runs"),
    responses: path.join(rootDir, "responses"),
    mentions: path.join(rootDir, "mentions"),
    citations: path.join(rootDir, "citations"),
    evidence: path.join(rootDir, "evidence"),
    batches: path.join(rootDir, "batches"),
    manifests: path.join(rootDir, "manifests"),
    summaries: path.join(rootDir, "summaries"),
    snapshots: path.join(rootDir, "metric-snapshots"),
  };

  const listJsonDirCached = createCachedListJsonDir();
  /** @type {Map<string, object|null>} */
  const evidenceCache = new Map();

  function getEvidenceCached(evidenceId) {
    if (!evidenceId) return null;
    if (evidenceCache.has(evidenceId)) return evidenceCache.get(evidenceId);
    const row = readJson(path.join(paths.evidence, `${evidenceId}.json`));
    evidenceCache.set(evidenceId, row);
    return row;
  }

  return {
    kind: "file",
    rootDir,
    rootResolution: resolved,
    durability: "local_dev_not_production",

    generateId: newId,

    async saveRun(run) {
      const runId = run.runId || newId("run");
      const record = { ...run, runId, savedAt: new Date().toISOString() };
      writeJson(path.join(paths.runs, `${runId}.json`), record);
      return record;
    },

    async getRun(runId) {
      return readJson(path.join(paths.runs, `${runId}.json`));
    },

    async saveResponse(response) {
      const responseId = response.responseId || newId("resp");
      const record = { ...response, responseId, savedAt: new Date().toISOString() };
      writeJson(path.join(paths.responses, `${responseId}.json`), record);
      return record;
    },

    async getResponse(responseId) {
      return readJson(path.join(paths.responses, `${responseId}.json`));
    },

    async saveMentions(responseId, mentions) {
      const record = {
        responseId,
        mentions: Array.isArray(mentions) ? mentions : [],
        savedAt: new Date().toISOString(),
      };
      writeJson(path.join(paths.mentions, `${responseId}.json`), record);
      return record;
    },

    async getMentions(responseId) {
      const record = readJson(path.join(paths.mentions, `${responseId}.json`));
      return record ? record.mentions : null;
    },

    async saveCitations(responseId, citations) {
      const record = {
        responseId,
        citations: Array.isArray(citations) ? citations : [],
        savedAt: new Date().toISOString(),
      };
      writeJson(path.join(paths.citations, `${responseId}.json`), record);
      return record;
    },

    async getCitations(responseId) {
      const record = readJson(path.join(paths.citations, `${responseId}.json`));
      return record ? record.citations : null;
    },

    async saveEvidence(evidence) {
      const evidenceId = evidence.evidenceId || newId("ev");
      const record = { ...evidence, evidenceId, savedAt: new Date().toISOString() };
      writeJson(path.join(paths.evidence, `${evidenceId}.json`), record);
      evidenceCache.set(evidenceId, record);
      return record;
    },

    async getEvidence(evidenceId) {
      return getEvidenceCached(evidenceId);
    },

    async saveBatch(batch) {
      const batchId = batch.batchId || newId("aiv_batch");
      const record = { ...batch, batchId, savedAt: new Date().toISOString() };
      writeJson(path.join(paths.batches, `${batchId}.json`), record);
      return record;
    },

    async getBatch(batchId) {
      return readJson(path.join(paths.batches, `${batchId}.json`));
    },

    async updateBatch(batchId, patch) {
      const existing = await this.getBatch(batchId);
      if (!existing) throw new Error(`Batch not found: ${batchId}`);
      const record = {
        ...existing,
        ...patch,
        batchId,
        updatedAt: new Date().toISOString(),
      };
      writeJson(path.join(paths.batches, `${batchId}.json`), record);
      return record;
    },

    async listBatches() {
      return listJsonDirCached(paths.batches);
    },

    async saveBatchManifest(manifest) {
      const batchId = manifest.batchId;
      if (!batchId) throw new Error("manifest.batchId required");
      const record = { ...manifest, savedAt: new Date().toISOString() };
      writeJson(path.join(paths.manifests, `${batchId}.json`), record);
      return record;
    },

    async getBatchManifest(batchId) {
      return readJson(path.join(paths.manifests, `${batchId}.json`));
    },

    async saveBatchSummary(summary) {
      const batchId = summary.batchId;
      if (!batchId) throw new Error("summary.batchId required");
      const record = { ...summary, savedAt: new Date().toISOString() };
      writeJson(path.join(paths.summaries, `${batchId}.json`), record);
      return record;
    },

    async getBatchSummary(batchId) {
      return readJson(path.join(paths.summaries, `${batchId}.json`));
    },

    async listBatchRuns(batchId) {
      return listJsonDirCached(paths.runs).filter((r) => r.batchId === batchId);
    },

    async listBatchSummaries(filter = {}) {
      let rows = listJsonDirCached(paths.summaries);
      if (filter.status) {
        rows = rows.filter((r) => r.status === filter.status);
      }
      if (filter.provider) {
        rows = rows.filter(
          (r) => String(r.provider?.name || r.provider || "").toLowerCase() === String(filter.provider).toLowerCase()
        );
      }
      const hasGeoLangFilter =
        Boolean(filter.geographyScope) ||
        Boolean(filter.commercialRegion) ||
        Boolean(filter.region) ||
        Boolean(filter.country) ||
        Boolean(filter.language);
      if (hasGeoLangFilter) {
        rows = rows.filter((r) => {
          if (isMultiSlotBatchSummary(r)) {
            return multiSlotSummaryMatchesStoreFilter(r, filter);
          }
          if (filter.geographyScope) {
            if (
              String(r.cohort?.geographyScope || "").toLowerCase() !==
              String(filter.geographyScope).toLowerCase()
            ) {
              return false;
            }
          }
          if (filter.commercialRegion || filter.region) {
            const reg = filter.commercialRegion || filter.region;
            if (
              String(r.cohort?.commercialRegion || "").toLowerCase() !== String(reg).toLowerCase()
            ) {
              return false;
            }
          }
          if (filter.country) {
            if (
              String(r.cohort?.country || "").toLowerCase() !== String(filter.country).toLowerCase()
            ) {
              return false;
            }
          }
          if (filter.language) {
            const want = String(filter.language).toLowerCase();
            const lang = r.language ?? r.cohort?.language;
            if (lang == null || lang === "") return want === "en";
            return String(lang).toLowerCase() === want;
          }
          return true;
        });
      }
      return rows.sort((a, b) =>
        String(b.completedAt || b.startedAt || "").localeCompare(String(a.completedAt || a.startedAt || ""))
      );
    },

    async listEvidence(filter = {}) {
      let rows = listJsonDirCached(paths.evidence);
      if (filter.batchId) {
        rows = rows.filter((r) => r.batchId === filter.batchId || r.payload?.batchId === filter.batchId);
      }
      if (filter.promptId) rows = rows.filter((r) => r.promptId === filter.promptId);
      if (filter.provider) {
        const want = String(filter.provider).toLowerCase();
        rows = rows.filter((r) => {
          const p = r.provider?.name || r.provider;
          // Older OpenAI-only evidence may omit provider — treat as openai only when filter is openai.
          if (p == null || p === "") return want === "openai";
          return String(p).toLowerCase() === want;
        });
      }
      if (filter.language) {
        const want = String(filter.language).toLowerCase();
        rows = rows.filter((r) => {
          const lang = r.language ?? r.payload?.language;
          if (lang == null || lang === "") return want === "en";
          return String(lang).toLowerCase() === want;
        });
      }
      if (filter.entityId) {
        rows = rows.filter((r) => {
          const mentions = r.payload?.mentions || r.mentions || [];
          return mentions.some(
            (m) =>
              m.entityId === filter.entityId ||
              m.resolvedEntityId === filter.entityId ||
              m.canonicalEntityId === filter.entityId
          );
        });
      }
      return rows;
    },

    async saveMetricSnapshot(snapshot) {
      const snapshotId =
        snapshot.snapshotId ||
        `${snapshot.batchId || "batch"}_${snapshot.entityId || "entity"}_${snapshot.metric || "metric"}`;
      const record = { ...snapshot, snapshotId, savedAt: new Date().toISOString() };
      writeJson(path.join(paths.snapshots, `${snapshotId}.json`), record);
      return record;
    },

    async listMetricSnapshots(filter = {}) {
      let rows = listJsonDirCached(paths.snapshots);
      if (filter.entityId) rows = rows.filter((r) => r.entityId === filter.entityId);
      if (filter.geographyScope) {
        rows = rows.filter(
          (r) =>
            String(r.geographyScope || "").toLowerCase() ===
            String(filter.geographyScope).toLowerCase()
        );
      }
      if (filter.region || filter.commercialRegion) {
        const reg = filter.region || filter.commercialRegion;
        rows = rows.filter(
          (r) =>
            String(r.commercialRegion || r.region || "").toLowerCase() ===
            String(reg).toLowerCase()
        );
      }
      if (filter.metric) rows = rows.filter((r) => r.metric === filter.metric);
      if (filter.provider) {
        rows = rows.filter((r) => {
          const p = r.provider?.name || r.provider;
          return String(p || "").toLowerCase() === String(filter.provider).toLowerCase();
        });
      }
      if (filter.language) {
        const want = String(filter.language).toLowerCase();
        rows = rows.filter((r) => {
          const lang = r.language;
          if (lang == null || lang === "") return want === "en";
          return String(lang).toLowerCase() === want;
        });
      }
      if (filter.startDate) {
        const start = Date.parse(filter.startDate);
        rows = rows.filter((r) => Date.parse(r.batchDate || r.savedAt || 0) >= start);
      }
      if (filter.endDate) {
        const end = Date.parse(filter.endDate);
        rows = rows.filter((r) => Date.parse(r.batchDate || r.savedAt || 0) <= end);
      }
      return rows.sort((a, b) =>
        String(a.batchDate || a.savedAt || "").localeCompare(String(b.batchDate || b.savedAt || ""))
      );
    },
  };
}

/** Default store factory for scripts/tests. */
export function createAiVisibilityStore(options = {}) {
  return createFileStore(options);
}
