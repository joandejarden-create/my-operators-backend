/**
 * Read-only federation of AI Visibility file stores.
 * Used so Brand UI can see OpenAI wave1-showcase + Gemini/Perplexity/Claude baselines together.
 */

import { createFileStore } from "./file-store.js";

function sortSummaries(rows) {
  return [...rows].sort((a, b) =>
    String(b.completedAt || b.startedAt || b.savedAt || "").localeCompare(
      String(a.completedAt || a.startedAt || a.savedAt || "")
    )
  );
}

/**
 * @param {{ rootDirs: string[], source?: string }} options
 */
export function createFederatedFileStore(options = {}) {
  const rootDirs = (options.rootDirs || []).filter(Boolean);
  if (!rootDirs.length) {
    throw new Error("createFederatedFileStore requires rootDirs");
  }
  const stores = rootDirs.map((rootDir) => createFileStore({ rootDir }));
  /** @type {Map<string, object>} */
  const batchToStore = new Map();

  function indexSummary(store, summary) {
    const id = summary?.batchId || summary?.wave1Id;
    if (id && !batchToStore.has(id)) batchToStore.set(id, store);
  }

  async function storeForBatch(batchId) {
    if (!batchId) return null;
    if (batchToStore.has(batchId)) return batchToStore.get(batchId);
    for (const store of stores) {
      const sum =
        typeof store.getBatchSummary === "function"
          ? await store.getBatchSummary(batchId)
          : null;
      if (sum) {
        batchToStore.set(batchId, store);
        return store;
      }
      const runs = await store.listBatchRuns(batchId);
      if (runs?.length) {
        batchToStore.set(batchId, store);
        return store;
      }
    }
    return null;
  }

  return {
    kind: "federated_file",
    rootDirs,
    rootResolution: {
      source: options.source || "federated_measured_baseline",
      federated: true,
      recoveredPhase2e: false,
      wave1Namespace: rootDirs.some((d) => /wave1-showcase/i.test(d)),
    },
    durability: "local_dev_not_production",

    async listBatchSummaries(filter = {}) {
      const out = [];
      const seen = new Set();
      for (const store of stores) {
        const rows = await store.listBatchSummaries(filter);
        for (const row of rows) {
          const id = row.batchId || row.wave1Id;
          if (!id || seen.has(id)) continue;
          seen.add(id);
          indexSummary(store, row);
          out.push(row);
        }
      }
      return sortSummaries(out);
    },

    async getBatchSummary(batchId) {
      const store = await storeForBatch(batchId);
      if (!store) return null;
      return store.getBatchSummary(batchId);
    },

    async getBatch(batchId) {
      const store = await storeForBatch(batchId);
      if (!store) return null;
      return store.getBatch(batchId);
    },

    async listBatchRuns(batchId) {
      const store = await storeForBatch(batchId);
      if (!store) return [];
      return store.listBatchRuns(batchId);
    },

    async getEvidence(evidenceId) {
      for (const store of stores) {
        const row = await store.getEvidence(evidenceId);
        if (row) return row;
      }
      return null;
    },

    async listEvidence(filter = {}) {
      const out = [];
      const seen = new Set();
      for (const store of stores) {
        const rows = await store.listEvidence(filter);
        for (const row of rows) {
          const id = row.evidenceId || `${row.batchId}:${row.promptId}`;
          if (seen.has(id)) continue;
          seen.add(id);
          out.push(row);
        }
      }
      return out;
    },

    async listMetricSnapshots(filter = {}) {
      const out = [];
      const seen = new Set();
      for (const store of stores) {
        const rows = await store.listMetricSnapshots(filter);
        for (const row of rows) {
          const id =
            row.snapshotId ||
            `${row.batchId}_${row.entityId}_${row.metric}_${row.geographyScope}_${row.language}`;
          if (seen.has(id)) continue;
          seen.add(id);
          out.push(row);
        }
      }
      return out.sort((a, b) =>
        String(a.batchDate || a.savedAt || "").localeCompare(String(b.batchDate || b.savedAt || ""))
      );
    },

    async listBatches() {
      const out = [];
      const seen = new Set();
      for (const store of stores) {
        const rows = (await store.listBatches()) || [];
        for (const row of rows) {
          if (!row.batchId || seen.has(row.batchId)) continue;
          seen.add(row.batchId);
          batchToStore.set(row.batchId, store);
          out.push(row);
        }
      }
      return out;
    },
  };
}
