/**
 * Leak Audit repository — storage-agnostic API for Phase 2A generation.
 *
 * Default: filesystem under data/ai-demand-positioning/leak-audit/live/
 * Optional: Airtable when AIRTABLE_LEAK_AUDIT_BASE_ID (+ API key) is configured.
 *
 * Generation code should depend on this repository, not on Airtable vs files.
 */

import { mkdirSync } from "node:fs";
import { join, resolve } from "node:path";
import { createLeakAuditStore, resolveLeakAuditRoot } from "./store-v1.js";
import {
  createLeakAuditAirtableClient,
  isLeakAuditAirtableConfigured,
  getLeakAuditAirtableConfig,
} from "./airtable-client.js";
import { LEAK_AUDIT_COLLECTIONS, AIRTABLE_TABLE_CONTRACTS } from "./airtable-schema.js";
import { assertLeakAuditWritePathAllowed } from "./isolation-guards-v1.js";

export const LEAK_AUDIT_LIVE_RELATIVE = "data/ai-demand-positioning/leak-audit/live";

export function resolveLeakAuditLiveRoot(overrideRoot) {
  if (overrideRoot) return resolve(overrideRoot);
  if (process.env.LEAK_AUDIT_ROOT) return resolve(process.env.LEAK_AUDIT_ROOT);
  return resolve(process.cwd(), LEAK_AUDIT_LIVE_RELATIVE);
}

export function detectLeakAuditStorageBackend(env = process.env) {
  const cfg = getLeakAuditAirtableConfig(env);
  if (cfg.configured) {
    return {
      backend: "airtable",
      label: "airtable",
      baseIdConfigured: true,
      filesystemFallback: false,
      reason: null,
    };
  }
  return {
    backend: "filesystem",
    label: "filesystem_fallback",
    baseIdConfigured: false,
    filesystemFallback: true,
    reason: cfg.reason || "airtable_env_missing",
  };
}

/**
 * Create repository. Phase 2A uses filesystem as source of truth always;
 * when Airtable is configured, writes are dual-written best-effort (non-fatal).
 */
export function createLeakAuditRepository(options = {}) {
  const env = options.env || process.env;
  const storage = detectLeakAuditStorageBackend(env);
  const root = resolveLeakAuditLiveRoot(options.root);
  mkdirSync(root, { recursive: true });
  assertLeakAuditWritePathAllowed(root, { root });

  const store = createLeakAuditStore({ root });
  const airtable =
    storage.backend === "airtable" || options.forceAirtableClient
      ? createLeakAuditAirtableClient({ env, fetchImpl: options.fetchImpl })
      : null;

  const dualWriteEnabled =
    Boolean(airtable?.configured) && options.dualWrite !== false;

  async function mirrorToAirtable(tableLogicalName, record) {
    if (!dualWriteEnabled || !record) return { mirrored: false };
    try {
      await airtable.upsertByLeakId(tableLogicalName, record);
      return { mirrored: true };
    } catch (err) {
      console.error(
        `[leak-audit repository] airtable mirror failed (${tableLogicalName}):`,
        err?.message || err
      );
      return { mirrored: false, error: err?.message || String(err) };
    }
  }

  const tableForCollection = Object.fromEntries(
    Object.entries(LEAK_AUDIT_COLLECTIONS).map(([table, collection]) => [collection, table])
  );

  function wrapCreate(methodName, collection) {
    return (...args) => {
      const record = store[methodName](...args);
      const table = tableForCollection[collection];
      if (table && dualWriteEnabled) {
        // Fire-and-forget mirror; filesystem remains authoritative for Phase 2A
        void mirrorToAirtable(table, record);
      }
      return record;
    };
  }

  return {
    ...store,
    root,
    storageBackend: storage.backend,
    storageInfo: {
      ...storage,
      root: root.replace(/\\/g, "/"),
      dualWriteEnabled,
      schemaTables: Object.keys(AIRTABLE_TABLE_CONTRACTS).length,
      liveRelative: LEAK_AUDIT_LIVE_RELATIVE,
    },
    airtableConfigured: Boolean(airtable?.configured),
    airtableClient: airtable,

    createRequest: wrapCreate("createRequest", "requests"),
    createHotel: wrapCreate("createHotel", "hotels"),
    createRun: wrapCreate("createRun", "runs"),
    createMetric: wrapCreate("createMetric", "metrics"),
    createCompetitor: wrapCreate("createCompetitor", "competitors"),
    createEvidence: wrapCreate("createEvidence", "evidence"),
    createAction: wrapCreate("createAction", "actions"),
    createReport: wrapCreate("createReport", "reports"),
    createPortfolio: wrapCreate("createPortfolio", "portfolios"),
    createPortfolioHotel: wrapCreate("createPortfolioHotel", "portfolio-hotels"),
    createPortfolioRun: wrapCreate("createPortfolioRun", "portfolio-runs"),
    createPortfolioReport: wrapCreate("createPortfolioReport", "portfolio-reports"),
    createPromotion: wrapCreate("createPromotion", "promotions"),

    async healthcheck() {
      return {
        ok: true,
        storageBackend: storage.backend,
        filesystemRoot: root.replace(/\\/g, "/"),
        airtable: airtable
          ? { configured: airtable.configured, config: airtable.config }
          : { configured: false },
      };
    },
  };
}

/** Prefer repository for admin/generation; keep createLeakAuditStore for unit tests. */
export function getDefaultLeakAuditRepository() {
  return createLeakAuditRepository();
}

export { resolveLeakAuditRoot, isLeakAuditAirtableConfigured };
