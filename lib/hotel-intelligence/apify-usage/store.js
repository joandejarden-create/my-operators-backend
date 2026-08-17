/**
 * Append-only local ledger for Apify usage / cost.
 * Path: data/hotel-intelligence/apify-usage/ (override via HOTEL_INTELLIGENCE_DATA_DIR)
 *
 * Separate from authoritative hotel / census data.
 */

import fs from "node:fs";
import path from "node:path";
import {
  ensureDir,
  readJsonFile,
  resolveDataRoot,
  writeJsonFile,
} from "../local-store.js";
import { APIFY_USAGE_VERSION } from "./constants.js";
import { buildApifyUsageRecord, summarizeApifyUsage } from "./normalize.js";

export function resolveApifyUsageDir(opts = {}) {
  const root = opts.root || resolveDataRoot(opts.env || process.env);
  return path.join(root, "apify-usage");
}

function emptyLedger() {
  return {
    version: APIFY_USAGE_VERSION,
    production_writes: false,
    authoritative_hotel_data: false,
    updated_at: null,
    runs: [],
  };
}

/**
 * @param {object} [opts]
 */
export function createApifyUsageStore(opts = {}) {
  const dir = resolveApifyUsageDir(opts);
  ensureDir(dir);
  const ledgerPath = path.join(dir, "ledger.json");
  const summaryPath = path.join(dir, "summary.json");

  function read() {
    return readJsonFile(ledgerPath, emptyLedger());
  }

  function write(ledger) {
    const next = {
      ...emptyLedger(),
      ...ledger,
      version: APIFY_USAGE_VERSION,
      production_writes: false,
      authoritative_hotel_data: false,
      updated_at: new Date().toISOString(),
      runs: Array.isArray(ledger.runs) ? ledger.runs : [],
    };
    // Guard against wiping a previously corrupted ledger read as empty.
    try {
      if (fs.existsSync(ledgerPath)) {
        const existingRaw = fs.readFileSync(ledgerPath);
        const looksCorrupt =
          !existingRaw.length ||
          existingRaw.every((b) => b === 0) ||
          existingRaw.toString("utf8").trimStart().startsWith("\u0000");
        if (!looksCorrupt) {
          const existing = JSON.parse(existingRaw.toString("utf8"));
          const existingRuns = Array.isArray(existing?.runs) ? existing.runs : [];
          if (existingRuns.length > next.runs.length + 5) {
            const byId = new Map();
            for (const r of existingRuns) {
              if (r?.run_id) byId.set(r.run_id, r);
            }
            for (const r of next.runs) {
              if (r?.run_id) byId.set(r.run_id, { ...(byId.get(r.run_id) || {}), ...r });
              else byId.set(`anon-${byId.size}`, r);
            }
            next.runs = [...byId.values()];
          }
        } else {
          const bak = `${ledgerPath}.corrupt.${Date.now()}.bak`;
          fs.copyFileSync(ledgerPath, bak);
        }
      }
    } catch {
      /* keep next.runs as-is */
    }
    writeJsonFile(ledgerPath, next);
    writeJsonFile(summaryPath, {
      ...summarizeApifyUsage(next.runs),
      updated_at: next.updated_at,
      ledger_path: ledgerPath,
    });
    return next;
  }

  /**
   * Upsert by run_id when present; otherwise append.
   * @param {object} input buildApifyUsageRecord input
   */
  function recordRun(input) {
    const row = buildApifyUsageRecord(input);
    const ledger = read();
    const runs = [...(ledger.runs || [])];
    if (row.run_id) {
      const idx = runs.findIndex((r) => r.run_id === row.run_id);
      if (idx >= 0) {
        // Preserve first recorded_at; merge outcome counters if newer payload fills gaps
        const prev = runs[idx];
        runs[idx] = {
          ...prev,
          ...row,
          recorded_at: prev.recorded_at || row.recorded_at,
          successful_matches:
            row.successful_matches ?? prev.successful_matches ?? null,
          successful_enrichments:
            row.successful_enrichments ?? prev.successful_enrichments ?? null,
          verified_enrichments:
            row.verified_enrichments ?? prev.verified_enrichments ?? null,
          records_requested:
            row.records_requested ?? prev.records_requested ?? null,
          notes: row.notes || prev.notes || null,
          label: row.label || prev.label || null,
        };
        // Recompute derived costs after merge
        const merged = runs[idx];
        const cost = merged.apify_run_cost_usd;
        const ret = merged.records_returned;
        const enr = merged.successful_enrichments;
        const ver = merged.verified_enrichments;
        merged.cost_per_returned_record =
          cost != null && ret ? Number((cost / ret).toFixed(8)) : null;
        merged.cost_per_successful_enrichment =
          cost != null && enr ? Number((cost / enr).toFixed(8)) : null;
        merged.cost_per_verified_enrichment =
          cost != null && ver ? Number((cost / ver).toFixed(8)) : null;
        write({ ...ledger, runs });
        return { row: runs[idx], created: false, ledger_path: ledgerPath };
      }
    }
    runs.push(row);
    write({ ...ledger, runs });
    return { row, created: true, ledger_path: ledgerPath };
  }

  function list(filter = {}) {
    let runs = read().runs || [];
    if (filter.use_case) {
      runs = runs.filter((r) => r.dealality_use_case === filter.use_case);
    }
    if (filter.actor_name) {
      runs = runs.filter((r) => r.actor_name === filter.actor_name);
    }
    return runs;
  }

  function summary() {
    if (fs.existsSync(summaryPath)) {
      return readJsonFile(summaryPath, summarizeApifyUsage(read().runs));
    }
    return summarizeApifyUsage(read().runs);
  }

  return {
    version: APIFY_USAGE_VERSION,
    dir,
    paths: { ledger: ledgerPath, summary: summaryPath },
    read,
    write,
    recordRun,
    list,
    summary,
  };
}
