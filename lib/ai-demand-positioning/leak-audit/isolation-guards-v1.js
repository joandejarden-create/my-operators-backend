/**
 * Isolation guards — free leak audits must never write production ADP / Census / Explorer paths.
 */

import { resolve } from "node:path";
import { FORBIDDEN_WRITE_PATH_FRAGMENTS } from "./schema-v1.js";

function normalizePath(p) {
  return resolve(String(p || ""))
    .replace(/\\/g, "/")
    .toLowerCase();
}

/**
 * Throws if a candidate write path escapes the leak-audit root or hits forbidden production trees.
 */
export function assertLeakAuditWritePathAllowed(candidatePath, options = {}) {
  const pathNorm = normalizePath(candidatePath);
  const rootHint = options.root
    ? normalizePath(options.root)
    : normalizePath(
        process.env.LEAK_AUDIT_ROOT ||
          resolve(process.cwd(), "data/ai-demand-positioning/leak-audit")
      );

  // Allow writes only under leak-audit root (or explicit test root)
  const underLeakRoot =
    pathNorm === rootHint ||
    pathNorm.startsWith(rootHint.endsWith("/") ? rootHint : `${rootHint}/`) ||
    pathNorm.includes("/leak-audit/");

  if (!underLeakRoot) {
    const err = new Error(`leak_audit_write_path_forbidden:${candidatePath}`);
    err.code = "LEAK_AUDIT_WRITE_ISOLATION";
    throw err;
  }

  for (const frag of FORBIDDEN_WRITE_PATH_FRAGMENTS) {
    const f = String(frag).replace(/\\/g, "/").toLowerCase();
    // Allow .../leak-audit/... even if parent is ai-demand-positioning
    if (pathNorm.includes("/leak-audit/")) continue;
    if (pathNorm.includes(f)) {
      const err = new Error(`leak_audit_write_path_forbidden_fragment:${frag}`);
      err.code = "LEAK_AUDIT_WRITE_ISOLATION";
      throw err;
    }
  }

  return { ok: true, path: pathNorm };
}

/**
 * Static inventory of production mutation entry points that leak-audit must not call.
 * Used by isolation tests.
 */
export const PRODUCTION_MUTATION_ENTRY_POINTS = Object.freeze([
  "lib/ai-demand-positioning/data-model.js#savePeriod",
  "lib/ai-demand-positioning/published-snapshot.js#savePublishedSnapshotBundle",
  "scripts/publish-ai-demand-positioning-snapshot.mjs",
  "lib/ai-demand-positioning/longitudinal",
  "Brand Explorer apply / presentation patch writers",
  "Operator Explorer fixture/production writers",
  "Hotel Property Census Airtable writers",
]);

export function assertNoProductionMutationImports(moduleSource) {
  const src = String(moduleSource || "");
  const forbidden = [
    /savePeriod\s*\(/,
    /savePublishedSnapshotBundle\s*\(/,
    /ADP_AIRTABLE_PUBLISH_APPLY/,
    /ADP_HISTORY_AIRTABLE_WRITE_APPLY/,
    /persistAdpHistoricalPeriod/,
  ];
  const hits = [];
  for (const re of forbidden) {
    if (re.test(src)) hits.push(String(re));
  }
  return { ok: hits.length === 0, hits };
}
