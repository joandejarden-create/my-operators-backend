/**
 * Offline staging for benchmark results — never writes production HI graph.
 */

import fs from "node:fs";
import path from "node:path";
import { P3A_POLICY } from "./contract.js";

export function createBenchmarkStagingStore(opts = {}) {
  const root =
    opts.root ||
    path.join(
      process.cwd(),
      "reports/hotel-explorer/he-intel-001-research-benchmark/runs"
    );
  const runId = opts.runId || `p3a_dry_${Date.now()}`;
  const runRoot = path.join(root, runId);
  const writes = [];

  function ensure() {
    fs.mkdirSync(runRoot, { recursive: true });
  }

  function writeJson(rel, data) {
    if (opts.memoryOnly) {
      writes.push({ rel, data });
      return { ok: true, path: rel, memoryOnly: true };
    }
    ensure();
    const full = path.join(runRoot, rel);
    fs.mkdirSync(path.dirname(full), { recursive: true });
    fs.writeFileSync(full, JSON.stringify(data, null, 2));
    writes.push({ rel, path: full });
    return { ok: true, path: full };
  }

  function assertNoProductionGraphWrite(target = {}) {
    if (
      target.productionAirtableWrite === true ||
      target.canonicalGraphWrite === true ||
      target.mutateHpc === true
    ) {
      return {
        ok: false,
        error: "P3A_PRODUCTION_GRAPH_WRITE_FORBIDDEN",
        policy: P3A_POLICY,
      };
    }
    if (
      target.baseId &&
      target.baseId !== P3A_POLICY.graph_base_id &&
      target.baseId === P3A_POLICY.forbidden_graph_base_id
    ) {
      return { ok: false, error: "FORBIDDEN_GRAPH_BASE" };
    }
    return { ok: true, stagingOnly: true };
  }

  return {
    runId,
    runRoot,
    writeJson,
    assertNoProductionGraphWrite,
    listWrites: () => [...writes],
    policy: {
      production_airtable_writes: false,
      canonical_graph_writes: false,
      staging_only: true,
    },
  };
}
