/**
 * Persist discovery query plans for first-time Full E2E audits (hotel-generic).
 * Used by V22/V23-style depth audits so "what was searched" is inspectable.
 */
import fs from "node:fs";
import path from "node:path";

export const FIRST_TIME_DISCOVERY_QUERY_PERSISTENCE_V1 =
  "gdi-first-time-discovery-query-persistence-v1";

/**
 * @param {object} plan
 * @param {string} plan.hotelId
 * @param {string} plan.workflowId
 * @param {string[]} plan.queries
 * @param {string[]} [plan.languages]
 * @param {string[]} [plan.geographies]
 * @param {string} [plan.horizon]
 * @param {string} outDir absolute directory
 */
export function writeDiscoveryQueryPlanArtifact(plan = {}, outDir) {
  if (!outDir) throw new Error("outDir required");
  fs.mkdirSync(outDir, { recursive: true });
  const workflowId = String(plan.workflowId || "unknown");
  const file = path.join(
    outDir,
    `discovery-query-plan__${workflowId.replace(/[^\w-]+/g, "_")}.json`,
  );
  const payload = {
    version: FIRST_TIME_DISCOVERY_QUERY_PERSISTENCE_V1,
    hotelId: plan.hotelId || null,
    workflowId,
    queries: Array.isArray(plan.queries) ? plan.queries : [],
    languages: Array.isArray(plan.languages) ? plan.languages : ["en"],
    geographies: Array.isArray(plan.geographies) ? plan.geographies : [],
    horizon: plan.horizon || "18_months",
    provider: plan.provider || "exa",
    writtenAt: new Date().toISOString(),
  };
  fs.writeFileSync(file, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  return { file, queryCount: payload.queries.length };
}

/**
 * Summarize persisted plans for certification / audit.
 */
export function summarizeDiscoveryQueryPlans(outDir) {
  if (!outDir || !fs.existsSync(outDir)) {
    return { plans: 0, totalQueries: 0, byWorkflow: {} };
  }
  const files = fs
    .readdirSync(outDir)
    .filter((f) => f.startsWith("discovery-query-plan__") && f.endsWith(".json"));
  const byWorkflow = {};
  let totalQueries = 0;
  for (const f of files) {
    try {
      const j = JSON.parse(fs.readFileSync(path.join(outDir, f), "utf8"));
      const n = Array.isArray(j.queries) ? j.queries.length : 0;
      byWorkflow[j.workflowId || f] = {
        queryCount: n,
        languages: j.languages || [],
        geographies: j.geographies || [],
      };
      totalQueries += n;
    } catch {
      /* skip corrupt */
    }
  }
  return { plans: files.length, totalQueries, byWorkflow };
}
