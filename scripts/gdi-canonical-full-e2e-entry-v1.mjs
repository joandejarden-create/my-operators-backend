#!/usr/bin/env node
/**
 * Canonical GDI Full E2E research entry CLI (P1-C + live safety gates).
 *
 *   npm run gdi:canonical-full-e2e -- --hotel <id> --mode fixture
 *   npm run gdi:canonical-full-e2e -- --hotel <id> --mode dry-run
 *   npm run gdi:canonical-full-e2e -- --hotel <id> --mode preflight
 *
 * Live paid Exa research (founder-authorized only):
 *   set GDI_EXA_AGENT_LIVE=1
 *   npm run gdi:canonical-full-e2e -- --hotel <id> --mode live \
 *     --authorize-paid-discovery --authorize-paid-research \
 *     --authorize-exa-agent-live --budget-ceiling-usd 15
 *
 * Never publishes. Research-inventory Airtable persistence is opt-in and
 * dry-run by default (--plan-research-persistence). Production apply requires
 * separate founder gates (bridge + env) — never unlocked by paid-research flags.
 */
import "dotenv/config";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runCanonicalFullE2eResearch,
  assessCanonicalLiveReadiness,
  CANONICAL_EXECUTION_MODE,
  GDI_CANONICAL_FULL_E2E_ENTRY_V1,
} from "../lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-entry-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function arg(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}
function has(flag) {
  return process.argv.includes(flag);
}

const hotelId = arg("--hotel") || arg("--hotels")?.split(",")[0];
const mode = arg("--mode") || CANONICAL_EXECUTION_MODE.FIXTURE;
const outDir = arg("--out") || null;
const maxCandidates = arg("--max-candidates");
const obsRoot = arg("--observability-root") || null;
const budgetCeilingUsd = Number(arg("--budget-ceiling-usd") || 0) || 0;
const exaEffort = arg("--exa-effort") || "medium";
const researchTrack = arg("--research-track") || null;

const opts = {
  hotelId,
  mode,
  dryRun: has("--dry-run") || mode === "dry-run",
  authorizePaidDiscovery: has("--authorize-paid-discovery"),
  authorizePaidResearch: has("--authorize-paid-research"),
  authorizeExaAgentLive: has("--authorize-exa-agent-live"),
  authorizePublish: has("--authorize-publish"),
  budgetCeilingUsd,
  maxCandidates: maxCandidates != null ? Number(maxCandidates) : 8,
  exaEffort,
  researchTrack,
  outDir,
  executionObservabilityRoot: obsRoot || undefined,
  executionRunId: arg("--execution-run-id") || arg("--resume-run") || undefined,
  skipHpc: !has("--prefer-live-hpc"),
  allowHistoricalReplay: !has("--no-historical-replay"),
  planResearchPersistence: has("--plan-research-persistence"),
  authorizeResearchPersistence: has("--authorize-research-persistence"),
  applyResearchPersistence: has("--apply-research-persistence"),
  exactWriteSetAuthorization: arg("--exact-write-set-authorization"),
  researchPersistenceWriteBudget: Number(
    arg("--research-persistence-write-budget") || 0,
  ),
  readBackResearchPersistence: has("--readback-research-persistence"),
};

if (!hotelId) {
  console.error(
    JSON.stringify(
      {
        ok: false,
        contract: GDI_CANONICAL_FULL_E2E_ENTRY_V1,
        error: "MISSING_HOTEL_ID",
        usage:
          "npm run gdi:canonical-full-e2e -- --hotel <recId> --mode fixture|dry-run|preflight|live",
      },
      null,
      2,
    ),
  );
  process.exit(2);
}

if (has("--assess-live-readiness")) {
  const readiness = assessCanonicalLiveReadiness({
    ...opts,
    mode: CANONICAL_EXECUTION_MODE.LIVE,
  });
  console.log(JSON.stringify(readiness, null, 2));
  process.exit(readiness.liveReady ? 0 : 2);
}

const result = await runCanonicalFullE2eResearch(opts);
console.log(JSON.stringify(result, null, 2));
process.exit(result.ok === false || result.blocked === true ? 2 : 0);
