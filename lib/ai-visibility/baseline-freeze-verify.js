/**
 * Verify immutable four-provider baseline freeze (Phase 3B.6).
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  BASELINE_FREEZE_ID,
  readBaselineFreezeMarker,
} from "./baseline-freeze.js";
import { METRIC_VERSION } from "./config.js";
import { PEER_SET_ID_V2 } from "./peer-sets.js";

export const BASELINE_FREEZE_VERIFY_VERSION = "ai_visibility_baseline_freeze_verify_v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const EXPECTED = Object.freeze({
  observationCount: 336,
  promptLibrary: "showcase_prompts_v1",
  peerSetId: PEER_SET_ID_V2,
  peerSetVersion: "2",
  metricVersion: METRIC_VERSION,
  providers: {
    openai: { model: "gpt-5.6", observations: 84 },
    gemini: { model: "gemini-3.6-flash", observations: 84 },
    perplexity: { model: "sonar", observations: 84 },
    claude: { model: "claude-sonnet-4-6", observations: 84 },
  },
});

function loadBaselineCosts(rootDir) {
  const runtime = rootDir || path.join(__dirname, "..", "..", "data", "ai-visibility", "runtime");
  const costs = {};
  const openaiSummary = path.join(
    runtime,
    "wave1-showcase",
    "summaries",
    "aiv_wave1_openai_showcase_20260814_0143_8367c6.json"
  );
  if (fs.existsSync(openaiSummary)) {
    const s = JSON.parse(fs.readFileSync(openaiSummary, "utf8"));
    costs.openai = s.costLedger?.actualUsd ?? s.usage?.estimatedCost ?? null;
  }
  for (const [provider, waveFile] of [
    ["gemini", "aiv_baseline_gemini_20260814_1105_9b7e19"],
    ["perplexity", "aiv_baseline_perplexity_20260814_1007_223198"],
    ["claude", "aiv_baseline_claude_20260814_1204_2a263a"],
  ]) {
    const p = path.join(
      runtime,
      "provider-baselines",
      provider,
      "waves",
      waveFile,
      "baseline-summary.json"
    );
    if (fs.existsSync(p)) {
      const s = JSON.parse(fs.readFileSync(p, "utf8"));
      costs[provider] = s.costUsd ?? s.costLedger?.actualUsd ?? null;
    }
  }
  return costs;
}

/**
 * @param {object} [opts]
 */
export function verifyBaselineFreeze(opts = {}) {
  const manifest = readBaselineFreezeMarker(opts.rootDir);
  const errors = [];

  if (!manifest) {
    return {
      BASELINE_FREEZE_VALID: false,
      freezeId: BASELINE_FREEZE_ID,
      errors: ["freeze_manifest_missing"],
    };
  }

  if (manifest.freezeId !== BASELINE_FREEZE_ID) errors.push("freeze_id_mismatch");
  if (manifest.IMMUTABLE !== true) errors.push("not_marked_immutable");
  if (manifest.observationCount !== EXPECTED.observationCount) {
    errors.push(`observation_count_${manifest.observationCount}`);
  }
  if (manifest.promptLibrary !== EXPECTED.promptLibrary) errors.push("prompt_library_mismatch");
  if (manifest.peerSetId !== EXPECTED.peerSetId) errors.push("peer_set_mismatch");
  if (manifest.metricVersion !== EXPECTED.metricVersion) errors.push("metric_version_mismatch");

  for (const [provider, exp] of Object.entries(EXPECTED.providers)) {
    const p = manifest.providers?.[provider];
    if (!p) {
      errors.push(`missing_provider_${provider}`);
      continue;
    }
    if (p.model !== exp.model) errors.push(`${provider}_model_mismatch`);
    if (p.observations !== exp.observations) errors.push(`${provider}_observations_mismatch`);
  }

  const baselineCosts = loadBaselineCosts(opts.rootDir);

  return {
    BASELINE_FREEZE_VALID: errors.length === 0,
    freezeId: manifest.freezeId,
    version: manifest.version,
    completedAt: manifest.completedAt,
    PROVIDERS: manifest.providers,
    OBSERVATIONS: manifest.observationCount,
    PROMPT_LIBRARY: manifest.promptLibrary,
    PEER_SET: manifest.peerSetId,
    PEER_SET_VERSION: manifest.peerSetVersion,
    METRIC_VERSION: manifest.metricVersion,
    IMMUTABLE: manifest.IMMUTABLE === true,
    baselineCosts,
    errors,
  };
}
