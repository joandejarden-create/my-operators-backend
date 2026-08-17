/**
 * Four-provider baseline freeze marker (Phase 3B.5).
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { METRIC_VERSION } from "./config.js";
import { PEER_SET_ID_V2 } from "./peer-sets.js";
import { PROVIDER_BASELINE_SERIES } from "./provider-baseline-state.js";
import { finalizeClaudeWebSearchTool } from "./providers/claude-tool-audit.js";

export const BASELINE_FREEZE_ID = "FOUR_PROVIDER_BASELINE_V1_COMPLETE";
export const BASELINE_FREEZE_VERSION = "ai_visibility_four_provider_baseline_freeze_v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export function buildBaselineFreezeManifest(manifestInput = {}) {
  const claudeTool = finalizeClaudeWebSearchTool();
  return {
    freezeId: BASELINE_FREEZE_ID,
    version: BASELINE_FREEZE_VERSION,
    completedAt: manifestInput.completedAt || new Date().toISOString(),
    observationCount: 336,
    targetObservationCount: 336,
    promptLibrary: "showcase_prompts_v1",
    peerSetId: PEER_SET_ID_V2,
    peerSetVersion: "2",
    metricVersion: METRIC_VERSION,
    providers: {
      openai: {
        model: "gpt-5.6",
        waveId: manifestInput.openai?.waveId || "aiv_wave1_openai_showcase_20260814_0143_8367c6",
        seriesId: PROVIDER_BASELINE_SERIES.openai,
        observations: 84,
        monitoringRunPurpose: "baseline",
        startedAt: manifestInput.openai?.startedAt || null,
        completedAt: manifestInput.openai?.completedAt || null,
      },
      gemini: {
        model: "gemini-3.6-flash",
        grounding: "google_search",
        waveId: manifestInput.gemini?.waveId || null,
        seriesId: PROVIDER_BASELINE_SERIES.gemini,
        observations: 84,
        monitoringRunPurpose: "baseline",
        startedAt: manifestInput.gemini?.startedAt || null,
        completedAt: manifestInput.gemini?.completedAt || null,
      },
      perplexity: {
        model: "sonar",
        waveId: manifestInput.perplexity?.waveId || null,
        seriesId: PROVIDER_BASELINE_SERIES.perplexity,
        observations: 84,
        monitoringRunPurpose: "baseline",
        startedAt: manifestInput.perplexity?.startedAt || null,
        completedAt: manifestInput.perplexity?.completedAt || null,
      },
      claude: {
        model: "claude-sonnet-4-6",
        webSearchTool: claudeTool.SELECTED_TOOL_VERSION,
        allowedCallers: claudeTool.SELECTED_ALLOWED_CALLERS,
        maxUses: claudeTool.MAX_USES,
        timeoutMs: claudeTool.TIMEOUT_MS,
        waveId: manifestInput.claude?.waveId || null,
        seriesId: PROVIDER_BASELINE_SERIES.claude,
        observations: 84,
        monitoringRunPurpose: "baseline",
        startedAt: manifestInput.claude?.startedAt || null,
        completedAt: manifestInput.claude?.completedAt || null,
      },
    },
    TREND_AVAILABLE: false,
    IMMUTABLE: true,
  };
}

export function writeBaselineFreezeMarker(manifest, rootDir) {
  const base =
    rootDir ||
    path.join(__dirname, "..", "..", "data", "ai-visibility", "runtime", "baseline-freeze");
  fs.mkdirSync(base, { recursive: true });
  const outPath = path.join(base, `${BASELINE_FREEZE_ID}.json`);
  fs.writeFileSync(outPath, JSON.stringify(manifest, null, 2), "utf8");
  return outPath;
}

export function readBaselineFreezeMarker(rootDir) {
  const base =
    rootDir ||
    path.join(__dirname, "..", "..", "data", "ai-visibility", "runtime", "baseline-freeze");
  const outPath = path.join(base, `${BASELINE_FREEZE_ID}.json`);
  if (!fs.existsSync(outPath)) return null;
  return JSON.parse(fs.readFileSync(outPath, "utf8"));
}
