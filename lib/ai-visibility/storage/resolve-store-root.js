/**
 * Canonical AI Visibility store root resolution.
 * Execution and authorized reads must share this helper — do not hardcode phase folders in UI routes.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNTIME_ROOT = path.resolve(__dirname, "..", "..", "..", "data", "ai-visibility", "runtime");
const PHASE2E_ROOT = path.join(RUNTIME_ROOT, "phase2e");
/** Wave-1 showcase OpenAI baseline — isolated from Phase 2E validation history. */
const WAVE1_ROOT = path.join(RUNTIME_ROOT, "wave1-showcase");
/** Phase 3B.2 multi-provider validation namespaces (per provider). */
const PROVIDER_VALIDATION_ROOT = path.join(RUNTIME_ROOT, "provider-validation");
/** Phase 3B.3 full provider baselines (per provider). */
const PROVIDER_BASELINE_ROOT = path.join(RUNTIME_ROOT, "provider-baselines");
/** Isolated Stage B repeated-testing store. Never federate into live Brand AI reads. */
const STABILITY_STAGE_B_ROOT = path.join(RUNTIME_ROOT, "stability-stage-b");

function dirHasJson(dir) {
  try {
    if (!fs.existsSync(dir)) return false;
    return fs.readdirSync(dir).some((f) => f.endsWith(".json"));
  } catch {
    return false;
  }
}

/**
 * Prefer explicit env/options, then recover historical Phase 2E live validation data
 * under runtime/phase2e when present, else the base runtime root.
 *
 * Wave-1 showcase live runs should set AI_VISIBILITY_STORE_ROOT to WAVE1_ROOT
 * (or pass rootDir) so Phase 2E history is never overwritten.
 *
 * @param {{ rootDir?: string|null, wave1?: boolean }} [options]
 * @returns {{ rootDir: string, source: string, recoveredPhase2e: boolean, wave1Namespace: boolean }}
 */
export function resolveAiVisibilityStoreRoot(options = {}) {
  // Explicit rootDir always wins (tests / custom namespaces).
  if (options.rootDir != null && String(options.rootDir).trim()) {
    const resolved = path.resolve(String(options.rootDir).trim());
    return {
      rootDir: resolved,
      source: "options.rootDir",
      recoveredPhase2e: false,
      wave1Namespace: resolved === WAVE1_ROOT || /wave1-showcase/i.test(resolved),
    };
  }

  if (options.wave1 === true) {
    return {
      rootDir: WAVE1_ROOT,
      source: "wave1_showcase",
      recoveredPhase2e: false,
      wave1Namespace: true,
    };
  }

  const explicit =
    (process.env.AI_VISIBILITY_STORE_ROOT != null &&
      String(process.env.AI_VISIBILITY_STORE_ROOT).trim()) ||
    "";

  if (explicit) {
    const resolved = path.resolve(explicit);
    return {
      rootDir: resolved,
      source: "AI_VISIBILITY_STORE_ROOT",
      recoveredPhase2e: false,
      wave1Namespace: resolved === WAVE1_ROOT || /wave1-showcase/i.test(resolved),
    };
  }

  const phase2eHasData =
    dirHasJson(path.join(PHASE2E_ROOT, "summaries")) ||
    dirHasJson(path.join(PHASE2E_ROOT, "batches")) ||
    dirHasJson(path.join(PHASE2E_ROOT, "metric-snapshots"));

  if (phase2eHasData) {
    return {
      rootDir: PHASE2E_ROOT,
      source: "runtime/phase2e_recovered",
      recoveredPhase2e: true,
      wave1Namespace: false,
    };
  }

  return {
    rootDir: RUNTIME_ROOT,
    source: "runtime_default",
    recoveredPhase2e: false,
    wave1Namespace: false,
  };
}

export {
  RUNTIME_ROOT,
  PHASE2E_ROOT,
  WAVE1_ROOT,
  PROVIDER_VALIDATION_ROOT,
  PROVIDER_BASELINE_ROOT,
  STABILITY_STAGE_B_ROOT,
};

/**
 * Resolve isolated store root for a provider full baseline.
 * @param {string} providerId
 */
export function resolveProviderBaselineStoreRoot(providerId) {
  const id = String(providerId || "").trim().toLowerCase();
  return path.join(PROVIDER_BASELINE_ROOT, id);
}

/**
 * Resolve isolated store root for a provider validation wave.
 * @param {string} providerId
 */
export function resolveProviderValidationStoreRoot(providerId) {
  const id = String(providerId || "").trim().toLowerCase();
  return path.join(PROVIDER_VALIDATION_ROOT, id);
}

const MEASURED_BASELINE_PROVIDERS = Object.freeze(["gemini", "perplexity", "claude"]);

/**
 * Roots that hold the frozen four-provider baseline (OpenAI wave1 + other providers).
 * Empty when none of those namespaces have summaries yet.
 * @returns {string[]}
 */
export function listMeasuredBaselineStoreRoots() {
  const roots = [];
  if (dirHasJson(path.join(WAVE1_ROOT, "summaries"))) {
    roots.push(WAVE1_ROOT);
  }
  for (const providerId of MEASURED_BASELINE_PROVIDERS) {
    const root = resolveProviderBaselineStoreRoot(providerId);
    if (dirHasJson(path.join(root, "summaries"))) {
      roots.push(root);
    }
  }
  return roots;
}

/**
 * Brand AI Visibility authorized reads should prefer the four-provider baseline
 * federation over legacy Phase 2E recovery when baseline artifacts exist.
 *
 * Explicit AI_VISIBILITY_STORE_ROOT / options.rootDir still win (single store).
 *
 * @param {{ rootDir?: string|null }} [options]
 * @returns {{
 *   mode: "explicit"|"federated_measured_baseline"|"phase2e_or_default",
 *   rootDir: string|null,
 *   rootDirs: string[],
 *   source: string,
 * }}
 */
export function resolveBrandAiVisibilityReadRoots(options = {}) {
  if (options.rootDir != null && String(options.rootDir).trim()) {
    const resolved = path.resolve(String(options.rootDir).trim());
    return {
      mode: "explicit",
      rootDir: resolved,
      rootDirs: [resolved],
      source: "options.rootDir",
    };
  }

  const explicit =
    (process.env.AI_VISIBILITY_STORE_ROOT != null &&
      String(process.env.AI_VISIBILITY_STORE_ROOT).trim()) ||
    "";
  if (explicit) {
    const resolved = path.resolve(explicit);
    return {
      mode: "explicit",
      rootDir: resolved,
      rootDirs: [resolved],
      source: "AI_VISIBILITY_STORE_ROOT",
    };
  }

  const measured = listMeasuredBaselineStoreRoots();
  if (measured.length) {
    return {
      mode: "federated_measured_baseline",
      rootDir: null,
      rootDirs: measured,
      source: "federated_measured_baseline",
    };
  }

  const fallback = resolveAiVisibilityStoreRoot({});
  return {
    mode: "phase2e_or_default",
    rootDir: fallback.rootDir,
    rootDirs: [fallback.rootDir],
    source: fallback.source,
  };
}
