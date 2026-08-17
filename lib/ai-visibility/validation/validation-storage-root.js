/**
 * Canonical AI Intelligence validation report storage root.
 * Writer (CLI) and reader (Scorecard API) MUST resolve the same path.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_VALIDATION_ROOT = path.resolve(
  __dirname,
  "../../../data/ai-visibility/validation"
);

export const VALIDATION_STORAGE_VERSION = "ai_intelligence_validation_storage_v1";

/**
 * @param {{ outDir?: string|null }} [options]
 */
export function resolveValidationStorageRoot(options = {}) {
  if (options.outDir != null && String(options.outDir).trim()) {
    return {
      rootDir: path.resolve(String(options.outDir).trim()),
      source: "options.outDir",
    };
  }
  const env =
    process.env.AI_INTELLIGENCE_VALIDATION_ROOT != null &&
    String(process.env.AI_INTELLIGENCE_VALIDATION_ROOT).trim();
  if (env) {
    return {
      rootDir: path.resolve(env),
      source: "AI_INTELLIGENCE_VALIDATION_ROOT",
    };
  }
  return {
    rootDir: DEFAULT_VALIDATION_ROOT,
    source: "default_data_ai_visibility_validation",
  };
}

export function ensureValidationStorageDirs(rootDir) {
  fs.mkdirSync(rootDir, { recursive: true });
  fs.mkdirSync(path.join(rootDir, "manifests"), { recursive: true });
  return rootDir;
}

export function validationReportPath(rootDir) {
  return path.join(rootDir, "latest-validation-report.json");
}

export function validationManifestPath(rootDir, batchId) {
  return path.join(rootDir, "manifests", `${batchId}.json`);
}

export { DEFAULT_VALIDATION_ROOT };
