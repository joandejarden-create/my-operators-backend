export { createFileStore, createAiVisibilityStore } from "./file-store.js";
export { createFederatedFileStore } from "./federated-file-store.js";
export {
  resolveAiVisibilityStoreRoot,
  resolveBrandAiVisibilityReadRoots,
  listMeasuredBaselineStoreRoots,
  RUNTIME_ROOT,
  PHASE2E_ROOT,
  WAVE1_ROOT,
  PROVIDER_BASELINE_ROOT,
  STABILITY_STAGE_B_ROOT,
} from "./resolve-store-root.js";
import { createFileStore } from "./file-store.js";
import { createFederatedFileStore } from "./federated-file-store.js";
import { resolveBrandAiVisibilityReadRoots } from "./resolve-store-root.js";

/**
 * Store used by Brand AI Visibility API reads.
 * Prefer federated four-provider baseline when present; else Phase 2E recovery.
 */
export function createBrandAiVisibilityReadStore(options = {}) {
  const resolved = resolveBrandAiVisibilityReadRoots(options);
  if (resolved.mode === "federated_measured_baseline" && resolved.rootDirs.length > 1) {
    return createFederatedFileStore({
      rootDirs: resolved.rootDirs,
      source: resolved.source,
    });
  }
  if (resolved.mode === "federated_measured_baseline" && resolved.rootDirs.length === 1) {
    return createFileStore({ rootDir: resolved.rootDirs[0] });
  }
  return createFileStore({
    rootDir: resolved.rootDir || resolved.rootDirs[0] || undefined,
  });
}
