/**
 * Internal Environment Status for Dealality Integrated Staging V1.
 * Read-only. Never triggers deploy or merge.
 */
import {
  buildEnvironmentStatusPayload,
  readStagingState,
  isIntegratedStagingCheckout,
  INTEGRATED_STAGING_DEFAULT_PATH,
  INTEGRATED_STAGING_PORT,
} from "../lib/dealality-dev-os/integrated-staging-v1.js";

function resolveStagingRoot() {
  if (isIntegratedStagingCheckout(process.cwd())) return process.cwd();
  return INTEGRATED_STAGING_DEFAULT_PATH;
}

export function getAdminIntegratedStagingStatus(req, res) {
  try {
    const stagingRoot = resolveStagingRoot();
    const state = readStagingState(stagingRoot);
    const payload = buildEnvironmentStatusPayload(state, {
      serverStatus: isIntegratedStagingCheckout(process.cwd())
        ? "RUNNING"
        : state.serverStatus || "UNKNOWN",
    });
    payload.servingCheckout = process.cwd();
    payload.isServingFromStagingCheckout = isIntegratedStagingCheckout(
      process.cwd(),
    );
    payload.expectedPort = INTEGRATED_STAGING_PORT;
    payload.stagingRootResolved = stagingRoot;
    return res.json(payload);
  } catch (err) {
    console.error("[integrated-staging-status]", err);
    return res.status(500).json({
      ok: false,
      error: "staging_status_unavailable",
      message: String(err?.message || err),
    });
  }
}
