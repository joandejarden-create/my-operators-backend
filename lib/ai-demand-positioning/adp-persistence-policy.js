/**
 * ADP persistence policy — explicit SoT contract for replication.
 *
 * Do not silently change architecture:
 * - PRIMARY_SOT = filesystem published snapshots
 * - LIVE_OVERLAY = Airtable Published Reports (optional)
 * - HISTORY_AIRTABLE = disabled until intentionally enabled
 */

export const ADP_PERSISTENCE_POLICY = Object.freeze({
  version: "adp_persistence_policy_v1",
  PRIMARY_SOT: "FILESYSTEM_SNAPSHOT",
  LIVE_OVERLAY: "AIRTABLE_PUBLISHED_REPORTS",
  HISTORY_AIRTABLE: "DISABLED",
  historyEnableEnv: "ADP_HISTORY_AIRTABLE_WRITE_APPLY",
  liveOverlayBaseEnv: "ADP_AIRTABLE_BASE_ID",
});

function envFlagOn(v) {
  const s = String(v || "").trim().toLowerCase();
  return s === "1" || s === "true";
}

export function getAdpPersistencePolicy(env = process.env) {
  const historyEnabled =
    envFlagOn(env.ADP_HISTORY_WRITES_ENABLED) &&
    envFlagOn(env.ADP_HISTORY_AIRTABLE_WRITE_APPLY);
  return {
    ...ADP_PERSISTENCE_POLICY,
    HISTORY_AIRTABLE: historyEnabled ? "ENABLED" : "DISABLED",
    historyEnableEnvAccepts: ["1", "true"],
    persistenceDistinctFromPublication: true,
    primaryCustomerAuthority: "FILESYSTEM_DURABLE_PUBLISHED_REGISTRY",
    airtableUnavailableImpliesSuccess: false,
    airtableFailureMustNotRollbackFsPublication: true,
    currentVersionRule: "ADP_CURRENT_CERTIFIED_MEASUREMENT_V1",
    secondHotelHistoryRequired: false,
    secondHotelHistoryReason:
      "Filesystem published snapshots + Live overlay (when configured) are sufficient for second-hotel replication; Airtable history remains optional and gated.",
  };
}
