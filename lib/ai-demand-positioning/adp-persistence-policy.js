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

export function getAdpPersistencePolicy(env = process.env) {
  const historyEnabled =
    String(env.ADP_HISTORY_WRITES_ENABLED || "").trim() === "1" &&
    String(env.ADP_HISTORY_AIRTABLE_WRITE_APPLY || "").trim() === "1";
  return {
    ...ADP_PERSISTENCE_POLICY,
    HISTORY_AIRTABLE: historyEnabled ? "ENABLED" : "DISABLED",
    secondHotelHistoryRequired: false,
    secondHotelHistoryReason:
      "Filesystem published snapshots + Live overlay (when configured) are sufficient for second-hotel replication; Airtable history remains optional and gated.",
  };
}
