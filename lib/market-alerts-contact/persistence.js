/**
 * Contact persistence mode — Phase A local_dev only.
 * Production-durable persistence is selected separately after the provider canary.
 * Do not create Airtable tables/fields here.
 */

import { logContactOp } from "./safe-log.js";

export const CONTACT_PERSISTENCE_MODE = Object.freeze({
  LOCAL_DEV: "local_dev",
  DISABLED: "disabled",
  // Future (not implemented): airtable | durable_store
});

export function getContactPersistenceMode() {
  const raw = String(process.env.CONTACT_PERSISTENCE_MODE || CONTACT_PERSISTENCE_MODE.LOCAL_DEV)
    .trim()
    .toLowerCase();
  if (raw === CONTACT_PERSISTENCE_MODE.DISABLED) return CONTACT_PERSISTENCE_MODE.DISABLED;
  return CONTACT_PERSISTENCE_MODE.LOCAL_DEV;
}

/**
 * Local JSON under data/market-alerts/contact-enrichment/ is Phase A development
 * persistence only — not production-durable.
 *
 * In NODE_ENV=production with CONTACT_PERSISTENCE_MODE=local_dev, writes require
 * ALLOW_EPHEMERAL_CONTACT_PERSISTENCE=true; otherwise persistence is blocked safely.
 */
export function assessContactPersistenceSafety() {
  const mode = getContactPersistenceMode();
  const isProduction = String(process.env.NODE_ENV || "").toLowerCase() === "production";
  const allowEphemeral =
    String(process.env.ALLOW_EPHEMERAL_CONTACT_PERSISTENCE || "").toLowerCase() === "true";

  if (mode === CONTACT_PERSISTENCE_MODE.DISABLED) {
    return {
      ok: false,
      mode,
      writable: false,
      reason: "CONTACT_PERSISTENCE_DISABLED",
      durable: false,
      note: "Persistence disabled. Production durable store not configured.",
    };
  }

  if (isProduction && mode === CONTACT_PERSISTENCE_MODE.LOCAL_DEV && !allowEphemeral) {
    return {
      ok: false,
      mode,
      writable: false,
      reason: "EPHEMERAL_PERSISTENCE_BLOCKED_IN_PRODUCTION",
      durable: false,
      note:
        "CONTACT_PERSISTENCE_MODE=local_dev is Phase A development only. Set ALLOW_EPHEMERAL_CONTACT_PERSISTENCE=true to permit ephemeral local JSON in production, or select production persistence after the provider canary.",
    };
  }

  return {
    ok: true,
    mode,
    writable: true,
    reason: null,
    durable: false,
    note:
      mode === CONTACT_PERSISTENCE_MODE.LOCAL_DEV
        ? "Phase A local_dev JSON only — not production-durable. Production persistence TBD after canary."
        : null,
  };
}

export function assertContactPersistenceWritable() {
  const assessment = assessContactPersistenceSafety();
  if (!assessment.writable) {
    logContactOp("persistence_blocked", {
      reason: assessment.reason,
      mode: assessment.mode,
      nodeEnv: process.env.NODE_ENV || null,
    });
  }
  return assessment;
}
