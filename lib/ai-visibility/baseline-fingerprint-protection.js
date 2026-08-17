/**
 * Baseline fingerprint protection — completed observations must not re-execute (Phase 3B.5).
 */

export const BASELINE_FINGERPRINT_PROTECTION_VERSION =
  "ai_visibility_baseline_fingerprint_protection_v1";

export class CompletedFingerprintProtectionError extends Error {
  constructor(fingerprint, provider, waveId) {
    super(
      `Completed fingerprint ${fingerprint} for ${provider} baseline ${waveId} is protected from re-execution`
    );
    this.name = "CompletedFingerprintProtectionError";
    this.fingerprint = fingerprint;
    this.provider = provider;
    this.waveId = waveId;
  }
}

/**
 * Abort locally before provider request if fingerprint already succeeded.
 * @param {object} checkpoint
 * @param {string} fingerprint
 * @param {object} [opts]
 */
export function assertFingerprintExecutable(checkpoint, fingerprint, opts = {}) {
  if (!checkpoint) {
    throw new Error("assertFingerprintExecutable: missing checkpoint");
  }
  const fp = String(fingerprint || "");
  if (!fp) throw new Error("assertFingerprintExecutable: missing fingerprint");

  if (checkpoint.completedFingerprints?.[fp]) {
    if (opts.protectCompleted !== false) {
      throw new CompletedFingerprintProtectionError(
        fp,
        checkpoint.provider,
        checkpoint.waveId
      );
    }
    return { executable: false, reason: "already_completed" };
  }
  return { executable: true };
}

/**
 * Count protected successful fingerprints across checkpoint map(s).
 * @param {Record<string, object>} checkpointsByProvider
 */
export function countProtectedFingerprints(checkpointsByProvider = {}) {
  let count = 0;
  for (const cp of Object.values(checkpointsByProvider)) {
    if (!cp?.completedFingerprints) continue;
    count += Object.keys(cp.completedFingerprints).length;
  }
  return count;
}
