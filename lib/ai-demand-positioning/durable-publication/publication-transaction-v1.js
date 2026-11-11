/**
 * Governed ADP publication transaction against durable store.
 *
 * Steps:
 *  1. Validate authorized staged version (caller-supplied gate)
 *  2. Acquire per-property lock (concurrent conflict prevention)
 *  3. Write immutable snapshot (+ evidence/metadata)
 *  4. Verify SHA-256
 *  5. Atomically activate published-current pointer
 *  6. Verify resolve
 *  7. Preserve previous version in registry history
 *
 * If anything fails before pointer activation, previous published remains current.
 */

import { createHash, randomBytes } from "crypto";
import {
  createFilesystemDurablePublishedStore,
  ADP_DURABLE_LOCK_HELD,
} from "./filesystem-durable-store-v1.js";
import { isAdpDurablePublishedStoreEnabled } from "./resolve-durable-root-v1.js";

export const ADP_DURABLE_PUBLICATION_TXN_V1 = "ADP_DURABLE_PUBLICATION_TXN_V1";

function considerationOf(report) {
  return (
    report?.payload?.executiveMetrics?.considerationRate?.rate ??
    report?.executiveMetrics?.considerationRate?.rate ??
    report?.payload?.executiveMetrics?.considerationRate?.percentage ??
    null
  );
}

/**
 * @param {object} input
 * @param {string} input.propertyId
 * @param {string} input.periodId
 * @param {object} input.report
 * @param {object} [input.evidence]
 * @param {object} [input.manifest]
 * @param {string} [input.sourceStagedVersionId]
 * @param {object} [input.store] durable store instance
 * @param {object} [input.env]
 * @param {boolean} [input.skipStagedValidation] test-only
 * @param {(args:object)=>void} [input.validateStaged] optional gate; throw to abort
 */
export function commitDurableAdpPublication(input = {}) {
  const env = input.env || process.env;
  const propertyId = String(input.propertyId || "").trim();
  const periodId = String(input.periodId || input.manifest?.latestPeriodId || "").trim();
  if (!propertyId || !periodId) {
    return { ok: false, code: "MISSING_IDS", message: "propertyId and periodId required" };
  }
  if (!input.report) {
    return { ok: false, code: "MISSING_REPORT", message: "report required" };
  }

  if (typeof input.validateStaged === "function") {
    input.validateStaged({
      propertyId,
      periodId,
      sourceStagedVersionId: input.sourceStagedVersionId || null,
    });
  } else if (input.skipStagedValidation !== true && !input.sourceStagedVersionId && input.requireStagedVersionId === true) {
    return {
      ok: false,
      code: "STAGED_VERSION_REQUIRED",
      message: "Governed publication requires sourceStagedVersionId",
    };
  }

  const store =
    input.store ||
    createFilesystemDurablePublishedStore({
      env,
      root: input.root,
    });

  let lock;
  try {
    lock = store.acquirePublicationLock(propertyId);
  } catch (err) {
    if (err.code === ADP_DURABLE_LOCK_HELD) {
      return { ok: false, code: ADP_DURABLE_LOCK_HELD, message: err.message, lock: err.lock };
    }
    throw err;
  }

  const txnId = `txn_${propertyId}_${Date.now()}_${randomBytes(4).toString("hex")}`;
  const previous = store.getCurrentEntry(propertyId);

  try {
    store.beginStaging(txnId);

    const written = store.writeImmutableSnapshot({
      propertyId,
      periodId,
      report: input.report,
      evidence: input.evidence,
      manifest: {
        ...(input.manifest || {}),
        propertyId,
        latestPeriodId: periodId,
        sourceStagedVersionId: input.sourceStagedVersionId || input.manifest?.sourceStagedVersionId || null,
      },
    });

    // Pointer activation is the commit point
    const activated = store.activatePublishedCurrent(
      {
        propertyId,
        periodId,
        propertyName: input.manifest?.propertyName || input.report?.property?.name || null,
        reportFile: `report-${periodId}.json`,
        evidenceFile: `evidence-${periodId}.json`,
        reportSha256: written.reportSha256,
        evidenceSha256: written.evidenceSha256,
        reportBytes: Buffer.byteLength(JSON.stringify(input.report)),
        // Authoritative publication event timestamp for Admin "Published".
        // Distinct from measurementPeriodId (Measured). Never omit on commit.
        latestPublishedAt:
          input.latestPublishedAt ||
          input.manifest?.latestPublishedAt ||
          new Date().toISOString(),
        sourceStagedVersionId:
          input.sourceStagedVersionId || input.manifest?.sourceStagedVersionId || null,
        considerationRate: considerationOf(input.report),
        publishStatus: "Live",
        provenance: input.provenance || ADP_DURABLE_PUBLICATION_TXN_V1,
      },
      { previousEntry: previous }
    );

    const resolved = store.readPublishedReport(propertyId);
    if (!resolved || resolved.entry.periodId !== periodId) {
      return {
        ok: false,
        code: "POST_COMMIT_RESOLVE_FAILED",
        message: "Published pointer did not resolve to new period after activation",
        previous,
        written,
        activated,
      };
    }
    if (resolved.entry.reportSha256 !== written.reportSha256) {
      return {
        ok: false,
        code: "POST_COMMIT_INTEGRITY_FAILED",
        message: "Resolved report sha mismatch after activation",
        previous,
        written,
        activated,
      };
    }

    return {
      ok: true,
      contract: ADP_DURABLE_PUBLICATION_TXN_V1,
      txnId,
      propertyId,
      periodId,
      previous,
      current: activated.current,
      reportSha256: written.reportSha256,
      evidenceSha256: written.evidenceSha256,
      durableRoot: store.root,
    };
  } catch (err) {
    // Fail before / during activation → previous remains current
    const still = store.getCurrentEntry(propertyId);
    return {
      ok: false,
      code: err.code || "PUBLICATION_TXN_FAILED",
      message: err.message,
      previous,
      stillCurrent: still,
      previousRemainsAvailable:
        !previous || (still && still.periodId === previous.periodId),
      recover: store.recoverInterruptedPublication(propertyId),
    };
  } finally {
    lock?.release?.();
  }
}

export function durableStoreEnabled(env = process.env) {
  return isAdpDurablePublishedStoreEnabled(env);
}

export function fingerprintReport(report) {
  return createHash("sha256").update(JSON.stringify(report)).digest("hex");
}
