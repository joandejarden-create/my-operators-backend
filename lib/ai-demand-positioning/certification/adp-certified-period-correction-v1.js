/**
 * ADP_CERTIFIED_PERIOD_CORRECTION_V1 — immutable correction lineage.
 * Never silently overwrite certified published metrics.
 */

import { existsSync, mkdirSync, readFileSync, writeFileSync, readdirSync } from "fs";
import { join } from "path";

export const ADP_CERTIFIED_PERIOD_CORRECTION_V1 = "ADP_CERTIFIED_PERIOD_CORRECTION_V1";
export const ADP_CERTIFIED_DATA_CORRECTION_GOVERNANCE = "ADP_CERTIFIED_DATA_CORRECTION_GOVERNANCE";

const CORRECTIONS_DIR = join(
  process.cwd(),
  "data/ai-demand-positioning/corrections/certified-period-corrections-v1"
);

export function ensureCorrectionsDir() {
  mkdirSync(CORRECTIONS_DIR, { recursive: true });
  return CORRECTIONS_DIR;
}

/**
 * @param {object} input
 */
export function buildCertifiedPeriodCorrectionRecord(input = {}) {
  const timestamp = input.correctionTimestamp || new Date().toISOString();
  const correctionId =
    input.correctionId ||
    `adp_corr_${String(input.propertyId || "unknown").replace(/^adp_/, "")}_${Date.now().toString(36)}`;

  return {
    schema: ADP_CERTIFIED_PERIOD_CORRECTION_V1,
    correctionId,
    propertyId: input.propertyId,
    periodId: input.periodId,
    rootCause: input.rootCause || null,
    evidence: input.evidence || null,
    affectedMetrics: input.affectedMetrics || [],
    oldValues: input.oldValues || {},
    newValues: input.newValues || {},
    correctionTimestamp: timestamp,
    supersededVersion: input.supersededVersion || null,
    correctedVersion: input.correctedVersion || null,
    clientExposureStatus: input.clientExposureStatus || "NOT_DISTRIBUTED_EXTERNALLY",
    externalShareDistributed: Boolean(input.externalShareDistributed),
    immutableLineage: true,
    notes: input.notes || null,
  };
}

export function writeCertifiedPeriodCorrection(record) {
  ensureCorrectionsDir();
  const file = join(CORRECTIONS_DIR, `${record.correctionId}.json`);
  writeFileSync(file, JSON.stringify(record, null, 2) + "\n");
  return { ok: true, file, correctionId: record.correctionId };
}

export function listCertifiedPeriodCorrections() {
  if (!existsSync(CORRECTIONS_DIR)) return [];
  return readdirSync(CORRECTIONS_DIR)
    .filter((f) => f.endsWith(".json"))
    .map((f) => JSON.parse(readFileSync(join(CORRECTIONS_DIR, f), "utf8")));
}

export function evaluateCertifiedDataCorrectionGovernance({
  certifiedMetricsChanged = false,
  corrections = [],
  externalShareDistributed = false,
} = {}) {
  if (!certifiedMetricsChanged) {
    return {
      gate: ADP_CERTIFIED_DATA_CORRECTION_GOVERNANCE,
      pass: true,
      note: "No certified published metrics changed.",
    };
  }
  const missing = corrections.filter(
    (c) =>
      !c?.correctionId ||
      !c?.periodId ||
      !c?.oldValues ||
      !c?.newValues ||
      !c?.rootCause ||
      c.immutableLineage !== true
  );
  const exposureOk = corrections.every(
    (c) =>
      c.externalShareDistributed === Boolean(externalShareDistributed) ||
      c.clientExposureStatus === "NOT_DISTRIBUTED_EXTERNALLY"
  );
  const pass = missing.length === 0 && exposureOk;
  return {
    gate: ADP_CERTIFIED_DATA_CORRECTION_GOVERNANCE,
    pass,
    correctionCount: corrections.length,
    missingRequiredFields: missing.length,
    exposureOk,
    note: pass
      ? "Certified metric changes recorded with immutable correction lineage."
      : "Certified metrics changed without complete ADP_CERTIFIED_PERIOD_CORRECTION_V1 records.",
  };
}
