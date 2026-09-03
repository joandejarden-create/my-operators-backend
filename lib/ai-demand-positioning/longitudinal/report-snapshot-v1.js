/**
 * ADP_REPORT_SNAPSHOT_V1 — immutable certified customer report payload.
 *
 * AIRTABLE (future) = structured historical/query layer
 * IMMUTABLE SNAPSHOT = exact report reproduction layer
 *
 * HISTORICAL_REPORT_MUST_REPRODUCE_PUBLISHED_SNAPSHOT
 * WHAT_WAS_PUBLISHED_CAN_ALWAYS_BE_REPRODUCED
 * HISTORICAL_SNAPSHOT_NO_FUTURE_DATA
 * PUBLISHED_REPORT_SNAPSHOT_IMMUTABILITY
 * HISTORICAL_REPORT_DRIFT
 *
 * Presentation freeze recommendation: OPTION A — CONTENT SNAPSHOT
 * (exact historical analytical/customer content; controlled UI chrome may evolve;
 *  retain rendererVersion + publicationCommit for forensic reconstruction).
 */

import { createHash } from "crypto";
import { mkdirSync, writeFileSync, readFileSync, existsSync, readdirSync } from "fs";
import { join } from "path";

export const ADP_REPORT_SNAPSHOT_VERSION = "ADP_REPORT_SNAPSHOT_V1";

export const HISTORICAL_REPORT_MUST_REPRODUCE_PUBLISHED_SNAPSHOT =
  "HISTORICAL_REPORT_MUST_REPRODUCE_PUBLISHED_SNAPSHOT";
export const PUBLISHED_REPORT_SNAPSHOT_IMMUTABILITY = "PUBLISHED_REPORT_SNAPSHOT_IMMUTABILITY";
export const WHAT_WAS_PUBLISHED_CAN_ALWAYS_BE_REPRODUCED =
  "WHAT_WAS_PUBLISHED_CAN_ALWAYS_BE_REPRODUCED";
export const HISTORICAL_SNAPSHOT_NO_FUTURE_DATA = "HISTORICAL_SNAPSHOT_NO_FUTURE_DATA";
export const REPORT_HISTORY_PERSISTENCE_COMPLETE = "REPORT_HISTORY_PERSISTENCE_COMPLETE";
export const REPORT_SNAPSHOT_COMPLETENESS_INTEGRITY = "REPORT_SNAPSHOT_COMPLETENESS_INTEGRITY";
export const HISTORICAL_REPORT_REPRODUCTION_INTEGRITY =
  "HISTORICAL_REPORT_REPRODUCTION_INTEGRITY";
export const HISTORICAL_REPORT_DRIFT = "HISTORICAL_REPORT_DRIFT";

export const PRESENTATION_FREEZE_POLICY = Object.freeze({
  recommendation: "OPTION_A_CONTENT_SNAPSHOT",
  meaning:
    "Freeze exact historical analytical/customer content. Allow controlled UI styling evolution. Retain rendererVersion, publicationCommit, and original generated artifact for forensic reconstruction.",
  reject: "OPTION_B_FULL_PRESENTATION_SNAPSHOT_AS_DEFAULT",
  rejectReason:
    "Full UI freeze blocks design-system improvement and is harder to maintain across Webflow/chrome changes.",
});

/** Synthetic-only filesystem SoT for controlled validation (never production Airtable). */
export const SYNTHETIC_SNAPSHOT_ROOT = join(
  process.cwd(),
  "fixtures/ai-demand-positioning/longitudinal-synthetic/report-snapshots"
);

export const PRODUCTION_PUBLISHED_ROOT = join(
  process.cwd(),
  "data/ai-demand-positioning/published"
);

/**
 * Canonical JSON stringify for hashing (stable key order).
 */
export function stableStringify(value) {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  const keys = Object.keys(value).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(value[k])}`).join(",")}}`;
}

export function sha256OfPayload(payload) {
  return createHash("sha256").update(stableStringify(payload)).digest("hex");
}

/**
 * Build immutable report snapshot envelope.
 * `customerPayload` must contain all customer-visible content for that period.
 */
export function buildReportSnapshotV1({
  propertyId,
  periodId,
  monitoringDate,
  customerPayload,
  certificationStatus = "SYNTHETIC_CERTIFIED",
  disclosures = [],
  versions = {},
  synthetic = false,
  snapshotId = null,
}) {
  const generatedAt = new Date().toISOString();
  const id =
    snapshotId ||
    `adp_snap_${String(periodId || "").replace(/^adp_/, "")}_${createHash("sha256")
      .update(`${propertyId}|${periodId}|${generatedAt}`)
      .digest("hex")
      .slice(0, 10)}`;

  const body = {
    schema: ADP_REPORT_SNAPSHOT_VERSION,
    snapshotId: id,
    propertyId,
    periodId,
    monitoringDate,
    generatedAt,
    certifiedAt: generatedAt,
    publishedAt: null,
    certificationStatus,
    disclosures,
    synthetic: Boolean(synthetic),
    versions: {
      reportVersion: versions.reportVersion || ADP_REPORT_SNAPSHOT_VERSION,
      measurementContractVersion: versions.measurementContractVersion || "ADP_MEASUREMENT_CONTRACT_V1",
      parserVersion: versions.parserVersion || "adp_canonical_subject_parser_v1",
      entityResolverVersion: versions.entityResolverVersion || "adp_canonical_presence_per_observation_v1",
      rankingVersion: versions.rankingVersion || "adp_competitive_rank_history_v1",
      evidenceVersion: versions.evidenceVersion || "adp_positive_evidence_v1",
      assuranceVersion: versions.assuranceVersion || "adp_measurement_assurance_v1",
      rendererVersion: versions.rendererVersion || "adp_owner_report_renderer_v1",
      publicationCommit: versions.publicationCommit || null,
      promptIntegrityVersion: versions.promptIntegrityVersion || "ADP_PROMPT_INTEGRITY_CONTRACT_V1",
      promptPersistenceVersion: versions.promptPersistenceVersion || "ADP_PROMPT_PERSISTENCE_V1",
      promptManifestHash: versions.promptManifestHash || null,
    },
    presentationFreezePolicy: PRESENTATION_FREEZE_POLICY.recommendation,
    customerPayload,
  };

  const contentHash = sha256OfPayload(body.customerPayload);
  const envelopeHash = sha256OfPayload({ ...body, contentHash, envelopeHash: null });

  return {
    ...body,
    contentHash,
    envelopeHash,
    publishedAt: body.publishedAt,
    immutability: {
      frozen: true,
      overwriteProhibited: true,
      correctionRequiresNewSnapshot: true,
    },
  };
}

export function snapshotDir(propertyId) {
  return join(SYNTHETIC_SNAPSHOT_ROOT, propertyId);
}

export function persistReportSnapshot(snapshot, { root = SYNTHETIC_SNAPSHOT_ROOT } = {}) {
  if (!snapshot?.synthetic && root === SYNTHETIC_SNAPSHOT_ROOT) {
    // Allow only synthetic into fixture root by default
  }
  if (!snapshot?.synthetic && String(root).includes("longitudinal-synthetic")) {
    throw new Error("REFUSING_NON_SYNTHETIC_INTO_SYNTHETIC_SNAPSHOT_ROOT");
  }
  if (String(root).includes("data/ai-demand-positioning/published") && snapshot?.synthetic) {
    throw new Error("REFUSING_SYNTHETIC_INTO_PRODUCTION_PUBLISHED");
  }

  const dir = join(root, snapshot.propertyId);
  mkdirSync(dir, { recursive: true });
  const path = join(dir, `${snapshot.snapshotId}.json`);
  if (existsSync(path)) {
    throw new Error(`${PUBLISHED_REPORT_SNAPSHOT_IMMUTABILITY}: snapshot already exists ${path}`);
  }
  writeFileSync(path, JSON.stringify(snapshot, null, 2) + "\n");
  return path;
}

export function loadReportSnapshot(propertyId, snapshotId, { root = SYNTHETIC_SNAPSHOT_ROOT } = {}) {
  const path = join(root, propertyId, `${snapshotId}.json`);
  if (!existsSync(path)) return null;
  return JSON.parse(readFileSync(path, "utf8"));
}

export function listReportSnapshots(propertyId, { root = SYNTHETIC_SNAPSHOT_ROOT } = {}) {
  const dir = join(root, propertyId);
  if (!existsSync(dir)) return [];
  return readdirSync(dir)
    .filter((f) => f.endsWith(".json") && f.startsWith("adp_snap_"))
    .map((f) => JSON.parse(readFileSync(join(dir, f), "utf8")))
    .sort((a, b) => String(a.monitoringDate || "").localeCompare(String(b.monitoringDate || "")));
}

/**
 * Historical correction: preserve original; append corrected snapshot; mark current.
 */
export function createCorrectedSnapshot({
  originalSnapshot,
  correctedCustomerPayload,
  correctionReason,
  authorizedBy = "SYNTHETIC_ASSURANCE",
}) {
  if (!originalSnapshot) throw new Error("missing_original");
  const corrected = buildReportSnapshotV1({
    propertyId: originalSnapshot.propertyId,
    periodId: originalSnapshot.periodId,
    monitoringDate: originalSnapshot.monitoringDate,
    customerPayload: correctedCustomerPayload,
    certificationStatus: originalSnapshot.certificationStatus,
    disclosures: [
      ...(originalSnapshot.disclosures || []),
      {
        type: "HISTORICAL_CORRECTION",
        reason: correctionReason,
        replacesSnapshotId: originalSnapshot.snapshotId,
        authorizedBy,
      },
    ],
    versions: originalSnapshot.versions,
    synthetic: originalSnapshot.synthetic,
  });

  return {
    original: {
      ...originalSnapshot,
      supersededBy: corrected.snapshotId,
      historicalVersion: "ORIGINAL",
      isCurrentHistoricalVersion: false,
    },
    corrected: {
      ...corrected,
      correctsSnapshotId: originalSnapshot.snapshotId,
      correctionReason,
      correctionAuthorizedBy: authorizedBy,
      correctionAt: corrected.generatedAt,
      historicalVersion: "CORRECTED",
      isCurrentHistoricalVersion: true,
      changedFields: diffPayloadKeys(originalSnapshot.customerPayload, correctedCustomerPayload),
    },
  };
}

function diffPayloadKeys(a, b, prefix = "") {
  const changes = [];
  const keys = new Set([...Object.keys(a || {}), ...Object.keys(b || {})]);
  for (const k of keys) {
    const path = prefix ? `${prefix}.${k}` : k;
    const av = a?.[k];
    const bv = b?.[k];
    if (JSON.stringify(av) !== JSON.stringify(bv)) {
      if (
        av &&
        bv &&
        typeof av === "object" &&
        typeof bv === "object" &&
        !Array.isArray(av) &&
        !Array.isArray(bv)
      ) {
        changes.push(...diffPayloadKeys(av, bv, path));
      } else {
        changes.push(path);
      }
    }
  }
  return changes;
}

/**
 * Load historical report for viewer — knowledge stops at snapshot monitoring date.
 */
export function loadHistoricalReportView({
  propertyId,
  snapshotId,
  allSnapshots = null,
  root = SYNTHETIC_SNAPSHOT_ROOT,
}) {
  const snap = loadReportSnapshot(propertyId, snapshotId, { root });
  if (!snap) return { ok: false, error: "SNAPSHOT_NOT_FOUND" };

  const catalog = allSnapshots || listReportSnapshots(propertyId, { root });
  const futureLeaks = catalog.filter(
    (s) =>
      s.snapshotId !== snap.snapshotId &&
      String(s.monitoringDate || "") > String(snap.monitoringDate || "") &&
      // only relevant if somehow embedded in payload
      JSON.stringify(snap.customerPayload || {}).includes(s.periodId)
  );

  const defects = [];
  if (futureLeaks.length) {
    defects.push({
      code: HISTORICAL_SNAPSHOT_NO_FUTURE_DATA,
      detail: `payload references future periods: ${futureLeaks.map((s) => s.periodId).join(",")}`,
    });
  }

  // Trend points inside payload must not exceed monitoringDate
  const trendPoints = snap.customerPayload?.trends?.points || [];
  for (const pt of trendPoints) {
    if (String(pt.calendarDate || "") > String(snap.monitoringDate || "")) {
      defects.push({
        code: HISTORICAL_SNAPSHOT_NO_FUTURE_DATA,
        detail: `trend point ${pt.calendarDate} after snapshot ${snap.monitoringDate}`,
      });
    }
  }

  return {
    ok: defects.length === 0,
    mode: "HISTORICAL_REPORT_SNAPSHOT",
    snapshot: snap,
    customerPayload: snap.customerPayload,
    defects,
    knowledgeBoundary: `periodDate <= ${snap.monitoringDate}`,
  };
}

/**
 * Assert loaded historical payload equals frozen published payload (content).
 */
export function assertHistoricalReproduction(originalSnapshot, loadedView) {
  const defects = [];
  if (!loadedView?.ok) {
    defects.push({
      code: HISTORICAL_REPORT_REPRODUCTION_INTEGRITY,
      detail: loadedView?.error || "load failed",
    });
    return defects;
  }
  const a = sha256OfPayload(originalSnapshot.customerPayload);
  const b = sha256OfPayload(loadedView.customerPayload);
  if (a !== b) {
    defects.push({
      code: HISTORICAL_REPORT_DRIFT,
      detail: `contentHash mismatch ${a.slice(0, 12)} !== ${b.slice(0, 12)}`,
    });
  }
  if (originalSnapshot.contentHash && originalSnapshot.contentHash !== a) {
    defects.push({
      code: PUBLISHED_REPORT_SNAPSHOT_IMMUTABILITY,
      detail: "stored contentHash does not match payload",
    });
  }
  if (a === b) {
    // success marker
  }
  return defects;
}

/**
 * Simulate persistence completeness for weekly pipeline.
 */
export function evaluateReportHistoryPersistence({
  snapshotPersisted,
  structuredHistoryPersisted,
  snapshotHashVerified,
  certificationPassed,
}) {
  const defects = [];
  if (!certificationPassed) {
    defects.push({
      code: REPORT_HISTORY_PERSISTENCE_COMPLETE,
      detail: "cannot publish — certification not passed",
    });
  }
  if (!snapshotPersisted) {
    defects.push({
      code: REPORT_HISTORY_PERSISTENCE_COMPLETE,
      detail: "immutable snapshot not persisted",
    });
  }
  if (!structuredHistoryPersisted) {
    defects.push({
      code: REPORT_HISTORY_PERSISTENCE_COMPLETE,
      detail: "structured history not persisted",
    });
  }
  if (!snapshotHashVerified) {
    defects.push({
      code: REPORT_HISTORY_PERSISTENCE_COMPLETE,
      detail: "snapshot hash verification failed",
    });
  }
  return {
    pass: defects.length === 0,
    defects,
    publishAllowed: defects.length === 0 && certificationPassed,
  };
}
