/**
 * ADP staged report store — independent of customer published/ snapshots.
 * Founder local review loads staged; customers never see these via default report path.
 *
 * Contract (v1.1):
 * - Core owner reports promote `latestStagedVersionId` (customer-facing review pointer).
 * - Loyalty measurement artifacts stage separately via `latestLoyaltyDraftVersionId`
 *   and MUST NOT replace the Core customer pointer.
 * - `loadStagedOwnerPayload` never returns a LOYALTY_BPP-only draft as the Core report.
 */

import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
  readdirSync,
} from "fs";
import { join } from "path";
import { createHash } from "crypto";

export const ADP_STAGED_REPORT_STORE_VERSION = "adp_staged_report_store_v1";

function rootDir(cwd = process.cwd()) {
  return join(cwd, "data/ai-demand-positioning/staged");
}

function propertyDir(propertyId, cwd = process.cwd()) {
  return join(rootDir(cwd), String(propertyId || "").trim());
}

function sha(obj) {
  return createHash("sha256").update(JSON.stringify(obj)).digest("hex");
}

function unwrapPayload(doc) {
  if (!doc || typeof doc !== "object") return null;
  return doc.payload && typeof doc.payload === "object" ? doc.payload : doc;
}

/**
 * Loyalty STANDARD measurement artifacts are not Core owner reports.
 * They lack competitiveSet / Property Snapshot parents and must not drive SPA Core render.
 *
 * Payload shape wins over stale manifest certification: a Core owner payload with
 * competitiveSet/demandCapture is never treated as loyalty even if cert still says
 * STAGED_LOYALTY_DRAFT (legacy pointer corruption).
 */
export function isLoyaltyMeasurementArtifact(payload, certification = null) {
  if (!payload || typeof payload !== "object") {
    return (
      certification?.status === "STAGED_LOYALTY_DRAFT" ||
      certification?.measurementFamily === "LOYALTY_BPP"
    );
  }
  // Core-shaped payload always wins (do not let stale loyalty cert reject Core files).
  if (payload.competitiveSet || payload.demandCapture || payload.executiveMetrics) {
    if (payload.measurementFamily === "LOYALTY_BPP" && !payload.competitiveSet) {
      return true;
    }
    return false;
  }
  if (payload.measurementFamily === "LOYALTY_BPP") return true;
  if (payload.loyaltyBpp && !payload.competitiveSet && !payload.demandCapture) {
    return true;
  }
  if (certification?.status === "STAGED_LOYALTY_DRAFT") return true;
  if (certification?.measurementFamily === "LOYALTY_BPP") return true;
  return false;
}

/**
 * True when payload is a Core (or legacy Core-shaped) owner report suitable for ADP SPA.
 * Presence of competitiveSet parent is preferred; demandCapture/executiveMetrics also qualify.
 * Missing topObservedAlternative leaf is allowed (valid absence → "—").
 */
export function isCoreOwnerReportPayload(payload, certification = null) {
  if (!payload || typeof payload !== "object") return false;
  if (isLoyaltyMeasurementArtifact(payload, certification)) return false;
  return !!(
    payload.competitiveSet ||
    payload.demandCapture ||
    payload.executiveMetrics
  );
}

function readStagedDoc(propertyId, reportFile, cwd = process.cwd()) {
  if (!reportFile) return null;
  const reportPath = join(propertyDir(propertyId, cwd), reportFile);
  if (!existsSync(reportPath)) return null;
  try {
    return JSON.parse(readFileSync(reportPath, "utf8"));
  } catch {
    return null;
  }
}

function versionIdFromReportFile(reportFile) {
  const m = String(reportFile || "").match(/^report-(staged_[^.]+)\.json$/);
  return m ? m[1] : null;
}

/**
 * Walk previous pointer + directory listing to find the newest Core owner staged report.
 */
export function resolveCustomerFacingStagedReport(propertyId, cwd = process.cwd()) {
  const id = String(propertyId || "").trim();
  const manifest = loadStagedManifest(id, cwd);
  if (!manifest) return null;

  const tried = new Set();
  const candidates = [];

  function consider(versionId, reportFile, certHint) {
    if (!reportFile || tried.has(reportFile)) return;
    tried.add(reportFile);
    const doc = readStagedDoc(id, reportFile, cwd);
    if (!doc) return;
    const payload = unwrapPayload(doc);
    const cert = certHint || doc.certification || null;
    if (!isCoreOwnerReportPayload(payload, cert)) return;
    candidates.push({
      versionId: versionId || versionIdFromReportFile(reportFile) || manifest.latestStagedVersionId,
      reportFile,
      artifactHash: sha(doc),
      payload,
      certification: cert,
      stagedAt: doc.stagedAt || null,
    });
  }

  // Prefer explicit customer pointer when Core-shaped (authoritative after repair / Core save).
  if (manifest.reportFile) {
    const doc = readStagedDoc(id, manifest.reportFile, cwd);
    const payload = unwrapPayload(doc);
    if (doc && isCoreOwnerReportPayload(payload, null)) {
      return {
        versionId: manifest.latestStagedVersionId || versionIdFromReportFile(manifest.reportFile),
        reportFile: manifest.reportFile,
        artifactHash: manifest.artifactHash || sha(doc),
        payload,
        certification: manifest.certification || null,
        stagedAt: doc.stagedAt || manifest.stagedAt || null,
      };
    }
  }

  // Prefer current pointer when Core-shaped (ignore stale loyalty cert on Core files).
  consider(manifest.latestStagedVersionId, manifest.reportFile, null);

  // Walk previous pointer chain from manifest metadata.
  let prevId = manifest.previousStagedVersionId;
  let guard = 0;
  while (prevId && guard < 40) {
    guard += 1;
    const prevFile = `report-${prevId}.json`;
    consider(prevId, prevFile, null);
    const prevDoc = readStagedDoc(id, prevFile, cwd);
    // No nested previous in file; break after one hop unless we scan dir below.
    prevId = null;
    if (prevDoc?.previousStagedVersionId) {
      prevId = prevDoc.previousStagedVersionId;
    }
  }

  // Directory scan fallback (handles loyalty overwriting latest pointer).
  const dir = propertyDir(id, cwd);
  if (existsSync(dir)) {
    const files = readdirSync(dir)
      .filter((f) => f.startsWith("report-staged_") && f.endsWith(".json"))
      .sort()
      .reverse();
    for (const f of files) {
      consider(versionIdFromReportFile(f), f, null);
    }
  }

  if (!candidates.length) return null;

  // Newest Core by version id (time-encoded) / file name order already reverse-sorted preferred.
  candidates.sort((a, b) => String(b.versionId).localeCompare(String(a.versionId)));
  return candidates[0];
}

/**
 * Persist an immutable staged report version for review.
 * Does not touch data/ai-demand-positioning/published/.
 *
 * @param {object} opts
 * @param {boolean} [opts.customerFacing=true] — when false (Loyalty artifacts), do not
 *   promote `latestStagedVersionId` / `reportFile` away from the Core owner pointer.
 */
export function saveStagedAdpReport({
  propertyId,
  periodId = null,
  runId = null,
  workflowVersion = null,
  payload,
  certification = null,
  source = null,
  note = null,
  customerFacing = true,
  cwd = process.cwd(),
} = {}) {
  const id = String(propertyId || "").trim();
  if (!id.startsWith("adp_") || !payload || typeof payload !== "object") {
    return { ok: false, error: "invalid_staged_report_input" };
  }

  const dir = propertyDir(id, cwd);
  mkdirSync(dir, { recursive: true });

  const versionId = `staged_${Date.now().toString(36)}_${sha(payload).slice(0, 10)}`;
  const reportFile = `report-${versionId}.json`;
  const body =
    payload.payload && typeof payload.payload === "object"
      ? payload
      : {
          ok: true,
          propertyId: id,
          periodId,
          stagedAt: new Date().toISOString(),
          publicationStatus: "STAGED",
          payload,
        };

  const artifactHash = sha(body);
  writeFileSync(join(dir, reportFile), JSON.stringify(body, null, 2) + "\n", "utf8");

  const prior = loadStagedManifest(id, cwd);
  const unwrapped = unwrapPayload(body);
  const isLoyalty =
    customerFacing === false ||
    isLoyaltyMeasurementArtifact(unwrapped, certification);

  // Loyalty/measurement-only: keep Core customer pointer intact.
  if (isLoyalty) {
    const manifest = {
      storeVersion: ADP_STAGED_REPORT_STORE_VERSION,
      propertyId: id,
      // Preserve Core customer-facing pointer when prior was Core (or repairable).
      latestStagedVersionId: prior?.latestStagedVersionId || null,
      periodId: prior?.periodId || periodId || body.periodId || null,
      runId: prior?.runId || null,
      workflowVersion: prior?.workflowVersion || null,
      reportFile: prior?.reportFile || null,
      artifactHash: prior?.artifactHash || null,
      certification: prior?.certification || null,
      source: prior?.source || null,
      note: prior?.note || null,
      stagedAt: prior?.stagedAt || new Date().toISOString(),
      publicationStatus: "STAGED",
      previousStagedVersionId: prior?.previousStagedVersionId || null,
      latestLoyaltyDraftVersionId: versionId,
      loyaltyDraftReportFile: reportFile,
      loyaltyDraftArtifactHash: artifactHash,
      loyaltyDraftCertification: certification,
      loyaltyDraftSource: source,
      loyaltyDraftNote: note,
      loyaltyDraftStagedAt: new Date().toISOString(),
      loyaltyDraftPeriodId: periodId || body.periodId || null,
      loyaltyDraftRunId: runId,
    };

    // If prior pointer itself was a loyalty draft (legacy corruption), repair to Core.
    if (
      !prior?.reportFile ||
      isLoyaltyMeasurementArtifact(
        unwrapPayload(readStagedDoc(id, prior.reportFile, cwd)),
        prior.certification
      )
    ) {
      const core = resolveCustomerFacingStagedReport(id, cwd);
      if (core) {
        manifest.latestStagedVersionId = core.versionId;
        manifest.reportFile = core.reportFile;
        manifest.artifactHash = core.artifactHash;
        manifest.certification = core.certification;
        const coreDoc = readStagedDoc(id, core.reportFile, cwd);
        manifest.periodId = coreDoc?.periodId || unwrapPayload(coreDoc)?.period?.periodId || manifest.periodId;
      } else {
        // No Core available yet — do not leave loyalty as customer latest.
        manifest.latestStagedVersionId = null;
        manifest.reportFile = null;
        manifest.artifactHash = null;
        manifest.certification = null;
      }
    }

    writeFileSync(
      join(dir, "manifest.json"),
      JSON.stringify(manifest, null, 2) + "\n",
      "utf8"
    );

    return {
      ok: true,
      propertyId: id,
      versionId,
      artifactHash,
      path: join(dir, reportFile),
      manifest,
      customerFacing: false,
      role: "loyalty_measurement_artifact",
    };
  }

  const manifest = {
    storeVersion: ADP_STAGED_REPORT_STORE_VERSION,
    propertyId: id,
    latestStagedVersionId: versionId,
    periodId: periodId || body.periodId || null,
    runId,
    workflowVersion,
    reportFile,
    artifactHash,
    certification,
    source,
    note,
    stagedAt: new Date().toISOString(),
    publicationStatus: "STAGED",
    previousStagedVersionId: null,
  };

  if (prior?.latestStagedVersionId) {
    manifest.previousStagedVersionId = prior.latestStagedVersionId;
  }
  // Preserve loyalty draft pointers if present.
  if (prior?.latestLoyaltyDraftVersionId) {
    manifest.latestLoyaltyDraftVersionId = prior.latestLoyaltyDraftVersionId;
    manifest.loyaltyDraftReportFile = prior.loyaltyDraftReportFile || null;
    manifest.loyaltyDraftArtifactHash = prior.loyaltyDraftArtifactHash || null;
    manifest.loyaltyDraftCertification = prior.loyaltyDraftCertification || null;
  }

  writeFileSync(
    join(dir, "manifest.json"),
    JSON.stringify(manifest, null, 2) + "\n",
    "utf8"
  );

  return {
    ok: true,
    propertyId: id,
    versionId,
    artifactHash,
    path: join(dir, reportFile),
    manifest,
    customerFacing: true,
    role: "core_owner_report",
  };
}

/**
 * Repair manifests where Loyalty drafts incorrectly became latestStagedVersionId.
 * Does not delete loyalty files; does not touch published/ or measurement checkpoints.
 */
export function repairStagedCustomerPointers(cwd = process.cwd()) {
  const ids = listStagedPropertyIds(cwd);
  const repaired = [];
  const skipped = [];
  for (const id of ids) {
    const manifest = loadStagedManifest(id, cwd);
    if (!manifest?.reportFile) {
      skipped.push({ propertyId: id, reason: "no_report_file" });
      continue;
    }
    const doc = readStagedDoc(id, manifest.reportFile, cwd);
    const payload = unwrapPayload(doc);
    if (!isLoyaltyMeasurementArtifact(payload, manifest.certification)) {
      skipped.push({ propertyId: id, reason: "already_core" });
      continue;
    }
    const core = resolveCustomerFacingStagedReport(id, cwd);
    if (!core) {
      skipped.push({ propertyId: id, reason: "no_core_candidate" });
      continue;
    }
    const next = {
      ...manifest,
      latestStagedVersionId: core.versionId,
      reportFile: core.reportFile,
      artifactHash: core.artifactHash,
      // Never keep STAGED_LOYALTY_DRAFT on the customer-facing pointer.
      certification: core.certification || {
        status: "STAGED_DRAFT",
        measurementFamily: "CORE_ADP",
        repairedFromLoyaltyPointer: true,
      },
      latestLoyaltyDraftVersionId:
        manifest.latestLoyaltyDraftVersionId || manifest.latestStagedVersionId,
      loyaltyDraftReportFile:
        manifest.loyaltyDraftReportFile || manifest.reportFile,
      loyaltyDraftArtifactHash:
        manifest.loyaltyDraftArtifactHash || manifest.artifactHash,
      loyaltyDraftCertification:
        manifest.loyaltyDraftCertification || manifest.certification,
      loyaltyDraftNote:
        manifest.loyaltyDraftNote ||
        "Repaired: loyalty draft demoted from customer-facing latest pointer",
      repairedAt: new Date().toISOString(),
      repairReason: "loyalty_draft_was_customer_latest",
    };
    writeFileSync(
      join(propertyDir(id, cwd), "manifest.json"),
      JSON.stringify(next, null, 2) + "\n",
      "utf8"
    );
    repaired.push({
      propertyId: id,
      from: manifest.latestStagedVersionId,
      to: core.versionId,
    });
  }
  return { ok: true, repaired, skipped };
}

export function loadStagedManifest(propertyId, cwd = process.cwd()) {
  const p = join(propertyDir(propertyId, cwd), "manifest.json");
  if (!existsSync(p)) return null;
  try {
    return JSON.parse(readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

/**
 * Load latest customer-facing staged owner payload (unwrapped).
 * Skips LOYALTY_BPP measurement artifacts even if still pointed by a legacy manifest.
 */
/**
 * Load an exact staged Core owner report by immutable versionId.
 * Used for version-pinned external share candidates — never falls back to latest/published.
 */
export function loadStagedOwnerPayloadByVersionId(
  propertyId,
  versionId,
  cwd = process.cwd()
) {
  const id = String(propertyId || "").trim();
  const vid = String(versionId || "").trim();
  if (!id || !vid) {
    return { ok: false, error: "property_or_version_required", propertyId: id, versionId: vid };
  }
  const reportFile = `report-${vid}.json`;
  const doc = readStagedDoc(id, reportFile, cwd);
  if (!doc) {
    return {
      ok: false,
      error: "staged_version_not_found",
      propertyId: id,
      versionId: vid,
      reportFile,
    };
  }
  const payload = unwrapPayload(doc);
  const cert = doc.certification || null;
  if (!isCoreOwnerReportPayload(payload, cert)) {
    return {
      ok: false,
      error: "staged_version_not_core_owner_report",
      propertyId: id,
      versionId: vid,
    };
  }
  const artifactHash = sha(doc);
  return {
    ok: true,
    source: "staged_snapshot_version_pinned",
    versionId: vid,
    artifactHash,
    periodId: payload?.period?.periodId || null,
    publicationStatus: "STAGED",
    stagedAt: doc.stagedAt || null,
    certification: cert,
    payload: {
      ...payload,
      _adpReportVersion: "STAGED",
      _adpStagedVersionId: vid,
      _adpArtifactHash: artifactHash,
      _adpVersionPin: true,
    },
    manifest: loadStagedManifest(id, cwd),
  };
}

export function loadStagedOwnerPayload(propertyId, cwd = process.cwd()) {
  const id = String(propertyId || "").trim();
  const resolved = resolveCustomerFacingStagedReport(id, cwd);
  if (!resolved) {
    // Explicit: do not return loyalty-only drafts as Core owner payloads.
    const manifest = loadStagedManifest(id, cwd);
    if (manifest?.reportFile) {
      const doc = readStagedDoc(id, manifest.reportFile, cwd);
      const payload = unwrapPayload(doc);
      if (isLoyaltyMeasurementArtifact(payload, manifest.certification)) {
        return {
          ok: false,
          error: "staged_loyalty_artifact_not_core_owner_report",
          propertyId: id,
          loyaltyDraftVersionId:
            manifest.latestLoyaltyDraftVersionId || manifest.latestStagedVersionId,
          message:
            "Latest staged file is a Loyalty measurement artifact without competitiveSet; no Core owner staged report found.",
        };
      }
    }
    return null;
  }

  const manifest = loadStagedManifest(id, cwd);
  return {
    ok: true,
    source: "staged_snapshot",
    versionId: resolved.versionId,
    artifactHash: resolved.artifactHash,
    periodId:
      unwrapPayload(readStagedDoc(id, resolved.reportFile, cwd))?.period?.periodId ||
      manifest?.periodId ||
      null,
    publicationStatus: "STAGED",
    stagedAt: resolved.stagedAt || manifest?.stagedAt || null,
    certification: resolved.certification || manifest?.certification || null,
    payload: {
      ...resolved.payload,
      _adpReportVersion: "STAGED",
      _adpStagedVersionId: resolved.versionId,
      _adpArtifactHash: resolved.artifactHash,
    },
    manifest,
    resolvedFromLoyaltyCorruption:
      manifest?.reportFile &&
      manifest.reportFile !== resolved.reportFile &&
      isLoyaltyMeasurementArtifact(
        unwrapPayload(readStagedDoc(id, manifest.reportFile, cwd)),
        manifest.certification
      ),
  };
}

export function listStagedPropertyIds(cwd = process.cwd()) {
  const root = rootDir(cwd);
  if (!existsSync(root)) return [];
  return readdirSync(root, { withFileTypes: true })
    .filter((d) => d.isDirectory() && d.name.startsWith("adp_"))
    .map((d) => d.name);
}
