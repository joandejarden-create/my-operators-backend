/**
 * ADP Executive Read V3 — immutable edition lineage + controlled activation helpers.
 * Doctrine: METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 * Never silently rewrite issued writeup/ux; additive V3 promotion only.
 */

import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync, readdirSync, copyFileSync } from "fs";
import { join } from "path";
import {
  ADP_EXECUTIVE_READ_COMPOSITION_V3,
  COMPOSITION_V3_STATUS,
} from "../governance/adp-executive-read-composition-v3.js";
import { composeExecutiveReadV3 } from "./compose-executive-read-v3.js";
import { resolveExecutiveReadCompositionWritePolicyV3 } from "./historical-immutability-v3.js";
import {
  loadPublishedReport,
  loadPublishedManifest,
  listPublishedPropertyIds,
} from "../published-snapshot.js";
import { enrichPayloadOptionalMetrics } from "../published-read-service.js";
import { loadPropertyProfile } from "../data-model.js";

export const ADP_EXECUTIVE_V3_IMMUTABLE_EDITION_LINEAGE =
  "ADP_EXECUTIVE_V3_IMMUTABLE_EDITION_LINEAGE";
export const ADP_EXECUTIVE_REAL_BROWSER_LAYOUT_PASS =
  "ADP_EXECUTIVE_REAL_BROWSER_LAYOUT_PASS";
export const ADP_EXECUTIVE_PRODUCTION_PDF_PARITY_PASS =
  "ADP_EXECUTIVE_PRODUCTION_PDF_PARITY_PASS";

const EDITIONS_DIR = join(
  process.cwd(),
  "data/ai-demand-positioning/editions/executive-read-v3"
);
const RECOVERY_DIR = join(
  process.cwd(),
  "reports/ai-demand-positioning/v3-activation-recovery"
);

function sha256(text) {
  return createHash("sha256").update(String(text || "")).digest("hex");
}

function hashWriteupUx(er) {
  return sha256(
    JSON.stringify({
      writeup: er?.writeup || null,
      ux: er?.ux || null,
    })
  );
}

export function isCompositionV3Activated() {
  return COMPOSITION_V3_STATUS.activated === true;
}

/**
 * Classify property distribution posture for edition policy.
 * Active share token ⇒ ISSUED (do not destroy writeup); still may receive additive V3 edition.
 */
export function classifyPropertyDistributionStatus(propertyId, shareRegistry) {
  const tokens = shareRegistry?.tokens || {};
  const active = Object.values(tokens).filter(
    (t) => t.propertyId === propertyId && t.status === "ACTIVE"
  );
  const productionDist = active.some((t) =>
    String(t.label || "").startsWith("production-distribution:")
  );
  if (productionDist || active.length) {
    return {
      status: "ISSUED_SHARE_ACTIVE",
      activeShareCount: active.length,
      productionDistributionLabeled: productionDist,
      // Additive V3 edition allowed; in-place writeup mutation prohibited
      allowAdditiveV3Edition: true,
      allowWriteupMutation: false,
    };
  }
  return {
    status: "NON_DISTRIBUTED",
    activeShareCount: 0,
    productionDistributionLabeled: false,
    allowAdditiveV3Edition: true,
    allowWriteupMutation: false, // still never mutate writeup — additive only
  };
}

export function freezePreActivationRecoveryPoint(meta = {}) {
  mkdirSync(RECOVERY_DIR, { recursive: true });
  const publishedIds = listPublishedPropertyIds();
  const propertyHashes = {};
  for (const propertyId of publishedIds) {
    const report = loadPublishedReport(propertyId);
    const manifest = loadPublishedManifest(propertyId);
    propertyHashes[propertyId] = {
      periodId: manifest?.latestPeriodId || report?.periodId || null,
      reportEdition: manifest?.reportEdition || report?.reportEdition || null,
      writeupUxHash: hashWriteupUx(report?.executiveRead || report?.payload?.executiveRead),
      reportFileSha256: report
        ? sha256(JSON.stringify(report))
        : null,
      compositionVersion:
        report?.executiveRead?.compositionVersion ||
        report?.payload?.executiveRead?.compositionVersion ||
        null,
      compositionHash:
        report?.executiveRead?.compositionHash ||
        report?.payload?.executiveRead?.compositionHash ||
        null,
    };
  }
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const recovery = {
    gate: "PRE_ACTIVATION_RECOVERY_POINT",
    frozenAt: new Date().toISOString(),
    git: meta.git || null,
    compositionV3StatusBefore: { ...COMPOSITION_V3_STATUS },
    publishedUniverseCount: publishedIds.length,
    publishedPropertyIds: publishedIds,
    propertyHashes,
    shareState: meta.shareState || null,
    note: "Restore by reloading frozen report hashes / disabling COMPOSITION_V3_STATUS.activated; leave new edition artifacts intact for forensics.",
  };
  const path = join(RECOVERY_DIR, `pre-activation-recovery-${stamp}.json`);
  const latest = join(RECOVERY_DIR, "pre-activation-recovery-latest.json");
  writeFileSync(path, JSON.stringify(recovery, null, 2));
  writeFileSync(latest, JSON.stringify(recovery, null, 2));
  return { ...recovery, recoveryPath: path, latestPath: latest };
}

/**
 * Promote V3 onto executiveRead additively — never replaces writeup/ux.
 */
export function promoteExecutiveReadToV3Edition(executiveRead, v3, lineage) {
  const er = executiveRead && typeof executiveRead === "object" ? { ...executiveRead } : {};
  const beforeHash = hashWriteupUx(er);
  const priorEditionId =
    er.editionId ||
    er.compositionHash ||
    `prior_${beforeHash.slice(0, 12)}`;

  const editionId =
    lineage?.newEditionId ||
    `adp_er_v3_${(v3.propertyId || "prop").replace(/^adp_/, "")}_${Date.now().toString(36)}_${v3.compositionHash?.slice(0, 6) || "x"}`;

  er.compositionVersion = ADP_EXECUTIVE_READ_COMPOSITION_V3;
  er.compositionContract = ADP_EXECUTIVE_READ_COMPOSITION_V3;
  er.sections = v3.sections;
  er.numericAnchors = v3.numericAnchors;
  er.primaryIssueId = v3.primaryIssueId;
  er.insightArchetype = v3.insightArchetype;
  er.evidenceTrace = v3.evidenceTrace;
  er.qualityGates = v3.qualityGates;
  er.sourceSnapshotHash = v3.sourceSnapshotHash;
  er.compositionHash = v3.compositionHash;
  er.editionId = editionId;
  er.compositionV3 = {
    ...v3,
    activated: true,
    customerFacingDefault: true,
    editionId,
  };
  er.editionLineage = {
    gate: ADP_EXECUTIVE_V3_IMMUTABLE_EDITION_LINEAGE,
    priorEditionId,
    newEditionId: editionId,
    compositionVersion: ADP_EXECUTIVE_READ_COMPOSITION_V3,
    sourcePeriodId: v3.periodId || lineage?.sourcePeriodId || null,
    sourceSnapshotHash: v3.sourceSnapshotHash,
    compositionHash: v3.compositionHash,
    generatedAt: v3.generatedAt || new Date().toISOString(),
    writeupPreserved: true,
    writeupUxHashBefore: beforeHash,
    writeupUxHashAfter: hashWriteupUx(er),
  };

  if (er.editionLineage.writeupUxHashBefore !== er.editionLineage.writeupUxHashAfter) {
    throw new Error("WRITEUP_UX_MUTATION_DETECTED");
  }

  return { executiveRead: er, editionId, priorEditionId, writeupUxHash: beforeHash };
}

/**
 * Generate + optionally apply additive V3 edition for one published property.
 */
export function generateImmutableV3EditionForProperty(propertyId, opts = {}) {
  const apply = opts.apply === true;
  const manifest = loadPublishedManifest(propertyId);
  const payload = loadPublishedReport(propertyId);
  if (!payload) {
    return { ok: false, propertyId, reason: "MISSING_PUBLISHED_REPORT" };
  }
  const enriched = enrichPayloadOptionalMetrics(propertyId, payload) || payload;
  const profile = loadPropertyProfile(propertyId);
  const erBefore = enriched?.executiveRead;
  const writeupHashBefore = hashWriteupUx(erBefore);

  const classification = classifyPropertyDistributionStatus(
    propertyId,
    opts.shareRegistry || { tokens: {} }
  );

  const v3 = composeExecutiveReadV3(enriched, {
    propertyId,
    market: profile?.market,
    distributionStatus: "PUBLISHED_CURRENT",
    zeroCodePath: true,
    allowNewEdition: true,
  });
  if (!v3?.ok) {
    return {
      ok: false,
      propertyId,
      reason: v3?.reason || "V3_COMPOSE_FAILED",
      detail: v3?.detail,
      classification,
    };
  }

  mkdirSync(EDITIONS_DIR, { recursive: true });
  const promoted = promoteExecutiveReadToV3Edition(erBefore, v3, {
    sourcePeriodId: manifest?.latestPeriodId || enriched.period?.periodId,
  });

  const editionRecord = {
    gate: ADP_EXECUTIVE_V3_IMMUTABLE_EDITION_LINEAGE,
    propertyId,
    periodId: manifest?.latestPeriodId || enriched.period?.periodId,
    priorEditionId: promoted.priorEditionId,
    newEditionId: promoted.editionId,
    compositionVersion: ADP_EXECUTIVE_READ_COMPOSITION_V3,
    sourcePeriodId: manifest?.latestPeriodId || enriched.period?.periodId,
    sourceSnapshotHash: v3.sourceSnapshotHash,
    compositionHash: v3.compositionHash,
    primaryIssueId: v3.primaryIssueId,
    generatedAt: new Date().toISOString(),
    classification,
    writeupUxHash: promoted.writeupUxHash,
    sections: v3.sections,
    numericAnchors: v3.numericAnchors,
    appliedToPublished: false,
  };

  const editionPath = join(EDITIONS_DIR, `${promoted.editionId}.json`);
  writeFileSync(editionPath, JSON.stringify(editionRecord, null, 2));

  if (!apply) {
    return {
      ok: true,
      dryRun: true,
      propertyId,
      edition: editionRecord,
      editionPath,
      writeupPreserved: true,
    };
  }

  const reportPath = join(
    process.cwd(),
    "data/ai-demand-positioning/published",
    propertyId,
    manifest.reportFile || `report-${manifest.latestPeriodId}.json`
  );
  if (!existsSync(reportPath)) {
    return { ok: false, propertyId, reason: "REPORT_FILE_MISSING", reportPath };
  }

  const backupDir = join(RECOVERY_DIR, "published-backups");
  mkdirSync(backupDir, { recursive: true });
  const backupPath = join(
    backupDir,
    `${propertyId}__${manifest.latestPeriodId}__pre-v3.json`
  );
  if (!existsSync(backupPath)) {
    copyFileSync(reportPath, backupPath);
  }

  const reportObj = JSON.parse(readFileSync(reportPath, "utf8"));
  const targetEr = reportObj.payload?.executiveRead;
  if (!targetEr) {
    return { ok: false, propertyId, reason: "NO_EXECUTIVE_READ_ON_REPORT" };
  }
  const applied = promoteExecutiveReadToV3Edition(targetEr, v3, {
    newEditionId: promoted.editionId,
    sourcePeriodId: manifest.latestPeriodId,
  });
  reportObj.payload.executiveRead = applied.executiveRead;
  reportObj.executiveReadEditionId = applied.editionId;
  reportObj.compositionVersion = ADP_EXECUTIVE_READ_COMPOSITION_V3;

  writeFileSync(reportPath, JSON.stringify(reportObj, null, 2));

  const reloaded = JSON.parse(readFileSync(reportPath, "utf8"));
  const erAfter = reloaded.payload?.executiveRead;
  const writeupHashAfter = hashWriteupUx(erAfter);
  if (writeupHashAfter !== writeupHashBefore) {
    copyFileSync(backupPath, reportPath);
    return {
      ok: false,
      propertyId,
      reason: "WRITEUP_MUTATION_ABORT_RESTORED",
      writeupHashBefore,
      writeupHashAfter,
    };
  }

  editionRecord.appliedToPublished = true;
  editionRecord.backupPath = backupPath;
  writeFileSync(editionPath, JSON.stringify(editionRecord, null, 2));

  const manifestPath = join(
    process.cwd(),
    "data/ai-demand-positioning/published",
    propertyId,
    "manifest.json"
  );
  if (existsSync(manifestPath)) {
    const m = JSON.parse(readFileSync(manifestPath, "utf8"));
    m.executiveReadCompositionVersion = ADP_EXECUTIVE_READ_COMPOSITION_V3;
    m.executiveReadEditionId = applied.editionId;
    m.executiveReadCompositionHash = v3.compositionHash;
    m.priorExecutiveReadEditionId = promoted.priorEditionId;
    writeFileSync(manifestPath, JSON.stringify(m, null, 2));
  }

  return {
    ok: true,
    dryRun: false,
    propertyId,
    edition: editionRecord,
    editionPath,
    backupPath,
    writeupPreserved: true,
    primaryIssueId: v3.primaryIssueId,
  };
}

export function listEditionArtifacts() {
  if (!existsSync(EDITIONS_DIR)) return [];
  return readdirSync(EDITIONS_DIR)
    .filter((f) => f.endsWith(".json"))
    .map((f) => join(EDITIONS_DIR, f));
}

export {
  EDITIONS_DIR,
  RECOVERY_DIR,
  hashWriteupUx,
  sha256,
};
