/**
 * Permanent AVA audit runner — orchestrates certified V4 Full E2E + ADP↔AVA cert.
 * PowerShell and CLI call this; methodology is not duplicated.
 *
 * Paid research is never initiated unless AVA_AUDIT_PAID_RESEARCH_AUTHORIZED=1.
 * Default: reuse certified payload + run certification / PDF / archive path.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  AVA_AUDIT_EXECUTION_CONTRACT,
  AVA_AUDIT_RUN_STATUSES,
  AVA_AUDIT_CERTIFICATION_STATES,
  buildAuditExecutionIdentity,
  newAuditRunId,
  sha256File,
  sha256Json,
} from "./ava-audit-execution-contract-v1.js";
import {
  resolveAvaHotelIdentity,
  listAvaEligibleHotels,
  ensureIdentityMapScaffold,
} from "./ava-hotel-identity-resolver-v1.js";
import {
  saveImmutableAuditRun,
  loadAuditRun,
  listAuditRunsForProperty,
} from "./ava-audit-run-archive-v1.js";
import { synchronizeAuditRunToAirtable } from "./ava-audit-airtable-sync-v1.js";
import { compareAvaAuditRuns } from "./ava-audit-history-compare-v1.js";
import { runMandatoryAvaEvidenceStages } from "./ava-full-e2e-pipeline-v1.js";
import {
  resolveCustomerFacingAdpForAvaDelivery,
  certifyAvaAgainstCustomerFacingAdp,
  buildAvaAdpCrossReportIdentity,
  ADP_AVA_CERTIFICATION_STATUS,
} from "./adp-ava-permanent-cross-report-certification-v1.js";
import { assessCustomerQuality } from "./customer-quality-assurance-v1.js";
import { buildAiVisibilityAuditPdfHtmlV4 } from "./build-audit-pdf-html-v4.js";
import { generateAiVisibilityAuditPdfV1 } from "./generate-audit-pdf-v1.mjs";
import { stageAvaPdf } from "./ava-pdf-archive-v1.js";
import { saveAuditVersion, getLatestAuditVersion } from "./audit-store-v1.js";
import { buildAiVisibilityAuditPdfFilename } from "./ava-pdf-filename-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

function log(line, structured = null) {
  const row = {
    ts: new Date().toISOString(),
    msg: line,
    ...(structured || {}),
  };
  console.log(JSON.stringify(row));
}

function freezeInputManifest({ hotel, payload, adpResolution, mode, reproduceFrom }) {
  return {
    contract: AVA_AUDIT_EXECUTION_CONTRACT,
    frozenAt: new Date().toISOString(),
    mode,
    reproduceFrom: reproduceFrom || null,
    hotel: {
      propertyId: hotel.propertyId,
      airtableRecordId: hotel.airtableRecordId,
      name: hotel.name,
      payloadFile: hotel.payloadFile,
    },
    payloadContentSha256: sha256Json(payload),
    adp: {
      selectedSource: adpResolution?.selected?.source || null,
      selectedPeriodId: adpResolution?.selected?.periodId || null,
      selectedPackageId: adpResolution?.selected?.packageId || null,
      methodologyFingerprintSha256:
        adpResolution?.selected?.methodologyFingerprintSha256 || null,
      avaPinPeriodId: adpResolution?.avaPin?.periodId || null,
      alignment: adpResolution?.alignment || null,
    },
    paidResearchAuthorized:
      String(process.env.AVA_AUDIT_PAID_RESEARCH_AUTHORIZED || "").trim() === "1",
    codePaths: {
      pipeline: "ava-full-e2e-pipeline-v1.js",
      pdfTemplate: "build-audit-pdf-html-v4.js",
      crossReport: "adp-ava-permanent-cross-report-certification-v1.js",
    },
  };
}

/**
 * Chromium/Skia embeds /CreationDate and /ModDate at render time (first byte
 * diff ~offset 400). For NEW archives, stamp a deterministic date derived from
 * payload content so future semantic reproductions can approach byte parity.
 * Never rewrite an already-archived report.pdf in place from callers.
 */
function stampDeterministicPdfMetadata(pdfAbsPath, contentKey) {
  if (String(process.env.AVA_AUDIT_DETERMINISTIC_PDF_METADATA || "1").trim() === "0") {
    return { applied: false, reason: "disabled" };
  }
  const buf = fs.readFileSync(pdfAbsPath);
  const latin = buf.toString("latin1");
  // Fixed epoch — content identity is enforced by contentSha256, not wall clock.
  const stamp = "D:20260101000000+00'00'";
  let next = latin
    .replace(/\/CreationDate\s*\([^)]*\)/g, `/CreationDate (${stamp})`)
    .replace(/\/ModDate\s*\([^)]*\)/g, `/ModDate (${stamp})`);
  if (next === latin) {
    return { applied: false, reason: "no_date_fields", contentKey };
  }
  fs.writeFileSync(pdfAbsPath, Buffer.from(next, "latin1"));
  return { applied: true, stamp, contentKey };
}

async function generateV4Pdf(payload, outPath) {
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  await generateAiVisibilityAuditPdfV1({
    payload,
    outPath,
    pagesDir: null,
    buildHtml: buildAiVisibilityAuditPdfHtmlV4,
    minSections: 4,
  });
  stampDeterministicPdfMetadata(outPath, sha256Json(payload));
  return outPath;
}

/**
 * Execute one hotel audit per AVA_AUDIT_EXECUTION_CONTRACT_V1.
 */
export async function runPermanentAvaAudit(hotelRef, options = {}) {
  const env = options.env || process.env;
  ensureIdentityMapScaffold();

  const mode = options.mode || "full";
  // modes: full | dry-run | certify-only | reproduce
  const startedAt = new Date().toISOString();
  log("ava_audit_start", { hotelRef, mode });

  if (mode === "reproduce") {
    return reproduceAuditRun(options.auditRunId, { env, ...options });
  }

  const hotel = resolveAvaHotelIdentity(hotelRef);
  if (!hotel.ok) {
    return {
      ok: false,
      exitCode: 2,
      error: hotel.error,
      message: hotel.message || null,
      hotelRef,
    };
  }

  const payload = JSON.parse(fs.readFileSync(hotel.payloadPath, "utf8"));
  const adpResolution = resolveCustomerFacingAdpForAvaDelivery(hotel.propertyId);
  const inputManifest = freezeInputManifest({
    hotel,
    payload,
    adpResolution,
    mode,
  });

  const priorRuns = listAuditRunsForProperty(hotel.propertyId, env);
  const previousVersionRef = priorRuns[0]?.auditRunId || null;
  const auditRunId = options.auditRunId || newAuditRunId(hotel.propertyId);

  let identity = buildAuditExecutionIdentity({
    propertyId: hotel.propertyId,
    hotelAirtableRecordId: hotel.airtableRecordId,
    hotelName: hotel.name,
    auditRunId,
    reportingPeriodLabel: adpResolution.avaPin?.periodId || null,
    adpPackageId: adpResolution.selected?.packageId || null,
    adpPeriodId: adpResolution.avaPin?.periodId || adpResolution.selected?.periodId,
    adpMethodologyVersion:
      adpResolution.selected?.metrics?.aiConsideration?.methodologyId ||
      adpResolution.avaPin?.methodologyId ||
      null,
    adpCertificationFingerprint:
      adpResolution.selected?.methodologyFingerprintSha256 || null,
    sourceDataSnapshotRefs: [
      hotel.payloadPath,
      adpResolution.selected?.snapshotPath,
    ].filter(Boolean),
    previousVersionRef,
    executionStatus: AVA_AUDIT_RUN_STATUSES.RUNNING,
    certificationStatus: AVA_AUDIT_CERTIFICATION_STATES.DRAFT,
  });

  if (mode === "dry-run") {
    const cqa = assessCustomerQuality(payload, {
      buildHtml: buildAiVisibilityAuditPdfHtmlV4,
    });
    const cross = await certifyAvaAgainstCustomerFacingAdp(payload, {
      avaPdfPath: null,
    });
    identity = {
      ...identity,
      executionStatus: AVA_AUDIT_RUN_STATUSES.DRY_RUN,
      certificationStatus: !cross.ok
        ? AVA_AUDIT_CERTIFICATION_STATES.FAILED_CERTIFICATION
        : cross.status === ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_HISTORICAL_REFERENCE
          ? AVA_AUDIT_CERTIFICATION_STATES.HISTORICAL_REFERENCE
          : AVA_AUDIT_CERTIFICATION_STATES.AUDIT_CERTIFIED,
      crossReportStatus: cross.status,
      jointPublicationAllowed:
        cross.status === ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH,
    };
    const report = {
      ok: true,
      dryRun: true,
      exitCode: cross.ok ? 0 : 3,
      auditRunId,
      propertyId: hotel.propertyId,
      hotelName: hotel.name,
      identity,
      inputManifest,
      customerQuality: {
        certified: cqa.customerQualityCertified === true,
        defectCount: cqa.defectCount,
      },
      crossReport: {
        status: cross.status,
        ok: cross.ok,
        jointPublicationAllowed:
          cross.status === ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH,
        defectCount: (cross.defects || []).length,
      },
      note: "Dry-run: no PDF write, no archive, no Airtable, no Admin registration.",
      startedAt,
      finishedAt: new Date().toISOString(),
    };
    log("ava_audit_dry_run_complete", {
      propertyId: hotel.propertyId,
      crossStatus: cross.status,
    });
    return report;
  }

  // certify-only / full: no paid research unless authorized
  const paidOk =
    String(env.AVA_AUDIT_PAID_RESEARCH_AUTHORIZED || "").trim() === "1";
  if (!paidOk) {
    log("ava_audit_reuse_certified_payload", {
      propertyId: hotel.propertyId,
      reason: "paid_research_not_authorized",
    });
  }

  let working = structuredClone(payload);
  let pipelineResult = null;
  let pdfPath = null;
  let pdfSha = null;
  const stagingDir = path.join(ROOT, "tmp", "ava-permanent-audit", hotel.propertyId, auditRunId);
  fs.mkdirSync(stagingDir, { recursive: true });

  try {
    if (mode !== "certify-only") {
      // Full E2E evidence + CQA (dryRun research; never paid without auth)
      pipelineResult = await runMandatoryAvaEvidenceStages(working, {
        dryRun: true,
        persistCache: false,
        skipPaidResearch: !paidOk,
        generatePdfFn: async ({ payload: p }) => {
          const fname = buildAiVisibilityAuditPdfFilename({
            hotelName: p.property?.name,
            reportMonthLabel: p.reporting?.reportMonthLabel,
            date: p.reporting?.generatedAt,
          });
          const out = path.join(stagingDir, fname);
          await generateV4Pdf(p, out);
          pdfPath = out;
          return { buffer: fs.readFileSync(out), path: out };
        },
        forcePdfRegenerate: true,
        buildHtml: buildAiVisibilityAuditPdfHtmlV4,
      });
      working = pipelineResult.result?.payload || working;
    }

    if (!pdfPath || !fs.existsSync(pdfPath)) {
      const fname = buildAiVisibilityAuditPdfFilename({
        hotelName: working.property?.name,
        reportMonthLabel: working.reporting?.reportMonthLabel,
        date: working.reporting?.generatedAt || new Date().toISOString(),
      });
      pdfPath = path.join(stagingDir, fname);
      await generateV4Pdf(working, pdfPath);
    }
    pdfSha = sha256File(pdfPath);

    const cqa = assessCustomerQuality(working, {
      buildHtml: buildAiVisibilityAuditPdfHtmlV4,
      pdfBuffer: fs.readFileSync(pdfPath),
    });

    const cross = await certifyAvaAgainstCustomerFacingAdp(working, {
      avaPdfPath: pdfPath,
    });
    const crossIdentity = buildAvaAdpCrossReportIdentity(working, adpResolution, cross);
    working = {
      ...working,
      crossReportIdentity: crossIdentity,
      adpCrossReportCertification: cross,
    };

    const pipelinePass =
      mode === "certify-only"
        ? true
        : pipelineResult?.result?.eligibleForAuditValidated === true ||
          pipelineResult?.customerQualityAssurance?.customerQualityCertified === true;
    const certified =
      cross.ok === true &&
      cqa.customerQualityCertified === true &&
      (mode === "certify-only" || pipelinePass !== false);

    identity = {
      ...identity,
      contentSha256: sha256Json(working),
      pdfSha256: pdfSha,
      executionStatus: certified
        ? AVA_AUDIT_RUN_STATUSES.CERTIFIED
        : AVA_AUDIT_RUN_STATUSES.FAILED_CERTIFICATION,
      certificationStatus: certified
        ? AVA_AUDIT_CERTIFICATION_STATES.AUDIT_CERTIFIED
        : AVA_AUDIT_CERTIFICATION_STATES.FAILED_CERTIFICATION,
      crossReportStatus: cross.status,
      jointPublicationAllowed:
        cross.status === ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH && certified,
    };

    if (cross.status === ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_HISTORICAL_REFERENCE) {
      identity.certificationStatus = AVA_AUDIT_CERTIFICATION_STATES.HISTORICAL_REFERENCE;
      identity.jointPublicationAllowed = false;
    }

    const summary = {
      findingCount: (working.priorityFindings || working.findings || []).length,
      actionCount: (working.customerActionRegister?.actions || working.actions || []).length,
      kpiCount: (working.kpis || []).length,
      crossReportStatus: cross.status,
      customerQualityCertified: cqa.customerQualityCertified === true,
      auditCertified: certified,
      founderApproved: false,
      customerPublished: false,
    };

    const archived = saveImmutableAuditRun({
      identity,
      inputManifest,
      payload: working,
      certification: {
        cross,
        customerQuality: cqa,
        pipeline: pipelineResult
          ? {
              eligibleForAuditValidated:
                pipelineResult.result?.eligibleForAuditValidated === true,
              nextAllowedStatus: pipelineResult.nextAllowedStatus,
            }
          : null,
      },
      summary,
      pdfAbsPath: pdfPath,
      previousVersionRef,
      env,
    });

    // Register in AI Admin store as a new draft version (never publishes)
    let adminRegistration = { ok: false, skipped: true };
    try {
      const versionId = auditRunId;
      const staged = stageAvaPdf({
        propertyId: hotel.propertyId,
        generatedAbsPath: pdfPath,
        versionId,
        filename: path.basename(pdfPath),
        env,
      });
      saveAuditVersion(hotel.propertyId, versionId, working, {
        status: certified ? "PDF_STAGED" : "VALIDATION_REQUIRED",
        pdfRelativePath: staged.pdfRelativePath,
        validation: { cross, customerQualityCertified: cqa.customerQualityCertified },
        metaStatus: certified ? "PDF_STAGED" : "VALIDATION_REQUIRED",
        forceStatus: true,
      });
      adminRegistration = {
        ok: true,
        versionId,
        pdfRelativePath: staged.pdfRelativePath,
        pdfSha256: staged.sha256,
        publishedUntouched: staged.publishedUntouched === true,
      };
    } catch (err) {
      adminRegistration = {
        ok: false,
        error: err.message || String(err),
        note: "Local archive retained; Admin registration failed",
      };
    }

    const sync = await synchronizeAuditRunToAirtable(auditRunId, { env });
    if (sync.airtableIndexed === true && sync.ok) {
      identity.executionStatus = AVA_AUDIT_RUN_STATUSES.ARCHIVE_COMPLETE;
    } else {
      identity.executionStatus = AVA_AUDIT_RUN_STATUSES.ARCHIVE_PENDING_AIRTABLE;
      // rewrite identity in archive
      fs.writeFileSync(
        path.join(archived.dir, "execution-identity.json"),
        `${JSON.stringify(identity, null, 2)}\n`
      );
    }

    const report = {
      ok: certified,
      exitCode: certified ? 0 : 3,
      auditRunId,
      propertyId: hotel.propertyId,
      hotelName: hotel.name,
      airtableRecordId: hotel.airtableRecordId,
      identity,
      inputManifest,
      outputManifest: archived.outputManifest,
      archiveDir: archived.dir,
      pdfSha256: pdfSha,
      contentSha256: identity.contentSha256,
      customerQuality: {
        certified: cqa.customerQualityCertified === true,
        defectCount: cqa.defectCount,
        criticalCount: cqa.criticalCount,
      },
      crossReport: {
        status: cross.status,
        ok: cross.ok,
        jointPublicationAllowed: identity.jointPublicationAllowed,
        defectCount: (cross.defects || []).length,
        defects: (cross.defects || []).slice(0, 12),
      },
      adminRegistration,
      airtableSync: {
        status: sync.status,
        airtableIndexed: sync.airtableIndexed === true,
        archiveComplete: sync.archiveComplete === true,
        reason: sync.reason || null,
        mirrorPath: sync.mirrorPath || null,
      },
      lifecycle: {
        AUDIT_CERTIFIED: certified,
        ARCHIVE_COMPLETE: sync.archiveComplete === true,
        FOUNDER_APPROVED: false,
        CUSTOMER_PUBLISHED: false,
      },
      previousVersionRef,
      startedAt,
      finishedAt: new Date().toISOString(),
    };

    const reportPath = path.join(archived.dir, "execution-report.json");
    fs.writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`);
    log("ava_audit_complete", {
      propertyId: hotel.propertyId,
      auditRunId,
      certified,
      crossStatus: cross.status,
    });
    return report;
  } catch (err) {
    const fail = {
      ok: false,
      exitCode: 1,
      error: err.code || "AVA_AUDIT_EXECUTION_FAILED",
      message: err.message || String(err),
      auditRunId,
      propertyId: hotel.propertyId,
      identity: {
        ...identity,
        executionStatus: AVA_AUDIT_RUN_STATUSES.FAILED_CERTIFICATION,
        certificationStatus: AVA_AUDIT_CERTIFICATION_STATES.FAILED_CERTIFICATION,
      },
      startedAt,
      finishedAt: new Date().toISOString(),
    };
    log("ava_audit_failed", { propertyId: hotel.propertyId, error: fail.message });
    try {
      fs.writeFileSync(
        path.join(stagingDir, "execution-report-failed.json"),
        `${JSON.stringify(fail, null, 2)}\n`
      );
    } catch {
      /* ignore */
    }
    return fail;
  }
}

/**
 * Exact reproduction from frozen input manifest + payload.
 */
export async function reproduceAuditRun(auditRunId, options = {}) {
  const env = options.env || process.env;
  const loaded = loadAuditRun(auditRunId, env);
  if (!loaded.ok) {
    return { ok: false, exitCode: 2, error: loaded.error, auditRunId };
  }

  const frozenPayload = loaded.payload;
  const originalContentSha = loaded.identity?.contentSha256 || sha256Json(frozenPayload);
  const originalPdfSha = loaded.identity?.pdfSha256 || null;

  const newId = newAuditRunId(loaded.propertyId);
  const stagingDir = path.join(
    ROOT,
    "tmp",
    "ava-permanent-audit",
    loaded.propertyId,
    `reproduce-${newId}`
  );
  fs.mkdirSync(stagingDir, { recursive: true });
  const fname = `reproduce-${path.basename(loaded.pdfPath || "report.pdf")}`;
  const outPdf = path.join(stagingDir, fname);

  await generateV4Pdf(frozenPayload, outPdf);
  const newPdfSha = sha256File(outPdf);
  const newContentSha = sha256Json(frozenPayload);
  const contentParity = newContentSha === originalContentSha;
  const pdfByteParity = originalPdfSha ? newPdfSha === originalPdfSha : null;

  const reproduction = {
    ok: contentParity,
    exitCode: contentParity ? 0 : 4,
    mode: "reproduce",
    sourceAuditRunId: auditRunId,
    reproductionAttemptId: newId,
    contentSha256: { original: originalContentSha, reproduced: newContentSha, match: contentParity },
    pdfSha256: {
      original: originalPdfSha,
      reproduced: newPdfSha,
      match: pdfByteParity,
    },
    byteIdenticalPdf: pdfByteParity === true,
    semanticContentParity: contentParity,
    note:
      pdfByteParity === true
        ? "Byte-identical PDF reproduced."
        : pdfByteParity === false
          ? "PDF bytes differ (timestamps/fonts/renderer nondeterminism). Semantic content parity required; do not claim byte parity."
          : "No original PDF hash available for byte comparison.",
    reproducedPdfPath: outPdf,
  };
  log("ava_audit_reproduce_complete", {
    sourceAuditRunId: auditRunId,
    contentMatch: contentParity,
    pdfMatch: pdfByteParity,
  });
  return reproduction;
}

export async function runPermanentAvaAuditBatch(hotelRefs, options = {}) {
  const results = [];
  for (const ref of hotelRefs) {
    const r = await runPermanentAvaAudit(ref, options);
    results.push(r);
  }
  const failed = results.filter((r) => !r.ok);
  return {
    ok: failed.length === 0,
    exitCode: failed.length === 0 ? 0 : 3,
    count: results.length,
    failedCount: failed.length,
    results,
  };
}

export async function runPermanentAvaAuditAllEligible(options = {}) {
  const hotels = listAvaEligibleHotels();
  const refs = hotels.map((h) => h.propertyId);
  log("ava_audit_portfolio_start", { count: refs.length, mode: options.mode || "full" });
  return runPermanentAvaAuditBatch(refs, options);
}

export { compareAvaAuditRuns, listAvaEligibleHotels, resolveAvaHotelIdentity };
