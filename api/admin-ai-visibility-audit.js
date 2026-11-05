/**
 * AI Admin — AI Visibility Audit & Action Plan management (staged).
 * Reuses ADP monthly-review admin auth. No customer publication from this API.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadAdpBaselineForAudit, listAdpPropertyIdsWithStagedReports } from "../lib/ai-visibility-audit-action-plan/adp-baseline-loader-v1.js";
import {
  listStoredAudits,
  getAuditMeta,
  upsertAuditMeta,
  getLatestAuditVersion,
  loadAuditVersion,
  saveAuditVersion,
  recordRecommendationDecision,
  recordApprovalEvent,
  listAuditEvents,
  getAvaStorageStatus,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { AVA_STANDARD_MODULES } from "../lib/ai-visibility-audit-action-plan/modules-v1.js";
import {
  AVA_LIFECYCLE_STATUSES,
  canTransitionAvaLifecycle,
  isCustomerFacingPublicationAllowed,
  isAuditValidated,
  isFounderApprovedForCustomer,
} from "../lib/ai-visibility-audit-action-plan/lifecycle-v1.js";
import { assertAuditPdfPayload } from "../lib/ai-visibility-audit-action-plan/audit-pdf-contract-v1.js";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import {
  stageAvaPdf,
  resolveAvaPdfFileForDelivery,
  assertSafeAvaPropertyId,
  assertSafeAvaVersionId,
} from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import {
  AVA_PDF_CHANNEL,
  describeAvaPdfChannels,
  resolveAvaPdfByChannel,
} from "../lib/ai-visibility-audit-action-plan/ava-pdf-channels-v1.js";
import {
  isAvaDurableStorageEnabled,
  ensureAvaDurableStorage,
} from "../lib/ai-visibility-audit-action-plan/resolve-ava-storage-root-v1.js";
import {
  buildAiVisibilityAuditPdfFilename,
  buildAvaPdfFilenameFromVersion,
  contentDispositionForAvaPdf,
} from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";
import {
  certifyAvaEvidenceAndHyperlinks,
  assertEvidenceGateAllowsAuditValidated,
  loadManualVerificationsFromMeta,
  summarizeCertificationForAdmin,
} from "../lib/ai-visibility-audit-action-plan/evidence-hyperlink-certification-v1.js";
import {
  runMandatoryAvaEvidenceStages,
} from "../lib/ai-visibility-audit-action-plan/ava-full-e2e-pipeline-v1.js";
import {
  summarizeResolutionForAdmin,
} from "../lib/ai-visibility-audit-action-plan/autonomous-evidence-resolution-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

/** Staged payload map — hotel-specific JSON only (no hardcoded report copy). */
const AVA_PAYLOAD_BY_PROPERTY = Object.freeze({
  adp_spice_island_beach_resort:
    "lib/ai-visibility-audit-action-plan/payloads/spice-island-beach-resort-v1.json",
  adp_wyndham_san_diego_bayside:
    "lib/ai-visibility-audit-action-plan/payloads/wyndham-san-diego-bayside-v1.json",
  adp_waterstone_boca_raton:
    "lib/ai-visibility-audit-action-plan/payloads/waterstone-boca-raton-v1.json",
  adp_bethesda_marriott:
    "lib/ai-visibility-audit-action-plan/payloads/bethesda-marriott-v1.json",
  adp_castillo_hotel_son_vida:
    "lib/ai-visibility-audit-action-plan/payloads/castillo-hotel-son-vida-v1.json",
  adp_sheraton_mallorca_arabella_golf:
    "lib/ai-visibility-audit-action-plan/payloads/sheraton-mallorca-arabella-golf-v1.json",
  adp_ac_hotel_a_coruna:
    "lib/ai-visibility-audit-action-plan/payloads/ac-hotel-a-coruna-v1.json",
  adp_grand_fiesta_americana_chapultepec:
    "lib/ai-visibility-audit-action-plan/payloads/grand-fiesta-americana-chapultepec-v1.json",
  adp_fiesta_americana_condesa_cancun_ai:
    "lib/ai-visibility-audit-action-plan/payloads/fiesta-americana-condesa-cancun-ai-v1.json",
  adp_sheraton_guadalajara_expo:
    "lib/ai-visibility-audit-action-plan/payloads/sheraton-guadalajara-expo-v1.json",
  adp_voco_cancun_zona_hotelera:
    "lib/ai-visibility-audit-action-plan/payloads/voco-cancun-zona-hotelera-v1.json",
  adp_wyndham_alltra_punta_cana:
    "lib/ai-visibility-audit-action-plan/payloads/wyndham-alltra-punta-cana-v1.json",
  adp_jw_marriott_santo_domingo:
    "lib/ai-visibility-audit-action-plan/payloads/jw-marriott-santo-domingo-v1.json",
  adp_umusic_hotel_madrid:
    "lib/ai-visibility-audit-action-plan/payloads/umusic-hotel-madrid-v1.json",
  adp_st_regis_cap_cana:
    "lib/ai-visibility-audit-action-plan/payloads/st-regis-cap-cana-v1.json",
  adp_charlotte_marriott_city_center:
    "lib/ai-visibility-audit-action-plan/payloads/charlotte-marriott-city-center-v1.json",
  adp_tampa_airport_marriott:
    "lib/ai-visibility-audit-action-plan/payloads/tampa-airport-marriott-v1.json",
  adp_falls_church_marriott_fairview_park:
    "lib/ai-visibility-audit-action-plan/payloads/falls-church-marriott-fairview-park-v1.json",
  adp_renaissance_arlington_capital_view:
    "lib/ai-visibility-audit-action-plan/payloads/renaissance-arlington-capital-view-v1.json",
  adp_st_regis_mexico_city:
    "lib/ai-visibility-audit-action-plan/payloads/st-regis-mexico-city-v1.json",
  adp_w_rome:
    "lib/ai-visibility-audit-action-plan/payloads/w-rome-v1.json",
  adp_mayflower_hotel_washington_dc:
    "lib/ai-visibility-audit-action-plan/payloads/mayflower-hotel-washington-dc-v1.json",
  adp_hotel_phillips_kansas_city:
    "lib/ai-visibility-audit-action-plan/payloads/hotel-phillips-kansas-city-v1.json",
  adp_cambridge_beaches_bermuda:
    "lib/ai-visibility-audit-action-plan/payloads/cambridge-beaches-bermuda-v1.json",
  adp_nobu_london_portman:
    "lib/ai-visibility-audit-action-plan/payloads/nobu-london-portman-v1.json",
  adp_hilton_times_square:
    "lib/ai-visibility-audit-action-plan/payloads/hilton-times-square-v1.json",
  adp_radisson_blu_toronto_downtown:
    "lib/ai-visibility-audit-action-plan/payloads/radisson-blu-toronto-downtown-v1.json",
  adp_casas_del_xvi:
    "lib/ai-visibility-audit-action-plan/payloads/casas-del-xvi-v1.json",
  adp_faranda_collection_bogota:
    "lib/ai-visibility-audit-action-plan/payloads/faranda-collection-bogota-v1.json",
  adp_hotel_caribe_faranda_grand:
    "lib/ai-visibility-audit-action-plan/payloads/hotel-caribe-faranda-grand-v1.json",
  adp_jw_marriott_monterrey_valle:
    "lib/ai-visibility-audit-action-plan/payloads/jw-marriott-monterrey-valle-v1.json",
  adp_now_now_noho:
    "lib/ai-visibility-audit-action-plan/payloads/now-now-noho-v1.json",
  adp_radisson_santo_domingo:
    "lib/ai-visibility-audit-action-plan/payloads/radisson-santo-domingo-v1.json",
  adp_renaissance_times_square:
    "lib/ai-visibility-audit-action-plan/payloads/renaissance-times-square-v1.json",
  adp_westin_grand_munchen:
    "lib/ai-visibility-audit-action-plan/payloads/westin-grand-munchen-v1.json",
  adp_westin_monterrey_valle:
    "lib/ai-visibility-audit-action-plan/payloads/westin-monterrey-valle-v1.json",
  adp_westin_tampa_waterside:
    "lib/ai-visibility-audit-action-plan/payloads/westin-tampa-waterside-v1.json",
  adp_yotel_geneva_lake:
    "lib/ai-visibility-audit-action-plan/payloads/yotel-geneva-lake-v1.json",
});

function resolveStagedPayloadPath(propertyId) {
  const rel = AVA_PAYLOAD_BY_PROPERTY[propertyId];
  if (!rel) return null;
  return path.join(ROOT, rel);
}

function loadStagedPayload(propertyId) {
  const abs = resolveStagedPayloadPath(propertyId);
  if (!abs || !fs.existsSync(abs)) return null;
  return JSON.parse(fs.readFileSync(abs, "utf8"));
}

function sendJson(res, status, body) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.end(JSON.stringify(body));
}

/**
 * Prefer Express-parsed JSON (express.json already consumed the stream).
 * Fall back to raw stream only when body was not pre-parsed.
 */
function readBody(req) {
  if (req.body && typeof req.body === "object" && !Buffer.isBuffer(req.body)) {
    return Promise.resolve(req.body);
  }
  return new Promise((resolve, reject) => {
    // If the readable stream already ended with no listeners, avoid hanging.
    if (req.readableEnded || req.complete) {
      return resolve({});
    }
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      const raw = Buffer.concat(chunks).toString("utf8");
      if (!raw) return resolve({});
      try {
        resolve(JSON.parse(raw));
      } catch (err) {
        reject(err);
      }
    });
    req.on("error", reject);
  });
}

/** GET /api/admin/ai-visibility-audit/meta */
export function getAdminAvaMeta(req, res) {
  sendJson(res, 200, {
    ok: true,
    product: "AI Visibility Audit & Action Plan",
    statuses: AVA_LIFECYCLE_STATUSES,
    modules: AVA_STANDARD_MODULES,
    formalGates: {
      AUDIT_VALIDATED:
        "Internal research, evidence, recommendations, and PDF quality certified. Does not authorize customer distribution.",
      FOUNDER_APPROVED_FOR_CUSTOMER:
        "Explicit founder authorization permits the approved report version to be distributed. Separate from AUDIT_VALIDATED.",
    },
    publicationPolicy:
      "PDF success and AUDIT_VALIDATED never imply CUSTOMER_READY. FOUNDER_APPROVED_FOR_CUSTOMER + founder/customer publication flags required before CUSTOMER_READY/PUBLISHED.",
    paidResearchDefault: false,
    productionWrites: false,
  });
}

/** GET /api/admin/ai-visibility-audit/hotels */
export function getAdminAvaHotels(req, res) {
  try {
    // Universe = every ADP property with a staged certified/staged report.
    // Do NOT restrict to hotels already present in the AVA file store.
    const stagedIds = listAdpPropertyIdsWithStagedReports();
    let stored = [];
    try {
      stored = listStoredAudits();
    } catch (storeErr) {
      console.error("[ava-admin] listStoredAudits failed:", storeErr?.message || storeErr);
      stored = [];
    }
    const byId = new Map(stored.map((s) => [s.propertyId, s]));
    const hotels = [];
    for (const propertyId of stagedIds) {
      let baseline = { ok: false, error: "LOAD_FAILED" };
      try {
        baseline = loadAdpBaselineForAudit(propertyId);
      } catch (loadErr) {
        console.error(
          "[ava-admin] loadAdpBaselineForAudit failed:",
          propertyId,
          loadErr?.message || loadErr
        );
      }
      const meta = byId.get(propertyId) || {
        propertyId,
        status: baseline.ok ? "BASELINE_READY" : "NOT_STARTED",
      };
      const latestEntry =
        Array.isArray(meta.versions) && meta.latestVersionId
          ? meta.versions.find((v) => v && v.versionId === meta.latestVersionId) ||
            meta.versions[meta.versions.length - 1]
          : Array.isArray(meta.versions) && meta.versions.length
            ? meta.versions[meta.versions.length - 1]
            : null;
      const recommendationCount =
        Number(
          latestEntry?.actionCount ??
            meta.acceptedRecommendationCount ??
            0
        ) || 0;
      hotels.push({
        propertyId,
        name: baseline.baseline?.name || meta.name || propertyId,
        market: baseline.baseline?.market || meta.market || "",
        website: baseline.baseline?.website || "",
        adpBaselinePeriodId:
          baseline.baseline?.periodId || meta.adpBaselinePeriodId || null,
        adpBaselineOk: baseline.ok === true,
        auditStatus: meta.status || (baseline.ok ? "BASELINE_READY" : "NOT_STARTED"),
        acceptedRecommendationCount: meta.acceptedRecommendationCount || 0,
        recommendationCount,
        researchCostUsd: meta.researchCostUsd || 0,
        evidenceReviewStatus: meta.evidenceReviewStatus || "NOT_STARTED",
        pdfStatus: meta.pdfStatus || "MISSING",
        customerReadinessStatus:
          meta.customerReadinessStatus || "NOT_CUSTOMER_READY",
        publicationAuthorizationStatus:
          meta.publicationAuthorizationStatus || "NOT_AUTHORIZED",
        latestVersionId: meta.latestVersionId || null,
        founderApprovedForCustomer: meta.founderApprovedForCustomer === true,
        versionCount: Array.isArray(meta.versions) ? meta.versions.length : 0,
      });
    }
    hotels.sort((a, b) => String(a.name || "").localeCompare(String(b.name || "")));
    sendJson(res, 200, {
      ok: true,
      count: hotels.length,
      hotels,
    });
  } catch (err) {
    console.error("[ava-admin] getAdminAvaHotels failed:", err?.message || err);
    sendJson(res, 500, {
      ok: false,
      error: "HOTEL_LIST_FAILED",
      message: "Unable to load hotel data. Please try again.",
    });
  }
}

/** GET /api/admin/ai-visibility-audit/:propertyId */
export function getAdminAvaHotel(req, res) {
  const propertyId = req.params.propertyId;
  const baseline = loadAdpBaselineForAudit(propertyId);
  const meta = getAuditMeta(propertyId);
  const latest = getLatestAuditVersion(propertyId);
  const events = listAuditEvents(propertyId);
  const reportVersions = Array.isArray(meta?.versions)
    ? meta.versions.filter((v) => !String(v.versionId || "").includes("_decision"))
    : [];
  const decisionInducedVersions = Array.isArray(meta?.versions)
    ? meta.versions.filter((v) => String(v.versionId || "").includes("_decision"))
    : [];
  sendJson(res, 200, {
    ok: true,
    propertyId,
    baseline: baseline.ok ? baseline.baseline : null,
    baselineError: baseline.ok ? null : baseline.error,
    meta: meta || {
      propertyId,
      status: baseline.ok ? "BASELINE_READY" : "NOT_STARTED",
    },
    latestVersion: latest
      ? {
          versionId: latest.versionId,
          status: latest.status,
          savedAt: latest.savedAt,
          pdfRelativePath: latest.pdfRelativePath,
          actionCount: latest.payload?.actions?.length || 0,
          validation: latest.validation || null,
          kind: latest.kind || "REPORT_VERSION",
        }
      : null,
    versionHistory: {
      reportVersions,
      decisionInducedVersionsPreserved: decisionInducedVersions,
      note:
        "Historical _decision versionIds are preserved. New recommendation decisions append events only and do not create PDF versions.",
    },
    events,
    modules: AVA_STANDARD_MODULES,
    storage: getAvaStorageStatus(),
    evidenceHyperlinkCertification: {
      summary: summarizeCertificationForAdmin(
        meta?.evidenceHyperlinkCertification?.lastFullReport || null
      ),
      lastRun: meta?.evidenceHyperlinkCertification?.lastRun || null,
      manualVerificationCount: Object.keys(
        meta?.evidenceHyperlinkCertification?.manualVerifications || {}
      ).length,
    },
    autonomousEvidenceResolution: {
      pipelineStatus:
        meta?.evidencePipelineStatus || "EVIDENCE_VALIDATION_PENDING",
      summary: summarizeResolutionForAdmin(
        meta?.autonomousEvidenceResolution?.lastRun || null
      ),
      resolutionLog:
        meta?.autonomousEvidenceResolution?.lastRun?.resolutionLog || [],
      founderReviewDiffs:
        meta?.autonomousEvidenceResolution?.lastRun?.founderReviewDiffs || [],
    },
    customerQualityAssurance: meta?.customerQualityAssurance || {
      customerQualityCertified: false,
      gates: null,
      totalRepairs: 0,
      unresolvedMaterialIssues: [],
      assessedAt: null,
      note: "Customer-quality certification is independent of AUDIT_VALIDATED and founder approval.",
    },
    pdfChannels: describeAvaPdfChannels(propertyId),
  });
}

/**
 * POST /api/admin/ai-visibility-audit/:propertyId/evidence-validation
 * Mandatory autonomous evidence verification + remediation + PDF certification.
 * Does not publish or grant founder approval.
 */
export async function postAdminAvaEvidenceValidation(req, res) {
  try {
    const propertyId = assertSafeAvaPropertyId(req.params.propertyId);
    const body = await readBody(req).catch(() => ({}));
    const meta = getAuditMeta(propertyId) || { propertyId };
    const latest = getLatestAuditVersion(propertyId);
    let payload = latest?.payload || loadStagedPayload(propertyId);
    if (!payload) {
      return sendJson(res, 404, { ok: false, error: "NO_PAYLOAD" });
    }
    let pdfBuffer = null;
    const pdfFile = resolveAvaPdfFileForDelivery({
      propertyId,
      versionId: latest?.versionId || null,
      pdfRelativePath: latest?.pdfRelativePath || meta.pdfRelativePath || null,
    });
    if (pdfFile?.abs && fs.existsSync(pdfFile.abs)) {
      pdfBuffer = fs.readFileSync(pdfFile.abs);
    }

    upsertAuditMeta(propertyId, {
      evidencePipelineStatus: "EVIDENCE_REMEDIATION_RUNNING",
      forceStatus: true,
      founderApprovedForCustomer: meta.founderApprovedForCustomer === true,
      founderApprovedForCustomerAt: meta.founderApprovedForCustomerAt || null,
    });

    const pipeline = await runMandatoryAvaEvidenceStages(payload, {
      pdfBuffer,
      manualVerifications: loadManualVerificationsFromMeta(meta),
      claimSupportByUrl: body.claimSupportByUrl || {},
      dryRun: true,
      persistCache: true,
    });
    const remediation = pipeline.result;
    payload = remediation.payload;
    const certification = remediation.certification;
    const cqa =
      remediation.customerQualityAssurance ||
      pipeline.customerQualityAssurance ||
      null;

    // Persist payload when evidence or customer-quality repairs changed content
    const repaired =
      (remediation.urlsRepaired || []).length > 0 ||
      (remediation.claimsRevised || []).length > 0 ||
      (remediation.claimsRemoved || []).length > 0 ||
      (cqa?.totalRepairs || 0) > 0;
    if (repaired) {
      const stagedPath = resolveStagedPayloadPath(propertyId);
      if (stagedPath) {
        fs.writeFileSync(stagedPath, `${JSON.stringify(payload, null, 2)}\n`);
      }
    }

    const nextMeta = upsertAuditMeta(propertyId, {
      evidencePipelineStatus: remediation.pipelineStatus,
      evidenceHyperlinkCertification: {
        ...(meta.evidenceHyperlinkCertification || {}),
        lastRun: summarizeCertificationForAdmin(certification),
        lastFullReport: {
          checkedAt: certification.checkedAt,
          gate: certification.gate,
          summary: certification.summary,
          evidence: certification.evidence,
          pdfAnnotations: {
            uniqueUrls: certification.pdfAnnotations?.uniqueUrls || [],
            compare: certification.pdfAnnotations?.compare || null,
          },
        },
        manualVerifications:
          meta.evidenceHyperlinkCertification?.manualVerifications || {},
      },
      autonomousEvidenceResolution: {
        lastRun: {
          finishedAt: remediation.finishedAt,
          summary: remediation.summary,
          resolutionLog: remediation.resolutionLog,
          urlsRepaired: remediation.urlsRepaired,
          claimsRevised: remediation.claimsRevised,
          claimsRemoved: remediation.claimsRemoved,
          founderReviewDiffs: remediation.founderReviewDiffs,
          claimEvidenceGraph: {
            contract: remediation.claimEvidenceGraph?.contract,
            claimCount: remediation.claimEvidenceGraph?.claimCount,
          },
        },
      },
      customerQualityAssurance: cqa
        ? {
            contract: cqa.contract,
            customerQualityCertified: cqa.customerQualityCertified === true,
            assessedAt: cqa.assessment?.assessedAt || new Date().toISOString(),
            totalRepairs: cqa.totalRepairs || 0,
            gates: {
              researchQuality: cqa.assessment?.gates?.researchQuality?.pass,
              evidenceQuality: cqa.assessment?.gates?.evidenceQuality?.pass,
              communicationQuality:
                cqa.assessment?.gates?.communicationQuality?.pass,
              pdfPresentationQuality:
                cqa.assessment?.gates?.pdfPresentationQuality?.pass,
            },
            unresolvedMaterialIssues: cqa.unresolvedMaterialIssues || [],
            qaLog: (cqa.history || []).flatMap((h) => h.log || []).slice(-50),
            founderApprovedForCustomer: false,
            publicationAuthorized: false,
          }
        : meta.customerQualityAssurance || null,
      evidenceReviewStatus:
        certification.gate.pass && cqa?.customerQualityCertified !== false
          ? meta.evidenceReviewStatus || "STAGED"
          : "VALIDATION_REQUIRED",
      forceStatus: true,
      founderApprovedForCustomer: meta.founderApprovedForCustomer === true,
      founderApprovedForCustomerAt: meta.founderApprovedForCustomerAt || null,
    });
    // Flag previously validated hotels that now fail — do not clear founder approvals.
    if (
      !certification.gate.pass &&
      (meta.status === "AUDIT_VALIDATED" ||
        meta.status === "FOUNDER_REVIEWED_STAGED")
    ) {
      upsertAuditMeta(propertyId, {
        status: "VALIDATION_REQUIRED",
        statusNote:
          "Revalidation required: evidence/hyperlink certification gate failed after autonomous remediation.",
        forceStatus: true,
        founderApprovedForCustomer: meta.founderApprovedForCustomer === true,
        founderApprovedForCustomerAt: meta.founderApprovedForCustomerAt || null,
      });
    }
    sendJson(res, 200, {
      ok: true,
      propertyId,
      gatePass: certification.gate.pass === true,
      summary: certification.summary,
      reasons: certification.gate.reasons || [],
      evidence: certification.evidence,
      pdfAnnotations: certification.pdfAnnotations,
      autonomousEvidenceResolution: summarizeResolutionForAdmin(remediation),
      resolutionLog: remediation.resolutionLog,
      founderReviewDiffs: remediation.founderReviewDiffs,
      meta: getAuditMeta(propertyId) || nextMeta,
    });
  } catch (err) {
    if (err && err.code === "INVALID_PROPERTY_ID") {
      return sendJson(res, 400, { ok: false, error: err.code });
    }
    if (err && err.code === "AVA_EVIDENCE_STAGES_CANNOT_BE_SKIPPED") {
      return sendJson(res, 400, { ok: false, error: err.code, message: err.message });
    }
    sendJson(res, 500, {
      ok: false,
      error: String(err && err.message ? err.message : err),
    });
  }
}

/**
 * POST /api/admin/ai-visibility-audit/:propertyId/evidence-validation/manual
 * Record a manual verification decision with mandatory explanation + audit trail.
 */
export async function postAdminAvaEvidenceManualVerification(req, res) {
  try {
    const propertyId = assertSafeAvaPropertyId(req.params.propertyId);
    const body = await readBody(req);
    const url = String(body.url || "").trim();
    const decision = String(body.decision || "").toUpperCase();
    const explanation = String(body.explanation || "").trim();
    if (!/^https?:\/\//i.test(url)) {
      return sendJson(res, 400, { ok: false, error: "URL_REQUIRED" });
    }
    if (!["VERIFIED", "REJECTED", "DEFER"].includes(decision)) {
      return sendJson(res, 400, {
        ok: false,
        error: "DECISION_REQUIRED",
        message: "decision must be VERIFIED | REJECTED | DEFER",
      });
    }
    if (explanation.length < 12) {
      return sendJson(res, 400, {
        ok: false,
        error: "EXPLANATION_REQUIRED",
        message:
          "Manual override requires a written explanation (min 12 characters) and is audit-trailed.",
      });
    }
    const meta = getAuditMeta(propertyId) || { propertyId };
    const prior = meta.evidenceHyperlinkCertification || {};
    const manual = {
      ...(prior.manualVerifications || {}),
    };
    manual[url] = {
      decision,
      explanation,
      observation: body.observation || null,
      associatedClaimOrActionId: body.associatedClaimOrActionId || null,
      verifiedAt: new Date().toISOString(),
      verifiedBy: body.verifiedBy || "admin",
      finalResolvedUrl: body.finalResolvedUrl || url,
    };
    upsertAuditMeta(propertyId, {
      evidenceHyperlinkCertification: {
        ...prior,
        manualVerifications: manual,
      },
      forceStatus: true,
    });
    recordApprovalEvent(propertyId, {
      kind: "EVIDENCE_MANUAL_VERIFICATION",
      status: meta.status || null,
      note: `${decision}: ${url} — ${explanation}`,
      simulated: body.simulated === true,
    });
    sendJson(res, 200, {
      ok: true,
      propertyId,
      manualVerification: manual[url],
      note: "Manual verification recorded. Re-run evidence validation to refresh the gate.",
    });
  } catch (err) {
    if (err && err.code === "INVALID_PROPERTY_ID") {
      return sendJson(res, 400, { ok: false, error: err.code });
    }
    sendJson(res, 500, {
      ok: false,
      error: String(err && err.message ? err.message : err),
    });
  }
}

/** POST /api/admin/ai-visibility-audit/:propertyId/status */
export async function postAdminAvaStatus(req, res) {
  try {
    const propertyId = req.params.propertyId;
    const body = await readBody(req);
    const to = String(body.status || "");
    const meta = getAuditMeta(propertyId) || {
      propertyId,
      status: "NOT_STARTED",
    };
    if (!canTransitionAvaLifecycle(meta.status, to)) {
      return sendJson(res, 400, {
        ok: false,
        error: "TRANSITION_DENIED",
        from: meta.status,
        to,
      });
    }
    if (to === "AUDIT_VALIDATED") {
      const latest = getLatestAuditVersion(propertyId);
      const payload = latest?.payload || loadStagedPayload(propertyId);
      if (!payload) {
        return sendJson(res, 400, {
          ok: false,
          error: "NO_PAYLOAD",
          message: "Cannot validate audit without a staged payload.",
        });
      }
      let pdfBuffer = null;
      const pdfFile = resolveAvaPdfFileForDelivery({
        propertyId,
        versionId: latest?.versionId || null,
        pdfRelativePath: latest?.pdfRelativePath || meta.pdfRelativePath || null,
      });
      if (pdfFile?.abs && fs.existsSync(pdfFile.abs)) {
        pdfBuffer = fs.readFileSync(pdfFile.abs);
      }
      const certification = await certifyAvaEvidenceAndHyperlinks(payload, {
        pdfBuffer,
        manualVerifications: {
          ...loadManualVerificationsFromMeta(meta),
          ...(body.manualVerifications || {}),
        },
        claimSupportByUrl: body.claimSupportByUrl || {},
      });
      const gate = assertEvidenceGateAllowsAuditValidated(certification);
      upsertAuditMeta(propertyId, {
        evidenceHyperlinkCertification: {
          ...(meta.evidenceHyperlinkCertification || {}),
          lastRun: summarizeCertificationForAdmin(certification),
          lastFullReport: {
            checkedAt: certification.checkedAt,
            gate: certification.gate,
            summary: certification.summary,
            evidence: certification.evidence,
            pdfAnnotations: {
              uniqueUrls: certification.pdfAnnotations?.uniqueUrls || [],
              compare: certification.pdfAnnotations?.compare || null,
            },
          },
          manualVerifications:
            meta.evidenceHyperlinkCertification?.manualVerifications || {},
        },
        evidenceReviewStatus: gate.ok
          ? "AUDIT_VALIDATED"
          : "VALIDATION_REQUIRED",
        forceStatus: true,
      });
      if (!gate.ok) {
        if (meta.status !== "VALIDATION_REQUIRED") {
          upsertAuditMeta(propertyId, {
            status: "VALIDATION_REQUIRED",
            statusNote:
              "Evidence & hyperlink certification gate failed; AUDIT_VALIDATED blocked.",
            forceStatus: true,
            // Never clear founder approval flags automatically.
            founderApprovedForCustomer: meta.founderApprovedForCustomer === true,
            founderApprovedForCustomerAt: meta.founderApprovedForCustomerAt || null,
          });
        }
        return sendJson(res, 409, {
          ok: false,
          error: gate.error,
          message: gate.message,
          reasons: gate.reasons,
          summary: gate.summary,
          requiredStatus: "VALIDATION_REQUIRED",
        });
      }
    }
    if (to === "FOUNDER_APPROVED_FOR_CUSTOMER") {
      if (body.founderApprovedForCustomer !== true) {
        return sendJson(res, 403, {
          ok: false,
          error: "FOUNDER_APPROVAL_FLAG_REQUIRED",
          message:
            "FOUNDER_APPROVED_FOR_CUSTOMER requires founderApprovedForCustomer=true. AUDIT_VALIDATED alone is insufficient.",
        });
      }
      if (!isAuditValidated(meta.status) && meta.status !== "FOUNDER_REVIEWED_STAGED" && meta.status !== "FOUNDER_APPROVED") {
        return sendJson(res, 400, {
          ok: false,
          error: "AUDIT_VALIDATED_REQUIRED",
          message: "Founder customer approval requires AUDIT_VALIDATED (or founder review) first.",
          from: meta.status,
        });
      }
    }
    if (to === "CUSTOMER_READY" || to === "PUBLISHED") {
      if (
        !isCustomerFacingPublicationAllowed(to, {
          founderPublicationAuthorized: body.founderPublicationAuthorized === true,
          customerPublicationAuthorized: body.customerPublicationAuthorized === true,
          founderApprovedForCustomer:
            body.founderApprovedForCustomer === true ||
            isFounderApprovedForCustomer(meta.status),
        })
      ) {
        return sendJson(res, 403, {
          ok: false,
          error: "PUBLICATION_NOT_AUTHORIZED",
          message:
            "Customer-facing publication requires FOUNDER_APPROVED_FOR_CUSTOMER plus explicit founder and customer authorizations. AUDIT_VALIDATED / PDF success is not enough.",
        });
      }
    }
    const next = upsertAuditMeta(propertyId, {
      status: to,
      statusNote: body.note || null,
      auditValidatedAt:
        to === "AUDIT_VALIDATED" ? new Date().toISOString() : meta.auditValidatedAt || null,
      founderApprovedForCustomerAt:
        to === "FOUNDER_APPROVED_FOR_CUSTOMER"
          ? new Date().toISOString()
          : meta.founderApprovedForCustomerAt || null,
      founderApprovedForCustomer: to === "FOUNDER_APPROVED_FOR_CUSTOMER" ? true : meta.founderApprovedForCustomer === true,
      customerReadinessStatus:
        to === "CUSTOMER_READY" || to === "PUBLISHED"
          ? "CUSTOMER_READY"
          : "NOT_CUSTOMER_READY",
      publicationAuthorizationStatus:
        to === "PUBLISHED"
          ? "AUTHORIZED"
          : to === "FOUNDER_APPROVED_FOR_CUSTOMER" || to === "CUSTOMER_READY"
            ? "FOUNDER_AUTHORIZED_PENDING_DISTRIBUTION"
            : "NOT_AUTHORIZED",
    });
    if (
      to === "FOUNDER_APPROVED_FOR_CUSTOMER" ||
      to === "AUDIT_VALIDATED" ||
      to === "CUSTOMER_READY" ||
      to === "PUBLISHED"
    ) {
      recordApprovalEvent(propertyId, {
        kind:
          to === "FOUNDER_APPROVED_FOR_CUSTOMER"
            ? "FOUNDER_APPROVAL"
            : to === "AUDIT_VALIDATED"
              ? "AUDIT_VALIDATION"
              : "STATUS_TRANSITION",
        status: to,
        note: body.note || null,
        simulated: body.simulated === true,
      });
    }
    sendJson(res, 200, {
      ok: true,
      meta: getAuditMeta(propertyId) || next,
      gates: {
        auditValidated: isAuditValidated(next.status) || isFounderApprovedForCustomer(next.status),
        founderApprovedForCustomer: isFounderApprovedForCustomer(next.status),
        customerFacingPublicationAllowed: false,
      },
    });
  } catch (err) {
    const code = err && err.code === "AVA_STORAGE_UNAVAILABLE" ? 503 : 500;
    sendJson(res, code, {
      ok: false,
      error: err && err.code === "AVA_STORAGE_UNAVAILABLE" ? "AVA_STORAGE_UNAVAILABLE" : String(err && err.message ? err.message : err),
      message: err && err.message ? err.message : String(err),
    });
  }
}

/** GET /api/admin/ai-visibility-audit/:propertyId/versions/:versionId */
export function getAdminAvaVersion(req, res) {
  const { propertyId, versionId } = req.params;
  const version = loadAuditVersion(propertyId, versionId);
  if (!version) return sendJson(res, 404, { ok: false, error: "VERSION_NOT_FOUND" });
  sendJson(res, 200, { ok: true, version });
}

/** POST /api/admin/ai-visibility-audit/:propertyId/actions/:actionId/decision */
export async function postAdminAvaActionDecision(req, res) {
  try {
    const { propertyId, actionId } = req.params;
    const body = await readBody(req);
    const decision = String(body.decision || "").toUpperCase();
    if (!["APPROVE", "REJECT", "DEFER"].includes(decision)) {
      return sendJson(res, 400, { ok: false, error: "INVALID_DECISION" });
    }
    const latest = getLatestAuditVersion(propertyId);
    if (!latest) return sendJson(res, 404, { ok: false, error: "NO_AUDIT_VERSION" });
    const action = (latest.payload?.actions || []).find((a) => a.actionId === actionId);
    if (!action) return sendJson(res, 404, { ok: false, error: "ACTION_NOT_FOUND" });
    // Decisions are events — do NOT create a new report/PDF version.
    const meta = recordRecommendationDecision(propertyId, actionId, {
      decision,
      note: body.note || null,
      actor: "ai-admin",
    });
    sendJson(res, 200, {
      ok: true,
      actionId,
      decision,
      meta,
      versionId: meta.latestVersionId,
      createdNewReportVersion: false,
    });
  } catch (err) {
    const code = err && err.code === "AVA_STORAGE_UNAVAILABLE" ? 503 : 500;
    sendJson(res, code, {
      ok: false,
      error:
        err && err.code === "AVA_STORAGE_UNAVAILABLE"
          ? "AVA_STORAGE_UNAVAILABLE"
          : String(err && err.message ? err.message : err),
      message: err && err.message ? err.message : String(err),
    });
  }
}

/**
 * GET /api/admin/ai-visibility-audit/:propertyId/pdf
 * Query:
 *   download=1 — Content-Disposition: attachment (real browser download)
 *   channel=published|draft|history — required channel (default: draft)
 *   versionId= — required when channel=history; optional pin for draft
 *
 * Published and draft never silently substitute. Missing → explicit 404.
 * Internal admin download ≠ customer publication.
 */
export function getAdminAvaPdf(req, res) {
  try {
    const propertyId = assertSafeAvaPropertyId(req.params.propertyId);
    const download = String(req.query?.download || "") === "1";
    const versionIdRaw = String(req.query?.versionId || "").trim();
    if (versionIdRaw) assertSafeAvaVersionId(versionIdRaw);

    let channel = String(req.query?.channel || "")
      .trim()
      .toLowerCase();
    if (!channel) {
      // Legacy callers: versionId alone ⇒ history; otherwise draft (latest staged).
      channel = versionIdRaw ? AVA_PDF_CHANNEL.HISTORY : AVA_PDF_CHANNEL.DRAFT;
    }
    if (!Object.values(AVA_PDF_CHANNEL).includes(channel)) {
      return sendJson(res, 400, {
        ok: false,
        error: "INVALID_PDF_CHANNEL",
        allowed: Object.values(AVA_PDF_CHANNEL),
      });
    }

    const resolved = resolveAvaPdfByChannel({
      propertyId,
      channel,
      versionId: versionIdRaw || null,
    });
    if (!resolved.ok) {
      const status =
        resolved.error === "INVALID_PDF_CHANNEL" ||
        resolved.error === "VERSION_ID_REQUIRED"
          ? 400
          : 404;
      return sendJson(res, status, {
        ok: false,
        error: resolved.error,
        channel,
        versionId: resolved.versionId || versionIdRaw || null,
        message: resolved.message || null,
      });
    }

    const meta = getAuditMeta(propertyId);
    const version =
      (resolved.versionId && loadAuditVersion(propertyId, resolved.versionId)) ||
      getLatestAuditVersion(propertyId);
    const filename = buildAvaPdfFilenameFromVersion(version, meta || {});
    res.statusCode = 200;
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      contentDispositionForAvaPdf(filename, {
        disposition: download ? "attachment" : "inline",
      })
    );
    res.setHeader("X-AVA-PDF-Filename", filename);
    res.setHeader("X-AVA-PDF-Sha256", resolved.sha256);
    res.setHeader("X-AVA-PDF-Channel", resolved.channel);
    if (resolved.versionId) {
      res.setHeader("X-AVA-PDF-Version-Id", String(resolved.versionId));
    }
    if (resolved.mtime) {
      res.setHeader("X-AVA-PDF-Created", String(resolved.mtime));
    }
    if (resolved.bytes != null) {
      res.setHeader("X-AVA-PDF-Bytes", String(resolved.bytes));
    }
    // Do not leak storage paths to clients.
    fs.createReadStream(resolved.abs).pipe(res);
  } catch (err) {
    if (err && (err.code === "INVALID_PROPERTY_ID" || err.code === "INVALID_VERSION_ID")) {
      return sendJson(res, 400, { ok: false, error: err.code });
    }
    if (err && err.message === "INVALID_PDF_FILENAME") {
      return sendJson(res, 500, { ok: false, error: "INVALID_PDF_FILENAME" });
    }
    sendJson(res, 500, {
      ok: false,
      error: String(err && err.message ? err.message : err),
    });
  }
}

/** POST /api/admin/ai-visibility-audit/:propertyId/validate-payload */
export async function postAdminAvaValidatePayload(req, res) {
  try {
    const body = await readBody(req);
    const latest = getLatestAuditVersion(req.params.propertyId);
    const payload = body.payload || latest?.payload;
    if (!payload) return sendJson(res, 400, { ok: false, error: "NO_PAYLOAD" });
    const gate = assertAuditPdfPayload(payload);
    sendJson(res, 200, { ok: gate.ok, errors: gate.errors || [], templateId: gate.templateId });
  } catch (err) {
    sendJson(res, 500, {
      ok: false,
      error: String(err && err.message ? err.message : err),
    });
  }
}

/** GET /api/admin/ai-visibility-audit/:propertyId/recommendations */
export function getAdminAvaRecommendations(req, res) {
  const propertyId = req.params.propertyId;
  const latest = getLatestAuditVersion(propertyId);
  const payload = latest?.payload || loadStagedPayload(propertyId);
  if (!payload) return sendJson(res, 404, { ok: false, error: "NO_PAYLOAD" });
  const actions = (payload.actions || []).map((a) => ({
    actionId: a.actionId,
    headline: a.headline,
    priority: a.priority,
    claimType: a.claimType,
    whatWeFound: a.whatWeFound,
    whyItMatters: a.whyItMatters,
    accountableOwner: a.accountableOwner,
    supportingParties: a.supportingParties,
    suggestedTimeframe: a.suggestedTimeframe,
    implementationStatus: a.implementationStatus,
    requiresHotelApproval: a.requiresHotelApproval,
    evidence: a.supportingEvidence || [],
    adminDecision: a.adminDecision || null,
  }));
  sendJson(res, 200, {
    ok: true,
    propertyId,
    status: latest?.status || payload.status || "NOT_STARTED",
    researchCostUsd: latest?.researchCostUsd ?? 0,
    paidResearchAuthorized: false,
    actions,
  });
}

/**
 * POST /api/admin/ai-visibility-audit/:propertyId/generate-pdf
 * Staged generation only — never promotes CUSTOMER_READY / PUBLISHED.
 */
export async function postAdminAvaGeneratePdf(req, res) {
  try {
    const propertyId = req.params.propertyId;
    const body = await readBody(req).catch(() => ({}));
    if (body.authorizeCustomerPublication === true || body.publish === true) {
      return sendJson(res, 403, {
        ok: false,
        error: "PUBLICATION_BLOCKED",
        message: "PDF generation cannot authorize customer publication.",
      });
    }
    const baseline = loadAdpBaselineForAudit(propertyId);
    if (!baseline.ok) {
      return sendJson(res, 400, {
        ok: false,
        error: "ADP_BASELINE_REQUIRED",
        detail: baseline.error,
      });
    }
    let payload = loadStagedPayload(propertyId) || getLatestAuditVersion(propertyId)?.payload;
    if (!payload) {
      return sendJson(res, 404, {
        ok: false,
        error: "NO_STAGED_PAYLOAD",
        message:
          "No hotel-specific audit payload is staged for this property yet.",
      });
    }
    const gate = assertAuditPdfPayload(payload);
    if (!gate.ok) {
      return sendJson(res, 400, {
        ok: false,
        error: "PAYLOAD_INVALID",
        errors: gate.errors,
      });
    }

    if (isAvaDurableStorageEnabled()) {
      const storage = ensureAvaDurableStorage();
      if (!storage.ok || !storage.writable) {
        return sendJson(res, 503, {
          ok: false,
          error: "AVA_STORAGE_UNAVAILABLE",
          message: storage.message,
        });
      }
    }

    const filename = buildAiVisibilityAuditPdfFilename({
      hotelName: payload.property?.name,
      reportMonthLabel: payload.reporting?.reportMonthLabel,
      date: payload.reporting?.generatedAt,
    });
    const versionId = `ava_admin_${Date.now().toString(36)}`;
    const stagingDir = path.join(ROOT, "tmp", "ava-pdf-staging", propertyId);
    fs.mkdirSync(stagingDir, { recursive: true });
    const stagingPath = path.join(stagingDir, filename);

    const priorMeta = getAuditMeta(propertyId);
    upsertAuditMeta(propertyId, {
      evidencePipelineStatus: "EVIDENCE_REMEDIATION_RUNNING",
      forceStatus: true,
      founderApprovedForCustomer: priorMeta?.founderApprovedForCustomer === true,
      founderApprovedForCustomerAt: priorMeta?.founderApprovedForCustomerAt || null,
    });

    // Mandatory evidence stages 6–11 (verify → remediate → PDF → certify)
    const pipeline = await runMandatoryAvaEvidenceStages(payload, {
      manualVerifications: loadManualVerificationsFromMeta(priorMeta || {}),
      dryRun: true,
      persistCache: true,
      generatePdfFn: async ({ payload: p }) => {
        await generateAiVisibilityAuditPdfV1({
          payload: p,
          outPath: stagingPath,
          pagesDir: null,
          bridgeOutPath: path.join(
            stagingDir,
            "proposed-monthly-action-register-bridge.json"
          ),
        });
        return { buffer: fs.readFileSync(stagingPath), path: stagingPath };
      },
      forcePdfRegenerate: true,
    });
    const remediation = pipeline.result;
    payload = remediation.payload;
    const certification = remediation.certification;
    const generated = {
      filename,
      outPath: stagingPath,
    };
    if (!fs.existsSync(stagingPath)) {
      await generateAiVisibilityAuditPdfV1({
        payload,
        outPath: stagingPath,
        pagesDir: null,
        bridgeOutPath: path.join(
          stagingDir,
          "proposed-monthly-action-register-bridge.json"
        ),
      });
    }

    const staged = stageAvaPdf({
      propertyId,
      generatedAbsPath: stagingPath,
      versionId,
      filename,
    });

    // PDF generation never promotes to FOUNDER_APPROVED_FOR_CUSTOMER / CUSTOMER_READY.
    // Preserve AUDIT_VALIDATED / founder review statuses only when evidence gate still passes;
    // otherwise land at VALIDATION_REQUIRED / PDF_STAGED.
    const preserve = new Set([
      "AUDIT_VALIDATED",
      "FOUNDER_REVIEWED_STAGED",
      "FOUNDER_APPROVED",
      "FOUNDER_APPROVED_FOR_CUSTOMER",
    ]);
    const priorStatus = priorMeta?.status || payload.status;
    let nextStatus = preserve.has(priorStatus) ? priorStatus : "PDF_STAGED";
    if (!certification.gate.pass) {
      nextStatus = "VALIDATION_REQUIRED";
    }
    const meta = saveAuditVersion(propertyId, versionId, payload, {
      status: nextStatus,
      metaStatus: nextStatus,
      forceStatus: true,
      kind: "REPORT_VERSION",
      pdfRelativePath: staged.pdfRelativePath,
      researchCostUsd: 0,
      evidenceReviewStatus: certification.gate.pass
        ? nextStatus === "AUDIT_VALIDATED"
          ? "AUDIT_VALIDATED"
          : "STAGED"
        : "VALIDATION_REQUIRED",
      evidenceRegister: {
        contract: certification.contract,
        checkedAt: certification.checkedAt,
        summary: certification.summary,
        evidence: certification.evidence,
      },
      validation: {
        contractOk: true,
        generatedVia: "ai-admin",
        generatedAt: new Date().toISOString(),
        customerReadyBlocked: true,
        auditValidated: nextStatus === "AUDIT_VALIDATED",
        founderApprovedForCustomer: false,
        pdfSha256: staged.sha256,
        evidenceHyperlinkGatePass: certification.gate.pass === true,
        evidenceHyperlinkReasons: certification.gate.reasons || [],
        autonomousEvidencePipelineStatus: remediation.pipelineStatus,
      },
    });
    upsertAuditMeta(propertyId, {
      pdfRelativePath: staged.pdfRelativePath,
      pdfSha256: staged.sha256,
      draftVersionId: versionId,
      draftPdfRelativePath:
        staged.draftPdfRelativePath || staged.pdfRelativePath,
      draftPdfSha256: staged.sha256,
      draftUpdatedAt: new Date().toISOString(),
      // Never touch publishedPdf* here — staging is draft-only.
      evidencePipelineStatus: remediation.pipelineStatus,
      evidenceHyperlinkCertification: {
        ...(priorMeta?.evidenceHyperlinkCertification || {}),
        lastRun: summarizeCertificationForAdmin(certification),
        lastFullReport: {
          checkedAt: certification.checkedAt,
          gate: certification.gate,
          summary: certification.summary,
          evidence: certification.evidence,
          pdfAnnotations: {
            uniqueUrls: certification.pdfAnnotations?.uniqueUrls || [],
            compare: certification.pdfAnnotations?.compare || null,
          },
        },
        manualVerifications:
          priorMeta?.evidenceHyperlinkCertification?.manualVerifications || {},
      },
      autonomousEvidenceResolution: {
        lastRun: {
          finishedAt: remediation.finishedAt,
          summary: remediation.summary,
          resolutionLog: remediation.resolutionLog,
          urlsRepaired: remediation.urlsRepaired,
          claimsRevised: remediation.claimsRevised,
          claimsRemoved: remediation.claimsRemoved,
          founderReviewDiffs: remediation.founderReviewDiffs,
        },
      },
      // Preserve founder approval flags even when revalidation is required.
      founderApprovedForCustomer: priorMeta?.founderApprovedForCustomer === true,
      founderApprovedForCustomerAt: priorMeta?.founderApprovedForCustomerAt || null,
      forceStatus: true,
    });

    sendJson(res, 200, {
      ok: true,
      propertyId,
      versionId,
      pdfRelativePath: staged.pdfRelativePath,
      pdfSha256: staged.sha256,
      filename: generated.filename,
      status: meta.status,
      researchCostUsd: 0,
      customerReadinessStatus: "NOT_CUSTOMER_READY",
      publicationAuthorizationStatus: "NOT_AUTHORIZED",
      durableStorage: staged.durable === true,
      evidenceHyperlinkGate: {
        pass: certification.gate.pass === true,
        summary: certification.summary,
        reasons: certification.gate.reasons || [],
      },
      gates: {
        auditValidated: nextStatus === "AUDIT_VALIDATED",
        founderApprovedForCustomer: false,
      },
      note: "Staged PDF only. Evidence/hyperlink gate runs after generation. AUDIT_VALIDATED ≠ FOUNDER_APPROVED_FOR_CUSTOMER. CUSTOMER_READY requires separate founder authorization.",
    });
  } catch (err) {
    const code = err && err.code === "AVA_STORAGE_UNAVAILABLE" ? 503 : 500;
    sendJson(res, code, {
      ok: false,
      error:
        err && err.code === "AVA_STORAGE_UNAVAILABLE"
          ? "AVA_STORAGE_UNAVAILABLE"
          : String(err && err.message ? err.message : err),
      message: err && err.message ? err.message : String(err),
    });
  }
}

/** GET /api/admin/ai-visibility-audit/storage-status */
export function getAdminAvaStorageStatus(_req, res) {
  sendJson(res, 200, {
    ok: true,
    storage: getAvaStorageStatus(),
    eventsContract: "RECOMMENDATION_DECISION | FOUNDER_APPROVAL | AUDIT_VALIDATION",
  });
}
