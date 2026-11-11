/**
 * ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_LEDGER_V1
 *
 * Immutable ledger of founder EXECUTIVE_PDF_DESIGN approvals.
 * Only record when an explicit founder directive is present in-session
 * (or a future authenticated founder action). Automation must never invent hashes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_V1,
  FOUNDER_DESIGN_ACCEPTED,
  assessExecutivePdfFounderDesignAcceptanceV1,
} from "./adp-executive-pdf-founder-design-acceptance-v1.js";
import {
  resolveFullAdpE2ePdfArtifact,
  sha256File,
} from "./adp-full-e2e-pdf-artifact-store-v1.js";

export const ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_LEDGER_V1 =
  "ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_LEDGER_V1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");
export const FOUNDER_PDF_DESIGN_ACCEPTANCE_LEDGER_PATH = path.join(
  ROOT,
  "data/ai-demand-positioning/full-adp-e2e-pdf/founder-design-acceptance-ledger-v1.json"
);

function loadLedger() {
  if (!fs.existsSync(FOUNDER_PDF_DESIGN_ACCEPTANCE_LEDGER_PATH)) {
    return {
      contract: ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_LEDGER_V1,
      acceptanceGate: ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_V1,
      entries: [],
      updatedAt: null,
    };
  }
  return JSON.parse(
    fs.readFileSync(FOUNDER_PDF_DESIGN_ACCEPTANCE_LEDGER_PATH, "utf8")
  );
}

function saveLedger(doc) {
  fs.mkdirSync(path.dirname(FOUNDER_PDF_DESIGN_ACCEPTANCE_LEDGER_PATH), {
    recursive: true,
  });
  doc.updatedAt = new Date().toISOString();
  fs.writeFileSync(
    FOUNDER_PDF_DESIGN_ACCEPTANCE_LEDGER_PATH,
    `${JSON.stringify(doc, null, 2)}\n`,
    "utf8"
  );
  return doc;
}

/**
 * Record founder EXECUTIVE_PDF_DESIGN acceptance for an exact PDF revision.
 * Requires live artifact SHA to match opts.expectedPdfSha256.
 */
export function recordFounderExecutivePdfDesignAcceptanceV1(opts = {}) {
  const propertyId = String(opts.propertyId || "").trim();
  const stagedVersionId = String(opts.stagedVersionId || "").trim();
  const expectedPdfSha256 = String(opts.expectedPdfSha256 || "").trim().toLowerCase();
  const founderAcceptedBy = String(opts.founderAcceptedBy || "").trim();
  if (!propertyId || !stagedVersionId) {
    throw new Error("propertyId_and_stagedVersionId_required");
  }
  if (!expectedPdfSha256 || expectedPdfSha256.length !== 64) {
    throw new Error("expectedPdfSha256_required_full_64");
  }
  if (!founderAcceptedBy) {
    throw new Error("founderAcceptedBy_required");
  }

  const artifact = resolveFullAdpE2ePdfArtifact(propertyId, {
    expectedStagedVersionId: stagedVersionId,
  });
  if (!artifact.available || !artifact.pdfAbsolutePath) {
    throw new Error(`pdf_artifact_unavailable:${artifact.reason || "missing"}`);
  }
  const liveSha = sha256File(artifact.pdfAbsolutePath).toLowerCase();
  if (liveSha !== expectedPdfSha256) {
    throw Object.assign(new Error("pdf_sha_mismatch_refuse_approval"), {
      code: "ADP_FOUNDER_PDF_APPROVAL_SHA_MISMATCH",
      expectedPdfSha256,
      liveSha,
      propertyId,
      stagedVersionId,
    });
  }

  const acceptance = assessExecutivePdfFounderDesignAcceptanceV1({
    technicallyValid: opts.technicallyValid !== false,
    contentParityPass: opts.contentParityPass !== false,
    visualQaPass: opts.visualQaPass !== false,
    bethesdaDesignReviewReady: opts.bethesdaDesignReviewReady !== false,
    founderAccepted: true,
    founderAcceptedBy,
    founderAcceptedAt: opts.founderAcceptedAt || new Date().toISOString(),
    founderAcceptedNote:
      opts.founderAcceptedNote ||
      "EXECUTIVE_PDF_DESIGN approval — does not authorize publication, deploy, or external share issuance.",
  });

  const entry = {
    entryId: `pdf_design_accept_${propertyId}_${stagedVersionId}_${liveSha.slice(0, 12)}`,
    approvalScope: "EXECUTIVE_PDF_DESIGN",
    status: FOUNDER_DESIGN_ACCEPTED,
    contract: ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_V1,
    propertyId,
    stagedVersionId,
    pdfSha256: liveSha,
    pdfBytes: artifact.byteLength || null,
    pageCount: artifact.meta?.pageCount ?? opts.pageCount ?? null,
    templateId:
      artifact.meta?.templateId ||
      opts.templateId ||
      "AI_DEMAND_PERFORMANCE_REVIEW_CURRENT_TEMPLATE_V1",
    designAuthority:
      opts.designAuthority || "ADP_EXECUTIVE_REPORT_PDF_BETHESDA_MASTER_V1",
    renderer: artifact.meta?.renderer || opts.renderer || "AdpMonthlyReviewReportPdfV1",
    artifactType: artifact.artifactType,
    artifactPath: path
      .relative(ROOT, artifact.pdfAbsolutePath)
      .replace(/\\/g, "/"),
    aiAdminEndpoint: artifact.viewUrl || null,
    founderAcceptedBy,
    founderAcceptedAt: acceptance.founderAcceptedAt,
    founderAcceptedNote: acceptance.founderAcceptedNote,
    authorizationProvenance:
      opts.authorizationProvenance ||
      "founder_chat_directive_explicit_pdf_design_approval",
    doesNotAuthorize: [
      "CUSTOMER_PUBLICATION",
      "PRODUCTION_DEPLOYMENT",
      "EXTERNAL_CLIENT_SHARE_ISSUANCE",
      "PAID_PROVIDER_MONITORING",
      "AIRTABLE_WRITES",
    ],
    recordedAt: new Date().toISOString(),
  };

  const ledger = loadLedger();
  // Immutable: do not mutate prior entries; append superseding acceptance for same property+staged.
  const prior = ledger.entries.filter(
    (e) =>
      e.propertyId === propertyId &&
      e.stagedVersionId === stagedVersionId &&
      e.status === FOUNDER_DESIGN_ACCEPTED
  );
  for (const p of prior) {
    p.supersededAt = entry.recordedAt;
    p.supersededByEntryId = entry.entryId;
  }
  ledger.entries.push(entry);
  saveLedger(ledger);

  // Stamp artifact meta (non-secret) so AI Admin / parity can read acceptance.
  if (artifact.meta && fs.existsSync(path.join(path.dirname(artifact.pdfAbsolutePath), "meta.json"))) {
    const metaPath = path.join(path.dirname(artifact.pdfAbsolutePath), "meta.json");
    const meta = JSON.parse(fs.readFileSync(metaPath, "utf8"));
    meta.founderDesignAcceptance = FOUNDER_DESIGN_ACCEPTED;
    meta.founderDesignAcceptanceContract =
      ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_V1;
    meta.founderAcceptedBy = founderAcceptedBy;
    meta.founderAcceptedAt = acceptance.founderAcceptedAt;
    meta.founderAcceptedPdfSha256 = liveSha;
    meta.founderAcceptanceEntryId = entry.entryId;
    meta.visualQaStatus = FOUNDER_DESIGN_ACCEPTED;
    fs.writeFileSync(metaPath, `${JSON.stringify(meta, null, 2)}\n`, "utf8");
  }

  return { ok: true, entry, acceptance, ledgerPath: FOUNDER_PDF_DESIGN_ACCEPTANCE_LEDGER_PATH };
}

export function getFounderExecutivePdfDesignAcceptance(propertyId, stagedVersionId = null) {
  const ledger = loadLedger();
  const id = String(propertyId || "").trim();
  const ver = stagedVersionId ? String(stagedVersionId).trim() : null;
  const hits = ledger.entries
    .filter(
      (e) =>
        e.propertyId === id &&
        (!ver || e.stagedVersionId === ver) &&
        e.status === FOUNDER_DESIGN_ACCEPTED &&
        !e.supersededAt
    )
    .sort((a, b) => String(b.recordedAt).localeCompare(String(a.recordedAt)));
  return hits[0] || null;
}

export function listFounderExecutivePdfDesignAcceptances() {
  return loadLedger();
}
