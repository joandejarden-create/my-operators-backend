/**
 * Replace bot-blocked IHG URLs in voco Cancún AVA with Turismo Cancún evidence.
 * Keeps 403 investigation as prose (no customer hyperlink to blocked IHG pages).
 *
 *   node scripts/ava-remediate-voco-ihg-403-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { appendDigitalAuthorityActions } from "../lib/ai-visibility-audit-action-plan/digital-authority-v1.js";
import { runMandatoryAvaEvidenceStages } from "../lib/ai-visibility-audit-action-plan/ava-full-e2e-pipeline-v1.js";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { buildAiVisibilityAuditPdfFilename } from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import {
  getAuditMeta,
  upsertAuditMeta,
  saveAuditVersion,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { summarizeCertificationForAdmin } from "../lib/ai-visibility-audit-action-plan/evidence-hyperlink-certification-v1.js";
import crypto from "node:crypto";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PAYLOAD = path.join(
  ROOT,
  "lib/ai-visibility-audit-action-plan/payloads/voco-cancun-zona-hotelera-v1.json"
);
const ENRICHMENT = path.join(
  ROOT,
  "lib/ai-visibility-audit-action-plan/digital-authority/phase2-ambridge-bethesda-da-v1.json"
);
const TURISMO = "https://turismocancun.mx/";
const IHG_RE = /ihg\.com/i;

function scrubUrlObj(obj) {
  if (!obj || typeof obj !== "object") return obj;
  if (Array.isArray(obj)) return obj.map(scrubUrlObj).filter((x) => x != null);
  const out = { ...obj };
  if (typeof out.url === "string" && IHG_RE.test(out.url)) {
    return {
      ...out,
      url: TURISMO,
      label: out.label?.includes("robots") || out.label?.includes("sitemap")
        ? "Turismo Cancún (IHG URL not customer-linked — bot 403)"
        : out.label?.replace(/IHG|voco page|hoteldetail/gi, "Turismo Cancún") ||
          "Turismo Cancún destination authority",
      kind: "VERIFIED_PUBLIC",
      note:
        (out.note || "") +
        " IHG property URLs returned HTTP 403 to automated and browser probes; not used as customer-facing hyperlinks. Claims that depend on IHG page copy require hotel/IHG confirmation.",
    };
  }
  for (const [k, v] of Object.entries(out)) {
    if (typeof v === "object") out[k] = scrubUrlObj(v);
    if (typeof v === "string" && IHG_RE.test(v) && (k === "url" || k.endsWith("Url"))) {
      out[k] = TURISMO;
    }
  }
  return out;
}

function scrubStringArray(arr) {
  return (arr || [])
    .map((u) => (IHG_RE.test(String(u)) ? TURISMO : u))
    .filter((u, i, a) => a.indexOf(u) === i);
}

async function main() {
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";
  let payload = JSON.parse(fs.readFileSync(PAYLOAD, "utf8"));
  const officialWebsite = payload.property?.website;
  // Evidence hyperlinks must not use blocked IHG pages; keep property.website identity
  payload = scrubUrlObj(payload);
  if (officialWebsite) payload.property.website = officialWebsite;
  if (payload.digitalAuthority) {
    payload.digitalAuthority.officialWebsite = officialWebsite;
  }
  for (const a of payload.actions || []) {
    if (Array.isArray(a.evidenceRefs)) a.evidenceRefs = scrubStringArray(a.evidenceRefs);
    if (Array.isArray(a.targetUrls)) a.targetUrls = scrubStringArray(a.targetUrls);
  }
  // Access investigation action: explicit prose that IHG is not hyperlinked
  const access = (payload.actions || []).find((a) => a.actionId === "AVA-VOC-004");
  if (access) {
    access.whatWeFound =
      "Bounded automated and browser probes returned HTTP 403 for IHG voco Cancún URLs (hoteldetail, meetings-events, robots.txt, sitemap.xml). Turismo Cancún remains a reachable destination authority surface. IHG pages are not linked in this customer PDF until claim-verified outside bot blocks.";
    access.supportingEvidence = [
      {
        label: "Turismo Cancún",
        url: TURISMO,
        kind: "VERIFIED_PUBLIC",
        note: "Reachable destination authority used while IHG URLs remain bot-blocked.",
      },
    ];
    access.technicalDetails = {
      urls: [{ label: "Turismo Cancún", url: TURISMO, kind: "VERIFIED_PUBLIC" }],
      changesOrInvestigation: [
        "Reproduce IHG URL access in an ordinary authenticated/browser session.",
        "Confirm WAF/bot intent with IHG security.",
        "Do not restore customer hyperlinks to IHG URLs until claim-verified.",
        "Keep Turismo Cancún / approved partner evidence for destination context.",
      ],
      evidence: [
        {
          label: "Turismo Cancún",
          url: TURISMO,
          kind: "VERIFIED_PUBLIC",
        },
      ],
      verificationMethod:
        "Manual IHG browser confirmation logged in Admin; customer PDF uses only reachable evidence URLs.",
      dependencies: ["IHG digital/CDN security owner"],
      notes: "403 is a technical observation, not proof of a public outage.",
    };
  }

  const enrich = JSON.parse(fs.readFileSync(ENRICHMENT, "utf8")).hotels
    .adp_voco_cancun_zona_hotelera;
  payload = appendDigitalAuthorityActions(
    payload,
    {
      propertyId: "adp_voco_cancun_zona_hotelera",
      hotelName: payload.property.name,
      officialWebsite: payload.property.website,
      ...enrich,
      paidProviderUsed: false,
      paidResearchUsd: 0,
      researchMode: "PUBLIC_PARTIAL",
    },
    { metaPaidResearchAuthorized: false }
  );

  fs.writeFileSync(PAYLOAD, `${JSON.stringify(payload, null, 2)}\n`);

  const slug = "voco-cancun-zona-hotelera";
  const outDir = path.join(
    ROOT,
    "reports/dealality-ai-visibility-audit-action-plan-pilot-v1",
    slug,
    "pdf-staged"
  );
  fs.mkdirSync(outDir, { recursive: true });
  const filename = buildAiVisibilityAuditPdfFilename({
    hotelName: payload.property.name,
    reportMonthLabel: "NOVEMBER2026",
  });
  const outPath = path.join(outDir, filename);

  const priorMeta = getAuditMeta("adp_voco_cancun_zona_hotelera") || {};
  const pipeline = await runMandatoryAvaEvidenceStages(payload, {
    dryRun: true,
    persistCache: true,
    forcePdfRegenerate: true,
    generatePdfFn: async ({ payload: p }) => {
      await generateAiVisibilityAuditPdfV1({ payload: p, outPath, pagesDir: null });
      return { buffer: fs.readFileSync(outPath), path: outPath };
    },
  });

  payload = pipeline.result.payload;
  fs.writeFileSync(PAYLOAD, `${JSON.stringify(payload, null, 2)}\n`);
  const pdfBuf = fs.readFileSync(outPath);
  const pdfSha = crypto.createHash("sha256").update(pdfBuf).digest("hex");
  const ihgInPdf = pdfBuf.toString("latin1").includes("ihg.com/voco");
  const annotations = (
    await import(
      "../lib/ai-visibility-audit-action-plan/pdf-hyperlink-annotations-v1.js"
    )
  ).extractPdfUriAnnotationsFromBuffer(pdfBuf);
  const ihgClickable = annotations.uniqueUrls.filter((u) => /ihg\.com/i.test(u));

  const gatePass =
    pipeline.result.certification?.gate?.pass === true && ihgClickable.length === 0;
  const versionId = `ava_v1_voco_ihg403_${Date.now().toString(36)}`;
  const staged = stageAvaPdf({
    propertyId: "adp_voco_cancun_zona_hotelera",
    generatedAbsPath: outPath,
    versionId,
    filename,
  });
  saveAuditVersion("adp_voco_cancun_zona_hotelera", versionId, payload, {
    status: gatePass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
    metaStatus: gatePass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
    forceStatus: true,
    kind: "REPORT_VERSION",
    pdfRelativePath: staged.pdfRelativePath,
    researchCostUsd: 0,
    validation: {
      enhancement: "voco-ihg-403-remediation-v1",
      evidenceHyperlinkGatePass: gatePass,
      founderApprovedForCustomer: priorMeta.founderApprovedForCustomer === true,
      customerReadyBlocked: true,
      pdfSha256: staged.sha256,
      ihgClickableCount: ihgClickable.length,
    },
  });
  upsertAuditMeta("adp_voco_cancun_zona_hotelera", {
    status: gatePass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
    forceStatus: true,
    pdfRelativePath: staged.pdfRelativePath,
    pdfSha256: pdfSha,
    founderApprovedForCustomer: priorMeta.founderApprovedForCustomer === true,
    evidencePipelineStatus: pipeline.result.pipelineStatus,
    evidenceHyperlinkCertification: {
      ...(priorMeta.evidenceHyperlinkCertification || {}),
      lastRun: summarizeCertificationForAdmin(pipeline.result.certification),
    },
  });

  console.log(
    JSON.stringify(
      {
        gatePass,
        status: gatePass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
        pdfSha256: pdfSha,
        ihgClickable,
        ihgInPdfBytes: ihgInPdf,
        reasons: pipeline.result.certification?.gate?.reasons || [],
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
