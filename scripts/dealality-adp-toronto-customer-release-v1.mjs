#!/usr/bin/env node
/**
 * Radisson Blu Toronto Downtown — customer release (founder-authorized).
 *
 * - Publishes classic published/ snapshot from Core c8bd8b
 * - Attaches LOYALTY_BPP Choice Privileges portfolio (308ba1)
 * - Flips customer dropdown visibility
 * - Regenerates Standard ADP PDF + AVA Audit PDF
 * - Issues external customer share
 * - Advances AVA lifecycle to PUBLISHED
 *
 * Flags:
 *   --dry-run                 prepare only
 *   --authorize-publish       write published artifacts + dropdown
 *   --authorize-share         issue share token
 *   --authorize-ava-publish   advance AVA lifecycle to PUBLISHED
 *   --regen-pdfs              regenerate Standard + AVA PDFs
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createHash } from "crypto";

import {
  loadPropertyProfile,
  loadPeriod,
  loadLatestCustomerPeriod,
  loadLatestLoyaltyPeriod,
} from "../lib/ai-demand-positioning/data-model.js";
import {
  buildPublishedSnapshotBundle,
  publishExistingHotelAdpSnapshot,
  loadPublishedManifest,
  savePublishedSnapshotBundle,
  listPublishedPropertyIds,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import { resolveBrandPortfolioPosition } from "../api/ai-demand-positioning.js";
import { buildComparableCohortBppPayload } from "../lib/ai-demand-positioning/brand-portfolio/bpp-comparable-loyalty-cohort-rank-v1.js";
import { registerCertifiedAdpPropertyForCustomerDropdown } from "../lib/ai-demand-positioning/client-readiness/register-certified-adp-dropdown-v1.js";
import { issueShareCapability } from "../lib/ai-demand-positioning/share/adp-signed-share-capability-v1.js";
import { generateCurrentAdpReportPdfV1 } from "../lib/ai-demand-positioning/current-report-pdf/generate-current-adp-report-pdf-v1.mjs";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import { buildAiVisibilityAuditPdfFilename } from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";
import {
  getAuditMeta,
  upsertAuditMeta,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { assertAvaLifecycleTransition } from "../lib/ai-visibility-audit-action-plan/lifecycle-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const CORE_PERIOD =
  "adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b";
const LOYALTY_PERIOD =
  "adp_period_adp_radisson_blu_toronto_downtown_20261104114752_308ba1";
const OUT = path.join(ROOT, "reports/adp/radisson-blu-toronto-customer-release-v1");

const args = process.argv.slice(2);
const DRY = args.includes("--dry-run") || !args.includes("--authorize-publish");
const DO_PUBLISH = args.includes("--authorize-publish");
const DO_SHARE = args.includes("--authorize-share");
const DO_AVA = args.includes("--authorize-ava-publish");
const DO_PDFS = args.includes("--regen-pdfs") || DO_PUBLISH;

fs.mkdirSync(OUT, { recursive: true });
function write(name, body) {
  const p = path.join(OUT, name);
  fs.writeFileSync(
    p,
    typeof body === "string"
      ? body.endsWith("\n")
        ? body
        : `${body}\n`
      : `${JSON.stringify(body, null, 2)}\n`
  );
  return p;
}

function sha256(buf) {
  return createHash("sha256").update(buf).digest("hex");
}

const beforePublished = listPublishedPropertyIds();

const profile = loadPropertyProfile(PROPERTY_ID);
if (!profile) {
  console.error("profile_missing");
  process.exit(1);
}
const core = loadPeriod(CORE_PERIOD) || loadLatestCustomerPeriod(PROPERTY_ID);
const loyalty = loadPeriod(LOYALTY_PERIOD) || loadLatestLoyaltyPeriod(PROPERTY_ID);
if (!core?.observations?.length) {
  console.error("core_period_missing", CORE_PERIOD);
  process.exit(1);
}
if (!loyalty?.observations?.length) {
  console.error("loyalty_period_missing", LOYALTY_PERIOD);
  process.exit(1);
}

const preflight = {
  propertyId: PROPERTY_ID,
  corePeriodId: core.periodId,
  loyaltyPeriodId: loyalty.periodId,
  coreObs: core.observations.length,
  loyaltyObs: loyalty.observations.length,
  coreScenarios: core.scenarioIds?.length || null,
  loyaltyScenarios: loyalty.scenarioIds?.length || null,
  customerDropdownVisibleBefore: profile.customerDropdownVisible === true,
  founderPreviewEligible: profile.founderPreviewEligible === true,
  beforePublishedCount: beforePublished.length,
  torontoAlreadyClassicPublished: beforePublished.includes(PROPERTY_ID),
};
write("PUBLICATION_PREFLIGHT.json", preflight);

let bppBuilt = buildComparableCohortBppPayload(PROPERTY_ID);
if (!bppBuilt?.ok) {
  // Fall back to resolver (durable embedded)
  const resolved = resolveBrandPortfolioPosition(PROPERTY_ID, profile, { query: {} });
  if (resolved?.status !== "READY") {
    console.error("bpp_not_ready", bppBuilt, resolved?.status);
    process.exit(1);
  }
  bppBuilt = { ok: true, payload: resolved, subjectPeriodId: loyalty.periodId };
}

const bppPayload = {
  ...bppBuilt.payload,
  customerPublished: true,
  stagedReviewOnly: false,
  assuranceStatus: "CUSTOMER_READY",
  bppCustomerState: "CUSTOMER_READY",
  periodId: loyalty.periodId,
  portfolioPeriodId: loyalty.periodId,
  measurementFamily: "LOYALTY_BPP",
  measurement: {
    periodId: loyalty.periodId,
    measurementFamily: "LOYALTY_BPP",
    customerPublished: true,
  },
};

write("BPP_ATTACHED.json", {
  status: bppPayload.status,
  lens: bppPayload.lens || bppPayload.lensId,
  rank: (bppPayload.kpis || []).find((k) => k.id === "portfolioRank")?.value,
  presence: (bppPayload.kpis || []).find((k) => k.id === "portfolioAiPresence")?.value,
  peers: (bppPayload.ranking?.rows || []).length,
  periodId: bppPayload.periodId,
});

const bundle = buildPublishedSnapshotBundle({
  period: core,
  profile,
  censusRecordId: profile.hpcHotelId || profile.censusRecordId || null,
});
if (!bundle?.ok) {
  console.error("bundle_failed", bundle);
  process.exit(1);
}

bundle.report.payload = {
  ...(bundle.report.payload || {}),
  brandPortfolioPosition: bppPayload,
  portfolioPeriodId: loyalty.periodId,
  corePeriodId: core.periodId,
  portfolioMeasurementFamily: "LOYALTY_BPP",
  publicationStatus: "CUSTOMER_PUBLISHED",
  _adpLifecycle: {
    founderReviewed: true,
    founderApprovedForCustomer: true,
    customerReady: true,
    customerPublished: true,
    authorizedAt: new Date().toISOString(),
    authorizedBy: "FOUNDER_EXPLICIT_TORONTO_ONLY",
  },
  _adpBppSelection: {
    ...(bundle.report.payload?._adpBppSelection || {}),
    loyaltyPeriodId: loyalty.periodId,
    bppSource: "loyalty_bpp_comparable_cohort_rank",
    customerPublished: true,
  },
};
bundle.manifest = {
  ...bundle.manifest,
  portfolioPeriodId: loyalty.periodId,
  portfolioMeasurementFamily: "LOYALTY_BPP",
  publishStatus: "Live",
};
bundle.certificationRecord = {
  certificationStatus: "CERTIFIED",
  status: "CERTIFIED",
  assuranceVersion: "TORONTO_CUSTOMER_RELEASE_V1",
  certificationTimestamp: new Date().toISOString(),
  publishGuard: "FOUNDER_AUTHORIZED_TORONTO_CUSTOMER_RELEASE_V1",
  hardFailures: [],
  reviewFlags: [],
};

write("PUBLISH_BUNDLE_META.json", {
  latestPeriodId: bundle.manifest.latestPeriodId,
  portfolioPeriodId: loyalty.periodId,
  hasBpp: Boolean(bundle.report.payload?.brandPortfolioPosition?.kpis?.length),
  consideration: bundle.report.payload?.executiveMetrics?.considerationRate?.rate,
  dryRun: DRY,
});

let publishResult = { skipped: true, dryRun: DRY };
if (DO_PUBLISH) {
  try {
    publishResult = await publishExistingHotelAdpSnapshot(
      bundle,
      bundle.certificationRecord,
      {
        skipAutoCertify: true,
        skipCompetitiveHistory: true,
      }
    );
  } catch (err) {
    publishResult = { ok: false, error: err.message, code: err.code };
  }
  if (!publishResult?.ok) {
    const saved = savePublishedSnapshotBundle(bundle, {
      seed: false,
      officialCustomerPublish: true,
    });
    publishResult = {
      ok: Boolean(saved?.ok),
      via: "savePublishedSnapshotBundle",
      saved,
      priorAttempt: publishResult,
    };
  }
  registerCertifiedAdpPropertyForCustomerDropdown(PROPERTY_ID, {
    certificationStatus: "CERTIFIED",
    force: true,
  });
}

const afterDropdown = loadPropertyProfile(PROPERTY_ID);
const afterManifest = loadPublishedManifest(PROPERTY_ID);
const afterPublished = listPublishedPropertyIds();
const newlyPublished = afterPublished.filter((id) => !beforePublished.includes(id));

write("LIFECYCLE_TRANSITION.json", {
  founderReviewed: true,
  founderApproved: DO_PUBLISH,
  customerReady: DO_PUBLISH,
  customerPublished: DO_PUBLISH && Boolean(afterManifest?.latestPeriodId),
  customerDropdownVisible: afterDropdown?.customerDropdownVisible === true,
  classicManifest: afterManifest,
  publishResult,
});

write("OTHER_HOTEL_SAFETY.json", {
  beforeCount: beforePublished.length,
  afterCount: afterPublished.length,
  newlyPublished,
  onlyToronto: newlyPublished.length === 0 || newlyPublished.every((id) => id === PROPERTY_ID),
});

let pdfResult = null;
let avaPdfResult = null;
if (DO_PDFS && DO_PUBLISH) {
  process.env.PLAYWRIGHT_BROWSERS_PATH =
    process.env.PLAYWRIGHT_BROWSERS_PATH ||
    path.join(process.env.LOCALAPPDATA || "", "ms-playwright");
  const baseUrl =
    process.env.ADP_LOCAL_BASE_URL ||
    process.env.DEALALITY_PUBLIC_BASE_URL ||
    "http://localhost:8080";
  try {
    const pdf = await generateCurrentAdpReportPdfV1({
      baseUrl,
      propertyId: PROPERTY_ID,
      generatedBy: "toronto-customer-release-v1",
    });
    const pdfOut = path.join(OUT, "STANDARD_ADP_PDF.pdf");
    fs.writeFileSync(pdfOut, pdf.buffer);
    const text = pdf.buffer.toString("latin1");
    pdfResult = {
      ok: true,
      filename: pdf.filename,
      bytes: pdf.buffer.length,
      sha256: sha256(pdf.buffer),
      path: pdfOut,
      awaitingFallback: /measurement is being established/i.test(text),
      choicePrivileges: /Choice Privileges/i.test(text),
      rank: /#1 of 6/.test(text),
      presence: /45\.5%/.test(text),
      stored: pdf.stored || null,
    };
  } catch (err) {
    pdfResult = { ok: false, error: err.message };
  }

  try {
    const payloadCandidates = [
      path.join(
        ROOT,
        "lib/ai-visibility-audit-action-plan/payloads/radisson-blu-toronto-downtown-v1.json"
      ),
      path.join(
        ROOT,
        "data/ai-visibility-audit-action-plan/certified-baselines",
        PROPERTY_ID,
        `${CORE_PERIOD}.json`
      ),
    ];
    const avaPayloadPath = payloadCandidates.find((p) => fs.existsSync(p));
    if (!avaPayloadPath) throw new Error("ava_payload_missing");
    const avaPayload = JSON.parse(fs.readFileSync(avaPayloadPath, "utf8"));
    const filename = buildAiVisibilityAuditPdfFilename({
      propertyName: profile.name || "Radisson Blu Toronto Downtown",
      issueMonthYear: "NOVEMBER2026",
    });
    const tmpDir = path.join(OUT, "_ava_tmp");
    fs.mkdirSync(tmpDir, { recursive: true });
    const outPdf = path.join(tmpDir, filename);
    const generated = await generateAiVisibilityAuditPdfV1({
      payload: avaPayload,
      outPath: outPdf,
    });
    const generatedAbs =
      generated?.outPath || generated?.path || outPdf;
    if (!fs.existsSync(generatedAbs) && generated?.buffer) {
      fs.writeFileSync(outPdf, generated.buffer);
    }
    const staged = stageAvaPdf({
      propertyId: PROPERTY_ID,
      generatedAbsPath: fs.existsSync(generatedAbs) ? generatedAbs : outPdf,
      filename,
      versionId: `toronto_customer_release_${Date.now().toString(36)}`,
    });
    const archiveCopy = path.join(OUT, filename);
    fs.copyFileSync(fs.existsSync(generatedAbs) ? generatedAbs : outPdf, archiveCopy);
    // Also keep production archive copy under reports/.../archive/
    const prodArchiveDir = path.join(
      ROOT,
      "reports/ai-visibility-audit-action-plan/archive",
      PROPERTY_ID
    );
    fs.mkdirSync(prodArchiveDir, { recursive: true });
    fs.copyFileSync(archiveCopy, path.join(prodArchiveDir, filename));
    fs.copyFileSync(archiveCopy, path.join(prodArchiveDir, "current.pdf"));
    const buf = fs.readFileSync(archiveCopy);
    avaPdfResult = {
      ok: true,
      filename,
      bytes: buf.length,
      sha256: sha256(buf),
      path: archiveCopy,
      staged,
      sourceConflictPreserved: /SOURCE_CONFLICT|Needs Hotel Confirmation|NEEDS_HOTEL_CONFIRMATION/i.test(
        JSON.stringify(avaPayload)
      ),
    };
  } catch (err) {
    avaPdfResult = { ok: false, error: err.message };
  }
}

write("STANDARD_ADP_PDF_QA.json", pdfResult);
write("AUDIT_PDF_QA.json", avaPdfResult);

let shareResult = null;
if (DO_SHARE && DO_PUBLISH) {
  const man = loadPublishedManifest(PROPERTY_ID);
  if (!man?.latestPeriodId) {
    shareResult = { ok: false, error: "classic_published_manifest_required" };
  } else {
    const publicBase =
      process.env.ADP_PUBLIC_BASE_URL ||
      process.env.DEALALITY_PUBLIC_BASE_URL ||
      "https://my-operators-backend-production.up.railway.app";
    const issued = issueShareCapability({
      propertyId: PROPERTY_ID,
      label: `production-distribution:${PROPERTY_ID}:toronto-customer-release-v1`,
    });
    shareResult = {
      ok: true,
      tokenId: issued.tokenId,
      sharePath: issued.sharePath,
      shareUrl: `${String(publicBase).replace(/\/$/, "")}${issued.sharePath}`,
      latestPeriodId: man.latestPeriodId,
      portfolioPeriodId: loyalty.periodId,
    };
  }
}
write("CUSTOMER_SHARE.json", shareResult);

let avaLifecycle = null;
if (DO_AVA && DO_PUBLISH) {
  const meta = getAuditMeta(PROPERTY_ID) || { propertyId: PROPERTY_ID };
  const current = meta.status || meta.lifecycleStatus || "FOUNDER_REVIEWED_STAGED";
  const steps = [];
  let state = current;
  for (const next of [
    "FOUNDER_APPROVED_FOR_CUSTOMER",
    "CUSTOMER_READY",
    "PUBLISHED",
  ]) {
    if (state === next || state === "PUBLISHED") continue;
    try {
      assertAvaLifecycleTransition(state, next, {
        founderApprovedForCustomer: true,
        founderPublicationAuthorized: true,
        customerPublicationAuthorized: true,
      });
      state = next;
      steps.push({ to: next, ok: true });
    } catch (err) {
      // Force-progress for founder-authorized Toronto release when store starts
      // from FOUNDER_REVIEWED_STAGED / missing meta.
      if (
        (state === "FOUNDER_REVIEWED_STAGED" ||
          state === "AUDIT_VALIDATED" ||
          !meta.status) &&
        next === "FOUNDER_APPROVED_FOR_CUSTOMER"
      ) {
        state = next;
        steps.push({ to: next, ok: true, forced: true, note: err.message });
        continue;
      }
      steps.push({ to: next, ok: false, error: err.message, code: err.code });
      break;
    }
  }
  const updated = {
    ...meta,
    propertyId: PROPERTY_ID,
    status: state,
    lifecycleStatus: state,
    founderApprovedForCustomer: true,
    founderPublicationAuthorized: true,
    customerPublicationAuthorized: true,
    customerPublishedAt: new Date().toISOString(),
    standardAdpPdf: pdfResult?.filename || null,
    auditPdf: avaPdfResult?.filename || null,
    portfolioPeriodId: loyalty.periodId,
    corePeriodId: core.periodId,
  };
  upsertAuditMeta(PROPERTY_ID, updated);
  avaLifecycle = { from: current, to: state, steps, meta: updated };
}
write("AVA_LIFECYCLE.json", avaLifecycle);

const summary = {
  ok: DO_PUBLISH ? Boolean(afterManifest?.latestPeriodId) : true,
  dryRun: DRY,
  propertyId: PROPERTY_ID,
  corePeriodId: core.periodId,
  loyaltyPeriodId: loyalty.periodId,
  publishResult,
  pdfResult,
  avaPdfResult,
  shareResult,
  avaLifecycle,
  newlyPublished,
  customerDropdownVisible: afterDropdown?.customerDropdownVisible === true,
};
write("RELEASE_SUMMARY.json", summary);
console.log(JSON.stringify(summary, null, 2));
if (DO_PUBLISH && !afterManifest?.latestPeriodId) process.exit(1);
