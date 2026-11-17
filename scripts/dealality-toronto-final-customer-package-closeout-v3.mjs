#!/usr/bin/env node
/**
 * Toronto Final Customer Package Closeout V3
 * Derived-payload / wording only. No providers, no new periods.
 *
 *   --dry-run | --apply [--regen-pdf]
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";

import { loadPeriod, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import {
  loadPublishedManifest,
  loadPublishedReport,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import { buildExecutiveReadWithUx } from "../lib/ai-demand-positioning/customer/executive-read-v2.js";
import { composeExecutiveReadV3 } from "../lib/ai-demand-positioning/executive-read-v3/compose-executive-read-v3.js";
import { COMPOSITION_V3_STATUS } from "../lib/ai-demand-positioning/governance/adp-executive-read-composition-v3.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const CORE =
  "adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b";
const LOYALTY =
  "adp_period_adp_radisson_blu_toronto_downtown_20261104114752_308ba1";
const PR_V3 =
  "adp_mr_radisson_blu_toronto_dow_2026-11_v3";
const PR_V4 =
  "adp_mr_radisson_blu_toronto_dow_2026-11_v4";
const AVA_PAYLOAD = path.join(
  ROOT,
  "lib/ai-visibility-audit-action-plan/payloads/radisson-blu-toronto-downtown-v1.json"
);
const OUT = path.join(
  ROOT,
  "reports/adp/radisson-blu-toronto-final-customer-package-closeout-v3"
);

const args = process.argv.slice(2);
const APPLY = args.includes("--apply");
const REGEN = args.includes("--regen-pdf");

fs.mkdirSync(OUT, { recursive: true });
const writeJson = (p, o) => {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(o, null, 2)}\n`);
};
const writeText = (p, s) => {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, s.endsWith("\n") ? s : `${s}\n`);
};
const sha256 = (buf) => crypto.createHash("sha256").update(buf).digest("hex");

const OUTDOOR_SAFE =
  "Outdoor Event Space remains weakly represented in the monitored corpus. This overlaps some scenarios where the hotel is absent, but the monitoring does not establish that this attribute caused the absence.";

const PRIORITY_SUMMARY =
  "Only 4 of 21 monitored attributes are consistently recognized (19.0% Reality Coverage). Not every gap deserves equal urgency. Monitor / validate representation first: Family-Friendly, City Views, Pet-Friendly, EV Charging, Choice Hotels Distribution, Near Convention Center. Ballroom remains NEEDS HOTEL CONFIRMATION because public meeting inventories conflict. Outdoor Event Space: MONITOR.";

function sanitizeEvidenceLanguage(s) {
  return String(s || "")
    .replace(
      /Related Reality Gap pressure includes Outdoor Event Space at 0% recognition, which may help explain why the hotel is not carried into those answers\.?/gi,
      OUTDOOR_SAFE
    )
    .replace(
      /Outdoor Event Space at 0% recognition, which may help explain why the hotel is not carried into those answers\.?/gi,
      OUTDOOR_SAFE
    )
    .replace(/may help explain why/gi, "may warrant review alongside")
    .replace(/helps explain why/gi, "may warrant review alongside")
    .replace(/repeatable substitution pattern/gi, "observed co-appearance/absence pattern")
    .replace(/substitution pattern/gi, "observed co-appearance/absence pattern")
    .replace(
      /may improve representation if source defects are confirmed/gi,
      "would create a clearer and more consistent public source record if a source defect is confirmed"
    )
    .replace(
      /Actionable now:\s*Family-Friendly;\s*City Views\.[^.]*\./gi,
      "Monitor / validate representation first: Family-Friendly, City Views, Pet-Friendly, EV Charging, Choice Hotels Distribution, Near Convention Center."
    )
    .replace(/AI Consideration is 29%/g, "AI Consideration is 33.2%")
    .replace(/appeared 29% of the time/g, "appeared 33.2% of the time")
    .replace(/in 29% of individual/g, "in 33.2% of individual")
    .replace(/consideration only 29%/g, "consideration only 33.2%")
    .replace(/"displayValue": "29%"/g, '"displayValue": "33.2%"')
    .replace(/displayValue":"29%"/g, 'displayValue":"33.2%"');
}

function walkSanitize(node) {
  if (node == null) return node;
  if (typeof node === "string") return sanitizeEvidenceLanguage(node);
  if (Array.isArray(node)) return node.map(walkSanitize);
  if (typeof node === "object") {
    const out = {};
    for (const [k, v] of Object.entries(node)) out[k] = walkSanitize(v);
    return out;
  }
  return node;
}

function ensureConsiderationAnchor(er) {
  const anchors = Array.isArray(er.numericAnchors) ? [...er.numericAnchors] : [];
  const has = anchors.some(
    (a) =>
      /consideration/i.test(a.metricId || "") &&
      String(a.displayValue).includes("33.2")
  );
  if (!has) {
    anchors.unshift({
      metricId: "executiveMetrics.considerationRate.rate",
      displayValue: "33.2%",
      section: "KEY_INSIGHT",
      reason: "Canonical AI Consideration",
      paritySource: "executiveMetrics.considerationRate.rate",
      removingWeakensInsight: true,
      anchorType: "severity_indicator",
    });
  } else {
    for (const a of anchors) {
      if (/consideration/i.test(a.metricId || "") && a.displayValue === "29%") {
        a.displayValue = "33.2%";
      }
    }
  }
  er.numericAnchors = anchors;
  if (er.compositionV3) {
    er.compositionV3.numericAnchors = anchors;
  }
  return er;
}

function rebuildExecutiveRead(published, period, profile) {
  let er = buildExecutiveReadWithUx(published, period, [], profile, {});
  const v3 = composeExecutiveReadV3(published, {
    propertyId: PROPERTY_ID,
    market: profile.market,
    distributionStatus: "CUSTOMER_PUBLISHED",
    zeroCodePath: true,
    allowNewEdition: true,
  });
  if (v3?.ok && COMPOSITION_V3_STATUS?.activated === true) {
    const beforeWriteup = er.writeup;
    const beforeUx = er.ux;
    er = {
      ...er,
      compositionVersion: v3.compositionVersion,
      compositionContract: v3.compositionContract,
      sections: v3.sections,
      numericAnchors: v3.numericAnchors,
      primaryIssueId: "scenario_presence_inconsistency",
      insightArchetype: v3.insightArchetype,
      evidenceTrace: v3.evidenceTrace,
      qualityGates: v3.qualityGates,
      sourceSnapshotHash: v3.sourceSnapshotHash,
      compositionHash: v3.compositionHash,
      compositionV3: { ...v3, activated: true, customerFacingDefault: true },
      writeup: beforeWriteup,
      ux: beforeUx,
    };
  }
  // Prefer entry constraint framing in keyInsight when composer overweighted displacement.
  const entryLead =
    "Radisson Blu Toronto Downtown’s primary constraint is inconsistent entry into AI answer sets (AI Consideration 33.2%; Scenario Presence 38.1%). Bisha Hotel appears in 24 scenarios where the hotel is absent — observed competitor context, not a proven cause. ";
  if (er.sections?.keyInsight) {
    er.sections.keyInsight = entryLead + OUTDOOR_SAFE;
  }
  if (er.compositionV3?.sections?.keyInsight) {
    er.compositionV3.sections.keyInsight = er.sections.keyInsight;
  }
  er = ensureConsiderationAnchor(walkSanitize(er));
  return er;
}

function writeExecutiveReadToWrappers(er) {
  const results = [];
  const classicPath = path.join(
    ROOT,
    "data/ai-demand-positioning/published",
    PROPERTY_ID,
    `report-${CORE}.json`
  );
  const durablePath = path.join(
    ROOT,
    "data/adp-durable-published/snapshots",
    PROPERTY_ID,
    `report-${CORE}.json`
  );
  for (const reportPath of [classicPath, durablePath]) {
    if (!fs.existsSync(reportPath)) {
      results.push({ reportPath, ok: false, error: "missing" });
      continue;
    }
    const raw = JSON.parse(fs.readFileSync(reportPath, "utf8"));
    const bak = `${reportPath}.bak-closeout-v3-${Date.now().toString(36)}`;
    fs.copyFileSync(reportPath, bak);
    if (raw.payload && typeof raw.payload === "object") {
      raw.payload.executiveRead = er;
      // Also sanitize nested narrative fields that still quote stale consideration.
      raw.payload = walkSanitize(raw.payload);
      raw.payload.executiveRead = er;
      raw.payload._adpTorontoCloseoutV3 = {
        contract: "TORONTO_FINAL_CUSTOMER_PACKAGE_CLOSEOUT_V3",
        repairedAt: new Date().toISOString(),
        repairedFields: ["executiveRead"],
        note: "Derived narrative rebuild only — raw observations unchanged",
      };
    } else {
      raw.executiveRead = er;
      Object.assign(raw, walkSanitize(raw));
      raw.executiveRead = er;
    }
    fs.writeFileSync(reportPath, `${JSON.stringify(raw, null, 2)}\n`);
    results.push({
      reportPath,
      ok: true,
      backup: bak,
      sha256: sha256(fs.readFileSync(reportPath)),
      has29: JSON.stringify(raw).includes("AI Consideration is 29%"),
      has332: JSON.stringify(er).includes("33.2%"),
    });
  }
  return results;
}

function patchPerformanceReview(v3) {
  let r = structuredClone(v3);
  r.reviewId = PR_V4;
  r.correctionVersion = "TORONTO_FINAL_CUSTOMER_PACKAGE_CLOSEOUT_V3";

  // Reality gap prioritization table already corrected in V2; keep MONITOR/VALIDATE.
  if (Array.isArray(r.realityGapPrioritization)) {
    r.realityGapPrioritization = r.realityGapPrioritization.map((row) => {
      if (row.attribute === "event_space_outdoor") {
        return { ...row, priority: "MONITOR", why: OUTDOOR_SAFE };
      }
      if (row.attribute === "family_friendly" || row.attribute === "city_views") {
        return {
          ...row,
          priority: "VALIDATE_SOURCE_REPRESENTATION",
          why: sanitizeEvidenceLanguage(
            row.why ||
              "Official representation exists; soft recognition alone is not a verified HIGH source defect."
          ),
        };
      }
      return { ...row, why: sanitizeEvidenceLanguage(row.why) };
    });
  }

  // Fix residual expansion / section insight copies
  r = walkSanitize(r);

  // Force priority summary blocks if present
  const forceSummary = (obj) => {
    if (!obj || typeof obj !== "object") return;
    for (const [k, v] of Object.entries(obj)) {
      if (typeof v === "string" && /Actionable now:\s*Family-Friendly/i.test(v)) {
        obj[k] = PRIORITY_SUMMARY;
      } else if (typeof v === "object") forceSummary(v);
    }
  };
  forceSummary(r);

  // Reinforce executive conclusion hierarchy
  const ex = r.executiveAssessment;
  if (ex?.expansion?.platformSections) {
    ex.expansion.platformSections.keyInsight = sanitizeEvidenceLanguage(
      ex.expansion.platformSections.keyInsight
    );
  }
  if (ex?.narrativeBlocks) {
    ex.narrativeBlocks.whatDataTellsUs = sanitizeEvidenceLanguage(
      `Bisha Hotel appears in 24 monitored scenarios where Radisson Blu Toronto Downtown is absent — observed competitor context, not a proven cause. Softest Scenario Capture is Celebration at 20.0%. ${OUTDOOR_SAFE}`
    );
    ex.narrativeBlocks.conclusion = sanitizeEvidenceLanguage(
      "Radisson Blu Toronto Downtown’s primary constraint is inconsistent entry into AI answer sets across relevant traveler needs — not competitor causation."
    );
  }

  return r;
}

function patchAva(payload) {
  const p = structuredClone(payload);
  p.correctionVersion = "TORONTO_FINAL_CUSTOMER_PACKAGE_CLOSEOUT_V3";

  const actions = p.actions || [];
  for (const a of actions) {
    if (a.actionId === "AVA-RBTD-001") {
      a.completionCriteria = {
        investigationCompleted:
          "Current restaurant/lounge operating status confirmed by hotel Operations.",
        correctionSubmitted:
          "Choice page updated or closure notice dated/removed; Booking.com dining copy aligned.",
        correctionVerifiedLive:
          "Selected priority OTAs checked for dining consistency; live verification evidence archived.",
        noChangeRequired:
          "Valid only if Operations documents current dining status already matches all priority channels with evidence retained.",
      };
      a.claimStatus = "VERIFIED_PROBLEM";
    }
    if (a.actionId === "AVA-RBTD-005") {
      a.whatWeFound =
        "Distinct public source facts need hotel confirmation before alignment: Choice (17 spaces / >10,000 sq ft / up to 400 guests); Cvent (~11,000 sq ft); DTHA listing (14 venues / 11,000 sq ft / up to 380 guests) with separate event messaging up to 200 guests. These are not a single compressed conflict — each source states different inventory figures.";
      a.completionCriteria = {
        investigationCompleted:
          "Hotel-approved meetings fact sheet completed (venue count, total meeting space, largest-room/maximum-capacity, meetings contact).",
        correctionSubmitted:
          "Choice listing aligned; Cvent listing aligned; DTHA corrected if necessary.",
        correctionVerifiedLive: "Live verification archived for Choice, Cvent, and DTHA as applicable.",
        noChangeRequired:
          "Valid only if hotel documents that every listed source already matches the approved fact sheet.",
      };
    }
    if (a.actionId === "AVA-RBTD-002" || /family|harbourfront|city-access/i.test(a.headline || "")) {
      a.claimType = "RECOMMENDED_INVESTIGATION";
      a.claimStatus = "NEEDS_INVESTIGATION";
      a.recommendedSteps = [
        "Confirm whether current official wording is sufficiently precise before making any change.",
        "If wording is already accurate, document no-change with evidence — do not prescribe a rewrite without a verified defect.",
        "Cascade only if a material precision gap is confirmed.",
      ];
      a.whatWeFound =
        "Official Choice copy already supports family/harbourfront/convention-access themes while related Reality Gap recognition remains soft. Soft recognition alone is not a verified platform defect.";
      a.completionCriteria = {
        investigationCompleted:
          "Hotel confirms whether current official family/harbourfront/city-access wording is sufficiently precise.",
        correctionSubmitted:
          "Update Choice only if a verified precision gap is confirmed; otherwise document no-change.",
        correctionVerifiedLive: "Decision and any live page verification archived.",
        noChangeRequired:
          "Preferred when official wording is already accurate and no platform defect was verified.",
      };
    }
    if (a.actionId === "AVA-RBTD-003" || /pet and EV/i.test(a.headline || "")) {
      a.claimType = "MONITOR";
      a.claimStatus = "MONITOR";
      a.recommendedSteps = [
        "Confirm pet and EV facts remain current on the Choice brand page.",
        "Spot-check priority channels for consistency.",
        "Do not make stronger corrective changes solely because Core recognition is 0%.",
      ];
      a.completionCriteria = {
        investigationCompleted: "Pet and EV facts confirmed current on Choice.",
        correctionSubmitted:
          "Priority-channel spot-check logged; corrections only if a real inconsistency is found.",
        correctionVerifiedLive: "Spot-check evidence archived.",
        noChangeRequired:
          "Preferred when Choice listings remain accurate and no OTA inconsistency is found.",
      };
    }
  }

  // Platform group label note in payload
  if (p.digitalPlatformReadinessV3) {
    p.digitalPlatformReadinessV3.additionalGroupTitle =
      "Additional platforms — partial assessment due to access restrictions";
    p.digitalPlatformReadinessV3.additionalGroupNote =
      "These rows have terminal PARTIAL_ASSESSMENT_ACCESS_RESTRICTED status and do not represent unfinished research.";
  }
  if (p.digitalContentReadiness?.executiveAssessment) {
    p.digitalContentReadiness.executiveAssessment = String(
      p.digitalContentReadiness.executiveAssessment
    ).replace(
      /Terminal classifications replace IDENTIFIED_NOT_ASSESSED placeholders\.?/gi,
      "Every applicable platform has a terminal classification, including PARTIAL_ASSESSMENT_ACCESS_RESTRICTED where authenticated field audit was blocked."
    );
  }

  // Distinct meetings facts in findings if present
  const blob = JSON.stringify(p);
  if (/compress|conflate/i.test(blob)) {
    /* noop */
  }
  return p;
}

async function regenAvaPdf(payload) {
  const { generateAiVisibilityAuditPdfV1 } = await import(
    "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs"
  );
  const { stageAvaPdf } = await import(
    "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js"
  );
  const { buildAiVisibilityAuditPdfFilename } = await import(
    "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js"
  );
  const { upsertAuditMeta, saveAuditVersion, getAuditMeta } = await import(
    "../lib/ai-visibility-audit-action-plan/audit-store-v1.js"
  );
  const { ensurePublishedPdfFrozen } = await import(
    "../lib/ai-visibility-audit-action-plan/ava-pdf-channels-v1.js"
  );

  const tmpDir = path.join(OUT, "_tmp-ava");
  fs.mkdirSync(tmpDir, { recursive: true });
  const tmp = path.join(tmpDir, "out.pdf");
  await generateAiVisibilityAuditPdfV1({ payload, outPath: tmp });
  const filename = buildAiVisibilityAuditPdfFilename({
    hotelName: payload.property.name,
    reportMonthLabel: payload.reporting.reportMonthLabel,
  });
  const versionId = `ava_toronto_closeout_v3_${Date.now().toString(36)}`;
  stageAvaPdf({
    propertyId: PROPERTY_ID,
    generatedAbsPath: tmp,
    versionId,
    filename,
  });
  const archiveDir = path.join(
    ROOT,
    "reports/ai-visibility-audit-action-plan/archive",
    PROPERTY_ID
  );
  fs.copyFileSync(tmp, path.join(archiveDir, "current.pdf"));
  fs.copyFileSync(tmp, path.join(archiveDir, filename));
  const buf = fs.readFileSync(tmp);
  const digest = sha256(buf);
  const rel = `reports/ai-visibility-audit-action-plan/archive/${PROPERTY_ID}/current.pdf`;
  saveAuditVersion(PROPERTY_ID, versionId, payload, {
    pdfRelativePath: rel,
    status: "AUDIT_VALIDATED",
    forceStatus: true,
  });
  upsertAuditMeta(PROPERTY_ID, {
    pdfRelativePath: rel,
    pdfSha256: digest,
    latestVersionId: versionId,
    pdfStatus: "AVAILABLE",
    status: "PUBLISHED",
    lifecycleStatus: "PUBLISHED",
    customerReady: true,
    customerPublished: true,
    publishedPdfRelativePath: rel,
    publishedPdfSha256: digest,
    publishedVersionId: `published_frozen_${digest.slice(0, 12)}`,
    publishedFrozenAt: new Date().toISOString(),
    corePeriodId: CORE,
    portfolioPeriodId: LOYALTY,
    forceStatus: true,
  });
  ensurePublishedPdfFrozen(PROPERTY_ID);
  fs.copyFileSync(tmp, path.join(OUT, filename));
  const durablePdf = path.join(ROOT, "data/ava-data/pdfs", PROPERTY_ID, "published.pdf");
  fs.mkdirSync(path.dirname(durablePdf), { recursive: true });
  fs.copyFileSync(tmp, durablePdf);
  fs.copyFileSync(tmp, path.join(path.dirname(durablePdf), "current.pdf"));
  const prodMeta = path.join(
    ROOT,
    "data/ai-visibility-audit-action-plan/production/store",
    PROPERTY_ID,
    "AUDIT_META.json"
  );
  if (fs.existsSync(prodMeta)) {
    const meta = getAuditMeta(PROPERTY_ID) || {};
    const prod = JSON.parse(fs.readFileSync(prodMeta, "utf8"));
    writeJson(prodMeta, {
      ...prod,
      ...meta,
      pdfRelativePath: rel,
      pdfSha256: digest,
      latestVersionId: versionId,
      status: "PUBLISHED",
      lifecycleStatus: "PUBLISHED",
      customerReady: true,
      customerPublished: true,
      publishedPdfRelativePath: rel,
      publishedPdfSha256: digest,
      updatedAt: new Date().toISOString(),
    });
  }
  return { filename, sha256: digest, versionId, bytes: buf.length };
}

async function regenPrPdf(review) {
  const reportingMonthKey = "2026-11";
  const dir = path.join(
    ROOT,
    "reports/ai-demand-positioning/monthly-review/archive",
    PROPERTY_ID,
    reportingMonthKey,
    PR_V4
  );
  fs.mkdirSync(dir, { recursive: true });
  writeJson(path.join(dir, "review.json"), review);
  const meta = {
    reviewId: PR_V4,
    propertyId: PROPERTY_ID,
    propertyName: "Radisson Blu Toronto Downtown",
    reportingMonth: "November 2026",
    reportingMonthKey,
    currentMonitoringDate: "2026-11-04",
    sourceCurrentPeriodId: CORE,
    versionNumber: 4,
    versionLabel: "V4",
    reviewStatus: "READY_FOR_REVIEW",
    generationStatus: "SUCCESS",
    generationReason: "TORONTO_FINAL_CUSTOMER_PACKAGE_CLOSEOUT_V3",
    pdfStatus: "PENDING",
    actionCount: (review.actionAgenda || []).length,
    generatedAt: new Date().toISOString(),
    isCurrent: true,
    supersedesReviewId: PR_V3,
    builderVersion: "ADP_MONTHLY_REVIEW_BUILDER_V1",
  };
  writeJson(path.join(dir, "metadata.json"), meta);

  const indexPath = path.join(
    ROOT,
    "reports/ai-demand-positioning/monthly-review/archive/index.json"
  );
  const index = JSON.parse(fs.readFileSync(indexPath, "utf8"));
  for (const row of index.reviews) {
    if (row.propertyId === PROPERTY_ID && row.reviewId !== PR_V4) {
      row.isCurrent = false;
      if (row.reviewStatus !== "CLIENT_ISSUED" && row.reviewStatus !== "ARCHIVED") {
        row.reviewStatus = "SUPERSEDED";
      }
    }
  }
  const i = index.reviews.findIndex((r) => r.reviewId === PR_V4);
  if (i >= 0) index.reviews[i] = { ...index.reviews[i], ...meta };
  else index.reviews.push(meta);
  index.updatedAt = new Date().toISOString();
  writeJson(indexPath, index);

  const { renderAndAttachMonthlyReviewPdfV1 } = await import(
    "../lib/ai-demand-positioning/monthly-review/admin/render-and-attach-monthly-review-pdf-v1.mjs"
  );
  const result = await renderAndAttachMonthlyReviewPdfV1({
    reviewId: PR_V4,
    propertyId: PROPERTY_ID,
    baseUrl: process.env.ADP_PDF_BASE_URL || "http://127.0.0.1:8080",
  });
  const pdfPath = path.join(dir, "report.pdf");
  const named = path.join(
    OUT,
    "Dealality - Radisson Blu Toronto Downtown - AI Demand Performance Review - NOVEMBER2026.pdf"
  );
  if (fs.existsSync(pdfPath)) fs.copyFileSync(pdfPath, named);
  return { reviewId: PR_V4, result, named, pdfExists: fs.existsSync(pdfPath) };
}

async function main() {
  const profile = loadPropertyProfile(PROPERTY_ID);
  const period = loadPeriod(CORE);
  const published = loadPublishedReport(PROPERTY_ID);
  const manifest = loadPublishedManifest(PROPERTY_ID);

  const er = rebuildExecutiveRead(published, period, profile);
  const erBlob = JSON.stringify(er);
  const erQa = {
    has29: /AI Consideration is 29%|appeared 29%|"29%"/.test(erBlob),
    has332: erBlob.includes("33.2%"),
    hasExplainWhy: /help explain why|helps explain why/i.test(erBlob),
    hasSubstitution: /substitution pattern/i.test(erBlob),
    keyInsight: er.sections?.keyInsight || null,
  };
  writeJson(path.join(OUT, "EXECUTIVE_READ_REBUILT.json"), { erQa, er });

  const prBefore = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "reports/ai-demand-positioning/monthly-review/archive",
        PROPERTY_ID,
        "2026-11",
        PR_V3,
        "review.json"
      ),
      "utf8"
    )
  );
  const prAfter = patchPerformanceReview(prBefore);
  const avaBefore = JSON.parse(fs.readFileSync(AVA_PAYLOAD, "utf8"));
  const avaAfter = patchAva(avaBefore);

  writeJson(path.join(OUT, "performance-review-closeout-v3.json"), prAfter);
  writeJson(path.join(OUT, "ava-payload-closeout-v3.json"), avaAfter);

  const summary = {
    apply: APPLY,
    regen: REGEN,
    erQa,
    prHasExplain: /help explain why/i.test(JSON.stringify(prAfter)),
    prHasActionableNow: /Actionable now:\s*Family-Friendly/i.test(
      JSON.stringify(prAfter)
    ),
    avaAction1DiningOnly: !/pets\/EV|family\/harbourfront|meetings inventory/i.test(
      JSON.stringify(avaAfter.actions?.find((a) => a.actionId === "AVA-RBTD-001")?.completionCriteria || {})
    ),
  };
  writeJson(path.join(OUT, "APPLY_SUMMARY.json"), summary);

  if (!APPLY) {
    console.log(JSON.stringify({ ok: true, dryRun: true, summary }, null, 2));
    return;
  }

  const publishWrites = writeExecutiveReadToWrappers(er);
  writeJson(AVA_PAYLOAD, avaAfter);

  let avaPdf = null;
  let prPdf = null;
  if (REGEN) {
    avaPdf = await regenAvaPdf(avaAfter);
    prPdf = await regenPrPdf(prAfter);
  } else {
    const dir = path.join(
      ROOT,
      "reports/ai-demand-positioning/monthly-review/archive",
      PROPERTY_ID,
      "2026-11",
      PR_V4
    );
    fs.mkdirSync(dir, { recursive: true });
    writeJson(path.join(dir, "review.json"), prAfter);
  }

  // Re-read published for QA
  const after = loadPublishedReport(PROPERTY_ID);
  const afterBlob = JSON.stringify(after.executiveRead || {});
  writeJson(path.join(OUT, "APPLY_SUMMARY.json"), {
    ...summary,
    publishWrites,
    avaPdf,
    prPdf,
    afterEr: {
      has29: /AI Consideration is 29%|appeared 29%/.test(afterBlob),
      has332: afterBlob.includes("33.2%"),
      rate: after.executiveMetrics?.considerationRate?.rate,
      manifestLatest: manifest?.latestPeriodId || null,
    },
  });
  console.log(
    JSON.stringify(
      {
        ok: true,
        applied: true,
        afterErHas29: /AI Consideration is 29%/.test(afterBlob),
        afterErHas332: afterBlob.includes("33.2%"),
        avaPdf,
        prPdf,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
