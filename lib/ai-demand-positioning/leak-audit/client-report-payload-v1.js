/**
 * Strip internal fields for client-facing leak audit report payload.
 * Never expose full prompts, production hotel IDs, Airtable IDs, or system logs.
 */

import { WHO_DOES_THE_WORK } from "./report-generator-v1.js";

const FORBIDDEN_CLIENT_KEYS = new Set([
  "fullPromptText",
  "promptText",
  "_internalFullPromptText",
  "errorLog",
  "matchedHotelId",
  "censusRecordId",
  "censusRecordID",
  "airtableId",
  "airtableRecordId",
  "productionHotelId",
  "propertyId",
  "internalSourceTable",
  "scoringCode",
  "rawSystemLog",
  "matchMeta",
]);

const FORBIDDEN_BLOB_PATTERNS = [
  /fullprompttext/i,
  /_internalfullprompttext/i,
  /internal —/i,
  /\bapp[a-z0-9]{14}\b/i, // Airtable base ids
  /\brec[a-z0-9]{14}\b/i, // Airtable record ids
  /\badp_[a-z0-9_]+/i, // production ADP property ids
  /hotel property census/i,
  /adp monitoring periods/i,
  /ai demand positioning - published reports/i,
  /brand explorer/i,
  /operator explorer/i,
];

export function buildClientSafeReportPayload({ report, request, run }) {
  if (!report) return null;

  const payload = {
    productName: "AI Demand Leak Audit",
    productFamily: "Dealality AI Demand Positioning",
    layoutMode: report.layoutMode || "three_page_v1",
    title: `AI Demand Leak Audit — ${report.hotelName || request?.hotelName || "Hotel"}`,
    confidentialLabel: "Confidential — recipient only",
    limitedDiagnosticDisclaimer:
      "This is a limited diagnostic, not a full ADP monitoring report. Results are based on a controlled sample of monitored AI responses. AI outputs can vary by provider, timing, prompt framing, and available source signals.",
    coverDisclaimer:
      report.coverDisclaimer ||
      "This AI Demand Leak Audit is a limited diagnostic. It uses the same reading logic as Dealality AI Demand Positioning, but is not a full monthly monitoring report. Findings are inferred from monitored AI answers and do not prove causation or replace hotel commercial judgment.",
    hotelName: report.hotelName || request?.hotelName || "",
    runDate: report.runDate || run?.runDate || null,
    providersUsed: report.providersUsed || run?.providersUsed || [],
    reportStatus: report.reportStatus,
    reportType: report.reportType || "Limited Diagnostic",
    bottomLineSummary: report.bottomLineSummary,
    biggestDemandLeak: report.biggestDemandLeak,
    mainCompetitorShowingUpInstead: report.mainCompetitorShowingUpInstead,
    competitorDisplacement: Array.isArray(report.competitorDisplacement)
      ? report.competitorDisplacement.map((c) => ({
          competitorName: c.competitorName,
          demandSegment: c.demandSegment,
          displacementCount: Number(c.displacementCount) || 0,
          evidenceExcerpt: String(c.evidenceExcerpt || "").slice(0, 220),
        }))
      : [],
    likelyReason: report.likelyReason,
    firstFix: report.firstFix,
    secondFix: report.secondFix,
    thirdFix: report.thirdFix,
    fixes: Array.isArray(report.fixes)
      ? report.fixes.map((f) => ({
          title: f.title,
          whatToUpdate: f.whatToUpdate,
          whereToUpdate: f.whereToUpdate,
          whoNeedsToApprove: f.whoNeedsToApprove,
          dealalityCanPrepare: f.dealalityCanPrepare || [],
          hotelMustConfirm: f.hotelMustConfirm || [],
          whyThisMatters: f.whyThisMatters,
          targetSources: f.targetSources,
          expectedNextMonitoringCheck: f.expectedNextMonitoringCheck,
          summary: f.summary,
        }))
      : [],
    whoDoesTheWork: report.whoDoesTheWork || WHO_DOES_THE_WORK,
    recommendedNextStep: report.recommendedNextStep,
    executiveSummary: report.executiveSummary || null,
    executiveSignals: report.executiveSignals || null,
    demandAreaToReview: report.demandAreaToReview || report.inferredDemandLeak || null,
    inferredDemandLeak: report.inferredDemandLeak || null,
    supportingEvidence: report.supportingEvidence || null,
    evidenceLibrary: report.evidenceLibrary || null,
    scenariosMonitored: report.scenariosMonitored || null,
    howToRead: report.howToRead || null,
    competitorDisplacementRank: report.competitorDisplacementRank || null,
    hotelLocation: report.hotelLocation || report.locationLabel || "",
    locationLabel: report.locationLabel || report.hotelLocation || "",
    coverMeta: report.coverMeta || null,
    sampleFindingCount: report.sampleFindingCount || null,
    shareToken: report.shareToken || null,
    // Public report id only — not internal Airtable / census / ADP property ids
    reportId: report.id,
  };

  for (const key of Object.keys(payload)) {
    if (FORBIDDEN_CLIENT_KEYS.has(key)) delete payload[key];
  }

  const safety = assertClientReportSafety(payload);
  if (!safety.ok) {
    const err = new Error(`client_report_safety_failed:${safety.failures.join(",")}`);
    err.failures = safety.failures;
    throw err;
  }

  return payload;
}

export function assertNoFullPromptInClientPayload(payload) {
  const text = JSON.stringify(payload || {});
  const bad =
    /fullPromptText|_internalFullPromptText|"promptText"|INTERNAL —/i.test(text) ||
    /proprietary prompt template/i.test(text);
  return { ok: !bad, bad };
}

/**
 * Full client-report safety gate used by tests and payload builder.
 */
export function assertClientReportSafety(payload) {
  const failures = [];
  const text = JSON.stringify(payload || {});
  const lower = text.toLowerCase();

  if (!payload) failures.push("missing_payload");
  if (!payload?.whoDoesTheWork) failures.push("missing_who_does_the_work");
  if (!/limited diagnostic/i.test(text)) failures.push("missing_limited_diagnostic_language");

  if (!assertNoFullPromptInClientPayload(payload).ok) {
    failures.push("full_prompt_text_present");
  }

  for (const re of FORBIDDEN_BLOB_PATTERNS) {
    if (re.test(text)) failures.push(`forbidden_pattern:${re}`);
  }

  // Production hotel / property ids must not appear as structured fields
  for (const key of [
    "matchedHotelId",
    "propertyId",
    "censusRecordId",
    "airtableId",
    "airtableRecordId",
  ]) {
    if (payload && Object.prototype.hasOwnProperty.call(payload, key)) {
      failures.push(`forbidden_field:${key}`);
    }
  }

  const causation = [
    /\bthis proves\b/i,
    /\bai is penalizing\b/i,
    /\bfixing this will increase visibility\b/i,
    /\bthis will improve ranking\b/i,
  ];
  for (const re of causation) {
    if (re.test(text)) failures.push(`unsupported_causation:${re}`);
  }

  if (payload?.whoDoesTheWork) {
    const w = payload.whoDoesTheWork;
    if (!Array.isArray(w.dealalityPrepares) || !w.dealalityPrepares.length) {
      failures.push("who_does_work_missing_dealality_prepares");
    }
    if (!Array.isArray(w.hotelApproves) || !w.hotelApproves.length) {
      failures.push("who_does_work_missing_hotel_approves");
    }
  }

  // Prefer at least one competitor block or explicit no-competitor sentence
  if (
    (!payload?.competitorDisplacement || payload.competitorDisplacement.length === 0) &&
    !/no single dominant competitor/i.test(String(payload?.mainCompetitorShowingUpInstead || ""))
  ) {
    // soft: still require competitor section text
    if (!payload?.mainCompetitorShowingUpInstead) {
      failures.push("missing_competitor_section");
    }
  }

  if (!payload?.fixes || payload.fixes.length < 3) {
    if (!payload?.firstFix || !payload?.secondFix || !payload?.thirdFix) {
      failures.push("missing_three_fixes");
    }
  }

  void lower;
  return { ok: failures.length === 0, failures };
}
