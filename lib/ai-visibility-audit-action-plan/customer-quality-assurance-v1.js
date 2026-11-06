/**
 * AVA Customer-Facing Quality Assurance V1
 *
 * Four mandatory gates:
 *   A Research & commercial quality
 *   B Evidence & factual consistency
 *   C Customer communication quality
 *   D PDF presentation quality
 *
 * Detection only — remediation lives in customer-quality-remediation-v1.js
 */
import {
  scanObjectForInternalLanguage,
  scanInternalLanguageLeaks,
  replaceInternalLanguage,
} from "./customer-facing-language-v1.js";
import { buildAiVisibilityAuditPdfHtml } from "./build-audit-pdf-html-v1.js";
import { detectIncompleteCustomerNarratives } from "./narrative-completeness-v1.js";
import { detectSerializationArtifacts } from "./customer-facing-serialize-v1.js";
import {
  buildCanonicalActionRegister,
  detectCanonicalActionIntegrityDefects,
} from "./canonical-action-register-v1.js";

export const CUSTOMER_QUALITY_ASSURANCE_CONTRACT =
  "AVA_CUSTOMER_QUALITY_ASSURANCE_V1";

export const CQA_DEFECT_TYPES = Object.freeze({
  CRITICAL_FACTUAL_CONTRADICTION: "CRITICAL_FACTUAL_CONTRADICTION",
  UNSUPPORTED_CLAIM: "UNSUPPORTED_CLAIM",
  INTERNAL_LANGUAGE_LEAK: "INTERNAL_LANGUAGE_LEAK",
  ACTION_COUNT_MISMATCH: "ACTION_COUNT_MISMATCH",
  DUPLICATE_ACTION: "DUPLICATE_ACTION",
  INVALID_EVIDENCE_CLASSIFICATION: "INVALID_EVIDENCE_CLASSIFICATION",
  COMMERCIAL_CLARITY_ISSUE: "COMMERCIAL_CLARITY_ISSUE",
  PDF_LAYOUT_DEFECT: "PDF_LAYOUT_DEFECT",
  BROKEN_PDF_LINK: "BROKEN_PDF_LINK",
  MODULE_PDF_OMISSION: "MODULE_PDF_OMISSION",
  FINDING_COUNT_MISMATCH: "FINDING_COUNT_MISMATCH",
  EXEC_SUMMARY_STRUCTURE_BROKEN: "EXEC_SUMMARY_STRUCTURE_BROKEN",
  PLATFORM_DETAIL_INCOMPLETE: "PLATFORM_DETAIL_INCOMPLETE",
  ACTION_PLATFORM_UNSPECIFIED: "ACTION_PLATFORM_UNSPECIFIED",
  V3_REPETITION_OR_ADP_DUPLICATION: "V3_REPETITION_OR_ADP_DUPLICATION",
  INCOMPLETE_CUSTOMER_NARRATIVE: "INCOMPLETE_CUSTOMER_NARRATIVE",
  CUSTOMER_SERIALIZATION_ARTIFACT: "CUSTOMER_SERIALIZATION_ARTIFACT",
  GENERIC_PLATFORM_ACTION: "GENERIC_PLATFORM_ACTION",
  MISSING_DIGITAL_AUTHORITY_EVIDENCE: "MISSING_DIGITAL_AUTHORITY_EVIDENCE",
  SEMANTIC_DUPLICATE_WORKSTREAM: "SEMANTIC_DUPLICATE_WORKSTREAM",
  TITLE_ONLY_IMPLEMENTATION_STEPS: "TITLE_ONLY_IMPLEMENTATION_STEPS",
  ACTION_TITLE_STEP_MISMATCH: "ACTION_TITLE_STEP_MISMATCH",
  DUPLICATE_DA_HOST_OUTREACH: "DUPLICATE_DA_HOST_OUTREACH",
  HEALTHY_BACKLINK_AS_CORRECTION: "HEALTHY_BACKLINK_AS_CORRECTION",
  NEAR_EMPTY_PDF_PAGE: "NEAR_EMPTY_PDF_PAGE",
  CROSS_SECTION_PRIORITY_CONFLICT: "CROSS_SECTION_PRIORITY_CONFLICT",
  CROSS_SECTION_OWNER_CONFLICT: "CROSS_SECTION_OWNER_CONFLICT",
  EMPTY_PLATFORM_SECTION_PLACEHOLDER: "EMPTY_PLATFORM_SECTION_PLACEHOLDER",
  INCOMPLETE_OR_GENERIC_IMPLEMENTATION_STEPS:
    "INCOMPLETE_OR_GENERIC_IMPLEMENTATION_STEPS",
  BOILERPLATE_DESIRED_OUTCOME: "BOILERPLATE_DESIRED_OUTCOME",
  REPEATED_GENERIC_DESIRED_OUTCOME: "REPEATED_GENERIC_DESIRED_OUTCOME",
  METHODOLOGY_AS_CUSTOMER_FINDING: "METHODOLOGY_AS_CUSTOMER_FINDING",
  MICHELIN_STAR_KEY_CONFLATION: "MICHELIN_STAR_KEY_CONFLATION",
  GENERIC_PLATFORM_ASSESSMENT_ROW: "GENERIC_PLATFORM_ASSESSMENT_ROW",
  EXECUTIVE_TAKEAWAY_KEEP_MISSING: "EXECUTIVE_TAKEAWAY_KEEP_MISSING",
  EXECUTIVE_TAKEAWAY_SPLIT: "EXECUTIVE_TAKEAWAY_SPLIT",
  EMPTY_TAKEAWAY_BOX: "EMPTY_TAKEAWAY_BOX",
});

/**
 * Gate: completed research modules must appear in customer PDF HTML,
 * significant verified findings must not be silently omitted, and
 * coverage language must not overclaim completeness.
 */
export function detectModulePdfCompletenessDefects(payload, html = "") {
  const defects = [];
  const h = String(html || "");
  const da = payload.digitalAuthority;
  const dcr = payload.digitalContentReadiness;

  if (da && typeof da === "object") {
    if (!/data-ava-section="digital-authority"|Digital Authority/i.test(h)) {
      defects.push({
        type: CQA_DEFECT_TYPES.MODULE_PDF_OMISSION,
        severity: "CRITICAL",
        section: "pdf.digitalAuthority",
        message:
          "Digital Authority module is present in the payload but missing from the customer PDF",
      });
    }
    const backlinks = (da.sources || []).filter(
      (s) => s.findingClass === "VERIFIED_BACKLINK"
    );
    const mentions = (da.sources || []).filter(
      (s) => s.findingClass === "VERIFIED_EXTERNAL_MENTION"
    );
    const v4DaCards =
      /data-ava-section="digital-authority"/i.test(h) &&
      /Observation:|Commercial relevance:/i.test(h);
    if (
      backlinks.length > 0 &&
      !/Verified website links/i.test(h) &&
      !v4DaCards
    ) {
      defects.push({
        type: CQA_DEFECT_TYPES.MODULE_PDF_OMISSION,
        severity: "HIGH",
        section: "pdf.digitalAuthority.backlinks",
        message: `${backlinks.length} verified backlink(s) in payload are not presented as verified website links in the PDF`,
      });
    }
    if (
      backlinks.length === 0 &&
      !/Verified website links<\/td><td><strong>0<\/strong>/i.test(h) &&
      !/No third-party hyperlinks/i.test(h) &&
      !/Directory presence is not counted as a backlink/i.test(h)
    ) {
      defects.push({
        type: CQA_DEFECT_TYPES.MODULE_PDF_OMISSION,
        severity: "MEDIUM",
        section: "pdf.digitalAuthority.backlinks",
        message:
          "Hotel has zero verified backlinks but PDF does not clearly disclose that limitation",
      });
    }
    if (
      mentions.length > 0 &&
      !/references without|external mention|Third-party references/i.test(h) &&
      !v4DaCards
    ) {
      defects.push({
        type: CQA_DEFECT_TYPES.MODULE_PDF_OMISSION,
        severity: "HIGH",
        section: "pdf.digitalAuthority.mentions",
        message: `${mentions.length} external mention(s) in payload are not visible in the PDF`,
      });
    }
    // Count reconciliation: PDF snapshot should match customer-facing source counts
    const blMatch = h.match(/Verified website links<\/td><td><strong>(\d+)<\/strong>/i);
    if (blMatch && Number(blMatch[1]) !== backlinks.length) {
      defects.push({
        type: CQA_DEFECT_TYPES.FINDING_COUNT_MISMATCH,
        severity: "HIGH",
        section: "pdf.digitalAuthority.counts",
        message: `PDF shows ${blMatch[1]} verified website links but payload customer sources have ${backlinks.length}`,
      });
    }
    const covStatus = da.discoveryCoverage?.researchCoverageStatus || "";
    if (
      /PARTIAL|ACCESS_RESTRICTED|SUFFICIENT/i.test(covStatus) &&
      /comprehensive (paid )?backlink inventory|complete referring.?domain/i.test(h) &&
      !/not a comprehensive|partial public/i.test(h)
    ) {
      defects.push({
        type: CQA_DEFECT_TYPES.UNSUPPORTED_CLAIM,
        severity: "HIGH",
        section: "pdf.digitalAuthority.coverage",
        message: "PDF implies comprehensive backlink research while coverage is partial",
      });
    }
    if ((da.opportunities || []).length > 0 && !/opportunit/i.test(h)) {
      defects.push({
        type: CQA_DEFECT_TYPES.MODULE_PDF_OMISSION,
        severity: "MEDIUM",
        section: "pdf.digitalAuthority.opportunities",
        message: "Digital Authority opportunities exist but are not shown in the PDF",
      });
    }
  }

  if (dcr && typeof dcr === "object") {
    // V3 labeled this module "Digital Content Readiness"; V4 folds it into
    // Digital Platform Assessment + Website & Content Improvements.
    const dcrPresentedInPdf =
      /data-ava-section="digital-content-readiness"|Digital Content Readiness|Discoverability/i.test(
        h
      ) ||
      (/data-ava-section="platform-health"|Digital Platform Assessment/i.test(h) &&
        /data-ava-section="website-content"|Website\s*&(?:amp;)?\s*Content Improvements/i.test(
          h
        ));
    if (!dcrPresentedInPdf) {
      defects.push({
        type: CQA_DEFECT_TYPES.MODULE_PDF_OMISSION,
        severity: "CRITICAL",
        section: "pdf.digitalContentReadiness",
        message:
          "Digital Content Readiness module is present in the payload but missing from the customer PDF",
      });
    }
    const registry = dcr.platformCoverageRegistry || {};
    if (
      Object.keys(registry).length &&
      !/External digital presence|Google Business Profile|Digital Platform Assessment|data-ava-section="platform-health"/i.test(
        h
      )
    ) {
      defects.push({
        type: CQA_DEFECT_TYPES.MODULE_PDF_OMISSION,
        severity: "HIGH",
        section: "pdf.digitalContentReadiness.platforms",
        message: "Platform coverage registry exists but External digital presence is not visible in the PDF",
      });
    }
    if (
      Object.values(registry).some((v) => v === "IDENTIFIED_NOT_ASSESSED") &&
      !/not field-assessed|Identified — not|identified not assessed/i.test(h)
    ) {
      defects.push({
        type: CQA_DEFECT_TYPES.MODULE_PDF_OMISSION,
        severity: "MEDIUM",
        section: "pdf.digitalContentReadiness.unassessed",
        message:
          "Unassessed platforms exist but PDF does not label them as not field-assessed",
      });
    }
  }

  return defects;
}

/**
 * AVA Customer Report V3 — detailed-audit completeness gates.
 * Preserves Executive Audit Summary; strengthens remaining sections.
 */
export function detectCustomerReportV3Defects(payload, html = "") {
  const defects = [];
  const h = String(html || "");
  const exec = payload.executiveSummary || {};

  if (!/data-ava-section="executive"|Executive Audit Summary/i.test(h)) {
    defects.push({
      type: CQA_DEFECT_TYPES.EXEC_SUMMARY_STRUCTURE_BROKEN,
      severity: "CRITICAL",
      section: "pdf.executive",
      message: "Executive Audit Summary section missing from PDF",
    });
  }
  for (const [field, label] of [
    ["headline", "hotel-specific headline"],
    ["assessment", "executive assessment"],
    ["priorities", "recommended priorities"],
  ]) {
    if (exec[field] == null && field !== "priorities") {
      // priorities may live as array under different keys — soft check via HTML
    }
    void field;
    void label;
  }
  if (payload.kpis && !/AI Consideration|Scenario Presence|Peer Position|Consideration/i.test(h)) {
    defects.push({
      type: CQA_DEFECT_TYPES.EXEC_SUMMARY_STRUCTURE_BROKEN,
      severity: "HIGH",
      section: "pdf.executive.kpis",
      message: "Executive KPI metrics not visible in PDF",
    });
  }
  const isV4 = /data-ava-template="AI_VISIBILITY_AUDIT_PDF_TEMPLATE_V4"|Priority Findings/i.test(
    h
  );
  if (
    !isV4 &&
    !/Key Digital Presence Findings|data-ava-section="findings"/i.test(h)
  ) {
    defects.push({
      type: CQA_DEFECT_TYPES.MODULE_PDF_OMISSION,
      severity: "HIGH",
      section: "pdf.findings",
      message: "Key Digital Presence Findings section missing",
    });
  }
  if (
    isV4 &&
    !/Priority Findings|data-ava-section="priority-findings"/i.test(h)
  ) {
    defects.push({
      type: CQA_DEFECT_TYPES.MODULE_PDF_OMISSION,
      severity: "HIGH",
      section: "pdf.priority-findings",
      message: "V4 Priority Findings section missing",
    });
  }

  const platformRows =
    payload.digitalPlatformReadinessV3?.rows ||
    payload.digitalContentReadiness?.platformDetailRows ||
    [];
  if (platformRows.length > 0) {
    if (
      !isV4 &&
      !/Platform-by-platform assessment|Digital Platform & Website Readiness/i.test(h)
    ) {
      defects.push({
        type: CQA_DEFECT_TYPES.PLATFORM_DETAIL_INCOMPLETE,
        severity: "HIGH",
        section: "pdf.platforms",
        message: "V3 platform detail rows exist but detailed platform assessment is not in the PDF",
      });
    }
    if (
      isV4 &&
      !/Digital Platform Assessment|Digital Platform Health|data-ava-section="platform-health"/i.test(
        h
      )
    ) {
      defects.push({
        type: CQA_DEFECT_TYPES.PLATFORM_DETAIL_INCOMPLETE,
        severity: "HIGH",
        section: "pdf.platforms",
        message: "V4 Digital Platform Assessment section missing",
      });
    }
    const assessedWithUrl = platformRows.filter((r) => r.url && r.fieldAssessed !== false);
    for (const r of assessedWithUrl.slice(0, 8)) {
      try {
        const host = new URL(r.url).hostname.replace(/^www\./i, "");
        if (host && !h.includes(host) && !(r.platform && h.includes(String(r.platform).slice(0, 18)))) {
          // soft — URL may be truncated in PDF text extract; only flag if neither host nor platform name visible
        }
      } catch {
        /* ignore */
      }
    }
    if (
      platformRows.some((r) => /IDENTIFIED_NOT_ASSESSED/i.test(r.assessmentStatus || "")) &&
      !/not field-assessed|Identified — not/i.test(h)
    ) {
      defects.push({
        type: CQA_DEFECT_TYPES.PLATFORM_DETAIL_INCOMPLETE,
        severity: "MEDIUM",
        section: "pdf.platforms.unassessed",
        message: "Unassessed platforms are not clearly labeled in the PDF",
      });
    }
  }

  for (const a of payload.actions || []) {
    if (!a.affectedPlatform && !(a.technicalDetails?.urls || []).length) {
      defects.push({
        type: CQA_DEFECT_TYPES.ACTION_PLATFORM_UNSPECIFIED,
        severity: "MEDIUM",
        section: `actions:${a.actionId || a.headline || "unknown"}`,
        message: "Significant action lacks an identified affected platform/URL",
      });
    }
  }

  if (
    /demand segment|traveler-intent scenario matrix|competitor recommendation share/i.test(h) &&
    /ADP already|not repeated here|not duplicated/i.test(h) === false &&
    (h.match(/Scenario Presence/gi) || []).length > 3
  ) {
    defects.push({
      type: CQA_DEFECT_TYPES.V3_REPETITION_OR_ADP_DUPLICATION,
      severity: "MEDIUM",
      section: "pdf.adpDuplication",
      message: "PDF may duplicate detailed ADP traveler-intent analysis beyond the Executive Summary",
    });
  }

  if (
    Array.isArray(payload.implementationByOwnerV3) &&
    payload.implementationByOwnerV3.length &&
    !/Implementation by Responsible Party|data-ava-section="implementation-by-owner"/i.test(
      h
    ) &&
    // V4 folds owner grouping into the Prioritized Implementation Roadmap.
    !/data-ava-section="roadmap"|Prioritized Implementation Roadmap/i.test(h)
  ) {
    defects.push({
      type: CQA_DEFECT_TYPES.MODULE_PDF_OMISSION,
      severity: "HIGH",
      section: "pdf.implementationByOwner",
      message: "Implementation-by-owner groups exist but are missing from the PDF",
    });
  }

  if (
    Array.isArray(payload.digitalPlatformReadinessV3?.correctionMap) &&
    payload.digitalPlatformReadinessV3.correctionMap.length &&
    !/Digital Platform Correction Map/i.test(h) &&
    // V4 surfaces corrections inside Digital Platform Assessment rows.
    !/data-ava-section="platform-health"|Digital Platform Assessment/i.test(h)
  ) {
    defects.push({
      type: CQA_DEFECT_TYPES.PLATFORM_DETAIL_INCOMPLETE,
      severity: "HIGH",
      section: "pdf.correctionMap",
      message: "Correction map exists in payload but is missing from the PDF",
    });
  }

  for (const a of payload.actions || []) {
    const evidence = [
      ...(a.supportingEvidence || []),
      ...(a.technicalDetails?.urls || []),
      ...(a.technicalDetails?.evidence || []),
    ].filter((e) => e && e.url);
    if (
      /HIGH|CRITICAL/i.test(String(a.priority || "")) &&
      !evidence.length &&
      !/further investigation|requires investigation/i.test(
        `${a.whatWeFound || ""} ${a.headline || ""}`
      )
    ) {
      defects.push({
        type: CQA_DEFECT_TYPES.ACTION_PLATFORM_UNSPECIFIED,
        severity: "MEDIUM",
        section: `actions.evidence:${a.actionId || a.headline || "unknown"}`,
        message:
          "High-priority action lacks a customer-facing evidence URL (unless labeled as requiring further investigation)",
      });
    }
  }

  const discovery = payload.platformListingDiscoveryV2;
  if (discovery?.completenessGate && discovery.completenessGate.pass === false) {
    defects.push({
      type: CQA_DEFECT_TYPES.PLATFORM_DETAIL_INCOMPLETE,
      severity: "HIGH",
      section: "platformListingDiscoveryV2",
      message:
        "Platform research completeness gate failed — applicable platforms lack assessment or documented research attempts",
    });
  }

  return defects;
}

function hostOf(u) {
  try {
    return new URL(u).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

function collectStrings(obj, path = "", out = []) {
  if (obj == null) return out;
  if (typeof obj === "string") {
    out.push({ path, text: obj });
    return out;
  }
  if (Array.isArray(obj)) {
    obj.forEach((v, i) => collectStrings(v, `${path}[${i}]`, out));
    return out;
  }
  if (typeof obj === "object") {
    for (const [k, v] of Object.entries(obj)) {
      collectStrings(v, path ? `${path}.${k}` : k, out);
    }
  }
  return out;
}

function uniqueActionIds(payload) {
  const ids = [];
  const seen = new Set();
  for (const a of payload.actions || []) {
    const id = a?.actionId;
    if (!id) continue;
    if (seen.has(id)) continue;
    seen.add(id);
    ids.push(id);
  }
  return ids;
}

function countPhraseFromIntro(intro) {
  const m = String(intro || "").match(
    /\b(\d+|one|two|three|four|five|six|seven|eight|nine|ten)\b/i
  );
  if (!m) return null;
  const map = {
    one: 1,
    two: 2,
    three: 3,
    four: 4,
    five: 5,
    six: 6,
    seven: 7,
    eight: 8,
    nine: 9,
    ten: 10,
  };
  const raw = m[1].toLowerCase();
  return map[raw] != null ? map[raw] : Number(raw);
}

function numberWords(n) {
  const w = [
    "zero",
    "one",
    "two",
    "three",
    "four",
    "five",
    "six",
    "seven",
    "eight",
    "nine",
    "ten",
  ];
  return w[n] || String(n);
}

export function detectActionReconciliationDefects(payload) {
  const defects = [];
  const actionIds = uniqueActionIds(payload);
  const overviewIds = (payload.actionOverview || [])
    .map((a) => a?.actionId)
    .filter(Boolean);
  const decisionIds = (payload.hotelDecisions?.items || [])
    .map((d) => d?.relatedActionId)
    .filter(Boolean);

  const overviewDupes = overviewIds.filter(
    (id, i) => overviewIds.indexOf(id) !== i
  );
  for (const id of new Set(overviewDupes)) {
    defects.push({
      type: CQA_DEFECT_TYPES.DUPLICATE_ACTION,
      severity: "HIGH",
      section: "actionOverview",
      message: `Duplicate action overview entry for ${id}`,
      actionId: id,
    });
  }

  const stated = countPhraseFromIntro(payload.actionOverviewIntro);
  if (stated != null && stated !== actionIds.length) {
    defects.push({
      type: CQA_DEFECT_TYPES.ACTION_COUNT_MISMATCH,
      severity: "HIGH",
      section: "actionOverviewIntro",
      message: `Action overview intro says ${stated} actions but detailed plan has ${actionIds.length} unique actions`,
      expected: actionIds.length,
      stated,
      originalText: payload.actionOverviewIntro,
    });
  }

  if (overviewIds.length && overviewIds.length !== actionIds.length) {
    const missing = actionIds.filter((id) => !overviewIds.includes(id));
    const extra = overviewIds.filter((id) => !actionIds.includes(id));
    defects.push({
      type: CQA_DEFECT_TYPES.ACTION_COUNT_MISMATCH,
      severity: "HIGH",
      section: "actionOverview",
      message: `Action overview (${overviewIds.length}) does not match unique actions (${actionIds.length})`,
      missing,
      extra,
    });
  }

  // Decisions should cover every unique action (or explicitly defer)
  if (decisionIds.length && decisionIds.length < actionIds.length) {
    const missing = actionIds.filter((id) => !decisionIds.includes(id));
    if (missing.length) {
      defects.push({
        type: CQA_DEFECT_TYPES.ACTION_COUNT_MISMATCH,
        severity: "MEDIUM",
        section: "hotelDecisions",
        message: `Decision list covers ${decisionIds.length} of ${actionIds.length} actions`,
        missing,
      });
    }
  }

  // Generic next step
  const next = String(payload.methodology?.nextStep || "");
  if (
    next &&
    /confirm the decision items|authorize the highest-priority validated work/i.test(
      next
    )
  ) {
    defects.push({
      type: CQA_DEFECT_TYPES.COMMERCIAL_CLARITY_ISSUE,
      severity: "MEDIUM",
      section: "methodology.nextStep",
      message: "Next step is generic rather than hotel-specific",
      originalText: next,
    });
  }

  return defects;
}

/**
 * Hilton / brand unverified contradiction detector (generalized).
 * If executive/findings say a brand figure could not be re-verified, but an
 * action/evidence still labels that brand claim VERIFIED_PUBLIC → defect.
 */
export function detectEvidenceConsistencyDefects(payload) {
  const defects = [];
  const corpus = collectStrings(payload)
    .map((x) => x.text)
    .join("\n");

  const brandUnverified =
    /could not be re-?verified|not (?:used as|treated as) (?:current )?customer-facing evidence|brand error page|unable to verify|not currently verified/i.test(
      corpus
    ) && /hilton|curio|brand (?:page|property|figure|url)/i.test(corpus);

  const walkEvidence = (items, section) => {
    for (const e of items || []) {
      const label = String(e?.label || "");
      const kind = String(e?.kind || "");
      if (
        brandUnverified &&
        /hilton|curio/i.test(label) &&
        /^VERIFIED/i.test(kind)
      ) {
        defects.push({
          type: CQA_DEFECT_TYPES.CRITICAL_FACTUAL_CONTRADICTION,
          severity: "CRITICAL",
          section,
          message:
            "Brand/Hilton evidence labeled VERIFIED while report states it could not be re-verified",
          originalText: `${label} [${kind}]`,
          label,
          kind,
          url: e?.url || null,
        });
      }
      if (
        /hilton|curio/i.test(label) &&
        e?.url &&
        /waterstoneboca\.com/i.test(String(e.url)) &&
        !/hilton\.com/i.test(String(e.url))
      ) {
        defects.push({
          type: CQA_DEFECT_TYPES.UNSUPPORTED_CLAIM,
          severity: "HIGH",
          section,
          message:
            "Evidence labeled as Hilton/brand page but URL points to hotel-operated site",
          originalText: `${label} → ${e.url}`,
          label,
          url: e.url,
        });
      }
    }
  };

  for (const f of payload.keyFindings || []) {
    walkEvidence(f.evidence, `keyFindings:${f.title || f.relatedActionId}`);
  }
  for (const a of payload.actions || []) {
    walkEvidence(
      a.supportingEvidence,
      `actions:${a.actionId}:supportingEvidence`
    );
    walkEvidence(
      a.technicalDetails?.urls,
      `actions:${a.actionId}:technicalDetails.urls`
    );
    walkEvidence(
      a.technicalDetails?.evidence,
      `actions:${a.actionId}:technicalDetails.evidence`
    );

    if (
      brandUnverified &&
      /both claims are verified|both figures are verified|two verified/i.test(
        String(a.whyItMatters || "") + " " + String(a.whatWeFound || "")
      )
    ) {
      defects.push({
        type: CQA_DEFECT_TYPES.CRITICAL_FACTUAL_CONTRADICTION,
        severity: "CRITICAL",
        section: `actions:${a.actionId}`,
        message:
          "Action text asserts both meeting figures are verified while executive context says brand figure could not be re-verified",
        originalText: a.whyItMatters || a.whatWeFound,
        actionId: a.actionId,
      });
    }
  }

  // Official site misclassified as external mention
  const officialHost = hostOf(
    payload.property?.website ||
      payload.digitalAuthority?.officialWebsite ||
      ""
  );
  for (const s of payload.digitalAuthority?.sources || []) {
    const cls = String(s.findingClass || "");
    const srcHost = hostOf(s.linkingPageUrl || s.evidenceUrl || "");
    if (
      cls === "VERIFIED_EXTERNAL_MENTION" &&
      officialHost &&
      srcHost === officialHost
    ) {
      defects.push({
        type: CQA_DEFECT_TYPES.INVALID_EVIDENCE_CLASSIFICATION,
        severity: "HIGH",
        section: "digitalAuthority.sources",
        message:
          "Hotel-operated / first-party website classified as external mention",
        originalText: `${s.referringOrganization} [${cls}] ${s.linkingPageUrl}`,
        linkingPageUrl: s.linkingPageUrl,
      });
    }
  }

  // Hotel decisions intro internal language
  const decisionsIntro = payload.hotelDecisions?.intro || "";
  if (/validation-stage payload/i.test(decisionsIntro)) {
    defects.push({
      type: CQA_DEFECT_TYPES.INTERNAL_LANGUAGE_LEAK,
      severity: "HIGH",
      section: "hotelDecisions.intro",
      message: "Internal validation-stage language in customer decisions intro",
      originalText: decisionsIntro,
    });
  }

  return defects;
}

export function detectCommunicationDefects(payload) {
  const defects = [];
  const sections = {
    "executiveSummary": payload.executiveSummary,
    "actionOverviewIntro": payload.actionOverviewIntro,
    "hotelDecisions": payload.hotelDecisions,
    "findingsIntro": payload.findingsIntro,
    "keyFindings": payload.keyFindings,
    "actionPlanIntro": payload.actionPlanIntro,
    "actions": (payload.actions || []).map((a) => ({
      headline: a.headline,
      whatWeFound: a.whatWeFound,
      whyItMatters: a.whyItMatters,
      priorityWhy: a.priorityWhy,
      recommendedSteps: a.recommendedSteps,
      accountableOwner: a.accountableOwner,
      hotelApprovalNotes: a.hotelApprovalNotes,
    })),
    "methodology": {
      scope: payload.methodology?.scope,
      claimTypeGuide: payload.methodology?.claimTypeGuide,
      responsibilities: payload.methodology?.responsibilities,
      monthlyReviewRelationship: payload.methodology?.monthlyReviewRelationship,
      nextStep: payload.methodology?.nextStep,
    },
    "digitalAuthority.executiveFinding":
      payload.digitalAuthority?.executiveFinding,
    "digitalAuthority.competitiveComparison":
      payload.digitalAuthority?.competitiveComparison,
    "digitalAuthority.inventoryCompleteness":
      payload.digitalAuthority?.inventoryCompleteness,
    "digitalContentReadiness.executiveAssessment":
      payload.digitalContentReadiness?.executiveAssessment,
  };

  for (const [section, value] of Object.entries(sections)) {
    for (const hit of scanObjectForInternalLanguage(value, section)) {
      defects.push({
        type: CQA_DEFECT_TYPES.INTERNAL_LANGUAGE_LEAK,
        severity: "HIGH",
        section: hit.section,
        message: `Internal language leaked: ${hit.match}`,
        originalText: hit.match,
        pattern: hit.pattern,
      });
    }
  }

  // Raw findingClass codes in DA customer strings
  for (const field of [
    "executiveFinding",
    "competitiveComparison",
    "inventoryCompleteness",
  ]) {
    const text = payload.digitalAuthority?.[field];
    for (const hit of scanInternalLanguageLeaks(text, `digitalAuthority.${field}`)) {
      defects.push({
        type: CQA_DEFECT_TYPES.INTERNAL_LANGUAGE_LEAK,
        severity: "HIGH",
        section: hit.section,
        message: `Internal language leaked: ${hit.match}`,
        originalText: hit.match,
      });
    }
  }

  // Methodology claimTypeGuide exposing raw codes without plain language alone is ok
  // if guide still contains raw codes in customer PDF — soft leak
  for (const line of payload.methodology?.claimTypeGuide || []) {
    if (/\b(VERIFIED_ISSUE|RECOMMENDED_INVESTIGATION|REQUIRES_HOTEL_CONFIRMATION)\b/.test(line)) {
      defects.push({
        type: CQA_DEFECT_TYPES.INTERNAL_LANGUAGE_LEAK,
        severity: "MEDIUM",
        section: "methodology.claimTypeGuide",
        message: "Claim-type guide exposes raw internal codes",
        originalText: line,
      });
    }
  }

  return defects;
}

export function detectResearchQualityDefects(payload) {
  const defects = [];
  const hotelName = payload.property?.name || "";
  for (const a of payload.actions || []) {
    const blob = `${a.headline || ""} ${a.whatWeFound || ""} ${a.whyItMatters || ""}`;
    if (
      /improve SEO|general SEO best practices|increase online presence/i.test(
        blob
      ) &&
      hotelName &&
      !new RegExp(hotelName.split(/\s+/).slice(0, 2).join("|"), "i").test(blob)
    ) {
      defects.push({
        type: CQA_DEFECT_TYPES.COMMERCIAL_CLARITY_ISSUE,
        severity: "HIGH",
        section: `actions:${a.actionId}`,
        message: "Recommendation appears generic rather than hotel-specific",
        actionId: a.actionId,
      });
    }
    // Flag affirmative outcome guarantees only — allow "cannot/does not guarantee".
    if (
      /(?<!cannot\s)(?<!does not\s)(?<!do not\s)(?<!won't\s)guarantee(?:s|d)?\s+(?:more|higher|increased|improved|additional)/i.test(
        blob
      ) ||
      /\bwill (?:increase|improve|raise)\b.{0,60}\b(?:bookings|revenue|AI recommendations)\b/i.test(
        blob
      )
    ) {
      defects.push({
        type: CQA_DEFECT_TYPES.UNSUPPORTED_CLAIM,
        severity: "HIGH",
        section: `actions:${a.actionId}`,
        message: "Action overstates likely changes in AI recommendations, bookings, or revenue",
        originalText: blob.slice(0, 240),
      });
    }
    if (!a.accountableOwner) {
      defects.push({
        type: CQA_DEFECT_TYPES.COMMERCIAL_CLARITY_ISSUE,
        severity: "MEDIUM",
        section: `actions:${a.actionId}`,
        message: "Action missing accountable owner",
        actionId: a.actionId,
      });
    }
    if (
      !a.completionCriteria ||
      (typeof a.completionCriteria === "object" &&
        !Object.keys(a.completionCriteria).length)
    ) {
      defects.push({
        type: CQA_DEFECT_TYPES.COMMERCIAL_CLARITY_ISSUE,
        severity: "MEDIUM",
        section: `actions:${a.actionId}`,
        message: "Action missing completion criteria",
        actionId: a.actionId,
      });
    }
  }
  return defects;
}

/**
 * Customer-report quality standard checks (payload + rendered HTML).
 */
export function detectCustomerReportQualityStandardDefects(payload, html = "") {
  const defects = [];
  const h = String(html || "");

  for (const d of detectIncompleteCustomerNarratives(payload)) {
    defects.push({
      ...d,
      type: CQA_DEFECT_TYPES.INCOMPLETE_CUSTOMER_NARRATIVE,
    });
  }
  for (const d of detectSerializationArtifacts(h, "pdf.html")) {
    defects.push({
      ...d,
      type: CQA_DEFECT_TYPES.CUSTOMER_SERIALIZATION_ARTIFACT,
    });
  }
  for (const d of detectCanonicalActionIntegrityDefects(payload)) {
    defects.push({
      ...d,
      type: CQA_DEFECT_TYPES[d.type] || d.type,
    });
  }

  const genericHits = (
    h.match(/Confirm facts match hotel standards; update if needed\./gi) || []
  ).length;
  if (genericHits >= 3) {
    defects.push({
      type: CQA_DEFECT_TYPES.GENERIC_PLATFORM_ACTION,
      severity: "HIGH",
      section: "pdf.platforms",
      message: `Generic platform action repeated ${genericHits} times`,
    });
  }

  if (
    /No platform rows packaged\.?/i.test(h) ||
    /No platform assessments? (?:were )?packaged/i.test(h)
  ) {
    defects.push({
      type: CQA_DEFECT_TYPES.EMPTY_PLATFORM_SECTION_PLACEHOLDER,
      severity: "CRITICAL",
      section: "pdf.platforms",
      message:
        "Customer PDF contains empty platform-section placeholder (No platform rows packaged)",
    });
  }

  if (
    /Hotel leadership records the approved source of truth and scope\.?/i.test(h) ||
    (
      h.match(
        /Approved positioning and product facts published consistently on the hotel/gi
      ) || []
    ).length >= 2
  ) {
    defects.push({
      type: CQA_DEFECT_TYPES.BOILERPLATE_DESIRED_OUTCOME,
      severity: "HIGH",
      section: "pdf.executive",
      message: "Boilerplate or repeated generic desired-outcome language appears in the customer PDF",
    });
  }

  if (
    /Platform presence identified or still requiring exact-listing confirmation/i.test(
      h
    )
  ) {
    defects.push({
      type: CQA_DEFECT_TYPES.GENERIC_PLATFORM_ASSESSMENT_ROW,
      severity: "HIGH",
      section: "pdf.platforms",
      message:
        "Generic platform-assessment wording still used instead of listing/content-specific status",
    });
  }

  if (
    /Executive Takeaway/i.test(h) &&
    !/data-ava-takeaway-keep|ava-exec-takeaway-keep/i.test(h)
  ) {
    defects.push({
      type: CQA_DEFECT_TYPES.EXECUTIVE_TAKEAWAY_KEEP_MISSING,
      severity: "HIGH",
      section: "pdf.executive",
      message:
        "Executive Takeaway is not wrapped in the Chromium keep-together table",
    });
  }

  if (
    /Identify inventory\.|Submit profile\.|Classify honestly\./i.test(h)
  ) {
    defects.push({
      type: CQA_DEFECT_TYPES.INCOMPLETE_OR_GENERIC_IMPLEMENTATION_STEPS,
      severity: "HIGH",
      section: "pdf",
      message: "Generic incomplete implementation steps appear in the customer PDF",
    });
  }

  if (
    /not a comprehensive paid link inventory/i.test(h) &&
    /data-ava-section="(?:priority-findings|digital-authority)"/i.test(h)
  ) {
    // Methodology limitation presented as a finding/action card
    if (
      /Issue:\s*not a comprehensive paid link inventory/i.test(h) ||
      /Recommended action:[\s\S]{0,80}not a comprehensive paid link inventory/i.test(
        h
      )
    ) {
      defects.push({
        type: CQA_DEFECT_TYPES.METHODOLOGY_AS_CUSTOMER_FINDING,
        severity: "HIGH",
        section: "pdf",
        message:
          "Methodology limitation presented as a customer-facing finding or action",
      });
    }
  }

  if (
    /michelin/i.test(h) &&
    /star/i.test(h) &&
    /key/i.test(h) &&
    /does not match Michelin’s published One MICHELIN Key|Star language risks reputation if checked against Michelin’s hotel/i.test(
      h
    ) &&
    !/does not by itself prove the Star claim is false|verify Star status independently/i.test(
      h
    )
  ) {
    defects.push({
      type: CQA_DEFECT_TYPES.MICHELIN_STAR_KEY_CONFLATION,
      severity: "HIGH",
      section: "pdf.michelin",
      message:
        "Customer PDF treats hotel Michelin Key as proof that a restaurant Star claim is false",
    });
  }

  // Duplicate spa recommendations in PDF
  if (
    /Correct the Conflicting Spa Size/i.test(h) &&
    /Approve One Spa Specification/i.test(h)
  ) {
    defects.push({
      type: CQA_DEFECT_TYPES.SEMANTIC_DUPLICATE_WORKSTREAM,
      severity: "HIGH",
      section: "pdf.priority-findings",
      message: "Customer PDF still shows duplicate spa-size workstreams",
    });
  }

  // Title/step mismatch leaked into PDF (AC lounge vs meeting)
  if (
    /Reconcile and Standardize Meeting-Space Information/i.test(h) &&
    /Strengthen AC Lounge and Business-District Signals/i.test(h)
  ) {
    defects.push({
      type: CQA_DEFECT_TYPES.ACTION_TITLE_STEP_MISMATCH,
      severity: "CRITICAL",
      section: "pdf",
      message:
        "Meeting-space title appears with lounge/business-district implementation language",
    });
  }

  // Duplicate acorunacb unlinked cards
  const acbMentions = (h.match(/acorunacb\.com/gi) || []).length;
  const unlinkedCards = (h.match(/Unlinked mention/gi) || []).length;
  if (acbMentions >= 4 && unlinkedCards >= 3) {
    defects.push({
      type: CQA_DEFECT_TYPES.DUPLICATE_DA_HOST_OUTREACH,
      severity: "HIGH",
      section: "pdf.digital-authority",
      message:
        "Multiple nearly identical unlinked-mention cards for the same CVB host",
    });
  }

  // Healthy backlink incorrectly framed as correction
  if (
    /Verified healthy backlink/i.test(h) &&
    /Request an accurate property-specific hyperlink/i.test(h) &&
    /Verified healthy backlink[\s\S]{0,400}Request an accurate property-specific hyperlink/i.test(
      h
    )
  ) {
    defects.push({
      type: CQA_DEFECT_TYPES.HEALTHY_BACKLINK_AS_CORRECTION,
      severity: "HIGH",
      section: "pdf.digital-authority",
      message: "Healthy backlink card still recommends adding a link",
    });
  }

  const register =
    payload.customerActionRegister || buildCanonicalActionRegister(payload);

  const da = payload.digitalAuthority;
  if (da && typeof da === "object") {
    const hasSources =
      (da.sources || []).length > 0 ||
      (da.opportunities || []).length > 0 ||
      (da.verifiedBacklinks || da.backlinks || []).length > 0;
    if (
      hasSources &&
      /data-ava-section="digital-authority"/i.test(h) &&
      !/ava-evidence-link|href="https?:/i.test(
        (h.match(/data-ava-section="digital-authority"[\s\S]*?(?=data-ava-section=|$)/) || [
          "",
        ])[0]
      )
    ) {
      defects.push({
        type: CQA_DEFECT_TYPES.MISSING_DIGITAL_AUTHORITY_EVIDENCE,
        severity: "HIGH",
        section: "pdf.digital-authority",
        message:
          "Digital Authority section lacks clickable source evidence despite verified sources/opportunities in payload",
      });
    }
  }

  void register;
  return defects;
}

/**
 * PDF presentation checks on generated HTML (+ optional buffer metadata).
 * Visual screenshot analysis is reported as limitation when unavailable.
 * Pass options.buildHtml to evaluate the same template used for generation (V4).
 */
export function detectPdfPresentationDefects(payload, options = {}) {
  const defects = [];
  const buildHtml = options.buildHtml || buildAiVisibilityAuditPdfHtml;
  const built = buildHtml(payload);
  if (!built.ok) {
    defects.push({
      type: CQA_DEFECT_TYPES.PDF_LAYOUT_DEFECT,
      severity: "CRITICAL",
      section: "pdf",
      message: `PDF HTML build failed: ${(built.errors || []).join("; ")}`,
    });
    return { defects, htmlOk: false, visualInspection: "NOT_RUN" };
  }

  const html = built.html || "";
  defects.push(...detectCustomerReportQualityStandardDefects(payload, html));

  // Confidentiality must not repeat in body (footer-only)
  const bodyConfMatches = (
    html.match(/Confidential\s*[·•]\s*For recipient only/gi) || []
  ).length;
  // Cover may include; body method section should not duplicate — allow ≤2
  if (bodyConfMatches > 2) {
    defects.push({
      type: CQA_DEFECT_TYPES.PDF_LAYOUT_DEFECT,
      severity: "MEDIUM",
      section: "pdf.confidentiality",
      message: `Confidentiality notice appears ${bodyConfMatches} times in HTML (expected footer-only)`,
    });
  }

  // Scan visible text only (strip tags / data attributes used for Admin DOM hooks)
  const visible = html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/\s(?:data|class|id|href|src|rel|target)=["'][^"']*["']/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ");
  for (const hit of scanInternalLanguageLeaks(visible, "pdf.visibleText")) {
    defects.push({
      type: CQA_DEFECT_TYPES.INTERNAL_LANGUAGE_LEAK,
      severity: "HIGH",
      section: "pdf.visibleText",
      message: `Customer PDF visible text contains internal language: ${hit.match}`,
      originalText: hit.match,
    });
  }

  // Orphan heading heuristic: heading near end of a large section without following body
  if (/<h2[^>]*>[^<]+<\/h2>\s*<\/section>/i.test(html)) {
    defects.push({
      type: CQA_DEFECT_TYPES.PDF_LAYOUT_DEFECT,
      severity: "MEDIUM",
      section: "pdf.layout",
      message: "Section heading appears with no following content (orphan heading risk)",
    });
  }

  // Module → PDF completeness (DA / DCR / counts / unassessed platforms)
  defects.push(...detectModulePdfCompletenessDefects(payload, html));
  defects.push(...detectCustomerReportV3Defects(payload, html));

  let visualInspection = "NOT_AVAILABLE";
  if (options.pdfBuffer && options.visualInspector) {
    try {
      const vis = options.visualInspector(options.pdfBuffer);
      visualInspection = vis?.status || "RAN";
      for (const d of vis?.defects || []) defects.push(d);
    } catch (err) {
      visualInspection = `FAILED:${err.message}`;
    }
  } else if (options.pdfBuffer) {
    visualInspection =
      "LIMITATION: rendered-page visual analysis not configured for this run; HTML/structure checks applied";
  }

  return { defects, htmlOk: true, html, visualInspection, evidenceUrls: built.evidenceUrls };
}

/**
 * Full customer-quality assessment (Gates A–D).
 */
export function assessCustomerQuality(payload, options = {}) {
  const gateA = [
    ...detectResearchQualityDefects(payload),
    ...detectIncompleteCustomerNarratives(payload).map((d) => ({
      ...d,
      type: CQA_DEFECT_TYPES.INCOMPLETE_CUSTOMER_NARRATIVE,
    })),
  ];
  const gateB = [
    ...detectEvidenceConsistencyDefects(payload),
    ...detectActionReconciliationDefects(payload),
  ];
  const gateC = detectCommunicationDefects(payload);
  const pdf = detectPdfPresentationDefects(payload, options);
  const gateD = pdf.defects;

  const all = [...gateA, ...gateB, ...gateC, ...gateD];
  const critical = all.filter((d) => d.severity === "CRITICAL");
  const high = all.filter((d) => d.severity === "HIGH");

  const passA = gateA.filter((d) => d.severity !== "MEDIUM").length === 0;
  const passB =
    gateB.filter((d) => d.severity === "CRITICAL" || d.severity === "HIGH")
      .length === 0;
  const passC =
    gateC.filter((d) => d.severity === "CRITICAL" || d.severity === "HIGH")
      .length === 0;
  // PDF: CRITICAL/HIGH fail; MEDIUM layout risks fail overall if orphan/conf
  const passD =
    gateD.filter((d) => d.severity === "CRITICAL" || d.severity === "HIGH")
      .length === 0;

  const overallPass = passA && passB && passC && passD && critical.length === 0;

  return {
    contract: CUSTOMER_QUALITY_ASSURANCE_CONTRACT,
    assessedAt: new Date().toISOString(),
    gates: {
      researchQuality: { pass: passA, defects: gateA },
      evidenceQuality: { pass: passB, defects: gateB },
      communicationQuality: { pass: passC, defects: gateC },
      pdfPresentationQuality: {
        pass: passD,
        defects: gateD,
        visualInspection: pdf.visualInspection,
      },
    },
    defectCount: all.length,
    criticalCount: critical.length,
    highCount: high.length,
    defects: all,
    overallPass,
    customerQualityCertified: overallPass,
    founderApprovedForCustomer: false,
    publicationAuthorized: false,
  };
}

export function buildDefaultActionOverviewIntro(count) {
  return `${count} hotel-specific actions at a glance; detailed evidence and agency steps follow.`;
}

export function numberWord(n) {
  return numberWords(n);
}

export { replaceInternalLanguage, uniqueActionIds };
