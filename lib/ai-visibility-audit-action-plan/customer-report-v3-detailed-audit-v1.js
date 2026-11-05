/**
 * AVA Customer Report V3 — Detailed Audit Enhancement
 *
 * Builds customer-facing presentation blocks from existing verified modules.
 * Does not invent platform deficiencies or re-run paid research.
 * Does not modify Executive Audit Summary structure.
 */
import { replaceInternalLanguage } from "./customer-facing-language-v1.js";
import { humanEvidenceLabel } from "./platform-listing-discovery-v2.js";

export const AVA_CUSTOMER_REPORT_V3_CONTRACT =
  "AVA_CUSTOMER_REPORT_V3_DETAILED_AUDIT_V1";

const PLATFORM_LABELS = Object.freeze({
  BRAND_PROPERTY_PAGE: "Brand property page",
  HOTEL_OPERATED_WEBSITE: "Hotel-operated website",
  HOTEL_OPERATED_SUBPAGE: "Hotel-operated subpage",
  GOOGLE_BUSINESS_PROFILE: "Google Business Profile",
  TRIPADVISOR: "Tripadvisor",
  OTA_BOOKING: "Booking.com",
  OTA_EXPEDIA: "Expedia / Hotels.com",
  APPLE_MAPS: "Apple Maps",
  BING_PLACES: "Bing Places",
  CVENT: "Cvent / meetings directories",
  DESTINATION_CVB: "Tourism board / convention bureau",
  SPECIALIST_DIRECTORY: "Specialist directory",
  TOURISM_BOARD: "Tourism board",
});

const STATUS_LABELS = Object.freeze({
  ASSESSED: "Assessed",
  VERIFIED_CORRECT: "Verified reachable",
  IDENTIFIED_NOT_ASSESSED: "Identified — not field-assessed",
  ACCESS_RESTRICTED: "Access restricted",
  NOT_FOUND: "Not found",
  NOT_RELEVANT: "Not applicable",
  NEEDS_CORRECTION: "Needs correction",
  CONFLICTING_FACTS: "Conflicting facts",
});

const OWNER_GROUPS = Object.freeze([
  {
    key: "hotel",
    label: "Hotel management",
    match: /hotel (management|leadership|marketing|gm)|general manager|property/i,
  },
  {
    key: "brand",
    label: "Brand",
    match: /brand|marriott|hilton|ihg|hyatt|radisson|wyndham|curio|luxury collection/i,
  },
  {
    key: "agency",
    label: "Digital marketing agency",
    match: /agency|marketing partner|digital marketing/i,
  },
  {
    key: "developer",
    label: "Website developer",
    match: /developer|web(site)? (dev|team)|cdn|it\b/i,
  },
  {
    key: "platform",
    label: "Third-party platform owner",
    match: /ota|tripadvisor|booking\.com|expedia|google|platform|directory/i,
  },
  {
    key: "tourism",
    label: "Tourism or industry organization",
    match: /tourism|cvb|convention|destination|visit /i,
  },
]);

function cust(s) {
  return replaceInternalLanguage(String(s || ""));
}

function hostOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./i, "");
  } catch {
    return "";
  }
}

function linkingUrl(s) {
  return s?.linkingPageUrl || s?.evidenceUrl || s?.url || null;
}

function uniqueHosts(sources) {
  const set = new Set();
  for (const s of sources || []) {
    const h = hostOf(linkingUrl(s));
    if (h) set.add(h.toLowerCase());
  }
  return [...set];
}

function humanStatus(v) {
  return STATUS_LABELS[v] || String(v || "").replace(/_/g, " ");
}

function humanPlatform(v) {
  return PLATFORM_LABELS[v] || String(v || "").replace(/_/g, " ");
}

function ownerBucket(ownerText) {
  const t = String(ownerText || "");
  for (const g of OWNER_GROUPS) {
    if (g.match.test(t)) return g;
  }
  return { key: "hotel", label: "Hotel management" };
}

/**
 * Expand key findings with commercial relevance using related actions / DCR
 * without inventing new defects.
 */
export function enrichKeyFindings(payload) {
  const findings = Array.isArray(payload.keyFindings)
    ? payload.keyFindings.map((f) => ({ ...f }))
    : [];
  const actionsById = new Map(
    (payload.actions || []).map((a) => [a.actionId, a])
  );
  const dcr = payload.digitalContentReadiness || {};
  const questions = Array.isArray(dcr.travelerQuestionCoverage)
    ? dcr.travelerQuestionCoverage
    : [];

  return findings.map((f) => {
    const action = actionsById.get(f.relatedActionId);
    const commercialRelevance =
      f.commercialRelevance ||
      action?.whyItMatters ||
      "Improves clarity of public facts travelers and partners use when comparing this hotel.";
    const diagnosisThemes = [];
    const blob = `${f.title || ""} ${f.summary || ""} ${action?.whatWeFound || ""}`.toLowerCase();
    if (/website|domain|redirect|official site|waterstoneboca|hotel.?operated/i.test(blob)) {
      diagnosisThemes.push("Official website clarity");
    }
    if (/brand|marriott|hilton|curio|luxury collection/i.test(blob)) {
      diagnosisThemes.push("Brand page versus hotel website");
    }
    if (/crawl|access|403|robots|sitemap|bot/i.test(blob)) {
      diagnosisThemes.push("Crawlability / content accessibility");
    }
    if (/schema|json-ld|structured/i.test(blob)) {
      diagnosisThemes.push("Structured data");
    }
    if (/conflict|discrepan|inconsist|900|990|18,?899|7,?500|address/i.test(blob)) {
      diagnosisThemes.push("Conflicting information");
    }
    if (/meeting|wedding|spa|restaurant|dining|golf|marina|leisure/i.test(blob)) {
      diagnosisThemes.push("Commercially relevant content (meetings / F&B / leisure)");
    }
    const relatedQ = questions.find(
      (q) =>
        q.relatedActionId === f.relatedActionId ||
        (f.title && String(q.travelerIntent || "").includes(String(f.title).slice(0, 12)))
    );
    if (
      relatedQ &&
      ["MISSING_INFORMATION", "INCONSISTENT_INFORMATION", "PARTIALLY_ANSWERED"].includes(
        relatedQ.classification
      )
    ) {
      diagnosisThemes.push("Traveler question coverage gap");
    }

    return {
      ...f,
      summary: cust(f.summary),
      commercialRelevance: cust(commercialRelevance),
      diagnosisThemes: [...new Set(diagnosisThemes)],
      affectedPlatform:
        f.affectedPlatform ||
        action?.technicalDetails?.urls?.[0]?.label ||
        action?.technicalDetails?.urls?.[0]?.url ||
        null,
    };
  });
}

/**
 * Build detailed platform rows from assessments + registry.
 * Unassessed platforms appear without invented defects.
 */
export function buildPlatformDetailRows(payload) {
  const pca = payload.platformCoverageAssessment || {};
  const dcr = payload.digitalContentReadiness || {};
  const assessments = [
    ...(Array.isArray(pca.assessments) ? pca.assessments : []),
    ...(Array.isArray(dcr.platformAssessments) ? dcr.platformAssessments : []),
  ];
  const corrections = [
    ...(Array.isArray(pca.corrections) ? pca.corrections : []),
    ...(Array.isArray(dcr.platformCorrections) ? dcr.platformCorrections : []),
    ...(Array.isArray(payload.digitalPlatformCorrectionRegister?.corrections)
      ? payload.digitalPlatformCorrectionRegister.corrections
      : []),
  ];
  const registry = {
    ...(dcr.platformCoverageRegistry || {}),
    ...(pca.platformCoverageRegistry || {}),
  };

  const seenUrl = new Set();
  const rows = [];

  for (const a of assessments) {
    if (!a?.relevant && a?.assessmentStatus === "NOT_RELEVANT") continue;
    const url = a.url || null;
    const key = `${a.category || a.platform}|${url || a.platform}`;
    if (seenUrl.has(key)) continue;
    seenUrl.add(key);

    const corr = corrections.find(
      (c) =>
        (url && c.url && c.url === url) ||
        (c.platform &&
          a.platform &&
          String(c.platform).toLowerCase().includes(String(a.platform).toLowerCase().slice(0, 12))) ||
        (c.category && c.category === a.category)
    );

    let verifiedFinding = cust(a.notes || "");
    if (a.schemaOrgDetected === false && /WEBSITE|SUBPAGE/i.test(a.category || "")) {
      verifiedFinding =
        (verifiedFinding ? `${verifiedFinding} ` : "") +
        "No Schema.org JSON-LD detected in the HTML sample retrieved for this audit.";
    }
    if (a.assessmentStatus === "ACCESS_RESTRICTED") {
      verifiedFinding =
        verifiedFinding ||
        "Automated access was restricted; this is not treated as proof the public page is broken.";
    }
    if (!verifiedFinding && a.assessmentStatus === "VERIFIED_CORRECT") {
      verifiedFinding = "Page reachable in this review; deeper field accuracy may still need hotel confirmation.";
    }

    let recommended =
      corr?.recommendedCorrection ||
      corr?.recommendedAction ||
      null;
    if (!recommended && a.assessmentStatus === "ACCESS_RESTRICTED") {
      recommended =
        "Investigate intended crawler access with brand/security owners; do not weaken legitimate bot protections without approval.";
    }
    if (!recommended && a.schemaOrgDetected === false && /WEBSITE/i.test(a.category || "")) {
      recommended =
        "Optional: add accurate Hotel/LodgingBusiness structured data after hotel fact sign-off.";
    }
    if (!recommended && a.assessmentStatus === "VERIFIED_CORRECT") {
      recommended = "No verified defect in this pass — keep facts aligned with the signed hotel source of truth.";
    }

    const researchStatus = a.researchStatus || null;
    const priority =
      corr?.priority ||
      ( /NEEDS|CONFLICT|INCONSIST|MISSING|CORRECTION/i.test(
        `${a.assessmentStatus || ""} ${verifiedFinding} ${recommended || ""}`
      )
        ? "HIGH"
        : "MEDIUM");

    let group = "additional";
    if (
      corr ||
      ["VERIFIED_INCONSISTENCY", "VERIFIED_MISSING_INFORMATION", "NEEDS_CORRECTION", "CONFLICTING_FACTS"].includes(
        a.assessmentStatus
      ) ||
      researchStatus === "VERIFIED_CORRECTION_REQUIRED"
    ) {
      group = "corrections";
    } else if (
      researchStatus === "VERIFIED_OPPORTUNITY" ||
      /outreach|inclusion|submit|request partner|listing opportunity/i.test(recommended || "")
    ) {
      group = "opportunities";
    } else if (
      (url &&
        ["VERIFIED_CORRECT", "VERIFIED_CONSISTENT", "ASSESSED"].includes(
          a.assessmentStatus
        )) ||
      researchStatus === "VERIFIED_CONSISTENT"
    ) {
      group = "consistent";
    } else if (
      ["IDENTIFIED_NOT_ASSESSED", "RESEARCH_INCOMPLETE"].includes(a.assessmentStatus) ||
      researchStatus === "RESEARCH_INCOMPLETE" ||
      researchStatus === "LISTING_NOT_FOUND_AFTER_RESEARCH"
    ) {
      group = "additional";
    } else if (url) {
      group = "opportunities";
    }

    rows.push({
      platform: a.platform || humanPlatform(a.category),
      category: a.category || null,
      url,
      assessmentStatus: a.assessmentStatus || "ASSESSED",
      assessmentStatusLabel: humanStatus(a.assessmentStatus || "ASSESSED"),
      researchStatus,
      verifiedFinding,
      recommendedCorrection: cust(recommended),
      responsibleParty: cust(
        corr?.responsibleOrganization === "RESPONSIBLE_PARTY_TO_CONFIRM"
          ? "Owner to confirm"
          : corr?.responsibleOrganization ||
              ( /BRAND/i.test(a.category || "") ? "Brand" : "Hotel management / marketing" )
      ),
      priority,
      evidenceLabel:
        a.evidenceLabel ||
        (url ? humanEvidenceLabel(a.category, url) : null),
      fieldAssessed: !["IDENTIFIED_NOT_ASSESSED", "NOT_FOUND", "RESEARCH_INCOMPLETE"].includes(
        a.assessmentStatus
      ),
      group,
    });
  }

  // Registry-only platforms not already represented
  for (const [cat, status] of Object.entries(registry)) {
    if (rows.some((r) => r.category === cat)) continue;
    if (status === "NOT_RELEVANT") continue;
    rows.push({
      platform: humanPlatform(cat),
      category: cat,
      url: null,
      assessmentStatus: status,
      assessmentStatusLabel: humanStatus(status),
      verifiedFinding:
        status === "IDENTIFIED_NOT_ASSESSED"
          ? "Relevant platform identified; listing URL was not field-audited in this research pass. This is not a finding that the listing is incorrect."
          : status === "ACCESS_RESTRICTED"
            ? "Access restricted during automated review."
            : "Status recorded from coverage registry.",
      recommendedCorrection:
        status === "IDENTIFIED_NOT_ASSESSED"
          ? "Schedule a field assessment when listing URL is confirmed — do not invent corrections."
          : "Follow hotel-approved process for this platform.",
      responsibleParty: "Owner to confirm",
      fieldAssessed: false,
    });
  }

  return rows;
}

export function buildDigitalAuthorityV3Extras(payload) {
  const da = payload.digitalAuthority || {};
  const backlinks = (da.sources || []).filter(
    (s) => s.findingClass === "VERIFIED_BACKLINK"
  );
  const mentions = (da.sources || []).filter(
    (s) => s.findingClass === "VERIFIED_EXTERNAL_MENTION"
  );
  const hosts = uniqueHosts(backlinks);
  const highValue = backlinks.filter((s) =>
    /visit|tourism|cvb|convention|golf|spa|destination|chamber|comercial|houses\.com|illesbalears|infogal|paxinas/i.test(
      `${s.referringOrganization || ""} ${linkingUrl(s) || ""}`
    )
  );

  return {
    uniqueReferringWebsites: hosts.length,
    uniqueReferringWebsiteList: hosts.slice(0, 12),
    highValueDestinationOrIndustryCount: highValue.length,
    backlinkCount: backlinks.length,
    mentionCount: mentions.length,
    distinguishNote:
      "Verified website links (backlinks) are different from mentions without hyperlinks, and both are different from measured AI citations in ADP.",
  };
}

/**
 * Ensure actions expose V3 execution fields without inventing platforms.
 */
export function enrichActionsForV3(payload) {
  return (payload.actions || []).map((a) => {
    const urls = a.technicalDetails?.urls || a.supportingEvidence || [];
    const primary = urls[0] || null;
    const affectedPlatform =
      a.affectedPlatform ||
      primary?.label ||
      (primary?.url ? hostOf(primary.url) : null) ||
      "Hotel digital presence";
    const specificIssue =
      a.specificIssue ||
      (a.whatWeFound ? String(a.whatWeFound).split(/(?<=\.)\s+/)[0] : a.headline);
    return {
      ...a,
      headline: a.headline || a.title,
      whatWeFound: cust(a.whatWeFound),
      whyItMatters: cust(a.whyItMatters),
      affectedPlatform: cust(affectedPlatform),
      specificIssue: cust(specificIssue),
      commercialImportance: cust(a.whyItMatters || a.businessRelevance || ""),
      accountableOwner: a.accountableOwner || a.responsibleParty || "Hotel leadership",
      suggestedTimeframe: a.suggestedTimeframe || "To be scheduled after hotel approval",
    };
  });
}

export function buildImplementationByOwner(actions) {
  const groups = new Map();
  for (const g of OWNER_GROUPS) {
    groups.set(g.key, { label: g.label, items: [] });
  }
  groups.set("other", { label: "Other / confirm owner", items: [] });

  for (const a of actions || []) {
    const bucket = ownerBucket(a.accountableOwner || a.responsibleParty);
    const g = groups.get(bucket.key) || groups.get("other");
    const steps = Array.isArray(a.recommendedSteps)
      ? a.recommendedSteps
      : a.technicalDetails?.changesOrInvestigation || [];
    g.items.push({
      actionId: a.actionId,
      headline: a.headline,
      affectedPlatform: a.affectedPlatform,
      specificIssue: a.specificIssue,
      steps: steps.slice(0, 6).map(cust),
      completionCriteria: a.completionCriteria || null,
      evidenceUrls: (a.technicalDetails?.urls || [])
        .map((u) => u.url)
        .filter(Boolean)
        .slice(0, 3),
    });
  }

  return [...groups.values()].filter((g) => g.items.length > 0);
}

/**
 * Apply V3 enrichment onto a payload (mutates copy).
 */
export function applyCustomerReportV3Enrichment(payload) {
  const next = JSON.parse(JSON.stringify(payload));
  next.contractVersion = next.contractVersion || "AI_VISIBILITY_AUDIT_ACTION_PLAN_V1";
  next.customerReportV3 = {
    contract: AVA_CUSTOMER_REPORT_V3_CONTRACT,
    generatedAt: new Date().toISOString(),
    executiveSummaryPreserved: true,
    adpDetailNotDuplicated: true,
  };

  next.keyFindings = enrichKeyFindings(next);
  next.findingsIntro = cust(
    "The findings below diagnose verified digital-presence issues that affect how travelers and partners understand this hotel. Detailed platform rows and authority evidence follow. Measured AI traveler-intent results remain in ADP and are not repeated here."
  );

  const platformRows = buildPlatformDetailRows(next);
  const groups = {
    corrections: platformRows.filter((r) => r.group === "corrections"),
    opportunities: platformRows.filter((r) => r.group === "opportunities"),
    consistent: platformRows.filter((r) => r.group === "consistent"),
    additional: platformRows.filter((r) => r.group === "additional"),
  };
  next.digitalPlatformReadinessV3 = {
    title: "Digital Platform & Website Readiness",
    intro:
      "Verified corrections and inclusion opportunities are listed first. Platforms marked as additional were researched but not fully field-assessed — that is not a claim the listing is wrong.",
    rows: platformRows,
    groups,
    correctionMap: [...groups.corrections, ...groups.opportunities].map((r) => ({
      platform: r.platform,
      exactPage: r.url,
      verifiedFinding: r.verifiedFinding,
      recommendedChange: r.recommendedCorrection,
      priority: r.priority,
      responsibleParty: r.responsibleParty,
      evidenceUrl: r.url,
      evidenceLabel: r.evidenceLabel,
    })),
  };

  // Keep DCR in sync for renderer fallback
  if (next.digitalContentReadiness) {
    next.digitalContentReadiness.platformDetailRows = platformRows;
    next.digitalContentReadiness.sectionTitleCustomer =
      "Digital Platform & Website Readiness";
  }

  next.digitalAuthority = {
    ...(next.digitalAuthority || {}),
    v3Extras: buildDigitalAuthorityV3Extras(next),
  };

  next.actions = enrichActionsForV3(next);

  // Humanize evidence labels after action enrichment
  next.keyFindings = (next.keyFindings || []).map((f) => ({
    ...f,
    evidence: (f.evidence || []).map((e) => ({
      ...e,
      label: humanizeEvidenceLabel(e),
    })),
  }));
  next.actions = (next.actions || []).map((a) => ({
    ...a,
    supportingEvidence: (a.supportingEvidence || []).map((e) => ({
      ...e,
      label: humanizeEvidenceLabel(e),
    })),
    technicalDetails: a.technicalDetails
      ? {
          ...a.technicalDetails,
          urls: (a.technicalDetails.urls || []).map((e) => ({
            ...e,
            label: humanizeEvidenceLabel(e),
          })),
          evidence: (a.technicalDetails.evidence || []).map((e) => ({
            ...e,
            label: humanizeEvidenceLabel(e),
          })),
        }
      : a.technicalDetails,
  }));

  // Always use the V3 intro — older payloads already had a shorter actionPlanIntro.
  next.actionPlanIntro = cust(
    "Each action names the affected platform, the verified issue, evidence, owner, timing, and completion criteria. Implementation steps are grouped by responsible party after the action cards."
  );

  next.implementationByOwnerV3 = buildImplementationByOwner(next.actions);

  if (next.methodology) {
    next.methodology.implementationByOwnerNote =
      "Use the Implementation by Responsible Party section for page-specific execution notes. Do not treat unassessed platforms as confirmed defects.";
  }

  return next;
}

function humanizeEvidenceLabel(e) {
  if (!e) return e;
  const url = e.url || "";
  const raw = String(e.label || "");
  if (/^https?:\/\//i.test(raw) || !raw || /^evidence$/i.test(raw)) {
    return humanEvidenceLabel(detectCategoryFromUrl(url), url) || raw || "View source";
  }
  // Soften long raw URL-ish labels
  if (raw.length > 80 && /hilton|marriott|tripadvisor|booking/i.test(raw)) {
    return humanEvidenceLabel(detectCategoryFromUrl(url), url);
  }
  return raw;
}

function detectCategoryFromUrl(url) {
  const u = String(url || "");
  if (/tripadvisor\./i.test(u)) return "TRIPADVISOR";
  if (/booking\.com/i.test(u)) return "OTA_BOOKING";
  if (/expedia\.|hotels\.com/i.test(u)) return "OTA_EXPEDIA";
  if (/maps\.google|google\.com\/maps|google\.com\/travel/i.test(u)) {
    return "GOOGLE_BUSINESS_PROFILE";
  }
  if (/hilton\.com\/.*hotels|marriott\.com\/.*hotels/i.test(u)) {
    return "BRAND_PROPERTY_PAGE";
  }
  if (/meeting/i.test(u)) return "HOTEL_OPERATED_SUBPAGE";
  if (/waterstoneboca|hotelacacoruna|castillosonvida/i.test(u)) {
    return "HOTEL_OPERATED_WEBSITE";
  }
  if (/visit-|cvb|convention|turismo/i.test(u)) return "DESTINATION_CVB";
  return null;
}

export function assertExecutiveSummaryStructureIntact(before, after) {
  const b = before?.executiveSummary || {};
  const a = after?.executiveSummary || {};
  const keys = [
    "headline",
    "assessment",
    "keyFindings",
    "priorities",
    "takeaway",
    "closing",
  ];
  const issues = [];
  for (const k of keys) {
    if (b[k] != null && a[k] == null) {
      issues.push(`executiveSummary.${k} removed`);
    }
  }
  if ((before?.kpis && !after?.kpis) || (before?.kpis && after?.kpis && Object.keys(before.kpis).length !== Object.keys(after.kpis).length && Object.keys(after.kpis).length < Object.keys(before.kpis).length)) {
    // allow scrub-only changes; only fail if KPIs wiped
    if (!after?.kpis || !Object.keys(after.kpis).length) {
      issues.push("kpis removed");
    }
  }
  return { ok: issues.length === 0, issues };
}
