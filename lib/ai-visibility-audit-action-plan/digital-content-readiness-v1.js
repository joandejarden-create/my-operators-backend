/**
 * AVA AI Discoverability & Digital Content Readiness V1
 *
 * Permanent Full E2E module: discover brand vs hotel-operated sources,
 * Hotel Facts Register, traveler-question coverage, cross-platform
 * consistency, and platform-specific correction ownership.
 *
 * Does not invent AI ranking causes. Distinguishes technical access
 * failures from factual defects. Idempotent merge into AVA payloads.
 */
import { normalizeDigitalAuthorityActionForPdf } from "./digital-authority-v1.js";
import {
  buildCorrectionRegisterFromDcr,
  attachCorrectionRegisterToPayload,
} from "./digital-platform-correction-register-v1.js";

export const DIGITAL_CONTENT_READINESS_CONTRACT =
  "AVA_DIGITAL_CONTENT_READINESS_V1";

export const DCR_SOURCE_CATEGORIES = Object.freeze({
  BRAND_PROPERTY_PAGE: "BRAND_PROPERTY_PAGE",
  HOTEL_OPERATED_WEBSITE: "HOTEL_OPERATED_WEBSITE",
  BOOKING_ENGINE: "BOOKING_ENGINE",
  HOTEL_OPERATED_SUBPAGE: "HOTEL_OPERATED_SUBPAGE",
  GOOGLE_BUSINESS_PROFILE: "GOOGLE_BUSINESS_PROFILE",
  TRIPADVISOR: "TRIPADVISOR",
  OTA_BOOKING: "OTA_BOOKING",
  OTA_EXPEDIA: "OTA_EXPEDIA",
  APPLE_MAPS: "APPLE_MAPS",
  BING_PLACES: "BING_PLACES",
  CVENT: "CVENT",
  DESTINATION_CVB: "DESTINATION_CVB",
  TOURISM_BOARD: "TOURISM_BOARD",
  SPECIALIST_DIRECTORY: "SPECIALIST_DIRECTORY",
  OTHER: "OTHER",
});

export const DCR_QUESTION_CLASSES = Object.freeze({
  ANSWERED_AND_VERIFIED: "ANSWERED_AND_VERIFIED",
  PARTIALLY_ANSWERED: "PARTIALLY_ANSWERED",
  INCONSISTENT_INFORMATION: "INCONSISTENT_INFORMATION",
  MISSING_INFORMATION: "MISSING_INFORMATION",
  UNABLE_TO_VERIFY: "UNABLE_TO_VERIFY",
});

export const DCR_CONSISTENCY_CLASSES = Object.freeze({
  CONSISTENT: "CONSISTENT",
  INCONSISTENT: "INCONSISTENT",
  MISSING: "MISSING",
  OUTDATED: "OUTDATED",
  HOTEL_CONFIRMATION_REQUIRED: "HOTEL_CONFIRMATION_REQUIRED",
  UNABLE_TO_VERIFY: "UNABLE_TO_VERIFY",
});

export const DCR_CORRECTION_STATUSES = Object.freeze({
  IDENTIFIED: "IDENTIFIED",
  FACT_CONFIRMATION: "FACT_CONFIRMATION",
  ASSIGNED: "ASSIGNED",
  SUBMITTED: "SUBMITTED",
  IMPLEMENTED: "IMPLEMENTED",
  VERIFIED: "VERIFIED",
  NO_CHANGE_REQUIRED: "NO_CHANGE_REQUIRED",
  DEFERRED: "DEFERRED",
  UNABLE_TO_VERIFY: "UNABLE_TO_VERIFY",
});

export const DCR_RESPONSIBILITY = Object.freeze({
  HOTEL_OPERATIONS: "Hotel Operations",
  HOTEL_SALES_MARKETING: "Hotel Sales & Marketing",
  HOTEL_GM: "Hotel General Manager",
  BRAND_CONTENT: "Brand Content Team",
  DIGITAL_AGENCY: "Digital Marketing Agency",
  WEBSITE_DEVELOPER: "Website Developer",
  REVENUE_DISTRIBUTION: "Revenue / Distribution",
  PR_PARTNERSHIPS: "PR / Partnerships",
  EXTERNAL_PLATFORM_ADMIN: "External Platform Administrator",
  TO_CONFIRM: "RESPONSIBLE_PARTY_TO_CONFIRM",
});

export function emptyDigitalContentReadinessBlock(opts = {}) {
  return {
    contract: DIGITAL_CONTENT_READINESS_CONTRACT,
    moduleId: "M11_DIGITAL_CONTENT_READINESS",
    propertyId: opts.propertyId || null,
    hotelName: opts.hotelName || null,
    researchMode: "PUBLIC_PARTIAL",
    paidProviderUsed: false,
    paidResearchUsd: 0,
    executiveAssessment: "",
    readinessSummary: {
      websiteAccessibility: "NOT_ASSESSED",
      contentCompleteness: "NOT_ASSESSED",
      structuredInformation: "NOT_ASSESSED",
      travelerQuestionCoverage: "NOT_ASSESSED",
      thirdPartyConsistency: "NOT_ASSESSED",
    },
    sourceInventory: [],
    hotelFactsRegister: [],
    websiteDiscoverability: {
      brandPropertyPage: null,
      hotelOperatedWebsite: null,
    },
    travelerQuestionCoverage: [],
    platformConsistency: [],
    platformCorrections: [],
    actions: [],
    methodologyNotes: [
      "Brand property pages and hotel-operated websites are assessed independently.",
      "A bot-blocked request is not proof that search engines cannot access a page.",
      "Missing content is a plausible improvement opportunity unless causal evidence exists.",
      "A brand page is not automatically authoritative for every attribute.",
    ],
    limitations: [
      "Public partial research only unless paid providers are founder-authorized.",
      "Platform ownership is confirmed only with evidence; otherwise RESPONSIBLE_PARTY_TO_CONFIRM.",
    ],
    generatedAt: new Date().toISOString(),
  };
}

function keyOf(parts) {
  return parts
    .map((p) => String(p || "").trim().toLowerCase())
    .filter(Boolean)
    .join("|");
}

function dedupeByKey(items, keyFn) {
  const seen = new Set();
  const out = [];
  for (const item of items || []) {
    const k = keyFn(item);
    if (!k || seen.has(k)) continue;
    seen.add(k);
    out.push(item);
  }
  return out;
}

export function collectDigitalContentReadinessEvidenceUrls(dcr) {
  const urls = [];
  const push = (u) => {
    if (u && /^https?:\/\//i.test(String(u))) urls.push(String(u));
  };
  for (const s of dcr?.sourceInventory || []) {
    push(s.url);
    push(s.evidenceUrl);
  }
  for (const f of dcr?.hotelFactsRegister || []) {
    push(f.evidenceUrl);
  }
  for (const q of dcr?.travelerQuestionCoverage || []) {
    push(q.brandPageUrl);
    push(q.hotelSiteUrl);
    for (const e of q.evidenceLinks || []) push(e.url || e);
  }
  for (const c of dcr?.platformConsistency || []) {
    push(c.sourceUrl);
    push(c.evidenceUrl);
  }
  for (const c of dcr?.platformCorrections || []) {
    push(c.url);
    push(c.evidenceUrl);
    for (const e of c.supportingEvidence || []) push(e.url || e);
  }
  for (const a of dcr?.actions || []) {
    for (const e of a.evidenceLinks || []) push(e.url || e);
    for (const e of a.supportingEvidence || []) push(e.url || e);
  }
  const wd = dcr?.websiteDiscoverability || {};
  push(wd.brandPropertyPage?.url);
  push(wd.hotelOperatedWebsite?.url);
  return [...new Set(urls)];
}

export function finalizeDigitalContentReadinessBlock(block = {}, _auth = {}) {
  const out = {
    ...emptyDigitalContentReadinessBlock(block),
    ...block,
    contract: DIGITAL_CONTENT_READINESS_CONTRACT,
    moduleId: "M11_DIGITAL_CONTENT_READINESS",
    paidProviderUsed: false,
    paidResearchUsd: 0,
    researchMode: "PUBLIC_PARTIAL",
  };

  out.sourceInventory = dedupeByKey(out.sourceInventory || [], (s) =>
    keyOf([s.sourceCategory, s.url, s.platformName || s.label])
  );
  out.hotelFactsRegister = dedupeByKey(out.hotelFactsRegister || [], (f) =>
    keyOf([f.attributeId || f.attribute, f.source])
  );
  out.travelerQuestionCoverage = dedupeByKey(
    out.travelerQuestionCoverage || [],
    (q) => keyOf([q.questionId || q.travelerIntent])
  );
  out.platformConsistency = dedupeByKey(out.platformConsistency || [], (c) =>
    keyOf([c.attribute, c.platform, c.sourceUrl])
  );
  out.platformCorrections = dedupeByKey(out.platformCorrections || [], (c) =>
    keyOf([c.findingId || c.correctionId, c.platform, c.url])
  ).map((c) => ({
    status: c.status || DCR_CORRECTION_STATUSES.IDENTIFIED,
    responsibleOrganization:
      c.responsibleOrganization || DCR_RESPONSIBILITY.TO_CONFIRM,
    ...c,
  }));
  out.actions = dedupeByKey(out.actions || [], (a) =>
    String(a.actionId || "").toLowerCase()
  ).map(normalizeDigitalAuthorityActionForPdf);
  out.generatedAt = out.generatedAt || new Date().toISOString();
  return out;
}

/**
 * Idempotent merge of Digital Content Readiness into AVA payload.
 * Dedupes actions against existing actionIds (including Digital Authority).
 */
export function appendDigitalContentReadinessActions(
  payload,
  digitalContentReadiness,
  _authOptions = {}
) {
  const dcr = finalizeDigitalContentReadinessBlock(digitalContentReadiness);
  const actionsById = new Map();
  for (const a of payload.actions || []) {
    if (a?.actionId) actionsById.set(a.actionId, a);
  }

  for (const a of dcr.actions || []) {
    if (!a?.actionId) continue;
    const existing = actionsById.get(a.actionId);
    if (existing) {
      const decision =
        existing.hotelDecision ||
        existing.founderDecision ||
        existing.decision ||
        null;
      if (decision && /APPROVE|APPROVED|REJECT|DEFER/i.test(String(decision))) {
        actionsById.set(a.actionId, {
          ...a,
          hotelDecision: existing.hotelDecision,
          founderDecision: existing.founderDecision,
          decision: existing.decision,
          decisionAt: existing.decisionAt,
          revisionNote:
            "Digital Content Readiness refreshed; prior decision preserved for review.",
        });
      } else {
        // Prefer richer existing non-DCR action copy when IDs collide with core AVA actions
        if (
          String(existing.actionId || "").includes("-DA-") ||
          String(existing.actionId || "").includes("-DCR-")
        ) {
          actionsById.set(a.actionId, { ...existing, ...a });
        } else {
          // Do not overwrite core AVA actions with DCR duplicates — skip
          continue;
        }
      }
    } else {
      actionsById.set(a.actionId, a);
    }
  }

  const overviewById = new Map();
  for (const row of payload.actionOverview || []) {
    if (row?.actionId) overviewById.set(row.actionId, row);
  }
  for (const a of dcr.actions || []) {
    if (!a?.actionId || !actionsById.has(a.actionId)) continue;
    if (overviewById.has(a.actionId)) continue;
    overviewById.set(a.actionId, {
      actionId: a.actionId,
      priority: a.priority,
      headline: a.headline,
      businessRelevance: a.whyItMatters || a.priorityWhy,
      accountableOwner: a.accountableOwner,
      suggestedTimeframe: a.suggestedTimeframe,
    });
  }

  const merged = {
    ...payload,
    digitalContentReadiness: dcr,
    actions: [...actionsById.values()],
    actionOverview: [...overviewById.values()],
  };
  return attachCorrectionRegisterToPayload(
    merged,
    buildCorrectionRegisterFromDcr(merged)
  );
}

export function summarizeDigitalContentReadiness(dcr) {
  const sources = dcr?.sourceInventory || [];
  const brandPage = sources.some(
    (s) => s.sourceCategory === DCR_SOURCE_CATEGORIES.BRAND_PROPERTY_PAGE
  );
  const hotelSite = sources.some(
    (s) => s.sourceCategory === DCR_SOURCE_CATEGORIES.HOTEL_OPERATED_WEBSITE
  );
  const questions = dcr?.travelerQuestionCoverage || [];
  const consistency = dcr?.platformConsistency || [];
  const corrections = dcr?.platformCorrections || [];
  const confirmations = [
    ...(dcr?.hotelFactsRegister || []).filter(
      (f) =>
        f.authorityStatus === "HOTEL_CONFIRMATION_REQUIRED" ||
        f.hotelConfirmationRequired === true
    ),
    ...consistency.filter(
      (c) => c.classification === DCR_CONSISTENCY_CLASSES.HOTEL_CONFIRMATION_REQUIRED
    ),
  ];
  const gaps = questions.filter((q) =>
    [
      DCR_QUESTION_CLASSES.MISSING_INFORMATION,
      DCR_QUESTION_CLASSES.INCONSISTENT_INFORMATION,
      DCR_QUESTION_CLASSES.PARTIALLY_ANSWERED,
    ].includes(q.classification)
  );
  const discrepancies = consistency.filter((c) =>
    [
      DCR_CONSISTENCY_CLASSES.INCONSISTENT,
      DCR_CONSISTENCY_CLASSES.OUTDATED,
      DCR_CONSISTENCY_CLASSES.MISSING,
    ].includes(c.classification)
  );

  return {
    platformsIdentified: sources.length,
    brandPageFound: brandPage,
    hotelOperatedWebsiteFound: hotelSite,
    travelerQuestionsAssessed: questions.length,
    materialContentGaps: gaps.length,
    verifiedDiscrepancies: discrepancies.length,
    hotelConfirmationsRequired: confirmations.length,
    platformSpecificCorrections: corrections.length,
    readinessSummary: dcr?.readinessSummary || null,
  };
}
