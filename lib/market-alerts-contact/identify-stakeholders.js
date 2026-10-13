/**
 * Zero-Surfe stakeholder identification for Market Alerts.
 * Persists Dealality-owned identity only — never calls Surfe.
 */

import { assessContactEnrichmentEligibility, CONTACT_BLOCK_REASONS } from "./eligibility.js";
import { stakeholderRoleLabels } from "./stakeholder-roles.js";
import {
  IDENTIFICATION_STATUS,
  CONTACT_LOOKUP_STATUS,
  STAKEHOLDER_SOURCE_TYPE,
  buildStakeholderStableKey,
} from "./stakeholder-schema.js";

/**
 * Identify stakeholders for one alert. Zero provider calls.
 * @returns {{
 *   eligible: boolean,
 *   identificationStatus: string,
 *   moduleVisible: boolean,
 *   stakeholders: object[],
 *   skipReason: string|null,
 *   surfeCalls: 0,
 *   categoryHint: string|null
 * }}
 */
export function identifyAlertStakeholders(alert = {}) {
  const assessment = assessContactEnrichmentEligibility(alert);
  const article = assessment.articleStakeholders || {};
  const people = Array.isArray(article.people) ? article.people : [];
  const commercialOrgs = assessment.organizations || [];
  const categoryHint = alert.category || alert.intelligence?.category || null;

  if (!assessment.eligible) {
    const status = mapSkipToIdentificationStatus(assessment.reason);
    return {
      eligible: false,
      identificationStatus: status,
      moduleVisible: false,
      stakeholders: [],
      skipReason: assessment.reason,
      surfeCalls: 0,
      categoryHint,
      commercialOrganizations: [],
      targetRoles: [],
      stakeholderExtractionSource: article.stakeholderExtractionSource || null,
      stakeholderExtractionStatus: article.stakeholderExtractionStatus || null,
    };
  }

  const targetCompany = commercialOrgs[0] || assessment.namedStakeholder?.companyName || null;
  const roleLabels = stakeholderRoleLabels(assessment.roles || []);
  const suggestedRole = roleLabels[0] || "VP / Head of Development";

  // Prefer selected enrichment candidates; fall back to ranked commercial people.
  const selected = (article.selectedForEnrichment || []).filter((p) => p?.personName);
  let candidates = selected.length ? selected.slice(0, 5) : [];
  if (!candidates.length) {
    candidates = people
      .filter(
        (p) =>
          p?.personName &&
          p.decisionMakerConfidence !== "LOW" &&
          !["GOVERNMENT", "ANALYST_CONSULTANT", "PR_MEDIA", "ARCHITECT_CONTRACTOR"].includes(
            p.stakeholderClassification
          )
      )
      .slice(0, 5);
  }

  const alertId = alert.id || alert.alertId || null;
  const now = new Date().toISOString();
  const stakeholders = [];
  const extractionMeta = {
    stakeholderExtractionSource:
      article.stakeholderExtractionSource || alert.stakeholderExtractionSource || null,
    stakeholderExtractionStatus:
      article.stakeholderExtractionStatus || alert.stakeholderExtractionStatus || null,
  };

  if (candidates.length >= 1 && candidates[0].personName) {
    const status =
      !selected.length && candidates.length > 1
        ? IDENTIFICATION_STATUS.MULTIPLE_CANDIDATES
        : IDENTIFICATION_STATUS.IDENTIFIED;

    candidates.forEach((person, idx) => {
      if (!person?.personName) return;
      stakeholders.push(
        buildStakeholderRow({
          alert,
          alertId,
          personName: person.personName,
          jobTitle: person.jobTitle || null,
          company: person.companyName || targetCompany,
          stakeholderClass: person.stakeholderClassification || "OTHER",
          relationshipToProject: person.relationshipToProject || null,
          whyRelevant: person.whyPerson || person.relevanceReason || "Decision-maker for this project",
          articleDerived: true,
          quoted: person.isQuoted === true,
          confidence: person.decisionMakerConfidence || "MEDIUM",
          identificationStatus: status,
          selectedPrimary: idx === 0,
          selectedSecondary: idx === 1,
          decisionOpen: inferDecisionOpen(alert),
          sourceType: STAKEHOLDER_SOURCE_TYPE.ARTICLE,
          articleMentionCount: person.articleMentionCount || 1,
          now,
        })
      );
    });

    return {
      eligible: true,
      identificationStatus: status,
      moduleVisible: true,
      stakeholders,
      skipReason: null,
      surfeCalls: 0,
      categoryHint,
      commercialOrganizations: commercialOrgs,
      targetRoles: roleLabels,
      targetCompany,
      suggestedRole,
      ...extractionMeta,
    };
  }

  if (targetCompany) {
    stakeholders.push(
      buildStakeholderRow({
        alert,
        alertId,
        personName: null,
        jobTitle: suggestedRole,
        company: targetCompany,
        stakeholderClass: inferCompanyClass(assessment),
        relationshipToProject: "Commercial organization behind this project",
        whyRelevant: `Developer / owner organization for outreach — suggested role: ${suggestedRole}`,
        articleDerived: false,
        quoted: false,
        confidence: "MEDIUM",
        identificationStatus: IDENTIFICATION_STATUS.COMPANY_ONLY,
        selectedPrimary: true,
        selectedSecondary: false,
        decisionOpen: inferDecisionOpen(alert),
        sourceType: STAKEHOLDER_SOURCE_TYPE.DEALALITY_INFER,
        articleMentionCount: 0,
        now,
      })
    );

    return {
      eligible: true,
      identificationStatus: IDENTIFICATION_STATUS.COMPANY_ONLY,
      moduleVisible: true,
      stakeholders,
      skipReason: null,
      surfeCalls: 0,
      categoryHint,
      commercialOrganizations: commercialOrgs,
      targetRoles: roleLabels,
      targetCompany,
      suggestedRole,
      ...extractionMeta,
    };
  }

  return {
    eligible: false,
    identificationStatus: IDENTIFICATION_STATUS.BLOCKED_ENTITY_UNKNOWN,
    moduleVisible: false,
    stakeholders: [],
    skipReason: CONTACT_BLOCK_REASONS.ENTITY_UNKNOWN,
    surfeCalls: 0,
    categoryHint,
    commercialOrganizations: [],
    targetRoles: roleLabels,
    ...extractionMeta,
  };
}

function mapSkipToIdentificationStatus(reason) {
  if (reason === CONTACT_BLOCK_REASONS.ENTITY_UNKNOWN) {
    return IDENTIFICATION_STATUS.BLOCKED_ENTITY_UNKNOWN;
  }
  return IDENTIFICATION_STATUS.NOT_APPLICABLE;
}

function inferDecisionOpen(alert) {
  const eventType = alert.eventType || alert.intelligence?.eventType || "";
  const signalType = alert.signalType || alert.intelligence?.signalType || "";
  if (/Brand Signing|Reflag|Conversion|Operator Appointment|Management Agreement/i.test(eventType)) {
    return false;
  }
  if (/Competitive Brand Move|Competitive Operator Move|Management Agreement Announced/i.test(signalType)) {
    return false;
  }
  return true;
}

function inferCompanyClass(assessment) {
  const roles = assessment.roles || [];
  if (roles.some((r) => /owner|developer/i.test(String(r.label || r.id || "")))) {
    return "DEVELOPER_PRINCIPAL";
  }
  return "OTHER";
}

function buildStakeholderRow({
  alert,
  alertId,
  personName,
  jobTitle,
  company,
  stakeholderClass,
  relationshipToProject,
  whyRelevant,
  articleDerived,
  quoted,
  confidence,
  identificationStatus,
  selectedPrimary,
  selectedSecondary,
  decisionOpen,
  sourceType,
  articleMentionCount,
  now,
}) {
  const stakeholderId = buildStakeholderStableKey({
    alertId,
    personName,
    company,
    stakeholderClass,
    jobTitle,
  });
  return {
    stakeholderId,
    alertId,
    personName: personName || null,
    jobTitle: jobTitle || null,
    company: company || null,
    stakeholderClass: stakeholderClass || "OTHER",
    relationshipToProject: relationshipToProject || null,
    whyRelevant: whyRelevant || null,
    articleDerived: articleDerived === true,
    quoted: quoted === true,
    confidence: confidence || "MEDIUM",
    identificationStatus,
    selectedPrimary: selectedPrimary === true,
    selectedSecondary: selectedSecondary === true,
    decisionOpen: decisionOpen !== false,
    sourceType,
    identifiedAt: now,
    updatedAt: now,
    articleMentionCount: Number(articleMentionCount) || 0,
    sourceArticleUrl: alert.sourceUrl || null,
    sourceArticleTitle: alert.title || null,
    contactLookupStatus: CONTACT_LOOKUP_STATUS.NOT_REQUESTED,
  };
}

/**
 * Shape for authenticated GET /contacts — no Surfe PII.
 */
export function toStakeholderUiCard(row = {}) {
  const status = row.identificationStatus || IDENTIFICATION_STATUS.NOT_APPLICABLE;
  const hasPerson = Boolean(row.personName);
  const hasCompany = Boolean(row.company || row.companyName);
  return {
    stakeholderId: row.stakeholderId || row.id || null,
    recordId: row.recordId || null,
    personName: row.personName || null,
    jobTitle: row.jobTitle || null,
    companyName: row.company || row.companyName || null,
    stakeholderClass: row.stakeholderClass || null,
    stakeholderRole: row.stakeholderClass || row.jobTitle || null,
    whyRelevant: row.whyRelevant || row.reasonSelected || null,
    relationshipToProject: row.relationshipToProject || null,
    confidence: row.confidence || row.matchConfidence || null,
    identificationStatus: status,
    articleDerived: row.articleDerived === true,
    quoted: row.quoted === true,
    selectedPrimary: row.selectedPrimary === true,
    selectedSecondary: row.selectedSecondary === true,
    decisionOpen: row.decisionOpen !== false,
    contactLookupStatus: row.contactLookupStatus || CONTACT_LOOKUP_STATUS.NOT_REQUESTED,
    // Dealality-resolved LinkedIn only (never Surfe-derived)
    linkedinUrl:
      row.linkedinResolutionSource === "DEALALITY_PUBLIC_RESEARCH" && row.linkedinUrl
        ? row.linkedinUrl
        : null,
    linkedinResolutionStatus: row.linkedinResolutionStatus || null,
    linkedinResolutionConfidence: row.linkedinResolutionConfidence || null,
    // CTA: person+company → Get contact details (no email/phone/LinkedIn prerequisite)
    cta:
      status === IDENTIFICATION_STATUS.COMPANY_ONLY || !hasPerson
        ? "FIND_DECISION_MAKER"
        : hasPerson && hasCompany
          ? "GET_CONTACT_DETAILS"
          : hasPerson
            ? "GET_CONTACT_DETAILS"
            : "FIND_DECISION_MAKER",
    // Explicitly never include Surfe contact details on this shape
    email: undefined,
    phone: undefined,
  };
}
