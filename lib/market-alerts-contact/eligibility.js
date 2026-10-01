/**
 * Contact enrichment eligibility for Market Alerts (Phase A).
 */

import { assessConsumerFluff } from "../market-alerts-consumer-fluff.js";
import { inferStakeholderRoles } from "./stakeholder-roles.js";
import { extractArticleStakeholders } from "./article-stakeholder-extract.js";

export const CONTACT_BLOCK_REASONS = Object.freeze({
  DISABLED: "CONTACT_ENRICHMENT_DISABLED",
  CONSUMER_FLUFF: "CONSUMER_FLUFF",
  NOT_WORTH_REVIEWING: "NOT_WORTH_REVIEWING",
  NOT_ACTIONABLE_OR_NAMED_WATCH: "NOT_ACTIONABLE_OR_NAMED_WATCH",
  ENTITY_UNKNOWN: "CONTACT_RESEARCH_BLOCKED_ENTITY_UNKNOWN",
  NO_ROLES: "NO_TARGET_ROLES",
  QUOTA: "MAX_ENRICHMENTS_REACHED",
  PROVIDER_CAP: "CONTACT_PROVIDER_RUN_CAP_REACHED",
  PERSISTENCE_BLOCKED: "CONTACT_PERSISTENCE_BLOCKED",
});

export const COMMERCIAL_ORG_CONFIDENCE = Object.freeze({
  HIGH: "HIGH",
  MEDIUM: "MEDIUM",
  LOW: "LOW",
});

/** Entities that must never alone trigger Surfe people search. */
const NON_COMMERCIAL_ORG_RE =
  /\b(?:tourism\s+(?:board|authority|ministry|department|office)|ministry\s+of\s+tourism|destination\s+authority|municipality|city\s+(?:council|hall|of)|county\s+of|government|chamber\s+of\s+commerce|convention\s+(?:and\s+)?visitors?\s+bureau|\bcvb\b|press\s+office|media\s+relations|public\s+relations|pr\s+agency|newsroom|newspaper|magazine|journal\b|university|think\s+tank|research\s+(?:firm|institute|company)|market\s+research|consultancy|consulting\s+(?:firm|group)|analyst\s+(?:firm|house))\b/i;

const COMMERCIAL_ORG_SIGNAL_RE =
  /\b(?:hospitality|hotels?|resorts?|developer|development|holdings?|partners?(?:\s+llc)?|capital|invest(?:ment|ors?)?|asset\s+management|realty|properties|property\s+group|lodging|owner|operator|management\s+(?:company|group)|group|llc|ltd|limited|inc\.?|corp\.?|gmbh|sas)\b/i;

/**
 * Plan eligibility (roles/orgs/named person). Does NOT gate on CONTACT_ENRICHMENT_ENABLED —
 * that flag only blocks paid provider calls in the orchestrator.
 */
export function assessContactEnrichmentEligibility(alert = {}) {
  const title = alert.title || "";
  const summary = alert.summary || "";
  const fluff = assessConsumerFluff({ title, summary });
  if (fluff.reject) {
    return empty(false, CONTACT_BLOCK_REASONS.CONSUMER_FLUFF);
  }

  const intel = alert.intelligence || {};
  const worth =
    intel.worthReviewing === true ||
    alert.worthReviewing === true ||
    alert.worthReviewingOwner === true ||
    alert.worthReviewingBrand === true ||
    alert.worthReviewingOperator === true;
  const actionable =
    intel.actionable === true ||
    alert.actionable === true ||
    alert.actionableOwner === true ||
    alert.actionableBrand === true ||
    alert.actionableOperator === true;

  if (!worth) {
    return empty(false, CONTACT_BLOCK_REASONS.NOT_WORTH_REVIEWING);
  }

  const eventType = intel.eventType || alert.eventType;
  const signalType = intel.signalType || alert.signalType;
  const closedBrand =
    ["Brand Signing", "Reflag", "Conversion"].includes(eventType) ||
    /Competitive Brand Move/i.test(String(signalType || ""));
  const closedOperator =
    ["Operator Appointment", "Management Agreement", "Operator Change"].includes(eventType) ||
    /Competitive Operator Move|Management Agreement Announced/i.test(String(signalType || ""));

  // Article stakeholder extraction BEFORE company+role Surfe search.
  const articleStakeholders = extractArticleStakeholders({
    ...alert,
    eventType,
    signalType,
    intelligence: intel,
  });

  const namedFromArticle = articleStakeholders.primaryCandidate
    ? {
        personName: articleStakeholders.primaryCandidate.personName,
        jobTitle: articleStakeholders.primaryCandidate.jobTitle,
        companyName: articleStakeholders.primaryCandidate.companyName,
        source: "article_stakeholder",
        articleDerived: true,
        isQuoted: articleStakeholders.primaryCandidate.isQuoted,
        stakeholderClassification: articleStakeholders.primaryCandidate.stakeholderClassification,
        decisionMakerConfidence: articleStakeholders.primaryCandidate.decisionMakerConfidence,
        whyPerson: articleStakeholders.primaryCandidate.whyPerson,
        enrichmentPath: articleStakeholders.primaryCandidate.enrichmentPath,
        selectedForEnrichment: true,
      }
    : pickWatchNamedFromPeople(articleStakeholders.people);

  const watchWithNamed = !actionable && worth && !!namedFromArticle?.personName;
  if (!actionable && !watchWithNamed) {
    return empty(false, CONTACT_BLOCK_REASONS.NOT_ACTIONABLE_OR_NAMED_WATCH, articleStakeholders);
  }

  const commercial = resolveCommercialOrganizations(alert, intel, {
    closedBrand,
    closedOperator,
  });

  // Article-selected person may supply their commercial company for direct enrich / search target.
  if (namedFromArticle?.companyName) {
    const scored = scoreCommercialOrganization(namedFromArticle.companyName, {
      kindHint: "article_stakeholder_company",
    });
    if (scored.commercial && scored.confidence !== COMMERCIAL_ORG_CONFIDENCE.LOW) {
      prependOrg(commercial, namedFromArticle.companyName, scored);
    }
  }

  if (namedFromArticle && !namedFromArticle.companyName && commercial.organizations[0]) {
    namedFromArticle.companyName = commercial.organizations[0];
  }

  const preferArticleStakeholder = Boolean(namedFromArticle);
  const needsPeopleSearchFallback = !preferArticleStakeholder;

  // People-search fallback requires HIGH/MEDIUM commercial org — never government/analyst-only.
  if (needsPeopleSearchFallback && !commercial.organizations.length) {
    return empty(false, CONTACT_BLOCK_REASONS.ENTITY_UNKNOWN, articleStakeholders);
  }

  // Direct article enrich still needs a resolvable commercial company.
  if (preferArticleStakeholder && !namedFromArticle.companyName && !commercial.organizations.length) {
    return empty(false, CONTACT_BLOCK_REASONS.ENTITY_UNKNOWN, articleStakeholders);
  }

  const orgs = commercial.organizations.length
    ? commercial.organizations
    : namedFromArticle?.companyName
      ? [namedFromArticle.companyName]
      : [];

  if (!orgs.length) {
    return empty(false, CONTACT_BLOCK_REASONS.ENTITY_UNKNOWN, articleStakeholders);
  }

  const roles = inferStakeholderRoles({
    eventType,
    signalType,
    actionable,
  });
  if (!roles.length && !namedFromArticle) {
    return empty(false, CONTACT_BLOCK_REASONS.NO_ROLES, articleStakeholders);
  }

  return {
    eligible: true,
    reason: null,
    roles,
    namedStakeholder: namedFromArticle || null,
    organizations: orgs,
    commercialOrganizations: commercial.details,
    actionable,
    worthReviewing: worth,
    articleStakeholders,
    preferArticleStakeholder,
    requiresPeopleSearch: needsPeopleSearchFallback && commercial.organizations.length > 0,
    peopleSearchAllowed: needsPeopleSearchFallback && commercial.organizations.length > 0,
  };
}

export function isContactEnrichmentEnabled() {
  return String(process.env.CONTACT_ENRICHMENT_ENABLED || "").toLowerCase() === "true";
}

export function getContactEnrichmentLimits() {
  return {
    maxCandidatesPerAlert: clampInt(process.env.MAX_CONTACT_CANDIDATES_PER_ALERT, 3, 1, 10),
    maxSearchesPerRun: clampInt(process.env.MAX_CONTACT_SEARCHES_PER_RUN, 3, 0, 100),
    maxEnrichmentsPerRun: clampInt(process.env.MAX_CONTACT_ENRICHMENTS_PER_RUN, 3, 0, 100),
    maxProviderCallsPerRun: clampInt(process.env.MAX_CONTACT_PROVIDER_CALLS_PER_RUN, 6, 0, 200),
    cacheDays: clampInt(process.env.CONTACT_CACHE_DAYS, 30, 1, 365),
    includeMobile: String(process.env.SURFE_INCLUDE_MOBILE || "").toLowerCase() === "true",
    dryRunProvider: String(process.env.CONTACT_ENRICHMENT_DRY_RUN_PROVIDER || "").toLowerCase() === "true",
  };
}

/** True when named person + company is enough for Surfe enrich without a people search. */
export function hasAdequateNamedIdentityForDirectEnrich({ namedStakeholder, companyName } = {}) {
  const name = String(namedStakeholder?.personName || "").trim();
  const company = String(companyName || "").trim();
  if (!name || name.split(/\s+/).length < 2) return false;
  if (!company || company.length < 2) return false;
  return true;
}

function empty(eligible, reason, articleStakeholders = null) {
  return {
    eligible,
    reason,
    roles: [],
    namedStakeholder: null,
    organizations: [],
    commercialOrganizations: [],
    actionable: false,
    worthReviewing: false,
    articleStakeholders: articleStakeholders || {
      articleTextSource: "empty",
      stakeholderExtractionSource: "UNAVAILABLE",
      stakeholderExtractionStatus: "ARTICLE_BODY_UNAVAILABLE",
      people: [],
      selectedForEnrichment: [],
      requiresPeopleSearch: true,
      primaryCandidate: null,
    },
    preferArticleStakeholder: false,
    requiresPeopleSearch: false,
    peopleSearchAllowed: false,
  };
}

/** Prefer selected commercial people when primaryCandidate is empty (WATCH path). */
function pickWatchNamedFromPeople(people = []) {
  const p = (people || []).find(
    (x) =>
      x?.personName &&
      x.selectedForEnrichment &&
      x.decisionMakerConfidence !== "LOW" &&
      !["GOVERNMENT", "ANALYST_CONSULTANT", "PR_MEDIA", "ARCHITECT_CONTRACTOR"].includes(
        x.stakeholderClassification
      )
  ) || (people || []).find(
    (x) =>
      x?.personName &&
      x.decisionMakerConfidence !== "LOW" &&
      !["GOVERNMENT", "ANALYST_CONSULTANT", "PR_MEDIA", "ARCHITECT_CONTRACTOR"].includes(
        x.stakeholderClassification
      )
  );
  if (!p) return null;
  return {
    personName: p.personName,
    jobTitle: p.jobTitle,
    companyName: p.companyName,
    source: "article_stakeholder",
    articleDerived: true,
    isQuoted: p.isQuoted,
    stakeholderClassification: p.stakeholderClassification,
    decisionMakerConfidence: p.decisionMakerConfidence,
    whyPerson: p.whyPerson,
    enrichmentPath: p.enrichmentPath,
    selectedForEnrichment: true,
  };
}

function clampInt(raw, fallback, min, max) {
  const n = parseInt(String(raw ?? fallback), 10);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, n));
}

function prependOrg(commercial, name, scored) {
  const s = String(name || "").trim();
  if (!s) return;
  if (commercial.organizations.some((o) => o.toLowerCase() === s.toLowerCase())) return;
  commercial.organizations.unshift(s);
  commercial.details.unshift({ name: s, ...scored });
}

/**
 * Score whether an organization is a commercially actionable Surfe search target.
 * @returns {{ commercial: boolean, confidence: string, kind: string, blockedReason: string|null }}
 */
export function scoreCommercialOrganization(name = "", { kindHint = null } = {}) {
  const s = String(name || "").trim();
  if (!s || s.length < 2) {
    return {
      commercial: false,
      confidence: COMMERCIAL_ORG_CONFIDENCE.LOW,
      kind: "unknown",
      blockedReason: "empty",
    };
  }

  if (NON_COMMERCIAL_ORG_RE.test(s)) {
    return {
      commercial: false,
      confidence: COMMERCIAL_ORG_CONFIDENCE.LOW,
      kind: "non_commercial",
      blockedReason: "NON_COMMERCIAL_ENTITY",
    };
  }

  let kind = kindHint || "commercial";
  if (/\b(?:asset\s+management|asset\s+manager)\b/i.test(s)) kind = "asset_manager";
  else if (/\b(?:invest|capital|fund)\b/i.test(s)) kind = "investment_company";
  else if (/\b(?:develop|developer|development)\b/i.test(s)) kind = "developer";
  else if (/\b(?:owner|holding|holdings|properties|realty)\b/i.test(s)) kind = "owner";
  else if (/\b(?:operator|management\s+(?:company|group)|hospitality)\b/i.test(s)) kind = "operator";
  else if (/\b(?:marriott|hilton|hyatt|ihg|accor|wyndham|choice|radisson)\b/i.test(s)) kind = "brand";
  else if (kindHint) kind = kindHint;

  const commercialKinds = new Set([
    "owner_developer",
    "owner",
    "developer",
    "investment_company",
    "asset_manager",
    "operator",
    "brand",
    "project_company",
    "article_stakeholder_company",
  ]);

  const hasCommercialSignal = COMMERCIAL_ORG_SIGNAL_RE.test(s);
  if (hasCommercialSignal || commercialKinds.has(kind)) {
    return {
      commercial: true,
      confidence: hasCommercialSignal ? COMMERCIAL_ORG_CONFIDENCE.HIGH : COMMERCIAL_ORG_CONFIDENCE.MEDIUM,
      kind,
      blockedReason: null,
    };
  }

  // Bare proper name without commercial cues — too weak for people-search fallback.
  return {
    commercial: false,
    confidence: COMMERCIAL_ORG_CONFIDENCE.LOW,
    kind: "weak",
    blockedReason: "LOW_CONFIDENCE_ORG",
  };
}

/**
 * Resolve HIGH/MEDIUM commercial organizations allowed for Surfe people-search fallback.
 * Excludes government, tourism boards, analysts, PR, municipalities, etc.
 */
export function resolveCommercialOrganizations(alert = {}, intel = {}, opts = {}) {
  const ents = intel.entities || alert.entities || {};
  const closedBrand = opts.closedBrand === true;
  const closedOperator = opts.closedOperator === true;
  const text = `${alert.title || ""} ${alert.summary || ""} ${intel.whatChanged || ""}`.trim();

  /** @type {{ organizations: string[], details: object[] }} */
  const commercial = { organizations: [], details: [] };

  const candidates = [
    { name: ents.ownerDeveloper || alert.ownerDeveloper || intel.ownerDeveloper, kindHint: "owner_developer" },
  ];

  if (!closedBrand) {
    candidates.push({ name: ents.brandInvolved || alert.brandInvolved, kindHint: "brand" });
  }
  if (!closedOperator) {
    candidates.push({ name: ents.operatorInvolved || alert.operatorInvolved, kindHint: "operator" });
  }

  const project = ents.hotelProject || alert.hotelProject;
  if (
    project &&
    /\b(group|holdings|development|developer|hospitality|hotels?|partners|capital|realty|properties|lifestyle)\b/i.test(
      project
    )
  ) {
    candidates.push({ name: project, kindHint: "project_company" });
  }

  // Title/summary company extraction when Airtable org fields are sparse.
  for (const extracted of extractCommercialOrgsFromText(text)) {
    candidates.push(extracted);
  }

  for (const c of candidates) {
    const scored = scoreCommercialOrganization(c.name, { kindHint: c.kindHint });
    if (!scored.commercial) continue;
    if (scored.confidence === COMMERCIAL_ORG_CONFIDENCE.LOW) continue;
    if (scored.kind === "brand" && closedBrand) continue;
    if (scored.kind === "operator" && closedOperator) continue;
    prependOrg(commercial, c.name, scored);
  }

  commercial.organizations.sort((a, b) => {
    const da = commercial.details.find((d) => d.name === a);
    const db = commercial.details.find((d) => d.name === b);
    const rank = (k) =>
      ({
        owner_developer: 0,
        owner: 0,
        developer: 1,
        investment_company: 2,
        asset_manager: 3,
        project_company: 4,
        operator: 5,
        brand: 6,
        text_extracted: 7,
      }[k] ?? 9);
    return rank(da?.kind) - rank(db?.kind);
  });

  return commercial;
}

/**
 * Pull likely commercial company names from title/summary when Airtable fields are empty.
 * Deterministic patterns only — no provider calls.
 */
const ORG_NAME_NOISE_RE =
  /\b(?:minister|ministry|comments?|said|says|according|analyst|report|outlook|arrivals?|tourism|government|mayor|governor|as|the|and|with|for|from|into|over|under)\b/i;

/** Proper-cased company token sequence (1–4 words), no sentence noise. */
const COMPANY_TOKEN =
  "[A-Z][A-Za-z0-9&.']+(?:\\s+[A-Z][A-Za-z0-9&.']+){0,3}";

export function extractCommercialOrgsFromText(text = "") {
  const t = String(text || "").trim();
  if (!t) return [];
  const out = [];
  const push = (name, kindHint) => {
    const s = String(name || "")
      .replace(/\s+/g, " ")
      .replace(/[.,;:]+$/g, "")
      .trim();
    if (!s || s.length < 3 || s.length > 60) return;
    if (ORG_NAME_NOISE_RE.test(s)) return;
    if (NON_COMMERCIAL_ORG_RE.test(s)) return;
    if (out.some((x) => x.name.toLowerCase() === s.toLowerCase())) return;
    out.push({ name: s, kindHint });
  };

  const patterns = [
    // Company Acquires / Breaks Ground / Secures / Assumes Management
    {
      re: new RegExp(
        `\\b(${COMPANY_TOKEN})\\s+(?:Acquires?|Acquire|Buys?|Bought|Secures?|Arranges?|Breaks?\\s+Ground|Assumes?\\s+Management|Awarded\\s+Management|Selected\\s+to\\s+Manage|Announces?|Unveils?|Partners?\\s+with)\\b`
      ),
      kind: "owner_developer",
    },
    // Developer/Owner Company Name …
    {
      re: new RegExp(
        `\\b(?:[Dd]eveloper|[Oo]wner|[Ii]nvestor)\\s+(${COMPANY_TOKEN})(?:\\s+(?:is|has|will|to|for|arranged|secured|advancing|closed))\\b`
      ),
      kind: "owner_developer",
    },
    // … by Company / of Company Hospitality
    {
      re: new RegExp(
        `\\b(?:by|from)\\s+(${COMPANY_TOKEN}(?:\\s+(?:Hospitality|Hotels|Resorts|Partners|Group|Holdings|Capital|Development|Properties))?)\\b`
      ),
      kind: "owner_developer",
    },
    // Company Hospitality / Partners / Group as subject (tight token bound)
    {
      re: new RegExp(
        `\\b(${COMPANY_TOKEN}\\s+(?:Hospitality|Hotels(?:\\s+&\\s+Resorts)?|Resorts|Partners|Holdings|Capital|Development|Properties|Lodging)|[A-Z][A-Za-z0-9&.']+\\s+(?:Hospitality|Hotels|Resorts|Partners|Holdings|Capital|Development|Properties|Lodging))\\b`
      ),
      kind: "owner_developer",
    },
  ];

  for (const { re, kind } of patterns) {
    const m = t.match(re);
    if (m?.[1]) push(m[1], kind);
  }
  return out;
}

/**
 * Prefer Owner/Developer, then Brand/Operator only when still open, then hotel/project name as weak org.
 * Legacy helper — returns names only; commercial filter applied via resolveCommercialOrganizations.
 */
export function resolveOrganizations(alert = {}, intel = {}) {
  const eventType = intel.eventType || alert.eventType;
  const signalType = intel.signalType || alert.signalType;
  const closedBrand =
    ["Brand Signing", "Reflag", "Conversion"].includes(eventType) ||
    /Competitive Brand Move/i.test(String(signalType || ""));
  const closedOperator =
    ["Operator Appointment", "Management Agreement", "Operator Change"].includes(eventType) ||
    /Competitive Operator Move|Management Agreement Announced/i.test(String(signalType || ""));
  return resolveCommercialOrganizations(alert, intel, { closedBrand, closedOperator }).organizations;
}

/**
 * Extract a named person already present in alert text / intelligence.
 */
export function extractNamedStakeholder(title = "", summary = "", intel = {}) {
  const text = `${title} ${summary}`.trim();
  const fromIntel = intel.namedPerson || intel.entities?.personName;
  if (fromIntel) {
    return { personName: String(fromIntel).trim(), jobTitle: intel.entities?.personTitle || null, source: "intelligence" };
  }

  const appoint = text.match(
    /\b(?:appoints?|names?|named|welcomes?)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s+as\s+([^,.;]+)/
  );
  if (appoint) {
    return {
      personName: appoint[1].trim(),
      jobTitle: appoint[2].trim().slice(0, 80),
      source: "article",
    };
  }
  const appointed = text.match(
    /\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s+(?:appointed|named)\s+as\s+([^,.;]+)/
  );
  if (appointed) {
    return {
      personName: appointed[1].trim(),
      jobTitle: appointed[2].trim().slice(0, 80),
      source: "article",
    };
  }

  const commaTitle = text.match(
    /\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2}),\s*((?:SVP|EVP|VP|Head|Director|CEO|CFO|CIO|President|Principal|Founder)[^,.;]{0,60})/
  );
  if (commaTitle) {
    return {
      personName: commaTitle[1].trim(),
      jobTitle: commaTitle[2].trim().slice(0, 80),
      source: "article",
    };
  }

  return null;
}
