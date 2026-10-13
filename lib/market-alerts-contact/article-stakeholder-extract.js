/**
 * Article Stakeholder Extraction for Market Alerts Contact Intelligence.
 *
 * Runs BEFORE Surfe search/enrichment. Prefers resolved full article body when
 * present on the alert (caller may fetch via ensureArticleBody…). Deterministic —
 * no provider calls.
 *
 * UI "Why" lines are short paraphrases only — never long copyrighted excerpts.
 */

import {
  STAKEHOLDER_EXTRACTION_SOURCE,
  STAKEHOLDER_EXTRACTION_STATUS,
  deriveStakeholderExtractionStatus,
} from "./fetch-article-body.js";

export const ARTICLE_STAKEHOLDER_CLASS = Object.freeze({
  OWNER_PRINCIPAL: "OWNER_PRINCIPAL",
  DEVELOPER_PRINCIPAL: "DEVELOPER_PRINCIPAL",
  DEVELOPMENT_EXECUTIVE: "DEVELOPMENT_EXECUTIVE",
  INVESTMENT_EXECUTIVE: "INVESTMENT_EXECUTIVE",
  ASSET_MANAGER: "ASSET_MANAGER",
  BRAND_DEVELOPMENT: "BRAND_DEVELOPMENT",
  OPERATOR_EXECUTIVE: "OPERATOR_EXECUTIVE",
  PREOPENING_LEADERSHIP: "PREOPENING_LEADERSHIP",
  LENDER: "LENDER",
  PROJECT_EXECUTIVE: "PROJECT_EXECUTIVE",
  ARCHITECT_CONTRACTOR: "ARCHITECT_CONTRACTOR",
  GOVERNMENT: "GOVERNMENT",
  ANALYST_CONSULTANT: "ANALYST_CONSULTANT",
  PR_MEDIA: "PR_MEDIA",
  OTHER: "OTHER",
});

export const DECISION_MAKER_CONFIDENCE = Object.freeze({
  HIGH: "HIGH",
  MEDIUM: "MEDIUM",
  LOW: "LOW",
});

export { STAKEHOLDER_EXTRACTION_SOURCE, STAKEHOLDER_EXTRACTION_STATUS };

const PERSON_NAME_RE = /([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})/;

const BYLINE_RE =
  /^(?:by|written by|reporting by|edited by)\s+/i;

const AUTHOR_CONTEXT_RE =
  /\b(?:author|editor|reporter|correspondent|staff writer|contributing writer|byline)\b/i;

const PR_CONTEXT_RE =
  /\b(?:media contact|press contact|PR contact|public relations|for media inquiries|spokes(?:person|woman|man))\b/i;

const TITLE_CREATIVE_RE =
  /\b(?:executive\s+chef|chef\b|pastry\s+chef|designer|interior\s+designer|architect)\b/i;

const QUOTE_TOPIC_HIGH_RE =
  /\b(?:acquir(?:e|ed|es|ing)|acquisition|site|land|develop(?:s|ed|ing|ment)?|invest(?:s|ed|ing|ment)?|financ(?:e|ing|ed)|partner(?:ship)?|brand\s+select|operator\s+select|management\s+agreement|franchise\s+agreement|construct(?:ion|ing)?|opening\s+(?:timeline|date)|pre[- ]?opening|commercial\s+strategy|will\s+develop|to\s+develop|evaluating\s+(?:hotel\s+)?brands?|operate\s+the\s+hotel|adaptive\s+reuse|historic\s+building|hotel\s+project|kansas\s+city\s+development|planned\s+(?:hotel|project)|conversion)\b/i;

const QUOTE_TOPIC_CONTEXTUAL_RE =
  /\b(?:tourism\s+arrivals|visitor\s+(?:growth|volumes?)|market\s+looks\s+strong|occupancy|revpar|demand\s+(?:is|continues)|arrivals\s+(?:are\s+)?increas)\b/i;

const DECISION_CLOSED_RE =
  /\b(?:franchise\s+agreement\s+(?:has\s+been\s+)?signed|management\s+agreement\s+(?:has\s+been\s+)?signed|already\s+(?:signed|announced|appointed)|brand\s+(?:has\s+been\s+)?selected|operator\s+(?:has\s+been\s+)?(?:selected|appointed))\b/i;

const DECISION_OPEN_RE =
  /\b(?:evaluating\s+(?:hotel\s+)?brands?|brand\s+(?:not\s+yet|still\s+to\s+be|unidentified)|seeking\s+(?:a\s+)?(?:brand|operator|partner)|financing\s+(?:forming|underway|in\s+progress)|pre[- ]?decision|partner\s+selection)\b/i;

const TITLE_OWNER_RE =
  /\b(?:owner|founder|co[- ]?founder|principal|managing\s+partner|proprietor)\b/i;
const TITLE_DEVELOPER_RE =
  /\b(?:developer|development\s+(?:partner|company)|hotelier)\b/i;
const TITLE_DEV_EXEC_RE =
  /\b(?:(?:svp|evp|vp|head|director|chief)\s+(?:of\s+)?development|cdo|chief\s+development|development\s+(?:director|executive|manager))\b/i;
const TITLE_INVEST_RE =
  /\b(?:cio|chief\s+investment|head\s+of\s+investments?|investment\s+(?:principal|director|partner)|cfo|chief\s+financial)\b/i;
const TITLE_ASSET_RE = /\b(?:asset\s+manager|vp\s+asset\s+management|head\s+of\s+asset)\b/i;
const TITLE_BRAND_RE =
  /\b(?:(?:svp|evp|vp|head|director).{0,20}(?:brand|franchise|affiliation)|brand\s+development|franchise\s+(?:vp|director))\b/i;
// Do NOT treat bare "CEO of … Hospitality" as operator — that is usually the
// project brand/platform. Require explicit operator / management-company signal.
const TITLE_OPERATOR_RE =
  /\b(?:(?:svp|evp|vp|coo|ceo|president).{0,40}(?:\boperator\b|management\s+company|hotel\s+operator)|general\s+manager\s+of\s+(?:operations)?|operations\s+(?:vp|director)|chief\s+operating)\b/i;

const COMPANY_DEVELOPER_RE =
  /\b(?:development|developer|developers|properties|property\s+group|holdings|realty|capital|partners|hospitality|lifestyle|hotels?)\b/i;

const SENIOR_TITLE_RE =
  /\b(?:CEO|Chief\s+Executive|President|Founder|Co[- ]?Founder|Managing\s+Director|MD\b|CDO|Chief\s+Development|EVP|SVP)\b/i;
const TITLE_PREOPEN_RE =
  /\b(?:pre[- ]?opening|opening)\s+(?:general\s+manager|gm|director\s+of\s+sales|dosm|director\s+of\s+revenue)|general\s+manager\b/i;
const TITLE_LENDER_RE = /\b(?:lender|banker|loan\s+officer|credit\s+officer)\b/i;
const TITLE_ARCH_RE = /\b(?:architect|contractor|general\s+contractor|design\s+partner)\b/i;
const TITLE_GOV_RE =
  /\b(?:minister|mayor|governor|secretary\s+of|tourism\s+(?:board|authority|minister)|council(?:lor|man|woman)|commissioner)\b/i;
const TITLE_ANALYST_RE =
  /\b(?:analyst|consultant|researcher|economist|industry\s+expert)\b/i;

/**
 * Resolve richest article text already available (no network fetch).
 * Preferred order: full body → enriched → whatChanged → summary → title → intel.
 * Does not overwrite editorial summary on the alert object.
 * @param {object} alert
 * @returns {{ text: string, source: string, extractionSource: string }}
 */
export function resolveArticleText(alert = {}) {
  const intel = alert.intelligence || {};
  const body =
    alert.articleBody || alert.articleText || alert.body || "";
  const enriched = alert.enrichedArticleText || "";
  const candidates = [
    { text: body, source: "article_body", tier: STAKEHOLDER_EXTRACTION_SOURCE.FULL_ARTICLE },
    { text: enriched, source: "enriched_text", tier: STAKEHOLDER_EXTRACTION_SOURCE.ENRICHED_TEXT },
    { text: alert.whatChanged || intel.whatChanged, source: "what_changed", tier: STAKEHOLDER_EXTRACTION_SOURCE.SUMMARY_ONLY },
    { text: alert.summary || intel.summary, source: "summary", tier: STAKEHOLDER_EXTRACTION_SOURCE.SUMMARY_ONLY },
    { text: alert.title, source: "title", tier: STAKEHOLDER_EXTRACTION_SOURCE.SUMMARY_ONLY },
    {
      text: [
        intel.entities?.ownerDeveloper || alert.ownerDeveloper,
        intel.entities?.hotelProject || alert.hotelProject,
        intel.entities?.brandInvolved || alert.brandInvolved,
        intel.entities?.operatorInvolved || alert.operatorInvolved,
      ]
        .filter(Boolean)
        .join(" "),
      source: "intelligence_fields",
      tier: STAKEHOLDER_EXTRACTION_SOURCE.SUMMARY_ONLY,
    },
  ];
  const parts = [];
  const used = new Set();
  let primaryTier = STAKEHOLDER_EXTRACTION_SOURCE.UNAVAILABLE;
  for (const c of candidates) {
    const t = String(c.text || "").trim();
    if (!t || used.has(c.source)) continue;
    used.add(c.source);
    parts.push(t);
    if (primaryTier === STAKEHOLDER_EXTRACTION_SOURCE.UNAVAILABLE) {
      primaryTier = c.tier;
    } else if (
      c.tier === STAKEHOLDER_EXTRACTION_SOURCE.FULL_ARTICLE ||
      (c.tier === STAKEHOLDER_EXTRACTION_SOURCE.ENRICHED_TEXT &&
        primaryTier === STAKEHOLDER_EXTRACTION_SOURCE.SUMMARY_ONLY)
    ) {
      primaryTier = c.tier;
    }
  }
  // Prefer FULL_ARTICLE whenever any body segment is present
  if (used.has("article_body")) {
    primaryTier = STAKEHOLDER_EXTRACTION_SOURCE.FULL_ARTICLE;
  } else if (used.has("enriched_text")) {
    primaryTier = STAKEHOLDER_EXTRACTION_SOURCE.ENRICHED_TEXT;
  } else if (parts.length) {
    primaryTier = STAKEHOLDER_EXTRACTION_SOURCE.SUMMARY_ONLY;
  }
  return {
    text: parts.join("\n").trim(),
    source: parts.length ? [...used].join("+") : "empty",
    extractionSource: parts.length ? primaryTier : STAKEHOLDER_EXTRACTION_SOURCE.UNAVAILABLE,
  };
}

/**
 * Extract + classify + score article stakeholders.
 * @param {object} alert
 * @returns {{
 *   articleTextSource: string,
 *   stakeholderExtractionSource: string,
 *   stakeholderExtractionStatus: string,
 *   people: object[],
 *   selectedForEnrichment: object[],
 *   requiresPeopleSearch: boolean,
 *   primaryCandidate: object|null,
 * }}
 */
export function extractArticleStakeholders(alert = {}) {
  const { text, source, extractionSource } = resolveArticleText(alert);
  const eventType = alert.eventType || alert.intelligence?.eventType || "";
  const brandClosed =
    ["Brand Signing", "Reflag", "Conversion"].includes(eventType) ||
    /Competitive Brand Move/i.test(String(alert.signalType || alert.intelligence?.signalType || ""));
  const operatorClosed =
    ["Operator Appointment", "Management Agreement", "Operator Change"].includes(eventType) ||
    /Competitive Operator Move|Management Agreement Announced/i.test(
      String(alert.signalType || alert.intelligence?.signalType || "")
    );
  const dealLikeEvent = isDealOrDevelopmentEvent(eventType, alert);

  if (!text) {
    return emptyResult(source, extractionSource || STAKEHOLDER_EXTRACTION_SOURCE.UNAVAILABLE);
  }

  const rawMentions = collectPersonMentions(text);
  const people = [];

  for (const m of rawMentions) {
    if (shouldExcludePerson(m, text)) continue;

    const classification = classifyStakeholder(m, text, { dealLikeEvent });
    const quoteAnalysis = analyzeQuote(m, text);
    const score = scoreDecisionMaker({
      mention: m,
      classification,
      quoteAnalysis,
      brandClosed,
      operatorClosed,
      dealLikeEvent,
      text,
    });
    const why = buildWhyParaphrase({ mention: m, classification, quoteAnalysis, score, dealLikeEvent });
    const selected = shouldSelectForEnrichment({
      classification,
      score,
      brandClosed,
      operatorClosed,
      quoteAnalysis,
      mention: m,
      dealLikeEvent,
    });

    people.push({
      personName: m.personName,
      jobTitle: m.jobTitle || null,
      companyName: m.companyName || null,
      quoteOrContext: truncateContext(m.contextSentence),
      relationshipToProject: inferRelationship(m, classification, quoteAnalysis),
      sourceSentence: truncateContext(m.contextSentence),
      evidenceText: truncateContext(m.contextSentence, 220),
      evidenceSource: extractionSource,
      isQuoted: Boolean(m.isQuoted),
      articleMentionCount: m.mentionCount,
      stakeholderClassification: classification,
      stakeholderClass: classification,
      relevanceReason: why,
      decisionMakerConfidence: score.confidence,
      confidence: score.confidence,
      decisionMakerScore: score.total,
      scoreBreakdown: score.breakdown,
      selectedForEnrichment: selected.select,
      enrichmentReason: selected.reason,
      enrichmentPath: selected.select
        ? hasDirectEnrichIdentity(m)
          ? "SURFE_DIRECT_ENRICH"
          : "SURFE_DIRECT_ENRICH_IF_COMPANY_RESOLVED"
        : "SKIP",
      articleDerived: true,
      whyPerson: why,
      decisionClosedBrand: classification === ARTICLE_STAKEHOLDER_CLASS.BRAND_DEVELOPMENT && brandClosed,
      decisionClosedOperator:
        classification === ARTICLE_STAKEHOLDER_CLASS.OPERATOR_EXECUTIVE && operatorClosed,
    });
  }

  // Sort: selected first, then score, then quoted
  people.sort((a, b) => {
    if (a.selectedForEnrichment !== b.selectedForEnrichment) {
      return a.selectedForEnrichment ? -1 : 1;
    }
    if (b.decisionMakerScore !== a.decisionMakerScore) {
      return b.decisionMakerScore - a.decisionMakerScore;
    }
    return (b.isQuoted ? 1 : 0) - (a.isQuoted ? 1 : 0);
  });

  const selectedForEnrichment = people.filter((p) => p.selectedForEnrichment);
  const primaryCandidate = selectedForEnrichment[0] || null;
  const requiresPeopleSearch = !primaryCandidate;
  const stakeholderExtractionStatus = deriveStakeholderExtractionStatus({
    stakeholderExtractionSource: extractionSource,
    people,
  });

  return {
    articleTextSource: source,
    stakeholderExtractionSource: extractionSource,
    stakeholderExtractionStatus,
    people,
    selectedForEnrichment,
    requiresPeopleSearch,
    primaryCandidate,
  };
}

function emptyResult(source, extractionSource = STAKEHOLDER_EXTRACTION_SOURCE.UNAVAILABLE) {
  return {
    articleTextSource: source || "empty",
    stakeholderExtractionSource: extractionSource,
    stakeholderExtractionStatus:
      extractionSource === STAKEHOLDER_EXTRACTION_SOURCE.UNAVAILABLE
        ? STAKEHOLDER_EXTRACTION_STATUS.ARTICLE_BODY_UNAVAILABLE
        : STAKEHOLDER_EXTRACTION_STATUS.NO_PERSON_FOUND,
    people: [],
    selectedForEnrichment: [],
    requiresPeopleSearch: true,
    primaryCandidate: null,
  };
}

function isDealOrDevelopmentEvent(eventType, alert = {}) {
  const blob = `${eventType || ""} ${alert.signalType || ""} ${alert.category || ""} ${alert.title || ""}`;
  return /\b(?:deal|adaptive\s+reuse|development|proposal|groundbreaking|construction|acquisition|jv|joint\s+venture|conversion|reflag|opening|pre[- ]?opening)\b/i.test(
    blob
  );
}

function collectPersonMentions(text) {
  /** @type {Map<string, object>} */
  const byName = new Map();

  // Drop leading byline lines so they do not poison context / exclusion.
  const cleaned = String(text || "")
    .replace(/^(?:by|written by|reporting by|edited by)\s+[^\n]+\n*/gim, "")
    .trim();
  const workText = cleaned || String(text || "");

  const titleLead =
    "(?:[Oo]wner|[Ff]ounder|co[- ]?[Ff]ounder|[Pp]rincipal|CEO|President|CIO|CFO|CDO|SVP|EVP|VP|Head|Director|Managing Partner|Managing Director|Chief Development Officer|General Manager|[Mm]inister|[Aa]nalyst|[Mm]ayor|[Cc]hef)";

  const patterns = [
    // Brand VP Name said … (keep case-sensitive for names)
    /\b((?:Marriott|Hilton|Hyatt|IHG|Accor|Wyndham|Choice|Radisson)\s+VP)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\s+(?:said|says|stated)\b/g,
    // “… added Name, CEO of Company.” / “said Name, title of Company”
    new RegExp(
      `\\b(?:added|said|says|stated|noted|explained|commented|according to)\\s+${PERSON_NAME_RE.source},\\s*((?:${titleLead})[^,]{0,60}?)(?:\\s+of\\s+([^,.;]{2,80}))?`,
      "g"
    ),
    // Name, title of Company, said …
    new RegExp(
      `${PERSON_NAME_RE.source},\\s*((?:${titleLead})[^,]{0,60}?)(?:\\s+of\\s+([^,.;]{2,80}))?[,]?\\s*(?:said|says|stated|noted|added|explained|commented)`,
      "g"
    ),
    // Name, title at/of Company
    new RegExp(
      `${PERSON_NAME_RE.source},\\s*((?:${titleLead})[^,]{0,80}?)(?:\\s+(?:of|at|for)\\s+([^,.;]{2,80}))?`,
      "g"
    ),
    // Name — Title, Company
    new RegExp(
      `${PERSON_NAME_RE.source}\\s+[—–-]\\s*((?:${titleLead})[^,]{0,40}),\\s*([^,.;]{2,80})`,
      "g"
    ),
    // Name (Company)
    new RegExp(`${PERSON_NAME_RE.source}\\s+\\(([^)]{2,80})\\)`, "g"),
    // according to Name of Company
    new RegExp(
      `\\baccording to\\s+${PERSON_NAME_RE.source}(?:\\s+of\\s+([^,.;]{2,80}))?`,
      "g"
    ),
    // Title Name of Company said
    new RegExp(
      `\\b((?:[Oo]wner|[Ff]ounder|CEO|President|[Mm]inister|[Aa]nalyst|[Mm]ayor)\\s+)${PERSON_NAME_RE.source}(?:\\s+of\\s+([^,.]{2,80}))?\\s+(?:said|says)`,
      "g"
    ),
    // Name said …
    new RegExp(`${PERSON_NAME_RE.source}\\s+(?:said|says|stated|noted|added|explained|commented)\\b`, "g"),
    // appoints Name as Title
    /\b(?:appoints?|names?|named|welcomes?)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s+as\s+([^,.;]{2,80})/g,
    /\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s+(?:appointed|named)\s+as\s+([^,.;]{2,80})/g,
    // Name, founder of Company
    new RegExp(
      `${PERSON_NAME_RE.source},\\s*((?:[Ff]ounder|[Oo]wner|[Pp]rincipal|CEO|[Pp]resident)[^,]{0,40})\\s+of\\s+([^,.;]{2,80})`,
      "g"
    ),
  ];

  for (const re of patterns) {
    re.lastIndex = 0;
    let match;
    let guard = 0;
    while ((match = re.exec(workText)) !== null) {
      if (++guard > 200) break;
      // Zero-width safety
      if (!match[0]) {
        re.lastIndex += 1;
        continue;
      }
      const snippet = surroundingSentence(workText, match.index, match[0].length);
      let personName = null;
      let jobTitle = null;
      let companyName = null;
      let isQuoted = /\b(?:said|says|stated|noted|added|explained|commented|according to)\b/i.test(match[0]);

      if (/^(?:Marriott|Hilton|Hyatt|IHG|Accor|Wyndham|Choice|Radisson)\s+VP\b/i.test(match[0])) {
        jobTitle = cleanTitle(match[1]);
        personName = cleanName(match[2]);
        companyName = cleanCompany(String(match[1]).split(/\s+/)[0]);
        isQuoted = true;
      } else if (/appoints?|names?|named|welcomes?|appointed/i.test(match[0])) {
        personName = cleanName(match[1]);
        jobTitle = cleanTitle(match[2]);
        isQuoted = false;
      } else if (/^(?:owner|founder|CEO|President|minister|analyst|mayor)\s+/i.test(match[0])) {
        jobTitle = cleanTitle(match[1]);
        personName = cleanName(match[2]);
        companyName = cleanCompany(match[3]);
        isQuoted = true;
      } else if (/\([A-Z]/.test(match[0]) && match[2] && !match[3] && !/,/.test(match[0].slice(0, 40))) {
        // Name (Company)
        personName = cleanName(match[1]);
        companyName = cleanCompany(match[2]);
      } else {
        personName = cleanName(match[1]);
        jobTitle = cleanTitle(match[2]);
        companyName = cleanCompany(match[3]);
      }

      if (!personName || personName.split(/\s+/).length < 2) continue;
      if (isLikelyNonPerson(personName)) continue;
      if (jobTitle) {
        jobTitle = jobTitle
          .replace(/\s+(?:Owner|Coastal)\b.*$/i, "")
          .trim()
          .slice(0, 60);
      }

      const key = personName.toLowerCase();
      const existing = byName.get(key);
      if (existing) {
        existing.mentionCount += 1;
        if (!existing.jobTitle && jobTitle) existing.jobTitle = jobTitle;
        // Prefer longer / more complete company names (avoid "UM" winning over "UMH Development")
        if (companyName) {
          if (
            !existing.companyName ||
            String(companyName).length > String(existing.companyName).length
          ) {
            existing.companyName = companyName;
          }
        }
        if (isQuoted) existing.isQuoted = true;
        if (snippet && snippet.length > (existing.contextSentence || "").length) {
          existing.contextSentence = snippet;
        }
        continue;
      }

      byName.set(key, {
        personName,
        jobTitle: jobTitle || null,
        companyName: companyName || null,
        isQuoted,
        contextSentence: snippet,
        mentionCount: 1,
        matchIndex: match.index,
      });
    }
  }

  // Pull company from "of X" / "at X" in context if missing
  for (const m of byName.values()) {
    if (!m.companyName && m.contextSentence) {
      const ofCo = m.contextSentence.match(
        new RegExp(
          `${escapeRegExp(m.personName)}.{0,80}\\b(?:of|at|for)\\s+([A-Z][A-Za-z0-9 &.'-]{2,60}?)(?:[,.]|\\s+said|\\s+says|\\s+to\\s+|$)`
        )
      );
      if (ofCo) m.companyName = cleanCompany(ofCo[1]);
    }
  }

  return [...byName.values()];
}

function surroundingSentence(text, index, len) {
  const start = Math.max(0, text.lastIndexOf(".", index - 1) + 1);
  let end = text.indexOf(".", index + len);
  if (end < 0) end = Math.min(text.length, index + len + 180);
  else end = Math.min(text.length, end + 1);
  return text.slice(start, end).replace(/\s+/g, " ").trim();
}

function cleanName(raw) {
  return String(raw || "")
    .replace(/^(?:by|and|with)\s+/i, "")
    .replace(/\s+/g, " ")
    .trim();
}

function cleanTitle(raw) {
  if (!raw) return null;
  return String(raw)
    .replace(/\s+/g, " ")
    .replace(/^(?:the\s+)/i, "")
    .trim()
    .slice(0, 80) || null;
}

function cleanCompany(raw) {
  if (!raw) return null;
  return String(raw)
    .replace(/\s+/g, " ")
    .replace(/^(?:the\s+)/i, "")
    .replace(/[."'\s]+$/g, "")
    .replace(/\s+(?:said|says|will|to|and)\b.*$/i, "")
    .trim()
    .slice(0, 80) || null;
}

function isLikelyNonPerson(name) {
  if (!name) return true;
  if (/\./.test(name)) return true;
  if (/^(?:the|a|an)\s/i.test(name)) return true;
  return /\b(?:Hotel|Resort|Group|Holdings|Partners|Limited|Inc|Corp|Marriott|Hilton|Hyatt|IHG|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|January|February|March|April|May|June|July|August|September|October|November|December)\b/.test(
    name
  );
}

function shouldExcludePerson(mention, fullText) {
  const name = mention.personName;
  const ctx = `${mention.contextSentence || ""} ${mention.jobTitle || ""}`;

  // Bylines: only exclude if this person appears on a By/Written-by line.
  const firstLines = String(fullText || "").split(/\n/).slice(0, 3).join("\n");
  const bylineLine = new RegExp(
    `^(?:by|written by|reporting by|edited by)\\s+${escapeRegExp(name)}\\b`,
    "im"
  );
  if (bylineLine.test(firstLines)) return true;

  // Context that is itself only a byline attribution for this person
  if (
    new RegExp(`^(?:by|written by)\\s+${escapeRegExp(name)}\\b`, "i").test(
      String(mention.contextSentence || "").trim()
    )
  ) {
    return true;
  }

  if (AUTHOR_CONTEXT_RE.test(ctx) && !TITLE_OWNER_RE.test(ctx) && !TITLE_DEVELOPER_RE.test(ctx) && !/\bowner\b/i.test(ctx)) {
    return true;
  }
  if (PR_CONTEXT_RE.test(ctx) && !TITLE_OWNER_RE.test(ctx) && !/\b(?:CEO|founder|owner|president)\b/i.test(ctx)) {
    return true;
  }
  return false;
}

function classifyStakeholder(mention, text, { dealLikeEvent = false } = {}) {
  const blob = `${mention.jobTitle || ""} ${mention.contextSentence || ""} ${mention.companyName || ""}`;
  const nameCtx = mention.contextSentence || "";
  const company = String(mention.companyName || "");
  const title = String(mention.jobTitle || "");

  if (TITLE_GOV_RE.test(blob) || /\btourism\s+minister\b/i.test(blob)) {
    return ARTICLE_STAKEHOLDER_CLASS.GOVERNMENT;
  }
  if (TITLE_ANALYST_RE.test(blob) || /\banalyst\s+\w+\s+\w+\s+said\b/i.test(blob)) {
    return ARTICLE_STAKEHOLDER_CLASS.ANALYST_CONSULTANT;
  }
  if (PR_CONTEXT_RE.test(blob) || AUTHOR_CONTEXT_RE.test(blob)) {
    return ARTICLE_STAKEHOLDER_CLASS.PR_MEDIA;
  }
  if (TITLE_CREATIVE_RE.test(blob) || TITLE_ARCH_RE.test(blob)) {
    return ARTICLE_STAKEHOLDER_CLASS.ARCHITECT_CONTRACTOR;
  }
  if (TITLE_PREOPEN_RE.test(blob) || /\bpre[- ]?opening\b/i.test(blob)) {
    return ARTICLE_STAKEHOLDER_CLASS.PREOPENING_LEADERSHIP;
  }
  // Founder / developer before generic "owner" bucket (founder ≠ hotel owner always).
  if (
    /\bfounder\b/i.test(title) ||
    /\bfounder\s+of\b/i.test(nameCtx) ||
    TITLE_DEVELOPER_RE.test(title) ||
    /\bwill\s+work\s+with\b.{0,40}\bdevelop\b/i.test(nameCtx) ||
    /\bto\s+develop\s+the\s+(?:resort|hotel)\b/i.test(nameCtx)
  ) {
    return ARTICLE_STAKEHOLDER_CLASS.DEVELOPER_PRINCIPAL;
  }
  // Company name signals developer/sponsor (e.g. UMH Development, … Hospitality & Lifestyle)
  if (
    SENIOR_TITLE_RE.test(title || nameCtx) &&
    (/\bdevelopment\b/i.test(company) ||
      /\bdeveloper\b/i.test(company) ||
      (dealLikeEvent && COMPANY_DEVELOPER_RE.test(company)))
  ) {
    if (/\bdevelopment\b/i.test(company) || /\bdeveloper\b/i.test(company)) {
      return ARTICLE_STAKEHOLDER_CLASS.DEVELOPER_PRINCIPAL;
    }
    return ARTICLE_STAKEHOLDER_CLASS.DEVELOPER_PRINCIPAL;
  }
  if (/\bowner\b/i.test(title) || /\bowner\s+of\b/i.test(nameCtx)) {
    return ARTICLE_STAKEHOLDER_CLASS.OWNER_PRINCIPAL;
  }
  if (TITLE_OWNER_RE.test(title) && !/\bfounder\b/i.test(title)) {
    return ARTICLE_STAKEHOLDER_CLASS.OWNER_PRINCIPAL;
  }
  if (TITLE_DEV_EXEC_RE.test(blob)) return ARTICLE_STAKEHOLDER_CLASS.DEVELOPMENT_EXECUTIVE;
  if (TITLE_INVEST_RE.test(blob)) return ARTICLE_STAKEHOLDER_CLASS.INVESTMENT_EXECUTIVE;
  if (TITLE_ASSET_RE.test(blob)) return ARTICLE_STAKEHOLDER_CLASS.ASSET_MANAGER;
  if (
    TITLE_BRAND_RE.test(blob) ||
    /\b(?:marriott|hilton|hyatt|ihg|accor|wyndham|choice)\b.{0,40}\b(?:vp|svp|franchise)\b/i.test(blob) ||
    /\b(?:vp|svp)\b.{0,40}\b(?:marriott|hilton|hyatt|ihg)\b/i.test(blob) ||
    /\bMarriott\s+VP\b/i.test(blob)
  ) {
    return ARTICLE_STAKEHOLDER_CLASS.BRAND_DEVELOPMENT;
  }
  if (TITLE_OPERATOR_RE.test(blob) || /\boperat(?:e|es|ing|or)\s+the\s+hotel\b/i.test(nameCtx)) {
    return ARTICLE_STAKEHOLDER_CLASS.OPERATOR_EXECUTIVE;
  }
  if (TITLE_LENDER_RE.test(blob)) return ARTICLE_STAKEHOLDER_CLASS.LENDER;
  if (/\bproject\s+(?:director|executive|lead|manager)\b/i.test(blob)) {
    return ARTICLE_STAKEHOLDER_CLASS.PROJECT_EXECUTIVE;
  }
  // Senior commercial title with company on a deal article → project executive / developer
  if (SENIOR_TITLE_RE.test(title) && company && dealLikeEvent && COMPANY_DEVELOPER_RE.test(company)) {
    return ARTICLE_STAKEHOLDER_CLASS.DEVELOPER_PRINCIPAL;
  }
  if (SENIOR_TITLE_RE.test(title) && company) {
    return ARTICLE_STAKEHOLDER_CLASS.PROJECT_EXECUTIVE;
  }
  return ARTICLE_STAKEHOLDER_CLASS.OTHER;
}

function analyzeQuote(mention, text) {
  const ctx = mention.contextSentence || "";
  const quoted = Boolean(mention.isQuoted);
  const highValue = QUOTE_TOPIC_HIGH_RE.test(ctx);
  const contextualOnly = QUOTE_TOPIC_CONTEXTUAL_RE.test(ctx) && !highValue;
  const decisionClosed = DECISION_CLOSED_RE.test(ctx) || DECISION_CLOSED_RE.test(text);
  const decisionOpen = DECISION_OPEN_RE.test(ctx) || DECISION_OPEN_RE.test(text);
  return {
    quoted,
    highValueTopic: highValue,
    contextualOnly,
    decisionClosed,
    decisionOpen,
    summary: paraphrasedQuoteTopic(ctx, highValue, contextualOnly),
  };
}

function paraphrasedQuoteTopic(ctx, highValue, contextualOnly) {
  if (!ctx) return null;
  if (/\bacquir/i.test(ctx) && /\b(?:site|land|brand)/i.test(ctx)) {
    return "Discussed site acquisition and partnership/brand evaluation";
  }
  if (/\bdevelop/i.test(ctx) && /\bpartner/i.test(ctx)) {
    return "Discussed development partnership for the project";
  }
  if (/\boperat/i.test(ctx)) return "Discussed operating the hotel";
  if (/\bfranchise/i.test(ctx) && /\bsigned/i.test(ctx)) {
    return "Confirmed franchise agreement signed";
  }
  if (contextualOnly) return "Commented on market/tourism conditions";
  if (highValue) return "Commented on project development or capital decisions";
  return null;
}

function scoreDecisionMaker({
  mention,
  classification,
  quoteAnalysis,
  brandClosed,
  operatorClosed,
  dealLikeEvent = false,
}) {
  let role = 0;
  let relationship = 0;
  let quote = 0;
  let openness = 0;
  let evidence = 0;

  const C = ARTICLE_STAKEHOLDER_CLASS;
  switch (classification) {
    case C.OWNER_PRINCIPAL:
    case C.DEVELOPER_PRINCIPAL:
      role = 40;
      break;
    case C.DEVELOPMENT_EXECUTIVE:
    case C.INVESTMENT_EXECUTIVE:
    case C.ASSET_MANAGER:
      role = 32;
      break;
    case C.PREOPENING_LEADERSHIP:
    case C.PROJECT_EXECUTIVE:
      role = 28;
      break;
    case C.LENDER:
      role = 18;
      break;
    case C.BRAND_DEVELOPMENT:
    case C.OPERATOR_EXECUTIVE:
      role = 22;
      break;
    case C.ARCHITECT_CONTRACTOR:
      role = 10;
      break;
    case C.GOVERNMENT:
    case C.ANALYST_CONSULTANT:
    case C.PR_MEDIA:
      role = 4;
      break;
    default:
      role = 8;
  }

  // Event-specific boost: deals / adaptive reuse prioritize developer / owner CEOs
  if (
    dealLikeEvent &&
    (classification === C.OWNER_PRINCIPAL ||
      classification === C.DEVELOPER_PRINCIPAL ||
      classification === C.DEVELOPMENT_EXECUTIVE)
  ) {
    role += 8;
  }

  const relBlob = `${mention.jobTitle || ""} ${mention.contextSentence || ""} ${mention.companyName || ""}`;
  if (/\b(?:owns?|owner|acquir|develop|financ|select(?:s|ed)?\s+(?:brand|operator)|signed|leads?\s+development|adaptive\s+reuse)\b/i.test(relBlob)) {
    relationship = 25;
  } else if (/\b(?:partner|project|resort|hotel|hospitality|lifestyle)\b/i.test(relBlob)) {
    relationship = 12;
  }

  if (quoteAnalysis.quoted && quoteAnalysis.highValueTopic) quote = 20;
  else if (quoteAnalysis.quoted && !quoteAnalysis.contextualOnly) quote = 10;
  else if (quoteAnalysis.quoted && quoteAnalysis.contextualOnly) quote = 2;

  if (quoteAnalysis.decisionOpen) openness = 12;
  else if (quoteAnalysis.decisionClosed) openness = -8;
  if (classification === C.BRAND_DEVELOPMENT && brandClosed) openness -= 15;
  if (classification === C.OPERATOR_EXECUTIVE && operatorClosed) openness -= 12;

  if (mention.jobTitle && mention.companyName) evidence = 15;
  else if (mention.jobTitle || mention.companyName) evidence = 10;
  else if (mention.isQuoted) evidence = 6;
  else evidence = 3;
  if (mention.mentionCount > 1) evidence += 2;

  const total = Math.max(0, role + relationship + quote + openness + evidence);
  let confidence = DECISION_MAKER_CONFIDENCE.LOW;
  if (total >= 70) confidence = DECISION_MAKER_CONFIDENCE.HIGH;
  else if (total >= 45) confidence = DECISION_MAKER_CONFIDENCE.MEDIUM;

  // Floor for clear owner/developer with high-value quote
  if (
    (classification === C.OWNER_PRINCIPAL || classification === C.DEVELOPER_PRINCIPAL) &&
    quoteAnalysis.highValueTopic &&
    mention.companyName
  ) {
    confidence = DECISION_MAKER_CONFIDENCE.HIGH;
  }

  // Floor: senior title + company + quoted on deal/development article
  if (
    dealLikeEvent &&
    SENIOR_TITLE_RE.test(mention.jobTitle || "") &&
    mention.companyName &&
    quoteAnalysis.quoted &&
    (classification === C.OWNER_PRINCIPAL ||
      classification === C.DEVELOPER_PRINCIPAL ||
      classification === C.DEVELOPMENT_EXECUTIVE ||
      classification === C.PROJECT_EXECUTIVE)
  ) {
    if (confidence === DECISION_MAKER_CONFIDENCE.LOW) {
      confidence = DECISION_MAKER_CONFIDENCE.MEDIUM;
    }
    if (quoteAnalysis.highValueTopic || /\bdevelopment\b/i.test(mention.companyName || "")) {
      confidence = DECISION_MAKER_CONFIDENCE.HIGH;
    }
  }

  // Cap government/analyst/creative
  if (
    classification === C.GOVERNMENT ||
    classification === C.ANALYST_CONSULTANT ||
    classification === C.PR_MEDIA ||
    classification === C.ARCHITECT_CONTRACTOR
  ) {
    confidence = DECISION_MAKER_CONFIDENCE.LOW;
  }

  return {
    total,
    confidence,
    breakdown: { role, relationship, quote, openness, evidence },
  };
}

function shouldSelectForEnrichment({
  classification,
  score,
  brandClosed,
  operatorClosed,
  quoteAnalysis,
  mention = {},
  dealLikeEvent = false,
}) {
  const C = ARTICLE_STAKEHOLDER_CLASS;

  if (
    classification === C.GOVERNMENT ||
    classification === C.ANALYST_CONSULTANT ||
    classification === C.PR_MEDIA ||
    classification === C.ARCHITECT_CONTRACTOR
  ) {
    return {
      select: false,
      reason: `${classification} — contextual; do not enrich by default`,
    };
  }

  if (classification === C.BRAND_DEVELOPMENT && (brandClosed || quoteAnalysis.decisionClosed)) {
    return {
      select: false,
      reason: "Brand stakeholder recognized but brand decision CLOSED — no competing brand outreach",
    };
  }

  if (classification === C.OPERATOR_EXECUTIVE && (operatorClosed || quoteAnalysis.decisionClosed)) {
    return {
      select: false,
      reason: "Operator stakeholder recognized but operator decision likely CLOSED",
    };
  }

  if (score.confidence === DECISION_MAKER_CONFIDENCE.HIGH) {
    return {
      select: true,
      reason: "HIGH decision-maker confidence from article evidence — priority enrichment",
    };
  }

  if (
    score.confidence === DECISION_MAKER_CONFIDENCE.MEDIUM &&
    [
      C.OWNER_PRINCIPAL,
      C.DEVELOPER_PRINCIPAL,
      C.DEVELOPMENT_EXECUTIVE,
      C.INVESTMENT_EXECUTIVE,
      C.ASSET_MANAGER,
      C.PREOPENING_LEADERSHIP,
      C.PROJECT_EXECUTIVE,
    ].includes(classification)
  ) {
    return {
      select: true,
      reason: "MEDIUM confidence project stakeholder — enrich before company+role search",
    };
  }

  // Named CEO/President + company on deal articles — show Get contact details
  if (
    dealLikeEvent &&
    SENIOR_TITLE_RE.test(mention.jobTitle || "") &&
    mention.companyName &&
    score.confidence !== DECISION_MAKER_CONFIDENCE.LOW
  ) {
    return {
      select: true,
      reason: "Senior commercial title with company on development/deal article",
    };
  }

  return {
    select: false,
    reason: "Insufficient decision-maker evidence for enrichment priority",
  };
}

function hasDirectEnrichIdentity(mention) {
  const name = String(mention.personName || "").trim();
  const company = String(mention.companyName || "").trim();
  return name.split(/\s+/).length >= 2 && company.length >= 2;
}

function inferRelationship(mention, classification, quoteAnalysis) {
  if (classification === ARTICLE_STAKEHOLDER_CLASS.OWNER_PRINCIPAL) return "Project owner / principal";
  if (classification === ARTICLE_STAKEHOLDER_CLASS.DEVELOPER_PRINCIPAL) {
    return "Developer / development partner";
  }
  if (quoteAnalysis.highValueTopic) return "Quoted on project decisions";
  if (classification === ARTICLE_STAKEHOLDER_CLASS.GOVERNMENT) return "Government / tourism official (contextual)";
  if (classification === ARTICLE_STAKEHOLDER_CLASS.ANALYST_CONSULTANT) return "Industry analyst (contextual)";
  return "Mentioned in article";
}

function buildWhyParaphrase({ mention, classification, quoteAnalysis, dealLikeEvent = false }) {
  const C = ARTICLE_STAKEHOLDER_CLASS;
  if (classification === C.OWNER_PRINCIPAL && quoteAnalysis.highValueTopic) {
    return "Owner quoted discussing development and partner selection";
  }
  if (classification === C.OWNER_PRINCIPAL) {
    return mention.companyName
      ? `Named as owner of ${mention.companyName}`
      : "Named as project owner in the article";
  }
  if (classification === C.DEVELOPER_PRINCIPAL && /\bdevelopment\b/i.test(mention.companyName || "")) {
    return mention.companyName
      ? `Development-company CEO directly associated with the project (${mention.companyName})`
      : "Development-company executive directly associated with the project";
  }
  if (classification === C.DEVELOPER_PRINCIPAL && quoteAnalysis.highValueTopic) {
    return "Developer quoted on partnership to develop the project";
  }
  if (classification === C.DEVELOPER_PRINCIPAL) {
    return mention.companyName
      ? `Senior executive behind the hospitality / development platform (${mention.companyName}) for this project`
      : "Named as leading the project development";
  }
  if (classification === C.PROJECT_EXECUTIVE && dealLikeEvent) {
    return mention.companyName
      ? `Senior executive of ${mention.companyName} associated with this project`
      : "Senior project executive named in the article";
  }
  if (classification === C.PREOPENING_LEADERSHIP) {
    return "Named in pre-opening leadership appointment";
  }
  if (classification === C.BRAND_DEVELOPMENT) {
    return "Brand executive mentioned regarding affiliation";
  }
  if (classification === C.OPERATOR_EXECUTIVE) {
    return "Operator executive referenced regarding hotel operations";
  }
  if (classification === C.GOVERNMENT) {
    return "Government speaker on market conditions (not primary outreach)";
  }
  if (classification === C.ANALYST_CONSULTANT) {
    return "Analyst commentary (not primary outreach)";
  }
  if (classification === C.ARCHITECT_CONTRACTOR) {
    return "Design / culinary / construction participant (lower commercial priority)";
  }
  if (quoteAnalysis.quoted && quoteAnalysis.summary) return quoteAnalysis.summary;
  if (mention.jobTitle) return `Named in article as ${mention.jobTitle}`;
  return "Named stakeholder in article";
}

function truncateContext(s, max = 160) {
  const t = String(s || "").replace(/\s+/g, " ").trim();
  if (t.length <= max) return t;
  return `${t.slice(0, max - 1)}…`;
}

function escapeRegExp(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Map article classification → Surfe role label for display / targeting.
 */
export function articleClassToRoleLabel(classification) {
  const map = {
    [ARTICLE_STAKEHOLDER_CLASS.OWNER_PRINCIPAL]: "Owner principal",
    [ARTICLE_STAKEHOLDER_CLASS.DEVELOPER_PRINCIPAL]: "Developer principal",
    [ARTICLE_STAKEHOLDER_CLASS.DEVELOPMENT_EXECUTIVE]: "Head / VP / SVP Development",
    [ARTICLE_STAKEHOLDER_CLASS.INVESTMENT_EXECUTIVE]: "Head of Investments / CIO",
    [ARTICLE_STAKEHOLDER_CLASS.ASSET_MANAGER]: "Asset manager",
    [ARTICLE_STAKEHOLDER_CLASS.BRAND_DEVELOPMENT]: "Brand development",
    [ARTICLE_STAKEHOLDER_CLASS.OPERATOR_EXECUTIVE]: "Operator executive",
    [ARTICLE_STAKEHOLDER_CLASS.PREOPENING_LEADERSHIP]: "Pre-opening leadership",
    [ARTICLE_STAKEHOLDER_CLASS.LENDER]: "Lender",
    [ARTICLE_STAKEHOLDER_CLASS.PROJECT_EXECUTIVE]: "Project lead",
    [ARTICLE_STAKEHOLDER_CLASS.ARCHITECT_CONTRACTOR]: "Architect / contractor",
    [ARTICLE_STAKEHOLDER_CLASS.GOVERNMENT]: "Government",
    [ARTICLE_STAKEHOLDER_CLASS.ANALYST_CONSULTANT]: "Analyst / consultant",
    [ARTICLE_STAKEHOLDER_CLASS.PR_MEDIA]: "PR / media",
    [ARTICLE_STAKEHOLDER_CLASS.OTHER]: "Article stakeholder",
  };
  return map[classification] || "Article stakeholder";
}
