/**
 * Structural entity-truth gate for GDI customer opportunities.
 * Rejects UI chrome, platform attribution, session/article titles,
 * directories, hotel promotions — without brittle exact-string-only lists.
 */

import { cleanEntityDisplayName } from "./hidden-demand/v3-entity-clean.js";

export const ENTITY_CLASS = Object.freeze({
  COMPANY: "COMPANY",
  ASSOCIATION_SUBGROUP: "ASSOCIATION_SUBGROUP",
  TEAM: "TEAM",
  AGENCY: "AGENCY",
  PRODUCTION_COMPANY: "PRODUCTION_COMPANY",
  TOUR_OPERATOR: "TOUR_OPERATOR",
  DELEGATION: "DELEGATION",
  UNIVERSITY_GROUP: "UNIVERSITY_GROUP",
  PROJECT_TEAM: "PROJECT_TEAM",
  VENDOR: "VENDOR",
  OTHER_VALID_ORGANIZATION: "OTHER_VALID_ORGANIZATION",
  UI_CHROME: "UI_CHROME",
  CTA_TEXT: "CTA_TEXT",
  PLATFORM_ATTRIBUTION: "PLATFORM_ATTRIBUTION",
  NAVIGATION_TEXT: "NAVIGATION_TEXT",
  SESSION_TITLE: "SESSION_TITLE",
  ARTICLE_TITLE: "ARTICLE_TITLE",
  DIRECTORY_TITLE: "DIRECTORY_TITLE",
  CATEGORY_LABEL: "CATEGORY_LABEL",
  CITY_NAME: "CITY_NAME",
  VENUE_NAME_ONLY: "VENUE_NAME_ONLY",
  EVENT_TITLE_ONLY: "EVENT_TITLE_ONLY",
  PROMOTION: "PROMOTION",
  HOTEL_PROMOTION: "HOTEL_PROMOTION",
  GENERIC_MARKET_PAGE: "GENERIC_MARKET_PAGE",
  SUPPLY_SIDE_PROMOTION: "SUPPLY_SIDE_PROMOTION",
  UNKNOWN_INVALID: "UNKNOWN_INVALID",
});

const VALID_ENTITY = new Set([
  ENTITY_CLASS.COMPANY,
  ENTITY_CLASS.ASSOCIATION_SUBGROUP,
  ENTITY_CLASS.TEAM,
  ENTITY_CLASS.AGENCY,
  ENTITY_CLASS.PRODUCTION_COMPANY,
  ENTITY_CLASS.TOUR_OPERATOR,
  ENTITY_CLASS.DELEGATION,
  ENTITY_CLASS.UNIVERSITY_GROUP,
  ENTITY_CLASS.PROJECT_TEAM,
  ENTITY_CLASS.VENDOR,
  ENTITY_CLASS.OTHER_VALID_ORGANIZATION,
]);

const CTA_VERBS_RE =
  /\b(interested in|become an?|register now|learn more|contact us|sign up|log\s?in|view (all|floor|details)|explore (your|the)|apply now|book now|submit|download|get started|scan exhibitors)\b/i;

const PLATFORM_RE =
  /\b(powered by|a2z\s*events?|a2zinc|expocad|map.?your.?show|colleqt|cvent supplier|easy\s*rfp|expolink)\b/i;

const UI_CHROME_RE =
  /\b(exhibitor (application|console|login|portal|list|directory|success webinar)|interested in exhibiting|supporters?\s*&\s*sponsors?|sponsor opportunities|floor plan|booth map|why exhibit|how to exhibit|explore your exhibitor|scan exhibitors)\b/i;

const DIRECTORY_RE =
  /\b(venues? in|conference venues|hotel(s)? in|things to do|event(s)? calendar|upcoming events|venue guide|rfp (list|guide)|tech conference venues)\b/i;

const SUPPLY_PROMO_RE =
  /\b(hotel week|staycation|room package|discount(ed)? (rate|stay)|holiday (celebration )?offer|promotional rate|supply.?side|hotel promotion|book (your|direct) stay)\b/i;

const SESSION_TITLE_RE =
  /\b(how to |managing the |and the computer|session\s*\d|workshop:|keynote:|panel:|breakout:|agenda item)\b/i;

const CITY_ONLY_RE =
  /^(new york( city)?|nyc|manhattan|midtown|times square|bethesda|washington|rome|london)$/i;

const CORP_MARKERS_RE =
  /\b(inc|llc|corp|ltd|co\.|company|group|agency|association|society|university|institute|foundation|alliance|committee|federation|authority|solutions|systems|industries|partners|productions|studios|tours?|travel)\b/i;

function blobFor(opp = {}) {
  const org = cleanEntityDisplayName(opp.organizationName || opp.company || "");
  const title = cleanEntityDisplayName(opp.title || opp.opportunityName || "");
  const bareTitle = title
    .replace(/\s*[—\-]\s*(EXHIBITOR|VENDOR|SPONSOR)\s*BLOCK\s*$/i, "")
    .trim();
  return { org, title, bareTitle, combined: `${org} ${bareTitle}`.trim() };
}

function hasOrgDomainEvidence(opp = {}) {
  const domain =
    opp.organizationDomain ||
    opp.companyDomain ||
    opp.primaryDomain ||
    opp.website ||
    null;
  if (domain && /\./.test(String(domain)) && !/a2zinc|expolink|example\./i.test(String(domain))) {
    return true;
  }
  const sources = Array.isArray(opp.sources) ? opp.sources : [];
  for (const s of sources) {
    const url = typeof s === "string" ? s : s?.url || "";
    if (!url) continue;
    try {
      const host = new URL(url).hostname.replace(/^www\./, "");
      if (
        host &&
        !/a2zinc|expolinkshop|cvent\.com|easyhotelrfp|chancity|functionalfabricfair|boutique\.a2zinc/i.test(
          host
        )
      ) {
        // Company-owned domain (not pure directory platform) is weak positive signal
        if (/\.(com|org|net|edu|io|co)$/i.test(host) && !/event|expo|show|fair/i.test(host)) {
          return true;
        }
      }
    } catch {
      /* ignore */
    }
  }
  return false;
}

function looksLikeSessionOrArticleTitle(text = "") {
  const t = String(text || "").trim();
  if (!t) return false;
  if (SESSION_TITLE_RE.test(t)) return true;
  // Title-case prose with ellipsis / topic connectors and no corp marker
  if (
    /\.{2,}|…/.test(t) &&
    !CORP_MARKERS_RE.test(t) &&
    t.split(/\s+/).length >= 5
  ) {
    return true;
  }
  if (
    /^(how|why|what|when|where|managing|understanding|navigating)\b/i.test(t) &&
    !CORP_MARKERS_RE.test(t)
  ) {
    return true;
  }
  return false;
}

/**
 * Classify whether the opportunity resolves to an addressable demand entity.
 */
export function classifyEntityTruth(opp = {}) {
  const { org, title, bareTitle, combined } = blobFor(opp);
  const name = org || bareTitle || title;
  const reasons = [];

  if (!name || name.length < 3) {
    return {
      entityClass: ENTITY_CLASS.UNKNOWN_INVALID,
      validEntity: false,
      reasons: ["empty_or_short_name"],
      displayName: name || null,
    };
  }

  if (CITY_ONLY_RE.test(name)) {
    return {
      entityClass: ENTITY_CLASS.CITY_NAME,
      validEntity: false,
      reasons: ["city_name_only"],
      displayName: name,
    };
  }

  if (PLATFORM_RE.test(name) || (/^powered by\b/i.test(name) && !CORP_MARKERS_RE.test(name))) {
    return {
      entityClass: ENTITY_CLASS.PLATFORM_ATTRIBUTION,
      validEntity: false,
      reasons: ["platform_attribution"],
      displayName: name,
    };
  }

  if (UI_CHROME_RE.test(name) || UI_CHROME_RE.test(combined)) {
    return {
      entityClass: ENTITY_CLASS.UI_CHROME,
      validEntity: false,
      reasons: ["ui_chrome_pattern"],
      displayName: name,
    };
  }

  if (CTA_VERBS_RE.test(name) && !CORP_MARKERS_RE.test(name)) {
    return {
      entityClass: ENTITY_CLASS.CTA_TEXT,
      validEntity: false,
      reasons: ["cta_navigation_text"],
      displayName: name,
    };
  }

  if (SUPPLY_PROMO_RE.test(combined) || SUPPLY_PROMO_RE.test(title)) {
    // Hotel Week / holiday offers / room packages — supply side
    const isHotelPromo =
      /\bhotel week\b|\broom package\b|\bstay\b.*\boffer\b|\bholiday (celebration )?offer\b/i.test(
        combined
      );
    return {
      entityClass: isHotelPromo
        ? ENTITY_CLASS.HOTEL_PROMOTION
        : ENTITY_CLASS.SUPPLY_SIDE_PROMOTION,
      validEntity: false,
      reasons: ["supply_side_promotion"],
      displayName: name,
    };
  }

  if (DIRECTORY_RE.test(combined) || DIRECTORY_RE.test(title)) {
    return {
      entityClass: ENTITY_CLASS.GENERIC_MARKET_PAGE,
      validEntity: false,
      reasons: ["generic_directory_or_market_page"],
      displayName: name,
    };
  }

  if (looksLikeSessionOrArticleTitle(name) || looksLikeSessionOrArticleTitle(bareTitle)) {
    // Session/program titles must never become the company — even if scraped from a rich domain
    return {
      entityClass: ENTITY_CLASS.SESSION_TITLE,
      validEntity: false,
      reasons: ["session_or_content_title"],
      displayName: name,
    };
  }

  // Extraction / CMS debris promoted as opportunity titles
  if (
    /\b(skip to main content|passcode|meeting id\s+\d|browse\s*&\s*search|enter meeting id)\b/i.test(
      combined
    )
  ) {
    return {
      entityClass: ENTITY_CLASS.UI_CHROME,
      validEntity: false,
      reasons: ["extraction_cms_debris"],
      displayName: name,
    };
  }

  // Org equals a generic directory publisher with directory title
  if (
    /^(easy rfp|cvent|city of new york|various corporations)$/i.test(org) &&
    (DIRECTORY_RE.test(title) || SUPPLY_PROMO_RE.test(title) || /venues? in/i.test(title))
  ) {
    return {
      entityClass: ENTITY_CLASS.GENERIC_MARKET_PAGE,
      validEntity: false,
      reasons: ["publisher_plus_directory_title"],
      displayName: name,
    };
  }

  // Pure event title used as org (org missing / org == title event name only)
  if (
    !org &&
    /\b(annual meeting|conference|forum|summit|symposium|world cup)\b/i.test(bareTitle) &&
    !CORP_MARKERS_RE.test(bareTitle)
  ) {
    reasons.push("event_title_without_org");
  }

  // Positive org signals
  if (CORP_MARKERS_RE.test(name) || hasOrgDomainEvidence(opp) || opp.boothNumber) {
    let entityClass = ENTITY_CLASS.COMPANY;
    if (/\b(association|society|alliance|committee|federation)\b/i.test(name)) {
      entityClass = ENTITY_CLASS.ASSOCIATION_SUBGROUP;
    } else if (/\b(university|college|school)\b/i.test(name)) {
      entityClass = ENTITY_CLASS.UNIVERSITY_GROUP;
    } else if (/\b(agency|trade agency)\b/i.test(name)) {
      entityClass = ENTITY_CLASS.AGENCY;
    } else if (/\b(tours?|travel)\b/i.test(name)) {
      entityClass = ENTITY_CLASS.TOUR_OPERATOR;
    } else if (/\b(productions?|studios?|crew)\b/i.test(name)) {
      entityClass = ENTITY_CLASS.PRODUCTION_COMPANY;
    } else if (/\b(inc|llc|corp|ltd|solutions|systems|industries)\b/i.test(name)) {
      entityClass = ENTITY_CLASS.VENDOR;
    } else {
      entityClass = ENTITY_CLASS.OTHER_VALID_ORGANIZATION;
    }
    return {
      entityClass,
      validEntity: true,
      reasons: reasons.length ? reasons : ["org_markers_or_evidence"],
      displayName: name,
    };
  }

  // Known association / bar / league style names without Inc
  if (
    /\b(bar|league|association|society|alliance|dames|committee|organization|organisation)\b/i.test(
      name
    )
  ) {
    return {
      entityClass: ENTITY_CLASS.ASSOCIATION_SUBGROUP,
      validEntity: true,
      reasons: ["association_pattern"],
      displayName: name,
    };
  }

  // Multi-word proper org without chrome — provisional valid if not prose title
  if (
    name.split(/\s+/).length >= 2 &&
    !CTA_VERBS_RE.test(name) &&
    !looksLikeSessionOrArticleTitle(name) &&
    !/^(the|a|an)\s+/i.test(name)
  ) {
    return {
      entityClass: ENTITY_CLASS.OTHER_VALID_ORGANIZATION,
      validEntity: true,
      reasons: ["multi_word_org_provisional"],
      displayName: name,
    };
  }

  return {
    entityClass: ENTITY_CLASS.UNKNOWN_INVALID,
    validEntity: false,
    reasons: ["no_addressable_entity_signals"],
    displayName: name,
  };
}

export function isValidCustomerEntity(opp = {}) {
  const result = classifyEntityTruth(opp);
  return result.validEntity === true && VALID_ENTITY.has(result.entityClass);
}

export { VALID_ENTITY };
