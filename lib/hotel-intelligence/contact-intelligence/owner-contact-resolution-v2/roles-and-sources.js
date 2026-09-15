/**
 * Source-class helpers + role ranking for Contact Resolution V2.
 */

import { ROLE_TIER, SOURCE_CLASS } from "./vocabulary.js";

const TIER1_RE =
  /\b(owner|founder|fundador|principal|chairman|chairwoman|presidente|president|ceo|chief executive|managing partner|socio\s+gestor)\b/i;
const TIER2_RE =
  /\b(managing director|director general|chief development|head of development|svp|vp\s+development|acquisitions|acquisition|investment|asset management|desarrollador|inversiones)\b/i;
const TIER3_RE =
  /\b(operations|coo|cfo|finance|finanzas|general counsel|corporate|comercial)\b/i;
const EXCLUDED_RE =
  /\b(general manager|gm\b|front desk|reservations|reservas|concierge|guest services|brand ambassador|revenue manager)\b/i;

export function rankRoleTier(title = "") {
  const t = String(title || "");
  if (EXCLUDED_RE.test(t)) return ROLE_TIER.EXCLUDED;
  if (TIER1_RE.test(t)) return ROLE_TIER.TIER_1;
  if (TIER2_RE.test(t)) return ROLE_TIER.TIER_2;
  if (TIER3_RE.test(t)) return ROLE_TIER.TIER_3;
  return ROLE_TIER.TIER_3;
}

export function isOwnerSideRole(title = "") {
  return rankRoleTier(title) !== ROLE_TIER.EXCLUDED;
}

export function classifySourceClass(url, hints = {}) {
  if (hints.source_class && SOURCE_CLASS[hints.source_class]) return hints.source_class;
  const u = String(url || "").toLowerCase();
  if (/\.pdf(\?|$)/i.test(u)) return SOURCE_CLASS.PDF_DOCUMENT;
  if (/linkedin\.com/i.test(u)) return SOURCE_CLASS.PUBLIC_PROFESSIONAL_PROFILE;
  if (/prnewswire|businesswire|globenewswire|einpresswire/i.test(u)) {
    return SOURCE_CLASS.OWNER_PRESS_RELEASE;
  }
  if (/sec\.gov|bmv\.com|sedar|companieshouse|rpc\.|siem\.|rues\.|sunat|dgii/i.test(u)) {
    return SOURCE_CLASS.COMPANY_REGISTRY;
  }
  if (/gob\.mx|gov\.|gobierno|ministerio|sectur|procolombia/i.test(u)) {
    return SOURCE_CLASS.GOVERNMENT_REGISTRY;
  }
  if (/tripadvisor|booking\.com|expedia|hotels\.com/i.test(u)) {
    return SOURCE_CLASS.OTHER_PUBLIC_SOURCE;
  }
  if (hints.is_owner_domain) return SOURCE_CLASS.OFFICIAL_OWNER_SITE;
  if (hints.is_hotel_domain) return SOURCE_CLASS.OFFICIAL_HOTEL_SITE;
  if (/news|noticia|article|economia|elfinanciero|reuters/i.test(u)) {
    return SOURCE_CLASS.NEWS_ARTICLE;
  }
  return SOURCE_CLASS.OTHER_PUBLIC_SOURCE;
}

export function createEvidenceFact({
  claim,
  source_url,
  source_type,
  extracted_text_or_fact,
  observed_at = null,
} = {}) {
  return {
    claim: claim || null,
    source_url: source_url || null,
    source_type: source_type || SOURCE_CLASS.OTHER_PUBLIC_SOURCE,
    extracted_text_or_fact: extracted_text_or_fact || null,
    observed_at: observed_at || new Date().toISOString(),
  };
}
