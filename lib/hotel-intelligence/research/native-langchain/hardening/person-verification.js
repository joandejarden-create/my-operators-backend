/**
 * Packet 2.8C-2 — Person verification pipeline.
 * Company LinkedIn page ≠ person profile. Search results ≠ verified profile.
 */

export const PERSON_VERIFICATION_VERSION = "person-verification-v1";

export function buildPersonSearchQueries({ person, organization, hotel_name } = {}) {
  const p = String(person || "").trim();
  const o = String(organization || "").trim();
  const h = String(hotel_name || "").trim();
  if (!p) return [];
  return [
    `"${p}" "${o}"`,
    `"${p}" LinkedIn`,
    o ? `"${p}" "${o}" LinkedIn` : null,
    h ? `"${p}" "${h}" LinkedIn` : null,
    o ? `site:linkedin.com/in "${p}" "${o}"` : `site:linkedin.com/in "${p}"`,
  ].filter(Boolean);
}

/**
 * Classify a discovered person profile URL/result.
 */
export function classifyPersonProfileResult(result = {}, { person, organization } = {}) {
  const url = String(result.url || "");
  const title = String(result.title || "");
  const snippet = String(result.snippet || "");
  const blob = `${title} ${snippet} ${url}`.toLowerCase();
  const personHit = person ? blob.includes(String(person).toLowerCase().split(/\s+/)[0]) : false;
  const orgHit = organization ? blob.includes(String(organization).toLowerCase().split(/\s+/)[0]) : false;

  if (/linkedin\.com\/company\//i.test(url)) {
    return {
      status: "REJECTED",
      reason: "company_linkedin_page_not_person_profile",
      person_level: false,
    };
  }
  if (/linkedin\.com\/pub\/dir|\/search\//i.test(url)) {
    return {
      status: "REJECTED",
      reason: "search_results_page_not_profile",
      person_level: false,
    };
  }
  if (/linkedin\.com\/in\//i.test(url) && personHit) {
    return {
      status: orgHit ? "VERIFIED" : "PROBABLE",
      reason: orgHit ? "person_profile_org_match" : "person_profile_name_match_org_weak",
      person_level: true,
      profile_url: url,
    };
  }
  if (personHit && orgHit && /about|biography|management|director|ceo|chairman/i.test(blob)) {
    return {
      status: "PROBABLE",
      reason: "first_party_or_bio_page",
      person_level: true,
      profile_url: url,
    };
  }
  return {
    status: "REJECTED",
    reason: "insufficient_person_org_match",
    person_level: false,
  };
}

/**
 * Only VERIFIED profiles are display-safe.
 */
export function filterDisplayablePeople(people = []) {
  return (people || []).filter((p) => String(p.verification || p.status || "").toUpperCase() === "VERIFIED");
}
