/**
 * Extract distinct teammate candidates from a verified org About / team page.
 * Used to fill an optional secondary contact when research packages only carried
 * a primary — never invents email/phone; profileUrl = the page itself.
 */
import { looksLikePersonName } from "../secondary-named-contact-v1.js";

export const GDI_TEAM_PAGE_SECONDARY_CANDIDATES_V1 =
  "gdi-team-page-secondary-candidates-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function nameKey(name) {
  return text(name)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/** Soft-title-case ALL-CAPS scraped names for customer display. */
function displayPersonName(name) {
  const n = text(name);
  if (!n) return n;
  const letters = n.replace(/[^A-Za-zÀ-ÿ]/g, "");
  if (!letters || letters !== letters.toUpperCase()) return n;
  return n
    .toLowerCase()
    .replace(/(^|[\s'-])([\p{L}])/gu, (_, a, c) => a + c.toUpperCase());
}

function toVisibleText(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

const ROLE_FRAGMENT =
  String.raw`(?:Sales\s+Manager|Account\s+Manager|Event\s+Manager|Project\s+Manager|Program\s+Manager|General\s+Manager|Office\s+Manager|Property\s+Manager|Tenant\s+Experience\s+Manager|Meeting\s+Planner|Procurement\s+Manager|Director(?:a)?(?:\s+(?:de|of|General|Comercial|Ejecutiv[oa]|Adquisiciones))?|Gerente(?:\s+(?:de|General|Comercial|de\s+Obra))?|Coordinador(?:a)?(?:\s+(?:General|de\s+\w+))?|Jefe(?:a)?(?:\s+de\s+\w+)?|CEO|Chief\s+\w+|President(?:e)?|Founder|Vice\s+President|VP|Head\s+of\s+\w+|Specialist|Coordinator|Analyst|Officer|Secretary|Secretari[oa])`;

/**
 * Parse Name + Role pairs from About/team visible text.
 */
export function extractTeamPagePeople(html, opts = {}) {
  const visible = toVisibleText(html);
  if (!visible) return [];
  const primaryKey = nameKey(opts.primaryName);
  const pageUrl = text(opts.pageUrl);
  const organization = text(opts.organization);
  const out = [];
  const seen = new Set();

  const re = new RegExp(
    String.raw`([\p{Lu}][\p{L}'\.-]+(?:\s+(?:de|van|von|del|de\s+la|da|di|[\p{Lu}][\p{L}'\.-]+)){1,4})\s+(${ROLE_FRAGMENT})`,
    "gu",
  );

  let match;
  while ((match = re.exec(visible)) !== null) {
    const name = displayPersonName(match[1]);
    const title = text(match[2]);
    if (!looksLikePersonName(name)) continue;
    const key = nameKey(name);
    if (!key || (primaryKey && key === primaryKey)) continue;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({
      name,
      title,
      organization: organization || null,
      profileUrl: pageUrl || null,
      sourceUrl: pageUrl || null,
      evidenceUrls: pageUrl ? [pageUrl] : [],
      relationshipToOpportunity: pageUrl
        ? `Publicly listed ${title}${organization ? ` at ${organization}` : ""} on the organization team / about page — alternative referral path beside the primary contact. Not a verified hotel buyer unless separately confirmed.`
        : `Publicly listed ${title} — alternative referral path beside the primary contact.`,
      whyRelevant: `Listed with the primary contact on the same organization team / about page.`,
      allowSeniorAsReferral: true,
      contactClassification: /sales|event|meeting|procurement|housing|tenant|workplace|operations|gerente|director|coordinador/i.test(
        title,
      )
        ? "REFERRAL_CONTACT"
        : "REFERRAL_CONTACT",
    });
    if (out.length >= 8) break;
  }
  return out;
}

/**
 * Live-fetch an About/team URL and return secondary candidates (excluding primary).
 */
export async function fetchTeamPageSecondaryCandidates(url, opts = {}) {
  const pageUrl = text(url);
  if (!pageUrl) return { ok: false, candidates: [], reason: "NO_URL" };
  const timeoutMs = Number(opts.timeoutMs || 8000);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(pageUrl, {
      method: "GET",
      redirect: "follow",
      signal: controller.signal,
      headers: {
        "User-Agent": "DealalityTeamPageSecondary/1.0",
        Accept: "text/html,application/xhtml+xml",
      },
    });
    clearTimeout(timer);
    if (!res.ok) {
      return {
        ok: false,
        candidates: [],
        reason: "HTTP_NOT_OK",
        httpStatus: res.status,
      };
    }
    const html = await res.text();
    const candidates = extractTeamPagePeople(html, {
      primaryName: opts.primaryName,
      pageUrl,
      organization: opts.organization,
    });
    return {
      ok: candidates.length > 0,
      candidates,
      reason: candidates.length ? "TEAMMATES_FOUND" : "NO_TEAMMATES_PARSED",
      httpStatus: res.status,
    };
  } catch (err) {
    clearTimeout(timer);
    return {
      ok: false,
      candidates: [],
      reason: "FETCH_FAILED",
      message: String(err?.message || err),
    };
  }
}

export default {
  GDI_TEAM_PAGE_SECONDARY_CANDIDATES_V1,
  extractTeamPagePeople,
  fetchTeamPageSecondaryCandidates,
};
