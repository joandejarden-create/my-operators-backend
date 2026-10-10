/**
 * Page-level research — fetch real pages, extract evidence, never invent facts.
 */

import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../hotel-intelligence/room-count-research/fetch.js";
import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import { applyPublicDataCeiling } from "../opportunity-who-resolution-v1.js";

const DATE_RE = /\b(20(?:2[6-9]|3[0-2]))(?:[-/.](\d{1,2})(?:[-/.](\d{1,2}))?)?\b/g;
const EMAIL_RE = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
const LODGING_PAGE_RE =
  /\b(room block|host hotel|housing|accommodation|hébergement|alojamiento|hotel list|preferred hotel|overflow)\b/i;
const PROC_STATUS_RE = {
  OPEN: /\b(open|ouvert|abierta|deadline|date limite|closing)\b/i,
  AWARDED: /\b(awarded|attribué|adjudicad)\b/i,
  EXPIRED: /\b(closed|expired|clôturé|cerrad)\b/i,
  CANCELLED: /\b(cancel+ed|annul)\b/i,
};

function hasSerp() {
  return Boolean(String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim());
}

export async function fetchCandidatePage(url, { maxChars = 12000 } = {}) {
  if (!url || !/^https?:\/\//i.test(url)) {
    return { ok: false, url, error: "bad_url", text: "" };
  }
  try {
    const page = await fetchResearchPage(url, { timeoutMs: 20000 });
    if (!page.ok) {
      return { ok: false, url, error: page.error || `status_${page.status}`, text: "" };
    }
    const text = htmlToSearchableText(page.text || "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, maxChars);
    return { ok: true, url: page.url || url, text, fetchedAt: new Date().toISOString() };
  } catch (err) {
    return { ok: false, url, error: String(err?.message || err).slice(0, 120), text: "" };
  }
}

/**
 * Extract structured updates from page text — FACT only when pattern matches.
 */
export function extractEvidenceFromPage(opp = {}, page = {}) {
  const text = String(page.text || "");
  const updates = {};
  const facts = [];
  const provenance = {
    url: page.url || opp.officialSource,
    fetchedAt: page.fetchedAt || new Date().toISOString(),
  };

  // Timing
  const dates = [];
  let m;
  const re = new RegExp(DATE_RE.source, "g");
  while ((m = re.exec(text)) !== null) {
    const y = m[1];
    const mo = m[2] ? String(m[2]).padStart(2, "0") : null;
    const d = m[3] ? String(m[3]).padStart(2, "0") : null;
    if (mo && d) dates.push(`${y}-${mo}-${d}`);
    else if (mo) dates.push(`${y}-${mo}`);
    else dates.push(y);
  }
  const futureDates = dates.filter((x) => String(x).slice(0, 4) >= "2026");
  if (futureDates.length && !opp.eventStartDate) {
    const exact = futureDates.find((x) => /^\d{4}-\d{2}-\d{2}$/.test(x));
    if (exact) {
      updates.eventStartDate = exact;
      updates.futureCycleEvidenceState = "CURRENT_FUTURE_CYCLE_CONFIRMED";
      facts.push({ field: "eventStartDate", value: exact, kind: "FACT", ...provenance });
    } else {
      const year = futureDates.find((x) => /^\d{4}/.test(x));
      if (year) {
        updates.eventYear = String(year).slice(0, 4);
        updates.futureCycleEvidenceState = "FUTURE_CYCLE_UNCONFIRMED";
        facts.push({ field: "eventYear", value: updates.eventYear, kind: "FACT", ...provenance });
      }
    }
  }

  // Lodging
  if (LODGING_PAGE_RE.test(text) || LODGING_PAGE_RE.test(page.url || "")) {
    const roomBlock = /\broom block|host hotel|bloc de chambres|bloque de habitaciones\b/i.test(text);
    const housing = /\bhousing|accommodation|hébergement|alojamiento\b/i.test(text);
    updates.lodgingEvidence = {
      ...(opp.lodgingEvidence || {}),
      housingPageFound: housing || /accommodation|housing/i.test(page.url || ""),
      roomBlockMentioned: roomBlock,
      status: roomBlock ? "CREDIBLE" : housing ? "WEAK" : "WEAK",
      sourceUrl: page.url,
    };
    facts.push({
      field: "lodgingEvidence",
      value: updates.lodgingEvidence,
      kind: "FACT",
      ...provenance,
    });
  }

  // Contact
  const emails = [...new Set((text.match(EMAIL_RE) || []).slice(0, 5))];
  const contactish = emails.find((e) => !/example\.|sentry|wixpress|schema/i.test(e));
  if (contactish && !opp.primaryContact?.email && !opp.functionalContactEmail) {
    updates.functionalContactEmail = contactish;
    updates.contactResearchAttempted = true;
    updates.contactResearchState = "ATTEMPTED";
    facts.push({ field: "functionalContactEmail", value: contactish, kind: "FACT", ...provenance });
  }
  if (/\/contact/i.test(page.url || "") || /\bcontact us\b|\bcontactez\b|\bcontacto\b/i.test(text)) {
    updates.organizationContactUrl = page.url || opp.organizationContactUrl;
    updates.officialContactPath = page.url || opp.officialContactPath;
    updates.contactResearchAttempted = true;
    facts.push({ field: "organizationContactUrl", value: updates.organizationContactUrl, kind: "FACT", ...provenance });
  }

  // Procurement status
  let procurementStatus = "UNKNOWN";
  if (PROC_STATUS_RE.CANCELLED.test(text)) procurementStatus = "CANCELLED";
  else if (PROC_STATUS_RE.AWARDED.test(text)) procurementStatus = "AWARDED";
  else if (PROC_STATUS_RE.EXPIRED.test(text) && !PROC_STATUS_RE.OPEN.test(text)) procurementStatus = "EXPIRED";
  else if (PROC_STATUS_RE.OPEN.test(text)) procurementStatus = "OPEN";
  if (procurementStatus !== "UNKNOWN") {
    updates.procurementStatus = procurementStatus;
    facts.push({ field: "procurementStatus", value: procurementStatus, kind: "FACT", ...provenance });
  }

  // Venue mention
  const venueMatch = text.match(
    /\b(?:venue|lieu|sede|hosted at|at the)\s*[:\-]?\s*([A-Z][\w\s&'-]{3,60}(?:Hotel|Centre|Center|Palais|Arena|Palexpo)?)/
  );
  if (venueMatch && (!opp.venueStatus || /^UNKNOWN$/i.test(String(opp.venueStatus)))) {
    updates.venueStatus = venueMatch[1].trim().slice(0, 80);
    facts.push({ field: "venueStatus", value: updates.venueStatus, kind: "FACT", ...provenance });
  }

  // Enrich thesis if lodging found and thesis thin
  if (updates.lodgingEvidence && (!opp.hotelOpportunityThesis || opp.hotelOpportunityThesis.length < 40)) {
    updates.hotelOpportunityThesis = `${opp.organizationName || opp.title}: public lodging/housing evidence found at ${page.url}. Target hotel may pursue overflow / preferred listing pending confirmation.`;
  }

  if (!facts.length) {
    return { updates: {}, facts: [], emptyPage: !text };
  }

  updates.sources = [
    ...(Array.isArray(opp.sources) ? opp.sources : []),
    { url: page.url, kind: "candidate_completion_v2_page", date: provenance.fetchedAt },
  ];
  updates.lastResearchedAt = provenance.fetchedAt;
  updates.gdiCompletionVersion = "candidate_completion_jev_v2";

  return { updates, facts, emptyPage: false };
}

/**
 * One targeted SERP follow-up for a blocker (optional).
 */
export async function runTargetedCompletionSearch(query, { hl = "en", gl = "us" } = {}) {
  if (!hasSerp() || !query) return { hits: [], costUsd: 0 };
  try {
    const serp = await serpapiSearch({ engine: "google", q: query, num: 5, hl, gl });
    return { hits: serp?.data?.organic_results || [], costUsd: 0.05 };
  } catch {
    return { hits: [], costUsd: 0.05 };
  }
}

/**
 * Apply public data ceiling when WHO research attempted with no contact.
 */
export function stampWhoCeilingIfNeeded(opp = {}, attempted = false) {
  if (!attempted) return opp;
  if (opp.primaryContact?.email || opp.functionalContactEmail || opp.organizationContactUrl) {
    return { ...opp, contactResearchAttempted: true };
  }
  return applyPublicDataCeiling(opp, "PUBLIC_DATA_CEILING");
}
