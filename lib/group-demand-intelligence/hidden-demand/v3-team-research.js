/**
 * Bounded exhibitor → team / travel / WHO evidence research.
 */

import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../hotel-intelligence/room-count-research/fetch.js";
import { fetchAndExtractPdf, extractHousingSignalsFromText } from "./extract-pdf.js";
import { cleanEntityDisplayName } from "./v3-entity-clean.js";
import {
  ADDRESSABILITY,
  CONTACT_CURRENTNESS,
  EXHIBITOR_WHO_ROLES,
} from "./v3-states.js";

function hasSerp() {
  return !!(process.env.SERPAPI_API_KEY || process.env.SERPAPI_KEY);
}

const ROLE_RE =
  /\b(events?\s+manager|field\s+marketing|trade\s*show\s+manager|experiential|marketing\s+operations|sales\s+operations|project\s+manager|travel\s+manager|operations\s+lead|director\s+of\s+events|vp\s+marketing)\b/i;

const REJECT_WHO_RE =
  /\b(ceo|chief\s+executive|president|general\s+counsel|board\s+member|investor\s+relations|press\s+contact|media\s+contact)\b/i;

const NAME_RE = /\b([A-Z][a-z]+(?:\s+[A-Z][a-z.'-]+){1,2})\b/g;

/**
 * Extract team evidence from page text.
 */
export function extractTeamEvidenceFromText(text = "", companyName = "") {
  const t = String(text || "");
  const companyEventPage =
    /booth|exhibit|trade\s*show|conference|event\s+team|see\s+us\s+at|visit\s+us\s+at/i.test(t) &&
    (companyName ? new RegExp(companyName.split(/\s+/)[0], "i").test(t) : true);

  const named = [];
  let m;
  NAME_RE.lastIndex = 0;
  while ((m = NAME_RE.exec(t)) && named.length < 12) {
    const n = m[1];
    if (
      /^(The|And|For|With|New|York|Booth|Visit|See|Join|Our|Team|Press|Release|Event|Show|Home|About|Contact|Search|Filter|View|All|More|Login|Register|Scan|Exhibitors|Sponsors|Supporters|Powered|Interested|Exhibiting)$/i.test(
        n
      ) ||
      /^(Booth\s+Booth|New\s+York|Press\s+Release)$/i.test(n)
    ) {
      continue;
    }
    // Prefer names near role/event words
    const ctx = t.slice(Math.max(0, m.index - 40), m.index + n.length + 60);
    if (ROLE_RE.test(ctx) || /speaker|panelist|booth|attending|represent/i.test(ctx)) {
      if (!REJECT_WHO_RE.test(ctx)) named.push({ name: n, context: ctx.slice(0, 120) });
    }
  }

  const speakerCount = (t.match(/\b(speaker|keynote|panelist|presenter)\b/gi) || []).length;
  const agency =
    t.match(
      /\b([A-Z][A-Za-z0-9&.\-]+(?:\s+[A-Z][A-Za-z0-9&.\-]+){0,3})\s+(?:Experiential\s+)?Agency\b/i
    ) ||
    t.match(
      /\b(?:produced|partnered|activation)\s+by\s+([A-Z][A-Za-z0-9&.\-]+(?:\s+[A-Z][A-Za-z0-9&.\-]+){0,3})/i
    ) ||
    null;

  return {
    companyEventPage,
    namedPeople: dedupeNames(named),
    multipleNamedPeople: named.length >= 2,
    speakerStaffSignal: speakerCount > 0 || /booth\s*staff|demo\s+staff/i.test(t),
    speakerCount,
    agencyRelationship: agency
      ? { name: agency[1], client: companyName, role: "AGENCY" }
      : /experiential\s+agency|activation\s+agency/i.test(t)
        ? { name: "agency_mentioned", client: companyName, role: "AGENCY" }
        : null,
    multiDay: /multi[- ]day|three[- ]day|setup|breakdown|load[- ]in|load[- ]out|\d+\s*days?\b/i.test(t),
    setupBreakdown: /setup|breakdown|load[- ]in|load[- ]out|install/i.test(t),
    productLaunch: /product\s+launch|unveil|debut/i.test(t),
    pressEvent: /press\s+(event|conference|day)/i.test(t),
    teamSupported:
      named.length >= 1 ||
      speakerCount > 0 ||
      /booth\s*staff|sales\s*team|marketing\s*team|event\s+team|delegation/i.test(t) ||
      companyEventPage,
    snippets: [],
  };
}

function dedupeNames(rows) {
  const seen = new Set();
  const out = [];
  for (const r of rows) {
    const k = r.name.toLowerCase();
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(r);
  }
  return out;
}

/**
 * Extract WHO candidates from text — exhibitor-specific roles only.
 */
export function extractExhibitorWhoFromText(text = "", companyName = "") {
  const t = String(text || "");
  const contacts = [];
  const emailRe =
    /([A-Z][a-z]+(?:\s+[A-Z][a-z.'-]+){1,2})\s*[,|\-|–]?\s*((?:Events?|Field|Trade|Marketing|Sales|Travel|Operations|Project)[^,.\n]{0,40})?\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})?/g;
  let m;
  while ((m = emailRe.exec(t)) && contacts.length < 8) {
    const role = (m[2] || "").trim();
    if (REJECT_WHO_RE.test(`${m[1]} ${role}`)) continue;
    const roleKey = mapRoleKey(role || m[0]);
    contacts.push({
      name: m[1],
      role: role || roleKey,
      roleKey,
      email: m[3] || null,
      company: companyName,
      currentness: CONTACT_CURRENTNESS.CURRENT_LIKELY,
      sourceScope: "first_party_or_event",
    });
  }

  // Role-only functional path
  if (!contacts.length && ROLE_RE.test(t)) {
    const roleM = t.match(ROLE_RE);
    contacts.push({
      name: null,
      role: roleM[0],
      roleKey: mapRoleKey(roleM[0]),
      email: null,
      company: companyName,
      currentness: CONTACT_CURRENTNESS.UNKNOWN,
      sourceScope: "functional",
      functionalOnly: true,
    });
  }

  return contacts;
}

function mapRoleKey(role = "") {
  const r = String(role).toLowerCase();
  if (/trade\s*show/.test(r)) return "TRADE_SHOW_MANAGER";
  if (/field\s+marketing/.test(r)) return "FIELD_MARKETING";
  if (/experiential/.test(r)) return "EXPERIENTIAL_MARKETING";
  if (/events?\s+director|director\s+of\s+events/.test(r)) return "EVENTS_DIRECTOR";
  if (/events?/.test(r)) return "EVENT_MARKETING";
  if (/travel/.test(r)) return "TRAVEL_MANAGER";
  if (/sales\s+operations|regional\s+sales/.test(r)) return "SALES_OPERATIONS";
  if (/marketing\s+operations/.test(r)) return "MARKETING_OPERATIONS";
  if (/project/.test(r)) return "PROJECT_MANAGER";
  if (/operations/.test(r)) return "OPERATIONS_LEAD";
  if (/agency|account/.test(r)) return "AGENCY_ACCOUNT_LEAD";
  return "OTHER_RELEVANT";
}

export function classifyAddressability(contacts = []) {
  if (contacts.some((c) => c.name && !c.functionalOnly && c.email)) {
    return ADDRESSABILITY.NAMED_PERSON;
  }
  if (contacts.some((c) => c.name && !c.functionalOnly)) {
    return ADDRESSABILITY.NAMED_PERSON;
  }
  if (contacts.some((c) => c.functionalOnly || (!c.name && c.role))) {
    return ADDRESSABILITY.FUNCTIONAL_PATH;
  }
  if (contacts.length === 0) return ADDRESSABILITY.NO_USABLE_PATH;
  return ADDRESSABILITY.COMPANY_PATH;
}

/**
 * Run bounded SERP + page fetches for one exhibitor entity.
 */
export async function deepenExhibitorEntity(entity = {}, opts = {}) {
  const company = cleanEntityDisplayName(entity.entityName);
  const eventHint = entity.demandGeneratorName || entity.eventName || "New York trade show";
  const year = entity.year || 2027;
  const maxQueries = opts.maxQueries ?? 5;
  const maxFetches = opts.maxFetches ?? 8;
  const pathHint = opts.pathHint || "COMPANY_EVENT_PAGE";

  const queries = buildQueriesForPath(pathHint, company, eventHint, year).slice(0, maxQueries);
  const pages = [];
  const urls = new Set();
  let queryCount = 0;
  let fetchCount = 0;
  let combinedText = "";
  let housing = null;

  if (hasSerp()) {
    for (const q of queries) {
      queryCount += 1;
      try {
        const serp = await serpapiSearch({
          engine: "google",
          q,
          num: 5,
          hl: "en",
          gl: "us",
        });
        for (const hit of serp?.data?.organic_results || []) {
          if (hit.link) urls.add(hit.link);
          combinedText += ` ${hit.title || ""} ${hit.snippet || ""}`;
        }
      } catch {
        /* continue */
      }
    }
  }

  // Prefer company.com and event pages; PDFs only for enrichment
  const ordered = [...urls].sort((a, b) => scoreUrl(a, company) - scoreUrl(b, company));
  for (const url of ordered) {
    if (fetchCount >= maxFetches) break;
    fetchCount += 1;
    try {
      if (/\.pdf($|\?)/i.test(url)) {
        const pdf = await fetchAndExtractPdf(url, {
          demandGeneratorName: eventHint,
          year,
          family: entity.family,
        });
        if (pdf.ok) {
          combinedText += ` ${pdf.textLen ? "" : ""}${JSON.stringify(pdf.entities?.slice?.(0, 5) || [])}`;
          // Re-fetch text via housing extract on stored — use entity names as weak signal
          pages.push({ url, kind: "pdf", ok: true });
          if (pdf.housing?.roomBlockMentioned) housing = pdf.housing;
        }
        continue;
      }
      const page = await fetchResearchPage(url);
      if (!page.ok) continue;
      const text = htmlToSearchableText(page.text || "");
      combinedText += ` ${text.slice(0, 12000)}`;
      pages.push({ url: page.url || url, kind: "html", ok: true, len: text.length });
      const h = extractHousingSignalsFromText(text, page.url || url);
      if (h.roomBlockMentioned || h.housingPageFound) housing = h;
    } catch {
      /* continue */
    }
  }

  const team = extractTeamEvidenceFromText(combinedText, company);
  const who = extractExhibitorWhoFromText(combinedText, company);
  const addressability =
    who.length > 0 ? classifyAddressability(who) : ADDRESSABILITY.COMPANY_PATH;

  // Directory-only with no deepen hits → company path at best
  const addressabilityFinal =
    pages.length === 0 && who.length === 0
      ? ADDRESSABILITY.COMPANY_PATH
      : addressability;

  return {
    company,
    queries,
    queryCount,
    fetchCount,
    pages,
    team,
    who,
    addressability: addressabilityFinal,
    housing,
    deepenText: combinedText.slice(0, 50000),
    pathHint,
  };
}

function buildQueriesForPath(path, company, eventHint, year) {
  const base = [
    `"${company}" "${eventHint}" booth OR exhibit OR attending ${year}`,
    `"${company}" trade show team OR booth staff OR field marketing ${year}`,
  ];
  switch (path) {
    case "NEWSROOM":
      return [
        `"${company}" press release exhibit OR conference OR booth ${year}`,
        ...base,
      ];
    case "SPEAKER_ROSTER":
      return [
        `"${company}" speaker OR panelist ${eventHint} ${year}`,
        ...base,
      ];
    case "TRAVEL_EVIDENCE":
      return [
        `"${company}" hotel OR lodging OR accommodation OR "room block" ${eventHint}`,
        `"${company}" travel OR team attending ${eventHint}`,
      ];
    case "AGENCY_RELATIONSHIP":
      return [
        `"${company}" experiential agency OR activation ${eventHint}`,
        ...base,
      ];
    case "FUNCTIONAL_CONTACT":
      return [
        `"${company}" "trade show manager" OR "field marketing" OR "events manager"`,
        `"${company}" events OR experiential contact`,
      ];
    case "PROFESSIONAL_STAFF":
      return [
        `"${company}" "events manager" OR "trade show" director`,
        ...base,
      ];
    case "COMPANY_EVENT_PAGE":
    default:
      return [
        `"${company}" "see us at" OR "visit us at" OR booth ${year}`,
        `"${company}" ${eventHint} exhibitor`,
        ...base,
      ];
  }
}

function scoreUrl(url, company) {
  const u = String(url).toLowerCase();
  const c = company.toLowerCase().split(/\s+/)[0];
  let s = 50;
  if (c && u.includes(c.slice(0, 6))) s -= 20;
  if (/\.pdf($|\?)/.test(u)) s += 15; // prefer html slightly
  if (/linkedin|facebook|twitter|instagram|youtube/.test(u)) s += 40;
  if (/exhibitor|booth|events|newsroom|press/.test(u)) s -= 10;
  return s;
}
