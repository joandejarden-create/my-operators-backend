/**
 * Contact Intelligence V1.2 — live native contact discovery.
 * Fixes: SerpAPI organic parse (res.data.organic_results), Census join by record ID,
 * brand-central vs property phones, execution traces, once-per-owner research.
 * Paid enrichment / Webhound remain disabled.
 */

import "dotenv/config";
import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import { fetchResearchPage, htmlToSearchableText } from "../room-count-research/fetch.js";
import {
  ATTRIBUTION,
  CHANNEL_KIND,
  PROPERTY_RELEVANCE,
  USAGE_RIGHTS,
  UNRESOLVED_REASON,
  DELIVERABILITY,
  ROLE_CURRENCY,
} from "./vocabulary.js";
import { createChannel, createEvidenceRef, createPersonContact } from "./contact-record.js";
import { isGenericMailboxEmail, isRoleMailboxEmail, isNamedPersonEmail } from "./dimensions.js";
import { buildOrganizationContactRoute } from "./owner-reuse.js";
import { joinCensusContactByRecordId } from "./census-contact-join.js";
import { CI_FAILURE_CODE, createEmptyTrace, addCode } from "./failure-codes.js";
import {
  extractLeadershipPeople,
  classifyContactPageMechanism,
  classifyLeadershipCategory,
} from "./leadership-extraction.js";

export const LIVE_NATIVE_DISCOVERY_VERSION = "contact-live-native-discovery-v1.2";
const SERPAPI_USD = 0.01;

const PHONE_RE =
  /(?:\+|00)(?:\d[\s().-]?){8,14}\d|(?:\(?\d{2,4}\)?[\s.-]\d{3,4}[\s.-]\d{4})/g;
const EMAIL_RE = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
const TEL_HREF_RE = /href=["']tel:([^"']+)["']/gi;
const MAILTO_RE = /href=["']mailto:([^"']+)["']/gi;

/** OTA / social / aggregator / job-board hosts — do not use bare `hotels.com` (matches krystal-hotels.com). */
const REJECT_HOST_RE =
  /(?:^|\.)(?:tripadvisor|booking\.com|expedia|hotels\.com|hotel\.com|facebook|instagram|linkedin|wikipedia|yelp|google|atrapalo|com-hotel\.com|com-website\.com|mejor-hoteles|hoteles-en|guadalajara-hotels|bestday|kayak|recruit\.net|jobsora|expertini|indeed|glassdoor|realestatemarket|remax|tecnocasa)\b/i;

/** Generic tokens that must not alone prove owner-domain match. */
const WEAK_OWNER_TOKENS = new Set([
  "hotel",
  "hotels",
  "hotelero",
  "group",
  "grupo",
  "management",
  "company",
  "co",
  "the",
  "and",
  "de",
  "la",
  "del",
  "inmobiliaria",
]);

const BRAND_CENTRAL_PATH_RE =
  /\/explore(?:\/|$)|\/reservations?(?:\/|$)|\/customer-care|\/contact-us\/?$|\/call-us|\/global\/|\/brands?\//i;

function normalizePhone(raw) {
  const display = String(raw || "").trim().replace(/\s+/g, " ").slice(0, 40);
  const digits = display.replace(/\D/g, "");
  if (digits.length < 10 || digits.length > 15) return null;
  if (/^(19|20)\d{2}/.test(digits) && digits.length <= 8) return null;
  if (/^0{3,}/.test(digits)) return null;
  // Copyright / year footers: "2026 2025 2024"
  if (/^(20\d{2}){2,4}$/.test(digits)) return null;
  if (/\b20\d{2}\s+20\d{2}\s+20\d{2}\b/.test(display)) return null;
  return display;
}

/**
 * Owner/org page must match seed domain or a strong owner-name token in the host.
 * Prevents attributing Palladium / press / job-board contacts to Alliance/HNF.
 */
export function isPlausibleOwnerPage(url, ownerName, seedOwnerSite = null) {
  const host = hostOf(url);
  if (!host || REJECT_HOST_RE.test(host)) return false;
  const hostCompact = host.replace(/[^a-z0-9]/g, "");
  if (seedOwnerSite) {
    const seedHost = hostOf(seedOwnerSite);
    if (seedHost) {
      const seedCompact = seedHost.replace(/[^a-z0-9]/g, "");
      if (
        host === seedHost ||
        host.endsWith(`.${seedHost}`) ||
        seedHost.endsWith(`.${host}`) ||
        (seedCompact.length >= 6 &&
          (hostCompact === seedCompact ||
            hostCompact.includes(seedCompact) ||
            seedCompact.includes(hostCompact)))
      ) {
        return true;
      }
    }
  }
  const tokens = String(ownerName || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .split(/[^a-z0-9]+/)
    .filter((t) => t.length >= 3 && !WEAK_OWNER_TOKENS.has(t));
  if (!tokens.length) return false;
  // Require token as a host label segment (blocks apexalliance matching "alliance")
  const hostParts = host.split(/[^a-z0-9]+/).filter(Boolean);
  return tokens.some((t) => t.length >= 4 && (hostParts.includes(t) || hostCompact === t));
}

function hostOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return "";
  }
}

function pathOf(url) {
  try {
    return new URL(url).pathname || "/";
  } catch {
    return "";
  }
}

/**
 * Brand property pages (marriott.com/.../hotels/..., ihg.com/.../hoteldetail) are OK.
 * Brand explore / root hubs are WRONG_DOMAIN / BRAND_CENTRAL.
 */
export function classifyOfficialUrl(url, { hotelName = "" } = {}) {
  const u = String(url || "").trim();
  if (!u.startsWith("http")) {
    return { status: "invalid", code: CI_FAILURE_CODE.OFFICIAL_URL_MISSING, brand_central: false };
  }
  const host = hostOf(u);
  const path = pathOf(u);
  if (REJECT_HOST_RE.test(host)) {
    return {
      status: "rejected",
      code: CI_FAILURE_CODE.WRONG_DOMAIN,
      brand_central: false,
      host,
      detail: "ota_or_social",
    };
  }
  if (BRAND_CENTRAL_PATH_RE.test(path) || /\/explore\b/i.test(u)) {
    return {
      status: "brand_central",
      code: CI_FAILURE_CODE.BRAND_CENTRAL_RESERVATIONS,
      brand_central: true,
      host,
      detail: "brand_central_or_explore_path",
    };
  }
  if (
    /^(marriott|ihg|hilton|hyatt|radisson|wyndham|accor|krystal-hotels)\.com$/i.test(host) &&
    (path === "/" || path.length < 8)
  ) {
    return {
      status: "brand_central",
      code: CI_FAILURE_CODE.BRAND_CENTRAL_RESERVATIONS,
      brand_central: true,
      host,
      detail: "brand_root_without_property",
    };
  }
  const nameToken = String(hotelName || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "")
    .slice(0, 10);
  const looksProperty =
    /hoteldetail|\/hotels\/|hotel-|resort|grand-|realinn|cambridge|puerto|cancun|guadalajara|vallarta/i.test(
      u
    ) ||
    (nameToken.length >= 5 &&
      (host.includes(nameToken.slice(0, 6)) || path.includes(nameToken.slice(0, 6))));
  return {
    status: "ok",
    code: null,
    brand_central: false,
    host,
    property_like: looksProperty,
  };
}

export function classifyRouteKind({ value, context = "" }) {
  const v = String(value || "").toLowerCase();
  const ctx = String(context || "").toLowerCase();
  if (!v) return "NONE";
  if (v.includes("@")) {
    if (isRoleMailboxEmail(v) || isGenericMailboxEmail(v)) return "CORPORATE_ROUTE";
    return "DIRECT_ATTRIBUTED_CANDIDATE";
  }
  if (/contact|contacto|contáctenos|fale conosco|contato/i.test(ctx)) return "CONTACT_PAGE";
  if (
    /switchboard|conmutador|central telef|main line|reception|recepción|reservations? 1-?800|1-?800/i.test(
      ctx
    )
  ) {
    return "CORPORATE_SWITCHBOARD";
  }
  return "PROPERTY_OR_ORG_LINE";
}

export function extractContactsFromHtml(html, pageUrl, opts = {}) {
  const text = htmlToSearchableText(html);
  const phonesTel = new Set();
  const phonesBody = new Set();
  const emailsMailto = new Set();
  const emailsBody = new Set();
  let m;
  const telRe = new RegExp(TEL_HREF_RE.source, "gi");
  while ((m = telRe.exec(html))) {
    const p = normalizePhone(decodeURIComponent(m[1]));
    if (p) phonesTel.add(p);
  }
  const mailRe = new RegExp(MAILTO_RE.source, "gi");
  while ((m = mailRe.exec(html))) {
    const e = decodeURIComponent(m[1]).split("?")[0].trim().toLowerCase();
    if (e.includes("@")) emailsMailto.add(e);
  }
  for (const e of text.match(EMAIL_RE) || []) {
    const low = e.toLowerCase();
    if (!/example\.|domain\.|email@|sentry\.|wixpress|schema\.org|webpack|placeholder/i.test(low)) {
      emailsBody.add(low);
    }
  }
  for (const p of text.match(PHONE_RE) || []) {
    const n = normalizePhone(p);
    if (n && n.replace(/\D/g, "").length >= 10) phonesBody.add(n);
  }

  const urlClass = classifyOfficialUrl(pageUrl, opts);
  const brandCentral = urlClass.brand_central === true;

  const phonesPreferred = [...phonesTel];
  if (phonesPreferred.length < 2) {
    for (const p of phonesBody) {
      if (!phonesPreferred.includes(p)) phonesPreferred.push(p);
      if (phonesPreferred.length >= 4) break;
    }
  }
  const emailsPreferred = [...emailsMailto];
  for (const e of emailsBody) {
    if (!emailsPreferred.includes(e)) emailsPreferred.push(e);
    if (emailsPreferred.length >= 8) break;
  }

  const retrieved_at = opts.retrieved_at || new Date().toISOString();
  const publication_date = opts.publication_date || null;
  const leadershipPeople = extractLeadershipPeople(text, pageUrl, {
    publication_date,
    retrieved_at,
  });
  const contactMechanism = classifyContactPageMechanism(html, pageUrl);

  // Legacy delimiter fallback only when structured leadership extract is empty
  const people = leadershipPeople.map((p) => ({
    display_name: p.display_name,
    title: p.title,
    source_url: p.source_url || pageUrl,
    leadership_category: p.leadership_category,
    source_context: p.source_context,
    extraction_method: p.extraction_method,
    publication_date: p.publication_date,
    retrieved_at: p.retrieved_at,
    role_currency_note: p.role_currency_note,
  }));

  if (!people.length) {
    const titleBlocks =
      text.match(
        /([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ.'-]+){1,5})\s[,–—-]\s*((?:CEO|CFO|COO|Director|Managing Director|General Manager|Asset Manager|Vice President|VP|President|Presidente|Chairman|Founder|Socio|Director General|Gerente)[^.]{0,40})/gi
      ) || [];
    for (const block of titleBlocks.slice(0, 8)) {
      const parts = block.split(/\s[,–—-]\s/);
      if (parts.length < 2) continue;
      const display_name = parts[0].trim();
      let title = parts.slice(1).join(" - ").trim().slice(0, 80);
      if (!display_name || display_name.split(/\s+/).length < 2) continue;
      if (/cookie|sitio|navegaci|configurar|rechazar|aceptar|^te ci|ndiente/i.test(display_name + " " + title)) {
        continue;
      }
      people.push({
        display_name,
        title,
        source_url: pageUrl,
        leadership_category: classifyLeadershipCategory(title),
        extraction_method: "delimited_fallback",
        publication_date,
        retrieved_at,
        role_currency_note: publication_date
          ? null
          : "Source page undated — retrieval date is not appointment evidence.",
      });
    }
  }

  return {
    phones: phonesPreferred.slice(0, 8),
    emails: emailsPreferred.slice(0, 12),
    phones_from_tel: [...phonesTel],
    emails_from_mailto: [...emailsMailto],
    people,
    is_contact_page:
      contactMechanism.kind !== "NOT_CONTACT_PAGE" ||
      /contact|contacto|contáctenos|fale conosco|contato/i.test(pageUrl + " " + text.slice(0, 800)),
    contact_page_mechanism: contactMechanism,
    brand_central: brandCentral,
    url_class: urlClass,
    likely_dynamic_miss:
      phonesTel.size === 0 &&
      phonesBody.size === 0 &&
      /react|__NEXT_DATA__|ng-app/i.test(html) &&
      /tel[eé]fono|phone|contact/i.test(text.slice(0, 2000)),
  };
}

/**
 * CRITICAL V1.2 fix: SerpAPI client returns { ok, data: { organic_results } }.
 */
export async function serpGoogle(q, cost, { hl = "es", gl = "mx", num = 5 } = {}) {
  const res = await serpapiSearch({ engine: "google", q, num, hl, gl }, { timeoutMs: 25000 });
  cost.serpapi_searches += 1;
  const credits = Number(res?.creditsCharged ?? 1);
  cost.serpapi_usd += Number.isFinite(credits) ? SERPAPI_USD * credits : SERPAPI_USD;
  const organic =
    res?.data?.organic_results ||
    res?.organic_results ||
    res?.data?.organic ||
    res?.organic ||
    [];
  const parse_ok = Array.isArray(organic);
  const usedNested = Array.isArray(res?.data?.organic_results);
  return {
    organic: parse_ok ? organic : [],
    ok: res?.ok !== false,
    parse_ok,
    used_nested_data_shape: usedNested,
    error: res?.error || null,
  };
}

export function pickOfficialish(organic, nameHints = []) {
  const scored = (organic || []).map((r) => {
    const url = String(r.link || r.url || "");
    const title = String(r.title || "");
    const host = hostOf(url);
    let score = 0;
    if (/hotel|resort|gsf|dovetail|alliance|krystal|cambridge|sheraton|realinn|voco|marriott|ihg/i.test(host))
      score += 3;
    if (
      nameHints.some((h) =>
        host.includes(String(h).toLowerCase().replace(/[^a-z0-9]/g, "").slice(0, 8))
      )
    )
      score += 4;
    if (REJECT_HOST_RE.test(host)) score -= 8;
    if (/contact|contacto|oficial|official/i.test(url + title)) score += 2;
    const cls = classifyOfficialUrl(url);
    if (cls.brand_central) score -= 4;
    if (cls.property_like) score += 3;
    return { url, title, host, score, snippet: r.snippet || "", url_class: cls };
  });
  scored.sort((a, b) => b.score - a.score);
  return scored.filter((s) => s.score > 0 && s.url.startsWith("http"));
}

function relevanceForTitle(title, hotelName, ownerName) {
  const t = String(title || "").toLowerCase();
  if (
    /asset management|asset manager|inversiones|investment|ownership|propietario|desarrollo|development|brand|conversi[oó]n|franchise|gesti[oó]n hotelera/i.test(
      t
    )
  ) {
    return {
      relevance: PROPERTY_RELEVANCE.OWNER_ORG,
      why: `Title indicates functional responsibility (${title}) relevant to ownership / brand / asset decisions for ${hotelName}.`,
    };
  }
  if (/general manager|gerente general|hotel manager/i.test(t)) {
    return {
      relevance: PROPERTY_RELEVANCE.PROPERTY_DIRECT,
      why: `Property leadership title may influence on-site brand/operator execution for ${hotelName}, but is not automatically the ownership decision maker.`,
    };
  }
  if (/ceo|president|presidente|chairman|founder|director general|board/i.test(t)) {
    return {
      relevance: PROPERTY_RELEVANCE.OWNER_ORG,
      why: `Senior corporate title at ${ownerName || "owner org"} — candidate only; not auto-selected as decision maker without property/geographic evidence.`,
    };
  }
  return {
    relevance: PROPERTY_RELEVANCE.OWNER_ORG,
    why: `Appears affiliated with ${ownerName || "owner organization"}; relevance to ${hotelName} requires role/geography confirmation.`,
  };
}

function dedupeChannels(channels) {
  const seen = new Set();
  const out = [];
  for (const c of channels) {
    const k = `${c.kind}|${String(c.value || "").toLowerCase()}`;
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(c);
  }
  return out;
}

function organicOrEmpty(serp) {
  return Array.isArray(serp?.organic) ? serp.organic : [];
}

function uniqueUrls(items) {
  const seen = new Set();
  const out = [];
  for (const it of items) {
    const u = String(it.url || "").trim();
    if (!u || seen.has(u)) continue;
    seen.add(u);
    out.push(it);
  }
  return out;
}

/**
 * @param {object} input
 * @param {object} [input.ownerResearchCache]
 */
export async function discoverHotelContactsLive(input = {}) {
  const started = Date.now();
  const cost = { serpapi_searches: 0, serpapi_usd: 0, pages_fetched: 0, fetch_errors: 0 };
  const unresolved = [];
  const inferred_email_candidates = [];
  const sources = [];
  const blockers = [];
  const rejected = [];

  const hotelId = String(input.hotel_id || "").trim();
  const hotelName = String(input.hotel_name || "").trim();
  const city = input.city || "";
  const lang = input.language || "es";
  const hl = lang === "pt" ? "pt" : lang === "en" ? "en" : "es";
  const gl = lang === "pt" ? "br" : lang === "en" ? "us" : "mx";
  const maxQ = input.max_serp_queries ?? 4;

  const ownershipOk = Boolean(input.ownershipAnchor?.ok);
  const ownerId =
    input.ownershipAnchor?.anchor?.primary_owner_entity_id || input.owner_entity_id || null;
  const ownerName =
    input.ownershipAnchor?.anchor?.owner_display_name || input.owner_display_name || null;
  const ownerProvenance = ownershipOk
    ? "ownership_surface"
    : input.owner_entity_id
      ? "development_cohort_seed"
      : null;

  const trace = createEmptyTrace({ hotel_id: hotelId, hotel_name: hotelName });
  trace.owner = {
    source: ownershipOk ? "ownership_evidence" : ownerId ? "cohort_seed" : "unresolved",
    owner_entity_id: ownerId,
    owner_display_name: ownerName,
    evidenced: ownershipOk,
  };
  if (!ownershipOk && ownerId) addCode(trace, CI_FAILURE_CODE.OWNER_SEED_ONLY, "seed_is_search_hypothesis");
  if (!ownerId) {
    unresolved.push(UNRESOLVED_REASON.OWNER_UNRESOLVED);
    addCode(trace, CI_FAILURE_CODE.OWNER_EVIDENCE_MISSING, "no_owner_id");
  }

  if (!String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim()) {
    blockers.push({
      code: "SERPAPI_KEY_MISSING",
      detail: "SERPAPI_KEY required for live native discovery searches",
    });
    addCode(trace, CI_FAILURE_CODE.SEARCH_NOT_EXECUTED, "SERPAPI_KEY_MISSING");
  }

  const hotelChannels = [];
  const orgChannels = [];
  const contactPageFallbacks = [];
  const brandCentralChannels = [];
  const people = [];

  const seedSite = input.seedHints?.hotel_website || null;
  const seedOwnerSite = input.seedHints?.owner_website || null;

  let censusJoin = null;
  try {
    censusJoin = await joinCensusContactByRecordId(hotelId, {
      fixtureRecord: input.censusFixture || null,
    });
    if (censusJoin.ok && censusJoin.record) {
      const r = censusJoin.record;
      trace.census = {
        joined: true,
        phone: r.phone,
        email: r.email,
        website: r.website,
        phone_source: "Hotel Property Census.Phone",
        website_source: "Hotel Property Census.Official Property URL",
        verification_status: r.verification_status,
        property_identity_key: r.property_identity_key,
        error: null,
      };
      trace.source_system_ids = {
        airtable_record_id: r.airtable_record_id,
        property_identity_key: r.property_identity_key,
        hbx_hotel_code: r.hbx_hotel_code,
      };
      addCode(trace, CI_FAILURE_CODE.CENSUS_JOIN_OK, null);
      if (r.phone) {
        hotelChannels.push(
          createChannel({
            kind: CHANNEL_KIND.HOTEL_PHONE,
            value: r.phone,
            display_label: "Hotel phone (Census candidate)",
            attribution: ATTRIBUTION.PROPERTY,
            line_type: "MAIN",
            verified: false,
            deliverability: DELIVERABILITY.UNKNOWN,
            property_relevance: PROPERTY_RELEVANCE.PROPERTY_DIRECT,
            customer_caveat:
              "From Hotel Property Census — candidate only; not auto-upgraded to officially verified.",
            evidence: [
              createEvidenceRef({
                source_title: "Hotel Property Census",
                source_url: null,
                source_type: "census",
                observed_at: r.last_reviewed_at || new Date().toISOString(),
                excerpt: `Phone field · confidence=${r.identity_confidence || "unknown"}`,
              }),
            ],
            provenance: {
              system: "hotel_property_census",
              field: "Phone",
              verification_status: "CENSUS_CANDIDATE_NOT_AUTO_VERIFIED",
            },
          })
        );
      } else {
        addCode(trace, CI_FAILURE_CODE.CENSUS_FIELD_ABSENT, "phone");
      }
      if (!r.website) addCode(trace, CI_FAILURE_CODE.CENSUS_FIELD_ABSENT, "website");
      if (!r.email) addCode(trace, CI_FAILURE_CODE.CENSUS_FIELD_ABSENT, "email_not_on_census_map");
    } else {
      trace.census.joined = false;
      trace.census.error = censusJoin?.detail || "join_failed";
      addCode(trace, censusJoin?.code || CI_FAILURE_CODE.IDENTITY_JOIN_FAILURE, censusJoin?.detail);
    }
  } catch (err) {
    addCode(trace, CI_FAILURE_CODE.IDENTITY_JOIN_FAILURE, String(err?.message || err).slice(0, 160));
  }

  const censusWebsite = censusJoin?.record?.website || null;

  try {
    if (!blockers.length) {
      const hotelQuery =
        `"${hotelName}" ${city} ${lang === "en" ? "official website phone contact" : lang === "pt" ? "site oficial telefone contato" : "sitio oficial teléfono contacto"}`.trim();
      const serp = await serpGoogle(hotelQuery, cost, { hl, gl, num: 8 });
      trace.queries.push({
        kind: "serp_hotel",
        query: hotelQuery,
        result_count: serp.organic.length,
        used_nested_data_shape: serp.used_nested_data_shape,
        ok: serp.ok,
      });
      sources.push({
        kind: "serp_hotel",
        query: hotelQuery,
        result_count: serp.organic.length,
        used_nested_data_shape: serp.used_nested_data_shape,
      });

      if (!serp.ok) {
        addCode(
          trace,
          CI_FAILURE_CODE.SEARCH_NOT_EXECUTED,
          String(serp.error || "serpapi_ok_false").slice(0, 120)
        );
      } else if (!serp.used_nested_data_shape && serp.organic.length === 0) {
        addCode(trace, CI_FAILURE_CODE.SEARCH_RESULTS_UNPARSED, "expected_data.organic_results");
      } else if (serp.organic.length === 0) {
        addCode(trace, CI_FAILURE_CODE.SEARCH_EMPTY, hotelQuery.slice(0, 80));
      }

      const picks = pickOfficialish(organicOrEmpty(serp), [
        hotelName,
        "krystal",
        "cambridge",
        "sheraton",
        "realinn",
        "voco",
        "marriott",
        "ihg",
      ]);
      for (const p of picks.slice(0, 6)) {
        trace.urls_discovered.push({ url: p.url, title: p.title, score: p.score, from: "serp_hotel" });
      }

      const urls = [];
      if (censusWebsite) urls.push({ url: censusWebsite, from: "census" });
      if (seedSite) urls.push({ url: seedSite, from: "seed" });
      for (const p of picks.slice(0, 4)) urls.push({ url: p.url, from: "serp" });

      let fetchedAny = false;
      for (const { url, from } of uniqueUrls(urls).slice(0, 5)) {
        const cls = classifyOfficialUrl(url, { hotelName });
        if (cls.status === "rejected") {
          rejected.push({ url, reason: cls.code, detail: cls.detail, from });
          addCode(trace, CI_FAILURE_CODE.WRONG_DOMAIN, url);
          continue;
        }
        if (cls.brand_central) {
          rejected.push({
            url,
            reason: CI_FAILURE_CODE.BRAND_CENTRAL_RESERVATIONS,
            detail: cls.detail,
            from,
          });
          addCode(trace, CI_FAILURE_CODE.BRAND_CENTRAL_RESERVATIONS, url);
        }

        const page = await fetchResearchPage(url, { timeoutMs: 14000 });
        cost.pages_fetched += 1;
        if (!page.ok) {
          cost.fetch_errors += 1;
          const blocked = page.blocked || page.status === 403 || page.status === 429;
          trace.fetch_failures.push({
            url,
            from,
            error: page.error || page.status || "fetch_failed",
            blocked: Boolean(blocked),
          });
          addCode(
            trace,
            blocked ? CI_FAILURE_CODE.POLICY_RESTRICTED : CI_FAILURE_CODE.FETCH_FAILED,
            `${url} · ${page.error || page.status}`
          );
          continue;
        }
        fetchedAny = true;
        trace.pages_fetched.push({ url: page.url, from, bytes: (page.text || "").length });
        const extracted = extractContactsFromHtml(page.text, page.url, { hotelName });
        for (const ph of extracted.phones) {
          trace.contacts_in_content.phones.push({ value: ph, url: page.url });
        }
        for (const em of extracted.emails) {
          trace.contacts_in_content.emails.push({ value: em, url: page.url });
        }

        if (extracted.likely_dynamic_miss) {
          addCode(trace, CI_FAILURE_CODE.DYNAMIC_CONTENT, page.url);
        }

        sources.push({
          kind: "hotel_page",
          url: page.url,
          from,
          phones: extracted.phones.length,
          emails: extracted.emails.length,
          contact_page: extracted.is_contact_page,
          brand_central: extracted.brand_central,
        });

        if (extracted.brand_central) {
          for (const phone of extracted.phones.slice(0, 2)) {
            brandCentralChannels.push(
              createChannel({
                kind: CHANNEL_KIND.SWITCHBOARD,
                value: phone,
                display_label: "Brand central reservations (not property-specific)",
                attribution: ATTRIBUTION.ORGANIZATION,
                line_type: "BRAND_CENTRAL",
                verified: false,
                deliverability: DELIVERABILITY.UNKNOWN,
                property_relevance: PROPERTY_RELEVANCE.BRAND_NETWORK,
                customer_caveat:
                  "Brand central / reservations number — not counted as hotel property phone.",
                evidence: [
                  createEvidenceRef({
                    source_title: "Brand central / explore page",
                    source_url: page.url,
                    source_type: "brand_central",
                    observed_at: new Date().toISOString(),
                  }),
                ],
              })
            );
          }
          continue;
        }

        if (extracted.is_contact_page && !extracted.phones.length && !extracted.emails.length) {
          contactPageFallbacks.push(
            createChannel({
              kind: CHANNEL_KIND.HOTEL_WEBSITE,
              value: page.url,
              display_label: "Hotel contact page (fallback)",
              attribution: ATTRIBUTION.OFFICIAL,
              claimed_official: true,
              property_relevance: PROPERTY_RELEVANCE.PROPERTY_DIRECT,
              evidence: [
                createEvidenceRef({
                  source_title: "Hotel contact page",
                  source_url: page.url,
                  source_type: "first_party",
                  observed_at: new Date().toISOString(),
                }),
              ],
            })
          );
          if (!trace.official_website.url) {
            trace.official_website = { status: "resolved_contact_page", url: page.url };
          }
        } else if (!trace.official_website.url || trace.official_website.status === "unknown") {
          trace.official_website = {
            status: extracted.url_class?.property_like ? "resolved_property" : "resolved",
            url: page.url,
          };
        }

        for (const phone of extracted.phones.slice(0, 2)) {
          const digits = phone.replace(/\D/g, "");
          const looksBrandTollFree =
            /ihg\.com|marriott\.com|hilton\.com/i.test(page.url) &&
            /^(1800|1888|1877|1855|1844|1833)/.test(digits);
          if (looksBrandTollFree) {
            brandCentralChannels.push(
              createChannel({
                kind: CHANNEL_KIND.SWITCHBOARD,
                value: phone,
                display_label: "Brand central reservations (not property-specific)",
                attribution: ATTRIBUTION.ORGANIZATION,
                line_type: "BRAND_CENTRAL",
                verified: false,
                deliverability: DELIVERABILITY.UNKNOWN,
                property_relevance: PROPERTY_RELEVANCE.BRAND_NETWORK,
                customer_caveat:
                  "Toll-free brand reservations on brand.com property page — not counted as property-specific phone.",
                evidence: [
                  createEvidenceRef({
                    source_title: "Brand property page (central reservations pattern)",
                    source_url: page.url,
                    source_type: "brand_central",
                    observed_at: new Date().toISOString(),
                  }),
                ],
              })
            );
            rejected.push({
              value: phone,
              reason: CI_FAILURE_CODE.BRAND_CENTRAL_RESERVATIONS,
              detail: "toll_free_on_brand_property_page",
            });
            continue;
          }
          hotelChannels.push(
            createChannel({
              kind: CHANNEL_KIND.HOTEL_PHONE,
              value: phone,
              display_label: "Hotel phone",
              attribution: ATTRIBUTION.PROPERTY,
              line_type: "MAIN",
              verified: false,
              deliverability: DELIVERABILITY.UNKNOWN,
              property_relevance: PROPERTY_RELEVANCE.PROPERTY_DIRECT,
              evidence: [
                createEvidenceRef({
                  source_title: "Hotel website / property page",
                  source_url: page.url,
                  source_type: "first_party_or_listing",
                  observed_at: new Date().toISOString(),
                }),
              ],
            })
          );
          trace.contacts_extracted.phones.push({ value: phone, url: page.url, kind: "HOTEL_PHONE" });
        }
        for (const email of extracted.emails.slice(0, 3)) {
          if (isGenericMailboxEmail(email) || isRoleMailboxEmail(email)) {
            hotelChannels.push(
              createChannel({
                kind: isRoleMailboxEmail(email) ? CHANNEL_KIND.ROLE_MAILBOX : CHANNEL_KIND.HOTEL_EMAIL,
                value: email,
                display_label: isRoleMailboxEmail(email) ? "Role mailbox" : "Hotel / property mailbox",
                attribution: isRoleMailboxEmail(email)
                  ? ATTRIBUTION.ROLE_MAILBOX
                  : ATTRIBUTION.ORGANIZATION,
                verified: false,
                deliverability: DELIVERABILITY.UNKNOWN,
                property_relevance: PROPERTY_RELEVANCE.PROPERTY_DIRECT,
                evidence: [
                  createEvidenceRef({
                    source_title: "Published on hotel page",
                    source_url: page.url,
                    source_type: "first_party",
                    observed_at: new Date().toISOString(),
                  }),
                ],
              })
            );
            trace.contacts_extracted.emails.push({ value: email, url: page.url, kind: "HOTEL_EMAIL" });
          } else {
            inferred_email_candidates.push({
              email,
              stage: "INFERRED_CANDIDATE",
              attribution: ATTRIBUTION.INFERRED,
              usage_rights: USAGE_RIGHTS.INTERNAL_ONLY,
              source_url: page.url,
              note: "Staged inferred candidate — not actionable coverage.",
            });
            rejected.push({
              value: email,
              reason: CI_FAILURE_CODE.POLICY_RESTRICTED,
              detail: "inferred_named_email_not_published_as_actionable",
            });
          }
        }

        const propertyPhones = hotelChannels.filter((c) => c.kind === CHANNEL_KIND.HOTEL_PHONE);
        if (
          propertyPhones.length >= 1 &&
          hotelChannels.some(
            (c) => c.kind === CHANNEL_KIND.HOTEL_EMAIL || c.kind === CHANNEL_KIND.ROLE_MAILBOX
          )
        ) {
          break;
        }
      }

      if (!fetchedAny && !censusWebsite && !seedSite && picks.length === 0) {
        addCode(trace, CI_FAILURE_CODE.OFFICIAL_URL_MISSING, "no_urls_to_fetch");
      }
      if (
        fetchedAny &&
        !hotelChannels.some(
          (c) => c.kind === CHANNEL_KIND.HOTEL_PHONE || c.kind === CHANNEL_KIND.HOTEL_EMAIL
        ) &&
        !contactPageFallbacks.length
      ) {
        addCode(trace, CI_FAILURE_CODE.NO_CONTACT_FOUND_AFTER_SEARCH, "hotel_pages_fetched");
      }
    }

    const cache = input.ownerResearchCache || null;
    // Once-per-owner: reuse company research across siblings; ownership_evidenced stays per-hotel.
    const canReuse =
      cache && ownerId && cache.get(ownerId) && input.allow_owner_reuse !== false;

    if (canReuse) {
      const reused = cache.takeReuse(ownerId);
      sources.push({
        kind: "owner_research_reuse",
        owner_entity_id: ownerId,
        reuse_count: reused.reuse_count,
      });
      for (const ch of reused.orgChannels || []) orgChannels.push(ch);
      for (const p of reused.people || []) people.push(structuredClone(p));
      for (const inf of reused.inferred_email_candidates || []) inferred_email_candidates.push(inf);
      trace.budget.stopping_reason = "owner_research_reused";
    } else if (
      !blockers.length &&
      ownerName &&
      cost.serpapi_searches < maxQ &&
      !input.skip_owner_research
    ) {
      if (!ownershipOk) {
        addCode(
          trace,
          CI_FAILURE_CODE.OWNER_SEED_ONLY,
          "researching_company_domain_not_ownership_proof"
        );
      }

      const orgQuery =
        lang === "en"
          ? `"${ownerName}" hotel (contact OR "investor relations" OR team OR leadership)`
          : `"${ownerName}" hotel (contacto OR "relaciones con inversionistas" OR equipo OR directivos)`;
      const serp = await serpGoogle(orgQuery, cost, { hl, gl, num: 8 });
      trace.queries.push({
        kind: "serp_owner",
        query: orgQuery,
        result_count: serp.organic.length,
        used_nested_data_shape: serp.used_nested_data_shape,
      });
      sources.push({
        kind: "serp_owner",
        query: orgQuery,
        result_count: serp.organic.length,
        used_nested_data_shape: serp.used_nested_data_shape,
      });
      if (serp.organic.length === 0) addCode(trace, CI_FAILURE_CODE.SEARCH_EMPTY, "owner_serp");

      const picks = pickOfficialish(organicOrEmpty(serp), [
        ownerName,
        "gsfhotels",
        "dovetail",
        "alliance",
      ]);
      for (const p of picks.slice(0, 5)) {
        trace.urls_discovered.push({
          url: p.url,
          title: p.title,
          score: p.score,
          from: "serp_owner",
        });
      }
      const urls = [];
      if (seedOwnerSite) urls.push({ url: seedOwnerSite, from: "seed_owner" });
      for (const p of picks.slice(0, 3)) urls.push({ url: p.url, from: "serp_owner" });

      for (const { url } of uniqueUrls(urls).slice(0, 4)) {
        if (!isPlausibleOwnerPage(url, ownerName, seedOwnerSite)) {
          rejected.push({
            url,
            reason: CI_FAILURE_CODE.ATTRIBUTION_UNRESOLVED,
            detail: "owner_page_host_does_not_match_owner_or_seed_domain",
          });
          addCode(trace, CI_FAILURE_CODE.ATTRIBUTION_UNRESOLVED, url);
          continue;
        }
        const page = await fetchResearchPage(url, { timeoutMs: 14000 });
        cost.pages_fetched += 1;
        if (!page.ok) {
          cost.fetch_errors += 1;
          trace.fetch_failures.push({ url, error: page.error || page.status });
          addCode(trace, CI_FAILURE_CODE.FETCH_FAILED, url);
          continue;
        }
        trace.pages_fetched.push({ url: page.url, from: "owner", bytes: (page.text || "").length });
        const extracted = extractContactsFromHtml(page.text, page.url);
        sources.push({
          kind: "owner_page",
          url: page.url,
          phones: extracted.phones.length,
          emails: extracted.emails.length,
          people: extracted.people.length,
        });

        orgChannels.push(
          createChannel({
            kind: CHANNEL_KIND.ORG_WEBSITE,
            value: page.url,
            display_label: "Organization website",
            attribution: ATTRIBUTION.ORGANIZATION,
            property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
            customer_caveat: ownershipOk
              ? undefined
              : "Company domain via seed hypothesis — does not prove current ownership of this hotel.",
            evidence: [
              createEvidenceRef({
                source_title: "Owner organization page",
                source_url: page.url,
                source_type: "first_party_or_corp",
                observed_at: new Date().toISOString(),
              }),
            ],
          })
        );

        for (const phone of extracted.phones.slice(0, 2)) {
          orgChannels.push(
            createChannel({
              kind: CHANNEL_KIND.SWITCHBOARD,
              value: phone,
              display_label: "Organization switchboard",
              attribution: ATTRIBUTION.ORGANIZATION,
              line_type: "SWITCHBOARD",
              claimed_direct_personal: false,
              deliverability: DELIVERABILITY.UNKNOWN,
              property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
              customer_caveat: "Corporate line — not a direct personal number.",
              evidence: [
                createEvidenceRef({
                  source_title: "Published on organization page",
                  source_url: page.url,
                  source_type: "first_party_or_corp",
                  observed_at: new Date().toISOString(),
                }),
              ],
            })
          );
          trace.contacts_extracted.phones.push({
            value: phone,
            url: page.url,
            kind: "ORG_SWITCHBOARD",
          });
        }
        for (const email of extracted.emails.slice(0, 4)) {
          if (isGenericMailboxEmail(email) || isRoleMailboxEmail(email)) {
            orgChannels.push(
              createChannel({
                kind: isRoleMailboxEmail(email) ? CHANNEL_KIND.ROLE_MAILBOX : CHANNEL_KIND.ORG_EMAIL,
                value: email,
                display_label: isRoleMailboxEmail(email)
                  ? "Organization role mailbox"
                  : "Organization mailbox",
                attribution: isRoleMailboxEmail(email)
                  ? ATTRIBUTION.ROLE_MAILBOX
                  : ATTRIBUTION.ORGANIZATION,
                deliverability: DELIVERABILITY.UNKNOWN,
                property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
                evidence: [
                  createEvidenceRef({
                    source_title: "Published organization email",
                    source_url: page.url,
                    source_type: "first_party_or_corp",
                    observed_at: new Date().toISOString(),
                  }),
                ],
              })
            );
            trace.contacts_extracted.emails.push({
              value: email,
              url: page.url,
              kind: "ORG_EMAIL",
            });
          } else {
            inferred_email_candidates.push({
              email,
              stage: "INFERRED_CANDIDATE",
              attribution: ATTRIBUTION.INFERRED,
              usage_rights: USAGE_RIGHTS.INTERNAL_ONLY,
              source_url: page.url,
              note: "Named-looking email on org page — staged only.",
            });
          }
        }

        for (const p of extracted.people.slice(0, 4)) {
          if (/press|media|comunicación|comunicacion|public relations|pr\b/i.test(p.title || "")) {
            rejected.push({
              value: p.display_name,
              reason: CI_FAILURE_CODE.POLICY_RESTRICTED,
              detail: "press_contact_not_development_dm",
            });
            continue;
          }
          const rel = relevanceForTitle(p.title, hotelName, ownerName);
          people.push(
            createPersonContact({
              display_name: p.display_name,
              title: p.title,
              organization_entity_id: ownerId,
              organization_name: ownerName,
              role_observed_at: new Date().toISOString(),
              role_currency: ROLE_CURRENCY.UNKNOWN,
              property_relevance: rel.relevance,
              channels: [],
              evidence: [
                createEvidenceRef({
                  source_title: "Team / leadership page cue",
                  source_url: p.source_url,
                  source_type: "first_party_or_corp",
                  observed_at: new Date().toISOString(),
                  excerpt: `${p.display_name} — ${p.title}`,
                }),
              ],
              unresolved_reasons: [UNRESOLVED_REASON.NO_PERSON_EVIDENCE],
              why_relevant: rel.why,
              publication_label: ownershipOk
                ? "EVIDENCED_ORG_PERSON_CANDIDATE"
                : "SEED_OWNER_PERSON_HYPOTHESIS",
            })
          );
        }
        if (orgChannels.some((c) => c.kind !== CHANNEL_KIND.ORG_WEBSITE)) break;
      }

      if (cache && ownerId) {
        cache.set(ownerId, {
          owner_display_name: ownerName,
          orgChannels: structuredClone(orgChannels),
          people: structuredClone(people),
          inferred_email_candidates: structuredClone(inferred_email_candidates),
          has_org_phone_email: orgChannels.some((c) =>
            [
              CHANNEL_KIND.ORG_PHONE,
              CHANNEL_KIND.ORG_EMAIL,
              CHANNEL_KIND.ROLE_MAILBOX,
              CHANNEL_KIND.SWITCHBOARD,
            ].includes(c.kind)
          ),
          ownership_evidenced_when_cached: ownershipOk,
        });
      }
    } else if (cost.serpapi_searches >= maxQ) {
      addCode(trace, CI_FAILURE_CODE.BUDGET_EXHAUSTED, `max_serp_queries=${maxQ}`);
      trace.budget.stopping_reason = "max_serp_queries";
    }
  } catch (err) {
    blockers.push({
      code: "LIVE_DISCOVERY_ERROR",
      detail: String(err?.message || err).slice(0, 240),
    });
  }

  for (const kp of input.seedHints?.known_people || []) {
    if (!kp?.display_name) continue;
    const rel = relevanceForTitle(kp.title, hotelName, ownerName);
    const personChannels = [];
    if (kp.linkedin && /^https?:\/\//i.test(kp.linkedin)) {
      personChannels.push(
        createChannel({
          kind: CHANNEL_KIND.PERSON_LINKEDIN,
          value: kp.linkedin,
          display_label: "Professional profile",
          attribution: ATTRIBUTION.NAMED_PERSON,
          verified: false,
          usage_rights: USAGE_RIGHTS.INTERNAL_ONLY,
        })
      );
    }
    people.push(
      createPersonContact({
        display_name: kp.display_name,
        title: kp.title || null,
        organization_entity_id: ownerId,
        organization_name: ownerName,
        role_observed_at: kp.role_observed_at || null,
        role_currency: ROLE_CURRENCY.UNKNOWN,
        property_relevance: rel.relevance,
        channels: personChannels,
        evidence: kp.evidence || [],
        unresolved_reasons: [
          UNRESOLVED_REASON.NO_PERSON_EVIDENCE,
          ...(ownershipOk ? [] : ["OWNER_SEED_HYPOTHESIS_ONLY"]),
        ],
        why_relevant: kp.why_relevant || rel.why,
        publication_label: "SEED_PERSON_HYPOTHESIS_NOT_CONFIRMED_DM",
        customer_caveat:
          "Cohort seed person — search hypothesis only; not confirmed as current decision maker.",
      })
    );
  }

  const byName = new Map();
  for (const p of people) {
    const key = String(p.display_name || "")
      .toLowerCase()
      .normalize("NFKD")
      .replace(/[\u0300-\u036f]/g, "");
    if (!key) continue;
    const prev = byName.get(key);
    if (!prev) byName.set(key, p);
    else {
      const score = (x) =>
        /asset|development|invers|brand|convers/i.test(String(x.title || ""))
          ? 3
          : x.publication_label === "SEED_PERSON_HYPOTHESIS_NOT_CONFIRMED_DM"
            ? 0
            : 1;
      if (score(p) > score(prev)) byName.set(key, p);
    }
  }
  const peopleDeduped = [...byName.values()].slice(0, 5);

  const corporateRoute = orgChannels.find((c) =>
    [CHANNEL_KIND.ORG_EMAIL, CHANNEL_KIND.ROLE_MAILBOX, CHANNEL_KIND.SWITCHBOARD].includes(c.kind)
  );
  for (const p of peopleDeduped) {
    const hasDirect = (p.channels || []).some(
      (c) => c.kind === CHANNEL_KIND.PERSON_EMAIL || c.kind === CHANNEL_KIND.PERSON_PHONE
    );
    if (!hasDirect && corporateRoute) {
      p.indirect_corporate_route = {
        ...corporateRoute,
        display_label: (corporateRoute.display_label || "Corporate route") + " (indirect)",
        customer_caveat: "Indirect organization route — not confirmed access to this person.",
        property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
      };
    }
    p.contact_source_date = (p.evidence || [])[0]?.observed_at || null;
    p.role_check_date = p.role_observed_at || null;
  }

  const hotelChannelsDeduped = dedupeChannels(hotelChannels);
  const orgChannelsDeduped = dedupeChannels(orgChannels);

  const hasHotelPhone = hotelChannelsDeduped.some((c) => c.kind === CHANNEL_KIND.HOTEL_PHONE);
  const hasHotelEmail = hotelChannelsDeduped.some(
    (c) => c.kind === CHANNEL_KIND.HOTEL_EMAIL || c.kind === CHANNEL_KIND.ROLE_MAILBOX
  );
  const hasHotelPhoneEmail = hasHotelPhone || hasHotelEmail;
  const hasOrgPhoneEmail = orgChannelsDeduped.some((c) =>
    [
      CHANNEL_KIND.ORG_PHONE,
      CHANNEL_KIND.ORG_EMAIL,
      CHANNEL_KIND.ROLE_MAILBOX,
      CHANNEL_KIND.SWITCHBOARD,
    ].includes(c.kind)
  );
  const hasOrgWebsiteOnly =
    orgChannelsDeduped.some((c) => c.kind === CHANNEL_KIND.ORG_WEBSITE) && !hasOrgPhoneEmail;

  const evidencedPeople = peopleDeduped.filter(
    (p) => p.publication_label !== "SEED_PERSON_HYPOTHESIS_NOT_CONFIRMED_DM"
  );

  if (!hasHotelPhoneEmail && !contactPageFallbacks.length) {
    unresolved.push(UNRESOLVED_REASON.NO_HOTEL_CONTACT);
  }
  if (!hasOrgPhoneEmail && !hasOrgWebsiteOnly) {
    unresolved.push(UNRESOLVED_REASON.NO_ORG_ROUTE);
  }
  if (!evidencedPeople.length) unresolved.push(UNRESOLVED_REASON.NO_PERSON_EVIDENCE);

  if (!trace.official_website.url) {
    if (censusWebsite) {
      const cls = classifyOfficialUrl(censusWebsite, { hotelName });
      trace.official_website = {
        status: cls.brand_central ? "census_brand_central" : "census_candidate",
        url: censusWebsite,
      };
    } else {
      trace.official_website = { status: "missing", url: null };
      if (!trace.codes.some((c) => c.code === CI_FAILURE_CODE.OFFICIAL_URL_MISSING)) {
        addCode(trace, CI_FAILURE_CODE.OFFICIAL_URL_MISSING, null);
      }
    }
  }

  trace.rejected = rejected;
  trace.budget = {
    serpapi_searches: cost.serpapi_searches,
    serpapi_usd: Number(cost.serpapi_usd.toFixed(4)),
    pages_fetched: cost.pages_fetched,
    fetch_errors: cost.fetch_errors,
    stopping_reason:
      trace.budget.stopping_reason ||
      (cost.serpapi_searches >= maxQ ? "max_serp_queries" : "complete"),
  };

  const orgRoute = buildOrganizationContactRoute({
    owner_entity_id: ownerId,
    owner_display_name: ownerName,
    channels: orgChannelsDeduped.filter(
      (c) => c.kind !== CHANNEL_KIND.ORG_WEBSITE || hasOrgWebsiteOnly
    ),
  });

  return {
    ok: blockers.length === 0,
    version: LIVE_NATIVE_DISCOVERY_VERSION,
    hotel_id: hotelId,
    hotel_name: hotelName,
    owner_entity_id: ownerId,
    owner_display_name: ownerName,
    owner_resolution_provenance: ownerProvenance,
    ownership_evidenced: ownershipOk,
    hotel_contact: { channels: [...hotelChannelsDeduped, ...contactPageFallbacks] },
    brand_central_reservations: brandCentralChannels,
    organization_contact_route: orgRoute,
    organization_website_only: orgChannelsDeduped.filter((c) => c.kind === CHANNEL_KIND.ORG_WEBSITE),
    people: peopleDeduped,
    people_public_candidates: evidencedPeople,
    inferred_email_candidates,
    unresolved_reasons: [...new Set(unresolved)],
    sources,
    execution_trace: trace,
    cost,
    elapsed_ms: Date.now() - started,
    blockers,
    flags: {
      has_official_website_resolved: Boolean(
        trace.official_website?.url &&
          !["missing", "unknown"].includes(trace.official_website.status)
      ),
      has_hotel_phone: hasHotelPhone,
      has_hotel_email: hasHotelEmail,
      has_hotel_phone_or_email: hasHotelPhoneEmail,
      has_hotel_contact_page_fallback: contactPageFallbacks.length > 0,
      has_evidenced_current_owner: ownershipOk,
      has_owner_any_provenance: Boolean(ownerId),
      has_org_phone_or_email: hasOrgPhoneEmail,
      has_org_website_only: hasOrgWebsiteOnly,
      has_org_contact_page_fallback: orgChannelsDeduped.some(
        (c) => c.kind === CHANNEL_KIND.ORG_WEBSITE
      ),
      has_relevant_person_evidenced: evidencedPeople.length > 0,
      has_relevant_person_seed_only: peopleDeduped.some(
        (p) => p.publication_label === "SEED_PERSON_HYPOTHESIS_NOT_CONFIRMED_DM"
      ),
      has_direct_person_email: evidencedPeople.some((p) =>
        (p.channels || []).some(
          (c) => c.kind === CHANNEL_KIND.PERSON_EMAIL && c.attribution === ATTRIBUTION.NAMED_PERSON
        )
      ),
      has_direct_person_phone: evidencedPeople.some((p) =>
        (p.channels || []).some(
          (c) => c.kind === CHANNEL_KIND.PERSON_PHONE && c.attribution === ATTRIBUTION.NAMED_PERSON
        )
      ),
      has_person_with_indirect_corporate_route: peopleDeduped.some((p) => p.indirect_corporate_route),
      deliverability_checked_email: false,
      independent_precision: "NOT_REVIEWED",
      census_joined: Boolean(trace.census?.joined),
      census_phone_reused: Boolean(trace.census?.phone),
    },
    contact_source_date: new Date().toISOString(),
    role_check_date: peopleDeduped[0]?.role_check_date || null,
  };
}

export { classifyOfficialUrl as classifyUrlForContacts };
