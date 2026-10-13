/**
 * Contact Intelligence — person + attributed business email discovery (Context.dev).
 * Org mailboxes (contacto@…) never satisfy the person-email objective.
 */

import {
  contextDevExtract,
  contextDevSearch,
  contextDevScrapeMarkdown,
  CONTEXT_DEV_ENDPOINTS,
} from "../../context-dev/index.js";
import { createContextDevCreditLedger, CONTEXT_DEV_CREDIT_COSTS } from "../../context-dev/credit-ledger.js";
import {
  ATTRIBUTION,
  CHANNEL_KIND,
  DELIVERABILITY,
  ROLE_CURRENCY,
  USAGE_RIGHTS,
  PROPERTY_RELEVANCE,
  UNRESOLVED_REASON,
} from "./vocabulary.js";
import { createChannel, createEvidenceRef, createPersonContact } from "./contact-record.js";
import {
  isGenericMailboxEmail,
  isRoleMailboxEmail,
  isNamedPersonEmail,
} from "./dimensions.js";
import { serpGoogle } from "./live-native-discovery.js";
import { LEADERSHIP_CATEGORY, classifyLeadershipCategory } from "./leadership-extraction.js";
import {
  buildInvestorSearchQueries,
  rankInvestorDocumentUrls,
  extractAttributedPersonEmailsFromText,
} from "./investor-document-discovery.js";

export const PERSON_EMAIL_DISCOVERY_VERSION = "contact-person-email-discovery-v1";

const EMAIL_RE = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
const GENERIC_LOCAL =
  /^(info|contact|contacto|hello|admin|office|press|media|reservas|reservations|sales|ventas|noreply|no-reply)$/i;

/** Third-party email directories — never count as first-party person attribution. */
const EMAIL_AGGREGATOR_HOST_RE =
  /contactout\.com|apollo\.io|zoominfo\.com|rocketreach\.co|hunter\.io|snov\.io|clearbit\.com|lusha\.com|signalhire\.com|seamless\.ai|anymailfinder\.com|voilanorbert\.com/i;

export function isEmailAggregatorUrl(url) {
  try {
    return EMAIL_AGGREGATOR_HOST_RE.test(new URL(url).hostname);
  } catch {
    return EMAIL_AGGREGATOR_HOST_RE.test(String(url || ""));
  }
}

/** Live validation status for wrapper endpoints used in this lane. */
export const WRAPPER_LIVE_VALIDATION = Object.freeze({
  extract: { validated: true, proof: "reports/context-dev-prove-extract.json" },
  search: { validated: false, proof: null }, // set true after successful live call in runner
  scrapeMarkdown: { validated: false, proof: null },
  brandRetrieve: { validated: false, proof: null },
  serpapiFallback: { validated: true, note: "Existing Contact Intelligence SerpAPI adapter" },
});

export function getEmailVerificationStatus(env = process.env) {
  const configured = Boolean(
    String(env.EMAIL_VERIFICATION_API_KEY || env.HUNTER_API_KEY || env.NEVERBOUNCE_API_KEY || "").trim()
  );
  return {
    status: configured ? "CONFIGURED_BUT_NOT_WIRED_IN_THIS_MODULE" : "VERIFICATION_NOT_CONFIGURED",
    note: configured
      ? "A verification-related env key exists, but no authorized Contact Intelligence verifier adapter is wired for this lane."
      : "No email-verification adapter configured. Mailbox acceptance checks are not available.",
  };
}

function domainHost(urlOrHost) {
  try {
    if (String(urlOrHost).includes("://")) {
      return new URL(urlOrHost).hostname.replace(/^www\./, "").toLowerCase();
    }
  } catch {
    /* ignore */
  }
  return String(urlOrHost || "")
    .replace(/^www\./, "")
    .toLowerCase();
}

function normalizeEmail(raw) {
  return String(raw || "")
    .trim()
    .toLowerCase()
    .replace(/^mailto:/i, "")
    .split("?")[0];
}

function isOrgGenericEmail(email) {
  return isGenericMailboxEmail(email) || isRoleMailboxEmail(email) || GENERIC_LOCAL.test(
    String(email).split("@")[0] || ""
  );
}

function nameTokens(displayName) {
  return String(displayName || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .split(/\s+/)
    .filter((t) => t.length >= 2 && !/^(de|del|la|los|las|von|van|jr|sr|lic|ing)$/i.test(t));
}

/**
 * Generate ≤3 same-domain inferred email candidates from observed local-part patterns.
 * Never labeled attributed/verified.
 */
export function generateInferredEmailCandidates(personName, companyDomain, observedNamedEmails = []) {
  const host = domainHost(companyDomain);
  if (!host || !personName) return [];

  const tokens = nameTokens(personName);
  if (tokens.length < 2) return [];

  const first = tokens[0];
  const last = tokens[tokens.length - 1];
  const patternsFromObserved = [];
  for (const em of observedNamedEmails) {
    const local = String(em).split("@")[0] || "";
    if (local.includes(".") && /^[a-z]+\.[a-z]+/.test(local)) patternsFromObserved.push("first.last");
    else if (local.includes("_")) patternsFromObserved.push("first_last");
    else if (local.length <= first.length + 1) patternsFromObserved.push("flast");
  }
  const patternSet = [...new Set(patternsFromObserved.length ? patternsFromObserved : ["first.last", "flast", "first"])];

  const out = [];
  for (const p of patternSet) {
    if (out.length >= 3) break;
    let local = null;
    if (p === "first.last") local = `${first}.${last}`;
    else if (p === "first_last") local = `${first}_${last}`;
    else if (p === "flast") local = `${first[0]}${last}`;
    else if (p === "first") local = first;
    if (!local) continue;
    const email = `${local}@${host}`;
    if (isOrgGenericEmail(email)) continue;
    out.push({
      email,
      attribution: ATTRIBUTION.INFERRED,
      pattern: p,
      basis: patternsFromObserved.length
        ? "same_domain_observed_naming_pattern"
        : "common_corporate_naming_pattern_no_observed_named_mailbox",
      usage_rights: USAGE_RIGHTS.INTERNAL_ONLY,
      deliverability: DELIVERABILITY.UNKNOWN,
      verification: "VERIFICATION_NOT_CONFIGURED",
    });
  }
  return out.slice(0, 3);
}

function excerptAround(text, needle, radius = 120) {
  const t = String(text || "");
  const idx = t.toLowerCase().indexOf(String(needle || "").toLowerCase());
  if (idx < 0) return null;
  return t.slice(Math.max(0, idx - radius), Math.min(t.length, idx + String(needle).length + radius)).replace(/\s+/g, " ").trim();
}

function personMentionsEmail(text, personName, email) {
  const t = String(text || "");
  const em = normalizeEmail(email);
  if (!t.toLowerCase().includes(em)) return false;
  const tokens = nameTokens(personName).filter((x) => x.length >= 4);
  if (!tokens.length) return false;
  // Require at least one strong name token within ~400 chars of the email
  const idx = t.toLowerCase().indexOf(em);
  const window = t.slice(Math.max(0, idx - 400), idx + em.length + 400).toLowerCase();
  return tokens.some((tok) => window.includes(tok));
}

async function chargedCall(ledger, kind, cost, meta, fn) {
  const gate = ledger.charge(kind, cost, meta);
  if (!gate.ok) return { ok: false, budget_blocked: true, error: gate, data: null };
  const result = await fn();
  if (!result.ok) {
    // Still spent — API charged on most failures after accept; keep ledger honest.
    return { ...result, budget_blocked: false, credit_entry: gate.entry };
  }
  return { ...result, budget_blocked: false, credit_entry: gate.entry };
}

/**
 * Live-validate Context.dev search (1 credit / ≤10 results).
 */
export async function validateContextDevSearch(ledger, { query = "Grupo Hotelero Santa Fe sitio oficial" } = {}) {
  const numResults = 10;
  const cost = ledger.estimateSearchCost(numResults);
  const result = await chargedCall(ledger, "search", cost, { query, validate: true }, () =>
    contextDevSearch({ query, numResults })
  );
  return {
    ...result,
    endpoint: CONTEXT_DEV_ENDPOINTS.search,
    validated_live: Boolean(result.ok && result.data?.results?.length >= 0),
  };
}

/**
 * Extract people from a starting URL (gobierno / about / team).
 */
export async function extractPeopleFromOrgPages(ledger, { url, instructions, maxPages = 4 } = {}) {
  const schema = {
    type: "object",
    properties: {
      page_kind: { type: "string" },
      executives: {
        type: "array",
        items: {
          type: "object",
          properties: {
            full_name: { type: "string" },
            title: { type: "string" },
            role_category: { type: "string" },
            source_page_url: { type: "string" },
            biography_excerpt: { type: "string" },
            attributed_email: {
              type: "string",
              description: "Only if an email is explicitly published next to this person. Never invent.",
            },
          },
          required: ["full_name", "title"],
          additionalProperties: false,
        },
      },
      named_person_emails: {
        type: "array",
        description: "Emails explicitly attributed to a named person (not contacto@/info@).",
        items: {
          type: "object",
          properties: {
            email: { type: "string" },
            person_name: { type: "string" },
            source_page_url: { type: "string" },
            excerpt: { type: "string" },
          },
          required: ["email", "person_name"],
          additionalProperties: false,
        },
      },
      organization_emails: {
        type: "array",
        items: {
          type: "object",
          properties: {
            email: { type: "string" },
            label: { type: "string" },
          },
          required: ["email"],
          additionalProperties: false,
        },
      },
    },
    required: ["executives"],
    additionalProperties: false,
  };

  return chargedCall(
    ledger,
    "extract",
    CONTEXT_DEV_CREDIT_COSTS.extract,
    { url, maxPages },
    () =>
      contextDevExtract({
        url,
        schema,
        maxPages,
        maxDepth: 2,
        factCheck: true,
        instructions:
          instructions ||
          "Prioritize leadership, gobierno corporativo, consejo, team, and contact pages. Return named people with titles. Only include attributed_email / named_person_emails when the address is explicitly published for that person. Do not invent emails. Do not treat contacto@ or info@ as a person email.",
      })
  );
}

async function searchWithFallback(ledger, { query, numResults = 10, country }, liveSearchOk) {
  if (liveSearchOk) {
    const cost = ledger.estimateSearchCost(numResults);
    const result = await chargedCall(ledger, "search", cost, { query }, () =>
      contextDevSearch({ query, numResults, country })
    );
    if (result.ok) {
      return {
        ok: true,
        provider: "context_dev_search",
        results: (result.data?.results || []).map((r) => ({
          url: r.url || r.link,
          title: r.title,
          snippet: r.description || r.snippet,
        })),
        credit_entry: result.credit_entry,
      };
    }
    // Do not silently return empty — fall through with error noted
    return {
      ok: false,
      provider: "context_dev_search",
      error: result.error,
      results: [],
      fallback_attempted: true,
    };
  }

  // Existing SerpAPI adapter — track separately (not Context.dev credits)
  const cost = { serpapi_searches: 0, serpapi_usd: 0 };
  try {
    const serp = await serpGoogle(query, cost, {
      hl: country === "mx" ? "es" : "en",
      gl: country || "us",
      num: Math.min(numResults, 10),
    });
    return {
      ok: true,
      provider: "serpapi_fallback",
      results: (serp.organic || []).map((r) => ({
        url: r.link || r.url,
        title: r.title,
        snippet: r.snippet,
      })),
      serpapi_cost: cost,
      context_dev_search_unavailable: true,
    };
  } catch (err) {
    return {
      ok: false,
      provider: "serpapi_fallback",
      error: { message: String(err?.message || err).slice(0, 240) },
      results: [],
    };
  }
}

async function scrapeMarkdownCharged(ledger, url) {
  return chargedCall(ledger, "scrape_markdown", CONTEXT_DEV_CREDIT_COSTS.scrape_markdown, { url }, () =>
    contextDevScrapeMarkdown({ url, useMainContentOnly: true })
  );
}

function pickPeople(seedPeople, extractedPeople, { preferFunctional = true, limit = 3 } = {}) {
  const merged = [];
  const push = (p, source) => {
    const name = String(p.full_name || p.display_name || p.name || "")
      .replace(/^(?:Lic\.|Ing\.|C\.P\.|CPA)\s+/i, "")
      .trim();
    const title = p.title || "";
    if (!name || name.split(/\s+/).length < 2) return;
    const key = name
      .toLowerCase()
      .normalize("NFKD")
      .replace(/[\u0300-\u036f]/g, "");
    if (merged.some((x) => x.key === key)) return;
    const leadership_category =
      p.leadership_category || classifyLeadershipCategory(title) || LEADERSHIP_CATEGORY.UNKNOWN;
    const functionally_relevant =
      p.functionally_relevant === true ||
      leadership_category === LEADERSHIP_CATEGORY.OPERATING_EXECUTIVE ||
      leadership_category === LEADERSHIP_CATEGORY.DEVELOPMENT_ASSET ||
      /ceo|founder|chief development|asset|investment|director general|managing/i.test(title);
    merged.push({
      key,
      display_name: name,
      title,
      leadership_category,
      functionally_relevant,
      source_page_url: p.source_page_url || p.source_url || null,
      biography_excerpt: p.biography_excerpt || null,
      attributed_email_seed: p.attributed_email ? normalizeEmail(p.attributed_email) : null,
      source,
      why_relevant: p.why_relevant || null,
      commercial_relevance: p.commercial_relevance || null,
    });
  };

  for (const p of extractedPeople || []) push(p, "context_dev_extract");
  for (const p of seedPeople || []) push(p, "contact_intelligence_seed");

  merged.sort((a, b) => {
    if (preferFunctional) {
      if (a.functionally_relevant !== b.functionally_relevant) return a.functionally_relevant ? -1 : 1;
    }
    return 0;
  });
  return merged.slice(0, limit);
}

/**
 * Research one person for attributed business email.
 */
export async function researchPersonBusinessEmail(ledger, person, org, { liveSearchOk }) {
  const attempts = [];
  const unresolved = [];
  const host = domainHost(org.domain);
  let attributed = null;
  const observedNamedOnDomain = [];

  if (person.attributed_email_seed && !isOrgGenericEmail(person.attributed_email_seed)) {
    if (isNamedPersonEmail(person.attributed_email_seed, person.display_name)) {
      attributed = {
        email: person.attributed_email_seed,
        attribution: ATTRIBUTION.NAMED_PERSON,
        source_url: person.source_page_url,
        excerpt: person.biography_excerpt || null,
        deliverability: DELIVERABILITY.UNKNOWN,
        deliverability_checked_at: null,
      };
      attempts.push({ kind: "extract_seed_email", ok: true });
    }
  }

  const queries = [
    `"${person.display_name}" "${host}" (email OR correo OR @${host})`,
    `"${person.display_name}" ${org.company_name} email OR correo`,
    `site:${host} "${person.display_name}"`,
  ];

  // Reusable investor/IR document discovery (teacher-learned surface class)
  const lang = org.language || (org.country === "mx" || org.country === "br" ? (org.country === "br" ? "pt" : "es") : "en");
  for (const iq of buildInvestorSearchQueries({
    domain: host,
    orgName: org.company_name,
    language: lang,
    maxQueries: 2,
  })) {
    queries.push(iq);
  }

  const urlsToScrape = [];
  if (person.source_page_url) urlsToScrape.push(person.source_page_url);

  for (const query of queries) {
    if (attributed) break;
    if (!ledger.canAfford(ledger.estimateSearchCost(10)) && liveSearchOk) {
      unresolved.push("CONTEXT_DEV_CREDIT_BUDGET_EXHAUSTED_FOR_SEARCH");
      break;
    }
    const search = await searchWithFallback(
      ledger,
      { query, numResults: 10, country: org.country || "us" },
      liveSearchOk
    );
    attempts.push({
      kind: "person_email_search",
      provider: search.provider,
      query,
      ok: search.ok,
      result_count: search.results?.length || 0,
      error: search.error || null,
    });
    if (!search.ok) {
      unresolved.push(`SEARCH_FAILED:${search.provider}`);
      if (search.provider === "context_dev_search") {
        // Try SerpAPI once if Context search failed mid-run
        const fb = await searchWithFallback(ledger, { query, numResults: 10, country: org.country }, false);
        attempts.push({
          kind: "person_email_search_fallback",
          provider: fb.provider,
          query,
          ok: fb.ok,
          result_count: fb.results?.length || 0,
        });
        for (const r of fb.results || []) {
          if (r.url && !urlsToScrape.includes(r.url)) urlsToScrape.push(r.url);
        }
      }
      continue;
    }
    for (const r of search.results || []) {
      const snip = `${r.title || ""} ${r.snippet || ""}`;
      for (const em of snip.match(EMAIL_RE) || []) {
        const email = normalizeEmail(em);
        if (!email.endsWith(`@${host}`)) continue;
        if (isOrgGenericEmail(email)) continue;
        observedNamedOnDomain.push(email);
        if (isEmailAggregatorUrl(r.url)) {
          attempts.push({
            kind: "aggregator_email_observed_not_attributed",
            url: r.url,
            email,
            note: "Third-party directory claim — not first-party attribution",
          });
          continue;
        }
        if (
          !attributed &&
          isNamedPersonEmail(email, person.display_name) &&
          personMentionsEmail(snip, person.display_name, email)
        ) {
          attributed = {
            email,
            attribution: ATTRIBUTION.NAMED_PERSON,
            source_url: r.url,
            excerpt: snip.slice(0, 280),
            deliverability: DELIVERABILITY.UNKNOWN,
            deliverability_checked_at: null,
          };
        }
      }
      if (r.url && /linkedin\.com|facebook\.com|twitter\.com|instagram\.com/i.test(r.url)) continue;
      if (r.url && isEmailAggregatorUrl(r.url)) continue;
      if (r.url && !urlsToScrape.includes(r.url) && urlsToScrape.length < 8) {
        // Prefer first-party or PDF/announcement
        if (domainHost(r.url) === host || /\.pdf($|\?)/i.test(r.url) || /press|news|bio|team|about|gobierno|contact|investor|inversionista|webcast/i.test(r.url)) {
          urlsToScrape.push(r.url);
        }
      }
    }
    // Promote ranked investor documents to the front of scrape queue
    for (const doc of rankInvestorDocumentUrls(search.results || [], { limit: 3 })) {
      if (!urlsToScrape.includes(doc.url)) urlsToScrape.unshift(doc.url);
      attempts.push({
        kind: "investor_doc_candidate",
        url: doc.url,
        score: doc.score,
        reasons: doc.reasons,
      });
    }
  }

  let scrapeAttempts = 0;
  for (const url of urlsToScrape) {
    if (attributed) break;
    if (scrapeAttempts >= 4) break;
    if (!ledger.canAfford(CONTEXT_DEV_CREDIT_COSTS.scrape_markdown)) {
      unresolved.push("CONTEXT_DEV_CREDIT_BUDGET_EXHAUSTED_FOR_SCRAPE");
      break;
    }
    scrapeAttempts += 1;
    const scraped = await scrapeMarkdownCharged(ledger, url);
    attempts.push({
      kind: "scrape_markdown",
      url,
      ok: scraped.ok,
      error: scraped.error || null,
    });
    if (!scraped.ok) continue;
    const md = scraped.data?.markdown || scraped.data?.content || "";
    const proximityHits = extractAttributedPersonEmailsFromText(md, {
      people: [{ full_name: person.display_name }],
      maxPerPerson: 2,
    });
    for (const hit of proximityHits) {
      const email = normalizeEmail(hit.email);
      if (!email.endsWith(`@${host}`)) continue;
      if (isOrgGenericEmail(email)) continue;
      observedNamedOnDomain.push(email);
      if (!attributed) {
        attributed = {
          email,
          attribution: ATTRIBUTION.NAMED_PERSON,
          source_url: url,
          excerpt: hit.excerpt,
          deliverability: DELIVERABILITY.UNKNOWN,
          deliverability_checked_at: null,
          extraction_method: "investor_doc_name_proximity",
        };
        attempts.push({
          kind: "investor_doc_attributed_email",
          url,
          email,
          hit_tokens: hit.hit_tokens,
        });
        break;
      }
    }
    if (attributed) break;
    for (const em of md.match(EMAIL_RE) || []) {
      const email = normalizeEmail(em);
      if (!email.endsWith(`@${host}`)) continue;
      if (isOrgGenericEmail(email)) continue;
      observedNamedOnDomain.push(email);
      if (
        isNamedPersonEmail(email, person.display_name) &&
        personMentionsEmail(md, person.display_name, email)
      ) {
        attributed = {
          email,
          attribution: ATTRIBUTION.NAMED_PERSON,
          source_url: url,
          excerpt: excerptAround(md, email),
          deliverability: DELIVERABILITY.UNKNOWN,
          deliverability_checked_at: null,
        };
        break;
      }
    }
  }

  const verification = getEmailVerificationStatus();
  let inferred = attributed
    ? []
    : generateInferredEmailCandidates(person.display_name, host, observedNamedOnDomain);

  // Preserve aggregator-observed same-domain addresses as admin-only inferred claims (not attributed)
  const aggregatorClaims = (attempts || [])
    .filter((a) => a.kind === "aggregator_email_observed_not_attributed" && a.email)
    .map((a) => ({
      email: a.email,
      attribution: ATTRIBUTION.INFERRED,
      pattern: "third_party_directory_claim",
      basis: "aggregator_directory_not_first_party",
      source_url: a.url,
      usage_rights: USAGE_RIGHTS.INTERNAL_ONLY,
      deliverability: DELIVERABILITY.UNKNOWN,
      verification: "VERIFICATION_NOT_CONFIGURED",
    }));
  if (!attributed && aggregatorClaims.length) {
    const seen = new Set(inferred.map((x) => x.email));
    for (const c of aggregatorClaims) {
      if (seen.has(c.email)) continue;
      inferred.unshift(c);
      seen.add(c.email);
    }
    inferred = inferred.slice(0, 3);
  }

  if (!attributed) {
    if (!unresolved.includes("NO_ATTRIBUTED_PERSON_EMAIL_FOUND")) {
      unresolved.push("NO_ATTRIBUTED_PERSON_EMAIL_FOUND");
    }
    unresolved.push(verification.status);
  }

  return {
    person,
    attributed_email: attributed,
    inferred_email_candidates: inferred,
    observed_same_domain_named_emails: [...new Set(observedNamedOnDomain)],
    attempts,
    unresolved_reasons: unresolved,
    email_verification: verification,
  };
}

/**
 * Build Contact Intelligence person records from research rows.
 */
export function toPersonContacts(researchRows, org) {
  return researchRows.map((row) => {
    const p = row.person;
    const channels = [];
    if (row.attributed_email) {
      channels.push(
        createChannel({
          kind: CHANNEL_KIND.PERSON_EMAIL,
          value: row.attributed_email.email,
          display_label: "Attributed business email",
          attribution: ATTRIBUTION.NAMED_PERSON,
          deliverability: row.attributed_email.deliverability || DELIVERABILITY.UNKNOWN,
          observed_at: row.attributed_email.deliverability_checked_at,
          property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
          usage_rights: USAGE_RIGHTS.PUBLIC_SAFE,
          evidence: [
            createEvidenceRef({
              source_title: "Person-attributed business email",
              source_url: row.attributed_email.source_url,
              source_type: "first_party_or_public",
              observed_at: new Date().toISOString(),
              excerpt: row.attributed_email.excerpt,
            }),
          ],
        })
      );
    }

    return createPersonContact({
      display_name: p.display_name,
      title: p.title,
      organization_entity_id: org.owner_entity_id,
      organization_name: org.company_name,
      role_observed_at: null,
      role_currency: ROLE_CURRENCY.UNKNOWN,
      property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
      channels,
      evidence: [
        createEvidenceRef({
          source_title: "Person discovery / role corroboration",
          source_url: p.source_page_url,
          source_type: "first_party_or_corp",
          observed_at: new Date().toISOString(),
          excerpt: p.biography_excerpt || `${p.display_name} — ${p.title}`,
        }),
      ],
      unresolved_reasons: row.attributed_email
        ? []
        : [UNRESOLVED_REASON.NO_PERSON_EVIDENCE, ...(row.unresolved_reasons || [])],
      why_relevant:
        p.why_relevant ||
        `${p.title} at ${org.company_name} — ${
          p.functionally_relevant ? "functionally relevant executive/development candidate" : "ownership/board leadership"
        }`,
      publication_label: "EVIDENCED_ORG_PERSON_CANDIDATE",
      customer_caveat: p.functionally_relevant
        ? null
        : "Board/ownership leadership — not confirmed as brand-conversion decision maker.",
      provenance: {
        evidenced_or_inferred: "EVIDENCED",
        identity_affiliation: "EVIDENCED",
        leadership_category: p.leadership_category,
        functionally_relevant: p.functionally_relevant,
        commercial_relevance: p.functionally_relevant
          ? "FUNCTIONALLY_RELEVANT_CANDIDATE"
          : "OWNERSHIP_LEADERSHIP_NOT_CONFIRMED_DM",
        person_email_research: PERSON_EMAIL_DISCOVERY_VERSION,
        inferred_email_candidates: row.inferred_email_candidates || [],
        research_attempts: row.attempts || [],
        unresolved_email_reasons: row.unresolved_reasons || [],
        email_verification: row.email_verification || null,
      },
    });
  });
}

export {
  pickPeople,
  searchWithFallback,
  domainHost,
  isOrgGenericEmail,
  chargedCall,
};
