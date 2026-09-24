/**
 * Native WHO V8 — V7 discovery + entity-type person gate + deterministic event ranking.
 * Preserves V6 precision gates. Same code path for both cohorts. Blind-safe.
 *
 * Cascade: A known event → B org search → C deep-link SERP → D aliases
 * → E PDFs → F operators/housing → G functional + researchCompleteness
 */

import { classifyEventFamily } from "../ontology.js";
import { WHO_RESEARCH_STATE } from "../who-gap.js";
import { serpapiSearch } from "../../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../../hotel-intelligence/room-count-research/fetch.js";
import {
  classifyDomain,
  hostnameOf,
  isOfficialClass,
  isSocialOrAggregator,
} from "./official-domain.js";
import { crawlOfficialStaffPages } from "./staff-directory-crawler.js";
import {
  collectPdfCandidates,
  extractFromPdfUrls,
} from "./pdf-contact-extraction.js";
import { extractContactsFromHtmlV5 } from "./html-contact-extract.js";
import {
  isStrictPersonName,
  expandMultiPersonWithSharedRole,
  splitSequentialNameRoleString,
} from "./person-boundary.js";
import {
  classifySectionFromUrl,
  sectionIsReject,
  sectionIsIdentityOnly,
} from "./section-semantics.js";
import {
  suppressCandidate,
  capConfirmedWho,
  preferredRoleFamilies,
  SUPPRESSION_CODE,
} from "./candidate-suppression.js";
import {
  inferRoleEvidenceTier,
  applyRejectionGates,
  confirmWhoFromEvidence,
  rankWhoCandidates,
  buildConfidenceComponents,
  buildEvidenceMatrix,
  ROLE_EVIDENCE_TIER,
} from "./role-evidence-gates.js";
import { resolveOfficialDomainsV7 } from "./domain-resolver-v7.js";
import {
  expandEntityAliasesV7,
  buildDiscoveryQueriesV7,
  eventCoreName,
} from "./entity-alias-expansion-v7.js";
import {
  classifySearchResultV7,
  rankUrlsForFetchV7,
  RESULT_TIER,
} from "./search-result-classifier-v7.js";
import {
  detectDynamicPageV7,
  extractStructuredPeopleV7,
  extractPeopleFromMarkdownV7,
} from "./structured-data-v7.js";
import { contextDevScrapeMarkdown } from "../../../context-dev/index.js";
import { personTypeGateV8, ENTITY_TYPE } from "./entity-type-gate-v8.js";

const EXTRACT_MODEL =
  process.env.GDI_NATIVE_WHO_MODEL || process.env.OPENAI_MODEL || "gpt-4o-mini";

const SYSTEM_WHO_V8 = `You extract WHO candidates for hotel group sales from official page SECTIONS.
Return JSON:
{
  "candidates": [
    {
      "name": "First Last",
      "role": "title",
      "organization": "org",
      "email": null,
      "phone": null,
      "gdiContactRole": "EVENT_OWNER|MEETINGS_OWNER|CONFERENCE_DIRECTOR|HOUSING_OWNER|REGISTRATION_OWNER|PARTNERSHIPS_OWNER|PROGRAM_OWNER|SPONSORSHIP_OWNER|UNKNOWN",
      "relevance": "PRIMARY_DECISION_MAKER|STRONG_INFLUENCER|OPERATIONAL_CONTACT|BACKUP_CONTACT",
      "sectionHint": "CONTACT|STAFF|TOURNAMENT_LEADERSHIP|CONFERENCE_STAFF|HOUSING|EVENT_TEAM|OTHER",
      "evidenceQuote": "quote proving event/meetings function",
      "sourceUrl": "url"
    }
  ],
  "functionalContacts": [{ "email": null, "phone": null, "role": null, "sourceUrl": null }],
  "multiPersonBlocks": [{ "raw": "Jane / John", "role": "Tournament Directors", "sourceUrl": null }],
  "distributedLeadership": false
}
STRICT: reject speakers/sponsors/board-only/press-only/volunteers/venue hotel staff.
Split multi-person leadership. Prefer staff/contact/event pages. Empty > garbage.`;

async function serp(query, num = 5) {
  const result = await serpapiSearch(
    { engine: "google", q: query, num, hl: "en", gl: "us" },
    { timeoutMs: 25000 }
  );
  if (!result.ok) return [];
  return (result.data?.organic_results || [])
    .map((r) => ({
      title: r.title || null,
      url: r.link || r.url || null,
      snippet: r.snippet || null,
      query,
    }))
    .filter((r) => r.url && !isSocialOrAggregator(r.url));
}

async function openaiExtract(system, user) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 40000);
  try {
    const res = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
        "Content-Type": "application/json",
      },
      signal: controller.signal,
      body: JSON.stringify({
        model: EXTRACT_MODEL,
        temperature: 0.05,
        max_tokens: 2200,
        response_format: { type: "json_object" },
        messages: [
          { role: "system", content: system },
          { role: "user", content: user },
        ],
      }),
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `openai_${res.status}`);
    return JSON.parse(json.choices?.[0]?.message?.content || "{}");
  } finally {
    clearTimeout(timer);
  }
}

async function fetchPage(url, pages, seen, input, metrics, limit) {
  if (pages.length >= limit || !url || seen.has(url)) return;
  if (isSocialOrAggregator(url)) return;
  if (sectionIsReject(classifySectionFromUrl(url)) && !/contact|staff|team/i.test(url)) {
    return;
  }
  if (/\.pdf($|\?)/i.test(url)) return;
  const fetched = await fetchResearchPage(url, { timeoutMs: 16000 });
  metrics.pagesFetched += 1;
  if (!fetched.ok || !fetched.text) return;
  seen.add(url);
  const text = htmlToSearchableText(fetched.text).replace(/\s+/g, " ").trim().slice(0, 9000);
  const dyn = detectDynamicPageV7(fetched.text, text);
  pages.push({
    url: fetched.url || url,
    text,
    html: fetched.text.slice(0, 250_000),
    kind: "v8_fetch",
    domainClass: classifyDomain(hostnameOf(url), input),
    dynamic: dyn,
  });
  if (dyn.likelyDynamic) metrics.dynamicPages += 1;
}

function cleanRole(role) {
  let r = String(role || "")
    .replace(/\[email\s*protected\]/gi, "")
    .replace(/\s+/g, " ")
    .trim();
  if (!r) return null;
  if (/^tour(nament)?$/i.test(r)) r = "Tournament Director";
  return r.slice(0, 120);
}

function mapRole(role) {
  const r = String(role || "");
  if (/housing/i.test(r)) return "HOUSING_OWNER";
  if (/registration|registrar/i.test(r)) return "REGISTRATION_OWNER";
  if (/partnership|business development|sponsorship/i.test(r)) return "PARTNERSHIPS_OWNER";
  if (/member services|meetings/i.test(r)) return "MEETINGS_OWNER";
  if (/tournament|cups? director|conference|audience engagement/i.test(r)) {
    return "CONFERENCE_DIRECTOR";
  }
  if (/executive director|vice president|co-?vp/i.test(r)) return "EVENT_OWNER";
  return "UNKNOWN";
}

function eventSpecificityOk(cand, input, aliases) {
  const quote = `${cand.evidenceQuote || ""} ${cand.sourceUrl || ""} ${cand.role || ""}`.toLowerCase();
  const core = String(aliases?.eventCore || eventCoreName(input.opportunityName || "")).toLowerCase();
  const coreTokens = core
    .split(/\s+/)
    .filter((t) => t.length >= 4 && !/^(spring|fall|winter|summer|annual|championships?|conference|expo)$/i.test(t));

  // Competing named events on list pages — require target event tokens nearby
  const competing =
    /\b(invitational|college\s*showcase|presidents?\s*cup|memorial\s*tournament|boys\s*showcase|girls\s*showcase|academy)\b/i.test(
      quote
    );
  const hasCore = coreTokens.some((t) => quote.includes(t));
  if (competing && !hasCore) return false;

  // State Cup opportunities: generic "Tournament Director" on sanctioned-list pages is not enough
  if (/state\s*cup/i.test(`${input.opportunityName || ""} ${core}`)) {
    if (/sanctioned-tournaments|invitational|showcase|presidents/i.test(quote) && !/state\s*cup|cups?\s*director/i.test(quote)) {
      return false;
    }
  }
  return true;
}

function scoreAndConfirm(raw, input, eventFamily, metrics, suppressionLog, aliases = {}) {
  const scored = [];
  for (const p of raw) {
    const sourceUrl = p.sourceUrl || null;
    const host = hostnameOf(sourceUrl);
    const domainClass = classifyDomain(host, input);
    const onOfficialDomain = isOfficialClass(domainClass) || Boolean(p.onOfficialDomain);
    const sectionKind = p.sectionKind || p.sectionHint || classifySectionFromUrl(sourceUrl);

    const cand = {
      name: p.name,
      role: cleanRole(p.role),
      organization: p.organization || input.organization,
      email:
        p.email && !/^(info|events|contact|hello|office|meetings|housing|registrar)@/i.test(p.email)
          ? p.email
          : null,
      phone: p.phone || null,
      gdiContactRole: p.gdiContactRole || mapRole(p.role),
      roleRelevance: p.relevance || "OPERATIONAL_CONTACT",
      currentness: p.currentness || "LIKELY_CURRENT",
      sourceUrl,
      evidenceQuote: p.evidenceQuote || null,
      onOfficialDomain,
      eventSpecificEvidence: Boolean(
        p.fromMultiPersonBlock ||
          p.fromPdf ||
          p.fromStructuredData ||
          p.fromRenderedMarkdown ||
          /TOURNAMENT|CONFERENCE|CONTACT|EVENT|HOUSING|MEETINGS|STAFF|ORGANIZERS|REGISTRATION/i.test(
            sectionKind
          )
      ),
      domainClass,
      sectionKind,
      fromPdf: Boolean(p.fromPdf),
      fromHtml: Boolean(p.fromHtml || p.fromStructuredData || p.fromRenderedMarkdown),
      fromMultiPersonBlock: Boolean(p.fromMultiPersonBlock),
      fromRenderedMarkdown: Boolean(p.fromRenderedMarkdown),
      forConfirmation: true,
    };


    const typeGate = personTypeGateV8(cand);
    if (typeGate.reject) {
      metrics.suppressed += 1;
      suppressionLog.push({
        name: cand.name,
        code: typeGate.gate,
        entityType: typeGate.entityType,
        sourceUrl,
      });
      continue;
    }
    cand.entityType = ENTITY_TYPE.PERSON;

    if (!eventSpecificityOk(cand, input, aliases)) {
      metrics.suppressed += 1;
      suppressionLog.push({
        name: cand.name,
        code: "EVENT_SPECIFICITY_MISS",
        sourceUrl,
      });
      continue;
    }

    const sup = suppressCandidate(cand, {
      organization: input.organization,
      opportunityName: input.opportunityName,
    });
    if (sup.suppress) {
      metrics.suppressed += 1;
      suppressionLog.push({ name: cand.name, code: sup.code, sourceUrl });
      continue;
    }

    cand.evidenceMatrix = buildEvidenceMatrix(cand, { eventFamily });
    cand.evidenceTier = inferRoleEvidenceTier(cand, { eventFamily });
    Object.assign(cand, buildConfidenceComponents(cand));

    const gateReject = applyRejectionGates(cand, {
      organization: input.organization,
      opportunityName: input.opportunityName,
      eventFamily,
    });
    if (gateReject.reject) {
      metrics.suppressed += 1;
      suppressionLog.push({ name: cand.name, code: gateReject.gate, sourceUrl });
      continue;
    }

    if (!isStrictPersonName(cand.name)) {
      metrics.suppressed += 1;
      suppressionLog.push({
        name: cand.name,
        code: SUPPRESSION_CODE.PERSON_BOUNDARY_ERROR,
        sourceUrl,
      });
      continue;
    }

    if (
      cand.role &&
      (/!{1,}|remember to|don't be shy|click here/i.test(cand.role) || String(cand.role).length > 100)
    ) {
      metrics.suppressed += 1;
      continue;
    }

    // Garbage multi-person role bleed ("Tournament Director, Parnell Hegngi Lead Assignor…")
    if (cand.role && /,\s*[A-Z][a-z]+\s+[A-Z][a-z]+/.test(cand.role) && cand.role.length > 40) {
      cand.role = cleanRole(cand.role.split(",")[0]);
    }

    const conf = confirmWhoFromEvidence(cand);
    if (conf.researchState === "REJECTED") {
      metrics.suppressed += 1;
      continue;
    }

    let researchState = conf.researchState;
    if (cand.evidenceTier === ROLE_EVIDENCE_TIER.C_STRONG_INFERENCE) {
      researchState = WHO_RESEARCH_STATE.NAMED_PERSON_NEEDS_VALIDATION;
    }
    if (
      researchState === WHO_RESEARCH_STATE.NAMED_PERSON_CONFIRMED &&
      !/TOURNAMENT|CONFERENCE|CONTACT|EVENT|HOUSING|MEETINGS|STAFF|ORGANIZERS|REGISTRATION|LEADERSHIP/i.test(
        sectionKind
      )
    ) {
      researchState = WHO_RESEARCH_STATE.NAMED_PERSON_NEEDS_VALIDATION;
    }

    scored.push({
      ...cand,
      researchState,
      gateReason: conf.reason,
      gateOk: researchState === WHO_RESEARCH_STATE.NAMED_PERSON_CONFIRMED,
      candidateState: "CANDIDATE_FOUND",
    });
  }
  scored.sort(rankWhoCandidates);

  const oppBlob = `${input.opportunityName || ""} ${input.organization || ""}`.toLowerCase();
  scored.sort((a, b) => {
    const score = (p) => {
      let s = 0;
      const role = `${p.role || ""}`.toLowerCase();
      const quote = `${p.evidenceQuote || ""} ${p.sourceUrl || ""}`.toLowerCase();
      if (/tournament director|cups? director|conference director|director of nice|program manager|housing|registration|member services|tournament chairman/i.test(role)) s += 8;
      if (/vice president|co-?vp/i.test(role) && /summit|forum|hits|health it|event|conference|tournament/i.test(role + " " + quote + " " + oppBlob)) s += 10;
      if (/chairman|co-?director|doubles coordinator/i.test(role)) s += 7;
      if (/^(president|treasurer|secretary|president-elect)\b/i.test(role.trim()) && !/summit|event|tournament|cups|conference|meetings/i.test(role)) s -= 6;
      if (/special events/i.test(role) && /health it|hits|nice|gad|show/i.test(oppBlob) && !/health it|hits/i.test(role)) s -= 3;
      if (/board-of-directors|\/board\//i.test(quote) && !/summit|tournament|cups|conference director|health it/i.test(role)) s -= 2;
      if (/education|forum|engagement|summit|cups|tournament|institute|gad|nice|show/i.test(oppBlob)) {
        if (/education|engagement|partnership|events|cups|tournament|summit|institute|director|vp|vice president|program/i.test(role)) s += 4;
      }
      const core = String(aliases?.eventCore || "").toLowerCase();
      for (const t of core.split(/\s+/).filter((x) => x.length >= 4)) {
        if (quote.includes(t) || role.includes(t)) s += 2;
      }
      if (p.email) s += 0.5;
      if (p.fromStructuredData || p.fromRenderedMarkdown) s += 1;
      if (p.fromMultiPersonBlock) s += 1;
      return s;
    };
    const d = score(b) - score(a);
    if (d !== 0) return d;
    return String(a.name || "").localeCompare(String(b.name || "")) || String(a.sourceUrl || "").localeCompare(String(b.sourceUrl || ""));
  });

  return scored;
}

function dedupeFunctional(list) {
  const seen = new Set();
  const out = [];
  for (const f of list) {
    const key = `${String(f.email || "").toLowerCase()}|${f.phone || ""}`;
    if (!f.email && !f.phone) continue;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(f);
  }
  return out.slice(0, 6);
}

function uniquePeople(pool, max = 3) {
  const seen = new Set();
  const out = [];
  for (const p of pool) {
    const key = String(p.name || "")
      .toLowerCase()
      .replace(/[^a-z\s]/g, "")
      .replace(/\s+/g, " ")
      .trim();
    if (!key || seen.has(key) || !isStrictPersonName(p.name)) continue;
    seen.add(key);
    out.push(p);
    if (out.length >= max) break;
  }
  return out;
}

/**
 * Blind Native WHO V7 for one opportunity.
 */
export async function discoverWhoV8(input) {
  const started = Date.now();
  const metrics = {
    queries: 0,
    pagesFetched: 0,
    pdfsInspected: 0,
    suppressed: 0,
    dynamicPages: 0,
    structuredPeople: 0,
    highValueHits: 0,
    stagesRun: [],
    stopReason: null,
  };
  const suppressionLog = [];
  const stageLog = [];
  const researchCompleteness = {
    officialOrgDomainChecked: false,
    eventDomainChecked: false,
    roleSpecificSearchChecked: false,
    deepLinkSearchChecked: false,
    documentSearchChecked: false,
    structuredDataChecked: false,
  };

  const eventFamily = classifyEventFamily({
    title: input.opportunityName,
    organizationName: input.organization,
    opportunityType: input.eventType,
    segment: input.segment,
  });
  const rolePrefs = preferredRoleFamilies(eventFamily);

  // ——— STAGE A/B: domain resolver ———
  metrics.stagesRun.push("A_B_DOMAIN");
  stageLog.push({ stage: "A_B", name: "DOMAIN_RESOLVER" });
  const resolved = await resolveOfficialDomainsV7(input);
  metrics.queries += resolved.queries.length;
  const aliases = resolved.aliases || expandEntityAliasesV7(input);
  const officialDomains = resolved.domains || [];
  researchCompleteness.officialOrgDomainChecked = officialDomains.some(
    (d) => d.role === "PRIMARY_ORG_DOMAIN" || d.class
  );
  researchCompleteness.eventDomainChecked = officialDomains.some((d) =>
    /EVENT/i.test(d.role || "")
  );

  const serpHits = [...(resolved.evidence || [])];
  const pages = [];
  const seen = new Set();

  // Seed eventSourceUrls (pre-WHO metadata only)
  for (const u of input.eventSourceUrls || []) {
    await fetchPage(u, pages, seen, input, metrics, 6);
  }

  // ——— STAGE C: search-first deep links ———
  metrics.stagesRun.push("C_DEEP_LINK");
  stageLog.push({ stage: "C", name: "DEEP_LINK_SEARCH" });
  const discoveryQs = buildDiscoveryQueriesV7(input, aliases, officialDomains);
  // Prefer site: queries (already ordered first); run up to 12
  for (const q of discoveryQs.slice(0, 12)) {
    metrics.queries += 1;
    const hits = await serp(q, 5);
    for (const h of hits) {
      const c = classifySearchResultV7(h, input, aliases);
      if (c.tier === RESULT_TIER.IRRELEVANT) continue;
      if (c.tier === RESULT_TIER.HIGH_VALUE_WHO_SOURCE) metrics.highValueHits += 1;
      serpHits.push({ ...h, tier: c.tier });
    }
  }
  researchCompleteness.deepLinkSearchChecked = true;
  researchCompleteness.roleSpecificSearchChecked = true;

  const officialHosts = new Set(officialDomains.map((d) => d.host));
  const ranked = rankUrlsForFetchV7(serpHits, input, aliases).sort((a, b) => {
    const aOff = officialHosts.has(hostnameOf(a.url)) ? 1 : 0;
    const bOff = officialHosts.has(hostnameOf(b.url)) ? 1 : 0;
    return bOff - aOff || (b.score || 0) - (a.score || 0);
  });
  for (const h of ranked.slice(0, 14)) {
    await fetchPage(h.url, pages, seen, input, metrics, 18);
  }

  // Speculative priority paths — only on evidence-backed official domains
  metrics.stagesRun.push("C2_PATH_PROBES");
  const pathProbes = [];
  for (const d of officialDomains.slice(0, 2)) {
    let base;
    try {
      base = d.seedUrl ? new URL(d.seedUrl).origin : `https://www.${d.host}`;
    } catch {
      base = `https://${d.host}`;
    }
    for (const path of [
      "/staff",
      "/event-staff",
      "/en/event-staff",
      "/about/staff",
      "/about-us/staff",
      "/about/meet-staff",
      "/about/meet-the-staff",
      "/meet-staff",
      "/meet-the-staff",
      "/team",
      "/our-team",
      "/leadership",
      "/board-of-directors",
      "/board",
      "/officers",
      "/contact",
      "/contact-us",
      "/about/staff/",
      "/events",
      "/conferences",
      "/meetings",
      "/programs",
      "/education",
      "/housing",
      "/registration",
      "/staff-directory",
      "/who-we-are",
    ]) {
      pathProbes.push(`${base}${path}`);
    }
  }
  for (const u of pathProbes.slice(0, 20)) {
    await fetchPage(u, pages, seen, input, metrics, 24);
  }

  // Staff crawl from best seeds
  const crawl = await crawlOfficialStaffPages({
    seeds: [
      ...officialDomains.map((d) => d.seedUrl).filter(Boolean),
      ...ranked.slice(0, 4).map((h) => h.url),
    ].slice(0, 6),
    maxPages: 6,
  });
  metrics.pagesFetched += crawl.pages.length;
  for (const p of crawl.pages) {
    if (!seen.has(p.url)) {
      seen.add(p.url);
      pages.push({
        ...p,
        dynamic: detectDynamicPageV7(p.html || "", p.text || ""),
      });
    }
  }

  // ——— STAGE C3: render recovery for captcha / empty shells (Context.dev) ———
  metrics.stagesRun.push("C3_RENDER_RECOVERY");
  const markdownPeople = [];
  const recoverCandidates = pages
    .filter(
      (p) =>
        p.dynamic?.needsRenderRecovery &&
        /staff|contact|team|leadership|tournament|memorial|directory|about-us|cups|conference|events|board|meet-staff|officers|event-staff/i.test(
          p.url || ""
        )
    )
    .slice(0, 6);
  // Also recover high-value SERP URLs we never successfully fetched (captcha blocked)
  for (const h of ranked.slice(0, 12)) {
    if (recoverCandidates.length >= 6) break;
    if (
      !/staff|contact|team|leadership|tournament|memorial|directory|about|board|meet-staff|officers|cups/i.test(
        h.url || ""
      )
    ) {
      continue;
    }
    if (pages.some((p) => p.url === h.url && (p.text || "").length > 400)) continue;
    if (!recoverCandidates.some((p) => p.url === h.url)) {
      recoverCandidates.push({ url: h.url, dynamic: { needsRenderRecovery: true } });
    }
  }
  for (const target of recoverCandidates) {
    try {
      const scraped = await contextDevScrapeMarkdown({
        url: target.url,
        maxAgeMs: 7 * 24 * 60 * 60 * 1000,
      });
      if (!scraped.ok) continue;
      const md = String(scraped.data?.markdown || "");
      if (md.length < 80) continue;
      metrics.dynamicPages += 1;
      const recoveredUrl = scraped.data?.url || target.url;
      const text = md.replace(/\s+/g, " ").trim().slice(0, 9000);
      // Replace or add page with recovered text for LLM
      const existing = pages.find((p) => p.url === target.url || p.url === recoveredUrl);
      if (existing) {
        existing.text = text;
        existing.html = existing.html || `<pre>${md.slice(0, 50_000)}</pre>`;
        existing.recoveredVia = "context_dev_scrape_markdown";
        existing.dynamic = { ...existing.dynamic, recovered: true };
      } else {
        pages.push({
          url: recoveredUrl,
          text,
          html: `<pre>${md.slice(0, 50_000)}</pre>`,
          kind: "v7_render_recovery",
          domainClass: classifyDomain(hostnameOf(recoveredUrl), input),
          dynamic: { likelyDynamic: true, recovered: true, reasons: ["render_recovery"] },
          recoveredVia: "context_dev_scrape_markdown",
        });
      }
      markdownPeople.push(...extractPeopleFromMarkdownV7(md, recoveredUrl));
    } catch {
      /* non-fatal */
    }
  }

  // ——— STAGE E: PDFs ———
  metrics.stagesRun.push("E_PDF");
  researchCompleteness.documentSearchChecked = true;
  const pdfUrls = collectPdfCandidates(serpHits, crawl.pdfUrls || []);
  const pdfExtract = await extractFromPdfUrls(pdfUrls.slice(0, 4));
  metrics.pdfsInspected = pdfExtract.inspected?.length || 0;

  // Extract HTML + structured
  const htmlPeople = [];
  const htmlFunctional = [];
  const structuredPeople = [];
  for (const p of pages) {
    if (!p.html) continue;
    if (sectionIsIdentityOnly(classifySectionFromUrl(p.url))) continue;
    const extracted = extractContactsFromHtmlV5(p.html, p.url);
    htmlPeople.push(...extracted.people);
    htmlFunctional.push(...extracted.functional);
    const struct = extractStructuredPeopleV7(p.html, p.url);
    structuredPeople.push(...struct);
  }
  structuredPeople.push(...markdownPeople);
  metrics.structuredPeople = structuredPeople.length;
  researchCompleteness.structuredDataChecked = true;

  // Optional LLM only when HTML/structured thin
  let extracted = {
    candidates: [],
    functionalContacts: [],
    multiPersonBlocks: [],
    distributedLeadership: false,
  };
  const strongHtml = [...htmlPeople, ...structuredPeople].filter(
    (p) =>
      isStrictPersonName(p.name) &&
      Boolean(p.role) &&
      (p.email ||
        /director|manager|coordinator|vp|vice president|tournament|cups|program|summit|executive|meetings|housing|registration/i.test(
          p.role || ""
        ))
  );
  // Always LLM when we lack ≥2 role-bearing people — do not let bare-name HTML skip extraction
  if (strongHtml.length < 2) {
    metrics.stagesRun.push("LLM_EXTRACT");
    try {
      const llmPages = pages
        .filter((p) => !sectionIsIdentityOnly(classifySectionFromUrl(p.url)))
        .sort((a, b) => {
          const sa = /staff|contact|team|leadership|cups|tournament/i.test(a.url) ? 0 : 1;
          const sb = /staff|contact|team|leadership|cups|tournament/i.test(b.url) ? 0 : 1;
          return sa - sb;
        })
        .slice(0, 6);
      extracted = await openaiExtract(
        SYSTEM_WHO_V8,
        JSON.stringify(
          {
            hotel: input.hotelName,
            opportunity: aliases.cleanEventTitle || input.opportunityName,
            organization: input.organization,
            eventFamily,
            preferredRoles: rolePrefs,
            pages: llmPages.map((p) => ({
              url: p.url,
              section: classifySectionFromUrl(p.url),
              text: String(p.text || "").slice(0, 4000),
              dynamic: p.dynamic?.likelyDynamic || false,
            })),
            htmlPeople: htmlPeople.slice(0, 20),
            structuredPeople: structuredPeople.slice(0, 12),
            pdfPeople: (pdfExtract.people || []).slice(0, 6),
          },
          null,
          2
        )
      );
    } catch (err) {
      extracted.error = String(err.message || err);
    }
  } else {
    extracted.candidates = strongHtml.slice(0, 10).map((p) => ({
      ...p,
      sectionHint: p.sectionKind || p.sectionHint || "STAFF",
      gdiContactRole: mapRole(p.role),
    }));
    extracted.distributedLeadership =
      strongHtml.filter((p) => /tournament|executive director|co-?vp/i.test(p.role || "")).length >=
      2;
  }

  for (const block of extracted.multiPersonBlocks || []) {
    const expanded = expandMultiPersonWithSharedRole(block.raw, block.role);
    for (const e of expanded) {
      (extracted.candidates ||= []).push({
        name: e.name,
        role: e.role || block.role,
        organization: input.organization,
        sourceUrl: block.sourceUrl || pages[0]?.url,
        sectionHint: "TOURNAMENT_LEADERSHIP",
        evidenceQuote: block.raw,
        fromMultiPersonBlock: true,
        gdiContactRole: "EVENT_OWNER",
        relevance: "PRIMARY_DECISION_MAKER",
      });
    }
  }

  // Stage 5: sequential recovery from page text
  metrics.stagesRun.push("F_MULTI_PERSON");
  for (const p of pages) {
    const text = String(p.text || "");
    const markers =
      /(?:executive staff|tournament (?:directors?|contacts?|leadership)|event staff|cups director|conference (?:staff|director)|please contact|contact:)/gi;
    let m;
    while ((m = markers.exec(text))) {
      const w = text.slice(m.index, m.index + 400);
      const seq = splitSequentialNameRoleString(w);
      if (seq.length >= 2) {
        for (const e of seq) {
          (extracted.candidates ||= []).push({
            name: e.name,
            role: e.role,
            sourceUrl: p.url,
            sectionHint: classifySectionFromUrl(p.url),
            evidenceQuote: w.slice(0, 200),
            fromMultiPersonBlock: true,
            gdiContactRole: "EVENT_OWNER",
            relevance: "PRIMARY_DECISION_MAKER",
          });
        }
      }
    }
  }

  const raw = [];
  for (const p of [
    ...(extracted.candidates || []),
    ...htmlPeople,
    ...structuredPeople,
    ...(pdfExtract.people || []).map((p) => ({
      ...p,
      fromPdf: true,
      sectionHint: classifySectionFromUrl(p.sourceUrl || ""),
    })),
  ]) {
    const expanded = expandMultiPersonWithSharedRole(p.name, p.role);
    if (expanded.length) {
      for (const e of expanded) {
        raw.push({
          ...p,
          name: e.name,
          role: e.role || p.role,
          fromMultiPersonBlock: e.fromMultiPersonBlock || p.fromMultiPersonBlock,
        });
      }
    } else if (isStrictPersonName(p.name)) {
      raw.push(p);
    }
  }

  let scored = scoreAndConfirm(raw, input, eventFamily, metrics, suppressionLog, aliases);

  const leadershipConfirmed = scored.filter(
    (p) =>
      p.gateOk &&
      isStrictPersonName(p.name) &&
      (/tournament|executive director|chairman|doubles|cups? director|co-?vp|vice president|summit|program manager|director of/i.test(
        p.role || ""
      ) ||
        p.sectionKind === "TOURNAMENT_LEADERSHIP" ||
        p.fromMultiPersonBlock)
  );
  const distributed =
    Boolean(extracted.distributedLeadership) || leadershipConfirmed.length >= 2;

  let people = [];
  // Prefer single best confirmed WHO unless true multi-person leadership of the same event
  const sameEventLeaders = leadershipConfirmed.filter((p) =>
    eventSpecificityOk(p, input, aliases)
  );
  if (sameEventLeaders.length >= 2 && (distributed || sameEventLeaders.length >= 2)) {
    people = uniquePeople(sameEventLeaders, 3).map((p, i) => ({
      ...p,
      whoSlot: i === 0 ? "PRIMARY" : "SECONDARY",
    }));
  } else {
    const capped = capConfirmedWho(scored, { distributedResponsibility: false });
    people = uniquePeople(
      capped.confirmed.length ? capped.confirmed : scored.filter((p) => p.gateOk),
      2
    ).map((p, i) => ({
      ...p,
      whoSlot: i === 0 ? "PRIMARY" : "SECONDARY",
    }));
  }

  const functionalContacts = dedupeFunctional([
    ...(extracted.functionalContacts || []),
    ...htmlFunctional,
    ...(pdfExtract.functional || []),
  ]);

  // ——— STAGE G: completeness before NO_WHO ———
  metrics.stagesRun.push("G_COMPLETENESS");
  const completenessScore = Object.values(researchCompleteness).filter(Boolean).length;
  const researchComplete = completenessScore >= 4;

  if (people.length) {
    metrics.stopReason = "ROLE_COVERAGE_MET";
  } else if (functionalContacts.length) {
    metrics.stopReason = "FUNCTIONAL_FALLBACK";
  } else if (!researchComplete) {
    metrics.stopReason = "DISCOVERY_INCOMPLETE";
  } else {
    metrics.stopReason = "NO_WHO_AFTER_COMPLETE_RESEARCH";
  }

  let emptyReason = null;
  if (!people.length) {
    if (!researchComplete) emptyReason = "RESEARCH_INCOMPLETE";
    else if (scored.length) emptyReason = "ROLE_NOT_ESTABLISHED";
    else if (metrics.pagesFetched === 0) emptyReason = "SOURCE_ACCESS_FAILED";
    else emptyReason = "NO_RELEVANT_PERSON_FOUND";
  }

  const primary = people[0] || null;
  let researchState = WHO_RESEARCH_STATE.NO_WHO_RESEARCHED;
  if (primary?.gateOk) researchState = WHO_RESEARCH_STATE.NAMED_PERSON_CONFIRMED;
  else if (scored[0]) researchState = WHO_RESEARCH_STATE.NAMED_PERSON_NEEDS_VALIDATION;
  else if (functionalContacts.length) {
    researchState = WHO_RESEARCH_STATE.FUNCTIONAL_CONTACT_ONLY_RESEARCHED;
  }

  const sourceDiscoveryMetrics = {
    officialDomainsFound: officialDomains.length,
    highValueHits: metrics.highValueHits,
    staffOrContactPages: pages.filter((p) =>
      /staff|contact|team|leadership|directory/i.test(p.url)
    ).length,
    eventContactPages: pages.filter((p) =>
      /event|conference|tournament|summit|cups|institute/i.test(p.url)
    ).length,
    pdfSources: metrics.pdfsInspected,
    dynamicPageRecoveries: metrics.dynamicPages,
    structuredPeople: metrics.structuredPeople,
    thirdPartyOperatorDomains: officialDomains.filter((d) =>
      /HOUSING|OPERATOR|THIRD/i.test(d.role || "")
    ).length,
    renderRecoveries: markdownPeople.length ? recoverCandidates.length : 0,
  };

  return {
    opportunityId: input.opportunityId,
    hotelId: input.hotelId,
    opportunityName: input.opportunityName,
    organization: input.organization,
    eventFamily,
    aliases,
    officialDomains,
    primaryKind: primary
      ? "NAMED_PERSON"
      : functionalContacts.length
        ? "FUNCTIONAL_ENTITY"
        : "UNRESOLVED",
    researchState,
    primary,
    secondary: people[1] || null,
    people,
    candidatesFound: scored.slice(0, 10).map((p) => ({
      name: p.name,
      role: p.role,
      sectionKind: p.sectionKind,
      researchState: p.researchState,
      sourceUrl: p.sourceUrl,
    })),
    functionalContacts,
    suppressionLog: suppressionLog.slice(0, 30),
    stageLog,
    researchCompleteness,
    researchComplete,
    sourceDiscoveryMetrics,
    queries: discoveryQs,
    urlsFetched: pages.map((p) => p.url),
    pdfsInspected: pdfExtract.inspected,
    metrics: { ...metrics, latencyMs: Date.now() - started, completenessScore },
    passVersion: "native_who_blind_v8",
  };
}
