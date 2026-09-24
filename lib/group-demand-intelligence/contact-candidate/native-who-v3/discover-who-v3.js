/**
 * Native WHO V3/V4 orchestrator — staff crawl + PDF/HTML extract + evidence matrix.
 * Blind-safe: caller must not pass expected person names / gold answers.
 *
 * V4 additions:
 * - Evidence matrix (identity/currentness/role/eventRelation/sourceAuthority)
 * - President/press cannot be Tier A from source authority alone
 * - Multi-person leadership completeness before stop
 * - Deterministic HTML mailto/staff-block extract
 * - Stronger official staff/contact query families
 * - Functional backup preservation
 */

import {
  buildCandidateSearchQueries,
  classifyEventFamily,
} from "../ontology.js";
import { WHO_RESEARCH_STATE } from "../who-gap.js";
import { isLikelyPersonName } from "../../contact-resolution.js";
import { serpapiSearch } from "../../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../../hotel-intelligence/room-count-research/fetch.js";
import {
  resolveOfficialDomains,
  classifyDomain,
  hostnameOf,
  isOfficialClass,
  isSocialOrAggregator,
  DOMAIN_CLASS,
} from "./official-domain.js";
import { crawlOfficialStaffPages } from "./staff-directory-crawler.js";
import {
  collectPdfCandidates,
  extractFromPdfUrls,
} from "./pdf-contact-extraction.js";
import { extractContactsFromHtml } from "./html-contact-extract.js";
import {
  inferRoleEvidenceTier,
  applyRejectionGates,
  confirmWhoFromEvidence,
  rankWhoCandidates,
  buildConfidenceComponents,
  buildEvidenceMatrix,
  roleCoverageSatisfied,
  ROLE_EVIDENCE_TIER,
} from "./role-evidence-gates.js";

const EXTRACT_MODEL =
  process.env.GDI_NATIVE_WHO_MODEL || process.env.OPENAI_MODEL || "gpt-4o-mini";

const SYSTEM_WHO = `You extract WHO (named people who control lodging/meetings/event sourcing) from official page text.
Return JSON only:
{
  "people": [
    {
      "name": "First Last",
      "role": "title",
      "organization": "org",
      "email": "only if explicitly published else null",
      "phone": "only if explicitly published else null",
      "emailKind": "PERSONAL_WORK|FUNCTIONAL_INBOX|null",
      "phoneKind": "DIRECT|ORG_MAIN|null",
      "gdiContactRole": "EVENT_OWNER|MEETINGS_OWNER|CONFERENCE_DIRECTOR|HOUSING_OWNER|REGISTRATION_OWNER|PROGRAM_OWNER|SPONSORSHIP_OWNER|PARTNERSHIPS_OWNER|SALES_OWNER|EXECUTIVE_SPONSOR|GENERAL_ORGANIZATION_CONTACT|UNKNOWN",
      "relevance": "PRIMARY_DECISION_MAKER|STRONG_INFLUENCER|OPERATIONAL_CONTACT|BACKUP_CONTACT|WEAK_RELEVANCE",
      "currentness": "CURRENT|LIKELY_CURRENT|HISTORICAL|UNKNOWN",
      "pdfRoleContext": "ORGANIZER|EVENT_STAFF|HOUSING|REGISTRATION|SPONSORSHIP|PROGRAM|SPEAKER|BOARD_MEMBER|VENDOR|UNKNOWN|null",
      "evidenceQuote": "short quote proving relevant FUNCTION (not a press quote)",
      "sourceUrl": "url"
    }
  ],
  "functionalContacts": [
    { "name": "desk or inbox", "email": null, "phone": null, "role": null, "sourceUrl": null }
  ],
  "leadershipSetComplete": false,
  "stopReason": "ROLE_COVERAGE_MET|TIER_A_FOUND|NONE|null"
}
STRICT:
- Only names explicitly on the pages. Never invent emails/phones.
- Split "A / B" into two people.
- When a page lists multiple executive/tournament/conference staff, extract ALL relevant names (not just one).
- Prefer: partnerships/events, meetings/conference directors, tournament directors, housing/registration, member services.
- REJECT: President/CEO-only, press-quote-only, speakers-only, board-only, venue hotel staff, sponsor-company panelists.
- A press release mentioning a President is NOT event-responsibility evidence.
- Functional inboxes (info@, events@, meetings@, housing@) go in functionalContacts — always capture when published.
- Max 5 people when leadership set is multi-person; otherwise max 3.
- Empty people is better than a weak executive guess.`;

function splitPeople(name) {
  const n = String(name || "").trim();
  if (!n) return [];
  if (n.includes("/")) {
    return n
      .split("/")
      .map((s) => s.trim())
      .filter((p) => isLikelyPersonName(p));
  }
  return isLikelyPersonName(n) ? [n] : [];
}

async function serp(query, num = 5) {
  const result = await serpapiSearch({
    engine: "google",
    q: query,
    num,
    hl: "en",
    gl: "us",
  });
  if (!result.ok) return { organic: [], error: result.error };
  return {
    organic: (result.data?.organic_results || []).map((r) => ({
      title: r.title || null,
      url: r.link || r.url || null,
      snippet: r.snippet || null,
    })),
  };
}

async function openaiExtract(system, user) {
  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: EXTRACT_MODEL,
      temperature: 0.1,
      max_tokens: 2600,
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
}

/**
 * Structured query families (event / org site: / document / staff).
 */
export function buildV3Queries(input, officialDomains = []) {
  const event = input.opportunityName || "";
  const org = input.organization || event;
  const queries = [];

  queries.push(
    `"${event}" contact`,
    `"${event}" organizer`,
    `"${event}" "conference director"`,
    `"${event}" housing`,
    `"${org}" ("staff directory" OR "our team" OR "contact us" OR "member services" OR "partnerships")`,
    `"${org}" ("meetings" OR "events" OR "business development") (director OR manager OR VP)`
  );

  for (const d of officialDomains.slice(0, 2)) {
    const host = d.host;
    queries.push(
      `site:${host} (staff OR team OR leadership OR "contact us")`,
      `site:${host} (meetings OR conferences OR events OR housing)`,
      `site:${host} ("business development" OR "member services" OR partnerships)`,
      `site:${host} filetype:pdf (contact OR prospectus OR housing OR conference)`
    );
  }

  if (!officialDomains.length) {
    queries.push(
      `"${org}" (staff OR "contact us" OR meetings OR events) (director OR manager)`,
      `"${org}" filetype:pdf (conference OR prospectus OR housing)`
    );
  }

  const plan = buildCandidateSearchQueries({
    id: input.opportunityId,
    title: input.opportunityName,
    organizationName: input.organization,
    opportunityType: input.eventType,
    segment: input.segment,
  });
  queries.push(...(plan.passes?.A_EVENT_SPECIFIC || []).slice(0, 2));
  queries.push(...(plan.passes?.B_ORGANIZATION_SPECIFIC || []).slice(0, 2));

  return [...new Set(queries.filter(Boolean))].slice(0, 8);
}

/**
 * @param {object} input blind WHO input
 * @param {{ version?: 'v3'|'v4' }} [opts]
 */
export async function discoverWhoV3(input, opts = {}) {
  const version = opts.version === "v4" ? "v4" : "v3";
  const started = Date.now();
  const metrics = {
    queries: 0,
    pagesFetched: 0,
    pdfsInspected: 0,
    staffCrawlPages: 0,
    htmlExtractPeople: 0,
    stopReason: null,
  };

  const eventFamily = classifyEventFamily({
    title: input.opportunityName,
    organizationName: input.organization,
    opportunityType: input.eventType,
    segment: input.segment,
  });

  const officialDomains = resolveOfficialDomains(input);
  const queries = buildV3Queries(input, officialDomains);
  metrics.queries = queries.length;

  const serpHits = [];
  for (const q of queries) {
    const r = await serp(q, 5);
    for (const o of r.organic || []) {
      if (o.url && !isSocialOrAggregator(o.url)) serpHits.push(o);
    }
  }

  serpHits.sort((a, b) => {
    const ca = classifyDomain(hostnameOf(a.url), input);
    const cb = classifyDomain(hostnameOf(b.url), input);
    const pressPenalty = (u) => (/\/press\/|press-release|newsroom|newsdesk/i.test(u) ? 1 : 0);
    return (
      (isOfficialClass(ca) ? 0 : 1) - (isOfficialClass(cb) ? 0 : 1) ||
      pressPenalty(a.url) - pressPenalty(b.url)
    );
  });

  const seedUrls = [
    ...(input.eventSourceUrls || []),
    ...officialDomains.map((d) => d.seedUrl).filter(Boolean),
    ...serpHits
      .filter((h) => isOfficialClass(classifyDomain(hostnameOf(h.url), input)))
      .map((h) => h.url)
      .slice(0, 5),
  ].filter((u) => u && /^https?:/i.test(u) && !isSocialOrAggregator(u));

  const crawl = await crawlOfficialStaffPages({
    seeds: [...new Set(seedUrls)].slice(0, 6),
    maxPages: version === "v4" ? 8 : 6,
  });
  metrics.staffCrawlPages = crawl.pages.length;
  metrics.pagesFetched += crawl.pages.length;

  const pages = [...crawl.pages];
  const seen = new Set(pages.map((p) => p.url));
  for (const hit of serpHits.slice(0, 10)) {
    if (pages.length >= (version === "v4" ? 10 : 8)) break;
    if (!hit.url || seen.has(hit.url) || isSocialOrAggregator(hit.url)) continue;
    if (/\/press\/|press-release|newsroom|newsdesk/i.test(hit.url) && pages.length >= 3) {
      continue; // deprioritize press when we already have pages
    }
    const cls = classifyDomain(hostnameOf(hit.url), input);
    if (!isOfficialClass(cls) && pages.length >= 5) continue;
    if (/\.pdf($|\?)/i.test(hit.url)) continue;
    const fetched = await fetchResearchPage(hit.url, { timeoutMs: 16000 });
    metrics.pagesFetched += 1;
    if (!fetched.ok || !fetched.text) continue;
    seen.add(hit.url);
    pages.push({
      url: fetched.url || hit.url,
      text: htmlToSearchableText(fetched.text).replace(/\s+/g, " ").trim().slice(0, 9000),
      html: fetched.text.slice(0, 200_000),
      kind: "serp_page",
      domainClass: cls,
    });
  }

  // Deterministic HTML extract (V4)
  const htmlPeople = [];
  const htmlFunctional = [];
  if (version === "v4") {
    for (const p of pages) {
      if (!p.html) continue;
      const extracted = extractContactsFromHtml(p.html, p.url);
      htmlPeople.push(...extracted.people);
      htmlFunctional.push(...extracted.functional);
    }
    metrics.htmlExtractPeople = htmlPeople.length;
  }

  const pdfUrls = collectPdfCandidates(serpHits, crawl.pdfUrls || []);
  const pdfExtract = await extractFromPdfUrls(
    pdfUrls.slice(0, version === "v4" ? 4 : 3)
  );
  metrics.pdfsInspected = pdfExtract.inspected.length;

  const user = JSON.stringify(
    {
      hotel: input.hotelName,
      opportunity: input.opportunityName,
      organization: input.organization,
      eventType: input.eventType,
      eventFamily,
      officialDomains,
      desiredRoles: input.desiredRoleFamilies,
      instructions:
        version === "v4"
          ? "Extract complete relevant leadership sets. Do not return President/CEO from press alone. Prefer partnerships/events and tournament directors. Always capture functional inboxes."
          : undefined,
      serpSnippets: serpHits.slice(0, 10),
      pages: pages.map((p) => ({
        url: p.url,
        kind: p.kind,
        text: String(p.text || "").slice(0, 5500),
      })),
      htmlPeople: htmlPeople.slice(0, 10),
      pdfPeople: pdfExtract.people.slice(0, 8),
      pdfFunctional: pdfExtract.functional.slice(0, 4),
    },
    null,
    2
  );

  let extracted = {
    people: [],
    functionalContacts: [],
    stopReason: null,
    leadershipSetComplete: false,
  };
  try {
    extracted = await openaiExtract(SYSTEM_WHO, user);
  } catch (err) {
    return {
      opportunityId: input.opportunityId,
      hotelId: input.hotelId,
      opportunityName: input.opportunityName,
      organization: input.organization,
      primaryKind: "UNRESOLVED",
      researchState: WHO_RESEARCH_STATE.NO_WHO_RESEARCHED,
      people: [],
      functionalContacts: [...htmlFunctional, ...(pdfExtract.functional || [])],
      queries,
      error: String(err.message || err),
      metrics: { ...metrics, latencyMs: Date.now() - started },
      passVersion: version === "v4" ? "native_who_blind_v4" : "native_who_blind_v3",
    };
  }

  // Merge HTML + PDF people (deterministic)
  for (const p of [...htmlPeople, ...(pdfExtract.people || [])]) {
    if (!p.name || !isLikelyPersonName(p.name)) continue;
    (extracted.people ||= []).push({
      name: p.name,
      role: p.role,
      organization: input.organization,
      email: p.email,
      phone: p.phone,
      emailKind: p.email ? "PERSONAL_WORK" : null,
      gdiContactRole: mapTitleToRole(p.role, p.pdfRoleContext),
      relevance: /tournament director|executive director|partnerships|member services/i.test(
        String(p.role || "")
      )
        ? "PRIMARY_DECISION_MAKER"
        : "OPERATIONAL_CONTACT",
      currentness: "LIKELY_CURRENT",
      pdfRoleContext: p.pdfRoleContext || null,
      evidenceQuote: p.evidenceQuote,
      sourceUrl: p.sourceUrl,
      fromPdf: Boolean(p.evidenceType?.startsWith("PDF")),
      fromHtml: Boolean(p.fromHtml),
      eventSpecificEvidence: Boolean(p.eventSpecificEvidence),
    });
  }
  for (const f of [...htmlFunctional, ...(pdfExtract.functional || [])]) {
    (extracted.functionalContacts ||= []).push(f);
  }

  const people = [];
  const rejectionLog = [];

  for (const p of extracted.people || []) {
    const names = splitPeople(p.name);
    const nameList = names.length ? names : isLikelyPersonName(p.name) ? [p.name] : [];
    for (const name of nameList) {
      const sourceUrl = p.sourceUrl || pages[0]?.url || null;
      const host = hostnameOf(sourceUrl);
      const domainClass = classifyDomain(host, input);
      const onOfficialDomain = isOfficialClass(domainClass);
      const eventSpecificEvidence =
        Boolean(p.fromPdf) ||
        Boolean(p.fromHtml && p.eventSpecificEvidence) ||
        /event|conference|housing|registration|organizer|tournament|executive staff|please .*contact/i.test(
          `${p.evidenceQuote || ""} ${p.role || ""}`
        ) ||
        domainClass === DOMAIN_CLASS.EVENT_OFFICIAL;

      const cand = {
        name,
        role: p.role || null,
        organization: p.organization || input.organization,
        email:
          p.emailKind === "FUNCTIONAL_INBOX" ||
          (p.email &&
            /^(info|events|contact|hello|office|meetings|housing)@/i.test(p.email))
            ? null
            : p.email || null,
        phone: p.phone || null,
        gdiContactRole: p.gdiContactRole || mapTitleToRole(p.role, p.pdfRoleContext),
        roleRelevance: p.relevance || "WEAK_RELEVANCE",
        currentness: p.currentness || "UNKNOWN",
        pdfRoleContext: p.pdfRoleContext || null,
        sourceUrl,
        evidenceQuote: p.evidenceQuote || null,
        onOfficialDomain,
        eventSpecificEvidence,
        domainClass,
        fromPdf: Boolean(p.fromPdf),
        fromHtml: Boolean(p.fromHtml),
      };

      cand.evidenceMatrix = buildEvidenceMatrix(cand, { eventFamily });
      cand.evidenceTier = inferRoleEvidenceTier(cand, { eventFamily });
      Object.assign(cand, buildConfidenceComponents(cand));

      const rejection = applyRejectionGates(cand, {
        organization: input.organization,
        opportunityName: input.opportunityName,
        eventFamily,
      });
      if (rejection.reject) {
        rejectionLog.push({ name, gate: rejection.gate, sourceUrl });
        continue;
      }

      const conf = confirmWhoFromEvidence(cand);
      if (conf.researchState === "REJECTED") {
        rejectionLog.push({ name, gate: "TIER_D_REJECT", sourceUrl });
        continue;
      }

      people.push({
        ...cand,
        researchState: conf.researchState,
        gateReason: conf.reason,
        gateOk: conf.researchState === WHO_RESEARCH_STATE.NAMED_PERSON_CONFIRMED,
      });
    }

    if (p.email && /^(info|events|contact|hello|meetings|housing)@/i.test(p.email)) {
      (extracted.functionalContacts ||= []).push({
        name: p.organization || input.organization || "Functional inbox",
        email: p.email,
        phone: null,
        role: "Functional inbox",
        sourceUrl: p.sourceUrl || null,
      });
    }
  }

  // Dedupe by name keeping best rank
  people.sort(rankWhoCandidates);
  const deduped = [];
  const seenNames = new Set();
  for (const p of people) {
    const key = String(p.name).toLowerCase();
    if (seenNames.has(key)) continue;
    seenNames.add(key);
    deduped.push(p);
  }

  const coverageMet = roleCoverageSatisfied(deduped, eventFamily);
  const tierA = deduped.filter((p) => p.evidenceTier === ROLE_EVIDENCE_TIER.A_DIRECT_EVENT);
  const confirmed = deduped.filter((p) => p.gateOk);

  // V4 stop: role coverage, not first person
  let capped;
  if (version === "v4") {
    if (/SPORTS/i.test(eventFamily) || extracted.leadershipSetComplete) {
      capped = deduped
        .filter((p) => p.gateOk || p.researchState === WHO_RESEARCH_STATE.NAMED_PERSON_NEEDS_VALIDATION)
        .slice(0, 4);
      metrics.stopReason = coverageMet ? "ROLE_COVERAGE_MET" : "MULTI_PERSON_PARTIAL";
    } else if (coverageMet && confirmed.length) {
      capped = confirmed.slice(0, 3);
      metrics.stopReason = "ROLE_COVERAGE_MET";
    } else if (tierA.length) {
      capped = deduped.filter((p) => p.evidenceTier === "A" || p.evidenceTier === "B").slice(0, 3);
      metrics.stopReason = "TIER_A_SET";
    } else if (confirmed.length) {
      capped = confirmed.slice(0, 2);
      metrics.stopReason = "TIER_B_CORROBORATED";
    } else if (deduped.length) {
      capped = deduped.slice(0, 1);
      metrics.stopReason = "VALIDATION_ONLY";
    } else {
      capped = [];
      metrics.stopReason = extracted.stopReason || "NONE";
    }
  } else {
    capped = tierA.length
      ? tierA.slice(0, 2)
      : confirmed.length
        ? confirmed.slice(0, 2)
        : deduped.slice(0, 1);
    metrics.stopReason = tierA.length ? "TIER_A_FOUND" : confirmed.length ? "TIER_B_CORROBORATED" : "NONE";
  }

  capped.sort(rankWhoCandidates);
  const primary = capped[0] || null;
  const secondary = capped[1] || null;
  const functionalContacts = dedupeFunctional(extracted.functionalContacts || []);

  let researchState = WHO_RESEARCH_STATE.NO_WHO_RESEARCHED;
  if (primary?.gateOk) researchState = WHO_RESEARCH_STATE.NAMED_PERSON_CONFIRMED;
  else if (primary) researchState = WHO_RESEARCH_STATE.NAMED_PERSON_NEEDS_VALIDATION;
  else if (functionalContacts.length) {
    researchState = WHO_RESEARCH_STATE.FUNCTIONAL_CONTACT_ONLY_RESEARCHED;
  }

  return {
    opportunityId: input.opportunityId,
    hotelId: input.hotelId,
    opportunityName: input.opportunityName,
    organization: input.organization,
    eventFamily,
    officialDomains,
    primaryKind: primary
      ? "NAMED_PERSON"
      : functionalContacts.length
        ? "FUNCTIONAL_ENTITY"
        : "UNRESOLVED",
    researchState,
    primary,
    secondary,
    people: capped,
    functionalContacts,
    rejectionLog: rejectionLog.slice(0, 20),
    queries,
    urlsFetched: pages.map((p) => p.url),
    pdfsInspected: pdfExtract.inspected,
    sourceHits: {
      staffDirectory: crawl.pages.length,
      eventPages: pages.filter((p) => p.kind !== "staff_crawl").length,
      pdfWho: pdfExtract.people.length,
      pdfEmail: pdfExtract.people.filter((p) => p.email).length,
      pdfPhone: pdfExtract.people.filter((p) => p.phone).length,
      htmlWho: htmlPeople.length,
      functional: functionalContacts.length,
    },
    metrics: { ...metrics, latencyMs: Date.now() - started },
    passVersion: version === "v4" ? "native_who_blind_v4" : "native_who_blind_v3",
  };
}

export async function discoverWhoV4(input) {
  return discoverWhoV3(input, { version: "v4" });
}

function mapTitleToRole(role, pdfCtx) {
  const r = String(role || "");
  if (/housing/i.test(r) || pdfCtx === "HOUSING") return "HOUSING_OWNER";
  if (/registration/i.test(r) || pdfCtx === "REGISTRATION") return "REGISTRATION_OWNER";
  if (/sponsor/i.test(r) || pdfCtx === "SPONSORSHIP") return "SPONSORSHIP_OWNER";
  if (/partnership/i.test(r)) return "PARTNERSHIPS_OWNER";
  if (/member services|meetings/i.test(r)) return "MEETINGS_OWNER";
  if (/tournament director|conference director|audience engagement|director of programs/i.test(r)) {
    return "CONFERENCE_DIRECTOR";
  }
  if (/executive director/i.test(r)) return "EVENT_OWNER";
  if (pdfCtx === "ORGANIZER" || pdfCtx === "EVENT_STAFF") return "CONFERENCE_DIRECTOR";
  return "UNKNOWN";
}

function mapPdfContextToRole(ctx) {
  return mapTitleToRole("", ctx);
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
  return out.slice(0, 8);
}

export { mapPdfContextToRole };
