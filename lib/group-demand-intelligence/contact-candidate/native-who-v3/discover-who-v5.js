/**
 * Native WHO V5 — precision-first: section semantics, person boundaries, suppression, confirm caps.
 * Blind-safe. Does not load gold / Surfe / PDL.
 */

import { classifyEventFamily } from "../ontology.js";
import { WHO_RESEARCH_STATE } from "../who-gap.js";
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
import { extractContactsFromHtmlV5 } from "./html-contact-extract.js";
import {
  isStrictPersonName,
  expandMultiPersonWithSharedRole,
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
import { buildV3Queries } from "./discover-who-v3.js";

const EXTRACT_MODEL =
  process.env.GDI_NATIVE_WHO_MODEL || process.env.OPENAI_MODEL || "gpt-4o-mini";

const SYSTEM_WHO_V5 = `You extract WHO candidates for hotel group sales from official page SECTIONS.
Return JSON:
{
  "candidates": [
    {
      "name": "First Last",
      "role": "title",
      "organization": "org matching the opportunity organizer",
      "email": null,
      "phone": null,
      "gdiContactRole": "EVENT_OWNER|MEETINGS_OWNER|CONFERENCE_DIRECTOR|HOUSING_OWNER|REGISTRATION_OWNER|PARTNERSHIPS_OWNER|PROGRAM_OWNER|SPONSORSHIP_OWNER|UNKNOWN",
      "relevance": "PRIMARY_DECISION_MAKER|STRONG_INFLUENCER|OPERATIONAL_CONTACT|BACKUP_CONTACT",
      "sectionHint": "CONTACT|STAFF|TOURNAMENT_LEADERSHIP|CONFERENCE_STAFF|HOUSING|PRESS|SPEAKERS|OTHER",
      "evidenceQuote": "quote from same section proving event/meetings function",
      "sourceUrl": "url"
    }
  ],
  "functionalContacts": [{ "email": null, "phone": null, "role": null, "sourceUrl": null }],
  "multiPersonBlocks": [{ "raw": "Jane / John", "role": "Tournament Directors", "sourceUrl": null }]
}
STRICT:
- Only people in Contact / Staff / Tournament Leadership / Conference / Housing / Meetings sections.
- REJECT speakers, sponsors, board-only, press-quote-only, volunteers, venue hotel staff.
- Split "A / B" and "A & B" into multiPersonBlocks AND separate candidates.
- Never invent names from titles (no "Entry Forms", "Volunteer Coordinator" as names).
- Prefer max 3 candidates. Empty is better than garbage.
- Functional inboxes always go to functionalContacts.`;

async function serp(query, num = 5) {
  const result = await serpapiSearch({
    engine: "google",
    q: query,
    num,
    hl: "en",
    gl: "us",
  });
  if (!result.ok) return { organic: [] };
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
      temperature: 0.05,
      max_tokens: 2000,
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

function buildV5Queries(input, officialDomains) {
  const base = buildV3Queries(input, officialDomains);
  const org = input.organization || input.opportunityName || "";
  const extra = [];
  for (const d of officialDomains.slice(0, 2)) {
    extra.push(
      `site:${d.host} ("business development" OR "member services" OR partnerships OR "staff directory" OR "our team")`,
      `site:${d.host} (contact OR "vice president" OR "tournament director" OR "executive staff")`,
      `"${org}" ("business development" OR "tournament director" OR "executive staff") (contact OR staff)`
    );
  }
  return [...new Set([...extra, ...base])].slice(0, 8);
}

/**
 * Blind Native WHO V5 for one opportunity.
 */
export async function discoverWhoV5(input) {
  const started = Date.now();
  const metrics = {
    queries: 0,
    pagesFetched: 0,
    pdfsInspected: 0,
    staffCrawlPages: 0,
    extractedCandidates: 0,
    confirmed: 0,
    suppressed: 0,
    multiPersonBlocks: 0,
    stopReason: null,
  };

  const eventFamily = classifyEventFamily({
    title: input.opportunityName,
    organizationName: input.organization,
    opportunityType: input.eventType,
    segment: input.segment,
  });
  const rolePrefs = preferredRoleFamilies(eventFamily);

  const officialDomains = resolveOfficialDomains(input);
  const queries = buildV5Queries(input, officialDomains);
  metrics.queries = queries.length;

  const serpHits = [];
  for (const q of queries) {
    const r = await serp(q, 5);
    for (const o of r.organic || []) {
      if (!o.url || isSocialOrAggregator(o.url)) continue;
      const sec = classifySectionFromUrl(o.url, o.title || "");
      if (sectionIsReject(sec) || sectionIsIdentityOnly(sec)) continue;
      serpHits.push({ ...o, sectionKind: sec });
    }
  }

  serpHits.sort((a, b) => {
    const ca = classifyDomain(hostnameOf(a.url), input);
    const cb = classifyDomain(hostnameOf(b.url), input);
    return (isOfficialClass(ca) ? 0 : 1) - (isOfficialClass(cb) ? 0 : 1);
  });

  const seedUrls = [
    ...(input.eventSourceUrls || []),
    ...officialDomains.map((d) => d.seedUrl).filter(Boolean),
    ...serpHits.filter((h) => isOfficialClass(classifyDomain(hostnameOf(h.url), input))).map((h) => h.url),
  ]
    .filter((u) => u && /^https?:/i.test(u) && !isSocialOrAggregator(u))
    .filter((u) => !sectionIsReject(classifySectionFromUrl(u)) && !sectionIsIdentityOnly(classifySectionFromUrl(u)));

  const crawl = await crawlOfficialStaffPages({
    seeds: [...new Set(seedUrls)].slice(0, 6),
    maxPages: 7,
  });
  metrics.staffCrawlPages = crawl.pages.length;
  metrics.pagesFetched += crawl.pages.length;

  const pages = [...crawl.pages];
  const seen = new Set(pages.map((p) => p.url));
  for (const hit of serpHits.slice(0, 8)) {
    if (pages.length >= 9) break;
    if (!hit.url || seen.has(hit.url)) continue;
    if (/\.pdf($|\?)/i.test(hit.url)) continue;
    const fetched = await fetchResearchPage(hit.url, { timeoutMs: 16000 });
    metrics.pagesFetched += 1;
    if (!fetched.ok || !fetched.text) continue;
    seen.add(hit.url);
    pages.push({
      url: fetched.url || hit.url,
      text: htmlToSearchableText(fetched.text).replace(/\s+/g, " ").trim().slice(0, 8000),
      html: fetched.text.slice(0, 200_000),
      kind: "serp_page",
      domainClass: classifyDomain(hostnameOf(hit.url), input),
    });
  }

  const htmlPeople = [];
  const htmlFunctional = [];
  for (const p of pages) {
    if (!p.html) continue;
    if (sectionIsReject(classifySectionFromUrl(p.url)) || sectionIsIdentityOnly(classifySectionFromUrl(p.url))) {
      continue;
    }
    const extracted = extractContactsFromHtmlV5(p.html, p.url);
    htmlPeople.push(...extracted.people);
    htmlFunctional.push(...extracted.functional);
  }

  const pdfUrls = collectPdfCandidates(serpHits, crawl.pdfUrls || []).filter(
    (u) => !/volunteer/i.test(u)
  );
  const pdfExtract = await extractFromPdfUrls(pdfUrls.slice(0, 3));
  metrics.pdfsInspected = pdfExtract.inspected.length;

  // Prefer official, non-press pages for LLM
  const llmPages = pages
    .filter((p) => !sectionIsIdentityOnly(classifySectionFromUrl(p.url)))
    .slice(0, 6);

  let extracted = { candidates: [], functionalContacts: [], multiPersonBlocks: [] };
  try {
    extracted = await openaiExtract(
      SYSTEM_WHO_V5,
      JSON.stringify(
        {
          hotel: input.hotelName,
          opportunity: input.opportunityName,
          organization: input.organization,
          eventFamily,
          preferredRoles: rolePrefs,
          pages: llmPages.map((p) => ({
            url: p.url,
            section: classifySectionFromUrl(p.url),
            text: String(p.text || "").slice(0, 4500),
          })),
          htmlPeople: htmlPeople.slice(0, 8),
          pdfPeople: (pdfExtract.people || []).slice(0, 6),
        },
        null,
        2
      )
    );
  } catch (err) {
    return emptyResult(input, metrics, started, String(err.message || err));
  }

  // Merge multi-person blocks
  for (const block of extracted.multiPersonBlocks || []) {
    const expanded = expandMultiPersonWithSharedRole(block.raw, block.role);
    metrics.multiPersonBlocks += expanded.length > 1 ? 1 : 0;
    for (const e of expanded) {
      (extracted.candidates ||= []).push({
        name: e.name,
        role: e.role || block.role,
        organization: input.organization,
        sourceUrl: block.sourceUrl || llmPages[0]?.url,
        sectionHint: "TOURNAMENT_LEADERSHIP",
        evidenceQuote: block.raw,
        fromMultiPersonBlock: true,
        gdiContactRole: "EVENT_OWNER",
        relevance: "PRIMARY_DECISION_MAKER",
      });
    }
  }

  // Merge HTML + PDF + LLM into raw candidate pool
  const raw = [];
  for (const p of [
    ...(extracted.candidates || []),
    ...htmlPeople,
    ...(pdfExtract.people || []).map((p) => ({
      ...p,
      sectionHint: classifySectionFromUrl(p.sourceUrl || ""),
      fromPdf: true,
    })),
  ]) {
    const expanded = expandMultiPersonWithSharedRole(p.name, p.role);
    for (const e of expanded) {
      raw.push({
        ...p,
        name: e.name,
        role: e.role || p.role,
        fromMultiPersonBlock: e.fromMultiPersonBlock || p.fromMultiPersonBlock,
      });
    }
  }

  metrics.extractedCandidates = raw.length;

  const suppressionLog = [];
  const scored = [];

  for (const p of raw) {
    const sourceUrl = p.sourceUrl || pages[0]?.url || null;
    const host = hostnameOf(sourceUrl);
    const domainClass = classifyDomain(host, input);
    const onOfficialDomain = isOfficialClass(domainClass);
    const sectionKind =
      p.sectionKind ||
      p.sectionHint ||
      classifySectionFromUrl(sourceUrl);

    const cand = {
      name: p.name,
      role: p.role || null,
      organization: p.organization || input.organization,
      email:
        p.email && !/^(info|events|contact|hello|office|meetings|housing)@/i.test(p.email)
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
          /TOURNAMENT|CONFERENCE|CONTACT|EVENT|HOUSING|MEETINGS|STAFF/i.test(sectionKind)
      ),
      domainClass,
      sectionKind,
      fromPdf: Boolean(p.fromPdf),
      fromHtml: Boolean(p.fromHtml),
      fromMultiPersonBlock: Boolean(p.fromMultiPersonBlock),
      forConfirmation: true,
    };

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

    // Section IDENTITY_ONLY already suppressed; require meetings role for confirm
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

    const conf = confirmWhoFromEvidence(cand);
    if (conf.researchState === "REJECTED") {
      metrics.suppressed += 1;
      continue;
    }

    // V5: Tier C stays candidate-only (never auto-confirm)
    let researchState = conf.researchState;
    if (cand.evidenceTier === ROLE_EVIDENCE_TIER.C_STRONG_INFERENCE) {
      researchState = WHO_RESEARCH_STATE.NAMED_PERSON_NEEDS_VALIDATION;
    }
    // Require HIGH/MEDIUM section + meetings-ish role for CONFIRMED
    if (
      researchState === WHO_RESEARCH_STATE.NAMED_PERSON_CONFIRMED &&
      !/TOURNAMENT|CONFERENCE|CONTACT|EVENT|HOUSING|MEETINGS|STAFF|ORGANIZERS|REGISTRATION/i.test(
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

  const distributed =
    /SPORTS/i.test(eventFamily) &&
    scored.filter((p) => p.fromMultiPersonBlock || /tournament|executive director/i.test(p.role || "")).length >= 2;

  const capped = capConfirmedWho(scored, { distributedResponsibility: distributed });
  const people = capped.confirmed;
  metrics.confirmed = people.length;
  metrics.stopReason = people.length ? "PRIMARY_COVERAGE" : "NO_CONFIRM";

  // Demote remaining scored to candidates-only metadata (not returned as confirmed)
  const functionalContacts = dedupeFunctional([
    ...(extracted.functionalContacts || []),
    ...htmlFunctional,
    ...(pdfExtract.functional || []),
  ]);

  const primary = people[0] || null;
  let researchState = WHO_RESEARCH_STATE.NO_WHO_RESEARCHED;
  if (primary?.gateOk) researchState = WHO_RESEARCH_STATE.NAMED_PERSON_CONFIRMED;
  else if (scored[0]) researchState = WHO_RESEARCH_STATE.NAMED_PERSON_NEEDS_VALIDATION;
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
    secondary: people[1] || null,
    people,
    candidatesFound: scored.slice(0, 8).map((p) => ({
      name: p.name,
      role: p.role,
      sectionKind: p.sectionKind,
      researchState: p.researchState,
      sourceUrl: p.sourceUrl,
    })),
    functionalContacts,
    suppressionLog: suppressionLog.slice(0, 25),
    queries,
    urlsFetched: pages.map((p) => p.url),
    pdfsInspected: pdfExtract.inspected,
    sourceHits: {
      staffDirectory: crawl.pages.length,
      eventPages: pages.filter((p) => p.kind !== "staff_crawl").length,
      pdfWho: (pdfExtract.people || []).filter((p) => isStrictPersonName(p.name)).length,
      htmlWho: htmlPeople.filter((p) => isStrictPersonName(p.name)).length,
      multiPersonBlocks: metrics.multiPersonBlocks,
      functional: functionalContacts.length,
      extractedCandidates: metrics.extractedCandidates,
      suppressed: metrics.suppressed,
    },
    metrics: { ...metrics, latencyMs: Date.now() - started },
    passVersion: "native_who_blind_v5",
  };
}

function mapRole(role) {
  const r = String(role || "");
  if (/housing/i.test(r)) return "HOUSING_OWNER";
  if (/registration/i.test(r)) return "REGISTRATION_OWNER";
  if (/partnership/i.test(r)) return "PARTNERSHIPS_OWNER";
  if (/member services|meetings/i.test(r)) return "MEETINGS_OWNER";
  if (/tournament director|conference|audience engagement|business development/i.test(r)) {
    return "CONFERENCE_DIRECTOR";
  }
  if (/executive director/i.test(r)) return "EVENT_OWNER";
  return "UNKNOWN";
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

function emptyResult(input, metrics, started, error) {
  return {
    opportunityId: input.opportunityId,
    hotelId: input.hotelId,
    opportunityName: input.opportunityName,
    organization: input.organization,
    researchState: WHO_RESEARCH_STATE.NO_WHO_RESEARCHED,
    people: [],
    functionalContacts: [],
    error,
    metrics: { ...metrics, latencyMs: Date.now() - started },
    passVersion: "native_who_blind_v5",
  };
}
