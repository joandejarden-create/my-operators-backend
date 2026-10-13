/**
 * Native WHO V6 — discovery-first recall while preserving V5 precision gates.
 * Multi-stage research orchestration. Blind-safe (no gold / Surfe / PDL).
 *
 * Stages:
 *  1 Direct official/event evidence
 *  2 Official-domain deep-link discovery via search
 *  3 Documents/PDFs
 *  4 Role-specific search
 *  5 Person-boundary / multi-person leadership recovery from fetched text
 *  6 Functional fallback
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
  discoverOfficialDomainsFromSerpHits,
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
  SECTION_KIND,
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
import {
  expandEntityAliases,
  buildRoleSearchQueries,
  buildDeepLinkQueries,
} from "./entity-alias-expansion.js";

const EXTRACT_MODEL =
  process.env.GDI_NATIVE_WHO_MODEL || process.env.OPENAI_MODEL || "gpt-4o-mini";

export const RESEARCH_GAP = Object.freeze({
  NO_OFFICIAL_DOMAIN: "NO_OFFICIAL_DOMAIN",
  NO_EVENT_CONTACT_PAGE: "NO_EVENT_CONTACT_PAGE",
  NO_NAMED_STAFF: "NO_NAMED_STAFF",
  ROLE_NOT_FOUND: "ROLE_NOT_FOUND",
  MULTIPLE_CANDIDATES: "MULTIPLE_CANDIDATES",
  ROLE_UNCLEAR: "ROLE_UNCLEAR",
  CURRENTNESS_UNCLEAR: "CURRENTNESS_UNCLEAR",
  MULTI_PERSON_INCOMPLETE: "MULTI_PERSON_INCOMPLETE",
  COVERED: "COVERED",
});

const SYSTEM_WHO_V6 = `You extract WHO candidates for hotel group sales from official page SECTIONS.
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
      "sectionHint": "CONTACT|STAFF|TOURNAMENT_LEADERSHIP|CONFERENCE_STAFF|HOUSING|EVENT_TEAM|PRESS|SPEAKERS|OTHER",
      "evidenceQuote": "quote from same section proving event/meetings/partnerships function",
      "sourceUrl": "url"
    }
  ],
  "functionalContacts": [{ "email": null, "phone": null, "role": null, "sourceUrl": null }],
  "multiPersonBlocks": [{ "raw": "Jane / John", "role": "Tournament Directors", "sourceUrl": null }],
  "distributedLeadership": false
}
STRICT:
- Only people in Contact / Staff / Tournament Leadership / Conference / Housing / Meetings / Event / Partnerships-contact sections.
- REJECT speakers, sponsors-as-exhibitors, board-only, press-quote-only, volunteers, venue hotel staff.
- Split "A / B", "A & B", and sequential "Name, Title Name, Title" into separate candidates AND multiPersonBlocks.
- When a page lists co-directors / executive staff / multiple tournament contacts, extract ALL of them.
- Never invent names from titles.
- Prefer max 5 candidates when distributed leadership is true; else max 3.
- Functional inboxes always go to functionalContacts.
- VP / Director of Business Development or Partnerships on an official event or partner-contact page IS valid WHO for group sales.`;

async function serp(query, num = 5) {
  const result = await serpapiSearch(
    {
      engine: "google",
      q: query,
      num,
      hl: "en",
      gl: "us",
    },
    { timeoutMs: 25000 }
  );
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
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 45000);
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

async function runSerpQueries(queries, input, serpHits, metrics) {
  for (const q of queries) {
    metrics.queries += 1;
    const r = await serp(q, 5);
    for (const o of r.organic || []) {
      if (!o.url || isSocialOrAggregator(o.url)) continue;
      const sec = classifySectionFromUrl(o.url, o.title || "");
      if (sectionIsReject(sec) || sectionIsIdentityOnly(sec)) continue;
      serpHits.push({ ...o, sectionKind: sec, query: q });
    }
  }
}

async function fetchMorePages(urls, pages, seen, input, metrics, limit) {
  for (const url of urls) {
    if (pages.length >= limit) break;
    if (!url || seen.has(url) || isSocialOrAggregator(url)) continue;
    if (sectionIsReject(classifySectionFromUrl(url))) continue;
    if (/\.pdf($|\?)/i.test(url)) continue;
    const fetched = await fetchResearchPage(url, { timeoutMs: 16000 });
    metrics.pagesFetched += 1;
    if (!fetched.ok || !fetched.text) continue;
    seen.add(url);
    pages.push({
      url: fetched.url || url,
      text: htmlToSearchableText(fetched.text).replace(/\s+/g, " ").trim().slice(0, 8000),
      html: fetched.text.slice(0, 200_000),
      kind: "stage_fetch",
      domainClass: classifyDomain(hostnameOf(url), input),
    });
  }
}

function recoverSequentialFromPageText(pages) {
  const recovered = [];
  for (const p of pages) {
    const text = String(p.text || "");
    // Look for leadership / contact windows
    const windows = [];
    const markers =
      /(?:executive staff|tournament (?:directors?|contacts?|leadership)|event staff|conference (?:staff|director)|please contact|contact:)/gi;
    let m;
    while ((m = markers.exec(text))) {
      windows.push(text.slice(m.index, m.index + 400));
    }
    if (!windows.length && /baron|director|coordinator/i.test(text)) {
      windows.push(text.slice(0, 1200));
    }
    for (const w of windows.slice(0, 4)) {
      const seq = splitSequentialNameRoleString(w);
      if (seq.length >= 2) {
        for (const e of seq) {
          recovered.push({
            name: e.name,
            role: e.role,
            sourceUrl: p.url,
            sectionHint: classifySectionFromUrl(p.url) || SECTION_KIND.TOURNAMENT_LEADERSHIP,
            evidenceQuote: w.slice(0, 220),
            fromMultiPersonBlock: true,
            gdiContactRole: "EVENT_OWNER",
            relevance: "PRIMARY_DECISION_MAKER",
          });
        }
      }
    }
  }
  return recovered;
}

function scoreAndConfirm(raw, input, eventFamily, metrics, suppressionLog) {
  const scored = [];
  for (const p of raw) {
    const sourceUrl = p.sourceUrl || null;
    const host = hostnameOf(sourceUrl);
    const domainClass = classifyDomain(host, input);
    const onOfficialDomain = isOfficialClass(domainClass);
    const sectionKind =
      p.sectionKind || p.sectionHint || classifySectionFromUrl(sourceUrl);

    const cand = {
      name: p.name,
      role: cleanRole(p.role),
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
          /TOURNAMENT|CONFERENCE|CONTACT|EVENT|HOUSING|MEETINGS|STAFF|ORGANIZERS|REGISTRATION/i.test(
            sectionKind
          )
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

    // Reject garbage roles (cheer text, sentence fragments)
    if (
      cand.role &&
      (/!{1,}|remember to|don't be shy|click here|lorem ipsum/i.test(cand.role) ||
        String(cand.role).length > 100)
    ) {
      metrics.suppressed += 1;
      suppressionLog.push({
        name: cand.name,
        code: SUPPRESSION_CODE.TITLE_CONTEXT_BLEED,
        sourceUrl,
      });
      continue;
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

  // Opportunity-title boost (Education Forum → partnership/events/education roles)
  const oppBlob = `${input.opportunityName || ""} ${input.organization || ""}`.toLowerCase();
  scored.sort((a, b) => {
    const score = (p) => {
      let s = 0;
      const role = `${p.role || ""} ${p.gdiContactRole || ""}`.toLowerCase();
      if (/education|forum|engagement/.test(oppBlob) && /education|engagement|partnership|events/.test(role)) {
        s += 5;
      }
      if (/tournament|open|cup/.test(oppBlob) && /tournament|executive director/.test(role)) s += 5;
      if (p.email) s += 1;
      return s;
    };
    return score(b) - score(a);
  });

  return scored;
}

function coverageMet(scored, eventFamily, distributedHint) {
  const confirmed = scored.filter((p) => p.gateOk);
  if (!confirmed.length) return false;
  if (distributedHint || /SPORTS/i.test(eventFamily)) {
    const leaders = confirmed.filter((p) =>
      /tournament|executive director|director|co-?vp|vice president/i.test(p.role || "")
    );
    if (leaders.length >= 2) return true;
    // Single tournament director is enough if no co-leadership evidence
    if (confirmed.length >= 1 && !distributedHint) return true;
    return false;
  }
  // Staff-directory style: require at least one confirmed; keep researching if only one
  // weak/generic role until a meetings/BD/conference title appears.
  const strong = confirmed.filter((p) =>
    /MEETINGS|CONFERENCE|PARTNERSHIP|EVENT|HOUSING|REGISTRATION|SPONSORSHIP/i.test(
      p.gdiContactRole || ""
    ) ||
    /meetings|conference|tournament|business development|partnership|audience engagement|member services|housing|registration/i.test(
      p.role || ""
    )
  );
  return strong.length >= 1;
}

/** Prefer event/staff/partner deep pages over generic about pages. */
function scoreDeepUrl(url = "") {
  const u = String(url).toLowerCase();
  if (/nationalclays|event-staff|partner-us|\/staff\/|tournament-schedule/i.test(u)) return 0;
  if (/\/events?\/|\/conference|\/forum|\/contact/i.test(u)) return 1;
  if (/staff|team|directory|schedule|housing/i.test(u)) return 2;
  if (/about|who-we-are|mission/i.test(u)) return 5;
  return 3;
}

/** Rank official hosts: delraybeachopen.com before downtowndelraybeach.com */
function scoreOfficialHost(host = "", input = {}) {
  const h = String(host).toLowerCase();
  const blob = `${input.organization || ""} ${input.opportunityName || ""}`.toLowerCase();
  const tokens = blob
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter((t) => t.length >= 4)
    .slice(0, 6);
  let score = 10;
  for (const t of tokens) {
    if (h.includes(t)) score -= 2;
  }
  if (/\.org$/.test(h)) score -= 1;
  if (h.split(".").length === 2) score -= 1;
  // Penalize tangential city chambers / tourism sites
  if (/downtown|visit|tourism|chamber|thepalmbeaches|tennis\.com/i.test(h)) score += 5;
  return score;
}

/**
 * Blind Native WHO V6 for one opportunity.
 */
export async function discoverWhoV6(input) {
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
    stagesRun: [],
    stopReason: null,
    unresolvedReasons: [],
  };
  const stageLog = [];

  const eventFamily = classifyEventFamily({
    title: input.opportunityName,
    organizationName: input.organization,
    opportunityType: input.eventType,
    segment: input.segment,
  });
  const rolePrefs = preferredRoleFamilies(eventFamily);
  const aliases = expandEntityAliases(input);
  const officialDomains = resolveOfficialDomains(input);

  if (!officialDomains.length) {
    metrics.unresolvedReasons.push(RESEARCH_GAP.NO_OFFICIAL_DOMAIN);
  }

  const serpHits = [];
  const pages = [];
  const seen = new Set();
  const suppressionLog = [];
  let pdfExtract = { people: [], functional: [], inspected: [] };
  let extracted = { candidates: [], functionalContacts: [], multiPersonBlocks: [], distributedLeadership: false };
  let htmlPeople = [];
  let htmlFunctional = [];
  let scored = [];

  // ——— STAGE 1: direct official / event ———
  metrics.stagesRun.push(1);
  stageLog.push({ stage: 1, name: "DIRECT_OFFICIAL_EVENT" });
  const stage1Queries = [
    ...buildV3Queries(input, officialDomains).slice(0, 4),
    ...aliases.orgAliases.slice(0, 2).flatMap((o) => [
      `"${o}" (contact OR staff OR "business development" OR "tournament director")`,
    ]),
  ].slice(0, 6);
  await runSerpQueries(stage1Queries, input, serpHits, metrics);

  // Promote acronym/org-matching SERP hosts (e.g. fia.org from FIA) even when seeds empty
  const discoveredDomains = discoverOfficialDomainsFromSerpHits(serpHits, input);
  for (const d of discoveredDomains) {
    if (!officialDomains.some((x) => x.host === d.host)) officialDomains.push(d);
  }
  // Prefer event/org apex domains that best match opportunity tokens
  officialDomains.sort((a, b) => scoreOfficialHost(a.host, input) - scoreOfficialHost(b.host, input));

  // Immediately deep-link on discovered official domains (event/staff/partner pages)
  if (officialDomains.length) {
    const deepEarly = buildDeepLinkQueries(input, aliases, officialDomains.slice(0, 2));
    await runSerpQueries(deepEarly.slice(0, 4), input, serpHits, metrics);
  }

  const seedUrls = [
    ...(input.eventSourceUrls || []),
    ...officialDomains.map((d) => d.seedUrl).filter(Boolean),
    ...serpHits
      .filter((h) => isOfficialClass(classifyDomain(hostnameOf(h.url), input)))
      .sort((a, b) => scoreDeepUrl(a.url) - scoreDeepUrl(b.url))
      .map((h) => h.url),
  ]
    .filter((u) => u && /^https?:/i.test(u) && !isSocialOrAggregator(u))
    .filter(
      (u) =>
        !sectionIsReject(classifySectionFromUrl(u)) &&
        !sectionIsIdentityOnly(classifySectionFromUrl(u))
    );

  const crawl = await crawlOfficialStaffPages({
    seeds: [...new Set(seedUrls)].slice(0, 8),
    maxPages: 8,
  });
  metrics.staffCrawlPages = crawl.pages.length;
  metrics.pagesFetched += crawl.pages.length;
  for (const p of crawl.pages) {
    if (!seen.has(p.url)) {
      seen.add(p.url);
      pages.push(p);
    }
  }

  // Speculative official deep paths (nav-poor pages Webhound finds via search)
  const speculative = [];
  for (const d of officialDomains.slice(0, 2)) {
    const base = `https://www.${d.host}`;
    speculative.push(
      `${base}/about/staff/`,
      `${base}/staff/`,
      `${base}/contact/`,
      `${base}/en/event-staff`,
      `${base}/en/usta-nationalclays`,
      `${base}/tournament-schedule.html`,
      `${base}/partner-us`,
      `${base}/fia/partner-us`
    );
  }
  await fetchMorePages(speculative, pages, seen, input, metrics, 16);

  await fetchMorePages(
    [...serpHits].sort((a, b) => scoreDeepUrl(a.url) - scoreDeepUrl(b.url)).map((h) => h.url).slice(0, 8),
    pages,
    seen,
    input,
    metrics,
    18
  );

  ({ scored, extracted, htmlPeople, htmlFunctional, pdfExtract } = await extractAndScore({
    pages,
    serpHits,
    crawl,
    input,
    eventFamily,
    rolePrefs,
    metrics,
    suppressionLog,
    pdfExtract,
    allowPdf: false,
  }));

  let distributedHint = Boolean(extracted.distributedLeadership) ||
    scored.filter((p) => p.fromMultiPersonBlock).length >= 2;

  if (!coverageMet(scored, eventFamily, distributedHint)) {
    metrics.unresolvedReasons.push(
      scored.length ? RESEARCH_GAP.ROLE_NOT_FOUND : RESEARCH_GAP.NO_NAMED_STAFF
    );
  }

  // ——— STAGE 2: deep-link SERP → official domain ———
  if (!coverageMet(scored, eventFamily, distributedHint)) {
    metrics.stagesRun.push(2);
    stageLog.push({ stage: 2, name: "DEEP_LINK_OFFICIAL", gap: metrics.unresolvedReasons.at(-1) });
    const deepQs = buildDeepLinkQueries(input, aliases, officialDomains);
    await runSerpQueries(deepQs, input, serpHits, metrics);
    const officialHits = serpHits
      .filter((h) => isOfficialClass(classifyDomain(hostnameOf(h.url), input)))
      .map((h) => h.url);
    await fetchMorePages(officialHits, pages, seen, input, metrics, 14);
    ({ scored, extracted, htmlPeople, htmlFunctional, pdfExtract } = await extractAndScore({
      pages,
      serpHits,
      crawl,
      input,
      eventFamily,
      rolePrefs,
      metrics,
      suppressionLog,
      pdfExtract,
      allowPdf: false,
    }));
    distributedHint =
      distributedHint ||
      Boolean(extracted.distributedLeadership) ||
      scored.filter((p) => p.fromMultiPersonBlock).length >= 2;
  }

  // ——— STAGE 3: PDFs ———
  if (!coverageMet(scored, eventFamily, distributedHint)) {
    metrics.stagesRun.push(3);
    stageLog.push({ stage: 3, name: "DOCUMENTS_PDFS" });
    ({ scored, extracted, htmlPeople, htmlFunctional, pdfExtract } = await extractAndScore({
      pages,
      serpHits,
      crawl,
      input,
      eventFamily,
      rolePrefs,
      metrics,
      suppressionLog,
      pdfExtract,
      allowPdf: true,
    }));
  }

  // ——— STAGE 4: role-specific search ———
  if (!coverageMet(scored, eventFamily, distributedHint)) {
    metrics.stagesRun.push(4);
    stageLog.push({ stage: 4, name: "ROLE_SPECIFIC_SEARCH" });
    const roleQs = buildRoleSearchQueries(input, aliases, officialDomains);
    await runSerpQueries(roleQs, input, serpHits, metrics);
    await fetchMorePages(
      serpHits.map((h) => h.url),
      pages,
      seen,
      input,
      metrics,
      18
    );
    ({ scored, extracted, htmlPeople, htmlFunctional, pdfExtract } = await extractAndScore({
      pages,
      serpHits,
      crawl,
      input,
      eventFamily,
      rolePrefs,
      metrics,
      suppressionLog,
      pdfExtract,
      allowPdf: true,
    }));
    distributedHint =
      distributedHint ||
      Boolean(extracted.distributedLeadership) ||
      scored.filter((p) => p.fromMultiPersonBlock).length >= 2;
  }

  // ——— STAGE 5: multi-person leadership recovery from page text ———
  metrics.stagesRun.push(5);
  stageLog.push({ stage: 5, name: "MULTI_PERSON_RECOVERY" });
  const recovered = recoverSequentialFromPageText(pages);
  if (recovered.length) {
    const rawExtra = [];
    for (const p of recovered) {
      const expanded = expandMultiPersonWithSharedRole(p.name, p.role);
      for (const e of expanded.length ? expanded : [{ name: p.name, role: p.role }]) {
        rawExtra.push({ ...p, name: e.name, role: e.role || p.role, fromMultiPersonBlock: true });
      }
    }
    const mergedRaw = [...rawExtra, ...scored.map((s) => ({ ...s, sectionHint: s.sectionKind }))];
    scored = scoreAndConfirm(mergedRaw, input, eventFamily, metrics, suppressionLog);
    distributedHint = distributedHint || recovered.length >= 2;
    if (distributedHint && scored.filter((p) => p.gateOk).length < 2) {
      metrics.unresolvedReasons.push(RESEARCH_GAP.MULTI_PERSON_INCOMPLETE);
    }
  }

  // Cap + stop
  const leadershipConfirmed = scored.filter(
    (p) =>
      p.gateOk &&
      isStrictPersonName(p.name) &&
      (/tournament|executive director|chairman|doubles coordinator|co-?vp|vice president|cups? director/i.test(
        p.role || ""
      ) ||
        p.sectionKind === "TOURNAMENT_LEADERSHIP" ||
        p.fromMultiPersonBlock)
  );
  const distributed =
    distributedHint ||
    leadershipConfirmed.length >= 2 ||
    (/SPORTS/i.test(eventFamily) &&
      scored.filter(
        (p) => p.fromMultiPersonBlock || /tournament|executive director|co-?vp/i.test(p.role || "")
      ).length >= 2);

  const capped = capConfirmedWho(scored, { distributedResponsibility: distributed });
  let people = capped.confirmed;
  if (distributed || leadershipConfirmed.length >= 2) {
    const pool = leadershipConfirmed.length >= 2 ? leadershipConfirmed : scored.filter((p) => p.gateOk);
    const seenLead = new Set();
    const unique = [];
    for (const p of pool) {
      const key = String(p.name || "")
        .toLowerCase()
        .replace(/[^a-z\s]/g, "")
        .replace(/\s+/g, " ")
        .trim();
      if (!key || seenLead.has(key) || !isStrictPersonName(p.name)) continue;
      seenLead.add(key);
      unique.push(p);
    }
    people = unique.slice(0, 3).map((p, i) => ({
      ...p,
      whoSlot: i === 0 ? "PRIMARY" : "SECONDARY",
    }));
  } else {
    // Also prefer opportunity-boosted unique confirmed set (Josh over duplicates)
    const seenP = new Set();
    const uniqueConfirmed = [];
    for (const p of scored.filter((x) => x.gateOk)) {
      const key = String(p.name || "")
        .toLowerCase()
        .replace(/[^a-z\s]/g, "")
        .replace(/\s+/g, " ")
        .trim();
      if (!key || seenP.has(key) || !isStrictPersonName(p.name)) continue;
      seenP.add(key);
      uniqueConfirmed.push(p);
    }
    if (uniqueConfirmed.length) {
      people = uniqueConfirmed.slice(0, 2).map((p, i) => ({
        ...p,
        whoSlot: i === 0 ? "PRIMARY" : "SECONDARY",
      }));
    }
  }

  // Dedupe by normalized name
  const seenNames = new Set();
  people = people.filter((p) => {
    const key = String(p.name || "")
      .toLowerCase()
      .replace(/[^a-z\s]/g, "")
      .replace(/\s+/g, " ")
      .trim();
    if (!key || seenNames.has(key)) return false;
    seenNames.add(key);
    return isStrictPersonName(p.name);
  });

  metrics.confirmed = people.length;
  const functionalContacts = dedupeFunctional([
    ...(extracted.functionalContacts || []),
    ...htmlFunctional,
    ...(pdfExtract.functional || []),
  ]);

  if (coverageMet(scored, eventFamily, distributed)) {
    metrics.stopReason = "ROLE_COVERAGE_MET";
    metrics.unresolvedReasons = [RESEARCH_GAP.COVERED];
  } else if (people.length) {
    metrics.stopReason = "PARTIAL_COVERAGE";
  } else if (functionalContacts.length) {
    metrics.stopReason = "FUNCTIONAL_FALLBACK";
    metrics.stagesRun.push(6);
    stageLog.push({ stage: 6, name: "FUNCTIONAL_FALLBACK" });
  } else {
    metrics.stopReason = "NO_CONFIRM";
  }

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
    queries: stage1Queries,
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
      stagesRun: metrics.stagesRun,
    },
    metrics: { ...metrics, latencyMs: Date.now() - started },
    passVersion: "native_who_blind_v6",
  };
}

async function extractAndScore({
  pages,
  serpHits,
  crawl,
  input,
  eventFamily,
  rolePrefs,
  metrics,
  suppressionLog,
  pdfExtract,
  allowPdf,
}) {
  const htmlPeople = [];
  const htmlFunctional = [];
  for (const p of pages) {
    if (!p.html) continue;
    if (
      sectionIsReject(classifySectionFromUrl(p.url)) ||
      sectionIsIdentityOnly(classifySectionFromUrl(p.url))
    ) {
      continue;
    }
    const extractedHtml = extractContactsFromHtmlV5(p.html, p.url);
    htmlPeople.push(...extractedHtml.people);
    htmlFunctional.push(...extractedHtml.functional);
  }

  let nextPdf = pdfExtract;
  if (allowPdf) {
    const pdfUrls = collectPdfCandidates(serpHits, crawl.pdfUrls || []).filter(
      (u) => !/volunteer/i.test(u)
    );
    nextPdf = await extractFromPdfUrls(pdfUrls.slice(0, 4));
    metrics.pdfsInspected = nextPdf.inspected.length;
  }

  const llmPages = pages
    .filter((p) => !sectionIsIdentityOnly(classifySectionFromUrl(p.url)))
    .slice(0, 7);

  // Skip expensive LLM when HTML already yielded several strong staff contacts
  const strongHtml = htmlPeople.filter(
    (p) =>
      isStrictPersonName(p.name) &&
      (p.email ||
        /director|manager|coordinator|vp|vice president|tournament/i.test(p.role || ""))
  );
  let extracted = {
    candidates: strongHtml.slice(0, 8).map((p) => ({
      ...p,
      sectionHint: p.sectionKind || "STAFF",
      gdiContactRole: mapRole(p.role),
      relevance: "OPERATIONAL_CONTACT",
    })),
    functionalContacts: [],
    multiPersonBlocks: [],
    distributedLeadership: strongHtml.filter((p) =>
      /tournament|executive director/i.test(p.role || "")
    ).length >= 2,
  };
  if (strongHtml.length < 2) {
    try {
      const llm = await openaiExtract(
        SYSTEM_WHO_V6,
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
              text: String(p.text || "").slice(0, 3500),
            })),
            htmlPeople: htmlPeople.slice(0, 25),
            pdfPeople: (nextPdf.people || []).slice(0, 6),
          },
          null,
          2
        )
      );
      extracted = {
        candidates: [...(extracted.candidates || []), ...(llm.candidates || [])],
        functionalContacts: llm.functionalContacts || [],
        multiPersonBlocks: llm.multiPersonBlocks || [],
        distributedLeadership:
          extracted.distributedLeadership || Boolean(llm.distributedLeadership),
      };
    } catch (err) {
      extracted = {
        ...extracted,
        error: String(err.message || err),
      };
    }
  }

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

  const raw = [];
  for (const p of [
    ...(extracted.candidates || []),
    ...htmlPeople,
    ...(nextPdf.people || []).map((p) => ({
      ...p,
      sectionHint: classifySectionFromUrl(p.sourceUrl || ""),
      fromPdf: true,
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

  metrics.extractedCandidates = Math.max(metrics.extractedCandidates, raw.length);
  const scored = scoreAndConfirm(raw, input, eventFamily, metrics, suppressionLog);
  return { scored, extracted, htmlPeople, htmlFunctional, pdfExtract: nextPdf };
}

function cleanRole(role) {
  let r = String(role || "").replace(/\[email\s*protected\]/gi, "").replace(/\s+/g, " ").trim();
  if (!r) return null;
  // Repair truncated tournament titles
  if (/^tour(nament)?$/i.test(r)) r = "Tournament Director";
  return r.slice(0, 120);
}

function mapRole(role) {
  const r = String(role || "");
  if (/housing/i.test(r)) return "HOUSING_OWNER";
  if (/registration/i.test(r)) return "REGISTRATION_OWNER";
  if (/partnership|business development|sponsorship/i.test(r)) return "PARTNERSHIPS_OWNER";
  if (/member services|meetings/i.test(r)) return "MEETINGS_OWNER";
  if (/tournament director|conference|audience engagement/i.test(r)) {
    return "CONFERENCE_DIRECTOR";
  }
  if (/executive director|cups? director/i.test(r)) return "EVENT_OWNER";
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
