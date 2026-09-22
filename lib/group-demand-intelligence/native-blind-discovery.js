/**
 * Production Native blind GDI discovery.
 * Blind = hotel + market + discovery contract only. No Webhound/Parallel/curated seeds.
 *
 * Pipeline: contract → vertical tasks → SerpAPI → page fetch → OpenAI JSON extract
 * → candidate rows → (caller) normalize/dedupe/gate.
 */

import {
  buildGdiDiscoveryContract,
  buildDiscoverySearchTasks,
  renderDiscoveryContractBrief,
} from "./discovery-contract.js";
import { serpapiSearch } from "../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../hotel-intelligence/room-count-research/fetch.js";
import { normalizeVenueStatus } from "./provider-normalization.js";
import { selectQueriesStratified } from "./discovery-recall-v4.js";
import { enrichCommercialEvidenceV4 } from "./commercial-evidence-v4.js";

export const NATIVE_BLIND_DISCOVERY_VERSION = "gdi_native_blind_discovery_v1";
export const NATIVE_BLIND_DISCOVERY_VERSION_V4 = "gdi_native_blind_discovery_v4";

const EXTRACT_MODEL =
  process.env.GDI_NATIVE_EXTRACT_MODEL ||
  process.env.OPENAI_MODEL ||
  "gpt-4o-mini";

function slugify(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}

function hasSerpKey() {
  return Boolean(String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim());
}

function hasOpenAiKey() {
  return Boolean(String(process.env.OPENAI_API_KEY || "").trim());
}

async function callOpenAiJson(system, user, { maxTokens = 3500 } = {}) {
  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: EXTRACT_MODEL,
      temperature: 0.1,
      max_tokens: maxTokens,
      response_format: { type: "json_object" },
      messages: [
        { role: "system", content: system },
        { role: "user", content: user },
      ],
    }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(json.error?.message || `openai_http_${res.status}`);
    err.status = res.status;
    throw err;
  }
  const content = json.choices?.[0]?.message?.content;
  if (!content) throw new Error("openai_empty_content");
  return {
    parsed: JSON.parse(content),
    usage: json.usage || {},
    model: json.model || EXTRACT_MODEL,
  };
}

/**
 * Map extracted candidate → Waterstone-compatible row for import/orchestrator.
 */
export function mapNativeCandidateRow(row, hotelId, index = 0) {
  const title = row.eventName || row.title || row.name;
  if (!title) return null;
  const sources = [];
  if (row.officialSource) {
    sources.push({
      title: row.officialSourceTitle || "Official source",
      url: row.officialSource,
      authority: "FIRST_PARTY",
    });
  }
  for (const s of row.supportingSources || row.sources || []) {
    if (!s) continue;
    if (typeof s === "string") sources.push({ url: s });
    else sources.push({ title: s.title, url: s.url, authority: s.authority || null });
  }
  const contact =
    row.primaryContactCandidate ||
    row.whoClues ||
    row.contactClues ||
    null;
  const contactObj =
    contact && typeof contact === "object"
      ? {
          name: contact.name || null,
          role: contact.role || contact.title || null,
          email: contact.email || null,
          phone: contact.phone || null,
          organization: contact.organization || null,
          sourceUrl: contact.sourceUrl || contact.url || null,
        }
      : typeof contact === "string"
        ? { name: contact, role: null, email: null, phone: null }
        : null;

  const base = {
    id: row.id || `gdi_opp_${slugify(title)}_${index}`,
    title,
    organizationName: row.organization || row.organizationName || null,
    opportunityType: row.opportunityType || row.eventType || "PRIMARY_PURSUIT",
    segment: row.segment || row.vertical || "Discovered",
    eventStartDate: row.startDate || row.eventStartDate || null,
    eventEndDate: row.endDate || row.eventEndDate || null,
    eventDateStatus: row.eventDateStatus || null,
    location: row.location || null,
    destinationStatus: row.location || row.geographyEvidence || null,
    venue: row.venue || null,
    // Canonicalize aliases (TBD / RFP / open) — raw "TBD" previously collapsed to UNKNOWN
    venueSourcingStatus: normalizeVenueStatus(
      row.sourcingStatus || row.venueSourcingStatus || row.venueStatus || "UNKNOWN"
    ),
    sourcingStatus: row.sourcingStatus || null,
    venueStatus: row.venueStatus || null,
    roomDemandStatus: /overflow|housing.?program|official.?hotel/i.test(
      String(row.roomDemandEvidence || row.housingEvidence || "")
    )
      ? /overflow/i.test(String(row.roomDemandEvidence || row.housingEvidence || ""))
        ? "OVERFLOW_ONLY"
        : "VERIFIED_HOUSING_PROGRAM"
      : row.roomDemandEvidence
        ? "ESTIMATED_ROOM_DEMAND"
        : row.peakRoomEstimate != null
          ? "ESTIMATED_ROOM_DEMAND"
          : "UNKNOWN",
    estimatedPeakRooms: row.peakRoomEstimate ?? row.estimatedPeakRooms ?? null,
    peakRoomsStatus: row.peakRoomsStatus || null,
    estimatedAttendance: row.attendance ?? row.estimatedAttendance ?? null,
    attendanceStatus: row.attendanceStatus || null,
    housingEvidence: row.housingEvidence || row.roomDemandEvidence || null,
    hotelDemandThesis: row.hotelDemandThesis || row.whyRelevantToHotel || null,
    demandTerritoryFit: row.demandTerritoryFit || null,
    hotelWinThesis: row.whyRelevantToHotel || row.winThesis || null,
    whyNow: row.whyNow || row.futureCycleEvidence || null,
    recommendedAction: row.recommendedAction || null,
    suggestedAction: row.suggestedAction || row.recommendedAction || null,
    primaryContactCandidate: contactObj,
    evidenceSources: sources,
    evidenceConfidence: row.evidenceConfidence ?? null,
    claimKinds: row.claimKinds || null,
    officialSource: row.officialSource || null,
    researchProvider: "native",
    discoverySource: "gdi_native_blind_discovery_v1",
    vertical: row.vertical || null,
    localAttendanceOnly: row.localAttendanceOnly || false,
    eventSeriesHint: row.eventSeriesHint || null,
    subEventHint: row.subEventHint || null,
    evidenceYears: row.evidenceYears || null,
    jsonLdDetected: Boolean(row.jsonLdDetected),
  };
  return enrichCommercialEvidenceV4(base);
}

async function runSerpQuery(query, { num = 8, hl = "en", gl = "us" } = {}) {
  const result = await serpapiSearch({
    engine: "google",
    q: query,
    num,
    hl,
    gl,
  });
  if (!result.ok) {
    return { ok: false, error: result.error, organic: [], query, hl, gl };
  }
  const organic = (result.data?.organic_results || []).map((r) => ({
    title: r.title || null,
    url: r.link || r.url || null,
    snippet: r.snippet || null,
  }));
  return { ok: true, organic, query, charged: result.charged ?? 1, hl, gl };
}

async function fetchPageSnippets(urls, { maxPages = 3, maxChars = 6000 } = {}) {
  const out = [];
  for (const url of urls.slice(0, maxPages)) {
    if (!url || !/^https?:\/\//i.test(url)) continue;
    const page = await fetchResearchPage(url, { timeoutMs: 20000 });
    if (!page.ok) {
      out.push({ url, ok: false, error: page.error || `status_${page.status}` });
      continue;
    }
    const text = htmlToSearchableText(page.text).replace(/\s+/g, " ").trim();
    out.push({
      url: page.url || url,
      ok: true,
      text: text.slice(0, maxChars),
    });
  }
  return out;
}

const SYSTEM_EXTRACT = `You are Dealality Native group-demand research extraction.
Extract ONLY concrete group-demand opportunities supported by the provided search results and page text.
Rules:
- Prefer first-party / official event, association, organizer, tournament, or institutional sources.
- Do not invent events not supported by the evidence.
- Label uncertain numeric fields as estimates in claimKinds.
- Missing contact email is OK — still return the opportunity.
- Include future cycles (save-the-date, RFP, destination announced) when evidenced.
- Return JSON: { "candidates": [ { ...fields } ] }
Candidate fields (omit if unsupported): eventName, organization, eventType, opportunityType, startDate, endDate, location, venue, venueStatus, sourcingStatus, attendance, peakRoomEstimate, roomDemandEvidence, housingEvidence, officialSource, supportingSources (array of {url,title}), geographyEvidence, futureCycleEvidence, whoClues ({name,role,organization,email,phone,sourceUrl}), whyRelevantToHotel, whyNow, recommendedAction, evidenceConfidence (0-100), claimKinds, demandTerritoryFit, vertical, segment.`;

/**
 * Run Native blind discovery for one hotel.
 */
export async function runNativeBlindDiscovery({
  hotelId,
  profile = null,
  config = null,
  dryRun = false,
  maxQueries = 10,
  maxPagesPerQuery = 2,
  maxExtractBatches = 4,
  /** Optional overrides for Discovery Recall V1/V4 (does not change default V1 path). */
  searchTasksOverride = null,
  briefOverride = null,
  extractSystemOverride = null,
  discoverySourceLabel = null,
  /** V4: stratified query budget (default true when label is V4). */
  stratifiedQueryBudget = null,
  /** V4: default SERP locale */
  serpLocale = null,
  /** Per-query locale map: query string → { hl, gl } */
  queryLocaleMap = null,
} = {}) {
  const startedAt = new Date().toISOString();
  const contract = buildGdiDiscoveryContract({ hotelId, profile, config });
  const tasks = Array.isArray(searchTasksOverride)
    ? searchTasksOverride
    : buildDiscoverySearchTasks(contract);
  const brief =
    typeof briefOverride === "string" && briefOverride.trim()
      ? briefOverride
      : renderDiscoveryContractBrief(contract);
  const extractSystem =
    typeof extractSystemOverride === "string" && extractSystemOverride.trim()
      ? extractSystemOverride
      : SYSTEM_EXTRACT;
  const useStratified =
    stratifiedQueryBudget === true ||
    (stratifiedQueryBudget == null &&
      String(discoverySourceLabel || "").includes("recall_v4"));

  const ledger = {
    serpQueries: 0,
    serpCharged: 0,
    pagesFetched: 0,
    pagesFailed: 0,
    openaiCalls: 0,
    extractBatches: [],
    serpDigest: [],
    errors: [],
    querySelection: useStratified ? "stratified" : "flat_slice",
    serpLocaleDefault: serpLocale || { hl: "en", gl: "us" },
  };

  if (dryRun) {
    return {
      version: discoverySourceLabel || NATIVE_BLIND_DISCOVERY_VERSION,
      hotelId,
      mode: "DRY_RUN",
      contract,
      tasks,
      brief,
      candidates: [],
      rows: [],
      ledger,
      startedAt,
      finishedAt: new Date().toISOString(),
    };
  }

  if (!hasSerpKey()) {
    const err = new Error("SERPAPI_KEY_required_for_native_gdi_discovery");
    err.code = "SERPAPI_KEY_required";
    throw err;
  }
  if (!hasOpenAiKey()) {
    const err = new Error("OPENAI_API_KEY_required_for_native_gdi_discovery");
    err.code = "OPENAI_API_KEY_required";
    throw err;
  }

  // Flatten queries with budget (stratified keeps later families alive)
  const queryPlan = [];
  for (const task of tasks) {
    const metas = Array.isArray(task.queryMeta) ? task.queryMeta : null;
    for (let qi = 0; qi < (task.queries || []).length; qi += 1) {
      const q = task.queries[qi];
      const meta = metas?.[qi] || null;
      queryPlan.push({
        vertical: task.vertical,
        query: typeof q === "string" ? q : q?.q || String(q),
        note: task.note,
        hl: meta?.hl || null,
        gl: meta?.gl || null,
        lang: meta?.lang || null,
      });
    }
  }
  const selectedQueries = useStratified
    ? selectQueriesStratified(queryPlan, maxQueries)
    : queryPlan.slice(0, maxQueries);

  const corpora = [];
  for (const item of selectedQueries) {
    try {
      const mappedLocale = queryLocaleMap?.[item.query] || null;
      const hl =
        item.hl || mappedLocale?.hl || serpLocale?.hl || "en";
      const gl =
        item.gl || mappedLocale?.gl || serpLocale?.gl || "us";
      const serp = await runSerpQuery(item.query, { hl, gl });
      ledger.serpQueries += 1;
      ledger.serpCharged += serp.charged || 0;
      if (!serp.ok) {
        ledger.errors.push({ stage: "serp", query: item.query, error: serp.error });
        continue;
      }
      const urls = serp.organic.map((o) => o.url).filter(Boolean);
      ledger.serpDigest.push({
        query: item.query,
        vertical: item.vertical,
        hl,
        gl,
        urlCount: urls.length,
        topTitles: (serp.organic || []).slice(0, 3).map((o) => o.title),
      });
      const pages = await fetchPageSnippets(urls, {
        maxPages: maxPagesPerQuery,
      });
      ledger.pagesFetched += pages.filter((p) => p.ok).length;
      ledger.pagesFailed += pages.filter((p) => !p.ok).length;
      corpora.push({
        vertical: item.vertical,
        query: item.query,
        hl,
        gl,
        organic: serp.organic,
        pages: pages.filter((p) => p.ok),
        pageFailures: pages.filter((p) => !p.ok),
      });
    } catch (err) {
      ledger.errors.push({
        stage: "search_fetch",
        query: item.query,
        error: String(err?.message || err).slice(0, 300),
      });
    }
  }

  // Batch extract
  const allCandidates = [];
  const batchSize = Math.ceil(corpora.length / Math.max(1, maxExtractBatches)) || 1;
  for (let i = 0; i < corpora.length; i += batchSize) {
    const batch = corpora.slice(i, i + batchSize);
    const evidenceBlob = batch
      .map((b) => {
        const serpLines = (b.organic || [])
          .slice(0, 6)
          .map((o) => `- ${o.title}\n  ${o.url}\n  ${o.snippet || ""}`)
          .join("\n");
        const pageLines = (b.pages || [])
          .map((p) => `URL: ${p.url}\nTEXT: ${p.text}`)
          .join("\n\n");
        return `VERTICAL: ${b.vertical}\nQUERY: ${b.query}\nLOCALE: hl=${b.hl} gl=${b.gl}\nSEARCH HITS:\n${serpLines}\n\nPAGE EXTRACTS:\n${pageLines}`;
      })
      .join("\n\n====\n\n")
      .slice(0, 90000);

    if (!evidenceBlob.trim()) continue;

    try {
      const { parsed, usage } = await callOpenAiJson(
        extractSystem,
        `${brief}\n\n--- RESEARCH EVIDENCE ---\n${evidenceBlob}`
      );
      ledger.openaiCalls += 1;
      ledger.openaiUsage = ledger.openaiUsage || [];
      ledger.openaiUsage.push(usage);
      const found = Array.isArray(parsed?.candidates) ? parsed.candidates : [];
      ledger.extractBatches.push({
        batchIndex: Math.floor(i / batchSize),
        candidatesReturned: found.length,
        completionTokens: usage?.completion_tokens ?? null,
        empty: found.length === 0,
      });
      for (const c of found) {
        if (!c.vertical && batch[0]?.vertical) c.vertical = batch[0].vertical;
        allCandidates.push(c);
      }
    } catch (err) {
      ledger.errors.push({
        stage: "extract",
        error: String(err?.message || err).slice(0, 300),
      });
      ledger.extractBatches.push({
        batchIndex: Math.floor(i / batchSize),
        candidatesReturned: 0,
        empty: true,
        error: String(err?.message || err).slice(0, 200),
      });
    }
  }

  const rows = [];
  for (let i = 0; i < allCandidates.length; i += 1) {
    const mapped = mapNativeCandidateRow(allCandidates[i], hotelId, i);
    if (mapped) {
      if (discoverySourceLabel) mapped.discoverySource = discoverySourceLabel;
      rows.push(mapped);
    }
  }

  return {
    version: discoverySourceLabel || NATIVE_BLIND_DISCOVERY_VERSION,
    hotelId,
    mode: "LIVE",
    blind: true,
    contamination: {
      webhound: false,
      parallel: false,
      curatedSeeds: false,
    },
    contract,
    tasks: selectedQueries,
    brief,
    candidates: allCandidates,
    rows,
    discoveryCount: rows.length,
    ledger,
    startedAt,
    finishedAt: new Date().toISOString(),
  };
}
