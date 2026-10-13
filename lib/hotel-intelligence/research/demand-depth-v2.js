/**
 * HI Evidence Depth V2 — Demand nodes secondary ladder.
 * Commercially meaningful generators only — not nearby restaurants/shops.
 */

import {
  HI_JEV_ACTIONS,
  RESEARCH_DEPTH,
  SOURCE_FAMILY,
  classifySourceFamily,
  mayDeclareResearchedEmpty,
  researchDepthToDomainStatus,
  resolveResearchDepth,
} from "./evidence-depth-v2.js";
import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  demandNodeDedupeKey,
  evidenceDedupeKey,
  HOTEL_INTELLIGENCE_SCHEMA_VERSION,
  VAL_DEMAND_NODE_TYPE,
} from "../schema/hotel-intelligence-schema-v1.js";
import { applyHotelIntelligencePacket } from "../schema/hi-airtable-store.js";
import { buildHotelIntelligenceProfile } from "../adp-attributes/build-hotel-intelligence-profile.js";
import { buildAdpHotelAttributes } from "../adp-attributes/build-adp-hotel-attributes.js";
import { syncHotelAdpAttributesToAirtable } from "../adp-attributes/airtable-store.js";
import { upsertDomainStatus } from "../onboarding/domain-status-store.js";
import { HI_DOMAIN, HI_DOMAIN_STATUS } from "../onboarding/domain-status-v1.js";

const NODE_TYPE_PATTERNS = [
  { type: "Convention / Conference", re: /convention\s*cent|centro\s*de\s*convenciones|congress\s*cent/i },
  { type: "Airport", re: /\bairport\b|\baeropuerto\b/i },
  { type: "University", re: /\buniversity\b|\buniversidad\b|\bcollege\b/i },
  { type: "Medical / Healthcare", re: /\bhospital\b|\bmedical\s*cent|\bclinic\b/i },
  { type: "Government", re: /\bcapitol\b|\bgovernment\b|\bministry\b|\bpalacio\b|\bembassy\b/i },
  { type: "Corporate", re: /\bheadquarters\b|\bcampus\b|\boffice\s*park\b|\bfinancial\s*district\b/i },
  { type: "Sports", re: /\bstadium\b|\barena\b|\bestadio\b/i },
  { type: "Entertainment", re: /\btheater\b|\btheatre\b|\bmuseum\b|\bbroadway\b|\bcruise\s*terminal\b/i },
  { type: "Tourism", re: /\bnational\s*park\b|\bhistoric\s*district\b|\bold\s*city\b|\bzona\s*colonial\b/i },
];

const REJECT_RE =
  /\b(restaurant|cafe|café|bar|shop|store|mall|pharmacy|gas\s*station|parking)\b/i;

async function fetchText(url, { timeoutMs = 20000 } = {}) {
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);
    const res = await fetch(url, {
      signal: ctrl.signal,
      redirect: "follow",
      headers: {
        "User-Agent": "DealalityHotelIntelligence/1.0 (+evidence-depth-v2)",
        Accept: "text/html,application/xhtml+xml",
      },
    });
    clearTimeout(t);
    const text = await res.text();
    return { ok: res.ok, status: res.status, url: res.url || url, text: text.slice(0, 400_000) };
  } catch (err) {
    return { ok: false, status: 0, url, error: err.message || String(err), text: "" };
  }
}

function classifyNodeType(name, snippet = "") {
  const blob = `${name} ${snippet}`;
  if (REJECT_RE.test(blob) && !/convention|university|hospital|airport|stadium/i.test(blob)) {
    return null;
  }
  for (const p of NODE_TYPE_PATTERNS) {
    if (p.re.test(blob)) return p.type;
  }
  return null;
}

export function decideDemandNextAction(ctx = {}) {
  const attempted = new Set((ctx.sourcesAttempted || []).map((s) => s.family));
  if (!attempted.has(SOURCE_FAMILY.FIRST_PARTY)) {
    return {
      action: HI_JEV_ACTIONS.FIND_DEMAND_GENERATOR_SOURCE,
      rationale: "Search first-party location / nearby demand cues",
      queryHint: `"${ctx.hotelName}" (near OR nearby OR location OR "walking distance") (convention OR airport OR university OR hospital OR downtown)`,
    };
  }
  if (!attempted.has(SOURCE_FAMILY.CVB) && !attempted.has(SOURCE_FAMILY.TOURISM_AUTHORITY)) {
    return {
      action: HI_JEV_ACTIONS.FIND_DEMAND_GENERATOR_SOURCE,
      rationale: "Search CVB / tourism demand generators for market",
      queryHint: `"${ctx.city || ctx.market || ""}" (CVB OR "convention bureau" OR turismo OR "visitor bureau") (convention OR airport OR university)`,
      sourceFamily: SOURCE_FAMILY.CVB,
    };
  }
  return {
    action: HI_JEV_ACTIONS.STOP_PUBLIC_DATA_CEILING,
    rationale: "Bounded demand ladder exhausted without supportable commercial nodes",
  };
}

function stageEvidence(hpcHotelId, fieldName, value, source) {
  return {
    evidenceId: evidenceDedupeKey(hpcHotelId, "Demand Node", fieldName, source.url, value),
    hpcHotelId,
    entityType: "Demand Node",
    fieldName,
    valueObserved: value,
    sourceName: source.name,
    sourceType: "Public Research",
    sourceUrl: source.url,
    retrievedAt: new Date().toISOString(),
    evidenceSnippet: source.snippet || null,
    confidence: source.confidence || "MEDIUM",
    evidenceStrength: "MODERATE",
    current: true,
    schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
  };
}

export async function researchDemandDepthV2(hpcHotelId, opts = {}) {
  const mode = opts.mode || "dry-run";
  const persistLedger = mode === "apply" || opts.persistDomainStatus === true;
  const budget = { queries: opts.maxQueries ?? 2, fetches: opts.maxFetches ?? 4, jev: opts.maxJev ?? 1 };
  const ledger = {
    queries: 0,
    fetches: 0,
    jevCalls: 0,
    jevMaterial: 0,
    sourcesAttempted: [],
    sourceFamilyYield: {},
  };

  const profile = await buildHotelIntelligenceProfile(hpcHotelId, {
    skipLiveHpc: opts.skipLiveHpc === true,
  });
  if (!profile.ok) return { ok: false, error: profile.error, hotelId: hpcHotelId };

  const id = profile.identity;
  const hotelName = id.hotelName;
  const city = id.city || profile.commercialProfile?.market || "";
  const market = profile.commercialProfile?.market || city;
  const existing = (profile.demandNodes || []).filter((d) => d.fromHiAirtable || d.name);
  if (existing.length > 0 && opts.skipIfPopulated !== false) {
    return {
      ok: true,
      mode,
      hotelId: id.hpcHotelId,
      hotelName,
      populated: true,
      skipped: true,
      domainStatus: HI_DOMAIN_STATUS.POPULATED,
      reason: "already_populated",
      ledger,
    };
  }

  const track = (family, ok, meta = {}) => {
    ledger.sourcesAttempted.push({ family, ok, ...meta });
    ledger.sourceFamilyYield[family] = ledger.sourceFamilyYield[family] || {
      attempts: 0,
      successes: 0,
      facts: 0,
    };
    ledger.sourceFamilyYield[family].attempts += 1;
    if (ok) ledger.sourceFamilyYield[family].successes += 1;
  };

  // Mark first-party location attempt if official URL exists
  if (id.officialPropertyUrl) {
    if (budget.fetches > 0) {
      budget.fetches -= 1;
      ledger.fetches += 1;
      const page = await fetchText(id.officialPropertyUrl);
      track(SOURCE_FAMILY.FIRST_PARTY, page.ok, { url: page.url });
    }
  } else {
    track(SOURCE_FAMILY.FIRST_PARTY, false, { note: "no_official_url" });
  }

  let jev = null;
  if (budget.jev > 0) {
    budget.jev -= 1;
    ledger.jevCalls += 1;
    jev = decideDemandNextAction({
      hotelName,
      city,
      market,
      sourcesAttempted: ledger.sourcesAttempted,
    });
  }

  const proposedNodes = [];
  const evidence = [];
  let providerFailed = false;

  const runQuery = async (q) => {
    if ((!process.env.SERPAPI_KEY && !process.env.SERPAPI_API_KEY) || budget.queries <= 0) {
      if (!process.env.SERPAPI_KEY && !process.env.SERPAPI_API_KEY) providerFailed = true;
      return [];
    }
    budget.queries -= 1;
    ledger.queries += 1;
    try {
      const serp = await serpapiSearch({ engine: "google", q, num: 8, hl: "en" });
      if (!serp.ok) return [];
      return (serp.data?.organic_results || []).map((h) => ({
        title: h.title,
        url: h.link || h.url,
        snippet: h.snippet || "",
      }));
    } catch {
      return [];
    }
  };

  const queries = [
    jev?.queryHint ||
      `"${hotelName}" ${city} (convention center OR airport OR university OR hospital OR stadium)`,
    `"${city || market}" (convention center OR "visitor bureau" OR CVB OR aeropuerto OR universidad)`,
  ].slice(0, 2);

  for (const q of queries) {
    if (budget.queries <= 0) break;
    const hits = await runQuery(q);
    for (const hit of hits.slice(0, 6)) {
      const nodeType = classifyNodeType(hit.title, hit.snippet);
      if (!nodeType || !VAL_DEMAND_NODE_TYPE.includes(nodeType)) continue;
      const name = String(hit.title || "")
        .replace(/\s*[|\-–].*$/, "")
        .trim()
        .slice(0, 120);
      if (!name || name.length < 4) continue;
      if (REJECT_RE.test(name)) continue;
      const family = classifySourceFamily(hit.url, hit.title);
      // Skip weak OTA marketing as sole identity
      if (family === SOURCE_FAMILY.OTHER && /tripadvisor|booking|expedia|trip\.com/i.test(hit.url || "")) {
        continue;
      }
      track(family, true, { url: hit.url, title: hit.title });
      if (jev) ledger.jevMaterial += 1;
      proposedNodes.push({
        nodeKey: demandNodeDedupeKey(id.hpcHotelId, name, nodeType),
        hpcHotelId: id.hpcHotelId,
        demandNodeName: name,
        demandNodeType: nodeType,
        city: city || null,
        whyRelevant: `Commercial demand generator near ${hotelName} (${nodeType})`,
        sourceUrl: hit.url,
        sourceName: family,
        sourceType: "Public Research",
        lastVerifiedAt: new Date().toISOString(),
        confidence: family === SOURCE_FAMILY.CVB || family === SOURCE_FAMILY.TOURISM_AUTHORITY ? "HIGH" : "MEDIUM",
        active: true,
        schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
      });
      evidence.push(
        stageEvidence(id.hpcHotelId, "Demand Node Name", name, {
          name: "Demand generator search",
          url: hit.url,
          snippet: hit.snippet?.slice(0, 180),
          confidence: "MEDIUM",
        })
      );
      ledger.sourceFamilyYield[family] = ledger.sourceFamilyYield[family] || {
        attempts: 0,
        successes: 0,
        facts: 0,
      };
      ledger.sourceFamilyYield[family].facts += 1;
      if (proposedNodes.length >= 4) break;
    }
    if (proposedNodes.length >= 4) break;
  }

  // Deduplicate by nodeKey
  const byKey = new Map();
  for (const n of proposedNodes) {
    if (!byKey.has(n.nodeKey)) byKey.set(n.nodeKey, n);
  }
  const uniqueNodes = [...byKey.values()];
  const populated = uniqueNodes.length > 0;

  // Optional fetch of one CVB page for provenance strengthen
  if (!populated && budget.fetches > 0 && jev?.queryHint) {
    // already searched; mark broader attempt
  }

  const ladderExhausted =
    mayDeclareResearchedEmpty(ledger.sourcesAttempted) ||
    (ledger.queries >= 2 && ledger.sourcesAttempted.length >= 2);

  const researchDepth = resolveResearchDepth({
    populated,
    sourcesAttempted: ledger.sourcesAttempted,
    providerFailed,
    ladderExhausted: ladderExhausted && !populated,
  });

  let domainStatus = researchDepthToDomainStatus(researchDepth);
  if (populated) domainStatus = HI_DOMAIN_STATUS.POPULATED;
  else if (ladderExhausted && mayDeclareResearchedEmpty(ledger.sourcesAttempted)) {
    domainStatus = HI_DOMAIN_STATUS.PUBLIC_DATA_CEILING;
  } else if (ladderExhausted || ledger.sourcesAttempted.length > 0) {
    // Depth-v2 attempt must not regress to NOT_RESEARCHED
    domainStatus = HI_DOMAIN_STATUS.PUBLIC_DATA_CEILING;
  }

  let applyResult = null;
  let adpSync = null;
  if (mode === "apply" && populated) {
    applyResult = await applyHotelIntelligencePacket(
      { commercial: null, eventSpaces: [], demandNodes: uniqueNodes, seasonality: [], evidence },
      { dryRun: false }
    );
    const attrs = await buildAdpHotelAttributes(id.hpcHotelId);
    adpSync = await syncHotelAdpAttributesToAirtable(attrs, { dryRun: false });
  }

  if (persistLedger) {
    upsertDomainStatus(id.hpcHotelId, HI_DOMAIN.DEMAND_NODES, {
      domainStatus,
      researchDepth,
      lastResearchedAt: new Date().toISOString(),
      researchRunId: opts.researchRunId || `hi_demand_depth_v2_${Date.now()}`,
      evidenceCount: evidence.length,
      rowCount: uniqueNodes.length,
      sourceCoverage: ledger.sourcesAttempted.map((s) => s.family),
      sourcesAttempted: ledger.sourcesAttempted,
      notes: populated
        ? `evidence_depth_v2_demand_populated depth=${researchDepth}`
        : `evidence_depth_v2_demand_ceiling depth=${researchDepth}`,
      blocker: populated ? null : domainStatus,
    });
  }

  return {
    ok: true,
    mode,
    hotelId: id.hpcHotelId,
    hotelName,
    populated,
    domainStatus,
    researchDepth,
    jev,
    ledger,
    discovered: { demandNodes: uniqueNodes },
    evidence,
    applyResult,
    adpSync,
  };
}
