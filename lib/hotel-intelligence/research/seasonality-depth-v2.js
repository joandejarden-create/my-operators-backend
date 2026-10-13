/**
 * HI Evidence Depth V2 — Seasonality / need periods ladder.
 * DESTINATION seasonality ≠ hotel need period.
 * Do not invent hotel-specific occupancy or need periods.
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
  evidenceDedupeKey,
  HOTEL_INTELLIGENCE_SCHEMA_VERSION,
  seasonalityDedupeKey,
} from "../schema/hotel-intelligence-schema-v1.js";
import { applyHotelIntelligencePacket } from "../schema/hi-airtable-store.js";
import { buildHotelIntelligenceProfile } from "../adp-attributes/build-hotel-intelligence-profile.js";
import { upsertDomainStatus } from "../onboarding/domain-status-store.js";
import { HI_DOMAIN, HI_DOMAIN_STATUS } from "../onboarding/domain-status-v1.js";

const SEASON_PATTERNS = [
  {
    label: "High / peak season",
    re: /high\s*season|peak\s*season|temporada\s*alta|busy\s*season/i,
    priority: "High",
  },
  {
    label: "Low / off season",
    re: /low\s*season|off[- ]?season|temporada\s*baja|quiet\s*season/i,
    priority: "Low",
  },
  {
    label: "Shoulder season",
    re: /shoulder\s*season|temporada\s*media/i,
    priority: "Medium",
  },
  {
    label: "Hurricane / rainy season",
    re: /hurricane\s*season|rainy\s*season|temporada\s*de\s*lluvias|wet\s*season/i,
    priority: "Medium",
  },
  {
    label: "Dry season",
    re: /dry\s*season|temporada\s*seca/i,
    priority: "High",
  },
];

const MONTH_RANGE_RE =
  /(january|february|march|april|may|june|july|august|september|october|november|december|enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)[^\n.]{0,40}(january|february|march|april|may|june|july|august|september|october|november|december|enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)/i;

function monthToMd(m) {
  const map = {
    january: "01-01",
    febrero: "02-01",
    february: "02-01",
    marzo: "03-01",
    march: "03-01",
    abril: "04-01",
    april: "04-01",
    mayo: "05-01",
    may: "05-01",
    junio: "06-01",
    june: "06-01",
    julio: "07-01",
    july: "07-01",
    agosto: "08-01",
    august: "08-01",
    septiembre: "09-01",
    september: "09-01",
    octubre: "10-01",
    october: "10-01",
    noviembre: "11-01",
    november: "11-01",
    diciembre: "12-01",
    december: "12-01",
    enero: "01-01",
  };
  return map[String(m || "").toLowerCase()] || null;
}

export function decideSeasonalityNextAction(ctx = {}) {
  const attempted = new Set((ctx.sourcesAttempted || []).map((s) => s.family));
  if (!attempted.has(SOURCE_FAMILY.TOURISM_AUTHORITY) && !attempted.has(SOURCE_FAMILY.CVB)) {
    return {
      action: HI_JEV_ACTIONS.FIND_DESTINATION_SEASONALITY_SOURCE,
      rationale: "Search official destination / CVB seasonality",
      queryHint: `"${ctx.city || ctx.market || ""}" (tourism OR CVB OR "visitor bureau" OR turismo) ("high season" OR "peak season" OR "low season" OR "temporada alta")`,
    };
  }
  return {
    action: HI_JEV_ACTIONS.STOP_PUBLIC_DATA_CEILING,
    rationale: "Bounded destination seasonality ladder exhausted",
  };
}

export async function researchSeasonalityDepthV2(hpcHotelId, opts = {}) {
  const mode = opts.mode || "dry-run";
  const persistLedger = mode === "apply" || opts.persistDomainStatus === true;
  const budget = { queries: opts.maxQueries ?? 2, fetches: opts.maxFetches ?? 3, jev: opts.maxJev ?? 1 };
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
  const existing = (profile.seasonality || []).length + (profile.needPeriods || []).length;
  if (existing > 0 && opts.skipIfPopulated !== false) {
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

  // First-party attempt
  track(SOURCE_FAMILY.FIRST_PARTY, Boolean(id.officialPropertyUrl), {
    url: id.officialPropertyUrl || null,
    note: "seasonality_first_party_probe",
  });

  let jev = null;
  if (budget.jev > 0) {
    budget.jev -= 1;
    ledger.jevCalls += 1;
    jev = decideSeasonalityNextAction({
      hotelName,
      city,
      market,
      sourcesAttempted: ledger.sourcesAttempted,
    });
  }

  const periods = [];
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

  const q =
    jev?.queryHint ||
    `"${city || market}" (tourism OR turismo) ("high season" OR "peak season" OR "low season" OR "temporada alta")`;
  const hits = await runQuery(q);

  for (const hit of hits.slice(0, 6)) {
    const family = classifySourceFamily(hit.url, hit.title);
    const blob = `${hit.title} ${hit.snippet}`;
    let matched = false;
    for (const pat of SEASON_PATTERNS) {
      if (!pat.re.test(blob)) continue;
      matched = true;
      const range = blob.match(MONTH_RANGE_RE);
      let start = null;
      let end = null;
      if (range) {
        start = monthToMd(range[1]);
        end = monthToMd(range[2]);
      }
      const periodType = "Public Seasonality"; // DESTINATION — not hotel need period
      periods.push({
        periodKey: seasonalityDedupeKey(
          id.hpcHotelId,
          periodType,
          start || pat.label,
          end || "annual",
          "destination"
        ),
        hpcHotelId: id.hpcHotelId,
        periodType,
        startMonthDay: start,
        endMonthDay: end,
        seasonLabel: pat.label,
        priority: pat.priority,
        segment: "All",
        description: `Destination seasonality (not hotel need period): ${hit.snippet.slice(0, 240)}`,
        sourceUrl: hit.url,
        sourceName: family,
        hotelSupplied: false,
        lastVerifiedAt: new Date().toISOString(),
        confidence:
          family === SOURCE_FAMILY.CVB || family === SOURCE_FAMILY.TOURISM_AUTHORITY
            ? "HIGH"
            : "MEDIUM",
        active: true,
        schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
        notes: "inference_state=destination_seasonality_not_hotel_need_period",
      });
      evidence.push({
        evidenceId: evidenceDedupeKey(
          id.hpcHotelId,
          "Seasonality",
          "Season Label",
          hit.url,
          pat.label
        ),
        hpcHotelId: id.hpcHotelId,
        entityType: "Seasonality",
        fieldName: "Season Label",
        valueObserved: pat.label,
        sourceName: "Destination seasonality source",
        sourceType: "Public Research",
        sourceUrl: hit.url,
        retrievedAt: new Date().toISOString(),
        evidenceSnippet: hit.snippet.slice(0, 180),
        confidence: "MEDIUM",
        evidenceStrength: "MODERATE",
        current: true,
        schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
        notes: "DESTINATION_SEASONALITY",
      });
    }
    track(family, matched, { url: hit.url, title: hit.title });
    if (matched && jev) ledger.jevMaterial += 1;
    if (periods.length >= 3) break;
  }

  const byKey = new Map();
  for (const p of periods) {
    if (!byKey.has(p.periodKey)) byKey.set(p.periodKey, p);
  }
  const unique = [...byKey.values()];
  const populated = unique.length > 0;

  const ladderExhausted = ledger.queries >= 1 && ledger.sourcesAttempted.length >= 2;
  const researchDepth = resolveResearchDepth({
    populated,
    sourcesAttempted: ledger.sourcesAttempted,
    providerFailed,
    ladderExhausted: ladderExhausted && !populated,
  });

  let domainStatus = researchDepthToDomainStatus(researchDepth);
  if (populated) domainStatus = HI_DOMAIN_STATUS.POPULATED;
  else if (ladderExhausted) domainStatus = HI_DOMAIN_STATUS.PUBLIC_DATA_CEILING;
  else if (ledger.sourcesAttempted.length > 0) {
    // A depth-v2 run that attempted sources must not leave NOT_RESEARCHED
    domainStatus = HI_DOMAIN_STATUS.PUBLIC_DATA_CEILING;
  }

  let applyResult = null;
  if (mode === "apply" && populated) {
    applyResult = await applyHotelIntelligencePacket(
      { commercial: null, eventSpaces: [], demandNodes: [], seasonality: unique, evidence },
      { dryRun: false }
    );
  }

  if (persistLedger) {
    upsertDomainStatus(id.hpcHotelId, HI_DOMAIN.SEASONALITY_NEED_PERIODS, {
      domainStatus,
      researchDepth,
      lastResearchedAt: new Date().toISOString(),
      researchRunId: opts.researchRunId || `hi_seasonality_depth_v2_${Date.now()}`,
      evidenceCount: evidence.length,
      rowCount: unique.length,
      sourceCoverage: ledger.sourcesAttempted.map((s) => s.family),
      sourcesAttempted: ledger.sourcesAttempted,
      notes: populated
        ? `evidence_depth_v2_destination_seasonality_populated depth=${researchDepth}`
        : `evidence_depth_v2_seasonality_ceiling depth=${researchDepth}`,
      blocker: populated ? null : domainStatus,
      destinationVsNeedPeriod: populated
        ? "DESTINATION_SEASONALITY_ONLY"
        : "NO_SUPPORTABLE_PUBLIC_SEASONALITY",
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
    discovered: { seasonality: unique },
    evidence,
    applyResult,
    note: "Populated rows are DESTINATION seasonality, not hotel need periods",
  };
}
