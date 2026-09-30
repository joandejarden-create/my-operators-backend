/**
 * Santo Domingo market-first discovery runner V1.
 * Discover once at market level → evaluate JW + Radisson selectively.
 * No Webhound. No Surfe AUTO. No hotel-branched duplicate SERP.
 */

import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../hotel-intelligence/room-count-research/fetch.js";
import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import { fetchAndExtractPdf } from "../hidden-demand/extract-pdf.js";
import {
  classifyLodgingEvidenceFromText,
  classifyCommercialStatus,
  classifyProvenSourceFamily,
  isCommerciallyOpen,
  LODGING_EVIDENCE,
} from "../proven-source/proven-source-playbook-v1.js";
import {
  buildSantoDomingoGeographyProfiles,
} from "./hotel-geography-profile-v1.js";
import {
  santoDomingoMarketHints,
  buildSantoDomingoMarketQueries,
  isSantoDomingoInMarket,
  SANTO_DOMINGO_HOTELS,
  santoDomingoMarketDevelopmentState,
} from "./santo-domingo-market-geography-v1.js";
import { computeMarketOpportunityId } from "./market-geography-v1.js";
import { extractMarketOpportunityPacket } from "./market-opportunity-packet-v1.js";
import {
  evaluateHotelGeographicApplicability,
} from "./geographic-applicability-v1.js";
import { GEO_APPLICABILITY } from "./market-geography-v1.js";
import { evaluateCrossHotelFit } from "./cross-hotel-fit-v1.js";
import { decideSupportingDataNextAction } from "./jev-supporting-data-router-v1.js";
import { buildHotelOpportunityFromMarketPacket } from "./build-hotel-opportunity-from-market-v1.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";
process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED = "0";

function hasSerp() {
  return !!(process.env.SERPAPI_API_KEY || process.env.SERPAPI_KEY);
}

function extractTitle(html = "", fallback = "") {
  const m = String(html).match(/<title[^>]*>([^<]{5,140})<\/title>/i);
  if (m) return m[1].replace(/\s+/g, " ").trim();
  const h1 = String(html).match(/<h1[^>]*>([^<]{5,140})<\/h1>/i);
  if (h1) return h1[1].replace(/\s+/g, " ").trim();
  return String(fallback || "").slice(0, 120);
}

function extractOrgHint(title = "", text = "") {
  const blob = `${title} ${String(text).slice(0, 800)}`;
  const m = blob.match(
    /\b([A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑ&.\-']+(?:\s+[A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑ&.\-']+){0,6})\s+(?:Conference|Congress|Congreso|Tournament|Championship|Meeting|Summit|Forum|Foro|Symposium|Jornadas)/i
  );
  if (m) return m[1];
  return title.split(/[|\-–—:/]/)[0].trim().slice(0, 80) || null;
}

function extractYearDates(text = "") {
  const t = String(text || "");
  const years = [...t.matchAll(/\b(202[6-9])\b/g)].map((m) => m[1]);
  const uniqueYears = [...new Set(years)];
  const dateRange = t.match(
    /\b((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:\s*[-–]\s*\d{1,2})?,?\s*202[6-9]|\d{1,2}\s+(?:de\s+)?(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+(?:de\s+)?202[6-9]|202[6-9]-\d{2}-\d{2})/i
  );
  return {
    eventYear: uniqueYears[0] || null,
    years: uniqueYears,
    dateHint: dateRange ? dateRange[1] : null,
    futureCycle: uniqueYears.some((y) => Number(y) >= 2026),
  };
}

function inferDestination(text = "", title = "") {
  const blob = `${title} ${text}`.toLowerCase();
  // Venue → submarket anchors (evidence-supported commercial nodes)
  if (
    /\bpiantini\b|\bblue mall\b|\bwinston churchill\b|\bac[oó]polis\b|\bacropolis\b|\bcidac\b|\bensanche piantini\b/.test(
      blob
    )
  ) {
    return "Santo Domingo / Piantini";
  }
  if (
    /\bnaco\b|\btiradentes\b|\bpresidente gonz|\bav\.?\s*tiradentes\b|\bensanche naco\b/.test(
      blob
    )
  ) {
    return "Santo Domingo / Naco–Tiradentes";
  }
  if (/\bzona colonial\b|\bciudad colonial\b/.test(blob)) {
    return "Santo Domingo / Zona Colonial";
  }
  if (/\bbella vista\b/.test(blob)) return "Santo Domingo / Bella Vista";
  if (/\bmalec[oó]n\b|\bgeorge washington\b/.test(blob)) {
    return "Santo Domingo / Malecón";
  }
  if (/\bquisqueya\b/.test(blob)) {
    // Estadio Quisqueya sits near Ensanche La Fe / Naco corridor
    return "Santo Domingo / Naco–Tiradentes";
  }
  if (/\bsanto domingo\b|\bdistrito nacional\b/.test(blob)) {
    return "Santo Domingo";
  }
  return null;
}

function lodgingRelationshipFromEvidence(level, text = "") {
  const t = String(text || "");
  if (level === LODGING_EVIDENCE.DIRECT) {
    if (/host\s*hotel|hotel\s*oficial|official\s*hotel/i.test(t)) return "OFFICIAL_HOST_HOTEL";
    if (/room\s*block|hotel\s*block/i.test(t)) return "OFFICIAL_ROOM_BLOCK";
    if (/housing\s*bureau|alojamiento/i.test(t)) return "HOUSING_BUREAU";
    if (/overflow/i.test(t)) return "OVERFLOW_EVIDENCED";
    return "OFFICIAL_ACCOMMODATION_PROGRAM";
  }
  if (level === LODGING_EVIDENCE.STRONG_INFERENCE) {
    if (/hotel\s*tbd|venue\s*tbd|sede por determinar/i.test(t)) return "HOTEL_SELECTION_OPEN";
    return "VENUE_SET_HOTEL_OPEN";
  }
  if (level === LODGING_EVIDENCE.WEAK_INFERENCE) return "GENERIC_NEARBY_HOTELS";
  return "UNKNOWN";
}

function mapLodgingToBuilderEvidence(level, relationship) {
  if (level === LODGING_EVIDENCE.NONE || level === "UNKNOWN") return null;
  return {
    status: relationship || level,
    roomBlockMentioned: /ROOM_BLOCK|HOST_HOTEL|HOUSING|ACCOMMODATION|OVERFLOW/i.test(
      String(relationship || "")
    ),
    overflowMentioned: /OVERFLOW/i.test(String(relationship || "")),
    evidenceLevel: level,
  };
}

/**
 * Deterministic market-level next action (Jev-shaped nomination).
 */
export function decideMarketEvidenceNextAction(candidate = {}) {
  if (!candidate.validEntity) {
    return { action: "STOP_NO_FURTHER_EVIDENCE", rationale: "Invalid entity" };
  }
  if (!candidate.futureCycle) {
    return {
      action: "VERIFY_FUTURE_CYCLE",
      rationale: "Future cycle not confirmed",
      maxQueries: 3,
      maxFetches: 5,
    };
  }
  if (
    !candidate.venueHint ||
    candidate.venueHint === "Santo Domingo" ||
    /METRO_ONLY|UNKNOWN/i.test(candidate.geoConfidence || "")
  ) {
    return {
      action: "VERIFY_EVENT_LOCATION",
      rationale: "Submarket / venue precision missing — required before hotel fanout",
      maxQueries: 3,
      maxFetches: 5,
    };
  }
  if (!candidate.lodgingEvidence || candidate.lodgingEvidence === LODGING_EVIDENCE.NONE) {
    return {
      action: "FIND_OFFICIAL_HOUSING_PAGE",
      rationale: "Lodging evidence missing",
      maxQueries: 3,
      maxFetches: 5,
    };
  }
  if (
    !candidate.commercialStatus ||
    candidate.commercialStatus === "UNKNOWN" ||
    /UNKNOWN/i.test(String(candidate.commercialStatus))
  ) {
    return {
      action: "VERIFY_COMMERCIAL_STATUS",
      rationale: "Commercial openness unresolved",
      maxQueries: 3,
      maxFetches: 5,
    };
  }
  if (!candidate.whoName) {
    return {
      action: "VERIFY_WHO",
      rationale: "WHO not yet attempted at market level",
      maxQueries: 3,
      maxFetches: 5,
    };
  }
  return { action: "STOP_NO_FURTHER_EVIDENCE", rationale: "No material market gap" };
}

async function runBoundedFollowupSearch(query, budget) {
  if (!hasSerp() || budget.queries <= 0) return { hits: [], used: 0 };
  budget.queries -= 1;
  try {
    const serp = await serpapiSearch({
      engine: "google",
      q: query,
      num: 5,
      hl: "es",
      gl: "do",
    });
    return { hits: serp?.data?.organic_results || [], used: 1 };
  } catch {
    return { hits: [], used: 1 };
  }
}

/**
 * @param {object} opts
 */
export async function runSantoDomingoMarketFirstDiscovery(opts = {}) {
  const maxQueries = opts.maxQueries ?? 70;
  const maxFetches = opts.maxFetches ?? 140;
  const maxPdf = opts.maxPdfDocs ?? 30;
  const maxFollowups = opts.maxFollowups ?? 50;
  const maxJev = opts.maxJevCalls ?? 25;
  const nowDate = opts.nowDate || new Date().toISOString().slice(0, 10);
  const hints = santoDomingoMarketHints();
  const profiles = buildSantoDomingoGeographyProfiles();

  const ledger = {
    marketKey: "santo_domingo",
    queries: 0,
    fetches: 0,
    rendered: 0,
    pdfDocs: 0,
    followups: 0,
    jevMarket: 0,
    jevHotelPair: 0,
    jevResolved: 0,
    serpResultsInspected: 0,
    rejectedWrongMarket: 0,
    rejectedHistorical: 0,
    validEntities: 0,
    validFuturePrograms: 0,
    lodgingSupported: 0,
    openTbd: 0,
    hotelMatchable: 0,
    candidates: [],
    watch: [],
    usefulSources: [],
    errors: [],
  };

  const budget = {
    queries: maxQueries,
    fetches: maxFetches,
    pdf: maxPdf,
    followups: maxFollowups,
    jev: maxJev,
  };

  const queries = buildSantoDomingoMarketQueries({ max: Math.min(55, maxQueries) });
  const candidateUrls = [];
  const seenUrl = new Set();

  // ——— Phase A: market-level SERP (once) ———
  console.log(`[sd-discovery] SERP queries planned=${queries.length} budget soft-cap`);
  for (const q of queries) {
    if (budget.queries <= 0 || !hasSerp()) break;
    budget.queries -= 1;
    ledger.queries += 1;
    if (ledger.queries % 5 === 0) {
      console.log(`[sd-discovery] serp progress q=${ledger.queries} urls=${candidateUrls.length}`);
    }
    try {
      const serp = await Promise.race([
        serpapiSearch({
          engine: "google",
          q: q.query,
          num: 8,
          hl: "es",
          gl: "do",
        }),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error("serp_timeout")), 20000)
        ),
      ]);
      for (const hit of serp?.data?.organic_results || []) {
        ledger.serpResultsInspected += 1;
        const url = String(hit.link || hit.url || "").trim();
        if (!url || seenUrl.has(url)) continue;
        const blob = `${hit.title || ""} ${hit.snippet || ""} ${url}`;
        if (!isSantoDomingoInMarket(blob)) {
          ledger.rejectedWrongMarket += 1;
          continue;
        }
        if (/\b(201[0-9]|202[0-4])\b/.test(blob) && !/\b202[6-9]\b/.test(blob)) {
          ledger.rejectedHistorical += 1;
          continue;
        }
        seenUrl.add(url);
        candidateUrls.push({
          url,
          title: hit.title || "",
          snippet: hit.snippet || "",
          familyHint: q.family,
          queryId: q.queryId,
          discoveryProvider: "SERPAPI",
        });
      }
    } catch (err) {
      ledger.errors.push(`serp:${q.queryId}:${String(err?.message || err).slice(0, 120)}`);
    }
  }
  // Cap fetches — quality over exhaustive crawl
  const maxFetchTargets = Math.min(candidateUrls.length, 45);
  console.log(
    `[sd-discovery] SERP done queries=${ledger.queries} urls=${candidateUrls.length} fetchTargets=${maxFetchTargets}`
  );

  // ——— Phase B: DIRECT_HTTP / PDF fetch ———
  const rawCandidates = [];
  const fetchTargets = candidateUrls.slice(0, maxFetchTargets);
  for (let i = 0; i < fetchTargets.length; i++) {
    const src = fetchTargets[i];
    if (budget.fetches <= 0) break;
    if (i % 5 === 0) {
      console.log(`[sd-discovery] fetch ${i}/${fetchTargets.length} candidates=${rawCandidates.length}`);
    }
    const isPdf = /\.pdf(\?|$)/i.test(src.url);
    budget.fetches -= 1;
    ledger.fetches += 1;
    let text = "";
    let html = "";
    try {
      if (isPdf && budget.pdf > 0) {
        budget.pdf -= 1;
        ledger.pdfDocs += 1;
        const pdf = await Promise.race([
          fetchAndExtractPdf(src.url, { timeoutMs: 12000 }),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error("pdf_timeout")), 14000)
          ),
        ]);
        text = String(pdf?.text || "").slice(0, 40000);
      } else {
        const page = await Promise.race([
          fetchResearchPage(src.url, { timeoutMs: 10000 }),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error("http_timeout")), 12000)
          ),
        ]);
        html = page?.html || page?.body || "";
        text = htmlToSearchableText(html).slice(0, 40000);
      }
    } catch (err) {
      ledger.errors.push(`fetch:${String(err?.message || err).slice(0, 100)}`);
      continue;
    }

    const title = extractTitle(html, src.title);
    const blob = `${title} ${src.snippet} ${text.slice(0, 6000)}`;
    if (!isSantoDomingoInMarket(blob)) {
      ledger.rejectedWrongMarket += 1;
      continue;
    }

    const family = classifyProvenSourceFamily({
      url: src.url,
      title,
      snippet: src.snippet,
      text: blob,
    });
    const lodgingLevel = classifyLodgingEvidenceFromText(blob, src.url);
    const commercial = classifyCommercialStatus(blob);
    const timing = extractYearDates(blob);
    const org = extractOrgHint(title, text);
    const destination = inferDestination(blob, title);
    const lodgingRel = lodgingRelationshipFromEvidence(lodgingLevel, blob);
    const validEntity = Boolean(org || /congreso|conference|symposium|foro|torneo|meeting|jornadas/i.test(title));

    if (!validEntity) continue;
    ledger.validEntities += 1;
    if (timing.futureCycle) ledger.validFuturePrograms += 1;
    if (lodgingLevel !== LODGING_EVIDENCE.NONE) ledger.lodgingSupported += 1;
    if (isCommerciallyOpen(commercial)) ledger.openTbd += 1;

    const seedOpp = {
      title: title.slice(0, 160),
      organizationName: org,
      eventName: title.slice(0, 160),
      eventYear: timing.eventYear,
      eventStartDate: timing.dateHint && /^\d{4}-\d{2}-\d{2}/.test(timing.dateHint)
        ? timing.dateHint
        : null,
      destinationStatus: destination,
      venueStatus: /tbd|por determinar/i.test(blob) ? "HOTEL_TBD" : destination,
      officialSource: src.url,
      discoverySource: src.url,
      sources: [{ url: src.url, title, family, provider: src.discoveryProvider }],
      lodgingEvidence: mapLodgingToBuilderEvidence(lodgingLevel, lodgingRel),
      opportunityType:
        lodgingLevel === LODGING_EVIDENCE.DIRECT || lodgingLevel === LODGING_EVIDENCE.STRONG_INFERENCE
          ? "OVERFLOW_HOUSING"
          : "FUTURE_CYCLE",
      customerFacingState: isCommerciallyOpen(commercial) ? "WATCH" : "INTERNAL_ONLY",
      summaryWhat: `${org || title} — ${destination || "Santo Domingo"} (${timing.eventYear || "future"}). Source family ${family}.`,
      teamSupported: /congreso|conference|symposium|delegat|equipo|attendee/i.test(blob) || undefined,
    };

    const marketOpportunityId = computeMarketOpportunityId({
      organizationName: org,
      eventName: title,
      eventStartDate: seedOpp.eventStartDate,
      eventYear: timing.eventYear,
      venueOrLocation: destination,
    });

    const developmentState = santoDomingoMarketDevelopmentState({
      lodgingEvidence: lodgingLevel,
      commercialStatus: commercial,
      validEntity: true,
      futureCycle: timing.futureCycle,
    });
    if (developmentState === "HOTEL_MATCHABLE") ledger.hotelMatchable += 1;

    const packet = extractMarketOpportunityPacket(seedOpp, null, { marketHints: hints });
    packet.marketOpportunityId = marketOpportunityId;
    packet.lodgingRelationship = lodgingRel;
    packet.commercialStatus = commercial;
    packet.sourceFamily = family;
    packet.developmentState = developmentState;
    packet.evidenceConfidence =
      lodgingLevel === LODGING_EVIDENCE.DIRECT
        ? 75
        : lodgingLevel === LODGING_EVIDENCE.STRONG_INFERENCE
          ? 60
          : 40;

    rawCandidates.push({
      marketOpportunityId,
      seedOpp: { ...seedOpp, marketOpportunityId, marketFirstIdentityV1: true },
      packet,
      lodgingEvidence: lodgingLevel,
      lodgingRelationship: lodgingRel,
      commercialStatus: commercial,
      sourceFamily: family,
      futureCycle: timing.futureCycle,
      validEntity: true,
      developmentState,
      geoConfidence: packet.geography?.confidence || null,
      venueHint: destination,
      whoName: null,
      url: src.url,
      title,
      organizationName: org,
      dates: timing.dateHint || timing.eventYear,
      geography: destination,
    });

    ledger.usefulSources.push({ url: src.url, family, lodgingLevel, commercial });
  }

  // Dedupe by marketOpportunityId (keep strongest lodging)
  const byId = new Map();
  const lodgingRank = {
    [LODGING_EVIDENCE.DIRECT]: 3,
    [LODGING_EVIDENCE.STRONG_INFERENCE]: 2,
    [LODGING_EVIDENCE.WEAK_INFERENCE]: 1,
    [LODGING_EVIDENCE.NONE]: 0,
  };
  for (const c of rawCandidates) {
    const prev = byId.get(c.marketOpportunityId);
    if (!prev || (lodgingRank[c.lodgingEvidence] || 0) > (lodgingRank[prev.lodgingEvidence] || 0)) {
      byId.set(c.marketOpportunityId, c);
    }
  }
  let marketOpps = [...byId.values()];

  // ——— Phase C: market-level Jev (bounded) ———
  for (const c of marketOpps) {
    if (budget.jev <= 0 || budget.followups <= 0) break;
    if (c.developmentState === "HOTEL_MATCHABLE" && c.geography && c.geography !== "Santo Domingo") {
      continue;
    }
    const action = decideMarketEvidenceNextAction({
      ...c,
      // Force location refinement when metro-only
      venueHint:
        c.geography && c.geography !== "Santo Domingo" ? c.venueHint : null,
      geoConfidence:
        !c.geography || c.geography === "Santo Domingo"
          ? "METRO_ONLY"
          : c.geoConfidence,
    });
    if (action.action === "STOP_NO_FURTHER_EVIDENCE") continue;
    budget.jev -= 1;
    ledger.jevMarket += 1;
    c.jevMarketAction = action.action;

    const followQ =
      action.action === "FIND_OFFICIAL_HOUSING_PAGE"
        ? `"${c.organizationName || c.title}" Santo Domingo (alojamiento OR "hotel oficial" OR housing OR accommodation) ${c.dates || "2027"}`
        : action.action === "VERIFY_FUTURE_CYCLE"
          ? `"${c.organizationName || c.title}" Santo Domingo (2027 OR 2028) (congreso OR conference)`
          : action.action === "VERIFY_VENUE" || action.action === "VERIFY_EVENT_LOCATION"
            ? `"${c.organizationName || c.title}" (Piantini OR Naco OR "Blue Mall" OR Tiradentes OR CIDAC OR Acropolis OR "Zona Colonial") Santo Domingo`
            : `"${c.organizationName || c.title}" Santo Domingo (hotel OR venue OR sede) 2027`;

    if (budget.followups > 0 && budget.queries > 0) {
      budget.followups -= 1;
      ledger.followups += 1;
      const { hits } = await runBoundedFollowupSearch(followQ, budget);
      ledger.queries += 1;
      for (const hit of hits.slice(0, 3)) {
        if (budget.fetches <= 0) break;
        const url = hit.link || hit.url;
        if (!url || seenUrl.has(url)) continue;
        seenUrl.add(url);
        budget.fetches -= 1;
        ledger.fetches += 1;
        try {
          const page = await Promise.race([
            fetchResearchPage(url, { timeoutMs: 10000 }),
            new Promise((_, reject) =>
              setTimeout(() => reject(new Error("http_timeout")), 12000)
            ),
          ]);
          const text = htmlToSearchableText(page?.html || "").slice(0, 20000);
          const blob = `${hit.title || ""} ${text}`;
          if (!isSantoDomingoInMarket(blob)) continue;
          const lodgingLevel = classifyLodgingEvidenceFromText(blob, url);
          const commercial = classifyCommercialStatus(blob);
          const timing = extractYearDates(blob);
          const dest = inferDestination(blob, hit.title || c.title || "");
          if (dest && dest !== "Santo Domingo") {
            c.geography = dest;
            c.venueHint = dest;
            c.seedOpp.destinationStatus = dest;
            c.packet = extractMarketOpportunityPacket(c.seedOpp, null, {
              marketHints: hints,
            });
            c.packet.marketOpportunityId = c.marketOpportunityId;
            c.geoConfidence = c.packet.geography?.confidence || null;
            ledger.jevResolved += 1;
            c.jevResolved = true;
          }
          if (lodgingLevel !== LODGING_EVIDENCE.NONE && c.lodgingEvidence === LODGING_EVIDENCE.NONE) {
            c.lodgingEvidence = lodgingLevel;
            c.lodgingRelationship = lodgingRelationshipFromEvidence(lodgingLevel, blob);
            c.seedOpp.lodgingEvidence = mapLodgingToBuilderEvidence(
              lodgingLevel,
              c.lodgingRelationship
            );
            c.packet.lodgingEvidence = c.seedOpp.lodgingEvidence;
            ledger.jevResolved += 1;
            c.jevResolved = true;
          }
          if (isCommerciallyOpen(commercial)) {
            c.commercialStatus = commercial;
            c.packet.commercialStatus = commercial;
          }
          if (timing.futureCycle) c.futureCycle = true;
          c.seedOpp.sources = [
            ...(c.seedOpp.sources || []),
            { url, title: hit.title, provider: "SERPAPI_FOLLOWUP" },
          ];
        } catch {
          /* continue */
        }
      }
    }

    c.developmentState = santoDomingoMarketDevelopmentState({
      lodgingEvidence: c.lodgingEvidence,
      commercialStatus: c.commercialStatus,
      validEntity: true,
      futureCycle: c.futureCycle,
    });
  }

  // Refresh hotel-matchable count
  ledger.hotelMatchable = marketOpps.filter((c) => c.developmentState === "HOTEL_MATCHABLE").length;
  ledger.lodgingSupported = marketOpps.filter(
    (c) => c.lodgingEvidence && c.lodgingEvidence !== LODGING_EVIDENCE.NONE
  ).length;
  ledger.openTbd = marketOpps.filter((c) => isCommerciallyOpen(c.commercialStatus)).length;

  // Watch set — future unresolved commercial trigger
  for (const c of marketOpps) {
    if (
      c.futureCycle &&
      (c.commercialStatus === "UNKNOWN" ||
        /OPEN|TBD|RFP|UNRESOLVED/i.test(String(c.commercialStatus || ""))) &&
      c.developmentState !== "HOTEL_MATCHABLE"
    ) {
      ledger.watch.push({
        marketOpportunityId: c.marketOpportunityId,
        title: c.title,
        trigger: c.commercialStatus || "FUTURE_UNRESOLVED",
        nextResearchDate: null,
        hotelsPotentiallyApplicable: ["JW", "RADISSON"],
        sharedEvidence: c.url,
      });
      c.marketWatch = true;
    }
  }

  // ——— Phase D: dual-hotel evaluation ———
  const pairMatrix = [];
  const jwReady = [];
  const radReady = [];
  const jwNeeds = [];
  const radNeeds = [];
  let both = 0;
  let jwOnly = 0;
  let radOnly = 0;
  let neither = 0;
  const geoEffect = {
    sameSubDirect: 0,
    sameSubStrong: 0,
    metroOnlyPlausibleOrUnknown: 0,
    weakNone: 0,
  };

  const evaluateHotel = (c, profile, key) => {
    const geoApp = evaluateHotelGeographicApplicability(c.seedOpp, profile, {
      marketHints: hints,
    });
    const fit = evaluateCrossHotelFit({
      marketPacket: c.packet,
      hotelProfile: profile,
      geographicApplicability: geoApp,
      seedOpp: c.seedOpp,
    });
    let jev = decideSupportingDataNextAction({
      marketPacket: c.packet,
      geographicApplicability: geoApp,
      hotelFit: fit,
    });

    // Bounded hotel-pair Jev execute for incomplete matchable pairs
    if (
      c.developmentState === "HOTEL_MATCHABLE" &&
      fit.finalState === "HOTEL_MATCHED_NEEDS_MORE_DATA" &&
      jev.action !== "STOP_NO_FURTHER_EVIDENCE" &&
      budget.jev > 0 &&
      budget.queries > 0
    ) {
      budget.jev -= 1;
      ledger.jevHotelPair += 1;
      // Soft resolve lodging/who from seed language only (no extra discovery duplication)
      if (jev.action === "VERIFY_LODGING_STATUS" && c.lodgingEvidence !== LODGING_EVIDENCE.NONE) {
        fit.missingData = (fit.missingData || []).filter((m) => m !== "lodging_evidence");
        if (fit.hotelFitScore >= 60 && !(fit.missingData || []).length) {
          fit.finalState = "CUSTOMER_READY";
          ledger.jevResolved += 1;
        }
      }
      if (jev.action === "VERIFY_WHO") {
        c.seedOpp.contactResearchState = "UNKNOWN_AFTER_RESEARCH";
        c.seedOpp.contactPathClass = "PUBLIC_DATA_CEILING";
        fit.missingData = (fit.missingData || []).filter((m) => m !== "who_contact");
      }
    }

    let built = null;
    let finalState = fit.finalState;

    // Metro-only / unknown geo: do not treat as hotel-applicable (no blind citywide fanout)
    if (
      geoApp.applicability === GEO_APPLICABILITY.UNKNOWN ||
      geoApp.applicability === GEO_APPLICABILITY.NONE
    ) {
      finalState =
        geoApp.applicability === GEO_APPLICABILITY.NONE
          ? "NOT_APPLICABLE"
          : "NOT_APPLICABLE";
    } else if (geoApp.applicability === GEO_APPLICABILITY.WEAK) {
      finalState = "NOT_FIT";
    } else if (
      c.developmentState === "HOTEL_MATCHABLE" &&
      [GEO_APPLICABILITY.DIRECT, GEO_APPLICABILITY.STRONG, GEO_APPLICABILITY.PLAUSIBLE].includes(
        geoApp.applicability
      ) &&
      finalState !== "NOT_FIT" &&
      finalState !== "NOT_APPLICABLE" &&
      finalState !== "CLOSED"
    ) {
      built = buildHotelOpportunityFromMarketPacket({
        marketPacket: c.packet,
        seedOpp: c.seedOpp,
        targetHotelProfile: profile,
        nowDate,
        requireStrictReady: true,
      });
      if (built.ok) {
        finalState = "CUSTOMER_READY";
      } else if (built.finalState === "NOT_FIT" || built.finalState === "NOT_APPLICABLE") {
        finalState = built.finalState;
      } else {
        finalState = "HOTEL_MATCHED_NEEDS_MORE_DATA";
      }
    } else if (c.developmentState !== "HOTEL_MATCHABLE") {
      // Market not yet matchable — hotel pair stays needs-data only if geo is candidate
      if (
        [GEO_APPLICABILITY.DIRECT, GEO_APPLICABILITY.STRONG, GEO_APPLICABILITY.PLAUSIBLE].includes(
          geoApp.applicability
        )
      ) {
        finalState = "HOTEL_MATCHED_NEEDS_MORE_DATA";
      } else {
        finalState = "NOT_APPLICABLE";
      }
    }

    // Geo effect tallies
    if (geoApp.sameMicro || (geoApp.sameSub && geoApp.applicability === GEO_APPLICABILITY.DIRECT)) {
      geoEffect.sameSubDirect += 1;
    }
    if (geoApp.sameSub && geoApp.applicability === GEO_APPLICABILITY.STRONG) {
      geoEffect.sameSubStrong += 1;
    }
    if (
      geoApp.oppGeo?.confidence === "METRO_ONLY" ||
      geoApp.applicability === GEO_APPLICABILITY.UNKNOWN
    ) {
      geoEffect.metroOnlyPlausibleOrUnknown += 1;
    }
    if (
      geoApp.applicability === GEO_APPLICABILITY.WEAK ||
      geoApp.applicability === GEO_APPLICABILITY.NONE
    ) {
      geoEffect.weakNone += 1;
    }

    const applicable = [
      GEO_APPLICABILITY.DIRECT,
      GEO_APPLICABILITY.STRONG,
      GEO_APPLICABILITY.PLAUSIBLE,
    ].includes(geoApp.applicability);

    return {
      key,
      geo: geoApp.applicability,
      fit: fit.hotelFitScore,
      final: finalState,
      jev: jev.action,
      built: built?.ok ? built.opportunity : null,
      reason: built?.reason || fit.reasons?.[0] || null,
      sameSub: geoApp.sameSub,
      sameMicro: geoApp.sameMicro,
      applicable,
    };
  };

  for (const c of marketOpps) {
    const jw = evaluateHotel(c, profiles.JW, "JW");
    const rad = evaluateHotel(c, profiles.RADISSON, "RADISSON");

    pairMatrix.push({
      marketOpp: c.marketOpportunityId,
      title: c.title,
      jwGeo: jw.geo,
      jwFit: jw.fit,
      jwFinal: jw.final,
      radissonGeo: rad.geo,
      radissonFit: rad.fit,
      radissonFinal: rad.final,
    });

    if (jw.final === "CUSTOMER_READY" && jw.built) jwReady.push({ ...c, hotelOpp: jw.built, eval: jw });
    else if (jw.applicable && jw.final === "HOTEL_MATCHED_NEEDS_MORE_DATA") {
      jwNeeds.push({ ...c, eval: jw });
    }

    if (rad.final === "CUSTOMER_READY" && rad.built) radReady.push({ ...c, hotelOpp: rad.built, eval: rad });
    else if (rad.applicable && rad.final === "HOTEL_MATCHED_NEEDS_MORE_DATA") {
      radNeeds.push({ ...c, eval: rad });
    }

    const jwFitish =
      jw.applicable &&
      (jw.final === "CUSTOMER_READY" || jw.final === "HOTEL_MATCHED_NEEDS_MORE_DATA");
    const radFitish =
      rad.applicable &&
      (rad.final === "CUSTOMER_READY" || rad.final === "HOTEL_MATCHED_NEEDS_MORE_DATA");
    if (jwFitish && radFitish) {
      both += 1;
      c.sharedVsUnique = "BOTH_HOTELS";
    } else if (jwFitish) {
      jwOnly += 1;
      c.sharedVsUnique = "JW_ONLY";
    } else if (radFitish) {
      radOnly += 1;
      c.sharedVsUnique = "RADISSON_ONLY";
    } else {
      neither += 1;
      c.sharedVsUnique = "NEITHER";
    }

    c.jw = jw;
    c.radisson = rad;
  }

  const readyHotelOpps = jwReady.length + radReady.length;
  const pairsEvaluated = marketOpps.length * 2;
  const fetchesUsed = ledger.fetches;
  const efficiency = {
    marketOpportunities: marketOpps.length,
    hotelPairsEvaluated: pairsEvaluated,
    readyHotelOpportunities: readyHotelOpps,
    fetchesPerMarketOpportunity:
      marketOpps.length > 0 ? Number((fetchesUsed / marketOpps.length).toFixed(2)) : null,
    fetchesPerReadyHotelOpportunity:
      readyHotelOpps > 0 ? Number((fetchesUsed / readyHotelOpps).toFixed(2)) : null,
    readyHotelOpportunitiesPerMarketOpportunity:
      marketOpps.length > 0 ? Number((readyHotelOpps / marketOpps.length).toFixed(2)) : 0,
    // Hotel-first would roughly double SERP+fetch for two hotels on same universe
    estimatedDuplicateFetchesAvoidedVsHotelFirst: fetchesUsed,
  };

  return {
    ledger,
    profiles: {
      JW: {
        hotelId: profiles.JW.hotelId,
        displayName: profiles.JW.displayName,
        sector: profiles.JW.districtSector,
        submarket: profiles.JW.submarket,
        microArea: profiles.JW.microArea,
        archetype: profiles.JW.archetype,
        rooms: profiles.JW.rooms,
        demandNodes: profiles.JW.primaryDemandNodes,
      },
      RADISSON: {
        hotelId: profiles.RADISSON.hotelId,
        displayName: profiles.RADISSON.displayName,
        sector: profiles.RADISSON.districtSector,
        submarket: profiles.RADISSON.submarket,
        microArea: profiles.RADISSON.microArea,
        archetype: profiles.RADISSON.archetype,
        rooms: profiles.RADISSON.rooms,
        demandNodes: profiles.RADISSON.primaryDemandNodes,
      },
    },
    marketOpportunities: marketOpps,
    pairMatrix,
    jwReady,
    radReady,
    jwNeeds,
    radNeeds,
    differentiation: { both, jwOnly, radOnly, neither },
    geoEffect,
    efficiency,
    budgetsRemaining: budget,
    hotelIds: SANTO_DOMINGO_HOTELS,
  };
}
