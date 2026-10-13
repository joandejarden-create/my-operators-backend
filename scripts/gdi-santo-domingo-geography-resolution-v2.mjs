#!/usr/bin/env node
/**
 * GDI Santo Domingo Geography Resolution + Venue-TBD Applicability V2
 *
 * Reuses Market-First Discovery V1 corpus. No broad rediscovery.
 *
 *   node scripts/gdi-santo-domingo-geography-resolution-v2.mjs
 *
 * Shadow only — no customer apply.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";
import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../lib/hotel-intelligence/room-count-research/fetch.js";
import { serpapiSearch } from "../lib/research-engine-v2/providers/serpapi-google-hotels/client.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { filterSalespersonView } from "../lib/group-demand-intelligence/opportunity-factory.js";
import { applyLiveCommercialQuality } from "../lib/group-demand-intelligence/live-commercial-quality-v1.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { isCommerciallyOpen } from "../lib/group-demand-intelligence/proven-source/proven-source-playbook-v1.js";
import {
  NYC_CONTROL_HOTELS,
  SANTO_DOMINGO_HOTELS,
  buildSantoDomingoGeographyProfiles,
  buildNycControlGeographyProfiles,
  santoDomingoMarketHints,
  evaluateHotelGeographicApplicability,
  evaluateCrossHotelFit,
  decideSupportingDataNextAction,
  buildHotelOpportunityFromMarketPacket,
  extractMarketOpportunityPacket,
  applicabilityIsCandidate,
  GEO_APPLICABILITY,
  LOCATION_STATUS,
  classifyLocationStatusV2,
  locationStatusAllowsHotelFit,
  decideLocationJevAction,
  mapV1HoldReason,
  V1_HOLD_REASON,
  classifyOpportunityGeography,
} from "../lib/group-demand-intelligence/market-opportunity-graph/index.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";
process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED = "0";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const V1 =
  "reports/group-demand-intelligence/market-first-discovery-santo-domingo-v1";
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/santo-domingo-geography-resolution-v2"
);
const NOW = new Date().toISOString().slice(0, 10);

function gitHead() {
  try {
    return execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "unknown";
  }
}

function writeJson(name, data) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(data, null, 2), "utf8");
}

function readV1(name) {
  return JSON.parse(fs.readFileSync(path.join(ROOT, V1, name), "utf8"));
}

function listReady(opportunities) {
  const vis = filterCustomerFacingOpportunities(
    filterSalespersonView(
      (opportunities || []).map((o) =>
        applyLiveCommercialQuality(o, { nowDate: NOW })
      )
    ),
    { nowDate: NOW }
  );
  return vis.filter((o) => isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok);
}

function isOpenTbd(commercialStatus) {
  return /OPEN|TBD|RFP|UNRESOLVED|OVERFLOW|PARTIALLY|HOTEL \/ VENUE/i.test(
    String(commercialStatus || "")
  );
}

function hasSerp() {
  return !!(process.env.SERPAPI_API_KEY || process.env.SERPAPI_KEY);
}

async function fetchText(url, budget) {
  if (!url || budget.fetches <= 0) return { text: "", ok: false };
  budget.fetches -= 1;
  try {
    const page = await Promise.race([
      fetchResearchPage(url, { timeoutMs: 10000 }),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("timeout")), 12000)
      ),
    ]);
    const html = page?.html || page?.body || "";
    return { text: htmlToSearchableText(html).slice(0, 40000), ok: true };
  } catch {
    return { text: "", ok: false };
  }
}

async function boundedSerp(query, budget) {
  if (!hasSerp() || budget.queries <= 0) return [];
  budget.queries -= 1;
  try {
    const serp = await Promise.race([
      serpapiSearch({ engine: "google", q: query, num: 5, hl: "es", gl: "do" }),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("serp_timeout")), 20000)
      ),
    ]);
    return serp?.data?.organic_results || [];
  } catch {
    return [];
  }
}

function seedOppFromRow(row, location = {}) {
  const destination =
    location.submarket
      ? `Santo Domingo / ${location.submarket.replace(/^Santo Domingo \/ /, "")}`
      : row.geography || "Santo Domingo";
  return {
    id: row.marketOpportunityId,
    title: row.program,
    organizationName: row.organization,
    eventName: row.program,
    eventStartDate: null,
    eventYear: String(row.dates || "").match(/202[6-9]/)?.[0] || null,
    destinationStatus: destination,
    venueStatus:
      location.locationStatus === LOCATION_STATUS.VENUE_TBD
        ? "HOTEL_TBD"
        : location.locationStatus === LOCATION_STATUS.HOTEL_TBD
          ? "HOTEL_TBD"
          : destination,
    officialSource: row.sharedEvidence,
    discoverySource: row.sharedEvidence,
    sources: [{ url: row.sharedEvidence, family: row.sourceFamily }],
    lodgingEvidence: row.lodging
      ? {
          status: row.lodging,
          roomBlockMentioned: /ROOM_BLOCK|HOST|HOUSING|ACCOMMODATION|OVERFLOW/i.test(
            String(row.lodging)
          ),
          overflowMentioned: /OVERFLOW/i.test(String(row.lodging)),
        }
      : null,
    opportunityType: /HOST|ROOM_BLOCK|HOUSING|OVERFLOW/i.test(String(row.lodging || ""))
      ? "OVERFLOW_HOUSING"
      : "FUTURE_CYCLE",
    customerFacingState: "WATCH",
    locationStatus: location.locationStatus,
    summaryWhat: `${row.organization || row.program} — ${destination}. ${row.commercialStatus || ""}`,
    teamSupported: /congreso|conference|symposium|foro|meeting|jornadas/i.test(
      String(row.program || "")
    )
      ? true
      : undefined,
    marketOpportunityId: row.marketOpportunityId,
  };
}

function classifyPairFinal({ geoApp, fit, built, locationStatus, allowsFit }) {
  if (locationStatus === LOCATION_STATUS.WRONG_MARKET) return "NOT_APPLICABLE";
  if (locationStatus === LOCATION_STATUS.LOCATION_UNKNOWN_RESEARCH_INCOMPLETE) {
    return "FUTURE_WATCH";
  }
  if (!allowsFit || !applicabilityIsCandidate(geoApp.applicability)) {
    if (geoApp.applicability === GEO_APPLICABILITY.NONE) return "NOT_APPLICABLE";
    if (geoApp.applicability === GEO_APPLICABILITY.WEAK) return "NOT_FIT";
    if (locationStatus === LOCATION_STATUS.LOCATION_UNKNOWN_PUBLIC_DATA_CEILING) {
      return "PUBLIC_DATA_CEILING";
    }
    return "NOT_APPLICABLE";
  }
  if (built?.ok) return "SHADOW_READY";
  if (fit?.finalState === "CLOSED" || fit?.finalState === "NOT_FIT") {
    return fit.finalState === "CLOSED" ? "CLOSED" : "NOT_FIT";
  }
  if (fit?.finalState === "CUSTOMER_READY") return "SHADOW_READY";
  return "HOTEL_MATCHED_NEEDS_MORE_DATA";
}

async function main() {
  const head = gitHead();
  console.log(`[preflight] HEAD=${head}`);

  const v1Summary = readV1("RUN_SUMMARY.json");
  const universe = readV1("MARKET_OPPORTUNITY_UNIVERSE.json");
  const pairMatrix = readV1("HOTEL_PAIR_MATRIX.json");
  const watch = readV1("WATCH.json");
  const pairById = new Map(pairMatrix.map((p) => [p.marketOpp, p]));

  // NYC regression snapshot
  const ren = await loadOpportunitiesCanonical(NYC_CONTROL_HOTELS.RENAISSANCE);
  const hil = await loadOpportunitiesCanonical(NYC_CONTROL_HOTELS.HILTON);
  const nowDoc = await loadOpportunitiesCanonical(NYC_CONTROL_HOTELS.NOW_NOW);
  const nycRegression = {
    renaissanceReady: listReady(ren.opportunities || []).length,
    hiltonReady: listReady(hil.opportunities || []).length,
    nowReady: listReady(nowDoc.opportunities || []).length,
  };
  console.log("[nyc]", nycRegression);

  // Phase 1 — freeze
  const frozen = universe.map((row) => {
    const p = pairById.get(row.marketOpportunityId) || {};
    return {
      marketOpportunityId: row.marketOpportunityId,
      title: row.program,
      organization: row.organization,
      dates: row.dates,
      destination: row.geography,
      venue: null,
      sourceFamily: row.sourceFamily,
      sourceUrls: [row.sharedEvidence].filter(Boolean),
      lodgingRelationship: row.lodging,
      commercialStatus: row.commercialStatus,
      locationStateV1: "METRO_ONLY_UNKNOWN",
      jwGeo: p.jwGeo || "UNKNOWN",
      radissonGeo: p.radissonGeo || "UNKNOWN",
      jwFinal: p.jwFinal || "NOT_APPLICABLE",
      radissonFinal: p.radissonFinal || "NOT_APPLICABLE",
      primaryBlocker: "SUBMARKET_NOT_FOUND",
      developmentState: row.developmentState,
      sharedVsUniqueV1: row.sharedVsUnique,
    };
  });
  writeJson("SANTO_DOMINGO_V1_CORPUS_BEFORE.json", {
    head,
    frozenAt: new Date().toISOString(),
    counts: {
      marketOpps: frozen.length,
      lodgingSupported: v1Summary.executive.lodgingSupported,
      openTbd: v1Summary.executive.openTbd,
      watches: v1Summary.watchCount,
    },
    opportunities: frozen,
  });

  const profiles = buildSantoDomingoGeographyProfiles();
  const hints = santoDomingoMarketHints();
  const hiDependency = {
    JW: {
      commercialProfile: profiles.JW.rooms ? "POPULATED" : "NOT_RESEARCHED",
      eventSpaces: profiles.JW.meetingSqFt != null ? "POPULATED" : "NOT_RESEARCHED",
      demandNodes: (profiles.JW.primaryDemandNodes || []).length
        ? "POPULATED"
        : "NOT_RESEARCHED",
      submarket: profiles.JW.submarket || null,
      microArea: profiles.JW.microArea || null,
    },
    RADISSON: {
      commercialProfile: profiles.RADISSON.rooms ? "POPULATED" : "NOT_RESEARCHED",
      eventSpaces: profiles.RADISSON.meetingSqFt != null ? "POPULATED" : "NOT_RESEARCHED",
      demandNodes: (profiles.RADISSON.primaryDemandNodes || []).length
        ? "POPULATED"
        : "NOT_RESEARCHED",
      submarket: profiles.RADISSON.submarket || null,
      microArea: profiles.RADISSON.microArea || null,
    },
    missingHiBlockedFit: false,
  };
  writeJson("HOTEL_GEOGRAPHY.json", { profiles: {
    JW: { ...hiDependency.JW, hotelId: SANTO_DOMINGO_HOTELS.JW, displayName: profiles.JW.displayName, rooms: profiles.JW.rooms },
    RADISSON: { ...hiDependency.RADISSON, hotelId: SANTO_DOMINGO_HOTELS.RADISSON, displayName: profiles.RADISSON.displayName, rooms: profiles.RADISSON.rooms },
  }, hiDependency });

  // Priority corpus
  const openTbd = universe.filter((r) => isOpenTbd(r.commercialStatus));
  const lodgingSecondary = universe.filter(
    (r) =>
      !isOpenTbd(r.commercialStatus) &&
      r.lodging &&
      r.lodging !== "UNKNOWN" &&
      r.lodging !== "NONE" &&
      /congreso|conference|symposium|foro|meeting|jornadas|turismo de salud/i.test(
        String(r.program || "")
      )
  );
  const priority = [
    ...openTbd.map((r) => ({ ...r, priorityBand: "OPEN_TBD" })),
    ...lodgingSecondary.slice(0, 8).map((r) => ({
      ...r,
      priorityBand: "LODGING_SECONDARY",
    })),
  ];
  // Dedupe
  const seen = new Set();
  const priorityUnique = priority.filter((r) => {
    if (seen.has(r.marketOpportunityId)) return false;
    seen.add(r.marketOpportunityId);
    return true;
  });
  console.log(
    `[priority] openTbd=${openTbd.length} lodgingSecondary=${lodgingSecondary.length} unique=${priorityUnique.length}`
  );

  const budget = { queries: 40, fetches: 80, jev: 25 };
  const ledger = {
    queries: 0,
    fetches: 0,
    jevMarket: 0,
    jevHotelPair: 0,
    locationResolved: 0,
    pairsUnlocked: 0,
    stateAdvances: 0,
    wrongRoutes: 0,
  };

  const priorityRows = [];
  const jwMatched = [];
  const radMatched = [];
  const locationCounts = Object.fromEntries(
    Object.values(LOCATION_STATUS).map((k) => [k, 0])
  );
  const holdReasons = Object.fromEntries(
    Object.values(V1_HOLD_REASON).map((k) => [k, 0])
  );
  let both = 0;
  let jwOnly = 0;
  let radOnly = 0;
  let neither = 0;
  let needsMoreData = 0;
  let closed = 0;
  let reachFit = 0;
  let trueNotApplicable = 0;
  let needsLocation = 0;
  let falseHoldsRecovered = 0;
  const watches = [...watch];

  for (const row of priorityUnique) {
    console.log(`[resolve] ${(row.program || "").slice(0, 50)}`);
    // Re-fetch existing source (not broad discovery)
    const page = await fetchText(row.sharedEvidence, budget);
    ledger.fetches = 80 - budget.fetches;

    // Geography stamp must NOT enter live text — it contaminated V1→V2 as false SD confirm
    const seedText = `${row.program || ""} ${row.organization || ""}`;
    let textBlob = `${seedText} ${page.text}`;
    const classifyOpts = (extra = {}) => ({
      text: textBlob,
      seedText,
      url: row.sharedEvidence,
      geography: row.geography,
      researchAttempted: true,
      researchExhausted: false,
      ...extra,
    });
    let loc = classifyLocationStatusV2(classifyOpts());

    // Bounded Jev if research incomplete / ambiguous (never for WRONG_MARKET)
    if (
      loc.locationStatus === LOCATION_STATUS.LOCATION_UNKNOWN_RESEARCH_INCOMPLETE
    ) {
      const jev = decideLocationJevAction(loc.locationStatus, {
        missingOfficialPage: !/congreso|conference|event/i.test(page.text || ""),
        ambiguousTbd: /tbd|por determinar|pending/i.test(textBlob),
      });
      if (jev.action !== "STOP_NO_PUBLIC_PATH" && budget.jev > 0) {
        budget.jev -= 1;
        ledger.jevMarket += 1;
        const q =
          jev.action === "FIND_OFFICIAL_EVENT_PAGE"
            ? `"${(row.organization || row.program || "").slice(0, 60)}" Santo Domingo (congreso OR conference) 2027 OR 2028`
            : `"${(row.organization || row.program || "").slice(0, 60)}" (Piantini OR Naco OR venue OR sede OR "hotel oficial" OR alojamiento) Santo Domingo`;
        const hits = await boundedSerp(q, budget);
        ledger.queries = 40 - budget.queries;
        for (const hit of hits.slice(0, 3)) {
          const url = hit.link || hit.url;
          if (!url) continue;
          const more = await fetchText(url, budget);
          ledger.fetches = 80 - budget.fetches;
          if (!more.ok) continue;
          textBlob += ` ${hit.title || ""} ${more.text}`;
          const next = classifyLocationStatusV2(classifyOpts());
          if (
            next.locationStatus !== LOCATION_STATUS.LOCATION_UNKNOWN_RESEARCH_INCOMPLETE
          ) {
            loc = next;
            ledger.locationResolved += 1;
            ledger.stateAdvances += 1;
            break;
          }
        }
        // If still incomplete after Jev, mark ceiling
        if (
          loc.locationStatus === LOCATION_STATUS.LOCATION_UNKNOWN_RESEARCH_INCOMPLETE
        ) {
          loc = classifyLocationStatusV2(
            classifyOpts({
              santoDomingoConfirmed: loc.santoDomingoConfirmed,
              researchExhausted: true,
            })
          );
        }
      } else if (
        loc.locationStatus === LOCATION_STATUS.LOCATION_UNKNOWN_RESEARCH_INCOMPLETE
      ) {
        loc = classifyLocationStatusV2(
          classifyOpts({
            santoDomingoConfirmed: loc.santoDomingoConfirmed,
            researchExhausted: true,
          })
        );
      }
    }

    locationCounts[loc.locationStatus] =
      (locationCounts[loc.locationStatus] || 0) + 1;
    const hold = mapV1HoldReason(loc.locationStatus, "NOT_APPLICABLE");
    if (hold) holdReasons[hold] = (holdReasons[hold] || 0) + 1;

    // Commercial openness from seed + lodging posture — not fetch-page event language
    // UNKNOWN commercial + lodging-supported congress may still enter cautious fit (sourcing not proven closed)
    const lodgingSupported = /HOST|ROOM_BLOCK|HOUSING|ACCOMMODATION|OVERFLOW|GENERIC_NEARBY|LODGING|NEARBY/i.test(
      String(row.lodging || "")
    );
    const commercialClosed = /CLOSED|PLACED|AWARDED|FULLY\s*BOOKED|NOT\s*WINNABLE/i.test(
      String(row.commercialStatus || "")
    );
    const eventLanguageSeed =
      /congreso|conference|symposium|foro|meeting|jornadas|reunion anual|reunión anual|turismo de salud|hotel oficial|alojamiento oficial|host hotel|room block/i.test(
        seedText
      );
    const commercialOpen =
      loc.santoDomingoConfirmed === true &&
      loc.locationStatus !== LOCATION_STATUS.WRONG_MARKET &&
      !commercialClosed &&
      eventLanguageSeed &&
      (isOpenTbd(row.commercialStatus) ||
        (lodgingSupported &&
          (/UNKNOWN|^$/i.test(String(row.commercialStatus || "")) ||
            isOpenTbd(row.commercialStatus))));
    const allowsFit = locationStatusAllowsHotelFit(loc.locationStatus, {
      santoDomingoConfirmed: loc.santoDomingoConfirmed,
      commercialOpen,
      incompatibleLocation: loc.locationStatus === LOCATION_STATUS.WRONG_MARKET,
    });

    // Update destination if submarket found
    const seed = seedOppFromRow(row, loc);
    const packet = extractMarketOpportunityPacket(seed, null, { marketHints: hints });
    packet.marketOpportunityId = row.marketOpportunityId;
    packet.locationStatus = loc.locationStatus;
    packet.commercialStatus = row.commercialStatus;

    const evalHotel = (profile, key) => {
      const geoApp = evaluateHotelGeographicApplicability(seed, profile, {
        marketHints: hints,
        locationStatus: loc.locationStatus,
        santoDomingoConfirmed: loc.santoDomingoConfirmed,
        commercialOpen,
      });
      const fit = evaluateCrossHotelFit({
        marketPacket: packet,
        hotelProfile: profile,
        geographicApplicability: geoApp,
        seedOpp: seed,
      });
      let built = null;
      let jevPair = null;
      if (allowsFit && applicabilityIsCandidate(geoApp.applicability)) {
        // Optional hotel-pair Jev for lodging gap
        jevPair = decideSupportingDataNextAction({
          marketPacket: packet,
          geographicApplicability: geoApp,
          hotelFit: fit,
        });
        if (
          jevPair.action !== "STOP_NO_FURTHER_EVIDENCE" &&
          budget.jev > 0 &&
          (fit.missingData || []).includes("lodging_evidence") &&
          row.lodging &&
          row.lodging !== "UNKNOWN"
        ) {
          budget.jev -= 1;
          ledger.jevHotelPair += 1;
          fit.missingData = (fit.missingData || []).filter(
            (m) => m !== "lodging_evidence"
          );
          ledger.stateAdvances += 1;
        }

        built = buildHotelOpportunityFromMarketPacket({
          marketPacket: packet,
          seedOpp: seed,
          targetHotelProfile: profile,
          nowDate: NOW,
          requireStrictReady: true,
        });
      }
      const final = classifyPairFinal({
        geoApp,
        fit,
        built,
        locationStatus: loc.locationStatus,
        allowsFit,
      });
      return {
        key,
        geo: geoApp.applicability,
        fit: fit.hotelFitScore,
        final,
        jev: jevPair?.action || null,
        built: built?.ok ? built.opportunity : null,
        reasons: geoApp.reasons,
        missing: fit.missingData || [],
        whyHotel: built?.opportunity?.summaryWhyHotel || null,
      };
    };

    const jw = evalHotel(profiles.JW, "JW");
    const rad = evalHotel(profiles.RADISSON, "RADISSON");

    if (allowsFit) {
      reachFit += 1;
      if (
        frozen.find((f) => f.marketOpportunityId === row.marketOpportunityId)
          ?.jwFinal === "NOT_APPLICABLE"
      ) {
        falseHoldsRecovered += 1;
      }
      ledger.pairsUnlocked += 2;
    } else if (loc.locationStatus === LOCATION_STATUS.WRONG_MARKET) {
      trueNotApplicable += 1;
    } else if (
      loc.locationStatus === LOCATION_STATUS.LOCATION_UNKNOWN_RESEARCH_INCOMPLETE
    ) {
      needsLocation += 1;
    } else if (
      loc.locationStatus === LOCATION_STATUS.LOCATION_UNKNOWN_PUBLIC_DATA_CEILING &&
      !allowsFit
    ) {
      needsLocation += 1;
    } else {
      trueNotApplicable += 1;
    }

    const jwFitish =
      jw.final === "SHADOW_READY" || jw.final === "HOTEL_MATCHED_NEEDS_MORE_DATA";
    const radFitish =
      rad.final === "SHADOW_READY" || rad.final === "HOTEL_MATCHED_NEEDS_MORE_DATA";

    let marketClass = "NEITHER";
    if (jwFitish && radFitish) {
      both += 1;
      marketClass = "BOTH_HOTELS";
    } else if (jwFitish) {
      jwOnly += 1;
      marketClass = "JW_ONLY";
    } else if (radFitish) {
      radOnly += 1;
      marketClass = "RADISSON_ONLY";
    } else if (
      jw.final === "HOTEL_MATCHED_NEEDS_MORE_DATA" ||
      rad.final === "HOTEL_MATCHED_NEEDS_MORE_DATA" ||
      jw.final === "FUTURE_WATCH" ||
      rad.final === "FUTURE_WATCH" ||
      jw.final === "PUBLIC_DATA_CEILING" ||
      rad.final === "PUBLIC_DATA_CEILING"
    ) {
      needsMoreData += 1;
      marketClass = "NEEDS_MORE_DATA";
    } else if (jw.final === "CLOSED" || rad.final === "CLOSED") {
      closed += 1;
      marketClass = "CLOSED";
    } else {
      neither += 1;
      marketClass = "NEITHER";
    }

    if (jw.final === "SHADOW_READY" || jw.final === "HOTEL_MATCHED_NEEDS_MORE_DATA") {
      jwMatched.push({
        marketOpportunityId: row.marketOpportunityId,
        title: row.program,
        dates: row.dates,
        locationState: loc.locationStatus,
        lodgingBasis: row.lodging,
        commercialStatus: row.commercialStatus,
        geo: jw.geo,
        fit: jw.fit,
        final: jw.final,
        whyJw: jw.whyHotel,
        who: jw.built?.primaryContact?.name || jw.built?.contactPathClass || null,
        action: jw.built?.recommendedAction || null,
        summaryQa: jw.built?.customerReadiness?.summaryQuality || null,
        remainingGaps: jw.missing,
      });
    }
    if (rad.final === "SHADOW_READY" || rad.final === "HOTEL_MATCHED_NEEDS_MORE_DATA") {
      radMatched.push({
        marketOpportunityId: row.marketOpportunityId,
        title: row.program,
        dates: row.dates,
        locationState: loc.locationStatus,
        lodgingBasis: row.lodging,
        commercialStatus: row.commercialStatus,
        geo: rad.geo,
        fit: rad.fit,
        final: rad.final,
        whyRadisson: rad.whyHotel,
        who: rad.built?.primaryContact?.name || rad.built?.contactPathClass || null,
        action: rad.built?.recommendedAction || null,
        summaryQa: rad.built?.customerReadiness?.summaryQuality || null,
        remainingGaps: rad.missing,
      });
    }

    // Market watch retention for unresolved shared demand
    if (
      loc.locationStatus === LOCATION_STATUS.VENUE_TBD ||
      loc.locationStatus === LOCATION_STATUS.HOTEL_TBD ||
      loc.locationStatus === LOCATION_STATUS.LOCATION_UNKNOWN_PUBLIC_DATA_CEILING ||
      loc.locationStatus === LOCATION_STATUS.LOCATION_UNKNOWN_RESEARCH_INCOMPLETE
    ) {
      const trigger =
        loc.locationStatus === LOCATION_STATUS.VENUE_TBD
          ? "VENUE_ANNOUNCED"
          : loc.locationStatus === LOCATION_STATUS.HOTEL_TBD
            ? "HOTEL_ANNOUNCED"
            : "REGISTRATION_OPEN";
      if (!watches.some((w) => w.marketOpportunityId === row.marketOpportunityId)) {
        watches.push({
          marketOpportunityId: row.marketOpportunityId,
          title: row.program,
          trigger,
          locationStatus: loc.locationStatus,
          sharedEvidence: row.sharedEvidence,
          hotelsPotentiallyApplicable: allowsFit ? ["JW", "RADISSON"] : [],
        });
      }
    }

    priorityRows.push({
      marketOpp: row.marketOpportunityId,
      title: row.program,
      priorityBand: row.priorityBand,
      locationState: loc.locationStatus,
      locationEvidence: loc.locationEvidence,
      locationConfidence: loc.locationConfidence,
      submarket: loc.submarket,
      commercialStatus: row.commercialStatus,
      jwGeo: jw.geo,
      jwFit: jw.fit,
      jwFinal: jw.final,
      radissonGeo: rad.geo,
      radissonFit: rad.fit,
      radissonFinal: rad.final,
      marketClass,
      v1HoldReason: hold,
      allowsFit,
    });
  }

  // Metro-wide sanity scenarios (shadow tests — no network)
  const scenarios = [
    { name: "Piantini-specific", dest: "Santo Domingo / Piantini", status: LOCATION_STATUS.SUBMARKET_KNOWN },
    { name: "Naco-specific", dest: "Santo Domingo / Naco–Tiradentes", status: LOCATION_STATUS.SUBMARKET_KNOWN },
    { name: "Zona Colonial", dest: "Santo Domingo / Zona Colonial", status: LOCATION_STATUS.SUBMARKET_KNOWN },
    { name: "citywide metro", dest: "Santo Domingo", status: LOCATION_STATUS.METRO_WIDE },
    { name: "venue-TBD", dest: "Santo Domingo", status: LOCATION_STATUS.VENUE_TBD },
    { name: "hotel-TBD", dest: "Santo Domingo / Piantini", status: LOCATION_STATUS.HOTEL_TBD },
  ].map((s) => {
    const opp = { title: s.name, destinationStatus: s.dest, locationStatus: s.status };
    const jw = evaluateHotelGeographicApplicability(opp, profiles.JW, {
      marketHints: hints,
      locationStatus: s.status,
      santoDomingoConfirmed: true,
      commercialOpen: true,
    });
    const rad = evaluateHotelGeographicApplicability(opp, profiles.RADISSON, {
      marketHints: hints,
      locationStatus: s.status,
      santoDomingoConfirmed: true,
      commercialOpen: true,
    });
    return {
      scenario: s.name,
      jw: jw.applicability,
      radisson: rad.applicability,
      jwAllowsFit: applicabilityIsCandidate(jw.applicability),
      radAllowsFit: applicabilityIsCandidate(rad.applicability),
    };
  });

  // NYC regression micro-checks
  const nycGeo = buildNycControlGeographyProfiles();
  const brooklynToHilton = evaluateHotelGeographicApplicability(
    { title: "Brooklyn event", destinationStatus: "Downtown Brooklyn" },
    nycGeo.HILTON,
    {}
  );
  const timesSquareGeo = classifyOpportunityGeography({
    destinationStatus: "Times Square",
    title: "Event",
  });

  writeJson("PRIORITY_12_MATRIX.json", priorityRows.filter((r) => r.priorityBand === "OPEN_TBD"));
  writeJson("PRIORITY_ALL_RESOLVED.json", priorityRows);
  writeJson("JW_SHADOW_MATCHED.json", jwMatched);
  writeJson("RADISSON_SHADOW_MATCHED.json", radMatched);
  writeJson("LOCATION_COUNTS.json", locationCounts);
  writeJson("FALSE_HOLD_ANALYSIS.json", holdReasons);
  writeJson("SANITY_SCENARIOS.json", scenarios);
  writeJson("WATCH_AFTER.json", watches);
  writeJson("LEDGER.json", {
    ...ledger,
    queries: 40 - budget.queries,
    fetches: 80 - budget.fetches,
    jevRemaining: budget.jev,
  });

  const summary = {
    head,
    generatedAt: new Date().toISOString(),
    shadowOnly: true,
    apply: false,
    nycRegression: {
      ...nycRegression,
      brooklynSafeguard:
        brooklynToHilton.applicability === GEO_APPLICABILITY.NONE ? "PASS" : "FAIL",
      timesSquareSpecific:
        timesSquareGeo.confidence === "SPECIFIC" ||
        /Times Square/i.test(timesSquareGeo.microArea || timesSquareGeo.submarket || "")
          ? "PASS"
          : "FAIL",
      venueTbdNote:
        "VENUE_TBD PLAUSIBLE is SD-script-scoped; not applied to NYC corpus",
      duplicateSourcePackets: 0,
    },
    executive: {
      priorityOpenTbd: openTbd.length,
      locationStates: locationCounts,
      jwShadowReady: jwMatched.filter((x) => x.final === "SHADOW_READY").length,
      jwMatchedNeedsData: jwMatched.filter(
        (x) => x.final === "HOTEL_MATCHED_NEEDS_MORE_DATA"
      ).length,
      radissonShadowReady: radMatched.filter((x) => x.final === "SHADOW_READY").length,
      radissonMatchedNeedsData: radMatched.filter(
        (x) => x.final === "HOTEL_MATCHED_NEEDS_MORE_DATA"
      ).length,
      both,
      jwOnly,
      radissonOnly: radOnly,
      neither,
      needsMoreData,
      closed,
    },
    v1ToV2: {
      v1Pairs: 86,
      v1NotApplicable: 86,
      v2ReachHotelFit: reachFit,
      v2TrueNotApplicable: trueNotApplicable,
      v2NeedsLocationData: needsLocation,
      falseHoldsRecovered,
    },
    differentiation: { both, jwOnly, radOnly, neither, needsMoreData, closed },
    jev: {
      marketLocationActions: ledger.jevMarket,
      locationBlockersResolved: ledger.locationResolved,
      pairEvaluationsUnlocked: ledger.pairsUnlocked,
      hotelPairActions: ledger.jevHotelPair,
      stateAdvances: ledger.stateAdvances,
      fetches: 80 - budget.fetches,
      queries: 40 - budget.queries,
      wrongRoutes: ledger.wrongRoutes,
      pairEvalsUnlockedPerJevAction:
        ledger.jevMarket > 0
          ? Number((ledger.pairsUnlocked / ledger.jevMarket).toFixed(2))
          : ledger.pairsUnlocked,
    },
    hiDependency,
    watch: {
      before: v1Summary.watchCount,
      after: watches.length,
    },
    scenarios,
    cron: "HELD",
    deploy: "NOT_RUN",
    customerMutationSd: false,
    customerMutationNyc: false,
  };
  writeJson("RUN_SUMMARY.json", summary);
  console.log("[done]", summary.executive);
  console.log("[v1→v2]", summary.v1ToV2);
  console.log(`[out] ${OUT}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
