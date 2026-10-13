/**
 * Market-level hidden-demand discovery — discover once, match many hotels.
 * No Hilton/Renaissance hardcodes in logic.
 */

import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../hotel-intelligence/room-count-research/fetch.js";
import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import { extractLodgingSignalsFromText } from "../commercial-depth-v2.js";
import {
  HIDDEN_DEMAND_VERSION,
  DISCOVERY_DEPTH,
  LODGING_SIGNAL_STRENGTH,
  CUSTOMER_PROMOTABLE_DEPTHS,
  DEMAND_FAMILY,
} from "./constants.js";
import {
  computeDemandGeneratorId,
  computeHiddenDemandId,
} from "./identity.js";
import {
  classifyDiscoveryDepth,
  isCustomerPromotableHiddenDemand,
  inferDemandFamily,
  reclassifyExistingOpportunity,
} from "./classify.js";
import { buildHiddenDemandMarketQueries } from "./discovery-queries.js";
import {
  evaluateHotelHiddenDemandFit,
  buildHotelSpecificThesis,
  buildHotelWhyNow,
  buildHotelRecommendedAction,
  inferHiddenMotion,
} from "./hotel-match.js";
import { decideHiddenDemandNextLayer } from "./jev-next-layer.js";
import {
  isPlausibleOrganizationName,
  passesHiddenDemandQualityGate,
} from "./quality-gate.js";

function clean(s) {
  return String(s || "").trim();
}

function hasSerp() {
  return !!(process.env.SERPAPI_API_KEY || process.env.SERPAPI_KEY);
}

function lodgingStrengthFromSignals(sig = {}) {
  if (sig.roomBlockMentioned && (sig.housingPageFound || sig.housingOpen === true)) {
    return LODGING_SIGNAL_STRENGTH.STRONG;
  }
  if (sig.roomBlockMentioned || sig.overflowMentioned || sig.hostHotelMentioned) {
    return LODGING_SIGNAL_STRENGTH.MEDIUM;
  }
  if (sig.snippets?.length) return LODGING_SIGNAL_STRENGTH.WEAK;
  return LODGING_SIGNAL_STRENGTH.UNKNOWN;
}

function extractOrgHints(title = "", snippet = "") {
  const blob = `${title} ${snippet}`;
  // Prefer company-like Title Case before exhibitor/sponsor language
  const m = blob.match(
    /\b([A-Z][A-Za-z0-9&.\-]+(?:\s+[A-Z][A-Za-z0-9&.\-]+){0,4})\s+(?:exhibitor|sponsor|vendor|agency|production|crew|tour|delegation|cohort)\b/
  );
  if (m && isPlausibleOrganizationName(m[1])) return clean(m[1]);
  const quoted = blob.match(/"([A-Z][^"]{2,58})"/);
  if (quoted && isPlausibleOrganizationName(quoted[1])) return clean(quoted[1]);
  // Org — Event pattern
  const dash = blob.match(
    /\b([A-Z][A-Za-z0-9&.\-]+(?:\s+[A-Z][A-Za-z0-9&.\-]+){1,5})\s+[—\-:|]\s+/
  );
  if (dash && isPlausibleOrganizationName(dash[1])) return clean(dash[1]);
  return null;
}

function looksLikeObviousCalendar(title = "", url = "") {
  return classifyDiscoveryDepth({ title, sourceUrl: url }) === DISCOVERY_DEPTH.OBVIOUS_MARKET_DEMAND;
}

/**
 * Convert a SERP hit + optional page extract into generator and/or hidden demand.
 */
export function signalToEntities({
  hit = {},
  family = null,
  marketKey = "nyc_midtown",
  year = null,
  lodgingSignals = null,
  pageText = "",
} = {}) {
  const title = clean(hit.title || hit.titleRaw);
  const url = clean(hit.link || hit.url);
  const snippet = clean(hit.snippet || hit.description || "");
  const blob = `${title} ${snippet} ${pageText.slice(0, 2000)}`;
  const fam = family || inferDemandFamily(blob);
  const y = year || new Date().getUTCFullYear() + 1;

  const isCalendar = looksLikeObviousCalendar(title, url);
  const orgHint = extractOrgHints(title, snippet) || extractOrgHints(title, pageText.slice(0, 800));
  const lodging =
    lodgingSignals != null
      ? lodgingStrengthFromSignals(lodgingSignals)
      : /hotel\s*block|room\s*block|housing|crew\s*lodging|accommodation/i.test(blob)
        ? LODGING_SIGNAL_STRENGTH.MEDIUM
        : LODGING_SIGNAL_STRENGTH.UNKNOWN;

  const generators = [];
  const hidden = [];

  if (isCalendar || (!orgHint && /conference|convention|trade\s*show|fashion\s*week|comic\s*con|nrf/i.test(title))) {
    const demandGeneratorId = computeDemandGeneratorId({
      name: title.slice(0, 80),
      marketKey,
      year: y,
    });
    generators.push({
      demandGeneratorId,
      name: title.slice(0, 120),
      marketKey,
      year: y,
      sourceUrl: url || null,
      discoveryDepth: DISCOVERY_DEPTH.OBVIOUS_MARKET_DEMAND,
      family: fam,
      kind: "DEMAND_GENERATOR",
    });

    // Layer-down ONLY when a plausible addressable org is identified
    if (orgHint && isPlausibleOrganizationName(orgHint)) {
      const organizationName = orgHint;
      const projectName = `${organizationName} ${fam} ${y}`;
      const hiddenDemandId = computeHiddenDemandId({
        organizationName,
        projectName,
        demandGeneratorId,
        family: fam,
        timingKey: String(y),
      });
      const depth = classifyDiscoveryDepth({
        title: projectName,
        organizationName,
        sourceUrl: url,
        hasParentGenerator: true,
        hasSubgroupEvidence: true,
        lodgingSignalStrength: lodging,
      });
      const row = {
        hiddenDemandId,
        demandGeneratorId,
        organizationName,
        projectName,
        title: projectName,
        family: fam,
        marketKey,
        year: y,
        timingKey: String(y),
        destination: "New York Midtown",
        sourceUrl: url || null,
        snippet: snippet.slice(0, 280),
        lodgingSignalStrength: lodging,
        lodgingEvidence: lodgingSignals || null,
        discoveryDepth: depth,
        kind: "HIDDEN_DEMAND",
        evidence: {
          entityExists: true,
          futureTiming: true,
          travelPresence: /travel|hotel|lodging|housing|midtown|manhattan|new york|nyc/i.test(blob),
          plausibleLodging: lodging !== LODGING_SIGNAL_STRENGTH.UNKNOWN,
          addressableOrg: true,
        },
      };
      if (passesHiddenDemandQualityGate(row).ok) hidden.push(row);
    }
  } else if (orgHint && isPlausibleOrganizationName(orgHint)) {
    // Entity-first (no major public calendar required) — org required
    const organizationName = orgHint;
    const projectName = title.slice(0, 100) || organizationName;
    const demandGeneratorId = null;
    const hiddenDemandId = computeHiddenDemandId({
      organizationName,
      projectName,
      demandGeneratorId: "entity_first",
      family: fam,
      timingKey: String(y),
    });
    const depth = classifyDiscoveryDepth({
      title: projectName,
      organizationName,
      sourceUrl: url,
      hasParentGenerator: false,
      hasSubgroupEvidence: /team|crew|cohort|delegation|block/i.test(blob),
      lodgingSignalStrength: lodging,
    });
    const row = {
      hiddenDemandId,
      demandGeneratorId,
      organizationName,
      projectName,
      title: projectName,
      family: fam,
      marketKey,
      year: y,
      timingKey: String(y),
      destination: "New York Midtown",
      sourceUrl: url || null,
      snippet: snippet.slice(0, 280),
      lodgingSignalStrength: lodging,
      lodgingEvidence: lodgingSignals || null,
      discoveryDepth: depth,
      kind: "HIDDEN_DEMAND",
      entityFirst: true,
      evidence: {
        entityExists: true,
        futureTiming: /\b20(2[6-9]|3[0-9])\b/.test(blob),
        travelPresence: /travel|hotel|lodging|housing|midtown|manhattan|new york|nyc/i.test(blob),
        plausibleLodging:
          lodging !== LODGING_SIGNAL_STRENGTH.UNKNOWN || /team|crew|cohort|group/i.test(blob),
        addressableOrg: true,
      },
    };
    if (passesHiddenDemandQualityGate(row).ok) hidden.push(row);
  }

  return { generators, hidden, family: fam };
}

/**
 * Deduplicate hidden demand by hiddenDemandId.
 */
export function dedupeHiddenDemands(rows = []) {
  const map = new Map();
  for (const row of rows) {
    if (!row?.hiddenDemandId) continue;
    const prev = map.get(row.hiddenDemandId);
    if (!prev) {
      map.set(row.hiddenDemandId, row);
      continue;
    }
    // Keep stronger lodging + richer evidence
    const rank = (r) =>
      ({ STRONG: 3, MEDIUM: 2, WEAK: 1, UNKNOWN: 0 }[r.lodgingSignalStrength] || 0);
    if (rank(row) > rank(prev)) map.set(row.hiddenDemandId, { ...prev, ...row });
    else map.set(row.hiddenDemandId, { ...row, ...prev, sources: undefined });
  }
  return [...map.values()];
}

/**
 * Match one hidden demand across multiple hotel configs.
 */
export function matchHiddenDemandToHotels(hiddenDemand, hotelConfigs = []) {
  const matches = [];
  for (const cfg of hotelConfigs) {
    if (!cfg?.hotelId) continue;
    const fit = evaluateHotelHiddenDemandFit(hiddenDemand, cfg);
    const thesis = buildHotelSpecificThesis(hiddenDemand, fit, cfg);
    const whyNow = buildHotelWhyNow(hiddenDemand, fit);
    const recommendedAction = buildHotelRecommendedAction(hiddenDemand, fit, null);
    matches.push({
      ...fit,
      thesis,
      whyNow,
      recommendedAction,
      whyThisHotel: thesis,
      hotelOpportunityTitle: `${hiddenDemand.organizationName || hiddenDemand.title} — ${String(fit.commercialMotion || "").replace(/_/g, " ")}`,
      discoveryDepth: hiddenDemand.discoveryDepth,
      family: hiddenDemand.family,
      lodgingSignalStrength: hiddenDemand.lodgingSignalStrength,
      demandGeneratorId: hiddenDemand.demandGeneratorId,
      sourceUrl: hiddenDemand.sourceUrl,
      marketKey: hiddenDemand.marketKey,
    });
  }
  return matches;
}

/**
 * Build customer opportunity candidate from hotel match (no competitor fields).
 */
export function hotelMatchToOpportunityCandidate(hiddenDemand, match, hotelConfig) {
  const promotable = isCustomerPromotableHiddenDemand(hiddenDemand.discoveryDepth, {
    deeperMotion: hiddenDemand.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.STRONG,
  });
  const actionable =
    promotable &&
    match.decision !== "INSUFFICIENT" &&
    match.fitScore >= 70 &&
    (hiddenDemand.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.STRONG ||
      hiddenDemand.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.MEDIUM) &&
    hiddenDemand.evidence?.addressableOrg &&
    hiddenDemand.sourceUrl;

  return {
    id: match.hotelOpportunityId,
    opportunityId: match.hotelOpportunityId,
    hotelId: match.hotelId,
    hiddenDemandId: hiddenDemand.hiddenDemandId,
    demandGeneratorId: hiddenDemand.demandGeneratorId || null,
    hotelMatchId: match.hotelMatchId,
    title: match.hotelOpportunityTitle,
    organizationName: hiddenDemand.organizationName,
    demandTrigger: hiddenDemand.demandGeneratorId
      ? `Linked demand generator ${hiddenDemand.demandGeneratorId}`
      : hiddenDemand.projectName || hiddenDemand.title,
    commercialMotion: match.commercialMotion,
    lodgingPrimaryMotion: match.commercialMotion,
    lodgingSignalStrength: hiddenDemand.lodgingSignalStrength,
    lodgingEvidence: hiddenDemand.lodgingEvidence || {
      roomBlockMentioned: hiddenDemand.lodgingSignalStrength !== LODGING_SIGNAL_STRENGTH.UNKNOWN,
    },
    hotelFitScore: match.fitScore,
    hotelFitDecision: match.decision,
    hotelOpportunityThesis: match.thesis,
    summaryWhyHotel: match.whyThisHotel,
    whyNow: match.whyNow,
    recommendedAction: match.recommendedAction,
    discoveryDepth: hiddenDemand.discoveryDepth,
    demandFamily: hiddenDemand.family,
    eventYear: hiddenDemand.year,
    timingKey: hiddenDemand.timingKey,
    destination: hiddenDemand.destination,
    officialSource: hiddenDemand.sourceUrl,
    sources: hiddenDemand.sourceUrl
      ? [{ url: hiddenDemand.sourceUrl, title: hiddenDemand.title, kind: "hidden_demand_signal" }]
      : [],
    priority: match.fitScore >= 80 ? "HIGH" : match.fitScore >= 65 ? "MEDIUM" : "WATCH",
    customerFacingState: actionable
      ? "ACTIONABLE_NOW"
      : promotable && match.decision !== "INSUFFICIENT"
        ? "WATCH"
        : "INTERNAL_ONLY",
    customerPromotable: promotable && match.decision !== "INSUFFICIENT",
    gdiVersion: HIDDEN_DEMAND_VERSION,
    segment: hiddenDemand.family,
    opportunityType: "HIDDEN_DEMAND",
  };
}

/**
 * Run shared market discovery (SERP + selective fetches). Budget-bounded.
 */
export async function runMarketHiddenDemandDiscovery(opts = {}) {
  const marketLabel = opts.marketLabel || "New York Midtown";
  const marketKey = opts.marketKey || "nyc_midtown";
  const year = opts.year || new Date().getUTCFullYear() + 1;
  const maxQueries = opts.maxQueries ?? 50;
  const maxFetches = opts.maxFetches ?? 100;
  const maxRendered = opts.maxRendered ?? 15;
  const enableJev = opts.enableJev !== false;
  const queries = buildHiddenDemandMarketQueries({
    marketLabel,
    year,
    maxQueries,
  });

  const ledger = {
    version: HIDDEN_DEMAND_VERSION,
    marketKey,
    marketLabel,
    year,
    queries: 0,
    fetches: 0,
    rendered: 0,
    serpErrors: [],
    fetchErrors: [],
    signals: [],
    generators: [],
    hidden: [],
    jev: { calls: 0, safeApply: 0, nextLayer: 0, helpfulDifferent: 0, same: 0, wrong: 0 },
    familyYield: Object.fromEntries(
      Object.values(DEMAND_FAMILY).map((f) => [f, { candidates: 0, lodging: 0 }])
    ),
  };

  let fetchBudget = maxFetches;
  let renderBudget = maxRendered;

  for (const q of queries) {
    if (!hasSerp()) break;
    ledger.queries += 1;
    let organic = [];
    try {
      const serp = await serpapiSearch({
        engine: "google",
        q: q.query,
        num: 5,
        hl: "en",
        gl: "us",
      });
      organic = serp?.data?.organic_results || [];
    } catch (err) {
      ledger.serpErrors.push({ query: q.query, error: String(err?.message || err) });
      continue;
    }

    // Optional Jev next-layer routing once per family lane
    if (enableJev && ledger.queries % 5 === 1) {
      try {
        const layer = await decideHiddenDemandNextLayer({
          demandGenerator: { name: marketLabel },
          knownEntities: ledger.hidden,
          currentFamily: q.family,
          defaultLayer: mapFamilyToLayer(q.family),
          enableSafeApply: true,
        });
        ledger.jev.calls += 1;
        ledger.jev.nextLayer += 1;
        if (layer.applied) {
          ledger.jev.safeApply += 1;
          ledger.jev.helpfulDifferent += 1;
          if (layer.family) q.family = layer.family;
        } else {
          ledger.jev.same += 1;
        }
      } catch {
        ledger.jev.wrong += 1;
      }
    }

    for (const hit of organic.slice(0, 4)) {
      ledger.signals.push({
        queryId: q.queryId,
        family: q.family,
        title: hit.title,
        url: hit.link,
        snippet: hit.snippet,
      });

      let lodgingSignals = null;
      let pageText = "";
      const url = hit.link;
      const shouldFetch =
        fetchBudget > 0 &&
        url &&
        (/exhibitor|sponsor|housing|hotel|tour|delegation|training|agency|showroom|crew|pdf/i.test(
          `${hit.title} ${hit.snippet} ${url}`
        ) ||
          renderBudget > 0);

      if (shouldFetch && fetchBudget > 0) {
        fetchBudget -= 1;
        ledger.fetches += 1;
        try {
          const page = await fetchResearchPage(url);
          if (page.ok) {
            pageText = htmlToSearchableText(page.text || "");
            lodgingSignals = extractLodgingSignalsFromText(pageText, page.url || url);
            if (renderBudget > 0) {
              renderBudget -= 1;
              ledger.rendered += 1;
            }
          }
        } catch (err) {
          ledger.fetchErrors.push({ url, error: String(err?.message || err) });
        }
      }

      const { generators, hidden } = signalToEntities({
        hit,
        family: q.family,
        marketKey,
        year,
        lodgingSignals,
        pageText,
      });
      ledger.generators.push(...generators);
      for (const h of hidden) {
        ledger.hidden.push(h);
        const fy = ledger.familyYield[h.family] || { candidates: 0, lodging: 0 };
        fy.candidates += 1;
        if (
          h.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.STRONG ||
          h.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.MEDIUM
        ) {
          fy.lodging += 1;
        }
        ledger.familyYield[h.family] = fy;
      }
    }
  }

  // Deduplicate
  const genMap = new Map();
  for (const g of ledger.generators) {
    if (!genMap.has(g.demandGeneratorId)) genMap.set(g.demandGeneratorId, g);
  }
  ledger.generators = [...genMap.values()];
  const beforeGate = ledger.hidden.length;
  ledger.hidden = dedupeHiddenDemands(ledger.hidden).filter(
    (h) => passesHiddenDemandQualityGate(h).ok
  );
  ledger.rejectedByQualityGate = beforeGate - ledger.hidden.length;
  // Rebuild family yield after gate
  ledger.familyYield = Object.fromEntries(
    Object.values(DEMAND_FAMILY).map((f) => [f, { candidates: 0, lodging: 0 }])
  );
  for (const h of ledger.hidden) {
    const fy = ledger.familyYield[h.family] || { candidates: 0, lodging: 0 };
    fy.candidates += 1;
    if (
      h.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.STRONG ||
      h.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.MEDIUM
    ) {
      fy.lodging += 1;
    }
    ledger.familyYield[h.family] = fy;
  }
  ledger.lodgingSupported = ledger.hidden.filter(
    (h) =>
      h.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.STRONG ||
      h.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.MEDIUM
  );
  ledger.promotableDepth = ledger.hidden.filter((h) =>
    CUSTOMER_PROMOTABLE_DEPTHS.includes(h.discoveryDepth)
  );

  return ledger;
}

function mapFamilyToLayer(family) {
  const m = {
    [DEMAND_FAMILY.EXHIBITOR_VENDOR]: "EXHIBITOR",
    [DEMAND_FAMILY.PRODUCTION_CREW]: "CREW",
    [DEMAND_FAMILY.CORPORATE_PROJECT]: "CORPORATE_TEAM",
    [DEMAND_FAMILY.TRAINING]: "CORPORATE_TEAM",
    [DEMAND_FAMILY.TOUR_SERIES]: "TOUR_OPERATOR",
    [DEMAND_FAMILY.DELEGATION]: "DELEGATION",
    [DEMAND_FAMILY.EDUCATION]: "EDUCATION",
    [DEMAND_FAMILY.SPORTS_ADJACENT]: "SPORTS_ADJACENT",
    [DEMAND_FAMILY.AGENCY]: "AGENCY",
    [DEMAND_FAMILY.FASHION]: "AGENCY",
    [DEMAND_FAMILY.MEDICAL_PHARMA]: "CORPORATE_TEAM",
    [DEMAND_FAMILY.SOCIAL]: "SOCIAL",
    [DEMAND_FAMILY.ASSOCIATION_SUBGROUP]: "ASSOCIATION_SUBGROUP",
  };
  return m[family] || "EXHIBITOR";
}

/**
 * Full pipeline: market discovery → multi-hotel match → candidates + reclass existing.
 */
export async function runHiddenDemandMultiHotelCycle({
  hotelConfigs = [],
  marketLabel = "New York Midtown",
  marketKey = "nyc_midtown",
  discoveryOpts = {},
  existingByHotel = {},
} = {}) {
  const discovery = await runMarketHiddenDemandDiscovery({
    marketLabel,
    marketKey,
    ...discoveryOpts,
  });

  const depthCounts = {
    OBVIOUS_MARKET_DEMAND: 0,
    ONE_LAYER_DEEP: 0,
    TWO_LAYERS_DEEP: 0,
    DIRECT_HIDDEN_SIGNAL: 0,
  };
  for (const h of discovery.hidden) {
    depthCounts[h.discoveryDepth] = (depthCounts[h.discoveryDepth] || 0) + 1;
  }

  const hotelResults = {};
  const sharedMatrix = {
    both: [],
    hiltonOnly: [],
    renaissanceOnly: [],
    neither: [],
    byHotel: {},
  };

  for (const cfg of hotelConfigs) {
    hotelResults[cfg.hotelId] = {
      hotelId: cfg.hotelId,
      displayName: cfg.displayName,
      matches: [],
      candidates: [],
      matched: 0,
      actionable: 0,
      watch: 0,
      dq: 0,
    };
  }

  for (const hd of discovery.hidden) {
    const matches = matchHiddenDemandToHotels(hd, hotelConfigs);
    const strong = [];
    for (const m of matches) {
      const hr = hotelResults[m.hotelId];
      if (!hr) continue;
      hr.matches.push(m);
      const cand = hotelMatchToOpportunityCandidate(hd, m, hotelConfigs.find((c) => c.hotelId === m.hotelId));
      hr.candidates.push(cand);
      if (m.decision === "INSUFFICIENT") hr.dq += 1;
      else {
        hr.matched += 1;
        if (cand.customerFacingState === "ACTIONABLE_NOW") hr.actionable += 1;
        else if (cand.customerFacingState === "WATCH") hr.watch += 1;
        else hr.dq += 1;
        if (m.decision === "MATCH" || m.decision === "MATCH_STRONG") strong.push(m.hotelId);
      }
    }
    if (strong.length >= 2) {
      sharedMatrix.both.push({
        hiddenDemandId: hd.hiddenDemandId,
        organizationName: hd.organizationName,
        title: hd.title,
        family: hd.family,
        matches: matches.map((m) => ({
          hotelId: m.hotelId,
          hotelName: m.hotelName,
          fit: m.fitScore,
          motion: m.commercialMotion,
          decision: m.decision,
          priority: m.fitScore >= 80 ? "HIGH" : m.fitScore >= 65 ? "MEDIUM" : "WATCH",
        })),
      });
    } else if (strong.length === 1) {
      // generic only/neither buckets filled by caller naming if needed
      sharedMatrix.byHotel[strong[0]] = sharedMatrix.byHotel[strong[0]] || [];
      sharedMatrix.byHotel[strong[0]].push(hd.hiddenDemandId);
    } else {
      sharedMatrix.neither.push(hd.hiddenDemandId);
    }
  }

  // Reclassify existing bags
  const reclass = {};
  for (const cfg of hotelConfigs) {
    const existing = existingByHotel[cfg.hotelId] || [];
    const rows = existing.map((opp) => ({
      id: opp.id,
      title: opp.title,
      ...reclassifyExistingOpportunity(opp),
    }));
    const tallies = {
      GENUINELY_HIDDEN: 0,
      OBVIOUS_GENERATOR_ONLY: 0,
      DEEPER_MOTION_EXISTS: 0,
      NEEDS_MORE_RESEARCH: 0,
      DEEPENED: 0,
      REMOVED_DOWNGRADED: 0,
    };
    for (const r of rows) {
      tallies[r.label] = (tallies[r.label] || 0) + 1;
      if (r.action === "DOWNGRADE_TO_GENERATOR") tallies.REMOVED_DOWNGRADED += 1;
    }
    reclass[cfg.hotelId] = { rows, tallies };
  }

  return {
    discovery,
    depthCounts,
    hotelResults,
    sharedMatrix,
    reclass,
    duplicateFetchesAvoided: Math.max(0, (hotelConfigs.length - 1) * discovery.fetches),
    sharedMarketFetches: discovery.fetches,
  };
}

export { reclassifyExistingOpportunity, inferHiddenMotion };
export { passesHiddenDemandQualityGate, isPlausibleOrganizationName } from "./quality-gate.js";
export { decideHiddenDemandNextLayer, HIDDEN_DEMAND_NEXT_LAYER } from "./jev-next-layer.js";
export {
  decideExhibitorTeamResearchPath,
  decideLodgingEvidenceNextStep,
} from "./jev-v3-routing.js";
export {
  runHiddenDemandV3Reprocess,
  loadV2StrictSurvivors,
  HIDDEN_DEMAND_V3,
  CANDIDATE_STATE,
} from "./reprocess-v3.js";
export { buildHiddenDemandMarketQueries } from "./discovery-queries.js";
export {
  evaluateHotelHiddenDemandFit,
  buildHotelSpecificThesis,
  buildHotelWhyNow,
  buildHotelRecommendedAction,
} from "./hotel-match.js";
export {
  HIDDEN_DEMAND_VERSION,
  DISCOVERY_DEPTH,
  DEMAND_FAMILY,
  HIDDEN_MOTION,
  LODGING_SIGNAL_STRENGTH,
  RECLASS_LABEL,
} from "./constants.js";
export {
  computeDemandGeneratorId,
  computeHiddenDemandId,
  computeHotelMatchId,
  computeHotelOpportunityId,
} from "./identity.js";

