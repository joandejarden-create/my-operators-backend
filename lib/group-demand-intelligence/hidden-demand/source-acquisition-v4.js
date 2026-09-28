/**
 * GDI Hidden Demand V4 — NYC-first structured source acquisition.
 * Source discovery → geo/timing/richness validation → extract → team/lodging/WHO → hotel match.
 */

import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../hotel-intelligence/room-count-research/fetch.js";
import { buildNycSourceAcquisitionQueries } from "./v4-routing-queries.js";
import {
  classifySourceCandidate,
  confirmSourceAfterFetch,
  shouldDeepParse,
  computeSourceId,
} from "./v4-source-validate.js";
import {
  HIDDEN_DEMAND_V4,
  SOURCE_STATUS,
  SOURCE_FAMILY,
  TEAM_EVIDENCE,
} from "./v4-constants.js";
import { decideSourceValidationNextStep } from "./jev-v4-source-validation.js";
import { decideStructuredSourcePriority } from "./jev-structured-source.js";
import {
  decideExhibitorTeamResearchPath,
  decideLodgingEvidenceNextStep,
} from "./jev-v3-routing.js";
import { extractEntitiesFromHtmlDirectory } from "./extract-directory.js";
import { fetchAndExtractPdf, extractHousingSignalsFromText } from "./extract-pdf.js";
import { dedupeExtractedEntities } from "./entity-normalize.js";
import { passesStructuredEntityQualityGate } from "./structured-quality-gate.js";
import { isV3QueueNoise, cleanEntityDisplayName } from "./v3-entity-clean.js";
import { deepenExhibitorEntity } from "./v3-team-research.js";
import {
  classifyTravelLikelihood,
  classifyLodgingProof,
  candidateStateFromEvidence,
  mayBecomeHotelOpportunity,
} from "./v3-lodging-ladder.js";
import { classifyOriginTravel, travelIncreasesLodging } from "./origin-travel.js";
import { triageExhibitorResearchValue } from "./v3-triage.js";
import {
  CANDIDATE_STATE,
  LODGING_PROOF,
  ADDRESSABILITY,
} from "./v3-states.js";
import {
  passesCustomerPromotionGateV3,
  mapContactDepthBucket,
} from "./v3-promotion-gate.js";
import { entityToHiddenDemand } from "./source-expansion-v2.js";
import {
  matchHiddenDemandToHotels,
  hotelMatchToOpportunityCandidate,
} from "./index.js";
import { SOURCE_TYPE, LODGING_SIGNAL_STRENGTH } from "./v2-constants.js";
import { computeDemandGeneratorId } from "./identity.js";

function hasSerp() {
  return !!(process.env.SERPAPI_API_KEY || process.env.SERPAPI_KEY);
}

function emptyJev() {
  return {
    calls: 0,
    safeApply: 0,
    sourceValidation: 0,
    structuredSourcePriority: 0,
    teamPath: 0,
    lodgingPath: 0,
    helpfulDifferent: 0,
    same: 0,
    wrong: 0,
    highConfWrong: 0,
    noisyFetchesAvoided: 0,
    wrongGeoFetchesAvoided: 0,
    validSourcesViaJev: 0,
    teamAttributable: 0,
    lodgingAttributable: 0,
    feedback: [],
  };
}

function recordJev(ledger, row) {
  ledger.jev.feedback.push(row);
  ledger.jev.calls += 1;
  if (row.applied) ledger.jev.safeApply += 1;
  if (row.decisionType === "SOURCE_VALIDATION_NEXT_STEP") ledger.jev.sourceValidation += 1;
  if (row.decisionType === "STRUCTURED_SOURCE_PRIORITY") ledger.jev.structuredSourcePriority += 1;
  if (row.decisionType === "EXHIBITOR_TEAM_RESEARCH_PATH") ledger.jev.teamPath += 1;
  if (row.decisionType === "LODGING_EVIDENCE_NEXT_STEP") ledger.jev.lodgingPath += 1;
  if (row.outcome === "HELPFUL_DIFFERENT") ledger.jev.helpfulDifferent += 1;
  else if (row.outcome === "SAME") ledger.jev.same += 1;
  else if (row.outcome === "WRONG") ledger.jev.wrong += 1;
}

function lodgingToV2(proof) {
  if (proof === LODGING_PROOF.CONFIRMED) return LODGING_SIGNAL_STRENGTH.STRONG;
  if (proof === LODGING_PROOF.STRONG_INFERENCE) return LODGING_SIGNAL_STRENGTH.MEDIUM;
  if (proof === LODGING_PROOF.PLAUSIBLE) return LODGING_SIGNAL_STRENGTH.WEAK;
  return LODGING_SIGNAL_STRENGTH.UNKNOWN;
}

function teamEvidenceLevel(team = {}) {
  if (!team.teamSupported) return TEAM_EVIDENCE.UNKNOWN;
  if (team.namedPeople?.length >= 2 || (team.companyEventPage && team.speakerStaffSignal)) {
    return TEAM_EVIDENCE.STRONG;
  }
  if (team.namedPeople?.length >= 1 || team.companyEventPage || team.speakerStaffSignal) {
    return TEAM_EVIDENCE.MEDIUM;
  }
  return TEAM_EVIDENCE.WEAK;
}

function emptyTypeStats() {
  const types = [
    "EXHIBITOR_DIRECTORY",
    "SPONSOR_DIRECTORY",
    "HOUSING",
    "PROGRAM",
    "TOUR",
    "EDUCATION",
    "DELEGATION",
    "PRODUCTION",
    "AGENCY",
    "OTHER",
  ];
  return Object.fromEntries(
    types.map((t) => [
      t,
      { found: 0, fetched: 0, validEntities: 0, team: 0, lodging: 0, hotelOpps: 0 },
    ])
  );
}

function mapTypeBucket(sourceType, family) {
  if (sourceType === SOURCE_TYPE.EXHIBITOR_DIRECTORY) return "EXHIBITOR_DIRECTORY";
  if (sourceType === SOURCE_TYPE.SPONSOR_DIRECTORY) return "SPONSOR_DIRECTORY";
  if (sourceType === SOURCE_TYPE.HOUSING_PDF || family === SOURCE_FAMILY.EXHIBITOR_HOUSING) {
    return "HOUSING";
  }
  if (String(sourceType).includes("PROGRAM") || sourceType === SOURCE_TYPE.PROGRAM_PDF) {
    return "PROGRAM";
  }
  if (family === SOURCE_FAMILY.TOUR_SERIES) return "TOUR";
  if (family === SOURCE_FAMILY.EDUCATION) return "EDUCATION";
  if (family === SOURCE_FAMILY.DELEGATION) return "DELEGATION";
  if (family === SOURCE_FAMILY.EVENT_SERVICE) return "PRODUCTION";
  if (family === SOURCE_FAMILY.ENTERTAINMENT_PRODUCTION) return "AGENCY";
  return "OTHER";
}

/**
 * Main V4 acquisition run.
 */
export async function runHiddenDemandSourceAcquisitionV4({
  hotelConfigs = [],
  year = 2027,
  marketKey = "nyc_midtown",
  maxRoutingQueries = 40,
  maxSourceCandidates = 120,
  maxDirectoryFetches = 40,
  maxPdfFetches = 20,
  maxRendered = 25,
  maxEntityFollowups = 20,
  maxDeepEntities = 12,
  enableJev = true,
  enableLiveResearch = true,
} = {}) {
  const ledger = {
    version: HIDDEN_DEMAND_V4,
    startedAt: new Date().toISOString(),
    year,
    marketKey,
    routingQueries: 0,
    sourceCandidates: [],
    validatedSources: [],
    rejected: {
      WRONG_GEOGRAPHY: 0,
      STALE: 0,
      LOW_RICHNESS: 0,
      PDF_NOISE: 0,
      UI_CHROME: 0,
      GENERIC_CALENDAR: 0,
      IRRELEVANT: 0,
      UNKNOWN: 0,
    },
    validNycStructured: 0,
    validNycUnstructured: 0,
    typeStats: emptyTypeStats(),
    fetches: { directory: 0, pdf: 0, rendered: 0, total: 0, queries: 0 },
    rawEntities: [],
    entities: [],
    rows: [],
    evidencePackets: [],
    sourceGraph: [],
    housingSignals: null,
    hotelResults: {},
    jev: emptyJev(),
    economics: { fetchCostUnits: 0, validEntities: 0, lodgingEntities: 0, hotelOpps: 0 },
  };

  for (const cfg of hotelConfigs) {
    ledger.hotelResults[cfg.hotelId] = {
      hotelId: cfg.hotelId,
      displayName: cfg.displayName,
      hotelOpportunity: 0,
      actionable: 0,
      watch: 0,
      candidates: [],
      contactBuckets: {
        NAMED_DIRECT: 0,
        NAMED_PARTIAL: 0,
        FUNCTIONAL: 0,
        ORG_PATH: 0,
        NO_CONTACT: 0,
      },
    };
  }

  if (!enableLiveResearch || !hasSerp()) {
    ledger.finishedAt = new Date().toISOString();
    ledger.note = "SERP unavailable or live research disabled";
    return ledger;
  }

  // --- Phase 1: routing queries → source candidates (no extract) ---
  const queries = buildNycSourceAcquisitionQueries({ year, max: maxRoutingQueries });
  const seenUrl = new Set();
  const candidates = [];

  console.error(`[hd-v4] routing ${queries.length} queries…`);
  for (const q of queries) {
    ledger.routingQueries += 1;
    ledger.fetches.queries += 1;
    if (ledger.routingQueries % 10 === 0) {
      console.error(`[hd-v4] queries ${ledger.routingQueries}/${queries.length} candidates=${candidates.length}`);
    }
    try {
      const serp = await serpapiSearch({
        engine: "google",
        q: q.query,
        num: 8,
        hl: "en",
        gl: "us",
      });
      for (const hit of serp?.data?.organic_results || []) {
        const url = hit.link;
        if (!url || seenUrl.has(url)) continue;
        seenUrl.add(url);
        if (candidates.length >= maxSourceCandidates) break;
        const cand = classifySourceCandidate(
          {
            url,
            title: hit.title,
            snippet: hit.snippet,
            family: q.family,
          },
          { year }
        );
        cand.queryId = q.queryId;
        cand.family = q.family;
        candidates.push(cand);

        const bucket = mapTypeBucket(cand.sourceType, cand.family);
        ledger.typeStats[bucket].found += 1;

        if (cand.status === SOURCE_STATUS.WRONG_GEOGRAPHY) ledger.rejected.WRONG_GEOGRAPHY += 1;
        else if (cand.status === SOURCE_STATUS.STALE) ledger.rejected.STALE += 1;
        else if (cand.status === SOURCE_STATUS.PDF_NOISE) ledger.rejected.PDF_NOISE += 1;
        else if (cand.status === SOURCE_STATUS.UI_CHROME) ledger.rejected.UI_CHROME += 1;
        else if (cand.status === SOURCE_STATUS.GENERIC_CALENDAR) {
          ledger.rejected.GENERIC_CALENDAR += 1;
        } else if (
          cand.status === SOURCE_STATUS.IRRELEVANT ||
          cand.richness === "LOW" ||
          cand.richness === "NONE"
        ) {
          ledger.rejected.LOW_RICHNESS += 1;
        } else if (cand.status === SOURCE_STATUS.UNKNOWN) ledger.rejected.UNKNOWN += 1;
        else if (cand.status === SOURCE_STATUS.VALID_NYC_STRUCTURED) {
          ledger.validNycStructured += 1;
        } else if (cand.status === SOURCE_STATUS.VALID_NYC_UNSTRUCTURED) {
          ledger.validNycUnstructured += 1;
        }
      }
    } catch {
      /* continue */
    }
    if (candidates.length >= maxSourceCandidates) break;
  }

  ledger.sourceCandidates = candidates;

  // Prefer structured + housing
  const fetchQueue = candidates
    .filter((c) => c.mayFetch || c.status === SOURCE_STATUS.VALID_NYC_STRUCTURED)
    .sort((a, b) => {
      const rank = (c) =>
        (c.status === SOURCE_STATUS.VALID_NYC_STRUCTURED ? 100 : 0) +
        (c.richness === "HIGH" ? 50 : c.richness === "MEDIUM" ? 25 : 0) +
        (c.family === SOURCE_FAMILY.EXHIBITOR_HOUSING ? 40 : 0) +
        (c.family === SOURCE_FAMILY.JAVITS_EXHIBITOR ? 30 : 0);
      return rank(b) - rank(a);
    });

  // Optional Jev structured source priority once
  if (enableJev && fetchQueue.length) {
    try {
      const pri = await decideStructuredSourcePriority({
        demandFamily: "EXHIBITOR_VENDOR",
        sourcesChecked: [],
        sourceCandidates: fetchQueue.slice(0, 12),
        lodgingGap: true,
        contactGap: true,
        remainingBudget: maxDirectoryFetches + maxPdfFetches,
        enableSafeApply: true,
      });
      recordJev(ledger, {
        decisionType: "STRUCTURED_SOURCE_PRIORITY",
        defaultPath: pri.defaultPath,
        jevPath: pri.jevPath,
        finalPath: pri.finalPath,
        applied: pri.applied,
        outcome: pri.applied ? "HELPFUL_DIFFERENT" : pri.agreement ? "SAME" : "UNKNOWN",
      });
    } catch {
      /* non-fatal */
    }
  }

  const allEntities = [];
  let sharedHousing = null;

  // Snapshot fetch queue — do not mutate while iterating (FIND_* pushes go to secondary)
  const primaryFetch = fetchQueue.slice(0, Math.min(fetchQueue.length, 45));
  const secondaryFetch = [];

  // --- Phase 2: Jev validate → fetch → confirm geo from page → extract ---
  const toProcess = [...primaryFetch];
  for (let i = 0; i < toProcess.length; i += 1) {
    const cand = toProcess[i];
    if (ledger.fetches.directory >= maxDirectoryFetches && ledger.fetches.pdf >= maxPdfFetches) {
      break;
    }
    if (i > 0 && i % 5 === 0) {
      console.error(
        `[hd-v4] source progress ${i}/${toProcess.length} dir=${ledger.fetches.directory} pdf=${ledger.fetches.pdf} entities=${allEntities.length}`
      );
    }

    let action = "FETCH_STATIC";
    // Skip Jev for clear rejects — only route borderline / fetchable sources
    const skipJev =
      cand.status === SOURCE_STATUS.WRONG_GEOGRAPHY ||
      cand.status === SOURCE_STATUS.UI_CHROME ||
      cand.status === SOURCE_STATUS.STALE ||
      cand.status === SOURCE_STATUS.PDF_NOISE ||
      cand.richness === "NONE";

    if (skipJev) {
      action = defaultFetchAction(cand);
      if (String(action).startsWith("REJECT") || action === "STOP") {
        ledger.jev.noisyFetchesAvoided += 1;
        continue;
      }
    } else if (enableJev) {
      try {
        const dec = await decideSourceValidationNextStep({
          candidate: cand,
          remainingBudget:
            maxDirectoryFetches + maxPdfFetches - ledger.fetches.directory - ledger.fetches.pdf,
          enableSafeApply: true,
        });
        recordJev(ledger, {
          decisionType: "SOURCE_VALIDATION_NEXT_STEP",
          sourceId: cand.sourceId,
          defaultPath: dec.defaultPath,
          jevPath: dec.jevPath,
          finalPath: dec.finalPath,
          applied: dec.applied,
          outcome: dec.applied ? "HELPFUL_DIFFERENT" : dec.agreement ? "SAME" : "UNKNOWN",
        });
        action = dec.finalPath;
        if (action === "REJECT_WRONG_GEO") {
          ledger.jev.wrongGeoFetchesAvoided += 1;
          ledger.jev.noisyFetchesAvoided += 1;
          continue;
        }
        if (action === "REJECT_LOW_RICHNESS" || action === "STOP") {
          ledger.jev.noisyFetchesAvoided += 1;
          continue;
        }
        if (dec.applied && (action === "FETCH_STATIC" || action === "FETCH_PDF" || action === "FETCH_RENDERED")) {
          ledger.jev.validSourcesViaJev += 1;
        }
      } catch {
        action = defaultFetchAction(cand);
      }
    } else {
      action = defaultFetchAction(cand);
    }

    if (action === "FIND_DIRECTORY" || action === "FIND_HOUSING" || action === "FIND_OFFICIAL_EVENT_PAGE") {
      // Bounded secondary SERP from this candidate domain
      try {
        const host = new URL(cand.sourceURL).hostname.replace(/^www\./, "");
        const subQ =
          action === "FIND_HOUSING"
            ? `site:${host} official housing OR hotel block OR exhibitor housing ${year}`
            : `site:${host} exhibitor directory OR exhibitor list OR who's exhibiting ${year}`;
        ledger.fetches.queries += 1;
        const serp = await serpapiSearch({ engine: "google", q: subQ, num: 5, hl: "en", gl: "us" });
        for (const hit of serp?.data?.organic_results || []) {
          if (!hit.link || seenUrl.has(hit.link)) continue;
          seenUrl.add(hit.link);
          const sub = classifySourceCandidate(
            { url: hit.link, title: hit.title, snippet: hit.snippet, family: cand.family },
            { year }
          );
          if (sub.mayFetch) secondaryFetch.push(sub);
        }
      } catch {
        /* continue */
      }
      continue;
    }

    const isPdf = action === "FETCH_PDF" || cand.requiresPDF;
    const isRendered = action === "FETCH_RENDERED" || cand.requiresRender;

    if (isPdf && ledger.fetches.pdf >= maxPdfFetches) continue;
    if (!isPdf && ledger.fetches.directory >= maxDirectoryFetches) continue;
    if (isRendered && ledger.fetches.rendered >= maxRendered) {
      // still allow static fallback
    }

    ledger.fetches.total += 1;
    ledger.economics.fetchCostUnits += isRendered ? 2 : 1;
    const bucket = mapTypeBucket(cand.sourceType, cand.family);
    ledger.typeStats[bucket].fetched += 1;

    try {
      if (isPdf) {
        ledger.fetches.pdf += 1;
        const pdf = await fetchAndExtractPdf(cand.sourceURL, {
          demandGeneratorName: cand.title,
          year,
          family: "EXHIBITOR_VENDOR",
        });
        if (!pdf.ok) continue;
        const confirmed = confirmSourceAfterFetch(cand, pdf.text || JSON.stringify(pdf.entities || []).slice(0, 2000));
        ledger.validatedSources.push(confirmed);
        if (confirmed.status === SOURCE_STATUS.WRONG_GEOGRAPHY) {
          ledger.rejected.WRONG_GEOGRAPHY += 1;
          continue;
        }
        if (!shouldDeepParse(confirmed)) continue;

        if (pdf.housing?.roomBlockMentioned) {
          sharedHousing = pdf.housing;
          ledger.housingSignals = pdf.housing;
        }
        // Housing PDFs enrich lodging; entity extraction only if participant-rich
        if (
          confirmed.family === SOURCE_FAMILY.EXHIBITOR_HOUSING ||
          confirmed.sourceType === SOURCE_TYPE.HOUSING_PDF
        ) {
          // attach housing to ledger; skip bulk entity create from housing-only PDF
          continue;
        }
        for (const e of pdf.entities || []) {
          allEntities.push({
            ...e,
            demandGeneratorName: cand.title,
            sourceURL: cand.sourceURL,
            sourceType: cand.sourceType,
            _sourceId: cand.sourceId,
            year: confirmed.timingYear || year,
            futureTiming: true,
          });
        }
        continue;
      }

      ledger.fetches.directory += 1;
      if (isRendered) ledger.fetches.rendered += 1;

      const page = await fetchResearchPage(cand.sourceURL);
      if (!page.ok) continue;
      const html = page.text || "";
      const text = htmlToSearchableText(html);
      const confirmed = confirmSourceAfterFetch(cand, text);
      ledger.validatedSources.push(confirmed);

      if (confirmed.status === SOURCE_STATUS.WRONG_GEOGRAPHY) {
        ledger.rejected.WRONG_GEOGRAPHY += 1;
        continue;
      }
      if (!shouldDeepParse(confirmed)) {
        if (confirmed.richness === "LOW" || confirmed.richness === "NONE") {
          ledger.rejected.LOW_RICHNESS += 1;
        }
        continue;
      }

      const housing = extractHousingSignalsFromText(text, page.url || cand.sourceURL);
      if (housing.roomBlockMentioned || housing.housingPageFound) {
        sharedHousing = housing;
        ledger.housingSignals = housing;
      }

      if (
        confirmed.family === SOURCE_FAMILY.EXHIBITOR_HOUSING &&
        confirmed.sourceType !== SOURCE_TYPE.EXHIBITOR_DIRECTORY
      ) {
        // housing page enrichment — no entity flood
        continue;
      }

      const genId = computeDemandGeneratorId({
        name: confirmed.title || cand.title,
        marketKey,
        year: confirmed.timingYear || year,
      });

      const extracted = extractEntitiesFromHtmlDirectory(html, {
        sourceType:
          confirmed.sourceType === SOURCE_TYPE.GENERIC_SERP
            ? SOURCE_TYPE.EXHIBITOR_DIRECTORY
            : confirmed.sourceType,
        sourceURL: page.url || cand.sourceURL,
        demandGeneratorId: genId,
        demandGeneratorName: `${confirmed.title || cand.title} — New York`,
        year: confirmed.timingYear || year,
        family: "EXHIBITOR_VENDOR",
        futureTiming: true,
      });

      ledger.sourceGraph.push({
        from: genId,
        to: confirmed.sourceId,
        kind: "GENERATOR_TO_SOURCE",
        url: confirmed.sourceURL,
      });

      for (const e of extracted) {
        e.entityName = cleanEntityDisplayName(e.entityName);
        e._sourceId = confirmed.sourceId;
        e.demandGeneratorId = genId;
        allEntities.push(e);
        ledger.sourceGraph.push({
          from: confirmed.sourceId,
          to: e.normalizeKey || e.entityName,
          kind: "SOURCE_TO_ENTITY",
          url: confirmed.sourceURL,
        });
      }
    } catch {
      /* continue */
    }
  }

  // Process bounded secondary FIND_* discoveries without further expansion
  for (const cand of secondaryFetch.slice(0, 15)) {
    if (ledger.fetches.directory >= maxDirectoryFetches && ledger.fetches.pdf >= maxPdfFetches) break;
    if (!cand.mayFetch) continue;
    try {
      const isPdf = cand.requiresPDF;
      if (isPdf) {
        if (ledger.fetches.pdf >= maxPdfFetches) continue;
        ledger.fetches.pdf += 1;
        ledger.fetches.total += 1;
        const pdf = await fetchAndExtractPdf(cand.sourceURL, {
          demandGeneratorName: cand.title,
          year,
          family: "EXHIBITOR_VENDOR",
        });
        if (!pdf.ok) continue;
        const confirmed = confirmSourceAfterFetch(cand, pdf.text || "");
        ledger.validatedSources.push(confirmed);
        if (pdf.housing?.roomBlockMentioned) {
          sharedHousing = pdf.housing;
          ledger.housingSignals = pdf.housing;
        }
        continue;
      }
      if (ledger.fetches.directory >= maxDirectoryFetches) continue;
      ledger.fetches.directory += 1;
      ledger.fetches.total += 1;
      const page = await fetchResearchPage(cand.sourceURL);
      if (!page.ok) continue;
      const text = htmlToSearchableText(page.text || "");
      const confirmed = confirmSourceAfterFetch(cand, text);
      ledger.validatedSources.push(confirmed);
      if (!shouldDeepParse(confirmed)) continue;
      const housing = extractHousingSignalsFromText(text, page.url || cand.sourceURL);
      if (housing.roomBlockMentioned) {
        sharedHousing = housing;
        ledger.housingSignals = housing;
      }
      const genId = computeDemandGeneratorId({
        name: confirmed.title || cand.title,
        marketKey,
        year: confirmed.timingYear || year,
      });
      const extracted = extractEntitiesFromHtmlDirectory(page.text || "", {
        sourceType: SOURCE_TYPE.EXHIBITOR_DIRECTORY,
        sourceURL: page.url || cand.sourceURL,
        demandGeneratorId: genId,
        demandGeneratorName: `${confirmed.title || cand.title} — New York`,
        year: confirmed.timingYear || year,
        family: "EXHIBITOR_VENDOR",
        futureTiming: true,
      });
      for (const e of extracted) {
        e.entityName = cleanEntityDisplayName(e.entityName);
        e._sourceId = confirmed.sourceId;
        e.demandGeneratorId = genId;
        allEntities.push(e);
      }
    } catch {
      /* continue */
    }
  }

  ledger.rawEntities = allEntities;

  // --- Phase 3: entity quality gate ---
  const gated = [];
  for (const ent of dedupeExtractedEntities(allEntities)) {
    if (isV3QueueNoise(ent)) continue;
    const gate = passesStructuredEntityQualityGate(ent);
    if (!gate.ok) continue;
    // Must be NYC generator
    if (!/new\s*york|nyc|javits|midtown/i.test(ent.demandGeneratorName || "")) continue;
    gated.push(ent);
    const src = ledger.validatedSources.find((s) => s.sourceId === ent._sourceId);
    const bucket = mapTypeBucket(ent.sourceType, src?.family);
    ledger.typeStats[bucket].validEntities += 1;
  }
  ledger.entities = gated;
  ledger.economics.validEntities = gated.length;

  // --- Phase 4: deep team/lodging/WHO for top entities ---
  const ranked = [...gated]
    .map((e) => ({ e, t: triageExhibitorResearchValue(e) }))
    .filter((x) => x.t.triage !== "STOP")
    .sort((a, b) => b.t.score - a.t.score)
    .slice(0, maxDeepEntities);

  let followups = 0;
  console.error(`[hd-v4] deep research on ${ranked.length} entities (cap followups=${maxEntityFollowups})`);
  for (const { e: entity, t: triage } of ranked) {
    if (followups >= maxEntityFollowups) break;
    followups += 1;
    console.error(`[hd-v4] deepen ${followups}/${Math.min(ranked.length, maxEntityFollowups)} ${entity.entityName}`);

    let pathHint = "COMPANY_EVENT_PAGE";
    if (enableJev) {
      try {
        const teamDec = await decideExhibitorTeamResearchPath({
          company: entity.entityName,
          eventRelationship: entity.participationRole,
          origin: triage.locality,
          participationIntensity: triage.participation?.participationDepth,
          knownEmployees: 0,
          travelEvidence: "unknown",
          gaps: ["team", "travel", "who"],
          remainingBudget: 8,
          enableSafeApply: true,
        });
        recordJev(ledger, {
          decisionType: "EXHIBITOR_TEAM_RESEARCH_PATH",
          company: entity.entityName,
          defaultPath: teamDec.defaultPath,
          jevPath: teamDec.jevPath,
          finalPath: teamDec.finalPath,
          applied: teamDec.applied,
          outcome: teamDec.applied ? "HELPFUL_DIFFERENT" : teamDec.agreement ? "SAME" : "UNKNOWN",
        });
        pathHint = teamDec.finalPath;
        if (String(pathHint).startsWith("STOP")) {
          ledger.jev.noisyFetchesAvoided += 1;
          continue;
        }
      } catch {
        /* default */
      }
    }

    const deepen = await deepenExhibitorEntity(entity, {
      maxQueries: 3,
      maxFetches: 5,
      pathHint,
    });
    ledger.fetches.queries += deepen.queryCount || 0;
    ledger.fetches.total += deepen.fetchCount || 0;

    if (enableJev) {
      try {
        const lodDec = await decideLodgingEvidenceNextStep({
          teamEvidence: deepen.team?.teamSupported ? "present" : "gap",
          travelEvidence: "unknown",
          companyOrigin: triage.locality,
          eventDuration: deepen.team?.multiDay ? "multi_day" : "unknown",
          housingEvidence: deepen.housing || sharedHousing ? "present" : "unknown",
          sourcesChecked: (deepen.pages || []).map((p) => p.url),
          hasHousing: Boolean(deepen.housing || sharedHousing),
          hasTeam: deepen.team?.teamSupported,
          enableSafeApply: true,
        });
        recordJev(ledger, {
          decisionType: "LODGING_EVIDENCE_NEXT_STEP",
          company: entity.entityName,
          defaultPath: lodDec.defaultPath,
          jevPath: lodDec.jevPath,
          finalPath: lodDec.finalPath,
          applied: lodDec.applied,
          outcome: lodDec.applied ? "HELPFUL_DIFFERENT" : lodDec.agreement ? "SAME" : "UNKNOWN",
        });
        if (
          !String(lodDec.finalPath).startsWith("STOP") &&
          (lodDec.applied || lodDec.finalPath === "HOUSING_SOURCE")
        ) {
          const lodDeep = await deepenExhibitorEntity(entity, {
            maxQueries: 2,
            maxFetches: 3,
            pathHint: "TRAVEL_EVIDENCE",
          });
          deepen.deepenText += ` ${lodDeep.deepenText || ""}`;
          if (lodDeep.housing) deepen.housing = lodDeep.housing;
          if (lodDeep.team?.teamSupported) Object.assign(deepen.team, lodDeep.team);
          if (lodDeep.who?.length) deepen.who = [...(deepen.who || []), ...lodDeep.who];
          ledger.fetches.queries += lodDeep.queryCount || 0;
          ledger.fetches.total += lodDeep.fetchCount || 0;
        }
      } catch {
        /* non-fatal */
      }
    }

    const origin = classifyOriginTravel(entity, deepen.deepenText);
    const travelLikelihood = classifyTravelLikelihood({
      travelClass: origin.travelClass,
      teamSupported: deepen.team?.teamSupported,
      namedPeople: deepen.team?.namedPeople?.length || 0,
      deepenText: deepen.deepenText,
      accommodationsMention: Boolean(deepen.housing?.roomBlockMentioned || sharedHousing?.roomBlockMentioned),
    });
    const lodging = classifyLodgingProof({
      deepenText: deepen.deepenText,
      // Shared event housing alone is not company CONFIRMED — pass only company-page housing
      housingSignals: deepen.housing || null,
      travelLikelihood,
      travelClass: origin.travelClass,
      teamSupported: deepen.team?.teamSupported,
      namedPeople: deepen.team?.namedPeople?.length || 0,
      multiDay: deepen.team?.multiDay,
      setupBreakdown: deepen.team?.setupBreakdown,
    });
    // Event-level housing lifts to STRONG_INFERENCE for out-of-market teams, not CONFIRMED
    let lodgingProof = lodging.lodgingProof;
    if (
      !deepen.housing?.roomBlockMentioned &&
      sharedHousing?.roomBlockMentioned &&
      deepen.team?.teamSupported &&
      travelIncreasesLodging(origin.travelClass)
    ) {
      lodgingProof = LODGING_PROOF.STRONG_INFERENCE;
    } else if (
      !deepen.housing?.roomBlockMentioned &&
      sharedHousing?.roomBlockMentioned &&
      lodgingProof === LODGING_PROOF.CONFIRMED
    ) {
      lodgingProof = LODGING_PROOF.STRONG_INFERENCE;
    } else {
      lodgingProof = lodging.lodgingProof;
    }

    const teamLevel = teamEvidenceLevel(deepen.team);
    const addressability = deepen.addressability || ADDRESSABILITY.COMPANY_PATH;
    const contactDepth = mapContactDepthBucket(addressability, deepen.who || []);

    if (deepen.team?.teamSupported) {
      ledger.jev.teamAttributable += 1;
      const src = ledger.validatedSources.find((s) => s.sourceId === entity._sourceId);
      const bucket = mapTypeBucket(entity.sourceType, src?.family);
      ledger.typeStats[bucket].team += 1;
    }
    if (mayBecomeHotelOpportunity(lodgingProof)) {
      ledger.jev.lodgingAttributable += 1;
      ledger.economics.lodgingEntities += 1;
      const src = ledger.validatedSources.find((s) => s.sourceId === entity._sourceId);
      const bucket = mapTypeBucket(entity.sourceType, src?.family);
      ledger.typeStats[bucket].lodging += 1;
    }

    // Hotel match: allow fit scoring at PLAUSIBLE+; opportunity only CONFIRMED/STRONG
    let hotelMatched = false;
    const hotelMatches = [];
    const canScoreFit =
      lodgingProof === LODGING_PROOF.PLAUSIBLE ||
      mayBecomeHotelOpportunity(lodgingProof);
    const canBeOpportunity = mayBecomeHotelOpportunity(lodgingProof);

    if (canScoreFit && hotelConfigs.length) {
      const hd = entityToHiddenDemand(
        {
          ...entity,
          destination: "New York Midtown",
          contacts: deepen.who,
        },
        {
          lodgingSignalStrength: lodgingToV2(lodgingProof),
          lodgingScore: lodging.lodgingLevel,
          lodgingSignals: lodging.lodgingSignals,
          housingEvidence: deepen.housing || sharedHousing,
        },
        marketKey
      );
      hd.gdiVersion = HIDDEN_DEMAND_V4;
      hd.lodgingProof = lodgingProof;
      hd.teamSupported = deepen.team?.teamSupported;
      hd.evidence = {
        ...hd.evidence,
        travelingTeam: deepen.team?.teamSupported,
        lodgingProof: lodgingProof,
      };

      const matches = matchHiddenDemandToHotels(hd, hotelConfigs);
      for (const m of matches) {
        if (m.decision === "INSUFFICIENT") {
          hotelMatches.push({ hotelId: m.hotelId, decision: "NEITHER", fitScore: m.fitScore });
          continue;
        }
        if (!canBeOpportunity) {
          hotelMatches.push({
            hotelId: m.hotelId,
            decision: "WATCH_FIT_ONLY",
            fitScore: m.fitScore,
          });
          continue;
        }
        hotelMatched = true;
        const cand = hotelMatchToOpportunityCandidate(
          hd,
          m,
          hotelConfigs.find((c) => c.hotelId === m.hotelId)
        );
        cand.gdiVersion = HIDDEN_DEMAND_V4;
        cand.lodgingProof = lodgingProof;
        cand.teamEvidenceLevel = teamLevel;
        cand.primaryContact = deepen.who?.[0]
          ? { name: deepen.who[0].name, role: deepen.who[0].role, email: deepen.who[0].email }
          : null;
        cand.contactDepth = contactDepth;
        cand.addressability = addressability;
        cand.timingTrigger = "exhibitor_list_published";
        cand.recommendedAction = deepen.who?.[0]?.name
          ? `Contact ${deepen.who[0].name} (${deepen.who[0].role || "events"}) at ${entity.entityName} to confirm Midtown lodging for their event team.`
          : `Identify the ${entity.entityName} trade-show / field-marketing owner and confirm Midtown lodging needs.`;

        const gate = passesCustomerPromotionGateV3({
          company: entity.entityName,
          futureTiming: true,
          year: entity.year || year,
          participationEvidence: true,
          participationRole: entity.participationRole,
          teamSupported: deepen.team?.teamSupported,
          lodgingProof: lodgingProof,
          hotelMatched: true,
          sourceUrl: entity.sourceURL,
          sourceChain: [{ url: entity.sourceURL }],
          contactResearchAttempted: true,
          addressability,
          timingTrigger: cand.timingTrigger,
          recommendedAction: cand.recommendedAction,
          who: deepen.who,
          primaryContactName: deepen.who?.[0]?.name,
        });
        cand.customerFacingState = gate.customerState;
        cand.customerPromotable = gate.ok || gate.watchAllowed;
        cand.candidateState =
          gate.customerState === "ACTIONABLE_NOW"
            ? CANDIDATE_STATE.ACTIONABLE_NOW
            : CANDIDATE_STATE.HOTEL_OPPORTUNITY;

        hotelMatches.push({
          hotelId: m.hotelId,
          decision: m.decision,
          fitScore: m.fitScore,
          candidate: cand,
        });

        const hr = ledger.hotelResults[m.hotelId];
        if (hr) {
          hr.candidates.push(cand);
          hr.hotelOpportunity += 1;
          if (cand.customerFacingState === "ACTIONABLE_NOW") hr.actionable += 1;
          else if (cand.customerPromotable) hr.watch += 1;
          hr.contactBuckets[contactDepth] = (hr.contactBuckets[contactDepth] || 0) + 1;
          const src = ledger.validatedSources.find((s) => s.sourceId === entity._sourceId);
          const b = mapTypeBucket(entity.sourceType, src?.family);
          ledger.typeStats[b].hotelOpps += 1;
        }
      }
    }

    const state = candidateStateFromEvidence({
      teamSupported: deepen.team?.teamSupported,
      lodgingProof: lodgingProof,
      addressability,
      hotelMatched,
    });

    const row = {
      company: entity.entityName,
      generator: entity.demandGeneratorName,
      sourceType: entity.sourceType,
      sourceId: entity._sourceId,
      sourceURL: entity.sourceURL,
      teamSupported: deepen.team?.teamSupported,
      teamEvidence: teamLevel,
      team: deepen.team,
      lodgingProof: lodgingProof,
      travelLikelihood,
      locality: triage.locality,
      who: deepen.who || [],
      contactDepth,
      addressability,
      candidateState: state,
      hotelMatches: hotelMatches.map((h) => ({
        hotelId: h.hotelId,
        decision: h.decision,
        fitScore: h.fitScore,
        customerState: h.candidate?.customerFacingState || null,
      })),
      midtownEligible: true,
    };
    ledger.rows.push(row);
    ledger.evidencePackets.push({
      company: row.company,
      generator: row.generator,
      teamEvidence: row.team,
      lodgingEvidence: { proof: row.lodgingProof },
      who: row.who,
      hotelMatches: row.hotelMatches,
      sourceChain: [{ url: row.sourceURL, sourceId: row.sourceId }],
      candidateState: row.candidateState,
    });
  }

  ledger.economics.hotelOpps = ledger.rows.filter((r) =>
    [CANDIDATE_STATE.HOTEL_OPPORTUNITY, CANDIDATE_STATE.ACTIONABLE_NOW].includes(r.candidateState)
  ).length;

  ledger.shared = {
    both: [],
    hiltonOnly: [],
    renaissanceOnly: [],
  };
  const hotelIds = hotelConfigs.map((c) => c.hotelId);
  for (const r of ledger.rows) {
    const matched = (r.hotelMatches || []).filter(
      (h) => h.decision === "MATCH" || h.decision === "MATCH_STRONG"
    );
    if (matched.length >= 2) ledger.shared.both.push(r.company);
    else if (matched.length === 1) {
      if (matched[0].hotelId === hotelIds[0]) ledger.shared.hiltonOnly.push(r.company);
      else ledger.shared.renaissanceOnly.push(r.company);
    }
  }

  ledger.finishedAt = new Date().toISOString();
  return ledger;
}

function defaultFetchAction(cand) {
  if (cand.status === SOURCE_STATUS.WRONG_GEOGRAPHY) return "REJECT_WRONG_GEO";
  if (
    cand.status === SOURCE_STATUS.UI_CHROME ||
    cand.status === SOURCE_STATUS.IRRELEVANT ||
    cand.richness === "NONE"
  ) {
    return "REJECT_LOW_RICHNESS";
  }
  if (cand.requiresPDF) return "FETCH_PDF";
  if (cand.requiresRender) return "FETCH_RENDERED";
  if (cand.mayFetch) return "FETCH_STATIC";
  return "STOP";
}

export { HIDDEN_DEMAND_V4, SOURCE_STATUS, computeSourceId };
