/**
 * GDI Hidden Demand Source Expansion V2 — per-hotel source-family experiments (AC / Spice).
 */

import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../hotel-intelligence/room-count-research/fetch.js";
import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import { SOURCE_TYPE, LODGING_SIGNAL_STRENGTH } from "./v2-constants.js";
import {
  classifyStructuredSource,
  structuredSourceScore,
} from "./source-classifier.js";
import { extractEntitiesFromHtmlDirectory } from "./extract-directory.js";
import { fetchAndExtractPdf } from "./extract-pdf.js";
import { dedupeExtractedEntities } from "./entity-normalize.js";
import { qualifyEntityLodging } from "./lodging-qualify.js";
import { passesStructuredEntityQualityGate } from "./structured-quality-gate.js";
import { decideStructuredSourcePriority } from "./jev-structured-source.js";
import { decideHiddenDemandNextLayer } from "./jev-next-layer.js";
import { computeDemandGeneratorId } from "./identity.js";
import { inferDemandFamily } from "./classify.js";
import {
  matchHiddenDemandToHotels,
  hotelMatchToOpportunityCandidate,
} from "./index.js";
import { entityToHiddenDemand } from "./source-expansion-v2.js";
import { buildGdiOpportunitySummary } from "../opportunity-summary-v1.js";
import { isGdiCustomerOpportunityReady } from "../customer-readiness-gate-v1.js";
import { applyGdiWhoHowResolution } from "../opportunity-who-resolution-v1.js";
import {
  SOURCE_FAMILY,
  FAMILY_PRIOR_STATUS,
  PRIOR_FAMILY_STATUS,
  ADDRESSABLE_MOTION,
  LODGING_EVIDENCE,
  TIMING_CLASS,
  PLAYBOOK_VERDICT,
  familiesForHotel,
  buildMotionQueries,
  classifyMotionFromText,
  classifyLodgingEvidence,
  classifyTiming,
  scoreFamilyVerdict,
} from "./source-family-catalog-v2.js";
import { isPlausibleOrganizationName } from "./quality-gate.js";
import { signalToEntities } from "./index.js";

function hasSerp() {
  return !!(process.env.SERPAPI_API_KEY || process.env.SERPAPI_KEY);
}

function marketMeta(hotelId, cfg) {
  if (hotelId === "rec2PVBDavppGpenm") {
    return {
      marketKey: "a_coruna_galicia",
      marketLabel: "A Coruña / Galicia",
      serpHl: "es",
      serpGl: "es",
      cityTokens: ["a coruña", "coruña", "galicia", "palexco", "expocoruña", "matogrande"],
    };
  }
  return {
    marketKey: "grenada_caribbean",
    marketLabel: "Grenada / Grand Anse",
    serpHl: "en",
    serpGl: "gd",
    cityTokens: ["grenada", "grand anse", "st george", "carriacou"],
  };
}

function wrongCity(text = "", cityTokens = []) {
  const t = String(text).toLowerCase();
  if (!t.trim()) return true;
  return !cityTokens.some((c) => t.includes(c));
}

function motionToLodgingStrength(evidence) {
  if (evidence === LODGING_EVIDENCE.DIRECT) return LODGING_SIGNAL_STRENGTH.STRONG;
  if (evidence === LODGING_EVIDENCE.STRONG_INFERENCE) return LODGING_SIGNAL_STRENGTH.MEDIUM;
  if (evidence === LODGING_EVIDENCE.WEAK_INFERENCE) return LODGING_SIGNAL_STRENGTH.WEAK;
  return LODGING_SIGNAL_STRENGTH.UNKNOWN;
}

function recordJev(ledger, row) {
  ledger.jev.calls += 1;
  ledger.jev.feedback.push(row);
  if (row.outcome === "HELPFUL_DIFFERENT") ledger.jev.helpfulDifferent += 1;
  else if (row.outcome === "SAME") ledger.jev.same += 1;
  else if (row.outcome === "WRONG") ledger.jev.wrong += 1;
  if (row.estimatedFetchesSaved) ledger.jev.fetchesAvoided += row.estimatedFetchesSaved;
}

/**
 * Run bounded source-family experiments for one hotel.
 */
export async function runHotelSourceFamilyExpansion(hotelConfig, opts = {}) {
  const hotelId = hotelConfig.hotelId;
  const meta = marketMeta(hotelId, hotelConfig);
  const year = opts.year || 2027;
  const maxQueries = opts.maxQueries ?? 60;
  const maxFetches = opts.maxFetches ?? 100;
  const maxRendered = opts.maxRendered ?? 20;
  const maxPdf = opts.maxPdfDocs ?? 25;
  const enableJev = opts.enableJev !== false;
  const families = (opts.families || familiesForHotel(hotelId)).filter((f) => {
    const prior = PRIOR_FAMILY_STATUS[hotelId]?.[f];
    return prior !== FAMILY_PRIOR_STATUS.SATURATED && prior !== FAMILY_PRIOR_STATUS.BLOCKED;
  });

  const ledger = {
    hotelId,
    displayName: hotelConfig.displayName,
    marketKey: meta.marketKey,
    queries: 0,
    fetches: 0,
    rendered: 0,
    pdfDocs: 0,
    followups: 0,
    validEntities: 0,
    addressableMotions: 0,
    lodgingSupported: 0,
    hotelFit: 0,
    whoResolved: 0,
    ready: 0,
    jev: {
      calls: 0,
      helpfulDifferent: 0,
      same: 0,
      wrong: 0,
      fetchesAvoided: 0,
      feedback: [],
    },
    familyExperiments: [],
    candidates: [],
    nearMisses: [],
    qualityFlags: {
      calendarOnly: 0,
      historicalOnly: 0,
      wrongCity: 0,
      thin: 0,
    },
  };

  let queryBudget = maxQueries;
  let fetchBudget = maxFetches;
  let renderBudget = maxRendered;
  let pdfBudget = maxPdf;

  for (const family of families) {
    if (queryBudget <= 0 || fetchBudget <= 0) break;

    const exp = {
      sourceFamily: family,
      hotel: hotelConfig.displayName,
      queries: [],
      fetches: [],
      validSources: [],
      entitySignals: [],
      addressableMotions: [],
      lodgingSupportedEntities: [],
      hotelFitMatches: [],
      whoPaths: [],
      readyOpportunities: [],
      cost: { queries: 0, fetches: 0 },
      runtimeMs: 0,
      metrics: {},
      verdict: PLAYBOOK_VERDICT.LOW_YIELD,
    };
    const t0 = Date.now();

    const motionQueries = buildMotionQueries(hotelId, family, hotelConfig, {
      max: Math.min(4, queryBudget),
    });
    if (!motionQueries.length) {
      exp.verdict = PLAYBOOK_VERDICT.DO_NOT_USE;
      exp.skipped = "prior_saturated_or_no_queries";
      ledger.familyExperiments.push(exp);
      continue;
    }

    const candidateSources = [];
    for (const q of motionQueries) {
      if (queryBudget <= 0) break;
      if (!hasSerp()) {
        exp.serpUnavailable = true;
        break;
      }
      queryBudget -= 1;
      ledger.queries += 1;
      exp.cost.queries += 1;
      exp.queries.push(q.query);

      if (enableJev && exp.cost.queries % 2 === 1) {
        try {
          const dec = await decideStructuredSourcePriority({
            demandFamily: family,
            sourcesChecked: candidateSources.map((s) => s.sourceType),
            sourceCandidates: candidateSources.slice(-5),
            lodgingGap: true,
            contactGap: true,
            remainingBudget: fetchBudget,
            enableSafeApply: true,
          });
          recordJev(ledger, {
            decisionType: "STRUCTURED_SOURCE_PRIORITY",
            family,
            defaultPath: dec.defaultPath,
            jevPath: dec.jevPath,
            finalPath: dec.finalPath,
            applied: dec.applied,
            outcome: dec.applied ? "HELPFUL_DIFFERENT" : dec.agreement ? "SAME" : "UNKNOWN",
            estimatedFetchesSaved: dec.applied && dec.finalPath === "STOP" ? 1 : 0,
          });
          if (dec.applied && dec.finalPath === "STOP") continue;
        } catch (err) {
          recordJev(ledger, {
            decisionType: "STRUCTURED_SOURCE_PRIORITY",
            outcome: "WRONG",
            error: String(err?.message || err),
          });
        }
      }

      try {
        const serp = await serpapiSearch({
          engine: "google",
          q: q.query,
          num: 6,
          hl: meta.serpHl,
          gl: meta.serpGl,
        });
        for (const hit of serp?.data?.organic_results || []) {
          const blob = `${hit.title || ""} ${hit.snippet || ""} ${hit.link || ""}`;
          if (wrongCity(blob, meta.cityTokens) && /calendar|events? in|what'?s on/i.test(blob)) {
            ledger.qualityFlags.calendarOnly += 1;
            continue;
          }
          const sourceType = classifyStructuredSource({
            url: hit.link,
            title: hit.title,
            snippet: hit.snippet,
          });
          if (sourceType === SOURCE_TYPE.GENERIC_SERP) {
            // Still allow motion signals from SERP snippet when specific
            const ents = signalToEntities({
              hit,
              family: inferDemandFamily(blob),
              marketKey: meta.marketKey,
              year,
              pageText: "",
            });
            for (const sig of ents.hidden || []) {
              exp.entitySignals.push({
                from: "serp_snippet",
                organizationName: sig.organizationName,
                snippet: hit.snippet,
                url: hit.link,
              });
            }
            continue;
          }
          candidateSources.push({
            url: hit.link,
            title: hit.title,
            snippet: hit.snippet,
            sourceType,
            family,
            score: structuredSourceScore(sourceType),
            queryId: q.queryId,
          });
        }
      } catch (err) {
        exp.serpErrors = exp.serpErrors || [];
        exp.serpErrors.push(String(err?.message || err));
      }
    }

    candidateSources.sort((a, b) => b.score - a.score);
    const seenUrl = new Set();
    const unique = [];
    for (const s of candidateSources) {
      if (!s.url || seenUrl.has(s.url)) continue;
      seenUrl.add(s.url);
      unique.push(s);
    }

    const familyEntities = [];
    const perFamilyFetchCap = Math.min(8, fetchBudget);
    let familyFetches = 0;

    for (const src of unique) {
      if (fetchBudget <= 0 || familyFetches >= perFamilyFetchCap) break;
      const isPdf =
        src.sourceType === SOURCE_TYPE.PROGRAM_PDF ||
        src.sourceType === SOURCE_TYPE.HOUSING_PDF ||
        /\.pdf($|\?)/i.test(src.url);

      if (isPdf && pdfBudget <= 0) continue;

      fetchBudget -= 1;
      familyFetches += 1;
      ledger.fetches += 1;
      exp.cost.fetches += 1;
      exp.fetches.push(src.url);
      exp.validSources.push({ url: src.url, sourceType: src.sourceType, title: src.title });

      const genName = src.title?.slice(0, 80) || meta.marketLabel;
      const demandGeneratorId = computeDemandGeneratorId({
        name: genName,
        marketKey: meta.marketKey,
        year,
      });

      try {
        if (isPdf) {
          pdfBudget -= 1;
          ledger.pdfDocs += 1;
          const pdf = await fetchAndExtractPdf(src.url, {
            sourceType: src.sourceType,
            demandGeneratorId,
            demandGeneratorName: genName,
            year,
            family: src.family,
            futureTiming: true,
          });
          if (pdf.ok) {
            for (const e of pdf.entities || []) familyEntities.push(e);
          }
        } else {
          const page = await fetchResearchPage(src.url);
          if (!page.ok) continue;
          if (renderBudget > 0) {
            renderBudget -= 1;
            ledger.rendered += 1;
          }
          const html = page.text || "";
          const text = htmlToSearchableText(html);
          if (wrongCity(text.slice(0, 3000), meta.cityTokens)) {
            ledger.qualityFlags.wrongCity += 1;
            continue;
          }
          const extracted = extractEntitiesFromHtmlDirectory(html, {
            sourceType: src.sourceType,
            sourceURL: page.url || src.url,
            demandGeneratorId,
            demandGeneratorName: genName,
            year,
            family: src.family,
            futureTiming: true,
          });
          for (const e of extracted) familyEntities.push(e);

          const serpEnts = signalToEntities({
            hit: { title: src.title, link: src.url, snippet: src.snippet },
            family: src.family,
            marketKey: meta.marketKey,
            year,
            pageText: text,
          });
          for (const h of serpEnts.hidden || []) {
            familyEntities.push({
              entityName: h.organizationName,
              entityType: "ORGANIZATION",
              sourceType: src.sourceType,
              sourceURL: src.url,
              evidenceSnippet: h.snippet || text.slice(0, 400),
              year,
              family: h.family || src.family,
              futureTiming: h.timing !== TIMING_CLASS.HISTORICAL_ONLY,
              confidence: 0.55,
            });
          }
        }
      } catch {
        /* non-fatal fetch */
      }
    }

    const deduped = dedupeExtractedEntities(familyEntities);
    for (const ent of deduped) {
      const gate = passesStructuredEntityQualityGate(ent);
      if (!gate.ok) continue;

      const blob = `${ent.entityName || ""} ${ent.evidenceSnippet || ""}`;
      if (wrongCity(blob, meta.cityTokens)) {
        ledger.qualityFlags.wrongCity += 1;
        continue;
      }

      const motion = classifyMotionFromText(blob, family);
      if (motion === ADDRESSABLE_MOTION.OTHER && !isPlausibleOrganizationName(ent.entityName)) {
        continue;
      }

      const timing = classifyTiming(blob, year);
      if (timing === TIMING_CLASS.HISTORICAL_ONLY) {
        ledger.qualityFlags.historicalOnly += 1;
        continue;
      }

      const lodgingEv = classifyLodgingEvidence(blob);
      if (lodgingEv === LODGING_EVIDENCE.NONE || lodgingEv === LODGING_EVIDENCE.WEAK_INFERENCE) {
        exp.entitySignals.push({
          organizationName: ent.entityName,
          motion,
          lodgingEv,
          timing,
          held: "lodging_too_weak",
          sourceUrl: ent.sourceURL,
        });
        continue;
      }

      ledger.validEntities += 1;
      exp.addressableMotions.push({
        organizationName: ent.entityName,
        motion,
        timing,
        lodgingEv,
        sourceUrl: ent.sourceURL,
      });
      ledger.addressableMotions += 1;

      if (
        lodgingEv === LODGING_EVIDENCE.DIRECT ||
        lodgingEv === LODGING_EVIDENCE.STRONG_INFERENCE
      ) {
        ledger.lodgingSupported += 1;
        exp.lodgingSupportedEntities.push(ent.entityName);
      }

      const lodging = qualifyEntityLodging(ent, null, blob);
      lodging.lodgingSignalStrength = motionToLodgingStrength(lodgingEv);
      const enriched = { ...ent, ...lodging, family: ent.family || family };

      const hd = entityToHiddenDemand(
        enriched,
        enriched,
        meta.marketKey,
        meta.marketLabel
      );
      hd.addressableMotion = motion;
      hd.timingClass = timing;
      hd.lodgingEvidenceClass = lodgingEv;

      const matches = matchHiddenDemandToHotels(hd, [hotelConfig]);
      for (const m of matches) {
        if (m.decision === "INSUFFICIENT") continue;
        let cand = hotelMatchToOpportunityCandidate(hd, m, hotelConfig);
        const who = applyGdiWhoHowResolution(cand, {
          markAttempted: true,
          ceilingReason: cand.primaryContactName ? undefined : "PUBLIC_DATA_CEILING",
        });
        cand = buildGdiOpportunitySummary(who.opportunity);
        const readiness = isGdiCustomerOpportunityReady(cand);
        cand.readiness = readiness;
        cand.sourceFamily = family;
        cand.addressableMotion = motion;

        if (m.decision === "MATCH" || m.decision === "MATCH_STRONG") {
          ledger.hotelFit += 1;
          exp.hotelFitMatches.push({
            organizationName: cand.organizationName,
            fit: m.fitScore,
            decision: m.decision,
          });
        }

        if (cand.primaryContactName || cand.organizationName) {
          ledger.whoResolved += 1;
          exp.whoPaths.push({
            organization: cand.organizationName,
            contact: cand.primaryContactName || null,
            role: cand.primaryContactRole || "ORGANIZATION_CONTACT",
          });
        }

        if (readiness.ok) {
          ledger.ready += 1;
          exp.readyOpportunities.push(cand);
          ledger.candidates.push(cand);
        } else {
          ledger.nearMisses.push({
            organizationName: cand.organizationName,
            motion,
            timing,
            lodgingEv,
            hotel: hotelConfig.displayName,
            reasonHeld: readiness.failed?.join(", ") || readiness.state,
            evidenceMissing: readiness.holds?.join(", ") || "readiness_gate",
            sourceUrl: ent.sourceURL,
            nextTrigger: "stronger lodging evidence or named WHO",
          });
        }
      }
    }

    exp.metrics = {
      fetches: exp.cost.fetches,
      validMotions: exp.addressableMotions.length,
      lodgingSupported: exp.lodgingSupportedEntities.length,
      fitMatches: exp.hotelFitMatches.length,
      who: exp.whoPaths.length,
      ready: exp.readyOpportunities.length,
      falsePositives: Math.max(0, exp.entitySignals.length - exp.addressableMotions.length),
    };
    exp.verdict = scoreFamilyVerdict(exp.metrics);
    exp.runtimeMs = Date.now() - t0;
    ledger.familyExperiments.push(exp);
  }

  return ledger;
}

export { SOURCE_FAMILY, PLAYBOOK_VERDICT, PRIOR_FAMILY_STATUS, FAMILY_PRIOR_STATUS };
