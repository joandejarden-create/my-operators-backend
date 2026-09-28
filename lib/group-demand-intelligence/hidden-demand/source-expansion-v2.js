/**
 * GDI Hidden Demand V2 — adaptive structured-source expansion orchestrator.
 * SERP = routing. Directories / PDFs = entity sources.
 * Jev SAFE APPLY for STRUCTURED_SOURCE_PRIORITY + HIDDEN_DEMAND_NEXT_LAYER.
 */

import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../hotel-intelligence/room-count-research/fetch.js";
import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  HIDDEN_DEMAND_VERSION,
  SOURCE_TYPE,
  DEMAND_FAMILY,
  DISCOVERY_DEPTH,
  LODGING_SIGNAL_STRENGTH,
  PARTICIPATION_ROLE,
} from "./v2-constants.js";
import { DEMAND_FAMILY as FAMILIES } from "./constants.js";
import {
  classifyStructuredSource,
  structuredSourceScore,
  buildStructuredRoutingQueries,
} from "./source-classifier.js";
import { extractEntitiesFromHtmlDirectory } from "./extract-directory.js";
import { fetchAndExtractPdf } from "./extract-pdf.js";
import { dedupeExtractedEntities, normalizeOrganizationName } from "./entity-normalize.js";
import { qualifyEntityLodging } from "./lodging-qualify.js";
import { passesStructuredEntityQualityGate } from "./structured-quality-gate.js";
import { decideStructuredSourcePriority } from "./jev-structured-source.js";
import { decideHiddenDemandNextLayer } from "./jev-next-layer.js";
import {
  computeDemandGeneratorId,
  computeHiddenDemandId,
} from "./identity.js";
import {
  classifyDiscoveryDepth,
  inferDemandFamily,
} from "./classify.js";
import {
  matchHiddenDemandToHotels,
  hotelMatchToOpportunityCandidate,
} from "./index.js";

function hasSerp() {
  return !!(process.env.SERPAPI_API_KEY || process.env.SERPAPI_KEY);
}

function emptySourceYield() {
  return Object.fromEntries(
    Object.values(SOURCE_TYPE).map((t) => [
      t,
      { fetches: 0, entities: 0, gateSurvivors: 0, lodgingSupported: 0, actionable: 0 },
    ])
  );
}

function recordJevFeedback(ledger, row) {
  ledger.jev.feedback.push(row);
  ledger.jev.calls += 1;
  if (row.applied) ledger.jev.safeApply += 1;
  if (row.outcome === "HELPFUL_DIFFERENT") ledger.jev.helpfulDifferent += 1;
  else if (row.outcome === "WRONG") ledger.jev.wrong += 1;
  else if (row.outcome === "SAME") ledger.jev.same += 1;
  if (row.decisionType === "STRUCTURED_SOURCE_PRIORITY") ledger.jev.structuredSourcePriority += 1;
  if (row.decisionType === "HIDDEN_DEMAND_NEXT_LAYER") ledger.jev.hiddenDemandNextLayer += 1;
}

/**
 * Convert gated entity → hidden demand record (market-level).
 */
export function entityToHiddenDemand(entity, lodging = null, marketKey = "nyc_midtown") {
  const org = entity.entityName;
  const family =
    entity.family ||
    inferDemandFamily(
      `${org} ${entity.participationRole || ""} ${entity.evidenceSnippet || ""} ${entity.sourceType || ""}`
    );
  const year = entity.year || new Date().getUTCFullYear() + 1;
  const genId =
    entity.demandGeneratorId ||
    (entity.demandGeneratorName
      ? computeDemandGeneratorId({
          name: entity.demandGeneratorName,
          marketKey,
          year,
        })
      : null);
  const projectName = `${org} ${entity.participationRole || "PARTICIPANT"} ${year}`;
  const hiddenDemandId = computeHiddenDemandId({
    organizationName: org,
    projectName,
    demandGeneratorId: genId || "entity_first",
    family,
    timingKey: entity.eventCycleId || String(year),
  });

  const lodgingStrength =
    lodging?.lodgingSignalStrength || LODGING_SIGNAL_STRENGTH.UNKNOWN;
  const depth = classifyDiscoveryDepth({
    title: projectName,
    organizationName: org,
    sourceUrl: entity.sourceURL,
    hasParentGenerator: Boolean(genId),
    hasSubgroupEvidence:
      entity.participationRole === PARTICIPATION_ROLE.COMMITTEE ||
      entity.participationRole === PARTICIPATION_ROLE.BOARD ||
      entity.entityType === "SUBGROUP" ||
      /team|crew|cohort|delegation/i.test(entity.evidenceSnippet || ""),
    lodgingSignalStrength: lodgingStrength,
  });

  return {
    hiddenDemandId,
    demandGeneratorId: genId,
    organizationName: org,
    organizationId: entity.normalizeKey || normalizeOrganizationName(org).normalizeKey,
    projectName,
    title: projectName,
    family,
    marketKey,
    year,
    timingKey: entity.eventCycleId || String(year),
    eventCycleId: entity.eventCycleId || String(year),
    destination: "New York Midtown",
    sourceUrl: entity.sourceURL,
    sourceType: entity.sourceType,
    snippet: entity.evidenceSnippet,
    participationRole: entity.participationRole,
    entityType: entity.entityType,
    lodgingSignalStrength: lodgingStrength,
    lodgingEvidence: lodging?.housingEvidence || lodging?.lodgingSignals || null,
    lodgingScore: lodging?.lodgingScore ?? 0,
    discoveryDepth: depth,
    kind: "HIDDEN_DEMAND",
    gdiVersion: HIDDEN_DEMAND_VERSION,
    evidence: {
      entityExists: true,
      futureTiming: entity.futureTiming !== false,
      travelPresence:
        lodgingStrength !== LODGING_SIGNAL_STRENGTH.UNKNOWN ||
        /travel|hotel|lodging|housing|midtown|manhattan|new york|nyc/i.test(
          entity.evidenceSnippet || ""
        ),
      plausibleLodging: lodgingStrength !== LODGING_SIGNAL_STRENGTH.UNKNOWN,
      addressableOrg: true,
    },
    contacts: entity.contacts || [],
    confidence: entity.confidence,
  };
}

/**
 * Main V2 live cycle.
 */
export async function runStructuredSourceExpansion(opts = {}) {
  const marketLabel = opts.marketLabel || "New York Midtown";
  const marketKey = opts.marketKey || "nyc_midtown";
  const year = opts.year || 2027;
  const hotelConfigs = opts.hotelConfigs || [];
  const maxRoutingQueries = opts.maxRoutingQueries ?? 25;
  const maxDirectoryFetches = opts.maxDirectoryFetches ?? 40;
  const maxPdfFetches = opts.maxPdfFetches ?? 30;
  const maxRendered = opts.maxRendered ?? 20;
  const maxEntityFollowups = opts.maxEntityFollowups ?? 40;
  const maxTotalFetches = opts.maxTotalFetches ?? 120;
  const enableJev = opts.enableJev !== false;
  const v1EntityReprocess = opts.v1EntityReprocess || null;

  const ledger = {
    version: HIDDEN_DEMAND_VERSION,
    marketKey,
    marketLabel,
    year,
    routingQueries: 0,
    structuredSourcesFound: 0,
    directoryFetches: 0,
    pdfFetches: 0,
    rendered: 0,
    totalFetches: 0,
    entityFollowups: 0,
    sourceYield: emptySourceYield(),
    rawEntities: [],
    gatedEntities: [],
    hidden: [],
    generators: [],
    contactsFound: [],
    jev: {
      calls: 0,
      safeApply: 0,
      helpfulDifferent: 0,
      same: 0,
      wrong: 0,
      highConfWrong: 0,
      structuredSourcePriority: 0,
      hiddenDemandNextLayer: 0,
      fetchesAvoided: 0,
      qualifiedAttributable: 0,
      lodgingAttributable: 0,
      contactUpgrades: 0,
      feedback: [],
    },
    serpErrors: [],
    fetchErrors: [],
    routingHits: [],
  };

  const budget = {
    total: maxTotalFetches,
    directory: maxDirectoryFetches,
    pdf: maxPdfFetches,
    rendered: maxRendered,
    followups: maxEntityFollowups,
  };

  const spend = () => {
    ledger.totalFetches += 1;
    budget.total -= 1;
    return budget.total >= 0;
  };

  // --- Phase 1: routing SERP (find structured sources) ---
  const queries = buildStructuredRoutingQueries({
    marketLabel,
    year,
    max: maxRoutingQueries,
  });
  const candidateSources = [];

  for (const q of queries) {
    if (!hasSerp()) break;
    ledger.routingQueries += 1;

    // Jev: which structured source type to prefer next for this family
    if (enableJev && ledger.routingQueries % 4 === 1) {
      const checked = [...new Set(candidateSources.map((s) => s.sourceType))];
      try {
        const dec = await decideStructuredSourcePriority({
          demandFamily: q.family,
          sourcesChecked: checked,
          sourceCandidates: candidateSources.slice(-10),
          lodgingGap: true,
          contactGap: true,
          remainingBudget: budget.total,
          enableSafeApply: true,
        });
        const outcome =
          dec.applied ? "HELPFUL_DIFFERENT" : dec.agreement ? "SAME" : "UNKNOWN";
        recordJevFeedback(ledger, {
          decisionType: "STRUCTURED_SOURCE_PRIORITY",
          inputSummary: { family: q.family, checked },
          defaultPath: dec.defaultPath,
          jevPath: dec.jevPath,
          finalPath: dec.finalPath,
          applied: dec.applied,
          outcome,
          fetchesUsed: 0,
          estimatedFetchesSaved: dec.applied && dec.finalPath === "STOP" ? 2 : 0,
        });
        if (dec.applied && dec.finalPath === "STOP") {
          ledger.jev.fetchesAvoided += 2;
        }
        q.preferredSourceType = dec.finalPath;
      } catch (err) {
        recordJevFeedback(ledger, {
          decisionType: "STRUCTURED_SOURCE_PRIORITY",
          outcome: "WRONG",
          applied: false,
          error: String(err?.message || err),
        });
      }
    }

    let organic = [];
    try {
      const serp = await serpapiSearch({
        engine: "google",
        q: q.query,
        num: 8,
        hl: "en",
        gl: "us",
      });
      organic = serp?.data?.organic_results || [];
    } catch (err) {
      ledger.serpErrors.push({ query: q.query, error: String(err?.message || err) });
      continue;
    }

    for (const hit of organic) {
      const sourceType = classifyStructuredSource({
        url: hit.link,
        title: hit.title,
        snippet: hit.snippet,
      });
      const row = {
        url: hit.link,
        title: hit.title,
        snippet: hit.snippet,
        sourceType,
        family: q.family,
        score: structuredSourceScore(sourceType),
        queryId: q.queryId,
        preferredSourceType: q.preferredSourceType || null,
      };
      ledger.routingHits.push(row);
      if (sourceType !== SOURCE_TYPE.GENERIC_SERP) {
        candidateSources.push(row);
        ledger.structuredSourcesFound += 1;
      }
    }
  }

  // Sort structured sources by score; boost Jev-preferred types
  candidateSources.sort((a, b) => {
    const boost = (s) =>
      s.preferredSourceType && s.sourceType === s.preferredSourceType ? 25 : 0;
    return b.score + boost(b) - (a.score + boost(a));
  });

  // Dedupe URLs
  const seenUrl = new Set();
  const uniqueSources = [];
  for (const s of candidateSources) {
    if (!s.url || seenUrl.has(s.url)) continue;
    seenUrl.add(s.url);
    uniqueSources.push(s);
  }

  // --- Phase 2: fetch structured sources ---
  const allEntities = [];
  let sharedHousing = null;

  for (const src of uniqueSources) {
    if (budget.total <= 0) break;
    const isPdf =
      src.sourceType === SOURCE_TYPE.PROGRAM_PDF ||
      src.sourceType === SOURCE_TYPE.HOUSING_PDF ||
      src.sourceType === SOURCE_TYPE.REGISTRATION_PDF ||
      /\.pdf($|\?)/i.test(src.url);

    const genName = src.title?.slice(0, 80) || marketLabel;
    const demandGeneratorId = computeDemandGeneratorId({
      name: genName,
      marketKey,
      year,
    });
    ledger.generators.push({
      demandGeneratorId,
      name: genName,
      sourceUrl: src.url,
      sourceType: src.sourceType,
      family: src.family,
    });

    if (isPdf) {
      if (budget.pdf <= 0) continue;
      if (!spend()) break;
      budget.pdf -= 1;
      ledger.pdfFetches += 1;
      ledger.sourceYield[src.sourceType].fetches += 1;
      try {
        const pdf = await fetchAndExtractPdf(src.url, {
          sourceType: src.sourceType,
          demandGeneratorId,
          demandGeneratorName: genName,
          year,
          family: src.family,
          futureTiming: true,
        });
        if (!pdf.ok) {
          ledger.fetchErrors.push({ url: src.url, reason: pdf.reason });
          continue;
        }
        if (pdf.housing?.roomBlockMentioned) sharedHousing = pdf.housing;
        for (const e of pdf.entities) {
          allEntities.push(e);
          ledger.sourceYield[src.sourceType].entities += 1;
        }
        for (const c of pdf.contacts || []) ledger.contactsFound.push(c);
      } catch (err) {
        ledger.fetchErrors.push({ url: src.url, error: String(err?.message || err) });
      }
      continue;
    }

    // Directory / HTML structured pages
    if (budget.directory <= 0) continue;
    if (!spend()) break;
    budget.directory -= 1;
    ledger.directoryFetches += 1;
    ledger.sourceYield[src.sourceType].fetches += 1;
    try {
      const page = await fetchResearchPage(src.url);
      if (!page.ok) {
        ledger.fetchErrors.push({ url: src.url, status: page.status });
        continue;
      }
      if (budget.rendered > 0) {
        budget.rendered -= 1;
        ledger.rendered += 1;
      }
      const html = page.text || "";
      const extracted = extractEntitiesFromHtmlDirectory(html, {
        sourceType: src.sourceType,
        sourceURL: page.url || src.url,
        demandGeneratorId,
        demandGeneratorName: genName,
        year,
        family: src.family,
        futureTiming: true,
      });
      // Also scan plain text for housing language
      const text = htmlToSearchableText(html);
      if (/hotel\s*block|room\s*block|housing/i.test(text) && !sharedHousing) {
        const { extractHousingSignalsFromText } = await import("./extract-pdf.js");
        sharedHousing = extractHousingSignalsFromText(text, page.url || src.url);
      }
      for (const e of extracted) {
        allEntities.push(e);
        ledger.sourceYield[src.sourceType].entities += 1;
      }
    } catch (err) {
      ledger.fetchErrors.push({ url: src.url, error: String(err?.message || err) });
    }
  }

  // --- Phase 2b: reprocess V1 entity if provided ---
  if (v1EntityReprocess?.organizationName) {
    const v1 = {
      entityName: v1EntityReprocess.organizationName,
      entityType: "SUBGROUP",
      sourceType: SOURCE_TYPE.PROGRAM_PAGE,
      sourceURL: v1EntityReprocess.sourceUrl || null,
      participationRole: PARTICIPATION_ROLE.BOARD,
      futureTiming: true,
      year,
      evidenceSnippet: v1EntityReprocess.snippet || v1EntityReprocess.organizationName,
      confidence: 0.6,
      family: FAMILIES.ASSOCIATION_SUBGROUP,
      demandGeneratorName: "Independent Lodging Congress",
    };
    allEntities.push(v1);
  }

  ledger.rawEntities = dedupeExtractedEntities(allEntities);

  // --- Phase 3: quality gate + lodging + bounded follow-up ---
  const gated = [];
  for (const ent of ledger.rawEntities) {
    const gate = passesStructuredEntityQualityGate(ent);
    if (!gate.ok) continue;

    let lodging = qualifyEntityLodging(ent, sharedHousing, "");
    // Bounded follow-up for promising exhibitors without lodging yet
    if (
      lodging.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.UNKNOWN &&
      budget.followups > 0 &&
      budget.total > 0 &&
      hasSerp() &&
      (ent.participationRole === PARTICIPATION_ROLE.EXHIBITOR ||
        ent.participationRole === PARTICIPATION_ROLE.SPONSOR ||
        ent.participationRole === PARTICIPATION_ROLE.BOARD)
    ) {
      // Jev next layer
      if (enableJev) {
        try {
          const layer = await decideHiddenDemandNextLayer({
            demandGenerator: { name: ent.demandGeneratorName },
            knownEntities: gated,
            currentFamily: ent.family,
            defaultLayer:
              ent.participationRole === PARTICIPATION_ROLE.EXHIBITOR
                ? "EXHIBITOR"
                : ent.participationRole === PARTICIPATION_ROLE.BOARD
                  ? "ASSOCIATION_SUBGROUP"
                  : "AGENCY",
            enableSafeApply: true,
          });
          recordJevFeedback(ledger, {
            decisionType: "HIDDEN_DEMAND_NEXT_LAYER",
            defaultPath: layer.defaultLayer,
            jevPath: layer.jevLayer,
            finalPath: layer.applied ? layer.jevLayer : layer.defaultLayer,
            applied: layer.applied,
            outcome: layer.applied ? "HELPFUL_DIFFERENT" : layer.agreement ? "SAME" : "UNKNOWN",
          });
          if (layer.jevLayer === "STOP") {
            ledger.jev.fetchesAvoided += 1;
            // still keep entity if gate passed — just skip follow-up
          } else if (layer.applied) {
            ledger.jev.qualifiedAttributable += 1;
          }
        } catch {
          /* non-fatal */
        }
      }

      if (budget.followups > 0 && budget.total > 0) {
        budget.followups -= 1;
        ledger.entityFollowups += 1;
        const fq = `${ent.entityName} ${ent.demandGeneratorName || ""} hotel block OR booth staff OR team travel OR New York lodging ${year}`;
        try {
          if (spend()) {
            const serp = await serpapiSearch({
              engine: "google",
              q: fq,
              num: 3,
              hl: "en",
              gl: "us",
            });
            const snips = (serp?.data?.organic_results || [])
              .map((r) => `${r.title} ${r.snippet}`)
              .join(" ");
            lodging = qualifyEntityLodging(ent, sharedHousing, snips);
            if (
              lodging.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.STRONG ||
              lodging.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.MEDIUM
            ) {
              ledger.jev.lodgingAttributable += 1;
            }
          }
        } catch {
          /* ignore */
        }
      }
    }

    const enriched = { ...ent, ...lodging };
    gated.push(enriched);
    const st = ent.sourceType || SOURCE_TYPE.GENERIC_SERP;
    if (ledger.sourceYield[st]) {
      ledger.sourceYield[st].gateSurvivors += 1;
      if (
        lodging.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.STRONG ||
        lodging.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.MEDIUM
      ) {
        ledger.sourceYield[st].lodgingSupported += 1;
      }
    }
  }

  ledger.gatedEntities = gated;

  // --- Phase 4: hidden demand + multi-hotel match ---
  const hidden = gated.map((e) => entityToHiddenDemand(e, e, marketKey));
  // Dedupe by hiddenDemandId
  const hdMap = new Map();
  for (const h of hidden) {
    if (!hdMap.has(h.hiddenDemandId)) hdMap.set(h.hiddenDemandId, h);
  }
  ledger.hidden = [...hdMap.values()];

  const hotelResults = {};
  const both = [];
  const only = {};
  for (const cfg of hotelConfigs) {
    hotelResults[cfg.hotelId] = {
      hotelId: cfg.hotelId,
      displayName: cfg.displayName,
      matched: 0,
      actionable: 0,
      watch: 0,
      dq: 0,
      candidates: [],
    };
    only[cfg.hotelId] = [];
  }

  for (const hd of ledger.hidden) {
    const matches = matchHiddenDemandToHotels(hd, hotelConfigs);
    const strong = [];
    for (const m of matches) {
      const hr = hotelResults[m.hotelId];
      if (!hr) continue;
      const cand = hotelMatchToOpportunityCandidate(
        hd,
        m,
        hotelConfigs.find((c) => c.hotelId === m.hotelId)
      );
      // Attach structured provenance + contacts (WHO) when present
      if (hd.contacts?.length) {
        const c0 = hd.contacts[0];
        cand.primaryContact = {
          name: c0.name,
          role: c0.title,
          email: c0.email || null,
          source: c0.source,
        };
        cand.primaryContactName = c0.name;
        ledger.jev.contactUpgrades += 1;
      }
      cand.sourceType = hd.sourceType;
      cand.gdiVersion = HIDDEN_DEMAND_VERSION;
      hr.candidates.push(cand);
      if (m.decision === "INSUFFICIENT") hr.dq += 1;
      else {
        hr.matched += 1;
        if (cand.customerFacingState === "ACTIONABLE_NOW") hr.actionable += 1;
        else if (cand.customerFacingState === "WATCH" || cand.customerPromotable) hr.watch += 1;
        else hr.dq += 1;
        if (m.decision === "MATCH" || m.decision === "MATCH_STRONG") strong.push(m.hotelId);
      }
    }
    if (strong.length >= 2) {
      both.push({
        hiddenDemandId: hd.hiddenDemandId,
        organizationName: hd.organizationName,
        family: hd.family,
        lodging: hd.lodgingSignalStrength,
        depth: hd.discoveryDepth,
        matches: matches.map((m) => ({
          hotelId: m.hotelId,
          hotelName: m.hotelName,
          fit: m.fitScore,
          motion: m.commercialMotion,
        })),
      });
    } else if (strong.length === 1) {
      only[strong[0]].push(hd.hiddenDemandId);
    }
  }

  // Family yield
  const familyYield = Object.fromEntries(
    Object.values(FAMILIES).map((f) => [
      f,
      { raw: 0, qualified: 0, lodging: 0, hilton: 0, renaissance: 0, actionable: 0 },
    ])
  );
  for (const e of ledger.rawEntities) {
    const f = e.family || inferDemandFamily(e.entityName);
    if (familyYield[f]) familyYield[f].raw += 1;
  }
  for (const h of ledger.hidden) {
    const f = h.family;
    if (!familyYield[f]) continue;
    familyYield[f].qualified += 1;
    if (
      h.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.STRONG ||
      h.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.MEDIUM
    ) {
      familyYield[f].lodging += 1;
    }
  }
  const hotelIds = hotelConfigs.map((c) => c.hotelId);
  for (const id of hotelIds) {
    for (const c of hotelResults[id]?.candidates || []) {
      const f = c.demandFamily;
      if (!familyYield[f]) continue;
      if (id === hotelIds[0]) familyYield[f].hilton += 1;
      if (id === hotelIds[1]) familyYield[f].renaissance += 1;
      if (c.customerFacingState === "ACTIONABLE_NOW") familyYield[f].actionable += 1;
    }
  }

  return {
    ledger,
    hotelResults,
    shared: {
      both,
      only,
      neither: ledger.hidden.length - both.length - Object.values(only).flat().length,
    },
    familyYield,
    duplicateFetchesAvoided: Math.max(0, (hotelConfigs.length - 1) * ledger.totalFetches),
    sharedMarketFetches: ledger.totalFetches,
  };
}
