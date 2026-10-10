/**
 * Opportunity Discovery Rebuild V5 — buyer-first hotel-demand motion orchestrator.
 * Funnel: SIGNAL → RESEARCH_LEAD → CANDIDATE → CUSTOMER_READY (no SIGNAL→CANDIDATE).
 * Thresholds unchanged. Jev advisory only after admission.
 */

import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import { isGdiCustomerOpportunityReady } from "../customer-readiness-gate-v1.js";
import { isValidFutureWatch } from "../future-watch/is-valid-future-watch-v1.js";
import { FUNNEL_STAGE } from "./funnel.js";
import { buildBuyerFirstQueryLibrary } from "./demand-motion-queries.js";
import { selectPriorityQueries } from "./hotels.js";
import {
  isGdiResearchLeadWorthPursuing,
  isGdiCandidateOpportunity,
  LEAD_CLASS,
} from "./research-lead-gate.js";
import { resolveGdiDemandBuyer } from "./buyer-resolution.js";
import {
  validateResearchLeadPage,
  applyPageEvidenceToLead,
} from "./page-validation.js";
import { buildGdiCompetitiveDemandLead } from "./competitive-demand.js";
import { buildRotationRecord, ROTATION_TIMING_STATE } from "./rotation-intelligence.js";
import { runTargetedResearchSteps, jevAdviseResearchLead } from "./next-research.js";

const DIRECTORY_NOISE_RE =
  /\b(booking\.com|expedia|hotels\.com|trivago|tripadvisor|airbnb|kayak|indeed\.|tempslibre)\b/i;

function hasSerp() {
  return Boolean(String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim());
}

function slugId(hotelKey, title, url) {
  const base = String(title || url || "sig")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
  let host = "x";
  try {
    host = new URL(url).hostname.replace(/\W/g, "").slice(0, 12);
  } catch {
    /* ignore */
  }
  return `gdi_v5_${String(hotelKey).toLowerCase()}_${base}_${host}`.slice(0, 96);
}

function hitToSignal(hit, meta = {}) {
  const title = String(hit.title || "").trim();
  const link = String(hit.link || hit.url || "").trim();
  const snippet = String(hit.snippet || hit.description || "").trim();
  if (!title || !link) return null;
  if (DIRECTORY_NOISE_RE.test(`${title} ${link} ${snippet}`)) return null;

  const orgGuess = title.split(/[|\-—:]/)[0].trim().slice(0, 120);
  const yearMatch = `${title} ${snippet}`.match(/\b(202[6-9]|203[0-2])\b/);
  const lodgingHint =
    /\b(room block|host hotel|housing|accommodation|hôtel officiel|alojamiento|hébergement|hotel block|RFP|tender|licitación|crew hotel|workforce lodging)\b/i.test(
      `${title} ${snippet} ${link}`
    );

  return {
    id: slugId(meta.hotelKey || "h", title, link),
    title,
    organizationName: orgGuess,
    opportunityName: title,
    opportunityType:
      meta.engine === "PROCUREMENT"
        ? "PRIMARY_PURSUIT"
        : lodgingHint
          ? "OVERFLOW_HOUSING"
          : "FUTURE_CYCLE",
    officialSource: link,
    discoverySource: link,
    sources: [{ url: link, kind: "opportunity_discovery_v5", engine: meta.engine }],
    summaryWhat: snippet.slice(0, 280),
    snippet,
    hotelOpportunityThesis: lodgingHint
      ? `${meta.hotelLabel || "Hotel"} can pursue group lodging / housing / overflow related to ${title}.`
      : `${title} may imply hotel-demand motion for ${meta.hotelLabel || "hotel"} — page-validate before candidate.`,
    whyNow: "Buyer-first discovery signal — not yet a candidate.",
    recommendedAction: "Page-validate; resolve buyer; confirm lodging + future decision.",
    summaryWhyMatters: snippet.slice(0, 200),
    summaryWhyHotel: meta.fitLine || "",
    hotelFitScore: meta.defaultFitScore ?? 48,
    defaultFitScore: meta.defaultFitScore ?? 48,
    venueStatus: "Unknown",
    eventLocationSummary: meta.lodgingMarket || null,
    lodgingMarket: meta.lodgingMarket || null,
    originMarket: meta.originMarket || null,
    feederMarket: meta.marketRole === "FEEDER" ? meta.originMarket : null,
    engine: meta.engine,
    signalType: meta.engine,
    queryLanguage: meta.language,
    queryLocalized: meta.queryLocalized,
    competitorHotel: meta.competitorHotel || null,
    eventYear: yearMatch ? yearMatch[1] : null,
    eventStartDate: null,
    lodgingEvidence: lodgingHint
      ? {
          housingPageFound: /accommodation|housing|hotel-block|alojamiento|hébergement/i.test(link),
          roomBlockMentioned: /room.?block|host.?hotel|bloc/i.test(`${title} ${snippet}`),
          status: "WEAK",
        }
      : null,
    groupMotion: meta.engine || null,
    gdiDiscoveryVersion: "opportunity_discovery_v5",
    funnelStage: FUNNEL_STAGE.SIGNAL,
    createdAt: new Date().toISOString(),
  };
}

async function runSerpQuery(q, hotel, budget) {
  if (!hasSerp() || budget.queriesLeft <= 0) return { hits: [], costUsd: 0 };
  budget.queriesLeft -= 1;
  budget.queriesRun += 1;
  try {
    const serp = await serpapiSearch({
      engine: "google",
      q: q.queryLocalized,
      num: 5,
      hl: q.language === "fr" ? "fr" : q.language === "es" || q.language === "gl" ? "es" : hotel.serpHlPrimary || "en",
      gl: hotel.serpGl || "us",
    });
    budget.costUsd += 0.05;
    return { hits: serp?.data?.organic_results || [], costUsd: 0.05 };
  } catch (err) {
    budget.errors.push(String(err?.message || err).slice(0, 160));
    budget.costUsd += 0.05;
    return { hits: [], costUsd: 0.05 };
  }
}

/**
 * Run buyer-first V5 discovery for one hotel.
 */
export async function runOpportunityDiscoveryV5ForHotel(hotel = {}, opts = {}) {
  const budget = {
    queriesLeft: opts.maxQueries ?? 12,
    queriesRun: 0,
    costUsd: 0,
    errors: [],
  };

  const allQueries = buildBuyerFirstQueryLibrary(hotel);
  const queries = selectPriorityQueries(allQueries, { maxQueries: opts.maxQueries ?? 12 });

  const rawSignals = [];
  const seen = new Set();

  for (const q of queries) {
    if (budget.queriesLeft <= 0) break;
    const { hits } = await runSerpQuery(q, hotel, budget);
    for (const hit of hits.slice(0, 4)) {
      const sig = hitToSignal(hit, {
        hotelKey: hotel.hotelKey,
        hotelLabel: hotel.label || hotel.hotelName,
        engine: q.engine,
        language: q.language,
        queryLocalized: q.queryLocalized,
        lodgingMarket: q.lodgingMarket,
        originMarket: q.originMarket,
        marketRole: q.marketRole,
        competitorHotel: q.competitorHotel,
        fitLine: hotel.fitLine,
        defaultFitScore: hotel.defaultFitScore,
      });
      if (!sig) continue;
      const key = `${hotel.hotelKey}|${String(sig.officialSource).toLowerCase().replace(/\/$/, "")}`;
      if (seen.has(key)) continue;
      seen.add(key);
      sig.hotelKey = hotel.hotelKey;
      sig.hotelId = hotel.hotelId;
      rawSignals.push(sig);
    }
  }

  // Stage: SIGNAL → RESEARCH_LEAD gate
  const researchLeads = [];
  const signalOnly = [];
  const rejected = [];

  for (const sig of rawSignals) {
    const gate = isGdiResearchLeadWorthPursuing(sig, { geoTokens: hotel.geoTokens });
    sig.admissionClass = gate.class;
    sig.supportSignals = gate.supportSignals;
    sig.supportCount = gate.supportCount;
    sig.admissionReasons = gate.reasons;
    if (gate.ok) {
      sig.funnelStage = FUNNEL_STAGE.RESEARCH_LEAD;
      researchLeads.push(sig);
    } else if (gate.class === LEAD_CLASS.SIGNAL_ONLY) {
      sig.funnelStage = FUNNEL_STAGE.SIGNAL;
      signalOnly.push(sig);
    } else {
      sig.funnelStage = FUNNEL_STAGE.REJECTED;
      rejected.push(sig);
    }
  }

  // Page-level validation BEFORE candidate creation (cap)
  const maxPageValidate = opts.maxPageValidate ?? Math.min(8, researchLeads.length);
  const pageValidated = [];
  const pageRows = [];
  const buyerRows = [];
  const rotationRows = [];
  const competitiveLeads = [];
  const jevRows = [];
  const candidates = [];
  const customerReady = [];
  const futureWatch = [];

  // Prefer procurement / lodging / competitive for page validation
  const pageQueue = [...researchLeads].sort((a, b) => {
    const score = (x) =>
      (x.engine === "PROCUREMENT" ? 5 : 0) +
      (x.lodgingEvidence ? 3 : 0) +
      (x.engine === "COMPETITIVE" ? 4 : 0) +
      (x.supportCount || 0);
    return score(b) - score(a);
  });

  let researched = 0;
  for (const lead of pageQueue) {
    if (researched >= maxPageValidate) {
      // keep as research lead without page → cannot become candidate
      lead.funnelStage = FUNNEL_STAGE.RESEARCH_LEAD;
      lead.pageValidated = false;
      continue;
    }

    const pageEv = await validateResearchLeadPage(lead);
    budget.costUsd += 0.01;
    pageRows.push({
      opportunityId: lead.id,
      hotelKey: hotel.hotelKey,
      url: pageEv.url,
      pageOk: pageEv.pageOk,
      pageKind: pageEv.pageKind,
      factsCount: pageEv.factsCount,
      evidencePersisted: pageEv.evidencePersisted,
      error: pageEv.error || "",
    });

    let enriched = applyPageEvidenceToLead(lead, pageEv);
    researched += 1;

    // Buyer resolution (entity/function)
    const buyer = resolveGdiDemandBuyer(enriched, pageEv);
    enriched = {
      ...enriched,
      buyerEntity: buyer.buyerEntity,
      buyerType: buyer.buyerType,
      buyerRole: buyer.buyerRole,
      organizer: buyer.organizer,
      agency: buyer.agency,
      housingPartner: buyer.housingPartner,
      organizationContactUrl: enriched.organizationContactUrl || buyer.publicContactPath,
    };
    buyerRows.push({
      opportunityId: enriched.id,
      hotelKey: hotel.hotelKey,
      ...buyer,
    });

    // Rotation
    const rot = buildRotationRecord(enriched, pageEv, hotel);
    rotationRows.push(rot);
    if (rot.timingState === ROTATION_TIMING_STATE.HISTORICAL_ONLY) {
      enriched.timingState = rot.timingState;
      enriched.funnelStage = FUNNEL_STAGE.REJECTED;
      rejected.push(enriched);
      continue;
    }
    enriched.timingState = rot.timingState;

    // Competitive demand lead (page-validated competitive / host-hotel patterns)
    if (enriched.engine === "COMPETITIVE" || enriched.competitorHotel || /host hotel|official hotel|room block/i.test(`${enriched.title} ${enriched.summaryWhat}`)) {
      const comp = buildGdiCompetitiveDemandLead(enriched, hotel);
      if (comp) {
        competitiveLeads.push(comp);
        if (comp.couldTargetCompeteNext) {
          enriched.hotelOpportunityThesis = comp.targetHotelCompeteThesis;
          enriched.competitiveDemand = true;
        }
      }
    }

    // Jev advisory + up to 2 targeted steps (only after admission)
    const research = await runTargetedResearchSteps(enriched, hotel, budget, {
      maxSteps: opts.maxResearchSteps ?? 2,
      allowThirdStep: false,
    });
    enriched = research.lead;
    for (const j of research.jevLog) jevRows.push(j);

    // Initial advisory row even if no steps
    if (!research.jevLog.length) {
      jevRows.push(
        jevAdviseResearchLead(enriched, {
          admitted: true,
          stepsTaken: 0,
          placeNames: hotel.placeNames,
        })
      );
    }

    // Candidate gate — requires page validation
    if (!enriched.pageValidated) {
      enriched.funnelStage = FUNNEL_STAGE.RESEARCH_LEAD;
      pageValidated.push(enriched);
      continue;
    }

    const candGate = isGdiCandidateOpportunity(enriched, pageEv);
    if (!candGate.ok) {
      enriched.funnelStage = FUNNEL_STAGE.RESEARCH_LEAD;
      enriched.candidateMissing = candGate.missing;
      pageValidated.push(enriched);
      continue;
    }

    enriched.funnelStage = FUNNEL_STAGE.CANDIDATE_OPPORTUNITY;
    candidates.push(enriched);

    const ready = isGdiCustomerOpportunityReady(enriched, {
      nowDate: opts.nowDate || "2026-10-03",
    });
    const watch = isValidFutureWatch(enriched, { nowDate: opts.nowDate || "2026-10-03" });
    const watchOk =
      watch?.ok === true || watch?.valid === true || watch?.class === "VALID_FUTURE_WATCH";

    if (ready.ok) {
      enriched.funnelStage = FUNNEL_STAGE.CUSTOMER_READY_OPPORTUNITY;
      customerReady.push(enriched);
    } else if (watchOk) {
      enriched.funnelStage = FUNNEL_STAGE.VALID_FUTURE_WATCH;
      futureWatch.push(enriched);
    }

    pageValidated.push(enriched);
  }

  // Also build competitive leads from non-page-validated competitive signals (structure only)
  for (const sig of researchLeads) {
    if (sig.engine !== "COMPETITIVE" && !sig.competitorHotel) continue;
    if (competitiveLeads.some((c) => c.id === sig.id)) continue;
    const comp = buildGdiCompetitiveDemandLead(sig, hotel);
    if (comp) competitiveLeads.push(comp);
  }

  // Engine-bucket helpers for reports (unique by id)
  const byEngine = (eng) => {
    const seenIds = new Set();
    const out = [];
    for (const x of rawSignals) {
      if (!String(x.engine || x.signalType || "").toUpperCase().includes(eng)) continue;
      if (seenIds.has(x.id)) continue;
      seenIds.add(x.id);
      out.push(x);
    }
    return out;
  };

  return {
    hotelId: hotel.hotelId,
    hotelKey: hotel.hotelKey,
    label: hotel.label || hotel.hotelName,
    queryLibrarySize: allQueries.length,
    queriesSelected: queries,
    queriesRun: budget.queriesRun,
    costUsd: budget.costUsd,
    errors: budget.errors,
    serpEnabled: hasSerp(),
    rawSignals,
    signalOnly,
    researchLeads,
    pageValidated,
    pageRows,
    buyerRows,
    rotationRows,
    competitiveLeads,
    jevRows,
    candidates,
    customerReady,
    futureWatch,
    rejected,
    buckets: {
      ASSOCIATION: byEngine("ASSOCIATION"),
      CORPORATE: byEngine("CORPORATE"),
      PHARMA: byEngine("PHARMA"),
      PROJECT_CREW: byEngine("PROJECT"),
      SPORTS: byEngine("SPORTS"),
      UNIVERSITY: byEngine("UNIVERSITY"),
      PROCUREMENT: byEngine("PROCUREMENT"),
      COMPETITIVE: byEngine("COMPETITIVE"),
      MULTILINGUAL: rawSignals.filter((s) => s.queryLanguage && s.queryLanguage !== "en"),
      FEEDER: rawSignals.filter((s) => s.feederMarket || s.originMarket !== s.lodgingMarket),
    },
    counts: {
      rawSignals: rawSignals.length,
      researchLeads: researchLeads.length,
      candidates: candidates.length,
      customerReady: customerReady.length,
      futureWatch: futureWatch.length,
      rejected: rejected.length,
      signalOnly: signalOnly.length,
    },
  };
}

export { hasSerp, FUNNEL_STAGE };
