/**
 * Ten Bases of Demand V1 — hotel-level controlled pilot orchestrator.
 * Persists baseOfDemand + demandEngine. Jev only after packet admission.
 */

import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  COMP_SET_TARGET_HOTELS,
  runCompSetDemandMiningForHotel,
  applyPublicIdentity,
  EVIDENCE_CLASS,
} from "../comp-set-demand-mining-v1/index.js";
import {
  evaluateCompleteDemandPacket,
  PACKET_QUALITY,
  isQualifiedForExpensiveCompletion,
} from "../complete-demand-packet-v8/packet-schema.js";
import { successfulPacketPatternMatch } from "../complete-demand-packet-v8/success-calibration.js";
import { completeDemandPacket, jevAdvisePacket } from "../complete-demand-packet-v8/packet-completion.js";
import { BASE_OF_DEMAND_LIST, GDI_BASE_OF_DEMAND, OPPORTUNITY_MATURITY } from "./taxonomy.js";
import { getHotelBaseWeighting, getTenBasesPilotHotels } from "./hotel-weighting.js";
import { buildGdiBaseOfDemandCoverage } from "./coverage.js";
import { buildBaseQueries } from "./base-queries.js";
import { assignOpportunityMaturity } from "./maturity.js";
import { buildHotelOpportunityThesis } from "./thesis.js";
import { buildApifyTenBasesInventory, enrichCompHotelsViaTripadvisor } from "./apify-base-map.js";
import {
  decomposePublishedEventDemand,
  decomposeIntlOrgMeeting,
  mineEventParticipants,
  buildRotationOpportunityIntelligence,
  buildRecurringCorporateMeetingPattern,
  buildCorporateTriggerMeetingThesis,
  expandMedicalDemandEcosystem,
  decomposeProjectWorkforceDemand,
  decomposeSportsEntertainmentDemand,
} from "./decomposers.js";
import {
  buildHotelHistoricalDemandPattern,
  buildCompHistoricalDemandPattern,
  findGdiLookalikeAccounts,
} from "./hotel-history-lookalike.js";
import { scoreChildAccount } from "./child-account-gate.js";

const B = GDI_BASE_OF_DEMAND;
const DIRECTORY_NOISE_RE =
  /\b(booking\.com|expedia|hotels\.com|trivago|tripadvisor\.com\/Hotel_Review|airbnb|kayak)\b/i;

function hasSerp() {
  return Boolean(String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim());
}

function emptyTally() {
  return {
    researched: false,
    signals: 0,
    researchLeads: 0,
    completeDemandPackets: 0,
    customerReady: 0,
    futureWatch: 0,
    predicted: 0,
    costUsd: 0,
    sourceFamilies: new Set(),
    languages: new Set(),
    feederMarkets: new Set(),
    gaps: [],
  };
}

function hitToSignal(hit, meta = {}) {
  const title = String(hit.title || "").trim();
  const link = String(hit.link || hit.url || "").trim();
  const snippet = String(hit.snippet || hit.description || "").trim();
  if (!title || !link) return null;
  if (DIRECTORY_NOISE_RE.test(`${title} ${link}`)) return null;
  const orgGuess = title.split(/[|\-—:]/)[0].trim().slice(0, 120);
  const yearMatch = `${title} ${snippet}`.match(/\b(202[6-9]|203[0-2])\b/);
  const lodgingHint =
    /\b(room block|host hotel|housing|accommodation|hôtel officiel|alojamiento|hébergement|hotel block|RFP|crew hotel|workforce lodging)\b/i.test(
      `${title} ${snippet}`
    );
  return {
    id: `gdi_b10_${String(meta.hotelKey || "h").toLowerCase()}_${String(title)
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .slice(0, 40)}_${String(meta.base || "x")
      .slice(0, 8)
      .toLowerCase()}`,
    title,
    organizationName: orgGuess,
    organization: orgGuess,
    officialSource: link,
    source: link,
    summaryWhat: snippet.slice(0, 280),
    snippet,
    eventYear: yearMatch ? yearMatch[1] : null,
    lodgingEvidence: lodgingHint ? { housingPageFound: true, status: "WEAK" } : null,
    hotelFitScore: meta.defaultFitScore ?? 48,
    defaultFitScore: meta.defaultFitScore ?? 48,
    market: meta.market,
    lodgingMarket: meta.market,
    feederMarket: meta.feederMarket || null,
    originMarket: meta.originMarket || null,
    baseOfDemand: meta.base,
    demandEngine: meta.demandEngineHint || null,
    signalType: "BASE_SERP",
    sourceFamily: "SERP_WEB",
    language: meta.language || "en",
    queryLocalized: meta.query,
  };
}

function inferDemandEngine(base, blob = "") {
  if (base === B.PHARMA_MEDICAL_ECOSYSTEM || /pharma|medical|investigator/i.test(blob)) {
    return "PHARMA_LIFE_SCIENCES";
  }
  if (base === B.SPORTS_ENTERTAINMENT_PRODUCTION || /tournament|team|production/i.test(blob)) {
    return "SPORTS_ENTERTAINMENT_SOCIAL";
  }
  if (base === B.PROJECT_WORKFORCE_DEMAND || /construction|contractor|workforce/i.test(blob)) {
    return "PROJECT_CREW_WORKFORCE";
  }
  if (base === B.INTERNATIONAL_ORG_RECURRING_GROUPS || /UNECE|WHO|WIPO|WTO|ILO/i.test(blob)) {
    return "ASSOCIATION_NGO";
  }
  if (base === B.RECURRING_CORPORATE_MEETINGS || base === B.CORPORATE_TRIGGER_DEMAND) {
    return "CORPORATE_MEETINGS";
  }
  return "ASSOCIATION_NGO";
}

function isNoiseAccount(org = "", title = "") {
  const b = `${org} ${title}`;
  if (/\bbest\b.{0,40}\bhotels?\b/i.test(b)) return true;
  if (/hotels?\s+in\s+/i.test(b) && /conference|best|top/i.test(b)) return true;
  if (/[\u{1F300}-\u{1FAFF}]/u.test(b)) return true;
  if (/we're excited|kicked off|is here!|🙌|🥳/i.test(b)) return true;
  if (/^https?:/i.test(org)) return true;
  return false;
}

function leadToRecord(lead, hotel, base) {
  return {
    id: lead.id,
    hotelKey: hotel.hotelKey,
    hotelId: hotel.hotelId,
    title: lead.hotelMotionHypothesis || `${lead.organization} — ${lead.role}`,
    organizationName: lead.organization || lead.namedEntity,
    organization: lead.organization || lead.namedEntity,
    officialSource: lead.officialSource,
    source: lead.officialSource,
    groupMotion: lead.role || lead.participantType || lead.groupMotion,
    groupType: lead.participantType || lead.role,
    buyerEntity: lead.buyerEntity,
    organizer: lead.organizer,
    publicContactPath: lead.publicContactPath,
    eventYear: lead.eventYear || lead.futureTiming,
    eventStartDate: lead.eventStartDate || null,
    nextKnownCycle: lead.nextConfirmedCycle || lead.nextPredictedCycle || null,
    decisionWindow: lead.decisionWindow || null,
    timingState: lead.timingState || null,
    isPredicted: lead.isPredicted || false,
    competitorHotel: lead.competitorHotel || lead.seedCompetitorHotel,
    evidenceClass: lead.evidenceClass,
    lodgingEvidence: lead.lodgingEvidence || (lead.accommodationSignal ? { status: "WEAK" } : null),
    hotelFitScore: hotel.defaultFitScore ?? 48,
    defaultFitScore: hotel.defaultFitScore ?? 48,
    market: hotel.market,
    lodgingMarket: hotel.market,
    baseOfDemand: base,
    demandEngine: lead.demandEngine || inferDemandEngine(base, lead.organization),
    signalType: "CHILD_ACCOUNT_LEAD",
    sourceFamily: lead.sourceFamily || "BASE_DECOMPOSITION",
    hotelMotionHypothesis: lead.hotelMotionHypothesis,
    fact: lead.whySimilar || (lead.participationEvidence ? "PARTICIPATION_OR_PATTERN" : ""),
    inferenceLabel: lead.inferenceLabel || null,
    groupSize: lead.groupSize || "UNKNOWN",
    generatorId: lead.generatorId,
  };
}

function decomposeForBase(base, generator, hotel) {
  switch (base) {
    case B.PUBLISHED_EVENT_DECOMPOSITION:
      return decomposePublishedEventDemand(generator, hotel);
    case B.INTERNATIONAL_ORG_RECURRING_GROUPS:
      return decomposeIntlOrgMeeting(generator, hotel);
    case B.PARTICIPANT_EXHIBITOR_SPONSOR_MINING:
      return mineEventParticipants(generator, hotel);
    case B.PHARMA_MEDICAL_ECOSYSTEM:
      return expandMedicalDemandEcosystem(generator, hotel);
    case B.PROJECT_WORKFORCE_DEMAND:
      return decomposeProjectWorkforceDemand(generator, hotel);
    case B.SPORTS_ENTERTAINMENT_PRODUCTION:
      return decomposeSportsEntertainmentDemand(generator, hotel);
    default:
      return null;
  }
}

/**
 * Run all priority bases for one hotel.
 */
export async function runTenBasesForHotel(hotel = {}, opts = {}) {
  const weighting = getHotelBaseWeighting(hotel.hotelKey);
  const basesToRun = [
    ...weighting.high.map((b) => ({ base: b, priority: "HIGH" })),
    ...weighting.medium.map((b) => ({ base: b, priority: "MEDIUM" })),
  ];

  const tallies = Object.fromEntries(BASE_OF_DEMAND_LIST.map((b) => [b, emptyTally()]));
  const artifacts = {
    signals: [],
    generators: [],
    children: [],
    researchLeads: [],
    packets: [],
    predicted: [],
    theses: [],
    jevLog: [],
    rotation: [],
    corporateRecurring: [],
    corporateTrigger: [],
    lookalikes: [],
    hotelHistory: [],
    compPatterns: [],
    publishedDecomp: [],
    intlDelegations: [],
    participants: [],
    pharma: [],
    project: [],
    sports: [],
    multilingual: [],
    feeder: [],
    apifyRows: [],
    compTraces: [],
  };

  let costUsd = 0;
  const nowDate = opts.nowDate || "2026-10-04";

  // Comp-set once — feeds Base 10 + lodging pillar
  const comp = await runCompSetDemandMiningForHotel(hotel, {
    nowDate,
    maxCompetitors: opts.maxCompetitors ?? 3,
    maxQueries: opts.maxCompQueries ?? 8,
    maxPivotsPerCompetitor: 2,
    maxPagesPerCompetitor: 2,
    resolveIdentity: true,
  });
  costUsd += comp.costUsd || 0;
  artifacts.compTraces = comp.traces || [];

  let apify = { enabled: false, results: [], costUsd: 0 };
  // Apify is opt-in only (product default: off). Never enable for production GDI parity path.
  if (opts.enableApify === true) {
    apify = await enrichCompHotelsViaTripadvisor(comp.competitors || [], {
      maxComps: opts.maxApifyComps ?? 2,
    });
    costUsd += apify.costUsd || 0;
    for (const row of apify.results || []) {
      artifacts.apifyRows.push({
        hotelKey: hotel.hotelKey,
        baseOfDemand: B.HOTEL_HISTORY_LOOKALIKE,
        ...row,
        signalOnly: true,
        verifiedTruth: false,
      });
      if (!row.ok) continue;
      const idx = (comp.competitors || []).findIndex(
        (c) => c.competitorHotelId === row.competitorHotelId
      );
      if (idx >= 0) {
        comp.competitors[idx] = applyPublicIdentity(comp.competitors[idx], {
          publicPhone: row.publicPhone,
          address: row.address,
          domain: row.domain,
          currentWebsite: row.currentWebsite,
        });
      }
    }
  }

  // Comp traces as generators for relevant bases
  for (const t of comp.traces || []) {
    if (
      t.evidenceClass !== EVIDENCE_CLASS.DIRECT_CONFIRMED &&
      t.evidenceClass !== EVIDENCE_CLASS.STRONG_ASSOCIATION &&
      t.evidenceClass !== EVIDENCE_CLASS.WEAK_ASSOCIATION
    ) {
      continue;
    }
    const gen = {
      id: t.traceId,
      title: t.eventProgram || t.organization,
      organizationName: t.organization,
      organization: t.organization,
      officialSource: t.source,
      source: t.source,
      eventYear: t.eventYear,
      eventStartDate: t.eventDate,
      competitorHotel: t.competitorHotel,
      evidenceClass: t.evidenceClass,
      lodgingEvidence:
        t.evidenceClass === EVIDENCE_CLASS.DIRECT_CONFIRMED ||
        t.evidenceClass === EVIDENCE_CLASS.STRONG_ASSOCIATION
          ? { housingPageFound: true, status: "WEAK" }
          : null,
      fact: t.fact,
      snippet: t.fact,
      demandEngine: t.demandEngine,
      groupType: t.groupType,
    };
    artifacts.generators.push({ ...gen, baseOfDemand: "COMP_TRACE", hotelKey: hotel.hotelKey });
  }

  // Base 10 history + lookalike from comp patterns
  {
    const hist = buildHotelHistoricalDemandPattern(hotel, opts.privateHistory || []);
    const compHist = buildCompHistoricalDemandPattern(hotel, comp.traces || []);
    artifacts.hotelHistory.push(
      ...hist.patterns.map((p) => ({ ...p, hotelKey: hotel.hotelKey })),
      ...(!hist.available ? compHist.patterns.map((p) => ({ ...p, hotelKey: hotel.hotelKey })) : [])
    );
    artifacts.compPatterns.push(...compHist.patterns);
    const patterns = hist.available ? hist.patterns : compHist.patterns;
    const lookalikes = findGdiLookalikeAccounts(hotel, patterns, { maxLookalikes: 6 });
    artifacts.lookalikes.push(...lookalikes);
    const t10 = tallies[B.HOTEL_HISTORY_LOOKALIKE];
    t10.researched = true;
    t10.signals += patterns.length + lookalikes.length;
    t10.sourceFamilies.add(hist.available ? "PRIVATE_HOTEL_HISTORY" : "COMP_HISTORICAL_DEMAND");
    t10.sourceFamilies.add("APIFY_TRIPADVISOR_IDENTITY");
    t10.costUsd += (comp.costUsd || 0) * 0.25 + (apify.costUsd || 0);

    for (const lk of lookalikes) {
      const gate = scoreChildAccount(
        {
          ...lk,
          participationEvidence: true,
          historicAttendance: true,
          knownTravelPattern: true,
        },
        { title: lk.seedOrganization, organization: lk.seedOrganization },
        hotel
      );
      artifacts.children.push({ ...lk, ...gate, baseOfDemand: B.HOTEL_HISTORY_LOOKALIKE });
      if (gate.admitAsLead && lk.namedEntity) {
        artifacts.researchLeads.push(
          leadToRecord({ ...lk, ...gate, sourceFamily: "LOOKALIKE" }, hotel, B.HOTEL_HISTORY_LOOKALIKE)
        );
        t10.researchLeads += 1;
      }
    }
  }

  // Per-base SERP + decomposition
  for (const { base, priority } of basesToRun) {
    const tally = tallies[base];
    tally.researched = true;
    let queries = buildBaseQueries(hotel, base, {
      priority,
      maxQueries: opts.maxQueriesPerBase ?? (priority === "HIGH" ? 3 : 2),
    });
    // Language-aware shared generator — enrich provenance; keep base-queries as core set.
    if (opts.useGdiDiscoveryQueries !== false) {
      try {
        const { buildGdiDiscoveryQueries } = await import(
          "../discovery/gdi-discovery-queries-v1.js"
        );
        const lanePack = buildGdiDiscoveryQueries({
          hotelProfile: hotel,
          hotelId: hotel.hotelId,
          market: hotel.market || hotel.destinationMarket,
          country: hotel.country,
          baseOfDemand: base,
          lane: opts.queryLane || "COMBINED",
          maxPerBase: opts.maxQueriesPerBase ?? (priority === "HIGH" ? 3 : 2),
        });
        const byQ = new Map(queries.map((q) => [q.query, q]));
        for (const gq of lanePack.queries || []) {
          if (!byQ.has(gq.query) && queries.length < (opts.maxQueriesPerBase ?? 4) + 2) {
            queries.push({
              baseOfDemand: base,
              query: gq.query,
              language: gq.queryLanguage,
              marketRole: gq.marketRole || "DESTINATION",
              feederMarket: gq.feederMarket || null,
              queryLanguage: gq.queryLanguage,
              sourceLanguage: gq.sourceLanguage,
              queryFamily: gq.queryFamily,
              serpLocale: gq.serpLocale,
            });
          } else if (byQ.has(gq.query)) {
            Object.assign(byQ.get(gq.query), {
              queryLanguage: gq.queryLanguage,
              sourceLanguage: gq.sourceLanguage,
              queryFamily: gq.queryFamily,
              serpLocale: gq.serpLocale,
            });
          }
        }
      } catch {
        /* generator optional — base-queries remain authoritative fallback */
      }
    }

    const baseSignals = [];
    for (const q of queries) {
      tally.languages.add(q.language || q.queryLanguage);
      if (q.feederMarket) tally.feederMarkets.add(q.feederMarket);
      tally.sourceFamilies.add("SERP_WEB");
      artifacts.multilingual.push({
        hotelKey: hotel.hotelKey,
        baseOfDemand: base,
        language: q.language || q.queryLanguage,
        query: q.query,
        queryLanguage: q.queryLanguage || q.language,
        sourceLanguage: q.sourceLanguage || q.language,
        queryFamily: q.queryFamily || null,
        market: hotel.market || hotel.destinationMarket || null,
      });
      if (q.marketRole === "FEEDER") {
        artifacts.feeder.push({
          hotelKey: hotel.hotelKey,
          baseOfDemand: base,
          feederMarket: q.feederMarket,
          query: q.query,
        });
      }

      if (!hasSerp()) {
        tally.gaps.push("SERPAPI_MISSING");
        continue;
      }
      try {
        const serp = await serpapiSearch({
          q: q.query,
          num: 5,
          hl: q.language === "gl" ? "es" : q.language || hotel.serpHlPrimary || "en",
          gl: hotel.serpGl || "us",
        });
        costUsd += 0.01;
        tally.costUsd += 0.01;
        const hits = serp?.data?.organic_results || [];
        for (const hit of hits) {
          const sig = hitToSignal(hit, {
            hotelKey: hotel.hotelKey,
            base,
            market: hotel.market,
            language: q.language,
            query: q.query,
            feederMarket: q.feederMarket,
            originMarket: q.originMarket,
            defaultFitScore: hotel.defaultFitScore,
            demandEngineHint: inferDemandEngine(base, `${hit.title} ${hit.snippet}`),
          });
          if (!sig) continue;
          baseSignals.push(sig);
          artifacts.signals.push(sig);
          tally.signals += 1;
        }
      } catch (err) {
        tally.gaps.push(`SERP_ERROR:${String(err.message || err).slice(0, 40)}`);
      }
    }

    // Also seed from strong/weak comp traces for this base when thematic match
    const thematic = (artifacts.generators || []).filter((g) => {
      const blob = `${g.title} ${g.fact || ""}`;
      if (base === B.SPORTS_ENTERTAINMENT_PRODUCTION) return /sport|tournament|team|crew/i.test(blob);
      if (base === B.PHARMA_MEDICAL_ECOSYSTEM) return /pharma|medical|congress|advisory/i.test(blob);
      if (base === B.RECURRING_CORPORATE_MEETINGS) return /kickoff|corporate|partner|training/i.test(blob);
      if (base === B.PROJECT_WORKFORCE_DEMAND) return /project|construction|crew|workforce/i.test(blob);
      if (base === B.PUBLISHED_EVENT_DECOMPOSITION) return /conference|congress|summit|expo/i.test(blob);
      if (base === B.INTERNATIONAL_ORG_RECURRING_GROUPS) {
        return /UNECE|WHO|WIPO|WTO|ILO|UNHCR|delegation|working group/i.test(blob);
      }
      if (base === B.HISTORIC_ROTATION_PREDICTION) return /rotat|host|annual|202[6-9]/i.test(blob);
      if (base === B.CORPORATE_TRIGGER_DEMAND) return /acquisit|merger|opening|expansion|CEO/i.test(blob);
      if (base === B.PARTICIPANT_EXHIBITOR_SPONSOR_MINING) return /sponsor|exhibitor|speaker/i.test(blob);
      return false;
    });

    const generators = [...baseSignals, ...thematic].slice(0, priority === "HIGH" ? 8 : 4);

    for (const gen of generators) {
      gen.baseOfDemand = base;
      gen.demandEngine = gen.demandEngine || inferDemandEngine(base, `${gen.title} ${gen.snippet || ""}`);

      if (base === B.HISTORIC_ROTATION_PREDICTION) {
        const rot = buildRotationOpportunityIntelligence(gen, hotel);
        artifacts.rotation.push(rot);
        // Predicted child from rotation when buyer/organizer path exists
        if (rot.organizer && rot.decisionWindow && rot.targetMarketPlausible) {
          const rec = leadToRecord(
            {
              id: `rot_lead_${rot.eventSeriesId}`,
              organization: rot.organization || rot.organizer,
              namedEntity: rot.organization || rot.organizer,
              role: "ROTATION_SERIES_BUYER",
              participantType: "ROTATION",
              participationEvidence: true,
              officialSource: rot.officialSource,
              futureTiming: rot.nextConfirmedCycle || rot.nextPredictedCycle,
              nextConfirmedCycle: rot.nextConfirmedCycle,
              nextPredictedCycle: rot.nextPredictedCycle,
              decisionWindow: rot.decisionWindow,
              timingState: rot.timingState,
              isPredicted: rot.isPredicted,
              organizer: rot.organizer,
              buyerEntity: rot.buyer || rot.organizer,
              accommodationSignal: rot.housingHistory === "HINT",
              hotelMotionHypothesis: `Predicted next cycle lodging for ${rot.title}`,
              admitAsLead: true,
              score: 70,
            },
            hotel,
            base
          );
          artifacts.researchLeads.push(rec);
          tally.researchLeads += 1;
        }
        continue;
      }

      if (base === B.RECURRING_CORPORATE_MEETINGS) {
        const pat = buildRecurringCorporateMeetingPattern(gen, hotel);
        artifacts.corporateRecurring.push(pat);
        if (pat.company && pat.company.length >= 4) {
          artifacts.researchLeads.push(
            leadToRecord(
              {
                id: `corp_rec_${String(pat.company).slice(0, 40)}_${hotel.hotelKey}`,
                organization: pat.company,
                namedEntity: pat.company,
                role: pat.meetingType,
                participantType: "CORPORATE_MEETING",
                participationEvidence: true,
                officialSource: pat.officialSource,
                futureTiming: pat.nextLikelyCycle,
                decisionWindow: pat.decisionWindow,
                timingState: pat.timingState,
                isPredicted: pat.isPredicted,
                buyerEntity: pat.buyerFunction,
                organizer: pat.company,
                competitorHotel: pat.historicHotels,
                hotelMotionHypothesis: `${pat.meetingType} lodging pattern`,
                admitAsLead: true,
                score: 65,
              },
              hotel,
              base
            )
          );
          tally.researchLeads += 1;
        }
        continue;
      }

      if (base === B.CORPORATE_TRIGGER_DEMAND) {
        const thesis = buildCorporateTriggerMeetingThesis(gen, hotel);
        artifacts.corporateTrigger.push(thesis);
        if (thesis.supportingEvidencePresent && thesis.company) {
          artifacts.researchLeads.push(
            leadToRecord(
              {
                id: `corp_trig_${String(thesis.company).slice(0, 40)}_${hotel.hotelKey}`,
                organization: thesis.company,
                namedEntity: thesis.company,
                role: thesis.triggerType,
                participantType: "CORPORATE_TRIGGER",
                participationEvidence: true,
                officialSource: thesis.officialSource,
                futureTiming: thesis.expectedTimingRange,
                decisionWindow: thesis.expectedTimingRange,
                timingState: thesis.timingState,
                isPredicted: true,
                buyerEntity: thesis.whoLikelyControls,
                inferenceLabel: thesis.inferenceLabel,
                hotelMotionHypothesis: thesis.whyGroupMotionMayFollow,
                admitAsLead: true,
                score: 60,
              },
              hotel,
              base
            )
          );
          tally.researchLeads += 1;
        }
        continue;
      }

      if (base === B.HOTEL_HISTORY_LOOKALIKE) continue;

      const decomp = decomposeForBase(base, gen, hotel);
      if (!decomp) continue;

      artifacts.children.push(...decomp.children);
      if (base === B.PUBLISHED_EVENT_DECOMPOSITION) artifacts.publishedDecomp.push(decomp);
      if (base === B.INTERNATIONAL_ORG_RECURRING_GROUPS) artifacts.intlDelegations.push(decomp);
      if (base === B.PARTICIPANT_EXHIBITOR_SPONSOR_MINING) artifacts.participants.push(decomp);
      if (base === B.PHARMA_MEDICAL_ECOSYSTEM) artifacts.pharma.push(decomp);
      if (base === B.PROJECT_WORKFORCE_DEMAND) artifacts.project.push(decomp);
      if (base === B.SPORTS_ENTERTAINMENT_PRODUCTION) artifacts.sports.push(decomp);

      for (const lead of decomp.leads) {
        artifacts.researchLeads.push(leadToRecord(lead, hotel, base));
        tally.researchLeads += 1;
      }
    }

    if (tally.researchLeads === 0 && tally.signals > 0) {
      tally.gaps.push("SIGNALS_WITHOUT_CHILD_LEADS");
    }
  }

  // Packet evaluation + maturity + limited completion/Jev
  const completePackets = [];
  const finalOpps = [];
  let jevIssued = 0;
  let jevPillarsResolved = 0;
  let jevClassChanges = 0;
  let depths = [];

  // Deduplicate leads
  const seenLead = new Set();
  const uniqueLeads = [];
  for (const lead of artifacts.researchLeads) {
    const k = `${lead.hotelKey}|${String(lead.organizationName || "").toLowerCase()}|${lead.baseOfDemand}`;
    if (seenLead.has(k)) continue;
    seenLead.add(k);
    uniqueLeads.push(lead);
  }
  artifacts.researchLeads = uniqueLeads;

  for (const lead of uniqueLeads) {
    if (isNoiseAccount(lead.organizationName || lead.organization, lead.title)) {
      lead.packetQuality = PACKET_QUALITY.REJECTED;
      lead.maturity = OPPORTUNITY_MATURITY.REJECTED;
      lead.customerReady = false;
      lead.validFutureWatch = false;
      artifacts.packets.push(lead);
      continue;
    }

    const packetEval = evaluateCompleteDemandPacket(lead, {
      defaultFitScore: hotel.defaultFitScore,
      geoOk: true,
    });
    lead.packetQuality = packetEval.quality;
    lead.pillars = packetEval.pillars;
    const successMatchResult = successfulPacketPatternMatch(packetEval, {
      demandEngine: lead.demandEngine,
    });
    const successMatch = successMatchResult?.match || successMatchResult;
    lead.successMatch = successMatch;

    let working = lead;
    let depth = 0;
    if (
      isQualifiedForExpensiveCompletion(packetEval.quality) ||
      (packetEval.presentOrStrongCount >= 4 &&
        (successMatch === "STRONG" || successMatch === "PLAUSIBLE"))
    ) {
      const before = packetEval.quality;
      const result = await completeDemandPacket(working, hotel, {
        maxSteps: 1,
        successMatch,
        admitted: true,
      });
      costUsd += result.costUsd || 0;
      working = result.record || working;
      depth = result.depth || 0;
      depths.push(depth);
      const afterEval = evaluateCompleteDemandPacket(working, {
        defaultFitScore: hotel.defaultFitScore,
        geoOk: true,
      });
      working.packetQuality = afterEval.quality;
      working.pillars = afterEval.pillars;
      if (afterEval.quality !== before) jevClassChanges += 1;
      jevPillarsResolved += Number(result.pillarsResolved || 0);

      const advice = jevAdvisePacket(afterEval, {
        admitted: true,
        successMatch,
        depth,
      });
      if (advice.issued) {
        jevIssued += 1;
        artifacts.jevLog.push({
          hotelKey: hotel.hotelKey,
          opportunityId: working.id,
          baseOfDemand: working.baseOfDemand,
          ...advice,
          wroteFacts: false,
          promoted: false,
        });
      }
    }

    const maturity = assignOpportunityMaturity(working, {
      quality: working.packetQuality,
      pillars: working.pillars,
    });
    working.maturity = maturity.maturity;
    working.predictedLabel = maturity.predictedLabel;
    working.customerReady = maturity.customerReady;
    working.validFutureWatch = maturity.validFutureWatch;

    const thesis = buildHotelOpportunityThesis(working, hotel, {
      quality: working.packetQuality,
    });
    artifacts.theses.push(thesis);

    const base = working.baseOfDemand;
    if (tallies[base]) {
      if (
        working.packetQuality === PACKET_QUALITY.COMPLETE_STRONG ||
        working.packetQuality === PACKET_QUALITY.COMPLETE_PLAUSIBLE
      ) {
        tallies[base].completeDemandPackets += 1;
        completePackets.push(working);
      }
      if (maturity.maturity === OPPORTUNITY_MATURITY.PREDICTED_OPPORTUNITY) {
        tallies[base].predicted += 1;
        artifacts.predicted.push(working);
      }
      if (maturity.customerReady) tallies[base].customerReady += 1;
      if (maturity.validFutureWatch) tallies[base].futureWatch += 1;
    }

    artifacts.packets.push(working);
    if (
      maturity.customerReady ||
      maturity.validFutureWatch ||
      maturity.maturity === OPPORTUNITY_MATURITY.PREDICTED_OPPORTUNITY ||
      working.packetQuality === PACKET_QUALITY.COMPLETE_STRONG ||
      working.packetQuality === PACKET_QUALITY.COMPLETE_PLAUSIBLE
    ) {
      finalOpps.push(working);
    }
  }

  // Normalize tally sets for coverage
  const talliesForCoverage = {};
  for (const base of BASE_OF_DEMAND_LIST) {
    const t = tallies[base];
    talliesForCoverage[base] = {
      ...t,
      sourceFamilies: [...t.sourceFamilies],
      languages: [...t.languages],
      feederMarkets: [...t.feederMarkets],
    };
  }

  const coverage = buildGdiBaseOfDemandCoverage(hotel, talliesForCoverage, { nowDate });

  return {
    hotelKey: hotel.hotelKey,
    hotel,
    costUsd: Number(costUsd.toFixed(4)),
    coverage,
    tallies: talliesForCoverage,
    artifacts,
    completePackets,
    finalOpps,
    metrics: {
      signals: artifacts.signals.length,
      generators: artifacts.generators.length + artifacts.signals.length,
      children: artifacts.children.length,
      researchLeads: artifacts.researchLeads.length,
      completePackets: completePackets.length,
      predicted: artifacts.predicted.length,
      customerReady: finalOpps.filter((o) => o.customerReady).length,
      futureWatch: finalOpps.filter((o) => o.validFutureWatch).length,
      jevIssued,
      jevPillarsResolved,
      jevClassChanges,
      avgDepth: depths.length ? depths.reduce((a, b) => a + b, 0) / depths.length : 0,
      validatedCompTraces: (comp.traces || []).filter(
        (t) =>
          t.evidenceClass === EVIDENCE_CLASS.DIRECT_CONFIRMED ||
          t.evidenceClass === EVIDENCE_CLASS.STRONG_ASSOCIATION
      ).length,
    },
  };
}

export { COMP_SET_TARGET_HOTELS, getTenBasesPilotHotels, buildApifyTenBasesInventory };
