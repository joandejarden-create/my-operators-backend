/**
 * GDI Hidden Demand V3 — reprocess V2 strict survivors.
 * Triage → deepen → team/travel/lodging → hotel match only after lodging proof.
 * Discover once / match many. No Surfe. No Webhound required.
 */

import { entityToHiddenDemand } from "./source-expansion-v2.js";
import {
  matchHiddenDemandToHotels,
  hotelMatchToOpportunityCandidate,
} from "./index.js";
import { triageExhibitorResearchValue } from "./v3-triage.js";
import {
  cleanEntityDisplayName,
  isV3QueueNoise,
  scoreV3QueuePriority,
} from "./v3-entity-clean.js";
import {
  classifyTravelLikelihood,
  classifyTeamSizeBand,
  classifyLodgingProof,
  candidateStateFromEvidence,
  mayBecomeHotelOpportunity,
} from "./v3-lodging-ladder.js";
import { deepenExhibitorEntity } from "./v3-team-research.js";
import {
  decideExhibitorTeamResearchPath,
  decideLodgingEvidenceNextStep,
} from "./jev-v3-routing.js";
import {
  classifyEventGeography,
  eventEligibleForMidtownHotels,
} from "./v3-event-geography.js";
import {
  passesCustomerPromotionGateV3,
  mapContactDepthBucket,
} from "./v3-promotion-gate.js";
import {
  CANDIDATE_STATE,
  RESEARCH_TRIAGE,
  LODGING_PROOF,
  ADDRESSABILITY,
  HIDDEN_DEMAND_V3,
  TRAVEL_LIKELIHOOD,
} from "./v3-states.js";
import { TRAVEL_CLASS } from "./v3-constants.js";
import { travelIncreasesLodging } from "./origin-travel.js";
import { LODGING_SIGNAL_STRENGTH } from "./v2-constants.js";
import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../hotel-intelligence/room-count-research/fetch.js";
import { extractHousingSignalsFromText } from "./extract-pdf.js";

function hasSerp() {
  return !!(process.env.SERPAPI_API_KEY || process.env.SERPAPI_KEY);
}

function emptyJev() {
  return {
    calls: 0,
    safeApply: 0,
    exhibitorTeamPath: 0,
    lodgingNextStep: 0,
    helpfulDifferent: 0,
    same: 0,
    wrong: 0,
    highConfWrong: 0,
    fetchesAvoided: 0,
    teamEvidenceViaJev: 0,
    lodgingEvidenceViaJev: 0,
    whoUpgrades: 0,
    lowValueStopped: 0,
    feedback: [],
  };
}

function recordJev(ledger, row) {
  ledger.jev.feedback.push(row);
  ledger.jev.calls += 1;
  if (row.applied) ledger.jev.safeApply += 1;
  if (row.decisionType === "EXHIBITOR_TEAM_RESEARCH_PATH") {
    ledger.jev.exhibitorTeamPath += 1;
  }
  if (row.decisionType === "LODGING_EVIDENCE_NEXT_STEP") {
    ledger.jev.lodgingNextStep += 1;
  }
  if (row.outcome === "HELPFUL_DIFFERENT") ledger.jev.helpfulDifferent += 1;
  else if (row.outcome === "SAME") ledger.jev.same += 1;
  else if (row.outcome === "WRONG") ledger.jev.wrong += 1;
  if (row.highConfWrong) ledger.jev.highConfWrong += 1;
}

function lodgingProofToV2Strength(proof) {
  if (proof === LODGING_PROOF.CONFIRMED) return LODGING_SIGNAL_STRENGTH.STRONG;
  if (proof === LODGING_PROOF.STRONG_INFERENCE) return LODGING_SIGNAL_STRENGTH.MEDIUM;
  if (proof === LODGING_PROOF.PLAUSIBLE) return LODGING_SIGNAL_STRENGTH.WEAK;
  return LODGING_SIGNAL_STRENGTH.UNKNOWN;
}

function buildSourceGraph(entity, deepen, lodging) {
  const edges = [];
  const push = (from, to, kind, url) => {
    edges.push({ from, to, kind, url: url || null, provenance: kind });
  };
  const event = entity.demandGeneratorName || "EVENT";
  const company = cleanEntityDisplayName(entity.entityName);
  push(event, entity.sourceType || "SOURCE", "EVENT_TO_SOURCE", entity.sourceURL);
  push(entity.sourceType || "SOURCE", company, "SOURCE_TO_COMPANY", entity.sourceURL);
  for (const p of deepen?.pages || []) {
    push(company, p.url, p.kind === "pdf" ? "COMPANY_TO_PDF" : "COMPANY_TO_PAGE", p.url);
  }
  if (deepen?.team?.namedPeople?.length) {
    for (const n of deepen.team.namedPeople.slice(0, 5)) {
      push(company, n.name, "COMPANY_TO_EMPLOYEE", null);
    }
  }
  if (lodging?.lodgingProof && lodging.lodgingProof !== LODGING_PROOF.UNKNOWN) {
    push(company, lodging.lodgingProof, "COMPANY_TO_LODGING", null);
  }
  for (const c of deepen?.who || []) {
    if (c.name) push(company, c.name, "COMPANY_TO_CONTACT", null);
  }
  return edges;
}

function buildEvidencePacket(row) {
  return {
    company: row.company,
    generator: row.demandGeneratorName,
    futureTiming: row.futureTiming,
    year: row.year,
    participationEvidence: {
      role: row.participationRole,
      booth: row.boothNumber,
      sourceType: row.sourceType,
      snippet: row.evidenceSnippet,
    },
    teamEvidence: row.team,
    travelEvidence: {
      likelihood: row.travelLikelihood,
      locality: row.locality,
      travelClass: row.travelClass,
    },
    lodgingEvidence: {
      proof: row.lodgingProof,
      level: row.lodgingLevel,
      signals: row.lodgingSignals,
      housing: row.housing,
    },
    who: row.who,
    how: {
      addressability: row.addressability,
      contactDepth: row.contactDepth,
    },
    hotelMatches: row.hotelMatches,
    sourceChain: row.sourceGraph,
    candidateState: row.candidateState,
    eventGeography: row.eventGeography,
  };
}

/**
 * Load V2 strict survivors (quality gate) without rediscovery.
 */
export function loadV2StrictSurvivors(gatedEntities = [], qualityGateFn) {
  const out = [];
  for (const e of gatedEntities) {
    const g = qualityGateFn(e);
    if (!g.ok) continue;
    out.push({ ...e, entityName: cleanEntityDisplayName(e.entityName) });
  }
  return out;
}

/**
 * @param {{ entities, hotelConfigs, opts }}
 */
export async function runHiddenDemandV3Reprocess({
  entities = [],
  hotelConfigs = [],
  marketKey = "nyc_midtown",
  deepLimit = 25,
  maxQueriesPerEntity = 5,
  maxFetchesPerEntity = 8,
  maxAdditionalDirectories = 10,
  maxHousingQueries = 8,
  enableJev = true,
  enableLiveResearch = true,
} = {}) {
  const ledger = {
    version: HIDDEN_DEMAND_V3,
    startedAt: new Date().toISOString(),
    v2StrictCount: entities.length,
    triage: {
      HIGH_RESEARCH_VALUE: 0,
      MEDIUM_RESEARCH_VALUE: 0,
      LOW_RESEARCH_VALUE: 0,
      STOP: 0,
    },
    deeplyResearched: 0,
    teamSupported: 0,
    multipleNamed: 0,
    companyEventPage: 0,
    speakerStaff: 0,
    agency: 0,
    travel: { STRONG: 0, MEDIUM: 0, WEAK: 0, UNKNOWN: 0 },
    lodging: {
      CONFIRMED: 0,
      STRONG_INFERENCE: 0,
      PLAUSIBLE: 0,
      WEAK: 0,
      UNKNOWN: 0,
    },
    states: Object.fromEntries(Object.values(CANDIDATE_STATE).map((s) => [s, 0])),
    pdfEnrichment: { used: 0, teamUpgrades: 0, contactUpgrades: 0, lodgingUpgrades: 0 },
    housingSources: { found: 0, enriched: 0 },
    expansion: { directories: 0, entities: 0, fetches: 0 },
    jev: emptyJev(),
    rows: [],
    evidencePackets: [],
    hotelResults: {},
    fetchesUsed: 0,
    queriesUsed: 0,
  };

  for (const cfg of hotelConfigs) {
    ledger.hotelResults[cfg.hotelId] = {
      hotelId: cfg.hotelId,
      displayName: cfg.displayName,
      matched: 0,
      hotelOpportunity: 0,
      actionable: 0,
      watch: 0,
      neither: 0,
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

  // --- Phase 0: triage all ---
  const triaged = [];
  for (const ent of entities) {
    const t = triageExhibitorResearchValue(ent);
    ledger.triage[t.triage] = (ledger.triage[t.triage] || 0) + 1;
    triaged.push({ entity: ent, triage: t });
  }

  const researchQueue = triaged
    .filter(
      (r) =>
        r.triage.triage === RESEARCH_TRIAGE.HIGH_RESEARCH_VALUE ||
        r.triage.triage === RESEARCH_TRIAGE.MEDIUM_RESEARCH_VALUE
    )
    .sort(
      (a, b) =>
        scoreV3QueuePriority(b.entity) +
        b.triage.score -
        (scoreV3QueuePriority(a.entity) + a.triage.score)
    )
    .slice(0, deepLimit);

  const deepKeys = new Set(
    researchQueue.map((r) => r.entity.normalizeKey || r.entity.entityName.toLowerCase())
  );

  // Optional housing discovery (market-level, once)
  let sharedHousing = null;
  if (enableLiveResearch && hasSerp() && maxHousingQueries > 0) {
    const hq = [
      `New York Midtown trade show official housing hotel block ${new Date().getUTCFullYear() + 1}`,
      `Javits exhibitor housing accommodations room block`,
      `"official housing" exhibitor New York hotel`,
    ].slice(0, Math.min(3, maxHousingQueries));
    for (const q of hq) {
      try {
        ledger.queriesUsed += 1;
        const serp = await serpapiSearch({
          engine: "google",
          q,
          num: 5,
          hl: "en",
          gl: "us",
        });
        for (const hit of serp?.data?.organic_results || []) {
          const blob = `${hit.title || ""} ${hit.snippet || ""} ${hit.link || ""}`;
          if (/housing|hotel\s*block|room\s*block|accommodation/i.test(blob)) {
            ledger.housingSources.found += 1;
            if (hit.link && ledger.fetchesUsed < 15) {
              try {
                ledger.fetchesUsed += 1;
                const page = await fetchResearchPage(hit.link);
                if (page.ok) {
                  const text = htmlToSearchableText(page.text || "");
                  const h = extractHousingSignalsFromText(text, page.url || hit.link);
                  if (h.roomBlockMentioned || h.housingPageFound) {
                    sharedHousing = h;
                    ledger.housingSources.enriched += 1;
                  }
                }
              } catch {
                /* continue */
              }
            }
          }
        }
      } catch {
        /* continue */
      }
    }
  }

  // --- Phase 1: process each entity ---
  for (const { entity, triage } of triaged) {
    const company = cleanEntityDisplayName(entity.entityName);
    const geo = classifyEventGeography(entity);
    const key = entity.normalizeKey || company.toLowerCase();
    const doDeep = deepKeys.has(key) && enableLiveResearch && triage.triage !== RESEARCH_TRIAGE.STOP;

    let deepen = null;
    let pathHint = "COMPANY_EVENT_PAGE";
    let lodgingPath = "HOUSING_SOURCE";
    let teamBefore = false;

    if (doDeep) {
      ledger.deeplyResearched += 1;

      // Jev team path
      if (enableJev) {
        try {
          const teamDec = await decideExhibitorTeamResearchPath({
            company,
            eventRelationship: entity.participationRole,
            origin: triage.locality,
            participationIntensity: triage.participation?.participationDepth,
            knownEmployees: 0,
            travelEvidence: "unknown",
            gaps: ["team", "travel", "who"],
            remainingBudget: maxFetchesPerEntity,
            hasEventPage: false,
            hasTeam: false,
            hasWho: false,
            travelGap: true,
            enableSafeApply: true,
          });
          recordJev(ledger, {
            decisionType: "EXHIBITOR_TEAM_RESEARCH_PATH",
            company,
            defaultPath: teamDec.defaultPath,
            jevPath: teamDec.jevPath,
            finalPath: teamDec.finalPath,
            applied: teamDec.applied,
            outcome: teamDec.applied
              ? "HELPFUL_DIFFERENT"
              : teamDec.agreement
                ? "SAME"
                : "UNKNOWN",
          });
          pathHint = teamDec.finalPath;
          if (String(pathHint).startsWith("STOP")) {
            ledger.jev.fetchesAvoided += 1;
            ledger.jev.lowValueStopped += 1;
          }
        } catch {
          /* non-fatal */
        }
      }

      if (!String(pathHint).startsWith("STOP")) {
        deepen = await deepenExhibitorEntity(entity, {
          maxQueries: maxQueriesPerEntity,
          maxFetches: maxFetchesPerEntity,
          pathHint,
        });
        ledger.queriesUsed += deepen.queryCount || 0;
        ledger.fetchesUsed += deepen.fetchCount || 0;
        teamBefore = Boolean(deepen.team?.teamSupported);

        if ((deepen.pages || []).some((p) => p.kind === "pdf")) {
          ledger.pdfEnrichment.used += 1;
        }
      } else {
        deepen = {
          company,
          team: { teamSupported: false, namedPeople: [], companyEventPage: false },
          who: [],
          addressability: ADDRESSABILITY.COMPANY_PATH,
          housing: null,
          deepenText: "",
          pages: [],
          queryCount: 0,
          fetchCount: 0,
        };
      }
    } else {
      // Shallow: use existing entity fields only
      deepen = {
        company,
        team: {
          teamSupported: Boolean(entity.lodgingSignals?.teamCohort),
          namedPeople: [],
          companyEventPage: false,
          speakerStaffSignal: false,
          agencyRelationship: null,
          multiDay: Boolean(entity.lodgingSignals?.multiDay),
          setupBreakdown: false,
        },
        who: entity.contacts || [],
        addressability: ADDRESSABILITY.COMPANY_PATH,
        housing: entity.housingEvidence || null,
        deepenText: entity.evidenceSnippet || "",
        pages: [],
        queryCount: 0,
        fetchCount: 0,
        shallow: true,
      };
      if (triage.triage === RESEARCH_TRIAGE.STOP || triage.triage === RESEARCH_TRIAGE.LOW_RESEARCH_VALUE) {
        ledger.jev.lowValueStopped += 1;
      }
    }

    const team = deepen.team || {};
    const travelClass = triage.origin?.travelClass;
    const travelLikelihood = classifyTravelLikelihood({
      travelClass: deepen.shallow ? TRAVEL_CLASS.UNKNOWN : travelClass,
      teamSupported: deepen.shallow ? false : team.teamSupported,
      namedPeople: deepen.shallow ? 0 : team.namedPeople?.length || 0,
      deepenText: deepen.shallow ? "" : deepen.deepenText,
      accommodationsMention: Boolean(
        !deepen.shallow &&
          (deepen.housing?.roomBlockMentioned || entity.lodgingSignals?.hotelBooking)
      ),
    });
    const teamBand = classifyTeamSizeBand({
      namedPeople: team.namedPeople?.length || 0,
      speakerCount: team.speakerCount || 0,
      deepenText: deepen.deepenText,
    });

    // Jev lodging path (deep only)
    if (doDeep && enableJev && !String(pathHint).startsWith("STOP")) {
      try {
        const lodDec = await decideLodgingEvidenceNextStep({
          teamEvidence: team.teamSupported ? "present" : "gap",
          travelEvidence: travelLikelihood,
          companyOrigin: triage.locality,
          eventDuration: team.multiDay ? "multi_day" : "unknown",
          housingEvidence: deepen.housing ? "present" : "unknown",
          sourcesChecked: (deepen.pages || []).map((p) => p.url),
          lodgingProof: LODGING_PROOF.UNKNOWN,
          hasHousing: Boolean(deepen.housing),
          hasTeam: team.teamSupported,
          enableSafeApply: true,
        });
        recordJev(ledger, {
          decisionType: "LODGING_EVIDENCE_NEXT_STEP",
          company,
          defaultPath: lodDec.defaultPath,
          jevPath: lodDec.jevPath,
          finalPath: lodDec.finalPath,
          applied: lodDec.applied,
          outcome: lodDec.applied
            ? "HELPFUL_DIFFERENT"
            : lodDec.agreement
              ? "SAME"
              : "UNKNOWN",
        });
        lodgingPath = lodDec.finalPath;
        if (String(lodgingPath).startsWith("STOP")) {
          ledger.jev.fetchesAvoided += 1;
        } else if (
          lodDec.applied ||
          lodgingPath === "HOUSING_SOURCE" ||
          lodgingPath === "COMPANY_TRAVEL_PAGE"
        ) {
          const lodDeep = await deepenExhibitorEntity(entity, {
            maxQueries: 2,
            maxFetches: 3,
            pathHint:
              lodgingPath === "HOUSING_SOURCE" || lodgingPath === "COMPANY_TRAVEL_PAGE"
                ? "TRAVEL_EVIDENCE"
                : lodgingPath === "TEAM_ROSTER"
                  ? "SPEAKER_ROSTER"
                  : lodgingPath === "AGENCY_SOURCE"
                    ? "AGENCY_RELATIONSHIP"
                    : "TRAVEL_EVIDENCE",
          });
          ledger.queriesUsed += lodDeep.queryCount || 0;
          ledger.fetchesUsed += lodDeep.fetchCount || 0;
          deepen.deepenText += ` ${lodDeep.deepenText || ""}`;
          if (lodDeep.housing) deepen.housing = lodDeep.housing;
          if (lodDeep.team?.teamSupported) Object.assign(team, lodDeep.team);
          if (lodDeep.who?.length) {
            deepen.who = [...(deepen.who || []), ...lodDeep.who];
          }
        }
      } catch {
        /* non-fatal */
      }
    }

    // Shallow V2 hotelBooking flags are not lodging proof without deepen
    const lodging = classifyLodgingProof({
      deepenText: deepen.shallow ? "" : deepen.deepenText,
      housingSignals: deepen.shallow
        ? null
        : deepen.housing || sharedHousing || entity.housingEvidence,
      travelLikelihood: deepen.shallow ? TRAVEL_LIKELIHOOD.UNKNOWN : travelLikelihood,
      travelClass: deepen.shallow ? TRAVEL_CLASS.UNKNOWN : travelClass,
      teamSupported: deepen.shallow ? false : team.teamSupported,
      namedPeople: deepen.shallow ? 0 : team.namedPeople?.length || 0,
      multiDay: deepen.shallow ? false : team.multiDay || entity.lodgingSignals?.multiDay,
      setupBreakdown: deepen.shallow ? false : team.setupBreakdown,
    });

    // For shallow entities keep weak/unknown unless prior lodging was truly STRONG with housing evidence
    let lodgingProofFinal = lodging.lodgingProof;
    if (deepen.shallow) {
      if (
        entity.lodgingSignalStrength === "STRONG" &&
        entity.housingEvidence?.roomBlockMentioned
      ) {
        lodgingProofFinal = LODGING_PROOF.CONFIRMED;
      } else if (
        entity.lodgingSignals?.teamCohort &&
        travelIncreasesLodging(travelClass)
      ) {
        lodgingProofFinal = LODGING_PROOF.WEAK;
        team.teamSupported = true;
      } else {
        lodgingProofFinal = LODGING_PROOF.UNKNOWN;
        team.teamSupported = false;
      }
    }

    const midtownEligible = geo.geography === "NYC_MIDTOWN";

    if (team.teamSupported) ledger.teamSupported += 1;
    if (team.multipleNamedPeople) ledger.multipleNamed += 1;
    if (team.companyEventPage) ledger.companyEventPage += 1;
    if (team.speakerStaffSignal) ledger.speakerStaff += 1;
    if (team.agencyRelationship) ledger.agency += 1;
    ledger.travel[travelLikelihood] = (ledger.travel[travelLikelihood] || 0) + 1;
    ledger.lodging[lodgingProofFinal] = (ledger.lodging[lodgingProofFinal] || 0) + 1;

    if (doDeep && team.teamSupported && !teamBefore) {
      ledger.jev.teamEvidenceViaJev += 1;
    }
    if (
      doDeep &&
      (lodgingProofFinal === LODGING_PROOF.CONFIRMED ||
        lodgingProofFinal === LODGING_PROOF.STRONG_INFERENCE)
    ) {
      ledger.jev.lodgingEvidenceViaJev += 1;
    }
    if ((deepen.who || []).some((c) => c.name)) ledger.jev.whoUpgrades += 1;

    const addressability =
      deepen.addressability ||
      (deepen.who?.length ? ADDRESSABILITY.COMPANY_PATH : ADDRESSABILITY.NO_USABLE_PATH);
    const contactDepth = mapContactDepthBucket(addressability, deepen.who || []);

    // Hotel match ONLY after lodging qualification AND Midtown geography
    const hotelMatches = [];
    let hotelMatched = false;
    const canMatchHotel =
      midtownEligible && mayBecomeHotelOpportunity(lodgingProofFinal);

    let hd = null;
    if (canMatchHotel) {
      hd = entityToHiddenDemand(
        {
          ...entity,
          destination: geo.destination || "New York Midtown",
          contacts: deepen.who,
        },
        {
          lodgingSignalStrength: lodgingProofToV2Strength(lodgingProofFinal),
          lodgingScore: lodging.lodgingLevel,
          lodgingSignals: lodging.lodgingSignals,
          housingEvidence: deepen.housing || sharedHousing,
        },
        marketKey
      );
      hd.gdiVersion = HIDDEN_DEMAND_V3;
      hd.destination = geo.destination || hd.destination;
      hd.lodgingProof = lodgingProofFinal;
      hd.teamSupported = team.teamSupported;
      hd.evidence = {
        ...hd.evidence,
        travelingTeam: team.teamSupported,
        lodgingProof: lodgingProofFinal,
        addressableOrg: addressability !== ADDRESSABILITY.NO_USABLE_PATH,
      };

      const matches = matchHiddenDemandToHotels(hd, hotelConfigs);
      for (const m of matches) {
        if (m.decision === "INSUFFICIENT") {
          hotelMatches.push({
            hotelId: m.hotelId,
            decision: "NEITHER",
            fitScore: m.fitScore,
            match,
          });
          continue;
        }
        hotelMatched = true;
        const cand = hotelMatchToOpportunityCandidate(
          hd,
          m,
          hotelConfigs.find((c) => c.hotelId === m.hotelId)
        );
        cand.gdiVersion = HIDDEN_DEMAND_V3;
        cand.lodgingProof = lodgingProofFinal;
        cand.candidateState = CANDIDATE_STATE.HOTEL_OPPORTUNITY;
        cand.teamEvidence = team;
        cand.primaryContact = deepen.who?.[0]
          ? {
              name: deepen.who[0].name,
              role: deepen.who[0].role,
              email: deepen.who[0].email || null,
            }
          : null;
        cand.contactDepth = contactDepth;
        cand.addressability = addressability;
        cand.whyThisHotel = m.thesis;
        cand.recommendedAction = deepen.who?.[0]?.name
          ? `Contact ${deepen.who[0].name} (${deepen.who[0].role || "events"}) at ${company} to confirm lodging arrangements for the company's ${entity.demandGeneratorName || "event"} team and determine whether a Midtown room block is still open.`
          : `Identify the ${company} trade-show / field-marketing owner and ask whether Midtown lodging for their ${entity.demandGeneratorName || "event"} team is still open.`;
        cand.timingTrigger = entity.sourceType === "EXHIBITOR_DIRECTORY"
          ? "exhibitor_list_published"
          : team.companyEventPage
            ? "company_announced_attendance"
            : "show_approaching";
        cand.evidencePacketId = `evp_${hd.hiddenDemandId}`;

        const gate = passesCustomerPromotionGateV3({
          company,
          futureTiming: entity.futureTiming !== false,
          year: entity.year,
          participationEvidence: true,
          participationRole: entity.participationRole,
          teamSupported: team.teamSupported,
          lodgingProof: lodgingProofFinal,
          hotelMatched: true,
          sourceChain: [{ url: entity.sourceURL }],
          sourceUrl: entity.sourceURL,
          contactResearchAttempted: doDeep || Boolean(deepen.who?.length),
          addressability,
          timingTrigger: cand.timingTrigger,
          recommendedAction: cand.recommendedAction,
          who: deepen.who,
          primaryContactName: deepen.who?.[0]?.name,
        });
        cand.customerFacingState = gate.customerState;
        cand.customerPromotable = gate.ok || gate.watchAllowed;
        if (gate.customerState === "ACTIONABLE_NOW") {
          cand.candidateState = CANDIDATE_STATE.ACTIONABLE_NOW;
        }

        hotelMatches.push({
          hotelId: m.hotelId,
          decision: m.decision,
          fitScore: m.fitScore,
          candidate: cand,
        });

        const hr = ledger.hotelResults[m.hotelId];
        if (hr) {
          hr.matched += 1;
          hr.candidates.push(cand);
          if (cand.customerFacingState === "ACTIONABLE_NOW") hr.actionable += 1;
          else if (cand.customerPromotable) hr.watch += 1;
          hr.hotelOpportunity += 1;
          hr.contactBuckets[contactDepth] =
            (hr.contactBuckets[contactDepth] || 0) + 1;
        }
      }
    }

    const candidateState = candidateStateFromEvidence({
      teamSupported: team.teamSupported,
      lodgingProof: lodgingProofFinal,
      addressability,
      hotelMatched,
    });
    // Force market entity when not Midtown-eligible even with lodging
    let stateFinal = candidateState;
    if (!midtownEligible) {
      if (team.teamSupported && mayBecomeHotelOpportunity(lodgingProofFinal)) {
        stateFinal = CANDIDATE_STATE.MARKET_HIDDEN_CANDIDATE;
      } else if (team.teamSupported) {
        stateFinal = CANDIDATE_STATE.MARKET_HIDDEN_CANDIDATE;
      } else {
        stateFinal = CANDIDATE_STATE.MARKET_ENTITY;
      }
    }
    ledger.states[stateFinal] = (ledger.states[stateFinal] || 0) + 1;

    const row = {
      company,
      normalizeKey: key,
      demandGeneratorName: entity.demandGeneratorName,
      sourceType: entity.sourceType,
      participationRole: entity.participationRole,
      boothNumber: entity.boothNumber,
      futureTiming: entity.futureTiming !== false,
      year: entity.year,
      evidenceSnippet: entity.evidenceSnippet,
      triage: triage.triage,
      triageScore: triage.score,
      triageReasons: triage.reasons,
      locality: triage.locality,
      travelClass,
      participationDepth: triage.participation?.participationDepth,
      deeplyResearched: doDeep,
      team,
      teamSizeBand: teamBand,
      travelLikelihood,
      lodgingProof: lodgingProofFinal,
      lodgingLevel: lodging.lodgingLevel,
      lodgingSignals: lodging.lodgingSignals,
      housing: deepen.housing || sharedHousing,
      who: deepen.who || [],
      addressability,
      contactDepth,
      candidateState: stateFinal,
      eventGeography: geo,
      midtownEligible,
      hotelMatches: hotelMatches.map((h) => ({
        hotelId: h.hotelId,
        decision: h.decision,
        fitScore: h.fitScore,
        customerState: h.candidate?.customerFacingState || null,
      })),
      sourceGraph: buildSourceGraph(entity, deepen, lodging),
      jevPaths: { team: pathHint, lodging: lodgingPath },
    };

    ledger.rows.push(row);
    ledger.evidencePackets.push(buildEvidencePacket(row));
  }

  // Shared both / only / neither among hotel opportunities
  const oppOrgs = ledger.rows.filter((r) =>
    [
      CANDIDATE_STATE.HOTEL_OPPORTUNITY,
      CANDIDATE_STATE.ACTIONABLE_NOW,
      CANDIDATE_STATE.HOTEL_MATCH_CANDIDATE,
    ].includes(r.candidateState)
  );
  const both = [];
  const only = Object.fromEntries(hotelConfigs.map((c) => [c.hotelId, []]));
  for (const r of oppOrgs) {
    const matched = (r.hotelMatches || []).filter(
      (h) => h.decision === "MATCH" || h.decision === "MATCH_STRONG"
    );
    if (matched.length >= 2) both.push(r.company);
    else if (matched.length === 1) only[matched[0].hotelId].push(r.company);
  }

  ledger.finishedAt = new Date().toISOString();
  ledger.shared = {
    both,
    only,
    hotelOpportunityCount: oppOrgs.length,
    actionableCount: ledger.rows.filter(
      (r) => r.candidateState === CANDIDATE_STATE.ACTIONABLE_NOW
    ).length,
  };
  ledger.expansion.maxAdditionalDirectories = maxAdditionalDirectories;

  return ledger;
}

export {
  CANDIDATE_STATE,
  RESEARCH_TRIAGE,
  LODGING_PROOF,
  HIDDEN_DEMAND_V3,
  eventEligibleForMidtownHotels,
  passesCustomerPromotionGateV3,
};
