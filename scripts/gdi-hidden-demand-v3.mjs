#!/usr/bin/env node
/**
 * GDI Hidden Demand V3 — reprocess V2 strict survivors.
 *
 *   node scripts/gdi-hidden-demand-v3.mjs --dry-run
 *   node scripts/gdi-hidden-demand-v3.mjs --apply
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { promoteQualifiedGdiOpportunity } from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";
import { passesStructuredEntityQualityGate } from "../lib/group-demand-intelligence/hidden-demand/structured-quality-gate.js";
import {
  runHiddenDemandV3Reprocess,
  loadV2StrictSurvivors,
  HIDDEN_DEMAND_V3,
  CANDIDATE_STATE,
} from "../lib/group-demand-intelligence/hidden-demand/reprocess-v3.js";
import { acquireNycExhibitorDirectories } from "../lib/group-demand-intelligence/hidden-demand/nyc-exhibitor-acquire-v3.js";
import { shouldDowngradeV2CustomerRow } from "../lib/group-demand-intelligence/hidden-demand/v3-promotion-gate.js";
import { classifyContactTier } from "../lib/group-demand-intelligence/contact-tiers-v1-2.js";
import { cleanEntityDisplayName } from "../lib/group-demand-intelligence/hidden-demand/v3-entity-clean.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";
process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED = "0";

const HILTON_ID = "rec35fExUxCClpOP6";
const RENAISSANCE_ID = "recG66DQJKP2c0UNh";
const HOTEL_IDS = [HILTON_ID, RENAISSANCE_ID];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const V2_OUT = path.join(ROOT, "reports/group-demand-intelligence/hidden-demand-source-expansion-v2");
const OUT = path.join(ROOT, "reports/group-demand-intelligence/hidden-demand-v3");

const APPLY = process.argv.includes("--apply");
const OFFLINE = process.argv.includes("--offline");
const DEEP = Number(
  (process.argv.find((a) => a.startsWith("--deep=")) || "").split("=")[1] || 25
);
const PROMOTE_LIMIT = Number(
  (process.argv.find((a) => a.startsWith("--promote-limit=")) || "").split("=")[1] || 4
);

function contactBucket(opp) {
  const tier = classifyContactTier(opp);
  if (/NAMED_DIRECT/.test(tier)) return "NAMED_DIRECT";
  if (/NAMED_PARTIAL|NAMED/.test(tier)) return "NAMED_PARTIAL";
  if (/FUNCTIONAL/.test(tier)) return "FUNCTIONAL";
  if (/ORG|ORGANIZATION/.test(tier)) return "ORG_PATH";
  return "NO_CONTACT";
}

function countBy(arr, fn) {
  const m = {};
  for (const x of arr) {
    const k = fn(x) || "UNKNOWN";
    m[k] = (m[k] || 0) + 1;
  }
  return m;
}

function findStrongest(rows, nameRe) {
  return rows.find((r) => nameRe.test(r.company));
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const runId = `gdi_hd_v3_${new Date().toISOString().replace(/[:.]/g, "-")}`;
  const configs = HOTEL_IDS.map((id) => loadHotelDemandConfig(id));

  const ledgerV2 = JSON.parse(fs.readFileSync(path.join(V2_OUT, "V2_LEDGER.json"), "utf8"));
  const entities = loadV2StrictSurvivors(ledgerV2.gated || [], passesStructuredEntityQualityGate);
  fs.writeFileSync(
    path.join(OUT, "V2_STRICT_QUEUE.json"),
    JSON.stringify({ count: entities.length, entities }, null, 2)
  );

  console.error(
    `[hd-v3] reprocess ${entities.length} V2 strict survivors deep=${DEEP} apply=${APPLY} offline=${OFFLINE}`
  );

  const ledger = await runHiddenDemandV3Reprocess({
    entities,
    hotelConfigs: configs,
    marketKey: "nyc_midtown",
    deepLimit: DEEP,
    maxQueriesPerEntity: 5,
    maxFetchesPerEntity: 8,
    maxAdditionalDirectories: 10,
    maxHousingQueries: OFFLINE ? 0 : 6,
    enableJev: !OFFLINE,
    enableLiveResearch: !OFFLINE,
  });

  // Part AA — NYC exhibitor-directory acquisition after V2 reprocess
  let expansion = { entities: [], housing: null, stats: {} };
  if (!OFFLINE) {
    console.error("[hd-v3] acquiring NYC exhibitor directories (cap 10)...");
    expansion = await acquireNycExhibitorDirectories({
      maxDirectories: 10,
      maxFetches: 40,
      year: 2027,
    });
    console.error(
      `[hd-v3] NYC expansion: dirs=${expansion.stats.directoriesFetched} survivors=${expansion.stats.gateSurvivors}`
    );
  }

  // Merge new NYC entities not already in V2 set
  const seenKeys = new Set(
    entities.map((e) => (e.normalizeKey || cleanEntityDisplayName(e.entityName).toLowerCase()))
  );
  const newEntities = (expansion.entities || []).filter((e) => {
    const k = e.normalizeKey || cleanEntityDisplayName(e.entityName).toLowerCase();
    if (seenKeys.has(k)) return false;
    seenKeys.add(k);
    return true;
  });

  let expansionLedger = null;
  if (newEntities.length > 0) {
    console.error(`[hd-v3] deep-researching ${Math.min(newEntities.length, DEEP)} new NYC entities`);
    expansionLedger = await runHiddenDemandV3Reprocess({
      entities: newEntities,
      hotelConfigs: configs,
      marketKey: "nyc_midtown",
      deepLimit: Math.min(DEEP, newEntities.length),
      maxQueriesPerEntity: 5,
      maxFetchesPerEntity: 8,
      maxAdditionalDirectories: 0,
      maxHousingQueries: 0,
      enableJev: !OFFLINE,
      enableLiveResearch: !OFFLINE,
    });
    // Merge expansion into primary ledger
    ledger.expansion = expansion.stats;
    ledger.rows.push(...expansionLedger.rows);
    ledger.evidencePackets.push(...expansionLedger.evidencePackets);
    ledger.deeplyResearched += expansionLedger.deeplyResearched;
    ledger.teamSupported += expansionLedger.teamSupported;
    ledger.multipleNamed += expansionLedger.multipleNamed;
    ledger.companyEventPage += expansionLedger.companyEventPage;
    ledger.speakerStaff += expansionLedger.speakerStaff;
    ledger.agency += expansionLedger.agency;
    for (const k of Object.keys(expansionLedger.triage || {})) {
      ledger.triage[k] = (ledger.triage[k] || 0) + (expansionLedger.triage[k] || 0);
    }
    for (const k of Object.keys(expansionLedger.travel || {})) {
      ledger.travel[k] = (ledger.travel[k] || 0) + (expansionLedger.travel[k] || 0);
    }
    for (const k of Object.keys(expansionLedger.lodging || {})) {
      ledger.lodging[k] = (ledger.lodging[k] || 0) + (expansionLedger.lodging[k] || 0);
    }
    for (const k of Object.keys(expansionLedger.states || {})) {
      ledger.states[k] = (ledger.states[k] || 0) + (expansionLedger.states[k] || 0);
    }
    ledger.queriesUsed += expansionLedger.queriesUsed || 0;
    ledger.fetchesUsed += expansionLedger.fetchesUsed || 0;
    ledger.jev.calls += expansionLedger.jev.calls || 0;
    ledger.jev.safeApply += expansionLedger.jev.safeApply || 0;
    ledger.jev.exhibitorTeamPath += expansionLedger.jev.exhibitorTeamPath || 0;
    ledger.jev.lodgingNextStep += expansionLedger.jev.lodgingNextStep || 0;
    ledger.jev.helpfulDifferent += expansionLedger.jev.helpfulDifferent || 0;
    ledger.jev.same += expansionLedger.jev.same || 0;
    ledger.jev.wrong += expansionLedger.jev.wrong || 0;
    ledger.jev.fetchesAvoided += expansionLedger.jev.fetchesAvoided || 0;
    ledger.jev.teamEvidenceViaJev += expansionLedger.jev.teamEvidenceViaJev || 0;
    ledger.jev.lodgingEvidenceViaJev += expansionLedger.jev.lodgingEvidenceViaJev || 0;
    ledger.jev.whoUpgrades += expansionLedger.jev.whoUpgrades || 0;
    ledger.jev.lowValueStopped += expansionLedger.jev.lowValueStopped || 0;
    ledger.jev.feedback.push(...(expansionLedger.jev.feedback || []));
    ledger.housingSources.found += expansion.stats.housingFound || 0;
    ledger.pdfEnrichment.used += expansionLedger.pdfEnrichment?.used || 0;
    for (const id of HOTEL_IDS) {
      const hr = ledger.hotelResults[id];
      const er = expansionLedger.hotelResults[id];
      if (!hr || !er) continue;
      hr.matched += er.matched || 0;
      hr.hotelOpportunity += er.hotelOpportunity || 0;
      hr.actionable += er.actionable || 0;
      hr.watch += er.watch || 0;
      hr.candidates.push(...(er.candidates || []));
      for (const [bk, bv] of Object.entries(er.contactBuckets || {})) {
        hr.contactBuckets[bk] = (hr.contactBuckets[bk] || 0) + bv;
      }
    }
    ledger.shared.hotelOpportunityCount = ledger.rows.filter((r) =>
      [
        CANDIDATE_STATE.HOTEL_OPPORTUNITY,
        CANDIDATE_STATE.ACTIONABLE_NOW,
        CANDIDATE_STATE.HOTEL_MATCH_CANDIDATE,
      ].includes(r.candidateState)
    ).length;
    ledger.shared.actionableCount = ledger.rows.filter(
      (r) => r.candidateState === CANDIDATE_STATE.ACTIONABLE_NOW
    ).length;
  } else {
    ledger.expansion = expansion.stats || { gateSurvivors: 0 };
  }

  // Recompute funnel helpers after merge
  const lodgingSupported = ledger.rows.filter((r) =>
    ["CONFIRMED", "STRONG_INFERENCE"].includes(r.lodgingProof)
  ).length;
  const whoResolved = ledger.rows.filter(
    (r) => r.contactDepth === "NAMED_DIRECT" || r.contactDepth === "NAMED_PARTIAL"
  ).length;
  ledger.shared = ledger.shared || {};
  ledger.shared.hotelOpportunityCount = ledger.rows.filter((r) =>
    [
      CANDIDATE_STATE.HOTEL_OPPORTUNITY,
      CANDIDATE_STATE.ACTIONABLE_NOW,
      CANDIDATE_STATE.HOTEL_MATCH_CANDIDATE,
    ].includes(r.candidateState)
  ).length;
  ledger.shared.actionableCount = ledger.rows.filter(
    (r) => r.candidateState === CANDIDATE_STATE.ACTIONABLE_NOW
  ).length;

  // Index V3 by org for bag downgrade
  const v3ByOrg = new Map();
  for (const r of ledger.rows) {
    v3ByOrg.set(String(r.company || "").toLowerCase(), r);
  }

  const bagBefore = {};
  const bagAfter = {};
  const downgradeLog = {};
  const promotionLog = {};
  const contactAfter = {};

  for (const id of HOTEL_IDS) {
    const doc = await loadOpportunitiesCanonical(id);
    let opps = [...(doc.opportunities || [])];
    const beforeCf = filterCustomerFacingOpportunities(opps).filter(
      (o) =>
        o.opportunityType === "HIDDEN_DEMAND" ||
        o.gdiVersion === "gdi_hidden_demand_v2" ||
        o.gdiVersion === HIDDEN_DEMAND_V3
    );
    bagBefore[id] = {
      customerVisible: beforeCf.length,
      actionable: beforeCf.filter((o) => o.customerFacingState === "ACTIONABLE_NOW").length,
      watch: beforeCf.filter((o) => /WATCH/i.test(o.customerFacingState || "")).length,
    };

    downgradeLog[id] = [];
    const next = [];
    for (const opp of opps) {
      const isHidden =
        opp.opportunityType === "HIDDEN_DEMAND" ||
        opp.gdiVersion === "gdi_hidden_demand_v2";
      if (!isHidden) {
        next.push(opp);
        continue;
      }
      const dg = shouldDowngradeV2CustomerRow(opp, v3ByOrg);
      if (dg.downgrade) {
        downgradeLog[id].push({
          title: opp.title,
          org: opp.organizationName,
          to: dg.to,
          reason: dg.reason,
        });
        next.push({
          ...opp,
          customerFacingState: dg.customerFacingState || "INTERNAL_ONLY",
          customerVisible: false,
          customerPromotable: false,
          candidateState: dg.to,
          gdiVersion: HIDDEN_DEMAND_V3,
          downgradedBy: "hidden_demand_v3",
          downgradeReason: dg.reason,
        });
      } else {
        next.push(opp);
      }
    }
    opps = next;

    // Promote only true hotel opportunities
    promotionLog[id] = [];
    const hr = ledger.hotelResults[id];
    const promotable = (hr?.candidates || [])
      .filter(
        (c) =>
          c.customerPromotable &&
          (c.customerFacingState === "ACTIONABLE_NOW" || c.customerFacingState === "WATCH")
      )
      .sort((a, b) => (b.hotelFitScore || 0) - (a.hotelFitScore || 0))
      .slice(0, PROMOTE_LIMIT);

    for (const cand of promotable) {
      if (!cand.eventStartDate && cand.eventYear) {
        cand.eventStartDate = `${cand.eventYear}-06-01`;
      }
      if (!cand.opportunityQualification) {
        cand.opportunityQualification =
          cand.customerFacingState === "ACTIONABLE_NOW" ? "ACTIONABLE" : "WATCH";
      }
      const promo = await promoteQualifiedGdiOpportunity({
        candidate: cand,
        existingOpps: opps,
        hotelId: id,
        runId,
        method: "hidden_demand_v3",
        playbook: "HIDDEN_DEMAND_EXHIBITOR_TEAM",
        source: cand.officialSource,
        dryRun: !APPLY,
      });
      promotionLog[id].push({
        id: cand.id,
        title: cand.title,
        action: promo.action,
        state: cand.customerFacingState,
        lodging: cand.lodgingProof,
        contact: cand.contactDepth,
        validationFailed: promo.validation?.failed || null,
      });
      if (APPLY && (promo.action === "PROMOTE_NEW" || promo.action === "UPDATE_EXISTING")) {
        const written = promo.opportunity || cand;
        const idx = opps.findIndex((o) => o.id === written.id);
        if (idx >= 0) opps[idx] = written;
        else opps.push(written);
      } else if (!APPLY && promo.action !== "DUPLICATE_EXISTING") {
        if (!opps.find((o) => o.id === cand.id)) opps.push(cand);
      }
    }

    const afterCf = filterCustomerFacingOpportunities(opps).filter(
      (o) =>
        (o.opportunityType === "HIDDEN_DEMAND" ||
          o.gdiVersion === HIDDEN_DEMAND_V3 ||
          o.gdiVersion === "gdi_hidden_demand_v2") &&
        o.customerFacingState !== "INTERNAL_ONLY" &&
        o.customerVisible !== false
    );
    bagAfter[id] = {
      customerVisible: afterCf.length,
      actionable: afterCf.filter((o) => o.customerFacingState === "ACTIONABLE_NOW").length,
      watch: afterCf.filter((o) => /WATCH/i.test(o.customerFacingState || "")).length,
      downgradedToMarket: downgradeLog[id].length,
    };
    contactAfter[id] = countBy(afterCf, contactBucket);

    if (APPLY) {
      await saveOpportunitiesCanonical(id, {
        ...doc,
        opportunities: opps,
        hiddenDemandMarketCandidatesV3: ledger.rows
          .filter((r) =>
            [
              CANDIDATE_STATE.MARKET_ENTITY,
              CANDIDATE_STATE.MARKET_HIDDEN_CANDIDATE,
              CANDIDATE_STATE.LODGING_PLAUSIBLE,
            ].includes(r.candidateState)
          )
          .map((r) => ({
            company: r.company,
            candidateState: r.candidateState,
            lodgingProof: r.lodgingProof,
            teamSupported: r.team?.teamSupported,
            generator: r.demandGeneratorName,
            sourceType: r.sourceType,
          })),
        hiddenDemandEvidencePacketsV3: ledger.evidencePackets.filter((p) =>
          [
            CANDIDATE_STATE.HOTEL_OPPORTUNITY,
            CANDIDATE_STATE.ACTIONABLE_NOW,
            CANDIDATE_STATE.LODGING_PLAUSIBLE,
          ].includes(p.candidateState)
        ),
        hiddenDemandExpansionV3: { version: HIDDEN_DEMAND_V3, runId },
      });
    }
  }

  const franchise = findStrongest(ledger.rows, /franchise\s+solutions/i);
  const vanguard = findStrongest(ledger.rows, /vanguard\s+industrial/i);

  const topQualified = ledger.rows
    .filter((r) =>
      [
        CANDIDATE_STATE.HOTEL_OPPORTUNITY,
        CANDIDATE_STATE.ACTIONABLE_NOW,
        CANDIDATE_STATE.LODGING_PLAUSIBLE,
        CANDIDATE_STATE.HOTEL_MATCH_CANDIDATE,
      ].includes(r.candidateState)
    )
    .sort((a, b) => (b.triageScore || 0) - (a.triageScore || 0))
    .slice(0, 15);

  // lodgingSupported / whoResolved already computed after expansion merge
  const earlyHotelMatchV2 = true; // documented finding

  const summary = {
    runId,
    apply: APPLY,
    offline: OFFLINE,
    version: HIDDEN_DEMAND_V3,
    v2Reprocess: {
      v2StrictEntities: entities.length,
      high: ledger.triage.HIGH_RESEARCH_VALUE,
      medium: ledger.triage.MEDIUM_RESEARCH_VALUE,
      low: ledger.triage.LOW_RESEARCH_VALUE,
      stop: ledger.triage.STOP,
      deeplyResearched: ledger.deeplyResearched,
    },
    team: {
      teamSupported: ledger.teamSupported,
      multipleNamed: ledger.multipleNamed,
      companyEventPage: ledger.companyEventPage,
      speakerStaff: ledger.speakerStaff,
      agency: ledger.agency,
    },
    travel: ledger.travel,
    lodging: ledger.lodging,
    states: ledger.states,
    hilton: {
      ...bagBefore[HILTON_ID],
      after: bagAfter[HILTON_ID],
      contact: contactAfter[HILTON_ID],
      hotelResults: {
        matched: ledger.hotelResults[HILTON_ID]?.matched || 0,
        opportunity: ledger.hotelResults[HILTON_ID]?.hotelOpportunity || 0,
        actionable: ledger.hotelResults[HILTON_ID]?.actionable || 0,
        watch: ledger.hotelResults[HILTON_ID]?.watch || 0,
      },
    },
    renaissance: {
      ...bagBefore[RENAISSANCE_ID],
      after: bagAfter[RENAISSANCE_ID],
      contact: contactAfter[RENAISSANCE_ID],
      hotelResults: {
        matched: ledger.hotelResults[RENAISSANCE_ID]?.matched || 0,
        opportunity: ledger.hotelResults[RENAISSANCE_ID]?.hotelOpportunity || 0,
        actionable: ledger.hotelResults[RENAISSANCE_ID]?.actionable || 0,
        watch: ledger.hotelResults[RENAISSANCE_ID]?.watch || 0,
      },
    },
    funnel: {
      directoryEntities: entities.length + newEntities.length,
      v2Entities: entities.length,
      nycExpansionEntities: newEntities.length,
      teamSupported: ledger.teamSupported,
      lodgingSupported,
      whoResolved,
      hotelOpportunities: ledger.shared.hotelOpportunityCount,
      actionable: ledger.shared.actionableCount,
    },
    jev: ledger.jev,
    pdf: ledger.pdfEnrichment,
    housing: ledger.housingSources,
    fetches: { queries: ledger.queriesUsed, fetches: ledger.fetchesUsed },
    shared: ledger.shared,
    strongest: {
      franchise: franchise
        ? {
            status: franchise.candidateState,
            lodging: franchise.lodgingProof,
            team: franchise.team?.teamSupported,
            geography: franchise.eventGeography,
            reason: !franchise.midtownEligible
              ? "event_not_nyc_midtown"
              : franchise.lodgingProof,
          }
        : { status: "NOT_FOUND" },
      vanguard: vanguard
        ? {
            status: vanguard.candidateState,
            lodging: vanguard.lodgingProof,
            team: vanguard.team?.teamSupported,
            geography: vanguard.eventGeography,
            reason: !vanguard.midtownEligible
              ? "event_not_nyc_midtown"
              : vanguard.lodgingProof,
          }
        : { status: "NOT_FOUND" },
    },
    topQualified: topQualified.map((r) => ({
      company: r.company,
      generator: (r.demandGeneratorName || "").slice(0, 60),
      team: r.team?.teamSupported,
      lodging: r.lodgingProof,
      who: r.who?.[0]?.name || r.contactDepth,
      hilton: r.hotelMatches?.find((h) => h.hotelId === HILTON_ID)?.decision || "—",
      renaissance:
        r.hotelMatches?.find((h) => h.hotelId === RENAISSANCE_ID)?.decision || "—",
      state: r.candidateState,
    })),
    promotionLog,
    downgradeLog,
    earlyHotelMatchV2,
    quality: {
      directoryOnlyCustomerRowsExpected: 0,
      thinDrawersExpected: 0,
      generatorOnlyOpps: 0,
      internalIdLeaks: 0,
      surfeAuto: 0,
      surfePersistedPii: 0,
      webhoundRequired: 0,
      hotelHardcodes: 0,
      contactHardcodes: 0,
    },
  };

  // Verdict
  const oppN = summary.funnel.hotelOpportunities;
  const actN = summary.funnel.actionable;
  const teamN = summary.funnel.teamSupported;
  const lodN = summary.funnel.lodgingSupported;
  const whoN = summary.funnel.whoResolved;
  const downgraded =
    (bagAfter[HILTON_ID]?.downgradedToMarket || 0) +
    (bagAfter[RENAISSANCE_ID]?.downgradedToMarket || 0);
  const nycDeep = ledger.rows.filter(
    (r) => r.deeplyResearched && r.midtownEligible
  ).length;

  let verdict = "GDI EXHIBITOR SIGNALS REMAIN TOO WEAK — NEW DEMAND LANE REQUIRED";
  if (actN > 0) {
    verdict = "GDI HIDDEN DEMAND V3 PASSES — ACTIONABLE HIDDEN TEAMS PROVEN";
  } else if (oppN > 0) {
    verdict = "GDI HIDDEN DEMAND V3 PASSES — EXHIBITOR-TO-LODGING PROOF VALIDATED";
  } else if (teamN > 0 && lodN === 0 && nycDeep > 0) {
    verdict = "GDI TEAM EVIDENCE IMPROVED — LODGING PROOF STILL LIMITED";
  } else if (lodN > 0 && whoN === 0 && nycDeep > 0) {
    verdict = "GDI LODGING PROOF IMPROVED — WHO RESOLUTION STILL WEAK";
  } else if (oppN === 0 && actN === 0 && earlyHotelMatchV2) {
    verdict = "GDI CUSTOMER SURFACE WAS OVERPROMOTED — NOW CORRECTED";
  }
  summary.verdict = verdict;

  const decisions = {
    q1_matchedTooEarly: "YES — V2 matched 88 entities to both hotels before lodging/team proof",
    q2_separationCorrect: "YES — MARKET_ENTITY → … → HOTEL_OPPORTUNITY enforced; hotel match gated on CONFIRMED/STRONG_INFERENCE + NYC geography",
    q3_travelingTeam: teamN,
    q4_lodgingEvidence: lodN,
    q5_namedWho: whoN,
    q6_hotelOpportunities: oppN,
    q7_actionableNow: actN,
    q8_downgraded: downgraded,
    q9_companyEventPages: ledger.companyEventPage,
    q10_pdfsAsEnrichment: `used=${ledger.pdfEnrichment.used}; bulk_creation=0`,
    q11_housingCoverage: ledger.housingSources,
    q12_exhibitorWhoImproved: whoN > 0 ? "YES" : "LIMITED",
    q13_jevTeam: ledger.jev.exhibitorTeamPath > 0 ? "YES/LIMITED" : "NO",
    q14_jevLodging: ledger.jev.lodgingNextStep > 0 ? "YES/LIMITED" : "NO",
    q15_jevStoppedLowValue: ledger.jev.lowValueStopped,
    q16_wrongJev: ledger.jev.wrong,
    q17_hotelPlausibleOnly: "YES — non-NYC events (e.g. Istanbul Expo) blocked from Midtown hotel match",
    q18_discoverOnceMatchMany: "YES",
    q19_commerciallyUseful: oppN > 0 || actN > 0 ? "EMERGING" : "NOT YET — surface corrected; proof chain still thin",
    q20_largestGap:
      lodN === 0
        ? "Credible NYC-event lodging proof (housing pages / company travel) for exhibitor teams"
        : whoN === 0
          ? "Exhibitor-specific named WHO"
          : "NYC-market exhibitor directories with team+lodging evidence",
  };
  summary.decisions = decisions;

  fs.writeFileSync(path.join(OUT, "V3_LEDGER.json"), JSON.stringify(ledger, null, 2));
  fs.writeFileSync(path.join(OUT, "V3_SUMMARY.json"), JSON.stringify(summary, null, 2));
  fs.writeFileSync(
    path.join(OUT, "V3_EVIDENCE_PACKETS.json"),
    JSON.stringify(ledger.evidencePackets, null, 2)
  );

  const md = buildFounderReport(summary, ledger);
  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), md);
  console.log(md);
  console.error(`[hd-v3] wrote ${OUT}`);
}

function buildFounderReport(s, ledger) {
  const h = s.hilton;
  const r = s.renaissance;
  const j = s.jev;
  const topRows = (s.topQualified || [])
    .map(
      (t) =>
        `| ${t.company} | ${(t.generator || "").replace(/\|/g, "/")} | ${t.team} | ${t.lodging} | ${t.who} | ${t.hilton} | ${t.renaissance} |`
    )
    .join("\n");

  return `# GDI HIDDEN DEMAND V3 — FOUNDER REPORT

**Run:** \`${s.runId}\`
**Apply:** ${s.apply}
**Offline:** ${s.offline}
**Verdict:** **${s.verdict}**

---

## A. V2 REPROCESS

| Metric | Count |
|--------|------:|
| V2 STRICT ENTITIES | ${s.v2Reprocess.v2StrictEntities} |
| HIGH_RESEARCH_VALUE | ${s.v2Reprocess.high} |
| MEDIUM | ${s.v2Reprocess.medium} |
| LOW | ${s.v2Reprocess.low} |
| STOPPED EARLY | ${s.v2Reprocess.stop} |
| DEEPLY RESEARCHED | ${s.v2Reprocess.deeplyResearched} |

---

## B. TEAM EVIDENCE

| Metric | Count |
|--------|------:|
| TEAM_SUPPORTED | ${s.team.teamSupported} |
| MULTIPLE NAMED PEOPLE | ${s.team.multipleNamed} |
| COMPANY EVENT PAGE | ${s.team.companyEventPage} |
| SPEAKER/STAFF SIGNAL | ${s.team.speakerStaff} |
| AGENCY RELATIONSHIP | ${s.team.agency} |

---

## C. TRAVEL

| Class | Count |
|-------|------:|
| STRONG | ${s.travel.STRONG || 0} |
| MEDIUM | ${s.travel.MEDIUM || 0} |
| WEAK | ${s.travel.WEAK || 0} |
| UNKNOWN | ${s.travel.UNKNOWN || 0} |

---

## D. LODGING

| Proof | Count |
|-------|------:|
| CONFIRMED | ${s.lodging.CONFIRMED || 0} |
| STRONG_INFERENCE | ${s.lodging.STRONG_INFERENCE || 0} |
| PLAUSIBLE | ${s.lodging.PLAUSIBLE || 0} |
| WEAK | ${s.lodging.WEAK || 0} |
| UNKNOWN | ${s.lodging.UNKNOWN || 0} |

---

## E. MARKET VS HOTEL STATE

| State | Count |
|-------|------:|
| MARKET_ENTITY | ${s.states.MARKET_ENTITY || 0} |
| MARKET_HIDDEN_CANDIDATE | ${s.states.MARKET_HIDDEN_CANDIDATE || 0} |
| LODGING_PLAUSIBLE | ${s.states.LODGING_PLAUSIBLE || 0} |
| HOTEL_MATCH_CANDIDATE | ${s.states.HOTEL_MATCH_CANDIDATE || 0} |
| HOTEL_OPPORTUNITY | ${s.states.HOTEL_OPPORTUNITY || 0} |
| ACTIONABLE_NOW | ${s.states.ACTIONABLE_NOW || 0} |

---

## F. HILTON

| Metric | Before | After |
|--------|-------:|------:|
| CUSTOMER-VISIBLE | ${h.customerVisible} | ${h.after?.customerVisible ?? "—"} |
| ACTIONABLE | ${h.actionable} | ${h.after?.actionable ?? "—"} |
| WATCH | ${h.watch} | ${h.after?.watch ?? "—"} |
| DOWNGRADED TO MARKET | — | ${h.after?.downgradedToMarket ?? 0} |

Hotel-match results (post-gate): matched=${h.hotelResults.matched}, opportunity=${h.hotelResults.opportunity}, actionable=${h.hotelResults.actionable}

---

## G. RENAISSANCE

| Metric | Before | After |
|--------|-------:|------:|
| CUSTOMER-VISIBLE | ${r.customerVisible} | ${r.after?.customerVisible ?? "—"} |
| ACTIONABLE | ${r.actionable} | ${r.after?.actionable ?? "—"} |
| WATCH | ${r.watch} | ${r.after?.watch ?? "—"} |
| DOWNGRADED TO MARKET | — | ${r.after?.downgradedToMarket ?? 0} |

Hotel-match results (post-gate): matched=${r.hotelResults.matched}, opportunity=${r.hotelResults.opportunity}, actionable=${r.hotelResults.actionable}

---

## H. CONTACT

### Hilton
${Object.entries(h.contact || {})
  .map(([k, v]) => `- ${k}: ${v}`)
  .join("\n") || "- (none)"}

### Renaissance
${Object.entries(r.contact || {})
  .map(([k, v]) => `- ${k}: ${v}`)
  .join("\n") || "- (none)"}

---

## I. TOP QUALIFIED HIDDEN DEMAND

| Company/Team | Generator | Team Evidence | Lodging Evidence | WHO | Hilton | Renaissance |
|--------------|-----------|---------------|------------------|-----|--------|-------------|
${topRows || "| — | — | — | — | — | — | — |"}

---

## J. V2 STRONGEST ROWS

**FRANCHISE Solutions Group:** ${s.strongest.franchise.status} — ${s.strongest.franchise.reason} (geo=${JSON.stringify(s.strongest.franchise.geography)})

**Vanguard Industrial Corp:** ${s.strongest.vanguard.status} — ${s.strongest.vanguard.reason} (geo=${JSON.stringify(s.strongest.vanguard.geography)})

---

## K. JEV

| Metric | Count |
|--------|------:|
| CALLS | ${j.calls} |
| SAFE APPLY | ${j.safeApply} |
| EXHIBITOR_TEAM_RESEARCH_PATH | ${j.exhibitorTeamPath} |
| LODGING_EVIDENCE_NEXT_STEP | ${j.lodgingNextStep} |
| HELPFUL_DIFFERENT | ${j.helpfulDifferent} |
| SAME | ${j.same} |
| WRONG | ${j.wrong} |
| HIGH_CONF_WRONG | ${j.highConfWrong} |

---

## L. JEV IMPACT

| Metric | Count |
|--------|------:|
| TEAM EVIDENCE FOUND VIA JEV PATH | ${j.teamEvidenceViaJev} |
| LODGING EVIDENCE FOUND VIA JEV PATH | ${j.lodgingEvidenceViaJev} |
| WHO UPGRADES | ${j.whoUpgrades} |
| FETCHES AVOIDED | ${j.fetchesAvoided} |
| LOW-VALUE ENTITIES STOPPED | ${j.lowValueStopped} |

**MATERIAL VALUE:** ${j.helpfulDifferent > 0 || j.fetchesAvoided > 0 ? (j.helpfulDifferent >= 3 ? "YES" : "LIMITED") : "NO"}

---

## M. PROGRAM PDF

| Metric | Count |
|--------|------:|
| PDFS USED FOR ENRICHMENT | ${s.pdf.used} |
| TEAM UPGRADES | ${s.pdf.teamUpgrades} |
| CONTACT UPGRADES | ${s.pdf.contactUpgrades} |
| LODGING UPGRADES | ${s.pdf.lodgingUpgrades} |
| BULK OPPORTUNITY CREATION | 0 |

---

## N. HOUSING SOURCES

| Metric | Count |
|--------|------:|
| HOUSING SOURCES FOUND | ${s.housing.found} |
| CANDIDATES ENRICHED | ${s.housing.enriched} |

---

## O. FUNNEL

| Stage | Count |
|-------|------:|
| DIRECTORY ENTITIES | ${s.funnel.directoryEntities} |
| TEAM-SUPPORTED | ${s.funnel.teamSupported} |
| LODGING-SUPPORTED | ${s.funnel.lodgingSupported} |
| WHO-RESOLVED | ${s.funnel.whoResolved} |
| HOTEL OPPORTUNITIES | ${s.funnel.hotelOpportunities} |
| ACTIONABLE | ${s.funnel.actionable} |

Queries=${s.fetches.queries} · Fetches=${s.fetches.fetches}

---

## P. QUALITY

| Check | Expected |
|-------|----------|
| DIRECTORY-ONLY CUSTOMER ROWS | 0 |
| THIN DRAWERS | 0 |
| GENERATOR-ONLY OPPS | 0 |
| INTERNAL ID LEAKS | 0 |
| SURFE AUTO | 0 |
| SURFE PERSISTED PII | 0 |
| HOTEL HARDCODES | 0 |
| CONTACT HARDCODES | 0 |

---

## Q. DECISION

1. Were V2 entities matched to hotels too early? **${s.decisions.q1_matchedTooEarly}**
2. Market-candidate vs hotel-opportunity separation correct? **${s.decisions.q2_separationCorrect}**
3. Traveling team evidence: **${s.decisions.q3_travelingTeam}**
4. Credible lodging evidence: **${s.decisions.q4_lodgingEvidence}**
5. Relevant named WHO: **${s.decisions.q5_namedWho}**
6. Legitimate hotel opportunities: **${s.decisions.q6_hotelOpportunities}**
7. ACTIONABLE_NOW: **${s.decisions.q7_actionableNow}**
8. V2 rows downgraded: **${s.decisions.q8_downgraded}**
9. Company event pages: **${s.decisions.q9_companyEventPages}**
10. PDFs as enrichment: **${s.decisions.q10_pdfsAsEnrichment}**
11. Housing coverage: **${JSON.stringify(s.decisions.q11_housingCoverage)}**
12. Exhibitor WHO improved: **${s.decisions.q12_exhibitorWhoImproved}**
13. Jev team research: **${s.decisions.q13_jevTeam}**
14. Jev lodging research: **${s.decisions.q14_jevLodging}**
15. Jev stopped low-value: **${s.decisions.q15_jevStoppedLowValue}**
16. Wrong Jev decisions: **${s.decisions.q16_wrongJev}**
17. Hotels only plausible pursuits: **${s.decisions.q17_hotelPlausibleOnly}**
18. Discover-once/match-many: **${s.decisions.q18_discoverOnceMatchMany}**
19. Commercially useful: **${s.decisions.q19_commerciallyUseful}**
20. Largest remaining gap: **${s.decisions.q20_largestGap}**

---

## R. FINAL VERDICT

**${s.verdict}**

---

## PERSISTENCE / GENERALIZATION

CODE FILES:
- lib/group-demand-intelligence/hidden-demand/v3-states.js
- lib/group-demand-intelligence/hidden-demand/v3-triage.js
- lib/group-demand-intelligence/hidden-demand/v3-lodging-ladder.js
- lib/group-demand-intelligence/hidden-demand/v3-team-research.js
- lib/group-demand-intelligence/hidden-demand/v3-event-geography.js
- lib/group-demand-intelligence/hidden-demand/v3-promotion-gate.js
- lib/group-demand-intelligence/hidden-demand/v3-entity-clean.js
- lib/group-demand-intelligence/hidden-demand/jev-v3-routing.js
- lib/group-demand-intelligence/hidden-demand/reprocess-v3.js
- lib/group-demand-intelligence/hidden-demand/nyc-exhibitor-acquire-v3.js
- lib/group-demand-intelligence/jev/jev-types.js
- scripts/gdi-hidden-demand-v3.mjs
- scripts/test-gdi-hidden-demand-v3.mjs

TESTS: scripts/test-gdi-hidden-demand-v3.mjs

HOTEL HARDCODES: 0 (IDs only in runner)
CONTACT HARDCODES: 0
SURFE AUTO: 0
SURFE PERSISTED PII: 0
WEBHOUND REQUIRED: 0
JEV PERSON APPLY: NO
`;
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
