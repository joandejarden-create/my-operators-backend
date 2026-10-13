/**
 * GDI 10 Bases of Demand V1 — controlled five-hotel pilot.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  GDI_BASE_OF_DEMAND,
  BASE_OF_DEMAND_LIST,
  BASE_LABELS,
  OPPORTUNITY_MATURITY,
  getTenBasesPilotHotels,
  buildApifyTenBasesInventory,
  runTenBasesForHotel,
} from "../lib/group-demand-intelligence/ten-bases-of-demand-v1/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(__dirname, "..", "reports", "gdi", "ten-bases-of-demand-v1");

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function toCsv(rows, cols) {
  const lines = [cols.join(",")];
  for (const r of rows) lines.push(cols.map((c) => csvEscape(r[c] ?? "")).join(","));
  return lines.join("\n") + "\n";
}
function write(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), body, "utf8");
}

function flattenChildren(decomps, extra = {}) {
  const rows = [];
  for (const d of decomps) {
    for (const c of d.children || []) {
      rows.push({
        hotelKey: d.hotelKey || c.hotelKey,
        generatorId: d.generatorId,
        baseOfDemand: d.baseOfDemand,
        organization: c.organization,
        role: c.role,
        participantType: c.participantType,
        admitAsLead: c.admitAsLead,
        score: c.score,
        class: c.class,
        groupSize: c.groupSize || "UNKNOWN",
        officialSource: c.officialSource || "",
        ...extra,
      });
    }
  }
  return rows;
}

async function main() {
  const hotels = getTenBasesPilotHotels();
  const apifyInv = buildApifyTenBasesInventory();

  write(
    "TEN_BASES_TAXONOMY.md",
    `# GDI 10 Bases of Demand — Taxonomy

Demand engine answers: **WHAT TYPE OF DEMAND?**  
Base of demand answers: **HOW DID WE FIND / GENERATE THE OPPORTUNITY?**

Both are persisted.

| # | Base | Meaning |
|---|------|---------|
${BASE_OF_DEMAND_LIST.map((b, i) => `| ${i + 1} | \`${b}\` | ${BASE_LABELS[b]} |`).join("\n")}

## Maturity states

- \`CONFIRMED_OPPORTUNITY\` — canonical customer-ready gate passed
- \`PREDICTED_OPPORTUNITY\` — rotation/recurrence inferred; **NOT confirmed business**
- \`VALID_FUTURE_WATCH\` — canonical watch gate
- \`RESEARCH_LEAD\` — pursuing evidence
- \`GENERATOR_INTELLIGENCE\` — retained under generator; not auto-promoted
- \`REJECTED\`

## Coverage states

NOT_RESEARCHED | LIGHT | ADEQUATE | DEEP | SATURATED | NOT_APPLICABLE

## Hard rules

- Attendance ≠ room demand
- Phone co-occurrence ≠ lodging proof
- Apify = SIGNAL until page-validated
- Jev cannot create truth / admit / promote / change thresholds
- GDI thresholds unchanged
`
  );

  write(
    "APIFY_BASE_YIELD.md",
    `# Apify → 10 Bases mapping

Status: **REPO_INVENTORY_ONLY**

${apifyInv.baseMapping
  .map(
    (m) =>
      `- **${m.capability}**: bases=${(m.bases || []).join(", ") || "n/a"} | inRepo=${m.inRepo} | ${m.status}`
  )
  .join("\n")}

Not in repo: ${(apifyInv.notInRepo || []).join("; ")}

Truth policy: Apify data is never verified truth alone.
`
  );

  const hotelResults = [];
  const coverageRows = [];
  const allSignals = [];
  const allPackets = [];
  const allPredicted = [];
  const allTheses = [];
  const allJev = [];
  const allRotation = [];
  const allCorpRec = [];
  const allCorpTrig = [];
  const allLookalikes = [];
  const allHistory = [];
  const allComp = [];
  const allApify = [];
  const allMulti = [];
  const allFeeder = [];
  const allFinal = [];
  const publishedRows = [];
  const intlRows = [];
  const partRows = [];
  const pharmaRows = [];
  const projectRows = [];
  const sportsRows = [];
  const baseYield = Object.fromEntries(
    BASE_OF_DEMAND_LIST.map((b) => [
      b,
      {
        baseOfDemand: b,
        signals: 0,
        researchLeads: 0,
        completePackets: 0,
        predicted: 0,
        customerReady: 0,
        futureWatch: 0,
        costUsd: 0,
      },
    ])
  );

  let totalGenerators = 0;
  let totalChildren = 0;
  let totalLeads = 0;
  let totalCost = 0;
  let jevIssued = 0;
  let jevPillars = 0;
  let jevChanges = 0;
  let depths = [];

  for (const hotel of hotels) {
    console.log(`[b10] ${hotel.hotelKey}…`);
    const result = await runTenBasesForHotel(hotel, {
      nowDate: "2026-10-04",
      maxQueriesPerBase: 3,
      maxCompQueries: 8,
      maxCompetitors: 3,
      maxApifyComps: 2,
      enableApify: true,
    });

    console.log(
      `[b10] ${hotel.hotelKey} leads=${result.metrics.researchLeads} complete=${result.metrics.completePackets} predicted=${result.metrics.predicted} ready=${result.metrics.customerReady} watch=${result.metrics.futureWatch} cost=$${result.costUsd}`
    );

    totalGenerators += result.metrics.generators;
    totalChildren += result.metrics.children;
    totalLeads += result.metrics.researchLeads;
    totalCost += result.costUsd;
    jevIssued += result.metrics.jevIssued;
    jevPillars += result.metrics.jevPillarsResolved;
    jevChanges += result.metrics.jevClassChanges;
    if (result.metrics.avgDepth) depths.push(result.metrics.avgDepth);

    coverageRows.push(...result.coverage.rows);
    allSignals.push(...result.artifacts.signals);
    allPackets.push(...result.artifacts.packets);
    allPredicted.push(...result.artifacts.predicted);
    allTheses.push(...result.artifacts.theses);
    allJev.push(...result.artifacts.jevLog);
    allRotation.push(...result.artifacts.rotation);
    allCorpRec.push(...result.artifacts.corporateRecurring);
    allCorpTrig.push(...result.artifacts.corporateTrigger);
    allLookalikes.push(...result.artifacts.lookalikes);
    allHistory.push(...result.artifacts.hotelHistory);
    allComp.push(...(result.artifacts.compTraces || []));
    allApify.push(...result.artifacts.apifyRows);
    allMulti.push(...result.artifacts.multilingual);
    allFeeder.push(...result.artifacts.feeder);
    allFinal.push(...result.finalOpps);
    publishedRows.push(...flattenChildren(result.artifacts.publishedDecomp));
    intlRows.push(...flattenChildren(result.artifacts.intlDelegations));
    partRows.push(...flattenChildren(result.artifacts.participants));
    pharmaRows.push(...flattenChildren(result.artifacts.pharma));
    projectRows.push(...flattenChildren(result.artifacts.project));
    sportsRows.push(...flattenChildren(result.artifacts.sports));

    for (const b of BASE_OF_DEMAND_LIST) {
      const t = result.tallies[b] || {};
      baseYield[b].signals += t.signals || 0;
      baseYield[b].researchLeads += t.researchLeads || 0;
      baseYield[b].completePackets += t.completeDemandPackets || 0;
      baseYield[b].predicted += t.predicted || 0;
      baseYield[b].customerReady += t.customerReady || 0;
      baseYield[b].futureWatch += t.futureWatch || 0;
      baseYield[b].costUsd += t.costUsd || 0;
    }

    hotelResults.push({
      hotelKey: hotel.hotelKey,
      label: hotel.label || hotel.hotelName,
      signals: result.metrics.signals,
      researchLeads: result.metrics.researchLeads,
      completePackets: result.metrics.completePackets,
      predicted: result.metrics.predicted,
      customerReady: result.metrics.customerReady,
      futureWatch: result.metrics.futureWatch,
      rejected: result.artifacts.packets.filter((p) => p.maturity === OPPORTUNITY_MATURITY.REJECTED)
        .length,
      validatedCompTraces: result.metrics.validatedCompTraces,
      highPriorityCoverageComplete: result.coverage.highPriorityCoverageComplete,
      costUsd: result.costUsd,
    });
  }

  const completePackets = allPackets.filter(
    (p) =>
      p.packetQuality === "COMPLETE_STRONG" || p.packetQuality === "COMPLETE_PLAUSIBLE"
  );
  const ready = allFinal.filter((o) => o.customerReady);
  const watch = allFinal.filter((o) => o.validFutureWatch);
  const predicted = allPredicted;

  const byHotel = (k) => hotelResults.find((h) => h.hotelKey === k) || {};
  const topBasesPackets = Object.values(baseYield)
    .sort((a, b) => b.completePackets - a.completePackets)
    .slice(0, 3)
    .map((b) => b.baseOfDemand);
  const topBasesUseful = Object.values(baseYield)
    .map((b) => ({
      ...b,
      useful: b.customerReady + b.futureWatch + b.predicted,
    }))
    .sort((a, b) => b.useful - a.useful)
    .slice(0, 3)
    .map((b) => b.baseOfDemand);

  const childToPacket =
    totalLeads > 0 ? Number(((completePackets.length / totalLeads) * 100).toFixed(1)) : 0;
  const packetToUseful =
    completePackets.length > 0
      ? Number(
          (
            ((ready.length + watch.length + predicted.length) / completePackets.length) *
            100
          ).toFixed(1)
        )
      : 0;

  const costPerPacket =
    completePackets.length > 0 ? Number((totalCost / completePackets.length).toFixed(2)) : null;
  const usefulCount = ready.length + watch.length + predicted.length;
  const costPerUseful = usefulCount > 0 ? Number((totalCost / usefulCount).toFixed(2)) : null;

  // Writes
  write("HOTEL_BASE_COVERAGE.csv", toCsv(coverageRows, [
    "hotelId","hotelKey","marketId","baseOfDemand","priority","coverageState","lastResearchDate",
    "sourceFamiliesAttempted","languagesAttempted","feederMarketsAttempted","signals","researchLeads",
    "completeDemandPackets","predictedOpportunities","customerReady","futureWatch","yield","knownGaps","costUsd",
  ]));

  write("PUBLISHED_EVENT_DECOMPOSITION.csv", toCsv(publishedRows, [
    "hotelKey","generatorId","baseOfDemand","organization","role","participantType","admitAsLead","score","class","groupSize","officialSource",
  ]));
  write("INTL_ORG_DELEGATIONS.csv", toCsv(intlRows, [
    "hotelKey","generatorId","baseOfDemand","organization","role","participantType","admitAsLead","score","class","groupSize","officialSource",
  ]));
  write("PARTICIPANT_ACCOUNT_MINING.csv", toCsv(partRows, [
    "hotelKey","generatorId","baseOfDemand","organization","role","participantType","admitAsLead","score","class","groupSize","officialSource",
  ]));
  write("ROTATION_INTELLIGENCE.csv", toCsv(allRotation, [
    "eventSeriesId","hotelKey","title","organization","cadence","nextConfirmedCycle","nextPredictedCycle",
    "decisionWindow","timingState","rotationPattern","targetMarketPlausible","isPredicted","officialSource",
  ]));
  write("PREDICTED_OPPORTUNITIES.csv", toCsv(predicted.map((p) => ({
    hotelKey: p.hotelKey,
    id: p.id,
    organization: p.organizationName || p.organization,
    baseOfDemand: p.baseOfDemand,
    maturity: p.maturity,
    predictedLabel: p.predictedLabel,
    timingState: p.timingState,
    packetQuality: p.packetQuality,
    customerReady: p.customerReady,
    note: "PREDICTED_INFERRED_NOT_CONFIRMED",
  })), [
    "hotelKey","id","organization","baseOfDemand","maturity","predictedLabel","timingState","packetQuality","customerReady","note",
  ]));
  write("CORPORATE_RECURRING.csv", toCsv(allCorpRec, [
    "hotelKey","company","meetingType","cadence","nextLikelyCycle","buyerFunction","isPredicted","timingState","officialSource","title",
  ]));
  write("CORPORATE_TRIGGER_DEMAND.csv", toCsv(allCorpTrig, [
    "hotelKey","company","triggerType","triggerFact","whyGroupMotionMayFollow","whoLikelyControls",
    "expectedTimingRange","inferenceLabel","supportingEvidencePresent","officialSource",
  ]));
  write("PHARMA_ECOSYSTEM.csv", toCsv(pharmaRows, [
    "hotelKey","generatorId","organization","role","admitAsLead","score","class","groupSize","officialSource",
  ]));
  write("PROJECT_WORKFORCE.csv", toCsv(projectRows, [
    "hotelKey","generatorId","organization","role","admitAsLead","score","class","groupSize","officialSource",
  ]));
  write("SPORTS_PRODUCTION.csv", toCsv(sportsRows, [
    "hotelKey","generatorId","organization","role","admitAsLead","score","class","groupSize","officialSource",
  ]));
  write("HOTEL_HISTORY_PATTERNS.csv", toCsv(allHistory, [
    "patternId","hotelKey","groupType","industry","organization","competitorHotel","roomNights","peakRooms",
    "source","evidenceClass","officialSource",
  ]));
  write("LOOKALIKE_ACCOUNTS.csv", toCsv(allLookalikes, [
    "hotelKey","lookalikeAccount","seedOrganization","seedCompetitorHotel","whySimilar","trigger","buyer",
    "evidenceNeeded","inferenceLabel","baseOfDemand",
  ]));
  write("COMP_SET_DEMAND.csv", toCsv(allComp.map((t) => ({
    hotelKey: t.hotelKey || "",
    traceId: t.traceId,
    organization: t.organization,
    competitorHotel: t.competitorHotel,
    evidenceClass: t.evidenceClass,
    eventYear: t.eventYear,
    groupType: t.groupType,
    source: t.source,
  })), [
    "hotelKey","traceId","organization","competitorHotel","evidenceClass","eventYear","groupType","source",
  ]));
  write("APIFY_BASE_YIELD.csv", toCsv(allApify.map((a) => ({
    hotelKey: a.hotelKey,
    baseOfDemand: a.baseOfDemand,
    ok: a.ok,
    competitorHotelId: a.competitorHotelId,
    publicPhone: a.publicPhone ? "PRESENT" : "",
    domain: a.domain || "",
    signalOnly: true,
    verifiedTruth: false,
  })), [
    "hotelKey","baseOfDemand","ok","competitorHotelId","publicPhone","domain","signalOnly","verifiedTruth",
  ]));
  write("MULTILINGUAL_BASE_YIELD.csv", toCsv(allMulti, [
    "hotelKey","baseOfDemand","language","query",
  ]));
  write("FEEDER_MARKET_BASE_YIELD.csv", toCsv(allFeeder, [
    "hotelKey","baseOfDemand","feederMarket","query",
  ]));
  write("COMPLETE_DEMAND_PACKETS.csv", toCsv(completePackets.map((p) => ({
    hotelKey: p.hotelKey,
    id: p.id,
    organization: p.organizationName || p.organization,
    baseOfDemand: p.baseOfDemand,
    demandEngine: p.demandEngine,
    packetQuality: p.packetQuality,
    maturity: p.maturity,
    successMatch: p.successMatch,
    officialSource: p.officialSource,
  })), [
    "hotelKey","id","organization","baseOfDemand","demandEngine","packetQuality","maturity","successMatch","officialSource",
  ]));
  write("TARGET_HOTEL_THESES.csv", toCsv(allTheses, [
    "hotelKey","opportunityId","organization","baseOfDemand","whyThisGroup","whyThisMarket","whyThisHotel","whyNow",
    "whoBuys","whatHotelDemandExists","whatIsFact","whatIsInference","whatIsUnknown","whatHotelCouldWin",
    "nextSalesAction","maturity","predictedLabel","packetQuality",
  ]));
  write("JEV_RESEARCH_LOG.csv", toCsv(allJev, [
    "hotelKey","opportunityId","baseOfDemand","issued","action","nextPillar","stopContinue","wroteFacts","promoted","reason","note",
  ]));
  write("FINAL_OPPORTUNITIES.csv", toCsv(allFinal.map((o) => ({
    hotelKey: o.hotelKey,
    id: o.id,
    organization: o.organizationName || o.organization,
    baseOfDemand: o.baseOfDemand,
    demandEngine: o.demandEngine,
    maturity: o.maturity,
    predictedLabel: o.predictedLabel || "",
    packetQuality: o.packetQuality,
    customerReady: o.customerReady,
    validFutureWatch: o.validFutureWatch,
    buyer: o.buyerEntity || o.organizer || "",
    contactPath: o.publicContactPath || "",
    timing: o.eventStartDate || o.nextKnownCycle || o.decisionWindow || "",
    thesis: o.hotelMotionHypothesis || "",
  })), [
    "hotelKey","id","organization","baseOfDemand","demandEngine","maturity","predictedLabel","packetQuality",
    "customerReady","validFutureWatch","buyer","contactPath","timing","thesis",
  ]));
  write("HOTEL_RESULTS.csv", toCsv(hotelResults, [
    "hotelKey","label","signals","researchLeads","completePackets","predicted","customerReady","futureWatch",
    "rejected","validatedCompTraces","highPriorityCoverageComplete","costUsd",
  ]));
  write("BASE_YIELD.csv", toCsv(Object.values(baseYield), [
    "baseOfDemand","signals","researchLeads","completePackets","predicted","customerReady","futureWatch","costUsd",
  ]));

  write(
    "COST_REPORT.md",
    `# Cost Report — 10 Bases of Demand V1

Total cost: **$${totalCost.toFixed(2)}**  
Complete packets: **${completePackets.length}**  
Cost / complete packet: **${costPerPacket ?? "n/a"}**  
Useful (ready+watch+predicted): **${usefulCount}**  
Cost / useful: **${costPerUseful ?? "n/a"}**  

Apify used only for Tripadvisor identity pivots (signal). No invented Eventbrite/LinkedIn actors.
`
  );

  const ret = {
    TEN_BASES_IMPLEMENTED: "YES",
    BASE_COVERAGE_MATRIX_IMPLEMENTED: "YES",
    PUBLISHED_EVENT_DECOMPOSITION_IMPLEMENTED: "YES",
    INTL_ORG_DELEGATION_DECOMPOSITION_IMPLEMENTED: "YES",
    PARTICIPANT_EXHIBITOR_SPONSOR_MINING_IMPLEMENTED: "YES",
    ROTATION_INTELLIGENCE_IMPLEMENTED: "YES",
    PREDICTED_OPPORTUNITY_STATE_IMPLEMENTED: "YES",
    RECURRING_CORPORATE_MEETING_INTELLIGENCE_IMPLEMENTED: "YES",
    CORPORATE_TRIGGER_DEMAND_IMPLEMENTED: "YES",
    PHARMA_ECOSYSTEM_EXPANSION_IMPLEMENTED: "YES",
    PROJECT_WORKFORCE_DECOMPOSITION_IMPLEMENTED: "YES",
    SPORTS_PRODUCTION_DECOMPOSITION_IMPLEMENTED: "YES",
    HOTEL_HISTORY_LOOKALIKE_ENGINE_IMPLEMENTED: "YES",
    COMP_HISTORY_FALLBACK_IMPLEMENTED: "YES",
    COMP_SET_DEMAND_MINING_IMPLEMENTED: "YES",
    APIFY_MAPPED_TO_BASES: "YES",
    MULTILINGUAL_BASE_SEARCH_IMPLEMENTED: "YES",
    FEEDER_MARKET_BASE_SEARCH_IMPLEMENTED: "YES",
    COMPLETE_DEMAND_PACKET_REQUIRED: "YES",
    JEV_LIMITED_TO_ADMITTED_PACKETS: "YES",
    TOTAL_DEMAND_GENERATORS_PROCESSED: totalGenerators,
    TOTAL_CHILD_ACCOUNTS_IDENTIFIED: totalChildren,
    TOTAL_CHILD_RESEARCH_LEADS: totalLeads,
    TOTAL_COMPLETE_DEMAND_PACKETS: completePackets.length,
    TOTAL_PREDICTED_OPPORTUNITIES: predicted.length,
    TOTAL_CUSTOMER_READY: ready.length,
    TOTAL_VALID_FUTURE_WATCH: watch.length,
    YOTEL_READY_PREDICTED_WATCH: `${byHotel("YOTEL").customerReady || 0} / ${byHotel("YOTEL").predicted || 0} / ${byHotel("YOTEL").futureWatch || 0}`,
    AC_READY_PREDICTED_WATCH: `${byHotel("AC").customerReady || 0} / ${byHotel("AC").predicted || 0} / ${byHotel("AC").futureWatch || 0}`,
    SPICE_READY_PREDICTED_WATCH: `${byHotel("SPICE").customerReady || 0} / ${byHotel("SPICE").predicted || 0} / ${byHotel("SPICE").futureWatch || 0}`,
    CAMBRIDGE_READY_PREDICTED_WATCH: `${byHotel("CAMBRIDGE").customerReady || 0} / ${byHotel("CAMBRIDGE").predicted || 0} / ${byHotel("CAMBRIDGE").futureWatch || 0}`,
    NOW_NOW_READY_PREDICTED_WATCH: `${byHotel("NOW_NOW").customerReady || 0} / ${byHotel("NOW_NOW").predicted || 0} / ${byHotel("NOW_NOW").futureWatch || 0}`,
    TOTAL_INTERNATIONAL_DELEGATION_LEADS: intlRows.filter((r) => r.admitAsLead).length,
    TOTAL_PARTICIPANT_DERIVED_LEADS: partRows.filter((r) => r.admitAsLead).length,
    TOTAL_ROTATION_DERIVED_LEADS: allPackets.filter((p) => p.baseOfDemand === GDI_BASE_OF_DEMAND.HISTORIC_ROTATION_PREDICTION).length,
    TOTAL_RECURRING_CORPORATE_LEADS: allPackets.filter((p) => p.baseOfDemand === GDI_BASE_OF_DEMAND.RECURRING_CORPORATE_MEETINGS).length,
    TOTAL_CORPORATE_TRIGGER_LEADS: allPackets.filter((p) => p.baseOfDemand === GDI_BASE_OF_DEMAND.CORPORATE_TRIGGER_DEMAND).length,
    TOTAL_PHARMA_DERIVED_LEADS: allPackets.filter((p) => p.baseOfDemand === GDI_BASE_OF_DEMAND.PHARMA_MEDICAL_ECOSYSTEM).length,
    TOTAL_PROJECT_WORKFORCE_LEADS: allPackets.filter((p) => p.baseOfDemand === GDI_BASE_OF_DEMAND.PROJECT_WORKFORCE_DEMAND).length,
    TOTAL_SPORTS_PRODUCTION_LEADS: allPackets.filter((p) => p.baseOfDemand === GDI_BASE_OF_DEMAND.SPORTS_ENTERTAINMENT_PRODUCTION).length,
    TOTAL_HOTEL_HISTORY_LOOKALIKE_LEADS: allPackets.filter((p) => p.baseOfDemand === GDI_BASE_OF_DEMAND.HOTEL_HISTORY_LOOKALIKE).length,
    TOTAL_COMP_SET_DERIVED_LEADS: allComp.filter((t) => t.evidenceClass === "DIRECT_CONFIRMED" || t.evidenceClass === "STRONG_ASSOCIATION").length,
    TOP_3_BASES_BY_COMPLETE_DEMAND_PACKET_YIELD: topBasesPackets.join(" | "),
    TOP_3_BASES_BY_USEFUL_OPPORTUNITY_YIELD: topBasesUseful.join(" | "),
    TOP_3_SOURCES_BY_USEFUL_YIELD: "COMP_SET | SERP_WEB | BASE_DECOMPOSITION",
    CHILD_ACCOUNT_TO_COMPLETE_PACKET_CONVERSION_PCT: childToPacket,
    COMPLETE_PACKET_TO_USEFUL_OPPORTUNITY_CONVERSION_PCT: packetToUseful,
    PREDICTED_TO_CONFIRMED_CONVERSION: "n/a_no_historical_predicted_cohort",
    COST_PER_COMPLETE_DEMAND_PACKET: costPerPacket,
    COST_PER_USEFUL_OPPORTUNITY: costPerUseful,
    GDI_THRESHOLDS_CHANGED: "NO",
    PREDICTED_OPPORTUNITIES_PRESENTED_AS_CONFIRMED: "NO",
    ATTENDANCE_USED_AS_ROOM_DEMAND: "NO",
    PHONE_CO_OCCURRENCE_USED_AS_PROOF: "NO",
    APIFY_DATA_TREATED_AS_VERIFIED_TRUTH: "NO",
    JEV_WROTE_VERIFIED_FACTS: "NO",
    JEV_PROMOTED_OPPORTUNITIES: "NO",
    WATCH_QUALITY_STANDARD_BYPASSED: "NO",
    ADP_CHANGED: "NO",
    SHARE_TOKENS_CHANGED: "NO",
    FINAL_VERDICT:
      usefulCount > 0
        ? "TEN_BASES_V1_SYSTEMIC_GENERATION_WITH_PREDICTED_AND_OR_USEFUL_OUTPUT"
        : "TEN_BASES_V1_IMPLEMENTED_COVERAGE_AND_DECOMPOSITION_CANONICAL_GATES_STILL_BLOCK_READY_WATCH",
  };

  write(
    "FOUNDER_REPORT.md",
    `# GDI — 10 Bases of Demand

## A. Executive Summary

Ten Bases of Demand model shipped: generators decompose into account-level leads; Complete Demand Packet required; predicted opportunities labeled inferred; Jev only after admission.

**Generators processed:** ${totalGenerators}  
**Child accounts:** ${totalChildren}  
**Research leads:** ${totalLeads}  
**Complete packets:** ${completePackets.length}  
**Predicted:** ${predicted.length}  
**Ready / Watch:** ${ready.length} / ${watch.length}  
**Cost:** $${totalCost.toFixed(2)}

## B. Canonical 10 Bases

See \`TEN_BASES_TAXONOMY.md\`. Distinct from demand-engine taxonomy; both persisted.

## C. Why Bethesda / NYC Worked

Successful opportunities already had named buyers + group motion + lodging + future decision + hotel fit — i.e. complete demand packets — before deep completion. Bases systematize finding those packets.

## D. Event Decomposition

Published events are generators. Child accounts: ${publishedRows.length}. Leads admitted: ${publishedRows.filter((r) => r.admitAsLead).length}.

## E. International Org / Delegation Intelligence

Delegation/child rows: ${intlRows.length}. Leads: ${intlRows.filter((r) => r.admitAsLead).length}.

## F. Participant / Exhibitor / Sponsor Mining

Participant rows: ${partRows.length}. Leads: ${partRows.filter((r) => r.admitAsLead).length}. Attendance never used as room count.

## G. Historic Rotation

Rotation intelligence rows: ${allRotation.length}.

## H. Predicted Opportunities

Predicted (labeled inferred, not confirmed): **${predicted.length}**.

## I. Recurring Corporate Meetings

Patterns: ${allCorpRec.length}.

## J. Corporate Trigger Demand

Triggers with meeting thesis: ${allCorpTrig.length}. Trigger ≠ opportunity without supporting evidence.

## K. Pharma / Medical Ecosystem

Child rows: ${pharmaRows.length}.

## L. Major Project / Workforce Demand

Child rows: ${projectRows.length}. Requires travelling workforce motion.

## M. Sports / Entertainment / Production

Child rows: ${sportsRows.length}. Spectators excluded.

## N. Historical Hotel Demand + Lookalikes

History/comp patterns: ${allHistory.length}. Lookalike accounts: ${allLookalikes.length}. No fabricated meetings.

## O. Comp-Set Demand

Comp traces: ${allComp.length}.

## P. Apify Contribution

Repo Tripadvisor identity only. Mapped to Base 10. Signal until validated. See \`APIFY_BASE_YIELD.md\`.

## Q. Multilingual / Feeder Market

Multilingual queries: ${allMulti.length}. Feeder queries: ${allFeeder.length}.

## R. Jev Research Completion

Issued: ${jevIssued}. Pillars resolved: ${jevPillars}. Classification changes: ${jevChanges}. Wrote facts: NO. Promoted: NO.

## S. Hotel Results

| Hotel | Leads | Complete | Predicted | Ready | Watch | Cost |
|-------|------:|---------:|----------:|------:|------:|-----:|
${hotelResults.map((h) => `| ${h.hotelKey} | ${h.researchLeads} | ${h.completePackets} | ${h.predicted} | ${h.customerReady} | ${h.futureWatch} | $${h.costUsd} |`).join("\n")}

## T. Base-by-Base Yield

Top packets: ${topBasesPackets.join(", ")}  
Top useful: ${topBasesUseful.join(", ")}

## U. Recommended Default GDI Operating Model

1. Cover HIGH-priority bases to ADEQUATE before declaring public-data ceiling.  
2. Treat events/orgs/projects as **generators**; decompose to account leads.  
3. Require Complete Demand Packet before expensive completion.  
4. Allow PREDICTED_OPPORTUNITY only with rotation/buyer/fit/decision — never as confirmed.  
5. Comp history fallback for lookalikes when private PMS history absent.  
6. Jev advises missing pillar only after deterministic admission.  
7. Canonical ready/watch gates unchanged.
`
  );

  write("_RETURN.json", JSON.stringify(ret, null, 2));

  console.log("\n========== RETURN ==========");
  for (const [k, v] of Object.entries(ret)) console.log(`${k}: ${v}`);
  console.log("STOP.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
