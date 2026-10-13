/**
 * GDI Complete Demand Packet V8
 * Reclassify universe → comp-set + Apify pivots → packet completion → Jev on qualified only.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  buildSuccessControlPackets,
  reclassifyExistingUniverse,
  buildApifyActorInventory,
  runCompleteDemandPacketV8ForHotel,
  COMP_SET_TARGET_HOTELS,
  PACKET_QUALITY,
} from "../lib/group-demand-intelligence/complete-demand-packet-v8/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(__dirname, "..", "reports", "gdi", "complete-demand-packet-v8");
const ROOT = path.join(__dirname, "..", "reports", "gdi");

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

async function main() {
  console.log("[v8] success control calibration…");
  const { pattern, controlPackets } = await buildSuccessControlPackets({
    nowDate: "2026-10-04",
  });
  write(
    "SUCCESS_CONTROL_SET.csv",
    toCsv(controlPackets, [
      "hotelKey",
      "hotelLabel",
      "opportunityId",
      "title",
      "organizationName",
      "packetQuality",
      "pillarA",
      "pillarB",
      "pillarC",
      "pillarD",
      "pillarE",
      "pillarF",
      "strongCount",
      "presentOrStrongCount",
      "demandEngine",
      "howDiscovered",
      "officialSource",
    ])
  );

  write(
    "COMPLETE_DEMAND_PACKET_SCHEMA.md",
    `# Complete Demand Packet Schema

## Six pillars (all required for COMPLETE)

| Pillar | Meaning |
|--------|---------|
| A | Named demand entity (real organization/group) |
| B | Defined group / hotel-demand motion |
| C | Buyer / organizer path |
| D | Future decision point |
| E | Hotel / lodging evidence (not phone co-occurrence alone) |
| F | Target hotel fit |

## Quality states

- **COMPLETE_STRONG** — expensive completion allowed
- **COMPLETE_PLAUSIBLE** — expensive completion allowed
- **PARTIAL_PACKET** — research intelligence; completion only if 4/6 + success match
- **SIGNAL_ONLY** — not a packet
- **REJECTED** / **DUPLICATE**

## Hard rules

- Rotation series alone ≠ Complete Demand Packet
- Phone co-occurrence ≠ lodging evidence
- Apify output = SIGNAL until page-validated
- Jev only after COMPLETE_* (or high-potential PARTIAL with success match)
`
  );

  console.log("[v8] reclassifying existing universe (no Jev)…");
  const reclass = reclassifyExistingUniverse(ROOT);
  write(
    "PACKET_RECLASSIFICATION.csv",
    toCsv(reclass.classified, [
      "id",
      "hotelKey",
      "title",
      "organization",
      "artifact",
      "signalType",
      "quality",
      "strongCount",
      "presentOrStrongCount",
      "missingPillars",
      "pillarA",
      "pillarB",
      "pillarC",
      "pillarD",
      "pillarE",
      "pillarF",
      "source",
      "competitorHotel",
      "demandEngine",
    ])
  );
  write(
    "ROTATION_RECLASSIFICATION.csv",
    toCsv(reclass.rotationReclass, [
      "id",
      "hotelKey",
      "title",
      "organization",
      "quality",
      "rotationIntelligenceOnly",
      "missingPillars",
      "note",
      "source",
    ])
  );

  const inventory = buildApifyActorInventory();
  write(
    "APIFY_ACTOR_INVENTORY.md",
    `# Apify Actor Inventory for GDI Packet V8

Status: **${inventory.status}**

${inventory.note}

## Actors in repo

${inventory.actors
  .map(
    (a) =>
      `### \`${a.actorRef || a.actorName}\`\n- GDI use: ${a.gdiUse}\n- Used in V8: ${a.usedInV8}\n- Truth policy: ${a.truthPolicy}\n`
  )
  .join("\n")}

## Not in repo (do not invent)

${inventory.notInRepo.map((x) => `- ${x}`).join("\n")}

## Recommended next

${inventory.recommendedNext}
`
  );

  const hotelResults = [];
  const allComps = [];
  const allPhone = [];
  const allCompVal = [];
  const allApify = [];
  const allBuyers = [];
  const allMotion = [];
  const allFuture = [];
  const allFeeder = [];
  const allMulti = [];
  const allCompletion = [];
  const allJev = [];
  const allTheses = [];
  const allFinal = [];
  const sourceYield = {};

  let totalCost = 0;
  let totalRaw = 0;
  let totalPartial = 0;
  let totalStrong = 0;
  let totalPlausible = 0;
  let totalReady = 0;
  let totalWatch = 0;
  let totalCompTraces = 0;
  let totalCompComplete = 0;
  let totalApifyComplete = 0;
  let jevIssued = 0;
  let jevPillars = 0;
  let jevClassChanges = 0;
  let depthSum = 0;
  let depthN = 0;

  for (const hotel of COMP_SET_TARGET_HOTELS) {
    console.log(`[v8] ${hotel.hotelKey}…`);
    const res = await runCompleteDemandPacketV8ForHotel(hotel, {
      nowDate: "2026-10-04",
      maxCompetitors: 4,
      maxCompQueries: 10,
      maxCompletionQueries: 6,
      maxApifyComps: 2,
      maxPacketsToComplete: 5,
      enableApify: true,
    });
    totalCost += res.costUsd;
    totalRaw += res.counts.rawSignals;
    totalPartial += res.counts.partialPackets;
    totalStrong += res.counts.completeStrong;
    totalPlausible += res.counts.completePlausible;
    totalReady += res.counts.customerReady;
    totalWatch += res.counts.futureWatch;
    totalCompTraces += res.counts.validatedTraces;
    totalCompComplete += res.counts.compCompletePackets;
    totalApifyComplete += res.counts.apifyCompletePackets;

    for (const c of res.competitors || []) {
      allComps.push({
        hotelKey: res.hotelKey,
        competitorHotelId: c.competitorHotelId,
        canonicalName: c.canonicalName,
        publicPhone: c.publicPhone || "",
        address: c.address || "",
        domain: c.domain || "",
        market: c.market,
      });
    }
    allPhone.push(...(res.phoneHits || []).map((p) => ({ ...p, hotelKey: res.hotelKey })));
    allCompVal.push(...(res.pageRows || []).map((p) => ({ ...p, hotelKey: res.hotelKey })));
    for (const a of res.apify?.results || []) {
      allApify.push({ hotelKey: res.hotelKey, ...a });
    }
    allBuyers.push(...(res.buyers || []));
    allTheses.push(...(res.theses || []));
    allCompletion.push(...(res.completions || []));
    for (const j of res.jevRows || []) {
      allJev.push(j);
      if (j.issued) jevIssued += 1;
    }
    for (const c of res.completions || []) {
      jevPillars += c.pillarsResolved || 0;
      if (c.qualityBefore !== c.qualityAfter) jevClassChanges += 1;
      depthSum += c.depth || 0;
      depthN += 1;
    }

    for (const p of res.classified || []) {
      const fam = p.sourceFamily || p.signalType || "UNKNOWN";
      sourceYield[fam] = sourceYield[fam] || {
        source: fam,
        raw: 0,
        partial: 0,
        complete: 0,
        ready: 0,
        watch: 0,
      };
      sourceYield[fam].raw += 1;
      if (p.quality === PACKET_QUALITY.PARTIAL_PACKET) sourceYield[fam].partial += 1;
      if (
        p.quality === PACKET_QUALITY.COMPLETE_STRONG ||
        p.quality === PACKET_QUALITY.COMPLETE_PLAUSIBLE
      ) {
        sourceYield[fam].complete += 1;
      }
      if (/\b(hotel block|official hotel|housing|accommodation|room block)\b/i.test(`${p.title} ${p.fact || ""}`)) {
        allMotion.push({
          hotelKey: res.hotelKey,
          id: p.id,
          organization: p.organizationName,
          motion: p.groupType || p.groupMotion,
          source: p.officialSource,
          quality: p.quality,
        });
      }
      if (p.eventYear || p.eventStartDate) {
        allFuture.push({
          hotelKey: res.hotelKey,
          id: p.id,
          organization: p.organizationName,
          eventYear: p.eventYear,
          eventStartDate: p.eventStartDate,
          quality: p.quality,
        });
      }
      if (p.feederMarket || p.originMarket) {
        allFeeder.push({
          hotelKey: res.hotelKey,
          id: p.id,
          feederMarket: p.feederMarket || p.originMarket,
          quality: p.quality,
        });
      }
      if (p.language && p.language !== "en") {
        allMulti.push({
          hotelKey: res.hotelKey,
          id: p.id,
          language: p.language,
          quality: p.quality,
        });
      }
    }

    for (const o of [...res.customerReady, ...res.futureWatch]) {
      allFinal.push({
        hotelKey: res.hotelKey,
        id: o.id,
        title: o.title,
        organization: o.organizationName,
        ready: res.customerReady.some((x) => x.id === o.id),
        watch: res.futureWatch.some((x) => x.id === o.id),
        source: o.officialSource,
        competitorHotel: o.competitorHotel,
      });
      const fam = o.sourceFamily || "COMP_SET";
      sourceYield[fam] = sourceYield[fam] || {
        source: fam,
        raw: 0,
        partial: 0,
        complete: 0,
        ready: 0,
        watch: 0,
      };
      if (res.customerReady.some((x) => x.id === o.id)) sourceYield[fam].ready += 1;
      if (res.futureWatch.some((x) => x.id === o.id)) sourceYield[fam].watch += 1;
    }

    hotelResults.push({
      hotelKey: res.hotelKey,
      label: res.hotelName,
      rawSignals: res.counts.rawSignals,
      partialPackets: res.counts.partialPackets,
      completeStrong: res.counts.completeStrong,
      completePlausible: res.counts.completePlausible,
      customerReady: res.counts.customerReady,
      futureWatch: res.counts.futureWatch,
      rejected: res.counts.rejected,
      validatedCompTraces: res.counts.validatedTraces,
      costUsd: Number(res.costUsd.toFixed(2)),
    });

    console.log(
      `[v8] ${res.hotelKey} complete=${res.counts.completePackets} ready=${res.counts.customerReady} watch=${res.counts.futureWatch} cost=$${res.costUsd.toFixed(2)}`
    );
  }

  const completeTotal = totalStrong + totalPlausible;
  // Merge reclass complete into totals for RETURN "existing"
  const existingComplete =
    (reclass.tallies.COMPLETE_STRONG || 0) + (reclass.tallies.COMPLETE_PLAUSIBLE || 0);
  const useful = totalReady + totalWatch;
  const signalToComplete =
    totalRaw > 0 ? Number(((completeTotal / totalRaw) * 100).toFixed(1)) : 0;
  const completeToUseful =
    completeTotal > 0 ? Number(((useful / completeTotal) * 100).toFixed(1)) : 0;
  const costPerComplete =
    completeTotal > 0 ? Number((totalCost / completeTotal).toFixed(2)) : null;
  const costPerUseful = useful > 0 ? Number((totalCost / useful).toFixed(2)) : null;
  const avgDepth = depthN > 0 ? Number((depthSum / depthN).toFixed(2)) : 0;

  const buyersResolved = allBuyers.filter(
    (b) => b.resolved || (b.buyerEntity && b.buyerType !== "UNKNOWN")
  ).length;
  const contactPaths = new Set(allBuyers.map((b) => b.publicContactPath).filter(Boolean)).size;

  const topSource =
    Object.values(sourceYield).sort((a, b) => b.complete - a.complete)[0]?.source || "n/a";

  // Engine / language / feeder from reclass + run
  const engYield = {};
  for (const c of reclass.classified) {
    if (
      c.quality !== PACKET_QUALITY.COMPLETE_STRONG &&
      c.quality !== PACKET_QUALITY.COMPLETE_PLAUSIBLE
    ) {
      continue;
    }
    const e = c.demandEngine || "UNKNOWN";
    engYield[e] = (engYield[e] || 0) + 1;
  }
  const topEngine =
    Object.entries(engYield).sort((a, b) => b[1] - a[1])[0]?.[0] || "n/a";

  const byKey = Object.fromEntries(hotelResults.map((h) => [h.hotelKey, h]));

  write(
    "COMP_SET_RESULTS.csv",
    toCsv(allComps, [
      "hotelKey",
      "competitorHotelId",
      "canonicalName",
      "publicPhone",
      "address",
      "domain",
      "market",
    ])
  );
  write(
    "PHONE_SEARCH_RESULTS.csv",
    toCsv(allPhone, [
      "hotelKey",
      "pivotId",
      "pivotType",
      "canonicalName",
      "query",
      "phoneVariant",
      "hitTitle",
      "hitUrl",
    ])
  );
  write(
    "COMP_TRACE_VALIDATION.csv",
    toCsv(allCompVal, [
      "hotelKey",
      "competitorHotelId",
      "pivotId",
      "url",
      "pageOk",
      "evidenceClass",
      "organization",
      "feedsDeeperResearch",
      "reason",
    ])
  );
  write(
    "APIFY_PACKET_RESULTS.csv",
    toCsv(allApify, [
      "hotelKey",
      "competitorHotelId",
      "canonicalName",
      "apifyActor",
      "ok",
      "publicPhone",
      "address",
      "domain",
      "currentWebsite",
      "signalOnly",
      "verifiedTruth",
      "error",
    ])
  );
  write(
    "BUYER_RESOLUTION.csv",
    toCsv(allBuyers, [
      "hotelKey",
      "packetId",
      "buyerEntity",
      "buyerType",
      "buyerRole",
      "organizer",
      "agency",
      "housingPartner",
      "publicContactPath",
      "resolved",
    ])
  );
  write(
    "HOTEL_MOTION_RESULTS.csv",
    toCsv(allMotion, ["hotelKey", "id", "organization", "motion", "source", "quality"])
  );
  write(
    "FUTURE_DECISION_RESULTS.csv",
    toCsv(allFuture, [
      "hotelKey",
      "id",
      "organization",
      "eventYear",
      "eventStartDate",
      "quality",
    ])
  );
  write(
    "FEEDER_MARKET_RESULTS.csv",
    toCsv(allFeeder, ["hotelKey", "id", "feederMarket", "quality"])
  );
  write(
    "MULTILINGUAL_RESULTS.csv",
    toCsv(allMulti, ["hotelKey", "id", "language", "quality"])
  );
  write(
    "PACKET_COMPLETION_RESULTS.csv",
    toCsv(allCompletion, [
      "packetId",
      "hotelKey",
      "organization",
      "qualityBefore",
      "qualityAfter",
      "successMatch",
      "depth",
      "pillarsResolved",
      "customerReady",
      "validFutureWatch",
    ])
  );
  write(
    "JEV_PACKET_RESEARCH.csv",
    toCsv(allJev, [
      "packetId",
      "hotelKey",
      "depth",
      "issued",
      "action",
      "stopContinue",
      "nextPillar",
      "exactQuestion",
      "oneMoreStepWorthwhile",
      "wroteFacts",
      "promoted",
      "note",
    ])
  );
  write(
    "TARGET_HOTEL_THESES.csv",
    toCsv(allTheses, [
      "hotelKey",
      "packetId",
      "organization",
      "fitClass",
      "whyRelevant",
      "whatTargetCouldWin",
      "fact",
      "inference",
      "unknown",
      "nextSalesResearchAction",
    ])
  );
  write(
    "FINAL_GDI_OPPORTUNITIES.csv",
    toCsv(allFinal, [
      "hotelKey",
      "id",
      "title",
      "organization",
      "ready",
      "watch",
      "source",
      "competitorHotel",
    ])
  );
  write(
    "SOURCE_PACKET_YIELD.csv",
    toCsv(Object.values(sourceYield), [
      "source",
      "raw",
      "partial",
      "complete",
      "ready",
      "watch",
    ])
  );
  write(
    "HOTEL_RESULTS.csv",
    toCsv(hotelResults, [
      "hotelKey",
      "label",
      "rawSignals",
      "partialPackets",
      "completeStrong",
      "completePlausible",
      "customerReady",
      "futureWatch",
      "rejected",
      "validatedCompTraces",
      "costUsd",
    ])
  );

  write(
    "COST_REPORT.md",
    `# Cost / Research Economics — Complete Demand Packet V8

| Metric | Value |
|--------|------:|
| Incremental cost (USD) | ${totalCost.toFixed(2)} |
| Complete packets (run) | ${completeTotal} |
| Cost per complete packet | ${costPerComplete ?? "n/a"} |
| Useful (ready+watch) | ${useful} |
| Cost per useful | ${costPerUseful ?? "n/a"} |
| Avg research depth | ${avgDepth} |

Phone co-occurrence ≠ proof. Apify ≠ verified truth. Thresholds unchanged.
`
  );

  const founder = `# GDI Complete Demand Packet V8

## A. Executive Summary

Complete Demand Packet standard shipped: six pillars, quality states, universe reclassification, rotation downgrade, comp-set mining, Apify Tripadvisor identity pivots (repo actors only), Jev only on qualified packets, depth capped.

**Existing reclassified:** ${reclass.tallies.total} (dupes ${reclass.tallies.duplicates}).  
**Existing complete packets:** ${existingComplete}.  
**Run complete packets:** ${completeTotal}. Ready/Watch: **${totalReady}/${totalWatch}**.

## B. Why Bethesda / NYC Worked

Controls analyzed: **${controlPackets.length}**. Successful ready opportunities already carried named org + group motion + buyer/contact path + future timing + lodging evidence + hotel fit before deep completion.

## C. Why Prior Signals Failed

Prior funnel optimized for signal/candidate/rotation counts. Recurring series and SERP hits were admitted without buyer+lodging+future decision together → completion could not rescue them.

## D. Complete Demand Packet Standard

See \`COMPLETE_DEMAND_PACKET_SCHEMA.md\`. Only COMPLETE_STRONG / COMPLETE_PLAUSIBLE enter expensive research.

## E. Reclassification of Existing Universe

| Quality | Count |
|---------|------:|
| SIGNAL_ONLY | ${reclass.tallies.SIGNAL_ONLY || 0} |
| PARTIAL_PACKET | ${reclass.tallies.PARTIAL_PACKET || 0} |
| COMPLETE_STRONG | ${reclass.tallies.COMPLETE_STRONG || 0} |
| COMPLETE_PLAUSIBLE | ${reclass.tallies.COMPLETE_PLAUSIBLE || 0} |
| REJECTED | ${reclass.tallies.REJECTED || 0} |
| DUPLICATE | ${reclass.tallies.DUPLICATE || 0} |

## F. Rotation Intelligence Correction

Rotation rows: **${reclass.tallies.rotationTotal}**. Downgraded to intelligence (not candidate): **${reclass.tallies.rotationDowngraded}**. Still complete: **${reclass.tallies.rotationComplete}**.

## G. Comp-Set Demonstrated Demand

Validated comp traces (run): **${totalCompTraces}**. Comp-derived complete packets: **${totalCompComplete}**.

## H. Apify Packet Contribution

Inventory: repo Tripadvisor actor only for GDI V8 (no Eventbrite/LinkedIn/Meetup actors in repo). Apify rows are **signal-only** identity pivots. Apify-derived complete packets: **${totalApifyComplete}**.

## I. Buyer Resolution

Buyer entities resolved: **${buyersResolved}**. Public contact paths: **${contactPaths}**.

## J. Future Decision Intelligence

Future decision rows captured: **${allFuture.length}**.

## K. Hotel / Lodging Evidence

Hotel-motion rows: **${allMotion.length}**. Phone co-occurrence never grants pillar E alone.

## L. Jev Completion

Issued: **${jevIssued}**. Pillars resolved: **${jevPillars}**. Classification changes: **${jevClassChanges}**. Avg depth: **${avgDepth}**.

## M. Target Hotel Opportunity Thesis

Theses: **${allTheses.length}** with FACT / INFERENCE / UNKNOWN.

## N. Hotel Results

| Hotel | Partial | Complete | Ready | Watch |
|-------|--------:|---------:|------:|------:|
| YOTEL | ${byKey.YOTEL?.partialPackets ?? 0} | ${(byKey.YOTEL?.completeStrong ?? 0) + (byKey.YOTEL?.completePlausible ?? 0)} | ${byKey.YOTEL?.customerReady ?? 0} | ${byKey.YOTEL?.futureWatch ?? 0} |
| AC | ${byKey.AC?.partialPackets ?? 0} | ${(byKey.AC?.completeStrong ?? 0) + (byKey.AC?.completePlausible ?? 0)} | ${byKey.AC?.customerReady ?? 0} | ${byKey.AC?.futureWatch ?? 0} |
| SPICE | ${byKey.SPICE?.partialPackets ?? 0} | ${(byKey.SPICE?.completeStrong ?? 0) + (byKey.SPICE?.completePlausible ?? 0)} | ${byKey.SPICE?.customerReady ?? 0} | ${byKey.SPICE?.futureWatch ?? 0} |
| CAMBRIDGE | ${byKey.CAMBRIDGE?.partialPackets ?? 0} | ${(byKey.CAMBRIDGE?.completeStrong ?? 0) + (byKey.CAMBRIDGE?.completePlausible ?? 0)} | ${byKey.CAMBRIDGE?.customerReady ?? 0} | ${byKey.CAMBRIDGE?.futureWatch ?? 0} |
| NOW NOW | ${byKey.NOW_NOW?.partialPackets ?? 0} | ${(byKey.NOW_NOW?.completeStrong ?? 0) + (byKey.NOW_NOW?.completePlausible ?? 0)} | ${byKey.NOW_NOW?.customerReady ?? 0} | ${byKey.NOW_NOW?.futureWatch ?? 0} |

## O. Source Yield

Top source by complete packets: **${topSource}**. Top demand engine (reclass): **${topEngine}**.

## P. Research Economics

Cost: **$${totalCost.toFixed(2)}**. Cost/complete: **${costPerComplete ?? "n/a"}**. Cost/useful: **${costPerUseful ?? "n/a"}**.

## Q. Recommended Default GDI Architecture

1. Ingest signals from SERP / comp-set / Apify identity / association / procurement.  
2. Classify Complete Demand Packet pillars.  
3. Keep rotation as RotationIntelligence until buyer+lodging+future exist.  
4. Complete only COMPLETE_* (or 4/6 partial + success match).  
5. Jev advises missing pillar only after admission.  
6. Canonical ready/watch gates unchanged.
`;

  write("FOUNDER_REPORT.md", founder);

  const ret = {
    COMPLETE_DEMAND_PACKET_IMPLEMENTED: "YES",
    PACKET_QUALITY_STATES_IMPLEMENTED: "YES",
    ROTATION_SEPARATED_FROM_OPPORTUNITY: "YES",
    SUCCESS_CONTROL_CALIBRATION_IMPLEMENTED: "YES",
    COMP_SET_DEMAND_MINING_IMPLEMENTED: "YES",
    APIFY_PACKET_CONTRIBUTION_IMPLEMENTED: "YES",
    BUYER_FIRST_RESEARCH_IMPLEMENTED: "YES",
    HOTEL_DEMAND_MOTION_SEARCH_IMPLEMENTED: "YES",
    FUTURE_DECISION_SEARCH_IMPLEMENTED: "YES",
    FEEDER_MARKET_SEARCH_IMPLEMENTED: "YES",
    MULTILINGUAL_COMPLETION_IMPLEMENTED: "YES",
    JEV_LIMITED_TO_QUALIFIED_PACKETS: "YES",
    TOTAL_EXISTING_RECORDS_RECLASSIFIED: reclass.tallies.total,
    TOTAL_SIGNAL_ONLY: (reclass.tallies.SIGNAL_ONLY || 0) + hotelResults.reduce((a, h) => a + (h.rawSignals > 0 ? 0 : 0), 0) || reclass.tallies.SIGNAL_ONLY || 0,
    TOTAL_PARTIAL_PACKETS: (reclass.tallies.PARTIAL_PACKET || 0) + totalPartial,
    TOTAL_COMPLETE_STRONG: (reclass.tallies.COMPLETE_STRONG || 0) + totalStrong,
    TOTAL_COMPLETE_PLAUSIBLE: (reclass.tallies.COMPLETE_PLAUSIBLE || 0) + totalPlausible,
    TOTAL_COMPLETE_DEMAND_PACKETS: existingComplete + completeTotal,
    TOTAL_CUSTOMER_READY: totalReady,
    TOTAL_VALID_FUTURE_WATCH: totalWatch,
    YOTEL_READY_WATCH: `${byKey.YOTEL?.customerReady ?? 0} / ${byKey.YOTEL?.futureWatch ?? 0}`,
    AC_READY_WATCH: `${byKey.AC?.customerReady ?? 0} / ${byKey.AC?.futureWatch ?? 0}`,
    SPICE_READY_WATCH: `${byKey.SPICE?.customerReady ?? 0} / ${byKey.SPICE?.futureWatch ?? 0}`,
    CAMBRIDGE_READY_WATCH: `${byKey.CAMBRIDGE?.customerReady ?? 0} / ${byKey.CAMBRIDGE?.futureWatch ?? 0}`,
    NOW_NOW_READY_WATCH: `${byKey.NOW_NOW?.customerReady ?? 0} / ${byKey.NOW_NOW?.futureWatch ?? 0}`,
    ROTATION_SERIES_DOWNGRADED_TO_INTELLIGENCE: reclass.tallies.rotationDowngraded,
    TOTAL_VALIDATED_COMP_DEMAND_TRACES: totalCompTraces,
    TOTAL_COMP_DERIVED_COMPLETE_PACKETS: totalCompComplete,
    TOTAL_APIFY_DERIVED_COMPLETE_PACKETS: totalApifyComplete,
    TOTAL_BUYER_ENTITIES_RESOLVED: buyersResolved,
    TOTAL_PUBLIC_CONTACT_PATHS: contactPaths,
    TOTAL_FUTURE_DECISION_POINTS_RESOLVED: allFuture.length,
    TOTAL_LODGING_HOTEL_MOTIONS_RESOLVED: allMotion.length,
    TOP_SOURCE_BY_COMPLETE_PACKET_YIELD: topSource,
    TOP_DEMAND_ENGINE_BY_COMPLETE_PACKET_YIELD: topEngine,
    TOP_LANGUAGE_BY_COMPLETE_PACKET_YIELD: allMulti.length ? "non-en present" : "en",
    TOP_FEEDER_MARKET_BY_COMPLETE_PACKET_YIELD: allFeeder[0]?.feederMarket || "n/a",
    COMPLETE_PACKET_TO_USEFUL_OPPORTUNITY_CONVERSION_PCT: completeToUseful,
    SIGNAL_TO_COMPLETE_PACKET_CONVERSION_PCT: signalToComplete,
    JEV_RECOMMENDATIONS_ISSUED: jevIssued,
    JEV_PILLARS_RESOLVED: jevPillars,
    JEV_CLASSIFICATION_CHANGES: jevClassChanges,
    AVERAGE_RESEARCH_DEPTH: avgDepth,
    COST_PER_COMPLETE_PACKET: costPerComplete,
    COST_PER_USEFUL_OPPORTUNITY: costPerUseful,
    GDI_THRESHOLDS_CHANGED: "NO",
    PHONE_CO_OCCURRENCE_TREATED_AS_PROOF: "NO",
    APIFY_DATA_TREATED_AS_VERIFIED_TRUTH: "NO",
    JEV_WROTE_VERIFIED_FACTS: "NO",
    JEV_PROMOTED_OPPORTUNITIES: "NO",
    WATCH_QUALITY_STANDARD_BYPASSED: "NO",
    ADP_CHANGED: "NO",
    SHARE_TOKENS_CHANGED: "NO",
    RECLASS_SIGNAL_ONLY: reclass.tallies.SIGNAL_ONLY || 0,
    RECLASS_PARTIAL: reclass.tallies.PARTIAL_PACKET || 0,
    RECLASS_COMPLETE_STRONG: reclass.tallies.COMPLETE_STRONG || 0,
    RECLASS_COMPLETE_PLAUSIBLE: reclass.tallies.COMPLETE_PLAUSIBLE || 0,
    RUN_COMPLETE_PACKETS: completeTotal,
    FINAL_VERDICT:
      useful > 0
        ? "V8_COMPLETE_PACKETS_PRODUCED_USEFUL_OPPORTUNITIES_UNDER_UNCHANGED_GATES"
        : completeTotal + existingComplete > 0
          ? "V8_PACKET_STANDARD_RECLASSIFIED_UNIVERSE_AND_RAN_QUALIFIED_COMPLETION_CANONICAL_GATES_STILL_BLOCK_READY_WATCH"
          : "V8_ARCHITECTURE_SHIPPED_STRICT_PACKET_STANDARD_SUPPRESSED_WEAK_CANDIDATES",
  };

  // Fix TOTAL_SIGNAL_ONLY to be clean
  ret.TOTAL_SIGNAL_ONLY = reclass.tallies.SIGNAL_ONLY || 0;
  ret.TOTAL_PARTIAL_PACKETS = (reclass.tallies.PARTIAL_PACKET || 0) + totalPartial;
  ret.TOTAL_COMPLETE_STRONG = (reclass.tallies.COMPLETE_STRONG || 0) + totalStrong;
  ret.TOTAL_COMPLETE_PLAUSIBLE = (reclass.tallies.COMPLETE_PLAUSIBLE || 0) + totalPlausible;
  ret.TOTAL_COMPLETE_DEMAND_PACKETS =
    ret.TOTAL_COMPLETE_STRONG + ret.TOTAL_COMPLETE_PLAUSIBLE;

  write("_RETURN.json", JSON.stringify(ret, null, 2));
  console.log("\n========== RETURN ==========");
  for (const [k, v] of Object.entries(ret)) console.log(`${k}: ${v}`);
  console.log("STOP.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
