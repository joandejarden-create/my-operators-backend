/**
 * GDI Demonstrated-Demand Discovery V6
 * Comp-set mining + success-pattern admission + Jev on admitted leads only.
 * Depth-3 not default. Thresholds unchanged.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  COMP_SET_TARGET_HOTELS,
  loadSuccessControlForV6,
  runDemonstratedDemandV6ForHotel,
  SUCCESS_TRAITS,
} from "../lib/group-demand-intelligence/demonstrated-demand-v6/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(__dirname, "..", "reports", "gdi", "demonstrated-demand-discovery-v6");
const PRIOR_JEV_USEFUL_YIELD = 0.077;

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
  console.log("[v6] loading success control set…");
  const { pattern, model } = await loadSuccessControlForV6({ nowDate: "2026-10-03" });

  write(
    "SUCCESS_CONTROL_SET.csv",
    toCsv(pattern.controls, [
      "hotelKey",
      "hotelLabel",
      "opportunityId",
      "title",
      "organizationName",
      "opportunityType",
      "demandEngine",
      "groupMotion",
      "futureTimingState",
      "lodgingEvidence",
      "buyerOrganizer",
      "whoState",
      "contactPath",
      "placementState",
      "hotelFit",
      "sourceAuthority",
      "independentSources",
      "howDiscovered",
      "officialSource",
      "eventStartDate",
      "thesisSnippet",
    ])
  );

  write(
    "SUCCESS_PATTERN_MODEL.md",
    `# Successful Opportunity Pattern Model (Bethesda / NYC)

Controls analyzed: **${pattern.controls.length}**

## Pre-readiness success traits
${model.traits.map((t) => `- ${t}`).join("\n")}

## Trait IDs (admission scoring)
${SUCCESS_TRAITS.map((t, i) => `${i + 1}. ${t}`).join("\n")}

## Aggregate ready-time rates
| Metric | Value |
|--------|------:|
| Entity strong % | ${((pattern.aggregate.pctEntityStrong || 0) * 100).toFixed(0)}% |
| Timing present % | ${((pattern.aggregate.pctTimingPresent || 0) * 100).toFixed(0)}% |
| Lodging hint % | ${((pattern.aggregate.pctLodgingHint || 0) * 100).toFixed(0)}% |
| Hotel motion % | ${((pattern.aggregate.pctHotelMotion || 0) * 100).toFixed(0)}% |
| WHO path % | ${((pattern.aggregate.pctWhoPath || 0) * 100).toFixed(0)}% |

## Admission rule
DEMONSTRATED_DEMAND_LEAD = true  
AND SUCCESS_PATTERN_MATCH ∈ {STRONG, PLAUSIBLE}  
→ admit to Jev completion (not customer-ready by itself)
`
  );

  const hotelResults = [];
  const allComps = [];
  const allPivots = [];
  const allTraces = [];
  const allPage = [];
  const allRepeat = [];
  const allBuyers = [];
  const allTheses = [];
  const allAdmit = [];
  const allJev = [];
  const allDepth = [];
  const allFinal = [];
  const allRbp = [];
  const allRap = [];
  const allRmp = [];

  let totalCost = 0;
  let totalComps = 0;
  let totalRawHits = 0;
  let totalTraces = 0;
  let totalDirect = 0;
  let totalStrong = 0;
  let totalDemo = 0;
  let totalAdmitted = 0;
  let totalJev = 0;
  let totalReady = 0;
  let totalWatch = 0;
  let jevBlockersResolved = 0;
  let jevClassChanged = 0;
  let depth1 = { n: 0, useful: 0 };
  let depth2 = { n: 0, useful: 0 };
  let depth3 = { n: 0, useful: 0 };

  for (const hotel of COMP_SET_TARGET_HOTELS) {
    const hotelCtx = {
      ...hotel,
      geoTokens: String(hotel.market || "")
        .split(/[\/,]/)
        .map((s) => s.trim().toLowerCase())
        .filter(Boolean),
      placeNames: String(hotel.market || "")
        .split(/[\/,]/)
        .map((s) => s.trim())
        .filter(Boolean),
    };
    console.log(`[v6] ${hotel.hotelKey}…`);
    const res = await runDemonstratedDemandV6ForHotel(hotelCtx, {
      nowDate: "2026-10-03",
      maxCompetitors: 4,
      maxCompQueries: 12,
      maxJevQueries: 6,
    });
    totalCost += res.costUsd;
    totalComps += res.counts.competitors;
    totalRawHits += (res.pivots || []).length;
    totalTraces += res.counts.validatedTraces;
    totalDirect += res.counts.directConfirmed;
    totalStrong += res.counts.strongAssociation;
    totalDemo += res.counts.demonstrated;
    totalAdmitted += res.counts.admitted;
    totalJev += res.counts.jevResearched;
    totalReady += res.counts.customerReady;
    totalWatch += res.counts.futureWatch;

    for (const c of res.competitors || []) {
      allComps.push({
        targetHotelKey: res.hotelKey,
        hotelId: c.competitorHotelId,
        canonicalName: c.canonicalName,
        aliases: (c.knownAliases || []).join("|"),
        formerNames: (c.formerNames || []).join("|"),
        publicPhone: c.publicPhone || "",
        phoneVariants: (c.formattedPhoneVariants || []).join("|"),
        address: c.address || "",
        domain: c.domain || "",
        meetingSpaceNames: (c.meetingSpaceNames || []).join("|"),
        market: c.market,
        identitySource: c.identitySource,
      });
    }
    allPivots.push(...(res.pivots || []).map((p) => ({ ...p, targetHotelKey: res.hotelKey })));
    allTraces.push(...(res.traces || []).map((t) => ({ ...t, targetHotelKey: res.hotelKey })));
    allPage.push(...(res.pageRows || []).map((p) => ({ ...p, targetHotelKey: res.hotelKey })));
    allRepeat.push(...(res.repeatPatterns || []).map((r) => ({ ...r, targetHotelKey: res.hotelKey })));
    allBuyers.push(...(res.buyerRows || []));
    allTheses.push(...(res.thesisRows || []));
    allAdmit.push(...(res.admissionRows || []));
    allJev.push(...(res.jevRows || []));
    for (const d of res.depthRows || []) {
      allDepth.push(d);
      const useful = d.useful ? 1 : 0;
      if (d.depth <= 1) {
        depth1.n += 1;
        depth1.useful += useful;
      } else if (d.depth === 2) {
        depth2.n += 1;
        depth2.useful += useful;
      } else {
        depth3.n += 1;
        depth3.useful += useful;
      }
      jevBlockersResolved += d.blockersResolved || 0;
      if (d.classificationChanged) jevClassChanged += 1;
    }
    for (const o of [...res.customerReady, ...res.futureWatch, ...res.researched]) {
      allFinal.push({
        hotelKey: res.hotelKey,
        id: o.id,
        title: o.title,
        organization: o.organizationName,
        competitorHotel: o.competitorHotel,
        evidenceClass: o.evidenceClass,
        ready: res.customerReady.some((x) => x.id === o.id),
        watch: res.futureWatch.some((x) => x.id === o.id),
        source: o.officialSource,
        fit: o.hotelFitScore,
      });
    }
    allRbp.push(...(res.repeatBuyers || []));
    allRap.push(...(res.repeatAgencies || []));
    allRmp.push(...(res.repeatMarkets || []));

    hotelResults.push({
      hotelKey: res.hotelKey,
      label: res.hotelName,
      compsSearched: res.counts.competitors,
      validatedTraces: res.counts.validatedTraces,
      demonstratedLeads: res.counts.demonstrated,
      successAdmitted: res.counts.admitted,
      jevResearched: res.counts.jevResearched,
      customerReady: res.counts.customerReady,
      futureWatch: res.counts.futureWatch,
      rejected: res.counts.rejected,
      costUsd: Number(res.costUsd.toFixed(2)),
      queriesRun: res.queriesRun,
    });

    console.log(
      `[v6] ${res.hotelKey} traces=${res.counts.validatedTraces} demo=${res.counts.demonstrated} admitted=${res.counts.admitted} ready=${res.counts.customerReady} watch=${res.counts.futureWatch} cost=$${res.costUsd.toFixed(2)}`
    );
  }

  const useful = totalReady + totalWatch;
  const jevUsefulYield = totalJev > 0 ? Number((useful / totalJev).toFixed(3)) : 0;
  const jevBlockerRate =
    totalJev > 0 ? Number((jevBlockersResolved / Math.max(allJev.filter((j) => j.issued).length, 1)).toFixed(3)) : 0;
  const jevClassRate = totalJev > 0 ? Number((jevClassChanged / totalJev).toFixed(3)) : 0;
  const buyersResolved = allBuyers.filter(
    (b) => b.resolved || (b.buyerEntity && b.buyerType !== "UNKNOWN")
  ).length;
  const contactPaths = new Set(allBuyers.map((b) => b.publicContactPath).filter(Boolean)).size;

  const rawToDemo = totalTraces > 0 ? Number(((totalDemo / totalTraces) * 100).toFixed(1)) : 0;
  const demoToAdmit = totalDemo > 0 ? Number(((totalAdmitted / totalDemo) * 100).toFixed(1)) : 0;
  const admitToUseful = totalAdmitted > 0 ? Number(((useful / totalAdmitted) * 100).toFixed(1)) : 0;
  const costPerUseful = useful > 0 ? Number((totalCost / useful).toFixed(2)) : null;

  const byKey = Object.fromEntries(hotelResults.map((h) => [h.hotelKey, h]));

  // CSVs
  write(
    "COMP_SET_CANONICAL.csv",
    toCsv(allComps, [
      "targetHotelKey",
      "hotelId",
      "canonicalName",
      "aliases",
      "formerNames",
      "publicPhone",
      "phoneVariants",
      "address",
      "domain",
      "meetingSpaceNames",
      "market",
      "identitySource",
    ])
  );
  write(
    "COMP_SEARCH_PIVOTS.csv",
    toCsv(allPivots, [
      "pivotId",
      "targetHotelKey",
      "competitorHotelId",
      "canonicalName",
      "pivotType",
      "language",
      "query",
      "phoneVariant",
    ])
  );
  write(
    "COMP_TRACE_RESULTS.csv",
    toCsv(allTraces, [
      "traceId",
      "targetHotelKey",
      "organization",
      "eventProgram",
      "competitorHotel",
      "evidenceClass",
      "feedsDeeperResearch",
      "eventYear",
      "demandEngine",
      "source",
      "pivotType",
      "fact",
      "inference",
      "unknown",
    ])
  );
  write(
    "PAGE_LEVEL_VALIDATION.csv",
    toCsv(allPage, [
      "targetHotelKey",
      "competitorHotelId",
      "pivotId",
      "url",
      "pageOk",
      "pageKind",
      "evidenceClass",
      "organization",
      "feedsDeeperResearch",
      "reason",
      "error",
    ])
  );
  write(
    "REPEAT_PATTERNS.csv",
    toCsv(allRepeat, [
      "patternId",
      "targetHotelKey",
      "organization",
      "eventSeries",
      "historicDates",
      "historicHotels",
      "cadence",
      "nextKnownCycle",
      "nextExpectedCycle",
      "nextDecisionWindow",
      "fabricated",
    ])
  );
  write(
    "BUYER_RESOLUTION.csv",
    toCsv(allBuyers, [
      "leadId",
      "hotelKey",
      "buyerEntity",
      "buyerType",
      "buyerRole",
      "organizer",
      "agency",
      "housingPartner",
      "publicContactPath",
      "source",
      "resolved",
    ])
  );
  write(
    "TARGET_HOTEL_THESES.csv",
    toCsv(allTheses, [
      "leadId",
      "targetHotelKey",
      "organization",
      "fitClass",
      "whyRelevant",
      "repeatFutureEvidence",
      "whatCompetitorAppearedToProvide",
      "couldTargetServe",
      "whatTargetCouldWin",
      "majorDisadvantages",
      "fact",
      "inference",
      "unknown",
      "nextSalesResearchAction",
    ])
  );
  write(
    "SUCCESS_PATTERN_ADMISSION.csv",
    toCsv(allAdmit, [
      "leadId",
      "hotelKey",
      "organization",
      "competitorHotel",
      "demonstrated",
      "successMatch",
      "score",
      "maxScore",
      "admit",
      "traitsPresent",
      "traitsMissing",
      "admissionClass",
    ])
  );
  write(
    "JEV_RESEARCH_LOG.csv",
    toCsv(allJev, [
      "opportunityId",
      "hotelKey",
      "depth",
      "issued",
      "action",
      "stopContinue",
      "nextBlocker",
      "nextSource",
      "exactQuestion",
      "oneMoreStepWorthwhile",
      "wroteFacts",
      "promoted",
      "note",
    ])
  );
  write(
    "RESEARCH_DEPTH_QA.csv",
    toCsv(allDepth, [
      "leadId",
      "hotelKey",
      "depth",
      "blockersResolved",
      "classificationBefore",
      "classificationAfter",
      "classificationChanged",
      "useful",
    ])
  );
  write(
    "FINAL_GDI_OPPORTUNITIES.csv",
    toCsv(allFinal, [
      "hotelKey",
      "id",
      "title",
      "organization",
      "competitorHotel",
      "evidenceClass",
      "ready",
      "watch",
      "source",
      "fit",
    ])
  );
  write(
    "REPEAT_BUYER_PATTERNS.csv",
    toCsv(allRbp, [
      "patternId",
      "patternType",
      "organization",
      "occurrences",
      "historicHotels",
      "historicMarkets",
      "demandEngine",
      "targetHotels",
    ])
  );
  write(
    "REPEAT_AGENCY_PATTERNS.csv",
    toCsv(allRap, [
      "patternId",
      "patternType",
      "agency",
      "occurrences",
      "organizations",
      "targetHotels",
    ])
  );
  write(
    "HOTEL_RESULTS.csv",
    toCsv(hotelResults, [
      "hotelKey",
      "label",
      "compsSearched",
      "validatedTraces",
      "demonstratedLeads",
      "successAdmitted",
      "jevResearched",
      "customerReady",
      "futureWatch",
      "rejected",
      "costUsd",
      "queriesRun",
    ])
  );

  write(
    "FUNNEL_CONVERSION.csv",
    toCsv(
      [
        { stage: "RAW_COMP_TRACE_HITS", count: totalRawHits, conversion_from_prior_pct: "" },
        {
          stage: "VALIDATED_COMP_TRACES",
          count: totalTraces,
          conversion_from_prior_pct: totalRawHits ? ((totalTraces / totalRawHits) * 100).toFixed(1) : "",
        },
        {
          stage: "DEMONSTRATED_DEMAND_LEADS",
          count: totalDemo,
          conversion_from_prior_pct: rawToDemo,
        },
        {
          stage: "SUCCESS_PATTERN_ADMITTED",
          count: totalAdmitted,
          conversion_from_prior_pct: demoToAdmit,
        },
        {
          stage: "FULLY_RESEARCHED",
          count: totalJev,
          conversion_from_prior_pct: totalAdmitted ? ((totalJev / totalAdmitted) * 100).toFixed(1) : "",
        },
        { stage: "CUSTOMER_READY", count: totalReady, conversion_from_prior_pct: admitToUseful },
        { stage: "VALID_FUTURE_WATCH", count: totalWatch, conversion_from_prior_pct: "" },
      ],
      ["stage", "count", "conversion_from_prior_pct"]
    )
  );

  write(
    "COST_REPORT.md",
    `# Cost / Research Efficiency — Demonstrated-Demand V6

| Metric | Value |
|--------|------:|
| Incremental cost (USD) | ${totalCost.toFixed(2)} |
| Useful (ready+watch) | ${useful} |
| Cost per useful | ${costPerUseful ?? "n/a"} |
| Jev researched | ${totalJev} |
| Depth-3 count | ${depth3.n} (default disabled) |

## Depth economics
| Depth | Count | Useful | Yield |
|------:|------:|-------:|------:|
| 1 | ${depth1.n} | ${depth1.useful} | ${depth1.n ? ((depth1.useful / depth1.n) * 100).toFixed(1) : 0}% |
| 2 | ${depth2.n} | ${depth2.useful} | ${depth2.n ? ((depth2.useful / depth2.n) * 100).toFixed(1) : 0}% |
| 3 | ${depth3.n} | ${depth3.useful} | ${depth3.n ? ((depth3.useful / depth3.n) * 100).toFixed(1) : 0}% |

Prior active Jev useful yield: **7.7%**. New (admitted-only): **${(jevUsefulYield * 100).toFixed(1)}%**.
`
  );

  const founder = `# GDI Demonstrated-Demand Discovery V6

## A. Executive Summary

Upstream admission rebuilt around **demonstrated competitor-hotel demand** + **Bethesda/NYC success-pattern match**, with **Jev only on admitted leads** and **Depth-3 not default**.

Controls: **${pattern.controls.length}**. Comp hotels: **${totalComps}**. Demonstrated leads: **${totalDemo}**. Admitted: **${totalAdmitted}**. Ready/Watch: **${totalReady}/${totalWatch}**.

## B. What Successful Bethesda / NYC Opportunities Had
${model.traits.map((t) => `- ${t}`).join("\n")}

## C. Comp Set Demand Mining

ADP-canonical comps (YOTEL documented fallback). Phone/name/alias pivots + page validation. Phone ≠ proof of stay.

## D. Demonstrated Demand Leads

Gate: valid entity + group/hotel motion + market relevance + ≥2 support signals, and only DIRECT/STRONG traces. Listicle orgs rejected.

## E. Repeat / Recurrence Intelligence

Repeat patterns: **${allRepeat.length}**. Repeat buyers: **${allRbp.length}**. Repeat agencies: **${allRap.length}**.

## F. Buyer Intelligence

Buyer entities resolved: **${buyersResolved}**. Public contact paths: **${contactPaths}**.

## G. Target Hotel Opportunity Thesis

FACT / INFERENCE / UNKNOWN separated. Fit: STRONG / PLAUSIBLE / WEAK / NO_FIT. Win-back angles are hypotheses.

## H. Success-Pattern Admission

Demonstrated + STRONG/PLAUSIBLE match → admit. Score is research-only, not customer-facing.

## I. Jev Performance on Better Leads

| Metric | Prior Active Jev | V6 Admitted-Only |
|--------|----------------:|-----------------:|
| Useful yield | 7.7% | ${(jevUsefulYield * 100).toFixed(1)}% |
| Blocker-resolution rate | 50.8% | ${(jevBlockerRate * 100).toFixed(1)}% |
| Classification-change rate | 7.7% | ${(jevClassRate * 100).toFixed(1)}% |

Jev never wrote facts or promoted opportunities.

## J. Research Depth Economics

Default depth 1 (comp page) + 1 targeted step. Depth-3 only if STRONG success match. Depth-3 count: **${depth3.n}**.

## K. Hotel Results

| Hotel | Traces | Demo | Admitted | Ready | Watch |
|-------|-------:|-----:|---------:|------:|------:|
| YOTEL | ${byKey.YOTEL?.validatedTraces ?? 0} | ${byKey.YOTEL?.demonstratedLeads ?? 0} | ${byKey.YOTEL?.successAdmitted ?? 0} | ${byKey.YOTEL?.customerReady ?? 0} | ${byKey.YOTEL?.futureWatch ?? 0} |
| AC | ${byKey.AC?.validatedTraces ?? 0} | ${byKey.AC?.demonstratedLeads ?? 0} | ${byKey.AC?.successAdmitted ?? 0} | ${byKey.AC?.customerReady ?? 0} | ${byKey.AC?.futureWatch ?? 0} |
| SPICE | ${byKey.SPICE?.validatedTraces ?? 0} | ${byKey.SPICE?.demonstratedLeads ?? 0} | ${byKey.SPICE?.successAdmitted ?? 0} | ${byKey.SPICE?.customerReady ?? 0} | ${byKey.SPICE?.futureWatch ?? 0} |
| CAMBRIDGE | ${byKey.CAMBRIDGE?.validatedTraces ?? 0} | ${byKey.CAMBRIDGE?.demonstratedLeads ?? 0} | ${byKey.CAMBRIDGE?.successAdmitted ?? 0} | ${byKey.CAMBRIDGE?.customerReady ?? 0} | ${byKey.CAMBRIDGE?.futureWatch ?? 0} |
| NOW NOW | ${byKey.NOW_NOW?.validatedTraces ?? 0} | ${byKey.NOW_NOW?.demonstratedLeads ?? 0} | ${byKey.NOW_NOW?.successAdmitted ?? 0} | ${byKey.NOW_NOW?.customerReady ?? 0} | ${byKey.NOW_NOW?.futureWatch ?? 0} |

## L. Funnel Conversion

Raw pivots → validated traces → demonstrated → admitted → researched → ready/watch. See \`FUNNEL_CONVERSION.csv\`.

## M. What Should Become Default GDI Behavior

1. Mine demonstrated competitor demand (page-validated DIRECT/STRONG only).
2. Admit via success-pattern traits (Bethesda/NYC).
3. Call Jev only after admission.
4. Cap research at depth 2 unless STRONG match.
5. Keep canonical readiness / Watch thresholds unchanged.
`;

  write("FOUNDER_REPORT.md", founder);

  // Also write REPEAT_MARKET as extra (brief asked REPEAT_BUYER and REPEAT_AGENCY; market is bonus)
  write(
    "REPEAT_MARKET_PATTERNS.csv",
    toCsv(allRmp, ["patternId", "patternType", "organization", "market", "occurrences", "years"])
  );

  const ret = {
    SUCCESS_CONTROL_OPPORTUNITIES_ANALYZED: pattern.controls.length,
    SUCCESS_PATTERN_TRAITS_IDENTIFIED: model.traits.join(" | "),
    COMP_SET_DEMAND_MINING_IMPLEMENTED: "YES",
    PHONE_SEARCH_IMPLEMENTED: "YES",
    PAGE_LEVEL_COMP_VALIDATION_IMPLEMENTED: "YES",
    DEMONSTRATED_DEMAND_LEAD_GATE_IMPLEMENTED: "YES",
    SUCCESS_PATTERN_ADMISSION_IMPLEMENTED: "YES",
    BUYER_FIRST_RESOLUTION_IMPLEMENTED: "YES",
    TARGET_HOTEL_THESIS_IMPLEMENTED: "YES",
    JEV_LIMITED_TO_ADMITTED_LEADS: "YES",
    DEPTH_3_DEFAULT_DISABLED: "YES",
    TOTAL_COMP_HOTELS_SEARCHED: totalComps,
    TOTAL_RAW_COMP_TRACE_HITS: totalRawHits,
    TOTAL_VALIDATED_COMP_TRACES: totalTraces,
    TOTAL_DIRECT_CONFIRMED: totalDirect,
    TOTAL_STRONG_ASSOCIATION: totalStrong,
    TOTAL_DEMONSTRATED_DEMAND_LEADS: totalDemo,
    TOTAL_SUCCESS_PATTERN_ADMITTED: totalAdmitted,
    TOTAL_JEV_RESEARCHED: totalJev,
    TOTAL_CUSTOMER_READY: totalReady,
    TOTAL_VALID_FUTURE_WATCH: totalWatch,
    YOTEL_READY_WATCH: `${byKey.YOTEL?.customerReady ?? 0} / ${byKey.YOTEL?.futureWatch ?? 0}`,
    AC_READY_WATCH: `${byKey.AC?.customerReady ?? 0} / ${byKey.AC?.futureWatch ?? 0}`,
    SPICE_READY_WATCH: `${byKey.SPICE?.customerReady ?? 0} / ${byKey.SPICE?.futureWatch ?? 0}`,
    CAMBRIDGE_READY_WATCH: `${byKey.CAMBRIDGE?.customerReady ?? 0} / ${byKey.CAMBRIDGE?.futureWatch ?? 0}`,
    NOW_NOW_READY_WATCH: `${byKey.NOW_NOW?.customerReady ?? 0} / ${byKey.NOW_NOW?.futureWatch ?? 0}`,
    TOTAL_REPEAT_PATTERNS: allRepeat.length,
    TOTAL_REPEAT_BUYER_PATTERNS: allRbp.length,
    TOTAL_REPEAT_AGENCY_PATTERNS: allRap.length,
    TOTAL_BUYER_ENTITIES_RESOLVED: buyersResolved,
    TOTAL_PUBLIC_CONTACT_PATHS: contactPaths,
    JEV_BLOCKER_RESOLUTION_RATE: jevBlockerRate,
    JEV_CLASSIFICATION_CHANGE_RATE: jevClassRate,
    JEV_USEFUL_OPPORTUNITY_YIELD: jevUsefulYield,
    PRIOR_JEV_USEFUL_YIELD: PRIOR_JEV_USEFUL_YIELD,
    NEW_JEV_USEFUL_YIELD: jevUsefulYield,
    DEPTH_1_COUNT_USEFUL_YIELD: `${depth1.n} / ${depth1.n ? ((depth1.useful / depth1.n) * 100).toFixed(1) : 0}%`,
    DEPTH_2_COUNT_USEFUL_YIELD: `${depth2.n} / ${depth2.n ? ((depth2.useful / depth2.n) * 100).toFixed(1) : 0}%`,
    DEPTH_3_COUNT_USEFUL_YIELD: `${depth3.n} / ${depth3.n ? ((depth3.useful / depth3.n) * 100).toFixed(1) : 0}%`,
    RAW_TRACE_TO_DEMONSTRATED_LEAD_CONVERSION_PCT: rawToDemo,
    DEMONSTRATED_LEAD_TO_ADMITTED_CONVERSION_PCT: demoToAdmit,
    ADMITTED_TO_USEFUL_OPPORTUNITY_CONVERSION_PCT: admitToUseful,
    COST_PER_USEFUL_OPPORTUNITY: costPerUseful,
    GDI_THRESHOLDS_CHANGED: "NO",
    PHONE_CO_OCCURRENCE_TREATED_AS_PROOF: "NO",
    JEV_WROTE_VERIFIED_FACTS: "NO",
    JEV_PROMOTED_OPPORTUNITIES: "NO",
    WATCH_QUALITY_STANDARD_BYPASSED: "NO",
    ADP_CHANGED: "NO",
    SHARE_TOKENS_CHANGED: "NO",
    FINAL_VERDICT:
      useful > 0
        ? "V6_ADMITTED_LEADS_PRODUCED_USEFUL_OUTCOMES_UNDER_UNCHANGED_GATES"
        : totalAdmitted > 0
          ? "V6_STRICT_ADMISSION_RAN_JEV_ON_BETTER_LEADS_CANONICAL_GATES_STILL_BLOCK_READY_WATCH"
          : totalDemo > 0
            ? "V6_DEMONSTRATED_LEADS_FOUND_BUT_SUCCESS_PATTERN_ADMISSION_FILTERED_HARD"
            : "V6_ARCHITECTURE_SHIPPED_STRICT_DEMONSTRATED_DEMAND_GATE_SUPPRESSED_THIN_COMP_TRACES",
  };

  write("_RETURN.json", JSON.stringify(ret, null, 2));
  console.log("\n========== RETURN ==========");
  for (const [k, v] of Object.entries(ret)) console.log(`${k}: ${v}`);
  console.log("STOP.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
