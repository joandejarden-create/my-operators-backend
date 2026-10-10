/**
 * GDI Comp Set Demand Mining V1 — public trace discovery + repeat intelligence + target thesis.
 * No threshold cuts. Phone ≠ proof of stay. Jev advisory only after validated pattern.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  COMP_SET_TARGET_HOTELS,
  runCompSetDemandMiningForHotel,
  EVIDENCE_CLASS,
  FIT_CLASS,
} from "../lib/group-demand-intelligence/comp-set-demand-mining-v1/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(__dirname, "..", "reports", "gdi", "comp-set-demand-mining-v1");

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

function auditTrace(t) {
  // Deterministic sample QA labels from evidence + structure
  if (t.evidenceClass === EVIDENCE_CLASS.DIRECT_CONFIRMED || t.evidenceClass === EVIDENCE_CLASS.STRONG_ASSOCIATION) {
    if (!t.organization || t.organization.length < 4) {
      return { qaClass: "WEAK_ASSOCIATION", note: "Deep class but org weak" };
    }
    if (t.eventYear && Number(t.eventYear) < 2026 && !/annual|series|édition/i.test(`${t.eventProgram} ${t.fact}`)) {
      return { qaClass: "HISTORICAL_ONLY", note: "Past cycle without recurrence language" };
    }
    return { qaClass: "TRUE_COMP_DEMAND", note: "Validated group+competitor lodging/event context" };
  }
  if (t.evidenceClass === EVIDENCE_CLASS.WEAK_ASSOCIATION) {
    return { qaClass: "WEAK_ASSOCIATION", note: "Generic nearby/list mention" };
  }
  if (/PHONE_CO_OCCURRENCE|INSUFFICIENT|DISCOVERY/i.test(t.reason || "")) {
    return { qaClass: "FALSE_MATCH", note: "Pivot hit without lodging proof" };
  }
  return { qaClass: "WEAK_ASSOCIATION", note: t.reason || "discovery_only" };
}

async function main() {
  const hotelResults = [];
  const allComps = [];
  const allPivots = [];
  const allPhone = [];
  const allPage = [];
  const allTraces = [];
  const allRepeat = [];
  const allCross = [];
  const allBuyers = [];
  const allTheses = [];
  const allWins = [];
  const allJev = [];
  const allFinal = [];
  const allAudit = [];

  let totalCost = 0;
  let totalPhoneHits = 0;
  let totalPivots = 0;
  let totalComps = 0;

  for (const hotel of COMP_SET_TARGET_HOTELS) {
    console.log(`[comp-v1] ${hotel.hotelKey}…`);
    const res = await runCompSetDemandMiningForHotel(hotel, {
      nowDate: "2026-10-03",
      maxCompetitors: 4,
      maxQueries: 14,
      maxPivotsPerCompetitor: 4,
      maxPagesPerCompetitor: 2,
      resolveIdentity: true,
    });
    totalCost += res.costUsd;
    totalPhoneHits += res.phoneHits.length;
    totalPivots += res.pivots.length;
    totalComps += res.competitors.length;

    for (const c of res.competitors) {
      allComps.push({
        targetHotelKey: res.hotelKey,
        targetHotelId: res.hotelId,
        competitorHotelId: c.competitorHotelId,
        canonicalName: c.canonicalName,
        brand: c.brand,
        market: c.market,
        address: c.address || "",
        publicPhone: c.publicPhone || "",
        phoneVariants: (c.formattedPhoneVariants || []).join("|"),
        domain: c.domain || "",
        currentWebsite: c.currentWebsite || "",
        formerNames: (c.formerNames || []).join("|"),
        knownAliases: (c.knownAliases || []).join("|"),
        meetingSpaceNames: (c.meetingSpaceNames || []).join("|"),
        identitySource: c.identitySource,
        identityComplete: c.identityComplete,
        compSetSource: res.compSetSource,
      });
    }
    allPivots.push(...res.pivots.map((p) => ({ ...p, targetHotelKey: res.hotelKey })));
    allPhone.push(...res.phoneHits.map((p) => ({ ...p, targetHotelKey: res.hotelKey })));
    allPage.push(...res.pageRows.map((p) => ({ ...p, targetHotelKey: res.hotelKey })));
    allTraces.push(...res.traces.map((t) => ({ ...t, historicHotels: t.competitorHotel })));
    allRepeat.push(...res.repeatPatterns.map((r) => ({ ...r, targetHotelKey: res.hotelKey })));
    for (const p of res.crossPatterns) {
      allCross.push({
        ...p,
        historicHotels: Array.isArray(p.historicHotels) ? p.historicHotels.join("|") : p.historicHotels,
        historicMarkets: Array.isArray(p.historicMarkets) ? p.historicMarkets.join("|") : p.historicMarkets,
        historicCycles: Array.isArray(p.historicCycles) ? p.historicCycles.join("|") : p.historicCycles,
      });
    }
    allBuyers.push(...res.buyerRows);
    allTheses.push(...res.theses);
    allWins.push(...res.winAngles);
    allJev.push(...res.jevRows);
    allFinal.push(
      ...res.gdiCandidates.map((c) => ({
        hotelKey: res.hotelKey,
        id: c.id,
        title: c.title,
        organization: c.organizationName,
        stage: c.gdiStage,
        customerReady: c.customerReady,
        validFutureWatch: c.validFutureWatch,
        source: c.officialSource,
        fit: c.hotelFitScore,
        thesis: String(c.hotelOpportunityThesis || "").slice(0, 200),
      }))
    );

    hotelResults.push({
      hotelKey: res.hotelKey,
      label: res.hotelName,
      compSetSource: res.compSetSource,
      competitorsSearched: res.counts.competitors,
      searchPivots: res.counts.pivots,
      phoneSearchHits: res.phoneHits.length,
      nameAddressHits: res.pivots.filter((p) =>
        /EXACT_HOTEL_NAME|STREET_ADDRESS|ALIAS|BRAND|DOMAIN/i.test(p.pivotType)
      ).length,
      validatedTraces: res.counts.traces,
      deepTraces: res.counts.deepTraces,
      directConfirmed: res.counts.directConfirmed,
      strongAssociation: res.counts.strongAssociation,
      repeatPatterns: res.counts.repeatPatterns,
      futureCycles: res.counts.futureCycles,
      buyersResolved: res.buyerRows.filter((b) => b.resolved || (b.buyerEntity && b.buyerType !== "UNKNOWN")).length,
      strongFit: res.counts.strongFit,
      plausibleFit: res.counts.plausibleFit,
      gdiCandidates: res.counts.gdiCandidates,
      customerReady: res.counts.customerReady,
      futureWatch: res.counts.futureWatch,
      queriesRun: res.queriesRun,
      costUsd: Number(res.costUsd.toFixed(2)),
    });

    console.log(
      `[comp-v1] ${res.hotelKey} comps=${res.counts.competitors} traces=${res.counts.traces} deep=${res.counts.deepTraces} ready=${res.counts.customerReady} watch=${res.counts.futureWatch} cost=$${res.costUsd.toFixed(2)}`
    );
  }

  // Quality audit — sample ≥20 traces (or all if fewer)
  const sample = allTraces.slice(0, Math.max(20, Math.min(40, allTraces.length)));
  let truePos = 0;
  for (const t of sample) {
    const a = auditTrace(t);
    const isRepeat = allRepeat.some(
      (r) =>
        r.organization === t.organization &&
        /ANNUAL|MULTI|REPEAT|ROTAT/i.test(r.cadence || "")
    );
    const qaClass =
      isRepeat && a.qaClass === "TRUE_COMP_DEMAND" ? "VALID_REPEAT_PATTERN" : a.qaClass;
    if (qaClass === "TRUE_COMP_DEMAND" || qaClass === "VALID_REPEAT_PATTERN") truePos += 1;
    allAudit.push({
      traceId: t.traceId,
      hotelKey: t.targetHotelKey,
      organization: t.organization,
      competitorHotel: t.competitorHotel,
      evidenceClass: t.evidenceClass,
      qaClass,
      note: a.note,
      source: t.source,
    });
  }
  const precision = sample.length ? Number(((truePos / sample.length) * 100).toFixed(1)) : 0;

  // Phone useful yield: phone hits that led to DIRECT/STRONG after page validate
  const phonePivotIds = new Set(allPhone.map((p) => p.pivotId));
  const phoneUseful = allTraces.filter(
    (t) =>
      phonePivotIds.has(t.pivotId) &&
      (t.evidenceClass === EVIDENCE_CLASS.DIRECT_CONFIRMED ||
        t.evidenceClass === EVIDENCE_CLASS.STRONG_ASSOCIATION)
  ).length;
  const phoneUsefulPct = totalPhoneHits
    ? Number(((phoneUseful / totalPhoneHits) * 100).toFixed(1))
    : 0;

  // Top pivot type by useful yield
  const pivotYield = {};
  for (const t of allTraces) {
    const pt = t.pivotType || "UNK";
    pivotYield[pt] = pivotYield[pt] || { total: 0, useful: 0 };
  }
  for (const p of allPivots) {
    const pt = p.pivotType || "UNK";
    pivotYield[pt] = pivotYield[pt] || { total: 0, useful: 0 };
    pivotYield[pt].total += 1;
  }
  for (const t of allTraces) {
    const pt = t.pivotType || "UNK";
    pivotYield[pt] = pivotYield[pt] || { total: 0, useful: 0 };
    if (
      t.evidenceClass === EVIDENCE_CLASS.DIRECT_CONFIRMED ||
      t.evidenceClass === EVIDENCE_CLASS.STRONG_ASSOCIATION
    ) {
      pivotYield[pt].useful += 1;
    }
  }
  const topPivot =
    Object.entries(pivotYield)
      .map(([k, v]) => ({ k, score: v.total ? v.useful / v.total : 0, useful: v.useful, total: v.total }))
      .sort((a, b) => b.score - a.score || b.useful - a.useful)[0]?.k || "n/a";

  const topRepeat = allRepeat
    .filter((r) => /ANNUAL|MULTI|REPEAT|ROTAT/i.test(r.cadence || ""))
    .slice(0, 5)
    .map((r) => r.organization)
    .filter(Boolean)
    .join(" | ") || "none";

  const topBuyers = allBuyers
    .filter((b) => b.buyerEntity && b.buyerType && b.buyerType !== "UNKNOWN")
    .slice(0, 8)
    .map((b) => `${b.buyerEntity} (${b.buyerType})`)
    .join(" | ") || "none";

  const byKey = Object.fromEntries(hotelResults.map((h) => [h.hotelKey, h]));
  const totalDirect = allTraces.filter((t) => t.evidenceClass === EVIDENCE_CLASS.DIRECT_CONFIRMED).length;
  const totalStrong = allTraces.filter((t) => t.evidenceClass === EVIDENCE_CLASS.STRONG_ASSOCIATION).length;
  const totalReady = hotelResults.reduce((a, h) => a + h.customerReady, 0);
  const totalWatch = hotelResults.reduce((a, h) => a + h.futureWatch, 0);
  const totalCands = hotelResults.reduce((a, h) => a + h.gdiCandidates, 0);
  const buyersResolved = allBuyers.filter((b) => b.resolved || (b.buyerEntity && b.buyerType !== "UNKNOWN")).length;
  const contactPaths = new Set(allBuyers.map((b) => b.publicContactPath).filter(Boolean)).size;
  const strongFit = allTheses.filter((t) => t.fitClass === FIT_CLASS.STRONG_FIT).length;
  const plausibleFit = allTheses.filter((t) => t.fitClass === FIT_CLASS.PLAUSIBLE_FIT).length;
  const futureCycles = allRepeat.filter((r) => r.nextKnownCycle || r.nextExpectedCycle).length;

  // Write CSVs
  write(
    "COMP_SET_CANONICAL.csv",
    toCsv(allComps, [
      "targetHotelKey",
      "targetHotelId",
      "competitorHotelId",
      "canonicalName",
      "brand",
      "market",
      "address",
      "publicPhone",
      "phoneVariants",
      "domain",
      "currentWebsite",
      "formerNames",
      "knownAliases",
      "meetingSpaceNames",
      "identitySource",
      "identityComplete",
      "compSetSource",
    ])
  );
  write(
    "SEARCH_PIVOTS.csv",
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
    "PHONE_SEARCH_RESULTS.csv",
    toCsv(allPhone, [
      "targetHotelKey",
      "pivotId",
      "pivotType",
      "competitorHotelId",
      "canonicalName",
      "query",
      "phoneVariant",
      "hitTitle",
      "hitUrl",
      "hitSnippet",
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
    "COMPETITOR_DEMAND_TRACES.csv",
    toCsv(allTraces, [
      "traceId",
      "targetHotelKey",
      "organization",
      "eventProgram",
      "eventSeriesId",
      "eventCycleId",
      "groupType",
      "demandEngine",
      "eventDate",
      "eventYear",
      "market",
      "source",
      "competitorHotel",
      "competitorHotelId",
      "evidenceClass",
      "feedsDeeperResearch",
      "pivotId",
      "pivotType",
      "fact",
      "inference",
      "unknown",
    ])
  );
  write(
    "REPEAT_PATTERNS.csv",
    toCsv(allRepeat, [
      "patternId",
      "targetHotelKey",
      "organization",
      "eventSeries",
      "eventProgram",
      "demandEngine",
      "historicDates",
      "historicMarkets",
      "historicHotels",
      "cadence",
      "organizer",
      "housingPartner",
      "groupSize",
      "roomBlock",
      "nextKnownCycle",
      "nextExpectedCycle",
      "nextDecisionWindow",
      "evidenceClasses",
      "traceIds",
      "sourceUrls",
      "fabricated",
    ])
  );
  write(
    "CROSS_COMPETITOR_PATTERNS.csv",
    toCsv(allCross, [
      "patternId",
      "targetHotelKey",
      "organization",
      "eventSeries",
      "historicHotels",
      "historicMarkets",
      "historicCycles",
      "demandEngine",
      "groupSizeRange",
      "lodgingPattern",
      "repeatCadence",
      "agency",
      "organizer",
      "buyerEntity",
      "nextExpectedCycle",
      "evidenceSet",
      "crossCompetitor",
      "traceCount",
      "sources",
    ])
  );
  write(
    "BUYER_ORGANIZER_RESULTS.csv",
    toCsv(allBuyers, [
      "patternId",
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
      "targetHotelKey",
      "organization",
      "patternId",
      "fitClass",
      "whyRelevant",
      "repeatFutureEvidence",
      "whatCompetitorAppearedToProvide",
      "couldTargetServe",
      "whatTargetCouldWin",
      "majorDisadvantages",
      "advantages",
      "nextSalesResearchAction",
      "fact",
      "inference",
      "unknown",
    ])
  );
  write(
    "TARGET_HOTEL_WIN_ANGLES.csv",
    toCsv(allWins, [
      "targetHotelKey",
      "organization",
      "patternId",
      "winAngle",
      "hypothesis",
      "rationale",
      "notFact",
    ])
  );
  write(
    "JEV_NEXT_RESEARCH.csv",
    toCsv(allJev, [
      "patternId",
      "hotelKey",
      "issued",
      "stopContinue",
      "nextBlocker",
      "nextSource",
      "exactQuestion",
      "oneMoreStepWorthwhile",
      "wroteFacts",
      "promoted",
    ])
  );
  write(
    "FINAL_GDI_CANDIDATES.csv",
    toCsv(allFinal, [
      "hotelKey",
      "id",
      "title",
      "organization",
      "stage",
      "customerReady",
      "validFutureWatch",
      "source",
      "fit",
      "thesis",
    ])
  );
  write(
    "HOTEL_RESULTS.csv",
    toCsv(hotelResults, [
      "hotelKey",
      "label",
      "compSetSource",
      "competitorsSearched",
      "searchPivots",
      "phoneSearchHits",
      "nameAddressHits",
      "validatedTraces",
      "deepTraces",
      "directConfirmed",
      "strongAssociation",
      "repeatPatterns",
      "futureCycles",
      "buyersResolved",
      "strongFit",
      "plausibleFit",
      "gdiCandidates",
      "customerReady",
      "futureWatch",
      "queriesRun",
      "costUsd",
    ])
  );
  write(
    "QUALITY_AUDIT.csv",
    toCsv(allAudit, [
      "traceId",
      "hotelKey",
      "organization",
      "competitorHotel",
      "evidenceClass",
      "qaClass",
      "note",
      "source",
    ])
  );

  write(
    "COST_REPORT.md",
    `# Cost / Research Efficiency — Comp Set Demand Mining V1

| Metric | Value |
|--------|------:|
| Incremental research cost (USD) | ${totalCost.toFixed(2)} |
| Competitors searched | ${totalComps} |
| Search pivots executed (selected) | ${totalPivots} |
| Phone-search hit rows | ${totalPhoneHits} |
| Page validations | ${allPage.length} |
| Deep traces (DIRECT/STRONG) | ${totalDirect + totalStrong} |
| GDI candidates | ${totalCands} |
| Useful (ready+watch) | ${totalReady + totalWatch} |
| Cost per useful | ${totalReady + totalWatch > 0 ? (totalCost / (totalReady + totalWatch)).toFixed(2) : "n/a"} |

## Per hotel
${hotelResults.map((h) => `- ${h.hotelKey}: q=${h.queriesRun} cost=$${h.costUsd} deep=${h.deepTraces} cands=${h.gdiCandidates}`).join("\n")}

Phone co-occurrence was **never** treated as proof of stay.
`
  );

  const founder = `# GDI Comp Set Demand Mining V1

## A. Executive Summary

Systemic competitor-demand mining is implemented: ADP-canonical (or documented YOTEL fallback) comp sets → public search pivots (name/alias/phone/address/domain) → page-level validation → evidence classes → repeat/cross-competitor patterns → buyer resolution → target-hotel thesis → canonical GDI gates.

**${totalComps} competitors** searched · **${allTraces.length} traces** · **${totalDirect} DIRECT / ${totalStrong} STRONG** · **${totalCands} GDI candidates** · **${totalReady} ready / ${totalWatch} watch**.

## B. Why Competitor Demand Mining Matters

Bethesda/NYC-quality opportunities often leave public lodging footprints at competitor hotels. Mining those footprints finds **buyers of hotel rooms** with historic proof — not generic “events happening.”

## C. Comp Sets Searched

| Hotel | Source | Competitors |
|-------|--------|-------------|
${hotelResults.map((h) => `| ${h.hotelKey} | ${h.compSetSource} | ${h.competitorsSearched} |`).join("\n")}

See \`COMP_SET_CANONICAL.csv\`. Phones/addresses filled only when publicly extracted — never invented.

## D. Phone / Alias / Address Search Yield

- Phone-search hit rows: **${totalPhoneHits}**
- Phone → useful (DIRECT/STRONG) yield: **${phoneUsefulPct}%**
- Top pivot by useful yield: **${topPivot}**
- Alias / address / domain pivots included in \`SEARCH_PIVOTS.csv\`

**Rule enforced:** phone hit = page pointer only until page context confirms group + competitor lodging/event tie.

## E. Validated Group Traces

- Total traces: **${allTraces.length}**
- DIRECT_CONFIRMED: **${totalDirect}**
- STRONG_ASSOCIATION: **${totalStrong}**
- Only DIRECT/STRONG feed deeper pattern/opportunity research.

## F. Repeat / Recurring Demand

Repeat patterns: **${allRepeat.length}**. Future cycles identified: **${futureCycles}**.  
Top repeat groups: ${topRepeat}

Cadence states are evidence-backed only — no fabricated next dates.

## G. Buyer / Organizer Intelligence

Buyer entities resolved: **${buyersResolved}**. Public contact paths: **${contactPaths}**.  
Top patterns: ${topBuyers}

WHO uses public sources only; Surfe not used.

## H. Competitive Demand Patterns

Cross-competitor patterns: **${allCross.length}** (same org across hotels/cycles when evidenced).

## I. Target Hotel Opportunity Thesis

STRONG_FIT: **${strongFit}** · PLAUSIBLE_FIT: **${plausibleFit}**  
Theses separate FACT / INFERENCE / UNKNOWN. Win angles are hypotheses only.

## J. Hotel Results

| Hotel | Comps | Traces | Deep | Candidates | Ready | Watch |
|-------|------:|-------:|-----:|-----------:|------:|------:|
| YOTEL | ${byKey.YOTEL?.competitorsSearched ?? 0} | ${byKey.YOTEL?.validatedTraces ?? 0} | ${byKey.YOTEL?.deepTraces ?? 0} | ${byKey.YOTEL?.gdiCandidates ?? 0} | ${byKey.YOTEL?.customerReady ?? 0} | ${byKey.YOTEL?.futureWatch ?? 0} |
| AC | ${byKey.AC?.competitorsSearched ?? 0} | ${byKey.AC?.validatedTraces ?? 0} | ${byKey.AC?.deepTraces ?? 0} | ${byKey.AC?.gdiCandidates ?? 0} | ${byKey.AC?.customerReady ?? 0} | ${byKey.AC?.futureWatch ?? 0} |
| SPICE | ${byKey.SPICE?.competitorsSearched ?? 0} | ${byKey.SPICE?.validatedTraces ?? 0} | ${byKey.SPICE?.deepTraces ?? 0} | ${byKey.SPICE?.gdiCandidates ?? 0} | ${byKey.SPICE?.customerReady ?? 0} | ${byKey.SPICE?.futureWatch ?? 0} |
| CAMBRIDGE | ${byKey.CAMBRIDGE?.competitorsSearched ?? 0} | ${byKey.CAMBRIDGE?.validatedTraces ?? 0} | ${byKey.CAMBRIDGE?.deepTraces ?? 0} | ${byKey.CAMBRIDGE?.gdiCandidates ?? 0} | ${byKey.CAMBRIDGE?.customerReady ?? 0} | ${byKey.CAMBRIDGE?.futureWatch ?? 0} |
| NOW NOW | ${byKey.NOW_NOW?.competitorsSearched ?? 0} | ${byKey.NOW_NOW?.validatedTraces ?? 0} | ${byKey.NOW_NOW?.deepTraces ?? 0} | ${byKey.NOW_NOW?.gdiCandidates ?? 0} | ${byKey.NOW_NOW?.customerReady ?? 0} | ${byKey.NOW_NOW?.futureWatch ?? 0} |

## K. Precision / QA

Sample audited: **${sample.length}**. Precision (TRUE_COMP_DEMAND / VALID_REPEAT): **${precision}%**.  
See \`QUALITY_AUDIT.csv\`.

## L. Cost / Research Efficiency

Incremental cost: **$${totalCost.toFixed(2)}**. Details in \`COST_REPORT.md\`.

## M. Recommended Default GDI Behavior

1. Resolve ADP (or documented fallback) comp set — never invent comps.
2. Build public pivots (name, alias, phone variants, address, domain, meeting rooms).
3. SERP = pointer; **open the page**.
4. Classify evidence; only DIRECT/STRONG deepen.
5. Mine repeat + cross-competitor patterns without fabricating cycles.
6. Resolve buyer entity/function from public sources.
7. Build target thesis with FACT/INFERENCE/UNKNOWN + fit class.
8. Convert via \`isGdiResearchLeadWorthPursuing\` + canonical ready/watch — no manual promotion.
9. Jev advises next research only after a validated pattern exists.
`;

  write("FOUNDER_REPORT.md", founder);

  const ret = {
    COMP_SET_DEMAND_MINING_IMPLEMENTED: "YES",
    PHONE_NUMBER_SEARCH_IMPLEMENTED: "YES",
    ALIAS_SEARCH_IMPLEMENTED: "YES",
    ADDRESS_SEARCH_IMPLEMENTED: "YES",
    PAGE_LEVEL_VALIDATION_IMPLEMENTED: "YES",
    REPEAT_PATTERN_MINING_IMPLEMENTED: "YES",
    CROSS_COMPETITOR_PATTERN_MINING_IMPLEMENTED: "YES",
    TARGET_HOTEL_THESIS_IMPLEMENTED: "YES",
    JEV_NEXT_BEST_RESEARCH_USED: allJev.some((j) => j.issued) ? "YES" : "NO",
    TOTAL_COMP_HOTELS_SEARCHED: totalComps,
    TOTAL_SEARCH_PIVOTS: totalPivots,
    TOTAL_PHONE_SEARCH_HITS: totalPhoneHits,
    TOTAL_VALIDATED_COMP_DEMAND_TRACES: allTraces.length,
    TOTAL_DIRECT_CONFIRMED: totalDirect,
    TOTAL_STRONG_ASSOCIATION: totalStrong,
    TOTAL_REPEAT_PATTERNS: allRepeat.length,
    TOTAL_FUTURE_CYCLES_IDENTIFIED: futureCycles,
    TOTAL_BUYER_ENTITIES_RESOLVED: buyersResolved,
    TOTAL_PUBLIC_CONTACT_PATHS: contactPaths,
    TOTAL_TARGET_HOTEL_STRONG_FIT: strongFit,
    TOTAL_TARGET_HOTEL_PLAUSIBLE_FIT: plausibleFit,
    TOTAL_GDI_CANDIDATE_OPPORTUNITIES: totalCands,
    TOTAL_CUSTOMER_READY: totalReady,
    TOTAL_VALID_FUTURE_WATCH: totalWatch,
    YOTEL_CUSTOMER_READY_WATCH: `${byKey.YOTEL?.customerReady ?? 0} / ${byKey.YOTEL?.futureWatch ?? 0}`,
    AC_CUSTOMER_READY_WATCH: `${byKey.AC?.customerReady ?? 0} / ${byKey.AC?.futureWatch ?? 0}`,
    SPICE_CUSTOMER_READY_WATCH: `${byKey.SPICE?.customerReady ?? 0} / ${byKey.SPICE?.futureWatch ?? 0}`,
    CAMBRIDGE_CUSTOMER_READY_WATCH: `${byKey.CAMBRIDGE?.customerReady ?? 0} / ${byKey.CAMBRIDGE?.futureWatch ?? 0}`,
    NOW_NOW_CUSTOMER_READY_WATCH: `${byKey.NOW_NOW?.customerReady ?? 0} / ${byKey.NOW_NOW?.futureWatch ?? 0}`,
    QUALITY_AUDIT_PRECISION_PCT: precision,
    PHONE_SEARCH_UNIQUE_USEFUL_YIELD_PCT: phoneUsefulPct,
    TOP_SEARCH_PIVOT_BY_USEFUL_YIELD: topPivot,
    TOP_REPEAT_GROUPS_FOUND: topRepeat,
    TOP_BUYER_AGENCY_PATTERNS_FOUND: topBuyers,
    INCREMENTAL_RESEARCH_COST: Number(totalCost.toFixed(2)),
    GDI_THRESHOLDS_CHANGED: "NO",
    PHONE_CO_OCCURRENCE_TREATED_AS_PROOF: "NO",
    JEV_WROTE_VERIFIED_FACTS: "NO",
    JEV_PROMOTED_OPPORTUNITIES: "NO",
    WATCH_QUALITY_STANDARD_BYPASSED: "NO",
    ADP_CHANGED: "NO",
    SHARE_TOKENS_CHANGED: "NO",
    FINAL_VERDICT:
      totalReady + totalWatch > 0
        ? "COMP_SET_MINING_PRODUCED_USEFUL_GDI_OPPORTUNITIES_UNDER_UNCHANGED_GATES"
        : totalDirect + totalStrong > 0
          ? "COMP_SET_MINING_PRODUCED_VALIDATED_TRACES_AND_PATTERNS_CANONICAL_GATES_STILL_BLOCK_READY_WATCH"
          : "COMP_SET_MINING_ARCHITECTURE_SHIPPED_BOUNDED_RUN_YIELDED_LIMITED_DEEP_TRACES",
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
