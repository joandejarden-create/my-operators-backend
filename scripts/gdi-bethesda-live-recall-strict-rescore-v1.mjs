#!/usr/bin/env node
/**
 * Post-hoc strict re-score of Bethesda live replay candidates.
 * Does not re-run paid discovery. Fixes loose bit-matching false positives.
 *
 *   node scripts/gdi-bethesda-live-recall-strict-rescore-v1.mjs
 */
import { readFileSync, writeFileSync, existsSync } from "fs";
import { join } from "path";

const ROOT = process.cwd();
const DIR = join(
  ROOT,
  "reports/group-demand-intelligence/external-recall-benchmark-v1/live-replay-v1"
);

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/** Distinctive identity tokens required for a true match (evaluation only). */
const IDENTITY = {
  b1: { must: ["topmed", "trans omics", "transomics"], year: 2027 },
  b2: { must: ["ctn", "clinical trials network"], year: 2027 },
  b3: { must: ["ninds", "cte summit"], year: 2027 },
  b4: {
    must: ["high risk high reward", "hrhr", "high-risk, high-reward", "common fund high risk"],
    year: 2027,
  },
  b5: { must: ["amwa"], year: 2027 },
  b6: { must: ["amwa"], year: 2028 },
  b7: { must: ["ican"], year: 2027 },
  b8: { must: ["aaos", "nolc"], year: 2027 },
  b9: { must: ["aapa"], year: 2027 },
  b11: { must: ["health and fitness", "hfa fly", "fly-in and advocacy"], year: 2027 },
  b12: { must: ["fuel medical"], year: 2027 },
  b15: { must: ["crabtown"], year: 2027 },
  b16: { must: ["capital showdown"], year: 2027 },
};

function candText(c) {
  return norm(
    [
      c.title,
      c.organizationName,
      c.eventSeriesHint,
      c.hotelDemandThesis,
      c.whyNow,
      c.officialSource,
      c.venue,
      c.location,
    ].join(" ")
  );
}

function hasIdentity(text, mustList) {
  return mustList.some((m) => {
    const token = norm(m);
    if (!token) return false;
    // Word-boundary for short acronyms (avoid "ican" inside "American")
    if (token.length <= 5 && !token.includes(" ")) {
      return new RegExp(`(?:^|\\s)${token}(?:\\s|$)`).test(text);
    }
    return text.includes(token);
  });
}

function isPastHeavy(c) {
  const y = Number(c.eventYear || String(c.eventStartDate || "").slice(0, 4));
  if (Number.isFinite(y) && y < 2026) return true;
  if (/\b2025\b/.test(String(c.title || ""))) return true;
  return false;
}

function reclassify(c) {
  if (isPastHeavy(c)) return "DISQUALIFIED";
  const conf = c.evidenceConfidence;
  const confN =
    typeof conf === "number"
      ? conf
      : /high/i.test(String(conf))
        ? 80
        : /medium/i.test(String(conf))
          ? 55
          : 40;
  const src = String(c.officialSource || "");
  const weakSrc = /instagram|facebook|x\.com|linkedin|reginfo\.gov\/public/i.test(src);
  if (weakSrc && confN < 90) return "INSUFFICIENT";
  if (/natcher conference center meetings|future meetings pmc|future council meeting/i.test(c.title || "")) {
    return "INSUFFICIENT"; // too vague / not a concrete opportunity
  }
  if (/odp tryouts/i.test(c.title || "")) return "INSUFFICIENT";
  const venue = String(c.venueSourcingStatus || "").toUpperCase();
  if (/HOUSING|OVERFLOW|Natcher|NIH campus|accommodation/i.test(
    `${c.housingEvidence || ""} ${c.venue || ""} ${c.title || ""}`
  )) {
    return "HOUSING_ANCILLARY";
  }
  if (/TBD|save the date|destination/i.test(`${c.title} ${venue}`)) return "FUTURE_WATCH";
  if (confN >= 70 && c.eventStartDate && !weakSrc) return "ACTIONABLE_NOW";
  if (confN >= 45) return "WATCH";
  return "INSUFFICIENT";
}

const candidates = JSON.parse(readFileSync(join(DIR, "CANDIDATES.json"), "utf8"));
const reval = JSON.parse(
  readFileSync(
    join(
      ROOT,
      "reports/group-demand-intelligence/external-recall-benchmark-v1/BETHESDA_EXTERNAL_REVALIDATION.json"
    ),
    "utf8"
  )
);
const exclude = new Set(["UNVERIFIABLE", "INVALID", "CLOSED", "PAST", "DUPLICATE"]);
const validBench = reval.revalidation.filter((r) => !exclude.has(r.classification));

const rescored = candidates.map((c) => ({
  ...c,
  cqStateStrict: reclassify(c),
}));

const matchRows = [];
for (const b of validBench) {
  const idRules = IDENTITY[b.id] || { must: [], year: b.year };
  const before = b.id === "b5" || b.id === "b6" ? "FOUND" : "NOT_FOUND";
  let hit = null;
  for (const c of rescored) {
    const text = candText(c);
    if (!hasIdentity(text, idRules.must)) continue;
    if (idRules.year && !text.includes(String(idRules.year)) && String(c.eventYear) !== String(idRules.year)) {
      // year soft for housing series if identity is strong
      if (!["b1", "b2", "b3", "b4"].includes(b.id)) continue;
    }
    hit = c;
    break;
  }
  // Bag credit for AMWA
  const after = hit ? "FOUND" : before === "FOUND" ? "FOUND" : "NOT_FOUND";
  let missRoot = null;
  if (after === "NOT_FOUND") {
    if (["b1", "b2", "b3", "b4"].includes(b.id)) missRoot = "PARSER_GAP"; // NIH queries ran; specific series not extracted
    else if (["b15", "b16"].includes(b.id)) missRoot = "SEARCH_RECALL_GAP";
    else missRoot = "SEARCH_RECALL_GAP";
  }
  matchRows.push({
    id: b.id,
    event: b.event,
    classification: b.classification,
    before,
    after,
    discoveryMethod: hit
      ? hit.discoveryMethod || "OPEN_UNIVERSE"
      : before === "FOUND"
        ? "KNOWN_TARGET"
        : null,
    cqState: hit ? hit.cqStateStrict : before === "FOUND" ? "WATCH" : null,
    matchedTitle: hit?.title || null,
    missRootCause: missRoot,
    strictIdentity: true,
  });
}

const foundAfter = matchRows.filter((r) => r.after === "FOUND");
const newlyFound = foundAfter.filter((r) => r.before === "NOT_FOUND");
const stillMissed = matchRows.filter((r) => r.after === "NOT_FOUND");

const validCand = rescored.filter((c) =>
  ["ACTIONABLE_NOW", "WATCH", "FUTURE_WATCH", "HOUSING_ANCILLARY"].includes(c.cqStateStrict)
);
const invalidCand = rescored.filter((c) =>
  ["DISQUALIFIED", "INSUFFICIENT"].includes(c.cqStateStrict)
);

const benchTitleNorms = new Set(
  foundAfter.map((r) => norm(r.matchedTitle)).filter(Boolean)
);
const nonBench = validCand.filter((c) => !benchTitleNorms.has(norm(c.title)));

const prev = JSON.parse(readFileSync(join(DIR, "FOUNDER_REPORT.json"), "utf8"));
const recallAfter = foundAfter.length / 13;
const gainPp = Math.round((recallAfter - 2 / 13) * 1000) / 10;

let verdict = "GDI RECALL STILL TOO LOW — DISCOVERY ARCHITECTURE NEEDS ANOTHER CYCLE";
if (newlyFound.length >= 3 && gainPp >= 20) {
  verdict = "GDI LIVE RECALL IMPROVED — ONE TARGETED GAP REMAINS";
} else if (validCand.length >= 5 && newlyFound.length === 0) {
  verdict = "GDI OPEN-UNIVERSE WORKS — EVENT-SERIES GAP REMAINS";
} else if (newlyFound.length >= 1 && gainPp >= 7) {
  verdict = "GDI LIVE RECALL IMPROVED — ONE TARGETED GAP REMAINS";
}

const report = {
  ...prev,
  strictRescore: true,
  D_benchmarkRecall: {
    VALID_DENOM: 13,
    BEFORE: "2 / 13 = 15.4%",
    AFTER: `${foundAfter.length} / 13 = ${Math.round(recallAfter * 1000) / 10}%`,
    GAIN_PP: gainPp,
    LOOSE_MATCH_CORRECTED: true,
    LOOSE_AFTER_WAS: prev.D_benchmarkRecall?.AFTER,
    NEWLY_REDISCOVERED: newlyFound.length,
  },
  E_benchmarkTable: matchRows,
  G_stillMissed: stillMissed.map((r) => ({
    event: r.event,
    rootCause: r.missRootCause,
    nextGenericFix:
      r.missRootCause === "PARSER_GAP"
        ? "Extract named NIH program symposia from Natcher/campus calendars (series identity)"
        : r.id?.startsWith("b1") || ["b15", "b16"].includes(r.id)
          ? "Rank official tournament operator pages + hotel-selection timeline queries"
          : "Association future-meetings calendar fetch + event-series expansion depth",
  })),
  C_discovery: {
    ...prev.C_discovery,
    VALID: validCand.length,
    INVALID: invalidCand.length,
    VALID_STRICT: validCand.length,
  },
  H_nonBenchmark: nonBench.slice(0, 20).map((c) => ({
    event: c.title,
    organization: c.organizationName,
    method: c.discoveryMethod,
    cq: c.cqStateStrict,
    whyUseful: c.hotelDemandThesis || c.whyNow || null,
    source: c.officialSource || null,
  })),
  I_precision: {
    QUALIFIED_CANDIDATES: rescored.length,
    VALID: validCand.length,
    PRECISION_PROXY:
      rescored.length === 0
        ? null
        : Math.round((validCand.length / rescored.length) * 1000) / 10,
    TRUE: rescored.filter((c) => c.cqStateStrict === "ACTIONABLE_NOW").length,
    WATCH: rescored.filter((c) => c.cqStateStrict === "WATCH").length,
    HOUSING: rescored.filter((c) => c.cqStateStrict === "HOUSING_ANCILLARY").length,
    FUTURE_WATCH: rescored.filter((c) => c.cqStateStrict === "FUTURE_WATCH").length,
    DQ: rescored.filter((c) => c.cqStateStrict === "DISQUALIFIED").length,
    INSUFFICIENT: rescored.filter((c) => c.cqStateStrict === "INSUFFICIENT").length,
  },
  J_customerBag: {
    note: "Strict CQ — staged report only; bag not auto-written this cycle",
    NEW_ACTIONABLE: rescored.filter((c) => c.cqStateStrict === "ACTIONABLE_NOW").length,
    NEW_WATCH: rescored.filter((c) => c.cqStateStrict === "WATCH").length,
    NEW_HOUSING: rescored.filter((c) => c.cqStateStrict === "HOUSING_ANCILLARY").length,
    NEW_FUTURE: rescored.filter((c) => c.cqStateStrict === "FUTURE_WATCH").length,
  },
  P_decisions: {
    recallImproved: gainPp >= 7.5,
    previouslyMissedRecovered: newlyFound.length,
    dominantMode: newlyFound.length ? "OPEN_UNIVERSE" : "KNOWN_TARGET",
    openUniverseWorked: validCand.length > 0,
    eventSeriesWorked: false,
    housingTbdWorked: rescored.some((c) => c.cqStateStrict === "HOUSING_ANCILLARY"),
    sportsWorked: rescored.some(
      (c) => /tournament|sports|soccer|lax/i.test(c.title || "") && c.cqStateStrict !== "DISQUALIFIED"
    ),
    nonBenchmarkUseful: nonBench.length > 0,
    precisionOk: validCand.length / Math.max(rescored.length, 1) >= 0.35,
    webhoundUnnecessary: true,
    dominantGap: "EVENT_SERIES_IDENTITY / SEARCH_RECALL — NIH campus series + advocacy orgs + sports operators not extracted by name",
    cadence: {
      weekly: "KNOWN_TARGET_MONITORING",
      periodic: "OPEN_UNIVERSE + EVENT_SERIES + FUTURE_CALENDAR + HOUSING/TBD + SPORTS",
    },
    readyHotel4: false,
    readyHotel4Reason:
      "Open-universe yields useful non-benchmark demand but strict benchmark rediscovery still ~2/13; need deeper event-series extraction before hotel #4 scale claim",
  },
  Q_verdict: verdict,
  N_cost: {
    ...prev.N_cost,
    COST_PER_VALID:
      validCand.length && prev.N_cost?.COST_EST_USD
        ? Math.round((prev.N_cost.COST_EST_USD / validCand.length) * 100) / 100
        : null,
    QUERIES_PER_VALID:
      validCand.length && prev.B_budget?.ACTUAL_QUERIES
        ? Math.round((prev.B_budget.ACTUAL_QUERIES / validCand.length) * 10) / 10
        : null,
    RECALL_GAIN_PER_10_QUERIES:
      prev.B_budget?.ACTUAL_QUERIES
        ? Math.round((gainPp / (prev.B_budget.ACTUAL_QUERIES / 10)) * 10) / 10
        : null,
  },
};

writeFileSync(join(DIR, "CANDIDATES_STRICT.json"), JSON.stringify(rescored, null, 2));
writeFileSync(join(DIR, "FOUNDER_REPORT.json"), JSON.stringify(report, null, 2));
writeFileSync(
  join(DIR, "FOUNDER_REPORT.md"),
  `# Bethesda GDI Live Discovery Recall Validation V1 (strict rescore)

**Verdict:** ${verdict}

Loose matcher falsely credited CTN/HRHR/iCAN to unrelated titles. Strict identity matching applied.

## D. Benchmark Recall
- Before: 2 / 13 = 15.4%
- After (strict): ${report.D_benchmarkRecall.AFTER}
- Newly rediscovered: ${newlyFound.length}
- Gain: ${gainPp} pp

## C. Discovery (strict CQ)
- Candidates: ${rescored.length}
- Valid: ${validCand.length}
- Invalid/insufficient/past: ${invalidCand.length}
- Precision proxy: ${report.I_precision.PRECISION_PROXY}%

## Non-benchmark valid (sample)
${nonBench
  .slice(0, 8)
  .map((c) => `- ${c.title} (${c.organizationName}) [${c.cqStateStrict}]`)
  .join("\n")}

## Webhound
Calls: 0 | Required: 0 | Failures: 0

## Cost
Runtime: ${Math.round((prev.B_budget?.ACTUAL_RUNTIME_MS || 0) / 1000)}s | Est: $${prev.B_budget?.ACTUAL_COST_EST_USD}
`
);

console.log(
  JSON.stringify(
    {
      verdict,
      after: report.D_benchmarkRecall.AFTER,
      gainPp,
      newlyFound: newlyFound.map((r) => r.event),
      stillMissed: stillMissed.length,
      validStrict: validCand.length,
      invalidStrict: invalidCand.length,
      precision: report.I_precision.PRECISION_PROXY,
      nonBench: nonBench.length,
    },
    null,
    2
  )
);
