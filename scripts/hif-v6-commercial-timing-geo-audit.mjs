#!/usr/bin/env node
/**
 * HIF V6 — Commercial Timing & Geographic Opportunity Audit.
 * Real compute against canonical inventory. CUSTOMER_BLOCKED. $0.
 *
 * Usage: node scripts/hif-v6-commercial-timing-geo-audit.mjs --run
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  TIMING_HORIZON,
  TIMING_HORIZON_ORDER,
  DECISION_WINDOW_KIND,
  deriveNextCommercialDecisionWindow,
  emptyHorizonCounts,
} from "../lib/hotel-intelligence/sec-edgar/hif-v6-commercial-timing-model-v6.js";
import { classifyPhysicalGeography, GEO_CLASS } from "../lib/hotel-intelligence/sec-edgar/hif-v6-geographic-confidence-v6.js";
import {
  classifyCommercialActionability,
  COMMERCIAL_PRIORITY,
} from "../lib/hotel-intelligence/sec-edgar/hif-v6-commercial-actionability-v6.js";
import {
  PRIORITY_COUNTRIES_V6,
} from "../lib/hotel-intelligence/sec-edgar/hif-v6-country-concentration-v6.js";
import {
  WORLDWIDE_ELIGIBILITY_POLICY_V6,
  buildDualGeographyViews,
  isPriorityMarket,
} from "../lib/hotel-intelligence/sec-edgar/hif-v6-worldwide-eligibility-v6.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-commercial-timing-geo-audit");
const RUN = process.argv.includes("--run");
const asOf = new Date().toISOString().slice(0, 10);
const writeJson = (fp, obj) => {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
};
const load = (rel) => {
  const fp = path.join(ROOT, rel);
  if (!fs.existsSync(fp)) return null;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
};

function loadInventory() {
  const byKey = new Map();
  for (const rel of [
    "reports/sec-edgar-hif-v6-canonical-inventory/TOWARD_1000.json",
    "reports/sec-edgar-hif-v6-canonical-inventory/ACTIVE_DATED.json",
    "reports/sec-edgar-hif-v6-canonical-inventory/HISTORICAL.json",
    "reports/sec-edgar-hif-v6-canonical-inventory/AGREEMENT_TYPE_UNRESOLVED.json",
  ]) {
    const j = load(rel);
    for (const r of j?.rows || j?.hotels || []) {
      const key =
        r.key ||
        `${String(r.hotelName || "")
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, " ")
          .trim()}|${String(r.hotelCountry || "US").toUpperCase()}|${r.expirationDate || "NODATE"}`;
      if (!byKey.has(key)) byKey.set(key, { ...r, key });
    }
  }
  return [...byKey.values()];
}

function inventoryBucket(row) {
  if (row.status === "HISTORICAL_OR_SUPERSEDED") return "HISTORICAL";
  if (row.commercialList === "BRAND_CONVERSION_TARGETS" && row.agreementTypeResolved) return "BCT";
  if (row.commercialList === "MANAGEMENT_CONTRACT_TARGETS" && row.agreementTypeResolved) return "MCT";
  if (row.status === "ACTIVE_UNDATED_EVIDENCE_BACKED") return "CONTRACTUAL_UNDATED";
  return "CONTRACTUAL";
}

function ensureMatrix() {
  return Object.fromEntries(TIMING_HORIZON_ORDER.map((h) => [h, 0]));
}

function bump(matrix, country, horizon) {
  const c = country || "UNATTRIBUTED";
  if (!matrix[c]) matrix[c] = ensureMatrix();
  matrix[c][horizon] = (matrix[c][horizon] || 0) + 1;
}

function withinMonths(months, max) {
  return months !== null && months !== undefined && months >= 0 && months < max;
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  if (!RUN) {
    console.log(JSON.stringify({ dryRun: true, asOf }, null, 2));
    return;
  }

  const rows = loadInventory();
  const audited = [];
  const matrices = {
    contractual: {},
    bct: {},
    mct: {},
    actionable: {}, // Priority A+B
  };
  const priorityCounts = {
    [COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW]: 0,
    [COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH]: 0,
    [COMMERCIAL_PRIORITY.C_STRATEGIC_WATCHLIST]: 0,
    [COMMERCIAL_PRIORITY.D_LONG_TERM_INTELLIGENCE]: 0,
    [COMMERCIAL_PRIORITY.HOLD]: 0,
  };
  const horizonTotals = emptyHorizonCounts();
  const windowKinds = {};
  const geoClasses = { VERIFIED: 0, REVIEW_REQUIRED: 0, UNRESOLVED: 0 };
  const byState = {};
  const byCity = {};
  let confirmedDecisionWindows = 0;
  let expirationOnly = 0;
  let timingUnknown = 0;
  let within12 = 0;
  let within24 = 0;
  let within36 = 0;
  let within60 = 0;
  let beyond60 = 0;
  let unresolvedAgr = 0;

  for (const row of rows) {
    const timing = deriveNextCommercialDecisionWindow(row, asOf);
    const geo = classifyPhysicalGeography(row);
    const action = classifyCommercialActionability(row, timing, geo);
    const bucket = inventoryBucket(row);

    const rec = {
      key: row.key,
      hotelName: row.hotelName,
      ...geo,
      expirationDate: row.expirationDate || null,
      ...timing,
      ...action,
      agreementType: row.agreementType || null,
      agreementTypeResolved: row.agreementTypeResolved !== false && action.agreementTypeConfidence !== "UNRESOLVED",
      commercialList: row.commercialList || null,
      status: row.status || null,
      inventoryBucket: bucket,
      sourceUrl: row.sourceUrl || null,
      sourceFamily: row.sourceFamily || null,
      wave: row.wave || null,
      isPriorityMarket: isPriorityMarket(geo.physicalCountry),
    };
    audited.push(rec);

    horizonTotals[timing.timingHorizon] = (horizonTotals[timing.timingHorizon] || 0) + 1;
    windowKinds[timing.decisionWindowKind] = (windowKinds[timing.decisionWindowKind] || 0) + 1;
    geoClasses[geo.geographicClass] = (geoClasses[geo.geographicClass] || 0) + 1;
    priorityCounts[action.commercialPriority] = (priorityCounts[action.commercialPriority] || 0) + 1;

    if (timing.decisionWindowKind === DECISION_WINDOW_KIND.CONFIRMED_DECISION_WINDOW) confirmedDecisionWindows += 1;
    if (timing.decisionWindowKind === DECISION_WINDOW_KIND.EXPIRATION_ONLY_PROXY) expirationOnly += 1;
    if (timing.decisionWindowKind === DECISION_WINDOW_KIND.TIMING_UNKNOWN) timingUnknown += 1;
    if (action.agreementTypeConfidence === "UNRESOLVED") unresolvedAgr += 1;

    const m = timing.monthsToDecision;
    if (withinMonths(m, 12)) within12 += 1;
    if (withinMonths(m, 24)) within24 += 1;
    if (withinMonths(m, 36)) within36 += 1;
    if (withinMonths(m, 60)) within60 += 1;
    if (m !== null && m >= 60) beyond60 += 1;

    bump(matrices.contractual, geo.physicalCountry || "UNATTRIBUTED", timing.timingHorizon);
    if (bucket === "BCT") bump(matrices.bct, geo.physicalCountry || "UNATTRIBUTED", timing.timingHorizon);
    if (bucket === "MCT") bump(matrices.mct, geo.physicalCountry || "UNATTRIBUTED", timing.timingHorizon);
    if (
      action.commercialPriority === COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW ||
      action.commercialPriority === COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH
    ) {
      bump(matrices.actionable, geo.physicalCountry || "UNATTRIBUTED", timing.timingHorizon);
    }

    if (geo.stateProvince) {
      const sk = `${geo.physicalCountry || "?"}|${geo.stateProvince}`;
      byState[sk] = byState[sk] || emptyHorizonCounts();
      byState[sk][timing.timingHorizon] = (byState[sk][timing.timingHorizon] || 0) + 1;
    }
    if (geo.city) {
      const ck = `${geo.physicalCountry || "?"}|${geo.city}`;
      byCity[ck] = byCity[ck] || emptyHorizonCounts();
      byCity[ck][timing.timingHorizon] = (byCity[ck][timing.timingHorizon] || 0) + 1;
    }
  }

  // Examples — real only
  const examples = {
    nearTermFranchise: audited
      .filter(
        (r) =>
          r.agreementTypeConfidence === "FRANCHISE" &&
          (r.timingHorizon === TIMING_HORIZON.M0_6 || r.timingHorizon === TIMING_HORIZON.M6_12),
      )
      .slice(0, 5),
    nearTermManagement: audited
      .filter(
        (r) =>
          r.agreementTypeConfidence === "MANAGEMENT" &&
          (r.timingHorizon === TIMING_HORIZON.M0_6 || r.timingHorizon === TIMING_HORIZON.M6_12),
      )
      .slice(0, 5),
    unresolvedNearTerm: audited
      .filter(
        (r) =>
          r.agreementTypeConfidence === "UNRESOLVED" &&
          (r.timingHorizon === TIMING_HORIZON.M0_6 ||
            r.timingHorizon === TIMING_HORIZON.M6_12 ||
            r.timingHorizon === TIMING_HORIZON.M12_24),
      )
      .slice(0, 8),
    confirmedDecisionWindows: audited
      .filter((r) => r.decisionWindowKind === DECISION_WINDOW_KIND.CONFIRMED_DECISION_WINDOW)
      .slice(0, 5),
    longTermWithTriggers: audited
      .filter(
        (r) =>
          (r.timingHorizon === TIMING_HORIZON.Y3_5 || r.timingHorizon === TIMING_HORIZON.Y5_PLUS) &&
          r.decisionWindowKind !== DECISION_WINDOW_KIND.EXPIRATION_ONLY_PROXY,
      )
      .slice(0, 5),
    priorityA: audited.filter((r) => r.commercialPriority === COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW).slice(0, 8),
    priorityB: audited.filter((r) => r.commercialPriority === COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH).slice(0, 8),
  };

  const dual = buildDualGeographyViews(
    Object.fromEntries(
      Object.entries(matrices.contractual).map(([c, horizons]) => [
        c,
        {
          distinctEvidenceBackedProspects: Object.values(horizons).reduce((s, n) => s + n, 0),
          distinctHotelsResearched: Object.values(horizons).reduce((s, n) => s + n, 0),
        },
      ]),
    ),
  );

  const summary = {
    wave: "COMMERCIAL_TIMING_GEO_AUDIT",
    asOf,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    worldwideEligibility: WORLDWIDE_ELIGIBILITY_POLICY_V6.priorityMarketsControl,
    inventoryAnalyzed: rows.length,
    measures: {
      contractualRecordsAnalyzed: rows.length,
      within12Months: within12,
      within24Months: within24,
      within36Months: within36,
      within5Years: within60,
      beyond5Years: beyond60,
      confirmedDecisionWindows,
      expirationOnlyProxy: expirationOnly,
      timingUnknown,
      agreementTypeUnresolved: unresolvedAgr,
      priorityA: priorityCounts[COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW],
      priorityB: priorityCounts[COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH],
      priorityC: priorityCounts[COMMERCIAL_PRIORITY.C_STRATEGIC_WATCHLIST],
      priorityD: priorityCounts[COMMERCIAL_PRIORITY.D_LONG_TERM_INTELLIGENCE],
      hold: priorityCounts[COMMERCIAL_PRIORITY.HOLD],
      qualifiedBct: audited.filter((r) => r.inventoryBucket === "BCT").length,
      qualifiedMct: audited.filter((r) => r.inventoryBucket === "MCT").length,
      outreachReady: 0,
      geoVerified: geoClasses.VERIFIED,
      geoReviewRequired: geoClasses.REVIEW_REQUIRED,
      geoUnresolved: geoClasses.UNRESOLVED,
    },
    horizonTotals,
    windowKinds,
    geoClasses,
    priorityCounts,
    priorityMarkets: PRIORITY_COUNTRIES_V6,
    dualGeography: dual,
    limitations: [
      "Nearly all current CMBS-derived rows use EXPIRATION_ONLY_PROXY — confirmed notice deadlines are rare in schedule extracts",
      "Physical country often REVIEW_REQUIRED when locality cues are weak; filing jurisdiction alone never verifies country",
      "Unresolved combined License/Franchise/Operating/Management columns cannot auto-qualify as BCT/MCT",
      "Priority A requires confirmed decision window or strong inferred active signal — expiration-only near-term is Priority B at best",
      "8-K undated management signals contribute UNKNOWN timing unless a date is extracted",
    ],
  };

  writeJson(path.join(OUT, "AUDIT_SUMMARY.json"), summary);
  writeJson(path.join(OUT, "AUDITED_RECORDS.json"), {
    count: audited.length,
    // Cap file size: full key index + sample detail
    records: audited,
  });
  writeJson(path.join(OUT, "MATRIX_CONTRACTUAL.json"), matrices.contractual);
  writeJson(path.join(OUT, "MATRIX_BCT.json"), matrices.bct);
  writeJson(path.join(OUT, "MATRIX_MCT.json"), matrices.mct);
  writeJson(path.join(OUT, "MATRIX_ACTIONABLE_AB.json"), matrices.actionable);
  writeJson(path.join(OUT, "BY_STATE_PROVINCE.json"), byState);
  writeJson(path.join(OUT, "BY_CITY.json"), byCity);
  writeJson(path.join(OUT, "EXAMPLES.json"), examples);
  writeJson(path.join(OUT, "PRIORITY_A.json"), {
    count: examples.priorityA.length,
    note: "Full Priority A set may be empty if no confirmed decision windows exist",
    hotels: audited.filter((r) => r.commercialPriority === COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW),
  });
  writeJson(path.join(OUT, "PRIORITY_B.json"), {
    count: audited.filter((r) => r.commercialPriority === COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH).length,
    hotels: audited
      .filter((r) => r.commercialPriority === COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH)
      .slice(0, 200),
  });

  // Markdown founder report
  const md = renderFounderMd(summary, matrices, examples, byState);
  fs.writeFileSync(path.join(OUT, "FOUNDER_COMMERCIAL_TIMING_GEO_AUDIT.md"), md, "utf8");

  console.log(JSON.stringify(summary.measures, null, 2));
}

function renderMatrixTable(matrix) {
  const countries = Object.keys(matrix).sort((a, b) => {
    const pa = isPriorityMarket(a) ? 0 : 1;
    const pb = isPriorityMarket(b) ? 0 : 1;
    if (pa !== pb) return pa - pb;
    const ta = Object.values(matrix[a]).reduce((s, n) => s + n, 0);
    const tb = Object.values(matrix[b]).reduce((s, n) => s + n, 0);
    return tb - ta;
  });
  const cols = [
    TIMING_HORIZON.NOW_ACTIVE_PROCESS,
    TIMING_HORIZON.M0_6,
    TIMING_HORIZON.M6_12,
    TIMING_HORIZON.M12_24,
    TIMING_HORIZON.M24_36,
    TIMING_HORIZON.Y3_5,
    TIMING_HORIZON.Y5_PLUS,
    TIMING_HORIZON.UNKNOWN,
  ];
  let out = `| Country | Now | 0–6m | 6–12m | 12–24m | 24–36m | 3–5y | 5+y | Unknown |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|\n`;
  for (const c of countries) {
    const row = matrix[c];
    const mark = isPriorityMarket(c) ? `**${c}**` : c;
    out += `| ${mark} | ${cols.map((k) => row[k] || 0).join(" | ")} |\n`;
  }
  return out;
}

function exLine(r) {
  return `- **${r.hotelName}** (${r.physicalCountry || "?"}${r.stateProvince ? "/" + r.stateProvince : ""}${r.city ? ", " + r.city : ""}) · ${r.nextCommercialDecisionDate || "n/a"} · ${r.timingHorizon} · ${r.decisionWindowKind} · ${r.agreementTypeConfidence} · ${r.commercialPriority}`;
}

function renderFounderMd(summary, matrices, examples, byState) {
  const m = summary.measures;
  const topStates = Object.entries(byState)
    .map(([k, v]) => [k, Object.values(v).reduce((s, n) => s + n, 0)])
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15);

  return `# HIF V6 — Commercial Timing & Geographic Opportunity Audit

**As of:** ${summary.asOf}  
**Publication:** CUSTOMER_BLOCKED  
**Research cost:** $0  
**Policy:** Priority markets = concentration only (worldwide eligibility)

## 1) Executive summary

| Measure | Count |
|---------|------:|
| Canonical records analyzed | **${m.contractualRecordsAnalyzed}** |
| Within 12 months | **${m.within12Months}** |
| Within 24 months | **${m.within24Months}** |
| Within 36 months | **${m.within36Months}** |
| Within 5 years | ${m.within5Years} |
| Beyond 5 years | ${m.beyond5Years} |
| Confirmed decision windows | **${m.confirmedDecisionWindows}** |
| Expiration-only proxy | ${m.expirationOnlyProxy} |
| Timing unknown | ${m.timingUnknown} |
| Agreement-type unresolved | **${m.agreementTypeUnresolved}** |
| Priority A | **${m.priorityA}** |
| Priority B | **${m.priorityB}** |
| Priority C / D / HOLD | ${m.priorityC} / ${m.priorityD} / ${m.hold} |
| Qualified BCT / MCT | ${m.qualifiedBct} / ${m.qualifiedMct} |
| Outreach-ready | ${m.outreachReady} |
| Geo VERIFIED / REVIEW / UNRESOLVED | ${m.geoVerified} / ${m.geoReviewRequired} / ${m.geoUnresolved} |

**Interpretation:** Most CMBS schedule dates are **EXPIRATION_ONLY_PROXY**. Near-term expiration alone does **not** create Priority A. Confirmed notice/termination windows remain scarce in current extracts.

## 2) Country × timing matrix (contractual intelligence)

${renderMatrixTable(matrices.contractual)}

### Qualified BCT matrix
${renderMatrixTable(matrices.bct)}

### Qualified MCT matrix
${renderMatrixTable(matrices.mct)}

### Priority A+B (commercially actionable research focus)
${renderMatrixTable(matrices.actionable)}

### Top state/province concentrations
${topStates.map(([k, n]) => `- ${k}: ${n}`).join("\n") || "- (insufficient locality cues)"}

## 3) Commercial qualification (separate measures)

- Contractual intelligence ≠ BCT ≠ MCT ≠ outreach-ready.
- Unresolved combined columns stay unresolved.
- Priority is a planning signal, not publication approval.

## 4) Examples (real inventory only)

### Near-term franchise (resolved type)
${examples.nearTermFranchise.map(exLine).join("\n") || "_None in current extract._"}

### Near-term management (resolved type)
${examples.nearTermManagement.map(exLine).join("\n") || "_None in current extract._"}

### Near-term unresolved agreement type
${examples.unresolvedNearTerm.map(exLine).join("\n") || "_None in current extract._"}

### Confirmed decision windows
${examples.confirmedDecisionWindows.map(exLine).join("\n") || "_None — no verified notice/termination deadlines in current inventory extracts._"}

### Priority A
${examples.priorityA.map(exLine).join("\n") || "_None — requires confirmed decision window or strong active signal; expiration-only excluded._"}

### Priority B (sample)
${examples.priorityB.map(exLine).join("\n") || "_None._"}

## 5) Limitations & next research

${summary.limitations.map((l) => `- ${l}`).join("\n")}

### Recommended allocation
1. Continue uncapped Related License discovery (still high yield).
2. Prioritize EX-10 / footnotes that expose **notice deadlines** and exclusive franchise vs management columns for Priority-B near-term hotels.
3. Deepen geographic verification (address/state) for REVIEW_REQUIRED US CMBS rows.
4. Do not auto-promote unresolved types into BCT/MCT.
`;
}

main().catch((e) => {
  console.error(String(e?.stack || e));
  process.exit(1);
});
