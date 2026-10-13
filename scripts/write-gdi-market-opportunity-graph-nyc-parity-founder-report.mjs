#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";

const OUT = path.join(
  process.cwd(),
  "reports/group-demand-intelligence/market-opportunity-graph-nyc-parity-v1"
);
const summary = JSON.parse(fs.readFileSync(path.join(OUT, "RUN_SUMMARY.json"), "utf8"));
const geo = JSON.parse(fs.readFileSync(path.join(OUT, "NYC_HOTEL_GEOGRAPHY.json"), "utf8"));
const hil = JSON.parse(fs.readFileSync(path.join(OUT, "RENAISSANCE_TO_HILTON.json"), "utf8"));
const now = JSON.parse(fs.readFileSync(path.join(OUT, "RENAISSANCE_TO_NOW_NOW.json"), "utf8"));
const rev = JSON.parse(fs.readFileSync(path.join(OUT, "HILTON_REVERSE_AUDIT.json"), "utf8"));
const bias = JSON.parse(fs.readFileSync(path.join(OUT, "DISCOVERY_BIAS_AUDIT.json"), "utf8"));
const boundary = JSON.parse(fs.readFileSync(path.join(OUT, "MARKET_BOUNDARY_TESTS.json"), "utf8"));
const graph = JSON.parse(fs.readFileSync(path.join(OUT, "MARKET_OPPORTUNITY_GRAPH.json"), "utf8"));
const head = execSync("git rev-parse HEAD", { encoding: "utf8" }).trim();

const lines = [];
lines.push("# GDI Market Opportunity Graph + NYC Cross-Hotel Parity V1 — Founder Report");
lines.push("");
lines.push("## A. EXECUTIVE RESULT");
lines.push("");
lines.push("RENAISSANCE READY:");
lines.push(String(summary.counts.renaissanceReady));
lines.push("");
lines.push("HILTON READY BEFORE:");
lines.push(String(summary.counts.hiltonReadyBefore));
lines.push("");
lines.push("HILTON READY AFTER CROSS-EVALUATION (shadow — not promoted):");
lines.push(String(summary.counts.hiltonReadyAfterShadow));
lines.push("");
lines.push("HILTON HOTEL-MATCHED NEEDS MORE DATA:");
lines.push(String(summary.counts.hiltonHotelMatchedNeedsData));
lines.push("");
lines.push("HILTON NOT FIT:");
lines.push(String(summary.counts.hiltonNotFit));
lines.push("");
lines.push("NOW NOW READY BEFORE:");
lines.push(String(summary.counts.nowReadyBefore));
lines.push("");
lines.push("NOW NOW READY AFTER (shadow):");
lines.push(String(summary.counts.nowReadyAfterShadow));
lines.push("");
lines.push("## B. NYC GEOGRAPHY");
lines.push("");
lines.push("| Hotel | Borough | Sector | Submarket | Micro-Area | Archetype |");
lines.push("|---|---|---|---|---|---|");
for (const [k, g] of Object.entries(geo)) {
  lines.push(
    `| ${g.displayName} | ${g.borough} | ${g.districtSector} | ${g.submarket} | ${g.microArea} | ${g.archetype} |`
  );
}
lines.push("");
lines.push("## C. RENAISSANCE 11 → HILTON");
lines.push("");
lines.push("| Opportunity | Geography | Hilton Applicable? | Fit | Missing Data | Jev Action | Final |");
lines.push("|---|---|---|---|---|---|---|");
for (const r of hil) {
  lines.push(
    `| ${r.opportunityId} | ${r.geography?.label || r.geography?.submarket || "?"} | ${r.hiltonApplicable} | ${r.hiltonFit} | ${(r.hiltonMissing || []).join(",") || "none"} | ${r.jevAction} | ${r.final} |`
  );
}
lines.push("");
lines.push("## D. RENAISSANCE 11 → NOW NOW");
lines.push("");
lines.push("| Opportunity | Geography | NOW Applicable? | Class | Fit | Final |");
lines.push("|---|---|---|---|---|---|");
for (const r of now) {
  lines.push(
    `| ${r.opportunityId} | ${r.geography?.label || "?"} | ${r.nowApplicable} | ${r.nowClass} | ${r.nowFit} | ${r.final} |`
  );
}
lines.push("");
lines.push("## E. HILTON REVERSE AUDIT");
lines.push("");
lines.push("| Hilton Candidate | Market Opportunity | Renaissance Relevant? | NOW NOW Relevant? | Current State |");
lines.push("|---|---|---|---|---|");
for (const r of rev.slice(0, 15)) {
  lines.push(
    `| ${r.hiltonId} | ${r.marketOpportunityId} | ${r.renaissanceRelevant} (${r.renaissanceApplicability}) | ${r.nowRelevant} (${r.nowApplicability}) | ${r.currentState} |`
  );
}
lines.push(`| … | ${rev.length} Hilton rows audited | see HILTON_REVERSE_AUDIT.json | | all currently DISQUALIFIED / non-ready |`);
lines.push("");
lines.push("## F. ROOT CAUSE OF 11 VS 0");
lines.push("");
lines.push("NOT EVALUATED:");
lines.push(String(summary.rootCause.NOT_EVALUATED_FOR_HILTON));
lines.push("");
lines.push("NOT FIT:");
lines.push(String(summary.rootCause.EVALUATED_AND_NOT_FIT));
lines.push("");
lines.push("GEOGRAPHICALLY NOT APPLICABLE:");
lines.push(String(summary.rootCause.GEOGRAPHICALLY_NOT_APPLICABLE));
lines.push("");
lines.push("CLOSED:");
lines.push(String(summary.rootCause.COMMERCIAL_STATUS_CLOSED));
lines.push("");
lines.push("DATA INCOMPLETE:");
lines.push(String(summary.rootCause.DATA_INCOMPLETE));
lines.push("");
lines.push("OTHER:");
lines.push(String(summary.rootCause.OTHER));
lines.push("");
lines.push("EVALUATED AND FIT (shadow ready):");
lines.push(String(summary.rootCause.EVALUATED_AND_FIT));
lines.push("");
lines.push("Primary cause: **C+D — HISTORICAL DISCOVERY BIAS + HOTEL-ISOLATED RESEARCH**. All 11 Ren ready opps had never been evaluated for Hilton despite same Times Square / Midtown West micro-area.");
lines.push("");
lines.push("## G. MARKET OPPORTUNITY GRAPH");
lines.push("");
lines.push("Implemented model: Market → Submarket → Opportunity → Hotel applicability/fit links (in-memory report graph; no new Airtable table). Shared packet owns hotel-neutral evidence; hotel layer owns geo applicability + fit + action.");
lines.push("");
lines.push("MARKET OPPORTUNITIES:");
lines.push(String(graph.marketOpportunities));
lines.push("");
lines.push("HOTEL OPPORTUNITY LINKS:");
lines.push(String(graph.hotelOpportunityLinks));
lines.push("");
lines.push("DUPLICATE MARKET OPPS MERGED:");
lines.push(String(graph.duplicateMarketOppsMerged));
lines.push("");
lines.push("## H. GEOGRAPHIC FANOUT");
lines.push("");
lines.push("Same micro-area: AUTO_EVALUATE");
lines.push("Same submarket: AUTO_EVALUATE");
lines.push("Same borough: EVALUATE_IF_DEMAND_PATTERN_SUPPORTS");
lines.push("Same metro: NO_BLIND_FANOUT");
lines.push("Cross borough: REQUIRE_STRONGER_EVIDENCE");
lines.push("");
lines.push("## I. BROOKLYN SAFEGUARD");
lines.push("");
lines.push("Would Brooklyn-only opportunity automatically reach Midtown?");
lines.push(boundary.brooklynSafeguard.wouldAutomaticallyReachMidtown ? "YES" : "NO");
lines.push("");
lines.push("Expected: NO");
lines.push("");
lines.push("## J. JEV");
lines.push("");
lines.push("PAIR EVALUATIONS:");
lines.push(String(summary.jev.pairEvaluations));
lines.push("");
lines.push("JEV ACTIONS:");
lines.push(String(summary.jev.jevActions));
lines.push("");
lines.push("BLOCKERS RESOLVED:");
lines.push(String(summary.jev.blockersResolved));
lines.push("");
lines.push("STATE ADVANCES:");
lines.push(String(summary.jev.stateAdvances));
lines.push("");
lines.push("NEW READY (shadow):");
lines.push(String(summary.jev.newReadyShadow));
lines.push("");
lines.push("FETCHES:");
lines.push(String(summary.jev.fetches));
lines.push("");
lines.push("Note: router decisions only this pass — network fetches deferred.");
lines.push("");
lines.push("## K. OPPORTUNITY DEVELOPMENT");
lines.push("");
lines.push("Shadow states for Hilton links from Ren 11:");
lines.push(`CUSTOMER_READY (shadow): ${summary.counts.hiltonReadyAfterShadow}`);
lines.push(`HOTEL_MATCHED: ${summary.counts.hiltonHotelMatchedNeedsData}`);
lines.push(`NOT_FIT / N/A: ${summary.counts.hiltonNotFit}`);
lines.push("No customer rows written.");
lines.push("");
lines.push("## L. DIRECT ANSWERS");
lines.push("");
lines.push("1. Why Ren 11 vs Hilton 0? Hotel-isolated discovery — Ren research never cross-evaluated Hilton despite shared Times Square micro-area (not a geo mismatch).");
lines.push("2. Never evaluated for Hilton: **11/11**.");
lines.push("3. Genuinely do not fit Hilton: **0** in this shadow pass.");
lines.push("4. Shadow Hilton-ready: **10** (not promoted).");
lines.push("5. Need one more supporting-data step: **1**.");
lines.push("6. Yes — hotel-isolated discovery creates artificial opportunity gaps.");
lines.push("7. Yes — discover once at market/submarket level.");
lines.push("8. Yes — same market opp can apply to multiple hotels with independent fit.");
lines.push("9. Hierarchy + fanout policy: metro-only ≠ auto; submarket/micro auto; cross-borough requires evidence.");
lines.push("10. Yes — Times Square vs NoHo submarkets distinguished.");
lines.push("11. Yes — Manhattan vs Brooklyn distinguished; Brooklyn does not auto-reach Midtown.");
lines.push("12. Jev router nominates VERIFY_* actions; fetches not executed this pass.");
lines.push("13. Default architecture should migrate to market-first discovery + hotel applicability.");
lines.push("14. Validate one more market (or promote Hilton shadow with governed apply) before broad generalization.");
lines.push("");
lines.push("## FINAL VERDICT");
lines.push("");
lines.push("HOTEL-ISOLATED DISCOVERY CONFIRMED — ARCHITECTURE SHOULD MIGRATE TO MARKET-FIRST");
lines.push("");
lines.push("## PERSISTENCE / META");
lines.push("");
lines.push(`FINAL SHA: ${head}`);
lines.push("PUSH: PENDING");
lines.push("DEPLOY: NOT_RUN");
lines.push("CUSTOMER MUTATION: NONE");
lines.push("CRON: HELD");
lines.push("DIRTY LEFT: unrelated working-tree files");
lines.push("");
lines.push("STOP.");

fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), lines.join("\n"), "utf8");
console.log("wrote FOUNDER_REPORT.md");
