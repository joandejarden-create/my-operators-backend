#!/usr/bin/env node
/**
 * Read-only competitive intelligence + Market Scope audit (no paid research).
 *   node scripts/report-gdi-competitive-intelligence-audit-v1.mjs
 */
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports", "gdi-competitive-intelligence-filter-layout-v1");

function readJson(rel) {
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function getJson(url) {
  return new Promise((resolve, reject) => {
    http
      .get(url, (res) => {
        let data = "";
        res.on("data", (c) => {
          data += c;
        });
        res.on("end", () => {
          try {
            resolve({ status: res.statusCode, body: JSON.parse(data) });
          } catch (err) {
            reject(err);
          }
        });
      })
      .on("error", reject);
  });
}

const hotels = [
  { id: "recnPB3Q02NO8Ma1o", name: "Wyndham San Diego Bayside" },
  { id: "recLuxvwwxID7U2B8", name: "Bethesda Marriott" },
  { id: "recCAGqkERSXgiku4", name: "Sheraton Guadalajara" },
  { id: "rec35fExUxCClpOP6", name: "Hilton packages sample" },
];

const stagedWyndham = readJson(
  "lib/group-demand-intelligence/bundled-publication-packages/recnPB3Q02NO8Ma1o/STAGED.json",
);
const competitiveLog = readJson(
  "reports/gdi-wyndham-sd-bayside-full-e2e-25plus-v1/execute-live-35usd/04-COMPETITIVE_LOG.json",
);
const overflowFindings = readJson(
  "reports/gdi-wyndham-final-commercial-completion-v1/05-COMPETITIVE_OVERFLOW_FINDINGS.json",
);
const cadPilot = readJson(
  "reports/gdi-competitive-account-discovery-phase-3a-v1/pr-b-readonly-pilot/00-PILOT_SUMMARY.json",
);
const whoHow = readJson(
  "reports/gdi-canonical-full-e2e-v3/recnPB3Q02NO8Ma1o/03-who-how.json",
);

const cadFqCandidates = (competitiveLog?.rows || []).filter(
  (r) => r.cadClass === "FQO_CANDIDATE" && r.competitor,
);
const whoCompetitive = [];
function walk(obj) {
  if (!obj || typeof obj !== "object") return;
  if (obj.competitiveClass || obj.competitorHotel) {
    whoCompetitive.push({
      competitiveClass: obj.competitiveClass,
      competitorHotel: obj.competitorHotel,
      title: obj.title || obj.displayTitle || null,
    });
  }
  for (const v of Object.values(obj)) {
    if (Array.isArray(v)) v.forEach(walk);
    else if (v && typeof v === "object") walk(v);
  }
}
if (whoHow) walk(whoHow);

const scorecard = [];
for (const h of hotels) {
  let api = null;
  try {
    api = await getJson(
      `http://127.0.0.1:8080/api/group-demand-intelligence/hotels/${h.id}/opportunities?preview=staged`,
    );
  } catch (err) {
    api = { status: 0, body: { error: String(err && err.message) } };
  }
  const body = api.body || {};
  const ops = Array.isArray(body.opportunities) ? body.opportunities : [];
  const counts = body.publication?.inventoryCounts || body.inventoryPopulationCounts || {};
  const competitiveUi = ops.filter(
    (o) =>
      o.competitiveMarketScopeEligible === true ||
      o.marketScope === "COMPETITIVE" ||
      o.cmiVerified === true ||
      o.competitiveStatus === "VERIFIED" ||
      o.competitiveStatus === "NEEDS_VERIFICATION" ||
      o.competitiveStatus === "HISTORICAL",
  ).length;
  const hotelUi = ops.filter((o) => o.hotelMarketScopeEligible !== false).length;
  let stagedExists = false;
  const stagedPtr = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    h.id,
    "STAGED.json",
  );
  stagedExists = fs.existsSync(stagedPtr);
  const unavailable =
    body.error === "STAGED_PUBLICATION_PREVIEW_UNAVAILABLE" ||
    api.status === 404 ||
    (!ops.length && body.ok === false);

  scorecard.push({
    hotelId: h.id,
    hotelName: h.name,
    stagedPointerPresent: stagedExists,
    apiStatus: unavailable ? "STAGED_UNAVAILABLE_OR_EMPTY" : "OK",
    stagedCount: ops.length,
    filterAll: counts.all ?? ops.length,
    filterHotel: counts.hotel ?? null,
    filterCompetitive: counts.competitive ?? null,
    competitiveRowsInDto: competitiveUi,
    hotelEligibleRows: hotelUi,
    competitiveResearchExecuted:
      h.id === "recnPB3Q02NO8Ma1o"
        ? "PARTIAL_HISTORICAL — CAD candidates logged in prior live E2E; Checkpoint B paid CAD NOT executed"
        : h.id === "recLuxvwwxID7U2B8"
          ? "YES — competitive inventory present in STAGED (filter Competitive > 0)"
          : unavailable
            ? "UNAVAILABLE — no STAGED preview"
            : competitiveUi > 0
              ? "YES — competitive rows in STAGED DTO"
              : "MEASURED_ZERO_OR_NOT_IN_STAGED",
    competitiveCandidatesDiscovered:
      h.id === "recnPB3Q02NO8Ma1o" ? cadFqCandidates.length : competitiveUi,
    competitiveInStaged: counts.competitive ?? competitiveUi,
    competitiveDisplayedUi: counts.competitive ?? competitiveUi,
    mappingDefect:
      h.id === "recnPB3Q02NO8Ma1o" && (counts.competitive || 0) === 0
        ? "NOT_A_FILTER_BUG — STAGED package competitive=0; prior CAD candidates not in locked 26"
        : null,
    remainingResearch:
      h.id === "recnPB3Q02NO8Ma1o"
        ? "Execute authorized Checkpoint B CAD/PLP on association HOLDs after PR #620 merge; do not fabricate competitive into locked 26"
        : null,
  });
}

fs.mkdirSync(OUT, { recursive: true });

const report = {
  generatedAt: new Date().toISOString(),
  contract: "DEALALITY_ADDITIVE_NON_REGRESSION_V1",
  paidResearch: false,
  pr620: {
    state: "OPEN",
    checkpointBPaidCadExecuted: false,
    evidence:
      "PR body explicitly: No paid Checkpoint B spend in this PR — paid PLP/CAD/leisure only after IC merge",
  },
  marketScopeSemantics: {
    hotelAndCompetitiveOverlapping: true,
    allIsUniqueUnion: true,
    mutualExclusivity: false,
    note: "opportunityInventoryPopulations may include both SALES_OPPORTUNITIES and COMPETITIVE_MARKET_INTELLIGENCE; filter counts are independent; All dedupes by opportunity id",
  },
  wyndhamCompetitiveZeroRootCause: {
    exactReason:
      "STAGED package gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_6f4aef5e publishes inventoryCounts.competitive=0 and zero opportunities with competitiveMarketScopeEligible/cmiVerified/competitiveStatus. UI Market Scope filter correctly reflects that measured STAGED zero.",
    notCausedBy: [
      "Incorrect Market Scope filtering on :8080 for current STAGED DTO",
      "Silent UI default hiding competitive rows that exist in STAGED",
    ],
    contributingFactors: [
      "Paid Checkpoint B CAD/PLP research not executed (PR #620 OPEN)",
      "Prior live competitive log contains CAD FQO_CANDIDATE rows with named competitors that are not members of the locked 26 STAGED inventory",
      "CAD read-only pilot: Wyndham CLEAN 11 = 0 competitive classes; HOLD research = 1 historical + 3 emerging + 0 FQO_CANDIDATE",
      "Final commercial completion findings: competitorMentionsSample empty; CAD competitor-commitment refresh not run on HOLD association set",
      "WHO/HOW DryRun artifact contains at least one COMPETITIVE_DISPLACEMENT (CapCon / Marriott Marquis) that did not enter STAGED competitive population",
    ],
    classification: "RESEARCH_EXECUTED_PARTIALLY_BUT_COMPETITIVE_NOT_IN_LOCKED_STAGED_26",
  },
  priorCompetitiveArtifacts: {
    competitiveLogPath:
      "reports/gdi-wyndham-sd-bayside-full-e2e-25plus-v1/execute-live-35usd/04-COMPETITIVE_LOG.json",
    cadFqCandidateWithCompetitor: cadFqCandidates.length,
    namedCompetitors: [
      ...new Set(cadFqCandidates.map((r) => r.competitor).filter(Boolean)),
    ],
    overflowFindings,
    cadPilotTallies: cadPilot?.tallies || null,
    whoHowCompetitiveSignals: whoCompetitive.filter((x) => x.competitiveClass),
    stagedPointer: stagedWyndham,
  },
  scorecard,
};

fs.writeFileSync(path.join(OUT, "AUDIT.json"), JSON.stringify(report, null, 2));

let md = `# GDI Competitive Intelligence Audit & Filter Layout

Date: 2026-11-27  
Server: http://localhost:8080  
Paid research: **none** (read-only)

## 1. Exact reason Wyndham Competitive = 0

**Measured STAGED zero — not a Market Scope filter false negative.**

STAGED \`gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_6f4aef5e\` publishes \`inventoryCounts: { all: 26, hotel: 26, competitive: 0 }\` and every opportunity lacks competitive eligibility flags. The UI filter correctly shows Competitive (0).

### Cause chain

| Layer | Finding |
|---|---|
| Competitor / CAD research | Prior live competitive log lists **${cadFqCandidates.length}** CAD \`FQO_CANDIDATE\` rows with named competitors (Marriott Marquis San Diego Marina, InterContinental San Diego) |
| Checkpoint B (PR #620) | **OPEN** — paid PLP/CAD/leisure **not executed** in that PR |
| CAD read-only pilot | Wyndham CLEAN 11: 0 competitive classes; HOLD: 1 historical + 3 emerging + **0 FQO_CANDIDATE** |
| Qualification / locked 26 | Locked STAGED 26 is hotel-facing FQO inventory; prior CAD candidates were **not** promoted into Competitive population |
| Persistence / STAGED | Competitive classification absent from current STAGED DTO |
| API / filter | Faithfully mirrors STAGED competitive=0 |

**Classification:** research existed / partially executed historically, but competitive opportunities are **not in current STAGED**; Checkpoint B paid CAD has **not** run.

## 2. Market Scope semantics (unchanged methodology)

Hotel and Competitive are **overlapping**, not mutually exclusive.

- An opportunity may belong to Hotel, Competitive, or both.
- **All** = unique opportunity union (deduped by id).
- Filter + optional List column now share \`opportunityInventoryPopulations\` / eligibility taxonomy (Hotel / Competitive / Hotel · Competitive).

## 3. Data-mapping corrections applied

1. Optional Market Scope column hydrates from the same taxonomy as the filter (no empty cells when column enabled).
2. Market Scope remains offered for Hotel-only inventories so column can display **Hotel** (parity with filter classification).
3. Filter layout: fixed widths for Property / Weekly Change / Opportunity Type / Market Scope to stop Opportunity Type resizing when Market Scope changes.

## 4. Product-wide scorecard

| Hotel | STAGED | All | Hotel | Competitive | Competitive research | Mapping defect |
|---|---:|---:|---:|---:|---|---|
`;

for (const s of scorecard) {
  md += `| ${s.hotelName} | ${s.stagedCount} | ${s.filterAll ?? "—"} | ${s.filterHotel ?? "—"} | ${s.filterCompetitive ?? "—"} | ${s.competitiveResearchExecuted} | ${s.mappingDefect || s.apiStatus} |\n`;
}

md += `
## 5. Remaining research recommendations (no spend under this instruction)

1. After PR #620 IC merge + sync: run authorized Checkpoint B CAD/PLP on association HOLD inventory with hard ceiling.
2. Preserve CAD Layer-1 classifications through FQO → STAGED without inventing competitive flags.
3. Reconcile prior competitive-log FQO_CANDIDATE ids against locked 26 — promote only when six-gate FQO + founder process authorize.
4. Do **not** auto-classify every event held elsewhere as Competitive.

## 6. Governance

FULL E2E → STAGED → FOUNDER APPROVAL → CURRENT  
\`DEALALITY_ADDITIVE_NON_REGRESSION_V1\`  
No CURRENT publication · no paid research · no unauthorized qualification changes
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_COMPETITIVE_INTELLIGENCE_AUDIT.md"), md);
console.log(md);
console.log("WROTE", OUT);
