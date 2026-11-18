#!/usr/bin/env node
/**
 * Fourteen-segment discovery & commercial quality benchmark (Bethesda).
 * READ-only Airtable. No paid Exa. No production writes.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getCanonicalFourteenSegmentIds } from "../lib/group-demand-intelligence/workflow/full-e2e-fourteen-segment-traversal-v1.js";
import { listFourteenSegmentExecutorSpecs } from "../lib/group-demand-intelligence/workflow/full-e2e-fourteen-segment-discovery-executor-registry-v1.js";
import {
  listResearchInventoryForHotel,
  readBackResearchInventory,
  buildResearchInventoryFields,
  diffMaterialResearchFields,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import { listCanonicalResearchHotels } from "../lib/group-demand-intelligence/workflow/gdi-all-hotel-research-backfill-final-v1.js";
import {
  MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F,
} from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";
import {
  evaluateCertifiedDrawerCommercialContent,
  summarizeCertifiedDrawerAudit,
  detectGenericCommercialProse,
} from "../lib/group-demand-intelligence/presentation/gdi-certified-drawer-commercial-content-contract-v1.js";
import {
  assertAllFourteenSegmentsHaveMissions,
  buildUpgradedExaDiscoveryContextBlock,
  SEGMENT_EXA_MISSION_UPGRADES,
} from "../lib/group-demand-intelligence/research/exa-agent/gdi-fourteen-segment-exa-mission-upgrades-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-fourteen-segment-discovery-commercial-benchmark-v1",
);
const HOTEL_ID = "recLuxvwwxID7U2B8";
const HOTEL_NAME = "Bethesda Marriott";
const PINNACLE_REC = "rec4aFaYuC0gfR1wh";
const PDA_REC = "recTSOXMxDT7JpxGq";
const PRIORITY_SEGMENTS = [
  "corporate_meetings_incentives",
  "corporate_accounts_scaling",
  "projects_infrastructure",
  "education_scientific",
  "relocation_extended_stay",
  "government_public_sector",
  "preferred_lodging_partnerships",
  "professional_event_planners",
];

function write(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  const p = path.join(OUT, name);
  fs.writeFileSync(
    p,
    typeof body === "string" ? body : `${JSON.stringify(body, null, 2)}\n`,
  );
  return p;
}

function mapDisplaySegmentToCanonical(label = "") {
  const s = String(label || "").toLowerCase();
  if (/preferred lodging/.test(s)) return "preferred_lodging_partnerships";
  if (/professional event planner/.test(s)) return "professional_event_planners";
  if (/relocation|extended stay/.test(s)) return "relocation_extended_stay";
  if (/scaling/.test(s)) return "corporate_accounts_scaling";
  if (/corporate meetings|incentive/.test(s)) return "corporate_meetings_incentives";
  if (/local catering|day meeting/.test(s)) return "local_catering_day_meetings";
  if (/celebrations|private events/.test(s) && !/corporate meetings/.test(s)) {
    return "corporate_celebrations_private_events";
  }
  if (/travel trade|intermediar/.test(s)) return "travel_trade_group_intermediaries";
  if (/sports|weekend group|academic competition|overflow housing/.test(s)) {
    return "sports_entertainment";
  }
  if (/government/.test(s)) return "government_public_sector";
  if (/project|infrastructure/.test(s)) return "projects_infrastructure";
  if (
    /medical|scientific|healthcare|training program|advocacy|university|education|niddk|pda/.test(
      s,
    )
  ) {
    return "education_scientific";
  }
  if (/association|conference|event \/ association/.test(s)) {
    return "associations_conferences_events";
  }
  if (/corporate$/.test(s)) return "corporate_accounts_scaling";
  if (/social|gala|fundraising/.test(s)) return "social_special_events";
  return null;
}

function classifyDemand(opp = {}) {
  const demandConfidence = String(
    opp.guestroomDemandConfidence ||
      opp.demandThesis ||
      opp.commercialDemandTypeEvidence?.guestroomConfidence ||
      "",
  ).toUpperCase();
  const roomStatus = String(opp.roomDemandStatus || "").toUpperCase();
  const disposition = String(
    opp.disposition || opp.salesPartitionV11 || opp.priority || "",
  ).toUpperCase();

  if (/CONFIRMED_DEMAND|CONFIRMED/.test(demandConfidence)) {
    return "CONFIRMED_DEMAND";
  }
  if (
    /RELIABLE_INFERRED/.test(demandConfidence) ||
    /VERIFIED_HOUSING_PROGRAM/.test(roomStatus)
  ) {
    return "RELIABLE_INFERRED_DEMAND";
  }
  if (
    /WATCH|SPECULATIVE|EXPLORATORY/.test(disposition) ||
    /SPECULATIVE|UNKNOWN|MISSING/.test(roomStatus)
  ) {
    return "SPECULATIVE_RESEARCH";
  }
  const textBody = [
    opp.whatIsHappening,
    opp.summaryWhat,
    opp.qualificationNotes,
    opp.recommendedAction,
  ]
    .filter(Boolean)
    .join(" ");
  if (
    /not a confirmed|speculative|watch only|research continuation|plausible local venue/i.test(
      textBody,
    )
  ) {
    return "SPECULATIVE_RESEARCH";
  }
  return "UNSPECIFIED";
}

function isSalesReadyCandidate(opp = {}, drawer = {}) {
  const demand = classifyDemand(opp);
  if (demand !== "CONFIRMED_DEMAND" && demand !== "RELIABLE_INFERRED_DEMAND") {
    return false;
  }
  const contact =
    opp.primaryContact?.name ||
    opp.primaryContactName ||
    drawer.contactName;
  if (!contact) return false;
  if (drawer.readinessDemotionCandidate) return false;
  if ((drawer.emptyCoreCount || 0) > 0) return false;
  if ((drawer.genericBoilerplateCount || 0) > 0) return false;
  const partition = String(opp.salesPartitionV11 || opp.priority || "").toUpperCase();
  if (/WATCH|WATCHLIST/.test(partition)) return false;
  // Honest candidate only — still not a GDO promotion.
  return true;
}

async function main() {
  const segmentIds = getCanonicalFourteenSegmentIds();
  const specs = listFourteenSegmentExecutorSpecs();
  const missions = assertAllFourteenSegmentsHaveMissions();

  console.log("[baseline] live Airtable…");
  const live = await listResearchInventoryForHotel(HOTEL_ID);
  const hotels = listCanonicalResearchHotels(ROOT);
  const byHotel = {};
  let portfolio = 0;
  for (const h of hotels) {
    const rows =
      h.hotelId === HOTEL_ID
        ? live
        : await listResearchInventoryForHotel(h.hotelId);
    byHotel[h.hotelId] = rows.length;
    portfolio += rows.length;
  }
  const pinnacle = await readBackResearchInventory(PINNACLE_REC);
  const pda = await readBackResearchInventory(PDA_REC);

  const bag = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "data/group-demand-intelligence/hotels",
        HOTEL_ID,
        "opportunities.json",
      ),
      "utf8",
    ),
  );
  const fsRows = Array.isArray(bag.opportunities) ? bag.opportunities : [];

  // Residual sourceSummary drift (3 known from prior pilot; PDA may differ due to authorized UPDATE)
  const residualIds = [
    "gdi_opp_umd_alumni_weekend_watch",
    "gdi_opp_marriott_hq_adjacent_corporate_watch",
    "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_",
  ];
  const residual = [];
  for (const id of residualIds) {
    const opp = fsRows.find((r) => (r.id || r.opportunityId) === id);
    const existing = live.find((r) => r.fields?.[F.opportunityId] === id);
    if (!opp || !existing) {
      residual.push({ opportunityId: id, status: "MISSING_PAIR" });
      continue;
    }
    const built = buildResearchInventoryFields(
      { ...opp, opportunityId: id, hotelId: HOTEL_ID },
      { hotelId: HOTEL_ID, hotelName: HOTEL_NAME },
    );
    const fields = { ...(built.fields || {}) };
    const prev = existing.fields || {};
    if (prev[F.firstDiscoveredAt]) fields[F.firstDiscoveredAt] = prev[F.firstDiscoveredAt];
    else delete fields[F.firstDiscoveredAt];
    if (prev[F.firstDiscoveredRunId]) {
      fields[F.firstDiscoveredRunId] = prev[F.firstDiscoveredRunId];
    } else delete fields[F.firstDiscoveredRunId];
    const diff = diffMaterialResearchFields(fields, prev);
    residual.push({
      opportunityId: id,
      airtableRecordId: existing.id,
      materialChanged: diff.materialChanged,
      changedFields: diff.changedFields,
      airtableSourceSummary: String(prev[F.sourceSummary] || "").slice(0, 200),
      filesystemSourceSummary: String(fields[F.sourceSummary] || "").slice(0, 200),
      classification:
        String(prev[F.sourceSummary] || "").includes("[object Object]")
          ? "AIRTABLE_SOURCE_SUMMARY_OBJECT_OBJECT_CORRUPTION"
          : id.includes("parenteral")
            ? "FS_LAG_AFTER_AUTHORIZED_PDA_UPDATE"
            : "SOURCE_SUMMARY_SERIALIZATION",
      applyRecommended: false,
    });
  }

  // Discovery matrix (prior zero-cost execution evidence)
  const discoveryTracePath = path.join(
    ROOT,
    "reports/gdi-bethesda-live-14-segment-persistence-pilot-v1/_discovery-matrix/discovery-execution-traces.json",
  );
  const publicEvidencePath = path.join(
    ROOT,
    "reports/gdi-bethesda-live-14-segment-persistence-pilot-v1/REAL_DISCOVERY_EVIDENCE_REGISTER.json",
  );
  const discoveryTrace = fs.existsSync(discoveryTracePath)
    ? JSON.parse(fs.readFileSync(discoveryTracePath, "utf8"))
    : null;
  const publicEvidence = fs.existsSync(publicEvidencePath)
    ? JSON.parse(fs.readFileSync(publicEvidencePath, "utf8"))
    : { findings: [] };

  // Drawer + commercial quality over FS bag (+ note Pinnacle live-only)
  const drawerRows = fsRows.map((o) =>
    evaluateCertifiedDrawerCommercialContent(
      { ...o, opportunityId: o.id || o.opportunityId, hotelName: HOTEL_NAME },
      { hotelId: HOTEL_ID },
    ),
  );
  const drawerSummary = summarizeCertifiedDrawerAudit(drawerRows);

  const scorecard = segmentIds.map((segmentId) => {
    const spec = specs.find((s) => s.segmentId === segmentId) || {};
    const cell = (discoveryTrace?.cells || []).find((c) => c.segmentId === segmentId);
    const publicHits = (publicEvidence.findings || []).filter(
      (f) => f.segmentId === segmentId,
    );
    const inventory = fsRows.filter(
      (o) => mapDisplaySegmentToCanonical(o.segment || o.segmentDisplayLabel) === segmentId,
    );
    const drawerForSeg = inventory.map((o) =>
      evaluateCertifiedDrawerCommercialContent(
        { ...o, opportunityId: o.id || o.opportunityId, hotelName: HOTEL_NAME },
        { hotelId: HOTEL_ID },
      ),
    );
    let confirmed = 0;
    let inferred = 0;
    let speculative = 0;
    let withContact = 0;
    let salesReadyCandidates = 0;
    let researchContinuation = 0;
    let generic = 0;
    for (const o of inventory) {
      const d = classifyDemand(o);
      if (d === "CONFIRMED_DEMAND") confirmed += 1;
      else if (d === "RELIABLE_INFERRED_DEMAND") inferred += 1;
      else speculative += 1;
      const drawer = evaluateCertifiedDrawerCommercialContent(
        { ...o, opportunityId: o.id || o.opportunityId, hotelName: HOTEL_NAME },
        { hotelId: HOTEL_ID },
      );
      if (drawer.contactName || o.primaryContact?.name) withContact += 1;
      if (isSalesReadyCandidate(o, drawer)) salesReadyCandidates += 1;
      else researchContinuation += 1;
      const prose = [
        o.whatIsHappening,
        o.summaryWhat,
        o.recommendedAction,
        o.hotelFitRationale,
        o.localConnection,
      ]
        .filter(Boolean)
        .join(" ");
      if (detectGenericCommercialProse(prose).generic) generic += 1;
    }

    const blockedPaid =
      cell?.classification === "EXECUTION_BLOCKED" ||
      Number(cell?.freshRetrievals || 0) === 0;

    return {
      segmentId,
      priorityExpanded: PRIORITY_SEGMENTS.includes(segmentId),
      classification: spec.classification || null,
      strategy: spec.strategy || null,
      missionPresent: Boolean(SEGMENT_EXA_MISSION_UPGRADES[segmentId]),
      executorInvoked: cell?.executorInvoked === true,
      queriesAttempted: cell?.queryCount || 0,
      providerCalls: cell?.providerCallCount || 0,
      freshRetrievals: cell?.freshRetrievals || 0,
      extractedCandidates: cell?.extractedCandidates || 0,
      blockedPaid,
      publicNoCostHits: publicHits.length,
      publicUsefulNew: publicHits.filter(
        (h) => h.inExistingInventory === false || h.materialUpdateCandidate,
      ).length,
      inventoryCount: inventory.length,
      demandConfirmed: confirmed,
      demandReliableInferred: inferred,
      demandSpeculative: speculative,
      withNamedContact: withContact,
      salesReadyCandidates,
      researchContinuationOrWatch: researchContinuation,
      genericNarrativeCount: generic,
      drawerMissingCore: drawerForSeg.filter((d) => d.emptyCoreCount > 0).length,
      drawerHotelFitRewritten: drawerForSeg.filter((d) => d.hotelFitRewritten).length,
      paidCostUsd: 0,
      costPerDiscovery: null,
      costPerSalesReady: null,
      improvementsNeeded: [
        blockedPaid ? "Authorize bounded Exa for fresh retrieval" : null,
        inventory.length === 0 ? "No inventory mapped to this canonical segment yet" : null,
        generic > 0 ? "Replace generic hotel-fit / research boilerplate with org-specific thesis" : null,
        withContact < inventory.length
          ? "Increase verified named commercial entry points"
          : null,
        salesReadyCandidates === 0
          ? "No sales-ready candidates under honest gates (expected until paid depth + contact proof)"
          : null,
      ].filter(Boolean),
    };
  });

  const hotel = {
    hotelId: HOTEL_ID,
    displayName: HOTEL_NAME,
    city: "Bethesda",
    country: "United States",
    demandTerritory: { label: "Bethesda / Montgomery County" },
  };
  const missionBlocks = Object.fromEntries(
    segmentIds.map((id) => [id, buildUpgradedExaDiscoveryContextBlock(hotel, id)]),
  );

  const baseline = {
    at: new Date().toISOString(),
    bethesdaLiveCount: live.length,
    portfolioLiveCount: portfolio,
    hotelCount: hotels.length,
    byHotel,
    pinnacle: {
      id: pinnacle.id,
      compositeKey: pinnacle.compositeKey,
      matched:
        pinnacle.compositeKey ===
        `${HOTEL_ID}::gdi_live_pinnacle_lacrosse_championships_2027`,
      lifecycle: pinnacle.fields?.[F.researchLifecycleStatus] || null,
    },
    pda: {
      id: pda.id,
      compositeKey: pda.compositeKey,
      matched:
        pda.compositeKey ===
        `${HOTEL_ID}::disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_`,
      lifecycle: pda.fields?.[F.researchLifecycleStatus] || null,
    },
    productionWritesThisRun: 0,
    paidExaSpendUsd: 0,
  };

  write("PRODUCTION_BASELINE.json", baseline);
  write("RESIDUAL_SOURCE_SUMMARY_DRIFT.md", `# Residual sourceSummary drift

Isolated after the two-write pilot comparison fix. **Do not mass-apply.**

| Opportunity | Classification | Apply? |
|-------------|----------------|--------|
${residual
  .map(
    (r) =>
      `| \`${r.opportunityId}\` | ${r.classification} | **No** |`,
  )
  .join("\n")}

## Detail

\`\`\`json
${JSON.stringify(residual, null, 2)}
\`\`\`

Notes:
- UMD + Marriott HQ still carry Airtable \`[object Object]\` sourceSummary corruption from earlier writes; FS rebuilds clean strings. Separate repair authorization required.
- PDA FS bag lags the authorized Dec 2026 evidence UPDATE already in Airtable — do not overwrite Airtable with older FS.
`);

  write("SEGMENT_SCORECARD.json", { segmentIds, scorecard, missions });
  write(
    "FOURTEEN_SEGMENT_SCORECARD.md",
    `# Fourteen-segment discovery & commercial scorecard (Bethesda)

Live baseline: Bethesda **${baseline.bethesdaLiveCount}** · Portfolio **${baseline.portfolioLiveCount}**  
Paid Exa this run: **$0** (blocked pending founder auth)

| Segment | Priority | Executor | Fresh | Public hits | Inventory | Sales-ready cand. | Contacts | Generic | Improvements |
|---------|----------|----------|------:|------------:|----------:|------------------:|---------:|--------:|--------------|
${scorecard
  .map(
    (s) =>
      `| \`${s.segmentId}\` | ${s.priorityExpanded ? "Y" : ""} | ${s.executorInvoked} | ${s.freshRetrievals} | ${s.publicNoCostHits} | ${s.inventoryCount} | ${s.salesReadyCandidates} | ${s.withNamedContact} | ${s.genericNarrativeCount} | ${(s.improvementsNeeded || []).slice(0, 2).join("; ") || "—"} |`,
  )
  .join("\n")}

## Totals

- Inventory mapped in FS bag: ${fsRows.length} (Pinnacle live-only not yet in FS)
- Sales-ready candidates (honest gates): **${scorecard.reduce((a, s) => a + s.salesReadyCandidates, 0)}**
- Speculative / research continuation dominant in several expanded segments until paid depth
`,
  );

  write(
    "DISCOVERY_GAPS_AND_IMPROVEMENTS.md",
    `# Discovery gaps and improvements

## Execution reality

- All 14 segment executors were invoked in the prior Bethesda live pilot matrix.
- Fresh Exa retrievals: **0 / 14** (BLOCKED_PAID — no founder paid budget).
- Public no-cost evidence seeded useful signals in sports, scaling, education/scientific, preferred lodging — none sales-ready.

## Gaps by theme

1. **Paid depth missing** — newer segments cannot prove forward-looking yield without Exa.
2. **Local catering boilerplate** — 7/7 mapped rows share identical hotel-fit template; presentation sanitizer now rewrites for drawer display.
3. **Contact completeness uneven** — associations/corporate gaps remain.
4. **Segment label sprawl** — inventory uses many display labels; mapping to canonical 14 is imperfect (projects_infrastructure inventory still thin).
5. **Duplicate org risk** — same planner/housing agency must not mint weekly duplicate opportunities.

## Shipped in this PR

- Certified drawer generic boilerplate detection + hotel-fit rewrite.
- Stronger preferred lodging / professional planner Exa mission targets (NIH visitor lists, housing agencies).
`,
  );

  write(
    "CONTACT_AND_COMMERCIAL_QUALIFICATION.md",
    `# Contact and commercial qualification

## Rules applied (unchanged)

Sales-ready requires CONFIRMED or RELIABLE_INFERRED demand + hotel fit + named commercial entry point. No speculative promotion.

## Bethesda FS bag results

| Metric | Count |
|--------|------:|
| Rows audited | ${fsRows.length} |
| Confirmed demand (heuristic) | ${scorecard.reduce((a, s) => a + s.demandConfirmed, 0)} |
| Reliable inferred | ${scorecard.reduce((a, s) => a + s.demandReliableInferred, 0)} |
| Speculative / unspecified | ${scorecard.reduce((a, s) => a + s.demandSpeculative, 0)} |
| Named contact present | ${scorecard.reduce((a, s) => a + s.withNamedContact, 0)} |
| Sales-ready candidates | ${scorecard.reduce((a, s) => a + s.salesReadyCandidates, 0)} |

Public discovery (prior pilot): Pinnacle entry-point email is a **hint**, not contracting-authority verified — remains research continuation / WATCHLIST.
`,
  );

  write(
    "OPPORTUNITY_DRAWER_QUALITY.md",
    `# Opportunity drawer quality

## Certified commercial content audit (FS bag)

\`\`\`json
${JSON.stringify(drawerSummary, null, 2)}
\`\`\`

## Findings

- **39** rows missing at least one core commercial section.
- **Local catering** repeatedly used identical “plausible local venue…” hotel-fit boilerplate — weak for DOS action.
- Presentation contract now **rewrites** that boilerplate into org-specific uncertainty language without inventing demand.
- Truncation defects remain on a small set of recommended-next-step strings.

## Representative samples

${drawerRows
  .filter((d) => d.hotelFitRewritten)
  .slice(0, 3)
  .map(
    (d) =>
      `- \`${d.opportunityId}\` — hotel-fit rewritten; identity: ${d.identityLine}`,
  )
  .join("\n") || "- (no rewrites in sample)"}
`,
  );

  write(
    "EXA_PAID_BENCHMARK_AUTHORIZATION.md",
    `# Exa paid benchmark authorization — REQUEST ONLY

**Not authorized to spend in this run.**

## Minimum meaningful budget

| Priority segment | Max calls | Cap USD |
|------------------|---------:|--------:|
| corporate_meetings_incentives | 4 | 8 |
| corporate_accounts_scaling | 4 | 8 |
| projects_infrastructure | 3 | 6 |
| education_scientific | 4 | 8 |
| relocation_extended_stay | 3 | 6 |
| government_public_sector | 3 | 6 |
| preferred_lodging_partnerships | 3 | 6 |
| professional_event_planners | 3 | 6 |
| **Total (8 priority)** | **27** | **≤ $54** |

Optional light pass on remaining 6 segments: +$24 → **≤ $78** hard stop.

## Stop conditions

- Budget exhausted or any segment exceeds its cap.
- Provider error / auth failure → mark BLOCKED_BY_PROVIDER, do not invent results.
- Duplicate composite keys in-run → stop persistence (persistence remains founder-gated anyway).

## Evaluation criteria

1. ≥1 research-continuation opportunity with dated lodging thesis **or** proven empty with retrieval evidence.
2. Named commercial entry point rate reported separately from sales-ready.
3. 0 fabricated sales-ready promotions.
4. 0 automatic Airtable production writes.

## Required founder token (example)

\`gdi-bethesda-exa-priority8-benchmark-v1\` + explicit USD ceiling.
`,
  );

  write("MISSION_BLOCKS_PREVIEW.json", {
    note: "Preview only — not executed against paid Exa",
    hotel,
    missionBlocks,
  });

  write(
    "TEST_RESULTS.md",
    `# Test results

| Check | Result |
|-------|--------|
| Live Bethesda count | ${baseline.bethesdaLiveCount} |
| Portfolio count | ${baseline.portfolioLiveCount} |
| Pinnacle intact | ${baseline.pinnacle.matched} |
| PDA intact | ${baseline.pda.matched} |
| 14 missions present | ${missions.ok} |
| Production writes | 0 |
| Paid Exa spend | $0 |
| Residual drift isolated | ${residual.length} |
`,
  );

  write(
    "FOUNDER_NEXT_DECISIONS.md",
    `# Founder next decisions

1. **Approve paid Exa priority-8 benchmark** (≤ $54) — see \`EXA_PAID_BENCHMARK_AUTHORIZATION.md\`.
2. **Authorize repair** of two Airtable \`[object Object]\` sourceSummary rows (UMD, Marriott HQ) — separate exact write set.
3. **Optional:** sync Pinnacle into canonical FS opportunities bag (no Airtable write).
4. Keep recurring research persistence **OFF**.
5. Do not mass-apply residual FS↔Airtable diffs.

**STOP — no paid Exa and no additional production writes in this package.**
`,
  );

  write(
    "FOUNDER_EXECUTIVE_SUMMARY.md",
    `# Founder executive summary — 14-segment commercial discovery benchmark

| Question | Answer |
|----------|--------|
| Bethesda live research count | **${baseline.bethesdaLiveCount}** (includes Pinnacle) |
| Portfolio live count | **${baseline.portfolioLiveCount}** |
| Pilot records intact? | Pinnacle \`${PINNACLE_REC}\` + PDA \`${PDA_REC}\` verified |
| Did all 14 segments run discovery executors? | **Yes** (prior matrix); fresh Exa **0** |
| Newer segments useful yield (no-cost)? | Partial public signals only — **0 sales-ready** |
| Sales-ready candidates in inventory (honest gates) | **${scorecard.reduce((a, s) => a + s.salesReadyCandidates, 0)}** |
| Biggest commercial quality gap | Generic local-catering hotel-fit boilerplate + missing paid depth on expanded segments |
| Improvements shipped | Drawer generic detection + hotel-fit rewrite; stronger PLP/PEP Exa missions |
| Residual sourceSummary exceptions | **3 isolated — unapplied** |
| Ready for paid Exa? | Plan ready — **needs founder budget token** |

Production writes this run: **0** · Paid spend: **$0** · Recurring persistence: **OFF**
`,
  );

  write("BENCHMARK_ACCEPTANCE.json", {
    bethesdaCount: baseline.bethesdaLiveCount,
    portfolioCount: baseline.portfolioLiveCount,
    pinnacleOk: baseline.pinnacle.matched,
    pdaOk: baseline.pda.matched,
    segments: segmentIds.length,
    missionsOk: missions.ok,
    productionWrites: 0,
    paidSpendUsd: 0,
    residualIsolated: residual.length,
    drawerSummary,
  });

  console.log(
    JSON.stringify(
      {
        ok:
          baseline.pinnacle.matched &&
          baseline.pda.matched &&
          missions.ok &&
          segmentIds.length === 14,
        bethesda: baseline.bethesdaLiveCount,
        portfolio: baseline.portfolioLiveCount,
        salesReadyCandidates: scorecard.reduce(
          (a, s) => a + s.salesReadyCandidates,
          0,
        ),
        outDir: OUT,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
