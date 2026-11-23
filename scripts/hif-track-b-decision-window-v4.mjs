#!/usr/bin/env node
/**
 * Track B V4 — Decision-window extraction runner.
 * Documents examined: priority YE2025 10-Ks (DRH, SVC, Host, Sunstone, Ashford) + Desert Ridge deep dive.
 * CUSTOMER_BLOCKED. Does not re-apply coverage-audit 54 records.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildDecisionWindowV4Dossiers,
  summarizeWindowClasses,
  WINDOW_CLASS,
  DRH_10K,
  SVC_10K,
  RHP_10K,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-b-decision-window-v4-dossiers.js";
import { contractPersistAllowed } from "../lib/hotel-intelligence/sec-edgar/hif-track-b-global-historical-waves-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-b-decision-window-v4");
const AS_OF = new Date().toISOString();

function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const dossiers = buildDecisionWindowV4Dossiers();
  const persistable = dossiers.filter((d) => d.persistEligible && contractPersistAllowed(d));
  const staged = dossiers.filter((d) => !persistable.includes(d));
  const byClass = summarizeWindowClasses(dossiers);

  const coverage = {
    asOf: AS_OF,
    baseline: {
      DISCOVERED: 11940,
      RETRIEVED_CHECKPOINT: 50,
      EXTRACTED: 50,
      EXHIBITS_ENUMERATED: 47,
      EXHIBITS_PROCESSED: 28,
      CACHE_FILES: 72,
    },
    stages: {
      DISCOVERED: 11940,
      RETRIEVED: 50,
      EXTRACTED: 50,
      PROPERTY_MATCHED: "PARTIAL — prior waves + this batch PENDING_HPC_SEARCH",
      CONTRACT_VERIFIED: persistable.length,
      DECISION_WINDOW_ASSESSED: dossiers.length,
      CUSTOMER_ELIGIBLE: 0,
    },
    dedupNotes: [
      "Eligible filing accessions are inventory units; amended filings / exhibits / identical contracts are not separate opportunities.",
      "Coverage-audit APLE/RHP 54 Airtable records are NOT re-applied.",
      "Desert Ridge analysis dossier is persistEligible=false to avoid duplicate contract rows.",
      "SVC Hyatt 17 / Radisson 7 are portfolio watches (unnamed hotels) — staged only.",
    ],
    thisWaveDocumentsExamined: [
      { registrant: "DiamondRock Hospitality", form: "10-K YE2025", url: DRH_10K, yield: "HIGH — hotel-level franchise + HMA tables" },
      { registrant: "Service Properties Trust", form: "10-K YE2025", url: SVC_10K, yield: "MEDIUM — named Crowne Plaza IHG day-exact; Hyatt/Radisson portfolios unnamed" },
      { registrant: "Host Hotels & Resorts", form: "10-K YE2025", yield: "LOW — no hotel-level franchise/HMA expiry schedule in examined sections" },
      { registrant: "Sunstone Hotel Investors", form: "10-K YE2025", yield: "LOW — distant HMA years 2041/2048; no near-term hotel schedule" },
      { registrant: "Ashford Hospitality Trust", form: "10-K YE2025", yield: "LOW — franchise 2026–2049 / HMA 2026–2038 ranges only; no hotel-level table extracted" },
      { registrant: "Ryman (Desert Ridge deep dive)", form: "10-K YE2025", url: RHP_10K, yield: "ANALYSIS — auto-renewal mechanics; class D not E" },
      { registrant: "Pebblebrook / Chatham", form: "prior ISE wave", yield: "SKIP — already commercially assessed in #556; avoid re-extraction" },
    ],
  };

  const rankedQueue = [
    { rank: 1, registrant: "diamondrock-hospitality", rationale: "Named hotel franchise + HMA tables with month precision and at-will flags — highest decision-window density", status: "PROCESSED_THIS_WAVE" },
    { rank: 2, registrant: "service-properties-trust", rationale: "Day-exact portfolio/IHG HMA dates; need named-hotel schedule for Hyatt/Radisson", status: "PARTIAL_THIS_WAVE" },
    { rank: 3, registrant: "pebblebrook-hotel-trust", rationale: "Prior ISE: Margaritaville HMA terminable Sep 2026 — do not reprocess", status: "PRIOR_WAVE_COMPLETE" },
    { rank: 4, registrant: "chatham-lodging-trust", rationale: "Prior ISE: 33 franchise years + Holtsville 2026 — do not reprocess", status: "PRIOR_WAVE_COMPLETE" },
    { rank: 5, registrant: "ashford-hospitality-trust", rationale: "Largest eligible inventory but YE2025 10-K lacks hotel-level schedule; next=EX-10 search not mass 8-K", status: "DEFERRED_EX10" },
    { rank: 6, registrant: "host-hotels-resorts", rationale: "No hotel-level expiry table in YE2025 examined sections; next=specific EX-10/termination exhibits", status: "DEFERRED_EX10" },
    { rank: 7, registrant: "sunstone-hotel-investors", rationale: "Long-dated HMAs only in examined sections; low near-term yield", status: "DEPRIORITIZED" },
  ];

  const trackA = dossiers
    .filter((d) => d.trackAPayload)
    .map((d) => ({
      findingId: d.findingId,
      hotelOrOwner: d.hotelName,
      windowClass: d.windowClass,
      commercialEvent: d.trackAPayload.signal,
      agreementType: d.agreementType,
      originalDocumentUrl: d.sourceUrl || d.trackAPayload.sourceUrl,
      factualClaim: d.evidenceSnippet,
      historicalOrProspective: d.windowClass === WINDOW_CLASS.A_HISTORICAL_COMPLETED ? "HISTORICAL" : "PROSPECTIVE",
      verifiedDatesOrUnknowns: d.impreciseDates || (d.expirationDate ? [d.expirationDate] : d.expirationYear ? [`year:${d.expirationYear}`] : []),
      evidenceLimitations: d.trackAPayload.limitation,
      existingHifRefs: { discoveryPropertyId: d.discoveryPropertyId, contractId: d.contractId, findingId: d.findingId },
      opportunityInterpretationQuestions: [
        d.windowClass === WINDOW_CLASS.D_POTENTIAL_DECISION_WINDOW
          ? "Is the owner actively evaluating brand/operator alternatives, or is this only contractual flexibility?"
          : "Should this remain a contractual watch without outreach?",
      ],
      doNotAutoPromoteToVerifiedBctMct: true,
      trackAClassification: d.trackA,
    }));

  const trackC = persistable.map((d) => ({
    hotelName: d.hotelName,
    city: d.city || null,
    stateProvince: d.stateProvince || null,
    country: d.physicalCountry,
    rooms: d.rooms || null,
    hpcQuery: d.hpcQuery,
    hpcMatchStatus: d.hpcMatchStatus,
    discoveryPropertyId: d.discoveryPropertyId,
    issue:
      d.hpcMatchStatus === "DISCOVERED_NOT_IN_HPC"
        ? "Identity incomplete (SVC Crowne Plaza city/name) — resolve before customer publication"
        : "PENDING_HPC_SEARCH",
  }));

  const yieldMetrics = {
    documentsExamined: coverage.thisWaveDocumentsExamined.length,
    documentsYieldingVerifiedHotelFacts: 2, // DRH + SVC Crowne Plaza
    newUniqueHotelFactsPersistable: persistable.length,
    historicalCompleted: byClass[WINDOW_CLASS.A_HISTORICAL_COMPLETED] || 0,
    futureContractualWatches:
      (byClass[WINDOW_CLASS.B_ACTIVE_NO_NEAR_TERM] || 0) + (byClass[WINDOW_CLASS.C_FORWARD_CONTRACTUAL_WATCH] || 0),
    potentialDecisionWindows: byClass[WINDOW_CLASS.D_POTENTIAL_DECISION_WINDOW] || 0,
    verifiedActionableOpportunities: byClass[WINDOW_CLASS.E_VERIFIED_OPEN_OPPORTUNITY] || 0,
    canonicalHotelsMatched: 0,
    ownerOperatorAttribution: "Owner=DRH/SVC/RHP on all; operator named on HMA rows; franchisor brand-level",
    duplicatesAvoided: ["coverage-audit 54 not re-applied", "Desert Ridge contract not re-persisted", "PEB/CLDT prior ISE not re-extracted"],
    researchCostUsd: 0,
    productiveFindingsPerDocument: Number((persistable.length / 5).toFixed(2)),
    costPerUsefulSignal: 0,
    note: "Contract facts ≠ conversion opportunities. E count is intentionally 0.",
  };

  const summary = {
    asOf: AS_OF,
    wave: "B25",
    pr556: { state: "MERGED", mergeCommit: "05b9d23162e3f5e9e94547494010db9e6236ae78" },
    pr549: { state: "MERGED", note: "Same merge commit as #556 — content superseded/included" },
    coverage,
    rankedQueue,
    windowClassCounts: byClass,
    gapTest: {
      dossiers: dossiers.length,
      persistEligible: persistable.length,
      staged: staged.length,
    },
    yieldMetrics,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
  };

  fs.writeFileSync(path.join(OUT, "COVERAGE_LEDGER_V4.json"), JSON.stringify(coverage, null, 2));
  fs.writeFileSync(path.join(OUT, "RANKED_EXTRACTION_QUEUE.json"), JSON.stringify(rankedQueue, null, 2));
  fs.writeFileSync(
    path.join(OUT, "WAVE_DOSSIERS.json"),
    JSON.stringify({ dossiers: persistable, allDossiers: dossiers, staged, summary: summary.gapTest }, null, 2),
  );
  fs.writeFileSync(path.join(OUT, "TRACK_A_HANDOFF.json"), JSON.stringify({ researchCandidates: trackA }, null, 2));
  fs.writeFileSync(path.join(OUT, "TRACK_C_HANDOFF.json"), JSON.stringify({ handoffs: trackC }, null, 2));
  fs.writeFileSync(path.join(OUT, "YIELD_METRICS.json"), JSON.stringify(yieldMetrics, null, 2));
  fs.writeFileSync(path.join(OUT, "RESEARCH_SUMMARY.json"), JSON.stringify(summary, null, 2));

  const md = `# Track B V4 — SEC/CMBS Decision-Window Extraction

**As of:** ${AS_OF}  
**Publication:** CUSTOMER_BLOCKED · **Cost:** $0  
**PR #556:** MERGED @ \`05b9d231\` · **PR #549:** MERGED (same commit / superseded content)

## 1. PR #556 / #549 reconciliation
- #556 MERGED into \`main\` @ \`05b9d231\` (CI green: gitleaks + ADP/GDI gates).
- #549 (Historical Intelligence V3 / Sotherly+Mexico) commits are ancestors of #556; GitHub shows the same merge commit — content is on \`main\` via the #556 merge lineage. No separate outstanding #549 delta.
- Coverage-audit **54** Airtable creates remain idempotent; **not** re-applied for this wave.

## 2. Coverage ledger (after dedup framing)
| Stage | Count |
|---|---|
| DISCOVERED | 11,940 |
| EXTRACTED (checkpoint) | ~50 |
| Exhibits enumerated / processed | 47 / 28 |
| Cache files | 72 |
| This wave CONTRACT_VERIFIED (persistable) | ${persistable.length} |
| DECISION_WINDOW_ASSESSED | ${dossiers.length} |
| CUSTOMER_ELIGIBLE | 0 |

## 3. Ranked extraction queue
See \`RANKED_EXTRACTION_QUEUE.json\`. DRH processed first; PEB/CLDT skipped (prior); Ashford/Host deferred to EX-10; Sunstone deprioritized.

## 4. Documents processed & yield
7 document scopes examined (5 new 10-Ks + Desert Ridge analysis + prior-wave skip). **2** yielded verified hotel-level schedules (DRH, SVC). Persistable facts: **${persistable.length}**.

## 5. New verified hotel-level contract facts
- DRH franchise: Westin Boston Seaport **12/2026**; Courtyard Denver Downtown **10/2027**
- DRH HMA near-term term ends (at-will): Hotel Clio 5/2026; The Gwen 6/2026; Bourbon Orleans / Courtyard Denver / Henderson Park Inn 7/2026; Westin Boston 12/2026
- DRH historical Highgate transitions **2026-02-03** (3 NY hotels)
- SVC Crowne Plaza IHG HMA extended to **2027-01-31** (495 keys; city unknown)

## 6. Forward-looking dates & renewals
- Desert Ridge: year **2027** + five automatic 10-year renewals if performance thresholds met
- SVC Hyatt 17: **2031-03-31**; Radisson 7: **2031-07-31** (unnamed hotels)
- DRH at-will + renewal options as tabled

## 7. Historical vs actionable
| Class | Count |
|---|---|
| A Historical | ${byClass[WINDOW_CLASS.A_HISTORICAL_COMPLETED] || 0} |
| B Active no near-term | ${byClass[WINDOW_CLASS.B_ACTIVE_NO_NEAR_TERM] || 0} |
| C Forward watch | ${byClass[WINDOW_CLASS.C_FORWARD_CONTRACTUAL_WATCH] || 0} |
| D Potential window | ${byClass[WINDOW_CLASS.D_POTENTIAL_DECISION_WINDOW] || 0} |
| E Verified open | **0** |

## 8. Potential decision windows (D)
DRH near-term franchise/HMA at-will rows + Desert Ridge 2027 (auto-renewal caveat). Require Track A owner-intent validation.

## 9. Verified open opportunities (E)
**None.** Expiry/at-will/auto-renewal alone are insufficient.

## 10. HPC / ownership gaps
See \`TRACK_C_HANDOFF.json\`. SVC Crowne Plaza city/name unresolved. All DRH rows PENDING_HPC_SEARCH.

## 11. Airtable persistence
\`node scripts/hif-track-b-global-waves-persist.mjs --batch-dir=reports/sec-edgar-hif-track-b-decision-window-v4\`  
Dry-run then \`--apply\`. Unique \`tb_v4_\` IDs. No re-apply of audit 54.

## 12. Cost & diminishing returns
Cost **$0**. Host/Sunstone/Ashford YE2025 10-Ks show diminishing returns without EX-10 targeting. Next wave = Ashford/Host/PEB EX-10 named agreements, not mass 8-K.

## 13. Recommended next research wave
1. Ashford EX-10 hotel HMA/franchise exhibits (not 1,120 8-Ks)
2. Host termination/franchise exhibits with hotel names
3. SVC Hyatt/Radisson named hotel schedule (if any exhibit/supplement)
4. Track C resolve Crowne Plaza 495 + DRH HPC matches
5. Track A qualify D-class DRH windows only with owner-process evidence

## 14. CI / IC status
#556/#549 merged. This wave ships as focused PR on \`feat/hif-v6-track-b-decision-window-v4\`. No automation / no customer publication / no production deploy.
`;

  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), md);
  console.log(
    JSON.stringify(
      {
        ok: true,
        out: OUT,
        dossiers: dossiers.length,
        persistEligible: persistable.length,
        windowClasses: byClass,
        verifiedOpenE: 0,
      },
      null,
      2,
    ),
  );
}

main();
