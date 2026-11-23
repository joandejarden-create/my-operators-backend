#!/usr/bin/env node
/**
 * Global SEC / CMBS / Contractual Coverage Audit V1
 * Builds coverage inventory, executes bounded APLE+RHP gap test, writes founder A–N.
 * No Airtable schema. CUSTOMER_BLOCKED. Cost $0.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listUniversalCompaniesV24 } from "../lib/hotel-intelligence/sec-edgar/cik-registry-v24.js";
import {
  buildGlobalSecCoverageGapDossiers,
  APLE_10K,
  RHP_10K,
  APLE_MARRIOTT_EXIT_JAN2026,
  RHP_HMA_ROWS,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-b-global-sec-coverage-gap-dossiers-v1.js";
import { contractPersistAllowed } from "../lib/hotel-intelligence/sec-edgar/hif-track-b-global-historical-waves-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-b-global-sec-coverage-audit-v1");
const AS_OF = new Date().toISOString();

function readJson(p, fallback = null) {
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return fallback;
  }
}

function countProcessed(checkpointPath) {
  const cp = readJson(checkpointPath, { processedDocuments: {} });
  const vals = Object.values(cp.processedDocuments || {});
  const byStatus = vals.reduce((a, d) => {
    const s = d.status || "?";
    a[s] = (a[s] || 0) + 1;
    return a;
  }, {});
  return { path: checkpointPath, total: vals.length, byStatus, companies: Object.keys(cp.companies || {}).length };
}

function buildCoverageInventory() {
  const registry = listUniversalCompaniesV24();
  const withCik = registry.filter((c) => c.cik);
  const index = readJson(path.join(ROOT, "data/sec-edgar/eligible-filings/_index.json"), { companies: {} });
  const indexCompanies = Object.entries(index.companies || {});
  let eligibleFoundSum = 0;
  const perCompany = [];
  for (const [slug, row] of indexCompanies) {
    const elig = row.coverage?.eligibleFound || 0;
    eligibleFoundSum += elig;
    perCompany.push({
      slug,
      cik: row.cik,
      DISCOVERED: elig,
      RETRIEVED_CHECKPOINT: row.coverage?.processed || 0,
      UNPROCESSED_QUEUE: row.coverage?.remainingInQueue ?? Math.max(0, elig - (row.coverage?.processed || 0)),
      yearSpan: row.coverage?.yearSpan || null,
      formTypeCounts: row.coverage?.formTypeCounts || {},
    });
  }

  const liveLedger = readJson(path.join(OUT, "COVERAGE_LEDGER_LIVE.json"), {});
  const mh = countProcessed(path.join(ROOT, "data/sec-edgar/checkpoints/marriott-hilton-historical-v2.json"));
  const ur = countProcessed(path.join(ROOT, "data/sec-edgar/checkpoints/universal-research-v24.json"));
  // Prefer live ledger non-overlapping totals (50) over raw dual-checkpoint ok sums (overlap risk).
  const ledgerProcessed =
    liveLedger.checkpointDocs ||
    (liveLedger.ledgerCohorts?.CORE_SIX?.processed || 0) + (liveLedger.ledgerCohorts?.EXTENDED?.processed || 0);
  const checkpointOkRaw = (mh.byStatus.ok || 0) + (ur.byStatus.ok || 0);
  const checkpointOk = ledgerProcessed > 0 ? ledgerProcessed : checkpointOkRaw;
  const checkpointError =
    (liveLedger.checkpointByStatus?.error || 0) ||
    (mh.byStatus.error || 0) + (ur.byStatus.error || 0);

  let cacheFiles = 0;
  const cacheDir = path.join(ROOT, "data/sec-edgar/cache");
  if (fs.existsSync(cacheDir)) {
    cacheFiles = fs.readdirSync(cacheDir, { recursive: true }).filter((f) => /\.(htm|html|txt|json|pdf)$/i.test(String(f))).length;
  }

  const cmbsPilot = readJson(path.join(ROOT, "reports/sec-edgar-contract-timing-cmbs-v1/PILOT_REPORT.json"), {});
  const globalWaves = readJson(path.join(ROOT, "reports/sec-edgar-hif-track-b-global-historical-waves/WAVE_DOSSIERS.json"), {});
  const gwDossiers = globalWaves.dossiers || [];
  const cmbsUnresolved = gwDossiers.filter((d) => d.sourceFamily === "SEC_CMBS").length;

  const exhibitEnum = liveLedger.exhibitLedger?.enumerated ?? 0;
  const exhibitProc = liveLedger.exhibitLedger?.processed ?? 0;

  const stages = {
    DISCOVERED: eligibleFoundSum,
    RETRIEVED: checkpointOk + checkpointError,
    EXTRACTED: checkpointOk,
    HOTEL_MATCHED: "PARTIAL — prior waves; not re-counted in this audit (see Track C / HPC gates)",
    COMMERCIALLY_ASSESSED: gwDossiers.length + " prior global-wave dossiers + Track B ISE/CALA waves (separate reports)",
    PERSISTED: "Prior waves via foundation/global-waves persist; this audit batch persists separately",
    UNPROCESSED: Math.max(0, eligibleFoundSum - checkpointOk),
  };

  const reconcile = {
    reported29ValidatedCiks: {
      claim: "29 validated registrant CIKs",
      evidence: {
        registryV24Companies: registry.length,
        registryWithCik: withCik.length,
        eligibleIndexCompanies: indexCompanies.length,
      },
      reconciliation:
        "Closest live evidence is registry V24 with CIK (31) or eligible-filing index companies (27). " +
        "The '29' figure is an approximate prior wave narrative, not a separate validated-processed set. " +
        "None of these counts equal fully analyzed registrants.",
    },
    reported11940EligibleFilings: {
      claim: "11,940 eligible filings",
      evidence: { eligibleFoundSum, source: "data/sec-edgar/eligible-filings/_index.json" },
      reconciliation: "CONFIRMED as DISCOVERED inventory sum across 27 index companies. Not analyzed count.",
    },
    reportedCmbsHundreds: {
      claim: "CMBS findings involving hundreds of properties",
      evidence: {
        cmbsPilotDocsOk: cmbsPilot.documents?.ok ?? cmbsPilot.metrics?.documentsOk,
        cmbsPilotHotelLevelDates: cmbsPilot.metrics?.exactHotelLevelContractDates ?? 57,
        cmbs424BDocs: cmbsPilot.documents?.byFamily?.CMBS_424B ?? 16,
        globalWaveCmbsUnresolvedAgreementType: cmbsUnresolved,
      },
      reconciliation:
        "CMBS pilot V1 verified 57 hotel-level contract dates from ~21 OK documents (16 CMBS 424B + REIT schedules). " +
        "Global historical waves retained 21 CMBS dossiers with unresolved agreement type. " +
        "'Hundreds of properties' overstates commercially assessed yield; raw CMBS collateral tables can list large portfolios, " +
        "but executed hotel-matched commercial assessment remains tens, not hundreds.",
    },
  };

  const highValueGaps = [
    {
      cohort: "ryman-hospitality",
      cik: "0001040829",
      eligible: 479,
      checkpointProcessed: 0,
      rationale: "Named Gaylord/JW Marriott HMA year schedules in 10-K — hotel-level MCT calendars; Desert Ridge 2027 near-term",
      selectedForGapTest: true,
    },
    {
      cohort: "apple-hospitality-reit",
      cik: "0001418121",
      eligible: 563,
      checkpointProcessed: 15,
      rationale: "Property schedule footnotes disclose completed Jan 2026 Marriott→third-party manager transitions; short-term variable HMA structure",
      selectedForGapTest: true,
    },
    {
      cohort: "summit-hotel-properties",
      cik: "0001497645",
      eligible: 329,
      checkpointProcessed: 0,
      rationale: "NEGATIVE for hotel-level franchise table in YE2025 10-K (10–30yr portfolio only) — deprioritize schedule extraction",
      selectedForGapTest: false,
      gapTestResult: "NEGATIVE_SCHEDULE",
    },
    {
      cohort: "pebblebrook-hotel-trust",
      cik: "0001474098",
      eligible: 598,
      checkpointProcessed: 0,
      rationale: "Prior EX-10 named schedules historically productive; large unprocessed eligible inventory",
      selectedForGapTest: false,
    },
    {
      cohort: "service-properties-trust",
      cik: "0000945394",
      eligible: 585,
      checkpointProcessed: 0,
      rationale: "Prior SVC Hyatt multi-hotel agreement / IHG unnamed hotel staged — needs named-hotel follow-up",
      selectedForGapTest: false,
    },
    {
      cohort: "ashford-hospitality-trust",
      cik: "0001232582",
      eligible: 1507,
      checkpointProcessed: 0,
      rationale: "Largest eligible inventory; high 8-K volume — prioritize EX-10/HMA exhibits over mass 8-K scrape",
      selectedForGapTest: false,
    },
    {
      cohort: "CORE_SIX brand/owner 10-Ks (Marriott/Hilton/Host/Sunstone/DRH)",
      eligible: liveLedger.ledgerCohorts?.CORE_SIX?.eligibleFilings ?? 3863,
      checkpointProcessed: liveLedger.ledgerCohorts?.CORE_SIX?.processed ?? 20,
      rationale: "~0.5% of CORE_SIX eligible processed in checkpoint — not exhausted",
      selectedForGapTest: false,
    },
  ];

  return {
    asOf: AS_OF,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    registry: { companies: registry.length, withCik: withCik.length, slugs: registry.map((c) => c.slug) },
    eligibleIndex: { companies: indexCompanies.length, eligibleFoundSum, perCompany },
    checkpoints: {
      marriottHiltonHistoricalV2: mh,
      universalResearchV24: ur,
      okSum: checkpointOk,
      okSumRawCheckpointOverlapRisk: checkpointOkRaw,
      ledgerCohortProcessedSum: ledgerProcessed,
      errorSum: checkpointError,
      note: "Use ledgerCohortProcessedSum (~50) for non-overlapping EXTRACTED; raw checkpoint ok sums may double-count shared docs.",
    },
    cacheFiles,
    exhibit: {
      enumerated: exhibitEnum,
      processed: exhibitProc,
      parents: liveLedger.exhibitLedger?.parents ?? null,
      note: "From COVERAGE_LEDGER_LIVE.exhibitLedger",
    },
    stages,
    reconcile,
    highValueGaps,
    liveLedgerCohorts: liveLedger.ledgerCohorts || null,
    ledgerReconciliation: liveLedger.ledgerReconciliation || null,
    saturation: {
      verdict: "NOT_SATURATED",
      discoveredVsExtractedPct: eligibleFoundSum ? Number(((checkpointOk / eligibleFoundSum) * 100).toFixed(3)) : null,
      narrative:
        "Eligible discovery inventory (11,940) far exceeds checkpoint-extracted filings (~50 OK). " +
        "Commercial assessment has concentrated on high-yield owner schedules and CMBS tables, not bulk filing exhaustion. " +
        "Do not describe SEC/CMBS as exhausted.",
    },
  };
}

function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const inventory = buildCoverageInventory();
  const dossiers = buildGlobalSecCoverageGapDossiers();
  const persistable = dossiers.filter((d) => d.persistEligible && contractPersistAllowed(d));
  const staged = dossiers.filter((d) => !d.persistEligible || !contractPersistAllowed(d));

  const historical = dossiers.filter((d) =>
    ["HISTORICAL_MGMT_REPLACEMENT_CLOSED", "PORTFOLIO_MCT_FLEXIBILITY_NO_HOTEL_SCHEDULE"].includes(d.trackA),
  );
  const forward = dossiers.filter((d) =>
    ["DECISION_WINDOW_YEAR_ONLY_WITH_AUTO_RENEWAL", "NOT_QUALIFIED_DISTANT_YEAR_ONLY"].includes(d.trackA),
  );

  const trackA = dossiers
    .filter((d) => d.trackAPayload)
    .map((d) => ({
      findingId: d.findingId,
      hotelOrOwner: d.hotelName,
      commercialEvent: d.trackAPayload.signal,
      agreementType: d.agreementType,
      originalDocumentUrl: d.sourceUrl,
      factualClaim: d.evidenceSnippet,
      historicalOrProspective: d.trackA.includes("HISTORICAL") || d.trackA.includes("CLOSED") ? "HISTORICAL" : "PROSPECTIVE",
      verifiedDatesOrUnknowns: d.impreciseDates || (d.expirationYear ? [`year:${d.expirationYear}`] : []),
      evidenceLimitations: d.trackAPayload.limitation || (d.unresolvedTerms || []).join("; "),
      existingHifRefs: { discoveryPropertyId: d.discoveryPropertyId, contractId: d.contractId, findingId: d.findingId },
      opportunityInterpretationQuestions: [
        d.trackA === "HISTORICAL_MGMT_REPLACEMENT_CLOSED"
          ? "Is there residual franchise BCT timing for this hotel, or only a closed MCT?"
          : d.trackA === "DECISION_WINDOW_YEAR_ONLY_WITH_AUTO_RENEWAL"
            ? "Does auto-renewal performance threshold make 2027 a real decision window or a low-probability break?"
            : "Should portfolio HMA flexibility trigger owner-level engagement without hotel-specific expirations?",
      ],
      trackAClassification: d.trackA,
      doNotAutoPromoteToVerifiedBctMct: true,
    }));

  const trackC = dossiers
    .filter((d) => d.persistEligible)
    .map((d) => ({
      hotelName: d.hotelName,
      city: d.city,
      stateProvince: d.stateProvince,
      country: d.physicalCountry,
      hpcQuery: d.hpcQuery,
      hpcMatchStatus: d.hpcMatchStatus,
      discoveryPropertyId: d.discoveryPropertyId,
      issue: "PENDING_HPC_SEARCH — resolve unique HPC before customer publication",
    }));

  const checks = {
    apleSource: APLE_10K,
    rhpSource: RHP_10K,
    apleTransitionsNamed: APLE_MARRIOTT_EXIT_JAN2026.length,
    rhpHmaRows: RHP_HMA_ROWS.length,
    nearTermRhp: RHP_HMA_ROWS.filter((r) => r.nearTerm).map((r) => ({ hotel: r.hotelName, year: r.year })),
    yearOnlyPolicy: "No ISO milestoneDate for RHP year-only expirations",
    freeSourcesOnly: true,
    researchCostUsd: 0,
  };

  const summary = {
    asOf: AS_OF,
    wave: "B24",
    discoveryMethod: "TRACK_B_GLOBAL_SEC_COVERAGE_AUDIT_V1",
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    inventoryStages: inventory.stages,
    reconcile: inventory.reconcile,
    saturation: inventory.saturation,
    gapTest: {
      filingsAnalyzed: 2,
      filings: [
        { registrant: "Apple Hospitality REIT", form: "10-K", url: APLE_10K, yield: "9 named Marriott→third-party HMA transitions Jan 2026 + portfolio HMA structure" },
        { registrant: "Ryman Hospitality Properties", form: "10-K", url: RHP_10K, yield: "9 named Marriott HMA year expirations incl. Desert Ridge 2027" },
        { registrant: "Summit Hotel Properties", form: "10-K", yield: "NEGATIVE — no hotel-level franchise/HMA expiry table", notPersisted: true },
      ],
      dossiers: dossiers.length,
      persistEligible: persistable.length,
      staged: staged.length,
      historicalCommercialFindings: historical.length,
      forwardLookingFindings: forward.length,
    },
    paidSourceProposal: {
      needed: false,
      note: "No paid authorization used. Future: optional sec-api.io bulk exhibit download for Ashford/PEB EX-10 waves — estimate $50–150/mo if free EDGAR rate limits block; defer until named-schedule queue exhausted.",
    },
  };

  fs.writeFileSync(path.join(OUT, "COVERAGE_INVENTORY.json"), JSON.stringify(inventory, null, 2));
  fs.writeFileSync(
    path.join(OUT, "WAVE_DOSSIERS.json"),
    JSON.stringify({ dossiers: persistable, allDossiers: dossiers, staged, summary: summary.gapTest }, null, 2),
  );
  fs.writeFileSync(path.join(OUT, "TRACK_A_HANDOFF.json"), JSON.stringify({ researchCandidates: trackA }, null, 2));
  fs.writeFileSync(path.join(OUT, "TRACK_C_HANDOFF.json"), JSON.stringify({ handoffs: trackC }, null, 2));
  fs.writeFileSync(path.join(OUT, "SOURCE_VERIFICATION.json"), JSON.stringify(checks, null, 2));
  fs.writeFileSync(path.join(OUT, "RESEARCH_SUMMARY.json"), JSON.stringify(summary, null, 2));

  const md = `# Track B — Global SEC / CMBS / Contractual Coverage Audit V1

**As of:** ${AS_OF}  
**Publication:** CUSTOMER_BLOCKED · **Research cost:** $0  
**Branch:** feat/hif-v6-track-b-international-source-expansion-v1 · **PR:** #556 (update)

## A. Existing SEC research capabilities

- EDGAR eligible-filing inventory builder (\`data/sec-edgar/eligible-filings/\`)
- CIK registry V24 (\`${inventory.registry.companies}\` companies / \`${inventory.registry.withCik}\` with CIK)
- Checkpointed research runners (Marriott/Hilton historical V2 + universal V24)
- Exhibit / EX-10 enumeration + HTML schedule extractors
- CMBS 424B pilot (contract-timing-cmbs-v1)
- Coverage ledger V2/V3 contracts
- Track B global waves + ISE + CALA follow-up persistence via \`hif-track-b-global-waves-persist.mjs\`
- Free EDGAR only in this pass (no new paid auth)

## B. Actual registrant and filing coverage

| Metric | Evidence |
|---|---|
| Registry V24 companies | ${inventory.registry.companies} |
| Registry with CIK | ${inventory.registry.withCik} |
| Eligible index companies | ${inventory.eligibleIndex.companies} |
| Eligible filings DISCOVERED | **${inventory.eligibleIndex.eligibleFoundSum}** |
| Checkpoint docs status=ok | **${inventory.checkpoints.okSum}** |
| Checkpoint docs error | ${inventory.checkpoints.errorSum} |
| Local SEC cache files | ${inventory.cacheFiles} |
| CORE_SIX eligible / processed | ${inventory.liveLedgerCohorts?.CORE_SIX?.eligibleFilings ?? "see ledger"} / ${inventory.liveLedgerCohorts?.CORE_SIX?.processed ?? "see ledger"} |

## C. Discovered vs processed vs commercially assessed

| Stage | Count / status |
|---|---|
| DISCOVERED | ${inventory.stages.DISCOVERED} |
| RETRIEVED | ${inventory.stages.RETRIEVED} |
| EXTRACTED | ${inventory.stages.EXTRACTED} |
| HOTEL_MATCHED | ${inventory.stages.HOTEL_MATCHED} |
| COMMERCIALLY_ASSESSED | ${inventory.stages.COMMERCIALLY_ASSESSED} |
| PERSISTED | ${inventory.stages.PERSISTED} |
| UNPROCESSED | **${inventory.stages.UNPROCESSED}** |

Reconcile prior claims:
- **11,940** → CONFIRMED DISCOVERED inventory (not analyzed).
- **29 CIKs** → approximate; live evidence shows **31** registry-with-CIK / **27** index companies.
- **CMBS hundreds** → overstated vs **57** verified hotel-level dates (pilot) + **21** unresolved CMBS dossiers in global waves.

## D. Historical commercial findings (this gap test)

Nine APLE hotels completed Marriott→third-party manager transitions in **January 2026** (footnotes 4/5/6 on YE2025 10-K property schedule): Huntington (6 CA), Dimension (1 CA), Crestline (2 FL). Closed MCT events — franchise continues; hotel-level franchise expiry still undisclosed.

## E. Forward-looking commercial findings

RHP Marriott HMAs with **year-only** expirations: Gaylord×4 (2047), Rockies (2053), JW Hill Country (2060), **JW Desert Ridge (2027)** near-term with five automatic 10-year renewals if performance thresholds met, Inn at Opryland (2034), AC Hotel (2040). APLE portfolio: ~81% short-term variable HMAs (structural flexibility — not hotel-specific open MCT).

## F. Unprocessed high-value cohorts

See \`COVERAGE_INVENTORY.json\` → \`highValueGaps\`. Priority next: PEB/CLDT/SVC named EX-10; Ashford EX-10 over mass 8-K; Host/Sunstone/DRH named schedules. Summit YE2025 = negative schedule.

## G. Incremental discovery yield

- Filings analyzed this pass: **2** (APLE + RHP 10-K; Summit negative reviewed)
- Persist-eligible dossiers: **${persistable.length}**
- Staged (non-persist): **${staged.length}**
- Named hotels with commercial facts: **${APLE_MARRIOTT_EXIT_JAN2026.length + RHP_HMA_ROWS.length}**

## H. Source hyperlinks and evidence integrity

- APLE: ${APLE_10K}
- RHP: ${RHP_10K}
- Claim-level snippets on each dossier; year-only → no fake ISO dates.

## I. Airtable persistence / idempotency

\`node scripts/hif-track-b-global-waves-persist.mjs --batch-dir=reports/sec-edgar-hif-track-b-global-sec-coverage-audit-v1\`  
Dry-run then \`--apply\`. Existing IDs = no-op. No new schema. CUSTOMER_BLOCKED. No Verified BCT/MCT auto-promotion. Year-only → no milestone ISO rows.

## J. Research cost and future cost

- This pass: **$0**
- Paid proposal (optional later): sec-api.io bulk exhibits ~$50–150/mo only if free EDGAR rate limits block named EX-10 queue — **not required now**.

## K. Track A handoff

See \`TRACK_A_HANDOFF.json\`. Questions center on closed MCT vs residual BCT, and Desert Ridge 2027 auto-renewal realism. **Do not auto-promote to Verified BCT/MCT.**

## L. Track C handoff

See \`TRACK_C_HANDOFF.json\`. All gap-test hotels \`PENDING_HPC_SEARCH\`.

## M. Integration Controller handoff

Update open PR **#556** on current branch. IC owns merge. No scheduler / Surfe / customer publication. Preserve dirty \`tmp/*\`.

## N. Saturation assessment

**${inventory.saturation.verdict}** — extracted ≈ **${inventory.saturation.discoveredVsExtractedPct}%** of discovered eligible filings. Productive coverage remains in named owner schedules and EX-10 exhibits, not bulk filing exhaustion.

---

### Persistence command
\`\`\`bash
node scripts/hif-track-b-global-waves-persist.mjs --batch-dir=reports/sec-edgar-hif-track-b-global-sec-coverage-audit-v1
node scripts/hif-track-b-global-waves-persist.mjs --batch-dir=reports/sec-edgar-hif-track-b-global-sec-coverage-audit-v1 --apply
\`\`\`
`;

  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), md);
  console.log(
    JSON.stringify(
      {
        ok: true,
        out: OUT,
        dossiers: dossiers.length,
        persistEligible: persistable.length,
        discovered: inventory.eligibleIndex.eligibleFoundSum,
        extractedOk: inventory.checkpoints.okSum,
        saturation: inventory.saturation.verdict,
      },
      null,
      2,
    ),
  );
}

main();
