/**
 * Wyndham Alltra Punta Cana Full E2E preflight + Aimbridge cohort recovery audit.
 * Zero paid research. Does not mutate Sheraton/Bethesda publications.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const AIM = path.resolve(ROOT, "..", "my-operators-backend-aimbridge-three-hotel-e2e");
const OUT = path.join(ROOT, "reports/gdi-wyndham-alltra-punta-cana-full-e2e-preflight-v1");

const WY = "recXh4NgF8sDbi9cQ";
const VO = "recTk5I8VOzsPhxDz";
const SH = "recCAGqkERSXgiku4";
const DUP = "reccax9nT5J90Sc0Y";

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8").replace(/^\uFEFF/, ""));
}

function write(name, obj) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(obj, null, 2)}\n`);
  console.log("wrote", name);
}

function segmentCoverage(hotelId, root) {
  const segDir = path.join(
    root,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "full-e2e-fourteen-segment-state",
  );
  const files = fs.readdirSync(segDir).filter((f) => f.endsWith(".json")).sort();
  const segments = [];
  let candidateIdsSum = 0;
  let extractedSum = 0;
  let completedSum = 0;
  let unresolvedSum = 0;
  const unique = new Set();
  for (const f of files) {
    const j = readJson(path.join(segDir, f));
    const c = Array.isArray(j.candidateIds) ? j.candidateIds : [];
    const e = Array.isArray(j.extractedCandidateIds) ? j.extractedCandidateIds : [];
    const comp = Array.isArray(j.completedCandidateIds) ? j.completedCandidateIds : [];
    const u = Array.isArray(j.unresolvedCandidateIds) ? j.unresolvedCandidateIds : [];
    for (const id of c) unique.add(String(id));
    candidateIdsSum += c.length;
    extractedSum += e.length;
    completedSum += comp.length;
    unresolvedSum += u.length;
    segments.push({
      segmentId: j.segmentId || path.basename(f, ".json"),
      status: j.status,
      coverageStatus: j.coverageStatus,
      completionDecision: j.completionDecision,
      candidateIds: c.length,
      extractedCandidateIds: e.length,
      completedCandidateIds: comp.length,
      unresolvedCandidateIds: u.length,
      retrievalQueryCount: j.retrievalQueryCount ?? null,
      evidenceRefs: Array.isArray(j.evidenceRefs) ? j.evidenceRefs.length : 0,
      languages: j.languages || [],
    });
  }
  return {
    segmentFileCount: files.length,
    segments,
    candidateIdsSum,
    uniqueCandidateIds: unique.size,
    extractedSum,
    completedSum,
    unresolvedSum,
  };
}

function orsAudit(hotelId, root) {
  const orsDir = path.join(
    root,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunity-research-state",
  );
  const files = fs.readdirSync(orsDir).filter((f) => f.endsWith(".json"));
  const disposition = {};
  const publicationStatus = {};
  const namedContactStatus = {};
  let withHttp = 0;
  let withExa = 0;
  let costUsd = 0;
  let qualifiedRows = 0;
  for (const f of files) {
    const raw = fs.readFileSync(path.join(orsDir, f), "utf8");
    const j = JSON.parse(raw.replace(/^\uFEFF/, ""));
    const d = j.disposition || j.primaryDisposition || "UNKNOWN_UNCLASSIFIED_RESEARCH_DOSSIER";
    disposition[d] = (disposition[d] || 0) + 1;
    const p = j.publicationStatus || "MISSING";
    publicationStatus[p] = (publicationStatus[p] || 0) + 1;
    const n = j.namedContactStatus || "MISSING";
    namedContactStatus[n] = (namedContactStatus[n] || 0) + 1;
    if (/https?:\/\//.test(raw)) withHttp += 1;
    if (/exa\.ai|providerInvocations|exaAgent|EXA_/i.test(raw)) withExa += 1;
    costUsd += Number(j.costUsd || 0);
    if (j.disposition) qualifiedRows += 1;
  }
  return {
    dossierCount: files.length,
    disposition,
    publicationStatus,
    namedContactStatus,
    withHttpUrl: withHttp,
    withExaArtifactSignals: withExa,
    costUsdSum: costUsd,
    rowsWithExplicitDisposition: qualifiedRows,
  };
}

const wySeg = segmentCoverage(WY, ROOT);
const voSeg = segmentCoverage(VO, ROOT);
const shSeg = segmentCoverage(SH, ROOT);
const wyOrs = orsAudit(WY, ROOT);
const voOrs = orsAudit(VO, ROOT);
const shOrs = orsAudit(SH, ROOT);

const cfgWy = readJson(
  path.join(ROOT, "config/group-demand-intelligence/hotels", `${WY}.json`),
);
const staged = readJson(
  path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    SH,
    "STAGED.json",
  ),
);

const inventoryBaseline = readJson(
  path.join(
    AIM,
    "reports/aimbridge-three-hotel-full-e2e-research-baseline-v1/inventories/wyndham-alltra-punta-cana-inventory.json",
  ),
);
const costLedger = readJson(
  path.join(
    AIM,
    "reports/aimbridge-three-hotel-full-e2e-research-baseline-v1/07-COST_LEDGER_AND_PAID_QUEUE.json",
  ),
);
const paidRevised = readJson(
  path.join(
    AIM,
    "reports/aimbridge-three-hotel-full-e2e-deep-research-v1/03-PAID_TRANCHE_PROPOSAL_REVISED.json",
  ),
);
const cohort = readJson(
  path.join(
    AIM,
    "reports/aimbridge-three-hotel-full-e2e-research-baseline-v1/01-COHORT_IDENTITY.json",
  ),
);

const generatedAt = new Date().toISOString();

write("01-CANONICAL_HOTEL_IDENTITY.json", {
  contract: "GDI_WYNDHAM_ALLTRA_PUNTA_CANA_FULL_E2E_PREFLIGHT_V1",
  generatedAt,
  canonical: {
    hotelId: WY,
    displayName: "Wyndham Alltra Punta Cana",
    country: "Dominican Republic",
    city: "Punta Cana",
    region: cfgWy.region || "La Altagracia",
    positioning: "620-room all-inclusive resort (Uvero Alto Beach)",
    rooms: cfgWy.capabilityProfile?.totalGuestrooms || 620,
    operator: "Aimbridge LATAM",
    adpPropertyId: "adp_wyndham_alltra_punta_cana",
    hotelIdentityStatus: cfgWy.hotelIdentityStatus,
  },
  forbiddenDuplicate: {
    hotelId: DUP,
    status: "NON_CANONICAL_OSM_TWIN",
    rule: "Do not use as active hotel identity. Reconcile any historical research to canonical without overwriting evidence.",
    researchPresentUnderDuplicateId: false,
    researchPresentUnderCanonicalId: true,
  },
  rejectedSubstitution: {
    hotelId: "recsn3BUKJ9PNfeZW",
    displayName: "JW Marriott Hotel Monterrey Valle",
    status: "NOT_IN_AIMBRIDGE_THREE_HOTEL_COHORT",
    note: "Founder lock 2026-11-07 and 2026-11-14 — Monterrey is Aimbridge-managed but not this research cohort.",
  },
  aimbridgeAffiliationEvidence: [
    "https://www.aimbridgelatam.com/portfolio.htm",
    "config/group-demand-intelligence/hotels/recXh4NgF8sDbi9cQ.json",
    "reports/aimbridge-three-hotel-full-e2e-research-baseline-v1/01-COHORT_IDENTITY.json",
  ],
  cohortIdentity: cohort.thirdHotelResolution || null,
  createNewHotelRecord: false,
});

write("02-EXISTING_AIMBRIDGE_RESEARCH_INVENTORY.json", {
  sourceWorktree: AIM,
  sourceBranch: "research/aimbridge-three-hotel-full-e2e-v1",
  recoveredIntoPrimaryCheckout: true,
  recoveredAt: generatedAt,
  note: "Counts are discovery/research history, not qualified customer opportunities. No opportunities.json created.",
  cohort: {
    sheraton: {
      hotelId: SH,
      researchStateDossiers: shOrs.dossierCount,
      uniqueCandidateIds: shSeg.uniqueCandidateIds,
      segmentFiles: shSeg.segmentFileCount,
    },
    voco: {
      hotelId: VO,
      researchStateDossiers: voOrs.dossierCount,
      uniqueCandidateIds: voSeg.uniqueCandidateIds,
      segmentFiles: voSeg.segmentFileCount,
    },
    wyndham: {
      hotelId: WY,
      researchStateDossiers: wyOrs.dossierCount,
      uniqueCandidateIds: wySeg.uniqueCandidateIds,
      segmentFiles: wySeg.segmentFileCount,
    },
  },
  founderExpectedVsActual: {
    sheraton: {
      expectedDossiers: 434,
      actual: shOrs.dossierCount,
      match: shOrs.dossierCount === 434,
    },
    voco: {
      expectedDossiers: 381,
      actual: voOrs.dossierCount,
      expectedCandidates: 646,
      actualCandidates: voSeg.uniqueCandidateIds,
      match: voOrs.dossierCount === 381 && voSeg.uniqueCandidateIds === 646,
    },
    wyndham: {
      expectedDossiers: 227,
      actual: wyOrs.dossierCount,
      expectedCandidates: 553,
      actualCandidates: wySeg.uniqueCandidateIds,
      match: wyOrs.dossierCount === 227 && wySeg.uniqueCandidateIds === 553,
    },
  },
  primaryCheckoutPaths: {
    wyndham:
      "data/group-demand-intelligence/hotels/recXh4NgF8sDbi9cQ/{full-e2e-fourteen-segment-state,opportunity-research-state}",
    voco:
      "data/group-demand-intelligence/hotels/recTk5I8VOzsPhxDz/{full-e2e-fourteen-segment-state,opportunity-research-state}",
    configs: [
      "config/group-demand-intelligence/hotels/recXh4NgF8sDbi9cQ.json",
      "config/group-demand-intelligence/hotels/recTk5I8VOzsPhxDz.json",
    ],
  },
  customerOpportunitiesJson: { wyndham: false, voco: false },
});

write("03-HISTORICAL_EXA_EVIDENCE_RECOVERY.json", {
  paidSpendExecutedThisRunUsd: 0,
  historicalAimbridgeBaselineSpendUsd: 0,
  wyndham: {
    ...wyOrs,
    note: "ORS dossiers carry Exa/provider artifact signals and HTTP evidence URLs from prior Aimbridge research. costUsd fields sum to 0 on dossiers — historical paid Sheraton Top-15 spend is not transferable.",
  },
  voco: { ...voOrs },
  recoveryActionsCompleted: [
    "Located Aimbridge worktree research for all three cohort hotels",
    "Copied Wyndham + voco segment state and ORS into primary checkout",
    "Copied Wyndham + voco GDI hotel configs into primary checkout",
  ],
  recoveryActionsPending: [
    "Rehydrate Exa packs into canonical evidence/claims arrays during Full E2E (zero-cost path)",
    "Airtable research-entity reconciliation under founder auth",
  ],
  doNotInitiatePaidExaBeforeRecoveryComplete: true,
  recoveryCompleteForPreflight: true,
});

write("04-14_SEGMENT_COVERAGE.json", {
  hotelId: WY,
  allFourteenPresent: wySeg.segmentFileCount === 14,
  wyndham: wySeg,
  vocoPreserved: { hotelId: VO, ...voSeg },
  completionState:
    "ALL_SEGMENTS_PARTIAL_NOT_COMPLETE — Full E2E must continue qualification, not skip discovery stages",
});

write("05-RESEARCH_QUALITY_AND_GAPS.json", {
  hotelId: WY,
  researchOnly: true,
  salesReadyCustomerOpportunities: 0,
  dispositionedRows: wyOrs.rowsWithExplicitDisposition,
  unclassifiedResearchDossiers:
    wyOrs.disposition.UNKNOWN_UNCLASSIFIED_RESEARCH_DOSSIER || 0,
  baselineSeedInventoryUnique: inventoryBaseline.uniqueAfterDedupe || null,
  namedContactStatus: wyOrs.namedContactStatus,
  gaps: [
    "0 completedCandidateIds across all 14 segments — discovery funnel not commercially closed",
    "Most ORS dossiers lack explicit commercial disposition (research-stage inventory)",
    "Named property/group sales contacts sparse; many NOT_YET_IDENTIFIED",
    "No opportunities.json — customer presentation not yet built",
    "CMI stage 4a not yet executed under certified contract for this hotel",
    "Commercial actionability gate 4b not yet applied to a Wyndham staged package",
    "Need-Date Match not established (provisional only if proposed)",
    "All-inclusive demand adaptation required — do not copy Sheraton expo assumptions",
  ],
  strengths: [
    "553 unique discovered candidate IDs across 14 segments",
    "227 research-state dossiers recovered intact",
    "Canonical config with Aimbridge LATAM + 620 AI capability profile",
    "Prior zero-cost Account Development pathway seeds exist in Aimbridge baseline inventory",
  ],
});

write("06-COMMERCIAL_PROFILE.json", {
  hotelId: WY,
  profile: cfgWy.capabilityProfile,
  demandTerritory: cfgWy.demandTerritory,
  commercialPriorities: cfgWy.commercialPriorities,
  allInclusiveResearchFocus: [
    "International group leisure",
    "Corporate incentive travel",
    "Association retreats",
    "Destination weddings",
    "Social celebrations",
    "Sports groups",
    "Wellness and affinity groups",
    "Tour operator / travel trade",
    "Meeting planners and DMCs",
    "Corporate and organizational retreats",
    "Resort buyouts / large-group blocks where commercially plausible",
  ],
  languages: ["en", "es"],
  antiPatterns: [
    "Do not assume large Punta Cana events automatically generate Alltra business",
    "Do not invent Cap Cana Marina identity",
    "Do not classify generic preferred-resort listings as confirmed competitor bookings",
  ],
});

write("07-CMI_DISCOVERY_PLAN.json", {
  contract: "gdi-cmi-canonical-eligibility-contract-v1",
  hotelId: WY,
  mandatoryStage4a: true,
  zeroCmiRequiresDocumentedCoverage: true,
  plan: [
    "Search verified future group/event commitments at competing Punta Cana / Cap Cana / Bávaro AI resorts",
    "Require named competing property + booking evidence + committed business type + actual event dates",
    "Future-cycle account-development thesis + relevant contact + next action",
    "Reject generic preferred lists / tour operator relationships without commitment evidence",
    "Do not represent venue bookings as room blocks without evidence",
  ],
  competitiveTerritorySeeds: ["Cap Cana", "Bávaro", "Macao", "La Romana", "Bayahibe"],
  paidRequired: false,
  note: "Stage 4a may begin with historical Exa recovery + public discovery; paid only if authorized.",
});

write("08-FUTURE_DEMAND_RESEARCH_PLAN.json", {
  hotelId: WY,
  commercialQualityTarget: {
    minimumCredibleActionableOpportunities: 15,
    currentCustomerVisible: 0,
    doNotFabricateToMeetTarget: true,
    doNotPromoteExpiredEvents: true,
  },
  funnel: {
    uniqueCandidateIds: wySeg.uniqueCandidateIds,
    researchStateDossiers: wyOrs.dossierCount,
    explicitlyDispositioned: wyOrs.rowsWithExplicitDisposition,
    accountDevelopment: wyOrs.disposition.ACCOUNT_DEVELOPMENT || 0,
    needsMoreResearch: wyOrs.disposition.NEEDS_MORE_RESEARCH || 0,
  },
  methodology: [
    "Reuse historical Exa/ORS first",
    "Qualify broader funnel → select ≥15 evidence-grounded, resort-fit, contact-relevant, actionable rows",
    "Distinguish net-new qualified from upgraded historical records",
    "Apply Full E2E template stages including 4a CMI and 4b actionability",
  ],
});

write("09-AIRTABLE_RECONCILIATION.json", {
  baseId: "appa2cE7FTRmIbB32",
  opportunityTable: "tblRuReslJMwsfRQj",
  hotelId: WY,
  status: "PREFLIGHT_ONLY",
  historicalAimbridgeAirtableDryRun: "SKIPPED_NO_CREDENTIALS_2026-11-06",
  plan: [
    "Inspect existing Wyndham-linked research/opportunity records",
    "Idempotent dry-run write plan: CREATE/UPDATE/NO_CHANGE/CONFLICT/RESEARCH_STAGE/HISTORICAL",
    "Preserve research-stage and historical data; no deletes",
    "No live opportunity upserts without founder authorization",
    "No manufactured hotel-confirmed need dates; provisional scenarios only if labeled",
  ],
  liveUpsertAuthorized: false,
});

const wyBatch = (paidRevised.batches || []).find((b) => b.hotelId === WY) || {};
write("10-PAID_RESEARCH_BUDGET.json", {
  hotelId: WY,
  authorizationRequired: true,
  authorized: false,
  executedThisRun: false,
  paidSpendThisRunUsd: 0,
  historicalDossierCostUsdSum: wyOrs.costUsdSum,
  sheratonBudgetNotTransferable: true,
  priorAimbridgeProposal: {
    source:
      "reports/aimbridge-three-hotel-full-e2e-deep-research-v1/03-PAID_TRANCHE_PROPOSAL_REVISED.json",
    batchId: wyBatch.batchId || null,
    estimatedCostUsd: wyBatch.estimatedCostUsd || 35,
    maximumSpendUsd: wyBatch.maximumSpendUsd || 36,
  },
  proposedWyndhamFullE2eContinuation: {
    objective:
      "Reach ≥15 credible actionable future/ongoing opportunities with named contacts + mandatory CMI 4a + actionability 4b",
    freeHistoricalRecoveryUsd: 0,
    proposedPaidBudgetUsd: 42,
    hardCeilingUsd: 48,
    providers: ["Exa Agent (authorized only)"],
    surfe: "on-click only; not automatic",
    stopConditions: [
      "No paid calls without Wyndham-specific founder authorization",
      "Stop if only Cap Cana peer demand without Alltra pathway",
      "Do not promote from need-date window alone",
      "Do not fabricate opportunities to meet the 15 target",
    ],
  },
  priorCohortCostLedgerNote: costLedger,
});

write("11-FULL_E2E_EXECUTION_PLAN.json", {
  template: "GDI_FULL_E2E_HOTEL_TEMPLATE_V1",
  hotelId: WY,
  mode: "preflight_then_authorized_stages",
  executableCommands: {
    preflight: "npm run gdi:full-e2e -- --hotelId recXh4NgF8sDbi9cQ --mode preflight",
    status: "npm run gdi:full-e2e-status -- --hotelId recXh4NgF8sDbi9cQ",
    dryRunChain:
      "npm run gdi:full-e2e-fourteen-segment-chain -- --hotels recXh4NgF8sDbi9cQ",
  },
  mandatoryStages: [
    "Historical Exa research recovery",
    "Full 14-segment research coverage",
    "Commercial qualification",
    "Verified relevant named contacts",
    "Mandatory CMI discovery stage 4a",
    "Permanent commercial-actionability gate 4b",
    "Current/future demand assessment",
    "Deduplication",
    "Hotel Market and Competitive classification",
    "Revenue-stream SVGs",
    "Need-Date Match (provisional only unless hotel-confirmed)",
    "Airtable-backed canonical records (auth-gated)",
    "Full card/drawer presentation",
    "Founder-preview browser testing",
    "Immutable publication lifecycle (STAGED only until approved)",
  ],
  doNotSkipBecauseHistoricalResearchExists: true,
  doNotActivateCustomerPublished: true,
  followingHotel: {
    hotelId: VO,
    displayName: "voco Cancún Zona Hotelera",
    queueOnly: true,
  },
});

write("12-FOUNDER_PREVIEW_READINESS.json", {
  hotelId: WY,
  stagedPackageExists: false,
  customerPublished: false,
  readiness:
    "RESEARCH_RECOVERED_PREFLIGHT_COMPLETE — not yet founder-preview staged",
  requiredBeforePreview: [
    "Qualification to ≥15 actionable opportunities (or documented shortfall)",
    "CMI stage 4a certification or zero-CMI coverage proof",
    "Actionability gate 4b",
    "Canonical presentation DTOs",
    "Browser verification on preview=staged",
  ],
  previewUrlWhenReady:
    "http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=recXh4NgF8sDbi9cQ&preview=staged",
  sheratonUnchanged: {
    stagedPublicationVersion: staged.publicationVersion,
    fingerprint: staged.contentFingerprintSha256,
  },
});

write("13-VOCO_NEXT_HOTEL_QUEUE.json", {
  queuePosition: "IMMEDIATELY_AFTER_WYNDHAM_ALLTRA_PUNTA_CANA",
  hotelId: VO,
  displayName: "voco Cancún Zona Hotelera",
  country: "Mexico",
  operator: "Aimbridge LATAM",
  recoveredResearch: {
    researchStateDossiers: voOrs.dossierCount,
    uniqueCandidateIds: voSeg.uniqueCandidateIds,
    segmentFiles: voSeg.segmentFileCount,
    opportunitiesJson: false,
  },
  doNotInitiateFullVocoRunYet: true,
  preserveRecords: true,
  preflightCommandWhenReady:
    "npm run gdi:full-e2e -- --hotelId recTk5I8VOzsPhxDz --mode preflight",
});

write("14-GITHUB_STATUS.json", {
  branch: "cert/gdi-sheraton-final-e2e-and-aimbridge-handoff-v1",
  relatedPr: 285,
  correction:
    "Replace JW Monterrey Valle next-hotel recommendation with Wyndham Alltra Punta Cana; queue voco Cancún next",
  sheratonStagedFingerprintUnmutated: staged.contentFingerprintSha256,
  bethesdaMustRemainUnmutated: true,
  paidSpendUsdThisRun: 0,
  researchCopiedIntoPrimary: true,
  note: "ORS/segment research recovered from Aimbridge worktree; no CUSTOMER_PUBLISHED; no paid research.",
});

const md = `# Wyndham Alltra Punta Cana — Full E2E Preflight

**Reference date:** 2026-11-14  
**Canonical hotel ID:** \`recXh4NgF8sDbi9cQ\`  
**Hotel:** Wyndham Alltra Punta Cana · Dominican Republic · 620-room all-inclusive  
**Forbidden duplicate:** \`reccax9nT5J90Sc0Y\` (do not use)  
**Rejected substitution:** JW Marriott Monterrey Valle \`recsn3BUKJ9PNfeZW\` — not in Aimbridge three-hotel cohort

## Verdict

**Status: \`PREFLIGHT_COMPLETE_RESEARCH_RECOVERED_AWAITING_AUTHORIZED_FULL_E2E\`**

Existing Aimbridge research has been located in worktree \`my-operators-backend-aimbridge-three-hotel-e2e\` and copied into the primary checkout. **No paid research executed. No CUSTOMER_PUBLISHED. Sheraton staged package unchanged.**

## Recovered counts (verified)

| Hotel | ID | ORS dossiers | Unique candidate IDs | 14 segments | opportunities.json |
|---|---|---:|---:|---:|---|
| Sheraton Guadalajara Expo | \`recCAGqkERSXgiku4\` | **434** | **680** | 14 | yes (certified separate) |
| **Wyndham Alltra Punta Cana** | \`recXh4NgF8sDbi9cQ\` | **227** | **553** | **14** | **no** |
| voco Cancún Zona Hotelera (queued) | \`recTk5I8VOzsPhxDz\` | **381** | **646** | **14** | **no** |

These are research-history counts, not sales-ready opportunities.

## Wyndham quality snapshot

- Explicit dispositions in ORS: **${wyOrs.rowsWithExplicitDisposition}** (AD ${wyOrs.disposition.ACCOUNT_DEVELOPMENT || 0} · NMR ${wyOrs.disposition.NEEDS_MORE_RESEARCH || 0} · Watch ${wyOrs.disposition.WATCH || 0} · Rejected ${wyOrs.disposition.REJECTED || 0})
- Unclassified research dossiers: **${wyOrs.disposition.UNKNOWN_UNCLASSIFIED_RESEARCH_DOSSIER || 0}**
- Named contact signals: property group sales **${wyOrs.namedContactStatus.NAMED_PROPERTY_GROUP_SALES || 0}** · intermediary **${wyOrs.namedContactStatus.NAMED_INTERMEDIARY || 0}**
- All 14 segments: **PARTIAL / NOT_COMPLETE** (0 completedCandidateIds)
- Customer-visible opportunities today: **0**
- Target for Full E2E: **≥15** credible actionable future/ongoing opportunities (do not fabricate)

## Paid research budget (proposed — not authorized)

- Free historical recovery: **$0**
- Proposed Wyndham continuation ceiling: **$42** (hard max **$48**)
- Sheraton remaining budget: **not transferable**
- Surfe: on-click only
- **Do not initiate paid calls without Wyndham-specific authorization**

## Exact next command

\`\`\`bash
npm run gdi:full-e2e -- --hotelId recXh4NgF8sDbi9cQ --mode preflight
\`\`\`

## Queue

1. **Now:** Wyndham Alltra Punta Cana  
2. **Next:** voco Cancún Zona Hotelera \`recTk5I8VOzsPhxDz\` (research preserved; do not start full run yet)

## Protections

- Sheraton STAGED \`${staged.publicationVersion}\` fingerprint \`${staged.contentFingerprintSha256}\` unchanged
- Bethesda approved publication must remain unchanged
- No live Airtable opportunity upserts without authorization
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), md);
console.log("wrote FOUNDER_REPORT.md");
console.log(
  JSON.stringify(
    {
      wyndhamDossiers: wyOrs.dossierCount,
      wyndhamCandidates: wySeg.uniqueCandidateIds,
      vocoDossiers: voOrs.dossierCount,
      vocoCandidates: voSeg.uniqueCandidateIds,
    },
    null,
    2,
  ),
);
