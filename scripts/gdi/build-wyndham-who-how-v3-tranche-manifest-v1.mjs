#!/usr/bin/env node
/**
 * Build top-25 Wyndham San Diego Bayside WHO/HOW V3 paid-tranche manifest.
 * $0 / DryRun prep only — does not execute paid research.
 *
 * Proposed authorization (requires separate founder GO before Live):
 *   FOUNDER_GO_WYNDHAM_WHO_HOW_V3_TRANCHE_20261127
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { quoteExaAgentRun, EXA_AGENT_EFFORT } from "../../lib/group-demand-intelligence/research/exa-agent/exa-agent-provider-adapter-v1.js";
import { WHO_HOW_RESEARCH_PURPOSE } from "../../lib/group-demand-intelligence/research/exa-agent/gdi-exa-who-how-live-adapter-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "../..");
const HOTEL_ID = "recnPB3Q02NO8Ma1o";
const AUTH_ID = "FOUNDER_GO_WYNDHAM_WHO_HOW_V3_TRANCHE_20261127";
const HARD_CEILING_USD = 5.0;
const LIMIT = 25;

const sourcePath = path.join(
  root,
  "reports/gdi-wyndham-final-commercial-completion-v1/06-WHO_HOW_TOP30_RECOVERY.json",
);
const outDir = path.join(
  root,
  "reports/gdi-wyndham-who-how-v3-tranche-20261127",
);

const source = JSON.parse(fs.readFileSync(sourcePath, "utf8"));
const quote = quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.MEDIUM });
const unit = Number(quote?.maxUsd ?? 0.1);

const ranked = (source.candidates || [])
  .filter((c) => c.missingWhoHow === true)
  .sort((a, b) => {
    const scoreDelta = Number(b.score || 0) - Number(a.score || 0);
    if (scoreDelta) return scoreDelta;
    const futureA = /FUTURE/i.test(a.competitiveClass || "") ? 1 : 0;
    const futureB = /FUTURE/i.test(b.competitiveClass || "") ? 1 : 0;
    // Prefer FUTURE_RECURRING then alphabetical org for stability
    if (futureB !== futureA) return futureB - futureA;
    return String(a.org || "").localeCompare(String(b.org || ""));
  })
  .slice(0, LIMIT);

const opportunities = ranked.map((c, idx) => ({
  rank: idx + 1,
  opportunityId: c.id,
  organizationName: c.org,
  title: c.title,
  segmentId: c.seg,
  competitiveClass: c.competitiveClass,
  priorScore: c.score,
  priorFailedGates: c.failedGates || [],
  previousWhoHowStatus: "CONTACT_RESEARCH_REQUIRED",
  previousFqoStatus: "NOT_FINALIZED",
  stageScope: "WHO_HOW_RESOLUTION_ONLY",
  researchPurpose: WHO_HOW_RESEARCH_PURPOSE,
  estimatedUnitUsd: unit,
  noStage1Rediscovery: true,
  noStage2Rerun: true,
  noFindymail: true,
  noSurfe: true,
  noAirtableWrites: true,
  noStagedActivation: true,
  noCurrentPublication: true,
}));

const manifest = {
  version: "gdi-wyndham-who-how-v3-tranche-manifest-v1",
  preparedAt: new Date().toISOString(),
  hotelId: HOTEL_ID,
  hotelName: "Wyndham San Diego Bayside",
  proposedAuthorizationId: AUTH_ID,
  authorizationStatus: "PROPOSED_AWAITING_FOUNDER_GO",
  stageScope: "WHO_HOW_RESOLUTION_ONLY",
  hardCeilingUsd: HARD_CEILING_USD,
  effort: EXA_AGENT_EFFORT.MEDIUM,
  estimatedUnitUsd: unit,
  maxUniqueOpportunities: LIMIT,
  selectedCount: opportunities.length,
  estimatedMaxSpendUsd: Number(
    Math.min(HARD_CEILING_USD, opportunities.length * unit).toFixed(4),
  ),
  inventoryPreserved: {
    note: "Does not rediscover or replace the existing 161-opportunity inventory",
    preserveExisting161: true,
  },
  freezes: {
    bethesda: "ENFORCED",
    clean11: "ENFORCED",
  },
  forbidden: [
    "STAGE_1_REDISCOVERY",
    "UNNECESSARY_STAGE_2_RERUN",
    "AUTOMATIC_FINDYMAIL",
    "AUTOMATIC_SURFE",
    "AIRTABLE_WRITES",
    "STAGED_ACTIVATION",
    "CURRENT_PUBLICATION",
  ],
  successTarget: {
    minRecoveredBuyerOrEntryPoint: 10,
    of: 25,
    note: "Do not relax verification to hit target",
  },
  source: {
    path: "reports/gdi-wyndham-final-commercial-completion-v1/06-WHO_HOW_TOP30_RECOVERY.json",
    sourceCount: source.count,
  },
  opportunities,
  scorecardTemplateFields: [
    "opportunityId",
    "organizationName",
    "segmentId",
    "previousWhoHowStatus",
    "updatedWhoHowStatus",
    "verifiedDirectBuyer",
    "verifiedBuyerInfluencer",
    "alternativeEntryPoint1",
    "alternativeEntryPoint2",
    "buyingAuthorityVerification",
    "supportingEvidenceUrls",
    "recommendedHotelSalesApproach",
    "personSpecificOutreachAvailability",
    "previousFqoStatus",
    "updatedFqoStatus",
    "remainingBlockers",
    "actualResearchCostUsd",
    "crossHotelReusePotential",
  ],
  aggregateMetricsPlanned: [
    "directBuyerRecoveryRate",
    "alternativeEntryPointRecoveryRate",
    "namedPersonRecoveryRate",
    "personSpecificReachabilityRate",
    "incrementalFqoCount",
    "costPerRecoveredOpportunity",
    "costPerIncrementalFqo",
    "falsePositiveRate",
    "settledJobReuseAvoidedSpendUsd",
  ],
  executionGate: {
    requires: [
      "PR_613_MERGED",
      "POST_MERGE_SYNC_GDI_HOME",
      "LIVE_ADAPTER_0USD_TESTS_PASS",
      "EXPLICIT_FOUNDER_GO_ON_AUTHORIZATION_TOKEN",
      "GDI_EXA_AGENT_LIVE=1",
      "authorizeExaAgentLive=true",
    ],
    paidExecutionStatus: "BLOCKED_AWAITING_FOUNDER_GO",
  },
};

fs.mkdirSync(outDir, { recursive: true });
const outJson = path.join(outDir, "01-TRANCHE_MANIFEST.json");
const outMd = path.join(outDir, "FOUNDER_WHO_HOW_V3_TRANCHE_PREP.md");
fs.writeFileSync(outJson, `${JSON.stringify(manifest, null, 2)}\n`);

const lines = [
  "# FOUNDER PREP — Wyndham WHO/HOW V3 Initial Paid Tranche",
  "",
  `**Prepared:** ${manifest.preparedAt}`,
  `**Hotel:** ${HOTEL_ID} (Wyndham San Diego Bayside)`,
  `**Proposed auth:** \`${AUTH_ID}\``,
  `**Status:** ${manifest.authorizationStatus}`,
  "",
  "## Controls",
  "",
  `- Stage 3 WHO/HOW only`,
  `- Max ${LIMIT} unique existing opportunities`,
  `- Hard ceiling **USD ${HARD_CEILING_USD.toFixed(2)}** (unit quote ~$${unit})`,
  `- Estimated max spend: **USD ${manifest.estimatedMaxSpendUsd}**`,
  `- Preserve existing 161-opportunity inventory`,
  `- No Stage 1 rediscovery / unnecessary Stage 2 / Findymail / Surfe / Airtable / STAGED / CURRENT`,
  `- Bethesda + CLEAN11 freezes enforced`,
  "",
  "## Selected opportunities (top 25)",
  "",
  "| # | Opportunity ID | Organization | Segment | Prior score |",
  "|---|---|---|---|---|",
  ...opportunities.map(
    (o) =>
      `| ${o.rank} | \`${o.opportunityId}\` | ${o.organizationName} | ${o.segmentId} | ${o.priorScore} |`,
  ),
  "",
  "## Authorization gate",
  "",
  "This token is **proposed only**. Paid execution requires explicit founder approval after:",
  "1. PR #613 merge + GDI post-merge sync",
  "2. Live adapter $0 tests PASS",
  "3. Founder GO on the exact authorization string above",
  "",
  `Manifest: \`${path.relative(root, outJson).replace(/\\\\/g, "/")}\``,
  "",
];
fs.writeFileSync(outMd, `${lines.join("\n")}\n`);

console.log(
  JSON.stringify(
    {
      ok: true,
      outJson,
      outMd,
      selectedCount: opportunities.length,
      estimatedMaxSpendUsd: manifest.estimatedMaxSpendUsd,
      authorizationStatus: manifest.authorizationStatus,
    },
    null,
    2,
  ),
);
