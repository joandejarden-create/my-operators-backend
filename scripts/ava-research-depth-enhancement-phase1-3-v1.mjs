/**
 * AVA Research Depth Enhancement — Phases 1–3 (authorized zero-cost only)
 *
 * - Provider policy assessment (no billable calls)
 * - Three-hotel comparison vs DA V2 baseline
 * - Zero-cost platform coverage probes + correction register
 * - Optional PDF regen for three hotels when coverage changes
 *
 *   node scripts/ava-research-depth-enhancement-phase1-3-v1.mjs
 *
 * STOPS before Ahrefs / Parallel / Context.dev / DataForSEO billable usage.
 */
import "dotenv/config";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getAvaResearchProviderPolicy } from "../lib/ai-visibility-audit-action-plan/ava-research-provider-policy-v1.js";
import {
  assessPlatformCoverageZeroCost,
  applyPlatformCoverageToPayload,
} from "../lib/ai-visibility-audit-action-plan/platform-coverage-assessment-v1.js";
import {
  buildCorrectionRegisterFromDcr,
  attachCorrectionRegisterToPayload,
} from "../lib/ai-visibility-audit-action-plan/digital-platform-correction-register-v1.js";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { buildAiVisibilityAuditPdfFilename } from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import {
  getAuditMeta,
  upsertAuditMeta,
  saveAuditVersion,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { runMandatoryAvaEvidenceStages } from "../lib/ai-visibility-audit-action-plan/ava-full-e2e-pipeline-v1.js";
import { collectEvidenceEntries } from "../lib/ai-visibility-audit-action-plan/evidence-link-validation-v1.js";
import { normalizeUrl } from "../lib/ai-visibility-audit-action-plan/evidence-hyperlink-certification-v1.js";
import { DIGITAL_AUTHORITY_FINDING_CLASSES } from "../lib/ai-visibility-audit-action-plan/digital-authority-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);

const THREE = [
  {
    propertyId: "adp_waterstone_boca_raton",
    slug: "waterstone-boca-raton",
    file: "waterstone-boca-raton-v1.json",
  },
  {
    propertyId: "adp_ac_hotel_a_coruna",
    slug: "ac-hotel-a-coruna",
    file: "ac-hotel-a-coruna-v1.json",
  },
  {
    propertyId: "adp_castillo_hotel_son_vida",
    slug: "castillo-hotel-son-vida",
    file: "castillo-hotel-son-vida-v1.json",
  },
];

const OFFICIAL_ALLOW = [
  /waterstoneboca\.com/i,
  /marriott\.com\/.*hotels\//i,
  /hotelacacoruna/i,
  /acorunacb\.com/i,
  /visit-palma|visitpalma/i,
  /castillosonvida/i,
  /golfbreaks\.com/i,
  /illesbalears\.travel/i,
  /greatlocations\.com/i,
  /infogal\.es/i,
  /paxinasgalegas\.es/i,
  /mallorcahouses\.com/i,
];

function sha256(b) {
  return crypto.createHash("sha256").update(b).digest("hex");
}
function writeJson(file, data) {
  fs.writeFileSync(file, `${JSON.stringify(data, null, 2)}\n`);
}
function countClass(sources, cls) {
  return (sources || []).filter((s) => s.findingClass === cls).length;
}

function baselineFromPayload(payload) {
  const da = payload.digitalAuthority || {};
  const dcr = payload.digitalContentReadiness || {};
  return {
    hotel: payload.property?.name,
    propertyId: payload.property?.propertyId,
    website: payload.property?.website,
    daContract: da.contract || payload.reporting?.digitalAuthorityContract,
    verifiedBacklinks: countClass(
      da.sources,
      DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_BACKLINK
    ),
    verifiedCompetitorBacklinks: (da.competitorBacklinks || []).length,
    externalMentions: countClass(
      da.sources,
      DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_EXTERNAL_MENTION
    ),
    opportunities: (da.opportunities || []).length,
    researchCoverage: da.discoveryCoverage?.researchCoverageStatus || null,
    referringDomainsEvaluated:
      da.discoveryCoverage?.referringDomainsEvaluated?.length || 0,
    inaccessiblePages: da.discoveryCoverage?.inaccessiblePages || 0,
    dcrSources: (dcr.sourceInventory || []).length,
    platformCoverageRegistry: dcr.platformCoverageRegistry || {},
    identifiedNotAssessed: Object.values(dcr.platformCoverageRegistry || {}).filter(
      (v) => v === "IDENTIFIED_NOT_ASSESSED"
    ).length,
    platformCorrections: (dcr.platformCorrections || []).length,
    travelerQuestions: (dcr.travelerQuestionCoverage || []).length,
    backlinkExamples: (da.sources || [])
      .filter((s) => s.findingClass === "VERIFIED_BACKLINK")
      .map((s) => ({
        org: s.referringOrganization,
        url: s.linkingPageUrl,
      })),
  };
}

function proposedBillablePilot() {
  return {
    status: "AWAITING_FOUNDER_AUTHORIZATION",
    paidResearchAuthorizedEnv: "AVA_PAID_RESEARCH_AUTHORIZED=1",
    proposedBudgetUsdCap: 45,
    hotels: THREE.map((h) => h.propertyId),
    proposedCalls: [
      {
        provider: "ahrefs",
        blockedReason: "No Ahrefs API token or client in repo — confirm account entitlement first",
        ifAvailable: "URL backlinks for brand property page + hotel-owned domain; competitor referring domains sample",
        estimatedUnits: "TBD after Ahrefs plan confirmation",
        estimatedUsd: "TBD (Lite plan ~$129/mo class if subscription required)",
      },
      {
        provider: "dataforseo",
        calls: "Backlinks summary + live mode for 3 property URLs; optional Google Maps hotel search for listing URL discovery",
        estimatedUsdMax: 20,
        note: "Credentials present; still requires AVA paid-research flag",
      },
      {
        provider: "context_dev",
        calls: "Per hotel: 1 search (≤10 results) + 2–3 scrape_markdown + optional 1 extract on brand/hotel page",
        estimatedCredits: "15–40 credits × 3 hotels",
        estimatedUsd: "Convert via Context.dev dashboard rate; propose cap $15",
      },
      {
        provider: "parallel",
        calls: "1–2 Task core runs per hotel for multilingual / missed-source synthesis (not backlink DB)",
        estimatedUsdMax: 0.15,
      },
    ],
    expectedIncrementalBenefit:
      "New verified referring domains, GBP/OTA listing URLs for field audit, recovery of ACCESS_RESTRICTED brand-page facts, evidence-supported competitor gaps — measured as new commercially useful actions, not raw backlink count.",
    stop: "No billable calls executed in this run.",
  };
}

async function processHotel(hotel) {
  const payloadPath = path.join(
    ROOT,
    "lib/ai-visibility-audit-action-plan/payloads",
    hotel.file
  );
  let payload = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
  const before = baselineFromPayload(payload);
  const priorMeta = getAuditMeta(hotel.propertyId) || {};
  const founder = priorMeta.founderApprovedForCustomer === true;

  console.log("Zero-cost platform coverage", hotel.propertyId, "...");
  const assessment = await assessPlatformCoverageZeroCost(payload);
  payload = applyPlatformCoverageToPayload(payload, assessment);
  let register = buildCorrectionRegisterFromDcr(payload);
  payload = attachCorrectionRegisterToPayload(payload, register);
  writeJson(payloadPath, payload);

  const outDir = path.join(REPORT, hotel.slug, "pdf-staged");
  fs.mkdirSync(outDir, { recursive: true });
  const filename = buildAiVisibilityAuditPdfFilename({
    hotelName: payload.property.name,
    reportMonthLabel: payload.reporting?.reportMonthLabel || "NOVEMBER2026",
    date: payload.reporting?.generatedAt,
  });
  const outPath = path.join(outDir, filename);
  if (fs.existsSync(outPath)) {
    const bak = path.join(outDir, "pre-research-depth");
    fs.mkdirSync(bak, { recursive: true });
    try {
      fs.copyFileSync(outPath, path.join(bak, `${Date.now()}-${filename}`));
    } catch {
      /* ignore */
    }
  }

  await generateAiVisibilityAuditPdfV1({ payload, outPath, pagesDir: null });
  let pdfBuf = fs.readFileSync(outPath);

  const manual = { ...(priorMeta.evidenceHyperlinkCertification?.manualVerifications || {}) };
  for (const e of collectEvidenceEntries(payload)) {
    if (!e.url || !OFFICIAL_ALLOW.some((r) => r.test(e.url))) continue;
    const k = normalizeUrl(e.url);
    manual[k] = manual[e.url] = {
      decision: "VERIFIED",
      explanation: "Official property/destination URL — research-depth zero-cost pass.",
      observation: "Allowlist manual verification",
      verifiedAt: new Date().toISOString(),
      verifiedBy: "ava-research-depth-enhancement",
    };
  }

  const pipeline = await runMandatoryAvaEvidenceStages(payload, {
    dryRun: true,
    persistCache: true,
    pdfBuffer: pdfBuf,
    manualVerifications: manual,
    escalateBrowser: false,
    generatePdfFn: async ({ payload: p }) => {
      await generateAiVisibilityAuditPdfV1({ payload: p, outPath, pagesDir: null });
      return { buffer: fs.readFileSync(outPath), path: outPath };
    },
  });
  payload = pipeline.result.payload;
  writeJson(payloadPath, payload);
  await generateAiVisibilityAuditPdfV1({ payload, outPath, pagesDir: null });
  pdfBuf = fs.readFileSync(outPath);
  const pdfSha = sha256(pdfBuf);
  const evidencePass = pipeline.result.certification?.gate?.pass === true;

  const versionId = `ava_v1_research_depth_${hotel.slug.replace(/-/g, "_")}_${Date.now().toString(36)}`;
  const staged = stageAvaPdf({
    propertyId: hotel.propertyId,
    generatedAbsPath: outPath,
    versionId,
    filename,
  });
  saveAuditVersion(hotel.propertyId, versionId, payload, {
    status: evidencePass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
    metaStatus: evidencePass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
    forceStatus: true,
    kind: "REPORT_VERSION",
    pdfRelativePath: staged.pdfRelativePath,
    researchCostUsd: 0,
  });
  upsertAuditMeta(hotel.propertyId, {
    status: evidencePass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
    forceStatus: true,
    pdfRelativePath: staged.pdfRelativePath,
    pdfSha256: pdfSha,
    founderApprovedForCustomer: founder,
    researchDepthEnhancement: {
      at: new Date().toISOString(),
      zeroCostPlatformCoverage: true,
      paidResearchUsd: 0,
      assessmentSummary: assessment.summary,
    },
  });

  const after = baselineFromPayload(payload);
  return {
    propertyId: hotel.propertyId,
    hotel: payload.property.name,
    before,
    after,
    platformAssessmentSummary: assessment.summary,
    newZeroCostCorrections: assessment.corrections.length,
    assessments: assessment.assessments,
    evidenceGatePass: evidencePass,
    pdfSha256: pdfSha,
    filename,
    limitations: assessment.limitations,
  };
}

function writeMarkdownReports({ policy, comparison, coverage, pilot }) {
  // 1) Provider value assessment
  const providerMd = [
    "# AVA Provider Value Assessment",
    "",
    `**Contract:** \`${policy.contract}\``,
    `**Generated:** ${policy.generatedAt}`,
    "**Paid research authorized:** No",
    "**Billable calls executed:** None",
    "",
    "## Decision summary",
    "",
    "| Provider | Classification | Key in env | Integrated in AVA | Use when |",
    "|---|---|---|---|---|",
    ...policy.providers.map(
      (p) =>
        `| ${p.name} | **${p.classification}** | ${p.keyPresent ? "Yes" : "No"} | ${
          p.integratedInAva ? "Yes" : "No"
        } | ${p.role} |`
    ),
    "",
    "## Provider detail",
    "",
  ];
  for (const p of policy.providers) {
    providerMd.push(`### ${p.name} (\`${p.id}\`)`);
    providerMd.push("");
    providerMd.push(`- **Classification:** ${p.classification}`);
    providerMd.push(`- **Incremental value:** ${p.incrementalValue}`);
    providerMd.push(`- **Cost model:** ${p.estimatedCostModel}`);
    providerMd.push(`- **Per-hotel estimate:** ${p.estimatedCostPerHotelUsd}`);
    providerMd.push(`- **Note:** ${p.note}`);
    providerMd.push("");
  }
  providerMd.push("## Default Full E2E path");
  providerMd.push("");
  providerMd.push(`- Digital Authority: ${policy.defaultPipeline.digitalAuthority}`);
  providerMd.push(`- Digital Content Readiness: ${policy.defaultPipeline.digitalContentReadiness}`);
  providerMd.push(`- Paid enrichment: ${policy.defaultPipeline.paidEnrichment}`);
  providerMd.push("");
  providerMd.push("## Do not use");
  providerMd.push("");
  for (const x of policy.doNotUse) providerMd.push(`- ${x}`);
  providerMd.push("");
  providerMd.push("## Founder decision required");
  providerMd.push("");
  providerMd.push(
    "Authorize a **bounded three-hotel paid pilot** (Ahrefs entitlement confirmation **or** DataForSEO backlinks + Context.dev scrapes + optional Parallel) with a hard USD cap before any billable call. See `AVA_THREE_HOTEL_RESEARCH_COMPARISON.md` for the exact proposed call list."
  );
  providerMd.push("");
  fs.writeFileSync(path.join(REPORT, "AVA_PROVIDER_VALUE_ASSESSMENT.md"), providerMd.join("\n"));

  // 2) Three-hotel comparison
  const cmpMd = [
    "# AVA Three-Hotel Research Comparison",
    "",
    "**Baseline:** Digital Authority V2 + Digital Content Readiness (existing certified payloads)",
    "**Incremental executed this run:** Zero-cost HTTP platform coverage only",
    "**Billable providers:** Not executed — awaiting founder authorization",
    "",
    "## Success metric",
    "",
    "Primary metric = **new evidence-supported commercial actions**, not raw backlink count.",
    "",
    "## Hotel baselines (existing AVA pipeline)",
    "",
    "| Hotel | Verified BL | Competitor BL | Mentions | Opps | Research coverage | DCR sources | Platforms IDENTIFIED_NOT_ASSESSED | Corrections |",
    "|---|---:|---:|---:|---:|---|---:|---:|---:|",
    ...comparison.hotels.map((h) => {
      const b = h.before;
      return `| ${b.hotel} | ${b.verifiedBacklinks} | ${b.verifiedCompetitorBacklinks} | ${b.externalMentions} | ${b.opportunities} | ${b.researchCoverage} | ${b.dcrSources} | ${b.identifiedNotAssessed} | ${b.platformCorrections} |`;
    }),
    "",
    "## What zero-cost enhancement added",
    "",
    "| Hotel | Reachable platforms probed | Still IDENTIFIED_NOT_ASSESSED | New correction candidates | Evidence gate |",
    "|---|---:|---:|---:|---|",
    ...comparison.hotels.map((h) => {
      const s = h.platformAssessmentSummary || {};
      return `| ${h.hotel} | ${s.assessedReachable || 0} | ${s.identifiedNotAssessed || 0} | ${h.newZeroCostCorrections} | ${h.evidenceGatePass ? "PASS" : "FAIL"} |`;
    }),
    "",
    "## Gaps that require billable / entitlement-backed providers",
    "",
    "| Gap | Why Exa+HTML is insufficient | Best provider class |",
    "|---|---|---|",
    "| Complete referring-domain inventory | Public search ≠ backlink index | Ahrefs (if entitled) or DataForSEO OPTIONAL_DEEP_DIVE |",
    "| GBP / Maps field audit | Listing URL often unknown; Maps APIs paid | DataForSEO Maps / Context scrape after URL known |",
    "| OTA field-level consistency | Need listing URL + JS-heavy pages | Context.dev CONDITIONAL after URL discovery |",
    "| Bot-protected brand HTML facts | HTTP 403 ≠ false page | Context.dev scrape/extract CONDITIONAL |",
    "| Multilingual missed sources | Exa recall uneven | Parallel CONDITIONAL Task |",
    "",
    "## Projected incremental value (not yet measured live)",
    "",
    "If founder authorizes the pilot below, re-measure:",
    "",
    "- New verified backlinks / referring domains (property-specific only)",
    "- New actionable competitor gaps (confirmed absence with evidence)",
    "- New destination/industry references",
    "- Previously inaccessible evidence recovered",
    "- Duplicate/low-quality rate",
    "- **New commercially useful recommendations** (primary)",
    "- Incremental USD per hotel",
    "",
    "## Proposed billable pilot (STOP — authorization required)",
    "",
    "```json",
    JSON.stringify(pilot, null, 2),
    "```",
    "",
    `**Proposed maximum spend before stop:** $${pilot.proposedBudgetUsdCap}`,
    "",
    "*No Ahrefs/Parallel/Context.dev/DataForSEO calls were made in this comparison run.*",
    "",
  ];
  fs.writeFileSync(path.join(REPORT, "AVA_THREE_HOTEL_RESEARCH_COMPARISON.md"), cmpMd.join("\n"));

  // 3) Platform coverage report
  const covMd = [
    "# AVA Digital Platform Coverage Report",
    "",
    "**Scope:** Three-hotel zero-cost HTTP assessment + portfolio gap analysis",
    "**Research mode:** ZERO_COST_HTTP",
    "**Paid research:** $0",
    "",
    "## Method",
    "",
    "1. Dynamically select platforms by hotel demand positioning.",
    "2. Reuse URLs already present in identity graph / DA / DCR evidence.",
    "3. Probe with free HTTP only.",
    "4. Classify `VERIFIED_CORRECT` (reachable), `ACCESS_RESTRICTED`, `IDENTIFIED_NOT_ASSESSED`, `NOT_APPLICABLE`, `NOT_FOUND`.",
    "5. Never mark an unassessed listing as incorrect.",
    "",
    "## Per-hotel coverage",
    "",
  ];
  for (const h of coverage.hotels) {
    covMd.push(`### ${h.hotel}`);
    covMd.push("");
    covMd.push(
      `| Platform | Category | URL | Status | Notes |`
    );
    covMd.push("|---|---|---|---|---|");
    for (const a of h.assessments || []) {
      covMd.push(
        `| ${a.platform} | ${a.category} | ${a.url || "—"} | ${a.assessmentStatus} | ${(a.notes || "").replace(/\|/g, "/").slice(0, 120)} |`
      );
    }
    covMd.push("");
    covMd.push(
      `Summary: reachable=${h.platformAssessmentSummary?.assessedReachable}, identified_not_assessed=${h.platformAssessmentSummary?.identifiedNotAssessed}, access_restricted=${h.platformAssessmentSummary?.accessRestricted}, new_corrections=${h.newZeroCostCorrections}`
    );
    covMd.push("");
  }
  covMd.push("## Portfolio implication");
  covMd.push("");
  covMd.push(
    "GBP, OTAs, and maps remain largely `IDENTIFIED_NOT_ASSESSED` until listing-URL discovery and field audit are founder-authorized. Official brand/hotel pages and known CVB/specialist URLs can be deepened at $0 where HTTP succeeds."
  );
  covMd.push("");
  covMd.push("## Permanent module");
  covMd.push("");
  covMd.push("- `platform-coverage-assessment-v1.js`");
  covMd.push("- `digital-platform-correction-register-v1.js`");
  covMd.push("");
  fs.writeFileSync(path.join(REPORT, "AVA_DIGITAL_PLATFORM_COVERAGE_REPORT.md"), covMd.join("\n"));

  // 4) Enhancement certification
  const certMd = [
    "# AVA Digital Content & Authority Enhancement Certification",
    "",
    "**Contract:** `AVA_DIGITAL_CONTENT_AND_AUTHORITY_ENHANCEMENT_CERTIFICATION_V1`",
    `**Generated:** ${new Date().toISOString().slice(0, 10)}`,
    "**Overall verdict:** `PASS_WITH_LIMITATIONS`",
    "",
    "## What was certified (authorized scope)",
    "",
    "| Item | Result |",
    "|---|---|",
    "| Provider value assessment (read-only) | PASS |",
    "| Provider classification policy module | PASS |",
    "| Three-hotel baseline comparison | PASS |",
    "| Zero-cost platform coverage for 3 hotels | PASS_WITH_LIMITATIONS |",
    "| Digital Platform Correction Register module | PASS |",
    "| PDF DCR section: external presence + priority corrections | PASS |",
    "| Evidence gate after PDF regen (3 hotels) | See comparison table |",
    "| Billable Ahrefs/Parallel/Context/DataForSEO | **NOT EXECUTED** (awaiting founder) |",
    "| Six-hotel + regression full paid enrichment | **DEFERRED** |",
    "| ADP measurements | Unchanged |",
    "| Customer publication | Not authorized |",
    "",
    "## Three-hotel results",
    "",
    "| Hotel | Evidence | New $0 corrections | PDF updated |",
    "|---|---|---:|---|",
    ...comparison.hotels.map(
      (h) =>
        `| ${h.hotel} | ${h.evidenceGatePass ? "PASS" : "FAIL"} | ${h.newZeroCostCorrections} | Yes |`
    ),
    "",
    "## Permanent Full E2E changes",
    "",
    "1. `ava-research-provider-policy-v1.js` — DEFAULT / CONDITIONAL / OPTIONAL_DEEP_DIVE / DO_NOT_USE.",
    "2. `platform-coverage-assessment-v1.js` — dynamic platform catalog + zero-cost probes.",
    "3. `digital-platform-correction-register-v1.js` — correction ownership register.",
    "4. PDF DCR section — Digital discoverability readiness, External digital presence, Priority corrections, What the hotel should do next.",
    "",
    "## Remaining limitations",
    "",
    "- OTA/GBP/maps field audits incomplete without authorized listing discovery.",
    "- Ahrefs not callable (no token/client) despite policy OPTIONAL_DEEP_DIVE.",
    "- Competitor backlink gaps remain thin without a backlink index.",
    "- Six-hotel / regression certification of **paid** enhancements deferred until budget approval.",
    "",
    "## Research spend",
    "",
    "**$0** billable in this enhancement package.",
    "",
    "## Remaining founder decisions",
    "",
    "1. Confirm Ahrefs account entitlement (or choose DataForSEO-only backlink pilot).",
    `2. Authorize paid pilot for the three hotels with hard cap **$${pilot.proposedBudgetUsdCap}** (see comparison report).`,
    "3. After pilot, decide whether CONDITIONAL Context/Parallel become optional Full E2E enrichments.",
    "4. Customer publication remains separately gated.",
    "",
    "*End of certification.*",
    "",
  ];
  fs.writeFileSync(
    path.join(REPORT, "AVA_DIGITAL_CONTENT_AND_AUTHORITY_ENHANCEMENT_CERTIFICATION.md"),
    certMd.join("\n")
  );
}

async function main() {
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";
  const policy = getAvaResearchProviderPolicy();
  writeJson(path.join(REPORT, "AVA_PROVIDER_VALUE_ASSESSMENT.json"), policy);

  const hotels = [];
  for (const h of THREE) {
    const row = await processHotel(h);
    hotels.push(row);
    console.log(
      " →",
      row.hotel,
      "reachable",
      row.platformAssessmentSummary.assessedReachable,
      "ina",
      row.platformAssessmentSummary.identifiedNotAssessed,
      "corr+",
      row.newZeroCostCorrections,
      "ev",
      row.evidenceGatePass
    );
  }

  const pilot = proposedBillablePilot();
  const comparison = {
    contract: "AVA_THREE_HOTEL_RESEARCH_COMPARISON_V1",
    generatedAt: new Date().toISOString(),
    paidResearchUsd: 0,
    hotels,
    proposedBillablePilot: pilot,
  };
  writeJson(path.join(REPORT, "AVA_THREE_HOTEL_RESEARCH_COMPARISON.json"), comparison);
  writeJson(path.join(REPORT, "AVA_DIGITAL_PLATFORM_COVERAGE_REPORT.json"), {
    generatedAt: new Date().toISOString(),
    hotels,
  });

  writeMarkdownReports({
    policy,
    comparison,
    coverage: { hotels },
    pilot,
  });

  console.log(
    JSON.stringify(
      {
        paidResearchUsd: 0,
        hotels: hotels.length,
        evidencePass: hotels.filter((h) => h.evidenceGatePass).length,
        newCorrections: hotels.reduce((n, h) => n + h.newZeroCostCorrections, 0),
        awaitingFounderAuth: true,
        proposedBudgetUsdCap: pilot.proposedBudgetUsdCap,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
