/**
 * Digital Authority V2 deep backlink pilot — six hotels
 *   node scripts/ava-digital-authority-v2-six-pilot-v1.mjs
 */
import "dotenv/config";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runDigitalAuthorityDiscoveryV2,
  inspectCandidatePage,
} from "../lib/ai-visibility-audit-action-plan/digital-authority-discovery-v2.js";
import {
  appendDigitalAuthorityActions,
  DIGITAL_AUTHORITY_FINDING_CLASSES,
} from "../lib/ai-visibility-audit-action-plan/digital-authority-v1.js";
import { buildPropertyUrlIdentityGraph } from "../lib/ai-visibility-audit-action-plan/property-url-identity-graph-v1.js";
import { runCustomerQualityAssuranceLoop } from "../lib/ai-visibility-audit-action-plan/customer-quality-remediation-v1.js";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { buildAiVisibilityAuditPdfFilename } from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import {
  getAuditMeta,
  upsertAuditMeta,
  saveAuditVersion,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const EXTRAS = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "lib/ai-visibility-audit-action-plan/digital-authority/wave1-da-v2-identity-extras-v1.json"
    ),
    "utf8"
  )
);

const PILOT = [
  {
    propertyId: "adp_waterstone_boca_raton",
    slug: "waterstone-boca-raton",
    payload: "waterstone-boca-raton-v1.json",
    seedPages: [
      "https://greatlocations.com/things-to-do-in-boca-raton-this-summer/",
    ],
  },
  {
    propertyId: "adp_castillo_hotel_son_vida",
    slug: "castillo-hotel-son-vida",
    payload: "castillo-hotel-son-vida-v1.json",
    seedPages: [
      "http://www.visit-palma.com/hotel-castillo-son-vida",
      "https://www.golfbreaks.com/en-gb/holidays/palma/castillo-hotel-son-vida/",
      "https://www.visitpalma.com/en/restaurants/pln704/es-castell",
    ],
  },
  {
    propertyId: "adp_sheraton_mallorca_arabella_golf",
    slug: "sheraton-mallorca-arabella-golf",
    payload: "sheraton-mallorca-arabella-golf-v1.json",
    seedPages: [
      "http://www.visit-palma.com/hotel-sheraton-arabella-golf",
      "https://golfnews.co.uk/golf-news/arabella-resort-mallorca/",
    ],
  },
  {
    propertyId: "adp_ac_hotel_a_coruna",
    slug: "ac-hotel-a-coruna",
    payload: "ac-hotel-a-coruna-v1.json",
    seedPages: [
      "https://acorunacb.com/ficha/ac-by-marriot-a-coruna/",
      "https://acorunacb.com/ficha/espacios-mice-ac-by-marriot-a-coruna/",
      "https://acorunacb.com/planea-tu-evento/alojamiento/hoteles/",
    ],
  },
  {
    propertyId: "adp_grand_fiesta_americana_chapultepec",
    slug: "grand-fiesta-americana-chapultepec",
    payload: "grand-fiesta-americana-chapultepec-v1.json",
    seedPages: [],
  },
  {
    propertyId: "adp_fiesta_americana_condesa_cancun_ai",
    slug: "fiesta-americana-condesa-cancun-ai",
    payload: "fiesta-americana-condesa-cancun-ai-v1.json",
    seedPages: ["https://turismocancun.mx/"],
  },
];

function writeJson(file, data) {
  fs.writeFileSync(file, `${JSON.stringify(data, null, 2)}\n`);
}

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function countClass(sources, cls) {
  return (sources || []).filter((s) => s.findingClass === cls).length;
}

function v1BaselineFromPayload(payload) {
  const da = payload.digitalAuthority || {};
  return {
    searchesPerformed: "manual_partial_fixture",
    candidateReferringPages: (da.sources || []).length,
    verifiedHotelBacklinks: countClass(
      da.sources,
      DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_BACKLINK
    ),
    verifiedCompetitorBacklinks: (da.competitorBacklinks || []).length,
    verifiedExternalMentions: countClass(
      da.sources,
      DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_EXTERNAL_MENTION
    ),
    confirmedCompetitiveGaps: (da.competitorGaps || []).filter(
      (g) => g.gapClass === "CONFIRMED_SUBJECT_ABSENCE"
    ).length,
    potentialBacklinkOpportunities: (da.opportunities || []).filter((o) =>
      /BACKLINK/i.test(o.findingClass || "")
    ).length,
    researchCoverage: da.inventoryCompleteness || "PARTIAL_PUBLIC_SEARCH",
    researchCost: 0,
  };
}

function mergeSources(prior = [], next = []) {
  const map = new Map();
  const keyOf = (s) =>
    `${s.findingClass}|${String(s.linkingPageUrl || "").toLowerCase()}|${String(
      s.hotelDestinationUrl || ""
    ).toLowerCase()}`;
  for (const s of [...prior, ...next]) {
    if (!s) continue;
    // Drop first-party misclassified as external (CQA already fixed some)
    if (s.findingClass === "FIRST_PARTY_OFFICIAL_SITE") continue;
    map.set(keyOf(s), s);
  }
  return [...map.values()];
}

async function processHotel(hotel) {
  const payloadPath = path.join(
    ROOT,
    "lib/ai-visibility-audit-action-plan/payloads",
    hotel.payload
  );
  let payload = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
  const before = v1BaselineFromPayload(payload);
  const priorMeta = getAuditMeta(hotel.propertyId) || {};
  const founder = priorMeta.founderApprovedForCustomer === true;
  const extras = EXTRAS.hotels[hotel.propertyId] || {};

  console.log("DA-V2 discovering", hotel.propertyId, "...");
  const discovery = await runDigitalAuthorityDiscoveryV2({
    payload,
    identityExtras: extras,
    competitors: extras.competitors || [],
    demandSegments: extras.demandSegments || [],
    maxSearches: 10,
    maxPagesInspected: 24,
    delayMs: 300,
  });

  // Seed known destination pages (re-verify hyperlinks)
  const graph =
    discovery.propertyUrlIdentityGraph ||
    buildPropertyUrlIdentityGraph(payload, extras);
  for (const seed of hotel.seedPages || []) {
    const inspection = await inspectCandidatePage(seed, graph, {
      strategy: "SEED_KNOWN_DESTINATION",
      organization: new URL(seed).hostname,
    });
    for (const f of inspection.findings || []) {
      discovery.digitalAuthority.sources = mergeSources(
        discovery.digitalAuthority.sources,
        [f]
      );
    }
    discovery.coverage.candidatePagesInspected =
      (discovery.coverage.candidatePagesInspected || 0) + 1;
  }

  // Merge with prior DA sources that remain valid classifications
  const priorSources = (payload.digitalAuthority?.sources || []).filter(
    (s) =>
      s.findingClass === "VERIFIED_BACKLINK" ||
      s.findingClass === "VERIFIED_EXTERNAL_MENTION"
  );
  discovery.digitalAuthority.sources = mergeSources(
    priorSources,
    discovery.digitalAuthority.sources
  );
  discovery.digitalAuthority.competitorBacklinks = mergeSources(
    payload.digitalAuthority?.competitorBacklinks || [],
    discovery.digitalAuthority.competitorBacklinks || []
  );
  discovery.digitalAuthority.opportunities = mergeSources(
    payload.digitalAuthority?.opportunities || [],
    discovery.digitalAuthority.opportunities || []
  ).slice(0, 10);

  // Refresh coverage counts after merge
  const sources = discovery.digitalAuthority.sources;
  discovery.coverage.verifiedBacklinks = countClass(
    sources,
    DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_BACKLINK
  );
  discovery.coverage.verifiedExternalMentions = countClass(
    sources,
    DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_EXTERNAL_MENTION
  );
  discovery.digitalAuthority.discoveryCoverage = discovery.coverage;
  discovery.digitalAuthority.executiveFinding = discovery.coverage.verifiedBacklinks
    ? `Public V2 discovery verified ${discovery.coverage.verifiedBacklinks} property-specific third-party hyperlink(s) and ${discovery.coverage.verifiedExternalMentions} external reference(s). Brand and hotel-operated destinations were evaluated separately.`
    : discovery.digitalAuthority.executiveFinding;

  payload = appendDigitalAuthorityActions(payload, discovery.digitalAuthority, {
    paidResearchAuthorized: false,
  });
  payload.digitalAuthority.contract = "AVA_DIGITAL_AUTHORITY_V2";
  payload.digitalAuthority.propertyUrlIdentityGraph = graph;
  payload.digitalAuthority.discoveryCoverage = discovery.coverage;
  payload.reporting = {
    ...(payload.reporting || {}),
    digitalAuthorityContract: "AVA_DIGITAL_AUTHORITY_V2",
    digitalAuthorityGeneratedAt: new Date().toISOString(),
    researchCostUsd: 0,
    paidResearchAuthorized: false,
  };

  // CQA + PDF
  const outDir = path.join(REPORT, hotel.slug, "pdf-staged");
  fs.mkdirSync(outDir, { recursive: true });
  const filename = buildAiVisibilityAuditPdfFilename({
    hotelName: payload.property.name,
    reportMonthLabel: payload.reporting?.reportMonthLabel || "NOVEMBER2026",
    date: payload.reporting?.generatedAt,
  });
  const outPath = path.join(outDir, filename);
  if (fs.existsSync(outPath)) {
    const bak = path.join(outDir, "pre-da-v2");
    fs.mkdirSync(bak, { recursive: true });
    fs.copyFileSync(outPath, path.join(bak, `${Date.now()}-${filename}`));
  }

  const cqa = await runCustomerQualityAssuranceLoop(payload, {
    limits: { maxIterations: 3, maxPdfRegenerations: 2 },
    regeneratePdfFn: async (p) => {
      try {
        await generateAiVisibilityAuditPdfV1({ payload: p, outPath, pagesDir: null });
      } catch (err) {
        console.warn("PDF regen:", err.message);
      }
    },
  });
  payload = cqa.payload;
  writeJson(payloadPath, payload);
  await generateAiVisibilityAuditPdfV1({ payload, outPath, pagesDir: null });
  const pdfBuf = fs.readFileSync(outPath);
  const versionId = `ava_v1_da2_${hotel.slug.replace(/-/g, "_")}_${Date.now().toString(36)}`;
  const staged = stageAvaPdf({
    propertyId: hotel.propertyId,
    generatedAbsPath: outPath,
    versionId,
    filename,
  });
  saveAuditVersion(hotel.propertyId, versionId, payload, {
    status: cqa.customerQualityCertified ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
    metaStatus: cqa.customerQualityCertified ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
    forceStatus: true,
    kind: "REPORT_VERSION",
    pdfRelativePath: staged.pdfRelativePath,
    researchCostUsd: 0,
    validation: {
      enhancement: "digital-authority-v2",
      customerQualityCertified: cqa.customerQualityCertified,
      founderApprovedForCustomer: founder,
      customerReadyBlocked: true,
      pdfSha256: staged.sha256,
    },
  });
  upsertAuditMeta(hotel.propertyId, {
    status: cqa.customerQualityCertified ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
    forceStatus: true,
    pdfRelativePath: staged.pdfRelativePath,
    pdfSha256: sha256(pdfBuf),
    founderApprovedForCustomer: founder,
    founderApprovedForCustomerAt: priorMeta.founderApprovedForCustomerAt || null,
    digitalAuthorityV2: {
      contract: "AVA_DIGITAL_AUTHORITY_V2",
      at: new Date().toISOString(),
      coverage: discovery.coverage,
      paidResearchUsd: 0,
    },
  });

  const after = {
    searchesPerformed: discovery.coverage.searchesCompleted,
    candidateReferringPages: discovery.coverage.candidatePagesDiscovered,
    pagesInspected: discovery.coverage.candidatePagesInspected,
    verifiedHotelBacklinks: discovery.coverage.verifiedBacklinks,
    verifiedCompetitorBacklinks: (
      payload.digitalAuthority?.competitorBacklinks || []
    ).length,
    verifiedExternalMentions: discovery.coverage.verifiedExternalMentions,
    confirmedCompetitiveGaps: (payload.digitalAuthority?.competitorGaps || []).filter(
      (g) => g.gapClass === "CONFIRMED_SUBJECT_ABSENCE"
    ).length,
    potentialBacklinkOpportunities: (payload.digitalAuthority?.opportunities || []).filter(
      (o) => /BACKLINK/i.test(o.findingClass || "")
    ).length,
    researchCoverage: discovery.coverage.researchCoverageStatus,
    researchCost: 0,
    strategiesCompleted: discovery.coverage.strategiesCompleted,
  };

  return {
    propertyId: hotel.propertyId,
    name: payload.property.name,
    before,
    after,
    verifiedBacklinkExamples: (payload.digitalAuthority?.sources || [])
      .filter((s) => s.findingClass === "VERIFIED_BACKLINK")
      .map((s) => ({
        referringOrganization: s.referringOrganization,
        linkingPageUrl: s.linkingPageUrl,
        hotelDestinationUrl: s.hotelDestinationUrl,
        destinationUrlClass: s.destinationUrlClass,
      })),
    opportunities: (payload.digitalAuthority?.opportunities || [])
      .slice(0, 3)
      .map((o) => ({
        title: o.title,
        organization: o.organization,
        url: o.targetOrganizationUrl,
        score: o.priorityScore,
      })),
    identityGraphUrlCount:
      payload.digitalAuthority?.propertyUrlIdentityGraph?.urls?.length || 0,
    customerQualityCertified: cqa.customerQualityCertified === true,
    pdfSha256: sha256(pdfBuf),
    filename,
    founderApprovedForCustomer: founder,
  };
}

async function main() {
  if (!process.env.EXA_API_KEY) {
    console.error("EXA_API_KEY required for V2 public discovery");
    process.exit(1);
  }
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";
  const rows = [];
  for (const h of PILOT) {
    const row = await processHotel(h);
    rows.push(row);
    console.log(
      " → BL",
      row.after.verifiedHotelBacklinks,
      "mentions",
      row.after.verifiedExternalMentions,
      "searches",
      row.after.searchesPerformed,
      "CQA",
      row.customerQualityCertified
    );
  }

  const report = {
    contract: "AVA_DIGITAL_AUTHORITY_V2_PILOT_RESULTS_V1",
    generatedAt: new Date().toISOString(),
    paidResearchUsd: 0,
    publicationAuthorized: false,
    rootCauseSummary:
      "V1 used manual destination fixtures with few pages inspected for outbound hrefs; no systematic multi-strategy search, no URL identity graph separating brand vs hotel sites, and no adaptive coverage tracking — yielding only Visit Palma property hyperlinks as verified portfolio backlinks.",
    hotels: rows,
    totals: {
      hotels: rows.length,
      verifiedBacklinks: rows.reduce((n, r) => n + r.after.verifiedHotelBacklinks, 0),
      verifiedMentions: rows.reduce((n, r) => n + r.after.verifiedExternalMentions, 0),
      cqaPass: rows.filter((r) => r.customerQualityCertified).length,
      searches: rows.reduce((n, r) => n + (r.after.searchesPerformed || 0), 0),
      pagesInspected: rows.reduce((n, r) => n + (r.after.pagesInspected || 0), 0),
    },
  };
  fs.mkdirSync(REPORT, { recursive: true });
  writeJson(path.join(REPORT, "AVA_DIGITAL_AUTHORITY_V2_PILOT_RESULTS.json"), report);
  console.log(JSON.stringify(report.totals, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
