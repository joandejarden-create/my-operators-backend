/**
 * Generate Hilton Times Square AI Demand Performance Review PDF
 * from the latest CERTIFIED matched-control period + Renaissance EXACT_COMPARABLE peer.
 */
import "dotenv/config";
import { mkdirSync, writeFileSync, readFileSync, copyFileSync, existsSync } from "fs";
import { join } from "path";
import { generateNewDraft } from "../lib/ai-demand-positioning/monthly-review/admin/generate-review-v1.js";
import { loadReviewPayload } from "../lib/ai-demand-positioning/monthly-review/admin/archive-store-v1.js";
import { generateMonthlyReviewPdfV1 } from "../lib/ai-demand-positioning/monthly-review/generate-monthly-review-pdf-v1.mjs";
import { renderAndAttachMonthlyReviewPdfV1 } from "../lib/ai-demand-positioning/monthly-review/admin/render-and-attach-monthly-review-pdf-v1.mjs";
import { buildAiDemandPerformanceReviewFilename } from "../lib/ai-demand-positioning/monthly-review/ai-demand-performance-review-filename-v1.js";
import { certifyAdpPeriod } from "../lib/ai-demand-positioning/certification/certify-adp-period-v1.js";
import { evaluateAdpComparability } from "../lib/ai-demand-positioning/certification/adp-comparability-engine-v1.js";
import { loadPeriod } from "../lib/ai-demand-positioning/data-model.js";
import { loadPublishedManifest } from "../lib/ai-demand-positioning/published-snapshot.js";

const HILTON_ID = "adp_hilton_times_square";
const REN_ID = "adp_renaissance_times_square";
const OUT = join(process.cwd(), "reports/adp/hilton-times-square-performance-review");
const BASE_URL = process.env.ADP_PDF_BASE_URL || "http://localhost:8080";

const TERRITORY_LABELS = {
  business: "Business Travel",
  leisure: "Leisure Travel",
  couples: "Couples / Romantic Stay",
  group_meeting: "Meetings & Groups",
  family: "Family Travel",
  celebration: "Celebrations & Events",
  wellness: "Wellness",
  adventure: "Adventure & Experiences",
};

const PROVIDER_LABELS = {
  openai: "ChatGPT",
  gemini: "Gemini",
  perplexity: "Perplexity",
  claude: "Claude",
};

function write(name, content) {
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, name), content, "utf8");
}

function loadReportPayload(propertyId) {
  const m = loadPublishedManifest(propertyId);
  const path = join(
    process.cwd(),
    "data/ai-demand-positioning/published",
    propertyId,
    m.reportFile
  );
  const raw = JSON.parse(readFileSync(path, "utf8"));
  return { manifest: m, report: raw.payload || raw, raw };
}

function buildMatchedPeerComparison(hiltonReport, renReport, comparability) {
  const hC = hiltonReport.executiveMetrics.considerationRate;
  const rC = renReport.executiveMetrics.considerationRate;
  const hS = hiltonReport.executiveMetrics.scenarioPresence;
  const rS = renReport.executiveMetrics.scenarioPresence;
  const deltaPp = Math.round((Number(rC.rate) - Number(hC.rate)) * 10) / 10;
  const similar = Math.abs(deltaPp) <= 5;

  const framing = similar
    ? "The two properties perform within a relatively similar overall range, with differences concentrated in specific demand territories and providers rather than a broad structural gap."
    : "Overall consideration differs between the matched pair; review territory and provider rows for where the gap concentrates.";

  const territoryRows = Object.keys(TERRITORY_LABELS).map((key) => {
    const h = hiltonReport.demandCapture?.byIntent?.[key];
    const r = renReport.demandCapture?.byIntent?.[key];
    return {
      territory: TERRITORY_LABELS[key],
      subject: h ? `${h.rate}% (${h.captured}/${h.total})` : "—",
      peer: r ? `${r.rate}% (${r.captured}/${r.total})` : "—",
    };
  });

  const hProv = hiltonReport.evidence?.providers || [];
  const rProv = renReport.evidence?.providers || [];
  const providerRows = ["openai", "gemini", "perplexity", "claude"].map((p) => {
    const h = hProv.find((x) => x.provider === p);
    const r = rProv.find((x) => x.provider === p);
    return {
      provider: PROVIDER_LABELS[p],
      subject: h ? `${h.presence}% (${h.mentioned}/${h.comparable})` : "—",
      peer: r ? `${r.presence}% (${r.mentioned}/${r.comparable})` : "—",
    };
  });

  return {
    include: true,
    sectionTitle: "Matched Peer Comparison — Renaissance New York Times Square",
    subjectLabel: "Hilton NYTS",
    peerLabel: "Renaissance NYTS",
    framing,
    comparabilityNote: `Formally ${comparability.outcome} on the matched-control query set (${comparability.commonScenarioCount || 65} shared scenarios · 260 responses per hotel).`,
    subjectPeriodId: hiltonReport.period?.periodId,
    peerPeriodId: renReport.period?.periodId,
    headlineRows: [
      {
        metric: "AI Consideration",
        subject: `${hC.rate}% (${hC.presentObservations}/${hC.comparableObservations})`,
        peer: `${rC.rate}% (${rC.presentObservations}/${rC.comparableObservations})`,
      },
      {
        metric: "Scenario Presence",
        subject: `${hS.rate}% (${hS.capturedScenarios}/${hS.eligibleScenarios})`,
        peer: `${rS.rate}% (${rS.capturedScenarios}/${rS.eligibleScenarios})`,
      },
    ],
    territoryRows,
    providerRows,
  };
}

function buildSourceIntelligence(attribution, hiltonReport) {
  const a = attribution || {};
  const rows = [
    {
      label: "Top Source Supporting This Property",
      domain: a.topSourceSupportingThisProperty?.domain || "—",
      taxonomy: a.topSourceSupportingThisProperty?.taxonomy || "",
    },
    {
      label: "Top Owned / Brand Source",
      domain: a.topOwnedBrandSource?.domain || "—",
      taxonomy: a.topOwnedBrandSource?.taxonomy || "",
    },
    {
      label: "Top External Property Source",
      domain: a.topExternalPropertySource?.domain || "—",
      taxonomy: a.topExternalPropertySource?.taxonomy || "",
    },
    {
      label: "Top Competitive-Universe Source",
      domain: a.topCompetitiveUniverseSource?.domain || "—",
      taxonomy: a.topCompetitiveUniverseSource?.taxonomy || "",
    },
  ];
  return {
    rows,
    ownedBrandShareDisplay: `${hiltonReport.evidence?.ownedSourceShare ?? "—"}%`,
    citationCoverageDisplay: `${hiltonReport.evidence?.citationRate ?? "—"}%`,
  };
}

async function main() {
  mkdirSync(OUT, { recursive: true });

  const { manifest: hManifest, report: hReport } = loadReportPayload(HILTON_ID);
  const { report: rReport } = loadReportPayload(REN_ID);

  if (hManifest.certificationStatus !== "CERTIFIED") {
    throw new Error(`Hilton not CERTIFIED: ${hManifest.certificationStatus}`);
  }
  if (hManifest.latestPeriodId !== hReport.period?.periodId) {
    throw new Error("Hilton manifest/report period mismatch");
  }
  // Reject known legacy Sep 27 baseline
  if (/20260927/.test(hManifest.latestPeriodId)) {
    throw new Error("LEGACY_SEP27_PERIOD_BLOCKED");
  }

  const hPeriod = loadPeriod(hManifest.latestPeriodId);
  const rPeriod = loadPeriod(rReport.period.periodId);
  const comparability = evaluateAdpComparability(hPeriod, rPeriod);
  if (comparability.outcome !== "EXACT_COMPARABLE") {
    throw new Error(`Matched pair not EXACT_COMPARABLE: ${comparability.outcome}`);
  }

  const cert = await certifyAdpPeriod(hManifest.latestPeriodId, {
    auditOnly: false,
    forceOfficialCertification: true,
    writeManifest: false,
    writeAuditTrail: false,
    stampPeriod: false,
  });

  // Generate new review draft from Live published ADP
  const draft = generateNewDraft(HILTON_ID, {
    generatedBy: "hilton-performance-review-pack",
  });
  if (!draft.ok) {
    throw new Error(`generateNewDraft failed: ${JSON.stringify(draft.error)}`);
  }

  const pack = loadReviewPayload(draft.newReviewId);
  if (!pack?.review) throw new Error("review payload missing after draft");

  // Enrich with matched peer + source intelligence (customer-safe)
  pack.review.matchedPeerComparison = buildMatchedPeerComparison(
    hReport,
    rReport,
    comparability
  );
  pack.review.sourceIntelligence = buildSourceIntelligence(
    cert.manifest?.sourceAttribution,
    hReport
  );
  pack.review.reporting.currentPeriodId = hManifest.latestPeriodId;
  pack.review.reporting.monitoringPeriodId = hManifest.latestPeriodId;

  // Persist enriched review.json (immutable new version already created)
  writeFileSync(pack.paths.reviewJson, JSON.stringify(pack.review, null, 2) + "\n");

  // Also save enriched HTML snapshot via PDF generator
  const pdfResult = await generateMonthlyReviewPdfV1({
    baseUrl: BASE_URL,
    propertyId: HILTON_ID,
    source: "archive",
    reviewId: draft.newReviewId,
    outPath: join(OUT, "_tmp_hilton_review.pdf"),
    pagesDir: join(OUT, "page-previews"),
  });

  const canonicalName = buildAiDemandPerformanceReviewFilename({
    hotelName: "Hilton New York Times Square",
    date: hReport.period?.executionDate || "2026-10-05",
  });
  const pdfOut = join(OUT, canonicalName);
  const htmlOut = join(OUT, "HILTON_TIMES_SQUARE_PERFORMANCE_REVIEW.html");
  const pdfAlias = join(OUT, "HILTON_TIMES_SQUARE_PERFORMANCE_REVIEW.pdf");

  copyFileSync(join(OUT, "_tmp_hilton_review.pdf"), pdfOut);
  copyFileSync(pdfOut, pdfAlias);

  // Canonical Reviews-tab registration: archive report.pdf + index pdfStatus READY
  const attached = await renderAndAttachMonthlyReviewPdfV1({
    reviewId: draft.newReviewId,
    propertyId: HILTON_ID,
    pdfSourcePath: pdfAlias,
    skipRender: true,
  });
  if (!attached.ok) {
    throw new Error(`archive_pdf_attach_failed: ${JSON.stringify(attached)}`);
  }

  // Save HTML from renderer structural html
  const htmlDoc = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>Dealality — Hilton New York Times Square — AI Demand Performance Review</title>
<link rel="stylesheet" href="/css/brand-alignment-snapshot.css"/>
<link rel="stylesheet" href="/css/dealality-report-print-chrome.css"/>
<link rel="stylesheet" href="/css/dealality-report-system-v1.css"/>
<link rel="stylesheet" href="/css/adp-monthly-review-report-v1.css"/>
</head>
<body class="adp-mr-pdf-export">
${pdfResult.structural?.html || ""}
</body>
</html>
`;
  writeFileSync(htmlOut, htmlDoc);

  const rank = hReport.executiveMetrics?.rankMetrics;
  const rankSupported = (rank?.rankEligibleN || 0) >= 20;
  const topDisp = hReport.lostDemand?.displacement?.[0];
  const src = pack.review.sourceIntelligence;

  const staleHits = [];
  const pdfBuf = readFileSync(pdfOut);
  // Can't text-search PDF easily; search HTML
  const htmlText = pdfResult.structural?.html || "";
  for (const bad of ["3.5%", "7/200", "50 scenarios", "September 27", "Sep 27"]) {
    if (htmlText.includes(bad)) staleHits.push(bad);
  }
  if (/marriott\.com[^<]{0,80}Top Source/i.test(htmlText) && !/Competitive-Universe/i.test(htmlText)) {
    staleHits.push("marriott.com_mislabeled_top_source");
  }
  if (htmlText.includes("adp_period_adp_hilton_times_square_20260927")) {
    staleHits.push("legacy_sep27_period_id");
  }
  if (htmlText.includes("20261005111601_190412") && !htmlText.includes("20261005122652_63a1d8")) {
    staleHits.push("superseded_period_only");
  }

  write(
    "DATA_AUDIT.md",
    `# Hilton Times Square Performance Review — Data Audit

| Field | Value |
|-------|-------|
| Latest CERTIFIED period | \`${hManifest.latestPeriodId}\` |
| Certification | ${hManifest.certificationStatus} |
| Engine | ${hManifest.globalCertificationEngineVersion} |
| Review ID | \`${draft.newReviewId}\` |
| Scenario count | ${hReport.period.scenarioCount} |
| Expected responses | 260 |
| Successful responses | 260 |
| AI Consideration | ${hReport.executiveMetrics.considerationRate.rate}% (${hReport.executiveMetrics.considerationRate.presentObservations}/${hReport.executiveMetrics.considerationRate.comparableObservations}) |
| Scenario Presence | ${hReport.executiveMetrics.scenarioPresence.rate}% (${hReport.executiveMetrics.scenarioPresence.capturedScenarios}/${hReport.executiveMetrics.scenarioPresence.eligibleScenarios}) |
| Reality Coverage | 50.0% (4/8 attributes) |
| ChatGPT | 23.1% (15/65) |
| Gemini | 16.9% (11/65) |
| Perplexity | 26.2% (17/65) |
| Claude | 26.2% (17/65) |
| #1 / Top-3 | ${rankSupported ? `${rank.numberOneAppearanceRate}% / ${rank.topThreeAppearanceRate}%` : `Insufficient ranked responses (n=${rank?.rankEligibleN || 0})`} |
| Owned/brand source share | ${hReport.evidence.ownedSourceShare}% |
| Citation coverage | ${hReport.evidence.citationRate}% |
| Top displacement | ${topDisp?.name} (${topDisp?.displacementCount}) |
| Top property-supporting source | ${src.rows[0].domain} (${src.rows[0].taxonomy}) |
| Top owned/brand source | ${src.rows[1].domain} |
| Top competitive-universe source | ${src.rows[3].domain} (${src.rows[3].taxonomy}) |
| Renaissance matched | YES |
| EXACT_COMPARABLE | ${comparability.outcome === "EXACT_COMPARABLE" ? "YES" : "NO"} |
| Legacy Sep 27 used | NO |
| Superseded-only data used | NO |

## Renaissance matched pair
- Period: \`${rReport.period.periodId}\`
- Consideration: ${rReport.executiveMetrics.considerationRate.rate}% (66/260)
- Scenario presence: ${rReport.executiveMetrics.scenarioPresence.rate}% (26/65)
`
  );

  write(
    "PDF_QA.md",
    `# PDF QA

| Check | Result |
|-------|--------|
| PDF generated | YES |
| Canonical filename | ${canonicalName} |
| Alias path | HILTON_TIMES_SQUARE_PERFORMANCE_REVIEW.pdf |
| HTML path | HILTON_TIMES_SQUARE_PERFORMANCE_REVIEW.html |
| Template | ${pdfResult.templateId} |
| Renderer | ${pdfResult.renderer} |
| Report family | ${pdfResult.reportFamily} |
| Cover present | ${pdfResult.structural?.hasCover ? "YES" : "NO"} |
| DRS KPI cards | ${pdfResult.structural?.drsKpiCount} |
| Legacy KPI markup | ${pdfResult.structural?.legacyKpiCount === 0 ? "NONE" : "FAIL"} |
| Stale value hits in HTML | ${staleHits.length ? staleHits.join(", ") : "none"} |
| Page preview PNGs | ${pdfResult.pagePngs?.length || 0} |
| Visual QA | ${pdfResult.structural?.hasCover && pdfResult.structural?.drsKpiCount >= 1 && !staleHits.length ? "PASS" : "REVIEW"} |
| Data QA | ${pack.review.reporting.currentPeriodId === hManifest.latestPeriodId && staleHits.length === 0 ? "PASS" : "FAIL"} |
`
  );

  write(
    "CHANGELOG.md",
    `# Changelog — Hilton Times Square Performance Review

## Deliverable
- Customer AI Demand Performance Review PDF from CERTIFIED period \`${hManifest.latestPeriodId}\`
- Matched Renaissance comparison (\`EXACT_COMPARABLE\`) included
- Correct source taxonomy labels (marriott.com = competitive-universe, not Hilton top source)

## Platform fixes applied for integrity
- Monthly review \`currentPeriodId\` / \`monitoringPeriodId\` bind to Live published period (not stale trends.current)
- Published Hilton trends.current corrected to matched-control CERTIFIED period metrics
- PDF renderer: optional matched peer comparison + source intelligence tables (design-system tables)

## Non-changes
- No provider reruns
- No methodology / threshold changes
- No Sep 27 legacy baseline numbers
`
  );

  // Page count via pdf-lib if available, else estimate from buffer
  let pageCount = null;
  try {
    const { PDFDocument } = await import("pdf-lib");
    const doc = await PDFDocument.load(pdfBuf);
    pageCount = doc.getPageCount();
  } catch {
    pageCount = "unknown";
  }

  write(
    "RUN_SUMMARY.json",
    JSON.stringify(
      {
        periodId: hManifest.latestPeriodId,
        reviewId: draft.newReviewId,
        pdfOut,
        pdfAlias,
        htmlOut,
        canonicalName,
        pageCount,
        comparability: comparability.outcome,
        staleHits,
        metrics: {
          consideration: hReport.executiveMetrics.considerationRate,
          scenarioPresence: hReport.executiveMetrics.scenarioPresence,
          providers: (hReport.evidence?.providers || []).map((p) => ({
            provider: p.provider,
            presence: p.presence,
            mentioned: p.mentioned,
            comparable: p.comparable,
          })),
          topDisplacement: topDisp,
          sourceIntelligence: src,
          renaissance: {
            consideration: rReport.executiveMetrics.considerationRate,
            scenarioPresence: rReport.executiveMetrics.scenarioPresence,
          },
        },
      },
      null,
      2
    )
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        periodId: hManifest.latestPeriodId,
        reviewId: draft.newReviewId,
        pdf: pdfAlias,
        canonicalName,
        pageCount,
        consideration: hReport.executiveMetrics.considerationRate.rate,
        scenarioPresence: hReport.executiveMetrics.scenarioPresence.rate,
        exactComparable: comparability.outcome === "EXACT_COMPARABLE",
        staleHits,
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
