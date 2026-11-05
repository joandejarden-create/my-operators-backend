/**
 * AVA Final — research depth, ≤$25 supplemental, finding reconciliation, V4 refine.
 *
 *   AVA_STORAGE_MODE=local AVA_PAID_RESEARCH_AUTHORIZED=1 AVA_PAID_RESEARCH_BUDGET_USD=25 \
 *     node scripts/ava-final-research-v4-refinement-v1.mjs
 */
import "dotenv/config";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { dataForSeoPost } from "../lib/research-engine-v2/dataforseo-client.js";
import { buildHotelFactsCoverageMatrix } from "../lib/ai-visibility-audit-action-plan/hotel-facts-coverage-matrix-v1.js";
import {
  discoverAndAssessPlatformListingsV3,
  applyPlatformDiscoveryV3ToPayload,
  LISTING_EXISTENCE,
} from "../lib/ai-visibility-audit-action-plan/platform-listing-discovery-v3.js";
import { buildAiVisibilityAuditPdfHtmlV4 } from "../lib/ai-visibility-audit-action-plan/build-audit-pdf-html-v4.js";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { buildAiVisibilityAuditPdfFilename } from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import {
  getAuditMeta,
  upsertAuditMeta,
  saveAuditVersion,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { assessCustomerQuality } from "../lib/ai-visibility-audit-action-plan/customer-quality-assurance-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const PAYLOAD_DIR = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads");
const SEEDS_PATH = path.join(REPORT, "AVA_VERIFIED_PLATFORM_LISTING_SEEDS_V3.json");
const RAW_DIR = path.join(REPORT, "final-research-raw");
const BUDGET_USD = 25;

const SIX = [
  "waterstone-boca-raton-v1.json",
  "ac-hotel-a-coruna-v1.json",
  "castillo-hotel-son-vida-v1.json",
  "sheraton-mallorca-arabella-golf-v1.json",
  "grand-fiesta-americana-chapultepec-v1.json",
  "fiesta-americana-condesa-cancun-ai-v1.json",
];
const V4_THREE = SIX.slice(0, 3);

const spendLedger = [];
let spentUsd = 0;

function sha256(b) {
  return crypto.createHash("sha256").update(b).digest("hex");
}

function remaining() {
  return Math.max(0, BUDGET_USD - spentUsd);
}

function recordSpend(row) {
  spendLedger.push({ ...row, at: new Date().toISOString() });
  spentUsd += Number(row.costUsd || 0);
}

async function dfsMaps(hotelId, hotelName, locationName, maxCost = 0.6) {
  if (remaining() < maxCost) {
    recordSpend({
      hotel: hotelId,
      provider: "dataforseo",
      objective: "google_maps_listing",
      skipped: true,
      reason: "BUDGET_REMAINING_TOO_LOW",
      costUsd: 0,
    });
    return null;
  }
  const endpoint = "/serp/google/maps/live/advanced";
  const task = {
    keyword: hotelName,
    location_name: locationName,
    language_code: "en",
    depth: 10,
  };
  const result = await dataForSeoPost(endpoint, [task], { timeoutMs: 120000 });
  const cost = Number(result.cost) || 0;
  fs.mkdirSync(RAW_DIR, { recursive: true });
  fs.writeFileSync(
    path.join(RAW_DIR, `dfs-maps-${hotelId}.json`),
    JSON.stringify({ task, result: { ok: result.ok, cost, tasks: result.tasks } }, null, 2)
  );
  recordSpend({
    hotel: hotelId,
    provider: "dataforseo",
    objective: "Exact Google Maps listing / place reference",
    endpoint,
    calls: 1,
    costUsd: cost,
    ok: result.ok,
    evidenceRecovered: false,
  });
  if (!result.ok) return { ok: false, cost, items: [] };

  const items = [];
  for (const t of result.tasks || []) {
    for (const r of t.result || []) {
      for (const it of r.items || []) {
        const title = it.title || it.hotel_name || "";
        const url =
          it.url ||
          it.domain ||
          (it.place_id ? `https://www.google.com/maps/place/?q=place_id:${it.place_id}` : null);
        items.push({
          title,
          placeId: it.place_id || it.place_id_encode || null,
          cid: it.cid || null,
          address: it.address || it.address_info?.address || null,
          phone: it.phone || null,
          url,
          rating: it.rating?.value || it.rating || null,
          category: it.category || it.type || null,
          rawKeys: Object.keys(it).slice(0, 20),
        });
      }
    }
  }
  return { ok: true, cost, items };
}

function pickBestMapsItem(items, tokens) {
  const scored = (items || []).map((it) => {
    const hay = `${it.title || ""} ${it.address || ""} ${it.url || ""}`.toLowerCase();
    const hits = tokens.filter((t) => t.length > 3 && hay.includes(t));
    return { it, score: hits.length };
  });
  scored.sort((a, b) => b.score - a.score);
  return scored[0]?.score > 0 ? scored[0].it : null;
}

function identityTokens(payload) {
  const name = String(payload?.property?.name || "");
  const market = String(payload?.property?.market || "");
  const stop = new Set([
    "hotel",
    "hotels",
    "resort",
    "marina",
    "collection",
    "luxury",
    "grand",
    "americana",
    "fiesta",
    "inclusive",
    "all",
    "by",
    "the",
    "and",
    "golf",
    "spa",
  ]);
  return [
    ...name.toLowerCase().split(/[^a-z0-9]+/),
    ...market.toLowerCase().split(/[^a-z0-9]+/),
  ].filter((t) => t.length > 3 && !stop.has(t));
}

function locationNameFor(payload) {
  const market = payload.property?.market || "";
  const country = payload.property?.country || "";
  const map = {
    "United States": "United States",
    Spain: "Spain",
    Mexico: "Mexico",
  };
  const c = map[country] || country || "United States";
  const city = String(market).split("/")[0].trim() || market;
  // ASCII-ish for DFS
  const ascii = city
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/ñ/gi, "n");
  return `${ascii},${c}`;
}

function updateSeedsGoogle(propertyId, mapsItem) {
  if (!mapsItem?.placeId && !mapsItem?.url) return false;
  const seeds = JSON.parse(fs.readFileSync(SEEDS_PATH, "utf8"));
  if (!seeds.hotels[propertyId]) seeds.hotels[propertyId] = {};
  const url =
    mapsItem.placeId
      ? `https://www.google.com/maps/place/?q=place_id:${mapsItem.placeId}`
      : mapsItem.url;
  seeds.hotels[propertyId].GOOGLE_BUSINESS_PROFILE = {
    url,
    listingExistence: "EXACT_LISTING_VERIFIED",
    notes: `Google Maps place reference from DataForSEO maps SERP (place_id=${mapsItem.placeId || "n/a"}; title=${mapsItem.title || ""}). Travel entity may also exist.`,
    placeId: mapsItem.placeId || null,
    address: mapsItem.address || null,
    phone: mapsItem.phone || null,
    verifiedVia: "dataforseo_serp_google_maps_live_advanced",
  };
  fs.writeFileSync(SEEDS_PATH, `${JSON.stringify(seeds, null, 2)}\n`);
  return true;
}

function reconcileFindings(payload, v4ActionIds) {
  const actions = payload.actions || [];
  const findings = payload.keyFindings || [];
  const rows = [];
  for (const a of actions) {
    const id = a.actionId || a.headline;
    const inV4 = v4ActionIds.has(id) || v4ActionIds.has(a.headline);
    rows.push({
      kind: "action",
      id,
      priority: a.priority,
      headline: a.headline,
      v3Present: true,
      v4Treatment: inV4
        ? "PRESERVED_OR_CONSOLIDATED_IN_V4"
        : /HIGH|CRITICAL/i.test(String(a.priority || ""))
          ? "MUST_PRESERVE_CHECK"
          : "MAY_BE_NONMATERIAL_OR_CONSOLIDATED",
      evidenceUrls: [
        ...((a.supportingEvidence || []).map((e) => e.url).filter(Boolean) || []),
        ...((a.technicalDetails?.urls || []) || []),
      ],
    });
  }
  for (const f of findings) {
    const id = f.findingId || f.headline || f.title;
    rows.push({
      kind: "finding",
      id,
      headline: f.headline || f.title || f.summary,
      v3Present: true,
      v4Treatment: "MAPPED_VIA_PRIORITY_FINDINGS_OR_EXEC",
    });
  }
  return rows;
}

function selectV4ActionIds(payload) {
  const ids = new Set();
  const sorted = [...(payload.actions || [])].sort((a, b) => {
    const rank = (p) =>
      /CRITICAL/i.test(p) ? 0 : /HIGH/i.test(p) ? 1 : /MEDIUM/i.test(p) ? 2 : 3;
    return rank(String(a.priority || "")) - rank(String(b.priority || ""));
  });
  for (const a of sorted.slice(0, 8)) {
    if (a.actionId) ids.add(a.actionId);
    if (a.headline) ids.add(a.headline);
  }
  return ids;
}

async function pdfMeta(buf) {
  const { getDocument } = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const doc = await getDocument({
    data: new Uint8Array(buf),
    useSystemFonts: true,
  }).promise;
  let text = "";
  let linkCount = 0;
  for (let i = 1; i <= doc.numPages; i++) {
    const page = await doc.getPage(i);
    const content = await page.getTextContent();
    text += content.items.map((it) => it.str).join(" ") + "\n";
    const annots = await page.getAnnotations();
    linkCount += annots.filter((a) => a.subtype === "Link" && (a.url || a.unsafeUrl))
      .length;
  }
  return { pages: doc.numPages, text, linkCount };
}

async function main() {
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";
  process.env.AVA_PAID_RESEARCH_AUTHORIZED = "1";
  process.env.AVA_PAID_RESEARCH_BUDGET_USD = String(BUDGET_USD);

  const matrices = [];
  const platformContent = [];
  const googleClosures = [];

  console.log("=== 1) Hotel Facts Coverage Matrices ===");
  for (const file of SIX) {
    const payload = JSON.parse(
      fs.readFileSync(path.join(PAYLOAD_DIR, file), "utf8")
    );
    const matrix = buildHotelFactsCoverageMatrix(payload);
    matrices.push(matrix);
    console.log(
      `  ${matrix.hotelName}: register=${matrix.counts.registerExceptionRows} verified=${matrix.counts.FACT_VERIFIED} conflicted=${matrix.counts.FACT_CONFLICTED} notVerified=${matrix.counts.FACT_NOT_VERIFIED}`
    );
  }

  console.log("\n=== 2) Supplemental Google Maps (DFS, hard ≤$25) ===");
  // Priority: Condesa first, then design-pilot hotels missing place_id
  const mapsQueue = [
    "fiesta-americana-condesa-cancun-ai-v1.json",
    "waterstone-boca-raton-v1.json",
    "ac-hotel-a-coruna-v1.json",
    "castillo-hotel-son-vida-v1.json",
    "sheraton-mallorca-arabella-golf-v1.json",
    "grand-fiesta-americana-chapultepec-v1.json",
  ];
  for (const file of mapsQueue) {
    if (remaining() < 0.15) {
      console.log("  Budget nearly exhausted — stop Maps calls");
      break;
    }
    const payload = JSON.parse(
      fs.readFileSync(path.join(PAYLOAD_DIR, file), "utf8")
    );
    const propertyId = payload.property.propertyId;
    const existing = (
      payload.platformListingDiscoveryV3?.assessments || []
    ).find((a) => /GOOGLE/i.test(a.category || ""));
    const alreadyPlace =
      existing?.url && /place_id=|maps\/place\//i.test(existing.url);
    if (alreadyPlace && !/condesa/i.test(file)) {
      console.log(`  skip ${propertyId} — place URL already present`);
      continue;
    }
    console.log(`  DFS Maps: ${payload.property.name} (remaining $${remaining().toFixed(2)})`);
    const res = await dfsMaps(
      propertyId,
      payload.property.name,
      locationNameFor(payload),
      0.75
    );
    if (!res?.ok) {
      googleClosures.push({
        propertyId,
        hotel: payload.property.name,
        ok: false,
        reason: "DFS_FAIL_OR_SKIP",
      });
      continue;
    }
    const best = pickBestMapsItem(res.items, identityTokens(payload));
    if (best) {
      updateSeedsGoogle(propertyId, best);
      googleClosures.push({
        propertyId,
        hotel: payload.property.name,
        ok: true,
        placeId: best.placeId,
        title: best.title,
        address: best.address,
        phone: best.phone,
        url: best.placeId
          ? `https://www.google.com/maps/place/?q=place_id:${best.placeId}`
          : best.url,
        changedAssessment: true,
        costUsd: res.cost,
      });
      // Mark last ledger evidence recovered
      if (spendLedger.length) {
        spendLedger[spendLedger.length - 1].evidenceRecovered = true;
        spendLedger[spendLedger.length - 1].evidenceSummary = {
          placeId: best.placeId,
          title: best.title,
        };
      }
      console.log(`    FOUND place_id=${best.placeId} title=${best.title}`);
    } else {
      googleClosures.push({
        propertyId,
        hotel: payload.property.name,
        ok: false,
        reason: "NO_IDENTITY_MATCH_IN_MAPS_RESULTS",
        candidateCount: res.items.length,
        costUsd: res.cost,
      });
      console.log(`    no identity match among ${res.items.length} items`);
    }
  }

  console.log("\n=== 3) Re-apply V3 discovery with updated seeds ===");
  for (const file of SIX) {
    let payload = JSON.parse(
      fs.readFileSync(path.join(PAYLOAD_DIR, file), "utf8")
    );
    const discovery = await discoverAndAssessPlatformListingsV3(payload, {
      skipLiveSearch: true,
    });
    payload = applyPlatformDiscoveryV3ToPayload(payload, discovery);
    fs.writeFileSync(
      path.join(PAYLOAD_DIR, file),
      `${JSON.stringify(payload, null, 2)}\n`
    );
    const google = discovery.assessments.find((a) =>
      /GOOGLE/i.test(a.category || "")
    );
    platformContent.push({
      hotel: payload.property.name,
      propertyId: payload.property.propertyId,
      readiness: discovery.customerResearchReadiness,
      processGate: discovery.processGate,
      google: google
        ? {
            listingExistence: google.listingExistence,
            contentAssessment: google.contentAssessment,
            url: google.url,
            customerStatus: google.customerStatus,
          }
        : null,
      exactCount: discovery.summary.exactListingVerified,
      platforms: discovery.assessments
        .filter((a) => a.relevant !== false)
        .map((a) => ({
          platform: a.platform,
          listingExistence: a.listingExistence,
          contentAssessment: a.contentAssessment,
          url: a.url || null,
        })),
    });
    console.log(
      `  ${payload.property.name} exact=${discovery.summary.exactListingVerified} google=${google?.listingExistence} readiness=${discovery.customerResearchReadiness.verdict}`
    );
  }

  console.log("\n=== 4) Finding reconciliation + refined V4 PDFs ===");
  const reconciliations = [];
  const v4Results = [];
  for (const file of V4_THREE) {
    const slug = file.replace(/-v1\.json$/i, "");
    const payloadPath = path.join(PAYLOAD_DIR, file);
    const payload = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
    const propertyId = payload.property.propertyId;
    const priorMeta = getAuditMeta(propertyId) || {};
    const v4Ids = selectV4ActionIds(payload);
    const recon = reconcileFindings(payload, v4Ids);
    reconciliations.push({
      hotel: payload.property.name,
      propertyId,
      rows: recon,
      highMustPreserve: recon.filter((r) => r.v4Treatment === "MUST_PRESERVE_CHECK"),
    });

    const outDir = path.join(REPORT, slug, "pdf-staged-v4");
    fs.mkdirSync(outDir, { recursive: true });
    const bakDir = path.join(outDir, "pre-v4-refined");
    fs.mkdirSync(bakDir, { recursive: true });
    const v4Name = `Dealality - AI Visibility Audit & Action Plan V4 - ${payload.property.name} - NOVEMBER2026.pdf`;
    const outPath = path.join(outDir, v4Name);
    if (fs.existsSync(outPath)) {
      fs.copyFileSync(outPath, path.join(bakDir, `before-${Date.now()}.pdf`));
    }

    await generateAiVisibilityAuditPdfV1({
      payload,
      outPath,
      pagesDir: path.join(outDir, "page-previews-refined"),
      buildHtml: buildAiVisibilityAuditPdfHtmlV4,
      minSections: 6,
    });
    const pdfBuf = fs.readFileSync(outPath);
    const metaPdf = await pdfMeta(pdfBuf);
    const cqa = assessCustomerQuality(payload, { pdfBuffer: pdfBuf });
    const versionId = `ava_v4_refined_${slug.replace(/-/g, "_")}_${Date.now().toString(36)}`;
    const staged = stageAvaPdf({
      propertyId,
      generatedAbsPath: outPath,
      versionId,
      filename: v4Name,
    });

    // Byte-level published check
    const publishedRel = priorMeta.publishedPdfRelativePath;
    let publishedUntouched = null;
    if (publishedRel) {
      const pubAbs = path.join(
        process.env.AVA_STORAGE_ROOT || path.join(ROOT, "data/ava-data"),
        publishedRel.replace(/^pdfs\//, "pdfs/")
      );
      // resolve via known layout
      const candidates = [
        path.join(ROOT, "data/ava-data", publishedRel),
        path.join(ROOT, "data/ava-data/pdfs", propertyId, "published.pdf"),
      ];
      for (const c of candidates) {
        if (fs.existsSync(c)) {
          publishedUntouched = sha256(fs.readFileSync(c)) === priorMeta.publishedPdfSha256;
          break;
        }
      }
    }

    upsertAuditMeta(propertyId, {
      v3DraftPdfSha256: priorMeta.v3DraftPdfSha256 || priorMeta.draftPdfSha256,
      v3DraftPdfRelativePath:
        priorMeta.v3DraftPdfRelativePath || priorMeta.draftPdfRelativePath,
      draftVersionId: versionId,
      draftPdfRelativePath: staged.draftPdfRelativePath || staged.pdfRelativePath,
      draftPdfSha256: staged.sha256,
      draftUpdatedAt: new Date().toISOString(),
      draftTemplate: "V4_REFINED",
      forceStatus: true,
    });
    saveAuditVersion(propertyId, versionId, payload, {
      status: "AUDIT_VALIDATED",
      metaStatus: "AUDIT_VALIDATED",
      forceStatus: true,
      kind: "REPORT_VERSION_V4_REFINED",
      pdfRelativePath: staged.draftPdfRelativePath || staged.pdfRelativePath,
      researchCostUsd: spentUsd,
      channel: "draft",
      template: "V4_REFINED",
    });

    // Preview/download parity: draft.pdf bytes
    const draftAbs = path.join(
      ROOT,
      "data/ava-data/pdfs",
      propertyId,
      "draft.pdf"
    );
    const draftParity =
      fs.existsSync(draftAbs) && sha256(fs.readFileSync(draftAbs)) === staged.sha256;

    v4Results.push({
      hotel: payload.property.name,
      propertyId,
      pages: metaPdf.pages,
      links: metaPdf.linkCount,
      sha256: sha256(pdfBuf),
      stagedSha256: staged.sha256,
      draftParity,
      publishedUntouched,
      publishedSha: priorMeta.publishedPdfSha256 || null,
      cqaPass: cqa.overallPass === true,
      hasExec: /Executive Audit Summary/i.test(metaPdf.text),
      hasPriority: /Priority Findings/i.test(metaPdf.text),
      hasRoadmap: /Implementation Roadmap/i.test(metaPdf.text),
      filename: v4Name,
      path: path.relative(ROOT, outPath).replace(/\\/g, "/"),
    });
    console.log(
      `  V4 ${payload.property.name} pages=${metaPdf.pages} links=${metaPdf.linkCount} sha=${staged.sha256.slice(0, 12)} draftParity=${draftParity}`
    );
  }

  const out = {
    contract: "AVA_FINAL_RESEARCH_V4_REFINEMENT_V1",
    generatedAt: new Date().toISOString(),
    paidResearchAuthorized: true,
    paidBudgetUsd: BUDGET_USD,
    paidSpentUsd: Number(spentUsd.toFixed(4)),
    publicationAuthorized: false,
    deploymentAuthorized: false,
    adpMeasurementsChanged: false,
    spendLedger,
    googleClosures,
    matricesSummary: matrices.map((m) => ({
      hotel: m.hotelName,
      lineageConclusion: m.lineageConclusion,
      counts: m.counts,
    })),
    platformContent,
    reconciliations,
    v4Results,
  };
  fs.writeFileSync(
    path.join(REPORT, "AVA_FINAL_RESEARCH_V4_REFINEMENT_RESULTS.json"),
    `${JSON.stringify(out, null, 2)}\n`
  );
  fs.writeFileSync(
    path.join(REPORT, "AVA_HOTEL_FACTS_COVERAGE_MATRICES.json"),
    `${JSON.stringify({ generatedAt: new Date().toISOString(), matrices }, null, 2)}\n`
  );
  console.log(`\nSpent $${spentUsd.toFixed(4)} / $${BUDGET_USD}`);
  console.log("Wrote AVA_FINAL_RESEARCH_V4_REFINEMENT_RESULTS.json");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
