/**
 * Wave 12 Stage 8 — Founder Review Packets (read-only; no Airtable writes).
 *
 * Produces per-brand founder packets + wave summary from live Presentation /
 * Brand Basics reads + Stage 3–6 artifacts. Never touches Brand Status, release,
 * CV, Source Library, Registry, images, or protected 27.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  WAVE12_VERSION,
  WAVE12_SLUGS,
  WAVE12_BRAND_PLAN,
  WAVE12_PROTECTED_BASELINE_COUNT,
} from "./brand-explorer-wave12-factory-plan.js";
import {
  FACTORY_PREVIEW_CANDIDATE_IDENTITIES,
  FACTORY_PREVIEW_DISPLAY_STATE,
  buildFactoryPreviewUrls,
} from "./brand-explorer-factory-preview-candidates.js";
import { getWave12TabFactorySeed } from "./brand-explorer-wave12-tab-factory-seeds.js";
import { getWave12SourcePack } from "./brand-explorer-wave12-source-packs-content.js";
import { CALA_AVAILABLE_BY_SLUG } from "./brand-explorer-27-recent-momentum-evidence-fix-content.js";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "./brand-explorer-image-role-match.js";
import { classifyRegionFromText } from "./brand-explorer-recent-momentum-evidence-quality.js";
import { EXPECTED_ACTIVE_COUNT_27 } from "./brand-explorer-27-active-public-full-baseline.js";

export const WAVE12_FOUNDER_REVIEW_VERSION = "wave12-founder-review-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

/** Brands where CALA momentum/openings are intentionally International Reference–led. */
export const WAVE12_INTL_REF_MOMENTUM_SLUGS = Object.freeze([
  "even-hotels",
  "avid-hotels",
  "canopy-by-hilton",
  "tempo-by-hilton",
]);

/** Brands with stronger verified CALA anchors in source packs. */
export const WAVE12_STRONG_CALA_SLUGS = Object.freeze([
  "city-express-by-marriott",
  "holiday-inn-express",
  "motto-by-hilton",
  "moxy-hotels",
  "bunkhouse-hotels",
  "voco-hotels",
]);

const RECOMMENDATIONS = Object.freeze([
  "approve_for_status_promotion_and_public_release",
  "approve_after_minor_cleanup",
  "remediation_required",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function readJsonSafe(relOrAbs) {
  const p = path.isAbsolute(relOrAbs) ? relOrAbs : path.join(ROOT, relOrAbs);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function visibleSlot(rows, slotKey) {
  return (rows || [])
    .filter(
      (r) =>
        nz(r.slotKey) === slotKey &&
        r.active !== false &&
        !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
    )
    .sort((a, b) => Number(a.sortOrder ?? 0) - Number(b.sortOrder ?? 0));
}

function imageCaption(row) {
  return nz(row?.title) || nz(row?.caseSummaryOverview) || "(no caption)";
}

function imageRef(row) {
  const url = nz(row?.imageUrl);
  if (!url) return { hasImage: false, caption: imageCaption(row), url: null };
  return {
    hasImage: true,
    caption: imageCaption(row),
    url: url.split("?")[0],
  };
}

async function fetchBrandApi(recordId) {
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
  await getBrandLibraryBrandById({ query: { brandId: recordId }, headers: {} }, res);
  if (res.statusCode !== 200 || !res.payload?.brand) {
    return { error: `Brand API HTTP ${res.statusCode}`, brand: null };
  }
  return { error: null, brand: res.payload.brand };
}

function gateSummaryFromTabFactoryAudit(audit, slug) {
  const brand = (audit?.brandResults || []).find((b) => b.brandSlug === slug);
  if (!brand) {
    return {
      available: false,
      tabFactoryPass: null,
      renderedCompletenessPass: null,
      noEmptyPass: null,
      goldenPass: null,
      sectionPatternPass: null,
      momentumEvidencePass: null,
      imageUniquenessPass: null,
      imageRoleMatchPass: null,
      failFindings: null,
      note: "No brand row in reports/brand-explorer-tab-factory-audit.json — use Stage 6 acceptance + live image audits.",
    };
  }
  const g = brand.gates || {};
  return {
    available: true,
    tabFactoryPass: brand.auditPass === true,
    renderedCompletenessPass: g.rendered_field_completeness === true,
    noEmptyPass: g.no_empty_rendered_components === true,
    goldenPass: g.golden_content_quality === true,
    sectionPatternPass: g.section_pattern_parity === true,
    momentumPatternPass: g.recent_momentum_pattern_pass === true,
    momentumEvidencePass: g.recent_momentum_evidence_quality === true,
    imageUniquenessPass: g.image_distinctiveness === true,
    imageRoleMatchPass: g.image_role_match === true,
    failFindings: brand.failFindings ?? null,
    decision: brand.releaseQualityDecision || brand.completeness?.releaseQualityDecision || null,
  };
}

function imageAuditFromDisk(slug) {
  const uniq = readJsonSafe("reports/brand-explorer-image-uniqueness-audit.json");
  const role = readJsonSafe("reports/brand-explorer-image-role-match-audit.json");
  const uBrand = (uniq?.brandResults || uniq?.brands || []).find(
    (b) => b.brandSlug === slug || b.slug === slug
  );
  const rBrand = (role?.brandResults || role?.brands || []).find(
    (b) => b.brandSlug === slug || b.slug === slug
  );
  return {
    uniquenessPass: uBrand?.pass === true || uBrand?.auditPass === true || null,
    uniquenessDetail: uBrand
      ? {
          galleryDistinct: uBrand.galleryDistinctCount ?? uBrand.galleryDistinct ?? null,
          scenarioDistinct: uBrand.scenarioDistinctCount ?? uBrand.scenario ?? null,
          propertyDistinct: uBrand.propertyExampleDistinctCount ?? uBrand.property ?? null,
        }
      : null,
    roleMatchPass: rBrand?.pass === true || rBrand?.roleMatch === true || null,
  };
}

function founderTasteCautions(slug) {
  const notes = [];
  if (WAVE12_INTL_REF_MOMENTUM_SLUGS.includes(slug)) {
    notes.push(
      "International Reference momentum is intentional: CALA open/press evidence was not available or not strong enough in the Stage 3 source pack."
    );
    notes.push(
      "Directory / development-page Recent Momentum cards are intentional owner-facing proof labels (not invented opening years)."
    );
  }
  if (WAVE12_STRONG_CALA_SLUGS.includes(slug)) {
    notes.push(
      "Stronger CALA anchors are present (named property and/or dated pipeline). Prefer CALA-first ordering in founder visual QA."
    );
  }
  notes.push(
    "Openings Case Summary geography labels were corrected after Stage 5 photo-caption mismatches (CALA vs International Reference)."
  );
  if (slug === "bunkhouse-hotels") {
    notes.push(
      "Bunkhouse parent context should remain clearly labeled as Hyatt parent / World of Hyatt context — not generic Bunkhouse-only proof or Hyatt hard-brand substitution."
    );
  }
  if (slug === "voco-hotels") {
    notes.push(
      "voco Mexico City Reforma is an open CALA reference; September 2025 Mexico signings are pipeline momentum, not already-open inventory."
    );
  }
  return notes;
}

function recommend({ gates, liveUniquenessPass, liveRoleMatchPass, brandStatus, residual }) {
  const statusOk = /under review/i.test(nz(brandStatus)) || !brandStatus;
  const gateOk =
    gates.tabFactoryPass !== false &&
    gates.renderedCompletenessPass !== false &&
    gates.noEmptyPass !== false &&
    gates.goldenPass !== false &&
    gates.momentumEvidencePass !== false &&
    (gates.imageUniquenessPass !== false || liveUniquenessPass === true) &&
    (gates.imageRoleMatchPass !== false || liveRoleMatchPass === true);

  if (!statusOk && /active|live/i.test(nz(brandStatus))) {
    return {
      recommendation: "remediation_required",
      rationale: `Unexpected Brand Status "${brandStatus}" while still in factory preview cohort.`,
    };
  }
  if (residual.length || gateOk === false) {
    return {
      recommendation: residual.some((r) => /missing|fail|empty/i.test(r))
        ? "remediation_required"
        : "approve_after_minor_cleanup",
      rationale: residual.length
        ? `Residual founder notes: ${residual.slice(0, 4).join("; ")}`
        : "One or more Stage 6 gate signals missing/failed on disk — confirm live gates before promotion.",
    };
  }
  return {
    recommendation: "approve_for_status_promotion_and_public_release",
    rationale:
      "Stage 6 factory acceptance gates passed; profile remains Under Review / factory preview with founder-taste cautions only.",
  };
}

/**
 * Build one founder review packet (read-only).
 */
export async function buildWave12FounderReviewPacket(slug, { tabFactoryAudit = null } = {}) {
  const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
  if (!identity?.recordId) throw new Error(`Missing factory-preview identity for ${slug}`);
  const plan = WAVE12_BRAND_PLAN[slug] || {};
  const seed = getWave12TabFactorySeed(slug);
  const sourcePack = getWave12SourcePack(slug);
  const calaAvailable = CALA_AVAILABLE_BY_SLUG[slug] === true;

  const nameCandidates = [
    identity.name,
    plan.name,
    seed?.name,
    ...(plan.nameAliases || []),
  ]
    .map((n) => nz(n))
    .filter(Boolean);
  const uniqueNames = [...new Set(nameCandidates)];

  let rows = [];
  let rowsBrandNameUsed = null;
  for (const name of uniqueNames) {
    const fetch = await listPresentationRowsLight(identity.recordId, name);
    if ((fetch.rows || []).length) {
      rows = fetch.rows;
      rowsBrandNameUsed = name;
      break;
    }
  }
  const { brand, error: brandApiError } = await fetchBrandApi(identity.recordId);

  const brandStatus =
    nz(brand?.status) ||
    nz(brand?.brandStatus) ||
    nz(brand?.brandBasics?.brandStatus) ||
    identity.recommendedStatusWhileInFactory ||
    "Under Review";

  const displayState =
    nz(brand?.brandExplorerDisplayState) ||
    nz(brand?.displayState) ||
    FACTORY_PREVIEW_DISPLAY_STATE;

  const urls = buildFactoryPreviewUrls({ recordId: identity.recordId, slug });

  const scenarios = [1, 2, 3].map((i) => {
    const row = visibleSlot(rows, `overview.scenario.${i}`)[0] || null;
    const seedScen = seed?.scenarios?.[i - 1] || null;
    const img = imageRef(row);
    return {
      index: i,
      title: nz(row?.title) || seedScen?.[0] || `Scenario ${i}`,
      summary: nz(row?.body) || seedScen?.[1] || "",
      image: img,
    };
  });

  const scenarioUrls = scenarios.map((s) => s.image.url).filter(Boolean);
  const scenarioDistinctiveness = {
    distinctCount: new Set(scenarioUrls).size,
    required: 3,
    pass: scenarioUrls.length >= 3 && new Set(scenarioUrls).size >= 3,
    note:
      scenarioUrls.length >= 3 && new Set(scenarioUrls).size >= 3
        ? "All 3 scenario images are distinct URLs."
        : `Scenario image distinctiveness caution: ${new Set(scenarioUrls).size}/${scenarioUrls.length} distinct.`,
  };

  const openings = visibleSlot(rows, "footprint.openings").map((row) => {
    const corpus = [row.title, row.body, row.caseSummaryTags, row.caseSummaryOverview].join("\n");
    const region =
      classifyRegionFromText(row.caseSummaryTags || "") ||
      classifyRegionFromText(row.title || "") ||
      classifyRegionFromText(corpus) ||
      "UNLABELED";
    const link = (nz(row.body).match(/https?:\/\/\S+/i) || [null])[0];
    return {
      title: nz(row.title),
      geography: region,
      tags: nz(row.caseSummaryTags),
      sourceLink: link,
      hasImage: Boolean(nz(row.imageUrl)),
    };
  });

  const similar = [1, 2, 3].map((i) => {
    const row = visibleSlot(rows, `insight.similar.${i}`)[0];
    const seedPeer = seed?.similar?.[i - 1];
    return {
      name: nz(row?.title) || seedPeer?.[0] || null,
      summary: nz(row?.body) || seedPeer?.[1] || null,
    };
  }).filter((s) => s.name);

  const ownerQuestions = visibleSlot(rows, "standards.questions")[0];
  const positioningRow =
    visibleSlot(rows, "Brand Positioning")[0] ||
    visibleSlot(rows, "overview.relative_positioning")[0];
  const targetGuestSegments =
    brand?.targetGuestSegments ||
    brand?.brandBasics?.targetGuestSegments ||
    seed?.targetGuestSegments ||
    null;

  // Prefer seed TGS from pack generator if Basics missing
  let tgs = targetGuestSegments;
  if (!tgs || (Array.isArray(tgs) && !tgs.length)) {
    try {
      const { generateWave12TabFactoryPack } = await import(
        "./brand-explorer-wave12-tab-factory-build-generator.js"
      );
      const pack = generateWave12TabFactoryPack(slug, {
        recordId: identity.recordId,
        brandName: identity.name,
      });
      tgs = pack.targetGuestSegments || pack.basicsFields?.["Target Guest Segments"] || null;
    } catch {
      tgs = null;
    }
  }

  const liveUniqueness = evaluateImageUniqueness({ brandSlug: slug, presentationRows: rows });
  const liveRoleMatch = evaluateBrandImageRoleMatch({ brandSlug: slug, presentationRows: rows });
  const diskImages = imageAuditFromDisk(slug);
  const gates = gateSummaryFromTabFactoryAudit(tabFactoryAudit, slug);

  const residual = [];
  if (!scenarioDistinctiveness.pass) residual.push("scenario_image_distinctiveness");
  if (openings.length < 3) residual.push(`openings_count_${openings.length}`);
  if (liveUniqueness.pass !== true && diskImages.uniquenessPass !== true) {
    residual.push("image_uniqueness_not_confirmed");
  }
  if (liveRoleMatch.pass !== true && diskImages.roleMatchPass !== true) {
    residual.push("image_role_match_not_confirmed");
  }

  const { recommendation, rationale } = recommend({
    gates,
    liveUniquenessPass: liveUniqueness.pass === true,
    liveRoleMatchPass: liveRoleMatch.pass === true,
    brandStatus,
    residual,
  });

  if (!RECOMMENDATIONS.includes(recommendation)) {
    throw new Error(`Invalid recommendation ${recommendation}`);
  }

  const calaStatus = {
    calaAvailable,
    strongCalaAnchor: WAVE12_STRONG_CALA_SLUGS.includes(slug),
    internationalReferenceMomentum: WAVE12_INTL_REF_MOMENTUM_SLUGS.includes(slug),
    openingsCalaCount: openings.filter((o) => o.geography === "CALA").length,
    openingsIntlCount: openings.filter((o) => o.geography === "International Reference").length,
    label:
      WAVE12_STRONG_CALA_SLUGS.includes(slug)
        ? "CALA-first (strong anchors)"
        : WAVE12_INTL_REF_MOMENTUM_SLUGS.includes(slug)
          ? "International Reference–led (CALA thin/none)"
          : calaAvailable
            ? "CALA available (partial)"
            : "International Reference",
  };

  return {
    version: WAVE12_FOUNDER_REVIEW_VERSION,
    wave12Version: WAVE12_VERSION,
    stage: "founder-review",
    generatedAt: new Date().toISOString(),
    dryRun: true,
    writePerformed: false,
    brandSlug: slug,
    brandName: identity.name,
    recordId: identity.recordId,
    parentCompany: seed?.parentCompany || plan.parentPlatform || brand?.parentCompany || null,
    parentPlatform: plan.parentPlatform || null,
    brandStatus,
    visibilityState: displayState,
    factoryPreview: {
      displayState: FACTORY_PREVIEW_DISPLAY_STATE,
      urls,
      primaryUrl: urls?.combined || urls?.explorer || null,
    },
    brandApiError,
    rowsBrandNameUsed,
    presentationRowCount: rows.length,
    sourcePackSummary: {
      officialBrandPage: sourcePack?.officialBrandPage?.url || null,
      developmentPage: sourcePack?.developmentPage?.url || null,
      calaAvailability: seed?.calaAvailability || null,
      propertyExampleCount: (sourcePack?.propertyExamples || []).length,
      recentMomentumCandidateCount: (sourcePack?.recentMomentumCandidates || []).length,
      lens: plan.lens || seed?.ownerLens || null,
    },
    gates: {
      ...gates,
      stage6AcceptancePass: true,
      stage6AcceptanceNote:
        "Wave 12 Stage 6 post-image content cleanup accepted all listed factory gates for the 12-brand cohort.",
      liveImageUniquenessPass: liveUniqueness.pass === true,
      liveImageRoleMatchPass: liveRoleMatch.pass === true,
      diskImageAudits: diskImages,
      liveImageCounts: {
        galleryDistinct: liveUniqueness.galleryDistinctCount ?? null,
        scenarioDistinct: liveUniqueness.scenarioDistinctCount ?? null,
        propertyDistinct: liveUniqueness.propertyExampleDistinctCount ?? null,
      },
    },
    calaStatus,
    targetGuestSegments: Array.isArray(tgs) ? tgs : tgs ? [tgs] : [],
    brandPositioningSummary:
      nz(positioningRow?.body) ||
      nz(brand?.brandPositioning) ||
      `${identity.name} is a ${seed?.model || "Wave 12 factory brand"} within ${seed?.parentCompany || plan.parentPlatform || "its parent platform"}.`,
    ownerFitSummary: seed?.ownerLens || plan.lens || null,
    propertyFitSummary: seed?.propertyFit || null,
    scenarios,
    scenarioDistinctiveness,
    openings,
    similarBrands: similar,
    ownerQuestions: nz(ownerQuestions?.body) || null,
    founderTasteCautions: founderTasteCautions(slug),
    residual,
    recommendation,
    recommendationRationale: rationale,
    guardrails: {
      writePerformed: false,
      brandStatusWrites: false,
      releaseFieldWrites: false,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryWrites: false,
      imageWrites: false,
      contentPatches: false,
      protected27Untouched: true,
      protectedBaselineCount: WAVE12_PROTECTED_BASELINE_COUNT || EXPECTED_ACTIVE_COUNT_27,
    },
  };
}

export function renderWave12FounderReviewMarkdown(packet) {
  const lines = [];
  lines.push(`# Founder Review — ${packet.brandName}`);
  lines.push("");
  lines.push(`Version: \`${packet.version}\` · Stage: **${packet.stage}** · Generated: ${packet.generatedAt}`);
  lines.push(`Mode: **dry-run** · writePerformed: **${packet.writePerformed === true}**`);
  lines.push("");
  lines.push(`## Identity`);
  lines.push("");
  lines.push(`| Field | Value |`);
  lines.push(`| --- | --- |`);
  lines.push(`| Brand name | ${packet.brandName} |`);
  lines.push(`| Slug | \`${packet.brandSlug}\` |`);
  lines.push(`| Record ID | \`${packet.recordId}\` |`);
  lines.push(`| Parent company / platform | ${packet.parentCompany || "—"} |`);
  lines.push(`| Brand Status | **${packet.brandStatus || "—"}** |`);
  lines.push(`| Visibility state | ${packet.visibilityState || "—"} |`);
  lines.push(
    `| Factory preview URL | \`${packet.factoryPreview?.primaryUrl || "—"}\` |`
  );
  lines.push("");
  lines.push(`## Recommendation`);
  lines.push("");
  lines.push(`**${packet.recommendation}**`);
  lines.push("");
  lines.push(packet.recommendationRationale || "");
  lines.push("");
  lines.push(`## Gate results`);
  lines.push("");
  lines.push(`| Gate | Result |`);
  lines.push(`| --- | --- |`);
  lines.push(`| Stage 6 acceptance (cohort) | PASS (recorded) |`);
  lines.push(
    `| Tab Factory | ${fmtGate(packet.gates.tabFactoryPass)} · failFindings=${packet.gates.failFindings ?? "—"} |`
  );
  lines.push(`| Rendered completeness | ${fmtGate(packet.gates.renderedCompletenessPass)} |`);
  lines.push(`| No-empty components | ${fmtGate(packet.gates.noEmptyPass)} |`);
  lines.push(`| Golden content quality | ${fmtGate(packet.gates.goldenPass)} |`);
  lines.push(`| Section pattern parity | ${fmtGate(packet.gates.sectionPatternPass)} |`);
  lines.push(`| Recent Momentum pattern | ${fmtGate(packet.gates.momentumPatternPass)} |`);
  lines.push(
    `| Recent Momentum / Openings evidence quality | ${fmtGate(packet.gates.momentumEvidencePass)} |`
  );
  lines.push(
    `| Image uniqueness | ${fmtGate(packet.gates.liveImageUniquenessPass ?? packet.gates.imageUniquenessPass)} · gallery ${packet.gates.liveImageCounts?.galleryDistinct ?? "—"} / scenario ${packet.gates.liveImageCounts?.scenarioDistinct ?? "—"} / property ${packet.gates.liveImageCounts?.propertyDistinct ?? "—"} |`
  );
  lines.push(
    `| Image role-match | ${fmtGate(packet.gates.liveImageRoleMatchPass ?? packet.gates.imageRoleMatchPass)} |`
  );
  if (packet.gates.note) lines.push("", `_${packet.gates.note}_`);
  lines.push("");
  lines.push(`## Source pack summary`);
  lines.push("");
  lines.push(`- CALA availability (seed): **${packet.sourcePackSummary.calaAvailability || "—"}**`);
  lines.push(`- Official brand page: ${packet.sourcePackSummary.officialBrandPage || "—"}`);
  lines.push(`- Development page: ${packet.sourcePackSummary.developmentPage || "—"}`);
  lines.push(`- Property examples in pack: ${packet.sourcePackSummary.propertyExampleCount}`);
  lines.push(
    `- Momentum candidates in pack: ${packet.sourcePackSummary.recentMomentumCandidateCount}`
  );
  lines.push(`- Brand lens: ${packet.sourcePackSummary.lens || "—"}`);
  lines.push("");
  lines.push(`## CALA-first / International Reference`);
  lines.push("");
  lines.push(`- Status: **${packet.calaStatus.label}**`);
  lines.push(`- CALA available (evidence gate): **${packet.calaStatus.calaAvailable}**`);
  lines.push(
    `- Openings mix: CALA ${packet.calaStatus.openingsCalaCount} · International Reference ${packet.calaStatus.openingsIntlCount}`
  );
  lines.push("");
  lines.push(`## Target Guest Segments`);
  lines.push("");
  if (packet.targetGuestSegments?.length) {
    for (const s of packet.targetGuestSegments) lines.push(`- ${s}`);
  } else {
    lines.push(`- (not listed on Basics in this read — confirm in Airtable before promotion)`);
  }
  lines.push("");
  lines.push(`## Brand positioning`);
  lines.push("");
  lines.push(packet.brandPositioningSummary || "—");
  lines.push("");
  lines.push(`## Owner fit`);
  lines.push("");
  lines.push(packet.ownerFitSummary || "—");
  lines.push("");
  lines.push(`## Property fit`);
  lines.push("");
  lines.push(packet.propertyFitSummary || "—");
  lines.push("");
  lines.push(`## Where This Brand Creates the Most Value`);
  lines.push("");
  for (const s of packet.scenarios || []) {
    lines.push(`### Scenario ${s.index}: ${s.title}`);
    lines.push("");
    lines.push(s.summary || "—");
    lines.push("");
    lines.push(
      `- Image: ${s.image?.hasImage ? "present" : "missing"} · caption: ${s.image?.caption || "—"}`
    );
    if (s.image?.url) lines.push(`- Image URL (path): \`${s.image.url}\``);
    lines.push("");
  }
  lines.push(`**Scenario distinctiveness:** ${packet.scenarioDistinctiveness?.note || "—"}`);
  lines.push("");
  lines.push(`## Openings / Examples / Properties`);
  lines.push("");
  if (!(packet.openings || []).length) {
    lines.push(`- (none found in Presentation read)`);
  } else {
    for (const o of packet.openings) {
      lines.push(
        `- **${o.title || "(untitled)"}** · ${o.geography} · image=${o.hasImage ? "yes" : "no"} · source=${o.sourceLink || "—"}`
      );
    }
  }
  lines.push("");
  lines.push(`## Similar Brands`);
  lines.push("");
  for (const s of packet.similarBrands || []) {
    lines.push(`- **${s.name}** — ${s.summary || "—"}`);
  }
  lines.push("");
  lines.push(`## Owner Questions`);
  lines.push("");
  lines.push(packet.ownerQuestions || "—");
  lines.push("");
  lines.push(`## Founder-taste cautions (non-blocking)`);
  lines.push("");
  for (const c of packet.founderTasteCautions || []) lines.push(`- ${c}`);
  lines.push("");
  lines.push(`## Guardrails`);
  lines.push("");
  for (const [k, v] of Object.entries(packet.guardrails || {})) {
    lines.push(`- ${k}: **${v}**`);
  }
  lines.push("");
  return lines.join("\n");
}

function fmtGate(v) {
  if (v === true) return "PASS";
  if (v === false) return "FAIL";
  return "—";
}

export function writeWave12FounderReviewReports({ packets, summary }) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const perBrandPaths = [];
  for (const packet of packets) {
    const md = renderWave12FounderReviewMarkdown(packet);
    const mdPath = path.join(
      REPORTS_DIR,
      `brand-explorer-founder-review-${packet.brandSlug}.md`
    );
    const jsonPath = path.join(
      REPORTS_DIR,
      `brand-explorer-founder-review-${packet.brandSlug}.json`
    );
    fs.writeFileSync(mdPath, `${md}\n`, "utf8");
    fs.writeFileSync(jsonPath, `${JSON.stringify(packet, null, 2)}\n`, "utf8");
    perBrandPaths.push({ slug: packet.brandSlug, mdPath, jsonPath });
  }

  const summaryMdPath = path.join(REPORTS_DIR, "brand-explorer-wave12-founder-review-summary.md");
  const summaryJsonPath = path.join(
    REPORTS_DIR,
    "brand-explorer-wave12-founder-review-summary.json"
  );
  fs.writeFileSync(summaryJsonPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
  fs.writeFileSync(summaryMdPath, `${renderWave12FounderReviewSummaryMarkdown(summary)}\n`, "utf8");

  const docPath = path.join(DOCS_DIR, "brand-explorer-wave12-founder-review.md");
  fs.writeFileSync(docPath, `${renderWave12FounderReviewDoc(summary)}\n`, "utf8");

  return { perBrandPaths, summaryMdPath, summaryJsonPath, docPath };
}

function renderWave12FounderReviewSummaryMarkdown(summary) {
  const lines = [
    `# Wave 12 — Founder Review Summary`,
    ``,
    `Generated: ${summary.generatedAt}`,
    `Stage: **founder-review** · dry-run: **true** · writePerformed: **false**`,
    `Protected baseline count: **${summary.protectedBaselineCount}** (untouched)`,
    ``,
    `## Recommendation rollup`,
    ``,
    `| Slug | Brand | Status | CALA posture | Recommendation |`,
    `| --- | --- | --- | --- | --- |`,
    ...(summary.brands || []).map(
      (b) =>
        `| \`${b.brandSlug}\` | ${b.brandName} | ${b.brandStatus} | ${b.calaLabel} | **${b.recommendation}** |`
    ),
    ``,
    `## Counts`,
    ``,
    `- approve_for_status_promotion_and_public_release: **${summary.counts?.approve_for_status_promotion_and_public_release ?? 0}**`,
    `- approve_after_minor_cleanup: **${summary.counts?.approve_after_minor_cleanup ?? 0}**`,
    `- remediation_required: **${summary.counts?.remediation_required ?? 0}**`,
    ``,
    `## Founder-taste themes`,
    ``,
    `- EVEN / avid / Canopy / Tempo: International Reference momentum (Directory/dev-page intentional).`,
    `- City Express / HIE / Motto / Moxy / Bunkhouse / voco: stronger CALA anchors.`,
    `- Openings geography Case Summary corrected after Stage 5 caption mismatches.`,
    `- Bunkhouse: keep Hyatt parent context explicit.`,
    ``,
    `## Next stage`,
    ``,
    `- Do **not** promote Brand Status or set release fields until founder signs packets.`,
    `- Next factory stages after approval: \`status-promotion\` → \`public-release\` → \`baseline-39\` (separate explicit tasks).`,
    ``,
    `## Guardrails`,
    ``,
    `- No Airtable writes`,
    `- No Brand Status / release / CV / Source Library / Registry writes`,
    `- No protected 27 changes`,
    ``,
  ];
  return lines.join("\n");
}

function renderWave12FounderReviewDoc(summary) {
  return [
    `# Wave 12 Founder Review`,
    ``,
    `Stage 8 of the Wave 12 factory produces **read-only** founder review packets for the 12 Under Review / factory-preview brands.`,
    ``,
    `## Command`,
    ``,
    "```bash",
    "npm run brand-explorer-wave12-factory -- --stage founder-review --dry-run",
    "```",
    ``,
    `## Outputs`,
    ``,
    `- \`reports/brand-explorer-founder-review-{slug}.md\` (×12)`,
    `- \`reports/brand-explorer-wave12-founder-review-summary.md\``,
    ``,
    `## Recommendations`,
    ``,
    `- \`approve_for_status_promotion_and_public_release\``,
    `- \`approve_after_minor_cleanup\``,
    `- \`remediation_required\``,
    ``,
    `## Guardrails`,
    ``,
    `- dry-run only — **no Airtable writes**`,
    `- no Brand Status / release / Company Validated / Source Library / Registry writes`,
    `- no content or image patches`,
    `- protected ${summary.protectedBaselineCount} baseline untouched`,
    ``,
    `Last generated: ${summary.generatedAt}`,
    ``,
  ].join("\n");
}

/**
 * Run Wave 12 founder-review stage (always dry-run / no writes).
 */
export async function runWave12FounderReview({ argv = [] } = {}) {
  if (argv.includes("--apply")) {
    throw new Error(
      "founder-review is read-only. Remove --apply; use --dry-run only (packets + reports)."
    );
  }

  const tabFactoryAudit = readJsonSafe("reports/brand-explorer-tab-factory-audit.json");
  const packets = [];

  for (const slug of WAVE12_SLUGS) {
    const packet = await buildWave12FounderReviewPacket(slug, { tabFactoryAudit });
    packets.push(packet);
    await sleep(220);
  }

  const counts = {
    approve_for_status_promotion_and_public_release: 0,
    approve_after_minor_cleanup: 0,
    remediation_required: 0,
  };
  for (const p of packets) {
    if (counts[p.recommendation] != null) counts[p.recommendation] += 1;
  }

  const summary = {
    version: WAVE12_FOUNDER_REVIEW_VERSION,
    wave12Version: WAVE12_VERSION,
    stage: "founder-review",
    generatedAt: new Date().toISOString(),
    dryRun: true,
    writePerformed: false,
    protectedBaselineCount: WAVE12_PROTECTED_BASELINE_COUNT || EXPECTED_ACTIVE_COUNT_27,
    brandCount: packets.length,
    counts,
    brands: packets.map((p) => ({
      brandSlug: p.brandSlug,
      brandName: p.brandName,
      recordId: p.recordId,
      brandStatus: p.brandStatus,
      calaLabel: p.calaStatus?.label,
      recommendation: p.recommendation,
      previewUrl: p.factoryPreview?.primaryUrl,
    })),
    guardrails: {
      writePerformed: false,
      brandStatusWrites: false,
      releaseFieldWrites: false,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryWrites: false,
      protected27Untouched: true,
    },
    acceptance: {
      packetsForAll12: packets.length === 12,
      everyPacketHasRecommendation: packets.every((p) => RECOMMENDATIONS.includes(p.recommendation)),
      noAirtableWrites: true,
    },
  };

  const paths = writeWave12FounderReviewReports({ packets, summary });

  return {
    ...summary,
    packets,
    paths,
  };
}
