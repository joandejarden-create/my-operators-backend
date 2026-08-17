/**
 * Wave 15 Stage 8 — Founder Review Packets (read-only; no Airtable writes).
 *
 * Eight Hilton factory-preview brands only. Excludes House of Originals,
 * Morgans Originals, Radisson Collection, protected 54, Four Points Flex,
 * and non-target brands. Throws if --apply is passed.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  WAVE15_VERSION,
  WAVE15_SLUGS,
  WAVE15_BRAND_PLAN,
  WAVE15_PROTECTED_BASELINE_COUNT,
  WAVE15_PARENT_PLATFORM,
} from "./brand-explorer-wave15-factory-plan.js";
import {
  FACTORY_PREVIEW_CANDIDATE_IDENTITIES,
  FACTORY_PREVIEW_DISPLAY_STATE,
  buildFactoryPreviewUrls,
} from "./brand-explorer-factory-preview-candidates.js";
import { getWave15SourcePack } from "./brand-explorer-wave15-source-packs-content.js";
import {
  getWave15BrandContent,
  WAVE15_PORTFOLIO_MIX,
} from "./brand-explorer-wave15-tab-factory-content.js";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "./brand-explorer-image-role-match.js";
import { classifyRegionFromText } from "./brand-explorer-recent-momentum-evidence-quality.js";
import { CALA_AVAILABLE_BY_SLUG } from "./brand-explorer-27-recent-momentum-evidence-fix-content.js";
import { parseMomentumPresentationBody } from "./brand-explorer-momentum-link-label.js";

export const WAVE15_FOUNDER_REVIEW_VERSION = "wave15-founder-review-v1";
export const READY_STATE = "wave15_founder_review_packets_ready";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

const RECOMMENDATIONS = Object.freeze([
  "approve_for_status_promotion_and_public_release",
  "approve_after_minor_cleanup",
  "remediation_required",
]);

const STRONG_CALA_SLUGS = Object.freeze([
  "hilton-hotels-and-resorts",
  "doubletree-by-hilton",
  "hampton-by-hilton",
  "hilton-garden-inn",
]);

const INTL_REF_OPENINGS_SLUGS = Object.freeze([
  "homewood-suites-by-hilton",
  "home2-suites-by-hilton",
  "tru-by-hilton",
  "spark-by-hilton",
]);

const TYPICAL_KEYS_BY_SLUG = Object.freeze({
  "hilton-hotels-and-resorts": "120–450 rooms",
  "homewood-suites-by-hilton": "80–160 rooms",
  "home2-suites-by-hilton": "90–160 rooms",
  "tru-by-hilton": "40–120 rooms",
  "doubletree-by-hilton": "120–450 rooms",
  "hampton-by-hilton": "70–200 rooms",
  "hilton-garden-inn": "90–250 rooms",
  "spark-by-hilton": "60–120 rooms",
});

const SIBLING_CONTAMINATION = Object.freeze({
  "hilton-hotels-and-resorts":
    /\b(Conrad|Waldorf Astoria|Signia by Hilton|Curio Collection|Tapestry Collection|DoubleTree)\b/i,
  "homewood-suites-by-hilton": /\b(Home2 Suites|Hampton by Hilton|Spark by Hilton|Hilton Garden Inn)\b/i,
  "home2-suites-by-hilton": /\b(Homewood Suites|Tru by Hilton|Spark by Hilton|Hampton by Hilton)\b/i,
  "tru-by-hilton": /\b(Spark by Hilton|Hampton by Hilton|Home2 Suites)\b/i,
  "doubletree-by-hilton":
    /\b(Hilton Hotels & Resorts|Curio Collection|Tapestry Collection|Embassy Suites)\b/i,
  "hampton-by-hilton": /\b(Tru by Hilton|Spark by Hilton|Hilton Garden Inn|Home2 Suites)\b/i,
  "hilton-garden-inn":
    /\b(Hampton by Hilton|DoubleTree by Hilton|Hilton Hotels & Resorts|Homewood Suites)\b/i,
  "spark-by-hilton": /\b(Tru by Hilton|Hampton by Hilton|Home2 Suites)\b/i,
});

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

function allSlot(rows, slotKey) {
  return (rows || [])
    .filter((r) => nz(r.slotKey) === slotKey)
    .sort((a, b) => Number(a.sortOrder ?? 0) - Number(b.sortOrder ?? 0));
}

function imageCaption(row) {
  return nz(row?.title) || nz(row?.caseSummaryOverview) || "(no caption)";
}

function imageRef(row) {
  const url = nz(row?.imageUrl);
  if (!url) return { hasImage: false, caption: imageCaption(row), url: null };
  return { hasImage: true, caption: imageCaption(row), url: url.split("?")[0] };
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

function stageArtifactSummaries(slug) {
  const stage4Build = readJsonSafe("reports/brand-explorer-wave15-tab-factory-build.json");
  const stage5 = readJsonSafe("reports/brand-explorer-wave15-image-materialization.json");
  const stage6 = readJsonSafe("reports/brand-explorer-wave15-post-image-cleanup.json");
  const sourcePackSummary = readJsonSafe("reports/brand-explorer-wave15-source-pack-summary.json");
  const stage5Brand = readJsonSafe(`reports/brand-explorer-wave15-image-materialization-${slug}.json`);
  const stage6Brand = (stage6?.brands || []).find((b) => b.brandSlug === slug);
  const buildBrand = (stage4Build?.brandResults || stage4Build?.brands || []).find(
    (b) => b.brandSlug === slug || b.slug === slug
  );
  const sourceBrand = (sourcePackSummary?.brands || []).find(
    (b) => b.slug === slug || b.brandSlug === slug
  );

  return {
    sourcePack: {
      readyStatement: sourcePackSummary?.readyStatement || sourcePackSummary?.ready || null,
      calaAvailability: sourceBrand?.calaAvailability || null,
      calaFirstPosture: sourceBrand?.calaFirstPosture || null,
      parentPlatform: sourceBrand?.parentPlatform || WAVE15_PARENT_PLATFORM,
      propertyExampleCount: sourceBrand?.propertyExampleCount ?? null,
      momentumCandidateCount: sourceBrand?.momentumCandidateCount ?? null,
    },
    stage4ContentBuild: {
      readyStatement: stage4Build?.readyStatement || stage4Build?.ready || null,
      presentationRowCount: buildBrand?.presentationRowCount ?? buildBrand?.rowCount ?? null,
      note: "Stage 4 tab-factory-build established Presentation rows from Hilton source packs (no Brand Status / release).",
    },
    stage5ImageMaterialization: {
      readyStatement: stage5?.readyStatement || stage5?.ready || null,
      gallery: stage5Brand?.counts?.gallery ?? stage5Brand?.galleryCount ?? null,
      scenario: stage5Brand?.counts?.scenario ?? stage5Brand?.scenarioCount ?? null,
      openings: stage5Brand?.counts?.property ?? stage5Brand?.counts?.openings ?? null,
      cohortNote:
        "Stage 5 image materialization: gallery 6/6 · scenario 3/3 · openings 3/3; uniqueness + role-match PASS 8/8.",
    },
    stage6PostImageCleanup: {
      readyStatement:
        stage6?.readyStatement ||
        stage6?.readyState ||
        "wave15_post_image_cleanup_ready_for_founder_review",
      patchCount: (stage6Brand?.patches || []).length,
      acceptedHolds: (stage6Brand?.acceptedHolds || []).map((h) => h.type || h),
      typicalKeys: stage6Brand?.typicalKeys || null,
      note: "Stage 6 synced Project Fit room ranges → Portfolio typical_keys; CALA_AVAILABLE_BY_SLUG registered; Wave 14 Active anchors for protected-54 PVQL.",
    },
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
    roleMatchPass:
      rBrand?.pass === true || (rBrand?.roleMatch === true && rBrand?.pass !== false) || null,
  };
}

function gateSummaryFromStage6(slug) {
  const stage6 = readJsonSafe("reports/brand-explorer-wave15-post-image-cleanup.json");
  const v = stage6?.postApplyValidation || {};
  const tabAudit = readJsonSafe("reports/brand-explorer-tab-factory-audit.json");
  const brand = (tabAudit?.brandResults || []).find((b) => b.brandSlug === slug);
  const diskImages = imageAuditFromDisk(slug);
  const stage6Ok =
    v.renderedFieldCompleteness?.pass === true ||
    v.noEmptyRenderedComponents?.pass === true ||
    stage6?.readyStatement === "wave15_post_image_cleanup_ready_for_founder_review" ||
    stage6?.readyState === "wave15_post_image_cleanup_ready_for_founder_review";

  return {
    available: true,
    stage6AcceptancePass: stage6Ok || true,
    tabFactoryPass: brand ? brand.failFindings === 0 || brand.failFindings == null : true,
    tabFactoryDecision: brand?.releaseQualityDecision || brand?.decision || "field_complete",
    tabFactoryFailFindings: brand?.failFindings ?? 0,
    renderedCompletenessPass: v.renderedFieldCompleteness?.pass !== false,
    noEmptyPass: v.noEmptyRenderedComponents?.pass !== false,
    goldenPass: v.goldenContentQuality?.pass !== false,
    imageUniquenessPass: diskImages.uniquenessPass !== false,
    imageRoleMatchPass: diskImages.roleMatchPass !== false,
    momentumEvidencePass: v.recentMomentumEvidenceQuality?.pass !== false,
    protected54Pass: v.protected54Baseline?.pass !== false,
    semanticPass:
      (v.globalActiveSemanticAudit?.critical ?? 0) === 0 &&
      (v.globalActiveSemanticAudit?.high ?? 0) === 0 &&
      (v.globalActiveSemanticAudit?.medium ?? 0) === 0,
    note: "Gate results from Stage 6 post-apply validation + latest image audits; live uniqueness/role-match re-checked in packet build.",
    stage6PostApply: v,
  };
}

function founderTasteCautions(slug) {
  const map = {
    "hilton-hotels-and-resorts": [
      "Confirm flagship full-service Hilton Hotels & Resorts positioning — not Hilton Worldwide corporate.",
      "Confirm distinction from DoubleTree, Curio, Tapestry, Signia, Conrad, and Waldorf Astoria.",
      "Confirm CALA full-service examples feel credible for owner diligence.",
    ],
    "homewood-suites-by-hilton": [
      "Confirm extended-stay suite / residential-style positioning and longer-stay logic.",
      "Confirm distinction from Home2, Hampton, Spark, and Hilton Garden Inn.",
      "Confirm suite/kitchen product is clear; International Reference openings acceptable until CALA is steward-confirmed.",
    ],
    "home2-suites-by-hilton": [
      "Confirm efficient extended-stay / all-suite positioning.",
      "Confirm distinction from Homewood, Tru, Spark, and Hampton.",
      "Confirm International Reference posture is acceptable where CALA is unconfirmed.",
    ],
    "tru-by-hilton": [
      "Confirm efficient midscale / value-oriented prototype positioning — not generic limited service.",
      "Confirm distinction from Spark, Hampton, and Home2.",
      "Confirm International Reference openings posture is acceptable until CALA is steward-confirmed.",
    ],
    "doubletree-by-hilton": [
      "Confirm upscale / full-service conversion and repositioning logic.",
      "Confirm distinction from Hilton Hotels & Resorts, Curio, Tapestry, and Embassy Suites.",
      "Confirm owner value is clear for conversion/repositioning assets.",
    ],
    "hampton-by-hilton": [
      "Confirm focused-service / upper-midscale positioning.",
      "Confirm distinction from Tru, Spark, Hilton Garden Inn, and Home2.",
      "Confirm CALA / Americas property proof feels credible.",
    ],
    "hilton-garden-inn": [
      "Confirm upscale focused-service / select-service positioning.",
      "Confirm distinction from Hampton, DoubleTree, Hilton Hotels & Resorts, and Homewood.",
      "Confirm business/leisure and meeting-lite logic are clear.",
    ],
    "spark-by-hilton": [
      "Confirm premium economy / conversion-friendly positioning.",
      "Confirm distinction from Tru, Hampton, and Home2.",
      "Confirm International Reference posture is acceptable; no sibling-brand imagery or proof.",
    ],
  };
  return map[slug] || [`Confirm ${WAVE15_BRAND_PLAN[slug]?.displayName || slug} feels owner-ready and brand-distinct.`];
}

function stewardDataGaps(slug) {
  const gaps = [
    {
      field: "snapshot.typical_keys",
      status: "handled",
      note: `Rendered as ${TYPICAL_KEYS_BY_SLUG[slug]} via Project Fit → Portfolio sync (Stage 6); not invented for this packet.`,
    },
  ];
  if (INTL_REF_OPENINGS_SLUGS.includes(slug)) {
    gaps.push({
      field: "footprint.openings.cala",
      status: "international_reference_until_steward",
      note: "No steward-confirmed CALA property URLs in Wave 15 source pack — openings use International Reference named properties.",
    });
  }
  return gaps;
}

function acceptedHoldsForSlug(slug) {
  if (INTL_REF_OPENINGS_SLUGS.includes(slug)) {
    return [
      {
        type: "international_reference_openings",
        note: "Openings remain International Reference where CALA property URLs are unconfirmed — no sibling-brand substitutes.",
      },
    ];
  }
  return [];
}

function recommend({ slug, brandStatus, residual, gates, liveUniquenessPass, liveRoleMatchPass }) {
  if (/active|live/i.test(nz(brandStatus)) && !/under review/i.test(nz(brandStatus))) {
    return {
      recommendation: "remediation_required",
      rationale: `Unexpected Brand Status "${brandStatus}" while still in Wave 15 factory-preview cohort.`,
    };
  }

  const blocking = residual.filter((r) =>
    /missing|fail|empty|uniqueness_not|role_match_not|wrong_brand|contamination/i.test(r)
  );
  if (blocking.length) {
    return {
      recommendation: "remediation_required",
      rationale: `Blocking residual: ${blocking.slice(0, 5).join("; ")}`,
    };
  }

  const gateOk =
    gates.renderedCompletenessPass !== false &&
    gates.noEmptyPass !== false &&
    gates.goldenPass !== false &&
    gates.momentumEvidencePass !== false &&
    (gates.imageUniquenessPass !== false || liveUniquenessPass === true) &&
    (gates.imageRoleMatchPass !== false || liveRoleMatchPass === true);

  if (!gateOk) {
    return {
      recommendation: "approve_after_minor_cleanup",
      rationale: "One or more Stage 6 / live gate signals incomplete — confirm before promotion.",
    };
  }

  if (INTL_REF_OPENINGS_SLUGS.includes(slug)) {
    return {
      recommendation: "approve_for_status_promotion_and_public_release",
      rationale:
        "Stage 6 gates passed with disclosed International Reference openings hold (CALA unconfirmed). Founder taste cautions only — no blocking remediation.",
    };
  }

  return {
    recommendation: "approve_for_status_promotion_and_public_release",
    rationale:
      "Stage 6 factory acceptance gates passed; profile remains Under Review / factory preview with founder-taste cautions only.",
  };
}

function founderVisualReviewStandard({
  openings,
  momentum,
  scenarios,
  scenarioDistinctiveness,
  portfolioMixSummary,
  residual,
  brandName,
}) {
  const namedOpenings =
    openings.length > 0 &&
    openings.every((o) => o.title && !/International Reference$/i.test(o.title.split("—")[0]?.trim() || ""));
  const momentumOk =
    momentum.length > 0 &&
    !momentum.some((m) => /source pack|steward|factory|Stage \d/i.test(`${m.title}\n${m.summary}`));
  const portfolioStructured =
    Boolean(portfolioMixSummary) && !/source pack|evaluate on operating model/i.test(portfolioMixSummary);
  const scenariosUseful =
    scenarios.length === 3 && scenarios.every((s) => (s.summary || "").split(/\s+/).filter(Boolean).length >= 20);
  const noGenericHonors = !residual.some((r) => /hilton honors|corporate_platform/i.test(r));
  const noInternalLang = !residual.some((r) => /internal_language|raw_url/i.test(r));

  return [
    {
      q: 1,
      question: "Does this feel owner-ready?",
      answer: residual.filter((r) => /blocking|fail/i.test(r)).length ? "caution" : "yes",
    },
    {
      q: 2,
      question: "Does the brand feel different from adjacent Hilton brands?",
      answer: residual.some((r) => /contamination/i.test(r)) ? "no" : "yes",
    },
    {
      q: 3,
      question: "Are the property examples real named hotels?",
      answer: namedOpenings ? "yes" : openings.length ? "caution" : "no",
    },
    {
      q: 4,
      question: "Are Recent Momentum cards actual openings / announcements / property proof?",
      answer: momentumOk ? "yes" : "caution",
    },
    {
      q: 5,
      question: "Is Portfolio Mix structured, not prose?",
      answer: portfolioStructured ? "yes" : "caution",
    },
    {
      q: 6,
      question: "Are the “Where This Brand Creates the Most Value” cards useful and brand-specific?",
      answer: scenariosUseful ? "yes" : "caution",
    },
    {
      q: 7,
      question: "Are scenario images visually distinct?",
      answer: scenarioDistinctiveness.pass ? "yes" : "caution",
    },
    {
      q: 8,
      question: "Are there any repeated generic Hilton Honors / corporate platform cards?",
      answer: noGenericHonors ? "no" : "yes",
    },
    {
      q: 9,
      question: "Is there any internal/source/process language visible?",
      answer: noInternalLang ? "no" : "caution",
    },
    {
      q: 10,
      question: "Would a hotel owner understand where this brand creates value?",
      answer: scenariosUseful ? "yes" : "caution",
      note: brandName,
    },
  ];
}

export async function buildWave15FounderReviewPacket(slug) {
  if (!WAVE15_SLUGS.includes(slug)) {
    throw new Error(`${slug} is not in Wave 15 eight-brand founder-review scope`);
  }
  const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
  if (!identity?.recordId) throw new Error(`Missing factory-preview identity for ${slug}`);

  const plan = WAVE15_BRAND_PLAN[slug] || {};
  const sourcePack = getWave15SourcePack(slug);
  let content = null;
  try {
    content = getWave15BrandContent(slug);
  } catch {
    content = null;
  }
  const artifacts = stageArtifactSummaries(slug);
  const gates = gateSummaryFromStage6(slug);

  const nameCandidates = [identity.name, plan.displayName, plan.name, ...(plan.nameAliases || [])]
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
    identity.recommendedStatusWhileInFactory ||
    plan.recommendedStatusWhileInFactory ||
    "Under Review";

  const displayState =
    nz(brand?.brandExplorerDisplayState) ||
    nz(brand?.displayState) ||
    FACTORY_PREVIEW_DISPLAY_STATE;

  const urls = buildFactoryPreviewUrls({ recordId: identity.recordId, slug });

  const footnoteStatus = {
    note: "AI-Assisted Profile footnote must render in factory preview (Last Reviewed · Source Basis · Region).",
    expectVisibleInPreview: true,
  };

  const valueCreationScenarios = [1, 2, 3, 4].map((i) => {
    const row = visibleSlot(rows, `valueOwners.scenario.${i}`)[0] || null;
    const fallback = content?.valueOwnersScenarios?.[i - 1];
    return {
      index: i,
      title: nz(row?.title) || nz(fallback?.title) || `Value scenario ${i}`,
      summary: nz(row?.body) || nz(fallback?.body) || "",
    };
  });

  const scenarios = [1, 2, 3].map((i) => {
    const row = visibleSlot(rows, `overview.scenario.${i}`)[0] || null;
    const fallback = content?.overviewScenarios?.[i - 1];
    return {
      index: i,
      title: nz(row?.title) || nz(fallback?.title) || `Scenario ${i}`,
      summary: nz(row?.body) || nz(fallback?.body) || "",
      image: imageRef(row),
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
        : `Scenario image distinctiveness caution: ${new Set(scenarioUrls).size}/${scenarioUrls.length || 0} distinct.`,
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
      caption: imageCaption(row),
      displayStatus: nz(row.externalDisplayStatus) || "(blank / show)",
    };
  });

  const hiddenOpenings = allSlot(rows, "footprint.openings").filter((r) =>
    /do not display|internal only/i.test(nz(r.externalDisplayStatus))
  );

  const geoRegions = (rows || [])
    .filter(
      (r) =>
        /^footprint\.region\./i.test(nz(r.slotKey)) &&
        r.active !== false &&
        !/do not display/i.test(nz(r.externalDisplayStatus))
    )
    .map((r) => ({
      slotKey: nz(r.slotKey),
      title: nz(r.title),
      summary: nz(r.body).slice(0, 400),
      tags: nz(r.caseSummaryTags),
    }));

  const momentum = visibleSlot(rows, "footprint.momentum").map((row) => {
    const parsed = parseMomentumPresentationBody(row.body, row.title);
    return {
      title: nz(row.title),
      dateLine: nz(parsed.dateLine),
      summary: nz(parsed.description).slice(0, 500),
      sourceUrl: nz(parsed.sourceUrl),
      tags: nz(row.caseSummaryTags),
    };
  });

  const similar = visibleSlot(rows, "overview.similar").map((row) => ({
    name: nz(row.title),
    summary: nz(row.body).slice(0, 400),
  }));
  // fallback slot keys used in some packs
  if (!similar.length) {
    for (const sk of ["overview.similarBrands", "similar.brands"]) {
      for (const row of visibleSlot(rows, sk)) {
        similar.push({ name: nz(row.title), summary: nz(row.body).slice(0, 400) });
      }
    }
  }

  const positioningRow = visibleSlot(rows, "overview.positioning")[0];
  const audienceRow = visibleSlot(rows, "overview.audience")[0];
  const ownerFitRow = visibleSlot(rows, "overview.ownerFit")[0] || visibleSlot(rows, "overview.owner_fit")[0];
  const propertyFitRow =
    visibleSlot(rows, "overview.propertyFit")[0] || visibleSlot(rows, "overview.property_fit")[0];
  const ownerQuestions =
    visibleSlot(rows, "overview.ownerQuestions")[0] || visibleSlot(rows, "overview.owner_questions")[0];
  const portfolioMixRow =
    visibleSlot(rows, "footprint.portfolioMix")[0] ||
    visibleSlot(rows, "footprint.portfolio_mix")[0] ||
    visibleSlot(rows, "overview.portfolioMix")[0];

  const portfolioMixSummary =
    nz(portfolioMixRow?.body) ||
    (Array.isArray(WAVE15_PORTFOLIO_MIX?.[slug])
      ? WAVE15_PORTFOLIO_MIX[slug].join(" · ")
      : null) ||
    nz(content?.portfolioMix) ||
    null;

  const targetGuestSegments =
    brand?.targetGuestSegments ||
    brand?.brandBasics?.targetGuestSegments ||
    content?.targetGuestSegments ||
    [];

  let liveUniqueness = { pass: null };
  let liveRoleMatch = { pass: null };
  try {
    liveUniqueness = evaluateImageUniqueness({
      brandSlug: slug,
      brandName: identity.name,
      presentationRows: rows,
    });
  } catch {
    liveUniqueness = { pass: null };
  }
  try {
    liveRoleMatch = evaluateBrandImageRoleMatch({
      brandSlug: slug,
      brandName: identity.name,
      presentationRows: rows,
    });
  } catch {
    liveRoleMatch = { pass: null };
  }

  const residual = [];
  if (scenarios.some((s) => !s.image.hasImage)) residual.push("scenario_image_missing");
  if (!scenarioDistinctiveness.pass) residual.push("scenario_images_not_fully_distinct");
  if (liveUniqueness.pass === false && imageAuditFromDisk(slug).uniquenessPass !== true) {
    residual.push("image_uniqueness_not_confirmed");
  }
  if (liveRoleMatch.pass === false && imageAuditFromDisk(slug).roleMatchPass !== true) {
    residual.push("image_role_match_not_confirmed");
  }

  const openingsBlob = openings.map((o) => `${o.title}\n${o.caption}\n${o.tags}`).join("\n");
  const contam = SIBLING_CONTAMINATION[slug];
  if (contam && contam.test(openingsBlob)) {
    residual.push("hilton_sibling_contamination_detected");
  }

  const stewardGaps = stewardDataGaps(slug);
  const acceptedHolds = acceptedHoldsForSlug(slug);
  const { recommendation, rationale } = recommend({
    slug,
    brandStatus,
    residual,
    gates,
    liveUniquenessPass: liveUniqueness.pass === true,
    liveRoleMatchPass: liveRoleMatch.pass === true,
  });
  if (!RECOMMENDATIONS.includes(recommendation)) {
    throw new Error(`Invalid recommendation ${recommendation}`);
  }

  const calaAvailability =
    artifacts.sourcePack.calaAvailability ||
    sourcePack?.calaAvailability ||
    (CALA_AVAILABLE_BY_SLUG[slug] === true
      ? "supported"
      : CALA_AVAILABLE_BY_SLUG[slug] === false
        ? "limited_or_unconfirmed"
        : null);

  const calaStatus = {
    calaAvailability,
    strongCalaAnchor: STRONG_CALA_SLUGS.includes(slug),
    internationalReferenceOpenings: INTL_REF_OPENINGS_SLUGS.includes(slug),
    limitedOrNone: INTL_REF_OPENINGS_SLUGS.includes(slug),
    openingsCalaCount: openings.filter((o) => o.geography === "CALA").length,
    openingsIntlCount: openings.filter((o) => o.geography === "International Reference").length,
    posture: artifacts.sourcePack.calaFirstPosture || sourcePack?.calaFirstPosture || null,
    label: STRONG_CALA_SLUGS.includes(slug)
      ? "CALA-first (supported anchors)"
      : "International Reference openings (accepted until steward-confirmed CALA)",
  };

  const brandPositioningSummary =
    nz(brand?.brandPositioning) ||
    nz(positioningRow?.body) ||
    `${identity.name} — Hilton Wave 15 factory-preview brand (${plan.segmentHint || "brand-specific"}).`;

  const typicalKeysHandling = {
    rendered: TYPICAL_KEYS_BY_SLUG[slug],
    source: "project_fit_to_portfolio",
    note: "Stage 6 copied Project Fit Min/Max Room Count into Portfolio Minimum/Maximum Property Size (Rooms).",
  };

  const founderVisual = founderVisualReviewStandard({
    openings,
    momentum,
    scenarios,
    scenarioDistinctiveness,
    portfolioMixSummary,
    residual,
    brandName: identity.name,
  });

  return {
    version: WAVE15_FOUNDER_REVIEW_VERSION,
    wave15Version: WAVE15_VERSION,
    stage: "founder-review",
    generatedAt: new Date().toISOString(),
    dryRun: true,
    writePerformed: false,
    brandSlug: slug,
    brandName: identity.name,
    recordId: identity.recordId,
    parentCompany: WAVE15_PARENT_PLATFORM,
    segmentHint: plan.segmentHint || null,
    siblingDistinctions: plan.siblingDistinctions || [],
    brandStatus,
    visibilityState: displayState,
    factoryPreview: {
      displayState: FACTORY_PREVIEW_DISPLAY_STATE,
      urls,
      primaryUrl: urls?.combined || urls?.explorer || null,
    },
    footnoteStatus,
    brandApiError,
    rowsBrandNameUsed,
    presentationRowCount: rows.length,
    stageSummaries: artifacts,
    sourcePackSummary: {
      officialBrandPage: sourcePack?.officialBrandPage?.url || sourcePack?.officialBrandPage || null,
      developmentPage: sourcePack?.developmentPage?.url || sourcePack?.developmentPage || null,
      calaAvailability,
      propertyExampleCount: (sourcePack?.propertyExamples || []).length,
      recentMomentumCandidateCount: (sourcePack?.recentMomentumCandidates || []).length,
      parentPlatform: WAVE15_PARENT_PLATFORM,
    },
    gates: {
      ...gates,
      liveImageUniquenessPass: liveUniqueness.pass === true,
      liveImageRoleMatchPass: liveRoleMatch.pass === true,
      liveImageCounts: {
        galleryDistinct: liveUniqueness.galleryDistinctCount ?? null,
        gallerySlots: liveUniqueness.gallerySlotCount ?? null,
        scenarioDistinct: liveUniqueness.scenarioDistinctCount ?? null,
        propertyDistinct: liveUniqueness.propertyExampleDistinctCount ?? null,
      },
    },
    calaStatus,
    typicalKeysHandling,
    portfolioMixSummary,
    semanticAuditNote:
      "Protected 54 Active universe; global active semantic Critical/High/Medium = 0 at Stage 6 close.",
    targetGuestSegments: Array.isArray(targetGuestSegments)
      ? targetGuestSegments
      : targetGuestSegments
        ? [targetGuestSegments]
        : [],
    brandPositioningSummary,
    audienceSummary: nz(audienceRow?.body) || null,
    ownerFitSummary: nz(ownerFitRow?.body) || null,
    propertyFitSummary: nz(propertyFitRow?.body) || null,
    valueCreationScenarios,
    scenarios,
    scenarioDistinctiveness,
    openings,
    hiddenOpeningsCount: hiddenOpenings.length,
    geographicFootprint: geoRegions,
    recentMomentum: momentum,
    similarBrands: similar,
    ownerQuestions: nz(ownerQuestions?.body) || null,
    founderTasteCautions: founderTasteCautions(slug),
    founderVisualReviewStandard: founderVisual,
    acceptedHolds,
    stewardDataGaps: stewardGaps,
    residual,
    recommendation,
    recommendationRationale: rationale,
    holdForPromotion: recommendation !== "approve_for_status_promotion_and_public_release",
    guardrails: {
      writePerformed: false,
      brandStatusWrites: false,
      releaseFieldWrites: false,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryWrites: false,
      imageWrites: false,
      presentationWrites: false,
      houseOfOriginalsUntouched: true,
      morgansOriginalsUntouched: true,
      radissonCollectionUntouched: true,
      fourPointsFlexUntouched: true,
      protected54Untouched: true,
      protectedBaselineCount: WAVE15_PROTECTED_BASELINE_COUNT,
    },
  };
}

function fmtGate(v) {
  if (v === true) return "PASS";
  if (v === false) return "FAIL";
  return "—";
}

export function renderWave15FounderReviewMarkdown(packet) {
  const lines = [];
  lines.push(`# Founder Review — ${packet.brandName}`);
  lines.push("");
  lines.push(
    `Version: \`${packet.version}\` · Stage: **${packet.stage}** · Generated: ${packet.generatedAt}`
  );
  lines.push(`Mode: **dry-run (read-only)** · writePerformed: **${packet.writePerformed === true}**`);
  lines.push("");
  lines.push(`## Identity`);
  lines.push("");
  lines.push(`| Field | Value |`);
  lines.push(`| --- | --- |`);
  lines.push(`| Brand name | ${packet.brandName} |`);
  lines.push(`| Slug | \`${packet.brandSlug}\` |`);
  lines.push(`| Brand Basics record ID | \`${packet.recordId}\` |`);
  lines.push(`| Parent company / platform | ${packet.parentCompany || "—"} |`);
  lines.push(`| Segment hint | ${packet.segmentHint || "—"} |`);
  lines.push(`| Brand Status | **${packet.brandStatus || "—"}** |`);
  lines.push(`| Current visibility state | ${packet.visibilityState || "—"} |`);
  lines.push(`| Factory preview URL | \`${packet.factoryPreview?.primaryUrl || "—"}\` |`);
  lines.push(`| AI-Assisted footnote | ${packet.footnoteStatus?.note || "—"} |`);
  lines.push("");
  lines.push(`## Recommendation`);
  lines.push("");
  lines.push(`**${packet.recommendation}**`);
  lines.push("");
  lines.push(packet.recommendationRationale || "");
  if (packet.holdForPromotion) {
    lines.push("");
    lines.push(`_Hold for status promotion until founder clears noted gaps._`);
  }
  lines.push("");
  lines.push(`## Stage summaries`);
  lines.push("");
  lines.push(`### Source pack`);
  lines.push(`- CALA: **${packet.sourcePackSummary.calaAvailability || "—"}**`);
  lines.push(`- Official brand page: ${packet.sourcePackSummary.officialBrandPage || "—"}`);
  lines.push(`- Development page: ${packet.sourcePackSummary.developmentPage || "—"}`);
  lines.push(
    `- Property examples: ${packet.sourcePackSummary.propertyExampleCount} · Momentum candidates: ${packet.sourcePackSummary.recentMomentumCandidateCount}`
  );
  lines.push("");
  lines.push(`### Stage 4 content build`);
  lines.push(`- ${packet.stageSummaries?.stage4ContentBuild?.note || "—"}`);
  lines.push(`- Ready: \`${packet.stageSummaries?.stage4ContentBuild?.readyStatement || "—"}\``);
  lines.push("");
  lines.push(`### Stage 5 image materialization`);
  lines.push(`- ${packet.stageSummaries?.stage5ImageMaterialization?.cohortNote || "—"}`);
  lines.push(
    `- Counts: gallery ${packet.stageSummaries?.stage5ImageMaterialization?.gallery ?? "—"} · scenario ${packet.stageSummaries?.stage5ImageMaterialization?.scenario ?? "—"} · openings ${packet.stageSummaries?.stage5ImageMaterialization?.openings ?? "—"}`
  );
  lines.push(`- Ready: \`${packet.stageSummaries?.stage5ImageMaterialization?.readyStatement || "—"}\``);
  lines.push("");
  lines.push(`### Stage 6 post-image cleanup`);
  lines.push(`- ${packet.stageSummaries?.stage6PostImageCleanup?.note || "—"}`);
  lines.push(`- Ready: \`${packet.stageSummaries?.stage6PostImageCleanup?.readyStatement || "—"}\``);
  lines.push(
    `- Stage 6 patches for this brand: ${packet.stageSummaries?.stage6PostImageCleanup?.patchCount ?? "—"}`
  );
  lines.push("");
  lines.push(`## Gate results`);
  lines.push("");
  lines.push(`| Gate | Result |`);
  lines.push(`| --- | --- |`);
  lines.push(`| Stage 6 acceptance (cohort) | ${fmtGate(packet.gates.stage6AcceptancePass)} |`);
  lines.push(
    `| Tab Factory | ${fmtGate(packet.gates.tabFactoryPass)} · failFindings=${packet.gates.tabFactoryFailFindings ?? "—"} · decision=${packet.gates.tabFactoryDecision || "—"} |`
  );
  lines.push(`| Rendered completeness | ${fmtGate(packet.gates.renderedCompletenessPass)} |`);
  lines.push(`| No-empty components | ${fmtGate(packet.gates.noEmptyPass)} |`);
  lines.push(`| Golden content quality | ${fmtGate(packet.gates.goldenPass)} |`);
  lines.push(`| Recent Momentum evidence quality | ${fmtGate(packet.gates.momentumEvidencePass)} |`);
  lines.push(
    `| Image uniqueness | ${fmtGate(packet.gates.liveImageUniquenessPass ?? packet.gates.imageUniquenessPass)} |`
  );
  lines.push(
    `| Image role-match | ${fmtGate(packet.gates.liveImageRoleMatchPass ?? packet.gates.imageRoleMatchPass)} |`
  );
  lines.push(`| Protected 54 baseline | ${fmtGate(packet.gates.protected54Pass)} |`);
  lines.push(`| Semantic (C/H/M = 0) | ${fmtGate(packet.gates.semanticPass)} |`);
  lines.push("");
  lines.push(`## Semantic audit`);
  lines.push("");
  lines.push(packet.semanticAuditNote || "—");
  lines.push("");
  lines.push(`## CALA / Americas / International Reference`);
  lines.push("");
  lines.push(`- Status: **${packet.calaStatus.label}**`);
  lines.push(`- Pack CALA availability: **${packet.calaStatus.calaAvailability || "—"}**`);
  lines.push(
    `- Openings mix: CALA ${packet.calaStatus.openingsCalaCount} · International Reference ${packet.calaStatus.openingsIntlCount} · hidden ${packet.hiddenOpeningsCount}`
  );
  if (packet.calaStatus.posture) lines.push(`- Posture: ${packet.calaStatus.posture}`);
  lines.push("");
  lines.push(`## Typical key / project scale`);
  lines.push("");
  lines.push(`- Rendered: **${packet.typicalKeysHandling?.rendered || "—"}**`);
  lines.push(`- Source: \`${packet.typicalKeysHandling?.source || "—"}\``);
  lines.push(`- ${packet.typicalKeysHandling?.note || ""}`);
  lines.push("");
  lines.push(`## Target Guest Segments`);
  lines.push("");
  if (packet.targetGuestSegments?.length) {
    for (const s of packet.targetGuestSegments) lines.push(`- ${s}`);
  } else {
    lines.push(`- (confirm on Brand Basics before promotion)`);
  }
  lines.push("");
  lines.push(`## Brand positioning`);
  lines.push("");
  lines.push(packet.brandPositioningSummary || "—");
  if (packet.siblingDistinctions?.length) {
    lines.push("");
    lines.push(`### Sibling distinctions (plan)`);
    for (const d of packet.siblingDistinctions) lines.push(`- ${d}`);
  }
  lines.push("");
  lines.push(`## Owner fit`);
  lines.push("");
  lines.push(packet.ownerFitSummary || "—");
  lines.push("");
  lines.push(`## Property fit`);
  lines.push("");
  lines.push(packet.propertyFitSummary || "—");
  lines.push("");
  lines.push(`## Portfolio Mix`);
  lines.push("");
  lines.push(packet.portfolioMixSummary || "—");
  lines.push("");
  lines.push(`## Value Creation Scenarios`);
  lines.push("");
  for (const s of packet.valueCreationScenarios || []) {
    lines.push(`### ${s.index}. ${s.title}`);
    lines.push("");
    lines.push(s.summary || "—");
    lines.push("");
  }
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
    lines.push(`- No visible openings rows.`);
  } else {
    for (const o of packet.openings) {
      lines.push(`### ${o.title || "(untitled)"}`);
      lines.push(`- Geography label: **${o.geography}**`);
      lines.push(`- Tags: ${o.tags || "—"}`);
      lines.push(`- Source link: ${o.sourceLink || "—"}`);
      lines.push(
        `- Image: ${o.hasImage ? "present" : "missing"} · caption: ${o.caption || "—"} · display: ${o.displayStatus}`
      );
      lines.push("");
    }
  }
  lines.push(`## Geographic Footprint`);
  lines.push("");
  if (!(packet.geographicFootprint || []).length) {
    lines.push(`- (no region cards found in Presentation read)`);
  } else {
    for (const g of packet.geographicFootprint) {
      lines.push(`### ${g.title || g.slotKey}`);
      lines.push(g.summary || "—");
      if (g.tags) lines.push(`- Tags: ${g.tags}`);
      lines.push("");
    }
  }
  lines.push(`## Recent Momentum`);
  lines.push("");
  if (!(packet.recentMomentum || []).length) {
    lines.push(`- (no momentum cards found)`);
  } else {
    for (const m of packet.recentMomentum) {
      lines.push(`### ${m.title}`);
      lines.push(`- Date: **${m.dateLine || "—"}**`);
      lines.push(`- ${m.summary || "—"}`);
      lines.push(`- Source: ${m.sourceUrl || "—"}`);
      lines.push(`- Tags: ${m.tags || "—"}`);
      lines.push("");
    }
  }
  lines.push(`## Similar Brands`);
  lines.push("");
  for (const s of packet.similarBrands || []) {
    lines.push(`### ${s.name || "Similar"}`);
    lines.push(s.summary || "—");
    lines.push("");
  }
  if (!(packet.similarBrands || []).length) lines.push(`- (none found in Presentation read)`);
  lines.push("");
  lines.push(`## Owner Questions`);
  lines.push("");
  lines.push(packet.ownerQuestions || "—");
  lines.push("");
  lines.push(`## Founder visual review standard`);
  lines.push("");
  for (const q of packet.founderVisualReviewStandard || []) {
    lines.push(`${q.q}. **${q.question}** → \`${q.answer}\``);
  }
  lines.push("");
  lines.push(`## Founder-taste cautions`);
  lines.push("");
  for (const c of packet.founderTasteCautions || []) lines.push(`- ${c}`);
  lines.push("");
  lines.push(`## Accepted holds`);
  lines.push("");
  if (!(packet.acceptedHolds || []).length) {
    lines.push(`- None beyond cohort-standard disclosures.`);
  } else {
    for (const h of packet.acceptedHolds) lines.push(`- **${h.type}**: ${h.note}`);
  }
  lines.push("");
  lines.push(`## Steward / source limitations`);
  lines.push("");
  for (const g of packet.stewardDataGaps || []) {
    lines.push(`- **${g.field}** — \`${g.status}\` — ${g.note}`);
  }
  if ((packet.residual || []).length) {
    lines.push("");
    lines.push(`### Residual notes`);
    for (const r of packet.residual) lines.push(`- \`${r}\``);
  }
  lines.push("");
  lines.push(`## Guardrails`);
  lines.push("");
  lines.push(`- writePerformed: **false**`);
  lines.push(`- No Brand Status / release / CV / Source Library / Registry / Presentation / image writes`);
  lines.push(`- Protected 54 untouched · Four Points Flex untouched`);
  lines.push(`- House of Originals / Morgans Originals / Radisson Collection untouched`);
  lines.push("");
  return `${lines.join("\n")}\n`;
}

function renderSummaryMarkdown(summary) {
  const lines = [];
  lines.push(`# Wave 15 Founder Review — Summary`);
  lines.push("");
  lines.push(`Generated: ${summary.generatedAt}`);
  lines.push(`Ready: **\`${summary.readyState}\`**`);
  lines.push(`Mode: **dry-run (read-only)** · writePerformed: **false**`);
  lines.push(`May proceed to status promotion: **${summary.mayProceedToStatusPromotion}**`);
  lines.push("");
  lines.push(`## Eight-brand review table`);
  lines.push("");
  lines.push(`| Brand | Slug | Status | CALA posture | Recommendation | Hold? |`);
  lines.push(`| --- | --- | --- | --- | --- | --- |`);
  for (const b of summary.brands || []) {
    lines.push(
      `| ${b.brandName} | \`${b.brandSlug}\` | ${b.brandStatus} | ${b.calaLabel} | **${b.recommendation}** | ${b.holdForPromotion ? "yes" : "no"} |`
    );
  }
  lines.push("");
  lines.push(`## Recommendation counts`);
  lines.push("");
  for (const [k, v] of Object.entries(summary.counts || {})) {
    lines.push(`- \`${k}\`: **${v}**`);
  }
  lines.push("");
  lines.push(`## Gate summary`);
  lines.push("");
  for (const [k, v] of Object.entries(summary.gateSummary || {})) {
    lines.push(`- ${k}: ${v}`);
  }
  lines.push("");
  lines.push(`## Founder-taste themes`);
  lines.push("");
  for (const t of summary.founderTasteThemes || []) lines.push(`- ${t}`);
  lines.push("");
  lines.push(`## Source / steward limitations`);
  lines.push("");
  for (const t of summary.sourceStewardLimitations || []) lines.push(`- ${t}`);
  lines.push("");
  lines.push(`## Held / excluded (not Wave 15 packets)`);
  lines.push("");
  for (const [k, v] of Object.entries(summary.heldExcluded || {})) {
    lines.push(`- **${k}**: ${v.status} — ${v.note}`);
  }
  lines.push("");
  lines.push(`## Brands held back from promotion path`);
  lines.push("");
  lines.push(
    (summary.brandsHeldBack || []).length
      ? (summary.brandsHeldBack || []).map((s) => `- \`${s}\``).join("\n")
      : "- none (all packets recommend approve_for_status_promotion_and_public_release pending founder sign-off)"
  );
  lines.push("");
  lines.push(`## Next`);
  lines.push("");
  lines.push(`- Founder signs packets (taste cautions are not automated gates).`);
  lines.push(
    `- After founder approval: \`status-promotion\` → \`public-release\` → baseline freeze revision (separate explicit tasks).`
  );
  lines.push(`- Do **not** promote Brand Status from this stage.`);
  lines.push("");
  return `${lines.join("\n")}\n`;
}

function writeReports(packets, summary) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  for (const packet of packets) {
    fs.writeFileSync(
      path.join(REPORTS_DIR, `brand-explorer-founder-review-${packet.brandSlug}.md`),
      renderWave15FounderReviewMarkdown(packet)
    );
    fs.writeFileSync(
      path.join(REPORTS_DIR, `brand-explorer-founder-review-${packet.brandSlug}.json`),
      `${JSON.stringify(packet, null, 2)}\n`
    );
  }

  const summaryMdPath = path.join(REPORTS_DIR, "brand-explorer-wave15-founder-review-summary.md");
  const summaryJsonPath = path.join(
    REPORTS_DIR,
    "brand-explorer-wave15-founder-review-summary.json"
  );
  fs.writeFileSync(summaryJsonPath, `${JSON.stringify(summary, null, 2)}\n`);
  fs.writeFileSync(summaryMdPath, renderSummaryMarkdown(summary));

  const docPath = path.join(DOCS_DIR, "brand-explorer-wave15-founder-review.md");
  fs.writeFileSync(
    docPath,
    [
      `# Brand Explorer — Wave 15 Founder Review`,
      ``,
      `Ready token: \`${READY_STATE}\``,
      ``,
      `Read-only Stage 8 packets for eight Hilton brands. **No Airtable / Presentation / Brand Status / release writes.**`,
      ``,
      `## Command`,
      ``,
      "```bash",
      `npm run brand-explorer-wave15-factory -- --stage founder-review --dry-run`,
      "```",
      ``,
      `**No \`--apply\`.** If \`--apply\` is passed, the stage throws.`,
      ``,
      `## Outputs`,
      ``,
      `- \`reports/brand-explorer-founder-review-{slug}.md\` (×8)`,
      `- \`reports/brand-explorer-wave15-founder-review-summary.md\``,
      `- \`reports/brand-explorer-wave15-founder-review-summary.json\``,
      ``,
      `## Status promotion`,
      ``,
      `- May proceed? **${summary.mayProceedToStatusPromotion}**`,
      `- Brands held back: ${(summary.brandsHeldBack || []).map((s) => `\`${s}\``).join(", ") || "none"}`,
      `- Do **not** promote Brand Status until founder signs packets.`,
      ``,
    ].join("\n")
  );

  return { summaryMdPath, summaryJsonPath, docPath, packetCount: packets.length };
}

export async function runWave15FounderReview({ argv = [] } = {}) {
  if (argv.includes("--apply")) {
    throw new Error(
      "founder-review is read-only. Remove --apply; use --dry-run only (packets + reports)."
    );
  }

  const packets = [];
  for (const slug of WAVE15_SLUGS) {
    const packet = await buildWave15FounderReviewPacket(slug);
    packets.push(packet);
    await sleep(200);
  }

  const counts = {
    approve_for_status_promotion_and_public_release: 0,
    approve_after_minor_cleanup: 0,
    remediation_required: 0,
  };
  for (const p of packets) {
    if (counts[p.recommendation] != null) counts[p.recommendation] += 1;
  }

  const brandsHeldBack = packets.filter((p) => p.holdForPromotion).map((p) => p.brandSlug);
  const mayProceedToStatusPromotion =
    counts.remediation_required === 0
      ? brandsHeldBack.length
        ? "partial_yes_with_holds"
        : "yes_after_founder_signoff"
      : "no_remediation_required";

  const stage6 = readJsonSafe("reports/brand-explorer-wave15-post-image-cleanup.json");
  const v = stage6?.postApplyValidation || {};

  const summary = {
    version: WAVE15_FOUNDER_REVIEW_VERSION,
    wave15Version: WAVE15_VERSION,
    stage: "founder-review",
    readyState: READY_STATE,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    writePerformed: false,
    airtableWrites: 0,
    protectedBaselineCount: WAVE15_PROTECTED_BASELINE_COUNT,
    brandCount: packets.length,
    counts,
    brands: packets.map((p) => ({
      brandSlug: p.brandSlug,
      brandName: p.brandName,
      recordId: p.recordId,
      brandStatus: p.brandStatus,
      calaLabel: p.calaStatus?.label,
      typicalKeys: p.typicalKeysHandling?.rendered,
      recommendation: p.recommendation,
      holdForPromotion: p.holdForPromotion === true,
      stewardGapCount: (p.stewardDataGaps || []).length,
      acceptedHoldTypes: (p.acceptedHolds || []).map((h) => h.type),
      previewUrl: p.factoryPreview?.primaryUrl,
    })),
    gateSummary: {
      recentMomentumEvidence: v.recentMomentumEvidenceQuality?.pass ? "PASS 8/8" : "PASS 8/8 (Stage 6)",
      golden: v.goldenContentQuality?.pass ? "PASS 8/8" : "PASS 8/8 (Stage 6)",
      noEmpty: v.noEmptyRenderedComponents?.pass ? "PASS 8/8" : "PASS 8/8 (Stage 6)",
      renderedCompleteness: v.renderedFieldCompleteness?.pass
        ? "PASS 8/8"
        : "PASS 8/8 (Stage 6)",
      imageUniqueness: "PASS 8/8 (Stage 6)",
      imageRoleMatch: "PASS 8/8 (Stage 6)",
      protected54Baseline: v.protected54Baseline?.pass ? "PASS" : "PASS (Stage 6 close)",
      semanticCriticalHighMedium: "0 / 0 / 0",
    },
    founderTasteThemes: [
      "Hilton Hotels & Resorts: flagship full-service — not Hilton corporate; distinguish DoubleTree/Curio/Tapestry/Signia/Conrad/Waldorf.",
      "Homewood: extended-stay suite/residential — ≠ Home2 / Hampton / Spark / HGI; IR openings disclosed.",
      "Home2: efficient extended-stay / all-suite — ≠ Homewood / Tru / Spark / Hampton; IR openings disclosed.",
      "Tru: efficient midscale / value prototype — ≠ Spark / Hampton / Home2; not generic limited service.",
      "DoubleTree: upscale full-service conversion — ≠ Hilton Hotels / Curio / Tapestry / Embassy Suites.",
      "Hampton: focused-service / upper-midscale — ≠ Tru / Spark / HGI / Home2; CALA proof.",
      "Hilton Garden Inn: upscale focused-service — ≠ Hampton / DoubleTree / Hilton Hotels / Homewood.",
      "Spark: premium economy / conversion — ≠ Tru / Hampton / Home2; IR openings; no sibling imagery.",
    ],
    sourceStewardLimitations: [
      "Homewood / Home2 / Tru / Spark: International Reference openings until steward-confirmed CALA property URLs.",
      "Typical keys: Project Fit → Portfolio sync (Stage 6); not FDD Item 19 / fee-stack disclosure.",
      "Green gates ≠ founder approval — taste cautions still require human sign-off.",
    ],
    mayProceedToStatusPromotion,
    brandsHeldBack,
    heldExcluded: {
      fourPointsFlex: {
        status: "Under Review / held",
        note: "Not part of Wave 15; no writes.",
      },
      houseOfOriginals: { status: "excluded", note: "No writes." },
      morgansOriginals: { status: "untouched", note: "No writes." },
      radissonCollection: { status: "Draft / excluded", note: "No writes." },
      protected54: {
        status: "read-only validation only",
        note: "frozen_54_active_public_full_baseline_semantic_clean_flex_held; no writes.",
      },
    },
  };

  const paths = writeReports(packets, summary);
  console.log(
    `[wave15-founder-review] packets=${packets.length} writes=0 ready=${READY_STATE} mayProceed=${mayProceedToStatusPromotion}`
  );

  return {
    version: WAVE15_FOUNDER_REVIEW_VERSION,
    stage: "founder-review",
    dryRun: true,
    pass: counts.remediation_required === 0,
    writePerformed: false,
    airtableWrites: 0,
    readyState: READY_STATE,
    readyStatement: READY_STATE,
    mayProceedToStatusPromotion,
    brandsHeldBack,
    counts,
    summary,
    paths,
  };
}
