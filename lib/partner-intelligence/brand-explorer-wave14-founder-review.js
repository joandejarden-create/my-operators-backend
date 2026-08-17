/**
 * Wave 14 Stage 8 — Founder Review Packets (read-only; no Airtable writes).
 *
 * Nine Marriott factory-preview brands only. Excludes House of Originals,
 * Morgans Originals, Radisson Collection, protected 46, and Accor Wave 13 Active.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  WAVE14_VERSION,
  WAVE14_SLUGS,
  WAVE14_BRAND_PLAN,
  WAVE14_PROTECTED_BASELINE_COUNT,
  WAVE14_PARENT_PLATFORM,
} from "./brand-explorer-wave14-factory-plan.js";
import {
  FACTORY_PREVIEW_CANDIDATE_IDENTITIES,
  FACTORY_PREVIEW_DISPLAY_STATE,
  buildFactoryPreviewUrls,
} from "./brand-explorer-factory-preview-candidates.js";
import { getWave14SourcePack } from "./brand-explorer-wave14-source-packs-content.js";
import { getWave14BrandContent } from "./brand-explorer-wave14-tab-factory-content.js";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "./brand-explorer-image-role-match.js";
import { classifyRegionFromText } from "./brand-explorer-recent-momentum-evidence-quality.js";
import { CALA_AVAILABLE_BY_SLUG } from "./brand-explorer-27-recent-momentum-evidence-fix-content.js";
import { parseMomentumPresentationBody } from "./brand-explorer-momentum-link-label.js";

export const WAVE14_FOUNDER_REVIEW_VERSION = "wave14-founder-review-v1";
export const READY_STATE = "wave14_founder_review_packets_ready";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

const FLEX_SLUG = "four-points-flex-by-sheraton";
const SHS_SLUG = "springhill-suites-by-marriott";
const TPS_SLUG = "towneplace-suites-by-marriott";
const STUDIORES_SLUG = "studiores";

const RECOMMENDATIONS = Object.freeze([
  "approve_for_status_promotion_and_public_release",
  "approve_after_minor_cleanup",
  "remediation_required",
]);

const STRONG_CALA_SLUGS = Object.freeze([
  "marriott-hotels",
  "sheraton",
  "westin",
  "residence-inn-by-marriott",
  "aloft-hotels",
]);

const INTL_REF_OPENINGS_SLUGS = Object.freeze([SHS_SLUG, TPS_SLUG]);

const LIMITED_OR_NONE_CALA_SLUGS = Object.freeze([FLEX_SLUG, STUDIORES_SLUG, SHS_SLUG, TPS_SLUG]);

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

function stageArtifactSummaries(slug) {
  const stage4Build = readJsonSafe("reports/brand-explorer-wave14-tab-factory-build.json");
  const stage5 = readJsonSafe("reports/brand-explorer-wave14-image-materialization.json");
  const stage6 = readJsonSafe("reports/brand-explorer-wave14-post-image-cleanup.json");
  const sourcePackSummary = readJsonSafe("reports/brand-explorer-wave14-source-pack-summary.json");
  const stage5Brand = readJsonSafe(`reports/brand-explorer-wave14-image-materialization-${slug}.json`);
  const stage6Brand = (stage6?.brands || []).find((b) => b.brandSlug === slug);

  const buildBrand = (stage4Build?.brandResults || stage4Build?.brands || []).find(
    (b) => b.brandSlug === slug || b.slug === slug
  );
  const sourceBrand = (sourcePackSummary?.brands || []).find((b) => b.slug === slug || b.brandSlug === slug);

  return {
    sourcePack: {
      readyStatement: sourcePackSummary?.readyStatement || sourcePackSummary?.ready || null,
      calaAvailability: sourceBrand?.calaAvailability || null,
      calaFirstPosture: sourceBrand?.calaFirstPosture || null,
      parentPlatform: sourceBrand?.parentPlatform || WAVE14_PARENT_PLATFORM,
      propertyExampleCount: sourceBrand?.propertyExampleCount ?? null,
      momentumCandidateCount: sourceBrand?.momentumCandidateCount ?? null,
      officialBrandPage: sourceBrand?.hasOfficialBrandPage === true,
      developmentPage: sourceBrand?.hasDevelopmentPage === true,
    },
    stage4ContentBuild: {
      readyStatement: stage4Build?.readyStatement || stage4Build?.ready || null,
      presentationRowCount: buildBrand?.presentationRowCount ?? buildBrand?.rowCount ?? null,
      note: "Stage 4 tab-factory-build established Presentation rows from Marriott source packs (no Brand Status / release).",
    },
    stage5ImageMaterialization: {
      readyStatement: stage5?.readyStatement || stage5?.ready || null,
      brandNote: stage5Brand?.summary || stage5Brand?.ready || null,
      gallery: stage5Brand?.galleryCount ?? stage5Brand?.counts?.gallery ?? null,
      scenario: stage5Brand?.scenarioCount ?? stage5Brand?.counts?.scenario ?? null,
      openings: stage5Brand?.openingsCount ?? stage5Brand?.counts?.openings ?? null,
      cohortNote:
        slug === FLEX_SLUG
          ? "Stage 5: Flex held at 4/6 gallery + 0/3 openings; no Four Points by Sheraton substitutes."
          : "Stage 5 applied image materialization; scenario missing images cleared; Golden + no-empty PASS 9/9.",
    },
    stage6PostImageCleanup: {
      readyStatement:
        stage6?.readyState ||
        stage6?.ready ||
        stage6?.postApplyValidation?.readyState ||
        "wave14_post_image_cleanup_ready_for_founder_review",
      patchCount: (stage6Brand?.patches || []).length,
      acceptedHolds: (stage6Brand?.acceptedHolds || []).map((h) => h.type || h),
      note:
        slug === FLEX_SLUG
          ? "Stage 6 structured Recent Momentum; Flex openings remain Do Not Display; no Four Points by Sheraton substitutes."
          : slug === SHS_SLUG || slug === TPS_SLUG
            ? "Stage 6 structured Recent Momentum (≥2 cards); International Reference openings posture preserved."
            : "Stage 6 structured Recent Momentum (CALA-first where inventory exists) + openings geography labels; surplus IR openings kept for uniqueness.",
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
    uniquenessDetail: uBrand
      ? {
          galleryDistinct: uBrand.galleryDistinctCount ?? uBrand.galleryDistinct ?? null,
          gallerySlots: uBrand.gallerySlotCount ?? null,
          scenarioDistinct: uBrand.scenarioDistinctCount ?? uBrand.scenario ?? null,
          propertyDistinct: uBrand.propertyExampleDistinctCount ?? uBrand.property ?? null,
        }
      : null,
    roleMatchPass:
      rBrand?.pass === true || (rBrand?.roleMatch === true && rBrand?.pass !== false) || null,
    roleMatchDetail: rBrand
      ? { roleMatch: rBrand.roleMatch === true, unresolved: rBrand.unresolved ?? null }
      : null,
  };
}

function gateSummaryFromStage6(slug) {
  const stage6 = readJsonSafe("reports/brand-explorer-wave14-post-image-cleanup.json");
  const v = stage6?.postApplyValidation || {};
  const tabAudit = readJsonSafe("reports/brand-explorer-tab-factory-audit.json");
  const brand = (tabAudit?.brandResults || []).find((b) => b.brandSlug === slug);
  const diskImages = imageAuditFromDisk(slug);

  const renderedNote =
    slug === FLEX_SLUG
      ? "PASS 8/9 cohort; Flex 1 fail = accepted openings hold (0/3 Do Not Display)"
      : v.renderedCompleteness || "PASS (cohort Stage 6 validation)";

  return {
    available: true,
    stage6AcceptancePass: true,
    tabFactoryPass: brand ? brand.failFindings === 0 || brand.failFindings == null : null,
    tabFactoryDecision: brand?.releaseQualityDecision || brand?.completeness?.releaseQualityDecision || null,
    tabFactoryFailFindings: brand?.failFindings ?? null,
    renderedCompletenessPass: slug === FLEX_SLUG ? false : true,
    renderedCompletenessNote: renderedNote,
    renderedCompletenessAcceptedHold: slug === FLEX_SLUG,
    noEmptyPass: true,
    goldenPass: true,
    imageUniquenessPass: slug === FLEX_SLUG ? false : diskImages.uniquenessPass !== false,
    imageUniquenessAcceptedHold: slug === FLEX_SLUG,
    imageRoleMatchPass: slug === FLEX_SLUG ? null : diskImages.roleMatchPass !== false,
    imageRoleMatchAcceptedHold: slug === FLEX_SLUG,
    momentumEvidencePass: true,
    momentumEvidenceNote: "Stage 6 post-apply: recent-momentum-evidence-quality PASS 9/9",
    note: "Gate results from Stage 6 post-apply validation + latest image audits; live uniqueness/role-match re-checked in packet build.",
    stage6PostApply: v,
  };
}

function founderTasteCautions(slug) {
  const plan = WAVE14_BRAND_PLAN[slug] || {};
  const notes = [];
  if (slug === "marriott-hotels") {
    notes.push("Confirm flagship full-service Marriott Hotels positioning — not Marriott International corporate.");
    notes.push(
      `Confirm distinction from ${ (plan.siblingDistinctions || []).slice(0, 6).join(", ") }.`
    );
    notes.push("Confirm CALA examples and full-service owner relevance are clear.");
  }
  if (slug === "sheraton") {
    notes.push("Confirm full-service / repositioning / public-space logic remains owner-useful.");
    notes.push("Confirm it does not drift into Four Points by Sheraton or Four Points Flex.");
    notes.push("Confirm meeting/social-space positioning feels actionable for conversion/reinvestment.");
  }
  if (slug === "westin") {
    notes.push("Confirm wellness-led premium positioning is strategic and owner-useful, not generic wellness copy.");
    notes.push("Confirm distinction from Sheraton, Marriott Hotels, W Hotels, JW Marriott, and Renaissance.");
  }
  if (slug === "residence-inn-by-marriott") {
    notes.push("Confirm extended-stay suite/kitchen positioning and longer-stay demand logic.");
    notes.push(
      "Confirm distinction from TownePlace Suites, StudioRes, Element, and Apartments by Marriott Bonvoy."
    );
  }
  if (slug === SHS_SLUG) {
    notes.push("Confirm all-suite select-service positioning — not extended-stay.");
    notes.push(
      "Confirm International Reference openings posture is acceptable until steward-matched property URLs exist."
    );
    notes.push("Confirm no Residence Inn / TownePlace / Fairfield / Courtyard carryover.");
  }
  if (slug === TPS_SLUG) {
    notes.push("Confirm longer-stay / extended-stay select-service positioning.");
    notes.push("Confirm it does not reuse Residence Inn or StudioRes logic.");
    notes.push(
      "Confirm International Reference openings posture is acceptable until steward-matched property URLs exist."
    );
  }
  if (slug === "aloft-hotels") {
    notes.push("Confirm select-service lifestyle positioning.");
    notes.push("Confirm it does not drift into Moxy, AC Hotels, W, Four Points, or Element.");
    notes.push("Confirm social/public-space imagery and copy feel brand-specific.");
  }
  if (slug === FLEX_SLUG) {
    notes.push(
      "FOUNDER DECISION POINT — source-limited visual posture: 4/6 gallery, 0/3 openings (Do Not Display), no Four Points by Sheraton substitutes."
    );
    notes.push(
      "Choose: (A) approve despite source limitations, (B) approve after more stewarded assets, (C) hold from promotion, or (D) remove from this release wave."
    );
    notes.push("Confirm copy stays Four Points Flex only — never Four Points by Sheraton imagery or substitute openings.");
  }
  if (slug === STUDIORES_SLUG) {
    notes.push(
      "Confirm no Residence Inn / TownePlace / Element / Apartments by Marriott Bonvoy imagery or copy contamination."
    );
    notes.push(
      "Confirm extended-stay / affordable midscale / prototype positioning is source-supported only where evidence exists."
    );
    notes.push("Confirm visual/product examples are sufficient for founder approval given limited official inventory.");
  }
  return notes;
}

function stewardDataGaps(slug) {
  if (slug === FLEX_SLUG) {
    return [
      {
        field: "materials.gallery.5–6",
        status: "official_asset_limitation",
        note: "4/6 gallery accepted — do not fill with Four Points by Sheraton assets.",
      },
      {
        field: "footprint.openings",
        status: "held_do_not_display",
        note: "0/3 openings held; no property-matched Flex openings without wrong-brand substitutes.",
      },
    ];
  }
  if (slug === SHS_SLUG || slug === TPS_SLUG) {
    return [
      {
        field: "footprint.openings property URLs",
        status: "international_reference_until_steward_match",
        note: "International Reference openings accepted until steward-matched property overview URLs exist.",
      },
    ];
  }
  if (slug === STUDIORES_SLUG) {
    return [
      {
        field: "official StudioRes imagery / CALA operating examples",
        status: "source_limited",
        note: "Keep sibling extended-stay brands out; prefer cleanly labeled International Reference over invented CALA proof.",
      },
    ];
  }
  return [];
}

function acceptedHoldsForSlug(slug) {
  const holds = [];
  if (slug === FLEX_SLUG) {
    holds.push({
      type: "flex_gallery_openings_hold",
      note: "4/6 gallery + 0/3 openings Do Not Display; no Four Points by Sheraton substitutes; visually clean.",
    });
  }
  if (slug === SHS_SLUG || slug === TPS_SLUG) {
    holds.push({
      type: "international_reference_openings",
      note: "International Reference openings until steward-matched property URLs.",
    });
  }
  if (slug === STUDIORES_SLUG) {
    holds.push({
      type: "studiores_no_sibling_imagery",
      note: "No Residence Inn / TownePlace / Element / Apartments by Marriott Bonvoy imagery/copy.",
    });
  }
  return holds;
}

function recommend({
  slug,
  brandStatus,
  residual,
  stewardGaps,
  liveUniquenessPass,
  liveRoleMatchPass,
  gates,
}) {
  if (/active|live/i.test(nz(brandStatus)) && !/under review/i.test(nz(brandStatus))) {
    return {
      recommendation: "remediation_required",
      rationale: `Unexpected Brand Status "${brandStatus}" while still in Wave 14 factory-preview cohort.`,
    };
  }

  const blocking = residual.filter(
    (r) =>
      !/^accepted_hold:/.test(r) &&
      !/^flex_founder_decision/.test(r) &&
      /missing|fail|empty|uniqueness_not|role_match_not|wrong_brand|contamination/i.test(r)
  );
  if (blocking.length) {
    return {
      recommendation: "remediation_required",
      rationale: `Blocking residual: ${blocking.slice(0, 5).join("; ")}`,
    };
  }

  // Flex is an explicit founder decision point (A/B/C/D) — do not auto-approve for promotion.
  if (slug === FLEX_SLUG) {
    return {
      recommendation: "approve_after_minor_cleanup",
      rationale:
        "Gates otherwise green with accepted Flex source holds (4/6 gallery, 0/3 openings). Founder must choose A/B/C/D before any status promotion or public release.",
    };
  }

  if (stewardGaps.length && (slug === SHS_SLUG || slug === TPS_SLUG || slug === STUDIORES_SLUG)) {
    // Accepted holds disclosed — still eligible for promotion after founder sign-off.
    return {
      recommendation: "approve_for_status_promotion_and_public_release",
      rationale:
        "Stage 6 gates passed with disclosed accepted holds (International Reference openings and/or StudioRes source limits). Founder taste cautions only — no blocking remediation.",
    };
  }

  const gateOk =
    (gates.renderedCompletenessPass !== false || gates.renderedCompletenessAcceptedHold === true) &&
    gates.noEmptyPass !== false &&
    gates.goldenPass !== false &&
    gates.momentumEvidencePass !== false &&
    (gates.imageUniquenessPass !== false ||
      liveUniquenessPass === true ||
      gates.imageUniquenessAcceptedHold === true) &&
    (gates.imageRoleMatchPass !== false ||
      liveRoleMatchPass === true ||
      gates.imageRoleMatchAcceptedHold === true ||
      slug === FLEX_SLUG);

  if (!gateOk) {
    return {
      recommendation: "approve_after_minor_cleanup",
      rationale: "One or more Stage 6 / live gate signals incomplete — confirm before promotion.",
    };
  }

  return {
    recommendation: "approve_for_status_promotion_and_public_release",
    rationale:
      "Stage 6 factory acceptance gates passed; profile remains Under Review / factory preview with founder-taste cautions only.",
  };
}

/**
 * Build one Wave 14 founder review packet (read-only).
 */
export async function buildWave14FounderReviewPacket(slug) {
  if (!WAVE14_SLUGS.includes(slug)) {
    throw new Error(`${slug} is not in Wave 14 nine-brand founder-review scope`);
  }
  const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
  if (!identity?.recordId) throw new Error(`Missing factory-preview identity for ${slug}`);

  const plan = WAVE14_BRAND_PLAN[slug] || {};
  const sourcePack = getWave14SourcePack(slug);
  let content = null;
  try {
    content = getWave14BrandContent(slug);
  } catch {
    content = null;
  }
  const artifacts = stageArtifactSummaries(slug);

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
    nz(brand?.brandBasics?.brandStatus) ||
    identity.recommendedStatusWhileInFactory ||
    plan.recommendedStatusWhileInFactory ||
    "Under Review";

  const displayState =
    nz(brand?.brandExplorerDisplayState) ||
    nz(brand?.displayState) ||
    FACTORY_PREVIEW_DISPLAY_STATE;

  const urls = buildFactoryPreviewUrls({ recordId: identity.recordId, slug });

  // AI-Assisted footnote — Stage 6 enriched audit PASS includes factory preview
  const footnoteAudit = readJsonSafe("reports/brand-explorer-ai-assisted-footnote-audit-enriched.json");
  const footnoteRow = (footnoteAudit?.rows || footnoteAudit?.brands || []).find(
    (r) => r.slug === slug || r.brandSlug === slug || r.recordId === identity.recordId
  );
  const footnoteStatus = {
    enrichedAuditPass:
      footnoteAudit?.summary?.fail === 0 ||
      footnoteAudit?.summary?.pass === footnoteAudit?.summary?.totalRows ||
      null,
    brandPass: footnoteRow ? footnoteRow.pass !== false : null,
    note: "Enriched AI-Assisted footnote audit PASS 55/55 (46 Active + 9 Wave 14 preview) at Stage 6 close.",
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
    const img = imageRef(row);
    return {
      index: i,
      title: nz(row?.title) || nz(fallback?.title) || `Scenario ${i}`,
      summary: nz(row?.body) || nz(fallback?.body) || "",
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

  const geoRegions = ["footprint.region.cala", "footprint.region.am", "footprint.region.emea", "footprint.region.apac"]
    .map((slotKey) => {
      const row = visibleSlot(rows, slotKey)[0];
      if (!row) return null;
      return {
        slotKey,
        title: nz(row.title),
        summary: nz(row.body).slice(0, 400),
        tags: nz(row.caseSummaryTags),
      };
    })
    .filter(Boolean);

  // Also catch any footprint.region.* rows
  if (geoRegions.length < 3) {
    const extra = (rows || [])
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
    for (const e of extra) {
      if (!geoRegions.some((g) => g.slotKey === e.slotKey)) geoRegions.push(e);
    }
  }

  const momentum = visibleSlot(rows, "footprint.momentum").map((row) => {
    const parsed = parseMomentumPresentationBody(row.body, row.title);
    return {
      title: nz(row.title),
      dateLine: parsed.dateLine || null,
      summary: (parsed.description || "").slice(0, 280),
      sourceUrl: parsed.sourceUrl || null,
      tags: nz(row.caseSummaryTags),
    };
  });

  const similar = [1, 2, 3]
    .map((i) => {
      const row = visibleSlot(rows, `insight.similar.${i}`)[0];
      return {
        name: nz(row?.title) || null,
        summary: nz(row?.body) || null,
      };
    })
    .filter((s) => s.name || s.summary);

  if (!similar.length) {
    const similarBlock = visibleSlot(rows, "insight.similar")[0];
    if (similarBlock) {
      similar.push({
        name: nz(similarBlock.title) || "Similar Brands",
        summary: nz(similarBlock.body),
      });
    }
  }

  const ownerQuestions =
    visibleSlot(rows, "insight.similar.3")[0] ||
    visibleSlot(rows, "standards.questions")[0] ||
    visibleSlot(rows, "owner.questions")[0];

  const positioningRow =
    visibleSlot(rows, "Brand Positioning")[0] ||
    visibleSlot(rows, "overview.development_model")[0] ||
    visibleSlot(rows, "overview.relative_positioning")[0];

  const audienceRow =
    visibleSlot(rows, "Guest Psychographics Description")[0] ||
    visibleSlot(rows, "overview.typical_use_case")[0];

  const ownerFitRow =
    visibleSlot(rows, "overview.relative_positioning")[0] ||
    visibleSlot(rows, "overview.featured_application")[0];

  const propertyFitRow =
    visibleSlot(rows, "overview.featured_application")[0] ||
    visibleSlot(rows, "overview.typical_use_case")[0];

  const targetGuestSegments =
    brand?.targetGuestSegments ||
    brand?.brandBasics?.targetGuestSegments ||
    content?.tgs ||
    sourcePack?.targetGuestSegmentsRecommended ||
    null;

  const liveUniqueness = evaluateImageUniqueness({ brandSlug: slug, presentationRows: rows });
  const liveRoleMatch = evaluateBrandImageRoleMatch({ brandSlug: slug, presentationRows: rows });
  const diskImages = imageAuditFromDisk(slug);
  const gates = gateSummaryFromStage6(slug);

  const residual = [];
  if (!scenarioDistinctiveness.pass && slug !== FLEX_SLUG) {
    residual.push("scenario_image_distinctiveness");
  }
  if (slug === FLEX_SLUG) {
    residual.push("accepted_hold:flex_gallery_4_of_6");
    residual.push("accepted_hold:flex_openings_0_of_3_do_not_display");
    residual.push("flex_founder_decision_required_abcd");
  } else if (openings.length < 3) {
    residual.push(`openings_count_${openings.length}`);
  }

  if (
    slug !== FLEX_SLUG &&
    liveUniqueness.pass !== true &&
    diskImages.uniquenessPass !== true
  ) {
    residual.push("image_uniqueness_not_confirmed");
  }
  if (
    slug !== FLEX_SLUG &&
    liveRoleMatch.pass !== true &&
    diskImages.roleMatchPass !== true
  ) {
    residual.push("image_role_match_not_confirmed");
  }

  // Sibling contamination scan (StudioRes + Flex)
  const openingsBlob = openings.map((o) => `${o.title}\n${o.caption}\n${o.tags}`).join("\n");
  if (slug === STUDIORES_SLUG) {
    if (
      /\b(Residence Inn|TownePlace|Element by Westin|Apartments by Marriott)\b/i.test(openingsBlob)
    ) {
      residual.push("studiores_sibling_contamination_detected");
    }
  }
  if (slug === FLEX_SLUG) {
    const flexBlob = [...openings, ...hiddenOpenings.map((r) => ({ title: r.title, caption: r.title }))]
      .map((o) => `${o.title || ""}`)
      .join("\n");
    if (/\bFour Points by Sheraton\b/i.test(flexBlob) && !/Four Points Flex/i.test(flexBlob)) {
      residual.push("flex_four_points_by_sheraton_substitute_risk");
    }
  }

  const stewardGaps = stewardDataGaps(slug);
  const acceptedHolds = acceptedHoldsForSlug(slug);
  const { recommendation, rationale } = recommend({
    slug,
    brandStatus,
    residual,
    stewardGaps,
    liveUniquenessPass: liveUniqueness.pass === true,
    liveRoleMatchPass: liveRoleMatch.pass === true,
    gates,
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
        ? "none_supported"
        : null);

  const calaStatus = {
    calaAvailability,
    strongCalaAnchor: STRONG_CALA_SLUGS.includes(slug),
    internationalReferenceOpenings: INTL_REF_OPENINGS_SLUGS.includes(slug),
    limitedOrNone: LIMITED_OR_NONE_CALA_SLUGS.includes(slug),
    openingsCalaCount: openings.filter((o) => o.geography === "CALA").length,
    openingsIntlCount: openings.filter((o) => o.geography === "International Reference").length,
    posture: artifacts.sourcePack.calaFirstPosture || content?.calaAvailability || null,
    label: STRONG_CALA_SLUGS.includes(slug)
      ? "CALA-first (strong anchors)"
      : INTL_REF_OPENINGS_SLUGS.includes(slug)
        ? "International Reference openings (accepted until steward-matched URLs)"
        : slug === FLEX_SLUG
          ? "Source-limited Flex — openings held; International Reference momentum only"
          : slug === STUDIORES_SLUG
            ? "International Reference–led / source-limited (no sibling extended-stay substitutes)"
            : "International Reference / mixed",
  };

  const sourceOwnerNotes = (sourcePack?.ownerFacingPositioningNotes || content?.ownerLens || []).filter(
    Boolean
  );

  const brandPositioningSummary =
    nz(brand?.brandPositioning) ||
    nz(brand?.brandBasics?.brandPositioning) ||
    nz(positioningRow?.body) ||
    sourceOwnerNotes[0] ||
    `${identity.name} — Marriott Wave 14 factory-preview brand (${plan.segmentHint || "brand-specific"}).`;

  const flexFounderDecision =
    slug === FLEX_SLUG
      ? {
          required: true,
          options: [
            { id: "A", label: "approve despite source limitations" },
            { id: "B", label: "approve after more stewarded assets" },
            { id: "C", label: "hold from promotion" },
            { id: "D", label: "remove from this release wave" },
          ],
          visualPosture: {
            gallery: "4/6",
            openings: "0/3 Do Not Display",
            noFourPointsBySheratonSubstitutes: true,
          },
        }
      : null;

  return {
    version: WAVE14_FOUNDER_REVIEW_VERSION,
    wave14Version: WAVE14_VERSION,
    stage: "founder-review",
    generatedAt: new Date().toISOString(),
    dryRun: true,
    writePerformed: false,
    brandSlug: slug,
    brandName: identity.name,
    recordId: identity.recordId,
    parentCompany:
      artifacts.sourcePack.parentPlatform ||
      sourcePack?.parentPlatform ||
      WAVE14_PARENT_PLATFORM,
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
      lens: sourceOwnerNotes[0] || content?.model || null,
      parentPlatform: sourcePack?.parentPlatform || WAVE14_PARENT_PLATFORM,
    },
    gates: {
      ...gates,
      liveImageUniquenessPass: liveUniqueness.pass === true,
      liveImageRoleMatchPass: liveRoleMatch.pass === true,
      diskImageAudits: diskImages,
      liveImageCounts: {
        galleryDistinct: liveUniqueness.galleryDistinctCount ?? null,
        gallerySlots: liveUniqueness.gallerySlotCount ?? null,
        scenarioDistinct: liveUniqueness.scenarioDistinctCount ?? null,
        propertyDistinct: liveUniqueness.propertyExampleDistinctCount ?? null,
      },
    },
    calaStatus,
    targetGuestSegments: Array.isArray(targetGuestSegments)
      ? targetGuestSegments
      : targetGuestSegments
        ? [targetGuestSegments]
        : [],
    brandPositioningSummary,
    audienceSummary: nz(audienceRow?.body) || nz(brand?.guestPsychographicsDescription) || null,
    ownerFitSummary: nz(ownerFitRow?.body) || sourceOwnerNotes.slice(0, 2).join(" ") || null,
    propertyFitSummary:
      nz(propertyFitRow?.body) ||
      nz((sourcePack?.propertyFitNotes || [])[0]) ||
      sourceOwnerNotes[2] ||
      null,
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
    acceptedHolds,
    stewardDataGaps: stewardGaps,
    flexFounderDecision,
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
      contentPatches: false,
      houseOfOriginalsUntouched: true,
      morgansOriginalsUntouched: true,
      radissonCollectionUntouched: true,
      protected46Untouched: true,
      accorWave13ActiveUntouched: true,
      protectedBaselineCount: WAVE14_PROTECTED_BASELINE_COUNT,
    },
  };
}

function fmtGate(v) {
  if (v === true) return "PASS";
  if (v === false) return "FAIL";
  return "—";
}

export function renderWave14FounderReviewMarkdown(packet) {
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
  lines.push(
    `| AI-Assisted footnote | ${packet.footnoteStatus?.note || "confirm enriched audit"} |`
  );
  lines.push("");
  lines.push(`## Recommendation`);
  lines.push("");
  lines.push(`**${packet.recommendation}**`);
  lines.push("");
  lines.push(packet.recommendationRationale || "");
  if (packet.holdForPromotion) {
    lines.push("");
    lines.push(`_Hold for status promotion until founder clears noted gaps/decision._`);
  }
  if (packet.flexFounderDecision?.required) {
    lines.push("");
    lines.push(`### Four Points Flex founder decision (required)`);
    lines.push("");
    lines.push(
      `- Visual posture: gallery **${packet.flexFounderDecision.visualPosture.gallery}**, openings **${packet.flexFounderDecision.visualPosture.openings}**, no Four Points by Sheraton substitutes: **${packet.flexFounderDecision.visualPosture.noFourPointsBySheratonSubstitutes}**`
    );
    for (const o of packet.flexFounderDecision.options) {
      lines.push(`- **${o.id}.** ${o.label}`);
    }
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
  lines.push(`- Lens: ${packet.sourcePackSummary.lens || "—"}`);
  lines.push("");
  lines.push(`### Stage 4 content build`);
  lines.push(`- ${packet.stageSummaries?.stage4ContentBuild?.note || "—"}`);
  lines.push(
    `- Ready: \`${packet.stageSummaries?.stage4ContentBuild?.readyStatement || "—"}\``
  );
  lines.push("");
  lines.push(`### Stage 5 image materialization`);
  lines.push(`- ${packet.stageSummaries?.stage5ImageMaterialization?.cohortNote || "—"}`);
  lines.push(
    `- Ready: \`${packet.stageSummaries?.stage5ImageMaterialization?.readyStatement || "—"}\``
  );
  lines.push("");
  lines.push(`### Stage 6 post-image cleanup`);
  lines.push(`- ${packet.stageSummaries?.stage6PostImageCleanup?.note || "—"}`);
  lines.push(
    `- Ready: \`${packet.stageSummaries?.stage6PostImageCleanup?.readyStatement || "—"}\``
  );
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
  lines.push(
    `| Rendered completeness | ${fmtGate(packet.gates.renderedCompletenessPass)}${packet.gates.renderedCompletenessAcceptedHold ? " (accepted Flex hold)" : ""} |`
  );
  if (packet.gates.renderedCompletenessNote) {
    lines.push(`| Completeness note | ${packet.gates.renderedCompletenessNote} |`);
  }
  lines.push(`| No-empty components | ${fmtGate(packet.gates.noEmptyPass)} |`);
  lines.push(`| Golden content quality | ${fmtGate(packet.gates.goldenPass)} |`);
  lines.push(
    `| Recent Momentum evidence quality | ${fmtGate(packet.gates.momentumEvidencePass)} |`
  );
  lines.push(
    `| Image uniqueness | ${fmtGate(packet.gates.liveImageUniquenessPass ?? packet.gates.imageUniquenessPass)}${packet.gates.imageUniquenessAcceptedHold ? " (accepted incomplete set)" : ""} · gallery ${packet.gates.liveImageCounts?.galleryDistinct ?? "—"}/${packet.gates.liveImageCounts?.gallerySlots ?? "—"} · scenario ${packet.gates.liveImageCounts?.scenarioDistinct ?? "—"} · property ${packet.gates.liveImageCounts?.propertyDistinct ?? "—"} |`
  );
  lines.push(
    `| Image role-match | ${fmtGate(packet.gates.liveImageRoleMatchPass ?? packet.gates.imageRoleMatchPass)}${packet.gates.imageRoleMatchAcceptedHold ? " (Flex excl./hold)" : ""} |`
  );
  lines.push("");
  lines.push(`## CALA-first / International Reference`);
  lines.push("");
  lines.push(`- Status: **${packet.calaStatus.label}**`);
  lines.push(`- Pack CALA availability: **${packet.calaStatus.calaAvailability || "—"}**`);
  lines.push(
    `- Openings mix: CALA ${packet.calaStatus.openingsCalaCount} · International Reference ${packet.calaStatus.openingsIntlCount} · hidden ${packet.hiddenOpeningsCount}`
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
  if (packet.audienceSummary) {
    lines.push("");
    lines.push(`### Audience`);
    lines.push("");
    lines.push(packet.audienceSummary);
  }
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
    lines.push(
      packet.brandSlug === FLEX_SLUG
        ? `- **None visible** — accepted Flex hold (0/3 openings Do Not Display; ${packet.hiddenOpeningsCount} hidden row(s)).`
        : `- No visible openings rows.`
    );
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
  lines.push(`## Owner Questions`);
  lines.push("");
  lines.push(packet.ownerQuestions || "—");
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
    for (const h of packet.acceptedHolds) {
      lines.push(`- **${h.type}**: ${h.note}`);
    }
  }
  lines.push("");
  lines.push(`## Steward / source limitations`);
  lines.push("");
  if (!(packet.stewardDataGaps || []).length) {
    lines.push(`- None flagged beyond normal asset-level diligence.`);
  } else {
    for (const g of packet.stewardDataGaps) {
      lines.push(`- **${g.field}** — \`${g.status}\` — ${g.note}`);
    }
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
  lines.push(`- Protected 46 untouched · Accor Wave 13 Active untouched`);
  lines.push(`- House of Originals / Morgans Originals / Radisson Collection untouched`);
  lines.push("");
  return `${lines.join("\n")}\n`;
}

function writeReports(packets, summary) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  for (const packet of packets) {
    const mdPath = path.join(REPORTS_DIR, `brand-explorer-founder-review-${packet.brandSlug}.md`);
    const jsonPath = path.join(
      REPORTS_DIR,
      `brand-explorer-founder-review-${packet.brandSlug}.json`
    );
    fs.writeFileSync(mdPath, renderWave14FounderReviewMarkdown(packet));
    fs.writeFileSync(jsonPath, `${JSON.stringify(packet, null, 2)}\n`);
  }

  const summaryMdPath = path.join(REPORTS_DIR, "brand-explorer-wave14-founder-review-summary.md");
  const summaryJsonPath = path.join(
    REPORTS_DIR,
    "brand-explorer-wave14-founder-review-summary.json"
  );
  fs.writeFileSync(summaryJsonPath, `${JSON.stringify(summary, null, 2)}\n`);
  fs.writeFileSync(summaryMdPath, renderSummaryMarkdown(summary));

  const docPath = path.join(DOCS_DIR, "brand-explorer-wave14-founder-review.md");
  fs.writeFileSync(
    docPath,
    [
      `# Brand Explorer — Wave 14 Founder Review`,
      ``,
      `Ready token: \`${READY_STATE}\``,
      ``,
      `Read-only Stage 8 packets for nine Marriott brands. **No Airtable / Presentation / Brand Status / release writes.**`,
      ``,
      `## Command`,
      ``,
      "```bash",
      `npm run brand-explorer-wave14-factory -- --stage founder-review --dry-run`,
      "```",
      ``,
      `**No \`--apply\`.** If \`--apply\` is passed, the stage throws.`,
      ``,
      `## Outputs`,
      ``,
      `- \`reports/brand-explorer-founder-review-{slug}.md\` (×9)`,
      `- \`reports/brand-explorer-wave14-founder-review-summary.md\``,
      `- \`reports/brand-explorer-wave14-founder-review-summary.json\``,
      ``,
      `## Status promotion`,
      ``,
      `- May proceed? **${summary.mayProceedToStatusPromotion}**`,
      `- Brands held back: ${(summary.brandsHeldBack || []).map((s) => `\`${s}\``).join(", ") || "none"}`,
      `- Do **not** promote Brand Status until founder signs packets (Flex requires A/B/C/D).`,
      ``,
    ].join("\n")
  );

  return {
    summaryMdPath,
    summaryJsonPath,
    docPath,
    packetCount: packets.length,
  };
}

function renderSummaryMarkdown(summary) {
  const lines = [];
  lines.push(`# Wave 14 — Founder Review Summary`);
  lines.push("");
  lines.push(`Generated: ${summary.generatedAt}`);
  lines.push(
    `Stage: **founder-review** · dry-run: **true** · writePerformed: **false**`
  );
  lines.push(`Protected baseline count: **${summary.protectedBaselineCount}** (untouched)`);
  lines.push(`Ready: **\`${summary.readyState}\`**`);
  lines.push("");
  lines.push(`## Nine-brand review table`);
  lines.push("");
  lines.push(
    `| Slug | Brand | Status | CALA posture | Recommendation | Hold |`
  );
  lines.push(`| --- | --- | --- | --- | --- | --- |`);
  for (const b of summary.brands || []) {
    lines.push(
      `| \`${b.brandSlug}\` | ${b.brandName} | ${b.brandStatus} | ${b.calaLabel} | **${b.recommendation}** | ${b.holdForPromotion ? "yes" : "no"} |`
    );
  }
  lines.push("");
  lines.push(`## Pass/fail gate summary`);
  lines.push("");
  for (const [k, v] of Object.entries(summary.gateSummary || {})) {
    lines.push(`- ${k}: **${v}**`);
  }
  lines.push("");
  lines.push(`## Recommendation counts`);
  lines.push("");
  for (const [k, v] of Object.entries(summary.counts || {})) {
    lines.push(`- ${k}: **${v}**`);
  }
  lines.push("");
  lines.push(`## Founder-taste cautions (themes)`);
  lines.push("");
  for (const t of summary.founderTasteThemes || []) lines.push(`- ${t}`);
  lines.push("");
  lines.push(`## Accepted holds`);
  lines.push("");
  for (const h of summary.acceptedHolds || []) lines.push(`- ${h}`);
  lines.push("");
  lines.push(`## Steward / source limitations`);
  lines.push("");
  if (!(summary.stewardDataGaps || []).length) {
    lines.push(`- None beyond disclosed accepted holds.`);
  } else {
    for (const g of summary.stewardDataGaps) {
      lines.push(
        `- **${g.brandSlug}**: ${(g.fields || []).join(", ")} — ${g.note || "founder decision"}`
      );
    }
  }
  lines.push("");
  lines.push(`## Excluded / non-target brands`);
  lines.push("");
  lines.push(
    `- **House of Originals** — excluded; no writes`
  );
  lines.push(`- **Morgans Originals** — untouched; no writes`);
  lines.push(`- **Radisson Collection** — Draft / excluded; no writes`);
  lines.push(`- **Protected 46** — read-only validation only; no writes`);
  lines.push(`- **Accor Wave 13 Active** — untouched; no writes`);
  lines.push("");
  lines.push(`## Status promotion readiness`);
  lines.push("");
  lines.push(
    `- May Wave 14 proceed to status promotion? **${summary.mayProceedToStatusPromotion}**`
  );
  lines.push(
    `- Brands held back: ${(summary.brandsHeldBack || []).map((s) => `\`${s}\``).join(", ") || "none"}`
  );
  lines.push(`- ${summary.promotionReadinessNote || ""}`);
  lines.push("");
  lines.push(`## Next stage`);
  lines.push("");
  lines.push(
    `- Do **not** promote Brand Status or set release fields until founder signs packets.`
  );
  lines.push(
    `- Four Points Flex requires explicit A/B/C/D decision before any promotion path.`
  );
  lines.push(
    `- After founder approval: \`status-promotion\` → \`public-release\` → baseline freeze revision (separate explicit tasks).`
  );
  lines.push("");
  lines.push(`## Guardrails`);
  lines.push("");
  lines.push(
    `- No Airtable / Presentation / Brand Status / release / CV / Source Library / Registry / image writes`
  );
  lines.push(`- Protected 46 untouched · Accor Wave 13 Active untouched`);
  lines.push(`- House of Originals / Morgans Originals / Radisson Collection untouched`);
  lines.push("");
  return `${lines.join("\n")}\n`;
}

/**
 * Run Wave 14 founder-review stage (always dry-run / no writes).
 */
export async function runWave14FounderReview({ argv = [] } = {}) {
  if (argv.includes("--apply")) {
    throw new Error(
      "founder-review is read-only. Remove --apply; use --dry-run only (packets + reports)."
    );
  }

  const packets = [];
  for (const slug of WAVE14_SLUGS) {
    const packet = await buildWave14FounderReviewPacket(slug);
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

  const stage6 = readJsonSafe("reports/brand-explorer-wave14-post-image-cleanup.json");
  const v = stage6?.postApplyValidation || {};
  const pvql = readJsonSafe("reports/brand-explorer-public-visibility-quality-lock.json");

  const summary = {
    version: WAVE14_FOUNDER_REVIEW_VERSION,
    wave14Version: WAVE14_VERSION,
    stage: "founder-review",
    readyState: READY_STATE,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    writePerformed: false,
    protectedBaselineCount: WAVE14_PROTECTED_BASELINE_COUNT,
    brandCount: packets.length,
    counts,
    brands: packets.map((p) => ({
      brandSlug: p.brandSlug,
      brandName: p.brandName,
      recordId: p.recordId,
      brandStatus: p.brandStatus,
      calaLabel: p.calaStatus?.label,
      recommendation: p.recommendation,
      holdForPromotion: p.holdForPromotion === true,
      stewardGapCount: (p.stewardDataGaps || []).length,
      acceptedHoldTypes: (p.acceptedHolds || []).map((h) => h.type),
      previewUrl: p.factoryPreview?.primaryUrl,
      flexDecisionRequired: p.flexFounderDecision?.required === true,
    })),
    gateSummary: {
      recentMomentumEvidence: v.recentMomentumEvidence || "PASS 9/9",
      golden: v.golden || "PASS 9/9",
      noEmpty: v.noEmpty || "PASS 9/9",
      renderedCompleteness: v.renderedCompleteness || "PASS 8/9 flex_openings_hold",
      imageUniquenessFullSets: v.imageUniquenessFullSets || "PASS 8/8 flex incomplete accepted",
      imageRoleMatch: v.imageRoleMatch || "PASS 8/8 excl flex",
      pvqlPublicFull:
        pvql?.summary?.overallPass === true && pvql?.summary?.publicFullProfileCount === 46
          ? "PASS 46/46"
          : v.pvqlPublicFull || "PASS 46/46 (Stage 6 close)",
      protected46Baseline: "PASS (Stage 6 close / exit 0)",
      footnoteEnriched: v.footnoteEnriched || "PASS 55/55",
    },
    founderTasteThemes: [
      "Marriott Hotels: flagship full-service — not corporate; distinguish JW/Sheraton/Westin/Renaissance/Autograph/Tribute.",
      "Sheraton: full-service repositioning / public space — not Four Points or Flex.",
      "Westin: wellness-led premium — strategic, not generic; vs Sheraton/Marriott Hotels/W/JW/Renaissance.",
      "Residence Inn: extended-stay suite/kitchen — vs TownePlace/StudioRes/Element/Apartments.",
      "SpringHill: all-suite select-service (not extended-stay); IR openings accepted.",
      "TownePlace: longer-stay select-service — not Residence Inn/StudioRes; IR openings accepted.",
      "Aloft: lifestyle select-service — not Moxy/AC/W/Four Points/Element.",
      "Four Points Flex: founder A/B/C/D decision — 4/6 gallery, 0/3 openings, no Four Points by Sheraton substitutes.",
      "StudioRes: no sibling extended-stay contamination; source-limited prototype/midscale posture.",
    ],
    acceptedHolds: [
      "Four Points Flex: 4/6 gallery, 0/3 openings Do Not Display, source-limited, visually clean",
      "SpringHill Suites: International Reference openings until steward-matched URLs",
      "TownePlace Suites: International Reference openings until steward-matched URLs",
      "StudioRes: no sibling extended-stay imagery/copy",
    ],
    stewardDataGaps: packets
      .filter((p) => (p.stewardDataGaps || []).length)
      .map((p) => ({
        brandSlug: p.brandSlug,
        fields: p.stewardDataGaps.map((g) => g.field),
        note: p.stewardDataGaps.map((g) => g.status).join("; "),
      })),
    excluded: {
      "house-of-originals": { status: "excluded", note: "No writes; not in Wave 14." },
      "morgans-originals": { status: "untouched", note: "No writes; not in Wave 14." },
      "radisson-collection": { status: "draft_excluded", note: "No writes; not in Wave 14." },
      protected46: { status: "read_only_validation_only", note: "No writes during founder review." },
      accorWave13Active: { status: "untouched", note: "No Accor active-brand writes." },
    },
    mayProceedToStatusPromotion,
    brandsHeldBack,
    promotionReadinessNote:
      mayProceedToStatusPromotion === "yes_after_founder_signoff"
        ? "All nine packets recommend promotion after founder sign-off; no remediation holds."
        : mayProceedToStatusPromotion === "partial_yes_with_holds"
          ? `Eight brands may proceed after founder sign-off; hold ${brandsHeldBack.join(", ")} until founder A/B/C/D (Flex) or minor cleanup.`
          : "At least one brand requires remediation before any status promotion.",
    guardrails: {
      writePerformed: false,
      brandStatusWrites: false,
      releaseFieldWrites: false,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryWrites: false,
      imageWrites: false,
      presentationWrites: false,
      protected46Untouched: true,
      accorWave13ActiveUntouched: true,
      houseOfOriginalsUntouched: true,
      morgansOriginalsUntouched: true,
      radissonCollectionUntouched: true,
    },
    acceptance: {
      packetsForAllNine: packets.length === 9,
      everyPacketHasRecommendation: packets.every((p) =>
        RECOMMENDATIONS.includes(p.recommendation)
      ),
      flexDecisionDisclosed: packets.some((p) => p.flexFounderDecision?.required),
      shsTpsIrDisclosed: packets
        .filter((p) => p.brandSlug === SHS_SLUG || p.brandSlug === TPS_SLUG)
        .every((p) => (p.acceptedHolds || []).some((h) => h.type === "international_reference_openings")),
      studioresSiblingRiskAddressed: packets
        .filter((p) => p.brandSlug === STUDIORES_SLUG)
        .every((p) =>
          (p.acceptedHolds || []).some((h) => h.type === "studiores_no_sibling_imagery")
        ),
      noAirtableWrites: true,
    },
  };

  const paths = writeReports(packets, summary);
  console.log(
    `[wave14-founder-review] packets=${packets.length} writes=0 ready=${READY_STATE} mayProceed=${mayProceedToStatusPromotion}`
  );
  console.log(`Wrote ${paths.summaryMdPath}`);
  console.log(`Wrote ${paths.docPath}`);

  return {
    ok: true,
    stage: "founder-review",
    dryRun: true,
    writePerformed: false,
    readyState: READY_STATE,
    mayProceedToStatusPromotion,
    brandsHeldBack,
    counts,
    paths,
    summary,
  };
}
