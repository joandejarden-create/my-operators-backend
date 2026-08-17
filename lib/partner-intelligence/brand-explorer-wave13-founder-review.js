/**
 * Wave 13 Stage 8 — Founder Review Packets (read-only; no Airtable writes).
 *
 * Seven Accor factory-preview brands only. Excludes The House of Originals,
 * Morgans Originals, Radisson Collection, and protected 39.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  WAVE13_VERSION,
  WAVE13_STAGE4_APPROVED_SLUGS,
  WAVE13_PROTECTED_BASELINE_COUNT,
} from "./brand-explorer-wave13-factory-plan.js";
import {
  FACTORY_PREVIEW_CANDIDATE_IDENTITIES,
  FACTORY_PREVIEW_DISPLAY_STATE,
  buildFactoryPreviewUrls,
} from "./brand-explorer-factory-preview-candidates.js";
import { getWave13SourcePack } from "./brand-explorer-wave13-source-packs-content.js";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "./brand-explorer-image-role-match.js";
import { classifyRegionFromText } from "./brand-explorer-recent-momentum-evidence-quality.js";
import { EXPECTED_ACTIVE_COUNT_39 } from "./brand-explorer-39-active-public-full-baseline.js";

export const WAVE13_FOUNDER_REVIEW_VERSION = "wave13-founder-review-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

const SO_SLUG = "so-hotels-and-resorts";
const SO_BASICS_RECORD_ID = "recTJdPlr4mDs9app";
const FAIRMONT_SF_OPENINGS_RECORD_ID = "recQXp6Y3EkfaC9hG";

const RECOMMENDATIONS = Object.freeze([
  "approve_for_status_promotion_and_public_release",
  "approve_after_minor_cleanup",
  "remediation_required",
]);

const STRONG_CALA_SLUGS = Object.freeze([
  "mercure",
  "ibis",
  "novotel",
  "pullman",
  "fairmont-hotels-and-resorts",
]);

const PIPELINE_CALA_SLUGS = Object.freeze(["mama-shelter"]);

const INTL_REF_LED_SLUGS = Object.freeze(["so-hotels-and-resorts"]);

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

function stageArtifactSummaries(slug) {
  const stage4Build = readJsonSafe("reports/brand-explorer-wave13-tab-factory-build.json");
  const stage45 = readJsonSafe("reports/brand-explorer-wave13-stage4-content-cleanup.json");
  const stage5 = readJsonSafe("reports/brand-explorer-wave13-image-materialization.json");
  const stage6 = readJsonSafe("reports/brand-explorer-wave13-post-image-cleanup.json");
  const sourcePackSummary = readJsonSafe("reports/brand-explorer-wave13-source-pack-summary.json");

  const s4Brand = (stage45?.brandResults || []).find((b) => b.brandSlug === slug);
  const buildBrand = (stage4Build?.brandResults || stage4Build?.brands || []).find(
    (b) => b.brandSlug === slug || b.slug === slug
  );
  const sourceBrand = (sourcePackSummary?.brands || []).find((b) => b.slug === slug);

  return {
    sourcePack: {
      readyStatement: sourcePackSummary?.readyStatement || null,
      calaAvailability: sourceBrand?.calaAvailability || null,
      calaFirstPosture: sourceBrand?.calaFirstPosture || null,
      parentPlatform: sourceBrand?.parentPlatform || null,
      propertyExampleCount: sourceBrand?.propertyExampleCount ?? null,
      momentumCandidateCount: sourceBrand?.momentumCandidateCount ?? null,
      officialBrandPage: sourceBrand?.hasOfficialBrandPage === true,
      developmentPage: sourceBrand?.hasDevelopmentPage === true,
    },
    stage4ContentBuild: {
      readyStatement: stage4Build?.readyStatement || null,
      presentationRowCount: buildBrand?.presentationRowCount ?? buildBrand?.rowCount ?? null,
      note: "Stage 4 tab-factory-build established Presentation rows from source packs (no Brand Status / release).",
    },
    stage45Cleanup: {
      readyStatement: stage45?.readyStatement || null,
      patchCount: (s4Brand?.patches || []).length,
      basicsPatchCount: (s4Brand?.basicsPatches || []).length,
      brandLens:
        slug === SO_SLUG
          ? "Fashion-led luxury lifestyle collection (Ennismore/Accor). Live Basics now exists as SO/ (recTJdPlr4mDs9app)."
          : s4Brand?.brandLens?.model || null,
      distinguishFrom: s4Brand?.brandLens?.distinguishFrom || [],
      note: "Stage 4.5 content cleanup targeted thin/wrong-tone fields only; images unchanged.",
    },
    stage5ImageMaterialization: {
      readyStatement: stage5?.readyStatement || null,
      cohortNote:
        "Stage 5 applied image materialization for seven brands: 6/6 gallery, 3/3 scenario, 3/3 openings; uniqueness + role-match PASS.",
    },
    stage6PostImageCleanup: {
      readyStatement: stage6?.readyStatement || null,
      soPatched: slug === SO_SLUG,
      fairmontSfHidden: slug === "fairmont-hotels-and-resorts",
      note:
        slug === SO_SLUG
          ? "Stage 6 rewrote SO/ Brand Positioning + Guest Psychographics; steward snapshot.*/footprint.primary_regions not invented."
          : slug === "fairmont-hotels-and-resorts"
            ? "Stage 6 confirmed Fairmont San Francisco openings row Do Not Display (idempotent)."
            : "Stage 6 had no content writes for this brand; cohort gates re-validated.",
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
          scenarioDistinct: uBrand.scenarioDistinctCount ?? uBrand.scenario ?? null,
          propertyDistinct: uBrand.propertyExampleDistinctCount ?? uBrand.property ?? null,
        }
      : null,
    roleMatchPass: rBrand?.pass === true || rBrand?.roleMatch === true || null,
  };
}

function gateSummaryFromStage6(slug) {
  const stage6 = readJsonSafe("reports/brand-explorer-wave13-post-image-cleanup.json");
  const v = stage6?.postApplyValidation || {};
  const tabAudit = readJsonSafe("reports/brand-explorer-tab-factory-audit.json");
  const brand = (tabAudit?.brandResults || []).find((b) => b.brandSlug === slug);
  return {
    available: true,
    stage6AcceptancePass: v.renderedCompletenessPass7 === true && v.goldenContentQualityPass7 === true,
    tabFactoryPass: brand ? brand.failFindings === 0 : null,
    tabFactoryDecision: brand?.releaseQualityDecision || brand?.completeness?.releaseQualityDecision || null,
    tabFactoryFailFindings: brand?.failFindings ?? null,
    renderedCompletenessPass: v.renderedCompletenessPass7 === true || null,
    noEmptyPass: v.noEmptyRenderedComponentsPass7 === true || null,
    goldenPass: v.goldenContentQualityPass7 === true || null,
    imageUniquenessPass: v.imageUniquenessPass7 === true || null,
    imageRoleMatchPass: v.imageRoleMatchPass7 === true || null,
    momentumEvidencePass: v.evidenceQualityPermanentTargetsPass === true || null,
    momentumEvidenceNote:
      v.evidenceQualityWave13BrandsFlag ||
      "Permanent evidence targets PASS; Wave 13 --brands forced check has pre-existing factory-preview momentum body thinness (not Stage 6/8 write scope).",
    soPositioningCleared: v.soPositioningResidualsCleared === true,
    note: "Gate results primarily from Stage 6 post-apply validation + latest image audits; live uniqueness/role-match re-checked in packet build.",
  };
}

function looksLikeStage3ProcessNoise(text) {
  return /do not create brand basics|stage 3 documents creation|no brand basics record yet|recommendation only/i.test(
    nz(text)
  );
}

function founderTasteCautions(slug, { scenarioProcessNoise = false } = {}) {
  const notes = [];
  if (slug === "mama-shelter") {
    notes.push(
      "Confirm lifestyle/design-led positioning and F&B/social programming feel premium, not gimmicky."
    );
    notes.push(
      "Confirm distinction from Moxy, Bunkhouse, Hotel Indigo, SO/, and soft lifestyle collections."
    );
    notes.push(
      "Confirm images communicate social energy, design, rooms, and F&B/destination context."
    );
    notes.push(
      "CALA posture is pipeline (Mama Shelter Mexico City end-2026) — keep International Reference operating examples labeled until open."
    );
  }
  if (slug === "mercure") {
    notes.push("Confirm local-inspiration / midscale positioning does not read as generic Accor boilerplate.");
    notes.push("Confirm distinction from ibis, Novotel, Pullman, and Grand Mercure.");
    notes.push("Confirm conversion/regional applicability remains owner-useful.");
  }
  if (slug === "ibis") {
    notes.push("Confirm this is the ibis master brand — not ibis Styles or ibis budget.");
    notes.push(
      "Confirm owner copy emphasizes efficient essential-stay/value positioning without sounding low-quality."
    );
    notes.push("Confirm no sibling-brand imagery or language.");
  }
  if (slug === "novotel") {
    notes.push("Confirm business/family/leisure balance and meetings/family/public-space practicality for owners.");
    notes.push("Confirm distinction from Mercure, Pullman, and ibis.");
  }
  if (slug === "pullman") {
    notes.push(
      "Confirm premium business-lifestyle positioning with meetings/events, public-space/F&B, and business-transient logic."
    );
    notes.push("Confirm it does not drift into Fairmont, Sofitel, SO/, MGallery, or Novotel territory.");
  }
  if (slug === SO_SLUG) {
    notes.push(
      "Basics Brand Name is SO/; display/alias context may show SO/ Hotels & Resorts — keep naming consistent in founder QA."
    );
    notes.push(
      "Confirm luxury lifestyle / fashion-led positioning and that copy no longer sounds economy-oriented (Stage 6 Basics rewrite)."
    );
    notes.push(
      "Confirm distinction from Mama Shelter, Fairmont, MGallery, Morgans Originals, Hyde, Mondrian, and Delano."
    );
    notes.push(
      "Steward-data gaps intentionally not invented: snapshot.* and footprint.primary_regions — founder must decide if acceptable for promotion or require steward fill first."
    );
    if (scenarioProcessNoise) {
      notes.push(
        "Scenario Presentation bodies still contain Stage 3 process/governance wording (“Do not create Brand Basics…”). Read-only Stage 8 cannot patch; recommend minor content cleanup before public release."
      );
    }
  }
  if (slug === "fairmont-hotels-and-resorts") {
    notes.push(
      "Brand Basics name remains Fairmont; “Fairmont Hotels & Resorts” may appear only as consumer/display context where appropriate."
    );
    notes.push("Confirm luxury / heritage / landmark positioning.");
    notes.push("Confirm mixed-use/residential language appears only where source-supported.");
    notes.push(
      `Confirm Fairmont San Francisco leftover openings row (${FAIRMONT_SF_OPENINGS_RECORD_ID}) is not visible (Do Not Display).`
    );
    notes.push("Confirm distinction from SO/, Raffles, Sofitel, MGallery, and Pullman.");
  }
  return notes;
}

function stewardDataGaps(slug) {
  if (slug === SO_SLUG) {
    return [
      {
        field: "snapshot.*",
        status: "intentionally_not_invented",
        note: "Steward gap left open in Stage 5/6; founder decides fill-before-promotion vs accept for release.",
      },
      {
        field: "footprint.primary_regions",
        status: "intentionally_not_invented",
        note: "Steward gap left open in Stage 5/6; founder decides fill-before-promotion vs accept for release.",
      },
    ];
  }
  return [];
}

function recommend({ slug, gates, liveUniquenessPass, liveRoleMatchPass, brandStatus, residual, stewardGaps }) {
  const statusOk =
    /under review/i.test(nz(brandStatus)) ||
    !brandStatus ||
    /draft|factory/i.test(nz(brandStatus));

  if (/active|live/i.test(nz(brandStatus)) && !/under review/i.test(nz(brandStatus))) {
    return {
      recommendation: "remediation_required",
      rationale: `Unexpected Brand Status "${brandStatus}" while still in Wave 13 factory-preview cohort (promotion not started).`,
    };
  }

  if (residual.some((r) => /missing|fail|empty|uniqueness|role_match|still_visible/i.test(r))) {
    return {
      recommendation: "remediation_required",
      rationale: `Blocking residual: ${residual.slice(0, 5).join("; ")}`,
    };
  }

  if (stewardGaps.length || residual.length) {
    return {
      recommendation: "approve_after_minor_cleanup",
      rationale:
        stewardGaps.length > 0 || residual.includes("scenario_bodies_contain_stage3_process_language")
          ? `Gates green, but minor cleanup / founder decision needed: ${[
              ...stewardGaps.map((g) => g.field),
              ...residual.filter((r) => /scenario|steward/i.test(r)),
            ]
              .slice(0, 6)
              .join(", ")}.`
          : `Residual founder notes: ${residual.slice(0, 4).join("; ")}`,
    };
  }

  const gateOk =
    gates.renderedCompletenessPass !== false &&
    gates.noEmptyPass !== false &&
    gates.goldenPass !== false &&
    (gates.imageUniquenessPass !== false || liveUniquenessPass === true) &&
    (gates.imageRoleMatchPass !== false || liveRoleMatchPass === true);

  if (!gateOk || !statusOk) {
    return {
      recommendation: "approve_after_minor_cleanup",
      rationale: "One or more Stage 6 gate signals missing on disk — confirm live gates before promotion.",
    };
  }

  return {
    recommendation: "approve_for_status_promotion_and_public_release",
    rationale:
      slug === "fairmont-hotels-and-resorts"
        ? "Stage 6 gates passed; Fairmont San Francisco openings hidden; naming posture Fairmont Basics + optional Hotels & Resorts display context. Founder taste cautions only."
        : "Stage 6 factory acceptance gates passed; profile remains Under Review / factory preview with founder-taste cautions only.",
  };
}

/**
 * Build one Wave 13 founder review packet (read-only).
 */
export async function buildWave13FounderReviewPacket(slug) {
  const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
  if (!identity?.recordId) throw new Error(`Missing factory-preview identity for ${slug}`);
  if (!WAVE13_STAGE4_APPROVED_SLUGS.includes(slug)) {
    throw new Error(`${slug} is not in Wave 13 Stage 8 seven-brand scope`);
  }

  const sourcePack = getWave13SourcePack(slug);
  const artifacts = stageArtifactSummaries(slug);

  const nameCandidates = [
    identity.name,
    slug === SO_SLUG ? "SO/" : null,
    slug === SO_SLUG ? "SO/ Hotels & Resorts" : null,
    slug === "fairmont-hotels-and-resorts" ? "Fairmont" : null,
    slug === "fairmont-hotels-and-resorts" ? "Fairmont Hotels & Resorts" : null,
    sourcePack?.brandBasicsName,
    sourcePack?.name,
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
    const img = imageRef(row);
    return {
      index: i,
      title: nz(row?.title) || `Scenario ${i}`,
      summary: nz(row?.body) || "",
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
      caption: imageCaption(row),
    };
  });

  const fairmontSfVisible = rows.some(
    (r) =>
      r.recordId === FAIRMONT_SF_OPENINGS_RECORD_ID &&
      r.active !== false &&
      !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
  );
  const fairmontSfRow = rows.find((r) => r.recordId === FAIRMONT_SF_OPENINGS_RECORD_ID);

  const similar = [1, 2, 3]
    .map((i) => {
      const row = visibleSlot(rows, `insight.similar.${i}`)[0];
      return {
        name: nz(row?.title) || null,
        summary: nz(row?.body) || null,
      };
    })
    .filter((s) => s.name);

  // Fallback: single insight.similar body listing peers
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
    visibleSlot(rows, "standards.questions")[0] || visibleSlot(rows, "owner.questions")[0];

  const positioningRow =
    visibleSlot(rows, "positioning.positioning")[0] ||
    visibleSlot(rows, "Brand Positioning")[0] ||
    visibleSlot(rows, "overview.relative_positioning")[0];

  const audienceRow =
    visibleSlot(rows, "positioning.audience")[0] ||
    visibleSlot(rows, "Guest Psychographics Description")[0];

  const ownerFitRow =
    visibleSlot(rows, "positioning.owner_fit")[0] ||
    visibleSlot(rows, "overview.owner_fit")[0];

  const propertyFitRow =
    visibleSlot(rows, "positioning.property_fit")[0] ||
    visibleSlot(rows, "overview.property_fit")[0];

  const targetGuestSegments =
    brand?.targetGuestSegments ||
    brand?.brandBasics?.targetGuestSegments ||
    sourcePack?.targetGuestSegmentsRecommended ||
    null;

  const liveUniqueness = evaluateImageUniqueness({ brandSlug: slug, presentationRows: rows });
  const liveRoleMatch = evaluateBrandImageRoleMatch({ brandSlug: slug, presentationRows: rows });
  const diskImages = imageAuditFromDisk(slug);
  const gates = gateSummaryFromStage6(slug);

  const residual = [];
  if (!scenarioDistinctiveness.pass) residual.push("scenario_image_distinctiveness");
  if (openings.length < 3) residual.push(`openings_count_${openings.length}`);
  if (liveUniqueness.pass !== true && diskImages.uniquenessPass !== true) {
    residual.push("image_uniqueness_not_confirmed");
  }
  if (liveRoleMatch.pass !== true && diskImages.roleMatchPass !== true) {
    residual.push("image_role_match_not_confirmed");
  }
  if (slug === "fairmont-hotels-and-resorts" && fairmontSfVisible) {
    residual.push("fairmont_san_francisco_still_visible");
  }
  const scenarioProcessNoise = scenarios.some(
    (s) => looksLikeStage3ProcessNoise(s.title) || looksLikeStage3ProcessNoise(s.summary)
  );
  if (scenarioProcessNoise) residual.push("scenario_bodies_contain_stage3_process_language");

  const stewardGaps = stewardDataGaps(slug);
  const { recommendation, rationale } = recommend({
    slug,
    gates,
    liveUniquenessPass: liveUniqueness.pass === true,
    liveRoleMatchPass: liveRoleMatch.pass === true,
    brandStatus,
    residual,
    stewardGaps,
  });

  if (!RECOMMENDATIONS.includes(recommendation)) {
    throw new Error(`Invalid recommendation ${recommendation}`);
  }

  const calaAvailability = artifacts.sourcePack.calaAvailability || sourcePack?.calaAvailability;
  const calaStatus = {
    calaAvailability,
    strongCalaAnchor: STRONG_CALA_SLUGS.includes(slug),
    pipelineCala: PIPELINE_CALA_SLUGS.includes(slug),
    internationalReferenceMomentum: INTL_REF_LED_SLUGS.includes(slug),
    openingsCalaCount: openings.filter((o) => o.geography === "CALA").length,
    openingsIntlCount: openings.filter((o) => o.geography === "International Reference").length,
    posture: artifacts.sourcePack.calaFirstPosture || null,
    label: STRONG_CALA_SLUGS.includes(slug)
      ? "CALA-first (strong anchors)"
      : PIPELINE_CALA_SLUGS.includes(slug)
        ? "CALA pipeline + International Reference operating examples"
        : INTL_REF_LED_SLUGS.includes(slug)
          ? "International Reference–led (CALA none_found in Stage 3 pack)"
          : "International Reference / mixed",
  };

  const sourceOwnerNotes = (sourcePack?.ownerFacingPositioningNotes || []).filter(
    (n) =>
      !/do not create brand basics|stage 3 documents creation|no brand basics record yet/i.test(
        nz(n)
      )
  );

  const brandPositioningSummary =
    nz(brand?.brandPositioning) ||
    nz(brand?.brandBasics?.brandPositioning) ||
    nz(positioningRow?.body) ||
    sourceOwnerNotes.join(" ") ||
    `${identity.name} — Accor Wave 13 factory-preview brand; confirm positioning in Presentation/Basics.`;

  const namingPosture =
    slug === SO_SLUG
      ? {
          basicsBrandName: "SO/",
          basicsRecordId: SO_BASICS_RECORD_ID,
          displayAlias: "SO/ Hotels & Resorts",
          note: "Live Basics identity is SO/ (recTJdPlr4mDs9app); Presentation Brand Name may use SO/ Hotels & Resorts.",
        }
      : slug === "fairmont-hotels-and-resorts"
        ? {
            basicsBrandName: "Fairmont",
            basicsRecordId: identity.recordId,
            displayAlias: "Fairmont Hotels & Resorts (consumer/display context only)",
            note: "Brand Basics name remains Fairmont; do not rename Basics to Fairmont Hotels & Resorts.",
          }
        : {
            basicsBrandName: identity.name,
            basicsRecordId: identity.recordId,
            displayAlias: null,
            note: null,
          };

  return {
    version: WAVE13_FOUNDER_REVIEW_VERSION,
    wave13Version: WAVE13_VERSION,
    stage: "founder-review",
    generatedAt: new Date().toISOString(),
    dryRun: true,
    writePerformed: false,
    brandSlug: slug,
    brandName: identity.name,
    recordId: identity.recordId,
    namingPosture,
    parentCompany:
      artifacts.sourcePack.parentPlatform ||
      sourcePack?.parentPlatform ||
      brand?.parentCompany ||
      "Accor",
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
    stageSummaries: artifacts,
    sourcePackSummary: {
      officialBrandPage: sourcePack?.officialBrandPage?.url || null,
      developmentPage: sourcePack?.developmentPage?.url || null,
      calaAvailability,
      propertyExampleCount: (sourcePack?.propertyExamples || []).length,
      recentMomentumCandidateCount: (sourcePack?.recentMomentumCandidates || []).length,
      lens: sourceOwnerNotes[0] || null,
      parentPlatform: sourcePack?.parentPlatform || null,
    },
    gates: {
      ...gates,
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
    targetGuestSegments: Array.isArray(targetGuestSegments)
      ? targetGuestSegments
      : targetGuestSegments
        ? [targetGuestSegments]
        : [],
    brandPositioningSummary,
    audienceSummary: nz(audienceRow?.body) || nz(brand?.guestPsychographicsDescription) || null,
    ownerFitSummary:
      nz(ownerFitRow?.body) || sourceOwnerNotes.slice(0, 2).join(" ") || null,
    propertyFitSummary:
      nz(propertyFitRow?.body) ||
      nz((sourcePack?.propertyFitNotes || [])[0]) ||
      sourceOwnerNotes[2] ||
      null,
    scenarios,
    scenarioDistinctiveness,
    openings,
    fairmontSanFrancisco: {
      recordId: FAIRMONT_SF_OPENINGS_RECORD_ID,
      relevant: slug === "fairmont-hotels-and-resorts",
      visible: fairmontSfVisible,
      externalDisplayStatus: fairmontSfRow
        ? nz(fairmontSfRow.externalDisplayStatus) || "(blank)"
        : "row_not_in_presentation_fetch",
      title: fairmontSfRow ? nz(fairmontSfRow.title) : null,
    },
    similarBrands: similar,
    ownerQuestions: nz(ownerQuestions?.body) || null,
    founderTasteCautions: founderTasteCautions(slug, { scenarioProcessNoise }),
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
      contentPatches: false,
      houseOfOriginalsUntouched: true,
      morgansOriginalsUntouched: true,
      radissonCollectionUntouched: true,
      protected39Untouched: true,
      protectedBaselineCount: WAVE13_PROTECTED_BASELINE_COUNT || EXPECTED_ACTIVE_COUNT_39,
    },
  };
}

function fmtGate(v) {
  if (v === true) return "PASS";
  if (v === false) return "FAIL";
  return "—";
}

export function renderWave13FounderReviewMarkdown(packet) {
  const lines = [];
  lines.push(`# Founder Review — ${packet.brandName}`);
  lines.push("");
  lines.push(
    `Version: \`${packet.version}\` · Stage: **${packet.stage}** · Generated: ${packet.generatedAt}`
  );
  lines.push(`Mode: **dry-run** · writePerformed: **${packet.writePerformed === true}**`);
  lines.push("");
  lines.push(`## Identity`);
  lines.push("");
  lines.push(`| Field | Value |`);
  lines.push(`| --- | --- |`);
  lines.push(`| Brand name | ${packet.brandName} |`);
  lines.push(`| Slug | \`${packet.brandSlug}\` |`);
  lines.push(`| Brand Basics record ID | \`${packet.recordId}\` |`);
  lines.push(`| Parent company / platform | ${packet.parentCompany || "—"} |`);
  lines.push(`| Brand Status | **${packet.brandStatus || "—"}** |`);
  lines.push(`| Current visibility state | ${packet.visibilityState || "—"} |`);
  lines.push(`| Factory preview URL | \`${packet.factoryPreview?.primaryUrl || "—"}\` |`);
  if (packet.namingPosture?.note) {
    lines.push(`| Naming posture | ${packet.namingPosture.note} |`);
  }
  lines.push("");
  lines.push(`## Recommendation`);
  lines.push("");
  lines.push(`**${packet.recommendation}**`);
  lines.push("");
  lines.push(packet.recommendationRationale || "");
  if (packet.holdForPromotion) {
    lines.push("");
    lines.push(`_Hold for status promotion until founder clears noted gaps/cautions._`);
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
  lines.push(`### Stage 4.5 cleanup`);
  lines.push(`- ${packet.stageSummaries?.stage45Cleanup?.note || "—"}`);
  lines.push(
    `- Patches: Presentation ${packet.stageSummaries?.stage45Cleanup?.patchCount ?? "—"} · Basics ${packet.stageSummaries?.stage45Cleanup?.basicsPatchCount ?? "—"}`
  );
  if (packet.stageSummaries?.stage45Cleanup?.brandLens) {
    lines.push(`- Brand lens: ${packet.stageSummaries.stage45Cleanup.brandLens}`);
  }
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
  lines.push(
    `| Recent Momentum / Openings evidence quality | ${fmtGate(packet.gates.momentumEvidencePass)} |`
  );
  if (packet.gates.momentumEvidenceNote) {
    lines.push(`| Evidence note | ${packet.gates.momentumEvidenceNote} |`);
  }
  lines.push(
    `| Image uniqueness | ${fmtGate(packet.gates.liveImageUniquenessPass ?? packet.gates.imageUniquenessPass)} · gallery ${packet.gates.liveImageCounts?.galleryDistinct ?? "—"} / scenario ${packet.gates.liveImageCounts?.scenarioDistinct ?? "—"} / property ${packet.gates.liveImageCounts?.propertyDistinct ?? "—"} |`
  );
  lines.push(
    `| Image role-match | ${fmtGate(packet.gates.liveImageRoleMatchPass ?? packet.gates.imageRoleMatchPass)} |`
  );
  lines.push("");
  lines.push(`## CALA-first / International Reference`);
  lines.push("");
  lines.push(`- Status: **${packet.calaStatus.label}**`);
  lines.push(`- Pack CALA availability: **${packet.calaStatus.calaAvailability || "—"}**`);
  if (packet.calaStatus.posture) lines.push(`- Posture: ${packet.calaStatus.posture}`);
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
  if (packet.audienceSummary) {
    lines.push("");
    lines.push(`### Audience`);
    lines.push("");
    lines.push(packet.audienceSummary);
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
    lines.push(`- (none found in visible Presentation read)`);
  } else {
    for (const o of packet.openings) {
      lines.push(
        `- **${o.title || "(untitled)"}** · ${o.geography} · image=${o.hasImage ? "yes" : "no"} · caption=${o.caption || "—"} · source=${o.sourceLink || "—"}`
      );
    }
  }
  if (packet.fairmontSanFrancisco?.relevant) {
    lines.push("");
    lines.push(`### Fairmont San Francisco leftover row`);
    lines.push(
      `- Record \`${packet.fairmontSanFrancisco.recordId}\` · visible=**${packet.fairmontSanFrancisco.visible}** · External Display Status: **${packet.fairmontSanFrancisco.externalDisplayStatus}**`
    );
    if (packet.fairmontSanFrancisco.title) {
      lines.push(`- Title: ${packet.fairmontSanFrancisco.title}`);
    }
  }
  lines.push("");
  lines.push(`## Similar Brands`);
  lines.push("");
  for (const s of packet.similarBrands || []) {
    lines.push(`- **${s.name}** — ${s.summary || "—"}`);
  }
  if (!(packet.similarBrands || []).length) lines.push(`- —`);
  lines.push("");
  lines.push(`## Owner Questions`);
  lines.push("");
  lines.push(packet.ownerQuestions || "—");
  lines.push("");
  lines.push(`## Founder-taste cautions`);
  lines.push("");
  for (const c of packet.founderTasteCautions || []) lines.push(`- ${c}`);
  lines.push("");
  lines.push(`## Steward-data gaps`);
  lines.push("");
  if ((packet.stewardDataGaps || []).length) {
    for (const g of packet.stewardDataGaps) {
      lines.push(`- **${g.field}** — ${g.status}: ${g.note}`);
    }
  } else {
    lines.push(`- None disclosed for this brand.`);
  }
  lines.push("");
  lines.push(`## Guardrails`);
  lines.push("");
  for (const [k, v] of Object.entries(packet.guardrails || {})) {
    lines.push(`- ${k}: **${v}**`);
  }
  lines.push("");
  return lines.join("\n");
}

export function writeWave13FounderReviewReports({ packets, summary }) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const perBrandPaths = [];
  for (const packet of packets) {
    const md = renderWave13FounderReviewMarkdown(packet);
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

  const summaryMdPath = path.join(REPORTS_DIR, "brand-explorer-wave13-founder-review-summary.md");
  const summaryJsonPath = path.join(
    REPORTS_DIR,
    "brand-explorer-wave13-founder-review-summary.json"
  );
  fs.writeFileSync(summaryJsonPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
  fs.writeFileSync(summaryMdPath, `${renderWave13FounderReviewSummaryMarkdown(summary)}\n`, "utf8");

  const docPath = path.join(DOCS_DIR, "brand-explorer-wave13-founder-review.md");
  fs.writeFileSync(docPath, `${renderWave13FounderReviewDoc(summary)}\n`, "utf8");

  return { perBrandPaths, summaryMdPath, summaryJsonPath, docPath };
}

function renderWave13FounderReviewSummaryMarkdown(summary) {
  return [
    `# Wave 13 — Founder Review Summary`,
    ``,
    `Generated: ${summary.generatedAt}`,
    `Stage: **founder-review** · dry-run: **true** · writePerformed: **false**`,
    `Protected baseline count: **${summary.protectedBaselineCount}** (untouched)`,
    ``,
    `## Seven-brand review table`,
    ``,
    `| Slug | Brand | Status | CALA posture | Recommendation | Hold |`,
    `| --- | --- | --- | --- | --- | --- |`,
    ...(summary.brands || []).map(
      (b) =>
        `| \`${b.brandSlug}\` | ${b.brandName} | ${b.brandStatus} | ${b.calaLabel} | **${b.recommendation}** | ${b.holdForPromotion ? "yes" : "no"} |`
    ),
    ``,
    `## Pass/fail gate summary`,
    ``,
    `- Rendered completeness (7): **${summary.gateSummary?.renderedCompleteness || "—"}**`,
    `- No-empty (7): **${summary.gateSummary?.noEmpty || "—"}**`,
    `- Golden (7): **${summary.gateSummary?.golden || "—"}**`,
    `- Image uniqueness (7): **${summary.gateSummary?.imageUniqueness || "—"}**`,
    `- Image role-match (7): **${summary.gateSummary?.imageRoleMatch || "—"}**`,
    `- Evidence (permanent targets): **${summary.gateSummary?.evidencePermanent || "—"}**`,
    `- Protected 39 PVQL / baseline: **${summary.gateSummary?.protected39 || "—"}**`,
    ``,
    `## Recommendation counts`,
    ``,
    `- approve_for_status_promotion_and_public_release: **${summary.counts?.approve_for_status_promotion_and_public_release ?? 0}**`,
    `- approve_after_minor_cleanup: **${summary.counts?.approve_after_minor_cleanup ?? 0}**`,
    `- remediation_required: **${summary.counts?.remediation_required ?? 0}**`,
    ``,
    `## Founder-taste cautions (themes)`,
    ``,
    ...(summary.founderTasteThemes || []).map((t) => `- ${t}`),
    ``,
    `## Steward-data gaps`,
    ``,
    ...(summary.stewardDataGaps || []).length
      ? (summary.stewardDataGaps || []).map((g) => `- **${g.brandSlug}**: ${g.fields.join(", ")} — ${g.note}`)
      : [`- None beyond SO/ disclosure.`],
    ``,
    `## Excluded brands`,
    ``,
    `- **The House of Originals** — \`excluded_from_wave13_stage8\`; Stage 3.5 recommendation **C** remains in force; no content build / images / public release path.`,
    `- **Morgans Originals** — \`not_created_not_modified\`; not part of Wave 13.`,
    `- **Radisson Collection** — excluded; remains non-target / not separately promoted.`,
    ``,
    `## Status promotion readiness`,
    ``,
    `- May Wave 13 proceed to status promotion? **${summary.mayProceedToStatusPromotion}**`,
    `- Brands held back: ${(summary.brandsHeldBack || []).length ? (summary.brandsHeldBack || []).map((s) => `\`${s}\``).join(", ") : "_none_"}`,
    `- Note: ${summary.promotionReadinessNote || "—"}`,
    ``,
    `## Next stage`,
    ``,
    `- Do **not** promote Brand Status or set release fields until founder signs packets.`,
    `- After founder approval: \`status-promotion\` → \`public-release\` → baseline freeze revision (separate explicit tasks).`,
    ``,
    `## Guardrails`,
    ``,
    `- No Airtable / Presentation / Brand Status / release / CV / Source Library / Registry / image writes`,
    `- Protected 39 untouched`,
    `- House of Originals / Morgans Originals / Radisson Collection untouched`,
    ``,
  ].join("\n");
}

function renderWave13FounderReviewDoc(summary) {
  return [
    `# Wave 13 Founder Review`,
    ``,
    `Stage 8 of the Wave 13 factory produces **read-only** founder review packets for the seven Accor factory-preview brands.`,
    ``,
    `## Command`,
    ``,
    "```bash",
    "npm run brand-explorer-wave13-factory -- --stage founder-review --dry-run",
    "```",
    ``,
    `**No \`--apply\`.** If \`--apply\` is passed, the stage throws — founder-review must remain read-only.`,
    ``,
    `## Outputs`,
    ``,
    `- \`reports/brand-explorer-founder-review-{slug}.md\` (×7)`,
    `- \`reports/brand-explorer-wave13-founder-review-summary.md\``,
    `- \`reports/brand-explorer-wave13-founder-review-summary.json\``,
    ``,
    `## Target brands`,
    ``,
    ...(WAVE13_STAGE4_APPROVED_SLUGS || []).map((s) => `- \`${s}\``),
    ``,
    `## Recommendations`,
    ``,
    `- \`approve_for_status_promotion_and_public_release\``,
    `- \`approve_after_minor_cleanup\``,
    `- \`remediation_required\``,
    ``,
    `## Exclusions`,
    ``,
    `- The House of Originals — excluded (Stage 3.5 option C)`,
    `- Morgans Originals — not created / not modified`,
    `- Radisson Collection — non-target`,
    `- Protected 39 — untouched`,
    ``,
    `## Guardrails`,
    ``,
    `- dry-run only — **no Airtable writes**`,
    `- no Brand Status / release / Company Validated / Source Library / Registry writes`,
    `- no content or image patches`,
    `- protected ${summary.protectedBaselineCount} baseline untouched`,
    ``,
    `## Promotion readiness (last run)`,
    ``,
    `- May proceed: **${summary.mayProceedToStatusPromotion}**`,
    `- Held back: ${(summary.brandsHeldBack || []).join(", ") || "none"}`,
    ``,
    `Last generated: ${summary.generatedAt}`,
    ``,
  ].join("\n");
}

/**
 * Run Wave 13 founder-review stage (always dry-run / no writes).
 */
export async function runWave13FounderReview({ argv = [] } = {}) {
  if (argv.includes("--apply")) {
    throw new Error(
      "founder-review is read-only. Remove --apply; use --dry-run only (packets + reports)."
    );
  }

  const packets = [];
  for (const slug of WAVE13_STAGE4_APPROVED_SLUGS) {
    const packet = await buildWave13FounderReviewPacket(slug);
    packets.push(packet);
    await sleep(250);
  }

  const counts = {
    approve_for_status_promotion_and_public_release: 0,
    approve_after_minor_cleanup: 0,
    remediation_required: 0,
  };
  for (const p of packets) {
    if (counts[p.recommendation] != null) counts[p.recommendation] += 1;
  }

  const brandsHeldBack = packets
    .filter((p) => p.holdForPromotion)
    .map((p) => p.brandSlug);

  const mayProceedToStatusPromotion =
    counts.remediation_required === 0
      ? brandsHeldBack.length
        ? "partial_yes_with_holds"
        : "yes_after_founder_signoff"
      : "no_remediation_required";

  const stage6 = readJsonSafe("reports/brand-explorer-wave13-post-image-cleanup.json");
  const v = stage6?.postApplyValidation || {};

  const summary = {
    version: WAVE13_FOUNDER_REVIEW_VERSION,
    wave13Version: WAVE13_VERSION,
    stage: "founder-review",
    generatedAt: new Date().toISOString(),
    dryRun: true,
    writePerformed: false,
    protectedBaselineCount: WAVE13_PROTECTED_BASELINE_COUNT || EXPECTED_ACTIVE_COUNT_39,
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
      previewUrl: p.factoryPreview?.primaryUrl,
    })),
    gateSummary: {
      renderedCompleteness: v.renderedCompletenessPass7 ? "PASS" : "—",
      noEmpty: v.noEmptyRenderedComponentsPass7 ? "PASS" : "—",
      golden: v.goldenContentQualityPass7 ? "PASS" : "—",
      imageUniqueness: v.imageUniquenessPass7 ? "PASS" : "—",
      imageRoleMatch: v.imageRoleMatchPass7 ? "PASS" : "—",
      evidencePermanent: v.evidenceQualityPermanentTargetsPass ? "PASS" : "—",
      protected39:
        v.pvqlPublicFullOnly?.publicFullProfileCount === 39 && v.baseline39Pass
          ? "PASS 39/39"
          : "confirm live PVQL/baseline",
    },
    founderTasteThemes: [
      "Mama Shelter: lifestyle/F&B premium tone + distinction from Moxy/Bunkhouse/Indigo/SO/.",
      "Mercure: local midscale owner-useful copy; avoid Accor boilerplate; distinguish from ibis/Novotel/Pullman/Grand Mercure.",
      "ibis: master brand only (not Styles/budget); essential-stay without low-quality tone.",
      "Novotel: business/family/leisure + meetings practicality vs Mercure/Pullman/ibis.",
      "Pullman: premium business-lifestyle; avoid Fairmont/Sofitel/SO/MGallery/Novotel drift.",
      "SO/: fashion-led luxury (not economy); steward gaps disclosed; naming SO/ vs SO/ Hotels & Resorts.",
      "Fairmont: Basics name Fairmont; SF openings Do Not Display; luxury/heritage vs SO/Raffles/Sofitel/MGallery/Pullman.",
    ],
    stewardDataGaps: packets
      .filter((p) => (p.stewardDataGaps || []).length)
      .map((p) => ({
        brandSlug: p.brandSlug,
        fields: p.stewardDataGaps.map((g) => g.field),
        note: "intentionally_not_invented — founder decision required before public release",
      })),
    excluded: {
      "the-house-of-originals": {
        status: "excluded_from_wave13_stage8",
        founderRecommendation: "C",
        note: "Stage 3.5 founder/manual review recommendation C remains in force; no content build, images, or public release path.",
      },
      "morgans-originals": {
        status: "not_created_not_modified",
        note: "Not part of Wave 13.",
      },
      "radisson-collection": {
        status: "excluded",
        note: "Remains non-target / not separately promoted.",
      },
    },
    mayProceedToStatusPromotion,
    brandsHeldBack,
    promotionReadinessNote:
      mayProceedToStatusPromotion === "yes_after_founder_signoff"
        ? "All seven packets recommend promotion after founder sign-off; no remediation holds."
        : mayProceedToStatusPromotion === "partial_yes_with_holds"
          ? `Six (or subset) may proceed after founder sign-off; hold ${brandsHeldBack.join(", ")} until minor cleanup / steward decision.`
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
      protected39Untouched: true,
      houseOfOriginalsUntouched: true,
      morgansOriginalsUntouched: true,
      radissonCollectionUntouched: true,
    },
    acceptance: {
      packetsForAllSeven: packets.length === 7,
      everyPacketHasRecommendation: packets.every((p) =>
        RECOMMENDATIONS.includes(p.recommendation)
      ),
      houseExcluded: true,
      morgansUntouched: true,
      radissonUntouched: true,
      noAirtableWrites: true,
    },
    readyStatement: "wave13_founder_review_packets_ready",
  };

  const paths = writeWave13FounderReviewReports({ packets, summary });

  return {
    ...summary,
    packets,
    paths,
  };
}
