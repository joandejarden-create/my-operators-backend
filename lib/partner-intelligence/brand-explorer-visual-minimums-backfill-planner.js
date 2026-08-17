/**
 * Brand Explorer Visual Minimums & Image Backfill Planner v25.
 *
 * Read-only planner: defines minimum visual coverage contract, audits current
 * slot coverage, and recommends backfill candidates from Brand Asset Registry.
 * No Airtable writes, no downloads/attachments, no slot promotion.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import {
  listRegistryRecordsRaw,
  mapRecordToVisualSlot,
  VISUAL_SLOT,
  MAP_VISUAL_SLOT,
} from "./brand-explorer-visual-slot-requirements.js";
import {
  normalizeRegistryAssetRecord,
  MAP_BRAND_ASSET,
} from "./brand-asset-registry-workflow.js";
import { isFormallyApprovedRecord } from "./brand-asset-review-decision-writer.js";

export const PLANNER_VERSION = "25-R2";
export const REPORT_JSON_NAME = "brand-explorer-visual-minimums-backfill-planner.json";
export const REPORT_MD_NAME = "brand-explorer-visual-minimums-backfill-planner.md";
export const DOC_MD_NAME = "brand-explorer-visual-minimums-backfill-planner-v25-R2.md";

export const PROVISIONAL_ASSIGNMENT_LABEL =
  "Provisional fallback · Pending founder visual review · Not company-validated · Not Marriott-validated · Replaceable later";

/** Pinned strict assignments when registry record remains valid. */
const PINNED_STRICT_ASSET_BY_SLOT = {
  "materials.gallery.3": "recxVPbTlsrP9v4bQ",
};

const DEFAULT_BRAND_ID = "recCvV0PuZOi8c3hC";
const CURIO_BRAND_ID = "receQkxgjlezsc1xg";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const VISUAL_MINIMUMS = {
  hero: {
    section: "Hero",
    minimum: 1,
    slots: ["overview.hero"],
  },
  valueScenarios: {
    section: "Where This Brand Creates the Most Value",
    minimum: 3,
    slots: ["overview.scenario.1", "overview.scenario.2", "overview.scenario.3"],
    noBlankPlaceholder: true,
  },
  gallery: {
    section: "Image Gallery",
    minimum: 6,
    slots: [
      "materials.gallery.1",
      "materials.gallery.2",
      "materials.gallery.3",
      "materials.gallery.4",
      "materials.gallery.5",
      "materials.gallery.6",
    ],
    noSkippedNumber: true,
  },
  openings: {
    section: "Openings / Examples / Properties",
    minimum: 1,
    slots: ["footprint.openings"],
    requiredCardFields: ["image", "title", "location_or_descriptor", "summary", "url_if_available"],
  },
  momentum: {
    section: "Recent Momentum",
    minimum: 1,
    slots: ["footprint.momentum"],
    requiresDatedSourceRows: true,
  },
};

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}
function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}
function wordCount(v) {
  return nz(v).split(/\s+/).filter(Boolean).length;
}
function lower(v) {
  return nz(v).toLowerCase();
}

function readJson(rel) {
  try {
    return JSON.parse(fs.readFileSync(path.join(ROOT, rel), "utf8"));
  } catch {
    return null;
  }
}

function normalizeBrandInput(raw) {
  const n = lower(raw);
  if (!n || n === "tribute-portfolio" || n === "tribute portfolio") return DEFAULT_BRAND_ID;
  return nz(raw);
}

async function fetchBrand(idOrName) {
  const req = { query: { brandId: idOrName, refresh: "1" }, headers: {} };
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
      return this;
    },
  };
  await getBrandLibraryBrandById(req, res);
  return res.statusCode < 400 ? res.payload?.brand : null;
}

function blocksForSlot(brand, slotKey) {
  return (brand?.brandExplorer?.blocks || []).filter((b) => nz(b.slotKey) === nz(slotKey));
}

function parseMomentumBlock(row) {
  const body = nz(row?.body);
  const parts = body.split(/\n\n+/).map((x) => nz(x)).filter(Boolean);
  const dateLine = parts[0] || "";
  const url = parts.find((p) => /^https?:\/\//i.test(p)) || "";
  return {
    dateLike: /\b(?:\d{4}|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b/i.test(dateLine),
    hasUrl: Boolean(url),
  };
}

function parseOpeningsCard(row) {
  const body = nz(row?.body);
  const parts = body.split(/\n\n+/).map((x) => nz(x)).filter(Boolean);
  const url = parts.find((p) => /^https?:\/\//i.test(p)) || "";
  const locationOrDescriptor = parts[1] || parts[0] || "";
  const summary = parts[4] || parts[2] || parts[0] || "";
  return {
    hasImage: hasVal(row?.imageUrl),
    hasTitle: hasVal(row?.title),
    hasLocationOrDescriptor: hasVal(locationOrDescriptor),
    hasSummary: hasVal(summary),
    hasUrl: hasVal(url),
    isRenderable:
      hasVal(row?.imageUrl) &&
      hasVal(row?.title) &&
      hasVal(locationOrDescriptor) &&
      hasVal(summary),
  };
}

function normalizeRegistryForPlanner(rawRecord) {
  const base = normalizeRegistryAssetRecord(rawRecord);
  const f = rawRecord.fields || {};
  const visualSlotStatus = nz(f[MAP_VISUAL_SLOT.validationStatus]);
  const visualSlot = nz(f[MAP_VISUAL_SLOT.explorerSection]) || mapRecordToVisualSlot(base);
  const sourcePageConfirmsContext = nz(f[MAP_VISUAL_SLOT.sourcePageConfirmsContext]);
  const propertyConfirmed = nz(f[MAP_VISUAL_SLOT.propertyConfirmed]);
  const brandConfirmed = nz(f[MAP_VISUAL_SLOT.brandConfirmed]);
  const calaRelevant = nz(f[MAP_VISUAL_SLOT.calaRelevant]);
  const relatedPropertyName = nz(f[MAP_VISUAL_SLOT.relatedPropertyName]) || nz(base.relatedPropertyName);
  const relatedOpeningPr = nz(f[MAP_VISUAL_SLOT.relatedOpeningPr]);
  const countryRegion = nz(f[MAP_VISUAL_SLOT.countryRegion]);

  const rejected = /rejected|do not use|blocked/i.test(
    `${nz(base.assetStatus)} ${nz(base.usageReviewStatus)} ${nz(base.explorerUsePermission)}`
  );
  const superseded = /superseded|not selected/i.test(
    `${nz(base.reviewNotes)} ${nz(f[MAP_VISUAL_SLOT.validationNotes])}`
  );
  const formalApproved = isFormallyApprovedRecord({
    assetStatus: base.assetStatus,
    explorerUsePermission: base.explorerUsePermission,
    usageReviewStatus: base.usageReviewStatus,
    reviewNotes: base.reviewNotes,
  });
  const approved =
    formalApproved ||
    /approved for explorer use/i.test(nz(base.assetStatus)) ||
    /approved for explorer/i.test(nz(base.explorerUsePermission));
  const hasAttachment = Array.isArray(rawRecord.fields?.[MAP_BRAND_ASSET.attachment]) &&
    rawRecord.fields?.[MAP_BRAND_ASSET.attachment].length > 0;

  return {
    id: rawRecord.id,
    assetName: nz(base.assetName),
    sourceUrl: nz(base.sourceUrl),
    sourcePageUrl: nz(base.sourcePageUrl),
    sourceBasis: nz(base.sourceBasis),
    visualSlot,
    visualSlotStatus,
    relatedPropertyName,
    relatedOpeningPr,
    countryRegion,
    sourcePageConfirmsContext,
    propertyConfirmed,
    brandConfirmed,
    calaRelevant,
    explorerUsePermission: nz(base.explorerUsePermission),
    assetStatus: nz(base.assetStatus),
    usageReviewStatus: nz(base.usageReviewStatus),
    reviewNotes: nz(base.reviewNotes),
    approved,
    hasAttachment,
    rejected,
    superseded,
  };
}

function slotFromVisualSlot(asset) {
  if (asset.visualSlot === VISUAL_SLOT.HERO) return "overview.hero";
  if (asset.visualSlot === VISUAL_SLOT.GALLERY) return nz(asset.recommendedExplorerSlot) || "";
  return nz(asset.recommendedExplorerSlot) || nz(asset.visualSlot);
}

function isEligibleStrict(asset) {
  return asset.approved && !asset.rejected && !asset.superseded;
}

function isStrongConfirmedCandidate(asset) {
  const controlled = /marriott-controlled source|company-controlled source/i.test(asset.sourceBasis);
  return (
    controlled &&
    asset.brandConfirmed === "Yes" &&
    asset.propertyConfirmed === "Yes" &&
    asset.sourcePageConfirmsContext === "Yes" &&
    !asset.rejected &&
    !asset.superseded &&
    !asset.approved
  );
}

function collectPresentationImageUrls(brand) {
  const urls = new Set();
  for (const block of brand?.brandExplorer?.blocks || []) {
    const url = lower(block?.imageUrl);
    if (url) urls.add(url);
  }
  return urls;
}

function assetUsedInPresentation(asset, presentationUrls) {
  const url = lower(asset.sourceUrl);
  return Boolean(url && presentationUrls.has(url));
}

function imageSlotKeys() {
  return [
    ...VISUAL_MINIMUMS.hero.slots,
    ...VISUAL_MINIMUMS.valueScenarios.slots,
    ...VISUAL_MINIMUMS.gallery.slots,
  ];
}

function slotMatchKind(asset, slotKey) {
  const recSlot = nz(asset.recommendedExplorerSlot);
  if (recSlot === slotKey) return "exact_slot";
  if (slotKey.startsWith("overview.scenario.") && asset.visualSlot === VISUAL_SLOT.VALUE_DRIVER) {
    return "approved_fallback_value_driver";
  }
  if (slotKey.startsWith("materials.gallery.") && asset.visualSlot === VISUAL_SLOT.GALLERY) {
    return "approved_fallback_gallery";
  }
  if (slotKey.startsWith("overview.scenario.") && asset.visualSlot === VISUAL_SLOT.HERO) {
    return "approved_fallback_hero";
  }
  return null;
}

function classifyCandidate(asset, { usedAssetIds, presentationUrls, planMode, matchKind }) {
  const reuseInPlan = usedAssetIds.has(asset.id);
  const reuseInPresentation = assetUsedInPresentation(asset, presentationUrls);
  const provisionalReuse = reuseInPlan || reuseInPresentation;

  if (planMode === "strict") {
    if (!isEligibleStrict(asset)) return null;
    if (provisionalReuse) return null;
    if (matchKind !== "exact_slot") return null;
    return {
      status: "approved",
      confidence: asset.hasAttachment ? "high" : "medium",
      provisionalReuse: false,
      provisional: false,
      founderReviewRequired: false,
      matchKind,
    };
  }

  const isApprovedWithAttachment = asset.approved && asset.hasAttachment;
  const isApprovedNoAttachment = asset.approved && !asset.hasAttachment;
  const strongCandidate = isStrongConfirmedCandidate(asset);

  let status = "needs_source_capture";
  let confidence = "low";
  let founderReviewRequired = false;
  let provisional = false;

  if (isApprovedWithAttachment) {
    status = "approved";
    confidence = "high";
  } else if (isApprovedNoAttachment) {
    status = "approved";
    confidence = "medium";
  } else if (strongCandidate) {
    status = "candidate_pending_founder_review";
    confidence = "medium";
    founderReviewRequired = true;
    provisional = true;
  } else {
    return null;
  }

  if (provisionalReuse) {
    status = "provisional_fallback";
    provisional = true;
    founderReviewRequired = true;
  } else if (matchKind !== "exact_slot") {
    provisional = true;
    founderReviewRequired = true;
  }

  return {
    status,
    confidence,
    provisionalReuse,
    provisional,
    founderReviewRequired,
    matchKind,
  };
}

function diversityTag(asset) {
  const hay = `${asset.assetName} ${asset.relatedPropertyName}`.toLowerCase();
  if (/resort|beach|all-inclusive|cove|island|nizuc/i.test(hay)) return "resort/leisure";
  if (/urban|city|lima|medellin|rumbao|cartagena/i.test(hay)) return "urban/boutique";
  if (/heritage|historic|adaptive|ermita|conversion/i.test(hay)) return "adaptive reuse/heritage";
  if (/interior|lobby|room|suite|restaurant/i.test(hay)) return "interior";
  return "exterior";
}

function sectionCoverage(brand, sectionKey) {
  if (sectionKey === "hero") {
    const rows = blocksForSlot(brand, "overview.hero");
    const withImage = rows.filter((r) => hasVal(r.imageUrl));
    return {
      requiredImageCount: 1,
      currentImageCount: withImage.length,
      missingCount: Math.max(0, 1 - withImage.length),
      currentSlotsWithImages: withImage.length ? ["overview.hero"] : [],
      currentSlotsMissingImages: withImage.length ? [] : ["overview.hero"],
      rows,
      belowMinimum: withImage.length < 1,
    };
  }

  if (sectionKey === "valueScenarios") {
    const slots = VISUAL_MINIMUMS.valueScenarios.slots;
    const withImage = slots.filter((sk) => blocksForSlot(brand, sk).some((r) => hasVal(r.imageUrl)));
    const missing = slots.filter((sk) => !withImage.includes(sk));
    return {
      requiredImageCount: 3,
      currentImageCount: withImage.length,
      missingCount: missing.length,
      currentSlotsWithImages: withImage,
      currentSlotsMissingImages: missing,
      rows: slots.flatMap((sk) => blocksForSlot(brand, sk)),
      belowMinimum: missing.length > 0,
    };
  }

  if (sectionKey === "gallery") {
    const slots = VISUAL_MINIMUMS.gallery.slots;
    const withImage = slots.filter((sk) => blocksForSlot(brand, sk).some((r) => hasVal(r.imageUrl)));
    const missing = slots.filter((sk) => !withImage.includes(sk));
    return {
      requiredImageCount: 6,
      currentImageCount: withImage.length,
      missingCount: missing.length,
      currentSlotsWithImages: withImage,
      currentSlotsMissingImages: missing,
      rows: slots.flatMap((sk) => blocksForSlot(brand, sk)),
      belowMinimum: missing.length > 0,
    };
  }

  if (sectionKey === "openings") {
    const rows = blocksForSlot(brand, "footprint.openings");
    const cardEval = rows.map(parseOpeningsCard);
    const renderableCards = cardEval.filter((c) => c.isRenderable);
    const anyVisible = rows.length > 0;
    const belowMinimum = anyVisible && renderableCards.length < rows.length;
    return {
      requiredImageCount: rows.length || 1,
      currentImageCount: renderableCards.filter((c) => c.hasImage).length,
      missingCount: anyVisible ? rows.length - renderableCards.length : 1,
      currentSlotsWithImages: renderableCards.filter((c) => c.hasImage).length ? ["footprint.openings"] : [],
      currentSlotsMissingImages: anyVisible ? (belowMinimum ? ["footprint.openings"] : []) : ["footprint.openings"],
      rows,
      belowMinimum: anyVisible ? belowMinimum : true,
      anyVisible,
      renderableCards: renderableCards.length,
      totalCards: rows.length,
    };
  }

  const rows = blocksForSlot(brand, "footprint.momentum");
  const parsed = rows.map(parseMomentumBlock);
  const datedSourced = parsed.filter((x) => x.dateLike && x.hasUrl).length;
  return {
    requiredImageCount: 0,
    currentImageCount: 0,
    missingCount: datedSourced > 0 ? 0 : 1,
    currentSlotsWithImages: [],
    currentSlotsMissingImages: datedSourced > 0 ? [] : ["footprint.momentum"],
    rows,
    belowMinimum: datedSourced === 0,
    datedSourceRows: datedSourced,
  };
}

function buildCandidateRow(slotKey, asset, cls) {
  const safeForFutureWriter =
    cls.status === "approved" && !cls.provisionalReuse && cls.matchKind === "exact_slot";
  return {
    slotKey,
    assetRecordId: asset.id,
    assetName: asset.assetName,
    sourceUrl: asset.sourceUrl,
    sourcePageUrl: asset.sourcePageUrl,
    propertyName: asset.relatedPropertyName || "—",
    countryRegion: asset.countryRegion || "—",
    usageStatus: asset.explorerUsePermission || asset.assetStatus || "—",
    reviewStatus: cls.status,
    confidence: cls.confidence,
    matchKind: cls.matchKind,
    assignmentLabel: cls.provisional ? PROVISIONAL_ASSIGNMENT_LABEL : null,
    safeForFutureWriter,
    downloadAttachmentNeeded: !asset.hasAttachment,
    mediaPromotionNeeded: true,
    diversity: diversityTag(asset),
    provisionalReuse: cls.provisionalReuse,
    provisional: cls.provisional,
    founderReviewRequired: cls.founderReviewRequired,
  };
}

function candidateTier(row, planMode) {
  const status = row.status || row.reviewStatus;
  const matchKind = row.matchKind;
  const provisionalReuse = Boolean(row.provisionalReuse);
  if (planMode === "strict") {
    return matchKind === "exact_slot" && status === "approved" ? 4 : 0;
  }
  const kindRank = {
    exact_slot: 40,
    approved_fallback_value_driver: 35,
    approved_fallback_gallery: 30,
    approved_fallback_hero: 25,
  };
  const kindScore = kindRank[matchKind] || 0;
  if (status === "approved" && !provisionalReuse) return 100 + kindScore;
  if (status === "candidate_pending_founder_review" && !provisionalReuse) return 60 + kindScore;
  if (provisionalReuse && status === "approved") return 20 + kindScore;
  if (provisionalReuse) return 10 + kindScore;
  return kindScore;
}

function gatherCandidatesForSlot(slotKey, assets, presentationUrls, usedAssetIds, planMode) {
  const candidates = [];
  for (const asset of assets) {
    const matchKind = slotMatchKind(asset, slotKey);
    if (!matchKind) continue;
    const cls = classifyCandidate(asset, { usedAssetIds, presentationUrls, planMode, matchKind });
    if (!cls) continue;
    candidates.push(buildCandidateRow(slotKey, asset, cls));
  }
  candidates.sort((a, b) => candidateTier(b, planMode) - candidateTier(a, planMode) || b.confidence.localeCompare(a.confidence));
  return candidates;
}

function tryPinnedStrictAssignment(slotKey, assets) {
  const pinnedId = PINNED_STRICT_ASSET_BY_SLOT[slotKey];
  if (!pinnedId) return null;
  const asset = assets.find((a) => a.id === pinnedId);
  if (!asset || !isEligibleStrict(asset)) return null;
  return buildCandidateRow(slotKey, asset, {
    status: "approved",
    confidence: asset.hasAttachment ? "high" : "medium",
    provisionalReuse: false,
    provisional: false,
    founderReviewRequired: false,
    matchKind: "exact_slot",
  });
}

function buildImagePlan({ planMode, missingImageSlots, assets, brand }) {
  const presentationUrls = collectPresentationImageUrls(brand);
  const usedAssetIds = new Set();
  const assignments = [];
  const unresolvedSlots = [];
  const backfillCandidatesBySlot = {};

  for (const slotKey of missingImageSlots) {
    const candidates = gatherCandidatesForSlot(slotKey, assets, presentationUrls, usedAssetIds, planMode);
    backfillCandidatesBySlot[slotKey] = candidates;

    let chosen = null;
    if (planMode === "strict") {
      chosen = tryPinnedStrictAssignment(slotKey, assets) || candidates[0] || null;
    } else {
      const pinned = tryPinnedStrictAssignment(slotKey, assets);
      chosen = pinned || candidates[0] || null;
    }

    if (chosen) {
      assignments.push(chosen);
      if (!chosen.provisionalReuse) usedAssetIds.add(chosen.assetRecordId);
    } else {
      unresolvedSlots.push(slotKey);
    }
  }

  return { assignments, unresolvedSlots, backfillCandidatesBySlot };
}

function inferSourceCaptureTask(slotKey, section) {
  if (slotKey === "footprint.openings") {
    return "Capture property-specific opening example with image, location/descriptor, teaser, and property/source URL.";
  }
  if (slotKey === "footprint.momentum") {
    return "Capture dated momentum rows with source-backed announcement URLs.";
  }
  return `Capture source-backed ${section} visual for ${slotKey} from Marriott/company-controlled property pages.`;
}

function coverageSummaryTable(coverageBySection) {
  return Object.entries(coverageBySection).map(([k, v]) => ({
    sectionKey: k,
    section: VISUAL_MINIMUMS[k].section,
    requiredImageCount: v.requiredImageCount,
    currentImageCount: v.currentImageCount,
    missingCount: v.missingCount,
    belowMinimum: v.belowMinimum,
    currentSlotsWithImages: v.currentSlotsWithImages,
    currentSlotsMissingImages: v.currentSlotsMissingImages,
  }));
}

export async function buildBrandExplorerVisualMinimumsBackfillPlannerReport(options = {}) {
  const brandId = normalizeBrandInput(options.brandIdOrName);
  const tribute = await fetchBrand(brandId);
  const curio = await fetchBrand(CURIO_BRAND_ID);
  if (!tribute) throw new Error("Unable to load Tribute brand.");

  const registryRaw = await listRegistryRecordsRaw(brandId);
  const assets = registryRaw.map((r) => {
    const base = normalizeRegistryAssetRecord(r);
    return {
      ...normalizeRegistryForPlanner(r),
      recommendedExplorerSlot: nz(base.recommendedExplorerSlot),
    };
  });

  const coverageBySection = {
    hero: sectionCoverage(tribute, "hero"),
    valueScenarios: sectionCoverage(tribute, "valueScenarios"),
    gallery: sectionCoverage(tribute, "gallery"),
    openings: sectionCoverage(tribute, "openings"),
    momentum: sectionCoverage(tribute, "momentum"),
  };

  const usedAssetIds = new Set();
  const slotAssignments = [];
  const backfillCandidatesBySlot = {};
  const sourceCaptureTasks = [];

  const missingImageSlots = [
    ...coverageBySection.hero.currentSlotsMissingImages,
    ...coverageBySection.valueScenarios.currentSlotsMissingImages,
    ...coverageBySection.gallery.currentSlotsMissingImages,
  ].filter((sk, i, arr) => arr.indexOf(sk) === i);

  const slotsToFill = [
    ...missingImageSlots,
    ...coverageBySection.openings.currentSlotsMissingImages,
    ...coverageBySection.momentum.currentSlotsMissingImages,
  ].filter((sk, i, arr) => arr.indexOf(sk) === i);

  const planAStrict = buildImagePlan({
    planMode: "strict",
    missingImageSlots,
    assets,
    brand: tribute,
  });

  const planBProvisional = buildImagePlan({
    planMode: "provisional",
    missingImageSlots,
    assets,
    brand: tribute,
  });

  for (const slotKey of slotsToFill) {
    if (slotKey === "footprint.momentum") {
      sourceCaptureTasks.push({
        section: "Recent Momentum",
        slotKey,
        task: inferSourceCaptureTask(slotKey, "Recent Momentum"),
        visualMinimumUnresolved: true,
      });
      continue;
    }
    if (slotKey === "footprint.openings") {
      sourceCaptureTasks.push({
        section: "Openings / Examples / Properties",
        slotKey,
        task: inferSourceCaptureTask(slotKey, "Openings / Examples / Properties"),
        visualMinimumUnresolved: true,
      });
      continue;
    }
    if (!planAStrict.backfillCandidatesBySlot[slotKey]) {
      planAStrict.backfillCandidatesBySlot[slotKey] = gatherCandidatesForSlot(
        slotKey,
        assets,
        collectPresentationImageUrls(tribute),
        usedAssetIds,
        "strict"
      );
    }
    if (!planBProvisional.backfillCandidatesBySlot[slotKey]) {
      planBProvisional.backfillCandidatesBySlot[slotKey] = gatherCandidatesForSlot(
        slotKey,
        assets,
        collectPresentationImageUrls(tribute),
        usedAssetIds,
        "provisional"
      );
    }
    if (planAStrict.unresolvedSlots.includes(slotKey)) {
      sourceCaptureTasks.push({
        section: "Image backfill",
        slotKey,
        task: inferSourceCaptureTask(slotKey, "Image"),
        visualMinimumUnresolved: true,
        strictPlanUnresolved: true,
      });
    }
  }

  Object.assign(backfillCandidatesBySlot, planBProvisional.backfillCandidatesBySlot);
  slotAssignments.push(...planAStrict.assignments);

  const sectionsBelowMinimum = coverageSummaryTable(coverageBySection)
    .filter((x) => x.belowMinimum)
    .map((x) => x.section);

  const suppressionRecommendations = [];
  if (coverageBySection.valueScenarios.belowMinimum) {
    suppressionRecommendations.push({
      section: "Where This Brand Creates the Most Value",
      recommendation:
        "Do not render blank scenario image placeholder for overview.scenario.3; use Plan B provisional asset or suppress card image until assigned.",
      slotKey: "overview.scenario.3",
    });
  }
  if (coverageBySection.gallery.belowMinimum) {
    suppressionRecommendations.push({
      section: "Image Gallery",
      recommendation: "Do not render blank materials.gallery.3 slot; assign approved asset or suppress until ready.",
      slotKey: "materials.gallery.3",
    });
  }
  if (coverageBySection.openings.belowMinimum) {
    suppressionRecommendations.push({
      section: "Openings / Examples / Properties",
      recommendation:
        "Suppress Openings / Examples / Properties until complete cards exist (image, title, location/descriptor, body/summary, source or property URL). Do not fill with random images.",
      slotKey: "footprint.openings",
    });
  }
  if (coverageBySection.momentum.belowMinimum) {
    suppressionRecommendations.push({
      section: "Recent Momentum",
      recommendation: "Suppress Recent Momentum when no dated source-backed activity rows exist.",
      slotKey: "footprint.momentum",
    });
  }

  const planAAssignments = planAStrict.assignments;
  const planBAssignments = planBProvisional.assignments;
  const approvedCandidates = planAAssignments.filter((x) => x.reviewStatus === "approved" && !x.provisional);
  const provisionalCandidates = planBAssignments.filter(
    (x) => x.provisional || x.reviewStatus === "provisional_fallback" || x.founderReviewRequired
  );
  const anyReuse = planBAssignments.some((x) => x.provisionalReuse);
  const anyProvisional = planBAssignments.some((x) => x.provisional);
  const anyFounderReview = planBAssignments.some((x) => x.founderReviewRequired);

  const assignmentFor = (plan, slotKey) => plan.assignments.find((x) => x.slotKey === slotKey) || null;

  const v25BStrictSlotsAssets = planAAssignments
    .filter((x) => x.safeForFutureWriter)
    .map((x) => ({
      slotKey: x.slotKey,
      assetRecordId: x.assetRecordId,
      assetName: x.assetName,
      reviewStatus: x.reviewStatus,
      confidence: x.confidence,
      plan: "A_strict",
    }));

  const v25BProvisionalSlotsAssets = planBAssignments.map((x) => ({
    slotKey: x.slotKey,
    assetRecordId: x.assetRecordId,
    assetName: x.assetName,
    reviewStatus: x.reviewStatus,
    confidence: x.confidence,
    provisional: x.provisional,
    provisionalReuse: x.provisionalReuse,
    founderReviewRequired: x.founderReviewRequired,
    assignmentLabel: x.assignmentLabel,
    plan: "B_provisional",
  }));

  const v25BSlotsAssetsIfApproved = v25BStrictSlotsAssets;

  const curioCoverage = {
    overviewScenarioImages: [1, 2, 3].filter((i) =>
      blocksForSlot(curio, `overview.scenario.${i}`).some((r) => hasVal(r.imageUrl))
    ).length,
    galleryImages: [1, 2, 3, 4, 5, 6].filter((i) =>
      blocksForSlot(curio, `materials.gallery.${i}`).some((r) => hasVal(r.imageUrl))
    ).length,
    openingsRows: blocksForSlot(curio, "footprint.openings").length,
  };

  return {
    plannerVersion: PLANNER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    imagesUntouched: true,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    brand: {
      recordId: brandId,
      name: nz(tribute.name) || "Tribute Portfolio",
    },
    v25PlannerExists: true,
    v25R2PlannerExists: true,
    filesRead: [
      "AGENTS.md",
      "reports/brand-explorer-visual-display-defect-audit.md",
      "reports/brand-explorer-screenshot-seeded-remediation-review-package.md",
      "reports/brand-explorer-visual-qa-verification.md",
      "reports/tribute-visual-asset-slot-review.md",
      "reports/brand-asset-human-review-readiness.md",
      "reports/brand-asset-review-decision-writer.md",
      "reports/brand-asset-download-attachment-writer.md",
      "reports/explorer-media-promotion-writer.md",
      "docs/brand-explorer-presentation-slots.md",
      "api/brand-library.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "public/js/brand-explorer-gold-detail.js",
      "live Tribute Brand Explorer Presentation rows",
      "Tribute Brand Asset Registry records",
      "completed reference brands rows/image counts",
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-visual-minimums-backfill-planner.js",
      "scripts/brand-explorer-visual-minimums-backfill-planner.mjs",
      `docs/data-intelligence/${DOC_MD_NAME}`,
      `reports/${REPORT_MD_NAME}`,
      `reports/${REPORT_JSON_NAME}`,
      "package.json",
    ],
    visualMinimumContract: VISUAL_MINIMUMS,
    currentCoverageBySection: coverageSummaryTable(coverageBySection),
    sectionsBelowMinimum,
    missingImageSlots,
    backfillCandidatesBySlot,
    recommendedAssignments: planAAssignments,
    planAStrict: {
      name: "strict_governance",
      description: "Only ideal approved assets with exact slot match; no reuse; no unapproved candidates.",
      assignments: planAAssignments,
      unresolvedSlots: planAStrict.unresolvedSlots,
      assignmentCount: planAAssignments.length,
    },
    planBProvisional: {
      name: "provisional_visual_minimum",
      description:
        "Fills required image slots with best fallback asset; allows approved fallback, confirmed candidates, and provisional reuse to avoid blank placeholders.",
      assignments: planBAssignments,
      unresolvedSlots: planBProvisional.unresolvedSlots,
      assignmentCount: planBAssignments.length,
      provisionalAssignmentLabel: PROVISIONAL_ASSIGNMENT_LABEL,
    },
    assignmentOverviewScenario3: {
      planA: assignmentFor(planAStrict, "overview.scenario.3"),
      planB: assignmentFor(planBProvisional, "overview.scenario.3"),
      strictUnresolved: planAStrict.unresolvedSlots.includes("overview.scenario.3"),
      provisionalUnresolved: planBProvisional.unresolvedSlots.includes("overview.scenario.3"),
    },
    assignmentMaterialsGallery3: {
      planA: assignmentFor(planAStrict, "materials.gallery.3"),
      planB: assignmentFor(planBProvisional, "materials.gallery.3"),
    },
    anyAssignmentUsesReuse: anyReuse,
    anyAssignmentIsProvisional: anyProvisional,
    anyAssignmentRequiresFounderVisualReview: anyFounderReview,
    approvedCandidates,
    provisionalOrFounderReviewNeeded: provisionalCandidates,
    suppressUntilReadySections: suppressionRecommendations,
    sourceCaptureTasks,
    registryAssetSummary: {
      scanned: assets.length,
      approved: assets.filter((a) => a.approved).length,
      candidatesNotRejected: assets.filter((a) => !a.approved && !a.rejected && !a.superseded).length,
      rejectedOrSuperseded: assets.filter((a) => a.rejected || a.superseded).length,
    },
    referenceBrandCoverage: {
      curio: curioCoverage,
    },
    v25BWriterShouldBeBuilt: v25BStrictSlotsAssets.length > 0 || v25BProvisionalSlotsAssets.length > 0,
    v25BWriterSafeToBuildStrict: v25BStrictSlotsAssets.length > 0,
    v25BWriterSafeToBuildProvisional: v25BProvisionalSlotsAssets.length > 0,
    v25BSlotsAssetsToWriteIfApproved: v25BSlotsAssetsIfApproved,
    v25BStrictSlotsAssets,
    v25BProvisionalSlotsAssets,
    exactNextCommand:
      "npm run brand-explorer-visual-minimums-backfill-planner -- --brand tribute-portfolio --dry-run",
  };
}

export function buildBrandExplorerVisualMinimumsBackfillPlannerMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Visual Minimums & Image Backfill Planner v25-R2");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`);
  lines.push("");
  lines.push("## Visual Coverage By Section");
  for (const s of report.currentCoverageBySection) {
    lines.push(
      `- **${s.section}**: required=${s.requiredImageCount}, current=${s.currentImageCount}, missing=${s.missingCount}, belowMinimum=${s.belowMinimum ? "yes" : "no"}`
    );
  }
  lines.push("");
  lines.push("## Missing Image Slots");
  for (const sk of report.missingImageSlots) lines.push(`- ${sk}`);
  if (!report.missingImageSlots.length) lines.push("- none");
  lines.push("");
  lines.push("## Plan A — Strict Governance");
  for (const a of report.planAStrict?.assignments || []) {
    lines.push(
      `- \`${a.slotKey}\` ← ${a.assetName} (\`${a.assetRecordId}\`) · ${a.reviewStatus} · ${a.matchKind}`
    );
  }
  if (!report.planAStrict?.assignments?.length) lines.push("- none");
  if (report.planAStrict?.unresolvedSlots?.length) {
    lines.push(`- Unresolved: ${report.planAStrict.unresolvedSlots.join(", ")}`);
  }
  lines.push("");
  lines.push("## Plan B — Provisional Visual Minimum");
  for (const a of report.planBProvisional?.assignments || []) {
    lines.push(
      `- \`${a.slotKey}\` ← ${a.assetName} (\`${a.assetRecordId}\`) · ${a.reviewStatus} · ${a.matchKind}${a.provisionalReuse ? " · reuse" : ""}`
    );
    if (a.assignmentLabel) lines.push(`  - ${a.assignmentLabel}`);
  }
  if (!report.planBProvisional?.assignments?.length) lines.push("- none");
  if (report.planBProvisional?.unresolvedSlots?.length) {
    lines.push(`- Unresolved: ${report.planBProvisional.unresolvedSlots.join(", ")}`);
  }
  lines.push("");
  lines.push("## Key Slot Assignments");
  const s3 = report.assignmentOverviewScenario3;
  lines.push(`- \`overview.scenario.3\` Plan A: ${s3?.planA ? `${s3.planA.assetName} (\`${s3.planA.assetRecordId}\`)` : "unresolved"}`);
  lines.push(`- \`overview.scenario.3\` Plan B: ${s3?.planB ? `${s3.planB.assetName} (\`${s3.planB.assetRecordId}\`)` : "unresolved"}`);
  const g3 = report.assignmentMaterialsGallery3;
  lines.push(`- \`materials.gallery.3\` Plan A: ${g3?.planA ? `${g3.planA.assetName} (\`${g3.planA.assetRecordId}\`)` : "unresolved"}`);
  lines.push(`- \`materials.gallery.3\` Plan B: ${g3?.planB ? `${g3.planB.assetName} (\`${g3.planB.assetRecordId}\`)` : "unresolved"}`);
  lines.push("");
  lines.push("## Suppress Until Ready");
  for (const s of report.suppressUntilReadySections) {
    lines.push(`- **${s.section}** (\`${s.slotKey}\`): ${s.recommendation}`);
  }
  if (!report.suppressUntilReadySections.length) lines.push("- none");
  lines.push("");
  lines.push("## Source Capture Tasks");
  for (const t of report.sourceCaptureTasks) lines.push(`- \`${t.slotKey}\`: ${t.task}`);
  if (!report.sourceCaptureTasks.length) lines.push("- none");
  lines.push("");
  lines.push("## v25B Strict Write Set");
  for (const w of report.v25BStrictSlotsAssets || []) {
    lines.push(`- \`${w.slotKey}\` → \`${w.assetRecordId}\` (${w.assetName})`);
  }
  if (!report.v25BStrictSlotsAssets?.length) lines.push("- none");
  lines.push("");
  lines.push("## v25B Provisional Write Set");
  for (const w of report.v25BProvisionalSlotsAssets || []) {
    lines.push(`- \`${w.slotKey}\` → \`${w.assetRecordId}\` (${w.assetName}) · provisional=${w.provisional ? "yes" : "no"}`);
  }
  if (!report.v25BProvisionalSlotsAssets?.length) lines.push("- none");
  lines.push("");
  lines.push("## Next Command");
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  return lines.join("\n");
}
