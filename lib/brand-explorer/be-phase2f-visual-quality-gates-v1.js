/**
 * Brand Explorer Phase 2F — visual / presentation quality gates.
 *
 * Founder verdict: Phase 2E A/B/C = 1.0 is NOT production-quality evidence.
 * These gates encode screenshot failures: title misuse, missing imagery,
 * near-duplicate economics, empty lists, shallow scenarios, cardinality.
 *
 * Contract: BE_PHASE2F_VISUAL_QUALITY_GATES_V1
 */
import {
  BE_CONTENT_PRESENTATION_CONTRACT_V1,
  loadContentPresentationContract,
  ruleForSlot,
  semanticTypeDef,
  titleLooksLikeParagraph,
  isLikelyLogoUrl,
  isHttpImageUrl,
  isPlaceholderImage,
  textSimilarity,
  collectionCardinality,
  nz,
  wordCount,
} from "./be-phase2f-content-presentation-contract-v1.js";
import {
  isBrandLogoGalleryAsset,
  isGenericGalleryCaption,
  GALLERY_FAIL,
} from "./be-permanent-six-image-gallery-standard-v1.js";

export const BE_PHASE2F_VISUAL_QUALITY_GATES_V1 = "BE_PHASE2F_VISUAL_QUALITY_GATES_V1";

export const VISUAL_FAIL = Object.freeze({
  PARAGRAPH_IN_TITLE: "paragraph_in_title",
  EMPTY_LIST_ITEM: "empty_list_item",
  MISSING_REQUIRED_IMAGE: "missing_required_image",
  IMAGE_PLACEHOLDER: "image_placeholder",
  LOGO_AS_GALLERY: "logo_as_gallery_photo",
  GENERIC_GALLERY_CAPTION: "generic_gallery_caption",
  NEAR_DUPLICATE: "near_duplicate_cross_metric",
  INSUFFICIENT_DEPTH: "insufficient_scenario_depth",
  EXCESSIVE_LENGTH: "excessive_card_content",
  INCORRECT_CARDINALITY: "incorrect_cardinality",
  GENERIC_REPEATED: "generic_repeated_diligence",
  UNVERIFIED_PROPERTY: "unverified_property_example",
  WRONG_SEMANTIC_TYPE: "wrong_semantic_type",
});

function blocksBySlot(blocks) {
  const map = new Map();
  for (const b of blocks || []) {
    const k = nz(b.slotKey || b.slot);
    if (!k) continue;
    if (!map.has(k)) map.set(k, []);
    map.get(k).push(b);
  }
  return map;
}

function evaluateBlock(block, peers) {
  const slotKey = nz(block.slotKey || block.slot);
  const title = nz(block.title);
  const body = nz(block.body);
  const imageUrl = nz(block.imageUrl || block.image_url);
  const rule = ruleForSlot(slotKey);
  const typeId = rule?.semanticType || "passthrough";
  const type = semanticTypeDef(typeId) || {};
  const failures = [];

  if (titleLooksLikeParagraph(title) || (type.forbidParagraph && wordCount(title) > (type.maxWords || 12))) {
    failures.push({ code: VISUAL_FAIL.PARAGRAPH_IN_TITLE, detail: `title words=${wordCount(title)} chars=${title.length}` });
  }
  if (/^[a-z0-9_.]+$/.test(title) && title.includes(".")) {
    failures.push({ code: VISUAL_FAIL.WRONG_SEMANTIC_TYPE, detail: "title looks like evidence/slot key" });
  }

  const titleMax = type.titleMaxWords || (type.forbidParagraph ? type.maxWords : null);
  if (titleMax && wordCount(title) > titleMax) {
    failures.push({ code: VISUAL_FAIL.PARAGRAPH_IN_TITLE, detail: `title exceeds ${titleMax} words` });
  }

  const minW = type.bodyMinWords || type.minWords || null;
  const maxW = type.bodyMaxWords || type.maxWords || null;
  const bw = wordCount(body);
  // Depth floors apply only when the contract defines them for this semantic type
  if (minW && bw < minW) {
    failures.push({
      code: VISUAL_FAIL.INSUFFICIENT_DEPTH,
      detail: `${typeId} body words=${bw} min=${minW}`,
    });
  }
  if (maxW && bw > maxW * 1.5) {
    failures.push({ code: VISUAL_FAIL.EXCESSIVE_LENGTH, detail: `body words=${bw} max≈${maxW}` });
  }

  const imageRequired = type.imageRequired === true || rule?.semanticType === "gallery_image";
  if (imageRequired) {
    if (!imageUrl || isPlaceholderImage(imageUrl, title, body)) {
      failures.push({ code: VISUAL_FAIL.MISSING_REQUIRED_IMAGE, detail: "required image missing or placeholder" });
    } else if (!isHttpImageUrl(imageUrl)) {
      failures.push({ code: VISUAL_FAIL.MISSING_REQUIRED_IMAGE, detail: "image URL not HTTP asset" });
    }
  }
  if (
    /^materials\.gallery\./.test(slotKey) &&
    imageUrl &&
    (isLikelyLogoUrl(imageUrl) || isBrandLogoGalleryAsset(imageUrl, block.galleryMeta || {}))
  ) {
    failures.push({
      code: VISUAL_FAIL.LOGO_AS_GALLERY,
      detail: "logo/wordmark/social stub used as gallery photo",
      galleryFail: GALLERY_FAIL.LOGO,
    });
  }
  if (/^materials\.gallery\./.test(slotKey) && isGenericGalleryCaption(title, body)) {
    failures.push({
      code: VISUAL_FAIL.GENERIC_GALLERY_CAPTION,
      detail: `generic gallery caption rejected: ${JSON.stringify(title)}`,
      galleryFail: GALLERY_FAIL.GENERIC_CAPTION,
    });
  }
  if (/^materials\.gallery\./.test(slotKey) && imageUrl) {
    const dupPeers = (peers || []).filter(
      (p) => p !== block && /^materials\.gallery\./.test(nz(p.slotKey)) && nz(p.imageUrl) === imageUrl,
    );
    if (dupPeers.length >= 2) {
      failures.push({
        code: VISUAL_FAIL.MISSING_REQUIRED_IMAGE,
        detail: `same image URL reused across ${dupPeers.length + 1} gallery slots`,
      });
    }
  }

  if (type.forbidEmptyItems || typeId === "list_collection") {
    const lines = body.split(/\n+/).map((l) => l.trim()).filter(Boolean);
    if (lines.some((l) => wordCount(l) < 2 && l.length < 3)) {
      failures.push({ code: VISUAL_FAIL.EMPTY_LIST_ITEM, detail: "empty or near-empty list line" });
    }
    if (!body && !title) {
      failures.push({ code: VISUAL_FAIL.EMPTY_LIST_ITEM, detail: "empty collection item" });
    }
  }

  if (type.forbidCrossMetricNearDuplicate || type.mustDifferentiateAcrossPeers || rule?.mustDifferentiateAcrossPeers) {
    const threshold = type.nearDuplicateThreshold || 0.72;
    for (const peer of peers || []) {
      if (peer === block) continue;
      const peerBody = nz(peer.body);
      if (wordCount(body) < 12 || wordCount(peerBody) < 12) continue;
      const sim = textSimilarity(body, peerBody);
      if (sim >= threshold) {
        failures.push({
          code: VISUAL_FAIL.NEAR_DUPLICATE,
          detail: `similarity=${sim.toFixed(2)} vs ${nz(peer.slotKey)}`,
        });
        break;
      }
    }
  }

  // Generic diligence stamp: same title "Economics diligence" + FDD total investment echo
  if (/^economics\./.test(slotKey) && /Economics diligence/i.test(title) && /total investment/i.test(body)) {
    const peerStamp = (peers || []).filter(
      (p) => p !== block && /Economics diligence/i.test(nz(p.title)) && textSimilarity(body, nz(p.body)) > 0.65,
    );
    if (peerStamp.length >= 2) {
      failures.push({ code: VISUAL_FAIL.GENERIC_REPEATED, detail: `economics diligence stamp shared with ${peerStamp.length} peers` });
    }
  }

  if (/^footprint\.openings$|^materials\.caseStudy$/.test(slotKey)) {
    if (/diligence|TBD|do not invent|named openings require/i.test(body) && !/[A-Z][a-z]+ .+ (Hotel|Resort|Collection)/.test(title + " " + body)) {
      failures.push({ code: VISUAL_FAIL.UNVERIFIED_PROPERTY, detail: "no verified property example" });
    }
  }

  return {
    slotKey,
    semanticType: typeId,
    title,
    bodyWords: bw,
    hasImage: Boolean(imageUrl),
    pass: failures.length === 0,
    failures,
  };
}

/**
 * Evaluate atelier blocks (candidate overlay shape).
 */
export function evaluateVisualQuality(blocks, { brandName = "" } = {}) {
  const contract = loadContentPresentationContract();
  const list = Array.isArray(blocks) ? blocks : [];
  const bySlot = blocksBySlot(list);
  const fieldResults = [];
  const failureRegister = [];

  // Peer groups for near-duplicate checks
  const econPeers = list.filter((b) => /^economics\./.test(nz(b.slotKey)));
  const scenarioPeers = list.filter((b) => /scenario\.\d+$/.test(nz(b.slotKey)));
  const commercialLeverPeers = list.filter((b) => /^commercial\.(lever|kpi)\./.test(nz(b.slotKey)));
  const galleryPeers = list.filter((b) => /^materials\.gallery\./.test(nz(b.slotKey)));

  for (const block of list) {
    const key = nz(block.slotKey);
    let peers = [];
    if (/^economics\./.test(key)) peers = econPeers;
    else if (/scenario\.\d+$/.test(key)) peers = scenarioPeers.filter((p) => {
      const a = key.startsWith("valueOwners") ? "valueOwners" : "overview";
      return nz(p.slotKey).startsWith(a);
    });
    else if (/^commercial\.(lever|kpi)\./.test(key)) peers = commercialLeverPeers;
    else if (/^materials\.gallery\./.test(key)) peers = galleryPeers;

    const result = evaluateBlock(block, peers);
    fieldResults.push(result);
    for (const f of result.failures) {
      failureRegister.push({
        slotKey: key,
        code: f.code,
        detail: f.detail,
        brandName,
        founderApprovalBlocked: (contract.gates?.founderApprovalBlockedBy || []).includes(f.code),
      });
    }
  }

  // Cardinality — hard-block only on mandatory visual families (scenarios/gallery/value scenarios).
  // Broader baseline collection mins are recorded as non-blocking gaps until source-backed expansion exists.
  const cardinalityFailures = [];
  const hardCardinality = [
    { pattern: /^overview\.scenario\.\d+$/, label: "overview.scenario.*", min: 3 },
    { pattern: /^valueOwners\.scenario\.\d+$/, label: "valueOwners.scenario.*", min: 4 },
    { pattern: /^materials\.gallery\.\d+$/, label: "materials.gallery.*", min: 6 },
  ];
  for (const hc of hardCardinality) {
    const n = [...bySlot.keys()].filter((k) => hc.pattern.test(k)).length;
    if (n < hc.min) {
      const row = {
        slotKey: hc.label,
        code: VISUAL_FAIL.INCORRECT_CARDINALITY,
        detail: `items=${n} min=${hc.min}`,
        brandName,
        founderApprovalBlocked: true,
      };
      cardinalityFailures.push(row);
      failureRegister.push(row);
    }
  }
  const cardMap = contract.collectionCardinalityFromBaseline || {};
  for (const [slot, req] of Object.entries(cardMap)) {
    const n = (bySlot.get(slot) || []).length;
    if (n > 0 && n < (req.min || 0)) {
      cardinalityFailures.push({
        slotKey: slot,
        code: VISUAL_FAIL.INCORRECT_CARDINALITY,
        detail: `items=${n} baselineMin=${req.min} (non-blocking until source-backed expansion)`,
        brandName,
        founderApprovalBlocked: false,
      });
    }
  }

  const blocked = failureRegister.filter((f) => f.founderApprovalBlocked);
  const fieldsTested = fieldResults.length;
  const fieldsPassed = fieldResults.filter((r) => r.pass).length;
  const visualPassRate = fieldsTested ? fieldsPassed / fieldsTested : 0;

  // Structural completeness retained but labeled non-founder
  const populated = list.filter((b) => nz(b.body) || nz(b.title) || nz(b.imageUrl)).length;
  const structuralPopulation = list.length ? populated / list.length : 0;

  return {
    version: BE_PHASE2F_VISUAL_QUALITY_GATES_V1,
    contractVersion: BE_CONTENT_PRESENTATION_CONTRACT_V1,
    brandName,
    metrics: {
      structuralPopulation: Number(structuralPopulation.toFixed(4)),
      visualQualityPassRate: Number(visualPassRate.toFixed(4)),
      qualityAdjustedCompleteness: Number((structuralPopulation * visualPassRate).toFixed(4)),
      fieldsTested,
      fieldsPassed,
      fieldsFailed: fieldsTested - fieldsPassed,
      failureCount: failureRegister.length,
      founderApprovalBlocked: blocked.length > 0,
      founderApprovalStatus: blocked.length > 0 ? "NOT_FOUNDER_APPROVED" : "PENDING_FOUNDER_REVIEW",
      note: "Automated visual gates ≠ founder approval. A/B/C structural scores alone are insufficient.",
    },
    failureRegister,
    cardinalityFailures,
    fieldResults,
    target: 0.99,
    meetsAutomatedVisualTarget: visualPassRate >= 0.99 && blocked.length === 0,
  };
}

export function evaluateCandidateOverlay(overlay, opts = {}) {
  const blocks = overlay?.brandExplorer?.blocks || overlay?.blocks || [];
  return evaluateVisualQuality(blocks, {
    brandName: opts.brandName || overlay?.brandName || overlay?.name || "",
  });
}
