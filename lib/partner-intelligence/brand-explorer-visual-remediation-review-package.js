/**
 * Brand Explorer Visual Remediation Review Package v24A.
 *
 * Read-only remediation plan from v24 visual display defect audit.
 * No Airtable writes, no writer, no images, no Sort Order changes.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { TRIBUTE_RECORD_ID, BRAND_NAME } from "./tribute-portfolio-brand-package.js";
import { loadApprovedTributeSources } from "./tribute-portfolio-targeted-extract.js";
import { listPartnerFacts } from "./airtable-facts.js";
import { buildBrandExplorerVisualQaVerificationReport } from "./brand-explorer-visual-qa-verification.js";
import { listRegistryRecordsRaw } from "./brand-explorer-visual-slot-requirements.js";
import { MAP_BRAND_ASSET } from "./brand-asset-registry-workflow.js";
import { isFormallyApprovedRecord } from "./brand-asset-review-decision-writer.js";

export const PACKAGE_VERSION = "24A";
export const REPORT_JSON_NAME = "brand-explorer-visual-remediation-review-package.json";
export const REPORT_MD_NAME = "brand-explorer-visual-remediation-review-package.md";
export const DOC_MD_NAME = "brand-explorer-visual-remediation-review-package-v24A.md";

const CURIO_BRAND_ID = "receQkxgjlezsc1xg";
const DEFAULT_BRAND_ID = TRIBUTE_RECORD_ID;

const COPY_LABEL =
  "AI-drafted / pending founder review · Not company-validated · Not Marriott-validated";

const REMEDIATION_BUCKET = {
  COPY_CLEANUP_SAFE: "copy_cleanup_safe",
  MEDIA_ASSET_REQUIRED: "media_asset_required",
  SOURCE_EVIDENCE_REQUIRED: "source_evidence_required",
  SORT_ORDER_REQUIRED: "sort_order_required",
  FRONTEND_MAPPING_REQUIRED: "frontend_mapping_required",
  REMAIN_BLANK_OR_SUPPRESSED: "should_remain_blank_or_suppressed",
};

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const INPUT_PATHS = {
  v24Defect: "reports/brand-explorer-visual-display-defect-audit.json",
  sortOrder: "reports/brand-explorer-presentation-sort-order-audit.json",
  evidenceReview: "reports/brand-explorer-evidence-fact-review-package.json",
  visualQa: "reports/brand-explorer-visual-qa-verification.json",
  targetedExtract: "reports/tribute-portfolio-targeted-extract.json",
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

function readJson(relPath) {
  const abs = path.join(ROOT, relPath);
  if (!fs.existsSync(abs)) return null;
  try {
    return JSON.parse(fs.readFileSync(abs, "utf8"));
  } catch {
    return null;
  }
}

function normalizeBrandInput(raw) {
  const normalized = nz(raw).toLowerCase();
  if (!normalized || normalized === "tribute-portfolio" || normalized === "tribute portfolio") {
    return DEFAULT_BRAND_ID;
  }
  return nz(raw);
}

async function fetchBrandApiShape(brandIdOrName) {
  const req = { query: { brandId: brandIdOrName, refresh: "1" }, headers: {} };
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.payload = payload;
      return this;
    },
  };
  await getBrandLibraryBrandById(req, res);
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

async function fetchAllFacts(recordId) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ brandId: recordId, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all;
}

function mapRegistryToSlot(rawRecord) {
  const f = rawRecord.fields || {};
  const slot = nz(f[MAP_BRAND_ASSET.recommendedExplorerSlot]);
  if (slot === "Brand Setup — Explorer Hero") return "overview.hero";
  if (/^materials\.gallery\.[1-6]$/.test(slot)) return slot;
  if (/^overview\.scenario\.[1-3]$/.test(slot)) return slot;
  return "";
}

async function resolveMediaAssets(recordId) {
  const rawRegistry = await listRegistryRecordsRaw(recordId);
  const approved = rawRegistry.filter((r) =>
    isFormallyApprovedRecord({
      assetStatus: nz(r.fields?.[MAP_BRAND_ASSET.assetStatus]),
      explorerUsePermission: nz(r.fields?.[MAP_BRAND_ASSET.explorerUsePermission]),
      usageReviewStatus: nz(r.fields?.[MAP_BRAND_ASSET.usageReviewStatus]),
      reviewNotes: nz(r.fields?.[MAP_BRAND_ASSET.reviewNotes]),
    })
  );
  const bySlot = new Map();
  for (const rec of approved) {
    const slot = mapRegistryToSlot(rec);
    if (!slot) continue;
    if (!bySlot.has(slot)) bySlot.set(slot, []);
    bySlot.get(slot).push({
      recordId: rec.id,
      assetName: nz(rec.fields?.[MAP_BRAND_ASSET.assetName]),
      sourceUrl: nz(rec.fields?.[MAP_BRAND_ASSET.sourceUrl]),
    });
  }
  return { approvedCount: approved.length, bySlot };
}

function copyProposal(body, sourceBasis) {
  return {
    proposedTitle: "",
    proposedBody: body,
    copyLabel: COPY_LABEL,
    sourceBasis,
    reviewStatus: "pending_founder_review",
  };
}

function buildRemediationPlans(v24Report, sortOrderReport, evidenceReport, visualQaReport, brand, mediaAssets) {
  const vm = v24Report?.tributeVisibleModel?.sections || {};
  const defects = v24Report?.defects || [];
  const defectBySlot = new Map();
  for (const d of defects) defectBySlot.set(d.slotKey, d);

  const standardsFact = (evidenceReport?.factReviewRows || []).find(
    (f) => f.fieldKey === "be.standards.qualityAssuranceTheme"
  );
  const fddVintageFact = (evidenceReport?.factReviewRows || []).find(
    (f) => f.fieldKey === "be.meta.fddDocumentVintage"
  );

  const scenario3Assets = mediaAssets.bySlot.get("overview.scenario.3") || [];
  const gallery3Assets = mediaAssets.bySlot.get("materials.gallery.3") || [];

  const plans = [];

  // overview.scenario.3 image
  plans.push({
    section: "Where This Brand Creates the Most Value",
    slotKey: "overview.scenario.3",
    displayPath: "overview.scenario.3.imageUrl",
    currentTributeValue: vm.valueScenarios?.cards?.[2]?.body || "",
    referencePattern: "Curio scenario.3 has promoted scenario image",
    defectType: "missing_card_image",
    severity: "high",
    remediationBucket: scenario3Assets.length
      ? REMEDIATION_BUCKET.MEDIA_ASSET_REQUIRED
      : REMEDIATION_BUCKET.REMAIN_BLANK_OR_SUPPRESSED,
    proposedFix: scenario3Assets.length
      ? "Promote approved registry asset to presentation row Image attachment after founder media sign-off."
      : "No approved registry asset for overview.scenario.3 — keep image blank or suppress empty visual placeholder until heritage/conversion asset is approved in Brand Asset Registry.",
    proposedTitle: vm.valueScenarios?.cards?.[2]?.title || "Adaptive Reuse & Heritage Repositioning",
    proposedBody: null,
    sourceBasis: scenario3Assets.length
      ? `Approved registry: ${scenario3Assets.map((a) => a.assetName).join("; ")}`
      : visualQaReport?.remainingGapToFullVisualParity?.find((g) => /scenario\.3/i.test(g)) ||
        "Visual QA: intentionally unpopulated pending approved candidate",
    reviewStatus: scenario3Assets.length ? "pending_media_promotion" : "suppress_until_asset_approved",
    safeForFutureWriter: false,
    needsMedia: true,
    needsSourceEvidence: false,
    needsSortOrderCorrection: false,
    suppressUntilEvidence: !scenario3Assets.length,
    scenario3ImageCanBeSafelyFixedNow: scenario3Assets.length > 0,
  });

  // materials.gallery.3
  plans.push({
    section: "Brand Materials",
    slotKey: "materials.gallery.3",
    displayPath: "materials.gallery.3.imageUrl",
    currentTributeValue: "No image attachment",
    referencePattern: "Curio gallery slots populated where assets approved",
    defectType: "missing_card_image",
    severity: "medium",
    remediationBucket:
      gallery3Assets.length > 0
        ? REMEDIATION_BUCKET.MEDIA_ASSET_REQUIRED
        : REMEDIATION_BUCKET.REMAIN_BLANK_OR_SUPPRESSED,
    proposedFix:
      gallery3Assets.length > 0
        ? "Attach approved gallery asset after media review."
        : "Keep materials.gallery.3 blank until approved candidate exists — do not invent image.",
    proposedTitle: "",
    proposedBody: null,
    sourceBasis: gallery3Assets.length
      ? gallery3Assets.map((a) => a.assetName).join("; ")
      : "Visual QA: gallery.3 intentionally unpopulated",
    reviewStatus: gallery3Assets.length ? "pending_media_promotion" : "remain_blank",
    safeForFutureWriter: false,
    needsMedia: true,
    needsSourceEvidence: false,
    needsSortOrderCorrection: false,
    suppressUntilEvidence: !gallery3Assets.length,
  });

  // overview.why_value blank bullet 5
  const why5 = copyProposal(
    "Operator fit: owners and operators who can sustain design-forward full-service operations with collection QA—not limited-service reflag economics.",
    "Tribute consumer brand positioning + existing overview.why_value bullets 1–4 tone (no new KPIs)"
  );
  plans.push({
    section: "Why Value Is Strongest in These Scenarios",
    slotKey: "overview.why_value",
    displayPath: "overview.why_value.bullets[5]",
    currentTributeValue: `${vm.whyValueStrongest?.filledCount || 0}/5 bullets (bullet 5 empty)`,
    referencePattern: "Curio overview.why_value has 5 substantive line-broken bullets",
    defectType: "empty_bullet",
    severity: "medium",
    remediationBucket: REMEDIATION_BUCKET.COPY_CLEANUP_SAFE,
    proposedFix: "Append 5th line to overview.why_value Body (newline-separated) OR trim UI pad to 4 bullets in future frontend pass.",
    ...why5,
    safeForFutureWriter: true,
    needsMedia: false,
    needsSourceEvidence: false,
    needsSortOrderCorrection: false,
    suppressUntilEvidence: false,
  });

  // valueOwners.watchouts blank bullet 5
  const watch5 = copyProposal(
    "Collection standards and QA rhythm require sustained operating investment through conversion and hold—not a one-time affiliation event.",
    "Existing watchout bullets 1–4 + standards/QA theme (paraphrase only; no FDD legal text)"
  );
  plans.push({
    section: "Key Watchouts",
    slotKey: "valueOwners.watchouts",
    displayPath: "valueOwners.watchouts.bullets[5]",
    currentTributeValue: `${vm.keyWatchouts?.filledCount || 0}/5 bullets`,
    referencePattern: "Curio valueOwners.watchouts has 5 distinct considerations",
    defectType: "empty_bullet",
    severity: "high",
    remediationBucket: REMEDIATION_BUCKET.COPY_CLEANUP_SAFE,
    proposedFix: "Add 5th watchout line to valueOwners.watchouts Body.",
    ...watch5,
    safeForFutureWriter: true,
    needsMedia: false,
    needsSourceEvidence: false,
    needsSortOrderCorrection: false,
    suppressUntilEvidence: false,
  });

  // differentiators identity bullet 4
  const id4 = copyProposal(
    "Soft-collection positioning: independent hotel character with Marriott systems—not rigid chain retail.",
    "Tribute consumer site independent-collection statement (paraphrase)"
  );
  plans.push({
    section: "Key Differentiators",
    slotKey: "overview.differentiators.identity",
    displayPath: "overview.differentiators.identity.bullets[4]",
    currentTributeValue: "3/4 bullets",
    referencePattern: "Curio identity differentiators: 4 line-broken bullets",
    defectType: "empty_bullet",
    severity: "medium",
    remediationBucket: REMEDIATION_BUCKET.COPY_CLEANUP_SAFE,
    proposedFix: "Add 4th identity bullet to overview.differentiators.identity Body.",
    ...id4,
    safeForFutureWriter: true,
    needsMedia: false,
    needsSourceEvidence: false,
    needsSortOrderCorrection: false,
    suppressUntilEvidence: false,
  });

  // differentiators commercial bullet 4
  const com4 = copyProposal(
    "Conversion-led collection path—confirm development milestones, area of protection, and brand approval steps with Marriott development for your market.",
    "Tribute development_model presentation slot tone (no fee/KPI numerics)"
  );
  plans.push({
    section: "Key Differentiators",
    slotKey: "overview.differentiators.commercial",
    displayPath: "overview.differentiators.commercial.bullets[4]",
    currentTributeValue: "3/4 bullets",
    referencePattern: "Curio commercial differentiators: 4 bullets incl. distribution + conversion",
    defectType: "empty_bullet",
    severity: "medium",
    remediationBucket: REMEDIATION_BUCKET.COPY_CLEANUP_SAFE,
    proposedFix: "Add 4th commercial bullet to overview.differentiators.commercial Body.",
    ...com4,
    safeForFutureWriter: true,
    needsMedia: false,
    needsSourceEvidence: false,
    needsSortOrderCorrection: false,
    suppressUntilEvidence: false,
  });

  // valueOwners.scenario.1-4 title-only
  const ownerScenarioBodies = [
    {
      slotKey: "valueOwners.scenario.1",
      title: "Independent Reflag",
      body: "Independent and boutique full-service hotels that already have local story and design point of view—Tribute adds Bonvoy participation and Marriott commercial systems while preserving property individuality.",
      source: "overview.scenario.1 body tone + consumer independent-collection statement",
    },
    {
      slotKey: "valueOwners.scenario.2",
      title: "Tired Upscale Asset",
      body: "Upscale or upper-upscale assets needing repositioning where design narrative, F&B complexity, and service investment can be aligned to collection standards—confirm PIP scope and ramp assumptions before underwriting.",
      source: "overview.scenario.2 body + watchout PIP theme",
    },
    {
      slotKey: "valueOwners.scenario.3",
      title: "Markets With Strong Brand Presence",
      body: "Markets where Marriott distribution and Bonvoy member demand can complement a distinctive local asset—best when comp-set ADR supports full-service operating complexity, not select-service economics.",
      source: "Existing Tribute scenario copy patterns (no market KPIs)",
    },
    {
      slotKey: "valueOwners.scenario.4",
      title: "Third-Party Operator–Led",
      body: "Third-party management is common—fits when the operator can execute collection design compliance, service standards, and Marriott systems cutover while the sponsor underwrites conversion and hold-period economics.",
      source: "Curio valueOwners.scenarios pattern paraphrased for Tribute (no Hilton-specific claims)",
    },
  ];

  for (const sc of ownerScenarioBodies) {
    const cp = copyProposal(sc.body, sc.source);
    plans.push({
      section: "Value Creation Scenarios",
      slotKey: sc.slotKey,
      displayPath: `${sc.slotKey}.body`,
      currentTributeValue: sc.title,
      referencePattern: "Curio uses valueOwners.scenarios paragraph blocks (Tribute uses per-card slots)",
      defectType: "title_only_card",
      severity: "high",
      remediationBucket: REMEDIATION_BUCKET.SOURCE_EVIDENCE_REQUIRED,
      proposedFix: `Populate ${sc.slotKey} Body with owner-education copy after founder review.`,
      proposedTitle: sc.title,
      proposedBody: cp.proposedBody,
      copyLabel: cp.copyLabel,
      sourceBasis: cp.sourceBasis,
      reviewStatus: cp.reviewStatus,
      safeForFutureWriter: true,
      needsMedia: false,
      needsSourceEvidence: true,
      needsSortOrderCorrection: false,
      suppressUntilEvidence: false,
    });
  }

  // standards.requirement table
  plans.push({
    section: "Standard Detail, Where Available",
    slotKey: "standards.requirement",
    displayPath: "standards.requirement.table",
    currentTributeValue: "0 structured rows — placeholder visible",
    referencePattern: "Curio: 7+ standards.requirement rows with Typical consideration / Owner planning / Status / Notes body format",
    defectType: "missing_table_structure",
    severity: "critical",
    remediationBucket: REMEDIATION_BUCKET.SOURCE_EVIDENCE_REQUIRED,
    proposedFix:
      "Do not auto-build table from Internal Only FDD fragment. Workflow: (1) founder approves/rejects be.standards.qualityAssuranceTheme; (2) capture design-standards source or FDD Item excerpts with legal review; (3) human-author structured standards.requirement rows using owner-table template.",
    proposedTitle: null,
    proposedBody: null,
    sourceBasis: standardsFact
      ? `Pending fact ${standardsFact.fieldKey} — ${standardsFact.publicVisibility}, ${standardsFact.reviewStatus}; not external-display safe`
      : "No approved external-display standards facts",
    reviewStatus: "blocked_pending_fact_approval_and_source_capture",
    safeForFutureWriter: false,
    needsMedia: false,
    needsSourceEvidence: true,
    needsSortOrderCorrection: false,
    suppressUntilEvidence: true,
    standardsTableCanBeSafelyBuiltNow: false,
    relatedFacts: {
      qualityAssuranceTheme: standardsFact
        ? { fieldKey: standardsFact.fieldKey, visibility: standardsFact.publicVisibility, status: standardsFact.reviewStatus }
        : null,
      fddDocumentVintage: fddVintageFact
        ? { fieldKey: fddVintageFact.fieldKey, supportsSlot: "standards.last_reviewed", status: fddVintageFact.reviewStatus }
        : null,
    },
  });

  plans.push({
    section: "Standard Detail, Where Available",
    slotKey: "standards.requirement",
    displayPath: "renderStandardsOwnerConsiderations.fallback",
    currentTributeValue: "No owner planning checklist is published…",
    referencePattern: "Curio renders five-column owner standards table",
    defectType: "generic_placeholder_copy",
    severity: "critical",
    remediationBucket: REMEDIATION_BUCKET.SOURCE_EVIDENCE_REQUIRED,
    proposedFix: "Suppress placeholder only after standards.requirement rows exist — do not hide section without replacement table.",
    proposedTitle: null,
    proposedBody: null,
    sourceBasis: "Depends on standards.requirement row creation (v24C)",
    reviewStatus: "blocked_pending_table_rows",
    safeForFutureWriter: false,
    needsMedia: false,
    needsSourceEvidence: true,
    needsSortOrderCorrection: false,
    suppressUntilEvidence: true,
  });

  // Portfolio Context
  const portfolioCopy = copyProposal(
    "3",
    "Marriott chain scale tier index (upper-upscale soft collection) — matches portfolioLadderTierIndex logic"
  );
  const portfolioBody = copyProposal(
    "Upper-upscale soft collection within Marriott—Tribute sits among lifestyle and collection flags that preserve independent character with Bonvoy and Marriott commercial systems; not limited-service, extended-stay, or rigid full-service chain retail formats.",
    "Tribute consumer site + brandArchitecture (Autograph/Tribute/Luxury Collection family) — no hotel-count KPIs"
  );
  plans.push({
    section: "Portfolio Context",
    slotKey: "overview.portfolio_context",
    displayPath: "buildPortfolioLadderCellsHtml",
    currentTributeValue: "Generic Lower-scale / Mid-scale / Upscale / Upper-scale labels",
    referencePattern:
      "Curio overview.portfolio_context title=3 + body explaining Hilton ladder position; Hilton static ladder shows sibling brand names",
    defectType: "missing_peer_portfolio_context",
    severity: "high",
    remediationBucket: REMEDIATION_BUCKET.FRONTEND_MAPPING_REQUIRED,
    proposedFix:
      "Two-part fix: (A) populate overview.portfolio_context Title=3 + Body ladder narrative; (B) add Marriott static portfolio ladder mapping in brand-explorer-atelier-from-api.js (mirror HILTON_PORTFOLIO_* pattern for Marriott International).",
    proposedTitle: portfolioCopy.proposedBody,
    proposedBody: portfolioBody.proposedBody,
    copyLabel: portfolioBody.copyLabel,
    sourceBasis: portfolioBody.sourceBasis,
    reviewStatus: "pending_founder_review_and_frontend_mapping",
    safeForFutureWriter: false,
    needsMedia: false,
    needsSourceEvidence: false,
    needsSortOrderCorrection: false,
    suppressUntilEvidence: false,
    portfolioContextCanBeSafelyFixedNow: false,
    note: "Copy-only partial fix improves hint text but ladder cells stay generic until Marriott frontend mapping ships.",
  });

  // Featured Application
  const feat = vm.featuredApplication || {};
  const featShort = copyProposal(
    "Independent boutique hotels with distinctive style and local flavor—Bonvoy and Marriott commercial support without erasing individuality.",
    "brandPositioning shortened for 220-char UI slice — consumer site paraphrase"
  );
  plans.push({
    section: "Featured Application / Conversion Example",
    slotKey: "overview.featured_application",
    displayPath: "featured-case-preview__sub (brandPositioning.slice(0,220))",
    currentTributeValue: short(feat.body, 120),
    referencePattern: "Curio featured block uses substantive tagline + positioning within UI truncation",
    defectType: feat.truncatedInUi ? "truncated_copy" : "thin_copy_vs_reference",
    severity: "medium",
    remediationBucket: REMEDIATION_BUCKET.COPY_CLEANUP_SAFE,
    proposedFix: "Shorten featured lead copy to fit 220-char UI slice OR extend truncation in future frontend pass.",
    proposedTitle: feat.lead || "Exactly like nothing else.",
    proposedBody: featShort.proposedBody,
    copyLabel: featShort.copyLabel,
    sourceBasis: featShort.sourceBasis,
    reviewStatus: "pending_founder_review",
    safeForFutureWriter: true,
    needsMedia: false,
    needsSourceEvidence: false,
    needsSortOrderCorrection: false,
    suppressUntilEvidence: false,
  });

  // Sort Order
  const likelyDefaults = sortOrderReport?.sortOrderAuditSummary?.likelyWriterDefaultCount ?? 82;
  const correctionPlan = (sortOrderReport?.proposedSortOrderCorrectionPlan || []).slice(0, 5);
  plans.push({
    section: "Cross-section",
    slotKey: "(multi-row slots)",
    displayPath: "presentation.Sort Order",
    currentTributeValue: `${likelyDefaults} rows with index×10 writer defaults`,
    referencePattern: "Curio completed brands: Sort Order 0 predominant per slot family",
    defectType: "bad_sort_order",
    severity: "high",
    remediationBucket: REMEDIATION_BUCKET.SORT_ORDER_REQUIRED,
    proposedFix:
      "Future v24D gated Sort Order correction writer — normalize after v24A–v24C content batches; do not change Sort Order in this package.",
    proposedTitle: null,
    proposedBody: null,
    sourceBasis: "reports/brand-explorer-presentation-sort-order-audit.json",
    reviewStatus: "deferred_v24D",
    safeForFutureWriter: false,
    needsMedia: false,
    needsSourceEvidence: false,
    needsSortOrderCorrection: true,
    suppressUntilEvidence: false,
    v24DSampleCorrections: correctionPlan,
    tributeRowCount: sortOrderReport?.tributeRowCount,
  });

  return plans;
}

function short(text, max = 160) {
  const s = nz(text).replace(/\s+/g, " ");
  return s.length > max ? `${s.slice(0, max - 1)}…` : s;
}

function groupPlans(plans) {
  const byBucket = {};
  for (const key of Object.values(REMEDIATION_BUCKET)) byBucket[key] = [];
  for (const p of plans) {
    if (byBucket[p.remediationBucket]) byBucket[p.remediationBucket].push(p.slotKey);
  }
  return byBucket;
}

function assessPostSafeFixComparability(baseScore, copySafeCount, criticalRemaining) {
  const score = Number(baseScore) || 39;
  const projected = Math.min(55, score + copySafeCount * 2);
  return {
    visuallyComparableAfterOnlySafeFixes: false,
    projectedScoreAfterCopyCleanupOnly: projected,
    rationale:
      "Copy cleanup (bullets, featured lead, partial portfolio hint) cannot resolve critical standards table, title-only owner scenarios, or missing scenario.3 image — Curio parity requires v24B + v24C.",
    criticalDefectsRemainingAfterCopyOnly: criticalRemaining,
  };
}

export async function buildBrandExplorerVisualRemediationReviewPackageReport(options = {}) {
  const brandId = normalizeBrandInput(options.brandIdOrName);
  const v24Report = readJson(INPUT_PATHS.v24Defect);
  if (!v24Report) {
    throw new Error(`Missing ${INPUT_PATHS.v24Defect}. Run brand-explorer-visual-display-defect-audit first.`);
  }

  const sortOrderReport = readJson(INPUT_PATHS.sortOrder);
  const evidenceReport = readJson(INPUT_PATHS.evidenceReview);
  const visualQaReport = readJson(INPUT_PATHS.visualQa);
  const targetedExtract = readJson(INPUT_PATHS.targetedExtract);

  const brand = await fetchBrandApiShape(brandId);
  const curioBrand = await fetchBrandApiShape(CURIO_BRAND_ID);
  const sources = await loadApprovedTributeSources(brandId);
  const facts = await fetchAllFacts(brandId);
  const mediaAssets = await resolveMediaAssets(brandId);

  let liveVisualQa = visualQaReport;
  if (!liveVisualQa) {
    try {
      liveVisualQa = await buildBrandExplorerVisualQaVerificationReport({
        brandKey: "tribute-portfolio",
        brandRecordId: brandId,
      });
    } catch {
      liveVisualQa = null;
    }
  }

  const remediationPlans = buildRemediationPlans(
    v24Report,
    sortOrderReport,
    evidenceReport,
    liveVisualQa,
    brand,
    mediaAssets
  );

  const byBucket = groupPlans(remediationPlans);
  const copySafePlans = remediationPlans.filter(
    (p) => p.remediationBucket === REMEDIATION_BUCKET.COPY_CLEANUP_SAFE
  );
  const criticalRemaining = remediationPlans.filter(
    (p) => p.severity === "critical" && p.remediationBucket !== REMEDIATION_BUCKET.COPY_CLEANUP_SAFE
  ).length;

  const postSafe = assessPostSafeFixComparability(
    v24Report?.visualComparability?.score ?? 39,
    copySafePlans.length,
    criticalRemaining
  );

  const exactCopyFixes = copySafePlans
    .filter((p) => hasVal(p.proposedBody))
    .map((p) => ({
      slotKey: p.slotKey,
      displayPath: p.displayPath,
      proposedTitle: p.proposedTitle || null,
      proposedBody: p.proposedBody,
      copyLabel: p.copyLabel,
      sourceBasis: p.sourceBasis,
    }));

  const mediaNeeded = remediationPlans
    .filter((p) => p.needsMedia)
    .map((p) => ({
      slotKey: p.slotKey,
      approvedAssetExists: !p.suppressUntilEvidence,
      assets: mediaAssets.bySlot.get(p.slotKey) || [],
      action: p.proposedFix,
    }));

  const evidenceNeeded = remediationPlans
    .filter((p) => p.needsSourceEvidence)
    .map((p) => ({
      slotKey: p.slotKey,
      section: p.section,
      workflow: p.proposedFix,
      suppressUntilReady: p.suppressUntilEvidence,
    }));

  const scenario3Plan = remediationPlans.find((p) => p.slotKey === "overview.scenario.3" && p.needsMedia);
  const standardsPlan = remediationPlans.find((p) => p.slotKey === "standards.requirement" && p.severity === "critical");
  const portfolioPlan = remediationPlans.find((p) => p.slotKey === "overview.portfolio_context");

  return {
    packageVersion: PACKAGE_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    imagesUntouched: true,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    marriottValidationImplied: false,
    brand: { recordId: brandId, name: nz(brand?.name) || BRAND_NAME },
    filesRead: [
      "AGENTS.md",
      INPUT_PATHS.v24Defect,
      "reports/brand-explorer-visual-display-defect-audit.md",
      "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
      INPUT_PATHS.sortOrder,
      "reports/brand-explorer-presentation-sort-order-audit.md",
      INPUT_PATHS.evidenceReview,
      "reports/brand-explorer-evidence-fact-review-package.md",
      INPUT_PATHS.targetedExtract,
      "reports/tribute-portfolio-targeted-extract.md",
      INPUT_PATHS.visualQa,
      "reports/brand-explorer-visual-qa-verification.md",
      "reports/brand-explorer-slot-completion-remaining-plan.md",
      "docs/brand-explorer-presentation-slots.md",
      "api/brand-library.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "public/js/brand-explorer-gold-detail.js",
      "fixtures/brand-explorer-presentation-curio-full.json",
      "live Tribute/Curio presentation rows (API)",
      "Tribute approved sources + facts + asset registry",
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-visual-remediation-review-package.js",
      "scripts/brand-explorer-visual-remediation-review-package.mjs",
      `docs/data-intelligence/${DOC_MD_NAME}`,
      `reports/${REPORT_MD_NAME}`,
      `reports/${REPORT_JSON_NAME}`,
      "package.json",
    ],
    v24AReviewPackageExists: true,
    v24Baseline: {
      visualDisplayScore: v24Report?.visualComparability?.score ?? 39,
      tributeBlockCount: v24Report?.tributePresentationRowCount ?? 104,
      curioBlockCount: v24Report?.curioPresentationRowCount ?? 206,
      defectCount: v24Report?.defectCounts?.total ?? 17,
    },
    defectsReviewed: remediationPlans.length,
    remediationPlans,
    defectsByRemediationBucket: byBucket,
    copyCleanupSafe: byBucket[REMEDIATION_BUCKET.COPY_CLEANUP_SAFE],
    mediaAssetRequired: byBucket[REMEDIATION_BUCKET.MEDIA_ASSET_REQUIRED],
    sourceEvidenceRequired: byBucket[REMEDIATION_BUCKET.SOURCE_EVIDENCE_REQUIRED],
    sortOrderRequired: byBucket[REMEDIATION_BUCKET.SORT_ORDER_REQUIRED],
    frontendMappingRequired: byBucket[REMEDIATION_BUCKET.FRONTEND_MAPPING_REQUIRED],
    remainBlankOrSuppressed: byBucket[REMEDIATION_BUCKET.REMAIN_BLANK_OR_SUPPRESSED],
    exactProposedCopyForSafeFixes: exactCopyFixes,
    exactMediaAssetsNeeded: mediaNeeded,
    exactSourceEvidenceNeeded: evidenceNeeded,
    standardsTableCanBeSafelyBuiltNow: standardsPlan?.standardsTableCanBeSafelyBuiltNow ?? false,
    scenario3ImageCanBeSafelyFixedNow: scenario3Plan?.scenario3ImageCanBeSafelyFixedNow ?? false,
    portfolioContextCanBeSafelyFixedNow: portfolioPlan?.portfolioContextCanBeSafelyFixedNow ?? false,
    recommendedBatches: {
      v24A: {
        description: "Founder review of AI-drafted copy fixes (bullets, featured lead) — no Airtable writer yet",
        slotKeys: byBucket[REMEDIATION_BUCKET.COPY_CLEANUP_SAFE],
        count: byBucket[REMEDIATION_BUCKET.COPY_CLEANUP_SAFE].length,
      },
      v24B: {
        description: "Media/asset promotion when registry assets approved for scenario.3 and gallery.3",
        slotKeys: ["overview.scenario.3", "materials.gallery.3"],
        count: mediaNeeded.filter((m) => !m.approvedAssetExists).length + mediaNeeded.filter((m) => m.approvedAssetExists).length,
      },
      v24C: {
        description: "Source-evidence workflow: standards table, valueOwners scenario bodies, fact approval",
        slotKeys: byBucket[REMEDIATION_BUCKET.SOURCE_EVIDENCE_REQUIRED],
        count: byBucket[REMEDIATION_BUCKET.SOURCE_EVIDENCE_REQUIRED].length,
      },
      v24D: {
        description: "Sort Order correction writer after content stabilization",
        slotKeys: byBucket[REMEDIATION_BUCKET.SORT_ORDER_REQUIRED],
        rowCount: sortOrderReport?.tributeRowCount,
        likelyWriterDefaults: sortOrderReport?.sortOrderAuditSummary?.likelyWriterDefaultCount,
      },
    },
    recommendedNextBatch: "v24A_copy_cleanup_founder_review_then_v24C_standards_and_owner_scenarios",
    postSafeFixComparability: postSafe,
    sourceContext: {
      approvedSourceCount: sources.length,
      factCount: facts.length,
      pendingFacts: facts.filter((f) => nz(f.humanReviewStatus) === "Pending").length,
      approvedRegistryAssets: mediaAssets.approvedCount,
      targetedExtractAvailable: Boolean(targetedExtract),
    },
    curioReferenceSummary: {
      recordId: CURIO_BRAND_ID,
      blockCount: curioBrand?.brandExplorer?.blocks?.length || v24Report?.curioPresentationRowCount,
      portfolioContextTitle: "3",
      standardsRequirementRows: v24Report?.curioVisibleModelSummary?.standardsTableRows ?? 7,
    },
    exactNextCommand:
      "Founder review proposed copy in reports/brand-explorer-visual-remediation-review-package.json, then: npm run brand-explorer-visual-remediation-review-package -- --brand tribute-portfolio --dry-run",
  };
}

export function buildBrandExplorerVisualRemediationReviewPackageMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Visual Remediation Review Package v24A");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}** · Airtable modified: **no** · Images untouched: **yes**`);
  lines.push(`- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`);
  lines.push(`- v24 baseline score: **${report.v24Baseline.visualDisplayScore}/100**`);
  lines.push(`- Defects reviewed: **${report.defectsReviewed}**`);
  lines.push(`- Comparable after copy-only fixes: **${report.postSafeFixComparability.visuallyComparableAfterOnlySafeFixes ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Remediation buckets");
  lines.push(`- Copy cleanup safe: ${report.copyCleanupSafe.map((k) => `\`${k}\``).join(", ")}`);
  lines.push(`- Media/asset required: ${report.mediaAssetRequired.map((k) => `\`${k}\``).join(", ")}`);
  lines.push(`- Source evidence required: ${report.sourceEvidenceRequired.map((k) => `\`${k}\``).join(", ")}`);
  lines.push(`- Sort Order (v24D): ${report.sortOrderRequired.map((k) => `\`${k}\``).join(", ")}`);
  lines.push(`- Frontend mapping: ${report.frontendMappingRequired.map((k) => `\`${k}\``).join(", ")}`);
  lines.push(`- Remain blank/suppressed: ${report.remainBlankOrSuppressed.map((k) => `\`${k}\``).join(", ")}`);
  lines.push("");
  lines.push("## Safe copy proposals");
  for (const c of report.exactProposedCopyForSafeFixes) {
    lines.push(`### \`${c.slotKey}\``);
    lines.push(`- ${c.copyLabel}`);
    lines.push(`- Source: ${c.sourceBasis}`);
    lines.push(`- Proposed: ${c.proposedBody}`);
    lines.push("");
  }
  lines.push("## Gates");
  lines.push(`- Standards table safe now: **${report.standardsTableCanBeSafelyBuiltNow ? "yes" : "no"}**`);
  lines.push(`- scenario.3 image safe now: **${report.scenario3ImageCanBeSafelyFixedNow ? "yes" : "no"}**`);
  lines.push(`- Portfolio Context safe now: **${report.portfolioContextCanBeSafelyFixedNow ? "yes" : "no"}**`);
  lines.push("");
  lines.push(`## Recommended next: **${report.recommendedNextBatch}**`);
  lines.push("");
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  return lines.join("\n");
}
