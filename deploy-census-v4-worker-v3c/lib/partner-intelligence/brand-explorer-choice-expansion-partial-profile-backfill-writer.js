/**
 * Brand Explorer Choice Expansion Partial Profile Backfill + Visual Repair v31A.
 *
 * Targets closest Choice expansion brands: Suburban Studios and Radisson Individuals by Choice.
 * Backfills presentation rows (scenarios, portfolio mix, momentum), repairs thin visual cards,
 * normalizes sort drift, and classifies source/fact governance — no fact approval in this writer.
 *
 * @see docs/data-intelligence/brand-explorer-choice-expansion-partial-profile-backfill-writer-v31A.md
 */
import { fetchBrandBasics, fetchLiveState } from "./tribute-portfolio-package-pipeline.js";
import { WAVE1_EXPANSION_SLUGS } from "./brand-explorer-next-brand-selection-audit.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";
import { buildBrandExplorerRequiredSectionPopulationContractReport } from "./brand-explorer-required-section-population-contract.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import {
  assessBrandExplorerGovernanceReadiness,
  isApprovedExplorerSource,
} from "./profile-governance-publish-readiness.js";

export const WRITER_VERSION = "31A";
export const REPORT_JSON_NAME =
  "brand-explorer-choice-expansion-partial-profile-backfill-writer.json";
export const REPORT_MD_NAME =
  "brand-explorer-choice-expansion-partial-profile-backfill-writer.md";
export const DOC_MD_NAME =
  "brand-explorer-choice-expansion-partial-profile-backfill-writer-v31A.md";

export const TARGET_BRANDS = Object.freeze([
  { slug: "suburban-studios", recordId: "reclcjg5Foa9Vs5TC", name: "Suburban Studios" },
  {
    slug: "radisson-individuals-by-choice",
    recordId: "recRyvM8OmLlDj9G7",
    name: "Radisson Individuals by Choice",
  },
]);

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v31A-choice-expansion-partial-profile-backfill";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-choice-expansion-copy-repair";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "tribute-portfolio",
  "radisson",
  "ascend",
  "radisson-blu",
  "kimpton",
  "curio-collection",
  "woodspring-suites",
  "everhome-suites",
  "voco-hotels",
  "vignette-collection",
  "holiday-inn-express",
  "avid-hotels",
  "even-hotels",
  "tapestry-collection-by-hilton",
  "autograph-collection",
  "design-hotels",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const RADISSON_INDIVIDUALS_PRESS_KIT_URL =
  "https://media.choicehotels.com/Radisson-Individuals-press-kit";
const MIN_SCENARIO_WORDS = 15;
const MIN_OVERVIEW_SCENARIO_WORDS = 22;
const MIN_GALLERY_WORDS = 12;

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-complete-build-batch.md",
  "reports/brand-explorer-complete-build-batch.json",
  "reports/brand-explorer-complete-build-suburban-studios.md",
  "reports/brand-explorer-complete-build-suburban-studios.json",
  "reports/brand-explorer-complete-build-radisson-individuals-by-choice.md",
  "reports/brand-explorer-complete-build-radisson-individuals-by-choice.json",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "docs/brand-explorer-presentation-slots.md",
  "lib/partner-intelligence/brand-explorer-parent-aware-carryover.js",
  "lib/partner-intelligence/brand-explorer-brand-target-resolver.js",
  "api/brand-library.js",
  "live Suburban Studios / Radisson Individuals presentation rows",
  "live Partner Facts and Source Library for both brands",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-choice-expansion-partial-profile-backfill-writer.js",
  "scripts/brand-explorer-choice-expansion-partial-profile-backfill-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

const COMPANY_VALIDATION_BLOCK_RE =
  /company validated|validated by choice|validated by radisson|company-approved|company approved|official sign-off/i;

const SHARED_COPY_SAFETY_PATTERNS = [
  { id: "tribute", re: /\btribute portfolio\b/i },
  { id: "marriott", re: /\bmarriott\b/i },
  { id: "bonvoy", re: /\bmarriott bonvoy\b|\bbonvoy\b/i },
  { id: "curio", re: /\bcurio collection\b/i },
  { id: "hilton", re: /\bhilton honors\b|\bhilton\b/i },
  { id: "kimpton", re: /\bkimpton\b/i },
  { id: "ihg", re: /\bihg\b|\bintercontinental hotels group\b/i },
  { id: "company_validated", re: COMPANY_VALIDATION_BLOCK_RE },
  { id: "consumer_site", re: /\bconsumer site\b/i },
  { id: "internal", re: /\binternal extraction\b|\bpaste into airtable\b/i },
  { id: "fdd", re: /\bfranchise disclosure document\b|\bitem\s*19\b/i },
  { id: "source_capture", re: /\bsource capture\b|\bextraction run\b/i },
];

const BRAND_REPAIR_CONFIG = {
  "suburban-studios": {
    valueScenarioPackages: [
      {
        slotKey: "valueOwners.scenario.1",
        sort: 0,
        title: "Extended-Stay Studio Conversion",
        body:
          "Independent or economy extended-stay assets needing weekly-rate positioning and in-room kitchenettes—Suburban Studios fits when owners want Choice Privileges distribution without full-service F&B or daily housekeeping intensity.",
      },
      {
        slotKey: "valueOwners.scenario.2",
        sort: 1,
        title: "Weekly Corporate Demand Corridor",
        body:
          "Markets with project-based crews, training rotations, or insurance housing—Suburban works when ADR supports kitchenette operations and owners can staff lean extended-stay housekeeping within Choice standards.",
      },
      {
        slotKey: "valueOwners.scenario.3",
        sort: 2,
        title: "Conversion From Independent Extended Stay",
        body:
          "Mature extended-stay properties needing affiliation lift—Suburban competes when room modules already support kitchenettes and owners need CRS, loyalty, and revenue tools without repositioning to select-service economics.",
      },
      {
        slotKey: "valueOwners.scenario.4",
        sort: 3,
        title: "Third-Party Operator–Led",
        body:
          "Assets run by extended-stay operators who understand weekly billing and project demand—Suburban suits sponsors who need recognizable Choice affiliation while keeping operating complexity aligned to economy extended-stay norms.",
      },
    ],
    portfolioMixChips: [
      { title: "Extended-Stay Studio", body: "High", sort: 0 },
      { title: "Weekly Corporate Demand", body: "High", sort: 1 },
      { title: "Conversion / Repositioning", body: "Moderate", sort: 2 },
    ],
    overviewScenarioBodies: {
      "overview.scenario.1":
        "Economy extended-stay studios for contractors, project crews, and temporary housing—Suburban fits when owners need weekly-rate positioning, in-room kitchenettes, and Choice Privileges distribution without full-service operating load.",
      "overview.scenario.2":
        "Weekly-stay corridors near employment centers, hospitals, or training campuses—Suburban works when demand is project-driven and owners can align housekeeping and kitchenette FF&E to Suburban prototype bands.",
      "overview.scenario.3":
        "Kitchenette conversions from older select-service or independent extended-stay formats—Suburban competes when room modules support cooking facilities and owners want Choice scale without upscale public-space requirements.",
    },
    copyRepairTargets: [
      {
        slotKey: "loyalty.proof",
        titleMatch: /Campaign Scale/i,
        match: /\(consumer site, 14 programs evaluated\)/i,
        replace: "(third-party ranking, 14 programs evaluated)",
        reason: "remove_internal_source_capture_language",
      },
    ],
    pendingSourceReviewSections: ["footprint.openings", "footprint.momentum"],
  },
  "radisson-individuals-by-choice": {
    valueScenarioPackages: [
      {
        slotKey: "valueOwners.scenario.1",
        sort: 0,
        title: "Boutique Independent Conversion",
        body:
          "Hand-selected independent boutiques that need Choice Privileges and enterprise distribution while preserving local design narrative—Radisson Individuals fits when the asset already delivers upper-upscale guest experience without prototype homogenization.",
      },
      {
        slotKey: "valueOwners.scenario.2",
        sort: 1,
        title: "CALA Gateway Growth",
        body:
          "Gateway and resort corridors in CALA where guests compare branded upper-upscale flags—Radisson Individuals competes when owners want soft-brand flexibility with recognizable Choice affiliation and loyalty participation.",
      },
      {
        slotKey: "valueOwners.scenario.3",
        sort: 2,
        title: "Preserve Uniqueness + Choice Scale",
        body:
          "Owners who resist rigid prototype conversion but need financing credibility and CRS access—Individuals works when public spaces and guest rooms already support upper-upscale positioning and operator depth is proven.",
      },
      {
        slotKey: "valueOwners.scenario.4",
        sort: 3,
        title: "Third-Party Operator–Led",
        body:
          "Assets run by experienced lifestyle or upper-upscale operators—Radisson Individuals suits sponsors who need affiliation for sales velocity while keeping property-specific F&B and design decisions largely owner-controlled.",
      },
    ],
    portfolioMixChips: [
      { title: "Independent Boutique", body: "High", sort: 0 },
      { title: "CALA Gateway Growth", body: "High", sort: 1 },
      { title: "Conversion w/ Character", body: "Moderate", sort: 2 },
    ],
    overviewScenarioBodies: {
      "overview.scenario.1":
        "Boutique independent conversions needing upper-upscale affiliation without surrendering local story—Radisson Individuals fits when design, F&B identity, and operator depth already support hand-selected collection standards.",
      "overview.scenario.2":
        "CALA hand-selected growth in gateway cities—Individuals works when owners want Choice Privileges and enterprise channels while keeping property-specific character that rigid prototype brands would dilute.",
      "overview.scenario.3":
        "Owners prioritizing uniqueness plus Choice scale—Individuals competes when the asset can hold upper-upscale ADR and management can execute soft-brand QA without constant prototype waivers.",
    },
    momentumRepairs: [
      {
        recordId: "rec0an5blfW4FtMfE",
        title: "Radisson Individuals CALA portfolio expansion",
        dateLine: "2024",
        summary:
          "Hand-selected Radisson Individuals signings across Colombia and Panama reinforce Choice's upper-upscale soft-brand growth in CALA—each property retains local character within Individuals collection standards.",
        sourceUrl: RADISSON_INDIVIDUALS_PRESS_KIT_URL,
      },
      {
        recordId: "recb0WzRRu6jrev4c",
        title: "Individuals collection — Medellín and Cartagena",
        dateLine: "2024",
        summary:
          "Medellín and Cartagena Individuals openings illustrate owner-led boutique positioning with Choice Privileges distribution—compare against rigid prototype flags when underwriting conversion economics.",
        sourceUrl: RADISSON_INDIVIDUALS_PRESS_KIT_URL,
      },
      {
        recordId: "recpIgmBNBEMXVEda",
        title: "Individuals — Panama and regional growth",
        dateLine: "2024",
        summary:
          "Panama City and regional Individuals properties extend Choice's hand-selected upper-upscale footprint—owners should confirm Individuals QA, operator fit, and fee stack against other Choice soft brands.",
        sourceUrl: RADISSON_INDIVIDUALS_PRESS_KIT_URL,
      },
    ],
    galleryCaptions: {
      "materials.gallery.1":
        "Hotel Bambito by Faranda Boutique member reference—illustrates independent boutique character preserved under Radisson Individuals hand-selected collection positioning within Choice Privileges distribution.",
    },
    copyRepairTargets: [
      {
        slotKey: "loyalty.proof",
        titleMatch: /Campaign Scale/i,
        match: /\(consumer site, 14 programs evaluated\)/i,
        replace: "(third-party ranking, 14 programs evaluated)",
        reason: "remove_internal_source_capture_language",
      },
    ],
    pendingSourceReviewSections: [],
  },
};

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}

function wordCount(text) {
  return nz(text).split(/\s+/).filter(Boolean).length;
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function isLikelyWriterBatchSortOrder(sortOrder) {
  if (sortOrder == null || Number.isNaN(Number(sortOrder))) return false;
  const n = Number(sortOrder);
  return n >= 10 && n % 10 === 0;
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

function apiUrl(baseId, tableName, recordId = "") {
  const base = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
  return recordId ? `${base}/${encodeURIComponent(recordId)}` : base;
}

async function airtableFetch(baseId, apiKey, tableName, init = {}, recordId = "") {
  const res = await fetch(apiUrl(baseId, tableName, recordId), {
    ...init,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  return { res, json };
}

async function listPresentationRows(baseId, apiKey, brandRecordId, brandName) {
  const formula = `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`;
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    params.set("filterByFormula", formula);
    if (offset) params.set("offset", offset);
    const res = await fetch(`${apiUrl(baseId, PRESENTATION_TABLE)}?${params.toString()}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `List failed: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records.map((rec) => ({
    recordId: rec.id,
    slotKey: nz(rec.fields?.["Slot Key"]),
    title: nz(rec.fields?.Title),
    body: nz(rec.fields?.Body),
    sortOrder: rec.fields?.["Sort Order"],
    active: rec.fields?.Active,
    hasImage: Array.isArray(rec.fields?.Image) && rec.fields.Image.length > 0,
  }));
}

function presentationFields({ slotKey, title, body, sort, brandRecordId, brandName }) {
  return {
    "Slot Key": slotKey,
    Title: title || "",
    Body: body,
    "Brand Name": brandName,
    Brand: [brandRecordId],
    Active: true,
    "Sort Order": sort ?? 0,
  };
}

export function resolveTargetBrands(brandsArg) {
  const slugs = nz(brandsArg || TARGET_BRANDS.map((b) => b.slug).join(","))
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
  const resolved = [];
  for (const slug of slugs) {
    if (PROTECTED_BRAND_SLUGS.includes(slug)) {
      throw new Error(`Brand ${slug} is protected and cannot be modified by v31A`);
    }
    const meta = TARGET_BRANDS.find((b) => b.slug === slug);
    if (!meta) throw new Error(`v31A supports Suburban Studios and Radisson Individuals only; got: ${slug}`);
    if (!resolved.some((b) => b.slug === meta.slug)) resolved.push(meta);
  }
  if (!resolved.length) throw new Error("No target brands resolved");
  return resolved;
}

export function scanCopySafety(text) {
  const issues = [];
  for (const pattern of SHARED_COPY_SAFETY_PATTERNS) {
    if (pattern.re.test(nz(text))) issues.push(pattern.id);
  }
  return issues;
}

function getBrandConfig(slug) {
  const config = BRAND_REPAIR_CONFIG[slug];
  if (!config) throw new Error(`No repair config for ${slug}`);
  return config;
}

export function classifyExpansionFact(fact, brandSlug) {
  const status = nz(fact.humanReviewStatus);
  const visibility = nz(fact.publicVisibility);
  const value = nz(fact.approvedValue || fact.normalizedValue || fact.extractedValue);
  if (status === "Approved" || status === "Edited") {
    return {
      classification: visibility === "Internal Only" ? "approved_internal_only" : "approved_source_backed",
      shouldNotSurfaceExternally: visibility === "Internal Only",
      approveReady: false,
      recommendedAction: "already_approved_no_action_in_v31A",
    };
  }
  if (status === "Rejected") {
    return {
      classification: "weak_internal_reject_candidate",
      shouldNotSurfaceExternally: true,
      approveReady: false,
      recommendedAction: "keep_internal",
    };
  }
  if (status === "Pending") {
    return {
      classification: "pending_fact_review",
      shouldNotSurfaceExternally: true,
      approveReady: false,
      recommendedAction: "pending_fact_review",
    };
  }
  return {
    classification: "pending_source_review",
    rationale: `Fact for ${brandSlug} needs stewardship outside v31A.`,
    shouldNotSurfaceExternally: true,
    approveReady: false,
    recommendedAction: "pending_source_review",
  };
}

function proposeScenarioCreates(existingRows, brandRecordId, brandName, brandSlug) {
  const { valueScenarioPackages } = getBrandConfig(brandSlug);
  const creates = [];
  for (const pkg of valueScenarioPackages) {
    if (existingRows.some((r) => r.slotKey === pkg.slotKey)) continue;
    const issues = scanCopySafety(`${pkg.title}\n${pkg.body}`);
    if (issues.length || wordCount(pkg.body) < MIN_SCENARIO_WORDS) continue;
    creates.push({
      action: "create",
      slotKey: pkg.slotKey,
      fixReason: "missing_valueOwners_scenario_card",
      proposedTitle: pkg.title,
      proposedBody: pkg.body,
      fields: presentationFields({
        slotKey: pkg.slotKey,
        title: pkg.title,
        body: pkg.body,
        sort: pkg.sort,
        brandRecordId,
        brandName,
      }),
    });
  }
  return creates;
}

function proposeMixPlans(existingRows, brandRecordId, brandName, brandSlug) {
  const { portfolioMixChips } = getBrandConfig(brandSlug);
  const creates = [];
  const updates = [];
  for (const chip of portfolioMixChips) {
    const live =
      existingRows.find((r) => r.slotKey === "footprint.portfolio_mix" && nz(r.title) === chip.title) ||
      existingRows.find(
        (r) => r.slotKey === "footprint.portfolio_mix" && Number(r.sortOrder) === chip.sort
      );
    const issues = scanCopySafety(`${chip.title} ${chip.body}`);
    if (issues.length) continue;
    if (!live) {
      creates.push({
        action: "create",
        slotKey: "footprint.portfolio_mix",
        fixReason: "portfolio_mix_chip_backfill",
        proposedTitle: chip.title,
        proposedBody: chip.body,
        fields: presentationFields({
          slotKey: "footprint.portfolio_mix",
          title: chip.title,
          body: chip.body,
          sort: chip.sort,
          brandRecordId,
          brandName,
        }),
      });
    } else if (nz(live.body) !== chip.body || nz(live.title) !== chip.title) {
      updates.push({
        action: "update",
        recordId: live.recordId,
        slotKey: "footprint.portfolio_mix",
        fixReason: "portfolio_mix_chip_normalize",
        proposedTitle: chip.title,
        proposedBody: chip.body,
        fields: presentationFields({
          slotKey: "footprint.portfolio_mix",
          title: chip.title,
          body: chip.body,
          sort: chip.sort,
          brandRecordId,
          brandName,
        }),
      });
    }
  }
  return { creates, updates };
}

function proposeOverviewScenarioUpdates(existingRows, brandRecordId, brandName, brandSlug) {
  const { overviewScenarioBodies } = getBrandConfig(brandSlug);
  const updates = [];
  for (const [slotKey, proposedBody] of Object.entries(overviewScenarioBodies || {})) {
    const live = existingRows.find((r) => r.slotKey === slotKey);
    if (!live) continue;
    if (wordCount(live.body) >= MIN_OVERVIEW_SCENARIO_WORDS) continue;
    if (scanCopySafety(proposedBody).length) continue;
    updates.push({
      action: "update",
      recordId: live.recordId,
      slotKey,
      fixReason: "thin_overview_scenario_body",
      proposedTitle: live.title,
      proposedBody,
      fields: presentationFields({
        slotKey,
        title: live.title,
        body: proposedBody,
        sort: live.sortOrder ?? 0,
        brandRecordId,
        brandName,
      }),
    });
  }
  return updates;
}

function normalizeBodyForCompare(text) {
  return nz(text)
    .replace(/\r\n/g, "\n")
    .replace(/\u2014/g, "—")
    .replace(/\s+/g, " ")
    .trim();
}

function proposeMomentumUpdates(existingRows, brandRecordId, brandName, brandSlug) {
  const repairs = getBrandConfig(brandSlug).momentumRepairs || [];
  const updates = [];
  for (const repair of repairs) {
    const live = existingRows.find((r) => r.recordId === repair.recordId);
    if (!live || live.slotKey !== "footprint.momentum") continue;
    const body = `${repair.dateLine}\n\n${repair.summary}\n\n${repair.sourceUrl}`;
    if (
      normalizeBodyForCompare(live.body) === normalizeBodyForCompare(body) &&
      nz(live.title) === nz(repair.title)
    ) {
      continue;
    }
    if (scanCopySafety(`${repair.title}\n${body}`).length) continue;
    updates.push({
      action: "update",
      recordId: live.recordId,
      slotKey: "footprint.momentum",
      fixReason: "momentum_row_source_backed_repair",
      proposedTitle: repair.title,
      proposedBody: body,
      fields: presentationFields({
        slotKey: "footprint.momentum",
        title: repair.title,
        body,
        sort: live.sortOrder ?? 0,
        brandRecordId,
        brandName,
      }),
    });
  }
  return updates;
}

function proposeGalleryUpdates(existingRows, brandRecordId, brandName, brandSlug) {
  const captions = getBrandConfig(brandSlug).galleryCaptions || {};
  const updates = [];
  for (const [slotKey, proposedBody] of Object.entries(captions)) {
    const live = existingRows.find((r) => r.slotKey === slotKey);
    if (!live) continue;
    if (wordCount(live.body) >= MIN_GALLERY_WORDS) continue;
    if (scanCopySafety(`${live.title}\n${proposedBody}`).length) continue;
    updates.push({
      action: "update",
      recordId: live.recordId,
      slotKey,
      fixReason: "title_only_gallery_caption",
      proposedTitle: live.title,
      proposedBody,
      imageUntouched: true,
      fields: presentationFields({
        slotKey,
        title: live.title,
        body: proposedBody,
        sort: live.sortOrder ?? 0,
        brandRecordId,
        brandName,
      }),
    });
  }
  return updates;
}

function proposeCopyRepairs(existingRows, brandRecordId, brandName, brandSlug) {
  const targets = getBrandConfig(brandSlug).copyRepairTargets || [];
  const updates = [];
  for (const target of targets) {
    for (const live of existingRows.filter((r) => r.slotKey === target.slotKey)) {
      if (target.titleMatch && !target.titleMatch.test(live.title)) continue;
      if (!target.match.test(live.body)) continue;
      const proposedBody = live.body.replace(target.match, target.replace);
      if (proposedBody === live.body || scanCopySafety(proposedBody).length) continue;
      updates.push({
        action: "update",
        recordId: live.recordId,
        slotKey: live.slotKey,
        fixReason: target.reason,
        proposedTitle: live.title,
        proposedBody,
        fields: presentationFields({
          slotKey: live.slotKey,
          title: live.title,
          body: proposedBody,
          sort: live.sortOrder ?? 0,
          brandRecordId,
          brandName,
        }),
      });
    }
  }
  return updates;
}

function proposeSortOrderRepairs(existingRows, brandRecordId, brandName) {
  const updates = [];
  for (const live of existingRows) {
    if (!isLikelyWriterBatchSortOrder(live.sortOrder)) continue;
    updates.push({
      action: "update",
      recordId: live.recordId,
      slotKey: live.slotKey,
      fixReason: "normalize_writer_batch_sort_order",
      proposedTitle: live.title,
      proposedBody: live.body,
      fields: presentationFields({
        slotKey: live.slotKey,
        title: live.title,
        body: live.body,
        sort: 0,
        brandRecordId,
        brandName,
      }),
    });
  }
  return updates;
}

function buildPendingSourceReviewQueues(brandSlug, existingRows, liveState) {
  const config = getBrandConfig(brandSlug);
  const queues = [];
  const approvedSources = (liveState?.sources || []).filter(isApprovedExplorerSource);
  for (const section of config.pendingSourceReviewSections || []) {
    const rows = existingRows.filter((r) => r.slotKey === section);
    if (rows.length >= 3) continue;
    queues.push({
      section,
      slotKey: section,
      status: "pending_source_review",
      reason:
        approvedSources.length === 0
          ? "No approved Explorer sources — capture Choice public sources before creating dated momentum/openings rows."
          : "Insufficient complete rows — source capture package required before v31A can safely create rows.",
      rowsNeeded: Math.max(0, 3 - rows.length),
      applyInV31A: false,
    });
  }
  return queues;
}

function buildPendingImageReview(existingRows, brandSlug) {
  const pending = [];
  for (const row of existingRows) {
    const needsImage =
      row.slotKey === "footprint.openings" ||
      row.slotKey.startsWith("valueOwners.scenario") ||
      row.slotKey.startsWith("overview.scenario") ||
      row.slotKey.startsWith("materials.gallery");
    if (!needsImage) continue;
    if (row.hasImage) {
      if (row.slotKey.startsWith("materials.gallery") && wordCount(row.body) < MIN_GALLERY_WORDS) {
        pending.push({
          recordId: row.recordId,
          slotKey: row.slotKey,
          status: "pending_caption_only",
          note: "Image present; caption backfill proposed separately.",
        });
      }
      continue;
    }
    if (row.slotKey === "footprint.openings" && hasVal(row.title) && wordCount(row.body) >= 20) {
      pending.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        status: "pending_image_review",
        note: "Opening row has copy but no approved image attachment.",
      });
    } else if (row.slotKey.startsWith("materials.gallery")) {
      pending.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        status: "pending_image_review",
        note: "Gallery slot missing image — do not materialize unapproved assets in v31A.",
      });
    }
  }
  return pending;
}

function estimatePostApplyScores({
  contractScore,
  pendingFactsCount,
  criticalRemaining,
  highRemaining,
  visualScoreProjected,
}) {
  const sourceGovernanceScore = Math.max(0, 100 - pendingFactsCount * 3);
  const visualCompletenessScore = Math.max(0, 100 - criticalRemaining * 20 - highRemaining * 8);
  const overallNumeric = Math.round(
    contractScore * 0.25 +
      visualScoreProjected * 0.2 +
      100 * 0.15 +
      sourceGovernanceScore * 0.15 +
      visualCompletenessScore * 0.25
  );
  let readiness = "not_ready";
  if (criticalRemaining === 0 && highRemaining === 0 && overallNumeric >= 85 && contractScore >= 85) {
    readiness = "ready";
  } else if (criticalRemaining === 0 && overallNumeric >= 70) {
    readiness = "almost_ready";
  } else if (criticalRemaining > 0) {
    readiness = "blocked";
  }
  return { overallNumeric, overallActiveProfileReadiness: readiness };
}

export function buildApplyCommand({ brands = TARGET_BRANDS.map((b) => b.slug).join(",") } = {}) {
  return [
    "npm run brand-explorer-choice-expansion-partial-profile-backfill-writer --",
    `--brands ${brands}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER,
    APPLY_FLAG_NO_VALIDATION,
  ].join(" ");
}

async function buildPerBrandReport(target, options) {
  const { apply, approveBatch, founderReviewed, noValidationClaim, baseId, apiKey } = options;
  const brandSlug = target.slug;
  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const liveState = await fetchLiveState(target.recordId);
  const presentationRows = await listPresentationRows(baseId, apiKey, target.recordId, target.name);
  const explorerGovernance = assessBrandExplorerGovernanceReadiness(liveState);

  const visualBefore = await buildBrandExplorerVisualDisplayDefectAuditReport({
    brandIdOrName: target.recordId,
  }).catch(() => null);
  const contractBefore = await buildBrandExplorerRequiredSectionPopulationContractReport({
    brandIdOrName: target.recordId,
  }).catch(() => ({ readinessScore: 0, brandExplorerRequiredSectionsReady: false }));
  const finalQaBefore = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.recordId,
  }).catch(() => null);
  const qaBrand = finalQaBefore?.brandReports?.[0] || {};

  const scenarioCreates = proposeScenarioCreates(presentationRows, target.recordId, target.name, brandSlug);
  const mixPlans = proposeMixPlans(presentationRows, target.recordId, target.name, brandSlug);
  const overviewUpdates = proposeOverviewScenarioUpdates(
    presentationRows,
    target.recordId,
    target.name,
    brandSlug
  );
  const momentumUpdates = proposeMomentumUpdates(presentationRows, target.recordId, target.name, brandSlug);
  const galleryUpdates = proposeGalleryUpdates(presentationRows, target.recordId, target.name, brandSlug);
  const copyRepairs = proposeCopyRepairs(presentationRows, target.recordId, target.name, brandSlug);
  const sortRepairs = proposeSortOrderRepairs(presentationRows, target.recordId, target.name);

  const rowsWouldCreate = [...scenarioCreates, ...mixPlans.creates];
  const rowsWouldUpdate = [
    ...mixPlans.updates,
    ...overviewUpdates,
    ...momentumUpdates,
    ...galleryUpdates,
    ...copyRepairs,
    ...sortRepairs,
  ];

  const pendingSourceReview = buildPendingSourceReviewQueues(brandSlug, presentationRows, liveState);
  const pendingImageReview = buildPendingImageReview(presentationRows, brandSlug);

  const explorerFacts = (liveState.facts || []).filter(
    (f) => nz(f.explorerType) === "Brand Explorer" || nz(f.fieldName).startsWith("be.")
  );
  const factClassification = explorerFacts.map((f) => ({
    factId: f.id,
    fieldName: f.fieldName,
    humanReviewStatus: f.humanReviewStatus,
    publicVisibility: f.publicVisibility,
    sourceRecordId: f.sourceRecordId,
    ...classifyExpansionFact(f, brandSlug),
  }));

  const sourceClassification = (liveState.sources || []).map((s) => ({
    sourceId: s.id,
    title: s.sourceTitle,
    approvedForExplorer: isApprovedExplorerSource(s),
    classification: isApprovedExplorerSource(s)
      ? "approved_source_backed"
      : "pending_source_review",
  }));

  const applyBlockers = [];
  const proposedCopySafety = [];
  for (const row of [...rowsWouldCreate, ...rowsWouldUpdate]) {
    if (row.fixReason === "normalize_writer_batch_sort_order") continue;
    const issues = scanCopySafety(`${row.proposedTitle || ""}\n${row.proposedBody || ""}`);
    if (issues.length) proposedCopySafety.push({ slotKey: row.slotKey, issues });
  }
  if (proposedCopySafety.length) {
    applyBlockers.push(`proposed_copy_safety_issues:${proposedCopySafety.length}`);
  }
  if (pendingSourceReview.some((q) => q.rowsNeeded > 0 && brandSlug === "suburban-studios")) {
    applyBlockers.push("suburban_openings_momentum_need_source_capture_first");
  }

  const visualDefectsBefore = visualBefore?.defectCounts || qaBrand.defectCounts || {};
  const projectedHigh = Math.max(
    0,
    (visualDefectsBefore.high || 0) -
      scenarioCreates.length -
      overviewUpdates.length -
      momentumUpdates.length -
      galleryUpdates.filter((r) => wordCount(r.proposedBody) >= MIN_GALLERY_WORDS).length
  );
  const projectedCritical = visualDefectsBefore.critical || 0;
  if (projectedCritical > 0) applyBlockers.push(`critical_defects_remain:${projectedCritical}`);
  if (projectedHigh > 0 && brandSlug === "suburban-studios") {
    applyBlockers.push(`high_defects_remain_projected:${projectedHigh}`);
  }

  const contractScoreProjected = Math.min(
    100,
    (contractBefore.readinessScore || 0) +
      mixPlans.creates.length * 4 +
      momentumUpdates.length * 5 +
      (pendingSourceReview.every((q) => q.rowsNeeded === 0) ? 5 : 0)
  );

  const projectedScores = estimatePostApplyScores({
    contractScore: contractScoreProjected,
    pendingFactsCount: explorerFacts.filter((f) => f.humanReviewStatus === "Pending").length,
    criticalRemaining: projectedCritical,
    highRemaining: projectedHigh,
    visualScoreProjected: Math.max(0, 100 - projectedHigh * 8),
  });

  const applyGatesReady = apply && approveBatch && founderReviewed && noValidationClaim;
  const hasWork = rowsWouldCreate.length > 0 || rowsWouldUpdate.length > 0;
  const canApply = applyGatesReady && applyBlockers.length === 0 && hasWork;

  let airtableModified = false;
  let applyResults = null;
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    const created = [];
    const updated = [];
    const errors = [];
    for (const row of rowsWouldCreate) {
      const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
        method: "POST",
        body: JSON.stringify({ fields: row.fields, typecast: true }),
      });
      if (!res.ok) errors.push({ action: "create", slotKey: row.slotKey, message: json.error?.message });
      else created.push({ recordId: json.id, slotKey: row.slotKey });
      await new Promise((r) => setTimeout(r, 220));
    }
    for (const row of rowsWouldUpdate) {
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        { method: "PATCH", body: JSON.stringify({ fields: row.fields, typecast: true }) },
        row.recordId
      );
      if (!res.ok) {
        errors.push({ action: "update", slotKey: row.slotKey, recordId: row.recordId, message: json.error?.message });
      } else updated.push({ recordId: row.recordId, slotKey: row.slotKey, fixReason: row.fixReason });
      await new Promise((r) => setTimeout(r, 220));
    }
    airtableModified = (created.length > 0 || updated.length > 0) && errors.length === 0;
    applyResults = { created, updated, errors };
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(target.recordId));
  } else if (apply) {
    applyResults = { created: [], updated: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const missingSectionsRepaired = [
    scenarioCreates.length ? "Value Creation Scenarios (valueOwners.scenario.*)" : null,
    mixPlans.creates.length || mixPlans.updates.length ? "Portfolio Mix" : null,
    momentumUpdates.length ? "Recent Momentum" : null,
    overviewUpdates.length ? "Overview scenario bodies" : null,
  ].filter(Boolean);

  return {
    brand: target,
    diagnosis: {
      contractScore: contractBefore.readinessScore,
      contractReady: contractBefore.brandExplorerRequiredSectionsReady,
      finalQaScore: qaBrand.scores?.overallNumeric,
      finalQaReadiness: qaBrand.scores?.overallActiveProfileReadiness,
      visualDefectCounts: visualDefectsBefore,
      governedPlatformReady: explorerGovernance.governedPlatformReady,
      activeProfileBlockers: (qaBrand.defects || [])
        .filter((d) => d.severity === "critical" || d.severity === "high")
        .map((d) => d.type || d.message),
    },
    rowsWouldCreate,
    rowsWouldUpdate,
    missingSectionsRepaired,
    pendingSourceReview,
    pendingImageReview,
    factClassification,
    sourceClassification,
    applyBlockers,
    dryRunClean: applyBlockers.length === 0 && hasWork,
    canApply,
    exactApplyCommand:
      applyBlockers.length === 0 && hasWork
        ? buildApplyCommand({ brands: brandSlug })
        : null,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    airtableModified,
    applyResults,
    expectedFinalQaAfterApply: projectedScores,
    expectedActiveProfileAfterApply:
      projectedScores.overallActiveProfileReadiness === "ready" && explorerGovernance.governedPlatformReady,
    separateFactStewardshipNeeded: factClassification.some(
      (f) => f.classification === "pending_fact_review"
    ),
    separateSourceCaptureNeeded: pendingSourceReview.some((q) => q.rowsNeeded > 0),
  };
}

export async function buildBrandExplorerChoiceExpansionPartialProfileBackfillWriterReport({
  brandsArg = TARGET_BRANDS.map((b) => b.slug).join(","),
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  noValidationClaim = false,
} = {}) {
  const targets = resolveTargetBrands(brandsArg);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandReports = [];
  for (const target of targets) {
    brandReports.push(
      await buildPerBrandReport(target, {
        apply,
        approveBatch,
        founderReviewed,
        noValidationClaim,
        baseId,
        apiKey,
      })
    );
    await new Promise((r) => setTimeout(r, 800));
  }

  const dryRunClean = brandReports.every((b) => b.dryRunClean);
  const report = {
    writerVersion: WRITER_VERSION,
    v31AWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (brandReports.some((b) => b.airtableModified) ? "apply" : "apply_blocked") : "dry-run",
    brands: targets,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    brandReports,
    dryRunClean,
    companyValidatedUntouched: brandReports.every((b) => b.companyValidatedUntouched),
    airtableModified: brandReports.some((b) => b.airtableModified),
    exactDryRunCommand: `npm run brand-explorer-choice-expansion-partial-profile-backfill-writer -- --brands ${targets.map((b) => b.slug).join(",")} --dry-run`,
    exactApplyCommand: dryRunClean
      ? buildApplyCommand({ brands: targets.map((b) => b.slug).join(",") })
      : null,
    perBrandApplyCommands: brandReports
      .filter((b) => b.dryRunClean && b.exactApplyCommand)
      .map((b) => ({ slug: b.brand.slug, command: b.exactApplyCommand })),
  };
  report.markdown = buildMarkdown(report);
  return report;
}

export function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Choice Expansion Partial Profile Backfill v${report.writerVersion}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- v31A exists: **yes**`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Dry-run clean: **${report.dryRunClean ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");

  for (const br of report.brandReports) {
    lines.push(`## ${br.brand.name} (\`${br.brand.slug}\`)`);
    lines.push(`- Contract score: **${br.diagnosis.contractScore}**`);
    lines.push(`- Final QA: **${br.diagnosis.finalQaScore}** (${br.diagnosis.finalQaReadiness})`);
    lines.push(`- Creates: **${br.rowsWouldCreate.length}** · Updates: **${br.rowsWouldUpdate.length}**`);
    lines.push(`- Expected QA after apply: **${br.expectedFinalQaAfterApply.overallNumeric}** (${br.expectedFinalQaAfterApply.overallActiveProfileReadiness})`);
    lines.push(`- Pending source review queues: **${br.pendingSourceReview.length}**`);
    lines.push(`- Pending image review: **${br.pendingImageReview.length}**`);
    lines.push(`- Apply blockers: ${br.applyBlockers.join(", ") || "none"}`);
    lines.push("");
  }

  lines.push("## Apply command");
  if (report.exactApplyCommand) {
    lines.push(`Batch: \`${report.exactApplyCommand}\``);
  } else if (report.perBrandApplyCommands?.length) {
    lines.push("Batch apply blocked — per-brand commands:");
    for (const item of report.perBrandApplyCommands) {
      lines.push(`- \`${item.slug}\`: \`${item.command}\``);
    }
  } else {
    lines.push("(none — dry-run not clean)");
  }
  return lines.join("\n");
}
