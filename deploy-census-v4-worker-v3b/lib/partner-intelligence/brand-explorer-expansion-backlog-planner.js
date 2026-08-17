/**
 * Brand Explorer Expansion Backlog + Wave Planner v28B (read-only by default).
 *
 * Normalizes the 56-brand expansion backlog, matches live Brand Setup / Explorer data,
 * plans factory waves, and emits review-queue + complete-build commands.
 *
 * Guardrails: no Airtable writes unless --apply-create-backlog; never Company Validated.
 *
 * @see docs/data-intelligence/brand-explorer-expansion-backlog-planner-v28B.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { fetchAllRecordsRest } from "../../api/lib/operator-setup-new-base-read.js";
import { PARTNER_INTELLIGENCE_LINKS } from "../../api/lib/partner-intelligence-field-map.js";
import { ACTIVE_BRAND_AUDIT_TARGETS } from "./brand-explorer-portfolio-mix-context-normalization-writer.js";

export const PLANNER_VERSION = "v28B";
export const REPORT_JSON_NAME = "brand-explorer-expansion-backlog-planner.json";
export const REPORT_MD_NAME = "brand-explorer-expansion-backlog-planner.md";
export const DOC_MD_NAME = "brand-explorer-expansion-backlog-planner-v28B.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const BRAND_BASICS_TABLE = PARTNER_INTELLIGENCE_LINKS.brandBasics;
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

export const REVIEW_QUEUE_STATES = Object.freeze([
  "pending_source_review",
  "pending_fact_review",
  "pending_image_review",
  "pending_founder_copy_review",
  "pending_legal_sensitivity_review",
  "ready_for_apply",
  "active_profile_ready",
]);

export const FACTORY_BATCH_POLICY = Object.freeze({
  continueOnPendingImageReview: true,
  blockActivationUntilImageApproval: true,
  description:
    "Factory stages continue across brands when image approval is pending; image-dependent activation remains blocked per brand until images are approved.",
});

export const APPLY_CREATE_BACKLOG_FLAG = "--apply-create-backlog";

const FILES_READ = [
  "AGENTS.md",
  "lib/partner-intelligence/brand-explorer-portfolio-mix-context-normalization-writer.js",
  "lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js",
  "lib/dealality-master-todo/founder-project-plan-explorer-brand-inventory.js",
  "docs/brand-explorer-presentation-slots.md",
  "live Brand Setup - Brand Basics (read-only)",
  "live Brand Setup - Brand Explorer Presentation (read-only)",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-expansion-backlog-planner.js",
  "scripts/brand-explorer-expansion-backlog-planner.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

/** @typedef {{
 *   brandName: string,
 *   proposedSlug?: string,
 *   parentFamily: string,
 *   brandType: string,
 *   likelyUseCase: string,
 *   likelyFactoryWave: number,
 *   sourceCaptureComplexity: number,
 *   imageApprovalComplexity: number,
 *   factGovernanceComplexity: number,
 *   specialHandling?: string[],
 * }} BacklogSeed */

/** @type {BacklogSeed[]} */
export const EXPANSION_BACKLOG_SEEDS = [
  { brandName: "AC Hotels by Marriott", parentFamily: "Marriott (source-confirmed)", brandType: "lifestyle soft", likelyUseCase: "lifestyle", likelyFactoryWave: 2, sourceCaptureComplexity: 2, imageApprovalComplexity: 3, factGovernanceComplexity: 2 },
  { brandName: "Bunkhouse Hotels", parentFamily: "Independent / Hyatt-adjacent (source-confirmed)", brandType: "lifestyle collection", likelyUseCase: "lifestyle", likelyFactoryWave: 7, sourceCaptureComplexity: 4, imageApprovalComplexity: 4, factGovernanceComplexity: 4, specialHandling: ["independent_portfolio", "limited_franchise_fdd"] },
  { brandName: "Canopy by Hilton", parentFamily: "Hilton (source-confirmed)", brandType: "lifestyle soft", likelyUseCase: "lifestyle", likelyFactoryWave: 3, sourceCaptureComplexity: 2, imageApprovalComplexity: 3, factGovernanceComplexity: 2 },
  { brandName: "Courtyard by Marriott", parentFamily: "Marriott (source-confirmed)", brandType: "select service", likelyUseCase: "select service", likelyFactoryWave: 2, sourceCaptureComplexity: 2, imageApprovalComplexity: 2, factGovernanceComplexity: 2 },
  { brandName: "Holiday Inn Express", parentFamily: "IHG (source-confirmed)", brandType: "select service", likelyUseCase: "select service", likelyFactoryWave: 4, sourceCaptureComplexity: 2, imageApprovalComplexity: 2, factGovernanceComplexity: 3 },
  { brandName: "Hyatt Place", parentFamily: "Hyatt (source-confirmed)", brandType: "select service", likelyUseCase: "select service", likelyFactoryWave: 4, sourceCaptureComplexity: 2, imageApprovalComplexity: 2, factGovernanceComplexity: 2 },
  { brandName: "Hyatt Zilara", parentFamily: "Hyatt (source-confirmed)", brandType: "all-inclusive resort", likelyUseCase: "all-inclusive", likelyFactoryWave: 7, sourceCaptureComplexity: 4, imageApprovalComplexity: 5, factGovernanceComplexity: 5, specialHandling: ["adults_only_resort", "legal_sensitivity"] },
  { brandName: "Hyatt Ziva", parentFamily: "Hyatt (source-confirmed)", brandType: "all-inclusive resort", likelyUseCase: "all-inclusive", likelyFactoryWave: 7, sourceCaptureComplexity: 4, imageApprovalComplexity: 5, factGovernanceComplexity: 5, specialHandling: ["family_resort", "legal_sensitivity"] },
  { brandName: "Marriott Hotels", parentFamily: "Marriott (source-confirmed)", brandType: "full service flagship", likelyUseCase: "full service", likelyFactoryWave: 2, sourceCaptureComplexity: 3, imageApprovalComplexity: 3, factGovernanceComplexity: 3 },
  { brandName: "Motto by Hilton", parentFamily: "Hilton (source-confirmed)", brandType: "micro-lifestyle", likelyUseCase: "lifestyle", likelyFactoryWave: 3, sourceCaptureComplexity: 3, imageApprovalComplexity: 3, factGovernanceComplexity: 2 },
  { brandName: "Mercure", parentFamily: "Accor (source-confirmed)", brandType: "midscale premium", likelyUseCase: "economy/midscale", likelyFactoryWave: 5, sourceCaptureComplexity: 3, imageApprovalComplexity: 2, factGovernanceComplexity: 3 },
  { brandName: "Secrets Resorts & Spas", parentFamily: "Hyatt / AMR (source-confirmed)", brandType: "adults-only all-inclusive", likelyUseCase: "all-inclusive", likelyFactoryWave: 7, sourceCaptureComplexity: 4, imageApprovalComplexity: 5, factGovernanceComplexity: 5, specialHandling: ["adults_only_resort", "legal_sensitivity"] },
  { brandName: "Sheraton", parentFamily: "Marriott (source-confirmed)", brandType: "full service", likelyUseCase: "full service", likelyFactoryWave: 2, sourceCaptureComplexity: 3, imageApprovalComplexity: 3, factGovernanceComplexity: 3 },
  { brandName: "Sunscape Resorts & Spas", parentFamily: "Hyatt / AMR (source-confirmed)", brandType: "family all-inclusive", likelyUseCase: "all-inclusive", likelyFactoryWave: 7, sourceCaptureComplexity: 4, imageApprovalComplexity: 5, factGovernanceComplexity: 5, specialHandling: ["family_resort", "legal_sensitivity"] },
  { brandName: "Tapestry Collection by Hilton", parentFamily: "Hilton (source-confirmed)", brandType: "soft collection", likelyUseCase: "soft brand", likelyFactoryWave: 1, sourceCaptureComplexity: 2, imageApprovalComplexity: 3, factGovernanceComplexity: 2, specialHandling: ["sibling_active_curio"] },
  { brandName: "Voco Hotels", parentFamily: "IHG (source-confirmed)", brandType: "conversion soft", likelyUseCase: "soft brand", likelyFactoryWave: 4, sourceCaptureComplexity: 3, imageApprovalComplexity: 3, factGovernanceComplexity: 3 },
  { brandName: "NH Hotels", parentFamily: "Minor / NH (source-confirmed)", brandType: "upper midscale", likelyUseCase: "conversion", likelyFactoryWave: 5, sourceCaptureComplexity: 3, imageApprovalComplexity: 3, factGovernanceComplexity: 3 },
  { brandName: "The Leading Hotels of the World", parentFamily: "LHW consortium (source-confirmed)", brandType: "luxury collection", likelyUseCase: "luxury collection", likelyFactoryWave: 8, sourceCaptureComplexity: 5, imageApprovalComplexity: 4, factGovernanceComplexity: 5, specialHandling: ["independent_consortium", "legal_sensitivity", "no_single_parent_fdd"] },
  { brandName: "Small Luxury Hotels of the World", parentFamily: "SLH consortium (source-confirmed)", brandType: "luxury collection", likelyUseCase: "luxury collection", likelyFactoryWave: 8, sourceCaptureComplexity: 5, imageApprovalComplexity: 4, factGovernanceComplexity: 5, specialHandling: ["independent_consortium", "legal_sensitivity"] },
  { brandName: "Westin", parentFamily: "Marriott (source-confirmed)", brandType: "full service wellness", likelyUseCase: "full service", likelyFactoryWave: 2, sourceCaptureComplexity: 3, imageApprovalComplexity: 3, factGovernanceComplexity: 3 },
  { brandName: "Autograph Collection", parentFamily: "Marriott (source-confirmed)", brandType: "soft collection", likelyUseCase: "soft brand", likelyFactoryWave: 1, sourceCaptureComplexity: 2, imageApprovalComplexity: 3, factGovernanceComplexity: 2, specialHandling: ["sibling_active_tribute"] },
  { brandName: "Residence Inn by Marriott", parentFamily: "Marriott (source-confirmed)", brandType: "extended stay", likelyUseCase: "extended stay", likelyFactoryWave: 2, sourceCaptureComplexity: 2, imageApprovalComplexity: 2, factGovernanceComplexity: 2 },
  { brandName: "SpringHill Suites by Marriott", parentFamily: "Marriott (source-confirmed)", brandType: "all-suites select", likelyUseCase: "select service", likelyFactoryWave: 2, sourceCaptureComplexity: 2, imageApprovalComplexity: 2, factGovernanceComplexity: 2 },
  { brandName: "TownePlace Suites by Marriott", parentFamily: "Marriott (source-confirmed)", brandType: "extended stay", likelyUseCase: "extended stay", likelyFactoryWave: 2, sourceCaptureComplexity: 2, imageApprovalComplexity: 2, factGovernanceComplexity: 2 },
  { brandName: "Aloft Hotels", parentFamily: "Marriott (source-confirmed)", brandType: "lifestyle select", likelyUseCase: "lifestyle", likelyFactoryWave: 2, sourceCaptureComplexity: 2, imageApprovalComplexity: 3, factGovernanceComplexity: 2 },
  { brandName: "Moxy Hotels", parentFamily: "Marriott (source-confirmed)", brandType: "lifestyle select", likelyUseCase: "lifestyle", likelyFactoryWave: 2, sourceCaptureComplexity: 2, imageApprovalComplexity: 3, factGovernanceComplexity: 2 },
  { brandName: "Home2 Suites by Hilton", parentFamily: "Hilton (source-confirmed)", brandType: "extended stay", likelyUseCase: "extended stay", likelyFactoryWave: 3, sourceCaptureComplexity: 2, imageApprovalComplexity: 2, factGovernanceComplexity: 2 },
  { brandName: "Tru by Hilton", parentFamily: "Hilton (source-confirmed)", brandType: "select service", likelyUseCase: "select service", likelyFactoryWave: 3, sourceCaptureComplexity: 2, imageApprovalComplexity: 2, factGovernanceComplexity: 2 },
  { brandName: "Tempo by Hilton", parentFamily: "Hilton (source-confirmed)", brandType: "lifestyle select", likelyUseCase: "lifestyle", likelyFactoryWave: 3, sourceCaptureComplexity: 3, imageApprovalComplexity: 3, factGovernanceComplexity: 2 },
  { brandName: "Unbound Collection by Hyatt", parentFamily: "Hyatt (source-confirmed)", brandType: "soft collection", likelyUseCase: "soft brand", likelyFactoryWave: 4, sourceCaptureComplexity: 3, imageApprovalComplexity: 3, factGovernanceComplexity: 3 },
  { brandName: "avid hotels", parentFamily: "IHG (source-confirmed)", brandType: "economy select", likelyUseCase: "economy/midscale", likelyFactoryWave: 4, sourceCaptureComplexity: 2, imageApprovalComplexity: 2, factGovernanceComplexity: 2 },
  { brandName: "Even Hotels", parentFamily: "IHG (source-confirmed)", brandType: "wellness select", likelyUseCase: "select service", likelyFactoryWave: 4, sourceCaptureComplexity: 2, imageApprovalComplexity: 2, factGovernanceComplexity: 2 },
  { brandName: "Vignette Collection", parentFamily: "IHG (source-confirmed)", brandType: "soft collection", likelyUseCase: "soft brand", likelyFactoryWave: 4, sourceCaptureComplexity: 3, imageApprovalComplexity: 3, factGovernanceComplexity: 3 },
  { brandName: "Iberostar Selection", parentFamily: "Iberostar / IHG beachfront (source-confirmed)", brandType: "resort", likelyUseCase: "resort", likelyFactoryWave: 7, sourceCaptureComplexity: 4, imageApprovalComplexity: 4, factGovernanceComplexity: 4, specialHandling: ["resort_governance"] },
  { brandName: "Esplendor by Wyndham", parentFamily: "Wyndham (source-confirmed)", brandType: "soft boutique", likelyUseCase: "soft brand", likelyFactoryWave: 6, sourceCaptureComplexity: 3, imageApprovalComplexity: 3, factGovernanceComplexity: 3 },
  { brandName: "Dazzler by Wyndham", parentFamily: "Wyndham (source-confirmed)", brandType: "soft boutique", likelyUseCase: "soft brand", likelyFactoryWave: 6, sourceCaptureComplexity: 3, imageApprovalComplexity: 3, factGovernanceComplexity: 3 },
  { brandName: "Trademark Collection by Wyndham", parentFamily: "Wyndham (source-confirmed)", brandType: "soft collection", likelyUseCase: "soft brand", likelyFactoryWave: 6, sourceCaptureComplexity: 3, imageApprovalComplexity: 3, factGovernanceComplexity: 3 },
  { brandName: "Wyndham", parentFamily: "Wyndham (source-confirmed)", brandType: "full service flagship", likelyUseCase: "full service", likelyFactoryWave: 6, sourceCaptureComplexity: 3, imageApprovalComplexity: 3, factGovernanceComplexity: 3, specialHandling: ["parent_brand_name_collision_risk"] },
  { brandName: "Travelodge by Wyndham", parentFamily: "Wyndham (source-confirmed)", brandType: "economy", likelyUseCase: "economy/midscale", likelyFactoryWave: 6, sourceCaptureComplexity: 2, imageApprovalComplexity: 2, factGovernanceComplexity: 2 },
  { brandName: "MGallery Collection", parentFamily: "Accor (source-confirmed)", brandType: "soft collection", likelyUseCase: "soft brand", likelyFactoryWave: 5, sourceCaptureComplexity: 3, imageApprovalComplexity: 3, factGovernanceComplexity: 3 },
  { brandName: "ibis", parentFamily: "Accor (source-confirmed)", brandType: "economy", likelyUseCase: "economy/midscale", likelyFactoryWave: 5, sourceCaptureComplexity: 2, imageApprovalComplexity: 2, factGovernanceComplexity: 2 },
  { brandName: "Novotel", parentFamily: "Accor (source-confirmed)", brandType: "midscale", likelyUseCase: "economy/midscale", likelyFactoryWave: 5, sourceCaptureComplexity: 2, imageApprovalComplexity: 2, factGovernanceComplexity: 2 },
  { brandName: "Pullman", parentFamily: "Accor (source-confirmed)", brandType: "upscale meetings", likelyUseCase: "full service", likelyFactoryWave: 5, sourceCaptureComplexity: 3, imageApprovalComplexity: 3, factGovernanceComplexity: 3 },
  { brandName: "Four Points Flex by Sheraton", parentFamily: "Marriott (source-confirmed)", brandType: "economy select", likelyUseCase: "select service", likelyFactoryWave: 2, sourceCaptureComplexity: 3, imageApprovalComplexity: 2, factGovernanceComplexity: 2, specialHandling: ["sheraton_sub_brand"] },
  { brandName: "City Express by Marriott", parentFamily: "Marriott (source-confirmed)", brandType: "select service regional", likelyUseCase: "select service", likelyFactoryWave: 2, sourceCaptureComplexity: 3, imageApprovalComplexity: 2, factGovernanceComplexity: 3, specialHandling: ["cala_regional_focus"] },
  { brandName: "StudioRes", parentFamily: "Marriott (source-confirmed)", brandType: "extended stay micro", likelyUseCase: "extended stay", likelyFactoryWave: 2, sourceCaptureComplexity: 3, imageApprovalComplexity: 2, factGovernanceComplexity: 2 },
  { brandName: "Handwritten Collection", parentFamily: "IHG (source-confirmed)", brandType: "soft collection", likelyUseCase: "soft brand", likelyFactoryWave: 4, sourceCaptureComplexity: 3, imageApprovalComplexity: 3, factGovernanceComplexity: 3 },
  { brandName: "Suburban Studios", parentFamily: "Choice (source-confirmed)", brandType: "extended stay economy", likelyUseCase: "extended stay", likelyFactoryWave: 6, sourceCaptureComplexity: 2, imageApprovalComplexity: 2, factGovernanceComplexity: 2 },
  { brandName: "WoodSpring Suites", parentFamily: "Choice (source-confirmed)", brandType: "extended stay economy", likelyUseCase: "extended stay", likelyFactoryWave: 6, sourceCaptureComplexity: 2, imageApprovalComplexity: 2, factGovernanceComplexity: 2 },
  { brandName: "Mr & Mrs Smith", parentFamily: "Hyatt / independent (source-confirmed)", brandType: "luxury collection", likelyUseCase: "luxury collection", likelyFactoryWave: 8, sourceCaptureComplexity: 4, imageApprovalComplexity: 4, factGovernanceComplexity: 5, specialHandling: ["hyatt_acquisition", "legal_sensitivity"] },
  { brandName: "Hampton by Hilton", parentFamily: "Hilton (source-confirmed)", brandType: "select service", likelyUseCase: "select service", likelyFactoryWave: 3, sourceCaptureComplexity: 2, imageApprovalComplexity: 2, factGovernanceComplexity: 2 },
  { brandName: "Hilton Garden Inn", parentFamily: "Hilton (source-confirmed)", brandType: "select service", likelyUseCase: "select service", likelyFactoryWave: 3, sourceCaptureComplexity: 2, imageApprovalComplexity: 2, factGovernanceComplexity: 2 },
  { brandName: "Spark by Hilton", parentFamily: "Hilton (source-confirmed)", brandType: "economy select", likelyUseCase: "economy/midscale", likelyFactoryWave: 3, sourceCaptureComplexity: 2, imageApprovalComplexity: 2, factGovernanceComplexity: 2 },
  { brandName: "Radisson Individuals by Choice", parentFamily: "Choice / Radisson Americas (source-confirmed)", brandType: "soft collection", likelyUseCase: "soft brand", likelyFactoryWave: 1, sourceCaptureComplexity: 2, imageApprovalComplexity: 3, factGovernanceComplexity: 2, specialHandling: ["sibling_active_radisson"] },
  { brandName: "Everhome Suites", parentFamily: "Choice (source-confirmed)", brandType: "extended stay", likelyUseCase: "extended stay", likelyFactoryWave: 6, sourceCaptureComplexity: 2, imageApprovalComplexity: 2, factGovernanceComplexity: 2 },
  { brandName: "Design Hotels", parentFamily: "Marriott affiliate (source-confirmed)", brandType: "soft collection", likelyUseCase: "soft brand", likelyFactoryWave: 1, sourceCaptureComplexity: 3, imageApprovalComplexity: 3, factGovernanceComplexity: 3, specialHandling: ["sibling_active_tribute", "marriott_affiliate_not_flag"] },
];

const ACTIVE_SLUGS = new Set(ACTIVE_BRAND_AUDIT_TARGETS.map((b) => b.slug));
const ACTIVE_NAMES_NORM = new Map(
  ACTIVE_BRAND_AUDIT_TARGETS.map((b) => [normalizeBrandNameForMatch(b.name), b])
);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

export function slugifyBrandName(name) {
  return nz(name)
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

export function normalizeBrandNameForMatch(name) {
  return nz(name)
    .toLowerCase()
    .replace(/\s+by\s+.+$/i, "")
    .replace(/\s+collection\b/gi, "")
    .replace(/\s+hotels?\b/gi, "")
    .replace(/\s+resorts?\s*&\s*spas?/gi, "")
    .replace(/[^a-z0-9]/g, "");
}

function fieldStr(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (Array.isArray(v) && v.length) return fieldStr(v[0]);
  if (typeof v === "object" && v.name) return nz(v.name);
  return nz(v);
}

function overallComplexity(seed) {
  return (
    seed.sourceCaptureComplexity +
    seed.imageApprovalComplexity +
    seed.factGovernanceComplexity
  );
}

function computeProcessingPriority(seed, match) {
  let score = 100;
  score -= overallComplexity(seed) * 2;
  if (seed.likelyUseCase === "soft brand") score += 12;
  if (seed.likelyFactoryWave === 1) score += 8;
  if (match?.brandBasicsRecordId) score += 6;
  if ((match?.presentationRowCount || 0) > 0) score += 4;
  if ((match?.presentationRowCount || 0) > 20) score += 3;
  if (seed.specialHandling?.length) score -= seed.specialHandling.length * 4;
  return Math.round(score * 10) / 10;
}

function detectActiveConflicts(seed, proposedSlug) {
  const conflicts = [];
  const norm = normalizeBrandNameForMatch(seed.brandName);
  if (ACTIVE_SLUGS.has(proposedSlug)) {
    conflicts.push({ type: "active_slug_collision", slug: proposedSlug });
  }
  const activeByNorm = ACTIVE_NAMES_NORM.get(norm);
  if (activeByNorm) {
    conflicts.push({
      type: "active_name_collision",
      activeSlug: activeByNorm.slug,
      activeName: activeByNorm.name,
    });
  }
  for (const active of ACTIVE_BRAND_AUDIT_TARGETS) {
    const activeNorm = normalizeBrandNameForMatch(active.name);
    if (norm && activeNorm && norm === activeNorm && active.slug !== proposedSlug) {
      conflicts.push({
        type: "normalized_name_overlap",
        activeSlug: active.slug,
        activeName: active.name,
      });
    }
  }
  const siblingFlags = (seed.specialHandling || []).filter((f) => f.startsWith("sibling_active_"));
  for (const flag of siblingFlags) {
    const siblingSlug = flag.replace("sibling_active_", "").replace(/_/g, "-");
    const sibling = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.slug === siblingSlug || b.slug.includes(siblingSlug));
    if (sibling) {
      conflicts.push({
        type: "related_active_brand",
        activeSlug: sibling.slug,
        activeName: sibling.name,
        note: "Not a duplicate — factory ladder sibling; reuse parent-family patterns with brand-specific copy.",
      });
    }
  }
  return conflicts;
}

function assignReviewQueue(seed, match) {
  const queue = {
    currentState: "pending_source_review",
    parallelFlags: [],
    factoryMayContinue: true,
    activationBlocked: false,
    notes: [],
  };

  if (match?.isActiveProfileBrand) {
    queue.currentState = match.activeProfileReady ? "active_profile_ready" : "pending_image_review";
    queue.activationBlocked = !match.activeProfileReady;
    queue.notes.push("Already in six-brand active factory set.");
    return queue;
  }

  if ((match?.presentationRowCount || 0) > 50) {
    queue.currentState = "pending_fact_review";
    queue.parallelFlags.push("pending_source_review");
  } else if ((match?.presentationRowCount || 0) > 10) {
    queue.currentState = "pending_fact_review";
  }

  if (seed.imageApprovalComplexity >= 4) {
    queue.parallelFlags.push("pending_image_review");
    queue.activationBlocked = true;
    queue.notes.push("Image-dependent activation blocked until image approval.");
  } else if (seed.imageApprovalComplexity >= 3) {
    queue.parallelFlags.push("pending_image_review");
  }

  if (seed.factGovernanceComplexity >= 4) {
    queue.parallelFlags.push("pending_founder_copy_review");
  }

  if (seed.specialHandling?.some((f) => /legal|consortium|independent|fdd/i.test(f))) {
    queue.parallelFlags.push("pending_legal_sensitivity_review");
    queue.notes.push("Legal/transaction-sensitive positioning — no company sign-off implied.");
  }

  if (!match?.brandBasicsRecordId) {
    queue.notes.push("Brand Basics record missing — backlog create gated behind --apply-create-backlog.");
  }

  queue.factoryMayContinue = FACTORY_BATCH_POLICY.continueOnPendingImageReview;
  return queue;
}

function matchBrandInIndex(seed, proposedSlug, index) {
  const exact = index.byExactName.get(seed.brandName.toLowerCase());
  if (exact) return exact;

  const norm = normalizeBrandNameForMatch(seed.brandName);
  const byNorm = index.byNormName.get(norm);
  if (byNorm) return { ...byNorm, matchMethod: "normalized_name" };

  const bySlug = index.bySlug.get(proposedSlug);
  if (bySlug) return { ...bySlug, matchMethod: "slug" };

  const fuzzy = index.records.find((r) => {
    const rNorm = normalizeBrandNameForMatch(r.name);
    return rNorm && norm && (rNorm.includes(norm) || norm.includes(rNorm));
  });
  if (fuzzy) return { ...fuzzy, matchMethod: "fuzzy_name", fuzzy: true };

  return null;
}

async function fetchBrandIndex() {
  const records = await fetchAllRecordsRest(BRAND_BASICS_TABLE);
  const byExactName = new Map();
  const byNormName = new Map();
  const bySlug = new Map();
  const normalized = [];

  for (const rec of records) {
    const name = fieldStr(rec.fields?.["Brand Name"] || rec.fields?.brand_name);
    if (!name) continue;
    const slug = slugifyBrandName(name);
    const status = fieldStr(rec.fields?.["Brand Status"] || rec.fields?.Status);
    const parentCompany = fieldStr(rec.fields?.["Parent Company"]);
    const entry = {
      recordId: rec.id,
      name,
      slug,
      status,
      parentCompany,
      companyValidated: fieldStr(rec.fields?.["Company Validated"]),
    };
    normalized.push(entry);
    byExactName.set(name.toLowerCase(), entry);
    const norm = normalizeBrandNameForMatch(name);
    if (norm && !byNormName.has(norm)) byNormName.set(norm, entry);
    if (slug && !bySlug.has(slug)) bySlug.set(slug, entry);
  }

  return { records: normalized, byExactName, byNormName, bySlug };
}

async function fetchPresentationCounts() {
  const rows = await fetchAllRecordsRest(PRESENTATION_TABLE).catch(() => []);
  const byBrandName = new Map();
  const byRecordId = new Map();

  for (const row of rows) {
    const brandName = fieldStr(row.fields?.["Brand Name"] || row.fields?.Brand);
    const brandLink = row.fields?.Brand;
    const linkId = Array.isArray(brandLink) ? brandLink[0] : null;
    if (brandName) byBrandName.set(brandName, (byBrandName.get(brandName) || 0) + 1);
    if (linkId) byRecordId.set(linkId, (byRecordId.get(linkId) || 0) + 1);
  }

  return { byBrandName, byRecordId, totalRows: rows.length };
}

function buildNormalizedBrand(seed, index, presentation) {
  const proposedSlug = seed.proposedSlug || slugifyBrandName(seed.brandName);
  const liveMatch = matchBrandInIndex(seed, proposedSlug, index);
  const activeTarget = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.slug === proposedSlug);
  const presentationRowCount = liveMatch
    ? presentation.byRecordId.get(liveMatch.recordId) ||
      presentation.byBrandName.get(liveMatch.name) ||
      0
    : 0;

  const slugCollision = liveMatch && liveMatch.slug !== proposedSlug && !liveMatch.fuzzy;
  const existingSlugOwner = index.bySlug.get(proposedSlug);

  const match = {
    brandBasicsRecordId: liveMatch?.recordId || null,
    brandBasicsName: liveMatch?.name || null,
    brandBasicsStatus: liveMatch?.status || null,
    brandBasicsParentCompany: liveMatch?.parentCompany || null,
    presentationRowCount,
    hasBrandExplorerData: presentationRowCount > 0,
    matchMethod: liveMatch?.matchMethod || (liveMatch ? "exact" : "none"),
    fuzzyMatch: Boolean(liveMatch?.fuzzy),
    isActiveProfileBrand: Boolean(activeTarget),
    activeProfileReady: false,
    existsInBrandSetup: Boolean(liveMatch),
    isNewBrandRecord: !liveMatch,
    slugCollisionWithExisting: Boolean(
      existingSlugOwner && existingSlugOwner.recordId !== liveMatch?.recordId
    ),
    existingSlugOwner: existingSlugOwner
      ? { recordId: existingSlugOwner.recordId, name: existingSlugOwner.name }
      : null,
  };

  const conflicts = detectActiveConflicts(seed, proposedSlug);
  const reviewQueue = assignReviewQueue(seed, match);
  const processingPriority = computeProcessingPriority(seed, match);
  const complexityTotal = overallComplexity(seed);

  return {
    brandName: seed.brandName,
    proposedSlug,
    parentFamily: seed.parentFamily,
    brandType: seed.brandType,
    likelyUseCase: seed.likelyUseCase,
    likelyFactoryWave: seed.likelyFactoryWave,
    sourceCaptureComplexity: seed.sourceCaptureComplexity,
    imageApprovalComplexity: seed.imageApprovalComplexity,
    factGovernanceComplexity: seed.factGovernanceComplexity,
    overallComplexity: complexityTotal,
    suggestedProcessingPriority: processingPriority,
    specialHandling: seed.specialHandling || [],
    activeConflicts: conflicts,
    dataPresence: match,
    reviewQueue,
    needsBrandBasicsRecord: !match.brandBasicsRecordId,
  };
}

function buildWaves(brands) {
  const waveMap = new Map();
  for (const brand of brands) {
    const waveNum = brand.likelyFactoryWave;
    if (!waveMap.has(waveNum)) {
      waveMap.set(waveNum, {
        wave: waveNum,
        label: waveLabel(waveNum),
        brands: [],
      });
    }
    waveMap.get(waveNum).brands.push(brand);
  }

  const waves = [...waveMap.values()]
    .sort((a, b) => a.wave - b.wave)
    .map((w) => {
      const slugs = w.brands
        .sort((a, b) => b.suggestedProcessingPriority - a.suggestedProcessingPriority)
        .map((b) => b.proposedSlug);
      return {
        ...w,
        brandCount: w.brands.length,
        slugs,
        dryRunCommand: buildWaveDryRunCommand(slugs),
        brands: w.brands
          .sort((a, b) => b.suggestedProcessingPriority - a.suggestedProcessingPriority)
          .map((b) => ({
            brandName: b.brandName,
            proposedSlug: b.proposedSlug,
            priority: b.suggestedProcessingPriority,
            existsInBrandSetup: b.dataPresence.existsInBrandSetup,
          })),
      };
    });

  return waves;
}

function waveLabel(n) {
  const labels = {
    1: "Wave 1 — Soft-collection pilots (closest to active six)",
    2: "Wave 2 — Marriott ladder (select, extended, lifestyle)",
    3: "Wave 3 — Hilton select / extended stay",
    4: "Wave 4 — Hyatt + IHG lifestyle & select",
    5: "Wave 5 — Accor / NH midscale ladder",
    6: "Wave 6 — Wyndham + Choice extended stay",
    7: "Wave 7 — Resort / all-inclusive / lifestyle independents",
    8: "Wave 8 — Independent luxury collections (highest governance)",
  };
  return labels[n] || `Wave ${n}`;
}

export function buildWaveDryRunCommand(slugs) {
  const list = Array.isArray(slugs) ? slugs.filter(Boolean) : [];
  if (!list.length) return "";
  return `npm run brand-explorer-complete-build -- --brands ${list.join(",")} --dry-run --target-quality active-profile`;
}

async function previewCreateBacklog(brands, applyCreate) {
  const toCreate = brands.filter((b) => b.needsBrandBasicsRecord);
  const preview = toCreate.map((b) => ({
    brandName: b.brandName,
    proposedSlug: b.proposedSlug,
    parentFamily: b.parentFamily,
    payload: { "Brand Name": b.brandName },
    note: "Minimal stub only — parent company and status require founder confirmation before apply.",
  }));

  if (!applyCreate) {
    return {
      attempted: false,
      created: [],
      preview,
      message: "Dry-run — no Brand Basics records created. Pass --apply-create-backlog to create stubs.",
    };
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required for --apply-create-backlog");

  const created = [];
  for (const item of preview) {
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BRAND_BASICS_TABLE)}`;
    const res = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ records: [{ fields: item.payload }], typecast: true }),
    });
    const json = await res.json();
    if (!res.ok || json.error) {
      throw new Error(json.error?.message || `Failed to create ${item.brandName}`);
    }
    const rec = json.records?.[0];
    created.push({ brandName: item.brandName, recordId: rec?.id, fields: rec?.fields });
  }

  return {
    attempted: true,
    created,
    preview,
    message: `Created ${created.length} minimal Brand Basics stub(s). Company Validated not touched.`,
  };
}

export async function buildBrandExplorerExpansionBacklogPlannerReport(options = {}) {
  const dryRun = options.dryRun !== false;
  const applyCreate = Boolean(options.applyCreateBacklog);

  if (applyCreate && dryRun) {
    throw new Error("--apply-create-backlog requires explicit apply intent; do not combine with default dry-run-only mode.");
  }

  const [index, presentation] = await Promise.all([
    fetchBrandIndex(),
    fetchPresentationCounts(),
  ]);

  const brands = EXPANSION_BACKLOG_SEEDS.map((seed) =>
    buildNormalizedBrand(seed, index, presentation)
  ).sort((a, b) => b.suggestedProcessingPriority - a.suggestedProcessingPriority);

  const waves = buildWaves(brands);
  const existingCount = brands.filter((b) => b.dataPresence.existsInBrandSetup).length;
  const newCount = brands.filter((b) => b.dataPresence.isNewBrandRecord).length;
  const withExplorer = brands.filter((b) => b.dataPresence.hasBrandExplorerData).length;
  const recommendedFirst10 = brands.slice(0, 10).map((b) => ({
    rank: brands.indexOf(b) + 1,
    brandName: b.brandName,
    proposedSlug: b.proposedSlug,
    priority: b.suggestedProcessingPriority,
    existsInBrandSetup: b.dataPresence.existsInBrandSetup,
    presentationRows: b.dataPresence.presentationRowCount,
  }));

  const highestComplexity = [...brands]
    .sort((a, b) => b.overallComplexity - a.overallComplexity)
    .slice(0, 10)
    .map((b) => ({
      brandName: b.brandName,
      proposedSlug: b.proposedSlug,
      overallComplexity: b.overallComplexity,
      sourceCaptureComplexity: b.sourceCaptureComplexity,
      imageApprovalComplexity: b.imageApprovalComplexity,
      factGovernanceComplexity: b.factGovernanceComplexity,
    }));

  const specialHandling = brands.filter((b) => b.specialHandling.length > 0);

  const createResult = await previewCreateBacklog(brands, applyCreate && !dryRun);

  const reviewQueueSummary = {};
  for (const state of REVIEW_QUEUE_STATES) {
    reviewQueueSummary[state] = brands.filter((b) => b.reviewQueue.currentState === state).length;
  }

  return {
    plannerVersion: PLANNER_VERSION,
    v28BExists: true,
    generatedAt: new Date().toISOString(),
    mode: applyCreate && !dryRun ? "apply-create-backlog" : "dry-run",
    dryRun: !applyCreate || dryRun,
    airtableModified: createResult.created.length > 0,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    factoryBatchPolicy: FACTORY_BATCH_POLICY,
    activeBrandCount: ACTIVE_BRAND_AUDIT_TARGETS.length,
    backlogTotal: brands.length,
    existingBrandBasicsCount: existingCount,
    newBrandBasicsNeeded: newCount,
    brandsWithExplorerPresentation: withExplorer,
    totalBrandBasicsInBase: index.records.length,
    totalPresentationRowsInBase: presentation.totalRows,
    proposedWaves: waves,
    recommendedProcessingSequence: brands.map((b, i) => ({
      rank: i + 1,
      brandName: b.brandName,
      proposedSlug: b.proposedSlug,
      wave: b.likelyFactoryWave,
      priority: b.suggestedProcessingPriority,
    })),
    recommendedFirst10,
    highestComplexityBrands: highestComplexity,
    brandsNeedingSpecialHandling: specialHandling.map((b) => ({
      brandName: b.brandName,
      proposedSlug: b.proposedSlug,
      specialHandling: b.specialHandling,
      activeConflicts: b.activeConflicts,
    })),
    reviewQueueModel: {
      states: REVIEW_QUEUE_STATES,
      summary: reviewQueueSummary,
      brands: brands.map((b) => ({
        brandName: b.brandName,
        proposedSlug: b.proposedSlug,
        reviewQueue: b.reviewQueue,
      })),
    },
    waveCommands: waves.map((w) => ({
      wave: w.wave,
      label: w.label,
      command: w.dryRunCommand,
    })),
    backlogCreate: createResult,
    brands,
    guardrails: {
      noAirtableWritesInDryRun: true,
      noAutoImageApproval: true,
      noAutoFactApproval: true,
      noCompanyValidatedChanges: true,
      createBacklogGate: APPLY_CREATE_BACKLOG_FLAG,
    },
  };
}

export function buildBrandExplorerExpansionBacklogPlannerMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Expansion Backlog + Wave Planner ${report.plannerVersion}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Backlog total: **${report.backlogTotal}** brands`);
  lines.push(`- Existing Brand Basics matches: **${report.existingBrandBasicsCount}**`);
  lines.push(`- New Brand Basics needed: **${report.newBrandBasicsNeeded}**`);
  lines.push(`- Brands with Explorer presentation rows: **${report.brandsWithExplorerPresentation}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Orchestrator integration (v28C)");
  lines.push("Wave commands use `proposedSlug` inputs. `brand-explorer-complete-build` resolves them via `lib/partner-intelligence/brand-explorer-brand-target-resolver.js` (`expansion_backlog` source).");
  lines.push("");
  lines.push("## Factory batch policy");
  lines.push(report.factoryBatchPolicy.description);
  lines.push("");
  lines.push("## Proposed waves");
  for (const wave of report.proposedWaves) {
    lines.push(`### ${wave.label} (${wave.brandCount} brands)`);
    lines.push(`\`${wave.dryRunCommand}\``);
    lines.push("");
    lines.push("| Brand | Slug | Priority | In Brand Setup |");
    lines.push("| --- | --- | ---: | --- |");
    for (const b of wave.brands) {
      lines.push(
        `| ${b.brandName} | \`${b.proposedSlug}\` | ${b.priority} | ${b.existsInBrandSetup ? "yes" : "no"} |`
      );
    }
    lines.push("");
  }
  lines.push("## Recommended first 10 brands");
  lines.push("| Rank | Brand | Slug | Priority | Brand Setup | Presentation rows |");
  lines.push("| ---: | --- | --- | ---: | --- | ---: |");
  for (const b of report.recommendedFirst10) {
    const full = report.brands.find((x) => x.proposedSlug === b.proposedSlug);
    lines.push(
      `| ${b.rank} | ${b.brandName} | \`${b.proposedSlug}\` | ${b.priority} | ${b.existsInBrandSetup ? "yes" : "no"} | ${full?.dataPresence.presentationRowCount ?? 0} |`
    );
  }
  lines.push("");
  lines.push("## Highest complexity brands");
  lines.push("| Brand | Slug | Overall | Source | Image | Fact |");
  lines.push("| --- | --- | ---: | ---: | ---: | ---: |");
  for (const b of report.highestComplexityBrands) {
    lines.push(
      `| ${b.brandName} | \`${b.proposedSlug}\` | ${b.overallComplexity} | ${b.sourceCaptureComplexity} | ${b.imageApprovalComplexity} | ${b.factGovernanceComplexity} |`
    );
  }
  lines.push("");
  lines.push("## Brands needing special handling");
  if (!report.brandsNeedingSpecialHandling.length) {
    lines.push("_None_");
  } else {
    for (const b of report.brandsNeedingSpecialHandling) {
      lines.push(`- **${b.brandName}** (\`${b.proposedSlug}\`): ${b.specialHandling.join(", ")}`);
    }
  }
  lines.push("");
  lines.push("## Review queue summary");
  for (const [state, count] of Object.entries(report.reviewQueueModel.summary)) {
    lines.push(`- \`${state}\`: ${count}`);
  }
  lines.push("");
  lines.push("## Suggested commands by wave");
  for (const w of report.waveCommands) {
    lines.push(`- Wave ${w.wave}: \`${w.command}\``);
  }
  lines.push("");
  lines.push("## Guardrails");
  lines.push("- Dry-run default — no Airtable writes");
  lines.push(`- Brand Basics create gated: \`${APPLY_CREATE_BACKLOG_FLAG}\``);
  lines.push("- No automatic image or fact approval");
  lines.push("- Company Validated never modified by this planner");
  return lines.join("\n");
}
