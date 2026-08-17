/**
 * Brand Explorer IHG-Family Active Profile Repair Batch Writer v30A.
 *
 * Dry-run / gated apply for Kimpton Hotels:
 * - create valueOwners.scenario.1–4 cards from approved aggregate narrative
 * - add gallery captions (images untouched)
 * - normalize writer-batch Sort Order drift
 * - classify pending facts generically (no auto-approval; separate stewardship gate)
 *
 * @see docs/data-intelligence/brand-explorer-ihg-family-active-profile-repair-writer-v30A.md
 */
import { fetchBrandBasics, fetchLiveState } from "./tribute-portfolio-package-pipeline.js";
import {
  ACTIVE_BRAND_AUDIT_TARGETS,
} from "./brand-explorer-portfolio-mix-context-normalization-writer.js";
import { WAVE1_EXPANSION_SLUGS } from "./brand-explorer-next-brand-selection-audit.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";
import { buildBrandExplorerRequiredSectionPopulationContractReport } from "./brand-explorer-required-section-population-contract.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";

export const WRITER_VERSION = "30A";
export const REPORT_JSON_NAME = "brand-explorer-ihg-family-active-profile-repair-writer.json";
export const REPORT_MD_NAME = "brand-explorer-ihg-family-active-profile-repair-writer.md";
export const DOC_MD_NAME = "brand-explorer-ihg-family-active-profile-repair-writer-v30A.md";

export const TARGET_BRANDS = Object.freeze([
  { slug: "kimpton", recordId: "recCKuXCmGvxHPfb3", name: "Kimpton Hotels" },
]);

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v30A-ihg-family-active-profile-repair";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-kimpton-copy-repair";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "tribute-portfolio",
  "curio-collection",
  "ascend",
  "radisson",
  "radisson-blu",
  ...WAVE1_EXPANSION_SLUGS,
]);

const IHG_BRAND_SLUGS = new Set(["kimpton"]);
const IHG_ALLOWED_COPY_PATTERN_IDS = new Set(["kimpton", "ihg", "one_rewards"]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-complete-build-kimpton.md",
  "reports/brand-explorer-complete-build-kimpton.json",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "reports/brand-explorer-choice-family-active-profile-repair-writer.md",
  "docs/brand-explorer-presentation-slots.md",
  "lib/partner-intelligence/brand-explorer-parent-aware-carryover.js",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
  "live Kimpton Brand Explorer Presentation rows",
  "live Kimpton Partner Facts",
  "live Kimpton Source Library records",
  "Tribute active-profile rows (quality reference only)",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-ihg-family-active-profile-repair-writer.js",
  "scripts/brand-explorer-ihg-family-active-profile-repair-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

const COMPANY_VALIDATION_BLOCK_RE =
  /company validated|validated by ihg|validated by choice|validated by radisson|approved by radisson|brand approved|company-approved|company approved|official sign-off/i;

const SHARED_COPY_SAFETY_PATTERNS = [
  { id: "tribute", re: /\btribute portfolio\b/i },
  { id: "marriott", re: /\bmarriott\b/i },
  { id: "bonvoy", re: /\bmarriott bonvoy\b|\bbonvoy\b/i },
  { id: "curio", re: /\bcurio collection\b/i },
  { id: "choice_hotels", re: /\bchoice hotels\b|\bchoice privileges\b/i },
  { id: "hilton", re: /\bhilton honors\b|\bhilton\b/i },
  { id: "ihg", re: /\bihg\b/i },
  { id: "kimpton", re: /\bkimpton\b/i },
  { id: "one_rewards", re: /\bihg one rewards\b|\bone rewards\b/i },
  { id: "company_validated", re: COMPANY_VALIDATION_BLOCK_RE },
  { id: "source_data", re: /\bsource data\b/i },
  { id: "metadata", re: /\bmetadata\b/i },
  { id: "consumer_site", re: /\bconsumer site\b/i },
  { id: "internal", re: /\binternal extraction\b|\bpaste into airtable\b/i },
  { id: "fdd", re: /\bfranchise disclosure document\b|\bitem\s*19\b/i },
  { id: "extraction", re: /\bsource capture\b|\bextraction run\b/i },
];

export const BRAND_REPAIR_CONFIG = {
  kimpton: {
    valueScenarioPackages: [
      {
        slotKey: "valueOwners.scenario.1",
        sort: 0,
        title: "Urban Lifestyle Conversion",
        body:
          "Independent or soft-brand urban conversion needing recognizable lifestyle retail and restaurant-forward economics—Kimpton fits when the asset can deliver design-led full-service guest experience and owners need IHG One Rewards distribution without surrendering neighborhood character.",
      },
      {
        slotKey: "valueOwners.scenario.2",
        sort: 1,
        title: "Gateway Design-Led Development",
        body:
          "New construction or adaptive reuse in gateway markets where design-led full-service competes on experience and ADR—Kimpton works when public spaces, F&B identity, and guest rooms can meet Kimpton design standards within IHG development milestones.",
      },
      {
        slotKey: "valueOwners.scenario.3",
        sort: 2,
        title: "IHG Lifestyle Portfolio Standardization",
        body:
          "Portfolio owners standardizing on IHG lifestyle after conversion PIP and design narrative alignment—Kimpton competes when sponsors need a recognizable lifestyle flag across mixed-use or multi-asset portfolios and can staff restaurant-forward operations.",
      },
      {
        slotKey: "valueOwners.scenario.4",
        sort: 3,
        title: "Third-Party Operator–Led",
        body:
          "Assets run by experienced third-party operators with full-service lifestyle depth—Kimpton suits sponsors when management can run Kimpton design compliance, social F&B, and IHG systems cutover without constant waivers, while IHG development approves brand milestones.",
      },
    ],
    galleryCaptions: {
      "materials.gallery.1":
        "Kimpton Las Mercedes Santo Domingo public spaces—Colonial City lifestyle boutique reference for urban conversion planning: rooftop activation, courtyard circulation, and restaurant-forward social zones typical of Kimpton collection properties.",
      "materials.gallery.2":
        "Kimpton Seafire Resort + Spa food and drink—Seven Mile Beach resort F&B reference for conversion scoping: beachfront dining, bar program, and social spaces owners should align to Kimpton lifestyle expectations.",
      "materials.gallery.3":
        "Kimpton Grand Roatán Resort + Spa meetings and events—Western Caribbean resort group and event product reference for owners underwriting meetings infrastructure and resort-scale public-space capex.",
      "materials.gallery.4":
        "Kimpton Mas Olas Resort & Spa luxury suite—Baja Sur Pacific resort wellness and suite experience reference for footprint planning: sleep zone, casegoods, and spa-adjacent guest-room expectations.",
      "materials.gallery.5":
        "Kimpton Virgilio guest bath—Mexico City Polanquito adaptive-reuse boutique room reference for conversion PIP scoping: fixtures, finishes, and accessibility band within historic fabric constraints.",
      "materials.gallery.6":
        "Kimpton lifestyle design detail—restaurant-forward F&B, wine hour, and design-led guestrooms illustrating the social and aesthetic cues owners should protect when underwriting Kimpton conversion economics.",
    },
    copyRepairTargets: [],
    sortOrderRepairSlotKeys: [
      "standards.requirement",
      "standards.questions",
      "footprint.geo_intro",
      "footprint.growth_themes",
      "footprint.editorial",
      "standards.deal_inputs",
      "standards.conversion",
    ],
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

function normalizeBody(v) {
  return nz(v).replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
}

function wordCount(text) {
  return nz(text).split(/\s+/).filter(Boolean).length;
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

export function isLikelyWriterBatchSortOrder(sortOrder) {
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
  const raw = nz(brandsArg || TARGET_BRANDS.map((b) => b.slug).join(","));
  const slugs = raw
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
  const resolved = [];
  for (const slug of slugs) {
    if (PROTECTED_BRAND_SLUGS.includes(slug)) {
      throw new Error(`Brand ${slug} is protected and cannot be modified by v30A`);
    }
    const meta =
      TARGET_BRANDS.find((b) => b.slug === slug) ||
      ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.slug === slug || b.recordId === slug);
    if (!meta || !TARGET_BRANDS.some((b) => b.slug === meta.slug)) {
      throw new Error(`v30A supports Kimpton only; got: ${slug}`);
    }
    if (!resolved.some((b) => b.slug === meta.slug)) resolved.push(meta);
  }
  if (!resolved.length) throw new Error("No target brands resolved");
  return resolved;
}

export function scanCopySafety(text, brandSlug) {
  const issues = [];
  const isIhgBrand = IHG_BRAND_SLUGS.has(brandSlug);
  for (const pattern of SHARED_COPY_SAFETY_PATTERNS) {
    if (isIhgBrand && IHG_ALLOWED_COPY_PATTERN_IDS.has(pattern.id)) continue;
    if (pattern.re.test(nz(text))) issues.push(pattern.id);
  }
  const config = BRAND_REPAIR_CONFIG[brandSlug];
  for (const pattern of config?.forbiddenInProposedCopy || []) {
    if (pattern.test(nz(text))) issues.push("forbidden_brand_phrase");
  }
  return issues;
}

export function classifyIhgFamilyPendingFact(fact, brandSlug) {
  const field = nz(fact.fieldName);
  const value = nz(fact.approvedValue || fact.normalizedValue || fact.extractedValue);
  const status = nz(fact.humanReviewStatus);
  if (status !== "Pending") return { classification: "not_pending", shouldNotSurfaceExternally: false };

  if (wordCount(value) < 5) {
    return {
      classification: "needs_founder_review",
      rationale: "Extract is too thin to approve without founder rewrite.",
      shouldNotSurfaceExternally: true,
      approveReady: false,
      recommendedAction: "needs_founder_review",
    };
  }
  if (/be\.(footprint|overview|loyalty|standards|economics)\./i.test(field) && wordCount(value) < 12) {
    return {
      classification: "needs_source_confirmation",
      rationale: `Pending ${field} extract needs source confirmation before approval.`,
      shouldNotSurfaceExternally: true,
      approveReady: false,
      recommendedAction: "needs_source_confirmation",
    };
  }
  if (/be\.overview\.(whyValue|typicalUseCase)/i.test(field)) {
    return {
      classification: "needs_founder_review",
      rationale: "Overview extract is partial or placeholder—requires rewritten owner-facing copy.",
      shouldNotSurfaceExternally: true,
      approveReady: false,
      recommendedAction: "needs_founder_review",
    };
  }
  return {
    classification: "needs_source_confirmation",
    rationale: `Pending fact for ${brandSlug} needs generic stewardship review—no auto-approval in v30A visual/copy writer.`,
    shouldNotSurfaceExternally: true,
    approveReady: false,
    recommendedAction: "needs_source_confirmation",
  };
}

function getBrandConfig(slug) {
  const config = BRAND_REPAIR_CONFIG[slug];
  if (!config) throw new Error(`No repair config for brand ${slug}`);
  return config;
}

function proposeScenarioCreates(existingRows, brandRecordId, brandName, brandSlug) {
  const { valueScenarioPackages } = getBrandConfig(brandSlug);
  const creates = [];
  for (const pkg of valueScenarioPackages) {
    const live = existingRows.find((r) => r.slotKey === pkg.slotKey);
    if (live) continue;
    const issues = scanCopySafety(`${pkg.title}\n${pkg.body}`, brandSlug);
    if (issues.length) continue;
    if (wordCount(pkg.body) < 15) continue;
    creates.push({
      action: "create",
      slotKey: pkg.slotKey,
      currentTitle: "",
      currentBody: "",
      proposedTitle: pkg.title,
      proposedBody: pkg.body,
      fixReason: "missing_valueOwners_scenario_card",
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

function proposeGalleryUpdates(existingRows, brandRecordId, brandName, brandSlug) {
  const { galleryCaptions } = getBrandConfig(brandSlug);
  const updates = [];
  for (const [slotKey, proposedBody] of Object.entries(galleryCaptions)) {
    const live = existingRows.find((r) => r.slotKey === slotKey);
    if (!live) continue;
    if (hasVal(live.body) && wordCount(live.body) >= 12) continue;
    const issues = scanCopySafety(`${live.title}\n${proposedBody}`, brandSlug);
    if (issues.length) continue;
    updates.push({
      action: "update",
      recordId: live.recordId,
      slotKey,
      currentTitle: live.title,
      currentBody: live.body,
      proposedTitle: live.title,
      proposedBody,
      fixReason: "title_only_gallery_caption",
      imageUntouched: live.hasImage,
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
  const { copyRepairTargets } = getBrandConfig(brandSlug);
  const updates = [];
  for (const target of copyRepairTargets) {
    const candidates = existingRows.filter((r) => r.slotKey === target.slotKey);
    for (const live of candidates) {
      if (target.titleMatch && !target.titleMatch.test(live.title)) continue;
      const body = normalizeBody(live.body);
      if (!target.match.test(body)) continue;
      const proposedBody = body.replace(target.match, target.replace);
      if (proposedBody === body) continue;
      const issues = scanCopySafety(proposedBody, brandSlug);
      if (issues.length) continue;
      updates.push({
        action: "update",
        recordId: live.recordId,
        slotKey: live.slotKey,
        currentTitle: live.title,
        currentBody: body,
        proposedTitle: live.title,
        proposedBody,
        fixReason: target.reason,
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

function proposeSortOrderRepairs(existingRows, brandRecordId, brandName, brandSlug) {
  const config = getBrandConfig(brandSlug);
  const explicitKeys = new Set(config.sortOrderRepairSlotKeys || []);
  const targetSort = 0;
  const updates = [];
  for (const live of existingRows) {
    const matchesExplicit = explicitKeys.has(live.slotKey);
    const matchesBatchDrift = isLikelyWriterBatchSortOrder(live.sortOrder);
    if (!matchesExplicit && !matchesBatchDrift) continue;
    const currentSort = live.sortOrder == null ? null : Number(live.sortOrder);
    if (currentSort === targetSort) continue;
    updates.push({
      action: "update",
      recordId: live.recordId,
      slotKey: live.slotKey,
      currentTitle: live.title,
      currentBody: live.body,
      proposedTitle: live.title,
      proposedBody: live.body,
      fixReason: "normalize_writer_batch_sort_order",
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

function estimatePostApplyScores({
  contractScore,
  pendingFactsCount,
  criticalRemaining,
  highRemaining,
  visualScoreProjected,
  carryoverScoreProjected = 100,
}) {
  const sourceGovernanceScore = Math.max(0, 100 - pendingFactsCount * 3);
  const visualCompletenessScore = Math.max(0, 100 - criticalRemaining * 20 - highRemaining * 8);
  const presentationQualityScore = Math.max(0, Math.round(visualScoreProjected));
  const overallNumeric = Math.round(
    contractScore * 0.25 +
      presentationQualityScore * 0.2 +
      carryoverScoreProjected * 0.15 +
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
  } else if (highRemaining > 0) {
    readiness = "almost_ready";
  }
  return {
    overallNumeric,
    overallActiveProfileReadiness: readiness,
    sourceGovernanceScore,
    visualCompletenessScore,
    presentationQualityScore,
    brandCarryoverRiskScore: carryoverScoreProjected,
  };
}

export function buildApplyCommand({ brands = TARGET_BRANDS.map((b) => b.slug).join(",") } = {}) {
  return [
    "npm run brand-explorer-ihg-family-active-profile-repair-writer --",
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
  const visualBefore = await buildBrandExplorerVisualDisplayDefectAuditReport({
    brandIdOrName: target.slug,
  }).catch(() => null);
  const contractBefore = await buildBrandExplorerRequiredSectionPopulationContractReport({
    brandIdOrName: target.slug,
  }).catch(() => ({
    readinessScore: 100,
    brandExplorerRequiredSectionsReady: true,
  }));
  const finalQaBefore = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.slug,
  }).catch(() => null);

  const qaBrand = finalQaBefore?.brandReports?.[0] || {};
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const explorerFacts = (liveState.facts || []).filter(
    (f) => nz(f.explorerType) === "Brand Explorer" || nz(f.fieldName).startsWith("be.")
  );
  const pendingFacts = explorerFacts.filter((f) => nz(f.humanReviewStatus) === "Pending");
  const pendingFactDiagnosis = pendingFacts.map((f) => ({
    factId: f.id,
    fieldName: f.fieldName,
    value: nz(f.approvedValue || f.normalizedValue || f.extractedValue),
    sourceRecordId: f.sourceRecordId,
    ...classifyIhgFamilyPendingFact(f, brandSlug),
  }));

  const scenarioCreates = proposeScenarioCreates(presentationRows, target.recordId, target.name, brandSlug);
  const galleryUpdates = proposeGalleryUpdates(presentationRows, target.recordId, target.name, brandSlug);
  const copyRepairs = proposeCopyRepairs(presentationRows, target.recordId, target.name, brandSlug);
  const sortRepairs = proposeSortOrderRepairs(presentationRows, target.recordId, target.name, brandSlug);

  const rowsWouldCreate = [...scenarioCreates];
  const rowsWouldUpdate = [...galleryUpdates, ...copyRepairs, ...sortRepairs];

  const carryoverBefore = qaBrand.carryoverFindings || [];
  const blockingCarryoverBefore = carryoverBefore.filter(
    (f) => f.carryoverClassification === "potential_wrong_brand_copy" || f.type === "brand_carryover"
  );

  const proposedCopySafety = [];
  for (const row of [...rowsWouldCreate, ...rowsWouldUpdate]) {
    if (row.fixReason === "normalize_writer_batch_sort_order") continue;
    const bodyChanged = normalizeBody(row.currentBody) !== normalizeBody(row.proposedBody);
    const titleChanged = nz(row.currentTitle) !== nz(row.proposedTitle);
    if (!bodyChanged && !titleChanged) continue;
    const combined = `${row.proposedTitle || ""}\n${row.proposedBody || ""}`;
    const issues = scanCopySafety(combined, brandSlug);
    if (issues.length) proposedCopySafety.push({ slotKey: row.slotKey, issues });
  }

  const applyBlockers = [];
  if (proposedCopySafety.length) {
    applyBlockers.push(`proposed_copy_safety_issues:${proposedCopySafety.length}`);
  }
  const config = getBrandConfig(brandSlug);
  const missingScenarioSlots = config.valueScenarioPackages
    .map((p) => p.slotKey)
    .filter((k) => !presentationRows.some((r) => r.slotKey === k));
  const projectedScenarioComplete =
    missingScenarioSlots.length === 0 || scenarioCreates.length === missingScenarioSlots.length;
  if (!projectedScenarioComplete) applyBlockers.push("valueOwners_scenario_slots_still_missing");

  for (const row of rowsWouldCreate) {
    if (wordCount(row.proposedBody) < 15) applyBlockers.push(`${row.slotKey}:thin_scenario_body`);
  }
  for (const row of galleryUpdates) {
    if (!hasVal(row.proposedBody)) applyBlockers.push(`${row.slotKey}:gallery_still_title_only`);
  }

  const applyGatesReady = apply && approveBatch && founderReviewed && noValidationClaim;
  const hasWork = rowsWouldCreate.length > 0 || rowsWouldUpdate.length > 0;
  const canApply = applyGatesReady && applyBlockers.length === 0 && hasWork;

  let airtableModified = false;
  let applyResults = null;
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
      if (!res.ok) {
        errors.push({ action: "create", slotKey: row.slotKey, message: json.error?.message || res.status });
      } else {
        created.push({ recordId: json.id, slotKey: row.slotKey });
      }
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
        errors.push({
          action: "update",
          slotKey: row.slotKey,
          recordId: row.recordId,
          message: json.error?.message || res.status,
        });
      } else {
        updated.push({ recordId: row.recordId, slotKey: row.slotKey, fixReason: row.fixReason });
      }
      await new Promise((r) => setTimeout(r, 220));
    }
    airtableModified = (created.length > 0 || updated.length > 0) && errors.length === 0;
    applyResults = { created, updated, errors };
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(target.recordId));
  } else if (apply) {
    applyResults = { created: [], updated: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const visualDefectsBefore = visualBefore?.defectCounts ||
    qaBrand.defectCounts || {
      total: 0,
      critical: 0,
      high: 0,
      medium: 0,
      titleOnlyOrThin: 0,
      sortOrder: 0,
    };

  const finalQaScoresBefore = qaBrand.scores || {
    overallNumeric: null,
    overallActiveProfileReadiness: "not_ready",
    visualCompletenessScore: 0,
    sourceGovernanceScore: 91,
    brandCarryoverRiskScore: 50,
  };

  const copyRepairCount = copyRepairs.length;
  const projectedCritical = Math.max(0, (visualDefectsBefore.critical || 0) - copyRepairCount);
  const projectedHigh = Math.max(
    0,
    (visualDefectsBefore.high || 0) -
      scenarioCreates.length -
      galleryUpdates.filter((r) => !hasVal(r.currentBody)).length -
      copyRepairCount
  );
  const projectedVisualScore = Math.max(
    0,
    100 - projectedCritical * 15 - projectedHigh * 8 - (sortRepairs.length > 0 ? 0 : 3)
  );
  const projectedCarryoverScore = Math.min(
    100,
    (finalQaScoresBefore.brandCarryoverRiskScore || 50) + copyRepairCount * 25
  );

  const projectedScores = estimatePostApplyScores({
    contractScore: contractBefore.readinessScore ?? 100,
    pendingFactsCount: pendingFacts.length,
    criticalRemaining: projectedCritical,
    highRemaining: projectedHigh,
    visualScoreProjected: projectedVisualScore,
    carryoverScoreProjected: projectedCarryoverScore,
  });

  const dryRunClean = applyBlockers.length === 0 && hasWork;
  const separateFactGovernanceNeeded = pendingFacts.length > 0;
  const expectedActiveProfileAfterApply =
    projectedScores.overallActiveProfileReadiness === "ready" && !separateFactGovernanceNeeded;

  return {
    brand: {
      slug: target.slug,
      name: target.name,
      recordId: target.recordId,
      parentCompany: nz(brandBasicsBefore?.fields?.["Parent Company"]),
    },
    diagnosis: {
      finalQaScoreBefore: finalQaScoresBefore.overallNumeric,
      finalQaReadinessBefore: finalQaScoresBefore.overallActiveProfileReadiness,
      contractScore: contractBefore.readinessScore ?? 100,
      contractReady: contractBefore.brandExplorerRequiredSectionsReady ?? true,
      visualDefectsBefore,
      criticalDefectsBefore: visualDefectsBefore.critical || 0,
      highDefectsBefore: visualDefectsBefore.high || 0,
      titleOnlyOrThinBefore: visualDefectsBefore.titleOnlyOrThin || 0,
      missingValueOwnersScenarioSlots: missingScenarioSlots,
      pendingFactsBefore: pendingFacts.length,
      sourceCount: (liveState.sources || []).length,
      approvedExplorerSources: (liveState.sources || []).filter((s) => nz(s.approvedForExplorerUse) === "Yes")
        .length,
      blockingCarryoverBefore,
      activeProfileBlockers: [
        ...(projectedScenarioComplete ? [] : ["missing_valueOwners_scenario_cards"]),
        ...(galleryUpdates.length ? [] : visualDefectsBefore.titleOnlyOrThin ? ["thin_gallery_captions"] : []),
        ...(blockingCarryoverBefore.length ? ["carryover_copy_risk"] : []),
        ...(pendingFacts.length ? ["pending_facts_governance"] : []),
        ...(visualDefectsBefore.sortOrder ? ["sort_order_drift"] : []),
      ],
      nextRepairsRequired: [
        "Create valueOwners.scenario.1–4 presentation rows with owner-facing bodies",
        "Add materials.gallery.1–6 captions (keep existing images)",
        "Normalize writer-batch Sort Order on editorial/standards rows",
        "Classify pending facts generically—no auto-approval in this visual/copy writer",
        "Run separate IHG pending-fact stewardship writer after v30A visual repairs",
      ],
    },
    pendingFactsDiagnosis: pendingFactDiagnosis,
    pendingFactsStewardshipNote:
      pendingFacts.length > 0
        ? `${pendingFacts.length} pending facts classified generically; separate stewardship apply required—pending facts do not block v30A visual/copy apply.`
        : null,
    valueCreationScenarioRepairPlan: {
      strategy: "create_missing_valueOwners_scenario_slots_from_aggregate_narrative",
      packages: config.valueScenarioPackages,
      rowsToCreate: scenarioCreates,
    },
    sourceEvidenceRepairPlan: {
      strategy: "use_existing_gallery_images_only",
      imagesAutoMaterialized: false,
      galleryRowsToUpdate: galleryUpdates,
      note: "Gallery rows already have approved images; writer adds captions only.",
    },
    copySafetyRepairs: copyRepairs,
    sortOrderRepairs: sortRepairs,
    rowsWouldUpdate,
    rowsWouldCreate,
    beforeAfterCopy: [...rowsWouldCreate, ...rowsWouldUpdate].map((row) => ({
      action: row.action,
      recordId: row.recordId || null,
      slotKey: row.slotKey,
      fixReason: row.fixReason,
      before: { title: row.currentTitle || "", body: row.currentBody || "" },
      after: { title: row.proposedTitle || "", body: row.proposedBody || "" },
    })),
    proposedCopySafetyIssues: proposedCopySafety,
    applyBlockers,
    dryRunClean,
    canApply,
    hasWork,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    ihgValidationImplied: false,
    airtableModified,
    applyResults,
    projectedFinalQa: {
      ...projectedScores,
      criticalDefectsAfter: projectedCritical,
      highDefectsAfter: projectedHigh,
      titleOnlyOrThinAfter: Math.max(
        0,
        (visualDefectsBefore.titleOnlyOrThin || 0) - scenarioCreates.length - galleryUpdates.length
      ),
      pendingFactsAfter: pendingFacts.length,
    },
    expectedActiveProfileAfterApply,
    separateFactGovernanceApplyNeeded: separateFactGovernanceNeeded,
    exactDryRunCommand: `npm run brand-explorer-ihg-family-active-profile-repair-writer -- --brands ${brandSlug} --dry-run`,
  };
}

export async function buildBrandExplorerIhgFamilyActiveProfileRepairWriterReport({
  brandsArg = TARGET_BRANDS.map((b) => b.slug).join(","),
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  noValidationClaim = false,
  stopOnCritical = false,
} = {}) {
  const targets = resolveTargetBrands(brandsArg);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandReports = [];
  let halted = false;
  let haltReason = "";

  for (const target of targets) {
    if (halted) {
      brandReports.push({
        brand: target,
        skipped: true,
        skipReason: haltReason,
      });
      continue;
    }

    const report = await buildPerBrandReport(target, {
      apply,
      approveBatch,
      founderReviewed,
      noValidationClaim,
      baseId,
      apiKey,
    });
    brandReports.push(report);
    await new Promise((r) => setTimeout(r, 1500));

    if (
      stopOnCritical &&
      apply &&
      (report.diagnosis?.criticalDefectsBefore > 0 || report.applyBlockers?.length > 0)
    ) {
      halted = true;
      haltReason = `Stopped after ${target.slug}: critical defects or apply blockers remain`;
    }
  }

  const airtableModified = brandReports.some((b) => b.airtableModified);
  const companyValidatedUntouched = brandReports.every((b) => b.skipped || b.companyValidatedUntouched);
  const dryRunClean = brandReports.every((b) => b.skipped || b.dryRunClean);
  const brandsApplyReady = brandReports.filter((b) => !b.skipped && b.dryRunClean && b.hasWork);
  const exactApplyCommand = buildApplyCommand({ brands: targets.map((b) => b.slug).join(",") });

  const report = {
    writerVersion: WRITER_VERSION,
    v30AWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (airtableModified ? "apply" : "apply_blocked") : "dry-run",
    brandsRequested: targets.map((b) => b.slug),
    protectedBrandsUntouched: PROTECTED_BRAND_SLUGS,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    brandReports,
    companyValidatedUntouched: brandReports.every((b) => b.skipped || b.companyValidatedUntouched),
    summary: {
      brandsProcessed: brandReports.filter((b) => !b.skipped).length,
      brandsSkipped: brandReports.filter((b) => b.skipped).length,
      rowsWouldCreate: brandReports.reduce((n, b) => n + (b.rowsWouldCreate?.length || 0), 0),
      rowsWouldUpdate: brandReports.reduce((n, b) => n + (b.rowsWouldUpdate?.length || 0), 0),
      dryRunClean,
      airtableModified,
      companyValidatedUntouched,
      expectedActiveProfileReadyBrands: brandReports
        .filter((b) => b.expectedActiveProfileAfterApply)
        .map((b) => b.brand?.slug),
    },
    halted,
    haltReason,
    exactDryRunCommand: `npm run brand-explorer-ihg-family-active-profile-repair-writer -- --brands ${targets
      .map((b) => b.slug)
      .join(",")} --dry-run`,
    exactApplyCommand: brandsApplyReady.length > 0 ? exactApplyCommand : null,
    perBrandApplyCommands: brandReports
      .filter((b) => !b.skipped && b.dryRunClean && b.hasWork)
      .map((b) => buildApplyCommand({ brands: b.brand.slug })),
  };

  report.markdown = buildMarkdown(report);
  return report;
}

export function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer IHG-Family Active Profile Repair Writer v${WRITER_VERSION}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Brands: **${report.brandsRequested.join(", ")}**`);
  lines.push(`- v30A exists: **${report.v30AWriterExists ? "yes" : "no"}**`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push(`- Dry-run clean: **${report.summary.dryRunClean ? "yes" : "no"}**`);
  lines.push("");

  for (const br of report.brandReports) {
    if (br.skipped) {
      lines.push(`## ${br.brand?.slug || "unknown"} — SKIPPED`);
      lines.push(br.skipReason || "");
      lines.push("");
      continue;
    }
    lines.push(`## ${br.brand.name} (\`${br.brand.recordId}\`)`);
    lines.push("");
    lines.push(
      `- Final QA before: **${br.diagnosis.finalQaScoreBefore}** (${br.diagnosis.finalQaReadinessBefore})`
    );
    lines.push(
      `- Visual defects before: **${br.diagnosis.visualDefectsBefore.total}** (critical ${br.diagnosis.criticalDefectsBefore}, high ${br.diagnosis.highDefectsBefore}, titleOnlyOrThin ${br.diagnosis.titleOnlyOrThinBefore})`
    );
    lines.push(`- Pending facts: **${br.diagnosis.pendingFactsBefore}**`);
    if (br.pendingFactsStewardshipNote) {
      lines.push(`- Pending facts note: ${br.pendingFactsStewardshipNote}`);
    }
    lines.push(
      `- Expected Final QA after apply: **~${br.projectedFinalQa.overallNumeric}** (${br.projectedFinalQa.overallActiveProfileReadiness})`
    );
    lines.push(`- Expected active-profile ready: **${br.expectedActiveProfileAfterApply ? "yes" : "no"}**`);
    lines.push(`- Rows to create: **${br.rowsWouldCreate.length}**; rows to update: **${br.rowsWouldUpdate.length}**`);
    lines.push("");
    lines.push("### Pending facts");
    for (const f of br.pendingFactsDiagnosis) {
      lines.push(
        `- \`${f.factId}\` **${f.fieldName}** → ${f.classification} / ${f.recommendedAction || "review"} (${f.rationale})`
      );
    }
    lines.push("");
    lines.push("### Before / after copy");
    for (const row of br.beforeAfterCopy) {
      lines.push(`#### ${row.slotKey} (${row.action})`);
      lines.push(`- Fix: ${row.fixReason}`);
      if (row.before.body !== row.after.body || row.before.title !== row.after.title) {
        lines.push(`- Before: ${(row.before.body || row.before.title || "(empty)").slice(0, 200)}`);
        lines.push(`- After: ${(row.after.body || row.after.title || "").slice(0, 200)}`);
      }
    }
    if (br.applyBlockers?.length) {
      lines.push("");
      lines.push("### Apply blockers");
      for (const b of br.applyBlockers) lines.push(`- ${b}`);
    }
    lines.push("");
  }

  lines.push("## Exact apply command");
  lines.push("```bash");
  lines.push(report.exactApplyCommand || "(dry-run not clean or no work proposed)");
  lines.push("```");
  return lines.join("\n");
}
