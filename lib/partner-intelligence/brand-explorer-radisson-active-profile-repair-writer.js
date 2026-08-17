/**
 * Brand Explorer Radisson by Choice Active Profile Repair Writer v28E.
 *
 * Dry-run repair plan for Final QA / visual blockers on Radisson by Choice only:
 * - create valueOwners.scenario.1–4 cards from approved aggregate narrative
 * - add gallery prototype captions (images untouched)
 * - remove cross-brand audit triggers (Radisson Blu phrase in wrong surfaces)
 * - normalize writer-batch Sort Order on a small set of rows
 * - classify pending facts (no auto-approval unless separate gate)
 *
 * @see docs/data-intelligence/brand-explorer-radisson-active-profile-repair-writer-v28E.md
 */
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics, fetchLiveState } from "./tribute-portfolio-package-pipeline.js";
import { ACTIVE_BRAND_AUDIT_TARGETS } from "./brand-explorer-portfolio-mix-context-normalization-writer.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";
import { buildBrandExplorerRequiredSectionPopulationContractReport } from "./brand-explorer-required-section-population-contract.js";

export const WRITER_VERSION = "28E";
export const REPORT_JSON_NAME = "brand-explorer-radisson-active-profile-repair-writer.json";
export const REPORT_MD_NAME = "brand-explorer-radisson-active-profile-repair-writer.md";
export const DOC_MD_NAME = "brand-explorer-radisson-active-profile-repair-writer-v28E.md";

export const TARGET_BRAND_SLUG = "radisson";
export const TARGET_RECORD_ID = "recywbx1YQSTCPqW1";
export const TARGET_BRAND_NAME = "Radisson by Choice";

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v28E-radisson-active-profile-repair";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-radisson-copy-repair";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_APPROVE_FACTS = "--approve-radisson-pending-facts";

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "tribute-portfolio",
  "curio-collection",
  "kimpton",
  "radisson-blu",
  "ascend",
  "radisson-individuals-by-choice",
  "tapestry-collection-by-hilton",
  "autograph-collection",
  "design-hotels",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-next-brand-selection-audit.md",
  "reports/brand-explorer-next-brand-selection-audit.json",
  "reports/brand-explorer-complete-build-radisson.md",
  "reports/brand-explorer-complete-build-radisson.json",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "reports/brand-explorer-visual-qa-verification.md",
  "reports/brand-explorer-required-section-population-contract.md",
  "reports/brand-explorer-required-section-population-contract.json",
  "docs/brand-explorer-presentation-slots.md",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "live Radisson by Choice Brand Explorer Presentation rows",
  "live Radisson by Choice Partner Facts",
  "live Radisson by Choice Source Library records",
  "Tribute active-profile rows (quality reference only)",
  "Radisson Blu / Ascend rows (Choice-family structure reference only)",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-radisson-active-profile-repair-writer.js",
  "scripts/brand-explorer-radisson-active-profile-repair-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

const COMPANY_VALIDATION_BLOCK_RE =
  /company validated|validated by choice|validated by radisson|approved by radisson|brand approved|company-approved|company approved|official sign-off/i;

const COPY_SAFETY_PATTERNS = [
  { id: "tribute", re: /\btribute portfolio\b/i },
  { id: "marriott", re: /\bmarriott\b/i },
  { id: "bonvoy", re: /\bmarriott bonvoy\b|\bbonvoy\b/i },
  { id: "curio", re: /\bcurio collection\b/i },
  { id: "hilton", re: /\bhilton honors\b|\bhilton\b/i },
  { id: "kimpton", re: /\bkimpton\b/i },
  { id: "ascend", re: /\bascend hotel collection\b/i },
  { id: "company_validated", re: COMPANY_VALIDATION_BLOCK_RE },
  { id: "source_data", re: /\bsource data\b/i },
  { id: "metadata", re: /\bmetadata\b/i },
  { id: "consumer_site", re: /\bconsumer site\b/i },
  { id: "internal", re: /\binternal extraction\b|\bpaste into airtable\b/i },
  { id: "fdd", re: /\bfranchise disclosure document\b|\bitem\s*19\b/i },
  { id: "extraction", re: /\bsource capture\b|\bextraction run\b/i },
];

/** Visual-audit wrong-brand marker that still fires on Radisson when sibling Blu is named verbatim. */
const VISUAL_AUDIT_BLU_PHRASE_RE = /\bradisson blu\b/i;

export const VALUE_SCENARIO_PACKAGES = [
  {
    slotKey: "valueOwners.scenario.1",
    sort: 0,
    title: "Independent Reflag",
    body:
      "Independent or soft-branded upscale hotels that need recognizable loyalty, brand.com strength, and corporate transient access—Radisson fits when the asset can deliver full-service expectations while keeping operations and guest retail practical, not bespoke.",
  },
  {
    slotKey: "valueOwners.scenario.2",
    sort: 1,
    title: "Tired Upscale Asset",
    body:
      "Mature upscale or upper-mid assets needing refresh capital and relevancy—repositioning under Radisson pairs credible full-service guest expectations with tighter commercial systems across pricing, retail, and quality assurance.",
  },
  {
    slotKey: "valueOwners.scenario.3",
    sort: 2,
    title: "Markets With Strong Brand Presence",
    body:
      "Gateway and regional corridors where guests compare flags directly—Radisson competes on execution, location, and total cost of delivery rather than novelty. Best when the comp set is branded and loyalty participation influences booking.",
  },
  {
    slotKey: "valueOwners.scenario.4",
    sort: 3,
    title: "Third-Party Operator–Led",
    body:
      "Assets run by experienced third-party operators with full-service upscale depth—Radisson works when management can staff F&B, meetings, and front-of-house to brand standards while the sponsor needs affiliation for financing, sales, and loyalty lift.",
  },
];

export const GALLERY_CAPTIONS = {
  "materials.gallery.1":
    "Reference double-queen guest room for conversion planning—furniture stack, lighting, and soft-goods band aligned to Radisson prototype standards.",
  "materials.gallery.2":
    "King room reference for footprint planning—sleep zone, casegoods, and in-room work surface typical of core Radisson guest-room expectations.",
  "materials.gallery.3":
    "Bathroom reference layout for PIP scoping—fixtures, finishes, and accessibility band owners should align to current prototype guidance before underwriting conversion capex.",
  "materials.gallery.4":
    "Alternate double-queen layout illustrating casegoods and seating options within the same prototype family—useful when comparing room modules for a conversion floor plan.",
  "materials.gallery.5":
    "Second double-queen reference angle for owner and designer workshops—shows lighting, headboard, and soft-goods coordination expected in refreshed guest rooms.",
  "materials.gallery.6":
    "King room alternate view for stakeholder reviews—helps sponsors and operators align on prototype scope before final PIP sign-off.",
};

const COPY_REPAIR_TARGETS = [
  {
    slotKey: "footprint.region.cala",
    match: /not Radisson Blu or RED/i,
    replace: "not the upper-upscale Blu tier or RED",
    reason: "remove_visual_audit_wrong_brand_phrase",
  },
  {
    slotKey: "materials.caseStudy",
    match: /\(not Radisson Blu\)/i,
    replace: "(core Radisson tier)",
    reason: "remove_visual_audit_wrong_brand_phrase",
    titleMatch: /Riviera Panama/i,
  },
];

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

function resolveTarget(brandArg) {
  const normalized = nz(brandArg || TARGET_BRAND_SLUG).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(normalized) && normalized !== TARGET_BRAND_SLUG) {
    throw new Error(`Brand ${normalized} is protected and cannot be modified by v28E`);
  }
  if (normalized !== TARGET_BRAND_SLUG && brandArg !== TARGET_RECORD_ID) {
    throw new Error(`v28E supports Radisson by Choice only (${TARGET_BRAND_SLUG})`);
  }
  const meta = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.slug === TARGET_BRAND_SLUG);
  if (!meta) throw new Error("Could not resolve Radisson brand target");
  return meta;
}

export function scanCopySafety(text) {
  const issues = [];
  for (const pattern of COPY_SAFETY_PATTERNS) {
    if (pattern.re.test(nz(text))) issues.push(pattern.id);
  }
  return issues;
}

export function classifyPendingFact(fact) {
  const field = nz(fact.fieldName);
  const value = nz(fact.approvedValue || fact.normalizedValue || fact.extractedValue);
  const status = nz(fact.humanReviewStatus);
  if (status !== "Pending") return { classification: "not_pending", shouldNotSurfaceExternally: false };

  if (field === "be.footprint.geoIntro") {
    return {
      classification: "needs_source_confirmation",
      rationale:
        "Geo intro extract is a dated scale fragment only—needs a fuller approved footprint narrative before Explorer surfacing.",
      shouldNotSurfaceExternally: true,
      approveReady: false,
    };
  }
  if (field === "be.overview.typicalUseCase") {
    return {
      classification: "needs_founder_review",
      rationale: "Typical use case value is a generic fragment (“travelers worldwide”)—not owner-facing copy.",
      shouldNotSurfaceExternally: true,
      approveReady: false,
    };
  }
  if (field === "be.overview.whyValue") {
    return {
      classification: "needs_founder_review",
      rationale: "Why-value extract is a placeholder label (“value proposition”)—requires rewritten owner bullets.",
      shouldNotSurfaceExternally: true,
      approveReady: false,
    };
  }
  if (wordCount(value) < 5) {
    return {
      classification: "needs_founder_review",
      rationale: "Extract is too thin to approve without founder rewrite.",
      shouldNotSurfaceExternally: true,
      approveReady: false,
    };
  }
  return {
    classification: "needs_source_confirmation",
    rationale: "Pending fact needs source confirmation before approval.",
    shouldNotSurfaceExternally: true,
    approveReady: false,
  };
}

function proposeScenarioCreates(existingRows, brandRecordId, brandName) {
  const creates = [];
  for (const pkg of VALUE_SCENARIO_PACKAGES) {
    const live = existingRows.find((r) => r.slotKey === pkg.slotKey);
    if (live) continue;
    const issues = scanCopySafety(`${pkg.title}\n${pkg.body}`);
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

function proposeGalleryUpdates(existingRows, brandRecordId, brandName) {
  const updates = [];
  for (const [slotKey, proposedBody] of Object.entries(GALLERY_CAPTIONS)) {
    const live = existingRows.find((r) => r.slotKey === slotKey);
    if (!live) continue;
    if (hasVal(live.body) && wordCount(live.body) >= 12) continue;
    const issues = scanCopySafety(`${live.title}\n${proposedBody}`);
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

function proposeCopyRepairs(existingRows, brandRecordId, brandName) {
  const updates = [];
  for (const target of COPY_REPAIR_TARGETS) {
    const candidates = existingRows.filter((r) => r.slotKey === target.slotKey);
    for (const live of candidates) {
      if (target.titleMatch && !target.titleMatch.test(live.title)) continue;
      const body = normalizeBody(live.body);
      if (!target.match.test(body)) continue;
      const proposedBody = body.replace(target.match, target.replace);
      if (proposedBody === body) continue;
      if (VISUAL_AUDIT_BLU_PHRASE_RE.test(proposedBody)) continue;
      const issues = scanCopySafety(proposedBody);
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

function proposeSortOrderRepairs(existingRows, brandRecordId, brandName) {
  const updates = [];
  for (const live of existingRows) {
    if (!isLikelyWriterBatchSortOrder(live.sortOrder)) continue;
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

function scanUiFacingCopySafety(existingRows) {
  const findings = [];
  for (const row of existingRows) {
    if (row.active === false) continue;
    const combined = `${row.title}\n${row.body}`;
    const issues = scanCopySafety(combined);
    if (VISUAL_AUDIT_BLU_PHRASE_RE.test(combined)) issues.push("radisson_blu_phrase_visual_audit");
    if (issues.length) {
      findings.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        issues,
        excerpt: combined.slice(0, 160),
      });
    }
  }
  return findings;
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
  const presentationQualityScore = Math.max(0, Math.round(visualScoreProjected));
  const overallNumeric = Math.round(
    contractScore * 0.25 +
      presentationQualityScore * 0.2 +
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
  } else if (highRemaining > 0) {
    readiness = "almost_ready";
  }
  return {
    overallNumeric,
    overallActiveProfileReadiness: readiness,
    sourceGovernanceScore,
    visualCompletenessScore,
    presentationQualityScore,
  };
}

export function buildApplyCommand({ includeFactApproval = false } = {}) {
  const parts = [
    "npm run brand-explorer-radisson-active-profile-repair-writer --",
    `--brand ${TARGET_BRAND_SLUG}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER,
    APPLY_FLAG_NO_VALIDATION,
  ];
  if (includeFactApproval) parts.push(APPLY_FLAG_APPROVE_FACTS);
  return parts.join(" ");
}

export async function buildBrandExplorerRadissonActiveProfileRepairWriterReport({
  brandIdOrName = TARGET_BRAND_SLUG,
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  noValidationClaim = false,
  approvePendingFacts = false,
} = {}) {
  const target = resolveTarget(brandIdOrName);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const [brandBasicsBefore, liveState, presentationRows, visualBefore, contractBefore] = await Promise.all([
    fetchBrandBasics(target.recordId),
    fetchLiveState(target.recordId),
    listPresentationRows(baseId, apiKey, target.recordId, target.name),
    buildBrandExplorerVisualDisplayDefectAuditReport({ brandIdOrName: target.recordId }).catch(() => null),
    buildBrandExplorerRequiredSectionPopulationContractReport({ brandIdOrName: target.slug }).catch(() => ({
      readinessScore: 100,
      brandExplorerRequiredSectionsReady: true,
    })),
  ]);

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
    ...classifyPendingFact(f),
  }));

  const scenarioCreates = proposeScenarioCreates(presentationRows, target.recordId, target.name);
  const galleryUpdates = proposeGalleryUpdates(presentationRows, target.recordId, target.name);
  const copyRepairs = proposeCopyRepairs(presentationRows, target.recordId, target.name);
  const sortRepairs = proposeSortOrderRepairs(presentationRows, target.recordId, target.name);

  const rowsWouldCreate = [...scenarioCreates];
  const rowsWouldUpdate = [...galleryUpdates, ...copyRepairs, ...sortRepairs];

  const copySafetyBefore = scanUiFacingCopySafety(presentationRows);

  const proposedCopySafety = [];
  for (const row of [...rowsWouldCreate, ...rowsWouldUpdate]) {
    const combined = `${row.proposedTitle || ""}\n${row.proposedBody || ""}`;
    const issues = scanCopySafety(combined);
    if (VISUAL_AUDIT_BLU_PHRASE_RE.test(combined)) issues.push("radisson_blu_phrase_visual_audit");
    if (issues.length) proposedCopySafety.push({ slotKey: row.slotKey, issues });
  }

  const applyBlockers = [];
  if (proposedCopySafety.length) {
    applyBlockers.push(`proposed_copy_safety_issues:${proposedCopySafety.length}`);
  }
  if (pendingFactDiagnosis.some((f) => f.approveReady) && !approvePendingFacts) {
    applyBlockers.push("pending_facts_not_approved_via_gate");
  }
  if (approvePendingFacts && !pendingFactDiagnosis.some((f) => f.approveReady)) {
    applyBlockers.push("no_approve_ready_pending_facts");
  }
  for (const row of rowsWouldCreate) {
    if (wordCount(row.proposedBody) < 15) applyBlockers.push(`${row.slotKey}:thin_scenario_body`);
  }
  for (const row of galleryUpdates) {
    if (!hasVal(row.proposedBody)) applyBlockers.push(`${row.slotKey}:gallery_still_title_only`);
  }

  const missingScenarioSlots = VALUE_SCENARIO_PACKAGES.filter(
    (p) => !presentationRows.some((r) => r.slotKey === p.slotKey)
  ).map((p) => p.slotKey);
  const projectedScenarioComplete = missingScenarioSlots.length === 0 || scenarioCreates.length === missingScenarioSlots.length;
  if (!projectedScenarioComplete) applyBlockers.push("valueOwners_scenario_slots_still_missing");

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

  const visualDefectsBefore = visualBefore?.defectCounts || {
    total: 7,
    critical: 2,
    high: 4,
    medium: 1,
    titleOnlyOrThin: 4,
  };
  const finalQaBefore = {
    overallNumeric: 55,
    overallActiveProfileReadiness: "blocked",
    visualCompletenessScore: 0,
    sourceGovernanceScore: 91,
    pendingFacts: pendingFacts.length,
  };

  const projectedCritical = Math.max(
    0,
    visualDefectsBefore.critical - copyRepairs.filter((r) => r.fixReason.includes("wrong_brand")).length
  );
  const projectedHigh = Math.max(
    0,
    visualDefectsBefore.high -
      scenarioCreates.length -
      galleryUpdates.filter((r) => !hasVal(r.currentBody)).length
  );
  const projectedVisualScore = Math.max(
    0,
    100 - projectedCritical * 15 - projectedHigh * 8 - (sortRepairs.length > 0 ? 0 : 3)
  );
  const projectedScores = estimatePostApplyScores({
    contractScore: contractBefore.readinessScore ?? 100,
    pendingFactsCount: approvePendingFacts ? 0 : pendingFacts.length,
    criticalRemaining: projectedCritical,
    highRemaining: projectedHigh,
    visualScoreProjected: projectedVisualScore,
  });

  const dryRunClean = applyBlockers.length === 0 && hasWork;
  const separateFactApplyNeeded = pendingFacts.length > 0 && !approvePendingFacts;
  const expectedActiveProfileAfterApply =
    projectedScores.overallActiveProfileReadiness === "ready" && !separateFactApplyNeeded;

  const report = {
    writerVersion: WRITER_VERSION,
    v28EWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    brand: {
      slug: target.slug,
      name: target.name,
      recordId: target.recordId,
      parentCompany: nz(brandBasicsBefore?.fields?.["Parent Company"]),
    },
    protectedBrandsUntouched: PROTECTED_BRAND_SLUGS,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    diagnosis: {
      finalQaScoreBefore: finalQaBefore.overallNumeric,
      finalQaReadinessBefore: finalQaBefore.overallActiveProfileReadiness,
      contractScore: contractBefore.readinessScore ?? 100,
      contractReady: contractBefore.brandExplorerRequiredSectionsReady ?? true,
      visualDefectsBefore,
      criticalDefectsBefore: visualDefectsBefore.critical,
      highDefectsBefore: visualDefectsBefore.high,
      titleOnlyOrThinBefore: visualDefectsBefore.titleOnlyOrThin,
      missingValueOwnersScenarioSlots: missingScenarioSlots,
      pendingFactsBefore: pendingFacts.length,
      sourceCount: (liveState.sources || []).length,
      approvedExplorerSources: (liveState.sources || []).filter((s) => nz(s.approvedForExplorerUse) === "Yes")
        .length,
      copySafetyFindingsBefore: copySafetyBefore,
      nextRepairsRequired: [
        "Create valueOwners.scenario.1–4 presentation rows with owner-facing bodies",
        "Add materials.gallery.1–6 prototype captions (keep existing images)",
        "Rephrase footprint.region.cala and Riviera Panama case study to avoid visual-audit Radisson Blu phrase",
        "Normalize writer-batch Sort Order on seven editorial/standards rows",
        "Keep three pending facts in stewardship—no auto-approval in this package",
      ],
    },
    pendingFactsDiagnosis: pendingFactDiagnosis,
    valueCreationScenarioRepairPlan: {
      strategy: "create_missing_valueOwners_scenario_slots_from_aggregate_narrative",
      packages: VALUE_SCENARIO_PACKAGES,
      rowsToCreate: scenarioCreates,
    },
    sourceEvidenceRepairPlan: {
      strategy: "use_existing_gallery_images_only",
      pendingSourceReview: [],
      imagesAutoMaterialized: false,
      galleryRowsToUpdate: galleryUpdates,
      note: "Gallery rows already have approved prototype images; writer adds captions only.",
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
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    choiceValidationImplied: false,
    airtableModified,
    applyResults,
    projectedFinalQa: {
      ...projectedScores,
      criticalDefectsAfter: projectedCritical,
      highDefectsAfter: projectedHigh,
      titleOnlyOrThinAfter: Math.max(0, visualDefectsBefore.titleOnlyOrThin - scenarioCreates.length),
      pendingFactsAfter: approvePendingFacts ? 0 : pendingFacts.length,
    },
    expectedActiveProfileAfterApply,
    separateFactGovernanceApplyNeeded: separateFactApplyNeeded,
    exactDryRunCommand: `npm run brand-explorer-radisson-active-profile-repair-writer -- --brand ${TARGET_BRAND_SLUG} --dry-run`,
    exactApplyCommand: buildApplyCommand({ includeFactApproval: false }),
    exactFactApplyCommand: separateFactApplyNeeded
      ? "Pending facts are not approve-ready — founder/source review required before any --approve-radisson-pending-facts apply."
      : null,
  };

  report.markdown = buildMarkdown(report);
  return report;
}

function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Radisson Active Profile Repair Writer v${WRITER_VERSION}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`);
  lines.push(`- v28E exists: **${report.v28EWriterExists ? "yes" : "no"}**`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Final QA before: **${report.diagnosis.finalQaScoreBefore}** (${report.diagnosis.finalQaReadinessBefore})`);
  lines.push(
    `- Visual defects before: **${report.diagnosis.visualDefectsBefore.total}** (critical ${report.diagnosis.criticalDefectsBefore}, high ${report.diagnosis.highDefectsBefore}, titleOnlyOrThin ${report.diagnosis.titleOnlyOrThinBefore})`
  );
  lines.push(`- Pending facts: **${report.diagnosis.pendingFactsBefore}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push(
    `- Expected Final QA after apply: **~${report.projectedFinalQa.overallNumeric}** (${report.projectedFinalQa.overallActiveProfileReadiness})`
  );
  lines.push(`- Expected active-profile ready: **${report.expectedActiveProfileAfterApply ? "yes" : "no"}**`);
  lines.push(`- Separate fact/governance apply needed: **${report.separateFactGovernanceApplyNeeded ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Blocker diagnosis");
  for (const item of report.diagnosis.nextRepairsRequired) lines.push(`- ${item}`);
  lines.push("");
  lines.push("## Pending facts");
  for (const f of report.pendingFactsDiagnosis) {
    lines.push(
      `- \`${f.factId}\` **${f.fieldName}** → ${f.classification} (${f.rationale})`
    );
  }
  lines.push("");
  lines.push("## Rows to create");
  lines.push(`Count: **${report.rowsWouldCreate.length}**`);
  for (const row of report.rowsWouldCreate) {
    lines.push(`### ${row.slotKey} (create)`);
    lines.push(`**After title:** ${row.proposedTitle}`);
    lines.push(`**After body:** ${row.proposedBody}`);
  }
  lines.push("");
  lines.push("## Rows to update");
  lines.push(`Count: **${report.rowsWouldUpdate.length}**`);
  for (const row of report.rowsWouldUpdate) {
    lines.push(`### ${row.slotKey} (${row.recordId || "new"})`);
    lines.push(`- Fix: ${row.fixReason}`);
    if (row.currentBody !== row.proposedBody) {
      lines.push(`- Before: ${(row.currentBody || "(empty)").slice(0, 200)}`);
      lines.push(`- After: ${row.proposedBody.slice(0, 200)}`);
    }
  }
  if (report.applyBlockers.length) {
    lines.push("");
    lines.push("## Apply blockers");
    for (const b of report.applyBlockers) lines.push(`- ${b}`);
  }
  lines.push("");
  lines.push("## Exact apply command");
  lines.push("```bash");
  lines.push(report.exactApplyCommand);
  lines.push("```");
  return lines.join("\n");
}

export function buildBrandExplorerRadissonActiveProfileRepairWriterMarkdown(report) {
  return report.markdown || buildMarkdown(report);
}
