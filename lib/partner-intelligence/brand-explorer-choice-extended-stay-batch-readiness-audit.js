/**
 * Brand Explorer Choice Extended-Stay Batch Readiness + Source Audit v32A.
 *
 * Read-only batch planning pass for WoodSpring Suites, Everhome Suites, and Suburban Studios.
 * No activation, no presentation apply, no image approval/materialization.
 *
 * @see docs/data-intelligence/brand-explorer-choice-extended-stay-batch-readiness-audit-v32A.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics, fetchLiveState } from "./tribute-portfolio-package-pipeline.js";
import {
  resolveBrandTarget,
  getBrandTargetResolverContext,
} from "./brand-explorer-brand-target-resolver.js";
import {
  resolveFinalQaBrandTarget,
  isExpansionBacklogBrandTarget,
  buildBrandExplorerFinalQaAuditorReport,
} from "./brand-explorer-final-qa-auditor.js";
import {
  buildBrandExplorerRequiredSectionPopulationContractReport,
} from "./brand-explorer-required-section-population-contract.js";
import {
  buildBrandExplorerVisualDisplayDefectAuditReport,
} from "./brand-explorer-visual-display-defect-audit.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import {
  DISCOVERY_BRAND_CONFIG,
  getDiscoveryBrandConfig,
} from "./brand-explorer-brand-asset-image-governance.js";
import {
  listRegistryAssetsForBrand,
  MAP_BRAND_ASSET,
} from "./brand-asset-registry-workflow.js";
import {
  normalizeRegistryRecordExtended,
  isDoNotUseRecord,
} from "./brand-explorer-radisson-individuals-asset-registry-normalization-writer.js";
import { REGISTRY_COMPLETENESS_FIELDS } from "./brand-explorer-radisson-individuals-image-asset-openings-root-cause-audit.js";
import { isTemporaryAirtableUrl } from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import {
  HIDDEN_EXTERNAL_DISPLAY_STATUSES,
  isPresentationRowVisibleInExplorer,
} from "./brand-explorer-radisson-individuals-openings-suppression-writer.js";
import {
  classifyMomentumSourceType,
  followsTributeMomentumRules,
  momentumEvidenceSourceRank,
  momentumLinkLabelForUrl,
  parseMomentumPresentationBody,
} from "./brand-explorer-momentum-link-label.js";
import { CHOICE_LEGACY_BRANDS } from "./choice-legacy-brand-source-package.js";
import { isApprovedExplorerSource } from "./profile-governance-publish-readiness.js";

export const AUDIT_VERSION = "v32A";
export const REPORT_JSON_NAME = "brand-explorer-choice-extended-stay-batch-readiness-audit.json";
export const REPORT_MD_NAME = "brand-explorer-choice-extended-stay-batch-readiness-audit.md";
export const DOC_MD_NAME = "brand-explorer-choice-extended-stay-batch-readiness-audit-v32A.md";

export const DEFAULT_BATCH_SLUGS = Object.freeze([
  "woodspring-suites",
  "everhome-suites",
  "suburban-studios",
]);

export const REFERENCE_COMPLETED_SLUGS = Object.freeze([
  "tribute-portfolio",
  "radisson",
  "ascend",
  "radisson-blu",
  "kimpton",
  "radisson-individuals-by-choice",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const INTERNAL_LANGUAGE_RES = [
  { id: "census", re: /\bdealality census\b|\bcensus property url\b|\bdealality census\b/i },
  { id: "item_19", re: /\bitem\s*19\b/i },
  { id: "fdd", re: /\bfdd\b|\bfranchise disclosure\b/i },
  { id: "active_property_page", re: /\bactive property page\b/i },
  { id: "confirm_fees", re: /\bconfirm flag\b|\bfees\b.*\bopening status\b/i },
  { id: "consumer_path", re: /\bconsumer (site|path)\b/i },
  { id: "metadata", re: /\bmetadata\b/i },
  { id: "internal", re: /\binternal extraction\b|\binternal\b.*\bsource\b/i },
  { id: "extraction", re: /\bextraction run\b|\bsource-capture\b/i },
  { id: "gateway_cala_label", re: /\bgateway cala\b.*\bsource\b/i },
];

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-radisson-individuals-final-presentation-quality-sync-writer.json",
  "reports/brand-explorer-radisson-individuals-opening-asset-approval-reconciliation-writer.json",
  "reports/brand-explorer-radisson-individuals-momentum-evidence-source-correction-writer.json",
  "reports/brand-explorer-radisson-individuals-image-asset-openings-root-cause-audit.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js",
  "lib/partner-intelligence/brand-explorer-momentum-link-label.js",
  "live Brand Setup / Presentation / Facts / Sources / Registry for batch brands",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-choice-extended-stay-batch-readiness-audit.js",
  "scripts/brand-explorer-choice-extended-stay-batch-readiness-audit.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function wordCount(text) {
  return nz(text).split(/\s+/).filter(Boolean).length;
}

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

export function v32aAuditExists() {
  return fs.existsSync(
    path.join(ROOT, "lib/partner-intelligence/brand-explorer-choice-extended-stay-batch-readiness-audit.js")
  );
}

function expectedSourceCatalog(slug) {
  const discovery = getDiscoveryBrandConfig(slug);
  const legacy = CHOICE_LEGACY_BRANDS.find((b) => b.key === slug || b.key === slug.replace(/-by-choice$/, ""));
  return {
    consumerPage: discovery?.consumerUrl || legacy?.consumerPage?.url || null,
    developmentPage: legacy?.developmentPage?.url || null,
    pressKit: discovery?.pressKitUrl || legacy?.pressKit?.url || null,
    officialDomains: discovery?.officialDomains || [],
  };
}

function classifySlotSection(slotKey) {
  const sk = nz(slotKey);
  if (sk === "overview.featured_application") return "overview.featured_application";
  if (/^overview\.scenario/.test(sk)) return "overview.scenario";
  if (/^valueOwners\.scenario/.test(sk)) return "valueOwners.scenario";
  if (sk === "footprint.openings") return "footprint.openings";
  if (sk === "footprint.momentum") return "footprint.momentum";
  if (/portfolio_mix|portfolio\.mix/i.test(sk)) return "portfolio_mix";
  if (/portfolio_context|portfolio\.context/i.test(sk)) return "portfolio_context";
  if (/standard_detail|standards\.detail/i.test(sk)) return "standard_detail";
  if (/demand_scenario|demand\.scenario/i.test(sk)) return "demand_scenario";
  if (/^loyalty\./.test(sk)) return "loyalty";
  if (/geographic|footprint\.geo/i.test(sk)) return "geographic_footprint";
  if (/^materials\.gallery/.test(sk)) return "materials.gallery";
  if (/^overview\./.test(sk) || sk === "overview.hero") return "overview";
  return "other";
}

function inventorySectionKeys() {
  return [
    "overview",
    "overview.featured_application",
    "overview.scenario",
    "valueOwners.scenario",
    "footprint.openings",
    "footprint.momentum",
    "portfolio_mix",
    "portfolio_context",
    "standard_detail",
    "demand_scenario",
    "loyalty",
    "geographic_footprint",
    "materials.gallery",
  ];
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

async function listPresentationRowsRaw(baseId, apiKey, brandRecordId, brandName) {
  const formula =
    "OR(FIND('" +
    escapeFormulaValue(brandRecordId) +
    "', ARRAYJOIN({Brand})), {Brand Name}='" +
    escapeFormulaValue(brandName) +
    "')";
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    params.set("filterByFormula", formula);
    if (offset) params.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params}`,
      { headers: { Authorization: `Bearer ${apiKey}` } }
    );
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `Presentation list failed: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records;
}

function normalizePresentationRow(rec) {
  const f = rec.fields || {};
  const imageArr = f.Image;
  const imageUrl = Array.isArray(imageArr) && imageArr[0]?.url ? nz(imageArr[0].url) : "";
  return {
    recordId: rec.id,
    slotKey: nz(f["Slot Key"]),
    title: nz(f.Title),
    body: nz(f.Body),
    externalDisplayStatus: nz(f["External Display Status"]),
    visibleInExplorer: isPresentationRowVisibleInExplorer(f),
    hidden: HIDDEN_EXTERNAL_DISPLAY_STATUSES.includes(nz(f["External Display Status"])),
    hasImage: Boolean(imageUrl),
    imageUrl,
    temporaryImageUrl: imageUrl ? isTemporaryAirtableUrl(imageUrl) : false,
    wordCount: wordCount(f.Body),
    thin: wordCount(f.Body) < 15 && !nz(f.Title),
    titleOnly: hasVal(f.Title) && !hasVal(f.Body),
    section: classifySlotSection(f["Slot Key"]),
  };
}

function scanInternalLanguage(text, slotKey, recordId) {
  const hits = [];
  for (const pat of INTERNAL_LANGUAGE_RES) {
    if (pat.re.test(nz(text))) {
      hits.push({ patternId: pat.id, slotKey, recordId, excerpt: nz(text).slice(0, 120) });
    }
  }
  return hits;
}

function auditRegistryCompleteness(registryAssets) {
  const normalized = registryAssets.map((r) => normalizeRegistryRecordExtended(r));
  const fieldGaps = [];
  for (const asset of normalized) {
    const missing = REGISTRY_COMPLETENESS_FIELDS.filter(
      (f) => f.activeProfile && f.required && !hasVal(asset[f.key])
    ).map((f) => f.label);
    if (missing.length) {
      fieldGaps.push({ recordId: asset.recordId, assetName: asset.assetName, missingFields: missing });
    }
  }
  const urlKeys = new Map();
  const duplicates = [];
  for (const asset of normalized) {
    const key = nz(asset.sourcePageUrl || asset.sourceUrl || asset.attachmentUrl).toLowerCase();
    if (!key) continue;
    if (urlKeys.has(key)) duplicates.push({ recordId: asset.recordId, duplicateOf: urlKeys.get(key), url: key });
    else urlKeys.set(key, asset.recordId);
  }
  return {
    total: normalized.length,
    approved: normalized.filter((a) => nz(a.explorerUsePermission) === "Approved For Explorer").length,
    pending: normalized.filter(
      (a) => /pending|not reviewed|candidate/i.test(nz(a.explorerUsePermission) + nz(a.usageReviewStatus))
    ).length,
    doNotUse: normalized.filter((a) => isDoNotUseRecord(a)).length,
    duplicates,
    incompleteComparedToTribute: fieldGaps,
    temporarySourceUrls: normalized.filter(
      (a) => isTemporaryAirtableUrl(a.sourcePageUrl) || isTemporaryAirtableUrl(a.sourceUrl)
    ).map((a) => ({ recordId: a.recordId, sourcePageUrl: a.sourcePageUrl })),
  };
}

function classifySectionReadiness(contractSection, extra = {}) {
  const cls = nz(contractSection?.classification);
  if (cls.startsWith("ready")) return "ready";
  if (extra.needsRegistry) return "needs_registry_repair";
  if (extra.needsImages) return "needs_image_approval";
  if (cls.includes("source")) return "needs_source_capture";
  if (cls.includes("copy") || cls.includes("thin")) return "needs_copy_backfill";
  if (cls.includes("blocked")) return "blocked";
  if (contractSection?.currentCount >= (contractSection?.requiredMinimum || 0) - 1) return "nearly_ready";
  return "needs_copy_backfill";
}

function auditMomentumRows(blocks) {
  const rows = blocks.filter((b) => b.slotKey === "footprint.momentum");
  return rows.map((row) => {
    const parsed = parseMomentumPresentationBody(row.body, row.title);
    const sourceUrl = parsed.sourceUrl;
    const rank = momentumEvidenceSourceRank(sourceUrl);
    const tributeRules = followsTributeMomentumRules(sourceUrl);
    return {
      recordId: row.recordId,
      title: row.title,
      dateLine: parsed.dateLine,
      sourceUrl,
      linkLabel: sourceUrl ? momentumLinkLabelForUrl(sourceUrl, row.title) : null,
      sourceType: classifyMomentumSourceType(sourceUrl),
      evidenceRank: rank,
      followsTributeRules: tributeRules.ok,
      ruleIssue: tributeRules.ok ? null : tributeRules.reason,
      visible: row.visibleInExplorer,
    };
  });
}

function auditOpeningsRows(blocks, registryAudit) {
  const rows = blocks.filter((b) => b.slotKey === "footprint.openings");
  return rows.map((row) => ({
    recordId: row.recordId,
    title: row.title,
    bodyWordCount: row.wordCount,
    hasImage: row.hasImage,
    temporaryImageUrl: row.temporaryImageUrl,
    ownerFacing: row.wordCount >= 12 || row.titleOnly,
    visible: row.visibleInExplorer,
    hidden: row.hidden,
    registrySupport: registryAudit.approved > 0 ? "partial_or_unknown" : "none_approved",
  }));
}

function auditSourceLibrary(sources, expected) {
  const approved = sources.filter((s) => isApprovedExplorerSource(s));
  const urls = sources.map((s) => nz(s.url || s.sourceUrl)).filter(Boolean);
  const flags = [];
  const hasUrl = (u) => urls.some((x) => x.includes(u) || u.includes(x));
  if (expected.consumerPage && !hasUrl(expected.consumerPage)) flags.push("missing_official_consumer_page");
  if (expected.developmentPage && !hasUrl(expected.developmentPage)) flags.push("missing_development_page");
  if (expected.pressKit && !hasUrl(expected.pressKit)) flags.push("missing_press_kit_or_newsroom");
  const expiredAirtable = urls.filter((u) => isTemporaryAirtableUrl(u));
  if (expiredAirtable.length) flags.push("expired_airtable_urls_in_source_library");
  const weakOnly = approved.length === 0 && sources.length > 0;
  if (weakOnly) flags.push("no_approved_explorer_sources");
  return {
    sourceCount: sources.length,
    approvedExplorerSources: approved.length,
    durableUrls: urls.filter((u) => !isTemporaryAirtableUrl(u)),
    expiredAirtableUrls: expiredAirtable,
    expectedCatalog: expected,
    flags,
    pressNeededForMomentum: flags.includes("missing_press_kit_or_newsroom"),
    propertyListingNeededForOpenings: true,
  };
}

function scoreResolverPath(slugTarget, recordIdTarget) {
  const slugExpansion = isExpansionBacklogBrandTarget(slugTarget);
  const recordExpansion = isExpansionBacklogBrandTarget(recordIdTarget);
  return {
    slugResolutionSource: slugTarget?.resolution?.resolutionSource,
    recordIdResolutionSource: recordIdTarget?.resolution?.resolutionSource,
    slugUsesExpansionScoring: slugExpansion,
    recordIdUsesExpansionScoring: recordExpansion,
    sameV31nScoringPath: slugExpansion === recordExpansion,
    completeBuildSafeBySlug: Boolean(slugTarget?.recordId),
    completeBuildSafeByRecordId: Boolean(recordIdTarget?.recordId),
  };
}

function assignBatchFeasibility({
  contractReport,
  sourceAudit,
  registryAudit,
  momentumAudit,
  openingsAudit,
  internalHits,
  resolverPath,
  finalQaScores,
}) {
  const readiness = contractReport?.readinessScore ?? 0;
  const openingsReady = (openingsAudit.filter((r) => r.visible && r.hasImage).length || 0) >= 3;
  const momentumReady = momentumAudit.filter((m) => m.followsTributeRules).length >= 3;
  if (!resolverPath.sameV31nScoringPath) {
    return { band: "needs_manual_review", reason: "slug_record_id_scoring_path_mismatch" };
  }
  if (internalHits.length > 5) {
    return { band: "needs_manual_review", reason: "elevated_internal_language_hits" };
  }
  if (registryAudit.doNotUse > 0 && registryAudit.approved === 0) {
    return { band: "batch_ready_after_image_review", reason: "registry_blocked_or_unapproved" };
  }
  if (sourceAudit.flags.includes("missing_press_kit_or_newsroom") || sourceAudit.approvedExplorerSources < 2) {
    return { band: "batch_ready_after_source_capture", reason: "source_library_gaps" };
  }
  if (!openingsReady || registryAudit.approved < 3) {
    return { band: "batch_ready_after_image_review", reason: "openings_or_registry_incomplete" };
  }
  if (!momentumReady) {
    return { band: "batch_ready_after_source_capture", reason: "momentum_evidence_gaps" };
  }
  if (readiness >= 85 && finalQaScores?.overallActiveProfileReadiness !== "ready") {
    return { band: "batch_ready_for_backfill", reason: "contract_strong_qa_not_ready" };
  }
  if (readiness >= 70) {
    return { band: "batch_ready_for_backfill", reason: "contract_partial_needs_writers" };
  }
  return { band: "blocked", reason: "contract_readiness_low" };
}

function rankFeasibility(brands) {
  const order = {
    batch_ready_for_backfill: 1,
    batch_ready_after_source_capture: 2,
    batch_ready_after_image_review: 3,
    needs_manual_review: 4,
    blocked: 5,
  };
  return [...brands].sort((a, b) => {
    const bandDiff = (order[a.feasibility.band] || 9) - (order[b.feasibility.band] || 9);
    if (bandDiff !== 0) return bandDiff;
    return (b.contractReadinessScore || 0) - (a.contractReadinessScore || 0);
  });
}

function recommendRepairSequence(brandAudits) {
  const needsSource = brandAudits.some(
    (b) =>
      b.feasibility.band === "batch_ready_after_source_capture" ||
      b.sourceLibraryAudit.approvedExplorerSources < 2 ||
      b.sourceLibraryAudit.flags.length > 0
  );
  const needsRegistry = brandAudits.some(
    (b) =>
      ["batch_ready_after_image_review", "needs_manual_review"].includes(b.feasibility.band) ||
      b.brandAssetRegistryAudit.total === 0 ||
      b.brandAssetRegistryAudit.incompleteComparedToTribute.length > 0
  );
  const sequence = [];
  if (needsSource) sequence.push("v32B — Choice extended-stay source capture writer (multi-brand)");
  if (needsRegistry) sequence.push("v32C — Brand Asset Registry normalization writer (per brand)");
  sequence.push("v32D — presentation backfill writer (per brand)");
  sequence.push("v32E — openings/momentum rebuild writer (per brand)");
  sequence.push("v32F — approved asset materialization writer (per brand)");
  sequence.push("v32G — final QA + complete-build per brand (activation one brand at a time)");
  return sequence;
}

async function auditSingleBrand(slug, options = {}) {
  const { includeFinalQa = false, includeCompleteBuild = false } = options;
  const ctx = await getBrandTargetResolverContext();
  const resolved = await resolveBrandTarget(slug, ctx);
  const recordId = resolved.recordId || getDiscoveryBrandConfig(slug)?.recordId;
  const name = resolved.name || getDiscoveryBrandConfig(slug)?.name || slug;

  const slugTarget = await resolveFinalQaBrandTarget(slug);
  const recordIdTarget = recordId ? await resolveFinalQaBrandTarget(recordId) : null;
  const resolverPath = scoreResolverPath(slugTarget, recordIdTarget || slugTarget);

  const brandBasics = recordId ? await fetchBrandBasics(recordId).catch(() => null) : null;
  const companyValidated = companyValidatedSnapshot(brandBasics);
  const liveState = recordId
    ? await fetchLiveState(recordId).catch(() => ({ sources: [], facts: [], brandBasics: null }))
    : { sources: [], facts: [], brandBasics: null };

  const brandApi = recordId ? await fetchBrandApiShape(recordId) : null;
  const apiBlocks = (brandApi?.brandExplorer?.blocks || []).map((b) => ({
    recordId: b.recordId,
    slotKey: nz(b.slotKey),
    title: nz(b.title),
    body: nz(b.body),
    imageUrl: nz(b.imageUrl),
    externalDisplayStatus: nz(b.externalDisplayStatus),
    visibleInExplorer: !HIDDEN_EXTERNAL_DISPLAY_STATUSES.includes(nz(b.externalDisplayStatus)),
    hidden: HIDDEN_EXTERNAL_DISPLAY_STATUSES.includes(nz(b.externalDisplayStatus)),
    hasImage: hasVal(b.imageUrl),
    temporaryImageUrl: isTemporaryAirtableUrl(b.imageUrl),
    wordCount: wordCount(b.body),
    thin: wordCount(b.body) < 15,
    titleOnly: hasVal(b.title) && !hasVal(b.body),
    section: classifySlotSection(b.slotKey),
  }));

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  let rawPresentation = [];
  if (baseId && apiKey && recordId) {
    rawPresentation = await listPresentationRowsRaw(baseId, apiKey, recordId, name);
  }
  const presentationRows = rawPresentation.map(normalizePresentationRow);
  const blocks = presentationRows.length ? presentationRows : apiBlocks;

  const inventory = {};
  for (const key of inventorySectionKeys()) {
    const rows = blocks.filter((b) => {
      if (key === "overview.scenario") return /^overview\.scenario/.test(b.slotKey);
      if (key === "valueOwners.scenario") return /^valueOwners\.scenario/.test(b.slotKey);
      if (key === "materials.gallery") return /^materials\.gallery/.test(b.slotKey);
      if (key === "overview") return /^overview\./.test(b.slotKey) && b.section === "overview";
      return b.section === key || b.slotKey === key;
    });
    inventory[key] = {
      existingRows: rows.length,
      visibleRows: rows.filter((r) => r.visibleInExplorer).length,
      hiddenRows: rows.filter((r) => r.hidden).length,
      withImages: rows.filter((r) => r.hasImage).length,
      temporaryImageUrls: rows.filter((r) => r.temporaryImageUrl).length,
      thinOrEmpty: rows.filter((r) => r.thin || r.titleOnly).length,
      missing: 0,
    };
  }

  const internalHits = [];
  for (const row of blocks) {
    internalHits.push(...scanInternalLanguage(`${row.title}\n${row.body}`, row.slotKey, row.recordId));
  }

  const registryAssets = recordId ? await listRegistryAssetsForBrand(recordId).catch(() => []) : [];
  const registryAudit = auditRegistryCompleteness(registryAssets);

  const expectedSources = expectedSourceCatalog(slug);
  const sourceAudit = auditSourceLibrary(liveState.sources || [], expectedSources);

  const momentumAudit = auditMomentumRows(blocks);
  const openingsAudit = auditOpeningsRows(blocks, registryAudit);

  const contractReport = recordId
    ? await buildBrandExplorerRequiredSectionPopulationContractReport({ brandIdOrName: recordId }).catch(
        () => null
      )
    : null;

  const visualReport = recordId
    ? await buildBrandExplorerVisualDisplayDefectAuditReport({ brandIdOrName: slug }).catch(() => null)
    : null;

  let finalQaScores = null;
  let completeBuildSummary = null;
  if (includeFinalQa && recordId) {
    const qa = await buildBrandExplorerFinalQaAuditorReport({ brandIdOrName: slug }).catch(() => null);
    finalQaScores = qa?.scores || qa?.brandReports?.[0]?.scores || null;
  }
  if (includeCompleteBuild && recordId) {
    const cb = await buildBrandExplorerCompleteBuildOrchestratorReport({
      brandIdOrName: slug,
      targetQuality: "active-profile",
    }).catch(() => null);
    const br = (cb?.brandResults || [])[0];
    completeBuildSummary = br
      ? {
          readyForActiveProfile: br.readyForActiveProfile,
          finalQaScores: br.finalQaScores,
          readinessBand: br.readinessBand,
        }
      : null;
  }

  const sectionReadiness = {};
  for (const s of contractReport?.sectionBySectionReadiness || []) {
    const sectionKey = nz(s.section).toLowerCase();
    const extra = {
      needsRegistry: sectionKey.includes("opening") && registryAudit.approved < 3,
      needsImages: sectionKey.includes("opening") && openingsAudit.filter((r) => r.hasImage).length < 3,
    };
    sectionReadiness[s.section] = {
      classification: classifySectionReadiness(s, extra),
      contractClassification: s.classification,
      currentCount: s.currentCount,
      requiredMinimum: s.requiredMinimum,
      rendersToday: s.rendersToday,
    };
  }
  sectionReadiness["Gallery / visual completeness"] = {
    classification:
      (visualReport?.defectCounts?.critical || 0) > 0
        ? "blocked"
        : (visualReport?.defectCounts?.high || 0) > 0
          ? "needs_image_approval"
          : (visualReport?.visualComparability?.score || 0) >= 85
            ? "ready"
            : "nearly_ready",
    score: visualReport?.visualComparability?.score ?? null,
    defectCounts: visualReport?.defectCounts ?? null,
  };
  sectionReadiness["Overview / featured application"] = {
    classification:
      inventory["overview.featured_application"]?.existingRows > 0 &&
      inventory["overview.featured_application"]?.thinOrEmpty === 0
        ? "ready"
        : inventory["overview.featured_application"]?.existingRows > 0
          ? "needs_copy_backfill"
          : "needs_copy_backfill",
    existingRows: inventory["overview.featured_application"]?.existingRows ?? 0,
  };

  const visualUiRisks = [];
  if (blocks.some((b) => b.hasImage && b.temporaryImageUrl)) {
    visualUiRisks.push({ type: "temporary_airtable_image_urls", count: blocks.filter((b) => b.temporaryImageUrl).length });
  }
  if (blocks.some((b) => b.slotKey === "footprint.openings" && !b.hasImage && b.visibleInExplorer)) {
    visualUiRisks.push({ type: "visible_opening_without_image" });
  }
  if (blocks.some((b) => b.hidden && b.hasImage && /^footprint\.openings/.test(b.slotKey))) {
    visualUiRisks.push({ type: "hidden_opening_rows_with_images" });
  }
  if (registryAudit.temporarySourceUrls.length) {
    visualUiRisks.push({ type: "registry_temporary_source_urls", count: registryAudit.temporarySourceUrls.length });
  }

  const feasibility = assignBatchFeasibility({
    contractReport,
    sourceAudit,
    registryAudit,
    momentumAudit,
    openingsAudit,
    internalHits,
    resolverPath,
    finalQaScores,
  });

  const momentumSourceSupport = {
    existingRows: momentumAudit.length,
    eventSupportingRows: momentumAudit.filter((m) => m.followsTributeRules).length,
    enoughForThreeRows:
      momentumAudit.filter((m) => m.followsTributeRules).length >= 3 ||
      (sourceAudit.pressNeededForMomentum === false && momentumAudit.length >= 1),
    gaps: momentumAudit.filter((m) => !m.followsTributeRules).map((m) => m.ruleIssue),
  };

  const openingsReadiness = {
    visibleRows: openingsAudit.filter((r) => r.visible).length,
    completeCards: openingsAudit.filter((r) => r.visible && r.hasImage && r.bodyWordCount >= 8).length,
    canReachThree: openingsAudit.filter((r) => r.visible).length >= 3 || registryAudit.approved >= 3,
    candidateRows: openingsAudit,
  };

  return {
    slug,
    displayName: name,
    recordId,
    parentEcosystem: "Choice Hotels",
    discoveryConfig: Boolean(getDiscoveryBrandConfig(slug)),
    discoveryConfigRecordId: getDiscoveryBrandConfig(slug)?.recordId || null,
    brandIdentity: {
      displayName: name,
      slug,
      brandSetupRecordId: recordId,
      discoveryConfigStatus: getDiscoveryBrandConfig(slug) ? "present" : "missing",
      expansionOrActive:
        slugTarget?.resolution?.resolutionSource === "active_registry"
          ? "active_registry"
          : "expansion_backlog",
      resolverPath,
      companyValidated,
    },
    profileInventory: inventory,
    requiredSectionReadiness: sectionReadiness,
    contractReadinessScore: contractReport?.readinessScore ?? null,
    sourceLibraryAudit: sourceAudit,
    momentumAudit: {
      rows: momentumAudit,
      sourceSupport: momentumSourceSupport,
    },
    openingsAudit: openingsReadiness,
    brandAssetRegistryAudit: registryAudit,
    visualUiRisks,
    internalLanguageRisks: internalHits,
    finalQaScores,
    completeBuildSummary,
    feasibility,
    presentationRowCount: blocks.length,
    factCount: (liveState.facts || []).length,
    explorerFactCount: (liveState.facts || []).filter(
      (f) => nz(f.explorerType) === "Brand Explorer" || nz(f.fieldName).startsWith("be.")
    ).length,
  };
}

export async function buildBrandExplorerChoiceExtendedStayBatchReadinessAuditReport(options = {}) {
  const brandList = nz(options.brands || options.brandsArg || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  const slugs = brandList.length ? brandList : [...DEFAULT_BATCH_SLUGS];
  const includeFinalQa = Boolean(options.includeFinalQa);
  const includeCompleteBuild = Boolean(options.includeCompleteBuild);

  const brandAudits = [];
  for (const slug of slugs) {
    brandAudits.push(
      await auditSingleBrand(slug, { includeFinalQa, includeCompleteBuild })
    );
    await new Promise((r) => setTimeout(r, 400));
  }

  const ranked = rankFeasibility(brandAudits);
  const repairSequence = recommendRepairSequence(brandAudits);

  const report = {
    auditVersion: AUDIT_VERSION,
    v32aAuditExists: true,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    companyValidatedUntouched: true,
    parentEcosystem: "Choice Hotels International",
    batchBrands: slugs,
    referenceCompletedBrands: REFERENCE_COMPLETED_SLUGS,
    factoryLessonsEnforced: [
      "slug_and_record_id_same_scoring_path_v31N",
      "expansion_backlog_scoring_for_discovery_brands",
      "durable_source_page_url_only",
      "momentum_event_supporting_sources",
      "openings_property_specific_sources_and_images",
      "property_listings_not_momentum_evidence",
      "tribute_level_registry_completeness",
      "duplicate_reconciliation_before_materialization",
      "do_not_use_assets_blocked",
      "company_validated_untouched",
      "activation_one_brand_at_a_time",
    ],
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    brandAudits,
    batchFeasibilityRanking: ranked.map((b, i) => ({
      rank: i + 1,
      slug: b.slug,
      displayName: b.displayName,
      band: b.feasibility.band,
      reason: b.feasibility.reason,
      contractReadinessScore: b.contractReadinessScore,
    })),
    recommendedRepairSequence: repairSequence,
    recommendedNextWriter: repairSequence[0] || "v32B — source capture writer",
    applyGuardrails: {
      blockCompanyValidatedChanges: true,
      blockAutoImageApproval: true,
      blockTemporaryAirtableSourceUrls: true,
      blockDoNotUseMaterialization: true,
      blockWrongBrandImages: true,
      blockPropertyListingMomentumWhenPressExists: true,
      blockInternalLanguageVisible: true,
      blockMultiBrandActivation: true,
    },
    pipelineCommands: [
      `npm run brand-explorer-choice-extended-stay-batch-readiness-audit -- --brands ${slugs.join(",")} --dry-run`,
      ...slugs.flatMap((s) => [
        `npm run brand-explorer-final-qa-auditor -- --brand ${s} --dry-run`,
        `npm run brand-explorer-complete-build -- --brand ${s} --dry-run --target-quality active-profile`,
      ]),
    ],
  };

  report.markdown = buildMarkdown(report);
  return report;
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Choice Extended-Stay Batch Readiness Audit v32A");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}** — read-only`);
  lines.push(`- v32A exists: **${report.v32aAuditExists ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **no**`);
  lines.push(`- Company Validated untouched: **yes**`);
  lines.push("");
  lines.push("## Batch feasibility ranking");
  for (const r of report.batchFeasibilityRanking) {
    lines.push(
      `${r.rank}. **${r.displayName}** (\`${r.slug}\`) — \`${r.band}\` — contract ${r.contractReadinessScore ?? "n/a"} — ${r.reason}`
    );
  }
  lines.push("");
  lines.push("## Recommended repair sequence");
  for (const step of report.recommendedRepairSequence) {
    lines.push(`- ${step}`);
  }
  lines.push("");
  lines.push(`**Next writer:** ${report.recommendedNextWriter}`);
  lines.push("");
  for (const b of report.brandAudits) {
    lines.push(`## ${b.displayName} (\`${b.slug}\`)`);
    lines.push(`- Record ID: \`${b.recordId || "unresolved"}\``);
    lines.push(`- Feasibility: **${b.feasibility.band}** — ${b.feasibility.reason}`);
    lines.push(`- Contract readiness: ${b.contractReadinessScore ?? "n/a"}`);
    lines.push(
      `- Resolver path aligned v31N: ${b.brandIdentity.resolverPath.sameV31nScoringPath ? "yes" : "no"}`
    );
    lines.push(`- Sources approved: ${b.sourceLibraryAudit.approvedExplorerSources}`);
    lines.push(`- Registry approved assets: ${b.brandAssetRegistryAudit.approved}`);
    lines.push(`- Momentum event-supporting rows: ${b.momentumAudit.sourceSupport.eventSupportingRows}`);
    lines.push(`- Openings complete cards: ${b.openingsAudit.completeCards}`);
    lines.push("");
  }
  return lines.join("\n");
}
