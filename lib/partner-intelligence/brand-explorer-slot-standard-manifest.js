import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";

export const WRITER_VERSION = "18";
export const REPORT_JSON_NAME = "brand-explorer-slot-standard-manifest.json";
export const REPORT_MD_NAME = "brand-explorer-slot-standard-manifest.md";
export const DOC_MD_NAME = "brand-explorer-slot-standard-manifest-v18.md";

const DEFAULT_BRAND_ID = "recCvV0PuZOi8c3hC";
const DEFAULT_BRAND_NAME = "Tribute Portfolio";

const REFERENCE_BRANDS = [
  "Radisson Blu by Choice",
  "Radisson by Choice",
  "Kimpton Hotels",
  "Curio Collection by Hilton",
  "Ascend Hotel Collection",
  "Comfort by Choice",
  "Quality by Choice",
  "Country Inn by Choice",
  "Everhome Suites",
  "Radisson RED by Choice",
  "Radisson Individuals by Choice",
];

const CORE_REQUIRED = new Set([
  "overview.typical_use_case",
  "overview.development_model",
  "overview.scenario.1",
  "overview.scenario.2",
  "valueOwners.overview",
  "standards.questions",
  "footprint.geo_intro",
  "insight.summary",
]);

const TAB_REQUIRED = new Set([
  "commercial.intro",
  "commercial.differentiator",
  "commercial.theme",
  "commercial.demand",
  "economics.intro",
  "economics.checklist",
  "loyalty.hero_title",
  "loyalty.owner_lens",
  "operations.standards_philosophy",
  "operations.operator_compat.summary",
  "materials.file",
  "materials.caseStudy",
]);

const SOFT_BRAND_REQUIRED = new Set([
  "overview.why_value",
  "overview.owner_experience",
  "overview.differentiators.identity",
  "overview.differentiators.commercial",
  "valueOwners.watchouts",
  "insight.similar",
]);

const COLLECTION_BRAND_REQUIRED = new Set([
  "overview.portfolio_context",
  "overview.portfolio_ladder_tier",
  "overview.relative_positioning",
]);

const NOT_APPLICABLE_TO_TRIBUTE = new Set([
  "footprint.openings",
  "overview.portfolio_context",
  "overview.portfolio_ladder_tier",
  "overview.relative_positioning",
]);

const SOURCE_EVIDENCE_REQUIRED_PATTERNS = [
  /^economics\./i,
  /^footprint\.openings$/i,
  /^materials\.file$/i,
  /^materials\.caseStudy$/i,
  /^loyalty\.kpi\./i,
];

const MEDIA_OPTIONAL_PATTERNS = [/^materials\.gallery\./i, /^overview\.scenario\.[1-3]$/i];

const SOURCE_MATERIAL_PATTERNS = [/^materials\./i, /^footprint\.openings$/i];

const CATEGORY = {
  coreRequired: "core_required",
  tabRequired: "tab_required",
  softBrandRequired: "soft_brand_required",
  collectionBrandRequired: "collection_brand_required",
  commonOptional: "common_optional",
  brandSpecific: "brand_specific",
  mediaOptional: "media_optional",
  sourceMaterial: "source_material",
  notApplicableToTribute: "not_applicable_to_tribute",
  requiresSourceEvidence: "requires_source_evidence",
  candidateForTributeCompletion: "candidate_for_tribute_completion",
};

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return String(v).trim() !== "";
}

function toText(v) {
  if (Array.isArray(v)) return v.filter(hasVal).map(String).join(", ");
  return hasVal(v) ? String(v).trim() : "";
}

function pct(n, d) {
  if (!d) return 0;
  return Math.round((n / d) * 100);
}

function normalizeBrandInput(raw) {
  const normalized = toText(raw).toLowerCase().trim();
  if (!normalized) return DEFAULT_BRAND_ID;
  if (normalized === "tribute-portfolio" || normalized === "tribute portfolio") return DEFAULT_BRAND_ID;
  return toText(raw).trim();
}

function tabFromSlot(slotKey) {
  if (slotKey.startsWith("overview.") || slotKey.startsWith("hero.")) return "Overview";
  if (slotKey.startsWith("valueOwners.")) return "Value to Owners";
  if (slotKey.startsWith("operations.")) return "Operating Model";
  if (slotKey.startsWith("standards.")) return "Owner Considerations";
  if (slotKey.startsWith("commercial.")) return "Commercial Engine";
  if (slotKey.startsWith("economics.")) return "Economics & Obligations";
  if (slotKey.startsWith("loyalty.")) return "Loyalty Program";
  if (slotKey.startsWith("footprint.")) return "Footprint & Growth";
  if (slotKey.startsWith("materials.")) return "Brand Materials";
  if (slotKey.startsWith("insight.")) return "Dealality Insight";
  return "Unknown";
}

function sectionFromSlot(slotKey) {
  if (/^overview\.scenario\./i.test(slotKey)) return "Overview scenarios";
  if (/^materials\.gallery\./i.test(slotKey)) return "Gallery visuals";
  if (/^materials\.file$/i.test(slotKey)) return "Source materials cards";
  if (/^materials\.caseStudy$/i.test(slotKey)) return "Case studies";
  if (/^footprint\.openings$/i.test(slotKey)) return "Recent openings";
  if (/^economics\./i.test(slotKey)) return "Economics editorial blocks";
  if (/^commercial\./i.test(slotKey)) return "Commercial engine blocks";
  if (/^loyalty\./i.test(slotKey)) return "Loyalty blocks";
  if (/^operations\./i.test(slotKey)) return "Operating model blocks";
  if (/^standards\./i.test(slotKey)) return "Owner considerations blocks";
  if (/^insight\./i.test(slotKey)) return "Dealality Insight blocks";
  if (/^valueOwners\./i.test(slotKey)) return "Value to Owners blocks";
  if (/^footprint\./i.test(slotKey)) return "Footprint blocks";
  return "General";
}

function firstUrl(text) {
  const match = toText(text).match(/https?:\/\/\S+/i);
  return match ? match[0] : "";
}

function summarizeBlocks(brandPayload) {
  const blocks = Array.isArray(brandPayload?.brandExplorer?.blocks) ? brandPayload.brandExplorer.blocks : [];
  const map = new Map();
  for (const b of blocks) {
    const slotKey = toText(b?.slotKey);
    if (!slotKey) continue;
    if (!map.has(slotKey)) {
      map.set(slotKey, {
        slotKey,
        rowCount: 0,
        hasTitle: false,
        hasBody: false,
        hasImage: false,
        hasUrl: false,
        rows: [],
      });
    }
    const row = map.get(slotKey);
    const title = toText(b?.title);
    const body = toText(b?.body);
    const imageUrl = toText(b?.imageUrl);
    const url = firstUrl(`${title}\n${body}`);
    row.rowCount += 1;
    row.hasTitle = row.hasTitle || hasVal(title);
    row.hasBody = row.hasBody || hasVal(body);
    row.hasImage = row.hasImage || hasVal(imageUrl);
    row.hasUrl = row.hasUrl || hasVal(url);
    row.rows.push({
      recordId: toText(b?.recordId),
      title,
      body,
      imageUrl,
      url,
      example: [title, body].filter(hasVal).join(" | "),
    });
  }
  return map;
}

async function fetchBrand(brandIdOrName) {
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

function readFixtureManifest() {
  const fixturesDir = path.join(ROOT, "fixtures");
  if (!fs.existsSync(fixturesDir)) return { files: [], slotKeys: [] };
  const files = fs.readdirSync(fixturesDir).filter((n) => /^brand-explorer-presentation-.*\.json$/i.test(n));
  const slotKeys = new Set();

  function visit(value) {
    if (!value || typeof value !== "object") return;
    if (Array.isArray(value)) {
      value.forEach(visit);
      return;
    }
    const direct = value.slotKey ?? value["Slot Key"];
    if (hasVal(direct)) slotKeys.add(String(direct).trim());
    for (const key of Object.keys(value)) visit(value[key]);
  }

  for (const name of files) {
    const abs = path.join(fixturesDir, name);
    try {
      const parsed = JSON.parse(fs.readFileSync(abs, "utf8"));
      visit(parsed);
    } catch {
      // ignore malformed fixture; manifest remains read-only
    }
  }

  return {
    files: files.map((f) => `fixtures/${f}`).sort((a, b) => a.localeCompare(b)),
    slotKeys: Array.from(slotKeys).sort((a, b) => a.localeCompare(b)),
  };
}

function anyPatternMatch(slotKey, patterns) {
  return patterns.some((rx) => rx.test(slotKey));
}

function summarizeNeed(slotKey, row, usagePct) {
  const base = {
    visibleInUi: row.tab !== "Unknown",
    needsSourceBackedEvidence: anyPatternMatch(slotKey, SOURCE_EVIDENCE_REQUIRED_PATTERNS),
    aiDraftHumanReviewAcceptable: true,
    shouldRemainBlank: NOT_APPLICABLE_TO_TRIBUTE.has(slotKey),
  };
  if (base.needsSourceBackedEvidence) base.aiDraftHumanReviewAcceptable = false;
  if (/^footprint\.openings$/i.test(slotKey)) {
    base.needsSourceBackedEvidence = true;
    base.aiDraftHumanReviewAcceptable = false;
    base.shouldRemainBlank = true;
  }
  if (anyPatternMatch(slotKey, MEDIA_OPTIONAL_PATTERNS)) {
    base.aiDraftHumanReviewAcceptable = false;
  }
  if (usagePct < 25 && !CORE_REQUIRED.has(slotKey) && !TAB_REQUIRED.has(slotKey)) {
    base.aiDraftHumanReviewAcceptable = false;
  }
  return base;
}

function classificationForSlot(slotKey, usagePct, tributeHas, need) {
  if (NOT_APPLICABLE_TO_TRIBUTE.has(slotKey)) return CATEGORY.notApplicableToTribute;
  if (CORE_REQUIRED.has(slotKey)) return CATEGORY.coreRequired;
  if (TAB_REQUIRED.has(slotKey)) return CATEGORY.tabRequired;
  if (SOFT_BRAND_REQUIRED.has(slotKey)) return CATEGORY.softBrandRequired;
  if (COLLECTION_BRAND_REQUIRED.has(slotKey)) return CATEGORY.collectionBrandRequired;
  if (anyPatternMatch(slotKey, SOURCE_MATERIAL_PATTERNS)) return CATEGORY.sourceMaterial;
  if (anyPatternMatch(slotKey, MEDIA_OPTIONAL_PATTERNS)) return CATEGORY.mediaOptional;
  if (need.needsSourceBackedEvidence) return CATEGORY.requiresSourceEvidence;
  if (!tributeHas && usagePct >= 60 && need.aiDraftHumanReviewAcceptable) return CATEGORY.candidateForTributeCompletion;
  if (usagePct >= 35) return CATEGORY.commonOptional;
  return CATEGORY.brandSpecific;
}

function rationaleForClassification(classification, usagePct, brandsUsing) {
  if (classification === CATEGORY.coreRequired) return "Core display model slot used to anchor completed-brand baseline sections.";
  if (classification === CATEGORY.tabRequired) return "Required to avoid tab-level blank/fallback-heavy experiences in completed profiles.";
  if (classification === CATEGORY.softBrandRequired) return "Soft-brand owner/positioning context expected for Tribute-style profile quality.";
  if (classification === CATEGORY.collectionBrandRequired) return "Collection/portfolio-context slot; required for some brand families, not universally.";
  if (classification === CATEGORY.commonOptional) return `Used by ${usagePct}% of completed brands; valuable but not universally mandatory.`;
  if (classification === CATEGORY.brandSpecific) return `Low adoption (${usagePct}%); treated as brand-specific unless future evidence raises priority.`;
  if (classification === CATEGORY.mediaOptional) return "Media/image slot; should only be populated with approved assets.";
  if (classification === CATEGORY.sourceMaterial) return "Material/source-linked slot with evidence and attachment expectations.";
  if (classification === CATEGORY.notApplicableToTribute) return "Slot should remain blank for Tribute until evidence or relevance changes.";
  if (classification === CATEGORY.requiresSourceEvidence)
    return `Evidence-sensitive slot (${brandsUsing.length} completed brands use it); source-backed data required.`;
  if (classification === CATEGORY.candidateForTributeCompletion) return "High-usage missing slot that is safe to fill via AI draft + human review.";
  return "Unclassified.";
}

function proposedAction(classification, need, tributeHas) {
  if (classification === CATEGORY.notApplicableToTribute || need.shouldRemainBlank) return "Leave blank; require approved source evidence before any future fill.";
  if (tributeHas) return "Retain; assess weak structure/media gaps before any copy edits.";
  if (classification === CATEGORY.mediaOptional) return "Do not create without approved media asset.";
  if (classification === CATEGORY.sourceMaterial || classification === CATEGORY.requiresSourceEvidence)
    return "Queue for source-backed capture; no AI-only write.";
  if (classification === CATEGORY.candidateForTributeCompletion || classification === CATEGORY.coreRequired || classification === CATEGORY.tabRequired)
    return "Include in v19 completion writer plan (dry-run only until approved).";
  if (classification === CATEGORY.softBrandRequired) return "Candidate for v19 AI-draft/human-review owner-facing copy.";
  if (classification === CATEGORY.commonOptional) return "Defer unless parity uplift target requires optional depth.";
  return "Exclude from current completion scope.";
}

function exactContentNeeded(slotKey, classification) {
  if (/^commercial\./i.test(slotKey)) return "Owner-facing commercial mechanism text (benefit + project impact), optionally KPI labels/values.";
  if (/^economics\./i.test(slotKey)) return "Source-backed economics/legal narrative, ranges, and risk framing with human review.";
  if (/^loyalty\./i.test(slotKey)) return "Program narrative, KPI values, owner implications, and proof rows aligned to available sources.";
  if (/^operations\./i.test(slotKey)) return "Operating model standards, flexibility levels, and compliance guidance copy.";
  if (/^footprint\./i.test(slotKey)) return "Geography/growth editorial, momentum timeline, and fit bullets from approved evidence.";
  if (/^materials\.file$/i.test(slotKey)) return "Material card rows with title/meta and approved attachment or URL.";
  if (/^materials\.caseStudy$/i.test(slotKey)) return "Case-study card narrative and optional case-summary fields with source-backed facts.";
  if (/^materials\.gallery\./i.test(slotKey)) return "Approved image attachment (optional title).";
  if (/^insight\./i.test(slotKey)) return "Dealality summary/peer rows with clearly labeled non-validation stance.";
  if (/^valueOwners\./i.test(slotKey) || /^overview\./i.test(slotKey) || /^standards\./i.test(slotKey))
    return "Owner-facing narrative and scenario copy aligned to Tribute positioning.";
  if (classification === CATEGORY.brandSpecific) return "Brand-specific content only if future Tribute strategy explicitly requires it.";
  return "Editorial slot content with tab-specific owner-facing detail.";
}

function scoreManifest(rows) {
  const requiredRows = rows.filter((r) =>
    [CATEGORY.coreRequired, CATEGORY.tabRequired, CATEGORY.softBrandRequired, CATEGORY.candidateForTributeCompletion].includes(
      r.classification
    )
  );
  const optionalRows = rows.filter((r) =>
    [CATEGORY.commonOptional, CATEGORY.mediaOptional, CATEGORY.sourceMaterial, CATEGORY.collectionBrandRequired].includes(
      r.classification
    )
  );
  const requiredTotal = requiredRows.length || 1;
  const requiredHas = requiredRows.filter((r) => r.tributeHasSlot).length;
  const optionalTotal = optionalRows.length || 1;
  const optionalHas = optionalRows.filter((r) => r.tributeHasSlot).length;
  const evidencePenalty = rows.filter((r) => r.classification === CATEGORY.requiresSourceEvidence && !r.tributeHasSlot).length;
  const base = (requiredHas / requiredTotal) * 80 + (optionalHas / optionalTotal) * 20;
  const penalty = Math.min(15, evidencePenalty * 0.4);
  return Math.max(0, Math.round(base - penalty));
}

export async function buildBrandExplorerSlotStandardManifestReport(options = {}) {
  const brandIdOrName = normalizeBrandInput(options.brandIdOrName);
  const tribute = await fetchBrand(brandIdOrName);
  if (!tribute) throw new Error(`Unable to read target brand: ${brandIdOrName}`);

  const refs = [];
  for (const name of REFERENCE_BRANDS) {
    const payload = await fetchBrand(name);
    refs.push({ name, payload, source: payload ? "live-api" : "unavailable", readable: Boolean(payload) });
  }
  const completedRefs = refs.filter((r) => r.payload);

  const fixtureManifest = readFixtureManifest();
  const tributeSlots = summarizeBlocks(tribute);
  const refSummaries = completedRefs.map((r) => ({ name: r.name, slots: summarizeBlocks(r.payload) }));

  const union = new Set();
  for (const { slots } of refSummaries) for (const slotKey of slots.keys()) union.add(slotKey);
  for (const slotKey of tributeSlots.keys()) union.add(slotKey);
  const allSlotKeys = Array.from(union).sort((a, b) => a.localeCompare(b));

  const rows = allSlotKeys.map((slotKey) => {
    const refStates = refSummaries.map((r) => {
      const hit = r.slots.get(slotKey);
      return {
        brand: r.name,
        hasSlot: Boolean(hit),
        rowCount: hit?.rowCount || 0,
        hasTitle: Boolean(hit?.hasTitle),
        hasBody: Boolean(hit?.hasBody),
        hasImage: Boolean(hit?.hasImage),
        hasUrl: Boolean(hit?.hasUrl),
        examples: (hit?.rows || []).slice(0, 1).map((x) => x.example).filter(hasVal),
      };
    });
    const tributeState = tributeSlots.get(slotKey);
    const brandsUsing = refStates.filter((x) => x.hasSlot).map((x) => x.brand);
    const completedCount = completedRefs.length;
    const usingCount = brandsUsing.length;
    const usagePct = pct(usingCount, completedCount);
    const need = summarizeNeed(
      slotKey,
      { tab: tabFromSlot(slotKey) },
      usagePct
    );
    const classification = classificationForSlot(slotKey, usagePct, Boolean(tributeState), need);
    const requiredForParity = [CATEGORY.coreRequired, CATEGORY.tabRequired, CATEGORY.softBrandRequired, CATEGORY.candidateForTributeCompletion].includes(
      classification
    );
    const safeForTribute = !need.shouldRemainBlank && !anyPatternMatch(slotKey, MEDIA_OPTIONAL_PATTERNS);
    const referenceExamples = refStates.flatMap((r) => r.examples.map((e) => `${r.brand}: ${e}`)).slice(0, 3);
    const proposedActionText = proposedAction(classification, need, Boolean(tributeState));

    return {
      slotKey,
      tab: tabFromSlot(slotKey),
      displaySection: sectionFromSlot(slotKey),
      brandsUsing,
      completedBrandsUsingCount: usingCount,
      completedBrandsUsingPct: usagePct,
      tributeHasSlot: Boolean(tributeState),
      tributeHasTitle: Boolean(tributeState?.hasTitle),
      tributeHasBody: Boolean(tributeState?.hasBody),
      tributeHasImage: Boolean(tributeState?.hasImage),
      tributeHasUrlMaterialLink: Boolean(tributeState?.hasUrl),
      referenceExamples,
      classification,
      classificationRationale: rationaleForClassification(classification, usagePct, brandsUsing),
      visibleInUi: need.visibleInUi,
      requiredForCompletedBrandParity: requiredForParity,
      safeForTribute,
      needsSourceBackedEvidence: need.needsSourceBackedEvidence,
      aiDraftHumanReviewAcceptable: need.aiDraftHumanReviewAcceptable,
      shouldRemainBlank: need.shouldRemainBlank,
      proposedAction: proposedActionText,
      missingTributeAssessment: {
        trulyNeededForTribute: !tributeState && requiredForParity && !need.shouldRemainBlank,
        completedBrandsUsing: brandsUsing,
        tabSectionSupport: `${tabFromSlot(slotKey)} / ${sectionFromSlot(slotKey)}`,
        visibleInUi: need.visibleInUi,
        requiresSourceEvidence: need.needsSourceBackedEvidence,
        aiDraftHumanReviewAcceptable: need.aiDraftHumanReviewAcceptable,
        shouldRemainBlank: need.shouldRemainBlank,
        exactContentNeeded: exactContentNeeded(slotKey, classification),
        includeInFutureV19Writer:
          !tributeState &&
          [CATEGORY.coreRequired, CATEGORY.tabRequired, CATEGORY.softBrandRequired, CATEGORY.candidateForTributeCompletion].includes(
            classification
          ) &&
          !need.shouldRemainBlank,
      },
    };
  });

  const requiredForParityRows = rows.filter((r) => r.requiredForCompletedBrandParity);
  const requiredHasRows = requiredForParityRows.filter((r) => r.tributeHasSlot);
  const requiredMissingRows = requiredForParityRows.filter((r) => !r.tributeHasSlot);
  const optionalNotRequiredRows = rows.filter((r) => !r.requiredForCompletedBrandParity && r.classification === CATEGORY.commonOptional);
  const brandSpecificExcludedRows = rows.filter((r) => r.classification === CATEGORY.brandSpecific);
  const sourceEvidenceRows = rows.filter((r) => r.needsSourceBackedEvidence);
  const aiSafeRows = rows.filter((r) => r.aiDraftHumanReviewAcceptable);
  const shouldRemainBlankRows = rows.filter((r) => r.shouldRemainBlank);

  const revisedScore = scoreManifest(rows);
  const comparable = revisedScore >= 85 && requiredMissingRows.length === 0;
  const v19Needed = !comparable;
  const v19CreateUpdateSlots = rows
    .filter((r) => r.missingTributeAssessment.includeInFutureV19Writer)
    .map((r) => r.slotKey)
    .sort((a, b) => a.localeCompare(b));

  return {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    imagesUntouched: true,
    copyUntouched: true,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    marriottValidationImplied: false,
    filesRead: [
      "AGENTS.md",
      "reports/brand-explorer-presentation-slot-coverage-audit.md",
      "reports/brand-explorer-presentation-slot-coverage-audit.json",
      "lib/partner-intelligence/brand-explorer-presentation-slot-coverage-audit.js",
      "docs/brand-explorer-presentation-slots.md",
      "api/brand-library.js",
      "api/lib/partner-intelligence-field-map.js",
      "api/lib/partner-intelligence-explorer-field-registry.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "public/js/brand-explorer-gold-detail.js",
      "reports/brand-explorer-display-parity-audit.md",
      "reports/brand-explorer-display-parity-audit.json",
      "reports/brand-explorer-display-content-completion-writer.md",
      "reports/brand-explorer-display-content-completion-writer.json",
      "reports/brand-explorer-visual-qa-verification.md",
      "reports/tribute-portfolio-package-pipeline.md",
      ...fixtureManifest.files,
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-slot-standard-manifest.js",
      "scripts/brand-explorer-slot-standard-manifest.mjs",
      "docs/data-intelligence/brand-explorer-slot-standard-manifest-v18.md",
      "reports/brand-explorer-slot-standard-manifest.md",
      "reports/brand-explorer-slot-standard-manifest.json",
      "package.json",
    ],
    v18ManifestExists: true,
    brand: { recordId: toText(tribute.id) || DEFAULT_BRAND_ID, name: toText(tribute.name) || DEFAULT_BRAND_NAME },
    referenceBrandsInspected: refs.map((r) => ({ name: r.name, source: r.source, readable: r.readable })),
    totalSlotsReviewed: rows.length,
    totalUniqueSlotKeysAcrossCompletedBrands: allSlotKeys.length,
    tributeSlotKeysPresent: Array.from(tributeSlots.keys()).sort((a, b) => a.localeCompare(b)),
    slotStandardManifestRows: rows,
    requiredSlotsForCompletedBrandParity: requiredForParityRows.map((r) => r.slotKey),
    requiredSlotsTributeAlreadyHas: requiredHasRows.map((r) => r.slotKey),
    requiredSlotsTributeMissing: requiredMissingRows.map((r) => r.slotKey),
    optionalSlotsNotRequired: optionalNotRequiredRows.map((r) => r.slotKey),
    brandSpecificSlotsExcluded: brandSpecificExcludedRows.map((r) => r.slotKey),
    slotsThatNeedSourceEvidence: sourceEvidenceRows.map((r) => r.slotKey),
    slotsSafeForAiDraftHumanReview: aiSafeRows.map((r) => r.slotKey),
    slotsShouldRemainBlankForTribute: shouldRemainBlankRows.map((r) => r.slotKey),
    revisedRealisticTributeCompletionScore: revisedScore,
    tributeCompletedBrandComparableUnderManifest: comparable,
    v19SlotCompletionWriterNeeded: v19Needed,
    v19CreateOrUpdateSlots: v19CreateUpdateSlots,
    fixtureReadSummary: {
      fixtureFilesRead: fixtureManifest.files.length,
      fixtureSlotUniverseCount: fixtureManifest.slotKeys.length,
    },
    exactNextCommand: "npm run brand-explorer-slot-standard-manifest -- --brand tribute-portfolio --dry-run",
  };
}

function short(text, max = 140) {
  const s = toText(text).replace(/\s+/g, " ");
  return s.length > max ? `${s.slice(0, max - 1)}...` : s;
}

export function buildBrandExplorerSlotStandardManifestMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Slot Standard Manifest v18");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push("");
  lines.push("## Reference brands inspected");
  report.referenceBrandsInspected.forEach((r) => {
    lines.push(`- ${r.name} · source: ${r.source} · readable: ${r.readable ? "yes" : "no"}`);
  });
  lines.push("");
  lines.push("## Manifest outcomes");
  lines.push(`- Total slots reviewed: **${report.totalSlotsReviewed}**`);
  lines.push(`- Total unique slot keys across completed brands: **${report.totalUniqueSlotKeysAcrossCompletedBrands}**`);
  lines.push(`- Tribute slot keys present: **${report.tributeSlotKeysPresent.length}**`);
  lines.push(`- Required slots for completed-brand parity: **${report.requiredSlotsForCompletedBrandParity.length}**`);
  lines.push(`- Required slots Tribute already has: **${report.requiredSlotsTributeAlreadyHas.length}**`);
  lines.push(`- Required slots Tribute is missing: **${report.requiredSlotsTributeMissing.length}**`);
  lines.push(`- Optional slots not required: **${report.optionalSlotsNotRequired.length}**`);
  lines.push(`- Brand-specific slots excluded: **${report.brandSpecificSlotsExcluded.length}**`);
  lines.push(`- Slots that require source evidence: **${report.slotsThatNeedSourceEvidence.length}**`);
  lines.push(`- Slots safe for AI-drafted/human-review content: **${report.slotsSafeForAiDraftHumanReview.length}**`);
  lines.push(`- Slots that should remain blank for Tribute: **${report.slotsShouldRemainBlankForTribute.length}**`);
  lines.push("");
  lines.push("## Revised completion verdict");
  lines.push(`- Revised realistic Tribute completion score: **${report.revisedRealisticTributeCompletionScore}/100**`);
  lines.push(
    `- Completed-brand comparable under slot manifest: **${report.tributeCompletedBrandComparableUnderManifest ? "yes" : "no"}**`
  );
  lines.push(`- v19 slot completion writer needed: **${report.v19SlotCompletionWriterNeeded ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Candidate v19 slot list");
  if (report.v19CreateOrUpdateSlots.length) {
    report.v19CreateOrUpdateSlots.forEach((slotKey) => lines.push(`- ${slotKey}`));
  } else {
    lines.push("- None");
  }
  lines.push("");
  lines.push("## Slot manifest table (selected)");
  lines.push("");
  lines.push("| Slot key | Tab | Use % | Tribute | Class | Required parity | Source evidence | AI-draft ok | Action |");
  lines.push("|---|---|---:|---|---|---|---|---|---|");
  report.slotStandardManifestRows.slice(0, 120).forEach((r) => {
    lines.push(
      `| ${r.slotKey} | ${r.tab} | ${r.completedBrandsUsingPct}% | ${r.tributeHasSlot ? "yes" : "no"} | ${r.classification} | ${r.requiredForCompletedBrandParity ? "yes" : "no"} | ${r.needsSourceBackedEvidence ? "yes" : "no"} | ${r.aiDraftHumanReviewAcceptable ? "yes" : "no"} | ${short(r.proposedAction)} |`
    );
  });
  if (report.slotStandardManifestRows.length > 120) {
    lines.push(`| ... ${report.slotStandardManifestRows.length - 120} additional rows | | | | | | | | |`);
  }
  lines.push("");
  lines.push("## Guardrails");
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push(`- Company Validation Date untouched: **${report.companyValidationDateUntouched ? "yes" : "no"}**`);
  lines.push(`- Images untouched: **${report.imagesUntouched ? "yes" : "no"}**`);
  lines.push(`- Copy untouched: **${report.copyUntouched ? "yes" : "no"}**`);
  lines.push(`- Marriott validation implied: **${report.marriottValidationImplied ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Next command");
  lines.push("");
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  lines.push("");
  return lines.join("\n");
}
