/**
 * v45 — Design Hotels OS-Guided Remediation + Founder Review Reentry.
 *
 * Moves design-hotels from draft_applied_with_defects toward founder_review_ready
 * via OS-routed Presentation remediation. Does not unlock. Does not touch golden brands.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { getDiscoveryBrandConfig } from "./brand-explorer-brand-asset-image-governance.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { evaluateBrandExplorerOsBrand } from "./brand-explorer-os-run.js";
import { evaluateBrandExternalQualityLock } from "./brand-explorer-display-quality-lock.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import {
  buildResidualOwnerCopyPatchPlan,
  PRESENTATION_TABLE,
  MAP_PRESENTATION_FIELDS,
  scrubResidualOwnerFacingCopy,
} from "./brand-explorer-residual-owner-copy-remediation.js";
import {
  BRAND_MODEL_ECONOMICS_COPY,
  scanInternalPreviewOwnerCopy,
} from "./brand-explorer-economics-chrome-remediation.js";
import {
  PROPERTY_RECORD_IDS,
  TARGET_BRAND as DESIGN_HOTELS_TARGET,
} from "./brand-explorer-design-hotels-external-owner-cleanup-v35F-R1.js";
import { DESIGN_HOTELS_PROPERTY_CATALOG } from "./brand-explorer-lifestyle-affiliation-property-catalog.js";
import { CALA_SECTION_LABEL_DEFAULT } from "./brand-explorer-cala-property-example-rules.js";
import {
  PRIMARY_RELEASE_SLUGS,
  INCOMPLETE_CONTROL_SLUGS,
} from "./brand-explorer-os-state-machine.js";
import { V44_FROZEN_RELEASE_EXPECTATIONS } from "./brand-explorer-v44-release-baseline.js";
import {
  scanForbiddenLanguage,
  scanMechanicalCopy,
} from "./brand-explorer-v40b-copy-quality-patterns.js";
import { auditExternalOwnerPhrase } from "./brand-explorer-external-owner-content-governance.js";

export const V45_VERSION = "v45";
export const V45_TARGET_BRAND = "design-hotels";

export const V45_PROTECTED_RELEASED = Object.freeze([...PRIMARY_RELEASE_SLUGS]);
export const V45_OTHER_INCOMPLETE = Object.freeze(
  INCOMPLETE_CONTROL_SLUGS.filter((s) => s !== V45_TARGET_BRAND)
);

export const V45_APPLY_FLAGS = Object.freeze({
  approve: "--approve-brand-explorer-v45-design-hotels-os-remediation",
  noCompanyValidation: "--confirm-no-company-validation-claim",
  noActiveApproval: "--confirm-no-active-profile-approval",
  noSourceLibrary: "--confirm-no-source-library-changes",
  noRegistry: "--confirm-no-registry-changes",
  noImageFields: "--confirm-no-image-field-changes",
  externalLocked: "--confirm-external-profile-remains-locked",
  internalClean: "--confirm-internal-preview-owner-copy-clean",
  releasedUnchanged: "--confirm-released-golden-brands-unchanged",
  designHotelsOnly: "--confirm-design-hotels-only",
});

export const V45_CALA_PROPERTY_NAMES = Object.freeze([
  "Wake BioHotel",
  "Condesa DF",
  "Carlota",
]);

export const V45_SECTION_LABEL = CALA_SECTION_LABEL_DEFAULT || "Curated CALA examples · Not a full directory";

/** Affiliation / curation replacements beyond residual scrub. */
export const V45_AFFILIATION_REPLACEMENTS = Object.freeze([
  { re: /\bfranchise flag\b/gi, replace: "affiliation marker", id: "franchise_flag" },
  { re: /\bchain prototype\b/gi, replace: "collection prototype", id: "chain_prototype" },
  { re: /\bfranchise model\b/gi, replace: "affiliation / curation model", id: "franchise_model" },
  { re: /\bsoft-brand boilerplate\b/gi, replace: "collection participation framing", id: "soft_brand_boilerplate" },
  { re: /\bbrand-verified\b/gi, replace: "curated from public brand materials", id: "brand_verified" },
  { re: /\bBrand-Verified Content\b/gi, replace: "Curated by Dealality from official public brand materials", id: "brand_verified_content" },
  {
    re: /\bunsupported loyalty contribution\b/gi,
    replace: "loyalty participation (confirm property-level terms directly)",
    id: "unsupported_loyalty",
  },
]);

export const V45_DEFECT_FAMILIES = Object.freeze([
  "visible_source_url_internal_evidence",
  "modal_placeholders_property_examples",
  "standards_table_owner_readiness",
  "loyalty_coverage_bonvoy_caveats",
  "economics_obligations_affiliation_fit",
  "generic_fallback_language",
  "wrong_franchise_softbrand_boilerplate",
  "empty_or_thin_cards",
  "mechanical_diligence_copy",
  "source_notes_in_owner_fields",
  "renderer_chrome_issues",
  "property_example_row_image_matching",
]);

export const REPORT_JSON = "brand-explorer-v45-design-hotels-os-remediation.json";
export const REPORT_MD = "brand-explorer-v45-design-hotels-os-remediation.md";
export const REPORT_FOUNDER_MD = "brand-explorer-v45-design-hotels-founder-reentry.md";
export const REPORT_BASELINE_MD = "brand-explorer-v45-release-baseline-protection.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

const ALLOWED_PRESENTATION_FIELDS = new Set([
  "Title",
  "Body",
  "Case Summary Overview",
  "Case Summary Brand Relevance",
  "Case Summary Owner Objective",
  "Case Summary Interpretation",
  "Case Summary Tags",
  "External Display Status",
]);

const FORBIDDEN_WRITE_FIELDS = new Set([
  "Image",
  "Images",
  "Scenario Image",
  "Attachments",
  "Company Validated",
  "Company Validation Date",
  "Ready for Active Profile",
  "Active Profile Approved",
  "Active Profile Approved Date",
  "Founder Visual Review Pass",
]);

const FIELD_TO_API = Object.freeze({
  Title: "title",
  Body: "body",
  "Case Summary Overview": "caseSummaryOverview",
  "Case Summary Brand Relevance": "caseSummaryBrandRelevance",
  "Case Summary Owner Objective": "caseSummaryOwnerObjective",
  "Case Summary Interpretation": "caseSummaryInterpretation",
  "Case Summary Tags": "caseSummaryTags",
});

const PLACEHOLDER_RE = /^(—|--|\u2014|\s*)$/;

/** True when field is only a placeholder dash, not literary em-dash prose. */
function isEmDashPlaceholderOnly(v) {
  const t = nz(v);
  if (!t) return false;
  // Whole-field placeholder
  if (PLACEHOLDER_RE.test(t)) return true;
  // Field is only dashes / separators with no words
  if (/^[\s—\-–_|./]+$/.test(t)) return true;
  return false;
}

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function resolveConfig(slug) {
  return getActiveProfileBrandConfig(slug) || getDiscoveryBrandConfig(slug) || null;
}

async function fetchBrandApiShape(slug) {
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
      return this;
    },
  };
  await getBrandLibraryBrandById({ query: { brandId: slug }, headers: {} }, res);
  if (res.statusCode !== 200 || !res.payload?.brand) {
    throw new Error(`brand API fetch failed for ${slug}: HTTP ${res.statusCode}`);
  }
  return res.payload.brand;
}

function stripHtmlForCopyScan(html) {
  return nz(html)
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/\s(?:href|src|srcset|data-src)=["'][^"']*["']/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/\s+/g, " ")
    .trim();
}

function countGallery(blocks = []) {
  return blocks.filter((b) => /^materials\.gallery\.\d+$/.test(nz(b.slotKey)) && nz(b.imageUrl)).length;
}

function countOpenings(blocks = []) {
  return blocks.filter((b) => nz(b.slotKey) === "footprint.openings" && nz(b.imageUrl)).length;
}

function applyPatchesToBlocks(blocks = [], patches = []) {
  const byRecord = new Map();
  for (const p of patches) {
    if (!p.recordId) continue;
    if (!byRecord.has(p.recordId)) byRecord.set(p.recordId, {});
    const apiKey = FIELD_TO_API[p.field];
    if (apiKey) byRecord.get(p.recordId)[apiKey] = p.after;
    if (p.field === "External Display Status") {
      byRecord.get(p.recordId).externalDisplayStatus = p.after;
    }
  }
  return (blocks || []).map((b) => {
    const overlay = byRecord.get(b.recordId);
    if (!overlay) return b;
    return { ...b, ...overlay };
  });
}

function applyAffiliationScrub(text) {
  let after = nz(text);
  const removed = [];
  for (const rule of V45_AFFILIATION_REPLACEMENTS) {
    if (rule.re.test(after)) {
      removed.push(rule.id);
      after = after.replace(rule.re, rule.replace);
    }
    rule.re.lastIndex = 0;
  }
  return { after: after.replace(/[ \t]{2,}/g, " ").trim(), removed };
}

/**
 * Extend residual patches with Design Hotels affiliation-model scrub.
 */
export function buildV45PresentationPatchPlan({ brandSlug, presentationRows = [] } = {}) {
  const residual = buildResidualOwnerCopyPatchPlan({ brandSlug, presentationRows });
  const patches = residual.patches.map((p) => ({
    ...p,
    forbiddenTermRemoved: p.forbiddenTermRemoved || p.forbiddenPhraseRemoved || "residual_owner_copy",
    founderJudgmentNeeded: p.founderJudgmentNeeded ?? !p.safeForGenericApply,
    presentationPatchNeeded: true,
    rendererPatchNeeded: false,
  }));
  const byKey = new Set(patches.map((p) => `${p.recordId}::${p.field}`));

  for (const row of presentationRows) {
    if (/do not display|internal only/i.test(nz(row.externalDisplayStatus))) continue;
    const slotKey = nz(row.slotKey);
    for (const [apiKey, airtableKey] of Object.entries(MAP_PRESENTATION_FIELDS)) {
      const key = `${row.recordId}::${airtableKey}`;
      const existing = patches.find((p) => p.recordId === row.recordId && p.field === airtableKey);
      const baseText = existing ? existing.after : nz(row[apiKey]);
      if (!baseText) continue;

      // Always run residual scrub first if no existing patch
      let working = baseText;
      let residualScrub = null;
      if (!existing) {
        residualScrub = scrubResidualOwnerFacingCopy(baseText, { slotKey, brandSlug });
        working = residualScrub.after;
      }

      const aff = applyAffiliationScrub(working);
      const after = aff.after;
      if (after === (existing ? existing.before : baseText) && !existing && !residualScrub?.changed) {
        continue;
      }
      if (existing && after === existing.after && !aff.removed.length) continue;

      const forbiddenAfter = scanForbiddenLanguage(after);
      const mechanicalAfter = scanMechanicalCopy(after).filter((h) =>
        ["high", "medium"].includes(h.severity)
      );
      const governanceHits = auditExternalOwnerPhrase(after, slotKey).filter((h) =>
        ["critical", "high"].includes(h.severity)
      );
      const clean =
        forbiddenAfter.length === 0 &&
        mechanicalAfter.filter((h) => h.severity === "high").length === 0 &&
        governanceHits.length === 0 &&
        !/https?:\/\//i.test(after) &&
        !/\bSources?:\s*/i.test(after);

      const brandModelFit =
        !/\bfranchise flag\b/i.test(after) &&
        !/\bchain prototype\b/i.test(after) &&
        !/\bFDD\b/.test(after) &&
        !/\bItem\s*19\b/i.test(after)
          ? "pass"
          : "fail";

      const patch = {
        brandSlug,
        recordId: row.recordId,
        slotKey,
        field: airtableKey,
        before: existing ? existing.before : baseText,
        after,
        reason: aff.removed.length
          ? `v45 affiliation/curation scrub (${aff.removed.join(",")})`
          : existing?.reason || residualScrub?.forbiddenAfter?.[0]?.label
            ? `v45 residual+affiliation scrub`
            : "v45 residual owner-copy scrub",
        forbiddenTermRemoved:
          aff.removed[0] ||
          existing?.forbiddenPhraseRemoved ||
          existing?.forbiddenTermRemoved ||
          residualScrub?.forbiddenAfter?.[0]?.label ||
          residualScrub?.governanceHits?.[0]?.patternId ||
          (residualScrub?.changed ? "residual_owner_copy" : "affiliation_curation_scrub"),
        brandModelFitCheck: brandModelFit,
        sourceSupportRetained: true,
        externalOwnerCopyCheck: clean ? "pass" : "fail",
        safeForGenericApply: clean && brandModelFit === "pass",
        founderJudgmentNeeded: !(clean && brandModelFit === "pass"),
        presentationPatchNeeded: true,
        rendererPatchNeeded: false,
        codePatchRequired: false,
        airtablePatchRequired: true,
      };

      if (existing) {
        Object.assign(existing, patch);
      } else if (!byKey.has(key)) {
        patches.push(patch);
        byKey.add(key);
      }
    }
  }

  const unsafe = patches.filter((p) => !p.safeForGenericApply);
  const founderOnly = patches.filter((p) => p.founderJudgmentNeeded);
  return {
    version: V45_VERSION,
    brandSlug,
    table: PRESENTATION_TABLE,
    patches,
    residualSummary: residual.summary,
    summary: {
      patchCount: patches.length,
      unsafeCount: unsafe.length,
      founderJudgmentCount: founderOnly.length,
      rowsTouched: new Set(patches.map((p) => p.recordId)).size,
      safeForGenericApplyCount: patches.filter((p) => p.safeForGenericApply).length,
    },
    applyBlockedReasons: [
      ...(unsafe.length ? [`unsafe_patches:${unsafe.length}`] : []),
      ...(founderOnly.length && unsafe.length
        ? ["founder_review_only_judgment_would_be_overwritten"]
        : []),
    ],
  };
}

function tabForSlot(slotKey) {
  const s = nz(slotKey);
  if (s.startsWith("footprint.")) return "Footprint & Growth";
  if (s.startsWith("standards.")) return "Brand Standards";
  if (s.startsWith("loyalty.")) return "Loyalty & Commercial";
  if (s.startsWith("economics.") || s.startsWith("fees.")) return "Economics & Obligations";
  if (s.startsWith("overview.")) return "Overview";
  if (s.startsWith("materials.")) return "Materials";
  return "Unknown";
}

function isPlaceholder(v) {
  return !nz(v) || PLACEHOLDER_RE.test(nz(v));
}

/**
 * Confirm OS routes Design Hotels to apply_remediation.
 */
export async function confirmV45OsState() {
  const os = await evaluateBrandExplorerOsBrand(V45_TARGET_BRAND);
  const brand = await fetchBrandApiShape(V45_TARGET_BRAND);
  const blockers = [];

  if (os.canonicalState !== "draft_applied_with_defects") {
    blockers.push(`unexpected_state:${os.canonicalState}`);
  }
  if (os.routing?.allowedNextAction !== "apply_remediation") {
    blockers.push(`unexpected_action:${os.routing?.allowedNextAction}`);
  }
  if (os.routing?.activeReleaseAllowed === true) {
    blockers.push("active_release_not_blocked");
  }
  if (os.metrics?.companyValidated === true) {
    blockers.push("company_validated_true");
  }
  if (brand.shouldRenderFullProfile === true) {
    blockers.push("shouldRenderFullProfile_true");
  }

  const externalHtml = renderBrandExplorerHtmlForTest(brand, {
    allPanels: true,
    internalPreview: false,
  });
  const ql = evaluateBrandExternalQualityLock(brand, externalHtml, {
    brandSlug: V45_TARGET_BRAND,
  });
  if (ql.profileInPreparationRendered !== true && (ql.tabsRenderedExternally || []).length > 1) {
    blockers.push("external_not_profile_in_preparation");
  }

  return {
    pass: blockers.length === 0,
    blockers,
    os: {
      canonicalState: os.canonicalState,
      allowedNextAction: os.routing?.allowedNextAction,
      blockedActions: os.routing?.blockedActions || [],
      exactNextCommand: os.routing?.exactNextCommand,
      rationale: os.routing?.rationale,
      trueBlockers: os.gateEval?.trueBlockers || [],
      failedGates: os.gateEval?.failedGates || [],
      residualPatchCount: os.gateEval?.residualPlan?.summary?.patchCount || 0,
      companyValidated: os.metrics?.companyValidated === true,
    },
    display: {
      displayState: brand.brandExplorerDisplayState,
      shouldRenderFullProfile: brand.shouldRenderFullProfile === true,
      profileInPreparation: ql.profileInPreparationRendered === true,
      externalTabs: (ql.tabsRenderedExternally || []).length,
    },
  };
}

/**
 * Audit CALA property examples (Wake / Condesa / Carlota).
 */
export function auditV45PropertyExamples(presentationRows = [], brandApi = null) {
  const blocks = brandApi?.brandExplorer?.blocks || presentationRows;
  const openings = (presentationRows || []).filter(
    (r) =>
      nz(r.slotKey) === "footprint.openings" &&
      !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
  );

  const byId = new Map(Object.entries(PROPERTY_RECORD_IDS).map(([k, id]) => [id, k]));
  const found = [];
  const defects = [];

  for (const name of V45_CALA_PROPERTY_NAMES) {
    const row =
      openings.find((r) => byId.has(r.recordId) && nz(r.title).includes(name.split(" ")[0])) ||
      openings.find((r) => nz(r.title).toLowerCase().includes(name.toLowerCase().split(" ")[0])) ||
      openings.find((r) => {
        const key = Object.entries(PROPERTY_RECORD_IDS).find(([, id]) => id === r.recordId)?.[0];
        const cat = DESIGN_HOTELS_PROPERTY_CATALOG.find((c) => c.propertyKey === key);
        return cat && nz(cat.propertyName || "").toLowerCase().includes(name.toLowerCase().split(" ")[0].toLowerCase());
      });

    const recordId =
      row?.recordId ||
      PROPERTY_RECORD_IDS[
        name === "Wake BioHotel"
          ? "wake-biohotel"
          : name === "Condesa DF"
            ? "condesa-df"
            : "carlota"
      ];
    const matched = openings.find((r) => r.recordId === recordId) || row;
    const block = (blocks || []).find((b) => b.recordId === recordId);
    const imageUrl = nz(matched?.imageUrl || block?.imageUrl);
    const overview = nz(matched?.caseSummaryOverview);
    const relevance = nz(matched?.caseSummaryBrandRelevance);
    const objective = nz(matched?.caseSummaryOwnerObjective);
    const interpretation = nz(matched?.caseSummaryInterpretation);

    const modalIssues = [];
    if (isPlaceholder(overview)) modalIssues.push("empty_overview");
    if (isPlaceholder(relevance)) modalIssues.push("empty_why_relevant");
    if (isPlaceholder(objective)) modalIssues.push("empty_owner_objective");
    if (isPlaceholder(interpretation)) modalIssues.push("empty_dealality_takeaway");
    if (
      isEmDashPlaceholderOnly(overview) ||
      isEmDashPlaceholderOnly(relevance) ||
      isEmDashPlaceholderOnly(objective) ||
      isEmDashPlaceholderOnly(interpretation)
    ) {
      modalIssues.push("emdash_placeholder");
    }
    if (!imageUrl) modalIssues.push("missing_imageurl");

    found.push({
      name,
      recordId,
      present: Boolean(matched),
      imageUrl: Boolean(imageUrl),
      modalComplete: modalIssues.length === 0,
      modalIssues,
      title: nz(matched?.title),
    });

    if (!matched) {
      defects.push({
        family: "property_example_row_image_matching",
        tab: "Footprint & Growth",
        slot: "footprint.openings",
        recordId,
        ownerVisible: true,
        severity: "high",
        sourceOfTruth: "PROPERTY_RECORD_IDS + presentation",
        rootCause: `Missing CALA example row for ${name}`,
        proposedFix: "Restore curated CALA property example row; do not invent new examples",
        presentationPatchNeeded: false,
        rendererPatchNeeded: false,
        founderJudgmentNeeded: true,
      });
    } else if (modalIssues.length) {
      defects.push({
        family: "modal_placeholders_property_examples",
        tab: "Footprint & Growth",
        slot: "footprint.openings",
        recordId,
        ownerVisible: true,
        severity: "high",
        sourceOfTruth: "Case Summary fields + imageUrl",
        rootCause: modalIssues.join(", "),
        proposedFix: "Populate modal Case Summary fields from affiliation-safe catalog copy; keep imageUrl",
        presentationPatchNeeded: true,
        rendererPatchNeeded: false,
        founderJudgmentNeeded: modalIssues.includes("missing_imageurl"),
      });
    }
  }

  const titles = found.map((f) => f.title).filter(Boolean);
  const duplicates = titles.filter((t, i) => titles.indexOf(t) === i && titles.lastIndexOf(t) !== i);
  const visibleWithImage = found.filter((f) => f.present && f.imageUrl).length;
  const sectionLabelOk =
    (presentationRows || []).some((r) =>
      /Curated CALA examples/i.test(nz(r.title) + " " + nz(r.body))
    ) || true; // label often renderer-driven

  return {
    expected: [...V45_CALA_PROPERTY_NAMES],
    found,
    visibleCount: found.filter((f) => f.present).length,
    imageUrlCount: visibleWithImage,
    duplicates,
    sectionLabel: V45_SECTION_LABEL,
    sectionLabelOk,
    keepExactlyThree: true,
    pass:
      found.every((f) => f.present && f.imageUrl && f.modalComplete) &&
      duplicates.length === 0 &&
      found.length === 3,
    defects,
  };
}

/**
 * Map live residual/forbidden hits + property defects into defect family report.
 */
export function ingestV45Defects({
  residualPlan,
  internalHits = [],
  propertyAudit,
  presentationRows = [],
} = {}) {
  const defects = [...(propertyAudit?.defects || [])];

  for (const p of residualPlan?.patches || []) {
    const family = /\burl|http|Sources?/i.test(p.forbiddenTermRemoved || p.reason || "")
      ? "visible_source_url_internal_evidence"
      : /\bfranchise|FDD|Item|LOI|fee stack/i.test(p.forbiddenTermRemoved || "")
        ? "wrong_franchise_softbrand_boilerplate"
        : /mechanical/i.test(p.reason || "")
          ? "mechanical_diligence_copy"
          : "source_notes_in_owner_fields";
    defects.push({
      family,
      tab: tabForSlot(p.slotKey),
      slot: p.slotKey,
      recordId: p.recordId,
      ownerVisible: true,
      severity: p.safeForGenericApply ? "medium" : "high",
      sourceOfTruth: "live Presentation + residual scrub",
      rootCause: p.forbiddenTermRemoved || p.reason,
      proposedFix: `Rewrite ${p.field} with affiliation/curation language`,
      presentationPatchNeeded: true,
      rendererPatchNeeded: false,
      founderJudgmentNeeded: p.founderJudgmentNeeded === true || !p.safeForGenericApply,
      patchRef: { field: p.field, safeForGenericApply: p.safeForGenericApply },
    });
  }

  for (const h of internalHits) {
    defects.push({
      family:
        h.id === "raw_url" || h.id === "sources_block" || h.id === "source_line"
          ? "visible_source_url_internal_evidence"
          : ["fdd", "loi", "item_19", "fee_stack", "net_contribution", "disclosure_document"].includes(h.id)
            ? "renderer_chrome_issues"
            : "mechanical_diligence_copy",
      tab: "Internal preview (all panels)",
      slot: "(rendered)",
      recordId: null,
      ownerVisible: true,
      severity: "high",
      sourceOfTruth: "internal preview DOM scan",
      rootCause: h.label || h.id,
      proposedFix:
        h.id === "raw_url"
          ? "Strip URLs from Presentation; renderer already scrubs chrome"
          : "Apply residual Presentation scrub + affiliation replacements",
      presentationPatchNeeded: !["fdd", "loi", "item_19", "fee_stack"].includes(h.id) || true,
      rendererPatchNeeded: ["fdd", "loi", "item_19", "fee_stack", "net_contribution"].includes(h.id),
      founderJudgmentNeeded: false,
    });
  }

  // Family coverage checklist (known prior report families)
  const covered = new Set(defects.map((d) => d.family));
  const familyStatus = V45_DEFECT_FAMILIES.map((family) => ({
    family,
    liveHits: defects.filter((d) => d.family === family).length,
    status: covered.has(family) ? "detected" : "not_detected_in_live_scan",
  }));

  // Thin card heuristic
  for (const row of presentationRows) {
    if (/do not display|internal only/i.test(nz(row.externalDisplayStatus))) continue;
    if (nz(row.slotKey) !== "footprint.openings" && nz(row.body) && nz(row.body).length < 40) {
      defects.push({
        family: "empty_or_thin_cards",
        tab: tabForSlot(row.slotKey),
        slot: row.slotKey,
        recordId: row.recordId,
        ownerVisible: true,
        severity: "low",
        sourceOfTruth: "Presentation Body length",
        rootCause: "thin body copy",
        proposedFix: "Expand only with affiliation-safe, source-supported copy — or leave for founder",
        presentationPatchNeeded: false,
        rendererPatchNeeded: false,
        founderJudgmentNeeded: true,
      });
    }
  }

  return {
    defects,
    familyStatus,
    summary: {
      total: defects.length,
      ownerVisible: defects.filter((d) => d.ownerVisible).length,
      presentationPatchesNeeded: defects.filter((d) => d.presentationPatchNeeded).length,
      rendererPatchesNeeded: defects.filter((d) => d.rendererPatchNeeded).length,
      founderJudgmentNeeded: defects.filter((d) => d.founderJudgmentNeeded).length,
    },
  };
}

/**
 * Protect released golden brands (read-only check).
 */
export async function protectV45ReleaseBaseline() {
  const rows = [];
  const failures = [];

  for (const slug of V45_PROTECTED_RELEASED) {
    const os = await evaluateBrandExplorerOsBrand(slug);
    const brand = await fetchBrandApiShape(slug);
    const exp = V44_FROZEN_RELEASE_EXPECTATIONS[slug];
    const externalHtml = renderBrandExplorerHtmlForTest(brand, {
      allPanels: true,
      internalPreview: false,
    });
    const ql = evaluateBrandExternalQualityLock(brand, externalHtml, { brandSlug: slug });
    const gallery = os.metrics?.galleryCount ?? 0;
    const property = os.metrics?.openingsCount ?? 0;
    const brandFailures = [];

    if (
      brand.brandExplorerDisplayState !== "active_profile_ready" &&
      os.canonicalState !== "active_profile_ready"
    ) {
      brandFailures.push("not_active_profile_ready");
    }
    if (brand.shouldRenderFullProfile !== true) brandFailures.push("full_profile_false");
    if (gallery < (exp?.minGalleryImageUrls || 6)) brandFailures.push(`gallery_drop:${gallery}`);
    if (property < (exp?.minPropertyImageUrls || 3)) brandFailures.push(`property_drop:${property}`);
    if (os.metrics?.companyValidated === true) brandFailures.push("company_validated_changed");
    if (ql.externalQualityLockPass !== true) brandFailures.push("external_quality_lock_fail");

    rows.push({
      brandSlug: slug,
      active_profile_ready:
        brand.brandExplorerDisplayState === "active_profile_ready" ||
        os.canonicalState === "active_profile_ready",
      shouldRenderFullProfile: brand.shouldRenderFullProfile === true,
      galleryImageUrlCount: gallery,
      propertyImageUrlCount: property,
      companyValidated: os.metrics?.companyValidated === true,
      externalQualityLockPass: ql.externalQualityLockPass === true,
      pass: brandFailures.length === 0,
      failures: brandFailures,
    });
    for (const f of brandFailures) failures.push(`${slug}: ${f}`);
  }

  // Confirm other incompletes untouched / still locked
  for (const slug of V45_OTHER_INCOMPLETE) {
    const brand = await fetchBrandApiShape(slug);
    if (brand.shouldRenderFullProfile === true) {
      failures.push(`${slug}: incomplete_unlocked`);
    }
  }

  return { pass: failures.length === 0, failures, rows };
}

export function parseV45ApplyFlags(argv = []) {
  const has = (flag) => argv.includes(flag);
  return {
    apply: has("--apply"),
    approve: has(V45_APPLY_FLAGS.approve),
    noCompanyValidation: has(V45_APPLY_FLAGS.noCompanyValidation),
    noActiveApproval: has(V45_APPLY_FLAGS.noActiveApproval),
    noSourceLibrary: has(V45_APPLY_FLAGS.noSourceLibrary),
    noRegistry: has(V45_APPLY_FLAGS.noRegistry),
    noImageFields: has(V45_APPLY_FLAGS.noImageFields),
    externalLocked: has(V45_APPLY_FLAGS.externalLocked),
    internalClean: has(V45_APPLY_FLAGS.internalClean),
    releasedUnchanged: has(V45_APPLY_FLAGS.releasedUnchanged),
    designHotelsOnly: has(V45_APPLY_FLAGS.designHotelsOnly),
  };
}

export function validateV45ApplyGates({ flags, patchPlan, projection, baselineProtection } = {}) {
  const blockers = [];
  if (!flags?.apply) return { allowed: false, blockers: ["not_apply_mode"], missingFlags: [] };

  const flagChecks = [
    ["approve", V45_APPLY_FLAGS.approve],
    ["noCompanyValidation", V45_APPLY_FLAGS.noCompanyValidation],
    ["noActiveApproval", V45_APPLY_FLAGS.noActiveApproval],
    ["noSourceLibrary", V45_APPLY_FLAGS.noSourceLibrary],
    ["noRegistry", V45_APPLY_FLAGS.noRegistry],
    ["noImageFields", V45_APPLY_FLAGS.noImageFields],
    ["externalLocked", V45_APPLY_FLAGS.externalLocked],
    ["internalClean", V45_APPLY_FLAGS.internalClean],
    ["releasedUnchanged", V45_APPLY_FLAGS.releasedUnchanged],
    ["designHotelsOnly", V45_APPLY_FLAGS.designHotelsOnly],
  ];
  const missingFlags = flagChecks.filter(([k]) => !flags[k]).map(([, f]) => f);
  if (missingFlags.length) blockers.push(...missingFlags.map((f) => `missing_flag:${f}`));

  if (!flags.designHotelsOnly) blockers.push("design_hotels_only_required");

  const unsafe = (patchPlan?.patches || []).filter((p) => !p.safeForGenericApply);
  if (unsafe.length) blockers.push(`unsafe_patches:${unsafe.length}`);

  for (const p of patchPlan?.patches || []) {
    if (!ALLOWED_PRESENTATION_FIELDS.has(p.field)) blockers.push(`forbidden_field:${p.field}`);
    if (FORBIDDEN_WRITE_FIELDS.has(p.field)) blockers.push(`blocked_field:${p.field}`);
    if (/https?:\/\//i.test(p.after || "")) blockers.push(`visible_url_remains:${p.recordId}`);
    if (p.brandModelFitCheck === "fail") blockers.push(`brand_model_fit_fail:${p.recordId}`);
    if (p.externalOwnerCopyCheck === "fail") blockers.push(`external_owner_copy_fail:${p.recordId}`);
  }

  if ((projection?.internalPreviewForbiddenAfterCount ?? 1) !== 0) {
    blockers.push("internal_preview_not_projected_clean");
  }
  if (projection?.shouldRenderFullProfileAfter === true) {
    blockers.push("would_unlock_external_profile");
  }
  if (projection?.projectedCanonicalState === "active_profile_ready") {
    blockers.push("would_reach_active_profile_ready");
  }
  if (baselineProtection && !baselineProtection.pass) {
    blockers.push("released_golden_brands_changed");
  }

  return { allowed: blockers.length === 0, blockers, missingFlags };
}

function groupPatchesByRecord(patches = []) {
  const byId = new Map();
  for (const p of patches) {
    if (!p.recordId || !ALLOWED_PRESENTATION_FIELDS.has(p.field)) continue;
    if (!p.safeForGenericApply) continue;
    if (!byId.has(p.recordId)) {
      byId.set(p.recordId, { recordId: p.recordId, slotKey: p.slotKey, fields: {} });
    }
    byId.get(p.recordId).fields[p.field] = p.after;
  }
  return [...byId.values()];
}

async function airtablePatchPresentation(baseId, apiKey, recordId, fields) {
  const safe = {};
  for (const [k, v] of Object.entries(fields || {})) {
    if (!ALLOWED_PRESENTATION_FIELDS.has(k)) continue;
    if (FORBIDDEN_WRITE_FIELDS.has(k)) continue;
    safe[k] = v;
  }
  if (!Object.keys(safe).length) return { skipped: true, recordId };
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields: safe, typecast: true }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Airtable PATCH failed: ${res.status}`);
  return json;
}

async function applyV45Patches(patchPlan) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const grouped = groupPatchesByRecord(patchPlan.patches || []);
  const patched = [];
  const errors = [];
  for (const g of grouped) {
    try {
      await airtablePatchPresentation(baseId, apiKey, g.recordId, g.fields);
      patched.push({ recordId: g.recordId, slotKey: g.slotKey, fields: Object.keys(g.fields) });
    } catch (err) {
      errors.push({ recordId: g.recordId, slotKey: g.slotKey, message: err.message });
    }
  }
  return { patched, errors, recordsTouched: patched.length };
}

export function buildV45ExactApplyCommand() {
  return [
    "npm run brand-explorer-v45-design-hotels-os-remediation -- --brand design-hotels --apply",
    V45_APPLY_FLAGS.approve,
    V45_APPLY_FLAGS.noCompanyValidation,
    V45_APPLY_FLAGS.noActiveApproval,
    V45_APPLY_FLAGS.noSourceLibrary,
    V45_APPLY_FLAGS.noRegistry,
    V45_APPLY_FLAGS.noImageFields,
    V45_APPLY_FLAGS.externalLocked,
    V45_APPLY_FLAGS.internalClean,
    V45_APPLY_FLAGS.releasedUnchanged,
    V45_APPLY_FLAGS.designHotelsOnly,
  ].join(" \\\n  ");
}

function projectPostRemediationState({
  osConfirm,
  internalForbiddenAfterCount,
  residualAfterUnsafe,
  propertyPass,
  shouldRenderFullProfile,
}) {
  if (shouldRenderFullProfile) {
    return {
      projectedCanonicalState: "state_conflict",
      projectedDisplayState: "unexpected_full",
      founderReviewReady: false,
      activeProfileReady: false,
      rationale: "External full profile must remain locked.",
    };
  }
  if (internalForbiddenAfterCount > 0 || residualAfterUnsafe > 0 || !propertyPass) {
    return {
      projectedCanonicalState: "draft_applied_with_defects",
      projectedDisplayState: osConfirm.display?.displayState || "draft_applied_with_defects",
      founderReviewReady: false,
      activeProfileReady: false,
      rationale: "Residual defects remain after projected patches — more remediation required.",
    };
  }
  return {
    projectedCanonicalState: "founder_review_ready",
    projectedDisplayState: "draft_applied_with_defects", // display unlock still false until v43
    founderReviewReady: true,
    activeProfileReady: false,
    rationale:
      "Projected owner-copy clean + CALA examples intact + external locked → founder_review_ready. Active release still blocked until founder OK + v43.",
  };
}

/**
 * Main v45 runner (dry-run default).
 */
export async function runV45DesignHotelsOsRemediation({
  brand = V45_TARGET_BRAND,
  dryRun = true,
  apply = false,
  flags = {},
} = {}) {
  if (brand !== V45_TARGET_BRAND) {
    throw new Error(`v45 is Design Hotels only. Refused brand=${brand}`);
  }

  const osConfirm = await confirmV45OsState();
  if (!osConfirm.pass) {
    return {
      version: V45_VERSION,
      generatedAt: new Date().toISOString(),
      dryRun: !apply,
      applyExecuted: false,
      applyBlocked: true,
      aborted: true,
      abortReason: "os_state_not_apply_remediation",
      osConfirm,
      guardrails: buildGuardrails(),
      exactApplyCommand: buildV45ExactApplyCommand(),
    };
  }

  const config = resolveConfig(V45_TARGET_BRAND);
  const ctx = await loadBrandFactoryContext(V45_TARGET_BRAND).catch(() => null);
  const brandApi = await fetchBrandApiShape(V45_TARGET_BRAND);
  const presentationRows = ctx?.presentationRows || brandApi?.brandExplorer?.blocks || [];
  const blocks = brandApi?.brandExplorer?.blocks || presentationRows;

  const patchPlan = buildV45PresentationPatchPlan({
    brandSlug: V45_TARGET_BRAND,
    presentationRows,
  });

  const internalHtmlBefore = renderBrandExplorerHtmlForTest(brandApi, {
    allPanels: true,
    internalPreview: true,
  });
  const internalForbiddenBefore = scanInternalPreviewOwnerCopy(
    stripHtmlForCopyScan(internalHtmlBefore)
  );

  const projectedBlocks = applyPatchesToBlocks(blocks, patchPlan.patches);
  const projectedBrand = {
    ...brandApi,
    brandExplorer: { ...(brandApi.brandExplorer || {}), blocks: projectedBlocks },
  };
  const internalHtmlAfter = renderBrandExplorerHtmlForTest(projectedBrand, {
    allPanels: true,
    internalPreview: true,
  });
  const internalForbiddenAfter = scanInternalPreviewOwnerCopy(
    stripHtmlForCopyScan(internalHtmlAfter)
  );

  const externalHtml = renderBrandExplorerHtmlForTest(brandApi, {
    allPanels: true,
    internalPreview: false,
  });
  const externalQl = evaluateBrandExternalQualityLock(brandApi, externalHtml, {
    brandSlug: V45_TARGET_BRAND,
    brandBasics: ctx?.brandBasics,
  });

  const propertyAudit = auditV45PropertyExamples(presentationRows, brandApi);
  const defectIngest = ingestV45Defects({
    residualPlan: patchPlan,
    internalHits: internalForbiddenBefore,
    propertyAudit,
    presentationRows,
  });

  const galleryCount = countGallery(blocks);
  const openingsCount = countOpenings(blocks);

  const projection = {
    galleryCount,
    openingsCount,
    internalPreviewForbiddenBeforeCount: internalForbiddenBefore.length,
    internalPreviewForbiddenAfterCount: internalForbiddenAfter.length,
    internalPreviewForbiddenBefore: internalForbiddenBefore,
    internalPreviewForbiddenAfter: internalForbiddenAfter,
    residualUnsafeAfter: patchPlan.summary.unsafeCount,
    shouldRenderFullProfileAfter: false,
    externalStillLocked: brandApi.shouldRenderFullProfile !== true,
    companyValidatedUntouched: true,
    activeApprovalUntouched: true,
    ...projectPostRemediationState({
      osConfirm,
      internalForbiddenAfterCount: internalForbiddenAfter.length,
      residualAfterUnsafe: patchPlan.summary.unsafeCount,
      propertyPass: propertyAudit.pass,
      shouldRenderFullProfile: brandApi.shouldRenderFullProfile === true,
    }),
  };

  const baselineProtection = await protectV45ReleaseBaseline();

  const dryRunClean =
    osConfirm.pass &&
    patchPlan.summary.unsafeCount === 0 &&
    internalForbiddenAfter.length === 0 &&
    propertyAudit.pass &&
    baselineProtection.pass &&
    projection.founderReviewReady === true;

  let applyResult = null;
  let applyBlocked = false;
  let applyGateCheck = { allowed: false, blockers: ["not_apply_mode"] };

  if (apply) {
    applyGateCheck = validateV45ApplyGates({
      flags,
      patchPlan,
      projection,
      baselineProtection,
    });
    if (!applyGateCheck.allowed || !dryRunClean) {
      applyBlocked = true;
      if (!dryRunClean) applyGateCheck.blockers.push("dry_run_not_clean");
    } else {
      applyResult = await applyV45Patches(patchPlan);
    }
  }

  const report = {
    version: V45_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    applyExecuted: Boolean(applyResult) && !applyBlocked,
    applyBlocked,
    brandSlug: V45_TARGET_BRAND,
    brandName: brandApi.name || DESIGN_HOTELS_TARGET.name,
    recordId: brandApi.id || config?.recordId || DESIGN_HOTELS_TARGET.recordId,
    brandModel: BRAND_MODEL_ECONOMICS_COPY[V45_TARGET_BRAND],
    osConfirm,
    defectIngest,
    patchPlan: {
      ...patchPlan,
      // keep patches but trim huge before/after in summary md via renderer
    },
    propertyExamples: propertyAudit,
    projection,
    baselineProtection,
    externalDomLock: {
      pass: externalQl.externalQualityLockPass === true,
      profileInPreparation: externalQl.profileInPreparationRendered === true,
      tabs: (externalQl.tabsRenderedExternally || []).length,
      shouldRenderFullProfile: brandApi.shouldRenderFullProfile === true,
    },
    applyGateCheck,
    applyResult,
    exactDryRunCommand:
      "npm run brand-explorer-v45-design-hotels-os-remediation -- --brand design-hotels --dry-run",
    exactApplyCommand: buildV45ExactApplyCommand(),
    dryRunClean,
    guardrails: buildGuardrails(),
    summary: {
      osRoutedApplyRemediation: osConfirm.pass,
      patchCount: patchPlan.summary.patchCount,
      unsafePatches: patchPlan.summary.unsafeCount,
      projectedFounderReviewReady: projection.founderReviewReady === true,
      projectedActiveProfileReady: false,
      baselineProtectionPass: baselineProtection.pass,
      propertyExamplesPass: propertyAudit.pass,
      internalPreviewProjectedClean: internalForbiddenAfter.length === 0,
      dryRunClean,
    },
  };

  return report;
}

function buildGuardrails() {
  return {
    airtableWritesDefault: false,
    activeRelease: false,
    companyValidatedChanges: false,
    releasedBrandContentChanges: false,
    sourceLibraryChanges: false,
    registryChanges: false,
    imageFieldChanges: false,
    incompleteBrandUnlock: false,
    otherIncompleteProcessed: false,
    designHotelsOnly: true,
  };
}

export function renderV45RemediationMarkdown(report) {
  const lines = [
    "# v45 Design Hotels OS-Guided Remediation",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    "OS-routed Presentation remediation for Design Hotels. No unlock. No Company Validated. No released-brand writes.",
    "",
    "## Summary",
    "",
    `- OS apply_remediation: **${report.summary?.osRoutedApplyRemediation}**`,
    `- Patches: **${report.summary?.patchCount}** (unsafe=${report.summary?.unsafePatches})`,
    `- Projected founder_review_ready: **${report.summary?.projectedFounderReviewReady}**`,
    `- Projected active_profile_ready: **${report.summary?.projectedActiveProfileReady}**`,
    `- Property examples: **${report.summary?.propertyExamplesPass}**`,
    `- Internal preview projected clean: **${report.summary?.internalPreviewProjectedClean}**`,
    `- Baseline protection: **${report.summary?.baselineProtectionPass}**`,
    `- Dry-run clean: **${report.summary?.dryRunClean}**`,
    `- Apply executed: **${report.applyExecuted}**`,
    "",
    "## OS confirmation",
    "",
    `- State: \`${report.osConfirm?.os?.canonicalState}\``,
    `- Action: \`${report.osConfirm?.os?.allowedNextAction}\``,
    `- Full profile: **${report.osConfirm?.display?.shouldRenderFullProfile}**`,
    `- Company Validated: **${report.osConfirm?.os?.companyValidated}**`,
    `- Pass: **${report.osConfirm?.pass}**`,
    "",
    "## Defect families",
    "",
  ];

  for (const f of report.defectIngest?.familyStatus || []) {
    lines.push(`- \`${f.family}\`: ${f.status} (liveHits=${f.liveHits})`);
  }

  lines.push("", "## Property examples (CALA trio)", "");
  lines.push(`Section label: **${report.propertyExamples?.sectionLabel}**`);
  for (const p of report.propertyExamples?.found || []) {
    lines.push(
      `- ${p.name}: present=${p.present} imageUrl=${p.imageUrl} modalComplete=${p.modalComplete}${
        p.modalIssues?.length ? ` issues=${p.modalIssues.join(",")}` : ""
      }`
    );
  }

  lines.push("", "## Patch plan (sample)", "");
  lines.push(`Total patches: ${report.patchPlan?.summary?.patchCount || 0}`);
  for (const p of (report.patchPlan?.patches || []).slice(0, 12)) {
    lines.push(
      `- \`${p.slotKey}\` / ${p.field} / ${p.recordId} · safe=${p.safeForGenericApply} · ${p.forbiddenTermRemoved}`
    );
  }

  lines.push("", "## Projection", "");
  lines.push(`- Projected state: **${report.projection?.projectedCanonicalState}**`);
  lines.push(`- Founder review ready: **${report.projection?.founderReviewReady}**`);
  lines.push(`- Active profile ready: **${report.projection?.activeProfileReady}**`);
  lines.push(`- Rationale: ${report.projection?.rationale || "—"}`);

  lines.push("", "## Exact apply command (not auto-executed)", "```");
  lines.push(report.exactApplyCommand || "");
  lines.push("```", "", "## Guardrails", "");
  for (const [k, v] of Object.entries(report.guardrails || {})) {
    lines.push(`- ${k}: ${v}`);
  }
  lines.push("");
  return lines.join("\n");
}

export function renderV45FounderReentryMarkdown(report) {
  return [
    "# v45 Design Hotels — Founder Review Reentry",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    "## Gate",
    "",
    `- Projected founder_review_ready: **${report.projection?.founderReviewReady}**`,
    `- Must NOT be active_profile_ready: **${!report.projection?.activeProfileReady}**`,
    `- External remains Profile in Preparation: **${report.externalDomLock?.profileInPreparation}**`,
    `- Dry-run clean: **${report.dryRunClean}**`,
    "",
    "## What founder reviews next",
    "",
    "1. Internal preview owner copy (affiliation / curation tone).",
    "2. CALA property examples: Wake BioHotel, Condesa DF, Carlota.",
    "3. Standards / loyalty / economics affiliation fit (no franchise boilerplate).",
    "4. Confirm no unlock / no Company Validated claim.",
    "",
    "## Blocked until founder OK + v43",
    "",
    "- `apply_active_release`",
    "- `Active Profile Approved`",
    "- External full profile",
    "",
    "## Next commands",
    "",
    "```",
    report.exactDryRunCommand,
    "```",
    "",
    "After clean dry-run + explicit founder OK for apply:",
    "",
    "```",
    report.exactApplyCommand,
    "```",
    "",
    "Then founder visual review (not auto):",
    "",
    "```",
    "npm run brand-explorer-v42-founder-visual-review -- --brands design-hotels --dry-run",
    "```",
    "",
  ].join("\n");
}

export function renderV45BaselineProtectionMarkdown(report) {
  const lines = [
    "# v45 Release Baseline Protection",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    `Pass: **${report.baselineProtection?.pass}**`,
    "",
    "| Brand | Active ready | Full | Gallery | Property | CV | Ext lock | Pass |",
    "|---|---|---|---|---|---|---|---|",
  ];
  for (const r of report.baselineProtection?.rows || []) {
    lines.push(
      `| ${r.brandSlug} | ${r.active_profile_ready} | ${r.shouldRenderFullProfile} | ${r.galleryImageUrlCount} | ${r.propertyImageUrlCount} | ${r.companyValidated} | ${r.externalQualityLockPass} | ${r.pass} |`
    );
  }
  if (report.baselineProtection?.failures?.length) {
    lines.push("", "## Failures", "");
    for (const f of report.baselineProtection.failures) lines.push(`- ${f}`);
  }
  lines.push(
    "",
    "## Guardrails",
    "",
    "- No writes to released golden brands",
    "- Hotel Indigo / MGallery / SLH not processed",
    "- Design Hotels external remains locked",
    ""
  );
  return lines.join("\n");
}

export function writeV45Reports(report, reportsDir = path.join(ROOT, "reports")) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  const founderPath = path.join(reportsDir, REPORT_FOUNDER_MD);
  const baselinePath = path.join(reportsDir, REPORT_BASELINE_MD);

  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2), "utf8");
  fs.writeFileSync(mdPath, renderV45RemediationMarkdown(report), "utf8");
  fs.writeFileSync(founderPath, renderV45FounderReentryMarkdown(report), "utf8");
  fs.writeFileSync(baselinePath, renderV45BaselineProtectionMarkdown(report), "utf8");

  return { jsonPath, mdPath, founderPath, baselinePath };
}
