/**
 * v40C — Run economics chrome + residual owner-copy remediation (dry-run by default).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { getDiscoveryBrandConfig } from "./brand-explorer-brand-asset-image-governance.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { evaluateBrandExternalQualityLock } from "./brand-explorer-display-quality-lock.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { buildResidualOwnerCopyPatchPlan, PRESENTATION_TABLE } from "./brand-explorer-residual-owner-copy-remediation.js";
import {
  V40C_VERSION,
  V40C_DEFAULT_BRANDS,
  V40C_INCOMPLETE_CONTROL,
  V40C_APPLY_FLAGS,
  ECONOMICS_CHROME_INVENTORY,
  BRAND_MODEL_ECONOMICS_COPY,
  scanInternalPreviewOwnerCopy,
  buildV40CApplyDesign,
} from "./brand-explorer-economics-chrome-remediation.js";

export { V40C_VERSION, V40C_DEFAULT_BRANDS, V40C_INCOMPLETE_CONTROL };

export const REPORT_JSON = "brand-explorer-v40c-economics-chrome-remediation.json";
export const REPORT_MD = "brand-explorer-v40c-economics-chrome-remediation.md";

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

/**
 * Parse CLI flags for gated Presentation apply.
 */
export function parseV40CApplyFlags(argv = []) {
  const has = (flag) => argv.includes(flag);
  return {
    apply: has("--apply"),
    approve: has(V40C_APPLY_FLAGS.approve),
    noCompanyValidation: has(V40C_APPLY_FLAGS.noCompanyValidation),
    noActiveApproval: has(V40C_APPLY_FLAGS.noActiveApproval),
    noSourceLibrary: has(V40C_APPLY_FLAGS.noSourceLibrary),
    noRegistry: has(V40C_APPLY_FLAGS.noRegistry),
    noImageFields: has(V40C_APPLY_FLAGS.noImageFields),
    externalLocked: has(V40C_APPLY_FLAGS.externalLocked),
    internalClean: has(V40C_APPLY_FLAGS.internalClean),
    brandOnly: has(V40C_APPLY_FLAGS.brandOnly),
  };
}

export function validateV40CApplyGates({ flags, brands, brandResults } = {}) {
  const blockers = [];
  if (!flags?.apply) return { allowed: false, blockers: ["not_apply_mode"], missingFlags: [] };

  const required = [
    ["approve", V40C_APPLY_FLAGS.approve],
    ["noCompanyValidation", V40C_APPLY_FLAGS.noCompanyValidation],
    ["noActiveApproval", V40C_APPLY_FLAGS.noActiveApproval],
    ["noSourceLibrary", V40C_APPLY_FLAGS.noSourceLibrary],
    ["noRegistry", V40C_APPLY_FLAGS.noRegistry],
    ["noImageFields", V40C_APPLY_FLAGS.noImageFields],
    ["externalLocked", V40C_APPLY_FLAGS.externalLocked],
    ["internalClean", V40C_APPLY_FLAGS.internalClean],
    ["brandOnly", V40C_APPLY_FLAGS.brandOnly],
  ];
  const missingFlags = required.filter(([k]) => !flags[k]).map(([, flag]) => flag);
  if (missingFlags.length) blockers.push(...missingFlags.map((f) => `missing_flag:${f}`));

  if (flags.brandOnly) {
    const illegal = (brands || []).filter((s) => !V40C_DEFAULT_BRANDS.includes(s));
    if (illegal.length) blockers.push(`brand_only_violation:${illegal.join(",")}`);
  }

  for (const b of brandResults || []) {
    if ((b.projection?.internalPreviewForbiddenAfterCount ?? 1) !== 0) {
      blockers.push(`internal_preview_not_projected_clean:${b.brandSlug}`);
    }
    if (b.externalDomLock?.profileInPreparationRendered !== true && b.shouldRenderFullProfile === true) {
      blockers.push(`external_not_locked:${b.brandSlug}`);
    }
    const unsafe = (b.residualPresentation?.patches || []).filter((p) => !p.safeForGenericApply);
    if (unsafe.length) blockers.push(`unsafe_patches:${b.brandSlug}:${unsafe.length}`);
    for (const p of b.residualPresentation?.patches || []) {
      if (!ALLOWED_PRESENTATION_FIELDS.has(p.field)) {
        blockers.push(`forbidden_field:${b.brandSlug}:${p.field}`);
      }
      if (FORBIDDEN_WRITE_FIELDS.has(p.field)) {
        blockers.push(`blocked_field:${b.brandSlug}:${p.field}`);
      }
    }
  }

  return { allowed: blockers.length === 0, blockers, missingFlags };
}

function groupPatchesByRecord(patches = []) {
  const byId = new Map();
  for (const p of patches) {
    if (!p.recordId || !ALLOWED_PRESENTATION_FIELDS.has(p.field)) continue;
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

async function applyResidualPatchesForBrand(brandResult) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const grouped = groupPatchesByRecord(brandResult.residualPresentation?.patches || []);
  const patched = [];
  const errors = [];
  for (const g of grouped) {
    try {
      await airtablePatchPresentation(baseId, apiKey, g.recordId, g.fields);
      patched.push({
        recordId: g.recordId,
        slotKey: g.slotKey,
        fields: Object.keys(g.fields),
      });
    } catch (err) {
      errors.push({ recordId: g.recordId, slotKey: g.slotKey, message: err.message });
    }
  }
  return { patched, errors, recordsTouched: patched.length };
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

function applyPatchesToBlocks(blocks = [], patches = []) {
  const byRecord = new Map();
  for (const p of patches) {
    if (!p.recordId) continue;
    if (!byRecord.has(p.recordId)) byRecord.set(p.recordId, {});
    const apiKey = FIELD_TO_API[p.field];
    if (apiKey) byRecord.get(p.recordId)[apiKey] = p.after;
  }
  return (blocks || []).map((b) => {
    const overlay = byRecord.get(b.recordId);
    if (!overlay) return b;
    return { ...b, ...overlay };
  });
}

function countGallery(blocks = []) {
  return blocks.filter((b) => /^materials\.gallery\.\d+$/.test(nz(b.slotKey)) && nz(b.imageUrl)).length;
}

function countOpenings(blocks = []) {
  return blocks.filter((b) => nz(b.slotKey) === "footprint.openings" && nz(b.imageUrl)).length;
}

function projectFounderDecision({ internalForbiddenAfter, externalLocked, galleryCount, openingsCount }) {
  if (!externalLocked) {
    return {
      decision: "not_owner_ready",
      rationale: "External profile unexpectedly unlocked — abort founder path.",
    };
  }
  if ((internalForbiddenAfter || []).length > 0) {
    return {
      decision: "more_remediation_required",
      rationale: `Internal preview still has forbidden owner copy: ${internalForbiddenAfter
        .map((h) => h.label)
        .join(", ")}`,
    };
  }
  if (galleryCount < 6 || openingsCount < 3) {
    return {
      decision: "not_owner_ready",
      rationale: `Visuals short (gallery ${galleryCount}/6, openings ${openingsCount}/3).`,
    };
  }
  return {
    decision: "founder_visual_review_ready",
    rationale:
      "Internal preview owner-copy clean; external remains locked; founder visual review allowed. Active approval still blocked.",
  };
}

export async function auditBrandV40C(brandSlug) {
  const config = resolveConfig(brandSlug);
  const ctx = await loadBrandFactoryContext(brandSlug).catch(() => null);
  const brandApi = await fetchBrandApiShape(brandSlug);
  const presentationRows = ctx?.presentationRows || brandApi?.brandExplorer?.blocks || [];
  const blocks = brandApi?.brandExplorer?.blocks || presentationRows;

  const residualPlan = buildResidualOwnerCopyPatchPlan({ brandSlug, presentationRows });

  // Before: live internal preview (chrome patch already in code; residual Presentation not yet applied)
  const internalHtmlBefore = renderBrandExplorerHtmlForTest(brandApi, {
    allPanels: true,
    internalPreview: true,
  });
  const internalTextBefore = stripHtmlForCopyScan(internalHtmlBefore);
  const internalForbiddenBefore = scanInternalPreviewOwnerCopy(internalTextBefore);

  const externalHtml = renderBrandExplorerHtmlForTest(brandApi, {
    allPanels: true,
    internalPreview: false,
  });
  const externalQl = evaluateBrandExternalQualityLock(brandApi, externalHtml, {
    brandSlug,
    brandBasics: ctx?.brandBasics,
  });

  // After projection: apply residual patches to API blocks in memory, re-render
  const projectedBlocks = applyPatchesToBlocks(blocks, residualPlan.patches);
  const projectedBrand = {
    ...brandApi,
    brandExplorer: {
      ...(brandApi.brandExplorer || {}),
      blocks: projectedBlocks,
    },
  };
  const internalHtmlAfter = renderBrandExplorerHtmlForTest(projectedBrand, {
    allPanels: true,
    internalPreview: true,
  });
  const internalTextAfter = stripHtmlForCopyScan(internalHtmlAfter);
  const internalForbiddenAfter = scanInternalPreviewOwnerCopy(internalTextAfter);

  const galleryCount = countGallery(blocks);
  const openingsCount = countOpenings(blocks);
  const externalLocked =
    brandApi.shouldRenderFullProfile !== true &&
    (externalQl.profileInPreparationRendered === true ||
      brandApi.brandExplorerDisplayState !== "external_owner_ready");

  const founderDecision = projectFounderDecision({
    internalForbiddenAfter,
    externalLocked,
    galleryCount,
    openingsCount,
  });

  return {
    brandSlug,
    brandName: brandApi.name || brandSlug,
    recordId: brandApi.id || config?.recordId || null,
    displayState: brandApi.brandExplorerDisplayState,
    shouldRenderFullProfile: brandApi.shouldRenderFullProfile === true,
    brandModel: BRAND_MODEL_ECONOMICS_COPY[brandSlug] || null,
    economicsChrome: {
      inventoryCount: ECONOMICS_CHROME_INVENTORY.length,
      patchedInCode: ECONOMICS_CHROME_INVENTORY.filter((i) => i.status === "patched_in_v40c").length,
      items: ECONOMICS_CHROME_INVENTORY,
    },
    residualPresentation: residualPlan,
    projection: {
      forbiddenEconomicsChromeBefore: internalForbiddenBefore.filter((h) =>
        ["fdd", "loi", "item_7", "item_19", "franchise_disclosure", "disclosure_document", "fee_stack", "net_contribution"].includes(
          h.id
        )
      ).length,
      forbiddenEconomicsChromeAfter: internalForbiddenAfter.filter((h) =>
        ["fdd", "loi", "item_7", "item_19", "franchise_disclosure", "disclosure_document", "fee_stack", "net_contribution"].includes(
          h.id
        )
      ).length,
      residualPresentationBlockersBefore: residualPlan.summary.patchCount,
      residualPresentationBlockersAfterProjected: residualPlan.patches.filter((p) => !p.safeForGenericApply)
        .length,
      internalPreviewForbiddenBefore: internalForbiddenBefore,
      internalPreviewForbiddenAfter: internalForbiddenAfter,
      internalPreviewForbiddenBeforeCount: internalForbiddenBefore.length,
      internalPreviewForbiddenAfterCount: internalForbiddenAfter.length,
      externalLockStatus: externalLocked ? "locked_profile_in_preparation" : "unexpected_unlocked",
      founderVisualReviewReadiness: founderDecision.decision,
      activeApprovalStatus: "blocked_untouched",
    },
    visuals: { galleryCount, openingsCount },
    externalDomLock: {
      profileInPreparationRendered: externalQl.profileInPreparationRendered === true,
      forbiddenStringsFound: externalQl.forbiddenStringsFound,
      tabsRendered: (externalQl.tabsRenderedExternally || []).length,
      pass: externalQl.profileInPreparationRendered === true,
    },
    founderDecision,
    guardrails: {
      airtableWrites: false,
      activeProfileApproval: false,
      companyValidatedChanges: false,
      unlock: false,
    },
  };
}

async function runIncompleteControlCheck() {
  const results = [];
  for (const brandSlug of V40C_INCOMPLETE_CONTROL) {
    const brandApi = await fetchBrandApiShape(brandSlug).catch((err) => ({
      error: err.message,
      brandExplorerDisplayState: null,
      shouldRenderFullProfile: null,
    }));
    if (brandApi.error) {
      results.push({ brandSlug, pass: false, error: brandApi.error });
      continue;
    }
    const html = renderBrandExplorerHtmlForTest(brandApi, { allPanels: true, internalPreview: false });
    const ql = evaluateBrandExternalQualityLock(brandApi, html, { brandSlug });
    const pass =
      brandApi.shouldRenderFullProfile !== true &&
      ql.profileInPreparationRendered === true &&
      (ql.tabsRenderedExternally || []).length <= 1;
    results.push({
      brandSlug,
      displayState: brandApi.brandExplorerDisplayState,
      shouldRenderFullProfile: brandApi.shouldRenderFullProfile === true,
      profileInPreparationRendered: ql.profileInPreparationRendered === true,
      forbiddenStringsFound: ql.forbiddenStringsFound,
      tabsRendered: (ql.tabsRenderedExternally || []).length,
      pass,
    });
  }
  return {
    allControlPass: results.every((r) => r.pass),
    results,
  };
}

export async function runV40CEconomicsChromeRemediation({
  brands = V40C_DEFAULT_BRANDS,
  dryRun = true,
  apply = false,
  flags = null,
} = {}) {
  const resolvedFlags = flags || {
    apply: false,
    approve: false,
    noCompanyValidation: false,
    noActiveApproval: false,
    noSourceLibrary: false,
    noRegistry: false,
    noImageFields: false,
    externalLocked: false,
    internalClean: false,
    brandOnly: false,
  };

  const brandResults = [];
  for (const brandSlug of brands) {
    const result = await auditBrandV40C(brandSlug);
    result.applyResult = null;
    brandResults.push(result);
  }
  const incompleteControl = await runIncompleteControlCheck();

  const gateCheck = validateV40CApplyGates({
    flags: { ...resolvedFlags, apply },
    brands,
    brandResults,
  });

  let applyExecuted = false;
  let applyBlocked = false;
  if (apply) {
    if (!gateCheck.allowed) {
      applyBlocked = true;
    } else {
      for (const b of brandResults) {
        b.applyResult = await applyResidualPatchesForBrand(b);
      }
      applyExecuted = true;
    }
  }

  return {
    version: V40C_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !applyExecuted,
    applyRequested: Boolean(apply),
    applyExecuted,
    applyBlocked,
    applyGateCheck: gateCheck,
    brands,
    brandResults,
    economicsChromeInventory: ECONOMICS_CHROME_INVENTORY,
    incompleteControl,
    applyDesign: buildV40CApplyDesign(brands),
    summary: {
      brandsRemediated: brandResults.length,
      chromeItemsPatchedInCode: ECONOMICS_CHROME_INVENTORY.length,
      totalResidualPatches: brandResults.reduce(
        (n, b) => n + (b.residualPresentation.summary.patchCount || 0),
        0
      ),
      recordsPatched: brandResults.reduce((n, b) => n + (b.applyResult?.recordsTouched || 0), 0),
      applyErrors: brandResults.reduce((n, b) => n + (b.applyResult?.errors?.length || 0), 0),
      internalPreviewCleanProjected: brandResults.filter(
        (b) => b.projection.internalPreviewForbiddenAfterCount === 0
      ).length,
      founderVisualReviewReadyProjected: brandResults.filter(
        (b) => b.founderDecision.decision === "founder_visual_review_ready"
      ).length,
      incompleteControlPass: incompleteControl.allControlPass,
      anyUnlock: false,
      activeApprovalTouched: false,
      companyValidatedTouched: false,
    },
    guardrails: {
      airtableWrites: applyExecuted,
      presentationWrites: applyExecuted,
      registryWrites: false,
      sourceLibraryWrites: false,
      imageFieldWrites: false,
      companyValidatedChanges: false,
      activeProfileApproval: false,
      unlock: false,
    },
  };
}

function renderBrandMd(brand) {
  const lines = [
    `# v40C Remediation — ${brand.brandName}`,
    "",
    `Slug: \`${brand.brandSlug}\` · Record: \`${brand.recordId || "n/a"}\``,
    "",
    "## Founder decision (projected)",
    "",
    `**${brand.founderDecision.decision}**`,
    "",
    brand.founderDecision.rationale,
    "",
    "## Brand model",
    "",
    brand.brandModel
      ? [
          `- Model: \`${brand.brandModel.model}\``,
          `- Use: ${brand.brandModel.use.join("; ")}`,
          `- Avoid: ${brand.brandModel.avoid.join("; ")}`,
        ].join("\n")
      : "- (no model profile)",
    "",
    "## Projection",
    "",
    `- Economics chrome forbidden before → after: ${brand.projection.forbiddenEconomicsChromeBefore} → ${brand.projection.forbiddenEconomicsChromeAfter}`,
    `- Residual Presentation patches planned: ${brand.residualPresentation.summary.patchCount}`,
    `- Internal preview forbidden before → after: ${brand.projection.internalPreviewForbiddenBeforeCount} → ${brand.projection.internalPreviewForbiddenAfterCount}`,
    `- External lock: ${brand.projection.externalLockStatus}`,
    `- Active approval: ${brand.projection.activeApprovalStatus}`,
    `- Gallery / openings: ${brand.visuals.galleryCount} / ${brand.visuals.openingsCount}`,
    "",
    "## Internal preview forbidden (before)",
    "",
  ];
  if (!brand.projection.internalPreviewForbiddenBefore.length) {
    lines.push("- none");
  } else {
    for (const h of brand.projection.internalPreviewForbiddenBefore) {
      lines.push(`- ${h.label}`);
    }
  }
  lines.push("", "## Internal preview forbidden (after projected Presentation scrub)", "");
  if (!brand.projection.internalPreviewForbiddenAfter.length) {
    lines.push("- none");
  } else {
    for (const h of brand.projection.internalPreviewForbiddenAfter) {
      lines.push(`- ${h.label}: ${h.snippet}`);
    }
  }
  lines.push("", "## Sample residual patches", "");
  for (const p of brand.residualPresentation.patches.slice(0, 12)) {
    lines.push(`### ${p.slotKey} · ${p.field}`);
    lines.push(`- reason: ${p.reason}`);
    lines.push(`- before: ${JSON.stringify((p.before || "").slice(0, 160))}`);
    lines.push(`- after: ${JSON.stringify((p.after || "").slice(0, 160))}`);
    lines.push("");
  }
  lines.push("## Guardrails", "", "- No active approval · no unlock · no Company Validated", "");
  return lines.join("\n");
}

export function writeV40CReports(report, reportsDir = path.join(ROOT, "reports")) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2), "utf8");

  const md = [
    "# v40C Economics Chrome + Residual Owner Copy Remediation",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    report.applyExecuted
      ? "Apply executed: Presentation residual owner-copy patches only. No unlock / no active approval / no Company Validated."
      : report.applyBlocked
        ? `Apply blocked. Gate failures: ${(report.applyGateCheck?.blockers || []).join(", ")}`
        : "Dry-run only. Renderer chrome patched in code. Presentation residual patches planned — not applied.",
    "",
    "## Summary",
    "",
    `- Brands: ${report.summary.brandsRemediated}`,
    `- Chrome inventory items patched in code: ${report.summary.chromeItemsPatchedInCode}`,
    `- Residual Presentation patches planned: ${report.summary.totalResidualPatches}`,
    `- Records patched: ${report.summary.recordsPatched ?? 0}`,
    `- Apply errors: ${report.summary.applyErrors ?? 0}`,
    `- Internal preview clean (projected): ${report.summary.internalPreviewCleanProjected}/${report.summary.brandsRemediated}`,
    `- Founder visual review ready (projected): ${report.summary.founderVisualReviewReadyProjected}`,
    `- Incomplete control pass: **${report.summary.incompleteControlPass ? "yes" : "no"}**`,
    `- Unlock: **no** · Active approval: **no**`,
    "",
    "## Economics chrome inventory",
    "",
  ];
  for (const item of report.economicsChromeInventory) {
    md.push(`### ${item.id}`);
    md.push(`- file: \`${item.sourceFile}\` · fn: \`${item.rendererFunction}\``);
    md.push(`- tab/section: ${item.tab} / ${item.section}`);
    md.push(`- hardcoded=${item.hardcoded} BrandSetup=${item.fromBrandSetup} Presentation=${item.fromPresentation}`);
    md.push(`- internalPreview=${item.appearsInternalPreview} external=${item.appearsExternal}`);
    md.push(`- status: **${item.status}**`);
    md.push(`- replacement: ${item.proposedReplacement}`);
    md.push("");
  }

  for (const b of report.brandResults) {
    md.push(`## ${b.brandSlug}`);
    md.push(`- decision: **${b.founderDecision.decision}**`);
    md.push(
      `- internal forbidden ${b.projection.internalPreviewForbiddenBeforeCount} → ${b.projection.internalPreviewForbiddenAfterCount}`
    );
    md.push(`- residual patches: ${b.residualPresentation.summary.patchCount}`);
    if (b.applyResult) {
      md.push(
        `- apply: records=${b.applyResult.recordsTouched} errors=${b.applyResult.errors?.length || 0}`
      );
    }
    md.push("");
  }

  md.push(report.applyExecuted ? "## Apply command used" : "## Designed apply");
  md.push("```");
  md.push(report.applyDesign.command);
  md.push("```");
  md.push("");

  fs.writeFileSync(mdPath, md.join("\n"), "utf8");

  const brandPaths = {};
  const shortNames = {
    "everhome-suites": "everhome",
    kimpton: "kimpton",
    "radisson-individuals-by-choice": "radisson",
  };
  for (const b of report.brandResults) {
    const short = shortNames[b.brandSlug] || b.brandSlug;
    const fname = `brand-explorer-v40c-${short}-remediation.md`;
    const fpath = path.join(reportsDir, fname);
    fs.writeFileSync(fpath, renderBrandMd(b), "utf8");
    brandPaths[b.brandSlug] = fpath;
  }

  const controlPath = path.join(reportsDir, "brand-explorer-v40c-incomplete-brand-control-check.md");
  const controlLines = [
    "# v40C Incomplete Brand Control Check",
    "",
    `Pass: **${report.incompleteControl.allControlPass ? "yes" : "no"}**`,
    "",
  ];
  for (const r of report.incompleteControl.results) {
    controlLines.push(`## ${r.brandSlug}`);
    controlLines.push(`- pass: ${r.pass}`);
    controlLines.push(`- displayState: ${r.displayState}`);
    controlLines.push(`- shouldRenderFullProfile: ${r.shouldRenderFullProfile}`);
    controlLines.push(`- profileInPreparation: ${r.profileInPreparationRendered}`);
    controlLines.push(`- tabs: ${r.tabsRendered}`);
    controlLines.push("");
  }
  fs.writeFileSync(controlPath, controlLines.join("\n"), "utf8");

  return { jsonPath, mdPath, brandPaths, controlPath };
}
