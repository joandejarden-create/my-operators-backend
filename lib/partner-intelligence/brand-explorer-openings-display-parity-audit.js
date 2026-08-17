/**
 * Brand Explorer Openings Display Parity Audit v31K.
 *
 * Compares footprint.openings (Openings / Examples / Properties) images and
 * label text across two brands — storage, API exposure, and frontend rendering.
 * Read-only — no Airtable writes.
 *
 * @see docs/data-intelligence/brand-explorer-openings-display-parity-audit-v31K.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { listRegistryAssetsForBrand } from "./brand-asset-registry-workflow.js";
import {
  assessPresentationRowImageGovernance,
  findRegistryAssetForPresentationRow,
  getDiscoveryBrandConfig,
  isRegistryAssetApprovedForExplorer,
} from "./brand-explorer-brand-asset-image-governance.js";
import { classifyRegistryAsset } from "./brand-explorer-radisson-individuals-approved-asset-materialization-writer.js";
import {
  HIDDEN_EXTERNAL_DISPLAY_STATUSES,
  isPresentationRowVisibleInExplorer,
} from "./brand-explorer-radisson-individuals-openings-suppression-writer.js";
import {
  assessOpeningsRowQuarantine,
  collectRowCopySurfaces,
  findInternalLanguageInRow,
  parseFootprintOpeningLocation,
} from "./brand-explorer-openings-ui-quarantine-governance.js";
import {
  getBrandTargetResolverContext,
  resolveBrandTarget,
} from "./brand-explorer-brand-target-resolver.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";

export const AUDIT_VERSION = "31K";
export const OPENINGS_SLOT = "footprint.openings";
export const REPORT_JSON_NAME = "brand-explorer-openings-display-parity-audit.json";
export const REPORT_MD_NAME = "brand-explorer-openings-display-parity-audit.md";
export const DOC_MD_NAME = "brand-explorer-openings-display-parity-audit-v31K.md";

export const DEFAULT_LEFT = "radisson";
export const DEFAULT_RIGHT = "radisson-individuals-by-choice";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const PRIOR_REPORTS = [
  "reports/brand-explorer-scenario-display-parity-audit.json",
  "reports/brand-explorer-radisson-individuals-openings-suppression-writer.json",
  "reports/brand-explorer-radisson-individuals-image-asset-openings-root-cause-audit.json",
  "reports/brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.json",
];

const FILES_READ = [
  "AGENTS.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "lib/partner-intelligence/brand-explorer-brand-asset-image-governance.js",
  "lib/partner-intelligence/brand-explorer-openings-ui-quarantine-governance.js",
  ...PRIOR_REPORTS.map((p) => p.replace(".json", ".md")),
  ...PRIOR_REPORTS,
  "live Brand Explorer Presentation rows (left + right)",
  "live API responses (left + right)",
  "live Brand Asset Registry rows (left + right)",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-openings-display-parity-audit.js",
  "scripts/brand-explorer-openings-display-parity-audit.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

const FRONTEND_OPENINGS_RENDERING = {
  component: "brand-explorer-atelier-from-api.js",
  sectionTitle: "Openings / Examples / Properties",
  sectionHint: "Curated · Not a Full Directory",
  slotKey: OPENINGS_SLOT,
  cardComponent: "property-example-card",
  emptyShellComponent: "propertyShell() × 3",
  imageLogic:
    "propertyExampleCardFromBlock: block.imageUrl → <img> in property-example-card__top; no imageUrl → top area empty (badge + titles only)",
  labelLogic:
    "parseFootprintOpeningParas(body): chips (tags), loc (subtitle), asset (meta), scenario (accent), situation (teaser); fallbacks from caseSummaryTags / caseSummaryOverview",
  sameComponentForAllBrands: true,
  activeRegistryOnlyLogic: false,
  expansionBrandSpecificBranch: false,
  externalDisplayStatusFilter:
    "api/brand-library.js normalizeBrandExplorerPresentationRecords skips Do Not Display / Internal Only before blocks[]",
  noBlocksFallback:
    "When explorerBlocksForSlot(brand, footprint.openings).length === 0, renders three empty property-example-card shells with oe-dd--empty placeholders",
};

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}

function targetFields(target) {
  return {
    recordId: target?.recordId || target?.resolution?.resolvedRecordId || "",
    name: target?.name || target?.resolution?.resolvedBrandName || "",
    slug: target?.slug || target?.resolution?.resolvedSlug || "",
    resolutionSource: target?.resolution?.resolutionSource || null,
    error: target?.resolution?.error || null,
  };
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

function apiUrl(baseId, tableName) {
  return `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
}

async function listPresentationRowsRaw(baseId, apiKey, brandRecordId, brandName) {
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
  return records;
}

function firstAttachmentUrl(fields) {
  const att = fields?.Image;
  if (!Array.isArray(att) || !att[0]?.url) return null;
  return nz(att[0].url);
}

function isSafeHttpUrl(v) {
  const s = nz(v);
  return /^https?:\/\//i.test(s);
}

function chipListFromCsv(csv) {
  return String(csv || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

/** Mirrors public/js/brand-explorer-atelier-from-api.js parseFootprintOpeningParas */
function parseFootprintOpeningParas(bodyRaw) {
  let paras = String(bodyRaw || "")
    .split(/\n\n+/)
    .map((s) => s.trim())
    .filter(Boolean);
  let summaryHref = "";
  if (paras.length && isSafeHttpUrl(paras[paras.length - 1])) {
    summaryHref = paras[paras.length - 1];
    paras = paras.slice(0, -1);
  }
  let chips = "";
  let loc = "";
  let asset = "";
  let scenario = "";
  let situation = "";
  let why = "";
  let takeaway = "";
  if (paras.length >= 6) {
    [chips, loc, asset, situation, why, takeaway] = paras;
  } else if (paras.length === 5) {
    [chips, loc, asset, scenario, situation] = paras;
  } else if (paras.length === 4) {
    [chips, loc, asset, situation] = paras;
  } else {
    return {
      summaryHref,
      chips: paras[0] || "",
      loc: paras[1] || "",
      asset: paras[2] || "",
      scenario: "",
      situation: paras[3] || paras[2] || "",
      why: "",
      takeaway: "",
      parseMode: paras.length <= 3 ? "short_fallback" : "partial",
    };
  }
  return {
    summaryHref,
    chips,
    loc,
    asset,
    scenario,
    situation,
    why,
    takeaway,
    parseMode: paras.length >= 6 ? "full_6_block" : paras.length === 5 ? "voco_5_block" : "standard_4_block",
  };
}

function blockCaseSummaryField(block, key) {
  if (!block || block[key] == null || block[key] === "") return "";
  return nz(block[key]);
}

function normalizeOpeningsAirtableRow(rec) {
  const f = rec.fields || {};
  const sk = nz(f["Slot Key"]);
  if (sk !== OPENINGS_SLOT) return null;
  const externalDisplayStatus = nz(f["External Display Status"]);
  const activeRaw = f.Active;
  const inactive =
    activeRaw === false ||
    String(activeRaw).toLowerCase() === "no" ||
    String(activeRaw).toLowerCase() === "false" ||
    activeRaw === 0;
  const body = nz(f.Body);
  const parsed = parseFootprintOpeningParas(body);
  const chips = chipListFromCsv(parsed.chips);
  return {
    recordId: rec.id,
    slotKey: sk,
    title: nz(f.Title),
    body,
    subtitle: nz(f.Subtitle),
    hasImageAttachment: Array.isArray(f.Image) && f.Image.length > 0,
    imageUrl: firstAttachmentUrl(f),
    sourceUrl: nz(f["Source URL"]) || null,
    sourcePageUrl: nz(f["Source Page URL"]) || null,
    summaryUrl: nz(f["Summary URL"]) || null,
    externalDisplayStatus,
    active: !inactive,
    sortOrder: f["Sort Order"] ?? null,
    caseSummaryOverview: nz(f["Case Summary Overview"]),
    caseSummaryOwnerObjective: nz(f["Case Summary Owner Objective"]),
    caseSummaryBrandRelevance: nz(f["Case Summary Brand Relevance"]),
    caseSummaryInterpretation: nz(f["Case Summary Interpretation"]),
    caseSummaryTags: nz(f["Case Summary Tags"]),
    quarantined: HIDDEN_EXTERNAL_DISPLAY_STATUSES.includes(externalDisplayStatus),
    visibleInExplorer: isPresentationRowVisibleInExplorer(f),
    filteredFromApi:
      inactive || HIDDEN_EXTERNAL_DISPLAY_STATUSES.includes(externalDisplayStatus),
    locationFromTitle: parseFootprintOpeningLocation(nz(f.Title), body),
    parsedLabels: {
      chips: parsed.chips,
      chipList: chips,
      location: parsed.loc,
      meta: parsed.asset,
      scenario: parsed.scenario,
      teaser: parsed.situation,
      parseMode: parsed.parseMode,
    },
    labelCompleteness: {
      hasTitle: hasVal(f.Title),
      hasLocation: hasVal(parsed.loc) || hasVal(parseFootprintOpeningLocation(nz(f.Title), body)),
      hasMeta: hasVal(parsed.asset),
      hasScenario: hasVal(parsed.scenario),
      hasTeaser: hasVal(parsed.situation) || hasVal(f["Case Summary Overview"]),
      hasTags: chips.length > 0 || hasVal(f["Case Summary Tags"]),
      hasImage: Array.isArray(f.Image) && f.Image.length > 0,
    },
  };
}

function normalizeOpeningsApiBlock(block) {
  const body = nz(block?.body);
  const parsed = parseFootprintOpeningParas(body);
  const chips = chipListFromCsv(parsed.chips);
  if (!chips.length) chips.push(...chipListFromCsv(blockCaseSummaryField(block, "caseSummaryTags")));
  let scenario = nz(parsed.scenario);
  if (!scenario && chips.length > 1) scenario = chips.slice(1).join(" / ");
  let teaser = nz(parsed.situation);
  if (!teaser) teaser = blockCaseSummaryField(block, "caseSummaryOverview");
  return {
    recordId: block?.recordId || null,
    slotKey: nz(block?.slotKey),
    title: nz(block?.title),
    body,
    imageUrl: nz(block?.imageUrl) || null,
    summaryUrl: nz(block?.summaryUrl) || null,
    sort: block?.sort ?? null,
    parsedLabels: {
      chips: parsed.chips,
      chipList: chips,
      location: parsed.loc,
      meta: parsed.asset,
      scenario,
      teaser,
      parseMode: parsed.parseMode,
    },
    labelCompleteness: {
      hasTitle: hasVal(block?.title),
      hasLocation: hasVal(parsed.loc),
      hasMeta: hasVal(parsed.asset),
      hasScenario: hasVal(scenario),
      hasTeaser: hasVal(teaser),
      hasTags: chips.length > 0,
      hasImage: hasVal(block?.imageUrl),
    },
  };
}

async function fetchBrandApiShape(brandRecordId) {
  const req = { query: { brandId: brandRecordId, refresh: "1" }, headers: {} };
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
  if (res.statusCode !== 200 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function apiBlocksForOpenings(brand) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  return blocks
    .filter((b) => nz(b?.slotKey) === OPENINGS_SLOT)
    .sort((a, b) => (a.sort ?? 0) - (b.sort ?? 0));
}

function frontendCardProjection(block) {
  const norm = block ? normalizeOpeningsApiBlock(block) : null;
  const imgUrl = norm?.imageUrl || "";
  return {
    component: "property-example-card",
    rendersImage: hasVal(imgUrl),
    rendersTitle: norm?.labelCompleteness.hasTitle ?? false,
    rendersLocation: norm?.labelCompleteness.hasLocation ?? false,
    rendersMeta: norm?.labelCompleteness.hasMeta ?? false,
    rendersScenario: norm?.labelCompleteness.hasScenario ?? false,
    rendersTeaser: norm?.labelCompleteness.hasTeaser ?? false,
    rendersTags: norm?.labelCompleteness.hasTags ?? false,
    parsedLabels: norm?.parsedLabels || null,
    emptyPlaceholderFields: norm
      ? [
          !norm.labelCompleteness.hasMeta && "meta",
          !norm.labelCompleteness.hasScenario && "scenario",
          !norm.labelCompleteness.hasTeaser && "teaser",
          !norm.labelCompleteness.hasTags && "tags",
          !norm.labelCompleteness.hasImage && "image",
        ].filter(Boolean)
      : ["title", "location", "meta", "scenario", "teaser", "tags", "image"],
  };
}

function frontendSectionProjection(apiBlocks) {
  const hasBlocks = apiBlocks.length > 0;
  return {
    sectionTitle: "Openings / Examples / Properties",
    mode: hasBlocks ? "property-example-card grid" : "propertyShell() × 3 empty shells",
    cardCount: hasBlocks ? apiBlocks.length : 3,
    cards: hasBlocks
      ? apiBlocks.map((b) => frontendCardProjection(b))
      : [1, 2, 3].map(() => ({
          component: "property-example-card (empty shell)",
          rendersImage: false,
          rendersTitle: false,
          rendersLocation: false,
          rendersMeta: false,
          rendersScenario: false,
          rendersTeaser: false,
          rendersTags: false,
          parsedLabels: null,
          emptyPlaceholderFields: ["title", "location", "meta", "scenario", "teaser", "tags", "image"],
        })),
    allLabelsEmpty: !hasBlocks,
    anyImageRendered: apiBlocks.some((b) => hasVal(b?.imageUrl)),
  };
}

function classifyRowGovernance(row, apiBlock, registryAssets, brandConfig, brandTarget) {
  const merged = {
    recordId: row?.recordId || apiBlock?.recordId,
    slotKey: OPENINGS_SLOT,
    title: row?.title || apiBlock?.title,
    body: row?.body || apiBlock?.body,
    imageUrl: apiBlock?.imageUrl || row?.imageUrl || null,
    summaryUrl: apiBlock?.summaryUrl || row?.summaryUrl || null,
    caseSummaryOverview: row?.caseSummaryOverview,
    caseSummaryTags: row?.caseSummaryTags,
  };
  const brandCfg =
    brandConfig || { name: brandTarget?.name || "", allowedSiblingMentions: [] };
  const assessment = assessPresentationRowImageGovernance(merged, brandCfg, registryAssets);
  const registryMatch = findRegistryAssetForPresentationRow(registryAssets, merged);
  const quarantine = assessOpeningsRowQuarantine(merged, assessment, registryMatch);
  const registryClass = registryMatch
    ? classifyRegistryAsset({
        assetStatus: registryMatch.assetStatus,
        explorerUsePermission: registryMatch.explorerUsePermission,
        usageReviewStatus: registryMatch.usageReviewStatus,
        assetName: registryMatch.assetName,
        doNotUseReason: registryMatch.doNotUseReason,
      })
    : null;
  const hasImage = Boolean(nz(merged.imageUrl));
  let displayClassification = "text_only_eligible";
  if (!hasImage) displayClassification = "missing_image";
  else if (registryClass === "Do Not Use") displayClassification = "do_not_use";
  else if (registryClass === "Approved" || assessment?.registryApproved)
    displayClassification = "approved_image";
  else if (assessment?.pendingImageReview) displayClassification = "pending_image_review";
  else if (!registryMatch) displayClassification = "pending_image_review";
  else if (registryClass === "source_reference_only") displayClassification = "source_reference_only";

  return {
    assessment,
    registryMatch: registryMatch
      ? {
          id: registryMatch.id,
          assetName: registryMatch.assetName,
          assetStatus: registryMatch.assetStatus,
          explorerUsePermission: registryMatch.explorerUsePermission,
          usageReviewStatus: registryMatch.usageReviewStatus,
          approved: isRegistryAssetApprovedForExplorer(registryMatch),
          classification: registryClass,
        }
      : null,
    quarantineAssessment: quarantine,
    displayClassification,
    internalLanguage: findInternalLanguageInRow(merged),
    expansionBrandGovernance: nz(brandTarget?.resolutionSource) === "expansion_backlog",
  };
}

function summarizeBrandSide({
  brand,
  airtableRows,
  apiBlocks,
  registryAssets,
  brandConfig,
  brandTarget,
}) {
  const apiBlockIds = new Set(apiBlocks.map((b) => b.recordId));
  const rowAudits = airtableRows.map((row) => {
    const apiBlock = apiBlocks.find((b) => b.recordId === row.recordId) || null;
    const governance = classifyRowGovernance(
      row,
      apiBlock,
      registryAssets,
      brandConfig,
      brandTarget
    );
    return {
      recordId: row.recordId,
      title: row.title,
      location: row.locationFromTitle || row.parsedLabels.location,
      sortOrder: row.sortOrder,
      externalDisplayStatus: row.externalDisplayStatus || null,
      quarantined: row.quarantined,
      visibleInApi: apiBlockIds.has(row.recordId),
      hasImageAttachment: row.hasImageAttachment,
      imageInApi: Boolean(apiBlock?.imageUrl),
      parsedLabels: row.parsedLabels,
      labelCompleteness: row.labelCompleteness,
      internalLanguage: governance.internalLanguage,
      imageGovernance: governance.displayClassification,
      registryMatchId: governance.registryMatch?.id || null,
      quarantineRecommendation: governance.quarantineAssessment?.recommendation || null,
      frontendCard: frontendCardProjection(apiBlock),
    };
  });

  const frontend = frontendSectionProjection(apiBlocks);
  const internalLanguageCount = rowAudits.reduce((n, r) => n + r.internalLanguage.length, 0);

  return {
    brand,
    counts: {
      airtableRows: airtableRows.length,
      quarantinedRows: airtableRows.filter((r) => r.quarantined).length,
      visibleInApi: apiBlocks.length,
      withImageInAirtable: airtableRows.filter((r) => r.hasImageAttachment).length,
      withImageInApi: apiBlocks.filter((b) => hasVal(b.imageUrl)).length,
      withCompleteLabels: rowAudits.filter(
        (r) =>
          r.labelCompleteness.hasTitle &&
          r.labelCompleteness.hasLocation &&
          r.labelCompleteness.hasMeta &&
          r.labelCompleteness.hasTeaser &&
          r.labelCompleteness.hasTags
      ).length,
      internalLanguageHits: internalLanguageCount,
      registryLinked: rowAudits.filter((r) => r.registryMatchId).length,
    },
    frontend,
    rows: rowAudits,
  };
}

function inferRootCause(left, right) {
  const causes = [];
  if (left.counts.visibleInApi === 0 && right.counts.visibleInApi === 0) {
    causes.push({ layer: "data", reason: "Neither brand exposes footprint.openings in API blocks" });
  }
  if (left.counts.visibleInApi > 0 && right.counts.visibleInApi === 0) {
    causes.push({
      layer: "data",
      reason: `Radisson exposes ${left.counts.visibleInApi} openings in API; Radisson Individuals exposes 0 — likely quarantine (Do Not Display) on all Individuals rows`,
    });
  }
  if (right.counts.quarantinedRows > 0 && right.counts.visibleInApi === 0) {
    causes.push({
      layer: "data",
      reason: `${right.counts.quarantinedRows}/${right.counts.airtableRows} Individuals openings rows quarantined — API filter excludes them before blocks[]`,
    });
  }
  if (left.frontend.anyImageRendered && !right.frontend.anyImageRendered) {
    causes.push({
      layer: "data",
      reason:
        "Frontend uses identical property-example-card logic; Individuals shows empty shells because API blocks[] is empty — not a brand-specific render branch",
    });
  }
  if (right.counts.internalLanguageHits > 0) {
    causes.push({
      layer: "data",
      reason: `Individuals openings copy has ${right.counts.internalLanguageHits} internal-language marker hit(s) — v31C quarantine trigger`,
    });
  }
  if (left.counts.withImageInApi !== right.counts.withImageInApi) {
    causes.push({
      layer: "api",
      reason: `API image exposure differs: left ${left.counts.withImageInApi} vs right ${right.counts.withImageInApi}`,
    });
  }
  if (right.brand.resolutionSource === "expansion_backlog") {
    causes.push({
      layer: "image_governance",
      reason: "Radisson Individuals is expansion_backlog — stricter registry/image governance before active display",
    });
  }
  return causes;
}

function buildRecommendation(left, right) {
  const recs = [];
  if (right.counts.visibleInApi === 0 && right.counts.airtableRows > 0) {
    recs.push({
      priority: "P1",
      action: "openings_rebuild_or_reactivate",
      detail:
        "Radisson Individuals has footprint.openings rows in Airtable but none in API blocks — run v31L openings rebuild (owner-facing copy + approved images + clear Do Not Display) before expecting parity with Radisson.",
    });
  }
  if (right.counts.internalLanguageHits > 0) {
    recs.push({
      priority: "P1",
      action: "repair_internal_language",
      detail:
        "Replace internal/census/source-capture labels in openings body and case-summary fields with owner-facing copy per brand-explorer-openings-ui-quarantine-governance.js proposeOwnerFacingOpeningsCopy pattern.",
    });
  }
  if (right.counts.withImageInAirtable > 0 && right.counts.withImageInApi === 0) {
    recs.push({
      priority: "P2",
      action: "review_quarantine_status",
      detail:
        "Images exist on quarantined Airtable rows but are invisible in UI — clear External Display Status only after copy/image/registry review.",
    });
  }
  if (right.counts.registryLinked < right.counts.airtableRows) {
    recs.push({
      priority: "P2",
      action: "registry_then_approval",
      detail:
        "Link Brand Asset Registry assets to each footprint.openings row, founder-approve, then materialize durable images.",
    });
  }
  if (left.frontend.mode !== right.frontend.mode) {
    recs.push({
      priority: "P3",
      action: "no_frontend_patch_needed",
      detail:
        "Rendering difference is data-driven (empty shells vs real cards). Optional: hide openings section entirely when zero API blocks — uniform for all brands.",
      scope: "frontend",
    });
  }
  if (recs.length === 0) {
    recs.push({
      priority: "info",
      action: "no_action",
      detail: "Openings display parity acceptable for current data state.",
    });
  }
  return recs;
}

export function v31kAuditExists() {
  return fs.existsSync(
    path.join(ROOT, "lib/partner-intelligence/brand-explorer-openings-display-parity-audit.js")
  );
}

export async function buildBrandExplorerOpeningsDisplayParityAuditReport({
  leftArg = DEFAULT_LEFT,
  rightArg = DEFAULT_RIGHT,
} = {}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const ctx = await getBrandTargetResolverContext({ refresh: true });
  const leftTarget = await resolveBrandTarget(leftArg, ctx);
  const rightTarget = await resolveBrandTarget(rightArg, ctx);
  const left = targetFields(leftTarget);
  const right = targetFields(rightTarget);
  if (left.error) throw new Error(`Left brand resolve failed: ${left.error}`);
  if (right.error) throw new Error(`Right brand resolve failed: ${right.error}`);
  if (!left.recordId) throw new Error(`Left brand resolve failed: no recordId for ${leftArg}`);
  if (!right.recordId) throw new Error(`Right brand resolve failed: no recordId for ${rightArg}`);

  const [leftBasics, rightBasics] = await Promise.all([
    fetchBrandBasics(left.recordId),
    fetchBrandBasics(right.recordId),
  ]);

  const [leftPresRaw, rightPresRaw, leftApi, rightApi, leftRegistry, rightRegistry] =
    await Promise.all([
      listPresentationRowsRaw(baseId, apiKey, left.recordId, left.name),
      listPresentationRowsRaw(baseId, apiKey, right.recordId, right.name),
      fetchBrandApiShape(left.recordId),
      fetchBrandApiShape(right.recordId),
      listRegistryAssetsForBrand(left.recordId).catch(() => []),
      listRegistryAssetsForBrand(right.recordId).catch(() => []),
    ]);

  const leftAirtableRows = leftPresRaw
    .map(normalizeOpeningsAirtableRow)
    .filter(Boolean)
    .sort((a, b) => (a.sortOrder ?? 0) - (b.sortOrder ?? 0));
  const rightAirtableRows = rightPresRaw
    .map(normalizeOpeningsAirtableRow)
    .filter(Boolean)
    .sort((a, b) => (a.sortOrder ?? 0) - (b.sortOrder ?? 0));

  const leftApiBlocks = apiBlocksForOpenings(leftApi).map(normalizeOpeningsApiBlock);
  const rightApiBlocks = apiBlocksForOpenings(rightApi).map(normalizeOpeningsApiBlock);

  const leftBrandConfig =
    getDiscoveryBrandConfig(left.slug) || { name: left.name, allowedSiblingMentions: [] };
  const rightBrandConfig =
    getDiscoveryBrandConfig(right.slug) || { name: right.name, allowedSiblingMentions: [] };

  const leftSummary = summarizeBrandSide({
    brand: { ...left, input: leftArg },
    airtableRows: leftAirtableRows,
    apiBlocks: leftApiBlocks,
    registryAssets: leftRegistry,
    brandConfig: leftBrandConfig,
    brandTarget: leftTarget,
  });
  const rightSummary = summarizeBrandSide({
    brand: { ...right, input: rightArg },
    airtableRows: rightAirtableRows,
    apiBlocks: rightApiBlocks,
    registryAssets: rightRegistry,
    brandConfig: rightBrandConfig,
    brandTarget: rightTarget,
  });

  const rootCauseMap = inferRootCause(leftSummary, rightSummary);
  const recommendedFix = buildRecommendation(leftSummary, rightSummary);

  const finalQaRight = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: right.slug,
  }).catch((err) => ({ error: err.message }));
  const completeRight = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: right.slug,
    targetQuality: "active-profile",
    dryRun: true,
  }).catch((err) => ({ error: err.message }));

  const report = {
    auditVersion: AUDIT_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    v31kAuditExists: v31kAuditExists(),
    slotKey: OPENINGS_SLOT,
    sectionTitle: "Openings / Examples / Properties",
    leftBrand: {
      input: leftArg,
      slug: left.slug,
      recordId: left.recordId,
      name: left.name,
      resolutionSource: left.resolutionSource,
    },
    rightBrand: {
      input: rightArg,
      slug: right.slug,
      recordId: right.recordId,
      name: right.name,
      resolutionSource: right.resolutionSource,
    },
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    companyValidatedLeft: companyValidatedSnapshot(leftBasics),
    companyValidatedRight: companyValidatedSnapshot(rightBasics),
    companyValidatedUntouched: true,
    airtableModified: false,
    imagesApproved: false,
    airtableRowComparison: {
      left: leftSummary,
      right: rightSummary,
    },
    apiComparison: {
      leftInBlocks: leftApiBlocks.length,
      rightInBlocks: rightApiBlocks.length,
      leftBlocks: leftApiBlocks,
      rightBlocks: rightApiBlocks,
      section: FRONTEND_OPENINGS_RENDERING.sectionTitle,
      apiTransformNotes: [
        "brand-library.js maps Image attachment → block.imageUrl",
        "Do Not Display / Internal Only rows excluded from blocks[]",
        "Multiple rows share slotKey footprint.openings — all visible rows returned in blocks[]",
        "No brand-specific openings transform",
      ],
    },
    frontendRenderingComparison: {
      left: leftSummary.frontend,
      right: rightSummary.frontend,
      sameComponent: true,
      parityGap:
        leftSummary.frontend.mode !== rightSummary.frontend.mode ||
        leftSummary.frontend.anyImageRendered !== rightSummary.frontend.anyImageRendered,
      staticAnalysis: FRONTEND_OPENINGS_RENDERING,
    },
    imageGovernanceComparison: {
      left: leftSummary.rows.map((r) => ({
        recordId: r.recordId,
        title: r.title,
        imageGovernance: r.imageGovernance,
        registryMatchId: r.registryMatchId,
        quarantineRecommendation: r.quarantineRecommendation,
      })),
      right: rightSummary.rows.map((r) => ({
        recordId: r.recordId,
        title: r.title,
        imageGovernance: r.imageGovernance,
        registryMatchId: r.registryMatchId,
        quarantineRecommendation: r.quarantineRecommendation,
      })),
    },
    labelsTextComparison: {
      leftComplete: leftSummary.counts.withCompleteLabels,
      rightComplete: rightSummary.counts.withCompleteLabels,
      leftInternalLanguageHits: leftSummary.counts.internalLanguageHits,
      rightInternalLanguageHits: rightSummary.counts.internalLanguageHits,
      labelFields: ["title", "location", "meta (asset)", "scenario", "teaser (situation)", "tags (chips)"],
      parseSource: "Body double-newline paragraphs per parseFootprintOpeningParas; caseSummaryTags/Overview fallbacks",
    },
    rootCauseMap,
    recommendedFix,
    rightBrandReadiness: {
      finalQa: finalQaRight?.brandReports?.[0]?.scores || null,
      completeBuild:
        (completeRight?.brandReports || []).find((b) => b.slug === right.slug) || null,
    },
    displayParitySummary: {
      shouldDisplayIdentically: false,
      differenceIsIntentional: true,
      unexplainedDisplayGap: false,
      leftMode: leftSummary.frontend.mode,
      rightMode: rightSummary.frontend.mode,
    },
  };

  report.markdown = buildMarkdownReport(report);
  return report;
}

function buildMarkdownReport(report) {
  const lines = [
    `# Brand Explorer Openings Display Parity Audit v31K`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Section: **${report.sectionTitle}** (\`${report.slotKey}\`)`,
    `- Left: **${report.leftBrand.name}** (\`${report.leftBrand.slug}\`)`,
    `- Right: **${report.rightBrand.name}** (\`${report.rightBrand.slug}\`)`,
    `- v31K exists: **${report.v31kAuditExists ? "yes" : "no"}**`,
    `- Mode: **${report.mode}** (audit only)`,
    `- Company Validated untouched: **yes**`,
    `- Airtable modified: **no**`,
    "",
    "## 1. Summary comparison",
    "",
    "| Metric | Radisson by Choice | Radisson Individuals |",
    "|--------|-------------------|----------------------|",
    `| Airtable rows | ${report.airtableRowComparison.left.counts.airtableRows} | ${report.airtableRowComparison.right.counts.airtableRows} |`,
    `| Quarantined | ${report.airtableRowComparison.left.counts.quarantinedRows} | ${report.airtableRowComparison.right.counts.quarantinedRows} |`,
    `| Visible in API | ${report.airtableRowComparison.left.counts.visibleInApi} | ${report.airtableRowComparison.right.counts.visibleInApi} |`,
    `| With image (API) | ${report.airtableRowComparison.left.counts.withImageInApi} | ${report.airtableRowComparison.right.counts.withImageInApi} |`,
    `| Complete labels | ${report.airtableRowComparison.left.counts.withCompleteLabels} | ${report.airtableRowComparison.right.counts.withCompleteLabels} |`,
    `| Internal-language hits | ${report.airtableRowComparison.left.counts.internalLanguageHits} | ${report.airtableRowComparison.right.counts.internalLanguageHits} |`,
    `| Frontend mode | ${report.frontendRenderingComparison.left.mode} | ${report.frontendRenderingComparison.right.mode} |`,
    "",
    "## 2. API comparison",
    "",
    `- Left blocks: **${report.apiComparison.leftInBlocks}**`,
    `- Right blocks: **${report.apiComparison.rightInBlocks}**`,
    "",
    "## 3. Frontend rendering",
    "",
    `- Left renders images: **${report.frontendRenderingComparison.left.anyImageRendered}**`,
    `- Right renders images: **${report.frontendRenderingComparison.right.anyImageRendered}**`,
    `- Same component: **yes** (property-example-card)`,
    `- Parity gap: **${report.frontendRenderingComparison.parityGap ? "yes" : "no"}**`,
    "",
    "## 4. Row detail — Radisson Individuals",
    "",
  ];

  for (const row of report.airtableRowComparison.right.rows) {
    lines.push(`### ${row.title || row.recordId}`);
    lines.push(`- Record: \`${row.recordId}\``);
    lines.push(`- Quarantined: ${row.quarantined} · In API: ${row.visibleInApi}`);
    lines.push(`- Image: Airtable ${row.hasImageAttachment} · API ${row.imageInApi}`);
    lines.push(
      `- Labels: title=${row.labelCompleteness.hasTitle} loc=${row.labelCompleteness.hasLocation} meta=${row.labelCompleteness.hasMeta} scenario=${row.labelCompleteness.hasScenario} teaser=${row.labelCompleteness.hasTeaser} tags=${row.labelCompleteness.hasTags}`
    );
    if (row.internalLanguage.length) {
      lines.push(`- Internal language: ${row.internalLanguage.map((h) => h.markerId).join(", ")}`);
    }
    lines.push("");
  }

  lines.push("## 5. Root cause", "");
  for (const c of report.rootCauseMap) {
    lines.push(`- **${c.layer}**: ${c.reason}`);
  }

  lines.push("", "## 6. Recommended fix", "");
  for (const r of report.recommendedFix) {
    lines.push(`- [${r.priority}] ${r.action}: ${r.detail}`);
  }

  return lines.join("\n");
}
