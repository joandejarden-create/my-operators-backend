/**
 * Brand Explorer WoodSpring Founder Visual QA Correction v33G.
 *
 * Corrects founder-visible WoodSpring UI issues: scenario.3 fallback rendering,
 * risky bestAt copy, and property-specific registry traceability for openings/gallery.
 *
 * @see docs/data-intelligence/brand-explorer-woodspring-founder-visual-correction-writer-v33G.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  listRegistryAssetsForBrand,
} from "./brand-asset-registry-workflow.js";
import {
  getDiscoveryBrandConfig,
  isRegistryAssetApprovedForExplorer,
} from "./brand-explorer-brand-asset-image-governance.js";
import {
  WOODSPRING_PROPERTY_CATALOG,
  TARGET_BRAND,
  PROTECTED_BRAND_SLUGS,
} from "./brand-explorer-woodspring-real-property-examples-writer.js";
import {
  QUARANTINED_SCENARIO3_RECORD_ID,
} from "./brand-explorer-woodspring-visual-completion-writer.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";

export const WRITER_VERSION = "v33G";
export const STAGING_RUN_ID = "v33G-woodspring-founder-visual-correction";
export const REPORT_JSON_NAME = "brand-explorer-woodspring-founder-visual-correction-writer.json";
export const REPORT_MD_NAME = "brand-explorer-woodspring-founder-visual-correction-writer.md";
export const DOC_MD_NAME = "brand-explorer-woodspring-founder-visual-correction-writer-v33G.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v33G-woodspring-founder-visual-correction";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_SOURCE_LIBRARY = "--confirm-no-source-library-changes";
export const APPLY_FLAG_NO_SUMMARY_URL = "--confirm-no-summary-url-field";
export const APPLY_FLAG_NO_OTHER_SECTIONS = "--confirm-no-momentum-proof-standard-changes";
export const APPLY_FLAG_WOODSPRING_ONLY = "--confirm-woodspring-only";

export { TARGET_BRAND };

export const CLEAN_SCENARIO3_RECORD_ID = "recc5VUX0jh58II0E";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const HIDE_DISPLAY_STATUS = "Do Not Display";
const SCENARIO3_SLOT = "overview.scenario.3";

const ATELIER_SCENARIO_DEFAULT_TITLES = Object.freeze([
  "Urban Repositioning",
  "Leisure-Forward Conversions",
  "Boutique Resort Adjacency",
]);

const ATELIER_BESTAT_DEFAULT_TITLES = Object.freeze([
  "Conversion & Repositioning",
  "Blended-Demand Markets",
  "Owner Speed-to-Flag",
]);

export const SCENARIO3_TARGET = Object.freeze({
  recordId: CLEAN_SCENARIO3_RECORD_ID,
  title: "Extended-Stay Competitive Positioning",
  body:
    "Owners comparing extended-stay brands in a market should diligence weekly-stay demand, kitchen-equipped room expectations, operating simplicity, Choice platform fit, and local competitive supply.",
});

export const BEST_AT_TARGET = Object.freeze({
  "overview.bestAt.1": {
    title: "Weekly & Longer-Stay Demand",
    body:
      "Markets with recurring weekly and longer-stay demand from workforce, relocation, project-based, or other extended-stay use cases.",
  },
  "overview.bestAt.2": {
    title: "Extended-Stay Brand Comparison",
    body:
      "Owners comparing extended-stay brand fit, local supply, operating model simplicity, and Choice platform participation.",
  },
  "overview.bestAt.3": {
    title: "Practical Extended-Stay Operations",
    body:
      "Operators prepared for a practical extended-stay service model, kitchen-equipped room expectations, and disciplined property-level execution.",
  },
});

const REGISTRY_LINK_TARGETS = Object.freeze([
  { recordId: "recI3cbO8mOhEpo1W", slotKey: "footprint.openings", label: "Orlando opening", propertyKey: "flf21" },
  { recordId: "recpNB0KoPq6y3Mhs", slotKey: "footprint.openings", label: "Charlotte opening", propertyKey: "ncb10" },
  { recordId: "rec4Eqp9lwXSP7UQE", slotKey: "footprint.openings", label: "Raleigh opening", propertyKey: "nc936" },
  { recordId: "rechUn7nwlxjW1jyV", slotKey: "materials.gallery.1", label: "Gallery 1", propertyKey: "nc936" },
  { recordId: "recXfIGZUrwap6AIK", slotKey: "materials.gallery.2", label: "Gallery 2", propertyKey: "ncb10" },
  { recordId: "recJokIWQxU64gVsl", slotKey: "materials.gallery.3", label: "Gallery 3", propertyKey: "flf21" },
]);

const BLOCKED_PRESENTATION_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Summary URL",
  "View Summary URL",
  "Case summary URL",
  "Image",
  "Scenario Image",
  "Active",
  "Sort Order",
  "Slot Key",
  "Brand",
  "Brand Name",
]);

const RISKY_COPY_RE =
  /\badr\b|amenity[- ]stack|\bfees?\b|net contribution after fees|tier-appropriate qa|opening discipline|boutique resort|resort adjacency|\bfdd\b|item\s*19/i;

const BOUTIQUE_RESORT_RE = /boutique resort|resort adjacency/i;

const FILES_READ = [
  "AGENTS.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "live Airtable presentation rows (WoodSpring)",
  "live Brand Asset Registry (WoodSpring)",
  "live Brand Library API response",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-woodspring-founder-visual-correction-writer.js",
  "scripts/brand-explorer-woodspring-founder-visual-correction-writer.mjs",
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

function hasVal(v) {
  return nz(v).length > 0;
}

function normalizeUrlKey(url) {
  return nz(url).replace(/\?.*$/, "").toLowerCase();
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

export function v33gWriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-woodspring-founder-visual-correction-writer.js"
    )
  );
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || TARGET_BRAND.slug).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v33G`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v33G supports WoodSpring Suites only; got: ${brandArg}`);
  }
  return TARGET_BRAND;
}

async function airtableFetch(baseId, apiKey, tableName, init = {}, recordId = "") {
  const url = recordId
    ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}/${recordId}`
    : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
  const res = await fetch(url, {
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
  const rows = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `Presentation list failed: ${res.status}`);
    rows.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return rows.map((rec) => {
    const f = rec.fields || {};
    const imageAtt = f.Image?.[0] || f["Scenario Image"]?.[0];
    const activeRaw = f.Active;
    const inactive =
      activeRaw === false ||
      String(activeRaw).toLowerCase() === "no" ||
      String(activeRaw).toLowerCase() === "false" ||
      activeRaw === 0;
    return {
      recordId: rec.id,
      slotKey: nz(f["Slot Key"]),
      title: nz(f.Title),
      body: nz(f.Body),
      sortOrder: f["Sort Order"] ?? 0,
      active: !inactive,
      externalDisplayStatus: nz(f["External Display Status"]),
      visible:
        !inactive &&
        nz(f["External Display Status"]).toLowerCase() !== HIDE_DISPLAY_STATUS.toLowerCase() &&
        nz(f["External Display Status"]).toLowerCase() !== "internal only",
      hasImageAttachment: Boolean(f.Image?.length || f["Scenario Image"]?.length),
      imageUrl: imageAtt?.url ? nz(imageAtt.url) : "",
      registryLinkIds: Array.isArray(f["Brand Asset Registry"]) ? f["Brand Asset Registry"] : [],
      rawFields: f,
    };
  });
}

function explorerBlocksForSlot(brand, slotKey) {
  const be = brand?.brandExplorer;
  if (!be || !Array.isArray(be.blocks)) return [];
  function imgRank(b) {
    if (!b || !b.imageUrl) return 0;
    const u = String(b.imageUrl).trim();
    return u.indexOf("http") === 0 ? 1 : 0;
  }
  const rows = be.blocks.filter((b) => b && String(b.slotKey) === String(slotKey));
  rows.sort((a, b) => {
    const ir = imgRank(b) - imgRank(a);
    if (ir !== 0) return ir;
    const as = typeof a.sort === "number" && !Number.isNaN(a.sort) ? a.sort : 0;
    const bs = typeof b.sort === "number" && !Number.isNaN(b.sort) ? b.sort : 0;
    if (as !== bs) return as - bs;
    return String(a.recordId || "").localeCompare(String(b.recordId || ""));
  });
  return rows;
}

function explorerFirstBlock(brand, slotKey) {
  const rows = explorerBlocksForSlot(brand, slotKey);
  return rows.length ? rows[0] : null;
}

function splitBullets(text) {
  const t = nz(text);
  if (!t) return [];
  return t
    .split(/\n+|(?:\s*[-•*]\s+)/)
    .map((s) => s.trim())
    .filter(Boolean);
}

function explorerParagraphs(brand, slotKey, max) {
  const rows = explorerBlocksForSlot(brand, slotKey);
  const out = [];
  for (const row of rows) {
    if (hasVal(row.body)) {
      out.push(...String(row.body).split(/\n+/).map((s) => s.trim()).filter(Boolean));
    }
    if (out.length >= max) break;
  }
  return out.slice(0, max);
}

/** Mirrors brand-explorer-atelier-from-api.js scenario card projection. */
export function simulateAtelierScenarioCards(brand) {
  const scenarioBodies = splitBullets(brand?.keyBrandDifferentiators).slice(0, 3);
  while (scenarioBodies.length < 3) scenarioBodies.push("");
  const scenarioTitles = [...ATELIER_SCENARIO_DEFAULT_TITLES];
  const scen3Para = explorerParagraphs(brand, "overview.scenarios", 3);
  for (let sj = 0; sj < scen3Para.length; sj++) {
    if (hasVal(scen3Para[sj])) scenarioBodies[sj] = scen3Para[sj];
  }
  for (let sj = 0; sj < 3; sj++) {
    const srowOv = explorerFirstBlock(brand, `overview.scenario.${sj + 1}`);
    if (srowOv && hasVal(srowOv.body)) scenarioBodies[sj] = String(srowOv.body).trim();
    if (srowOv && hasVal(srowOv.title)) scenarioTitles[sj] = String(srowOv.title).trim();
  }
  return scenarioTitles.map((title, i) => {
    const srowImg = explorerFirstBlock(brand, `overview.scenario.${i + 1}`);
    const imageUrl =
      srowImg && hasVal(srowImg.imageUrl) ? String(srowImg.imageUrl).trim() : "";
    return {
      slotKey: `overview.scenario.${i + 1}`,
      title,
      body: scenarioBodies[i],
      imageUrl,
      hasImage: hasVal(imageUrl),
      showsImagePlaceholder: !hasVal(imageUrl),
      winningRecordId: srowImg?.recordId || null,
      usesHardcodedTitleFallback: title === ATELIER_SCENARIO_DEFAULT_TITLES[i],
      usesHardcodedBodyFallback: !hasVal(scenarioBodies[i]),
    };
  });
}

/** Mirrors brand-explorer-atelier-from-api.js bestAt card projection. */
export function simulateAtelierBestAtCards(brand) {
  const pillarParts = splitBullets(brand?.brandPillars);
  return ATELIER_BESTAT_DEFAULT_TITLES.map((defaultTitle, i) => {
    const sk = `overview.bestAt.${i + 1}`;
    const row = explorerFirstBlock(brand, sk);
    let title = defaultTitle;
    if (row && hasVal(row.title)) title = String(row.title).trim();
    let body = pillarParts[i] || "";
    const slotRows = explorerBlocksForSlot(brand, sk);
    const slotBody = slotRows
      .map((r) => {
        const t = hasVal(r.title) ? String(r.title).trim() : "";
        const bd = hasVal(r.body) ? String(r.body).trim() : "";
        if (t && bd) return `${t}: ${bd}`;
        return bd || t;
      })
      .filter(hasVal)
      .join("\n\n");
    if (hasVal(slotBody)) {
      body = String(slotBody)
        .trim()
        .split(/\n+/)
        .map((s) => s.trim())
        .filter(Boolean)
        .join(" ");
    }
    return {
      slotKey: sk,
      title,
      body,
      winningRecordId: row?.recordId || null,
      usesHardcodedTitleFallback: title === defaultTitle && !hasVal(row?.title),
      usesHardcodedBodyFallback: !hasVal(slotBody) && !hasVal(pillarParts[i]),
      hasRiskyLanguage: RISKY_COPY_RE.test(`${title}\n${body}`),
    };
  });
}

function rowInApiBlocks(brand, recordId) {
  const blocks = brand?.brandExplorer?.blocks || [];
  return blocks.some((b) => b?.recordId === recordId);
}

function classifyScenario3RootCause({ scenario3Rows, apiBrand, uiScenario3 }) {
  const causes = [];
  const visible = scenario3Rows.filter((r) => r.visible);
  const apiBlocks = explorerBlocksForSlot(apiBrand, SCENARIO3_SLOT);
  const cleanRow = scenario3Rows.find((r) => r.recordId === CLEAN_SCENARIO3_RECORD_ID);
  const quarantined = scenario3Rows.find((r) => r.recordId === QUARANTINED_SCENARIO3_RECORD_ID);

  if (uiScenario3?.title === "Boutique Resort Adjacency") {
    causes.push("atelier_hardcoded_title_fallback — no winning API block supplies title for overview.scenario.3");
  }
  if (uiScenario3?.showsImagePlaceholder) {
    causes.push("atelier_image_placeholder — explorerFirstBlock(overview.scenario.3).imageUrl is empty");
  }
  if (!apiBlocks.length) {
    causes.push("api_slot_absent — no overview.scenario.3 rows pass Brand Library API filters (Active + External Display Status)");
  }
  if (apiBlocks.length > 1) {
    causes.push(`api_duplicate_blocks — ${apiBlocks.length} overview.scenario.3 rows visible in API; explorerFirstBlock sort may pick wrong row`);
  }
  if (cleanRow && !cleanRow.visible) {
    causes.push("clean_row_hidden — canonical replacement row is not externally displayable");
  }
  if (cleanRow && !rowInApiBlocks(apiBrand, cleanRow.recordId)) {
    causes.push("clean_row_missing_from_api — Airtable row exists but Brand Library API excludes it");
  }
  if (!scenario3Rows.some((r) => isNonQuarantinedScenario3Row(r) && r.visible)) {
    causes.push("clean_scenario3_row_missing — only quarantined or hidden overview.scenario.3 rows exist in Airtable");
  }
  if (quarantined?.visible) {
    causes.push("quarantined_row_still_visible — Everhome/wrong-brand scenario.3 row should be Do Not Display");
  }
  if (cleanRow && visible.length > 1) {
    causes.push("multiple_visible_scenario3_rows — more than one displayable overview.scenario.3 row");
  }
  if (apiBlocks.length === 1 && hasVal(apiBlocks[0].title) && uiScenario3?.usesHardcodedTitleFallback) {
    causes.push("api_ui_mapping_mismatch — API has title but atelier simulation still uses fallback (investigate cache or alternate renderer)");
  }
  return causes.length ? causes : ["no_root_cause_detected_in_dry_run"];
}

export function pickPropertySpecificRegistryAsset(registryAssets, target, presentationRow) {
  const slot = nz(target?.slotKey);
  const propertyKey = nz(target?.propertyKey).toLowerCase();
  const bodyUrl = nz(presentationRow?.body).match(/https?:\/\/[^\s<>"')]+/i)?.[0] || "";
  const catalogEntry = WOODSPRING_PROPERTY_CATALOG.find(
    (c) => c.presentationRecordId === target.recordId
  );

  const candidates = (registryAssets || []).filter(
    (a) =>
      nz(a.recommendedExplorerSlot) === slot &&
      !/do not use/i.test(nz(a.assetStatus)) &&
      isRegistryAssetApprovedForExplorer(a)
  );

  if (slot.startsWith("materials.gallery")) {
    const byExactSlot = candidates.filter((a) => nz(a.stagingRunId).startsWith("v33C-R2"));
    return byExactSlot[0] || candidates[0] || null;
  }

  const byPropertyKey = candidates.filter((a) => {
    const src = normalizeUrlKey(`${a.sourcePageUrl} ${a.sourceUrl} ${a.relatedPropertyName || ""}`);
    return propertyKey && src.includes(propertyKey);
  });

  const byCatalogUrl = catalogEntry
    ? candidates.filter(
        (a) => normalizeUrlKey(a.sourcePageUrl) === normalizeUrlKey(catalogEntry.sourcePageUrl)
      )
    : [];

  const byBody = bodyUrl
    ? candidates.filter((a) => normalizeUrlKey(a.sourcePageUrl) === normalizeUrlKey(bodyUrl))
    : [];

  const byStaging = candidates.filter((a) =>
    nz(a.stagingRunId).startsWith("v33C-R2")
  );

  return byCatalogUrl[0] || byPropertyKey[0] || byBody[0] || byStaging[0] || null;
}

function validateCopyPatch(fields, { slotKey = "", allowDisplayStatus = false } = {}) {
  const errors = [];
  for (const key of Object.keys(fields || {})) {
    if (BLOCKED_PRESENTATION_FIELDS.has(key)) errors.push(`blocked_field:${key}`);
    if (key === "External Display Status" && !allowDisplayStatus) {
      errors.push(`blocked_field:${key}`);
    }
    if (!["Title", "Body", "External Display Status", "Brand Asset Registry"].includes(key)) {
      errors.push(`unexpected_field:${key}`);
    }
  }
  const combined = `${fields.Title || ""}\n${fields.Body || ""}`;
  if (BOUTIQUE_RESORT_RE.test(combined)) errors.push("boutique_resort_language");
  if (/\beverhome\b|\bsuburban studios\b/i.test(combined)) errors.push("wrong_brand_reference");
  if (RISKY_COPY_RE.test(combined) && !slotKey.startsWith("footprint.")) {
    errors.push("risky_performance_language");
  }
  return errors;
}

function validateRegistryLinkPatch(fields) {
  const errors = [];
  for (const key of Object.keys(fields || {})) {
    if (BLOCKED_PRESENTATION_FIELDS.has(key)) errors.push(`blocked_field:${key}`);
    if (key !== "Brand Asset Registry") errors.push(`unexpected_field:${key}`);
  }
  if (!Array.isArray(fields?.["Brand Asset Registry"])) errors.push("registry_link_must_be_array");
  return errors;
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-woodspring-founder-visual-correction-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_SOURCE_LIBRARY,
    APPLY_FLAG_NO_SUMMARY_URL,
    APPLY_FLAG_NO_OTHER_SECTIONS,
    APPLY_FLAG_WOODSPRING_ONLY,
  ].join(" ");
}

function presentationFields({
  slotKey,
  title,
  body,
  sort,
  brandRecordId,
  brandName,
  externalDisplayStatus,
  registryLinkIds,
}) {
  const fields = {
    "Slot Key": slotKey,
    Title: title || "",
    Body: body,
    "Brand Name": brandName,
    Brand: [brandRecordId],
    Active: true,
    "Sort Order": sort ?? 0,
  };
  if (externalDisplayStatus) fields["External Display Status"] = externalDisplayStatus;
  if (registryLinkIds?.length) fields["Brand Asset Registry"] = registryLinkIds;
  return fields;
}

export function findScenario3RegistryAsset(registryAssets) {
  const candidates = (registryAssets || []).filter(
    (a) =>
      nz(a.recommendedExplorerSlot) === SCENARIO3_SLOT &&
      !/do not use/i.test(nz(a.assetStatus)) &&
      isRegistryAssetApprovedForExplorer(a) &&
      hasVal(a.sourceUrl)
  );
  const official = candidates.filter((a) =>
    /^https:\/\/(www\.)?(choicehotels\.com|woodspring)/i.test(nz(a.sourceUrl))
  );
  const staged = official.filter((a) => /v33D|v33C-R2|v33A/i.test(nz(a.stagingRunId)));
  if (staged[0] || official[0] || candidates[0]) {
    return staged[0] || official[0] || candidates[0];
  }

  // Fallback: reuse founder-approved v33C-R2 hoteldam property photography when no
  // dedicated overview.scenario.3 registry row exists (row was deleted from Airtable).
  const propertyFallback = (registryAssets || []).find(
    (a) =>
      isRegistryAssetApprovedForExplorer(a) &&
      nz(a.stagingRunId).startsWith("v33C-R2") &&
      /hoteldam/i.test(nz(a.sourceUrl)) &&
      /exterior|suite|room|kitchen/i.test(nz(a.sourceUrl))
  );
  return propertyFallback || null;
}

function isNonQuarantinedScenario3Row(row) {
  return row?.slotKey === SCENARIO3_SLOT && row.recordId !== QUARANTINED_SCENARIO3_RECORD_ID;
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer WoodSpring Founder Visual QA Correction v33G");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Dry-run clean: **${report.dryRunClean ? "yes" : "no"}**`);
  lines.push(`- Image fields untouched: **${report.imageFieldsUntouched ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Scenario rendering audit");
  for (const row of report.scenarioRenderingAudit) {
    lines.push(
      "- `" +
        row.recordId +
        "` **" +
        row.slotKey +
        "** — title: " +
        (row.title || "(empty)") +
        "; display: " +
        (row.externalDisplayStatus || "(default)") +
        "; API: " +
        (row.inApi ? "yes" : "no") +
        "; UI winner: " +
        (row.isUiWinner ? "yes" : "no") +
        "; image: " +
        row.imageStatus
    );
  }
  lines.push("");
  lines.push("## Root cause — Boutique Resort Adjacency / IMAGE placeholder");
  for (const c of report.scenario3RootCause) lines.push(`- ${c}`);
  lines.push("");
  lines.push("## Scenario 3 decision");
  lines.push(`- Action: **${report.scenario3Decision.action}**`);
  lines.push(`- Image decision: **${report.scenario3Decision.imageDecision}**`);
  lines.push(`- Rationale: ${report.scenario3Decision.rationale}`);
  lines.push("");
  lines.push("## overview.bestAt before/after");
  for (const row of report.bestAtBeforeAfter) {
    lines.push(`### ${row.slotKey}`);
    lines.push(`- Before title: ${row.beforeTitle}`);
    lines.push(`- After title: ${row.afterTitle}`);
    lines.push(`- Before body: ${row.beforeBody}`);
    lines.push(`- After body: ${row.afterBody}`);
  }
  lines.push("");
  lines.push("## Registry traceability before/after");
  for (const row of report.registryTraceability) {
    lines.push(
      "- `" +
        row.presentationRecordId +
        "` " +
        row.label +
        " — before: " +
        (row.beforeRegistryId || "(none)") +
        "; after: " +
        (row.afterRegistryId || "(none)") +
        "; property-specific: " +
        (row.propertySpecific ? "yes" : "no")
    );
  }
  lines.push("");
  lines.push(`## Patches — copy: ${report.presentationCopyPatches.length}, registry: ${report.presentationRegistryLinkPatches.length}, hide: ${report.presentationHidePatches.length}`);
  if (report.projectedUiConfirmation) {
    lines.push("");
    lines.push("## Projected UI confirmation");
    lines.push(`- Scenario 3 title: ${report.projectedUiConfirmation.scenario3Title}`);
    lines.push(`- Scenario 3 IMAGE placeholder: ${report.projectedUiConfirmation.scenario3ImagePlaceholder ? "yes" : "no"}`);
    lines.push(`- Risky bestAt language remains: ${report.projectedUiConfirmation.riskyBestAtLanguage ? "yes" : "no"}`);
    lines.push(`- Property-specific opening registry links: ${report.projectedUiConfirmation.propertySpecificOpeningLinks ? "yes" : "no"}`);
  }
  if (report.exactApplyCommand) {
    lines.push("");
    lines.push("```bash");
    lines.push(report.exactApplyCommand);
    lines.push("```");
  }
  if (report.applyBlockers?.length) {
    lines.push("");
    lines.push("## Apply blockers");
    for (const b of report.applyBlockers) lines.push(`- ${b}`);
  }
  return lines.join("\n");
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

function projectApiBrandAfterPatches(apiBrand, patches, { creates = [], imagePatches = [] } = {}) {
  if (!apiBrand?.brandExplorer) return apiBrand;
  const patchById = new Map(patches.map((p) => [p.recordId, p.fields || {}]));
  const imageById = new Map(imagePatches.map((p) => [p.recordId, p.fields?.Image?.[0]?.url || ""]));
  const hiddenIds = new Set(
    patches
      .filter((p) => nz(p.fields?.["External Display Status"]).toLowerCase() === HIDE_DISPLAY_STATUS.toLowerCase())
      .map((p) => p.recordId)
  );
  const blocks = (apiBrand.brandExplorer.blocks || [])
    .filter((b) => b && !hiddenIds.has(b.recordId))
    .map((b) => {
      const patch = patchById.get(b.recordId);
      const imageOverride = imageById.get(b.recordId);
      if (!patch && !imageOverride) return b;
      return {
        ...b,
        title: patch?.Title != null ? nz(patch.Title) : b.title,
        body: patch?.Body != null ? nz(patch.Body) : b.body,
        imageUrl: imageOverride || b.imageUrl,
      };
    });
  for (const create of creates) {
    const recordId = create.projectedRecordId || "projected_scenario3_create";
    if (hiddenIds.has(recordId)) continue;
    const imageOverride = imageById.get(recordId) || create.projectedImageUrl || "";
    blocks.push({
      recordId,
      slotKey: create.fields["Slot Key"],
      title: create.fields.Title,
      body: create.fields.Body,
      sort: create.fields["Sort Order"] ?? 3,
      imageUrl: imageOverride,
    });
  }
  blocks.sort((a, b) => {
    if (a.sort !== b.sort) return a.sort - b.sort;
    return String(a.recordId).localeCompare(String(b.recordId));
  });
  return {
    ...apiBrand,
    brandExplorer: { ...apiBrand.brandExplorer, blocks },
  };
}

export async function buildBrandExplorerWoodspringFounderVisualCorrectionWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  noValidationClaim = false,
  noSourceLibraryChanges = false,
  noSummaryUrl = false,
  noOtherSectionChanges = false,
  woodspringOnly = false,
} = {}) {
  const target = resolveTargetBrand(brandArg);
  getDiscoveryBrandConfig(target.slug);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const [presentationRows, registryAssetsRaw, apiBrandBefore] = await Promise.all([
    listPresentationRows(baseId, apiKey, target.recordId, target.name),
    listRegistryAssetsForBrand(target.recordId).catch(() => []),
    fetchBrandApiShape(target.recordId),
  ]);
  const registryAssets = Array.isArray(registryAssetsRaw) ? registryAssetsRaw : [];

  const uiScenariosBefore = simulateAtelierScenarioCards(apiBrandBefore || {});
  const uiBestAtBefore = simulateAtelierBestAtCards(apiBrandBefore || {});
  const uiScenario3Before = uiScenariosBefore[2];

  const scenario3Rows = presentationRows.filter((r) => r.slotKey === SCENARIO3_SLOT);
  const apiScenario3Winner = explorerFirstBlock(apiBrandBefore || {}, SCENARIO3_SLOT);

  const scenarioRenderingAudit = scenario3Rows.map((row) => ({
    recordId: row.recordId,
    slotKey: row.slotKey,
    title: row.title,
    body: row.body,
    externalDisplayStatus: row.externalDisplayStatus || "(default visible)",
    active: row.active,
    visible: row.visible,
    imageStatus: row.hasImageAttachment
      ? row.imageUrl
        ? "attachment_present"
        : "attachment_no_url"
      : "missing",
    registryLinkIds: row.registryLinkIds,
    inApi: rowInApiBlocks(apiBrandBefore, row.recordId),
    inApiBlocksForSlot: explorerBlocksForSlot(apiBrandBefore, SCENARIO3_SLOT).map((b) => b.recordId),
    isUiWinner: apiScenario3Winner?.recordId === row.recordId,
    isQuarantined: row.recordId === QUARANTINED_SCENARIO3_RECORD_ID,
    isCleanReplacement: row.recordId === CLEAN_SCENARIO3_RECORD_ID,
    staleFallbackRisk:
      row.recordId !== CLEAN_SCENARIO3_RECORD_ID &&
      row.visible &&
      BOUTIQUE_RESORT_RE.test(`${row.title} ${row.body}`),
  }));

  const scenario3RootCause = classifyScenario3RootCause({
    scenario3Rows,
    apiBrand: apiBrandBefore,
    uiScenario3: uiScenario3Before,
  });

  const presentationCopyPatches = [];
  const presentationHidePatches = [];
  const presentationRegistryLinkPatches = [];
  const presentationCreates = [];
  const presentationImagePatches = [];
  const safetyBlockers = [];

  const scenario3RegistryAsset = findScenario3RegistryAsset(registryAssets);
  const cleanScenario3 =
    scenario3Rows.find((r) => r.recordId === CLEAN_SCENARIO3_RECORD_ID) ||
    scenario3Rows.find((r) => isNonQuarantinedScenario3Row(r) && r.visible);
  let scenario3TargetRecordId = cleanScenario3?.recordId || CLEAN_SCENARIO3_RECORD_ID;

  const cleanBlock = explorerBlocksForSlot(apiBrandBefore, SCENARIO3_SLOT).find(
    (b) => b.recordId === scenario3TargetRecordId
  );
  const cleanHasApiImage = hasVal(
    (apiScenario3Winner?.recordId === scenario3TargetRecordId
      ? apiScenario3Winner?.imageUrl
      : cleanBlock?.imageUrl) || ""
  );

  let scenario3ImageDecision = "preserve_existing_image";
  let scenario3Action = "patch_copy";

  if (!cleanScenario3) {
    scenario3Action = "create_clean_scenario3_row";
    const createFields = presentationFields({
      slotKey: SCENARIO3_SLOT,
      title: SCENARIO3_TARGET.title,
      body: SCENARIO3_TARGET.body,
      sort: 3,
      brandRecordId: target.recordId,
      brandName: target.name,
      registryLinkIds: scenario3RegistryAsset?.id ? [scenario3RegistryAsset.id] : undefined,
    });
    presentationCreates.push({
      fields: createFields,
      slotKey: SCENARIO3_SLOT,
      reason: "create_missing_clean_scenario3_row",
      projectedRecordId: CLEAN_SCENARIO3_RECORD_ID,
      projectedImageUrl: scenario3RegistryAsset?.sourceUrl || "",
    });
    scenario3TargetRecordId = CLEAN_SCENARIO3_RECORD_ID;
  } else {
    const needsCopy =
      cleanScenario3.title !== SCENARIO3_TARGET.title ||
      cleanScenario3.body !== SCENARIO3_TARGET.body ||
      BOUTIQUE_RESORT_RE.test(`${cleanScenario3.title} ${cleanScenario3.body}`);

    if (needsCopy) {
      const fields = { Title: SCENARIO3_TARGET.title, Body: SCENARIO3_TARGET.body };
      const errors = validateCopyPatch(fields, { slotKey: SCENARIO3_SLOT });
      if (errors.length) safetyBlockers.push(`scenario3_copy:${errors.join(",")}`);
      else {
        presentationCopyPatches.push({
          recordId: cleanScenario3.recordId,
          slotKey: SCENARIO3_SLOT,
          fields,
          reason: "woodspring_scenario3_founder_copy",
        });
      }
    }

    if (
      scenario3RegistryAsset?.id &&
      !(cleanScenario3.registryLinkIds || []).includes(scenario3RegistryAsset.id)
    ) {
      const fields = { "Brand Asset Registry": [scenario3RegistryAsset.id] };
      const errors = validateRegistryLinkPatch(fields);
      if (errors.length) safetyBlockers.push(`scenario3_registry_link:${errors.join(",")}`);
      else {
        presentationRegistryLinkPatches.push({
          recordId: cleanScenario3.recordId,
          slotKey: SCENARIO3_SLOT,
          fields,
          reason: "link_scenario3_to_approved_registry_asset",
          canonicalRegistryId: scenario3RegistryAsset.id,
        });
      }
    }
  }

  const hasSafeImage =
    cleanScenario3?.hasImageAttachment ||
    cleanHasApiImage ||
    hasVal(scenario3RegistryAsset?.sourceUrl);

  if (!hasSafeImage) {
    scenario3ImageDecision = "hide_card_no_safe_image";
    scenario3Action = cleanScenario3 ? "hide_scenario3" : "create_hidden_scenario3";
    const hideRecordId = cleanScenario3?.recordId;
    if (hideRecordId && cleanScenario3.visible) {
      const fields = { "External Display Status": HIDE_DISPLAY_STATUS };
      const errors = validateCopyPatch(fields, { slotKey: SCENARIO3_SLOT, allowDisplayStatus: true });
      if (errors.length) safetyBlockers.push(`scenario3_hide:${errors.join(",")}`);
      else {
        presentationHidePatches.push({
          recordId: hideRecordId,
          slotKey: SCENARIO3_SLOT,
          fields,
          reason: "hide_scenario3_without_safe_image",
        });
      }
    }
  } else if (
    scenario3RegistryAsset?.sourceUrl &&
    !cleanScenario3?.hasImageAttachment &&
    !cleanHasApiImage
  ) {
    scenario3ImageDecision = "materialize_from_approved_registry_source_url";
    scenario3Action = cleanScenario3 ? "materialize_scenario3_image" : "create_and_materialize_scenario3_image";
    const imageFields = { Image: [{ url: scenario3RegistryAsset.sourceUrl }] };
    presentationImagePatches.push({
      recordId: scenario3TargetRecordId,
      slotKey: SCENARIO3_SLOT,
      fields: imageFields,
      reason: "materialize_scenario3_from_approved_registry",
      sourceUrl: scenario3RegistryAsset.sourceUrl,
      pendingCreate: !cleanScenario3,
    });
  }

  for (const row of scenario3Rows) {
    if (isNonQuarantinedScenario3Row(row) && row.visible && row.recordId !== cleanScenario3?.recordId) {
      const fields = { "External Display Status": HIDE_DISPLAY_STATUS };
      const errors = validateCopyPatch(fields, { slotKey: SCENARIO3_SLOT, allowDisplayStatus: true });
      if (errors.length) safetyBlockers.push(`scenario3_duplicate_hide:${row.recordId}:${errors.join(",")}`);
      else {
        presentationHidePatches.push({
          recordId: row.recordId,
          slotKey: SCENARIO3_SLOT,
          fields,
          reason: "hide_duplicate_scenario3",
        });
      }
    }
    if (row.recordId === QUARANTINED_SCENARIO3_RECORD_ID && row.visible) {
      const fields = { "External Display Status": HIDE_DISPLAY_STATUS };
      const errors = validateCopyPatch(fields, { slotKey: SCENARIO3_SLOT, allowDisplayStatus: true });
      if (errors.length) safetyBlockers.push(`scenario3_quarantine:${errors.join(",")}`);
      else {
        presentationHidePatches.push({
          recordId: row.recordId,
          slotKey: SCENARIO3_SLOT,
          fields,
          reason: "quarantine_duplicate_scenario3",
        });
      }
    }
  }

  if (!scenario3Rows.length && !presentationCreates.length) {
    safetyBlockers.push("missing_scenario3_row_and_no_create_planned");
  }

  const bestAtBeforeAfter = [];
  for (const [slotKey, targetCopy] of Object.entries(BEST_AT_TARGET)) {
    const row = presentationRows.find((r) => r.slotKey === slotKey);
    const uiCard = uiBestAtBefore.find((c) => c.slotKey === slotKey);
    const beforeTitle = row?.title || uiCard?.title || "";
    const beforeBody = row?.body || uiCard?.body || "";
    const afterTitle = targetCopy.title;
    const afterBody = targetCopy.body;
    bestAtBeforeAfter.push({
      slotKey,
      recordId: row?.recordId || null,
      beforeTitle,
      beforeBody,
      afterTitle,
      afterBody,
      riskyBefore: RISKY_COPY_RE.test(`${beforeTitle}\n${beforeBody}`),
      riskyAfter: RISKY_COPY_RE.test(`${afterTitle}\n${afterBody}`),
    });

    if (!row) {
      safetyBlockers.push(`missing_bestat_row:${slotKey}`);
      continue;
    }
    if (row.title === afterTitle && row.body === afterBody) continue;

    const fields = { Title: afterTitle, Body: afterBody };
    const errors = validateCopyPatch(fields, { slotKey });
    if (errors.length) safetyBlockers.push(`bestat_copy:${slotKey}:${errors.join(",")}`);
    else {
      presentationCopyPatches.push({
        recordId: row.recordId,
        slotKey,
        fields,
        reason: "remove_risky_bestat_language",
      });
    }
  }

  const registryTraceability = [];
  for (const linkTarget of REGISTRY_LINK_TARGETS) {
    const row = presentationRows.find((r) => r.recordId === linkTarget.recordId);
    if (!row) {
      safetyBlockers.push(`missing_presentation_row:${linkTarget.recordId}`);
      continue;
    }
    const canonical = pickPropertySpecificRegistryAsset(registryAssets, linkTarget, row);
    const beforeId = row.registryLinkIds?.[0] || null;
    const afterId = canonical?.id || null;
    const propertySpecific =
      Boolean(afterId) &&
      (linkTarget.slotKey.startsWith("materials.gallery")
        ? true
        : normalizeUrlKey(canonical?.sourcePageUrl || "").includes(linkTarget.propertyKey));

    registryTraceability.push({
      presentationRecordId: linkTarget.recordId,
      label: linkTarget.label,
      slotKey: linkTarget.slotKey,
      beforeRegistryId: beforeId,
      afterRegistryId: afterId,
      propertySpecific,
      canonicalSourcePageUrl: canonical?.sourcePageUrl || null,
      canonicalSourceUrl: canonical?.sourceUrl || null,
    });

    if (!afterId) {
      safetyBlockers.push(`missing_canonical_registry:${linkTarget.label}`);
      continue;
    }
    if (beforeId === afterId) continue;

    const fields = { "Brand Asset Registry": [afterId] };
    const errors = validateRegistryLinkPatch(fields);
    if (errors.length) safetyBlockers.push(`registry_link:${linkTarget.recordId}:${errors.join(",")}`);
    else {
      presentationRegistryLinkPatches.push({
        recordId: linkTarget.recordId,
        slotKey: linkTarget.slotKey,
        fields,
        reason: "property_specific_registry_traceability",
        canonicalRegistryId: afterId,
      });
    }
  }

  const allPresentationPatches = [
    ...presentationCopyPatches,
    ...presentationHidePatches,
    ...presentationRegistryLinkPatches,
    ...presentationImagePatches,
  ];

  const projectedApiBrand = projectApiBrandAfterPatches(apiBrandBefore, allPresentationPatches, {
    creates: presentationCreates,
    imagePatches: presentationImagePatches,
  });
  const uiScenariosAfter = simulateAtelierScenarioCards(projectedApiBrand || apiBrandBefore || {});
  const uiBestAtAfter = simulateAtelierBestAtCards(projectedApiBrand || apiBrandBefore || {});
  const uiScenario3After = uiScenariosAfter[2];

  if (BOUTIQUE_RESORT_RE.test(uiScenario3After?.title || "")) {
    safetyBlockers.push("projected_scenario3_still_boutique_resort_fallback");
  }
  if (
    uiScenario3After?.showsImagePlaceholder &&
    scenario3ImageDecision !== "hide_card_no_safe_image" &&
    !presentationImagePatches.length
  ) {
    safetyBlockers.push("projected_scenario3_image_placeholder_remains");
  }
  if (uiBestAtAfter.some((c) => c.hasRiskyLanguage)) {
    safetyBlockers.push("projected_bestat_risky_language_remains");
  }

  const openingLinks = registryTraceability.filter((r) => r.slotKey === "footprint.openings");
  if (openingLinks.some((r) => !r.propertySpecific || !r.afterRegistryId)) {
    safetyBlockers.push("opening_registry_not_property_specific");
  }

  const imageFieldsUntouched = presentationImagePatches.length === 0;
  const applyBlockers = [...new Set(safetyBlockers)];
  if (apply) {
    if (!approveBatch) applyBlockers.push("missing_approve_flag");
    if (!noValidationClaim) applyBlockers.push("missing_confirm_no_company_validation_claim");
    if (!noSourceLibraryChanges) applyBlockers.push("missing_confirm_no_source_library_changes");
    if (!noSummaryUrl) applyBlockers.push("missing_confirm_no_summary_url_field");
    if (!noOtherSectionChanges) applyBlockers.push("missing_confirm_no_momentum_proof_standard_changes");
    if (!woodspringOnly) applyBlockers.push("missing_confirm_woodspring_only");
  }

  const hasWork =
    allPresentationPatches.length > 0 || presentationCreates.length > 0;
  const dryRunClean = applyBlockers.length === 0;

  let airtableModified = false;
  const applyResults = { patched: [], created: [], errors: [] };

  const founderGatesReady =
    approveBatch &&
    noValidationClaim &&
    noSourceLibraryChanges &&
    noSummaryUrl &&
    noOtherSectionChanges &&
    woodspringOnly;

  const canApply = apply && founderGatesReady && applyBlockers.length === 0;
  if (canApply) {
    const createdIdByProjected = new Map();
    for (const create of presentationCreates) {
      try {
        const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
          method: "POST",
          body: JSON.stringify({ fields: create.fields, typecast: true }),
        });
        if (!res.ok) throw new Error(json.error?.message || `POST failed: ${res.status}`);
        createdIdByProjected.set(create.projectedRecordId, json.id);
        applyResults.created.push(json.id);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ recordId: create.projectedRecordId, message: err.message });
      }
    }

    for (const patch of allPresentationPatches) {
      try {
        let recordId = patch.recordId;
        if (patch.pendingCreate) {
          recordId = createdIdByProjected.get(patch.recordId) || patch.recordId;
        }
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          { method: "PATCH", body: JSON.stringify({ fields: patch.fields, typecast: true }) },
          recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `PATCH failed: ${res.status}`);
        applyResults.patched.push(recordId);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ recordId: patch.recordId, message: err.message });
      }
    }
  }

  const brandBasicsAfter =
    canApply && airtableModified ? await fetchBrandBasics(target.recordId) : brandBasicsBefore;
  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedSnapshot(brandBasicsAfter));

  const finalQaReport = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.slug,
  }).catch(() => null);
  const completeBuildReport = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: target.slug,
    targetQuality: "active-profile",
  }).catch(() => null);
  const visualDefectReport = await buildBrandExplorerVisualDisplayDefectAuditReport({
    brandIdOrName: target.recordId,
  }).catch(() => null);

  const report = {
    writerVersion: WRITER_VERSION,
    stagingRunId: STAGING_RUN_ID,
    v33gWriterExists: v33gWriterExists(),
    generatedAt: new Date().toISOString(),
    mode: canApply ? "apply" : "dry-run",
    brand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    scenarioRenderingAudit,
    scenario3RootCause,
    scenario3Decision: {
      action: scenario3Action,
      imageDecision: scenario3ImageDecision,
      rationale:
        scenario3ImageDecision === "hide_card_no_safe_image"
          ? "No approved scenario image available without writing Image fields — hide overview.scenario.3 instead of showing IMAGE placeholder."
          : "Preserve existing approved scenario image; patch title/body only.",
    },
    scenarioRowsPatched: presentationCopyPatches.filter((p) => p.slotKey === SCENARIO3_SLOT).map((p) => p.recordId),
    scenarioRowsCreated: presentationCreates.map((c) => c.projectedRecordId),
    scenarioRowsHidden: presentationHidePatches.filter((p) => p.slotKey === SCENARIO3_SLOT).map((p) => p.recordId),
    scenario3RegistryAssetId: scenario3RegistryAsset?.id || null,
    presentationCreates,
    presentationImagePatches,
    bestAtBeforeAfter,
    registryTraceability,
    presentationCopyPatches,
    presentationHidePatches,
    presentationRegistryLinkPatches,
    imageFieldsUntouched,
    companyValidatedBefore,
    companyValidatedAfter: companyValidatedSnapshot(brandBasicsAfter),
    companyValidatedUntouched,
    uiScenario3Before,
    uiScenario3After,
    uiBestAtBefore,
    uiBestAtAfter,
    projectedUiConfirmation: {
      scenario3Title: uiScenario3After?.title || "",
      scenario3ImagePlaceholder: Boolean(uiScenario3After?.showsImagePlaceholder),
      riskyBestAtLanguage: uiBestAtAfter.some((c) => c.hasRiskyLanguage),
      propertySpecificOpeningLinks: openingLinks.every((r) => r.propertySpecific),
      boutiqueResortLanguage: BOUTIQUE_RESORT_RE.test(uiScenario3After?.title || ""),
    },
    dryRunClean,
    applyBlockers,
    applyResults,
    expectedFinalQaResult: finalQaReport?.scores?.overallActiveProfileReadiness || "unknown",
    expectedFinalQaScore: finalQaReport?.scores?.overallNumeric ?? null,
    expectedCompleteBuildResult: completeBuildReport?.summary?.ready
      ? "ready"
      : completeBuildReport?.summary?.blocked
        ? "blocked"
        : "almost_ready_or_blocked",
    expectedVisualDefectResult: visualDefectReport?.defectCounts
      ? `${visualDefectReport.defectCounts.total} defects (critical ${visualDefectReport.defectCounts.critical}, high ${visualDefectReport.defectCounts.high})`
      : "unknown",
    exactApplyCommand: dryRunClean && hasWork ? buildApplyCommand({ brand: target.slug }) : null,
    exactDryRunCommand: `npm run brand-explorer-woodspring-founder-visual-correction-writer -- --brand ${target.slug} --dry-run`,
    airtableModified,
  };

  report.markdown = buildMarkdown(report);
  return report;
}
