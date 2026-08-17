/**
 * Brand Explorer WoodSpring Property Image Registry Linkage Recognition v33C-R3.
 *
 * Purpose: reconcile presentation rows to canonical approved registry assets so
 * Final QA / Visual Audit / Complete Build recognize openings + gallery images.
 *
 * Non-negotiables:
 * - Do NOT change image fields/attachments/URLs
 * - Do NOT change copy/titles/sort order/visibility (except registry link field)
 * - Do NOT touch Company Validated, Summary URLs, Source Library approvals
 *
 * @see docs/data-intelligence/brand-explorer-woodspring-property-image-registry-linkage-writer-v33C-R3.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  BRAND_ASSET_REGISTRY_TABLE,
  MAP_BRAND_ASSET,
  listRegistryAssetsForBrand,
} from "./brand-asset-registry-workflow.js";
import {
  getDiscoveryBrandConfig,
  isRegistryAssetApprovedForExplorer,
} from "./brand-explorer-brand-asset-image-governance.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";
import { TARGET_BRAND, PROTECTED_BRAND_SLUGS } from "./brand-explorer-woodspring-real-property-examples-writer.js";

export const WRITER_VERSION = "v33C-R3";
export const STAGING_RUN_ID = "v33C-R3-woodspring-property-image-registry-linkage";
export const REPORT_JSON_NAME =
  "brand-explorer-woodspring-property-image-registry-linkage-writer.json";
export const REPORT_MD_NAME =
  "brand-explorer-woodspring-property-image-registry-linkage-writer.md";
export const DOC_MD_NAME =
  "brand-explorer-woodspring-property-image-registry-linkage-writer-v33C-R3.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v33C-R3-woodspring-property-image-registry-linkage";
export const APPLY_FLAG_FOUNDER = "--founder-approved-woodspring-property-specific-hotel-images";
export const APPLY_FLAG_NO_IMAGE_CHANGES = "--confirm-no-image-field-changes";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_SOURCE_LIBRARY = "--confirm-no-source-library-changes";
export const APPLY_FLAG_NO_SUMMARY_URL = "--confirm-no-summary-url-field";
export const APPLY_FLAG_NO_OTHER_SECTIONS = "--confirm-no-momentum-proof-standard-changes";
export const APPLY_FLAG_WOODSPRING_ONLY = "--confirm-woodspring-only";

export { TARGET_BRAND };

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const HIDE_DISPLAY_STATUS = "Do Not Display";

const TARGET_PRESENTATION_ROWS = Object.freeze([
  { recordId: "recI3cbO8mOhEpo1W", slotKey: "footprint.openings", label: "Orlando opening" },
  { recordId: "recpNB0KoPq6y3Mhs", slotKey: "footprint.openings", label: "Charlotte opening" },
  { recordId: "rec4Eqp9lwXSP7UQE", slotKey: "footprint.openings", label: "Raleigh opening" },
  { recordId: "rechUn7nwlxjW1jyV", slotKey: "materials.gallery.1", label: "Gallery 1" },
  { recordId: "recXfIGZUrwap6AIK", slotKey: "materials.gallery.2", label: "Gallery 2" },
  { recordId: "recJokIWQxU64gVsl", slotKey: "materials.gallery.3", label: "Gallery 3" },
]);

const BLOCKED_PRESENTATION_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Summary URL",
  "View Summary URL",
  "Case summary URL",
  "Image",
  "Scenario Image",
  "Title",
  "Body",
  "Sort Order",
  "External Display Status",
]);

const FILES_READ = [
  "AGENTS.md",
  "live Airtable presentation rows (6)",
  "live Brand Asset Registry (WoodSpring)",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-visual-display-defect-audit.json",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-woodspring-property-image-registry-linkage-writer.js",
  "scripts/brand-explorer-woodspring-property-image-registry-linkage-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
  "lib/partner-intelligence/brand-explorer-brand-asset-image-governance.js",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
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

export function v33cR3WriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-woodspring-property-image-registry-linkage-writer.js"
    )
  );
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || TARGET_BRAND.slug).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v33C-R3`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v33C-R3 supports WoodSpring Suites only; got: ${brandArg}`);
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
    const imageAtt = f.Image?.[0];
    return {
      recordId: rec.id,
      slotKey: nz(f["Slot Key"]),
      title: nz(f.Title),
      body: nz(f.Body),
      externalDisplayStatus: nz(f["External Display Status"]),
      visible:
        nz(f["External Display Status"]).toLowerCase() !== HIDE_DISPLAY_STATUS.toLowerCase(),
      hasImage: Array.isArray(f.Image) && f.Image.length > 0,
      imageUrl: imageAtt?.url ? nz(imageAtt.url) : "",
      summaryUrl: nz(f["Summary URL"] || f["View Summary URL"]),
      sourcePageUrl: nz(f["Source Page URL"]),
      registryLinkIds: Array.isArray(f["Brand Asset Registry"]) ? f["Brand Asset Registry"] : [],
    };
  });
}

function registryTableName() {
  return process.env.PARTNER_INTELLIGENCE_ASSET_REGISTRY_TABLE_ID || BRAND_ASSET_REGISTRY_TABLE;
}

function pickCanonicalRegistryAssetForRow(registryAssets, row, { requiredStagingPrefix = "v33C-R2" } = {}) {
  const slot = nz(row?.slotKey);
  const bodyUrl = nz(row?.body).match(/https?:\/\/[^\s<>"')]+/i)?.[0] || "";
  const imageKey = normalizeUrlKey(row?.imageUrl);
  const candidates = (registryAssets || []).filter(
    (a) =>
      nz(a.recommendedExplorerSlot) === slot &&
      !/do not use/i.test(nz(a.assetStatus)) &&
      isRegistryAssetApprovedForExplorer(a)
  );

  const staged = candidates.filter((a) => nz(a.stagingRunId).startsWith(requiredStagingPrefix));
  const byExplicit = row?.registryLinkIds?.length
    ? candidates.filter((a) => row.registryLinkIds.includes(a.id))
    : [];
  const byBody = bodyUrl
    ? candidates.filter((a) => normalizeUrlKey(a.sourcePageUrl) === normalizeUrlKey(bodyUrl))
    : [];
  const byImage = imageKey
    ? candidates.filter((a) => normalizeUrlKey(a.sourceUrl) === imageKey)
    : [];

  return (
    byExplicit[0] ||
    staged[0] ||
    byBody[0] ||
    byImage[0] ||
    candidates[0] ||
    null
  );
}

function validatePresentationRegistryLinkPatch(fields) {
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
    "npm run brand-explorer-woodspring-property-image-registry-linkage-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER,
    APPLY_FLAG_NO_IMAGE_CHANGES,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_SOURCE_LIBRARY,
    APPLY_FLAG_NO_SUMMARY_URL,
    APPLY_FLAG_NO_OTHER_SECTIONS,
    APPLY_FLAG_WOODSPRING_ONLY,
  ].join(" ");
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer WoodSpring Property Image Registry Linkage v33C-R3");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Dry-run clean: **${report.dryRunClean ? "yes" : "no"}**`);
  lines.push(`- Image fields untouched: **${report.imageFieldsUntouched ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Registry linkage audit");
  for (const a of report.linkageAudit) {
    lines.push(
      `- \`${a.presentationRecordId}\` ${a.slotKey} **${a.title}** — image: ${a.imageUrlStatus}; linked registry: ${a.currentRegistryLink || "(none)"}; canonical: ${a.canonicalRegistryId || "(missing)"}; recognized: ${a.projectedRecognized ? "yes" : "no"}; why: ${a.whyNotRecognized || "n/a"}`
    );
  }
  lines.push("");
  lines.push("## Canonical registry rows selected");
  for (const c of report.canonicalRegistrySelected) {
    lines.push(
      `- \`${c.registryId}\` slot=${c.slotKey} staging=${c.stagingRunId || "(none)"} approved=${c.approved}`
    );
  }
  lines.push("");
  lines.push(`## Presentation rows relinked: ${report.presentationRegistryLinkPatches.length}`);
  if (report.exactApplyCommand) {
    lines.push("");
    lines.push("```bash");
    lines.push(report.exactApplyCommand);
    lines.push("```");
  }
  if (report.applyBlockers.length) {
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

export async function buildBrandExplorerWoodspringPropertyImageRegistryLinkageWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  founderApproved = false,
  noImageFieldChanges = false,
  noValidationClaim = false,
  noSourceLibraryChanges = false,
  noSummaryUrl = false,
  noOtherSectionChanges = false,
  woodspringOnly = false,
} = {}) {
  const target = resolveTargetBrand(brandArg);
  const brandConfig = getDiscoveryBrandConfig(target.slug);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const [presentationRows, registryAssetsRaw, brandApiBefore] = await Promise.all([
    listPresentationRows(baseId, apiKey, target.recordId, target.name),
    listRegistryAssetsForBrand(target.recordId).catch(() => []),
    fetchBrandApiShape(target.recordId),
  ]);

  const registryAssets = Array.isArray(registryAssetsRaw) ? registryAssetsRaw : [];

  const linkageAudit = [];
  const canonicalRegistrySelected = [];
  const presentationRegistryLinkPatches = [];
  const safetyBlockers = [];

  for (const targetRow of TARGET_PRESENTATION_ROWS) {
    const row = presentationRows.find((r) => r.recordId === targetRow.recordId);
    if (!row) {
      safetyBlockers.push(`missing_presentation_row:${targetRow.recordId}`);
      continue;
    }
    const currentRegistryLink = row.registryLinkIds?.[0] || null;
    const currentRegistryAsset = currentRegistryLink
      ? registryAssets.find((a) => a.id === currentRegistryLink) || null
      : null;

    const canonical = pickCanonicalRegistryAssetForRow(registryAssets, row);
    if (canonical) {
      canonicalRegistrySelected.push({
        registryId: canonical.id,
        slotKey: canonical.recommendedExplorerSlot,
        stagingRunId: canonical.stagingRunId || null,
        approved: isRegistryAssetApprovedForExplorer(canonical),
        sourcePageUrl: canonical.sourcePageUrl,
        sourceUrl: canonical.sourceUrl,
      });
    } else {
      safetyBlockers.push(`missing_canonical_registry:${row.recordId}`);
    }

    const projectedRecognized = Boolean(canonical && isRegistryAssetApprovedForExplorer(canonical));
    const whyNotRecognized = !canonical
      ? "no_canonical_registry_asset_found"
      : !isRegistryAssetApprovedForExplorer(canonical)
        ? "canonical_registry_not_approved"
        : null;

    linkageAudit.push({
      presentationRecordId: row.recordId,
      slotKey: row.slotKey,
      title: row.title,
      imageUrl: row.imageUrl,
      imageUrlStatus: row.imageUrl ? "present" : "missing",
      imageSource: row.imageUrl ? (/\bairtableusercontent\.com\b/i.test(row.imageUrl) ? "airtable_attachment" : "direct") : "missing",
      currentRegistryLink,
      currentRegistryApproved: currentRegistryAsset ? isRegistryAssetApprovedForExplorer(currentRegistryAsset) : false,
      currentRegistryExplorerSlot: currentRegistryAsset?.recommendedExplorerSlot || null,
      currentRegistrySourcePageUrl: currentRegistryAsset?.sourcePageUrl || null,
      currentRegistrySourceUrl: currentRegistryAsset?.sourceUrl || null,
      canonicalRegistryId: canonical?.id || null,
      canonicalApproved: canonical ? isRegistryAssetApprovedForExplorer(canonical) : false,
      canonicalExplorerSlot: canonical?.recommendedExplorerSlot || null,
      canonicalSourcePageUrl: canonical?.sourcePageUrl || null,
      canonicalSourceUrl: canonical?.sourceUrl || null,
      projectedRecognized,
      whyNotRecognized,
    });

    if (canonical) {
      const desired = [canonical.id];
      const same =
        Array.isArray(row.registryLinkIds) &&
        row.registryLinkIds.length === 1 &&
        row.registryLinkIds[0] === canonical.id;
      if (!same) {
        const fields = { "Brand Asset Registry": desired };
        const errors = validatePresentationRegistryLinkPatch(fields);
        if (errors.length) safetyBlockers.push(`registry_link_validation:${row.recordId}:${errors.join(";")}`);
        else {
          presentationRegistryLinkPatches.push({
            recordId: row.recordId,
            slotKey: row.slotKey,
            fields,
            reason: "link_presentation_to_canonical_registry_asset",
            canonicalRegistryId: canonical.id,
          });
        }
      }
    }
  }

  const imageFieldsUntouched = true;
  const applyBlockers = [...safetyBlockers];
  if (apply) {
    if (!approveBatch) applyBlockers.push("missing_approve_flag");
    if (!founderApproved) applyBlockers.push("missing_founder_approved_flag");
    if (!noImageFieldChanges) applyBlockers.push("missing_confirm_no_image_field_changes");
    if (!noValidationClaim) applyBlockers.push("missing_confirm_no_company_validation_claim");
    if (!noSourceLibraryChanges) applyBlockers.push("missing_confirm_no_source_library_changes");
    if (!noSummaryUrl) applyBlockers.push("missing_confirm_no_summary_url_field");
    if (!noOtherSectionChanges) applyBlockers.push("missing_confirm_no_momentum_proof_standard_changes");
    if (!woodspringOnly) applyBlockers.push("missing_confirm_woodspring_only");
  }

  const hasWork = presentationRegistryLinkPatches.length > 0;
  const dryRunClean =
    safetyBlockers.length === 0 && hasWork && applyBlockers.filter((b) => b.startsWith("missing_")).length === 0;

  let airtableModified = false;
  const applyResults = {
    presentationLinked: [],
    errors: [],
  };

  const founderGatesReady =
    approveBatch &&
    founderApproved &&
    noImageFieldChanges &&
    noValidationClaim &&
    noSourceLibraryChanges &&
    noSummaryUrl &&
    noOtherSectionChanges &&
    woodspringOnly;

  const canApply = apply && founderGatesReady && safetyBlockers.length === 0;
  if (canApply) {
    for (const patch of presentationRegistryLinkPatches) {
      try {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          { method: "PATCH", body: JSON.stringify({ fields: patch.fields, typecast: true }) },
          patch.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `PATCH failed: ${res.status}`);
        applyResults.presentationLinked.push(patch.recordId);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ recordId: patch.recordId, message: err.message });
      }
    }
  }

  const brandBasicsAfter =
    canApply && airtableModified ? await fetchBrandBasics(target.recordId) : brandBasicsBefore;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);
  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

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
    v33cR3WriterExists: v33cR3WriterExists(),
    generatedAt: new Date().toISOString(),
    mode: canApply ? "apply" : "dry-run",
    brand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    linkageAudit,
    canonicalRegistrySelected,
    presentationRegistryLinkPatches,
    registryRowPatches: [],
    oldGenericRegistryRowsSuperseded: [],
    auditorRecognitionIssue: "Airtable materialized attachments change imageUrl to airtableusercontent URLs; registry matching must use source page URL from row body or explicit linkage.",
    codePatches: [
      {
        file: "lib/partner-intelligence/brand-explorer-brand-asset-image-governance.js",
        change: "findRegistryAssetForPresentationRow: also match registry sourcePageUrl against URL extracted from row body",
      },
    ],
    imageFieldsUntouched,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched,
    dryRunClean,
    applyBlockers,
    applyResults,
    expectedFinalQaResult: finalQaReport?.brandReports?.[0]?.overallActiveProfileReadiness || "unknown",
    expectedCompleteBuildResult: completeBuildReport?.readyForActiveProfile ? "ready" : "almost_ready_or_blocked",
    expectedVisualDefectResult: visualDefectReport?.summary?.defectCount != null ? `${visualDefectReport.summary.defectCount} defects` : "unknown",
    exactApplyCommand: dryRunClean ? buildApplyCommand({ brand: target.slug }) : null,
    exactDryRunCommand: `npm run brand-explorer-woodspring-property-image-registry-linkage-writer -- --brand ${target.slug} --dry-run`,
    airtableModified,
  };

  report.markdown = buildMarkdown(report);
  return report;
}

