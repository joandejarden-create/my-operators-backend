/**
 * Brand Explorer Radisson Individuals Overview Scenario Reactivation + Registry Link v31H-R1.
 *
 * Clears Do Not Display on overview.scenario.1–3 and links pending-review registry assets.
 * Does not approve images, touch openings/gallery, or modify Company Validated.
 *
 * @see docs/data-intelligence/brand-explorer-radisson-individuals-overview-scenario-reactivation-writer-v31H-R1.md
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
  validateRegistryWritePayload,
} from "./brand-asset-registry-workflow.js";
import { ASSET_STATUS, SOURCE_BASIS } from "./brand-asset-pr-package-governance.js";
import {
  assessPresentationRowImageGovernance,
  DISCOVERY_BRAND_CONFIG,
  findRegistryAssetForPresentationRow,
  isRegistryAssetApprovedForExplorer,
} from "./brand-explorer-brand-asset-image-governance.js";
import { classifyRegistryAsset } from "./brand-explorer-radisson-individuals-approved-asset-materialization-writer.js";
import {
  findInternalLanguageInRow,
} from "./brand-explorer-openings-ui-quarantine-governance.js";
import {
  EXTERNAL_DISPLAY_STATUS_QUARANTINE,
  HIDDEN_EXTERNAL_DISPLAY_STATUSES,
  isPresentationRowVisibleInExplorer,
  TARGET_BRAND,
  PROTECTED_BRAND_SLUGS,
} from "./brand-explorer-radisson-individuals-openings-suppression-writer.js";
import { isTemporaryAirtableUrl } from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import { MAP_VISUAL_SLOT, VISUAL_SLOT } from "./brand-explorer-visual-slot-requirements.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";

export const WRITER_VERSION = "31H-R1";
export const REPORT_JSON_NAME =
  "brand-explorer-radisson-individuals-overview-scenario-reactivation-writer.json";
export const REPORT_MD_NAME =
  "brand-explorer-radisson-individuals-overview-scenario-reactivation-writer.md";
export const DOC_MD_NAME =
  "brand-explorer-radisson-individuals-overview-scenario-reactivation-writer-v31H-R1.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v31H-R1-overview-scenario-reactivation";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-radisson-individuals-overview-scenarios";
export const APPLY_FLAG_NO_IMAGE_APPROVAL = "--confirm-no-image-approval";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";

export const STAGING_RUN_ID = "v31H-R1-overview-scenario-reactivation";
export const SCENARIO_SLOTS = Object.freeze([
  "overview.scenario.1",
  "overview.scenario.2",
  "overview.scenario.3",
]);

/** Durable brand-level source for overview scenario registry metadata. */
export const SCENARIO_DURABLE_SOURCE_PAGE =
  "https://www.choicehotels.com/radisson-individuals-hotels";

export const RADISSON_REFERENCE = Object.freeze({
  slug: "radisson",
  recordId: "recywbx1YQSTCPqW1",
  name: "Radisson by Choice",
});

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const SCENARIO_SLOT_RE = /^overview\.scenario\.[123]$/;
const OPENINGS_SLOT = "footprint.openings";

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-scenario-display-parity-audit.md",
  "reports/brand-explorer-scenario-display-parity-audit.json",
  "reports/brand-explorer-radisson-individuals-image-asset-openings-root-cause-audit.md",
  "reports/brand-explorer-radisson-individuals-image-asset-openings-root-cause-audit.json",
  "reports/brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.md",
  "reports/brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.json",
  "live Radisson Individuals Brand Explorer Presentation rows",
  "live Radisson Individuals Brand Asset Registry rows",
  "live API response for Radisson Individuals",
  "Radisson by Choice overview.scenario.1–3 (reference only)",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "docs/brand-explorer-presentation-slots.md",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-radisson-individuals-overview-scenario-reactivation-writer.js",
  "scripts/brand-explorer-radisson-individuals-overview-scenario-reactivation-writer.mjs",
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

export function v31hR1WriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-radisson-individuals-overview-scenario-reactivation-writer.js"
    )
  );
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || TARGET_BRAND.slug).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v31H-R1`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v31H-R1 supports Radisson Individuals by Choice only; got: ${brandArg}`);
  }
  return TARGET_BRAND;
}

function apiUrl(baseId, tableName, recordId = "") {
  const base = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
  return recordId ? `${base}/${encodeURIComponent(recordId)}` : base;
}

async function airtableFetch(baseId, apiKey, tableName, options = {}, recordId = "") {
  const res = await fetch(apiUrl(baseId, tableName, recordId), {
    ...options,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  return { res, json };
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

function normalizePresentationRow(rec) {
  const f = rec.fields || {};
  return {
    recordId: rec.id,
    fields: f,
    slotKey: nz(f["Slot Key"]),
    title: nz(f.Title),
    body: nz(f.Body),
    sortOrder: f["Sort Order"],
    active: f.Active !== false,
    externalDisplayStatus: nz(f["External Display Status"]),
    quarantined: HIDDEN_EXTERNAL_DISPLAY_STATUSES.includes(nz(f["External Display Status"])),
    visibleInExplorer: isPresentationRowVisibleInExplorer(f),
    hasImage: Array.isArray(f.Image) && f.Image.length > 0,
    imageUrl: Array.isArray(f.Image) && f.Image[0]?.url ? nz(f.Image[0].url) : null,
    sourcePageUrl: nz(f["Source Page URL"]) || null,
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

function apiBlocksForSlots(brand, slotKeys) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  const set = new Set(slotKeys);
  return blocks.filter((b) => set.has(nz(b?.slotKey)));
}

function registryFieldsWouldApprove(fields) {
  return (
    fields[MAP_BRAND_ASSET.assetStatus] === ASSET_STATUS.APPROVED_EXPLORER ||
    fields[MAP_BRAND_ASSET.explorerUsePermission] === "Approved For Explorer" ||
    fields[MAP_BRAND_ASSET.usageReviewStatus] === "Usage Review Complete"
  );
}

export function buildPendingScenarioRegistryFields({
  slotKey,
  title,
  presentationRecordId,
  brandRecordId,
  brandName,
  imageUrl,
  sourcePageUrl = SCENARIO_DURABLE_SOURCE_PAGE,
}) {
  const assetName = `Radisson Individuals — ${title} — Overview Scenario Image`;
  return {
    [MAP_BRAND_ASSET.assetName]: assetName,
    [MAP_BRAND_ASSET.brand]: [brandRecordId],
    [MAP_BRAND_ASSET.brandRecordId]: brandRecordId,
    [MAP_BRAND_ASSET.parentCompany]: "Choice Hotels International",
    [MAP_BRAND_ASSET.assetType]: "Restaurant / Bar / Lifestyle",
    [MAP_BRAND_ASSET.assetStatus]: ASSET_STATUS.NEEDS_USAGE_REVIEW,
    [MAP_BRAND_ASSET.sourceBasis]: SOURCE_BASIS.RENDERED_OFFICIAL,
    [MAP_BRAND_ASSET.sourceUrl]: imageUrl || null,
    [MAP_BRAND_ASSET.sourcePageUrl]: sourcePageUrl,
    [MAP_BRAND_ASSET.usageReviewStatus]: "Pending Review",
    [MAP_BRAND_ASSET.explorerUsePermission]: "Candidate Only",
    [MAP_BRAND_ASSET.recommendedExplorerSlot]: slotKey,
    [MAP_BRAND_ASSET.isPrimaryCandidate]: true,
    [MAP_BRAND_ASSET.reviewNotes]:
      "v31H-R1 overview scenario reactivation — pending founder image review; not approved for active-profile.",
    [MAP_BRAND_ASSET.sourceNotes]: `Presentation row ${presentationRecordId}. Overview scenario card image — registry link only.`,
    [MAP_BRAND_ASSET.stagingRunId]: STAGING_RUN_ID,
    [MAP_BRAND_ASSET.companyValidated]: false,
    [MAP_VISUAL_SLOT.explorerSection]: VISUAL_SLOT.VALUE_DRIVER,
    [MAP_VISUAL_SLOT.slotPurpose]: "Overview scenario card image (overview.scenario.*)",
    [MAP_VISUAL_SLOT.relatedValueDriver]: title,
    [MAP_VISUAL_SLOT.brandConfirmed]: "Yes",
    [MAP_VISUAL_SLOT.validationStatus]: "Pending Review",
    [MAP_VISUAL_SLOT.validationNotes]:
      "Linked by v31H-R1 — image visible in Explorer after reactivation; founder approval required before active-profile evidence.",
  };
}

export async function verifyImageAttachmentAccessible(imageUrl) {
  const url = nz(imageUrl);
  if (!url) return { accessible: false, reason: "missing_image_url" };
  try {
    const res = await fetch(url, { method: "HEAD", redirect: "follow" });
    if (res.ok) return { accessible: true, status: res.status };
    if (res.status === 405) {
      const getRes = await fetch(url, { method: "GET", redirect: "follow" });
      return getRes.ok
        ? { accessible: true, status: getRes.status, method: "GET" }
        : { accessible: false, reason: `http_${getRes.status}`, status: getRes.status };
    }
    return { accessible: false, reason: `http_${res.status}`, status: res.status };
  } catch (err) {
    return { accessible: false, reason: err.message };
  }
}

export function classifyScenarioReactivationEligibility({
  row,
  registryMatch,
  brandConfig,
  imageCheck,
}) {
  const blockers = [];
  if (!SCENARIO_SLOT_RE.test(row.slotKey)) {
    blockers.push("not_overview_scenario_slot");
  }
  if (row.slotKey === OPENINGS_SLOT) {
    blockers.push("openings_row_blocked");
  }
  if (!row.quarantined) {
    blockers.push("not_quarantined");
  }
  if (row.externalDisplayStatus !== EXTERNAL_DISPLAY_STATUS_QUARANTINE) {
    blockers.push("unexpected_display_status");
  }
  const internalLanguage = findInternalLanguageInRow(row);
  if (internalLanguage.length) {
    blockers.push("internal_language_in_copy");
  }
  if (!row.hasImage || !row.imageUrl) {
    blockers.push("missing_image_attachment");
  } else if (!imageCheck?.accessible) {
    blockers.push("image_url_inaccessible_or_expired");
  }
  const assessment = assessPresentationRowImageGovernance(row, brandConfig, registryMatch ? [registryMatch] : []);
  if (assessment?.wrongBrandRisk) {
    blockers.push("wrong_brand_signage");
  }
  if (registryMatch && nz(registryMatch.explorerUsePermission) === "Do Not Use") {
    blockers.push("registry_do_not_use");
  }

  const ownerFacing =
    internalLanguage.length === 0 && Boolean(nz(row.title)) && Boolean(nz(row.body));
  const safeToReactivate = blockers.length === 0 && ownerFacing;

  return {
    recordId: row.recordId,
    slotKey: row.slotKey,
    title: row.title,
    bodyExcerpt: row.body.slice(0, 200),
    hasImageAttachment: row.hasImage,
    imageUrl: row.imageUrl,
    imageCheck,
    externalDisplayStatus: row.externalDisplayStatus,
    active: row.active,
    quarantined: row.quarantined,
    registryRecordId: registryMatch?.id || null,
    registryApproved: registryMatch ? isRegistryAssetApprovedForExplorer(registryMatch) : false,
    imageGovernance: assessment
      ? {
          pendingImageReview: assessment.pendingImageReview,
          registryApproved: assessment.registryApproved,
          recommendation: assessment.recommendation,
        }
      : null,
    internalLanguage,
    ownerFacingCopy: ownerFacing,
    safeToReactivate,
    blockers,
    hiddenReason:
      row.quarantined && row.externalDisplayStatus === EXTERNAL_DISPLAY_STATUS_QUARANTINE
        ? "External Display Status = Do Not Display — api/brand-library.js excludes row from brand.brandExplorer.blocks[]"
        : null,
    proposedDisplayStatus: "",
    proposedDisplayStatusNote:
      "Clear External Display Status (blank) — API treats blank as visible; no explicit Display select value required",
  };
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return `npm run brand-explorer-radisson-individuals-overview-scenario-reactivation-writer -- --brand ${brand} --apply ${APPLY_FLAG_APPROVE} ${APPLY_FLAG_FOUNDER} ${APPLY_FLAG_NO_IMAGE_APPROVAL} ${APPLY_FLAG_NO_VALIDATION}`;
}

export async function buildBrandExplorerRadissonIndividualsOverviewScenarioReactivationWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  noImageApproval = false,
  noValidationClaim = false,
} = {}) {
  const target = resolveTargetBrand(brandArg);
  const brandConfig = DISCOVERY_BRAND_CONFIG[target.slug];

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const [presentationRaw, registryAssetsRaw, brandApiBefore, radissonRefRaw] = await Promise.all([
    listPresentationRowsRaw(baseId, apiKey, target.recordId, target.name),
    listRegistryAssetsForBrand(target.recordId).catch(() => []),
    fetchBrandApiShape(target.recordId),
    listPresentationRowsRaw(
      baseId,
      apiKey,
      RADISSON_REFERENCE.recordId,
      RADISSON_REFERENCE.name
    ),
  ]);

  const allRows = presentationRaw.map(normalizePresentationRow);
  const scenarioRows = allRows
    .filter((r) => SCENARIO_SLOTS.includes(r.slotKey))
    .sort((a, b) => SCENARIO_SLOTS.indexOf(a.slotKey) - SCENARIO_SLOTS.indexOf(b.slotKey));

  const openingsTouchedCheck = allRows.filter(
    (r) => r.slotKey === OPENINGS_SLOT && !r.quarantined
  );

  const radissonReference = radissonRefRaw
    .map(normalizePresentationRow)
    .filter((r) => SCENARIO_SLOTS.includes(r.slotKey))
    .map((r) => ({
      recordId: r.recordId,
      slotKey: r.slotKey,
      title: r.title,
      externalDisplayStatus: r.externalDisplayStatus || null,
      quarantined: r.quarantined,
      hasImage: r.hasImage,
      visibleInExplorer: r.visibleInExplorer,
    }));

  const overviewScenarioDiagnosis = [];
  const proposedPresentationUpdates = [];
  const proposedRegistryCreates = [];
  const proposedRegistryUpdates = [];
  const rowsToReactivate = [];

  for (const row of scenarioRows) {
    const registryMatch =
      registryAssetsRaw.find((a) => nz(a.recommendedExplorerSlot) === row.slotKey) ||
      findRegistryAssetForPresentationRow(registryAssetsRaw, row);
    const imageCheck = row.imageUrl ? await verifyImageAttachmentAccessible(row.imageUrl) : null;
    const eligibility = classifyScenarioReactivationEligibility({
      row,
      registryMatch,
      brandConfig,
      imageCheck,
    });
    overviewScenarioDiagnosis.push(eligibility);

    if (!eligibility.safeToReactivate) continue;

    rowsToReactivate.push({
      recordId: row.recordId,
      slotKey: row.slotKey,
      title: row.title,
    });

    proposedPresentationUpdates.push({
      action: "reactivate_clear_display_status",
      recordId: row.recordId,
      slotKey: row.slotKey,
      title: row.title,
      fields: {
        "External Display Status": null,
        "Brand Name": target.name,
        Brand: [target.recordId],
      },
      before: { externalDisplayStatus: row.externalDisplayStatus },
      after: { externalDisplayStatus: null, visibleInExplorer: true },
    });

    const registryFields = buildPendingScenarioRegistryFields({
      slotKey: row.slotKey,
      title: row.title,
      presentationRecordId: row.recordId,
      brandRecordId: target.recordId,
      brandName: target.name,
      imageUrl: row.imageUrl,
      sourcePageUrl: SCENARIO_DURABLE_SOURCE_PAGE,
    });

    if (registryMatch?.id) {
      const patchFields = { ...registryFields };
      delete patchFields[MAP_BRAND_ASSET.brand];
      proposedRegistryUpdates.push({
        action: "link_update_registry",
        registryRecordId: registryMatch.id,
        slotKey: row.slotKey,
        presentationRecordId: row.recordId,
        fields: patchFields,
      });
    } else {
      proposedRegistryCreates.push({
        action: "create_registry",
        slotKey: row.slotKey,
        presentationRecordId: row.recordId,
        fields: registryFields,
      });
    }
  }

  const apiBlocksBefore = apiBlocksForSlots(brandApiBefore, SCENARIO_SLOTS);

  const applyBlockers = [];
  if (scenarioRows.length !== SCENARIO_SLOTS.length) {
    applyBlockers.push(`expected_${SCENARIO_SLOTS.length}_scenario_rows_found_${scenarioRows.length}`);
  }
  if (proposedPresentationUpdates.length === 0) {
    applyBlockers.push("no_eligible_scenario_rows_to_reactivate");
  }
  for (const u of [...proposedPresentationUpdates, ...proposedRegistryCreates, ...proposedRegistryUpdates]) {
    if (!SCENARIO_SLOT_RE.test(u.slotKey)) {
      applyBlockers.push(`non_scenario_slot_in_plan:${u.slotKey}`);
    }
    if (u.slotKey === OPENINGS_SLOT) {
      applyBlockers.push("openings_row_would_be_modified");
    }
  }
  if (
    proposedRegistryCreates.some((c) => registryFieldsWouldApprove(c.fields)) ||
    proposedRegistryUpdates.some((u) => registryFieldsWouldApprove(u.fields))
  ) {
    applyBlockers.push("images_would_be_marked_approved");
  }
  if (
    [...proposedRegistryCreates, ...proposedRegistryUpdates].some((r) =>
      isTemporaryAirtableUrl(r.fields[MAP_BRAND_ASSET.sourcePageUrl])
    )
  ) {
    applyBlockers.push("temporary_url_would_be_written_as_source_page_url");
  }
  for (const d of overviewScenarioDiagnosis) {
    if (d.blockers.includes("internal_language_in_copy")) {
      applyBlockers.push(`internal_language:${d.slotKey}`);
    }
    if (d.blockers.includes("missing_image_attachment")) {
      applyBlockers.push(`missing_image:${d.slotKey}`);
    }
    if (d.blockers.includes("image_url_inaccessible_or_expired")) {
      applyBlockers.push(`expired_image:${d.slotKey}`);
    }
  }

  const hasWork =
    proposedPresentationUpdates.length > 0 ||
    proposedRegistryCreates.length > 0 ||
    proposedRegistryUpdates.length > 0;
  const applyGatesReady =
    apply && approveBatch && founderReviewed && noImageApproval && noValidationClaim;
  const dryRunClean = applyBlockers.length === 0;
  const canApply = applyGatesReady && dryRunClean && hasWork;

  let airtableModified = false;
  let imagesApproved = false;
  let applyResults = {
    presentationUpdated: [],
    registryCreated: [],
    registryUpdated: [],
    errors: [],
    openingsRowsTouched: [],
    blocked: false,
    blockers: [],
  };
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    for (const create of proposedRegistryCreates) {
      const validation = validateRegistryWritePayload(create.fields);
      if (!validation.valid) {
        applyResults.errors.push({
          slotKey: create.slotKey,
          error: `registry_validation: ${validation.errors.join("; ")}`,
        });
        continue;
      }
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        BRAND_ASSET_REGISTRY_TABLE,
        {
          method: "POST",
          body: JSON.stringify({ records: [{ fields: create.fields }], typecast: true }),
        }
      );
      if (!res.ok) {
        applyResults.errors.push({
          slotKey: create.slotKey,
          error: json.error?.message || `registry create failed ${res.status}`,
        });
        continue;
      }
      const createdId = json.records?.[0]?.id;
      applyResults.registryCreated.push({ slotKey: create.slotKey, recordId: createdId });
      airtableModified = true;
      await new Promise((r) => setTimeout(r, 220));
    }

    for (const update of proposedRegistryUpdates) {
      if (registryFieldsWouldApprove(update.fields)) {
        applyResults.errors.push({
          registryRecordId: update.registryRecordId,
          error: "blocked_registry_approval_fields",
        });
        continue;
      }
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        BRAND_ASSET_REGISTRY_TABLE,
        {
          method: "PATCH",
          body: JSON.stringify({ fields: update.fields, typecast: true }),
        },
        update.registryRecordId
      );
      if (!res.ok) {
        applyResults.errors.push({
          registryRecordId: update.registryRecordId,
          error: json.error?.message || `registry PATCH failed ${res.status}`,
        });
        continue;
      }
      applyResults.registryUpdated.push(update.registryRecordId);
      airtableModified = true;
      await new Promise((r) => setTimeout(r, 220));
    }

    for (const update of proposedPresentationUpdates) {
      if (!SCENARIO_SLOT_RE.test(update.slotKey)) {
        applyResults.errors.push({ recordId: update.recordId, error: "blocked_non_scenario_slot" });
        continue;
      }
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        {
          method: "PATCH",
          body: JSON.stringify({ fields: update.fields, typecast: true }),
        },
        update.recordId
      );
      if (!res.ok) {
        applyResults.errors.push({
          recordId: update.recordId,
          error: json.error?.message || `presentation PATCH failed ${res.status}`,
        });
        continue;
      }
      applyResults.presentationUpdated.push({
        recordId: update.recordId,
        slotKey: update.slotKey,
      });
      airtableModified = true;
      await new Promise((r) => setTimeout(r, 220));
    }

    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(target.recordId));
    applyResults.companyValidatedChanged =
      JSON.stringify(companyValidatedBefore) !== JSON.stringify(companyValidatedAfter);
    applyResults.openingsRowsTouched = allRows
      .filter((r) => r.slotKey === OPENINGS_SLOT)
      .map((r) => ({
        recordId: r.recordId,
        externalDisplayStatus: r.externalDisplayStatus,
        stillQuarantined: r.quarantined,
      }));
  } else if (apply) {
    applyResults.blocked = true;
    applyResults.blockers = applyBlockers;
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  const brandApiAfter = canApply ? await fetchBrandApiShape(target.recordId) : brandApiBefore;
  const apiBlocksAfter = apiBlocksForSlots(brandApiAfter, SCENARIO_SLOTS);

  const finalQaBefore = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.slug,
  }).catch((err) => ({ error: err.message }));
  const completeBefore = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: target.slug,
    targetQuality: "active-profile",
    dryRun: true,
  }).catch((err) => ({ error: err.message }));

  const expectedUiResult = {
    before: {
      scenarioBlocksInApi: apiBlocksBefore.length,
      rendersBlankPlaceholders: apiBlocksBefore.length === 0,
    },
    afterApply: {
      scenarioBlocksInApi: canApply ? 3 : proposedPresentationUpdates.length,
      allThreeWithImageUrl: overviewScenarioDiagnosis.filter((d) => d.safeToReactivate && d.hasImageAttachment)
        .length,
      rendersLikeRadisson:
        "Three scenario-card--visual cards with title, body, and imageUrl — same atelier component as Radisson by Choice",
      noBlankImagePlaceholder: true,
    },
    reference: radissonReference,
  };

  const expectedActiveProfileResult = {
    note: "Scenarios become visible in Explorer draft after reactivation, but images remain Pending Review — active-profile evidence still requires founder registry approval (not done in v31H-R1).",
    pendingImageReview: true,
    openingsStillQuarantined: allRows.filter((r) => r.slotKey === OPENINGS_SLOT && r.quarantined).length,
    galleryPendingApproval: "unchanged — v31J gallery still pending founder review",
    completeBuildBefore:
      (completeBefore?.brandReports || []).find((b) => b.slug === target.slug)?.readiness || null,
    finalQaBefore: finalQaBefore?.brandReports?.[0]?.scores || null,
  };

  const report = {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: apply && canApply ? "apply" : "dry-run",
    v31hR1WriterExists: v31hR1WriterExists(),
    targetBrand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched,
    airtableModified,
    imagesApproved: false,
    overviewScenarioDiagnosis,
    rowsToReactivate,
    registryRecordsToCreate: proposedRegistryCreates.map((c) => ({
      slotKey: c.slotKey,
      presentationRecordId: c.presentationRecordId,
      assetName: c.fields[MAP_BRAND_ASSET.assetName],
      recommendedExplorerSlot: c.fields[MAP_BRAND_ASSET.recommendedExplorerSlot],
    })),
    registryRecordsToUpdate: proposedRegistryUpdates.map((u) => ({
      registryRecordId: u.registryRecordId,
      slotKey: u.slotKey,
      presentationRecordId: u.presentationRecordId,
    })),
    proposedPresentationUpdates,
    proposedRegistryCreates,
    proposedRegistryUpdates,
    openingsUntouched: {
      quarantinedOpeningsCount: allRows.filter((r) => r.slotKey === OPENINGS_SLOT && r.quarantined)
        .length,
      visibleOpeningsCount: openingsTouchedCheck.length,
      guarantee: "No footprint.openings rows in proposed updates",
    },
    apiComparison: {
      before: apiBlocksBefore.map((b) => ({
        recordId: b.recordId,
        slotKey: b.slotKey,
        imageUrl: Boolean(b.imageUrl),
      })),
      afterProjected: rowsToReactivate.map((r) => ({
        recordId: r.recordId,
        slotKey: r.slotKey,
        inApiAfterApply: true,
      })),
      actualAfterApply: apiBlocksAfter.map((b) => ({
        recordId: b.recordId,
        slotKey: b.slotKey,
        imageUrl: Boolean(b.imageUrl),
      })),
    },
    applyBlockers,
    dryRunClean,
    canApply,
    applyResults,
    expectedUiResult,
    expectedActiveProfileResult,
    exactApplyCommand: dryRunClean && hasWork ? buildApplyCommand({ brand: target.slug }) : null,
  };

  report.markdown = buildMarkdownReport(report);
  return report;
}

function buildMarkdownReport(report) {
  const lines = [
    `# Brand Explorer Radisson Individuals Overview Scenario Reactivation v31H-R1`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Brand: **${report.targetBrand.name}**`,
    `- v31H-R1 exists: **${report.v31hR1WriterExists ? "yes" : "no"}**`,
    `- Mode: **${report.mode}**`,
    `- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`,
    `- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    `- Images approved: **no**`,
    "",
    "## Overview scenario diagnosis",
    "",
  ];

  for (const d of report.overviewScenarioDiagnosis) {
    lines.push(`### ${d.slotKey} — ${d.title}`);
    lines.push(`- Record: \`${d.recordId}\``);
    lines.push(`- Quarantined: ${d.quarantined} · Image: ${d.hasImageAttachment}`);
    lines.push(`- Owner-facing copy: ${d.ownerFacingCopy ? "yes" : "no"}`);
    lines.push(`- Safe to reactivate: **${d.safeToReactivate ? "yes" : "no"}**`);
    if (d.blockers.length) lines.push(`- Blockers: ${d.blockers.join(", ")}`);
    if (d.hiddenReason) lines.push(`- Hidden reason: ${d.hiddenReason}`);
    lines.push("");
  }

  lines.push("## Rows to reactivate", "");
  if (!report.rowsToReactivate.length) lines.push("- None eligible");
  else {
    for (const r of report.rowsToReactivate) {
      lines.push(`- \`${r.recordId}\` ${r.slotKey} — ${r.title}`);
    }
  }

  lines.push("", "## Registry plan", "");
  lines.push(`- Create: ${report.registryRecordsToCreate.length}`);
  lines.push(`- Update: ${report.registryRecordsToUpdate.length}`);

  lines.push("", "## Expected UI result", "");
  lines.push(
    `- After apply: ${report.expectedUiResult.afterApply.scenarioBlocksInApi} scenario blocks in API with images`
  );
  lines.push(`- ${report.expectedUiResult.afterApply.rendersLikeRadisson}`);

  lines.push("", "## Expected active-profile result", "");
  lines.push(`- ${report.expectedActiveProfileResult.note}`);

  if (report.exactApplyCommand) {
    lines.push("", "## Apply command", "", "```bash", report.exactApplyCommand, "```");
  }
  if (report.applyBlockers.length) {
    lines.push("", "## Apply blockers", "");
    for (const b of report.applyBlockers) lines.push(`- ${b}`);
  }

  return lines.join("\n");
}
