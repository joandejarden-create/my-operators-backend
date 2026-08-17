/**
 * Explorer Media Promotion Writer v7.
 */
import {
  BRAND_ASSET_PILOT_CONFIG,
  MAP_BRAND_ASSET,
} from "./brand-asset-registry-workflow.js";
import {
  VISUAL_SLOT,
  MAP_VISUAL_SLOT,
  mapRecordToVisualSlot,
  listRegistryRecordsRaw,
} from "./brand-explorer-visual-slot-requirements.js";
import { isFormallyApprovedRecord } from "./brand-asset-review-decision-writer.js";

export const WRITER_VERSION = "7";
export const REPORT_JSON_NAME = "explorer-media-promotion-writer.json";
export const REPORT_MD_NAME = "explorer-media-promotion-writer.md";
const DEFAULT_BRAND_KEY = "tribute-portfolio";
const DEFAULT_BRAND_RECORD_ID = "recCvV0PuZOi8c3hC";
const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const DISALLOWED_VALIDATION_STATUSES = new Set([
  "Mock/Demo Guard",
  "Provenance Only",
  "Do Not Use",
  "Not Enough Context",
]);

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}
function escapeFormulaValue(v) {
  return String(v).replace(/'/g, "\\'");
}
function todayIso() {
  return new Date().toISOString().slice(0, 10);
}
function firstAttachmentUrl(value) {
  if (!Array.isArray(value)) return "";
  for (const item of value) {
    if (item && typeof item.url === "string" && item.url.trim()) return item.url.trim();
    if (item?.thumbnails?.large?.url) return String(item.thumbnails.large.url).trim();
  }
  return "";
}
function isAttachmentArray(value) {
  return Array.isArray(value) && value.length > 0 && typeof value[0] === "object";
}
function isBlankMedia(value) {
  if (value == null) return true;
  if (Array.isArray(value)) return value.length === 0 || !firstAttachmentUrl(value);
  if (typeof value === "string") return !value.trim();
  return false;
}
function looksMockLike(value) {
  const hay = Array.isArray(value)
    ? value.map((v) => `${v.filename || ""} ${v.url || ""}`).join(" ")
    : String(value || "");
  return /mock|demo|placeholder|sample|unverified/i.test(hay);
}
function buildAttachmentPayload(record) {
  const existingAttachmentUrl = firstAttachmentUrl(record.attachment);
  const sourceUrl = nz(record.sourceUrl);
  const resolvedUrl = existingAttachmentUrl || sourceUrl;
  if (!resolvedUrl) return [];
  const local = nz(record.localFilePath);
  const filename = local.includes("/") ? local.split("/").pop() : `asset-${record.id}`;
  return [{ url: resolvedUrl, filename }];
}
function valueDriverLabel(record) {
  const explicit = nz(record.relatedValueDriver);
  if (explicit && explicit !== "None") {
    const low = explicit.toLowerCase();
    if (low === "urban") return "Urban";
    if (low === "resort") return "Resort";
    if (low.includes("conversion")) return "Conversion / Adaptive Reuse";
    if (low.includes("boutique")) return "Boutique / Lifestyle";
    if (low.includes("mixed")) return "Mixed-Use";
    return explicit;
  }
  const hay = `${record.assetName} ${record.relatedPropertyName}`.toLowerCase();
  if (/resort|beach|cove|island|nizuc|holbox/.test(hay)) return "Resort";
  if (/humano|lima|urban|city|medellin|rumbao/.test(hay)) return "Urban";
  if (/conversion|adaptive|ermita|cartagena|heritage|colonial/.test(hay)) return "Conversion / Adaptive Reuse";
  return "Urban";
}
function classifyCurrentMediaState(value, verificationField = "") {
  if (isBlankMedia(value)) return "blank";
  if (looksMockLike(value) || /mock|demo|unverified/i.test(nz(verificationField))) return "mock-or-unverified";
  return "populated";
}
function shouldBlockOverwrite(currentState, allowFlag) {
  return currentState === "populated" && !allowFlag;
}

function apiUrl(baseId, tableName, recordId = "") {
  const encodedTable = encodeURIComponent(tableName);
  const base = `https://api.airtable.com/v0/${baseId}/${encodedTable}`;
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
async function listByFormula(baseId, apiKey, tableName, formula) {
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    params.set("filterByFormula", formula);
    if (offset) params.set("offset", offset);
    const res = await fetch(`${apiUrl(baseId, tableName)}?${params.toString()}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `List failed ${tableName}: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records;
}
async function getBaseSchema(baseId, apiKey) {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Meta tables failed: ${res.status}`);
  return json.tables || [];
}
function detectFieldByNamesOrRegex(fields, preferredNames = [], regex = null, typeAllow = null) {
  const byName = new Map(fields.map((f) => [f.name, f]));
  for (const name of preferredNames) {
    const found = byName.get(name);
    if (!found) continue;
    if (typeAllow && !typeAllow.has(found.type)) continue;
    return found;
  }
  if (regex) {
    for (const f of fields) {
      if (!regex.test(f.name)) continue;
      if (typeAllow && !typeAllow.has(f.type)) continue;
      return f;
    }
  }
  return null;
}
function normalizeRegistryRecord(rawRecord) {
  const f = rawRecord.fields || {};
  return {
    id: rawRecord.id,
    assetName: nz(f[MAP_BRAND_ASSET.assetName]),
    assetType: nz(f[MAP_BRAND_ASSET.assetType]),
    assetStatus: nz(f[MAP_BRAND_ASSET.assetStatus]),
    explorerUsePermission: nz(f[MAP_BRAND_ASSET.explorerUsePermission]),
    usageReviewStatus: nz(f[MAP_BRAND_ASSET.usageReviewStatus]),
    sourceUrl: nz(f[MAP_BRAND_ASSET.sourceUrl]),
    sourcePageUrl: nz(f[MAP_BRAND_ASSET.sourcePageUrl]),
    localFilePath: nz(f[MAP_BRAND_ASSET.localFilePath]),
    recommendedExplorerSlot: nz(f[MAP_BRAND_ASSET.recommendedExplorerSlot]),
    reviewNotes: nz(f[MAP_BRAND_ASSET.reviewNotes]),
    attachment: f[MAP_BRAND_ASSET.attachment],
    companyValidated: f[MAP_BRAND_ASSET.companyValidated],
    companyValidationDate: f[MAP_BRAND_ASSET.companyValidationDate],
    validationStatus: nz(f[MAP_VISUAL_SLOT.validationStatus]),
    relatedPropertyName: nz(f[MAP_VISUAL_SLOT.relatedPropertyName]),
    relatedValueDriver: nz(f[MAP_VISUAL_SLOT.relatedValueDriver]),
    mappedVisualSlot: mapRecordToVisualSlot({
      assetName: nz(f[MAP_BRAND_ASSET.assetName]),
      assetType: nz(f[MAP_BRAND_ASSET.assetType]),
      recommendedExplorerSlot: nz(f[MAP_BRAND_ASSET.recommendedExplorerSlot]),
      sourceUrl: nz(f[MAP_BRAND_ASSET.sourceUrl]),
      sourcePageUrl: nz(f[MAP_BRAND_ASSET.sourcePageUrl]),
    }),
  };
}
function evaluateEligibility(record) {
  const reasons = [];
  if (!isFormallyApprovedRecord(record)) reasons.push("Not formally approved");
  if (!record.sourceUrl) reasons.push("Missing Source URL");
  if (!record.recommendedExplorerSlot) reasons.push("Missing Recommended Explorer Slot");
  if (!/Approved after human source\/visual review by/i.test(record.reviewNotes || "")) {
    reasons.push("Missing human approval review note stamp");
  }
  if (!(isAttachmentArray(record.attachment) || nz(record.localFilePath))) {
    reasons.push("Missing Attachment and Local File Path");
  }
  if (DISALLOWED_VALIDATION_STATUSES.has(record.validationStatus)) {
    reasons.push(`Disallowed Visual Slot Validation Status: ${record.validationStatus}`);
  }
  if (
    record.mappedVisualSlot === VISUAL_SLOT.RECENT_OPENINGS ||
    record.mappedVisualSlot === VISUAL_SLOT.PR_LINK ||
    record.mappedVisualSlot === VISUAL_SLOT.BRAND_STANDARDS
  ) reasons.push(`Slot ${record.mappedVisualSlot} is not promotable`);
  if (record.companyValidated || record.companyValidationDate) {
    reasons.push("Company validation fields present");
  }
  return { eligible: reasons.length === 0, reasons };
}
function pickSlotAssets(eligibleRecords) {
  const out = { logo: null, hero: null, gallery: {}, valueDrivers: {} };
  for (const r of eligibleRecords) {
    const s = nz(r.recommendedExplorerSlot);
    if (s === "Brand Setup — Logo") out.logo = r;
    if (s === "Brand Setup — Explorer Hero") out.hero = r;
    const g = s.match(/^materials\.gallery\.(\d)$/);
    if (g) out.gallery[g[1]] = r;
    if (r.mappedVisualSlot === VISUAL_SLOT.VALUE_DRIVER || s === "overview.why_value") {
      out.valueDrivers[valueDriverLabel(r)] = r;
    }
  }
  return out;
}
function buildPresentationSlotMap(records) {
  const out = new Map();
  for (const rec of records) {
    const slotKey = nz(rec.fields?.["Slot Key"] || rec.fields?.slot_key);
    if (!slotKey) continue;
    if (!out.has(slotKey)) out.set(slotKey, []);
    out.get(slotKey).push(rec);
  }
  return out;
}
function planPresentationUpsert({
  slotKey, imageFieldName, brandLinkFieldName, brandNameFieldExists, brandRecordId, brandName,
  sourceRecord, existingRows, allowOverwrite, imageOnlyPatch = false,
}) {
  const attachmentPayload = buildAttachmentPayload(sourceRecord);
  if (!attachmentPayload.length) return { blocked: true, reason: "No attachment payload source URL available" };
  const existing = existingRows[0] || null;
  if (!existing) {
    const fields = {
      [brandLinkFieldName]: [brandRecordId],
      "Slot Key": slotKey,
      [imageFieldName]: attachmentPayload,
      Active: true,
      Body: nz(sourceRecord.assetName),
      Title: nz(sourceRecord.relatedPropertyName) || nz(sourceRecord.assetName),
      "Sort Order": 0,
    };
    if (brandNameFieldExists) fields["Brand Name"] = brandName;
    return { blocked: false, action: "create", recordId: null, fields, overwriteRisk: false };
  }
  const currentState = classifyCurrentMediaState(existing.fields?.[imageFieldName]);
  if (shouldBlockOverwrite(currentState, allowOverwrite)) {
    return { blocked: true, reason: `Slot ${slotKey} already has nonblank image`, currentState };
  }
  return {
    blocked: false,
    action: "update",
    recordId: existing.id,
    currentState,
    overwriteRisk: currentState === "populated",
    fields: imageOnlyPatch
      ? {
          [imageFieldName]: attachmentPayload,
        }
      : {
          [imageFieldName]: attachmentPayload,
          Active: true,
          Body: nz(sourceRecord.assetName),
          Title: nz(sourceRecord.relatedPropertyName) || nz(sourceRecord.assetName),
        },
  };
}

export async function buildExplorerMediaPromotionWriterReport({
  brandKey = DEFAULT_BRAND_KEY,
  brandRecordId = DEFAULT_BRAND_RECORD_ID,
  apply = false,
  applyApproved = false,
  allowLogoOverwrite = false,
  allowNonblankHeroOverwrite = false,
  allowPresentationSlotOverwrite = false,
  allowPresentationSlotImagePatch = false,
} = {}) {
  const pilot = BRAND_ASSET_PILOT_CONFIG[brandKey] || BRAND_ASSET_PILOT_CONFIG[DEFAULT_BRAND_KEY];
  const resolvedBrandRecordId = pilot?.recordId || brandRecordId;
  const brandName = pilot?.brandName || "Tribute Portfolio";
  const mode = apply && applyApproved ? "promotion-apply" : "dry-run";
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const tables = await getBaseSchema(baseId, apiKey);
  const brandBasicsSchema = tables.find((t) => t.name === BRAND_BASICS_TABLE);
  const presentationSchema = tables.find((t) => t.name === PRESENTATION_TABLE);
  if (!brandBasicsSchema || !presentationSchema) throw new Error("Required Airtable table schema missing");

  const brandBasicsFields = brandBasicsSchema.fields || [];
  const presentationFields = presentationSchema.fields || [];
  const logoField = detectFieldByNamesOrRegex(brandBasicsFields, ["Logo", "Brand Logo"], /logo/i, new Set(["multipleAttachments", "url"]));
  const heroField = detectFieldByNamesOrRegex(
    brandBasicsFields,
    ["Explorer Hero", "Explorer Hero Image", "Hero Image", "Hero"],
    /hero/i,
    new Set(["multipleAttachments", "url"])
  );
  const heroDataSourceField = detectFieldByNamesOrRegex(brandBasicsFields, ["Explorer Hero Data Source"], /hero data source/i);
  const heroVerificationField = detectFieldByNamesOrRegex(brandBasicsFields, ["Explorer Hero Verification"], /hero verification/i);
  const brandLinkField = detectFieldByNamesOrRegex(presentationFields, ["Brand"], /^Brand$/i, new Set(["multipleRecordLinks"]));
  const presentationImageField = detectFieldByNamesOrRegex(presentationFields, ["Image", "Images", "Scenario Image", "Attachments"], /image|attachment|photo/i, new Set(["multipleAttachments", "url"]));
  const brandNameFieldExists = presentationFields.some((f) => f.name === "Brand Name");

  if (!logoField || !brandLinkField || !presentationImageField) {
    throw new Error("Unable to detect required Brand Basics/Presentation media fields");
  }

  const brandBasicsRecord = (await airtableFetch(baseId, apiKey, BRAND_BASICS_TABLE, { method: "GET" }, resolvedBrandRecordId)).json;
  if (!brandBasicsRecord?.id) throw new Error(`Brand Basics record not found: ${resolvedBrandRecordId}`);
  const presentationRecords = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(resolvedBrandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`
  );
  const presentationBySlot = buildPresentationSlotMap(presentationRecords);

  const rawRegistry = await listRegistryRecordsRaw(resolvedBrandRecordId);
  const registryRecords = rawRegistry.map(normalizeRegistryRecord);
  const eligibleAssets = [];
  const ineligibleAssets = [];
  for (const r of registryRecords) {
    const e = evaluateEligibility(r);
    if (e.eligible) eligibleAssets.push(r);
    else ineligibleAssets.push({ recordId: r.id, assetName: r.assetName, reasons: e.reasons });
  }
  const slotAssets = pickSlotAssets(eligibleAssets);
  const brandFields = brandBasicsRecord.fields || {};
  const currentBrandSetupMediaState = {
    logoField: logoField.name,
    logoValueState: classifyCurrentMediaState(brandFields[logoField.name]),
    logoCurrentUrl: firstAttachmentUrl(brandFields[logoField.name]) || nz(brandFields[logoField.name]),
    heroField: heroField?.name || null,
    heroValueState: heroField
      ? classifyCurrentMediaState(
          brandFields[heroField.name],
          heroVerificationField ? brandFields[heroVerificationField.name] : ""
        )
      : "schema-not-found",
    heroCurrentUrl: heroField
      ? firstAttachmentUrl(brandFields[heroField.name]) || nz(brandFields[heroField.name])
      : "",
    heroVerificationField: heroVerificationField?.name || null,
    heroVerificationValue: heroVerificationField ? nz(brandFields[heroVerificationField.name]) : "",
    heroDataSourceField: heroDataSourceField?.name || null,
    heroDataSourceValue: heroDataSourceField ? nz(brandFields[heroDataSourceField.name]) : "",
  };
  const currentExplorerPresentationMediaState = {
    slotCount: presentationBySlot.size,
    slots: [...presentationBySlot.entries()].map(([slotKey, rows]) => ({
      slotKey,
      rowCount: rows.length,
      imageState: classifyCurrentMediaState(rows[0]?.fields?.[presentationImageField.name]),
      imageUrl: firstAttachmentUrl(rows[0]?.fields?.[presentationImageField.name]),
      title: nz(rows[0]?.fields?.Title),
      body: nz(rows[0]?.fields?.Body),
    })),
  };

  const proposedWrites = { brandBasicsUpdates: [], presentationCreates: [], presentationUpdates: [] };
  const overwriteRisks = [];
  const applyBlockers = [];

  if (slotAssets.logo) {
    const currentState = currentBrandSetupMediaState.logoValueState;
    if (shouldBlockOverwrite(currentState, allowLogoOverwrite)) {
      applyBlockers.push(`Logo field ${logoField.name} already populated; use --allow-logo-overwrite`);
    } else {
      if (currentState === "populated") overwriteRisks.push(`Logo overwrite on ${logoField.name}`);
      proposedWrites.brandBasicsUpdates.push({
        recordId: brandBasicsRecord.id,
        field: logoField.name,
        value: buildAttachmentPayload(slotAssets.logo),
        sourceRecordId: slotAssets.logo.id,
      });
    }
  } else applyBlockers.push("No eligible approved logo asset found");

  if (slotAssets.hero) {
    if (heroField) {
      const currentState = currentBrandSetupMediaState.heroValueState;
      const nonblankAndNotMock = currentState === "populated";
      if (nonblankAndNotMock && !allowNonblankHeroOverwrite) {
        applyBlockers.push(`Hero field ${heroField.name} already populated; use --allow-nonblank-hero-overwrite`);
      } else {
        if (nonblankAndNotMock) overwriteRisks.push(`Hero overwrite on ${heroField.name}`);
        proposedWrites.brandBasicsUpdates.push({
          recordId: brandBasicsRecord.id,
          field: heroField.name,
          value: buildAttachmentPayload(slotAssets.hero),
          sourceRecordId: slotAssets.hero.id,
        });
      }
    } else {
      const plan = planPresentationUpsert({
        slotKey: "overview.hero",
        imageFieldName: presentationImageField.name,
        brandLinkFieldName: brandLinkField.name,
        brandNameFieldExists,
        brandRecordId: resolvedBrandRecordId,
        brandName,
        sourceRecord: slotAssets.hero,
        existingRows: presentationBySlot.get("overview.hero") || [],
        allowOverwrite: allowNonblankHeroOverwrite || allowPresentationSlotOverwrite,
        imageOnlyPatch: allowPresentationSlotImagePatch,
      });
      if (plan.blocked) {
        applyBlockers.push(`Hero slot blocked: ${plan.reason}`);
      } else if (plan.action === "create") {
        proposedWrites.presentationCreates.push({
          slotKey: "overview.hero",
          fields: plan.fields,
          sourceRecordId: slotAssets.hero.id,
        });
      } else {
        proposedWrites.presentationUpdates.push({
          slotKey: "overview.hero",
          recordId: plan.recordId,
          fields: plan.fields,
          sourceRecordId: slotAssets.hero.id,
        });
      }
    }
    if (heroDataSourceField) {
      proposedWrites.brandBasicsUpdates.push({
        recordId: brandBasicsRecord.id,
        field: heroDataSourceField.name,
        value: "Brand Asset Registry v7 promotion writer",
        sourceRecordId: slotAssets.hero.id,
      });
    }
    if (heroVerificationField) {
      proposedWrites.brandBasicsUpdates.push({
        recordId: brandBasicsRecord.id,
        field: heroVerificationField.name,
        value: "Source-backed / human-approved registry asset",
        sourceRecordId: slotAssets.hero.id,
      });
    }
  } else {
    applyBlockers.push("No eligible approved hero asset found");
  }

  for (const i of ["1", "2", "4", "5", "6"]) {
    const src = slotAssets.gallery[i];
    const slotKey = `materials.gallery.${i}`;
    if (!src) {
      applyBlockers.push(`No eligible approved gallery asset for ${slotKey}`);
      continue;
    }
    const plan = planPresentationUpsert({
      slotKey,
      imageFieldName: presentationImageField.name,
      brandLinkFieldName: brandLinkField.name,
      brandNameFieldExists,
      brandRecordId: resolvedBrandRecordId,
      brandName,
      sourceRecord: src,
      existingRows: presentationBySlot.get(slotKey) || [],
      allowOverwrite: allowPresentationSlotOverwrite,
      imageOnlyPatch: allowPresentationSlotImagePatch,
    });
    if (plan.blocked) {
      applyBlockers.push(plan.reason);
      continue;
    }
    if (plan.overwriteRisk) overwriteRisks.push(`Presentation slot overwrite ${slotKey}`);
    if (plan.action === "create") proposedWrites.presentationCreates.push({ slotKey, fields: plan.fields, sourceRecordId: src.id });
    if (plan.action === "update") proposedWrites.presentationUpdates.push({ slotKey, recordId: plan.recordId, fields: plan.fields, sourceRecordId: src.id });
  }

  const valueDriverSlotMap = {
    Resort: "overview.scenario.1",
    Urban: "overview.scenario.2",
  };
  const fallbackValueDriverCandidate = (driver) => {
    return eligibleAssets.find((r) => {
      if (!(r.mappedVisualSlot === VISUAL_SLOT.VALUE_DRIVER || r.recommendedExplorerSlot === "overview.why_value")) {
        return false;
      }
      const name = `${r.assetName} ${r.relatedPropertyName}`.toLowerCase();
      if (driver === "Urban") return /urban|city|humano|lima|medellin|rumbao/.test(name);
      if (driver === "Resort") return /resort|beach|cove|island|nizuc|holbox/.test(name);
      return false;
    });
  };
  for (const [driver, slotKey] of Object.entries(valueDriverSlotMap)) {
    const src = slotAssets.valueDrivers[driver] || fallbackValueDriverCandidate(driver);
    if (!src) {
      applyBlockers.push(`No eligible approved value-driver asset for ${driver}`);
      continue;
    }
    const plan = planPresentationUpsert({
      slotKey,
      imageFieldName: presentationImageField.name,
      brandLinkFieldName: brandLinkField.name,
      brandNameFieldExists,
      brandRecordId: resolvedBrandRecordId,
      brandName,
      sourceRecord: src,
      existingRows: presentationBySlot.get(slotKey) || [],
      allowOverwrite: allowPresentationSlotOverwrite,
      imageOnlyPatch: allowPresentationSlotImagePatch,
    });
    if (plan.blocked) {
      applyBlockers.push(plan.reason);
      continue;
    }
    if (plan.overwriteRisk) overwriteRisks.push(`Presentation slot overwrite ${slotKey}`);
    if (plan.action === "create") proposedWrites.presentationCreates.push({ slotKey, fields: plan.fields, sourceRecordId: src.id });
    if (plan.action === "update") proposedWrites.presentationUpdates.push({ slotKey, recordId: plan.recordId, fields: plan.fields, sourceRecordId: src.id });
  }

  const slotsLeftUnchanged = [
    "materials.gallery.3",
    "overview.scenario.3",
    "footprint.openings",
    "PR / Opening Link",
    "Value Driver: Boutique / Lifestyle",
    "Value Driver: Mixed-Use",
    "Value Driver: Conversion / Adaptive Reuse",
  ];
  const applyMode = apply && applyApproved;
  let applyResult = { brandBasicsUpdated: [], presentationCreated: [], presentationUpdated: [], errors: [] };

  if (applyMode) {
    const byRecord = new Map();
    for (const u of proposedWrites.brandBasicsUpdates) {
      if (!byRecord.has(u.recordId)) byRecord.set(u.recordId, {});
      byRecord.get(u.recordId)[u.field] = u.value;
    }
    for (const [recordId, fields] of byRecord.entries()) {
      const { res, json } = await airtableFetch(baseId, apiKey, BRAND_BASICS_TABLE, {
        method: "PATCH",
        body: JSON.stringify({ fields, typecast: true }),
      }, recordId);
      if (!res.ok) {
        applyResult.errors.push(json.error?.message || `Brand Basics patch failed ${recordId}`);
      } else {
        applyResult.brandBasicsUpdated.push({ recordId, fields: Object.keys(fields) });
      }
    }
    for (const c of proposedWrites.presentationCreates) {
      const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
        method: "POST",
        body: JSON.stringify({ fields: c.fields, typecast: true }),
      });
      if (!res.ok) applyResult.errors.push(json.error?.message || `Presentation create failed ${c.slotKey}`);
      else applyResult.presentationCreated.push({ slotKey: c.slotKey, recordId: json.id });
    }
    for (const u of proposedWrites.presentationUpdates) {
      const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
        method: "PATCH",
        body: JSON.stringify({ fields: u.fields, typecast: true }),
      }, u.recordId);
      if (!res.ok) applyResult.errors.push(json.error?.message || `Presentation update failed ${u.slotKey}`);
      else applyResult.presentationUpdated.push({ slotKey: u.slotKey, recordId: u.recordId });
    }
  }

  return {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode,
    filesRead: [
      "AGENTS.md",
      "reports/brand-asset-download-attachment-writer.md",
      "reports/brand-asset-download-attachment-writer.json",
      "reports/brand-asset-review-decision-writer.md",
      "reports/brand-asset-human-review-readiness.md",
      "reports/tribute-visual-asset-slot-review.md",
      "reports/tribute-portfolio-package-pipeline.md",
      "lib/partner-intelligence/brand-asset-download-attachment-writer.js",
      "lib/partner-intelligence/brand-asset-review-decision-writer.js",
      "lib/partner-intelligence/brand-asset-human-review-readiness.js",
      "lib/partner-intelligence/tribute-visual-asset-slot-review.js",
      "api/brand-library.js",
      "public/js/brand-explorer-atelier-from-api.js",
    ],
    brand: { key: brandKey, recordId: resolvedBrandRecordId, name: brandName, textGovernancePlatformReady: true },
    schemaInspection: {
      brandBasicsTable: BRAND_BASICS_TABLE,
      presentationTable: PRESENTATION_TABLE,
      detectedFields: {
        logoField: logoField.name,
        heroField: heroField?.name || null,
        heroDataSourceField: heroDataSourceField?.name || null,
        heroVerificationField: heroVerificationField?.name || null,
        presentationBrandLinkField: brandLinkField.name,
        presentationImageField: presentationImageField.name,
      },
    },
    totalRegistryRecordsScanned: registryRecords.length,
    approvedRegistryAssetsAvailable: registryRecords.filter(isFormallyApprovedRecord).map((r) => ({
      recordId: r.id,
      assetName: r.assetName,
      recommendedExplorerSlot: r.recommendedExplorerSlot,
    })),
    eligibleAssetsForPromotion: eligibleAssets.map((r) => ({
      recordId: r.id,
      assetName: r.assetName,
      recommendedExplorerSlot: r.recommendedExplorerSlot,
    })),
    ineligibleAssets: ineligibleAssets,
    currentBrandSetupMediaState,
    currentExplorerPresentationMediaState,
    proposedLogoPromotion: slotAssets.logo
      ? { sourceRecordId: slotAssets.logo.id, targetField: logoField.name, blocked: applyBlockers.some((x) => /Logo field/.test(x)) }
      : null,
    proposedHeroPromotion: slotAssets.hero
      ? {
          sourceRecordId: slotAssets.hero.id,
          targetField: heroField?.name || "overview.hero (presentation slot)",
          targetVerificationField: heroVerificationField?.name || null,
          targetDataSourceField: heroDataSourceField?.name || null,
          blocked: applyBlockers.some((x) => /Hero field/.test(x)),
        }
      : null,
    proposedGalleryPromotions: ["1", "2", "4", "5", "6"].map((i) => ({
      slotKey: `materials.gallery.${i}`,
      sourceRecordId: slotAssets.gallery[i]?.id || null,
      proposedAction:
        proposedWrites.presentationCreates.find((x) => x.slotKey === `materials.gallery.${i}`)?.slotKey
          ? "create"
          : proposedWrites.presentationUpdates.find((x) => x.slotKey === `materials.gallery.${i}`)?.slotKey
            ? "update"
            : "none",
    })),
    proposedValueDriverPromotions: ["Resort", "Urban"].map((driver) => {
      const slotKey = driver === "Resort" ? "overview.scenario.1" : "overview.scenario.2";
      const planned = proposedWrites.presentationCreates.find((x) => x.slotKey === slotKey)
        || proposedWrites.presentationUpdates.find((x) => x.slotKey === slotKey);
      return {
        driver,
        slotKey,
        sourceRecordId: planned?.sourceRecordId || null,
        proposedAction: planned
          ? proposedWrites.presentationCreates.find((x) => x.slotKey === slotKey) ? "create" : "update"
          : "none",
      };
    }),
    slotsIntentionallyUnchanged: slotsLeftUnchanged,
    exactAirtableUpdatesProposed: proposedWrites,
    overwriteRisks: overwriteRisks,
    applyBlockers: applyBlockers,
    applyFlags: {
      applyRequested: apply,
      applyApproved,
      allowLogoOverwrite,
      allowNonblankHeroOverwrite,
      allowPresentationSlotOverwrite,
      allowPresentationSlotImagePatch,
    },
    applyResult,
    airtableModified: applyMode && applyResult.errors.length === 0 && (
      applyResult.brandBasicsUpdated.length + applyResult.presentationCreated.length + applyResult.presentationUpdated.length > 0
    ),
    brandSetupMediaFieldsTouched: applyMode ? applyResult.brandBasicsUpdated.length > 0 : false,
    explorerPresentationRecordsTouched:
      applyMode ? (applyResult.presentationCreated.length + applyResult.presentationUpdated.length > 0) : false,
    companyValidatedFieldsUntouched: true,
    exactApplyCommand:
      allowPresentationSlotImagePatch
        ? "npm run explorer-media-promotion-writer -- --brand tribute-portfolio --apply --approve-explorer-media-promotion --allow-presentation-slot-image-patch"
        : "npm run explorer-media-promotion-writer -- --brand tribute-portfolio --apply --approve-explorer-media-promotion",
    remainingWorkAfterPromotion: [
      "Approve and stage Gallery 3 before filling materials.gallery.3.",
      "Approve and stage Conversion value-driver visual before filling overview.scenario.3.",
      "Source Recent Openings property + PR/date evidence before footprint.openings media.",
      "Source Boutique / Lifestyle and Mixed-Use value-driver assets.",
      "PR / Opening Link remains provenance-only until rendered source capture.",
    ],
  };
}

export function buildExplorerMediaPromotionWriterMarkdown(report) {
  const lines = [];
  lines.push("# Explorer Media Promotion Writer v7");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push("");
  lines.push("## Summary");
  lines.push("");
  lines.push(`- Registry records scanned: **${report.totalRegistryRecordsScanned}**`);
  lines.push(`- Formally approved assets available: **${report.approvedRegistryAssetsAvailable.length}**`);
  lines.push(`- Eligible assets for promotion: **${report.eligibleAssetsForPromotion.length}**`);
  lines.push(`- Ineligible assets: **${report.ineligibleAssets.length}**`);
  lines.push(`- Brand Setup media fields touched: **${report.brandSetupMediaFieldsTouched ? "yes" : "no"}**`);
  lines.push(`- Explorer presentation records touched: **${report.explorerPresentationRecordsTouched ? "yes" : "no"}**`);
  lines.push(`- Company Validated fields untouched: **${report.companyValidatedFieldsUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Current Brand Setup Media State");
  lines.push("");
  lines.push(`- Logo field: \`${report.currentBrandSetupMediaState.logoField}\` (${report.currentBrandSetupMediaState.logoValueState})`);
  lines.push(`- Hero field: \`${report.currentBrandSetupMediaState.heroField}\` (${report.currentBrandSetupMediaState.heroValueState})`);
  lines.push(`- Hero verification field: \`${report.currentBrandSetupMediaState.heroVerificationField || "(none)"}\``);
  lines.push(`- Hero data-source field: \`${report.currentBrandSetupMediaState.heroDataSourceField || "(none)"}\``);
  lines.push("");
  lines.push("## Proposed Promotions");
  lines.push("");
  lines.push(`- Logo: ${report.proposedLogoPromotion ? `record \`${report.proposedLogoPromotion.sourceRecordId}\` -> \`${report.proposedLogoPromotion.targetField}\`` : "none"}`);
  lines.push(`- Hero: ${report.proposedHeroPromotion ? `record \`${report.proposedHeroPromotion.sourceRecordId}\` -> \`${report.proposedHeroPromotion.targetField}\`` : "none"}`);
  lines.push(`- Gallery slots: ${report.proposedGalleryPromotions.map((g) => `${g.slotKey}:${g.proposedAction}`).join(", ")}`);
  lines.push(`- Value drivers: ${report.proposedValueDriverPromotions.map((v) => `${v.driver}:${v.proposedAction}`).join(", ")}`);
  lines.push("");
  lines.push("## Slots Left Unchanged");
  lines.push("");
  for (const s of report.slotsIntentionallyUnchanged) lines.push(`- ${s}`);
  lines.push("");
  lines.push("## Apply Blockers / Overwrite Risks");
  lines.push("");
  if (!report.applyBlockers.length) lines.push("- None.");
  else for (const b of report.applyBlockers) lines.push(`- ${b}`);
  if (report.overwriteRisks.length) {
    lines.push("");
    lines.push("Overwrite risks:");
    for (const r of report.overwriteRisks) lines.push(`- ${r}`);
  }
  lines.push("");
  lines.push("## Apply Command");
  lines.push("");
  lines.push("```bash");
  lines.push(`${report.exactApplyCommand}`);
  lines.push("```");
  lines.push("");
  lines.push("Optional overwrite flags (only when intentionally needed):");
  lines.push("- --allow-logo-overwrite");
  lines.push("- --allow-nonblank-hero-overwrite");
  lines.push("- --allow-presentation-slot-overwrite");
  lines.push("- --allow-presentation-slot-image-patch");
  lines.push("");
  return lines.join("\n");
}
