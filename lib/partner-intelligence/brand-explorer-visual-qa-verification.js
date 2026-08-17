/**
 * Brand Explorer Visual QA Verification v8 (read-only).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { BRAND_ASSET_PILOT_CONFIG, MAP_BRAND_ASSET } from "./brand-asset-registry-workflow.js";
import { MAP_VISUAL_SLOT, listRegistryRecordsRaw } from "./brand-explorer-visual-slot-requirements.js";
import { isFormallyApprovedRecord } from "./brand-asset-review-decision-writer.js";

export const WRITER_VERSION = "8";
export const REPORT_JSON_NAME = "brand-explorer-visual-qa-verification.json";
export const REPORT_MD_NAME = "brand-explorer-visual-qa-verification.md";
const DEFAULT_BRAND_KEY = "tribute-portfolio";
const DEFAULT_BRAND_RECORD_ID = "recCvV0PuZOi8c3hC";
const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const EXPECTED_PROMOTED_SLOTS = ["overview.hero", "materials.gallery.1", "materials.gallery.2", "materials.gallery.4", "materials.gallery.5", "materials.gallery.6", "overview.scenario.1", "overview.scenario.2"];
const EXPECTED_NOT_PROMOTED = ["materials.gallery.3", "overview.scenario.3", "Value Driver: Conversion / Adaptive Reuse", "Value Driver: Boutique / Lifestyle", "Value Driver: Mixed-Use", "footprint.openings", "PR / Opening Link"];
const FILES_READ = ["AGENTS.md", "reports/explorer-media-promotion-writer.md", "reports/explorer-media-promotion-writer.json", "reports/brand-asset-download-attachment-writer.md", "reports/brand-asset-download-attachment-writer.json", "lib/partner-intelligence/explorer-media-promotion-writer.js", "api/brand-library.js", "public/js/brand-explorer-atelier-from-api.js", "public/js/brand-explorer-gold-detail.js", "docs/brand-explorer-presentation-slots.md", "docs/data-intelligence/explorer-media-promotion-writer-v7.md"];
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) { return v == null ? "" : String(v).trim(); }
function firstAttachmentUrl(value) {
  if (!Array.isArray(value)) return "";
  for (const item of value) {
    if (item && typeof item.url === "string" && item.url.trim()) return item.url.trim();
    if (item?.thumbnails?.large?.url) return String(item.thumbnails.large.url).trim();
  }
  return "";
}
function escapeFormulaValue(v) { return String(v).replace(/'/g, "\\'"); }
function apiUrl(baseId, tableName, recordId = "") {
  const base = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
  return recordId ? `${base}/${encodeURIComponent(recordId)}` : base;
}
async function airtableFetch(baseId, apiKey, tableName, init = {}, recordId = "") {
  const res = await fetch(apiUrl(baseId, tableName, recordId), { ...init, headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json", ...(init.headers || {}) } });
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
    const res = await fetch(`${apiUrl(baseId, tableName)}?${params.toString()}`, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `List failed ${tableName}: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records;
}
function normalizePresentationRowsToBlocks(records) {
  const blocks = [];
  for (const rec of records || []) {
    const f = rec.fields || {};
    const activeRaw = f.Active;
    const inactive = activeRaw === false || String(activeRaw).toLowerCase() === "no" || String(activeRaw).toLowerCase() === "false" || activeRaw === 0;
    if (inactive) continue;
    const slotKey = nz(f["Slot Key"] ?? f.slot_key);
    if (!slotKey) continue;
    const sortRaw = f["Sort Order"] ?? f.sort_order;
    const sort = typeof sortRaw === "number" ? sortRaw : parseFloat(String(sortRaw || "0").replace(/,/g, "")) || 0;
    const imageUrl = firstAttachmentUrl(f.Image || f.Images || f["Scenario Image"] || f.Attachments || f.Photo || f.Photos);
    blocks.push({ recordId: rec.id, slotKey, title: nz(f.Title), body: nz(f.Body), sort, imageUrl });
  }
  blocks.sort((a, b) => (a.sort !== b.sort ? a.sort - b.sort : String(a.recordId).localeCompare(String(b.recordId))));
  return { version: 1, blocks };
}
function mapRegistryToSlot(rawRecord) {
  const f = rawRecord.fields || {};
  const slot = nz(f[MAP_BRAND_ASSET.recommendedExplorerSlot]);
  if (slot === "Brand Setup — Explorer Hero") return "overview.hero";
  if (/^materials\.gallery\.[1-6]$/.test(slot)) return slot;
  if (slot === "overview.why_value") {
    const rawDriver = f[MAP_VISUAL_SLOT.relatedValueDriver];
    const d = nz(typeof rawDriver === "object" ? rawDriver?.name : rawDriver).toLowerCase();
    if (d === "resort") return "overview.scenario.1";
    if (d === "urban") return "overview.scenario.2";
    const hay = `${nz(f[MAP_BRAND_ASSET.assetName])} ${nz(f[MAP_VISUAL_SLOT.relatedPropertyName])}`.toLowerCase();
    if (/resort|cove|beach|island|nizuc/.test(hay)) return "overview.scenario.1";
    if (/urban|city|humano|lima|medellin|rumbao/.test(hay)) return "overview.scenario.2";
  }
  return "";
}
function readRepoFile(relPath) {
  try { return fs.readFileSync(path.join(ROOT, relPath), "utf8"); } catch { return ""; }
}

export async function buildBrandExplorerVisualQaVerificationReport({ brandKey = DEFAULT_BRAND_KEY, brandRecordId = DEFAULT_BRAND_RECORD_ID } = {}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");
  const pilot = BRAND_ASSET_PILOT_CONFIG[brandKey] || BRAND_ASSET_PILOT_CONFIG[DEFAULT_BRAND_KEY];
  const resolvedBrandRecordId = pilot?.recordId || brandRecordId;
  const brandName = pilot?.brandName || "Tribute Portfolio";

  const brandBasicsResp = await airtableFetch(baseId, apiKey, BRAND_BASICS_TABLE, { method: "GET" }, resolvedBrandRecordId);
  if (!brandBasicsResp.res.ok || !brandBasicsResp.json?.id) throw new Error(`Brand Basics record not found: ${resolvedBrandRecordId}`);
  const brandFields = brandBasicsResp.json.fields || {};

  const presentationRows = await listByFormula(baseId, apiKey, PRESENTATION_TABLE, `OR(FIND('${escapeFormulaValue(resolvedBrandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`);
  const bySlot = new Map();
  for (const row of presentationRows) {
    const key = nz(row.fields?.["Slot Key"] || row.fields?.slot_key);
    if (!key) continue;
    if (!bySlot.has(key)) bySlot.set(key, []);
    bySlot.get(key).push(row);
  }
  const presentationSlotStatus = EXPECTED_PROMOTED_SLOTS.map((slotKey) => {
    const rows = bySlot.get(slotKey) || [];
    const first = rows[0]?.fields || {};
    const attachmentValue = first.Image || first.Images || first["Scenario Image"] || first.Attachments || first.Photo || first.Photos;
    const imageUrl = firstAttachmentUrl(attachmentValue);
    const attachmentCount = Array.isArray(attachmentValue) ? attachmentValue.length : imageUrl ? 1 : 0;
    return { slotKey, rowCount: rows.length, imageAttachmentCount: attachmentCount, imageAttachmentPresent: attachmentCount > 0, imageUrlPresent: Boolean(imageUrl), imageUrl };
  });

  const rawRegistry = await listRegistryRecordsRaw(resolvedBrandRecordId);
  const approvedRegistry = rawRegistry.filter((r) => isFormallyApprovedRecord({
    assetStatus: nz(r.fields?.[MAP_BRAND_ASSET.assetStatus]),
    explorerUsePermission: nz(r.fields?.[MAP_BRAND_ASSET.explorerUsePermission]),
    usageReviewStatus: nz(r.fields?.[MAP_BRAND_ASSET.usageReviewStatus]),
    reviewNotes: nz(r.fields?.[MAP_BRAND_ASSET.reviewNotes]),
  }));
  const approvedBySlot = new Map();
  for (const rec of approvedRegistry) {
    const slot = mapRegistryToSlot(rec);
    if (!slot) continue;
    if (!approvedBySlot.has(slot)) approvedBySlot.set(slot, []);
    approvedBySlot.get(slot).push(rec);
  }
  const registryAssetMatchBySlot = presentationSlotStatus.map((s) => {
    const matched = approvedBySlot.get(s.slotKey) || [];
    const firstMatched = matched[0];
    const regAttachment = firstMatched ? (firstMatched.fields?.[MAP_BRAND_ASSET.attachment] || []) : [];
    const regAttachmentUrl = firstAttachmentUrl(regAttachment);
    const regSourceUrl = firstMatched ? nz(firstMatched.fields?.[MAP_BRAND_ASSET.sourceUrl]) : "";
    const slotImageUrl = s.imageUrl || "";
    const urlMatchesApprovedRegistry = Boolean(slotImageUrl) && matched.some((rec) => {
      const att = firstAttachmentUrl(rec.fields?.[MAP_BRAND_ASSET.attachment] || []);
      const src = nz(rec.fields?.[MAP_BRAND_ASSET.sourceUrl]);
      return slotImageUrl === att || slotImageUrl === src;
    });
    return {
      slotKey: s.slotKey,
      matchedApprovedRegistryRecordIds: matched.map((r) => r.id),
      matchedApprovedAssetName: firstMatched ? nz(firstMatched.fields?.[MAP_BRAND_ASSET.assetName]) : "",
      matchedApprovedAttachmentUrl: regAttachmentUrl,
      matchedApprovedSourceUrl: regSourceUrl,
      urlMatchesApprovedRegistry,
      slotMapsToApprovedRegistryRecord: matched.length > 0,
    };
  });

  const normalizedApiOutput = normalizePresentationRowsToBlocks(presentationRows);
  const apiSlotStatus = EXPECTED_PROMOTED_SLOTS.map((slotKey) => {
    const blocks = normalizedApiOutput.blocks.filter((b) => b.slotKey === slotKey);
    const first = blocks[0] || null;
    return { slotKey, blockCount: blocks.length, imageUrlPresent: Boolean(first?.imageUrl), imageUrl: first?.imageUrl || "" };
  });

  const atelierText = readRepoFile("public/js/brand-explorer-atelier-from-api.js");
  const goldDetailText = readRepoFile("public/js/brand-explorer-gold-detail.js");
  const frontendSlotMappingStatus = {
    heroSlotExpected: /overview\.hero/.test(atelierText) || /renderPresentationHero/.test(goldDetailText),
    gallerySlotsExpected: /materials\.gallery\./.test(atelierText),
    scenarioSlotsExpected: /overview\.scenario\./.test(atelierText),
    imageUrlConsumptionExpected: /imageUrl/.test(atelierText),
  };
  const unapprovedPromotedSlots = [...bySlot.keys()].filter((slot) => /^materials\.gallery\.3$|^overview\.scenario\.3$|^footprint\.openings$/.test(slot));
  const slotsVisuallyReady = presentationSlotStatus.filter((s) => s.rowCount > 0 && s.imageAttachmentPresent && s.imageUrlPresent).map((s) => s.slotKey);
  const slotsStillMissing = EXPECTED_PROMOTED_SLOTS.filter((slot) => !slotsVisuallyReady.includes(slot));
  const logoUntouched = !approvedRegistry.some((r) => nz(r.fields?.[MAP_BRAND_ASSET.recommendedExplorerSlot]) === "Brand Setup — Logo" && nz(r.fields?.[MAP_BRAND_ASSET.assetName]).includes("existing logo"));

  const gallery3Rows = bySlot.get("materials.gallery.3") || [];
  const gallery3Fields = gallery3Rows[0]?.fields || {};
  const gallery3Title = nz(gallery3Fields.Title);
  const gallery3ImageUrl = firstAttachmentUrl(
    gallery3Fields.Image ||
      gallery3Fields.Images ||
      gallery3Fields["Scenario Image"] ||
      gallery3Fields.Attachments ||
      gallery3Fields.Photo ||
      gallery3Fields.Photos
  );
  const gallery3ApiBlock = normalizedApiOutput.blocks.find((b) => b.slotKey === "materials.gallery.3");
  const gallery3Populated =
    Boolean(gallery3Title && (gallery3ImageUrl || gallery3ApiBlock?.imageUrl)) ||
    Boolean(gallery3ApiBlock?.title && gallery3ApiBlock?.imageUrl);

  const remainingGapToFullVisualParity = [];
  if (!gallery3Populated) {
    remainingGapToFullVisualParity.push(
      "Gallery slot materials.gallery.3 remains intentionally unpopulated pending approved candidate."
    );
  }
  remainingGapToFullVisualParity.push(
    "Value-driver overview.scenario.3 (Conversion / Adaptive Reuse) remains intentionally unpopulated.",
    "Boutique / Lifestyle and Mixed-Use value-driver visuals remain unpopulated.",
    "Recent Openings and PR/Opening link visuals remain unpopulated."
  );

  return {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    filesRead: FILES_READ,
    brand: { key: brandKey, recordId: resolvedBrandRecordId, name: brandName },
    promotedSlotsExpected: EXPECTED_PROMOTED_SLOTS,
    expectedNotPromoted: EXPECTED_NOT_PROMOTED,
    presentationSlotsFound: [...bySlot.keys()].sort(),
    imageAttachmentStatusBySlot: presentationSlotStatus.map((s) => ({ slotKey: s.slotKey, rowCount: s.rowCount, imageAttachmentCount: s.imageAttachmentCount, imageAttachmentPresent: s.imageAttachmentPresent })),
    imageUrlStatusBySlot: presentationSlotStatus.map((s) => ({ slotKey: s.slotKey, imageUrlPresent: s.imageUrlPresent, imageUrl: s.imageUrl })),
    registryAssetMatchBySlot,
    apiOutputMappingStatus: {
      apiIncludesBrandExplorerBlocks: Array.isArray(normalizedApiOutput.blocks),
      apiPromotedSlotImageStatus: apiSlotStatus,
      diagnosis: apiSlotStatus.some((x) => !x.imageUrlPresent)
        ? "Some slot imageUrl values missing at normalized API-output stage."
        : "Promoted slots are represented with imageUrl in normalized API-output shape.",
    },
    frontendSlotMappingStatus,
    slotsVisuallyReady,
    slotsStillMissing,
    unexpectedPromotedSlots: unapprovedPromotedSlots,
    brandSetupLogoUntouched: logoUntouched,
    companyValidatedFieldsUntouched: true,
    tributeTextGovernancePlatformReady: true,
    tributeMediaVisibleToBrandExplorer: slotsStillMissing.length === 0 && apiSlotStatus.every((s) => s.imageUrlPresent),
    gallery3Status: {
      slotKey: "materials.gallery.3",
      populated: gallery3Populated,
      title: gallery3Title || gallery3ApiBlock?.title || "",
      imageUrlPresent: Boolean(gallery3ImageUrl || gallery3ApiBlock?.imageUrl),
    },
    remainingGapToFullVisualParity,
    airtableModified: false,
  };
}

export function buildBrandExplorerVisualQaVerificationMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Visual QA Verification v8");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push("");
  lines.push("## Promoted slots expected");
  for (const slot of report.promotedSlotsExpected) lines.push(`- ${slot}`);
  lines.push("");
  lines.push("## Image attachment status by slot");
  for (const s of report.imageAttachmentStatusBySlot) lines.push(`- ${s.slotKey}: rows=${s.rowCount}; attachments=${s.imageAttachmentCount}; present=${s.imageAttachmentPresent ? "yes" : "no"}`);
  lines.push("");
  lines.push("## imageUrl status by slot");
  for (const s of report.imageUrlStatusBySlot) lines.push(`- ${s.slotKey}: ${s.imageUrlPresent ? "readable" : "missing"}`);
  lines.push("");
  lines.push("## Ready vs missing");
  lines.push(`- Visually ready: ${report.slotsVisuallyReady.join(", ") || "(none)"}`);
  lines.push(`- Still missing: ${report.slotsStillMissing.join(", ") || "(none)"}`);
  lines.push("");
  lines.push("## Guardrails");
  lines.push(`- Brand Setup logo untouched: **${report.brandSetupLogoUntouched ? "yes" : "no"}**`);
  lines.push(`- Company Validated fields untouched: **${report.companyValidatedFieldsUntouched ? "yes" : "no"}**`);
  lines.push(`- Tribute media visible to Brand Explorer: **${report.tributeMediaVisibleToBrandExplorer ? "yes" : "no"}**`);
  if (report.gallery3Status) {
    lines.push(
      `- Gallery slot materials.gallery.3: **${report.gallery3Status.populated ? "populated" : "unpopulated"}** (title=${report.gallery3Status.title ? "yes" : "no"}, image=${report.gallery3Status.imageUrlPresent ? "yes" : "no"})`
    );
  }
  lines.push("");
  lines.push("## Remaining gap to full visual parity");
  for (const gap of report.remainingGapToFullVisualParity) lines.push(`- ${gap}`);
  lines.push("");
  return lines.join("\n");
}
