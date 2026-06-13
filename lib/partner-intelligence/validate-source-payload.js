/**
 * Partner Intelligence — Source Library validation before Airtable writes.
 */
import {
  MAP_PARTNER_SOURCE,
  VAL_PARTNER_SOURCE_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";

const API_TO_AIRTABLE = Object.entries(MAP_PARTNER_SOURCE).reduce((acc, [apiKey, col]) => {
  acc[apiKey] = col;
  return acc;
}, {});

const LINK_API_KEYS = new Set([
  "parentCompany",
  "brand",
  "operator",
  "relatedContact",
  "duplicateOf",
]);

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function isRecId(v) {
  return typeof v === "string" && /^rec[a-zA-Z0-9]+$/.test(v);
}

function validateSelect(fieldLabel, value, allowed, required) {
  const s = nz(value);
  if (!s) {
    if (required) return { ok: false, error: `${fieldLabel} is required.` };
    return { ok: true, value: null };
  }
  if (!allowed.includes(s)) {
    return {
      ok: false,
      error: `${fieldLabel} must be one of: ${allowed.join(", ")}.`,
      invalidValue: s,
    };
  }
  return { ok: true, value: s };
}

function validateLink(fieldLabel, value, required) {
  if (value == null || value === "") {
    if (required) return { ok: false, error: `${fieldLabel} link is required.` };
    return { ok: true, value: null };
  }
  const id = Array.isArray(value) ? value[0] : value;
  if (!isRecId(id)) {
    return { ok: false, error: `${fieldLabel} must be a valid Airtable record id (rec…).` };
  }
  return { ok: true, value: [id] };
}

/**
 * @param {object} body — camelCase API payload
 * @param {{ mode: 'create'|'patch' }} opts
 */
export function validatePartnerSourcePayload(body, opts = {}) {
  const mode = opts.mode || "create";
  const errors = [];
  const fields = {};
  const b = body && typeof body === "object" ? body : {};

  if (mode === "create" && !nz(b.sourceTitle)) {
    errors.push("sourceTitle is required.");
  }

  const profileType = validateSelect(
    "profileType",
    b.profileType,
    VAL_PARTNER_SOURCE_SELECTS.profileType,
    mode === "create"
  );
  if (!profileType.ok) errors.push(profileType.error);
  else if (profileType.value) fields[MAP_PARTNER_SOURCE.profileType] = profileType.value;

  const scalarKeys = [
    ["sourceTitle", MAP_PARTNER_SOURCE.sourceTitle, false],
    ["region", MAP_PARTNER_SOURCE.region, false],
    ["countryMarket", MAP_PARTNER_SOURCE.countryMarket, false],
    ["sourceUrl", MAP_PARTNER_SOURCE.sourceUrl, false],
    ["notes", MAP_PARTNER_SOURCE.notes, false],
    ["extractionRunId", MAP_PARTNER_SOURCE.extractionRunId, false],
    ["localFilePath", MAP_PARTNER_SOURCE.localFilePath, false],
    ["confidentialityNotes", MAP_PARTNER_SOURCE.confidentialityNotes, false],
    ["permissionVisibilityNotes", MAP_PARTNER_SOURCE.permissionVisibilityNotes, false],
  ];

  for (const [apiKey, col, required] of scalarKeys) {
    if (b[apiKey] === undefined) continue;
    const v = nz(b[apiKey]);
    if (!v && required) {
      errors.push(`${apiKey} is required.`);
      continue;
    }
    if (v) fields[col] = v;
  }

  const selectMap = [
    ["sourceType", VAL_PARTNER_SOURCE_SELECTS, "sourceType"],
    ["sourceOrigin", VAL_PARTNER_SOURCE_SELECTS, "sourceOrigin"],
    ["visibility", VAL_PARTNER_SOURCE_SELECTS, "visibility"],
    ["verifiedSource", VAL_PARTNER_SOURCE_SELECTS, "verifiedSource"],
    ["sourceQuality", VAL_PARTNER_SOURCE_SELECTS, "sourceQuality"],
    ["status", VAL_PARTNER_SOURCE_SELECTS, "status"],
    ["approvedForExtraction", VAL_PARTNER_SOURCE_SELECTS, "approvedForExtraction"],
    ["approvedForExplorerUse", VAL_PARTNER_SOURCE_SELECTS, "approvedForExplorerUse"],
  ];

  for (const [apiKey, catalog, catalogKey] of selectMap) {
    if (b[apiKey] === undefined) continue;
    const result = validateSelect(apiKey, b[apiKey], catalog[catalogKey], false);
    if (!result.ok) errors.push(result.error);
    else if (result.value) fields[MAP_PARTNER_SOURCE[apiKey]] = result.value;
  }

  for (const apiKey of LINK_API_KEYS) {
    if (b[apiKey] === undefined && b[`${apiKey}Id`] === undefined) continue;
    const raw = b[apiKey] !== undefined ? b[apiKey] : b[`${apiKey}Id`];
    const link = validateLink(apiKey, raw, false);
    if (!link.ok) errors.push(link.error);
    else if (link.value) fields[MAP_PARTNER_SOURCE[apiKey]] = link.value;
  }

  // Shorthand: operatorId / brandId
  if (b.operatorId !== undefined) {
    const link = validateLink("operator", b.operatorId, false);
    if (!link.ok) errors.push(link.error);
    else if (link.value) fields[MAP_PARTNER_SOURCE.operator] = link.value;
  }
  if (b.brandId !== undefined) {
    const link = validateLink("brand", b.brandId, false);
    if (!link.ok) errors.push(link.error);
    else if (link.value) fields[MAP_PARTNER_SOURCE.brand] = link.value;
  }

  if (b.sourceDate !== undefined && nz(b.sourceDate)) {
    fields[MAP_PARTNER_SOURCE.sourceDate] = nz(b.sourceDate);
  }
  if (b.captureDate !== undefined && nz(b.captureDate)) {
    fields[MAP_PARTNER_SOURCE.captureDate] = nz(b.captureDate);
  }

  if (mode === "create") {
    if (!fields[MAP_PARTNER_SOURCE.status]) {
      fields[MAP_PARTNER_SOURCE.status] = "Captured";
    }
    if (!fields[MAP_PARTNER_SOURCE.verifiedSource]) {
      fields[MAP_PARTNER_SOURCE.verifiedSource] = "No";
    }
    if (!fields[MAP_PARTNER_SOURCE.approvedForExtraction]) {
      fields[MAP_PARTNER_SOURCE.approvedForExtraction] = "No";
    }
    if (!fields[MAP_PARTNER_SOURCE.approvedForExplorerUse]) {
      fields[MAP_PARTNER_SOURCE.approvedForExplorerUse] = "No";
    }
    if (!fields[MAP_PARTNER_SOURCE.captureDate]) {
      fields[MAP_PARTNER_SOURCE.captureDate] = new Date().toISOString().slice(0, 10);
    }
  }

  const profile = fields[MAP_PARTNER_SOURCE.profileType];
  const hasOperator = Boolean(fields[MAP_PARTNER_SOURCE.operator]);
  const hasBrand = Boolean(fields[MAP_PARTNER_SOURCE.brand]);
  if (mode === "create" && profile === "Operator" && !hasOperator) {
    errors.push("operatorId is required when profileType is Operator.");
  }
  if (mode === "create" && profile === "Brand" && !hasBrand) {
    errors.push("brandId is required when profileType is Brand.");
  }

  return {
    ok: errors.length === 0,
    errors,
    fields,
    fieldMapping: API_TO_AIRTABLE,
  };
}

export function inferFileTypeFromName(filename) {
  const ext = nz(filename).split(".").pop().toLowerCase();
  const map = {
    pdf: "PDF",
    pptx: "PPTX",
    ppt: "PPTX",
    docx: "DOCX",
    doc: "DOCX",
    xlsx: "XLSX",
    xls: "XLSX",
    html: "HTML",
    htm: "HTML",
    png: "PNG/JPG",
    jpg: "PNG/JPG",
    jpeg: "PNG/JPG",
    txt: "TXT",
  };
  return map[ext] || "Other";
}

export const PARTNER_SOURCE_ALLOWED_UPLOAD_EXT = [
  ".pdf",
  ".pptx",
  ".ppt",
  ".docx",
  ".doc",
  ".xlsx",
  ".xls",
  ".html",
  ".htm",
  ".png",
  ".jpg",
  ".jpeg",
  ".txt",
];

export const PARTNER_SOURCE_MAX_UPLOAD_BYTES = 50 * 1024 * 1024;
