/**
 * Operator Explorer Materials — child table (Brand Explorer Presentation parity).
 * Table: Operator Setup - Explorer Materials
 * UI: public/js/operator-dna-materials.js (presentation-first; Governance JSON fallback)
 */

import { sanitizeExternalCopy } from "../../lib/external-owner-copy.mjs";

export const OPERATOR_EXPLORER_MATERIALS_TABLE = "Operator Setup - Explorer Materials";

export const OPERATOR_MATERIALS_SLOT_FILE = "materials.file";
export const OPERATOR_MATERIALS_GALLERY_SLOT_PREFIX = "materials.gallery.";

/** @param {Record<string, unknown>} fields */
function fieldVal(fields, name) {
  const v = fields[name];
  if (v == null) return "";
  return v;
}

function getUrlFromAttachment(att) {
  if (!att || typeof att !== "object") return "";
  const u = att.url || att.thumbnails?.large?.url || att.thumbnails?.full?.url;
  return typeof u === "string" && u.trim() ? u.trim() : "";
}

/** First HTTPS URL from Airtable attachment field(s). */
export function firstAttachmentUrlFromFields(fields) {
  if (!fields || typeof fields !== "object") return "";
  const names = ["Image", "Images", "Attachments", "Photo", "Photos"];
  const tryArray = (val) => {
    if (typeof val === "string") {
      const s = val.trim();
      if (s.startsWith("http") || s.startsWith("//")) return s.startsWith("//") ? `https:${s}` : s;
      return "";
    }
    if (!Array.isArray(val) || val.length === 0) return "";
    for (const att of val) {
      const u = getUrlFromAttachment(att);
      if (u) return u;
    }
    return "";
  };
  for (const name of names) {
    const u = tryArray(fieldVal(fields, name));
    if (u) return u;
  }
  for (const key of Object.keys(fields)) {
    if (!/image|attachment|photo/i.test(key) || /logo|icon|favicon/i.test(key)) continue;
    const u = tryArray(fields[key]);
    if (u) return u;
  }
  return "";
}

/** @param {Record<string, unknown>} fields */
export function isExplorerMaterialActive(fields) {
  const activeRaw = fieldVal(fields, "Active");
  // Airtable checkboxes: checked → true; unchecked → field omitted (undefined) or false.
  // Opt-in: only explicitly active rows render in Explorer.
  return (
    activeRaw === true ||
    activeRaw === 1 ||
    String(activeRaw).toLowerCase() === "true" ||
    String(activeRaw).toLowerCase() === "yes" ||
    String(activeRaw).toLowerCase() === "1"
  );
}

/**
 * Normalize linked presentation rows → operatorExplorerMaterials for detail API.
 * @param {Array<{ id?: string, fields?: Record<string, unknown> }>} records
 * @returns {{ version: 1, blocks: Array<{ recordId: string, slotKey: string, title: string, body: string, sort: number, imageUrl: string }> }}
 */
export function normalizeOperatorExplorerPresentationRecords(records) {
  const list = Array.isArray(records) ? records : [];
  const blocks = [];
  for (const rec of list) {
    const f = rec.fields || {};
    if (!isExplorerMaterialActive(f)) continue;
    const slotKey = String(fieldVal(f, "Slot Key") ?? fieldVal(f, "slot_key") ?? "").trim();
    if (!slotKey) continue;
    const title = sanitizeExternalCopy(String(fieldVal(f, "Title") ?? "").trim());
    const body = sanitizeExternalCopy(String(fieldVal(f, "Body") ?? "").trim());
    const sortRaw = fieldVal(f, "Sort Order") ?? fieldVal(f, "sort_order");
    let sort = 0;
    if (typeof sortRaw === "number" && !Number.isNaN(sortRaw)) sort = sortRaw;
    else if (sortRaw != null && sortRaw !== "") sort = parseFloat(String(sortRaw).replace(/,/g, "")) || 0;
    const imageUrl = firstAttachmentUrlFromFields(f);
    blocks.push({
      recordId: rec.id || "",
      slotKey,
      title,
      body,
      sort,
      imageUrl,
    });
  }
  blocks.sort((a, b) => {
    if (a.sort !== b.sort) return a.sort - b.sort;
    return String(a.recordId).localeCompare(String(b.recordId));
  });
  return { version: 1, blocks };
}
