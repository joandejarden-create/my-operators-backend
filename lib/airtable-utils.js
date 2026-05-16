/** Shared Airtable formula / cell helpers (no secrets). */

export function escapeAirtableFormulaValue(value) {
  return String(value ?? "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

export function cellToString(value) {
  if (value == null) return "";
  if (typeof value === "string") return value.trim();
  if (typeof value === "number" && !Number.isNaN(value)) return String(value);
  if (typeof value === "object" && value !== null && typeof value.name === "string") {
    return value.name.trim();
  }
  if (Array.isArray(value) && value[0]) return cellToString(value[0]);
  return "";
}

/**
 * Read a cell from Airtable record.fields (keys are field names; ids rarely appear).
 * @param {object} fields
 * @param {string} [fieldName] e.g. "User_ID"
 * @param {string} [fieldId] e.g. "fldALlSB9UsnLhgvI"
 */
export function readAirtableField(fields, fieldName, fieldId) {
  if (!fields || typeof fields !== "object") return undefined;
  if (fieldName && fields[fieldName] !== undefined) return fields[fieldName];
  if (fieldId && fields[fieldId] !== undefined) return fields[fieldId];
  return undefined;
}

export function extractLinkedRecordIds(value) {
  const out = [];
  if (value == null) return out;
  if (Array.isArray(value)) {
    for (const item of value) {
      if (typeof item === "string" && item.startsWith("rec")) out.push(item);
      else if (item && typeof item === "object" && typeof item.id === "string" && item.id.startsWith("rec")) {
        out.push(item.id);
      }
    }
    return out;
  }
  if (typeof value === "string" && value.startsWith("rec")) return [value];
  return out;
}
