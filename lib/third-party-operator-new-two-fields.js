/**
 * New Two form: form field keys ↔ Airtable column names on split tables (bindings JSON — column titles match Airtable).
 * Regenerate JSON: `node scripts/generate-new-two-bindings.mjs`
 */
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { formatListValue, parseMultiValue } from "./third-party-operator-value-utils.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const data = JSON.parse(readFileSync(join(__dirname, "third-party-operator-new-two-field-bindings.json"), "utf8"));

export const NEW_TWO_FIELD_BINDINGS = data.bindings || [];

function pickRawFromBody(body, formKeys, fieldType) {
  if (fieldType === "multipleSelects") {
    for (let i = formKeys.length - 1; i >= 0; i--) {
      const k = formKeys[i];
      const raw = body[k];
      if (raw === undefined || raw === null) continue;
      if (Array.isArray(raw) && raw.length) return raw;
      if (typeof raw === "string" && raw.trim() !== "") return raw;
    }
    return undefined;
  }
  if (fieldType === "checkbox") {
    let any = false;
    for (const k of formKeys) {
      if (body[k] !== undefined) any = true;
    }
    if (!any) return undefined;
    for (const k of formKeys) {
      const raw = body[k];
      if (raw === true || raw === "yes" || raw === "on" || raw === "1") return true;
    }
    return false;
  }
  for (let i = formKeys.length - 1; i >= 0; i--) {
    const k = formKeys[i];
    const raw = body[k];
    if (raw === undefined || raw === null) continue;
    if (typeof raw === "string" && raw.trim() === "") continue;
    return raw;
  }
  return undefined;
}

function coerceIntakeValue(raw, fieldType) {
  if (fieldType === "checkbox") return raw === true || raw === false ? raw : !!raw;
  if (fieldType === "number") {
    const n = typeof raw === "number" ? raw : parseFloat(String(raw).replace(/,/g, ""));
    return Number.isFinite(n) ? n : undefined;
  }
  if (fieldType === "multipleSelects") {
    if (Array.isArray(raw)) return raw.map((x) => String(x).trim()).filter(Boolean);
    if (typeof raw === "string") {
      try {
        const p = JSON.parse(raw);
        if (Array.isArray(p)) return p.map((x) => String(x).trim()).filter(Boolean);
      } catch {
        /* fall through */
      }
      return raw
        .split(/[,\n]/)
        .map((s) => s.trim())
        .filter(Boolean);
    }
    return undefined;
  }
  if (typeof raw === "string") return raw;
  if (raw != null && typeof raw === "object") return JSON.stringify(raw);
  return String(raw);
}

/**
 * Merge New Two POST fields into the same compact column-name map used for Basics + split-table mirrors.
 * @param {Record<string, unknown>} compactFields
 * @param {Record<string, unknown>} body
 */
export function applyNewTwoFieldsToCompact(compactFields, body) {
  const out = { ...compactFields };
  for (const b of NEW_TWO_FIELD_BINDINGS) {
    const raw = pickRawFromBody(body, b.formKeys, b.fieldType);
    if (raw === undefined) continue;
    const coerced = coerceIntakeValue(raw, b.fieldType);
    if (coerced === undefined) continue;
    if (Array.isArray(coerced) && coerced.length === 0) continue;
    if (typeof coerced === "string" && coerced.trim() === "") continue;
    out[b.airtableName] = coerced;
  }
  return out;
}

function prefillValueForForm(cell, fieldType) {
  if (fieldType === "checkbox") {
    if (cell === true) return "yes";
    if (cell === false) return "";
    const t = formatListValue(cell);
    if (t === "1" || t.toLowerCase() === "true" || t.toLowerCase() === "yes") return "yes";
    return "";
  }
  if (fieldType === "multipleSelects") {
    if (Array.isArray(cell)) return cell.map((x) => formatListValue(x)).filter(Boolean);
    return parseMultiValue(formatListValue(cell));
  }
  return formatListValue(cell);
}

/**
 * Populate New Two form keys from linked split-table rows + Basics.
 * @param {Record<string, unknown>} prefill
 * @param {{ f: object, pf: object, sf: object, ff: object, ifields: object, of: object, dtf: object }} sources
 */
export function applyNewTwoPrefillFromSplitTables(prefill, sources) {
  const { f, pf, sf, ff, ifields, of, dtf } = sources;
  const map = {
    BASICS: f,
    PERF: pf,
    SERVICES: sf,
    FOOTPRINT: ff,
    IDEAL: ifields,
    OWNER_REL: of,
    DEAL_TERMS: dtf,
  };
  for (const b of NEW_TWO_FIELD_BINDINGS) {
    const row = map[b.tableKey];
    if (!row) continue;
    let cell = row[b.airtableName];
    /** New-base tables use intake `name` attributes as column titles (e.g. `cap_profile_operational`); bindings still reference legacy labels (e.g. "Operational Execution"). */
    if (cell == null && Array.isArray(b.formKeys)) {
      for (const fk of b.formKeys) {
        if (row[fk] != null && row[fk] !== "") {
          cell = row[fk];
          break;
        }
      }
    }
    if (cell == null) continue;
    if (b.fieldType === "checkbox") {
      const v = prefillValueForForm(cell, b.fieldType);
      for (const fk of b.formKeys) prefill[fk] = v;
      continue;
    }
    if (cell === "") continue;
    const v = prefillValueForForm(cell, b.fieldType);
    if (v === undefined) continue;
    if (Array.isArray(v) && v.length === 0) continue;
    if (typeof v === "string" && v.trim() === "") continue;
    for (const fk of b.formKeys) {
      prefill[fk] = v;
    }
  }
}
