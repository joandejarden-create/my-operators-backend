/**
 * Normalize read/write values against live Airtable select options + aliases.
 */

import {
  normalizeOptionKey,
  buildLiveOptionLookup,
  getLiveOptionsList,
  getFieldLiveMeta,
} from "./operator-alignment-airtable-options-loader.js";
import { resolveAliasToLiveLabel, getCanonicalCategory } from "./operator-alignment-airtable-option-aliases.js";

function toInputList(value) {
  if (value == null) return [];
  if (Array.isArray(value)) return value.map((x) => String(x).trim()).filter(Boolean);
  const s = String(value).trim();
  return s ? [s] : [];
}

/**
 * Resolve one value to exact live Airtable label (or null + warning).
 * @param {string} value
 * @param {string[]} allowedOptions - live options from Airtable
 * @param {Record<string, string>} [aliasToLive] - alias key → live label
 * @returns {{ label: string|null, canonical: string|null, warning: string|null, matchedVia: string|null }}
 */
export function resolveToLiveOption(value, allowedOptions, aliasToLive = {}) {
  const raw = String(value || "").trim();
  if (!raw) return { label: null, canonical: null, warning: null, matchedVia: null };

  const lookup = buildLiveOptionLookup(allowedOptions);
  const nk = normalizeOptionKey(raw);

  if (lookup.has(nk)) {
    const label = lookup.get(nk);
    return {
      label,
      canonical: getCanonicalCategory(label) || getCanonicalCategory(raw) || nk.replace(/\s+/g, "_"),
      warning: null,
      matchedVia: "exact_live",
    };
  }

  const aliasLabel = aliasToLive[nk] || resolveAliasToLiveLabel(raw, allowedOptions);
  if (aliasLabel) {
    const aliasNk = normalizeOptionKey(aliasLabel);
    if (lookup.has(aliasNk)) {
      return {
        label: lookup.get(aliasNk),
        canonical: getCanonicalCategory(lookup.get(aliasNk)) || nk.replace(/\s+/g, "_"),
        warning: null,
        matchedVia: "alias",
      };
    }
  }

  return {
    label: null,
    canonical: getCanonicalCategory(raw),
    warning: `Unmapped value "${raw}" — not in live Airtable options`,
    matchedVia: null,
  };
}

/**
 * @param {unknown} value
 * @param {string[]} allowedOptions
 * @param {Record<string, string>} [aliasToLive]
 * @param {{ allowNull?: boolean }} [opts]
 */
export function normalizeAirtableSelectValue(value, allowedOptions, aliasToLive = {}, opts = {}) {
  const r = resolveToLiveOption(value, allowedOptions, aliasToLive);
  if (!r.label && !opts.allowNull && value != null && String(value).trim()) {
    return { value: null, ...r, ok: false };
  }
  return { value: r.label, ...r, ok: Boolean(r.label || opts.allowNull) };
}

/**
 * @param {unknown} values
 * @param {string[]} allowedOptions
 * @param {Record<string, string>} [aliasToLive]
 */
export function normalizeAirtableMultiSelectValues(values, allowedOptions, aliasToLive = {}) {
  const inputs = toInputList(values);
  const out = [];
  const canonicals = [];
  const warnings = [];
  const seen = new Set();

  for (const v of inputs) {
    const r = resolveToLiveOption(v, allowedOptions, aliasToLive);
    if (r.label) {
      const k = normalizeOptionKey(r.label);
      if (!seen.has(k)) {
        seen.add(k);
        out.push(r.label);
        if (r.canonical) canonicals.push(r.canonical);
      }
    } else if (r.warning) warnings.push(r.warning);
  }

  return {
    values: out,
    canonicals: [...new Set(canonicals)],
    warnings,
    ok: warnings.length === 0 || out.length > 0,
  };
}

/**
 * Normalize for scoring reads (canonical categories, preserve labels).
 */
export function normalizeForScoring(value, allowedOptions, aliasToLive = {}) {
  const multi = Array.isArray(value) || (typeof value === "string" && value.includes(","));
  if (multi && !Array.isArray(value)) {
    const m = normalizeAirtableMultiSelectValues(
      String(value)
        .split(/\s*,\s*/)
        .filter(Boolean),
      allowedOptions,
      aliasToLive
    );
    return { labels: m.values, canonicals: m.canonicals, warnings: m.warnings };
  }
  if (Array.isArray(value)) {
    const m = normalizeAirtableMultiSelectValues(value, allowedOptions, aliasToLive);
    return { labels: m.values, canonicals: m.canonicals, warnings: m.warnings };
  }
  const s = normalizeAirtableSelectValue(value, allowedOptions, aliasToLive, { allowNull: true });
  return {
    labels: s.value ? [s.value] : [],
    canonicals: s.canonical ? [s.canonical] : [],
    warnings: s.warning ? [s.warning] : [],
  };
}

/**
 * Validate write payload values against live index.
 * @param {object} liveIndex
 * @param {string} tableKey
 * @param {string} fieldName
 * @param {unknown} value
 */
export function validateWriteValue(liveIndex, tableKey, fieldName, value) {
  const meta = getFieldLiveMeta(liveIndex, tableKey, fieldName);
  if (!meta || meta.status !== "ok") {
    return { ok: false, error: `Field not in live schema: ${tableKey}/${fieldName}` };
  }
  const allowed = meta.liveOptions || [];
  if (meta.fieldType === "multipleSelects") {
    const m = normalizeAirtableMultiSelectValues(value, allowed);
    return {
      ok: m.ok && m.warnings.length === 0,
      value: m.values,
      warnings: m.warnings,
      fieldType: meta.fieldType,
    };
  }
  if (meta.fieldType === "singleSelect") {
    const s = normalizeAirtableSelectValue(value, allowed);
    return { ok: s.ok, value: s.value, warnings: s.warning ? [s.warning] : [], fieldType: meta.fieldType };
  }
  return { ok: true, value, fieldType: meta.fieldType, warnings: [] };
}
