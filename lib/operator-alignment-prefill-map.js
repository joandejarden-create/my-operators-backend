/**
 * Maps Phase 5B Airtable column titles → camelCase prefill keys for scoring/narrative (read path).
 */

import { formatListValue } from "../api/lib/third-party-operator-value-utils.js";
import { OAS_OPERATOR_PREFILL_KEY_ALIASES } from "./operator-alignment-field-options.js";

function toList(v) {
  if (v == null) return [];
  if (Array.isArray(v)) return v.map((x) => formatListValue(x)).filter(Boolean);
  const s = formatListValue(v);
  if (!s) return [];
  return s.split(/\s*,\s*/).map((x) => x.trim()).filter(Boolean);
}

function isEmptyPrefillVal(cur) {
  return cur == null || cur === "" || (Array.isArray(cur) && cur.length === 0);
}

/**
 * @param {Record<string, unknown>} mergedRaw — merged Airtable fields from Master + 1:1 rows
 * @param {Record<string, unknown>} prefill — mutable prefill object
 */
export function applyOperatorAlignmentPrefillAliases(mergedRaw, prefill) {
  const raw = mergedRaw || {};
  for (const [camelKey, titles] of Object.entries(OAS_OPERATOR_PREFILL_KEY_ALIASES)) {
    if (!isEmptyPrefillVal(prefill[camelKey])) continue;
    for (const title of titles) {
      if (raw[title] == null || raw[title] === "") continue;
      const v = raw[title];
      if (Array.isArray(v)) {
        prefill[camelKey] = toList(v);
      } else if (typeof v === "number" && Number.isFinite(v)) {
        prefill[camelKey] = String(v);
      } else {
        const s = formatListValue(v);
        if (s) prefill[camelKey] = s;
      }
      if (!isEmptyPrefillVal(prefill[camelKey])) break;
    }
  }
}
