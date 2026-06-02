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

function alignmentContractDiagEnabled() {
  try {
    if (typeof process !== "undefined" && process.env) {
      const env = String(process.env.OPERATOR_SETUP_CONTRACT_DIAGNOSTICS || "").trim().toLowerCase();
      if (env === "1" || env === "true" || env === "yes" || env === "on") return true;
    }
    if (globalThis && (globalThis.__OPERATOR_SETUP_CONTRACT_DIAGNOSTICS === true || globalThis.__OPERATOR_SETUP_CONTRACT_DIAGNOSTICS === "1")) {
      return true;
    }
  } catch (_err) {}
  return false;
}

function emitAlignmentContractDiag(payload) {
  if (!alignmentContractDiagEnabled()) return;
  try {
    console.debug(
      "[operator_setup_contract_diag]",
      JSON.stringify(Object.assign({ scope: "alignment_prefill_alias_resolution" }, payload || {}))
    );
  } catch (_err) {}
}

/**
 * @param {Record<string, unknown>} mergedRaw — merged Airtable fields from Master + 1:1 rows
 * @param {Record<string, unknown>} prefill — mutable prefill object
 */
export function applyOperatorAlignmentPrefillAliases(mergedRaw, prefill) {
  const raw = mergedRaw || {};
  for (const [camelKey, titles] of Object.entries(OAS_OPERATOR_PREFILL_KEY_ALIASES)) {
    if (!isEmptyPrefillVal(prefill[camelKey])) {
      emitAlignmentContractDiag({
        concept: camelKey,
        canonicalKey: camelKey,
        sourceUsed: "prefill",
        keyUsed: camelKey,
        fallbackUsed: false,
        skippedAliasResolution: true,
      });
      continue;
    }
    let resolved = false;
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
      if (!isEmptyPrefillVal(prefill[camelKey])) {
        resolved = true;
        emitAlignmentContractDiag({
          concept: camelKey,
          canonicalKey: camelKey,
          sourceUsed: title === camelKey ? "canonical_raw" : "alias_raw",
          keyUsed: title,
          fallbackUsed: title !== camelKey,
          fallbackKey: title !== camelKey ? title : "",
        });
        break;
      }
    }
    if (!resolved) {
      emitAlignmentContractDiag({
        concept: camelKey,
        canonicalKey: camelKey,
        sourceUsed: "unresolved",
        keyUsed: "",
        fallbackUsed: false,
        unresolved: true,
      });
    }
  }
}
