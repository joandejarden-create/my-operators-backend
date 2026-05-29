/**
 * Full Brand Explorer presentation rows for a CHI brand (Airtable name).
 */
import { buildFullPresentationRows } from "./choice-explorer-full-builder.mjs";
import { buildChoicePrivilegesLoyaltyRows } from "./choice-privileges-loyalty-presentation-rows.mjs";
import { resolveProfileForAirtableName } from "./choice-chi-brand-resolve.mjs";

/**
 * @param {string} airtableBrandName
 */
export function buildCompletePresentationRows(airtableBrandName) {
  const profile = resolveProfileForAirtableName(airtableBrandName);
  const rows = buildFullPresentationRows(profile).filter(
    (r) => !String(r.slotKey || "").startsWith("loyalty.")
  );
  rows.push(...buildChoicePrivilegesLoyaltyRows(airtableBrandName));
  return rows;
}

/** @param {{ slotKey?: string }[]} rows */
export function slotKeyCounts(rows) {
  /** @type {Map<string, number>} */
  const counts = new Map();
  for (const r of rows) {
    const sk = String(r.slotKey || "").trim();
    if (!sk) continue;
    counts.set(sk, (counts.get(sk) || 0) + 1);
  }
  return counts;
}
