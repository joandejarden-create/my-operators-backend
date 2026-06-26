/**
 * Airtable base routing for Demand Anchors (Platform base).
 */

import Airtable from "airtable";
import { DEMAND_ANCHORS_TABLE } from "./airtable-demand-anchors-fields.js";

export function getDemandAnchorsBaseId() {
  return (
    process.env.AIRTABLE_DEMAND_ANCHORS_BASE_ID ||
    process.env.AIRTABLE_BASE_ID_ALT ||
    null
  );
}

/**
 * @returns {{ baseId: string, apiKey: string, base: import('airtable').Base, tableName: string } | null}
 */
export function getDemandAnchorsAirtableConfig() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = getDemandAnchorsBaseId();
  if (!apiKey || !baseId) return null;
  const tableName = process.env.AIRTABLE_TABLE_DEMAND_ANCHORS || DEMAND_ANCHORS_TABLE;
  return {
    baseId,
    apiKey,
    base: new Airtable({ apiKey }).base(baseId),
    tableName,
  };
}

/**
 * @param {string} baseId
 * @param {string} apiKey
 */
export async function resolveDemandAnchorsTableName(baseId, apiKey) {
  const preferred = process.env.AIRTABLE_TABLE_DEMAND_ANCHORS || DEMAND_ANCHORS_TABLE;
  try {
    const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    if (!res.ok) return preferred;
    const data = await res.json();
    const names = (data.tables || []).map((t) => t.name);
    if (names.includes(preferred)) return preferred;
    const hit = names.find((n) => /demand anchors/i.test(n));
    return hit || preferred;
  } catch {
    return preferred;
  }
}
