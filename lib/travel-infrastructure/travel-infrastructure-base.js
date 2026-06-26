/**
 * Airtable base routing for Travel Infrastructure (Platform base).
 */

import Airtable from "airtable";
import {
  TRAVEL_INFRASTRUCTURE_TABLE,
  TRAVEL_INFRASTRUCTURE_TABLE_LEGACY,
} from "./airtable-travel-infrastructure-fields.js";

export function getTravelInfrastructureBaseId() {
  return (
    process.env.AIRTABLE_TRAVEL_INFRASTRUCTURE_BASE_ID ||
    process.env.AIRTABLE_BASE_ID_ALT ||
    null
  );
}

/**
 * @returns {{ baseId: string, apiKey: string, base: import('airtable').Base, tableName: string } | null}
 */
export function getTravelInfrastructureAirtableConfig() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = getTravelInfrastructureBaseId();
  if (!apiKey || !baseId) return null;
  const tableName =
    process.env.AIRTABLE_TABLE_TRAVEL_INFRASTRUCTURE || TRAVEL_INFRASTRUCTURE_TABLE;
  return {
    baseId,
    apiKey,
    base: new Airtable({ apiKey }).base(baseId),
    tableName,
  };
}

/**
 * Resolve table name against meta (handles legacy casing).
 * @param {string} baseId
 * @param {string} apiKey
 */
export async function resolveTravelInfrastructureTableName(baseId, apiKey) {
  const preferred =
    process.env.AIRTABLE_TABLE_TRAVEL_INFRASTRUCTURE || TRAVEL_INFRASTRUCTURE_TABLE;
  try {
    const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    if (!res.ok) return preferred;
    const data = await res.json();
    const names = (data.tables || []).map((t) => t.name);
    if (names.includes(preferred)) return preferred;
    if (names.includes(TRAVEL_INFRASTRUCTURE_TABLE_LEGACY)) return TRAVEL_INFRASTRUCTURE_TABLE_LEGACY;
    const hit = names.find((n) => /travel infrastructure/i.test(n));
    return hit || preferred;
  } catch {
    return preferred;
  }
}
