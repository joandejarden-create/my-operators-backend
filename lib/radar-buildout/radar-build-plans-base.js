/**
 * Airtable base routing for CALA Radar Build Plans.
 */

import Airtable from "airtable";
import { RADAR_BUILD_PLANS_TABLE } from "./airtable-radar-build-plans-fields.js";

export function getRadarBuildPlansBaseId() {
  return (
    process.env.AIRTABLE_RADAR_BUILD_PLANS_BASE_ID ||
    process.env.AIRTABLE_BASE_ID_ALT ||
    null
  );
}

export function getRadarBuildPlansAirtableConfig() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = getRadarBuildPlansBaseId();
  if (!apiKey || !baseId) return null;
  const tableName = process.env.AIRTABLE_TABLE_RADAR_BUILD_PLANS || RADAR_BUILD_PLANS_TABLE;
  return {
    baseId,
    apiKey,
    base: new Airtable({ apiKey }).base(baseId),
    tableName,
  };
}

export async function resolveRadarBuildPlansTableName(baseId, apiKey) {
  const preferred = process.env.AIRTABLE_TABLE_RADAR_BUILD_PLANS || RADAR_BUILD_PLANS_TABLE;
  try {
    const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    if (!res.ok) return preferred;
    const data = await res.json();
    const names = (data.tables || []).map((t) => t.name);
    if (names.includes(preferred)) return preferred;
    const hit = names.find((n) => /radar build plans/i.test(n));
    return hit || preferred;
  } catch {
    return preferred;
  }
}
