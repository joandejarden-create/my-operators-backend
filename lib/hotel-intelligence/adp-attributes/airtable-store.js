/**
 * Airtable upsert for Hotel ADP Attributes (idempotent).
 */

import Airtable from "airtable";
import {
  HOTEL_ADP_ATTRIBUTES_TABLE,
  MAP_HOTEL_ADP_ATTRIBUTE as F,
} from "./field-map.js";
import { CANONICAL_INTELLIGENCE_BASE_ID } from "../../decision-outcomes/airtable-base.js";

function getBaseId() {
  return (
    process.env.ADP_AIRTABLE_BASE_ID ||
    process.env.AIRTABLE_INTELLIGENCE_BASE_ID ||
    CANONICAL_INTELLIGENCE_BASE_ID ||
    "appa2cE7FTRmIbB32"
  ).trim();
}

function getToken() {
  return process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
}

function toFields(attr) {
  const fields = {
    [F.attributeKey]: attr.attributeKey || attr.dedupeKey,
    [F.dedupeKey]: attr.dedupeKey,
    [F.hpcHotelId]: attr.hpcHotelId,
    [F.dealalityHotelId]: attr.dealalityHotelId || undefined,
    [F.adpPropertyId]: attr.adpPropertyId || undefined,
    [F.hotelName]: attr.hotelName || undefined,
    [F.attributeCategory]: attr.attributeCategory,
    [F.attributeName]: attr.attributeName,
    [F.attributeValue]: attr.attributeValue,
    [F.normalizedValue]: attr.normalizedValue,
    [F.usedInAdp]: attr.usedInAdp === true,
    [F.adpUseType]: attr.adpUseType || [],
    [F.sourceType]: attr.sourceType,
    [F.sourceRecordId]: attr.sourceRecordId || undefined,
    [F.sourceName]: attr.sourceName || undefined,
    [F.sourceUrl]: attr.sourceUrl || undefined,
    [F.confidence]: attr.confidence || undefined,
    [F.effectiveFrom]: attr.effectiveFrom || undefined,
    [F.attributeVersion]: attr.attributeVersion,
    [F.lastVerifiedAt]: attr.lastVerifiedAt || undefined,
    [F.active]: attr.active !== false,
    [F.notes]: attr.notes || undefined,
    [F.schemaVersion]: attr.schemaVersion || undefined,
  };
  if (attr.effectiveTo) fields[F.effectiveTo] = attr.effectiveTo;
  // Strip undefined
  for (const k of Object.keys(fields)) {
    if (fields[k] === undefined || fields[k] === null || fields[k] === "") delete fields[k];
  }
  return fields;
}

function escapeFormula(s) {
  return String(s || "").replace(/"/g, '\\"');
}

/**
 * Sync attributes for one hotel: upsert by Dedupe Key; deactivate missing actives.
 */
export async function syncHotelAdpAttributesToAirtable(packet, { dryRun = true } = {}) {
  const token = getToken();
  const baseId = getBaseId();
  if (!token) {
    return { ok: false, error: "missing_airtable_token", dryRun };
  }
  if (!packet?.ok || !Array.isArray(packet.attributes)) {
    return { ok: false, error: "invalid_packet", dryRun };
  }

  const base = new Airtable({ apiKey: token }).base(baseId);
  const table = base(HOTEL_ADP_ATTRIBUTES_TABLE);
  const hpcId = packet.hotelId;

  // Load existing active rows for hotel
  const existing = [];
  await table
    .select({
      filterByFormula: `AND({${F.hpcHotelId}} = "${escapeFormula(hpcId)}", {${F.active}} = 1)`,
      pageSize: 100,
    })
    .eachPage((records, next) => {
      for (const r of records) existing.push(r);
      next();
    });

  const byDedupe = new Map();
  for (const r of existing) {
    const key = r.fields?.[F.dedupeKey] || r.fields?.[F.attributeKey];
    if (key) byDedupe.set(key, r);
  }

  const desiredKeys = new Set(packet.attributes.map((a) => a.dedupeKey));
  const creates = [];
  const updates = [];
  const deactivates = [];

  for (const attr of packet.attributes) {
    const fields = toFields(attr);
    const prev = byDedupe.get(attr.dedupeKey);
    if (!prev) {
      creates.push({ fields });
    } else {
      updates.push({ id: prev.id, fields });
    }
  }

  for (const r of existing) {
    const key = r.fields?.[F.dedupeKey] || r.fields?.[F.attributeKey];
    if (key && !desiredKeys.has(key)) {
      deactivates.push({
        id: r.id,
        fields: {
          [F.active]: false,
          [F.effectiveTo]: new Date().toISOString().slice(0, 10),
          [F.notes]: [r.fields?.[F.notes], "Deactivated — no longer present in Hotel Intelligence profile."]
            .filter(Boolean)
            .join(" "),
        },
      });
    }
  }

  const plan = {
    ok: true,
    dryRun,
    baseId,
    table: HOTEL_ADP_ATTRIBUTES_TABLE,
    hotelId: hpcId,
    existingActive: existing.length,
    createCount: creates.length,
    updateCount: updates.length,
    deactivateCount: deactivates.length,
  };

  if (dryRun) {
    return {
      ...plan,
      preview: {
        creates: creates.slice(0, 5),
        updates: updates.slice(0, 3),
        deactivates: deactivates.slice(0, 5),
      },
    };
  }

  // Apply in batches of 10
  async function batch(rows, method) {
    for (let i = 0; i < rows.length; i += 10) {
      const chunk = rows.slice(i, i + 10);
      if (method === "create") await table.create(chunk.map((c) => ({ fields: c.fields })));
      else if (method === "update") await table.update(chunk);
    }
  }

  await batch(creates, "create");
  await batch(updates, "update");
  await batch(deactivates, "update");

  return { ...plan, applied: true };
}

export { getBaseId, toFields, HOTEL_ADP_ATTRIBUTES_TABLE };
