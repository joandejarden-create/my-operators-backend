/**
 * Idempotent ensure Scout Opportunity Signals table + fields (Metadata API).
 * Deal Capture Platform — AIRTABLE_BASE_ID_ALT only.
 */

import { metaFetch } from "../str-census-import/airtable-meta.mjs";
import {
  SCOUT_OPPORTUNITY_SIGNALS_TABLE,
  watchlistFieldSpecs,
} from "./scout-signal-watchlist-fields.js";

function findTable(tables, name) {
  return (tables || []).find((t) => t.name === name) || null;
}

function hasField(table, name) {
  return (table?.fields || []).some((f) => f.name === name);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function createField(baseId, token, tableId, spec, dryRun) {
  if (dryRun) return { ok: true, dry: true, created: spec.name };
  const { res, json } = await metaFetch(baseId, token, `/tables/${tableId}/fields`, {
    method: "POST",
    body: JSON.stringify({
      name: spec.name,
      type: spec.type,
      ...(spec.options ? { options: spec.options } : {}),
    }),
  });
  if (!res.ok) return { ok: false, status: res.status, json, name: spec.name };
  await sleep(220);
  return { ok: true, json };
}

/**
 * @param {{ apply?: boolean }} [opts]
 * @returns {Promise<{ ok: boolean, tableId: string|null, tableCreated: boolean, fieldsCreated: string[], fieldsExisting: string[], errors: string[] }>}
 */
export async function ensureScoutOpportunitySignalsSchema(opts = {}) {
  const dryRun = opts.apply !== true;
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  const errors = [];
  const fieldsCreated = [];
  const fieldsExisting = [];

  if (!token || !baseId) {
    return { ok: false, tableId: null, tableCreated: false, fieldsCreated, fieldsExisting, errors: ["Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT"] };
  }

  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) {
    return {
      ok: false,
      tableId: null,
      tableCreated: false,
      fieldsCreated,
      fieldsExisting,
      errors: [`List tables failed ${listRes.status}: ${JSON.stringify(listJson)}`],
    };
  }

  let tables = listJson.tables || [];
  let table = findTable(tables, SCOUT_OPPORTUNITY_SIGNALS_TABLE);
  let tableCreated = false;

  if (!table) {
    const specs = watchlistFieldSpecs();
    const body = {
      name: SCOUT_OPPORTUNITY_SIGNALS_TABLE,
      description:
        "Saved Scout opportunity signals for review, watchlist, and outreach workflow. Not auto-populated from generation.",
      fields: specs,
    };

    if (dryRun) {
      return {
        ok: true,
        tableId: null,
        tableCreated: true,
        fieldsCreated: specs.map((s) => s.name),
        fieldsExisting: [],
        errors: [],
        dryRun: true,
      };
    }

    const { res, json } = await metaFetch(baseId, token, "/tables", {
      method: "POST",
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      return {
        ok: false,
        tableId: null,
        tableCreated: false,
        fieldsCreated,
        fieldsExisting,
        errors: [`Create table failed ${res.status}: ${JSON.stringify(json)}`],
      };
    }
    table = json;
    tableCreated = true;
    fieldsCreated.push(...specs.map((s) => s.name));
    return {
      ok: true,
      tableId: table.id,
      tableCreated: true,
      fieldsCreated,
      fieldsExisting: [],
      errors: [],
    };
  }

  const specs = watchlistFieldSpecs();
  for (const spec of specs) {
    if (hasField(table, spec.name)) {
      fieldsExisting.push(spec.name);
      continue;
    }
    const result = await createField(baseId, token, table.id, spec, dryRun);
    if (!result.ok) {
      errors.push(`Field ${spec.name}: ${result.status} ${JSON.stringify(result.json)}`);
      continue;
    }
    if (dryRun) {
      fieldsCreated.push(spec.name);
    } else {
      fieldsCreated.push(spec.name);
      if (!table.fields) table.fields = [];
      table.fields.push({ name: spec.name, type: spec.type });
    }
  }

  return {
    ok: errors.length === 0,
    tableId: table.id,
    tableCreated,
    fieldsCreated,
    fieldsExisting,
    errors,
    dryRun,
  };
}
