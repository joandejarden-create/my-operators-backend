/**
 * Commit Demand Anchors import records to Airtable.
 */

import {
  fetchDemandAnchorRecords,
} from "./airtable-demand-anchors-io.js";
import {
  getDemandAnchorsAirtableConfig,
  resolveDemandAnchorsTableName,
} from "./demand-anchors-base.js";
import {
  buildDemandAnchorsImportPreview,
  filterCommitRecords,
  coerceImportPoint,
} from "./import-validation.js";
import { buildDemandAnchorAirtableFields } from "./import-airtable-fields.js";
import { fetchAirtableTableFieldNameSet } from "../third-party-operator-basics-airtable-column-aliases.js";
import { validateVerifiedFixtureRequirements } from "../location-verification/verified-fixture-gating.js";

const isDev = process.env.NODE_ENV !== "production";

function extractCommitItems(records) {
  return (records || []).map((r) => {
    if (r && r.normalized) return r.normalized;
    return coerceImportPoint(r);
  });
}

/**
 * @param {object[]} records — normalized points or preview rows
 * @param {object} [options]
 */
export async function commitDemandAnchorsImport(records, options = {}) {
  const cfg = getDemandAnchorsAirtableConfig();
  if (!cfg) return { ok: false, error: "airtable_config_missing" };

  const ctx = {
    market: options.market || "",
    country: options.country || "",
    region: options.region || "",
  };

  const items = extractCommitItems(records);
  if (!items.length) {
    return {
      ok: false,
      error: "validation_failed",
      message: "No records provided for import.",
      created: [],
      skipped: [],
      errors: [],
    };
  }

  const existingResult = await fetchDemandAnchorRecords({
    country: ctx.country,
    region: ctx.region,
    market: ctx.market,
    includeHidden: true,
  });
  if (existingResult.error) {
    return { ok: false, error: existingResult.error, message: existingResult.error };
  }

  const preview = buildDemandAnchorsImportPreview(
    items,
    existingResult.allPoints || existingResult.points || [],
    ctx
  );

  const skipDuplicates = options.skipDuplicates !== false;
  const rowsToSave = filterCommitRecords(preview.preview, skipDuplicates);

  const skipped = preview.preview
    .filter((row) => row.valid && !rowsToSave.includes(row))
    .map((row) => ({
      index: row.index,
      name: row.name,
      reason: row.duplicateStatus === "possible_duplicate" ? "duplicate" : "not_selected",
    }));

  const rejected = preview.preview
    .filter((row) => !row.valid)
    .map((row) => ({ index: row.index, name: row.name, errors: row.errors }));

  if (!rowsToSave.length) {
    return {
      ok: false,
      error: "validation_failed",
      message: "No valid records to import after validation.",
      created: [],
      skipped: [...skipped, ...rejected.map((r) => ({ ...r, reason: "invalid" }))],
      errors: rejected,
      preview,
    };
  }

  const tableName = await resolveDemandAnchorsTableName(cfg.baseId, cfg.apiKey);
  const schema = await fetchAirtableTableFieldNameSet(cfg.baseId, cfg.apiKey, tableName);

  const created = [];
  const errors = [];

  for (const row of rowsToSave) {
    try {
      const fields = buildDemandAnchorAirtableFields(row.normalized, {
        schema,
        dealRecordId: options.dealRecordId,
        linkedMarketId: options.linkedMarketId,
      });
      const rec = await cfg.base(tableName).create(fields, { typecast: true });
      created.push({
        id: rec.id,
        name: fields["Demand Anchor Name"] || row.name,
      });
    } catch (err) {
      if (isDev) console.error("[demand-anchors-import] create failed", err?.message);
      errors.push({
        index: row.index,
        name: row.name,
        message: err?.message || String(err),
      });
    }
  }

  return {
    ok: errors.length === 0 || created.length > 0,
    created,
    skipped,
    errors,
    summary: {
      submitted: items.length,
      created: created.length,
      skipped: skipped.length,
      failed: errors.length,
    },
  };
}

/**
 * Preview only — no writes.
 */
export async function previewDemandAnchorsImport(body) {
  const ctx = {
    market: body.market || "",
    country: body.country || "",
    region: body.region || "",
  };
  const points = Array.isArray(body.points) ? body.points : [];

  if (!points.length) {
    return {
      ok: false,
      error: "validation_failed",
      message: "points array is required and must not be empty",
    };
  }

  const verifiedGateErrors = validateVerifiedFixtureRequirements(body);
  if (verifiedGateErrors) {
    return {
      ok: false,
      error: "verified_fixture_required",
      message: verifiedGateErrors.join(" "),
      errors: verifiedGateErrors,
    };
  }

  const existingResult = await fetchDemandAnchorRecords({
    country: ctx.country,
    region: ctx.region,
    market: ctx.market,
    includeHidden: true,
  });

  const existing =
    existingResult.error === "demand_anchors_table_missing"
      ? []
      : existingResult.allPoints || existingResult.points || [];

  const result = buildDemandAnchorsImportPreview(points, existing, ctx);
  return { ok: true, ...result };
}
