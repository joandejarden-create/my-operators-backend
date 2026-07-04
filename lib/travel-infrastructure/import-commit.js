/**
 * Commit Travel Infrastructure additional-type imports to Airtable.
 */

import { fetchTravelInfrastructureRecords } from "./airtable-travel-infrastructure-io.js";
import {
  getTravelInfrastructureAirtableConfig,
  resolveTravelInfrastructureTableName,
} from "./travel-infrastructure-base.js";
import {
  buildTravelInfraImportPreview,
  coerceTravelInfraImportPoint,
} from "./import-validation.js";
import { buildTravelInfraAirtableFields } from "./import-airtable-fields.js";
import { filterCommitRecords } from "../demand-anchors/import-validation.js";
import { fetchAirtableTableFieldNameSet } from "../third-party-operator-basics-airtable-column-aliases.js";
import { validateVerifiedFixtureRequirements } from "../location-verification/verified-fixture-gating.js";

const isDev = process.env.NODE_ENV !== "production";

function extractItems(records) {
  return (records || []).map((r) => (r.normalized ? r.normalized : coerceTravelInfraImportPoint(r)));
}

export async function previewTravelInfrastructureImport(body) {
  const ctx = {
    market: body.market || "",
    country: body.country || "",
    region: body.region || "",
  };
  const points = Array.isArray(body.points) ? body.points : [];
  if (!points.length) {
    return { ok: false, error: "validation_failed", message: "points array is required" };
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

  const existingResult = await fetchTravelInfrastructureRecords({
    country: ctx.country,
    region: ctx.region,
    includeHidden: true,
  });
  const existing =
    existingResult.error === "travel_infrastructure_table_missing"
      ? []
      : existingResult.allPoints || existingResult.points || [];

  return { ok: true, ...buildTravelInfraImportPreview(points, existing, ctx) };
}

export async function commitTravelInfrastructureImport(records, options = {}) {
  const cfg = getTravelInfrastructureAirtableConfig();
  if (!cfg) return { ok: false, error: "airtable_config_missing" };

  const ctx = {
    market: options.market || "",
    country: options.country || "",
    region: options.region || "",
  };
  const items = extractItems(records);
  if (!items.length) {
    return { ok: false, error: "validation_failed", message: "No records provided", created: [], skipped: [], errors: [] };
  }

  const existingResult = await fetchTravelInfrastructureRecords({
    country: ctx.country,
    region: ctx.region,
    includeHidden: true,
  });
  const existing = existingResult.allPoints || existingResult.points || [];
  const preview = buildTravelInfraImportPreview(items, existing, ctx);
  const skipDuplicates = options.skipDuplicates !== false;
  const rowsToSave = filterCommitRecords(preview.preview, skipDuplicates);

  const skipped = preview.preview
    .filter((row) => row.valid && !rowsToSave.includes(row))
    .map((row) => ({ index: row.index, name: row.name, reason: row.duplicateStatus === "possible_duplicate" ? "duplicate" : "not_selected" }));

  if (!rowsToSave.length) {
    return {
      ok: false,
      error: "validation_failed",
      message: "No valid records to import",
      created: [],
      skipped,
      errors: preview.preview.filter((r) => !r.valid).map((r) => ({ index: r.index, errors: r.errors })),
    };
  }

  const tableName = await resolveTravelInfrastructureTableName(cfg.baseId, cfg.apiKey);
  const schema = await fetchAirtableTableFieldNameSet(cfg.baseId, cfg.apiKey, tableName);
  const created = [];
  const errors = [];

  for (const row of rowsToSave) {
    try {
      const fields = buildTravelInfraAirtableFields(row.normalized, { schema });
      const rec = await cfg.base(tableName).create(fields, { typecast: true });
      created.push({ id: rec.id, name: fields.Name || row.name });
    } catch (err) {
      if (isDev) console.error("[travel-infra-import]", err?.message);
      errors.push({ index: row.index, name: row.name, message: err?.message || String(err) });
    }
  }

  return {
    ok: created.length > 0,
    created,
    skipped,
    errors,
    summary: { submitted: items.length, created: created.length, skipped: skipped.length, failed: errors.length },
  };
}
