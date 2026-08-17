/**
 * Light Census processing gates for Autopilot (not full Brand Explorer PVQL).
 * Full BE gates run only after production apply when requested.
 */

export const CENSUS_PROCESSING_GATES = Object.freeze({
  no_city_centroid: true,
  no_zero_zero_coords: true,
  no_owner_operator_writes: true,
  no_date_writes: true,
  no_brand_explorer_writes: true,
  no_company_validated: true,
  no_brand_verified: true,
  no_recent_momentum: true,
  no_webhound_direct_writes: true,
  high_confidence_only_on_apply: true,
  held_records_excluded_from_writes: true,
  brand_unconfirmed_excluded_from_writes: true,
});

/**
 * Evaluate lightweight Autopilot gates against a run context.
 * @param {{
 *   patches?: Array<{ fields?: object, patch?: object }>,
 *   heldIds?: Set<string>|string[],
 *   brandUnconfirmedIds?: Set<string>|string[],
 *   geocodeProviderReady?: boolean,
 * }} ctx
 */
export function evaluateCensusProcessingGates(ctx = {}) {
  const failures = [];
  const held = new Set(ctx.heldIds || []);
  const unconfirmed = new Set(ctx.brandUnconfirmedIds || []);

  for (const p of ctx.patches || []) {
    const fields = p.patch || p.fields || {};
    const id = p.record_id || p.id;
    if (id && held.has(id)) failures.push({ gate: "held_records_excluded_from_writes", record_id: id });
    if (id && unconfirmed.has(id)) {
      failures.push({ gate: "brand_unconfirmed_excluded_from_writes", record_id: id });
    }
    if (fields["Company Validated"] != null) failures.push({ gate: "no_company_validated", field: "Company Validated" });
    if (fields["Brand Verified"] != null) failures.push({ gate: "no_brand_verified", field: "Brand Verified" });
    if (fields["Recent Momentum"] != null) failures.push({ gate: "no_recent_momentum", field: "Recent Momentum" });
    if (fields["Owner Name"] != null || fields["Operator / Management Company"] != null) {
      failures.push({ gate: "no_owner_operator_writes" });
    }
    if (
      fields["Opening Date"] != null ||
      fields["Renovation Date"] != null ||
      fields["Affiliation Start Date"] != null
    ) {
      failures.push({ gate: "no_date_writes" });
    }
    const lat = Number(fields.Latitude);
    const lng = Number(fields.Longitude);
    if (fields.Latitude != null && fields.Longitude != null) {
      if (lat === 0 && lng === 0) failures.push({ gate: "no_zero_zero_coords" });
      if (p.city_centroid) failures.push({ gate: "no_city_centroid" });
    }
    if (p.source === "webhound") failures.push({ gate: "no_webhound_direct_writes" });
  }

  return {
    ok: failures.length === 0,
    failures,
    gates: CENSUS_PROCESSING_GATES,
    note: "Full Brand Explorer gates are NOT run on Census dry-run; run after production apply only",
  };
}

/**
 * Whether to run full Brand Explorer gates (expensive).
 * @param {{ mode: string, afterApply?: boolean, force?: boolean }} opts
 */
export function shouldRunFullBrandExplorerGates(opts = {}) {
  if (opts.force) return true;
  if (opts.mode === "apply" && opts.afterApply) return true;
  return false;
}
