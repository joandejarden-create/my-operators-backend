/**
 * Peru MINCETUR steward review pack — curated tiers for human review before insert.
 *
 * No Airtable writes. Ownership signals stay sidecar (never Owner Name).
 */

import { MAP_PERU_MINCETUR } from "./peru-mincetur-open-data-adapter.js";
import { PERU_MINCETUR_PLAN_DECISIONS } from "./peru-mincetur-hpc-match-plan.js";

export const PERU_MINCETUR_STEWARD_PACK_VERSION = "peru-mincetur-steward-review-pack-v1";

export const PERU_MINCETUR_STEWARD_TIERS = Object.freeze({
  A: "A_official_url_rooms_ruc",
  B: "B_official_url_ruc",
  C: "C_ruc_inventory_only",
});

export const PERU_MINCETUR_REQUIRED_APPLY_CONFIRMS = Object.freeze([
  "--confirm-peru-mincetur-steward-insert",
  "--confirm-no-owner-operator-writes",
  "--confirm-hotel-property-census-only",
  "--confirm-no-legacy-census-writes",
]);

/**
 * @param {object} row — buildPeruMinceturHpcPlan row
 */
export function classifyPeruMinceturStewardTier(row = {}) {
  if (row.decision !== PERU_MINCETUR_PLAN_DECISIONS.STEWARD_HOLD_INSERT_CANDIDATE) {
    return null;
  }
  const hasUrl = Boolean(row.official_property_url);
  const hasRooms = row.rooms != null && Number(row.rooms) > 0;
  const hasRuc = Boolean(row.ruc_signal);
  if (hasUrl && hasRooms && hasRuc) return PERU_MINCETUR_STEWARD_TIERS.A;
  if (hasUrl && hasRuc) return PERU_MINCETUR_STEWARD_TIERS.B;
  if (hasRuc) return PERU_MINCETUR_STEWARD_TIERS.C;
  return null;
}

/**
 * Prefer larger hotels with commercial names for pilot readability.
 * @param {object} a
 * @param {object} b
 */
export function comparePeruMinceturStewardRows(a, b) {
  const roomsA = Number(a.rooms) || 0;
  const roomsB = Number(b.rooms) || 0;
  if (roomsB !== roomsA) return roomsB - roomsA;
  return String(a.property_name || "").localeCompare(String(b.property_name || ""));
}

/**
 * @param {object[]} planRows — from buildPeruMinceturHpcPlan().rows
 * @param {{ pilotLimit?: number, pilotTier?: string }} [opts]
 */
export function buildPeruMinceturStewardReviewPack(planRows = [], opts = {}) {
  const pilotLimit = Math.max(1, Number(opts.pilotLimit) || 25);
  const pilotTierKey = opts.pilotTier || PERU_MINCETUR_STEWARD_TIERS.A;

  /** @type {Record<string, object[]>} */
  const tiers = {
    [PERU_MINCETUR_STEWARD_TIERS.A]: [],
    [PERU_MINCETUR_STEWARD_TIERS.B]: [],
    [PERU_MINCETUR_STEWARD_TIERS.C]: [],
  };

  const skipped = [];
  for (const row of planRows) {
    const tier = classifyPeruMinceturStewardTier(row);
    if (!tier) {
      skipped.push({
        identity_key: row.identity_key,
        decision: row.decision,
        reason: "not_steward_insert_candidate_or_missing_ruc",
      });
      continue;
    }
    tiers[tier].push({
      ...row,
      steward_tier: tier,
      insert_payload_preview: row.insert_payload_preview || null,
    });
  }

  for (const key of Object.keys(tiers)) {
    tiers[key].sort(comparePeruMinceturStewardRows);
  }

  const pilotPool = tiers[pilotTierKey] || [];
  const pilotRows = pilotPool.slice(0, pilotLimit);
  const proposedInserts = pilotRows.map((r) => ({
    identity_key: r.identity_key,
    nro_certificado: r.nro_certificado,
    steward_tier: r.steward_tier,
    property_name: r.property_name,
    city: r.city,
    state: r.state,
    rooms: r.rooms,
    ruc_signal: r.ruc_signal,
    official_property_url: r.official_property_url,
    hpc_recommended_action: r.hpc_recommended_action,
    fields: r.insert_payload_preview?.fields || {},
    ownership_signal: r.insert_payload_preview?.ownership_signal || {
      tax_id: r.ruc_signal,
      lane: "ownership_enrichment_blocked",
    },
    field_mapping: MAP_PERU_MINCETUR,
  }));

  const base = {
    version: PERU_MINCETUR_STEWARD_PACK_VERSION,
    type: "peru_mincetur_steward_review_pack",
    dry_run: true,
    airtable_writes: false,
    ownership_writes: false,
    generated_at: new Date().toISOString(),
    recommended_pilot: {
      tier: pilotTierKey,
      limit: pilotLimit,
      count: proposedInserts.length,
      identity_keys: proposedInserts.map((p) => p.identity_key),
      rationale:
        "Tier A prefers Official Property URL + Rooms/Keys + RUC signal for highest steward confidence before first insert batch",
    },
    summary: {
      plan_rows: planRows.length,
      tier_a: tiers[PERU_MINCETUR_STEWARD_TIERS.A].length,
      tier_b: tiers[PERU_MINCETUR_STEWARD_TIERS.B].length,
      tier_c: tiers[PERU_MINCETUR_STEWARD_TIERS.C].length,
      skipped: skipped.length,
      pilot_proposed_inserts: proposedInserts.length,
    },
    tiers: {
      [PERU_MINCETUR_STEWARD_TIERS.A]: tiers[PERU_MINCETUR_STEWARD_TIERS.A].map(summarizeStewardRow),
      [PERU_MINCETUR_STEWARD_TIERS.B]: tiers[PERU_MINCETUR_STEWARD_TIERS.B].map(summarizeStewardRow),
      [PERU_MINCETUR_STEWARD_TIERS.C]: tiers[PERU_MINCETUR_STEWARD_TIERS.C].map(summarizeStewardRow),
    },
    proposed_inserts: proposedInserts,
    skipped_sample: skipped.slice(0, 25),
  };
  return finalizePeruMinceturStewardPack(base);
}

function summarizeStewardRow(r) {
  return {
    identity_key: r.identity_key,
    nro_certificado: r.nro_certificado,
    property_name: r.property_name,
    city: r.city,
    state: r.state,
    rooms: r.rooms,
    ruc_signal: r.ruc_signal,
    official_property_url: r.official_property_url,
    hpc_recommended_action: r.hpc_recommended_action,
    decision: r.decision,
    steward_tier: r.steward_tier,
  };
}

/**
 * Render steward review markdown.
 * @param {object} pack
 */
export function renderPeruMinceturStewardReviewMarkdown(pack) {
  const s = pack.summary || {};
  const pilot = pack.recommended_pilot || {};
  const sample = (pack.proposed_inserts || []).slice(0, 25);
  const tierA = (pack.tiers?.[PERU_MINCETUR_STEWARD_TIERS.A] || []).slice(0, 40);
  return [
    `# Peru MINCETUR Steward Review Pack`,
    ``,
    `**Status:** \`peru_mincetur_steward_review_pack_ready\``,
    `**Generated:** ${pack.generated_at}`,
    `**Version:** \`${pack.version}\``,
    `**Airtable writes:** none`,
    `**Owner Name writes:** none (RUC on ownership_signal only)`,
    ``,
    `## Summary`,
    ``,
    `| Bucket | Count |`,
    `| --- | ---: |`,
    `| Plan rows | ${s.plan_rows ?? 0} |`,
    `| Tier A (URL + rooms + RUC) | ${s.tier_a ?? 0} |`,
    `| Tier B (URL + RUC) | ${s.tier_b ?? 0} |`,
    `| Tier C (RUC inventory only) | ${s.tier_c ?? 0} |`,
    `| Recommended pilot | ${pilot.count ?? 0} (tier \`${pilot.tier || ""}\`, limit ${pilot.limit ?? 0}) |`,
    ``,
    `## Recommended pilot (steward approve before apply)`,
    ``,
    `${pilot.rationale || ""}`,
    ``,
    `| Identity | Name | City | Rooms | RUC | URL |`,
    `| --- | --- | --- | ---: | --- | --- |`,
    ...sample.map(
      (r) =>
        `| ${r.identity_key || ""} | ${r.property_name || ""} | ${r.city || ""} | ${r.rooms ?? ""} | ${r.ruc_signal || ""} | ${r.official_property_url || ""} |`
    ),
    ``,
    `## Tier A full sample (up to 40)`,
    ``,
    `| Identity | Name | City | Rooms | RUC |`,
    `| --- | --- | --- | ---: | --- |`,
    ...tierA.map(
      (r) =>
        `| ${r.identity_key || ""} | ${r.property_name || ""} | ${r.city || ""} | ${r.rooms ?? ""} | ${r.ruc_signal || ""} |`
    ),
    ``,
    `## Apply path (dry-run first)`,
    ``,
    `\`\`\`bash`,
    `npm run census:peru-mincetur-steward-insert-apply -- --pack <this-pack.json>`,
    `npm run census:peru-mincetur-steward-insert-apply -- --pack <this-pack.json> --enable-production-writes \\`,
    `  --confirm-peru-mincetur-steward-insert \\`,
    `  --confirm-no-owner-operator-writes \\`,
    `  --confirm-hotel-property-census-only \\`,
    `  --confirm-no-legacy-census-writes`,
    `\`\`\``,
    ``,
    `## Field mapping`,
    ``,
    `- \`MAP_PERU_MINCETUR\` inventory fields only`,
    `- Identity: \`gov_pe_mincetur_{NRO_CERTIFICADO}\``,
    `- RUC never maps to Owner Name`,
    ``,
  ].join("\n");
}

/**
 * Attach approval bundle + confirms onto pack.
 * @param {object} pack
 */
export function finalizePeruMinceturStewardPack(pack) {
  const next = { ...pack };
  next.approval_bundle = {
    ...(pack.approval_bundle || {}),
    type: "peru_mincetur_steward_insert_approval_bundle",
    version: PERU_MINCETUR_STEWARD_PACK_VERSION,
    queue: "peru_mincetur_steward_insert",
    dry_run_default: true,
    required_apply_confirms: [...PERU_MINCETUR_REQUIRED_APPLY_CONFIRMS],
    proposed_inserts: pack.proposed_inserts || [],
  };
  return next;
}
