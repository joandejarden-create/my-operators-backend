/**
 * Schema ensure — Rooms Evidence Tier on Hotel Property Census only.
 * Schema create only. No record writes. No Brand Setup / Brand Explorer.
 */

import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";
import { productionHotelPropertyCensus } from "./production-census-source-of-truth.js";

export const ROOMS_EVIDENCE_TIER_SCHEMA_VERSION =
  "production-census-rooms-evidence-tier-schema-v1";

export const ROOMS_EVIDENCE_TIER_FIELD = "Rooms Evidence Tier";

/** Founder Wave 2 allowed select values. */
export const ROOMS_EVIDENCE_TIER_OPTIONS = Object.freeze([
  "Tier 1 Official Parent / Brand Source",
  "Tier 2 Official Hotel Website",
  "Tier 3 Official Press Release",
  "Tier 4 Owner / Developer Source",
  "Tier 5 Tourism Board / Destination Authority",
  "Tier 6 Trusted Industry Source",
  "Tier 7 Steward Verified",
]);

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || productionHotelPropertyCensus.tableId;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function metaFetch(baseId, token, path, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${path}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

/**
 * Map internal evidence_tier codes → Airtable select labels.
 * @param {string} code
 */
export function mapEvidenceTierCodeToSelect(code) {
  const c = String(code || "");
  if (
    c === "official_high" ||
    c === "official_parent_brand" ||
    /tier.?1|official_parent|official_brand|official_factsheet|official_html|official_marriott/i.test(
      c
    )
  ) {
    return ROOMS_EVIDENCE_TIER_OPTIONS[0];
  }
  if (
    c === "official_hotel_website" ||
    /tier.?2|official_hotel_website|official_property/i.test(c)
  ) {
    return ROOMS_EVIDENCE_TIER_OPTIONS[1];
  }
  if (/tier.?3|press_release|opening_announcement/i.test(c)) {
    return ROOMS_EVIDENCE_TIER_OPTIONS[2];
  }
  if (/tier.?4|owner_developer|reit|asset_manager/i.test(c)) {
    return ROOMS_EVIDENCE_TIER_OPTIONS[3];
  }
  if (
    c.includes("tourism_board") ||
    /tier.?5|destination_authority|government_rnt|colombia_rnt|mincetur/i.test(c)
  ) {
    return ROOMS_EVIDENCE_TIER_OPTIONS[4];
  }
  if (/tier.?6|trade_publication|trusted_industry|licensed_hospitality/i.test(c)) {
    return ROOMS_EVIDENCE_TIER_OPTIONS[5];
  }
  if (/tier.?7|steward|conflict_steward/i.test(c)) {
    return ROOMS_EVIDENCE_TIER_OPTIONS[6];
  }
  return null;
}

/**
 * Ensure Rooms Evidence Tier exists (create if missing).
 * @param {{ dryRun?: boolean, env?: NodeJS.ProcessEnv, log?: Function }} [opts]
 */
export async function ensureRoomsEvidenceTierField(opts = {}) {
  const log = opts.log || (() => {});
  const dryRun = opts.dryRun !== false && opts.apply !== true;
  const apply = opts.apply === true;
  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!bases.target_base_id || !token) {
    return {
      ok: false,
      status: "blocked_missing_credentials",
      field: ROOMS_EVIDENCE_TIER_FIELD,
    };
  }

  const { res, json } = await metaFetch(bases.target_base_id, token, "/tables");
  if (!res.ok) {
    return {
      ok: false,
      status: "meta_list_failed",
      error: json.error || json,
    };
  }
  const census = (json.tables || []).find(
    (t) => t.id === CENSUS_TABLE_ID || t.name === "Hotel Property Census"
  );
  if (!census) {
    return { ok: false, status: "census_table_not_found" };
  }

  const existing = (census.fields || []).find(
    (f) => f.name === ROOMS_EVIDENCE_TIER_FIELD
  );
  if (existing) {
    const choices = (existing.options?.choices || []).map((c) => c.name);
    return {
      ok: true,
      status: "already_exists",
      field: ROOMS_EVIDENCE_TIER_FIELD,
      field_id: existing.id,
      type: existing.type,
      choices,
      created: false,
      dry_run: dryRun,
      table_id: census.id,
    };
  }

  const spec = {
    name: ROOMS_EVIDENCE_TIER_FIELD,
    type: "singleSelect",
    description:
      "Evidence tier for Rooms / Keys provenance (Wave 2 founder tiers). Blank until rooms write.",
    options: {
      choices: ROOMS_EVIDENCE_TIER_OPTIONS.map((name) => ({ name })),
    },
  };

  if (!apply) {
    return {
      ok: true,
      status: "dry_run_would_create",
      field: ROOMS_EVIDENCE_TIER_FIELD,
      created: false,
      dry_run: true,
      spec,
      table_id: census.id,
    };
  }

  let attempt = 0;
  while (attempt < 5) {
    attempt += 1;
    const created = await metaFetch(
      bases.target_base_id,
      token,
      `/tables/${encodeURIComponent(census.id)}/fields`,
      { method: "POST", body: JSON.stringify(spec) }
    );
    if (created.res.status === 429) {
      await sleep(1000 * attempt);
      continue;
    }
    if (!created.res.ok) {
      return {
        ok: false,
        status: "create_failed",
        error: created.json.error || created.json,
        http_status: created.res.status,
      };
    }
    log?.(
      `[rooms-evidence-tier] created field ${ROOMS_EVIDENCE_TIER_FIELD} id=${created.json.id}`
    );
    return {
      ok: true,
      status: "created",
      field: ROOMS_EVIDENCE_TIER_FIELD,
      field_id: created.json.id,
      type: "singleSelect",
      choices: ROOMS_EVIDENCE_TIER_OPTIONS.slice(),
      created: true,
      dry_run: false,
      table_id: census.id,
    };
  }

  return { ok: false, status: "create_rate_limited" };
}
