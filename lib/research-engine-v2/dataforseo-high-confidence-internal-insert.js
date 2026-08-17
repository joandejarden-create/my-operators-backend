/**
 * High-confidence DataForSEO new-hotel inserts — Census Only / Hold only.
 * Writes Hotel Property Census only. Never Brand Setup / Brand Explorer / VIC.
 * Never owner/operator/date / Company Validated / Brand Status / Recent Momentum.
 * Never direct DataForSEO coordinates on insert (Mapbox-after-address later).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";
import {
  assertProductionCensusWriteTarget,
  productionHotelPropertyCensus,
  BLOCKED_WRONG_CENSUS_TARGET,
} from "./production-census-source-of-truth.js";
import { isForbiddenAutopilotField } from "./census-autopilot-field-allowlist.js";
import { isStreetLevelAddress } from "./production-census-geocoding-providers.js";
import { isRejectedDiscoveryHost } from "./census-discovery-host-policy.js";
import { normalizePhoneNumber } from "./census-phone-number-enrichment.js";
import {
  INTERNAL_ONLY_INSERT_DEFAULTS,
  buildPhoneProvenanceNote,
} from "./census-confidence-tiered-internal-completion.js";
import {
  createHotelPropertyCensusRecords,
} from "./census-autopilot-discovery-insert-apply.js";
import {
  buildInsertReviewPack,
  resolveLatestCandidateInsertQueue,
} from "./dataforseo-new-hotel-insert-review-pack.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const DFS_HIGH_CONFIDENCE_INSERT_VERSION =
  "dataforseo-high-confidence-internal-insert-v1";

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || productionHotelPropertyCensus.tableId;

const INSERT_ALLOWED = new Set([
  "Property Name",
  "Canonical Property Name",
  "Property Identity Key",
  "Current Brand",
  "Brand Family",
  "City",
  "State / Region",
  "Country",
  "Market",
  "Address",
  "Address Confidence",
  "Address Source URL",
  "Official Property URL",
  "Source URL",
  "Source Type",
  "Source Confidence",
  "Identity Confidence",
  "Data Confidence Tier",
  "Phone",
  "Notes for Steward",
  "Production Use Status",
  "Public Display Review Status",
  "Radar Display Status",
  "Public Census Eligibility",
  "Human Review Required",
  "Enrichment Status",
  "Enrichment Priority",
  "Last Reviewed Date",
]);

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function slugPart(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 40);
}

function hostFromUrl(url) {
  try {
    return new URL(String(url || "")).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

/**
 * Select duplicate-safe high insert candidates from a review pack.
 */
export function selectHighConfidenceInsertCandidates(pack, opts = {}) {
  const max = Number(opts.maxInserts || 25);
  const rows = pack?.candidates || pack?.top_35 || [];
  const selected = [];
  const skipped = [];
  for (const c of rows) {
    if (c.recommended_action !== "approve_insert_high") {
      skipped.push({ name: c.candidate_name, reason: c.recommended_action });
      continue;
    }
    const near = c.duplicate_check_result?.near_duplicates || [];
    const matchClass = String(c.duplicate_check_result?.match_class || "");
    if (near.length > 0 || /match_high|match_medium/i.test(matchClass)) {
      skipped.push({ name: c.candidate_name, reason: "duplicate_risk" });
      continue;
    }
    if (!c.address && c.latitude_candidate == null) {
      skipped.push({ name: c.candidate_name, reason: "missing_address_and_coords" });
      continue;
    }
    if (!/hotel|resort|inn|lodge|suites/i.test(String(c.category || c.candidate_name || ""))) {
      skipped.push({ name: c.candidate_name, reason: "category_not_hotel" });
      continue;
    }
    selected.push(c);
    if (selected.length >= max) break;
  }
  return { selected, skipped };
}

/**
 * Build Census Only / Hold insert fields from a review-pack candidate.
 * Does not write DataForSEO coordinates (Mapbox-after-address later).
 */
export function buildInternalCensusOnlyInsertFields(candidate, opts = {}) {
  const name = String(candidate.candidate_name || "").trim();
  const country = String(candidate.country || "").trim() || "Unknown";
  const city = String(candidate.city || "").trim() || "Unknown";
  const placeId = candidate.place_id || candidate.external_id || "";
  const identityKey =
    opts.identityKey ||
    (placeId
      ? `dfs_maps_${String(placeId).replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 48)}`
      : `dfs_new_${slugPart(name)}_${slugPart(city)}_${slugPart(country)}`);

  const website = String(candidate.website_or_source_url || "").trim();
  const websiteOk =
    website && !isRejectedDiscoveryHost(hostFromUrl(website));
  const address = String(candidate.address || "").trim();
  const streetOk = address && isStreetLevelAddress(address);

  /** @type {Record<string, unknown>} */
  const fields = {
    "Property Name": name,
    "Canonical Property Name": name,
    "Property Identity Key": identityKey,
    City: city,
    Country: country,
    Market: candidate.market || undefined,
    ...INTERNAL_ONLY_INSERT_DEFAULTS,
    "Last Reviewed Date": todayIsoDate(),
    "Identity Confidence": "Medium",
    "Notes for Steward": [
      "insert_provenance",
      "source=dataforseo_google_maps",
      `place_id=${placeId || "n/a"}`,
      `review_action=${candidate.recommended_action}`,
      `ranking_score=${candidate.ranking_score ?? "n/a"}`,
      "public_exposure=false",
    ].join(" | "),
  };

  if (streetOk) {
    fields.Address = address;
    fields["Address Confidence"] = "Medium";
    if (websiteOk) fields["Address Source URL"] = website;
  }
  if (websiteOk) {
    fields["Official Property URL"] = website;
    fields["Source URL"] = website;
  }

  if (opts.allowPhone === true) {
    // Phone from insert pack is still held by default unless explicitly enabled
    // and present — prefer enrich pass for phone Medium writes.
  }

  // Strip undefined
  for (const [k, v] of Object.entries(fields)) {
    if (v === undefined || v === null || v === "") delete fields[k];
    if (isForbiddenAutopilotField(k) || !INSERT_ALLOWED.has(k)) delete fields[k];
  }

  return {
    ok: Boolean(fields["Property Name"] && fields["Property Identity Key"]),
    fields,
    identity_key: identityKey,
  };
}

/**
 * Apply high-confidence internal inserts from latest review pack / queue.
 */
export async function applyDataForSeoHighConfidenceInternalInserts(opts = {}) {
  const env = opts.env || process.env;
  const log = opts.log || (() => {});
  const insertsEnabled =
    String(env.ENABLE_DATAFORSEO_LOCAL_INSERTS || "0").trim() === "1" &&
    String(env.ENABLE_HIGH_CONFIDENCE_INSERTS || "0").trim() === "1";
  if (!insertsEnabled) {
    return {
      ok: true,
      inserts: 0,
      reason: "insert_flags_off",
      selected: 0,
      skipped: [],
    };
  }

  const sot = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!sot.ok) {
    return { ok: false, inserts: 0, error: BLOCKED_WRONG_CENSUS_TARGET };
  }

  const queuePath =
    opts.queuePath ||
    resolveLatestCandidateInsertQueue({ runDir: opts.runDir, root: ROOT });
  if (!queuePath || !fs.existsSync(queuePath)) {
    return { ok: false, inserts: 0, error: "candidate_queue_missing" };
  }
  const raw = JSON.parse(fs.readFileSync(queuePath, "utf8"));
  const pack = buildInsertReviewPack(raw, { sourceQueuePath: queuePath });
  const { selected, skipped } = selectHighConfidenceInsertCandidates(pack, {
    maxInserts: opts.maxInserts || 25,
  });

  const existingKeys = new Set(
    (opts.censusRecords || []).map(
      (r) => String(r.fields?.["Property Identity Key"] || "").trim()
    ).filter(Boolean)
  );
  const existingNames = new Set(
    (opts.censusRecords || []).map((r) =>
      String(r.fields?.["Canonical Property Name"] || r.fields?.["Property Name"] || "")
        .trim()
        .toLowerCase()
    )
  );

  const payloads = [];
  for (const c of selected) {
    const built = buildInternalCensusOnlyInsertFields(c, {
      allowPhone: false,
    });
    if (!built.ok) {
      skipped.push({ name: c.candidate_name, reason: "build_failed" });
      continue;
    }
    if (existingKeys.has(built.identity_key)) {
      skipped.push({ name: c.candidate_name, reason: "identity_key_exists" });
      continue;
    }
    const nm = String(built.fields["Property Name"] || "").toLowerCase();
    if (existingNames.has(nm)) {
      skipped.push({ name: c.candidate_name, reason: "property_name_exists" });
      continue;
    }
    payloads.push({ candidate: c, fields: built.fields, identity_key: built.identity_key });
    existingKeys.add(built.identity_key);
  }

  if (!opts.enableWrites) {
    return {
      ok: true,
      inserts: 0,
      dry_run: true,
      selected: payloads.length,
      skipped,
      preview: payloads.slice(0, 10).map((p) => ({
        name: p.fields["Property Name"],
        identity_key: p.identity_key,
        production_use: p.fields["Production Use Status"],
        human_review: p.fields["Human Review Required"],
      })),
    };
  }

  const bases = resolveTargetBase(env);
  const token = resolvePat(env);
  const created = await createHotelPropertyCensusRecords(
    bases.target_base_id,
    token,
    payloads.map((p) => ({ fields: p.fields }))
  );

  log(
    `[dfs-insert] created=${created.created?.length || 0} selected=${payloads.length} skipped=${skipped.length}`
  );

  return {
    ok: true,
    inserts: created.created?.length || 0,
    selected: payloads.length,
    skipped,
    created_record_ids: (created.created || []).map((r) => r.id).filter(Boolean),
    production_target: {
      base: "Deal Capture Platform",
      table: "Hotel Property Census",
      table_id: CENSUS_TABLE_ID,
    },
    brand_setup_writes: 0,
    brand_explorer_writes: 0,
    version: DFS_HIGH_CONFIDENCE_INSERT_VERSION,
  };
}
