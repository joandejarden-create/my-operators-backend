/**
 * DataForSEO discovery pilot v2 — stricter official source validation.
 * Candidate-only (no Hotel Property Census writes). DataForSEO ≠ SoT.
 *
 * Env:
 *   DATAFORSEO_ENABLED=1
 *   DATAFORSEO_WRITE_CANDIDATES_ONLY=1
 *   ENABLE_DATAFORSEO_VALIDATED_WRITES=0
 *   DATAFORSEO_LOGIN / DATAFORSEO_PASSWORD
 *   DATAFORSEO_MAX_RECORDS=200
 *   DATAFORSEO_MAX_QUERIES_PER_RECORD=6
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";
import { productionHotelPropertyCensus } from "./production-census-source-of-truth.js";
import {
  resolveDataForSeoCredentials,
  resolveDataForSeoLocationName,
  fetchGoogleOrganicLive,
  fetchGoogleMapsLive,
} from "./dataforseo-client.js";
import {
  classifySerpOrMapsItem,
  summarizeClassifiedCandidates,
  estimateOfficialUrlPrecision,
  SOURCE_TIER,
} from "./dataforseo-candidate-classifier.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const DATAFORSEO_DISCOVERY_PILOT_OBJECTIVE =
  "dataforseo-discovery-pilot-v2";
export const DATAFORSEO_DISCOVERY_PILOT_VERSION =
  "dataforseo-discovery-pilot-v2";

/** @deprecated v1 alias kept for older imports */
export const DATAFORSEO_DISCOVERY_PILOT_VERSION_V1 =
  "dataforseo-discovery-pilot-v1";

export const DATAFORSEO_DISCOVERY_STATUS = Object.freeze({
  COMPLETE: "production_census_dataforseo_discovery_pilot_v2_complete",
  PARTIAL_POLICY:
    "production_census_dataforseo_discovery_pilot_v2_partial_policy_decision_needed",
  PARTIAL_SOURCE:
    "production_census_dataforseo_discovery_pilot_v2_partial_source_remaining",
  BLOCKED: "production_census_dataforseo_discovery_pilot_v2_blocked",
});

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || productionHotelPropertyCensus.tableId;

const READ_FIELDS = [
  "Property Identity Key",
  "Property Name",
  "Canonical Property Name",
  "Current Brand",
  "Brand Family",
  "Country",
  "City",
  "Address",
  "Phone",
  "Rooms / Keys",
  "Official Property URL",
  "Latitude",
  "Longitude",
];

const SCALE_UNIVERSE = 1224;

const BRAND_SITE_DOMAINS = [
  { re: /marriott|city express|design hotels|courtyard|sheraton|westin|autograph|moxy|aloft|element/i, domain: "marriott.com" },
  { re: /ihg|holiday inn|crowne plaza|intercontinental|staybridge|candlewood|voco|kimpton/i, domain: "ihg.com" },
  { re: /hilton|hampton|doubletree|embassy|homewood|home2|curio|tapestry|lxr|waldorf/i, domain: "hilton.com" },
  { re: /choice|comfort|quality|sleep inn|clarion|cambria|ascend|radisson/i, domain: "choicehotels.com" },
  { re: /accor|ibis|novotel|mercure|sofitel|pullman|mgallery|fairmont|swissôtel|swissotel|mövenpick|movenpick/i, domain: "all.accor.com" },
  { re: /wyndham|ramada|days inn|super 8|la quinta|travelodge|microtel|trademark|wingate|hawthorn/i, domain: "wyndhamhotels.com" },
  { re: /preferred/i, domain: "preferredhotels.com" },
  { re: /hyatt/i, domain: "hyatt.com" },
];

function isBlank(v) {
  return v == null || !String(v).trim();
}

function writeJson(filePath, data) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function writeMd(filePath, md) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, md.endsWith("\n") ? md : `${md}\n`, "utf8");
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * @param {NodeJS.ProcessEnv} [env]
 */
export function assertDataForSeoCandidateOnlyMode(env = process.env) {
  const candidatesOnly =
    String(env.DATAFORSEO_WRITE_CANDIDATES_ONLY || "0").trim() === "1";
  const validatedWrites =
    String(env.ENABLE_DATAFORSEO_VALIDATED_WRITES || "0").trim() === "1";
  const enabled = String(env.DATAFORSEO_ENABLED || "0").trim() === "1";
  const blockers = [];
  if (!enabled) blockers.push("DATAFORSEO_ENABLED_not_1");
  if (!candidatesOnly) blockers.push("DATAFORSEO_WRITE_CANDIDATES_ONLY_must_be_1");
  if (validatedWrites) {
    blockers.push("ENABLE_DATAFORSEO_VALIDATED_WRITES_must_be_0_for_candidate_pilot");
  }
  return {
    ok: blockers.length === 0,
    blockers,
    candidates_only: candidatesOnly,
    validated_writes: validatedWrites,
    census_writes_allowed: false,
    dataforseo_is_source_of_truth: false,
  };
}

/**
 * Priority score — lower band = higher priority.
 * v2 order: 1 URL, 2 Rooms, 3 Address, 4 Phone, 5 Lat/Long
 */
export function scoreIncompletePriority(fields = {}) {
  let score = 1000;
  const missingUrl = isBlank(fields["Official Property URL"]);
  const missingRooms = isBlank(fields["Rooms / Keys"]);
  const missingAddress = isBlank(fields.Address);
  const missingPhone = isBlank(fields.Phone);
  const missingGeo =
    isBlank(fields.Latitude) || isBlank(fields.Longitude);

  if (missingUrl) score = Math.min(score, 1);
  if (missingRooms) score = Math.min(score, 2);
  if (missingAddress) score = Math.min(score, 3);
  if (missingPhone) score = Math.min(score, 4);
  if (missingGeo) score = Math.min(score, 5);

  const gapCount = [
    missingUrl,
    missingRooms,
    missingAddress,
    missingPhone,
    missingGeo,
  ].filter(Boolean).length;

  return {
    priority_band: score === 1000 ? 99 : score,
    gap_count: gapCount,
    missing: {
      official_property_url: missingUrl,
      rooms: missingRooms,
      address: missingAddress,
      phone: missingPhone,
      lat_long: missingGeo,
    },
    sort_key: (score === 1000 ? 99 : score) * 10 - gapCount,
  };
}

/**
 * Parent priority: Choice → Marriott → IHG → Accor → Wyndham → Preferred → evidence-backed → other
 * Lower = higher priority.
 */
export function scoreParentPriority(fields = {}) {
  const hay = [fields["Brand Family"], fields["Current Brand"]]
    .map((x) => String(x || ""))
    .join(" ")
    .toLowerCase();

  if (/choice/.test(hay)) return { parent_band: 1, parent_label: "Choice" };
  if (
    /marriott|city express|design hotels|autograph|courtyard|residence inn|fairfield|sheraton|westin|le m[eé]ridien|st\.?\s*regis|moxy|aloft|element|four points|tribute|edition|ritz/.test(
      hay
    )
  ) {
    return { parent_band: 2, parent_label: "Marriott" };
  }
  if (
    /ihg|holiday inn|crowne plaza|intercontinental|staybridge|candlewood|avid|voco|kimpton|regent|six senses|even hotels|hualuxe/.test(
      hay
    )
  ) {
    return { parent_band: 3, parent_label: "IHG" };
  }
  if (
    /accor|ibis|novotel|mercure|sofitel|pullman|mgallery|fairmont|swissôtel|swissotel|movenpick|mövenpick|raffles/.test(
      hay
    )
  ) {
    return { parent_band: 4, parent_label: "Accor" };
  }
  if (
    /wyndham|ramada|days inn|super 8|la quinta|travelodge|microtel|trademark|dolce|wingate|hawthorn/.test(
      hay
    )
  ) {
    return { parent_band: 5, parent_label: "Wyndham" };
  }
  if (/preferred/.test(hay)) {
    return { parent_band: 6, parent_label: "Preferred" };
  }
  if (String(fields["Brand Family"] || fields["Current Brand"] || "").trim()) {
    return { parent_band: 7, parent_label: "evidence_backed_other" };
  }
  return { parent_band: 8, parent_label: "unbranded_or_unknown" };
}

export function resolveBrandSiteDomain(fields = {}) {
  const hay = [fields["Brand Family"], fields["Current Brand"]]
    .map((x) => String(x || ""))
    .join(" ");
  for (const row of BRAND_SITE_DOMAINS) {
    if (row.re.test(hay)) return row.domain;
  }
  return null;
}

function languageForCountry(country) {
  const c = String(country || "").toLowerCase();
  if (c === "brazil" || c === "brasil") return "pt";
  if (
    /mexico|colombia|dominican|panama|costa rica|peru|chile|argentina|ecuador|guatemala|honduras|nicaragua|bolivia|paraguay|uruguay|venezuela|cuba|puerto rico/.test(
      c
    )
  ) {
    return "es";
  }
  return "en";
}

/**
 * Build multilingual discovery queries (capped).
 * @param {object} fields
 * @param {{ maxQueries?: number, enableSerp?: boolean, enableMaps?: boolean, mapsDepth?: number }} [opts]
 */
export function buildDiscoveryQueriesForRecord(fields, opts = {}) {
  const maxQueries = Math.max(1, Number(opts.maxQueries || 6));
  const enableSerp = opts.enableSerp !== false;
  const enableMaps = opts.enableMaps !== false;
  const mapsDepth = Number(opts.mapsDepth || 20);
  const name =
    String(fields["Canonical Property Name"] || fields["Property Name"] || "").trim();
  const city = String(fields.City || "").trim();
  const country = String(fields.Country || "").trim();
  const location_name = resolveDataForSeoLocationName({ city, country });
  const pri = scoreIncompletePriority(fields);
  const lang = languageForCountry(country);
  const brandDomain = resolveBrandSiteDomain(fields);
  /** @type {object[]} */
  const pool = [];

  if (!name) return { queries: [], priority: pri };

  const push = (q, weight) => {
    pool.push({ ...q, _weight: weight });
  };

  if (enableSerp && pri.missing.official_property_url) {
    push(
      {
        kind: "serp_organic",
        purpose: "official_hotel_url",
        keyword: `"${name}" official hotel`,
        location_name,
        language_code: "en",
      },
      10
    );
    push(
      {
        kind: "serp_organic",
        purpose: "official_website",
        keyword: `"${name}" official website`,
        location_name,
        language_code: "en",
      },
      11
    );
    push(
      {
        kind: "serp_organic",
        purpose: "sitio_oficial",
        keyword: `"${name}" sitio oficial`,
        location_name,
        language_code: lang === "pt" ? "pt" : "es",
      },
      12
    );
    push(
      {
        kind: "serp_organic",
        purpose: "pagina_oficial",
        keyword: `"${name}" página oficial`,
        location_name,
        language_code: lang === "pt" ? "pt" : "es",
      },
      13
    );
    push(
      {
        kind: "serp_organic",
        purpose: "hotel_oficial",
        keyword: `"${name}" hotel oficial`,
        location_name,
        language_code: lang === "pt" ? "pt" : "es",
      },
      14
    );
    push(
      {
        kind: "serp_organic",
        purpose: "official_site_geo",
        keyword: `"${name}" ${city} ${country} official site`.trim(),
        location_name,
        language_code: "en",
      },
      15
    );
    if (brandDomain) {
      push(
        {
          kind: "serp_organic",
          purpose: "site_brand_domain",
          keyword: `site:${brandDomain} "${name}"`,
          location_name,
          language_code: "en",
        },
        9
      );
    }
  }

  if (enableSerp && pri.missing.rooms) {
    push(
      {
        kind: "serp_organic",
        purpose: "rooms_en",
        keyword: `"${name}" rooms`,
        location_name,
        language_code: "en",
      },
      20
    );
    push(
      {
        kind: "serp_organic",
        purpose: "number_of_rooms",
        keyword: `"${name}" number of rooms`,
        location_name,
        language_code: "en",
      },
      21
    );
    push(
      {
        kind: "serp_organic",
        purpose: "hotel_keys",
        keyword: `"${name}" hotel keys`,
        location_name,
        language_code: "en",
      },
      22
    );
    push(
      {
        kind: "serp_organic",
        purpose: "habitaciones",
        keyword: `"${name}" habitaciones`,
        location_name,
        language_code: "es",
      },
      23
    );
    push(
      {
        kind: "serp_organic",
        purpose: "numero_habitaciones",
        keyword: `"${name}" número de habitaciones`,
        location_name,
        language_code: "es",
      },
      24
    );
    push(
      {
        kind: "serp_organic",
        purpose: "cuartos",
        keyword: `"${name}" cuartos`,
        location_name,
        language_code: "es",
      },
      25
    );
    push(
      {
        kind: "serp_organic",
        purpose: "quartos",
        keyword: `"${name}" quartos`,
        location_name,
        language_code: "pt",
      },
      26
    );
    push(
      {
        kind: "serp_organic",
        purpose: "apartamentos",
        keyword: `"${name}" apartamentos`,
        location_name,
        language_code: lang === "pt" ? "pt" : "es",
      },
      27
    );
    push(
      {
        kind: "serp_organic",
        purpose: "fact_sheet",
        keyword: `"${name}" fact sheet`,
        location_name,
        language_code: "en",
      },
      19
    );
    push(
      {
        kind: "serp_organic",
        purpose: "ficha_tecnica",
        keyword: `"${name}" ficha técnica`,
        location_name,
        language_code: "es",
      },
      19
    );
  }

  if (enableSerp && (pri.missing.address || pri.missing.phone)) {
    push(
      {
        kind: "serp_organic",
        purpose: "address_phone_en",
        keyword: `"${name}" address phone`,
        location_name,
        language_code: "en",
      },
      30
    );
    push(
      {
        kind: "serp_organic",
        purpose: "direccion_telefono",
        keyword: `"${name}" dirección teléfono`,
        location_name,
        language_code: "es",
      },
      31
    );
    push(
      {
        kind: "serp_organic",
        purpose: "contacto",
        keyword: `"${name}" contacto`,
        location_name,
        language_code: lang === "pt" ? "pt" : "es",
      },
      32
    );
    if (city) {
      push(
        {
          kind: "serp_organic",
          purpose: "city_direccion",
          keyword: `"${name}" ${city} dirección`,
          location_name,
          language_code: "es",
        },
        33
      );
      push(
        {
          kind: "serp_organic",
          purpose: "city_telefono",
          keyword: `"${name}" ${city} teléfono`,
          location_name,
          language_code: "es",
        },
        34
      );
    }
  }

  if (enableSerp && (pri.missing.rooms || pri.missing.official_property_url)) {
    push(
      {
        kind: "serp_organic",
        purpose: "registro_turismo",
        keyword: `"${name}" registro turismo`,
        location_name,
        language_code: "es",
      },
      40
    );
    push(
      {
        kind: "serp_organic",
        purpose: "rnt",
        keyword: `"${name}" RNT`,
        location_name,
        language_code: "es",
      },
      41
    );
    push(
      {
        kind: "serp_organic",
        purpose: "habitaciones_registro",
        keyword: `"${name}" habitaciones registro`,
        location_name,
        language_code: "es",
      },
      42
    );
    push(
      {
        kind: "serp_organic",
        purpose: "ministerio_turismo",
        keyword: `"${name}" ministerio turismo`,
        location_name,
        language_code: "es",
      },
      43
    );
    push(
      {
        kind: "serp_organic",
        purpose: "turismo_habitaciones",
        keyword: `"${name}" turismo habitaciones`,
        location_name,
        language_code: "es",
      },
      44
    );
  }

  if (
    enableMaps &&
    (pri.missing.phone ||
      pri.missing.address ||
      pri.missing.lat_long ||
      pri.missing.official_property_url)
  ) {
    push(
      {
        kind: "google_maps",
        purpose: "maps_local_match",
        keyword: `${name} ${city} ${country}`.trim(),
        location_name,
        language_code: lang === "pt" ? "pt" : lang === "es" ? "es" : "en",
        depth: mapsDepth,
      },
      5
    );
  }

  if (!pool.length && enableSerp) {
    push(
      {
        kind: "serp_organic",
        purpose: "general_discovery",
        keyword: `"${name}" hotel ${city} ${country}`.trim(),
        location_name,
        language_code: "en",
      },
      99
    );
  }

  pool.sort((a, b) => a._weight - b._weight);
  const selected = [];
  const seen = new Set();
  for (const q of pool) {
    const key = `${q.kind}|${q.keyword}|${q.language_code || ""}`;
    if (seen.has(key)) continue;
    seen.add(key);
    const { _weight, ...rest } = q;
    selected.push(rest);
    if (selected.length >= maxQueries) break;
  }

  return { queries: selected, priority: pri };
}

async function listIncompleteCensus(baseId, token, tableId, maxRecords) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of READ_FIELDS) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) {
      throw new Error(
        `census_list_failed:${res.status}:${json?.error?.message || ""}`
      );
    }
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);

  const scored = out
    .map((r) => {
      const pri = scoreIncompletePriority(r.fields || {});
      const parent = scoreParentPriority(r.fields || {});
      return {
        record: r,
        ...pri,
        ...parent,
        // field gaps first, then parent priority, then more gaps
        combined_sort:
          (pri.priority_band === 99 ? 99 : pri.priority_band) * 1000 +
          parent.parent_band * 100 -
          pri.gap_count,
      };
    })
    .filter((x) => x.priority_band < 99)
    .sort((a, b) => a.combined_sort - b.combined_sort);

  return scored.slice(0, maxRecords);
}

function pickTopCandidates(pool, n = 25) {
  return [...pool]
    .sort((a, b) => (b.match_confidence || 0) - (a.match_confidence || 0))
    .slice(0, n)
    .map((c) => ({
      hotel_name: c.hotel_name,
      record_id: c.record_id,
      url: c.url,
      host: c.host,
      source_tier: c.source_tier,
      categories: c.categories,
      match_confidence: c.match_confidence,
      title: c.title,
    }));
}

function renderPilotMd(report) {
  const lines = [
    `# DataForSEO Discovery Pilot v2`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Objective:** \`${report.objective}\``,
    `**Recommendation:** **${report.recommendation}**`,
    `**Generated:** ${report.generated_at}`,
    `**Mode:** candidates-only (no Hotel Property Census writes)`,
    `**Census mode:** field-completion-only`,
    `**Records piloted:** ${report.records_piloted}`,
    ``,
    `## Cost`,
    ``,
    `- Queries run: **${report.queries_run}**`,
    `- Estimated / reported API cost: **$${Number(report.estimated_cost_usd || 0).toFixed(4)}**`,
    `- Useful candidates: **${report.useful_candidates_found}**`,
    `- Trusted secondary (non-official): **${report.trusted_secondary_candidates_found || 0}**`,
    `- Cost per useful candidate: **${
      report.cost_per_useful_candidate == null
        ? "n/a"
        : `$${Number(report.cost_per_useful_candidate).toFixed(4)}`
    }**`,
    ``,
    `## Candidate yields`,
    ``,
    `- Official hotel URLs: **${report.official_hotel_urls_found}**`,
    `- Room evidence pages: **${report.rooms_evidence_pages_found}**`,
    `- Address candidates: **${report.address_candidates_found}**`,
    `- Phone candidates: **${report.phone_candidates_found}**`,
    `- Google Maps / local candidates: **${report.google_maps_candidates_found}**`,
    `- Lat/Long candidates: **${report.lat_long_candidates_found}**`,
    `- Tourism registry candidates: **${report.tourism_registry_candidates_found || 0}**`,
    ``,
    `## Source classifier`,
    ``,
    `- Precision estimate (official URL candidates): **${
      report.source_classifier_precision_estimate == null
        ? "n/a"
        : report.source_classifier_precision_estimate
    }**`,
    `- Brand-official URL candidates: **${report.precision_detail?.brand_official ?? 0}**`,
    `- Hotel-official URL candidates: **${report.precision_detail?.hotel_official ?? 0}**`,
    `- Classifier version: \`${report.classifier_version}\``,
    ``,
    `## Rejected sources by category`,
    ``,
  ];
  for (const [reason, n] of Object.entries(report.rejected_sources || {})) {
    lines.push(`- \`${reason}\`: ${n}`);
  }
  lines.push(
    ``,
    `## Source tier counts`,
    ``
  );
  for (const [tier, n] of Object.entries(report.source_tier_counts || {})) {
    lines.push(`- \`${tier}\`: ${n}`);
  }
  lines.push(
    ``,
    `## Safety`,
    ``,
    `- Census writes: **${report.census_writes}**`,
    `- Brand Setup / Brand Explorer writes: **0**`,
    `- DataForSEO treated as source of truth: **false**`,
    `- \`DATAFORSEO_WRITE_CANDIDATES_ONLY\`: ${report.gates?.candidates_only}`,
    `- \`ENABLE_DATAFORSEO_VALIDATED_WRITES\`: ${report.gates?.validated_writes}`,
    `- Google Maps: candidate-only (no writes)`,
    ``,
    `## Recommended write policy`,
    ``,
    report.recommended_write_policy || "",
    ``,
    `## Scale estimate (full ${SCALE_UNIVERSE} incomplete)`,
    ``,
    `- Estimated queries: **${report.scale_estimate?.estimated_queries ?? "n/a"}**`,
    `- Estimated cost: **$${Number(report.scale_estimate?.estimated_cost_usd || 0).toFixed(2)}**`,
    `- Estimated useful candidates: **${report.scale_estimate?.estimated_useful_candidates ?? "n/a"}**`,
    `- Estimated official URL candidates: **${report.scale_estimate?.estimated_official_urls ?? "n/a"}**`,
    ``,
    `## Recommendation`,
    ``,
    report.recommendation_notes || "",
    ``,
    `## Top 25 validated-looking official URLs`,
    ``
  );
  for (const row of report.top_25_official_urls || []) {
    lines.push(
      `- [${row.match_confidence}] ${row.hotel_name} · \`${row.source_tier}\` · ${row.url}`
    );
  }
  lines.push(``, `## Top 25 room evidence candidates`, ``);
  for (const row of report.top_25_rooms_evidence || []) {
    lines.push(
      `- [${row.match_confidence}] ${row.hotel_name} · \`${row.source_tier}\` · ${row.url}`
    );
  }
  lines.push(``);
  return lines.join("\n");
}

/**
 * @param {{
 *   env?: NodeJS.ProcessEnv,
 *   maxRecords?: number,
 *   log?: Function,
 *   fetchImpl?: typeof fetch,
 *   delayMs?: number,
 *   argv?: string[],
 *   args?: object,
 * }} [opts]
 */
export async function runDataForSeoDiscoveryPilot(opts = {}) {
  const env = opts.env || process.env;
  const log = opts.log || console.log;
  const gate = assertDataForSeoCandidateOnlyMode(env);
  if (!gate.ok) {
    return {
      ok: false,
      status: DATAFORSEO_DISCOVERY_STATUS.BLOCKED,
      objective: DATAFORSEO_DISCOVERY_PILOT_OBJECTIVE,
      reason: "candidate_only_gate_failed",
      blockers: gate.blockers,
      census_writes: 0,
    };
  }

  const creds = resolveDataForSeoCredentials(env);
  if (!creds.ok) {
    return {
      ok: false,
      status: DATAFORSEO_DISCOVERY_STATUS.BLOCKED,
      objective: DATAFORSEO_DISCOVERY_PILOT_OBJECTIVE,
      reason: "missing_dataforseo_credentials",
      census_writes: 0,
    };
  }

  const maxRecords = Number(
    opts.maxRecords || env.DATAFORSEO_MAX_RECORDS || 200
  );
  const maxQueries = Number(env.DATAFORSEO_MAX_QUERIES_PER_RECORD || 6);
  const enableSerp = String(env.DATAFORSEO_ENABLE_SERP || "1") === "1";
  const enableMaps = String(env.DATAFORSEO_ENABLE_GOOGLE_MAPS || "1") === "1";
  const mapsDepth = Number(env.DATAFORSEO_MAPS_DEPTH || 20);
  const delayMs = Number(opts.delayMs ?? env.DATAFORSEO_DELAY_MS ?? 350);

  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token || !bases.target_base_id) {
    return {
      ok: false,
      status: DATAFORSEO_DISCOVERY_STATUS.BLOCKED,
      objective: DATAFORSEO_DISCOVERY_PILOT_OBJECTIVE,
      reason: "missing_airtable_credentials",
      census_writes: 0,
    };
  }

  log(
    `[dataforseo-pilot-v2] listing incomplete Census (max=${maxRecords}) — candidate-only stricter classifier`
  );
  const workset = await listIncompleteCensus(
    bases.target_base_id,
    token,
    CENSUS_TABLE_ID,
    maxRecords
  );
  log(`[dataforseo-pilot-v2] workset=${workset.length}`);

  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const runDir = path.join(
    ROOT,
    "reports/research-engine-v2/autopilot",
    `${stamp}_CALA-dataforseo-discovery-pilot-v2`
  );
  fs.mkdirSync(runDir, { recursive: true });

  let queriesRun = 0;
  let estimatedCost = 0;
  /** @type {Record<string, number>} */
  const rejectedAgg = {};
  /** @type {Record<string, number>} */
  const tierAgg = {};
  let officialUrls = 0;
  let roomsPages = 0;
  let addressCands = 0;
  let phoneCands = 0;
  let mapsCands = 0;
  let latLongCands = 0;
  let tourismCands = 0;
  let usefulTotal = 0;
  let secondaryTotal = 0;
  /** @type {object[]} */
  const perRecord = [];
  /** @type {object[]} */
  const allOfficial = [];
  /** @type {object[]} */
  const allRooms = [];
  /** @type {object[]} */
  const queryLog = [];
  /** @type {Record<string, number>} */
  const parentMix = {};

  for (let i = 0; i < workset.length; i += 1) {
    const { record, missing, priority_band, parent_band, parent_label } =
      workset[i];
    const f = record.fields || {};
    const hotelName =
      f["Canonical Property Name"] || f["Property Name"] || "";
    parentMix[parent_label] = (parentMix[parent_label] || 0) + 1;

    const { queries } = buildDiscoveryQueriesForRecord(f, {
      maxQueries,
      enableSerp,
      enableMaps,
      mapsDepth,
    });

    /** @type {object[]} */
    const classified = [];
    const recordQueryCosts = [];

    for (const q of queries) {
      queriesRun += 1;
      let apiRes;
      if (q.kind === "google_maps") {
        apiRes = await fetchGoogleMapsLive(
          {
            keyword: q.keyword,
            location_name: q.location_name,
            language_code: q.language_code,
            depth: q.depth || mapsDepth,
          },
          { env, fetchImpl: opts.fetchImpl }
        );
      } else {
        apiRes = await fetchGoogleOrganicLive(
          {
            keyword: q.keyword,
            location_name: q.location_name,
            language_code: q.language_code,
            depth: 10,
          },
          { env, fetchImpl: opts.fetchImpl }
        );
      }
      estimatedCost += Number(apiRes.cost) || 0;
      recordQueryCosts.push({
        purpose: q.purpose,
        kind: q.kind,
        keyword: q.keyword,
        language_code: q.language_code || null,
        ok: apiRes.ok,
        cost: apiRes.cost,
        status_message: apiRes.status_message,
        items: (apiRes.items || []).length,
      });
      queryLog.push({
        record_id: record.id,
        ...recordQueryCosts[recordQueryCosts.length - 1],
      });

      for (const item of apiRes.items || []) {
        const c = classifySerpOrMapsItem(item, {
          hotelName,
          city: f.City,
        });
        classified.push(c);
        if (c.status === "rejected") {
          rejectedAgg[c.reason] = (rejectedAgg[c.reason] || 0) + 1;
        } else if (c.source_tier) {
          tierAgg[c.source_tier] = (tierAgg[c.source_tier] || 0) + 1;
        }
      }

      if (delayMs > 0) await sleep(delayMs);
    }

    const summary = summarizeClassifiedCandidates(classified);
    usefulTotal += summary.useful_count;
    secondaryTotal += summary.secondary_count;
    officialUrls += summary.official_hotel_urls.length;
    roomsPages += summary.rooms_evidence_pages.length;
    addressCands += summary.address_candidates.length;
    phoneCands += summary.phone_candidates.length;
    mapsCands += summary.google_maps_candidates.length;
    latLongCands += summary.lat_long_candidates.length;
    tourismCands += summary.tourism_registry_candidates.length;

    for (const u of summary.official_hotel_urls) {
      allOfficial.push({
        ...u,
        hotel_name: hotelName,
        record_id: record.id,
      });
    }
    for (const u of summary.rooms_evidence_pages) {
      allRooms.push({
        ...u,
        hotel_name: hotelName,
        record_id: record.id,
      });
    }

    perRecord.push({
      record_id: record.id,
      hotel_name: hotelName,
      country: f.Country,
      city: f.City,
      brand_family: f["Brand Family"] || null,
      parent_band,
      parent_label,
      priority_band,
      missing,
      queries: recordQueryCosts,
      summary: {
        useful_count: summary.useful_count,
        secondary_count: summary.secondary_count,
        official_hotel_urls: summary.official_hotel_urls.length,
        rooms_evidence_pages: summary.rooms_evidence_pages.length,
        address_candidates: summary.address_candidates.length,
        phone_candidates: summary.phone_candidates.length,
        google_maps_candidates: summary.google_maps_candidates.length,
        tourism_registry_candidates: summary.tourism_registry_candidates.length,
      },
      useful_candidates: classified
        .filter((c) => c.status === "useful" || c.status === "secondary")
        .slice(0, 25),
    });

    if ((i + 1) % 10 === 0 || i === workset.length - 1) {
      log(
        `[dataforseo-pilot-v2] progress ${i + 1}/${workset.length} queries=${queriesRun} cost~$${estimatedCost.toFixed(4)} useful=${usefulTotal} secondary=${secondaryTotal}`
      );
    }
  }

  const costPerUseful =
    usefulTotal > 0 ? estimatedCost / usefulTotal : null;
  const precision = estimateOfficialUrlPrecision(allOfficial);
  const recordsWithOfficial = perRecord.filter(
    (r) => r.summary.official_hotel_urls > 0
  ).length;
  const officialHitRate =
    workset.length > 0 ? recordsWithOfficial / workset.length : 0;
  const affiliateRejected =
    (rejectedAgg.rejected_affiliate_mirror || 0) +
    (rejectedAgg.rejected_ota_or_ugc_host || 0);

  let recommendation = "policy_decision_needed";
  let recommendation_notes = "";
  let status = DATAFORSEO_DISCOVERY_STATUS.PARTIAL_POLICY;
  let recommended_write_policy = "";

  if (queriesRun === 0) {
    recommendation = "blocked";
    status = DATAFORSEO_DISCOVERY_STATUS.BLOCKED;
    recommendation_notes = "No queries executed.";
    recommended_write_policy = "Do not enable Census writes.";
  } else if (
    precision.precision_estimate != null &&
    precision.precision_estimate >= 0.7 &&
    officialHitRate >= 0.2 &&
    usefulTotal >= workset.length * 0.4
  ) {
    recommendation = "scale_candidates_only";
    status = DATAFORSEO_DISCOVERY_STATUS.PARTIAL_POLICY;
    recommendation_notes =
      "Classifier precision and yield support scaling candidate discovery. Keep WRITE_CANDIDATES_ONLY=1 until steward validation path exists for High/Medium official URL and rooms writes.";
    recommended_write_policy = [
      "Keep DataForSEO candidate-only — never SoT.",
      "Allow future writes only after steward validation of brand_official / tourism_registry / factsheet_pdf candidates.",
      "Never write from hospitality_trade_secondary (Travel Weekly) without approved secondary rooms policy.",
      "Never write Google Maps fields without separate geocode/contact policy + storage terms.",
      `Target table if later approved: Hotel Property Census (${CENSUS_TABLE_ID}).`,
    ].join(" ");
  } else if (usefulTotal < workset.length * 0.1 || estimatedCost > 50) {
    recommendation = "stop";
    status = DATAFORSEO_DISCOVERY_STATUS.PARTIAL_SOURCE;
    recommendation_notes =
      "Low useful yield or high cost after stricter classification — stop or retune before spending more.";
    recommended_write_policy = "Do not enable Census writes. Retune queries / locations first.";
  } else {
    recommendation = "adjust_then_scale_candidates";
    status = DATAFORSEO_DISCOVERY_STATUS.PARTIAL_POLICY;
    recommendation_notes =
      "Moderate stricter-classifier yield — keep candidate-only, prefer site:brand-domain + Spanish/Portuguese rooms queries, deepen Maps only for contact/geo gaps, then re-pilot before full 1,224 scale.";
    recommended_write_policy = [
      "No Census writes yet.",
      "Promote only brand_official URL candidates and tourism_registry / factsheet rooms evidence after human or High extractor validation.",
      "Reject affiliate mirrors (already classifier-hard).",
      "Travel Weekly = verification only.",
      "Maps = candidate-only.",
    ].join(" ");
  }

  if (
    precision.precision_estimate != null &&
    precision.precision_estimate >= 0.85 &&
    officialHitRate >= 0.35 &&
    affiliateRejected > usefulTotal * 0.5
  ) {
    // Strong precision + noise rejected → still policy gate (no auto writes)
    status = DATAFORSEO_DISCOVERY_STATUS.PARTIAL_POLICY;
    if (recommendation === "scale_candidates_only") {
      recommendation_notes =
        "High precision after affiliate rejection. Ready to scale candidate discovery; policy decision still required before any Census write path.";
    }
  }

  const scaleFactor =
    workset.length > 0 ? SCALE_UNIVERSE / workset.length : 0;
  const scale_estimate = {
    universe: SCALE_UNIVERSE,
    pilot_records: workset.length,
    estimated_queries: Math.round(queriesRun * scaleFactor),
    estimated_cost_usd: +(estimatedCost * scaleFactor).toFixed(4),
    estimated_useful_candidates: Math.round(usefulTotal * scaleFactor),
    estimated_official_urls: Math.round(officialUrls * scaleFactor),
    estimated_rooms_evidence: Math.round(roomsPages * scaleFactor),
  };

  const report = {
    ok: true,
    status,
    objective: DATAFORSEO_DISCOVERY_PILOT_OBJECTIVE,
    recommendation,
    recommendation_notes,
    recommended_write_policy,
    version: DATAFORSEO_DISCOVERY_PILOT_VERSION,
    classifier_version: "dataforseo-candidate-classifier-v2",
    generated_at: new Date().toISOString(),
    production_target: {
      base: "Deal Capture Platform",
      table: "Hotel Property Census",
      table_id: CENSUS_TABLE_ID,
      role: "read_only_for_this_pilot",
    },
    gates: gate,
    census_writes: 0,
    brand_setup_writes: 0,
    brand_explorer_writes: 0,
    records_piloted: workset.length,
    queries_run: queriesRun,
    estimated_cost_usd: +estimatedCost.toFixed(6),
    useful_candidates_found: usefulTotal,
    trusted_secondary_candidates_found: secondaryTotal,
    cost_per_useful_candidate:
      costPerUseful == null ? null : +costPerUseful.toFixed(6),
    official_hotel_urls_found: officialUrls,
    rooms_evidence_pages_found: roomsPages,
    address_candidates_found: addressCands,
    phone_candidates_found: phoneCands,
    google_maps_candidates_found: mapsCands,
    lat_long_candidates_found: latLongCands,
    tourism_registry_candidates_found: tourismCands,
    rejected_sources: rejectedAgg,
    source_tier_counts: tierAgg,
    source_classifier_precision_estimate: precision.precision_estimate,
    precision_detail: precision,
    official_url_hit_rate_records: +officialHitRate.toFixed(3),
    parent_mix: parentMix,
    top_25_official_urls: pickTopCandidates(allOfficial, 25),
    top_25_rooms_evidence: pickTopCandidates(allRooms, 25),
    scale_estimate,
    run_dir: runDir,
  };

  writeJson(path.join(runDir, "pilot-report.json"), report);
  writeJson(path.join(runDir, "per-record-candidates.json"), {
    count: perRecord.length,
    records: perRecord,
  });
  writeJson(path.join(runDir, "query-log.json"), {
    count: queryLog.length,
    queries: queryLog,
  });

  const reportJson = path.join(
    ROOT,
    "reports/research-engine-v2/dataforseo-discovery-pilot-v2.json"
  );
  const reportMd = path.join(
    ROOT,
    "reports/research-engine-v2/dataforseo-discovery-pilot-v2.md"
  );
  const docsPath = path.join(
    ROOT,
    "docs/data-intelligence/dataforseo-discovery-pilot-v2.md"
  );
  writeJson(reportJson, report);
  const md = renderPilotMd(report);
  writeMd(reportMd, md);
  writeMd(docsPath, md);

  log(
    `[dataforseo-pilot-v2] done status=${status} recommendation=${recommendation} cost=$${estimatedCost.toFixed(4)} useful=${usefulTotal} census_writes=0`
  );
  return report;
}

/**
 * Autopilot mission wrapper — always candidate-only; ignores production write flags.
 */
export async function runDataForSeoDiscoveryPilotV2Mission(opts = {}) {
  const env = {
    ...(opts.env || process.env),
    DATAFORSEO_ENABLED: "1",
    DATAFORSEO_WRITE_CANDIDATES_ONLY: "1",
    ENABLE_DATAFORSEO_VALIDATED_WRITES: "0",
  };
  if (opts.args?.batchSize) {
    // batch-size is advisory for reporting; pilot uses MAX_RECORDS
  }
  return runDataForSeoDiscoveryPilot({
    ...opts,
    env,
    maxRecords: Number(
      env.DATAFORSEO_MAX_RECORDS || opts.maxRecords || 200
    ),
  });
}
