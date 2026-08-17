/**
 * DataForSEO Local Business Enrichment + CALA Hotel Discovery Pilot v1.
 * Candidate-only: no Hotel Property Census writes, no inserts.
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
  fetchGoogleMapsLive,
} from "./dataforseo-client.js";
import {
  scoreLocalBusinessToCensus,
  matchDiscoveryItemToCensus,
  buildLocalEnrichmentCandidate,
  classifyLodgingType,
  MATCH_CLASS,
  LODGING_CLASS,
} from "./dataforseo-local-match.js";
import {
  resolveCensusMode,
  assertNoInsertInFieldCompletionMode,
  CENSUS_MODE,
} from "./census-autopilot-full-latam-v3.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const DATAFORSEO_LOCAL_ENRICHMENT_OBJECTIVE =
  "dataforseo-local-business-enrichment-v1";
export const DATAFORSEO_LOCAL_ENRICHMENT_VERSION =
  "dataforseo-local-business-enrichment-v1";

export const DATAFORSEO_LOCAL_ENRICHMENT_STATUS = Object.freeze({
  COMPLETE:
    "production_census_dataforseo_local_business_enrichment_v1_complete",
  PARTIAL_POLICY:
    "production_census_dataforseo_local_business_enrichment_v1_partial_policy_decision_needed",
  PARTIAL_SOURCE:
    "production_census_dataforseo_local_business_enrichment_v1_partial_source_remaining",
  BLOCKED:
    "production_census_dataforseo_local_business_enrichment_v1_blocked",
});

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || productionHotelPropertyCensus.tableId;

/** Founder pilot markets (max 9). */
export const DATAFORSEO_LOCAL_PILOT_MARKETS = Object.freeze([
  {
    id: "cancun-riviera-maya",
    label: "Cancún / Riviera Maya",
    country: "Mexico",
    city: "Cancún",
    location_name: "Cancun,Mexico",
    keywords: ["hotel Cancún", "hoteles Riviera Maya", "hotel Playa del Carmen"],
  },
  {
    id: "los-cabos",
    label: "Los Cabos",
    country: "Mexico",
    city: "Cabo San Lucas",
    location_name: "Cabo San Lucas,Mexico",
    keywords: ["hotel Los Cabos", "hotel Cabo San Lucas", "hotel San José del Cabo"],
  },
  {
    id: "mexico-city",
    label: "Mexico City",
    country: "Mexico",
    city: "Mexico City",
    location_name: "Mexico City,Mexico",
    keywords: ["hotel Mexico City", "hoteles Ciudad de México", "hotel CDMX"],
  },
  {
    id: "punta-cana",
    label: "Punta Cana",
    country: "Dominican Republic",
    city: "Punta Cana",
    location_name: "Punta Cana,Dominican Republic",
    keywords: ["hotel Punta Cana", "resort Punta Cana", "hoteles Bávaro"],
  },
  {
    id: "santo-domingo",
    label: "Santo Domingo",
    country: "Dominican Republic",
    city: "Santo Domingo",
    location_name: "Santo Domingo,Dominican Republic",
    keywords: ["hotel Santo Domingo", "hoteles Santo Domingo"],
  },
  {
    id: "cartagena",
    label: "Cartagena",
    country: "Colombia",
    city: "Cartagena",
    location_name: "Cartagena,Colombia",
    keywords: ["hotel Cartagena", "hoteles Cartagena Colombia"],
  },
  {
    id: "bogota",
    label: "Bogotá",
    country: "Colombia",
    city: "Bogotá",
    location_name: "Bogota,Colombia",
    keywords: ["hotel Bogotá", "hoteles Bogotá Colombia"],
  },
  {
    id: "panama-city",
    label: "Panama City",
    country: "Panama",
    city: "Panama City",
    location_name: "Panama City,Panama",
    keywords: ["hotel Panama City", "hoteles Ciudad de Panamá"],
  },
  {
    id: "san-jose-cr",
    label: "San José, Costa Rica",
    country: "Costa Rica",
    city: "San José",
    location_name: "San Jose,Costa Rica",
    keywords: ["hotel San José Costa Rica", "hoteles San Jose Costa Rica"],
  },
]);

const READ_FIELDS = [
  "Property Identity Key",
  "Property Name",
  "Canonical Property Name",
  "Current Brand",
  "Brand Family",
  "Country",
  "City",
  "State / Region",
  "Market",
  "Submarket",
  "Address",
  "Phone",
  "Official Property URL",
  "Source URL",
  "Latitude",
  "Longitude",
  "Rooms / Keys",
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
export function assertDataForSeoLocalCandidateOnly(env = process.env) {
  const enabled = String(env.DATAFORSEO_ENABLED || "0").trim() === "1";
  const candidatesOnly =
    String(env.DATAFORSEO_WRITE_CANDIDATES_ONLY || "0").trim() === "1";
  const validated =
    String(env.ENABLE_DATAFORSEO_VALIDATED_WRITES || "0").trim() === "1";
  const maps = String(env.DATAFORSEO_ENABLE_GOOGLE_MAPS || "0").trim() === "1";
  const listings =
    String(env.DATAFORSEO_ENABLE_BUSINESS_LISTINGS || "0").trim() === "1";
  const addrW =
    String(env.ENABLE_DATAFORSEO_LOCAL_ADDRESS_WRITES || "0").trim() === "1";
  const phoneW =
    String(env.ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES || "0").trim() === "1";
  const webW =
    String(env.ENABLE_DATAFORSEO_LOCAL_WEBSITE_WRITES || "0").trim() === "1";
  const coordW =
    String(env.ENABLE_DATAFORSEO_LOCAL_COORDINATE_WRITES || "0").trim() === "1";

  const blockers = [];
  if (!enabled) blockers.push("DATAFORSEO_ENABLED_not_1");
  if (!candidatesOnly) {
    blockers.push("DATAFORSEO_WRITE_CANDIDATES_ONLY_must_be_1_for_pilot");
  }
  if (validated) {
    blockers.push("ENABLE_DATAFORSEO_VALIDATED_WRITES_must_be_0_for_local_pilot");
  }
  if (!maps && !listings) {
    blockers.push("maps_or_business_listings_must_be_enabled");
  }
  if (addrW || phoneW || webW || coordW) {
    blockers.push("local_field_write_flags_must_be_0_until_founder_approval");
  }

  return {
    ok: blockers.length === 0,
    blockers,
    candidates_only: candidatesOnly,
    dataforseo_is_source_of_truth: false,
    census_writes_allowed: false,
    inserts_allowed: false,
    rooms_from_maps_allowed: false,
    field_write_flags: {
      address: false,
      phone: false,
      website: false,
      coordinates: false,
    },
  };
}

export function scoreIncompleteLocalPriority(fields = {}) {
  const missingAddress = isBlank(fields.Address);
  const missingPhone = isBlank(fields.Phone);
  const missingUrl = isBlank(fields["Official Property URL"]);
  const missingGeo =
    isBlank(fields.Latitude) || isBlank(fields.Longitude);
  const gap = [missingAddress, missingPhone, missingUrl, missingGeo].filter(
    Boolean
  ).length;
  if (!gap) return { priority_band: 99, gap_count: 0, missing: {} };
  let band = 4;
  if (missingAddress) band = Math.min(band, 1);
  if (missingPhone) band = Math.min(band, 2);
  if (missingUrl) band = Math.min(band, 3);
  if (missingGeo) band = Math.min(band, 4);
  return {
    priority_band: band,
    gap_count: gap,
    missing: {
      address: missingAddress,
      phone: missingPhone,
      official_property_url: missingUrl,
      lat_long: missingGeo,
    },
    sort_key: band * 10 - gap,
  };
}

async function listCensus(baseId, token, tableId) {
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
  return out;
}

function renderReportMd(report) {
  const lines = [
    `# DataForSEO Local Business Enrichment v1`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Objective:** \`${report.objective}\``,
    `**Generated:** ${report.generated_at}`,
    `**Mode:** candidates-only (no Hotel Property Census writes / no inserts)`,
    ``,
    `## Scope`,
    ``,
    `- Existing records tested: **${report.existing_records_tested}**`,
    `- Markets tested: **${report.markets_tested}**`,
    `- Queries / tasks run: **${report.queries_run}**`,
    `- Estimated cost: **$${Number(report.estimated_cost_usd || 0).toFixed(4)}**`,
    ``,
    `## Enrichment (existing records)`,
    ``,
    `- Local/business candidates found: **${report.local_business_candidates_found}**`,
    `- High-confidence matches: **${report.high_confidence_matches}**`,
    `- Medium-confidence matches: **${report.medium_confidence_matches}**`,
    `- Duplicate-risk matches: **${report.duplicate_risk_matches}**`,
    `- Address candidates: **${report.address_candidates}**`,
    `- Phone candidates: **${report.phone_candidates}**`,
    `- Website candidates: **${report.website_candidates}**`,
    `- Coordinate candidates: **${report.coordinate_candidates}**`,
    `- Rooms evidence from Maps: **0** (not allowed)`,
    ``,
    `## Discovery (pilot markets)`,
    ``,
    `- New hotel candidates: **${report.new_hotel_candidates}**`,
    `- Already in census: **${report.already_in_census}**`,
    `- Possible duplicates: **${report.possible_duplicates}**`,
    `- Non-hotel / unsupported rejected: **${report.non_hotel_rejected}**`,
    ``,
    `## Cost efficiency`,
    ``,
    `- Cost per matched record (high+medium): **${
      report.cost_per_matched_record == null
        ? "n/a"
        : `$${Number(report.cost_per_matched_record).toFixed(4)}`
    }**`,
    `- Cost per new hotel candidate: **${
      report.cost_per_new_hotel_candidate == null
        ? "n/a"
        : `$${Number(report.cost_per_new_hotel_candidate).toFixed(4)}`
    }**`,
    `- Cost per address candidate: **${
      report.cost_per_address_candidate == null
        ? "n/a"
        : `$${Number(report.cost_per_address_candidate).toFixed(4)}`
    }**`,
    `- Cost per phone candidate: **${
      report.cost_per_phone_candidate == null
        ? "n/a"
        : `$${Number(report.cost_per_phone_candidate).toFixed(4)}`
    }**`,
    `- Cost per website candidate: **${
      report.cost_per_website_candidate == null
        ? "n/a"
        : `$${Number(report.cost_per_website_candidate).toFixed(4)}`
    }**`,
    `- Cost per geo candidate: **${
      report.cost_per_geo_candidate == null
        ? "n/a"
        : `$${Number(report.cost_per_geo_candidate).toFixed(4)}`
    }**`,
    ``,
    `## Safety`,
    ``,
    `- Census writes: **0**`,
    `- Inserts: **0** (candidate insert queue only)`,
    `- Brand Setup / Brand Explorer: **0**`,
    `- Rooms from Maps/local: **0**`,
    `- DataForSEO as SoT: **false**`,
    `- Field-level write flags all off`,
    ``,
    `## Recommended write policy`,
    ``,
    report.recommended_write_policy_md || "",
    ``,
    `## Scale estimate (full CALA)`,
    ``,
    report.scale_estimate_notes || "",
    ``,
  ];
  return lines.join("\n");
}

function buildWritePolicyRecommendation(stats) {
  const high = stats.high_confidence_matches || 0;
  const addr = stats.address_candidates || 0;
  const phone = stats.phone_candidates || 0;
  const web = stats.website_candidates || 0;
  const geo = stats.coordinate_candidates || 0;
  const lines = [
    `### A. Safe to approve now (after steward spot-check)`,
    high >= 10 && web > 0
      ? `- Website / Official Property URL from **match_high** + official/brand domain only (not OTA).`
      : `- Website writes: wait for more match_high official-domain samples (current high=${high}, website=${web}).`,
    high >= 10 && addr > 0
      ? `- Address from **match_high** local/business matches (spot-check 20 before scale).`
      : `- Address: keep candidate-only until match_high address sample is larger.`,
    ``,
    `### B. Needs founder / legal approval`,
    `- Phone from DataForSEO / Google local (${phone} candidates) — secondary contact policy.`,
    `- Storing Google-derived coordinates (${geo} candidates) — geocode storage / terms.`,
    `- Storing Google-derived address as production SoT.`,
    ``,
    `### C. Keep candidate-only`,
    `- match_medium steward queue`,
    `- duplicate_risk records`,
    `- new_hotel_candidates (no auto-insert in field-completion-only)`,
    `- category-ambiguous / hostel / apartment / VR`,
    `- Rooms / Keys from Maps (never)`,
  ];
  return lines.join("\n");
}

/**
 * @param {{
 *   argv?: string[],
 *   args?: object,
 *   env?: NodeJS.ProcessEnv,
 *   log?: Function,
 *   fetchImpl?: typeof fetch,
 * }} [opts]
 */
export async function runDataForSeoLocalBusinessEnrichmentV1Mission(opts = {}) {
  const env = opts.env || process.env;
  const log = opts.log || console.log;
  const args = opts.args || {};

  const censusMode = resolveCensusMode(opts.argv || [], {
    censusMode: opts.censusMode || args.censusMode || "field-completion-only",
  });
  const insertGuard = assertNoInsertInFieldCompletionMode(censusMode, 0);
  if (!insertGuard.ok) {
    return {
      ok: false,
      status: DATAFORSEO_LOCAL_ENRICHMENT_STATUS.BLOCKED,
      objective: DATAFORSEO_LOCAL_ENRICHMENT_OBJECTIVE,
      reason: insertGuard.reason,
      census_writes: 0,
      inserts: 0,
    };
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const runDir = path.join(
    ROOT,
    "reports/research-engine-v2/autopilot",
    `${stamp}_CALA-dataforseo-local-business-enrichment-v1`
  );
  fs.mkdirSync(runDir, { recursive: true });

  const gate = assertDataForSeoLocalCandidateOnly(env);
  if (!gate.ok) {
    const blocked = {
      ok: false,
      status: DATAFORSEO_LOCAL_ENRICHMENT_STATUS.BLOCKED,
      objective: DATAFORSEO_LOCAL_ENRICHMENT_OBJECTIVE,
      reason: "candidate_only_gate_failed",
      blockers: gate.blockers,
      census_writes: 0,
      inserts: 0,
      run_dir: runDir,
    };
    writeJson(path.join(runDir, "blocked.json"), blocked);
    return blocked;
  }

  const creds = resolveDataForSeoCredentials(env);
  if (!creds.ok) {
    return {
      ok: false,
      status: DATAFORSEO_LOCAL_ENRICHMENT_STATUS.BLOCKED,
      objective: DATAFORSEO_LOCAL_ENRICHMENT_OBJECTIVE,
      reason: "missing_dataforseo_credentials",
      census_writes: 0,
      inserts: 0,
      run_dir: runDir,
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token || !bases.target_base_id) {
    return {
      ok: false,
      status: DATAFORSEO_LOCAL_ENRICHMENT_STATUS.BLOCKED,
      objective: DATAFORSEO_LOCAL_ENRICHMENT_OBJECTIVE,
      reason: "missing_airtable_credentials",
      census_writes: 0,
      inserts: 0,
      run_dir: runDir,
    };
  }

  const maxRecords = Number(env.DATAFORSEO_MAX_RECORDS || 300);
  const maxMarkets = Number(
    env.DATAFORSEO_MAX_MARKETS || DATAFORSEO_LOCAL_PILOT_MARKETS.length
  );
  const maxQueriesPerMarket = Number(env.DATAFORSEO_MAX_QUERIES_PER_MARKET || 2);
  const mapsDepth = Number(env.DATAFORSEO_MAPS_DEPTH || 20);
  const delayMs = Number(opts.delayMs ?? env.DATAFORSEO_DELAY_MS ?? 300);
  const costCap = Number(env.DATAFORSEO_COST_CAP_USD || 5);

  log(`[dfs-local] listing Census for enrichment + discovery index`);
  const census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  log(`[dfs-local] census_rows=${census.length}`);

  const incomplete = census
    .map((r) => {
      const pri = scoreIncompleteLocalPriority(r.fields || {});
      return { record: r, ...pri };
    })
    .filter((x) => x.priority_band < 99)
    .sort((a, b) => a.sort_key - b.sort_key)
    .slice(0, maxRecords);

  log(`[dfs-local] enrichment workset=${incomplete.length}`);

  let queriesRun = 0;
  let estimatedCost = 0;
  /** @type {object[]} */
  const enrichmentCandidates = [];
  /** @type {object[]} */
  const discoveryCandidates = [];
  /** @type {object[]} */
  const insertQueue = [];

  let highMatches = 0;
  let mediumMatches = 0;
  let duplicateRisk = 0;
  let addressCands = 0;
  let phoneCands = 0;
  let websiteCands = 0;
  let geoCands = 0;
  let newHotels = 0;
  let alreadyIn = 0;
  let possibleDup = 0;
  let nonHotel = 0;
  let localCandidatesFound = 0;

  // --- Pass 1: enrich existing incomplete records ---
  for (let i = 0; i < incomplete.length; i += 1) {
    if (estimatedCost >= costCap) {
      log(`[dfs-local] cost cap $${costCap} reached during enrichment`);
      break;
    }
    const { record, missing } = incomplete[i];
    const f = record.fields || {};
    const name =
      f["Canonical Property Name"] || f["Property Name"] || "";
    if (!name) continue;
    const location_name = resolveDataForSeoLocationName({
      city: f.City,
      country: f.Country,
    });
    const keyword = `"${name}" hotel ${f.City || ""}`.trim();
    const apiRes = await fetchGoogleMapsLive(
      {
        keyword,
        location_name,
        language_code: /brazil/i.test(String(f.Country || "")) ? "pt" : "es",
        depth: Math.min(mapsDepth, 10),
      },
      { env, fetchImpl: opts.fetchImpl }
    );
    queriesRun += 1;
    estimatedCost += Number(apiRes.cost) || 0;

    let bestItem = null;
    let bestMatch = null;
    for (const item of apiRes.items || []) {
      localCandidatesFound += 1;
      const lodging = classifyLodgingType(item);
      if (
        lodging === LODGING_CLASS.NON_HOTEL ||
        lodging === LODGING_CLASS.CLOSED ||
        lodging === LODGING_CLASS.VACATION_RENTAL
      ) {
        continue;
      }
      const match = scoreLocalBusinessToCensus(item, f, {
        recordId: record.id,
      });
      if (
        !bestMatch ||
        match.match_confidence > bestMatch.match_confidence
      ) {
        bestItem = item;
        bestMatch = match;
      }
    }

    if (bestItem && bestMatch) {
      const cand = buildLocalEnrichmentCandidate(bestItem, bestMatch, {
        endpoint: "serp/google/maps/live/advanced",
      });
      cand.census_record_id = record.id;
      cand.hotel_name = name;
      cand.missing = missing;
      enrichmentCandidates.push(cand);

      if (bestMatch.match_class === MATCH_CLASS.MATCH_HIGH) highMatches += 1;
      else if (bestMatch.match_class === MATCH_CLASS.MATCH_MEDIUM) {
        mediumMatches += 1;
      } else if (bestMatch.match_class === MATCH_CLASS.DUPLICATE_RISK) {
        duplicateRisk += 1;
      }

      if (cand.fields.address) addressCands += 1;
      if (cand.fields.phone) phoneCands += 1;
      if (cand.fields.website) websiteCands += 1;
      if (cand.fields.latitude && cand.fields.longitude) geoCands += 1;
    }

    if (delayMs) await sleep(delayMs);
    if ((i + 1) % 25 === 0 || i === incomplete.length - 1) {
      log(
        `[dfs-local] enrichment ${i + 1}/${incomplete.length} queries=${queriesRun} cost~$${estimatedCost.toFixed(4)} high=${highMatches}`
      );
    }
  }

  // --- Pass 2: market discovery ---
  const markets = DATAFORSEO_LOCAL_PILOT_MARKETS.slice(0, maxMarkets);
  const seenPlaceIds = new Set();

  for (const market of markets) {
    if (estimatedCost >= costCap) {
      log(`[dfs-local] cost cap reached before market ${market.id}`);
      break;
    }
    const kws = market.keywords.slice(0, maxQueriesPerMarket);
    for (const keyword of kws) {
      if (estimatedCost >= costCap) break;
      const apiRes = await fetchGoogleMapsLive(
        {
          keyword,
          location_name: market.location_name,
          language_code: "es",
          depth: mapsDepth,
        },
        { env, fetchImpl: opts.fetchImpl }
      );
      queriesRun += 1;
      estimatedCost += Number(apiRes.cost) || 0;

      for (const item of apiRes.items || []) {
        const pid = item.place_id || `${item.title}|${item.address}`;
        if (seenPlaceIds.has(pid)) continue;
        seenPlaceIds.add(pid);
        localCandidatesFound += 1;

        const matched = matchDiscoveryItemToCensus(item, census);
        const row = {
          market_id: market.id,
          market_label: market.label,
          country: market.country,
          keyword,
          lodging_class: matched.lodging_class,
          discovery_class: matched.discovery_class,
          match_class: matched.match_class,
          match: matched.best,
          near_duplicates: matched.near_duplicates,
          candidate: buildLocalEnrichmentCandidate(item, matched.best, {
            endpoint: "serp/google/maps/live/advanced",
          }),
          insert_allowed: false,
          census_mode: CENSUS_MODE.FIELD_COMPLETION_ONLY,
        };
        discoveryCandidates.push(row);

        if (matched.discovery_class === "new_hotel_candidate") {
          newHotels += 1;
          insertQueue.push({
            ...row,
            queue_status: "candidate_insert_held",
            reason: "field_completion_only_no_auto_insert",
          });
        } else if (matched.discovery_class === "already_in_census") {
          alreadyIn += 1;
        } else if (matched.discovery_class === "possible_duplicate") {
          possibleDup += 1;
          duplicateRisk += 1;
        } else {
          nonHotel += 1;
        }
      }

      if (delayMs) await sleep(delayMs);
    }
    log(
      `[dfs-local] market ${market.id} done queries=${queriesRun} cost~$${estimatedCost.toFixed(4)} new=${newHotels}`
    );
  }

  const matchedTotal = highMatches + mediumMatches;
  const costPer = (n) =>
    n > 0 ? +(estimatedCost / n).toFixed(6) : null;

  const stats = {
    high_confidence_matches: highMatches,
    medium_confidence_matches: mediumMatches,
    address_candidates: addressCands,
    phone_candidates: phoneCands,
    website_candidates: websiteCands,
    coordinate_candidates: geoCands,
  };
  const recommended_write_policy_md = buildWritePolicyRecommendation(stats);

  let status = DATAFORSEO_LOCAL_ENRICHMENT_STATUS.PARTIAL_POLICY;
  if (queriesRun === 0) {
    status = DATAFORSEO_LOCAL_ENRICHMENT_STATUS.BLOCKED;
  } else if (highMatches >= 20 && newHotels >= 10) {
    status = DATAFORSEO_LOCAL_ENRICHMENT_STATUS.PARTIAL_POLICY;
  } else if (highMatches + newHotels < 5) {
    status = DATAFORSEO_LOCAL_ENRICHMENT_STATUS.PARTIAL_SOURCE;
  }

  const scaleFactor =
    incomplete.length > 0 ? Math.max(census.length, 1224) / incomplete.length : 0;

  const report = {
    ok: status !== DATAFORSEO_LOCAL_ENRICHMENT_STATUS.BLOCKED,
    status,
    objective: DATAFORSEO_LOCAL_ENRICHMENT_OBJECTIVE,
    version: DATAFORSEO_LOCAL_ENRICHMENT_VERSION,
    generated_at: new Date().toISOString(),
    gates: gate,
    census_writes: 0,
    inserts: 0,
    brand_setup_writes: 0,
    brand_explorer_writes: 0,
    rooms_writes_from_maps: 0,
    existing_records_tested: incomplete.length,
    markets_tested: markets.length,
    markets: markets.map((m) => m.id),
    queries_run: queriesRun,
    estimated_cost_usd: +estimatedCost.toFixed(6),
    local_business_candidates_found: localCandidatesFound,
    high_confidence_matches: highMatches,
    medium_confidence_matches: mediumMatches,
    duplicate_risk_matches: duplicateRisk,
    new_hotel_candidates: newHotels,
    already_in_census: alreadyIn,
    possible_duplicates: possibleDup,
    non_hotel_rejected: nonHotel,
    address_candidates: addressCands,
    phone_candidates: phoneCands,
    website_candidates: websiteCands,
    coordinate_candidates: geoCands,
    rooms_evidence_candidates: 0,
    cost_per_matched_record: costPer(matchedTotal),
    cost_per_new_hotel_candidate: costPer(newHotels),
    cost_per_address_candidate: costPer(addressCands),
    cost_per_phone_candidate: costPer(phoneCands),
    cost_per_website_candidate: costPer(websiteCands),
    cost_per_geo_candidate: costPer(geoCands),
    recommended_write_policy_md,
    recommended_fields_to_approve: {
      safe_now_after_spot_check: [
        highMatches >= 10 ? "website_official_domain_match_high" : null,
        highMatches >= 10 && addressCands >= 10
          ? "address_match_high_spot_check"
          : null,
      ].filter(Boolean),
      needs_founder_legal: [
        "phone_google_local",
        "coordinates_google_local_storage_terms",
        "address_as_production_sot",
      ],
      keep_candidate_only: [
        "match_medium",
        "duplicate_risk",
        "new_hotel_candidates",
        "rooms_from_maps",
      ],
    },
    scale_estimate_notes: `If enrichment yield holds across ~${census.length} Census rows: ~${Math.round(queriesRun * scaleFactor)} Maps queries, ~$${Number((estimatedCost * scaleFactor).toFixed(2))} estimated, ~${Math.round(highMatches * scaleFactor)} high matches, ~${Math.round(newHotels * (1224 / Math.max(markets.length, 1)))} new hotel candidates if discovery expanded beyond 9 markets (order-of-magnitude only).`,
    production_target: {
      base: "Deal Capture Platform",
      table: "Hotel Property Census",
      table_id: CENSUS_TABLE_ID,
      role: "read_only_candidate_pilot",
    },
    run_dir: runDir,
  };

  writeJson(path.join(runDir, "mission-report.json"), report);
  writeJson(path.join(runDir, "enrichment-candidates.json"), {
    count: enrichmentCandidates.length,
    candidates: enrichmentCandidates.slice(0, 800),
  });
  writeJson(path.join(runDir, "discovery-candidates.json"), {
    count: discoveryCandidates.length,
    candidates: discoveryCandidates.slice(0, 1200),
  });
  writeJson(path.join(runDir, "candidate-insert-queue.json"), {
    count: insertQueue.length,
    note: "field-completion-only — do not insert automatically",
    queue: insertQueue.slice(0, 800),
  });

  const reportJson = path.join(
    ROOT,
    "reports/research-engine-v2/dataforseo-local-business-enrichment-v1.json"
  );
  const reportMd = path.join(
    ROOT,
    "reports/research-engine-v2/dataforseo-local-business-enrichment-v1.md"
  );
  const docsPath = path.join(
    ROOT,
    "docs/data-intelligence/dataforseo-local-business-enrichment-v1.md"
  );
  writeJson(reportJson, report);
  const md = renderReportMd(report);
  writeMd(reportMd, md);
  writeMd(docsPath, md);

  log(
    `[dfs-local] done status=${status} queries=${queriesRun} cost=$${estimatedCost.toFixed(4)} high=${highMatches} new=${newHotels} census_writes=0 inserts=0`
  );
  return report;
}
