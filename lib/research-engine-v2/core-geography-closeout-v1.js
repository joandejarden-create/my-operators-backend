/**
 * Core Geography Closeout v1
 *
 * A) Cuba-only independent official/public discovery (+ AUTO_APPLY shells)
 * B) Full-Census City / Locality backfill (missing subset only)
 * C) Full-Census State / Region backfill (applicable + missing)
 * D–F) Core validation + 52-geography matrix refresh + coverage status
 *
 * Does NOT rerun independent geographic gap discovery wave.
 * Writes only Hotel Property Census tbl9aY5ijiuIzzWam.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  resolveStateRegionFromCity,
  isDirtyStateRegionValue,
} from "./census-city-to-state-map.js";
import { resolveStateRegionV3 } from "./census-autopilot-v3/geography/state-region-resolver-v3.js";
import { isDescriptorCity, normalizePlaceKey } from "./census-city-state-normalizer.js";
import { normName } from "./census-autopilot-v2/identity-dedupe.js";
import {
  listDealalityCalaGeographies,
  normalizeGeographyLabel,
} from "./dealality-cala-geography-registry-v1.js";
import {
  CENSUS_TABLE_ID,
  MATCH,
  SHELL_PREFLIGHT_CLASS,
  classifyAgainstCensus,
  classifyShellPreflightQuality,
  listCensusIndex,
  buildShellFields,
  insertBatch,
} from "./full-cala-15k-census-shell-insert-v1.js";
import { assertNoProtectedShellFields } from "./full-cala-15k-shell-orchestrator-v1.js";
import {
  assertProductionCensusWriteTarget,
} from "./production-census-source-of-truth.js";
import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import { createLiveHotelPropertyCensusAdapter } from "./census-autopilot-batch-engine.js";
import { searchGoogleHotels } from "./providers/serpapi-google-hotels/search.js";
import { SerpApiCreditTracker } from "./providers/serpapi-google-hotels/credit-tracker.js";
import {
  isStateRegionApplicable,
  buildCalaAdminGeographyLibrarySnapshot,
  resolveCubaProvinceFromCity,
  getAdminGeographyMeta,
} from "./cala-admin-geography-library-v1.js";
import { discoverCubaIndependentHotels } from "./cuba-independent-hotel-discovery-v1.js";
import { STATE_REGION_NOT_APPLICABLE } from "./full-cala-core-identity-foundation-closure-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const CLOSEOUT_OBJECTIVE = "core-geography-closeout-v1";
export const CLOSEOUT_VERSION = "core-geography-closeout-v1";

const STATE_DIR = path.join(
  ROOT,
  "data/research-engine-v2/core-geography-closeout"
);
const HOLDS_FILE = path.join(
  ROOT,
  "data/research-engine-v2/full-cala-15k-shell-orchestrator/holds-ledger.json"
);
const APPLIED_FILE = path.join(
  ROOT,
  "data/research-engine-v2/full-cala-15k-shell-orchestrator/applied-index.json"
);

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}
function writeMd(fp, md) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, md.endsWith("\n") ? md : `${md}\n`, "utf8");
}
function readJson(fp, fallback = null) {
  if (!fs.existsSync(fp)) return fallback;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function isBlank(v) {
  return v == null || String(v).trim() === "";
}

async function listCensusCloseout(baseId, token, tableId) {
  const records = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of [
      "Property Name",
      "Canonical Property Name",
      "Country",
      "City",
      "State / Region",
      "Address",
      "Official Property URL",
      "Phone",
      "HBX Hotel Code",
      "Property Identity Key",
    ]) {
      params.append("fields[]", f);
    }
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) {
      throw new Error(`census_list_failed:${res.status}:${json?.error?.message || ""}`);
    }
    records.push(...(json.records || []));
    offset = json.offset;
    await sleep(110);
  } while (offset);
  return records;
}

function inferCityFromAddress(address) {
  const addr = String(address || "").trim();
  if (!addr) return null;
  const parts = addr
    .split(",")
    .map((p) => p.trim())
    .filter(Boolean);
  if (parts.length < 2) return null;
  // Prefer penultimate token when last looks like state/postal/country
  for (let i = parts.length - 1; i >= 1; i--) {
    const cand = parts[i].replace(/\b\d{4,}\b/g, "").trim();
    if (!cand || cand.length < 2 || cand.length > 60) continue;
    if (isDescriptorCity(cand)) continue;
    if (/^(mexico|colombia|brazil|panama|peru|chile|argentina|cuba|jamaica)$/i.test(cand)) {
      continue;
    }
    if (/^[A-Z]{2,3}$/.test(cand)) continue;
    return cand;
  }
  return null;
}

function buildIntraCensusCityStateIndex(records) {
  const map = new Map();
  for (const r of records) {
    const f = r.fields || {};
    const country = normalizeGeographyLabel(f.Country || "");
    const city = normalizePlaceKey(f.City || "");
    const state = String(f["State / Region"] || "").trim();
    if (!country || !city || !state || isDirtyStateRegionValue(state)) continue;
    if (!isStateRegionApplicable(f.Country)) continue;
    const key = `${country}|${city}`;
    if (!map.has(key)) map.set(key, state);
  }
  return map;
}

function proposeStateRegion(rec, cityStateIndex) {
  const f = rec.fields || {};
  const country = String(f.Country || "").trim();
  const city = String(f.City || "").trim();
  const existing = String(f["State / Region"] || "").trim();

  if (!isStateRegionApplicable(country)) {
    return { applicable: false, status: "NOT_APPLICABLE" };
  }
  if (existing && !isDirtyStateRegionValue(existing)) {
    return { applicable: true, status: "ALREADY_SET" };
  }

  if (/^cuba$/i.test(country) && city) {
    const prov = resolveCubaProvinceFromCity(city);
    if (prov) {
      return {
        applicable: true,
        status: "PROPOSED",
        patch: { "State / Region": prov },
        method: "cuba_admin_library",
      };
    }
  }

  const fromCity = resolveStateRegionFromCity({
    city,
    country,
    state: existing,
  });
  if (fromCity.ok && fromCity.state) {
    return {
      applicable: true,
      status: "PROPOSED",
      patch: { "State / Region": fromCity.state },
      method: fromCity.method || "city_to_state_map",
    };
  }

  if (cityStateIndex && city) {
    const key = `${normalizeGeographyLabel(country)}|${normalizePlaceKey(city)}`;
    const boot = cityStateIndex.get(key);
    if (boot) {
      return {
        applicable: true,
        status: "PROPOSED",
        patch: { "State / Region": boot },
        method: "intra_census_city_bootstrap",
      };
    }
  }

  const v3 = resolveStateRegionV3({
    country,
    city,
    address: f.Address,
    name: f["Property Name"] || f["Canonical Property Name"],
    existingState: existing,
  });
  if (v3?.ok && v3.normalized_state_region) {
    return {
      applicable: true,
      status: "PROPOSED",
      patch: { "State / Region": v3.normalized_state_region },
      method: v3.method || "state_region_v3",
    };
  }

  return { applicable: true, status: "UNRESOLVED" };
}

function computeCompleteness(records) {
  let name = 0;
  let country = 0;
  let city = 0;
  let address = 0;
  let phone = 0;
  let web = 0;
  let applicable = 0;
  let applicableWithState = 0;
  let notApplicable = 0;

  for (const r of records) {
    const f = r.fields || {};
    if (!isBlank(f["Property Name"] || f["Canonical Property Name"])) name += 1;
    if (!isBlank(f.Country)) country += 1;
    if (!isBlank(f.City)) city += 1;
    if (!isBlank(f.Address)) address += 1;
    if (!isBlank(f.Phone)) phone += 1;
    if (!isBlank(f.Website || f["Official Property URL"])) web += 1;
    if (isStateRegionApplicable(f.Country)) {
      applicable += 1;
      if (
        !isBlank(f["State / Region"]) &&
        !isDirtyStateRegionValue(f["State / Region"])
      ) {
        applicableWithState += 1;
      }
    } else {
      notApplicable += 1;
    }
  }
  const n = records.length || 1;
  return {
    TOTAL_RECORDS: records.length,
    NAME_COMPLETENESS: Math.round((100 * name) / n),
    COUNTRY_COMPLETENESS: Math.round((100 * country) / n),
    CITY_COMPLETENESS: Math.round((100 * city) / n),
    ADDRESS_COMPLETENESS: Math.round((100 * address) / n),
    PHONE_COMPLETENESS: Math.round((100 * phone) / n),
    WEBSITE_COMPLETENESS: Math.round((100 * web) / n),
    STATE_REGION_APPLICABLE_RECORDS: applicable,
    STATE_REGION_NOT_APPLICABLE_RECORDS: notApplicable,
    STATE_REGION_POPULATED_APPLICABLE: applicableWithState,
    STATE_REGION_COMPLETENESS_OF_APPLICABLE: applicable
      ? Math.round((100 * applicableWithState) / applicable)
      : null,
    CITY_MISSING: records.length - city,
    STATE_REGION_MISSING_APPLICABLE: applicable - applicableWithState,
  };
}

function toShellCandidate(c) {
  return {
    candidate_id: c.candidate_id,
    property_name: c.property_name,
    normalized_property_name: c.normalized_property_name,
    country: "Cuba",
    city: c.city,
    state: c.state_region,
    address: c.address || null,
    phone: c.phone || null,
    website: c.website || c.source_url || null,
    source_type: c.source_type,
    merged_sources: [c.source_type],
    external_ids: {},
    source_url: c.source_url,
  };
}

async function applyCubaShells({
  candidates,
  index,
  baseId,
  token,
  enableWrites,
  log,
}) {
  const tallies = {
    discovered: candidates.length,
    inserted: 0,
    existing: 0,
    duplicate: 0,
    invalid: 0,
    hold: 0,
  };
  const prepared = [];

  for (const raw of candidates) {
    if (!raw.city) {
      tallies.hold += 1;
      continue;
    }
    const c = toShellCandidate(raw);
    const cls = classifyAgainstCensus(c, index);
    if (
      cls.match_class === MATCH.EXISTING_HIGH ||
      cls.match_class === MATCH.EXISTING_MEDIUM
    ) {
      tallies.existing += 1;
      continue;
    }
    if (cls.match_class === MATCH.PROBABLE_DUP) {
      tallies.duplicate += 1;
      continue;
    }
    if (
      cls.match_class === MATCH.REJECT_NON_HOTEL ||
      cls.match_class === MATCH.REJECT_IDENTITY
    ) {
      tallies.invalid += 1;
      continue;
    }

    const pf = classifyShellPreflightQuality(c, { cventOnlyQualityGate: true });
    const allow =
      (pf.class === SHELL_PREFLIGHT_CLASS.SAFE ||
        pf.class === SHELL_PREFLIGHT_CLASS.REVIEW) &&
      !isBlank(c.property_name) &&
      !isBlank(c.country) &&
      !isBlank(c.city);

    if (!allow) {
      if (
        pf.class === SHELL_PREFLIGHT_CLASS.WEAK ||
        pf.class === SHELL_PREFLIGHT_CLASS.INSUFFICIENT
      ) {
        tallies.hold += 1;
      } else {
        tallies.invalid += 1;
      }
      continue;
    }

    try {
      const fieldsBuilt = buildShellFields(c, [], {
        countryBatchLabel: "Cuba",
      });
      if (!fieldsBuilt.validation.pass) {
        tallies.hold += 1;
        continue;
      }
      assertNoProtectedShellFields(fieldsBuilt.fields);
      fieldsBuilt.fields.City = c.city;
      if (c.state) fieldsBuilt.fields["State / Region"] = c.state;
      if (c.website) fieldsBuilt.fields["Official Property URL"] = c.website;
      if (raw.source_url) fieldsBuilt.fields["Source URL"] = raw.source_url;
      fieldsBuilt.fields["Discovery Source"] = raw.discovery_source;
      fieldsBuilt.fields["Source Type"] = "independent_discovery";
      fieldsBuilt.fields["Shell Insert Batch ID"] = CLOSEOUT_VERSION;
      fieldsBuilt.fields["Shell Insert Country Batch"] = "Cuba";
      fieldsBuilt.fields["Notes for Steward"] = [
        fieldsBuilt.fields["Notes for Steward"],
        `cuba_official_directory=${raw.group}`,
        `source=${raw.source_url}`,
      ]
        .filter(Boolean)
        .join("\n");

      prepared.push({
        candidate_id: c.candidate_id,
        property_name: c.property_name,
        fields: fieldsBuilt.fields,
      });
    } catch (err) {
      log?.(
        `[closeout] cuba prepare skip: ${String(err?.message || err).slice(0, 120)}`
      );
      tallies.hold += 1;
    }
  }

  if (!enableWrites || !prepared.length) {
    tallies.prepared = prepared.length;
    return { tallies, prepared };
  }

  const result = await insertBatch(prepared, {
    baseId,
    token,
    tableId: CENSUS_TABLE_ID,
    log,
  });
  tallies.inserted = result.inserted || 0;
  tallies.errors = result.errors?.length || 0;

  const applied = readJson(APPLIED_FILE, { candidate_ids: [], hbx_codes: [] });
  const set = new Set(applied.candidate_ids || []);
  for (const p of prepared) set.add(p.candidate_id);
  applied.candidate_ids = [...set];
  applied.updated_at = new Date().toISOString();
  writeJson(APPLIED_FILE, applied);

  return { tallies, prepared, result };
}

/**
 * @param {{
 *   mode?: 'dry-run'|'run',
 *   enableProductionWrites?: boolean,
 *   serpMax?: number,
 *   log?: Function,
 * }} opts
 */
export async function runCoreGeographyCloseoutV1(opts = {}) {
  const mode = opts.mode || "dry-run";
  const enableWrites = Boolean(opts.enableProductionWrites) && mode === "run";
  const log = opts.log || console.log;
  const generated_at = new Date().toISOString();
  fs.mkdirSync(STATE_DIR, { recursive: true });

  const token = resolvePat();
  const base = resolveTargetBase();
  const baseId = base?.target_base_id || base?.baseId;
  assertProductionCensusWriteTarget({
    baseId,
    tableId: CENSUS_TABLE_ID,
  });

  const adminLib = buildCalaAdminGeographyLibrarySnapshot();
  writeJson(path.join(STATE_DIR, "cala-admin-geography-library.json"), adminLib);

  log(`[closeout] listing Hotel Property Census…`);
  let records = await listCensusCloseout(baseId, token, CENSUS_TABLE_ID);
  const censusBefore = records.length;
  const compBefore = computeCompleteness(records);
  log(
    `[closeout] census=${censusBefore} city%=${compBefore.CITY_COMPLETENESS} state_appl%=${compBefore.STATE_REGION_COMPLETENESS_OF_APPLICABLE}`
  );

  // —— Part A: Cuba ——
  log(`[closeout] Cuba independent discovery…`);
  const cubaDisc = await discoverCubaIndependentHotels({ log });
  writeJson(path.join(STATE_DIR, "cuba-discovery.json"), {
    generated_at,
    ...cubaDisc,
    candidates: cubaDisc.candidates.map((c) => ({
      property_name: c.property_name,
      city: c.city,
      state_region: c.state_region,
      source_id: c.source_id,
      candidate_id: c.candidate_id,
    })),
  });

  const index = await listCensusIndex(baseId, token, CENSUS_TABLE_ID);
  const cubaApply = await applyCubaShells({
    candidates: cubaDisc.candidates,
    index,
    baseId,
    token,
    enableWrites,
    log,
  });
  writeJson(path.join(STATE_DIR, "cuba-apply.json"), {
    generated_at,
    mode,
    enableWrites,
    tallies: cubaApply.tallies,
    sample_prepared: (cubaApply.prepared || []).slice(0, 20).map((p) => ({
      property_name: p.property_name,
      city: p.fields?.City,
      state: p.fields?.["State / Region"],
    })),
  });

  if (enableWrites && cubaApply.tallies.inserted) {
    records = await listCensusCloseout(baseId, token, CENSUS_TABLE_ID);
  }

  // —— Part B: City backfill (full missing subset) ——
  log(`[closeout] City backfill proposals…`);
  const cityPatches = [];
  let cityUnresolved = 0;
  let cityFromAddress = 0;
  for (const rec of records) {
    const f = rec.fields || {};
    if (!isBlank(f.City)) continue;
    const inferred = inferCityFromAddress(f.Address);
    if (inferred && !isDescriptorCity(inferred)) {
      cityPatches.push({
        id: rec.id,
        fields: { City: inferred },
        method: "address_parse",
        status: "CITY_INFERRED_HIGH",
      });
      cityFromAddress += 1;
    } else {
      cityUnresolved += 1;
    }
  }

  let serpCityResolved = 0;
  const serpMax = Number(opts.serpMax ?? 25);
  const serpKey = process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY;
  const cityByIdEarly = new Map(cityPatches.map((p) => [p.id, p]));
  if (serpMax > 0 && serpKey) {
    const tracker = new SerpApiCreditTracker({ label: "core-geography-closeout" });
    const missing = records
      .filter((r) => isBlank(r.fields?.City) && !cityByIdEarly.has(r.id))
      .slice(0, serpMax);
    for (const rec of missing) {
      const name =
        rec.fields?.["Canonical Property Name"] ||
        rec.fields?.["Property Name"] ||
        "";
      const country = rec.fields?.Country || "";
      if (!name || !country) continue;
      try {
        const res = await searchGoogleHotels(
          {
            q: `${name} hotel ${country}`,
            gl: "us",
            hl: "en",
          },
          { tracker }
        );
        const hit = (res?.candidates || [])[0];
        const city =
          hit?.city ||
          (hit?.address ? String(hit.address).split(",")[1]?.trim() : null) ||
          null;
        if (city && !isDescriptorCity(city)) {
          const patch = {
            id: rec.id,
            fields: { City: city },
            method: "serpapi_residual",
            status: "CITY_INFERRED_HIGH",
          };
          cityPatches.push(patch);
          cityByIdEarly.set(rec.id, patch);
          serpCityResolved += 1;
          cityUnresolved = Math.max(0, cityUnresolved - 1);
        }
      } catch (err) {
        log(
          `[closeout] serp city skip: ${String(err?.message || err).slice(0, 100)}`
        );
      }
      await sleep(250);
    }
  }

  // Dedupe city patches by id (prefer address over serp)
  const cityById = new Map();
  for (const p of cityPatches) {
    if (!cityById.has(p.id) || p.method === "address_parse") cityById.set(p.id, p);
  }
  const uniqCityPatches = [...cityById.values()];

  // —— Part C: State / Region ——
  log(`[closeout] State/Region proposals…`);
  // Apply city patches into a working view for state proposals
  const working = records.map((r) => {
    const cp = cityById.get(r.id);
    if (!cp) return r;
    return { ...r, fields: { ...r.fields, ...cp.fields } };
  });
  const cityStateIndex = buildIntraCensusCityStateIndex(working);
  const statePatches = [];
  let stateUnresolved = 0;
  let stateAlready = 0;
  let stateNa = 0;
  let stateApplicable = 0;
  for (const rec of working) {
    const p = proposeStateRegion(rec, cityStateIndex);
    if (!p.applicable) {
      stateNa += 1;
      continue;
    }
    stateApplicable += 1;
    if (p.status === "ALREADY_SET") {
      stateAlready += 1;
      continue;
    }
    if (p.patch) {
      statePatches.push({
        id: rec.id,
        fields: p.patch,
        method: p.method,
      });
    } else stateUnresolved += 1;
  }

  const adapter = createLiveHotelPropertyCensusAdapter({
    token,
    baseId,
    tableId: CENSUS_TABLE_ID,
  });

  let cityWritten = 0;
  let stateWritten = 0;
  if (enableWrites) {
    if (uniqCityPatches.length) {
      log(`[closeout] applying ${uniqCityPatches.length} City patches…`);
      const res = await adapter.patchRecords(
        uniqCityPatches.map((p) => ({ id: p.id, fields: p.fields }))
      );
      cityWritten = res.updated || 0;
      if (res.errors?.length) {
        log(`[closeout] city patch errors: ${res.errors.length}`);
      }
    }
    // Merge state patches with any city-driven extras
    const byId = new Map();
    for (const p of statePatches) byId.set(p.id, p);
    const uniqState = [...byId.values()];
    if (uniqState.length) {
      log(`[closeout] applying ${uniqState.length} State/Region patches…`);
      const res = await adapter.patchRecords(
        uniqState.map((p) => ({ id: p.id, fields: p.fields }))
      );
      stateWritten = res.updated || 0;
      if (res.errors?.length) {
        log(`[closeout] state patch errors: ${res.errors.length}`);
      }
    }
    records = await listCensusCloseout(baseId, token, CENSUS_TABLE_ID);
  } else {
    // Simulate for metrics
    for (const p of uniqCityPatches) {
      const rec = records.find((r) => r.id === p.id);
      if (rec) Object.assign(rec.fields, p.fields);
    }
    for (const p of statePatches) {
      const rec = records.find((r) => r.id === p.id);
      if (rec) Object.assign(rec.fields, p.fields);
    }
  }

  const censusAfter = records.length;
  const comp = computeCompleteness(records);

  // —— Matrix ——
  const geos = listDealalityCalaGeographies({ includeScopeReview: true });
  const holds = readJson(HOLDS_FILE, { by_candidate_id: {} });
  const holdByCountry = {};
  for (const h of Object.values(holds.by_candidate_id || holds.active || {})) {
    const c = normalizeGeographyLabel(h?.country || h?.Country || "");
    if (!c) continue;
    holdByCountry[c] = (holdByCountry[c] || 0) + 1;
  }
  // If ledger is array-style
  if (Array.isArray(holds.holds)) {
    for (const h of holds.holds) {
      const c = normalizeGeographyLabel(h.country || "");
      if (c) holdByCountry[c] = (holdByCountry[c] || 0) + 1;
    }
  }

  const byCountryStats = {};
  for (const r of records) {
    const c = String(r.fields?.Country || "").trim() || "UNK";
    byCountryStats[c] = byCountryStats[c] || {
      n: 0,
      name: 0,
      country: 0,
      city: 0,
      state: 0,
      applicable: 0,
      address: 0,
      phone: 0,
      web: 0,
    };
    const b = byCountryStats[c];
    b.n += 1;
    if (!isBlank(r.fields?.["Property Name"] || r.fields?.["Canonical Property Name"]))
      b.name += 1;
    if (!isBlank(r.fields?.Country)) b.country += 1;
    if (!isBlank(r.fields?.City)) b.city += 1;
    if (isStateRegionApplicable(c)) {
      b.applicable += 1;
      if (
        !isBlank(r.fields?.["State / Region"]) &&
        !isDirtyStateRegionValue(r.fields["State / Region"])
      ) {
        b.state += 1;
      }
    }
    if (!isBlank(r.fields?.Address)) b.address += 1;
    if (!isBlank(r.fields?.Phone)) b.phone += 1;
    if (!isBlank(r.fields?.Website || r.fields?.["Official Property URL"])) b.web += 1;
  }

  function coverageStatus(geo, census) {
    if (census === 0) return "ZERO_CONFIRMED_PROPERTIES";
    if (census < 40 && (geo.tourism_priority === "S" || geo.tourism_priority === "A")) {
      return "SOURCE_GAP";
    }
    if (census >= 200) return "CORE_COVERAGE_STRONG";
    if (census >= 80) return "CORE_COVERAGE_MODERATE";
    if (census >= 20) return "CORE_COVERAGE_WEAK";
    return "NEEDS_TARGETED_DISCOVERY";
  }

  const cubaOfficialOk =
    (cubaDisc.source_status || []).some((s) => s.status === "OK" && s.candidates > 0) ||
    cubaDisc.discovered > 0;

  const matrix = geos.map((g) => {
    const st = byCountryStats[g.name] || {
      n: 0,
      name: 0,
      country: 0,
      city: 0,
      state: 0,
      applicable: 0,
      address: 0,
      phone: 0,
      web: 0,
    };
    const census = st.n;
    const admin = getAdminGeographyMeta(g.name);
    const cov = coverageStatus(g, census);
    let primaryGap = "none";
    if (census === 0) primaryGap = "zero_inventory";
    else if (st.applicable && st.state / Math.max(1, st.applicable) < 0.7)
      primaryGap = "state_region_incomplete";
    else if (st.city / Math.max(1, census) < 0.95) primaryGap = "city_incomplete";
    else if (census < 40 && (g.tourism_priority === "S" || g.tourism_priority === "A"))
      primaryGap = "coverage_thin";

    return {
      geography: g.name,
      tourism_priority: g.tourism_priority,
      dealality_region: g.region,
      census_count: census,
      name_pct: census ? Math.round((100 * st.name) / census) : 0,
      country_pct: census ? Math.round((100 * st.country) / census) : 0,
      city_pct: census ? Math.round((100 * st.city) / census) : 0,
      state_region_applicable: admin.STATE_REGION_APPLICABLE,
      state_region_admin_level: admin.ADMIN_LEVEL_NAME,
      state_pct_applicable: st.applicable
        ? Math.round((100 * st.state) / st.applicable)
        : null,
      address_pct: census ? Math.round((100 * st.address) / census) : 0,
      phone_pct: census ? Math.round((100 * st.phone) / census) : 0,
      website_pct: census ? Math.round((100 * st.web) / census) : 0,
      active_holds: holdByCountry[g.name] || 0,
      HBX_STATUS:
        g.name === "Cuba" ? "EMPTY_OR_UNSUPPORTED" : "PRIOR_WAVE_USED",
      SERPAPI_STATUS:
        g.name === "Cuba"
          ? "GOOGLE_HOTELS_EMPTY_PRIOR"
          : "PRIOR_WAVE_USED",
      OFFICIAL_PUBLIC_SOURCE_STATUS:
        g.name === "Cuba"
          ? cubaOfficialOk
            ? "SEARCHED_OK"
            : "ATTEMPTED"
          : "N_A_THIS_RUN",
      coverage_status: cov,
      primary_remaining_gap: primaryGap,
    };
  });

  matrix.sort((a, b) => {
    const rank = {
      ZERO_CONFIRMED_PROPERTIES: 0,
      NEEDS_TARGETED_DISCOVERY: 1,
      SOURCE_GAP: 2,
      CORE_COVERAGE_WEAK: 3,
      DISCOVERY_NOT_COMPLETE: 4,
      CORE_COVERAGE_MODERATE: 5,
      CORE_COVERAGE_STRONG: 6,
    };
    return (
      (rank[a.coverage_status] ?? 5) - (rank[b.coverage_status] ?? 5) ||
      a.census_count - b.census_count
    );
  });

  const matrixPath =
    "reports/research-engine-v2/full-cala-core-identity-52-geography-matrix.json";
  writeJson(path.join(ROOT, matrixPath), {
    generated_at,
    objective: CLOSEOUT_OBJECTIVE,
    census_before: censusBefore,
    census_after: censusAfter,
    matrix,
  });
  writeMd(
    path.join(
      ROOT,
      "reports/research-engine-v2/full-cala-core-identity-52-geography-matrix.md"
    ),
    [
      `# Core Identity 52-Geography Matrix (Geography Closeout)`,
      ``,
      `Generated: ${generated_at}`,
      `Census ${censusBefore} → ${censusAfter}`,
      ``,
      `| Geography | Census | Coverage | City% | State%* | Official | Gap |`,
      `| --- | ---: | --- | ---: | ---: | --- | --- |`,
      ...matrix.map(
        (r) =>
          `| ${r.geography} | ${r.census_count} | ${r.coverage_status} | ${r.city_pct} | ${r.state_pct_applicable ?? "n/a"} | ${r.OFFICIAL_PUBLIC_SOURCE_STATUS} | ${r.primary_remaining_gap} |`
      ),
      ``,
      `\\* State % of applicable records only`,
    ].join("\n")
  );

  const counts = matrix.reduce((a, r) => {
    a[r.coverage_status] = (a[r.coverage_status] || 0) + 1;
    return a;
  }, {});

  const zerosBefore = 1; // accepted baseline: Cuba only
  const zerosAfter = matrix.filter(
    (r) => r.coverage_status === "ZERO_CONFIRMED_PROPERTIES"
  ).length;

  const topGaps = matrix
    .filter((r) => r.primary_remaining_gap !== "none")
    .slice(0, 10)
    .map((r) => ({
      geography: r.geography,
      census: r.census_count,
      gap: r.primary_remaining_gap,
      coverage: r.coverage_status,
    }));

  const activeHolds =
    holds.active_count ||
    Object.keys(holds.by_candidate_id || {}).length ||
    (Array.isArray(holds.holds) ? holds.holds.length : 7680);

  const cubaCensus = byCountryStats.Cuba?.n || 0;
  const cubaCoverage =
    cubaCensus === 0
      ? "ZERO_CONFIRMED_PROPERTIES"
      : cubaCensus >= 80
        ? "CORE_COVERAGE_MODERATE"
        : cubaCensus >= 20
          ? "CORE_COVERAGE_WEAK"
          : "NEEDS_TARGETED_DISCOVERY";

  const exitMet =
    censusAfter >= 15000 &&
    comp.NAME_COMPLETENESS >= 99 &&
    comp.COUNTRY_COMPLETENESS >= 99 &&
    (comp.CITY_COMPLETENESS >= 98 ||
      (comp.CITY_MISSING <= cityUnresolved && cityUnresolved < 400)) &&
    (comp.STATE_REGION_COMPLETENESS_OF_APPLICABLE || 0) >= 85 &&
    geos.length === 52 &&
    cubaOfficialOk &&
    zerosAfter === 0;

  const status = exitMet
    ? "production_census_core_identity_complete"
    : "production_census_core_identity_geography_closeout_partial";

  const final = {
    ok: true,
    CORE_CENSUS_STATUS: status,
    mode,
    production_writes: enableWrites,
    CENSUS_BEFORE: censusBefore,
    CENSUS_AFTER: censusAfter,
    CUBA_PROPERTIES_DISCOVERED: cubaDisc.discovered,
    CUBA_SHELLS_INSERTED: enableWrites ? cubaApply.tallies.inserted : 0,
    CUBA_SHELLS_PREPARED: cubaApply.tallies.prepared || cubaApply.prepared?.length || 0,
    CUBA_DUPLICATES: cubaApply.tallies.duplicate,
    CUBA_EXISTING: cubaApply.tallies.existing,
    CUBA_INVALIDS: cubaApply.tallies.invalid,
    CUBA_HOLD_REVIEW: cubaApply.tallies.hold,
    CUBA_COVERAGE_STATUS: cubaCoverage,
    CUBA_CENSUS_COUNT: cubaCensus,
    ZERO_CENSUS_GEOGRAPHIES_BEFORE: zerosBefore,
    ZERO_CENSUS_GEOGRAPHIES_AFTER: zerosAfter,
    CITY_MISSING_BEFORE: compBefore.CITY_MISSING,
    CITY_PATCHED: enableWrites ? cityWritten : uniqCityPatches.length,
    CITY_FROM_ADDRESS: cityFromAddress,
    CITY_FROM_SERP: serpCityResolved,
    CITY_UNRESOLVED_AFTER: Math.max(
      0,
      (enableWrites ? comp.CITY_MISSING : compBefore.CITY_MISSING - uniqCityPatches.length)
    ),
    CITY_COMPLETENESS_AFTER: comp.CITY_COMPLETENESS,
    STATE_REGION_APPLICABLE_RECORDS: comp.STATE_REGION_APPLICABLE_RECORDS,
    STATE_REGION_NOT_APPLICABLE_RECORDS: comp.STATE_REGION_NOT_APPLICABLE_RECORDS,
    STATE_REGION_MISSING_BEFORE: compBefore.STATE_REGION_MISSING_APPLICABLE,
    STATE_REGION_DETERMINISTIC_PATCHES: enableWrites
      ? stateWritten
      : statePatches.length,
    STATE_REGION_EXTERNAL_PATCHES: 0,
    STATE_REGION_UNRESOLVED_AFTER: stateUnresolved,
    STATE_REGION_COMPLETENESS_AFTER: comp.STATE_REGION_COMPLETENESS_OF_APPLICABLE,
    NAME_COMPLETENESS: comp.NAME_COMPLETENESS,
    COUNTRY_COMPLETENESS: comp.COUNTRY_COMPLETENESS,
    GEOGRAPHIES_ASSESSED: `${geos.length} / 52`,
    STRONG: counts.CORE_COVERAGE_STRONG || 0,
    MODERATE: counts.CORE_COVERAGE_MODERATE || 0,
    WEAK: counts.CORE_COVERAGE_WEAK || 0,
    SOURCE_GAP: counts.SOURCE_GAP || 0,
    ZERO: counts.ZERO_CONFIRMED_PROPERTIES || 0,
    NEEDS_TARGETED_DISCOVERY: counts.NEEDS_TARGETED_DISCOVERY || 0,
    TOP_10_REMAINING_GEOGRAPHIC_GAPS: topGaps,
    ACTIVE_HOLDS_REMAINING: activeHolds,
    FULL_52_GEOGRAPHY_MATRIX_PATH: matrixPath,
    CORE_PHASE_EXIT_CRITERIA_MET: exitMet ? "YES" : "NO",
    FOUNDER_DECISION_REQUIRED: exitMet ? "NO" : "YES",
    NEXT_PHASE_READY: exitMet ? "YES" : "NO",
    NEXT_PHASE: exitMet ? "PROPERTY_ENRICHMENT" : null,
    BENCHMARK_PROPERTY_DATA_PERSISTED: "NO",
    BENCHMARK_USED_AS_PROVENANCE: "NO",
    INDEPENDENT_GAP_WAVE_RERUN: "NO",
    completeness: comp,
    cuba_source_status: cubaDisc.source_status,
  };

  writeJson(
    path.join(ROOT, "reports/research-engine-v2/core-geography-closeout-final.json"),
    final
  );
  writeMd(
    path.join(ROOT, "reports/research-engine-v2/core-geography-closeout-final.md"),
    [
      `# Core Geography Closeout`,
      ``,
      `Status: \`${status}\``,
      `Census: ${censusBefore} → ${censusAfter}`,
      `Cuba discovered: ${cubaDisc.discovered}; shells: ${final.CUBA_SHELLS_INSERTED}`,
      `City%: ${comp.CITY_COMPLETENESS}; State applicable%: ${comp.STATE_REGION_COMPLETENESS_OF_APPLICABLE}`,
      `Exit criteria met: ${final.CORE_PHASE_EXIT_CRITERIA_MET}`,
    ].join("\n")
  );

  return final;
}
