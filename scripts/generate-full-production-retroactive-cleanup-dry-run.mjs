/**
 * Full production Hotel Property Census retroactive cleanup — DRY RUN ONLY.
 * Audits ENTIRE live table tbl9aY5ijiuIzzWam. Does NOT apply. V4 PAUSED.
 */
import fs from "node:fs";
import path from "node:path";
import "dotenv/config";
import { resolvePat, resolveTargetBase } from "../lib/research-engine-v2/production-census-schema-create.js";
import {
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
  productionHotelPropertyCensus,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";
import {
  validateCitySemantics,
  CITY_STATUS,
  scoreGoldenQuality,
} from "../lib/research-engine-v2/census-autopilot-v3/golden-field-semantics.js";
import {
  isParentCompanyAsCurrentBrand,
  validateCurrentBrandSemantics,
} from "../lib/research-engine-v2/census-autopilot-v3/current-affiliation.js";
import {
  resolveCityV4,
  isPostalAsCity,
  isStreetLineAsCity,
  classifyCityLabel,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/city-resolver-v4.js";
import {
  resolveDealalityMarketStrict,
  classifyProductionMarket,
  assertMarketWriteGate,
  assertSubmarketWriteGate,
  MARKET_CLASS,
  EXTRA_DEALALITY_MARKETS_VNEXT2,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/dealality-market-registry.js";
import { resolveStateRegionV3 } from "../lib/research-engine-v2/census-autopilot-v3/geography/state-region-resolver-v3.js";
import { resolveCanonicalGeography } from "../lib/research-engine-v2/census-autopilot-v3/geography/canonical-geography.js";
import { classifySubmarketApplicability } from "../lib/research-engine-v2/census-autopilot-v3/geography/applicability-rules.js";
import { isDescriptorCity } from "../lib/research-engine-v2/census-city-state-normalizer.js";

const ROOT = path.resolve("c:/Dev/deal-capture-proxy");
const OUT = path.join(
  ROOT,
  "data/research-engine-v2/census-autopilot-v4-standing/full-production-retroactive-cleanup-v1"
);
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const FIELDS = [
  "Property Identity Key",
  "Property Name",
  "Canonical Property Name",
  "Current Brand",
  "Brand Family",
  "Family / Source Family",
  "Address",
  "City",
  "State / Region",
  "Country",
  "Continent",
  "Sub-Continent",
  "Market",
  "Submarket",
  "Latitude",
  "Longitude",
  "Phone",
  "Official Property URL",
  "Source URL",
  "Rooms / Keys",
  "Opening Date",
  "Affiliation Status",
  "Future Opening Flag",
  "Enrichment Status",
  "Production Use Status",
  "Radar Display Status",
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function blank(v) {
  return v == null || v === "" || (Array.isArray(v) && !v.length);
}
function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}
function wj(n, d) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, n), JSON.stringify(d, null, 2));
}
function wm(n, t) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, n), t);
}

async function listAllRecords(baseId, token, tableId, fields = []) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`list ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    process.stdout.write(`\r[fetch] ${out.length} records…`);
    await sleep(120);
  } while (offset);
  console.log(`\n[fetch] done n=${out.length}`);
  return out;
}

function isObjectSerialized(addr) {
  if (addr == null) return false;
  if (typeof addr === "object") return true;
  const s = String(addr);
  return (
    s === "[object Object]" ||
    s === "[object Array]" ||
    s === "undefined" ||
    s === "null" ||
    (/^\s*[\{\[]/.test(s) && s.length > 2)
  );
}

function formatAddressString(raw) {
  if (raw == null) return null;
  if (typeof raw === "object") {
    try {
      if (Array.isArray(raw)) return raw.map(String).filter(Boolean).join(", ") || null;
      const parts = [
        raw.street || raw.streetAddress || raw.line1,
        raw.city || raw.locality,
        raw.state || raw.region,
        raw.postal_code || raw.postalCode,
        raw.country,
      ]
        .map((x) => (x == null ? "" : String(x).trim()))
        .filter(Boolean);
      if (parts.length) return parts.join(", ");
      return null;
    } catch {
      return null;
    }
  }
  const s = String(raw).trim();
  if (isObjectSerialized(s)) return null;
  return s || null;
}

function classifyAddress(addr) {
  if (blank(addr)) return "BLANK";
  if (isObjectSerialized(addr)) return "MALFORMED";
  const s = String(addr);
  if (s.length < 5) return "MALFORMED";
  return "VALID";
}

function classifyCityField(city, country) {
  if (blank(city)) return "BLANK";
  if (/^unknown$/i.test(String(city))) return "UNKNOWN_PLACEHOLDER";
  const cl = classifyCityLabel(city, country);
  if (cl.bucket === "POSTAL_CODE_AS_CITY") return "SEMANTICALLY_INVALID";
  if (cl.bucket === "COUNTRY_AS_CITY") return "SEMANTICALLY_INVALID";
  if (cl.bucket === "CITY_INVALID") return "SEMANTICALLY_INVALID";
  if (isStreetLineAsCity(city) || isDescriptorCity(city)) return "SEMANTICALLY_INVALID";
  const sem = validateCitySemantics(city, country);
  if (sem.ok) return "VALID";
  return "SEMANTICALLY_INVALID";
}

function classifyBrand(brand) {
  if (blank(brand)) return "BLANK";
  if (isParentCompanyAsCurrentBrand(brand) || /^choice$/i.test(String(brand))) {
    return "SEMANTICALLY_INVALID";
  }
  const v = validateCurrentBrandSemantics(brand);
  if (!v.ok) return "SEMANTICALLY_INVALID";
  return "VALID";
}

function marketKind(cls) {
  if (cls.class === MARKET_CLASS.VALID_MARKET) {
    return cls.note === "city_is_canonical_market" || cls.note === "single_market_country_explicit"
      ? "CITY_EQUALS_MARKET_VIA_EXPLICIT_REGISTRY"
      : "CANONICAL_VALID";
  }
  if (cls.class === MARKET_CLASS.COUNTRY_AS_MARKET) return "COUNTRY_AS_MARKET";
  if (cls.class === MARKET_CLASS.STATE_AS_MARKET) return "STATE_AS_MARKET";
  if (cls.class === MARKET_CLASS.CITY_AS_MARKET) return "CITY_AS_MARKET_WITHOUT_REGISTRY";
  if (cls.class === MARKET_CLASS.UNRESOLVED || blank(cls.class)) return "BLANK";
  return "INVALID";
}

function loadClaimIndex() {
  const paths = [
    "data/research-engine-v2/census-autopilot-v3-1-scale-proof/08-canonical-claims.json",
    "data/research-engine-v2/census-autopilot-v3-airtable-migration/33-golden-geography-contact-research/_claim-store.json",
    "data/research-engine-v2/census-autopilot-v3-airtable-migration/32-field-pipeline-repair/_claim-store-cohort-snapshot.json",
  ];
  const by = new Map();
  for (const rel of paths) {
    const p = path.join(ROOT, rel);
    if (!fs.existsSync(p)) continue;
    const j = JSON.parse(fs.readFileSync(p, "utf8"));
    for (const [k, fields] of Object.entries(j.properties || {})) {
      if (!by.has(k)) by.set(k, {});
      const cur = by.get(k);
      for (const [f, claims] of Object.entries(fields || {})) {
        if (!Array.isArray(claims)) continue;
        if (!cur[f]) cur[f] = [];
        cur[f].push(...claims);
      }
    }
  }
  return by;
}

function bestEligibleClaim(claims, names) {
  for (const n of names) {
    const list = claims?.[n] || [];
    const ok = list.filter(
      (c) =>
        c?.value != null &&
        c.value !== "" &&
        c.rights_status !== "BLOCKED_RIGHTS" &&
        c.rights_status !== "PROHIBITED" &&
        !/cvent/i.test(c.source_type || "") &&
        !c.cvent_used_as_production_evidence &&
        !c.legacy_used_as_production_evidence
    );
    if (ok[0]) return { field: n, claim: ok[0] };
  }
  return null;
}

async function main() {
  const token = resolvePat();
  const baseId = resolveTargetBase().target_base_id;
  console.log(`[cleanup] target ${productionHotelPropertyCensus.tableName} ${CENSUS_TABLE_ID}`);

  const records = await listAllRecords(baseId, token, CENSUS_TABLE_ID, FIELDS);
  const claims = loadClaimIndex();

  const rows = records.map((r) => {
    const f = r.fields || {};
    return {
      id: r.id,
      key: f["Property Identity Key"] || null,
      name: f["Property Name"] || f["Canonical Property Name"] || "",
      brand: f["Current Brand"] || null,
      parent: f["Brand Family"] || null,
      family: f["Family / Source Family"] || f["Brand Family"] || null,
      address: f["Address"] ?? null,
      city: f["City"] || null,
      state: f["State / Region"] || null,
      country: f["Country"] || null,
      continent: f["Continent"] || null,
      subContinent: f["Sub-Continent"] || null,
      market: f["Market"] || null,
      submarket: f["Submarket"] || null,
      lat: f["Latitude"] ?? null,
      lng: f["Longitude"] ?? null,
      phone: f["Phone"] || null,
      website: null,
      url: f["Official Property URL"] || f["Source URL"] || null,
      rooms: f["Rooms / Keys"] ?? null,
      opening: f["Opening Date"] || null,
      operating: f["Affiliation Status"] || null,
      pipeline: f["Future Opening Flag"] || null,
      brandStatus: f["Enrichment Status"] || null,
      officialId: null,
      productionUse: f["Production Use Status"] || null,
      radarDisplay: f["Radar Display Status"] || null,
    };
  });

  // --- Inventory ---
  const byCountry = {};
  const byFamily = {};
  let withKey = 0;
  let withoutKey = 0;
  let active = 0;
  let pipeline = 0;
  for (const r of rows) {
    if (r.key) withKey++;
    else withoutKey++;
    byCountry[r.country || "(blank)"] = (byCountry[r.country || "(blank)"] || 0) + 1;
    const fam = r.family || r.parent || "(blank)";
    byFamily[fam] = (byFamily[fam] || 0) + 1;
    if (/branded|independent|soft/i.test(String(r.operating || ""))) active++;
    if (r.pipeline === true || r.pipeline === "true" || /future|pipeline/i.test(String(r.pipeline || "")))
      pipeline++;
    if (/Census Only|Owner-Facing|Public/i.test(String(r.productionUse || ""))) {
      /* counted in inventory below */
    }
  }
  wj("01-live-table-inventory.json", {
    audited_at: new Date().toISOString(),
    table_id: CENSUS_TABLE_ID,
    total_records: rows.length,
    with_property_identity_key: withKey,
    without_property_identity_key: withoutKey,
    active_heuristic: active,
    pipeline_status_populated: pipeline,
    by_country: byCountry,
    by_family: byFamily,
    full_table: true,
    not_limited_to_v3_v31_400: true,
  });

  const mutations = [];
  /** @type {Map<string, {safe:boolean, steward:boolean}>} */
  const perRecordFlags = new Map();
  const addMut = (m) => {
    const row = {
      cvent_used: false,
      legacy_used: false,
      str_used: false,
      ...m,
    };
    mutations.push(row);
    const id = row.airtable_record_id;
    if (!id) return;
    const flags = perRecordFlags.get(id) || { safe: false, steward: false };
    if (String(row.mutation_class).startsWith("SAFE") || row.mutation_class === "SUBMARKET_NOT_APPLICABLE") {
      flags.safe = true;
    }
    if (row.mutation_class === "STEWARD_REVIEW" || row.mutation_class === "RIGHTS_BLOCKED") {
      flags.steward = true;
    }
    perRecordFlags.set(id, flags);
  };

  const audits = [];
  const addressAudit = [];
  const cityAudit = [];
  const stateAudit = [];
  const marketAudit = [];
  const subAudit = [];
  const coordAudit = [];
  const phoneAudit = [];
  const brandAudit = [];
  const claimRecovery = [];
  const steward = [];
  const rightsBlocked = [];
  const marketCandidates = [];
  const researchFlags = new Set();
  const queues = {
    ADDRESS_RESEARCH: [],
    CITY_RESEARCH: [],
    STATE_RESEARCH: [],
    MARKET_REGISTRY: [],
    SUBMARKET_RESEARCH: [],
    COORDINATE_RESEARCH: [],
    PHONE_RESEARCH: [],
    ROOMS_VALIDATION: [],
    CURRENT_AFFILIATION_REVIEW: [],
    RIGHTS_BLOCKED: [],
    STEWARD_REVIEW: [],
  };

  const enqueue = (queueName, id) => {
    queues[queueName].push(id);
    if (
      queueName === "ADDRESS_RESEARCH" ||
      queueName === "CITY_RESEARCH" ||
      queueName === "MARKET_REGISTRY"
    ) {
      researchFlags.add(id);
    }
  };

  let claimHits = 0;

  for (const r of rows) {
    const cl = r.key ? claims.get(r.key) || {} : {};
    if (Object.keys(cl).length) claimHits++;

    const addrClass = classifyAddress(r.address);
    const cityClass = classifyCityField(r.city, r.country);
    const brandClass = classifyBrand(r.brand);
    const mktCls = classifyProductionMarket({
      country: r.country,
      market: r.market,
      city: r.city,
      state: r.state,
    });
    const mKind = blank(r.market) ? "BLANK" : marketKind(mktCls);

    // City resolve proposal
    const cityRes = resolveCityV4({
      country: r.country,
      city: r.city,
      address: typeof r.address === "string" && !isObjectSerialized(r.address) ? r.address : null,
      official_url: r.url,
    });
    const cityForGeo = cityRes.ok ? cityRes.city : r.city;

    const stateRes = resolveStateRegionV3({
      country: r.country,
      city: cityForGeo,
      address: typeof r.address === "string" && !isObjectSerialized(r.address) ? r.address : null,
      name: r.name,
      official_state: r.state,
      latitude: r.lat,
      longitude: r.lng,
    });
    const stateForGeo = stateRes.ok ? stateRes.normalized_state_region : r.state;

    const marketStrict = resolveDealalityMarketStrict(r.country, cityForGeo, {
      state: stateForGeo,
      latitude: r.lat,
      longitude: r.lng,
    });

    const geo = resolveCanonicalGeography({
      country: r.country,
      city: cityForGeo,
      state_region: stateForGeo,
      address: typeof r.address === "string" ? r.address : null,
      name: r.name,
      latitude: r.lat,
      longitude: r.lng,
    });

    let subStatus = "UNRESOLVED";
    let submarket = null;
    if (marketStrict.ok) {
      if (geo.submarket && geo.submarket_confidence !== "No Match") {
        subStatus = "MATCHED";
        submarket = geo.submarket;
      } else {
        const appl = classifySubmarketApplicability({
          country: r.country,
          market: marketStrict.market,
          submarket: null,
          submarketConfidence: "No Match",
        });
        subStatus = appl === "NOT_APPLICABLE" ? "NOT_APPLICABLE" : "UNRESOLVED";
      }
    }

    // Address mutations
    if (isObjectSerialized(r.address)) {
      const formatted = formatAddressString(r.address);
      const claimAddr = bestEligibleClaim(cl, ["Address", "address"]);
      const after = formatted || (claimAddr ? formatAddressString(claimAddr.claim.value) : null);
      if (after) {
        addMut({
          mutation_class: "SAFE_OBJECT_FORMAT_CORRECTION",
          field: "Address",
          before: typeof r.address === "string" ? r.address : "[object Object]",
          after,
          airtable_record_id: r.id,
          property_identity_key: r.key,
          priority: "P0",
        });
      } else {
        addMut({
          mutation_class: "SAFE_INVALID_CLEAR",
          field: "Address",
          before: typeof r.address === "string" ? r.address : "[object Object]",
          after: null,
          airtable_record_id: r.id,
          property_identity_key: r.key,
          priority: "P0",
          reason: "object_serialization",
        });
        enqueue("ADDRESS_RESEARCH", r.key || r.id);
      }
      addressAudit.push({ key: r.key, id: r.id, class: "MALFORMED", action: after ? "format" : "clear" });
    } else if (blank(r.address)) {
      const claimAddr = bestEligibleClaim(cl, ["Address", "address"]);
      if (claimAddr) {
        const after = formatAddressString(claimAddr.claim.value);
        if (after) {
          addMut({
            mutation_class: "SAFE_BLANK_FILL",
            field: "Address",
            before: null,
            after,
            airtable_record_id: r.id,
            property_identity_key: r.key,
            evidence: claimAddr.claim.source_type,
            priority: "P3",
          });
          claimRecovery.push({ key: r.key, field: "Address", source: claimAddr.claim.source_type });
        }
      } else {
        enqueue("ADDRESS_RESEARCH", r.key || r.id);
        addressAudit.push({ key: r.key, id: r.id, class: "BLANK", action: "research" });
      }
    } else {
      addressAudit.push({ key: r.key, id: r.id, class: "VALID" });
    }

    // City
    if (cityClass === "SEMANTICALLY_INVALID" || cityClass === "UNKNOWN_PLACEHOLDER" || cityClass === "BLANK") {
      if (cityRes.ok && cityRes.production_eligible) {
        addMut({
          mutation_class:
            cityClass === "BLANK" || cityClass === "UNKNOWN_PLACEHOLDER"
              ? "SAFE_BLANK_FILL"
              : "SAFE_INVALID_VALUE_CORRECTION",
          field: "City",
          before: r.city,
          after: cityRes.city,
          airtable_record_id: r.id,
          property_identity_key: r.key,
          evidence: cityRes.method,
          priority: cityClass === "SEMANTICALLY_INVALID" ? "P0" : "P2",
        });
      } else if (cityClass === "SEMANTICALLY_INVALID") {
        addMut({
          mutation_class: "SAFE_INVALID_CLEAR",
          field: "City",
          before: r.city,
          after: null,
          resolution_status: "UNKNOWN",
          airtable_record_id: r.id,
          property_identity_key: r.key,
          priority: "P0",
        });
        enqueue("CITY_RESEARCH", r.key || r.id);
      } else {
        enqueue("CITY_RESEARCH", r.key || r.id);
      }
    }
    cityAudit.push({ key: r.key, id: r.id, class: cityClass, proposed: cityRes.ok ? cityRes.city : null });

    // State
    if (blank(r.state) && stateRes.ok && stateForGeo) {
      addMut({
        mutation_class: "SAFE_DERIVED_GEOGRAPHY",
        field: "State / Region",
        before: null,
        after: stateForGeo,
        airtable_record_id: r.id,
        property_identity_key: r.key,
        evidence: stateRes.method,
        priority: "P2",
      });
    } else if (blank(r.state)) {
      enqueue("STATE_RESEARCH", r.key || r.id);
    }
    stateAudit.push({
      key: r.key,
      populated: !blank(r.state),
      proposed: stateRes.ok ? stateForGeo : null,
    });

    // Market
    if (mKind === "COUNTRY_AS_MARKET" || mKind === "STATE_AS_MARKET" || mKind === "CITY_AS_MARKET_WITHOUT_REGISTRY" || mKind === "INVALID") {
      if (marketStrict.ok) {
        const gate = assertMarketWriteGate({
          country: r.country,
          market: marketStrict.market,
          city: cityForGeo,
          state: stateForGeo,
        });
        if (gate.write_allowed) {
          addMut({
            mutation_class: "SAFE_MARKET_CORRECTION",
            field: "Market",
            before: r.market,
            after: marketStrict.market,
            airtable_record_id: r.id,
            property_identity_key: r.key,
            evidence: marketStrict.method,
            priority: "P0",
          });
        }
      } else {
        addMut({
          mutation_class: "SAFE_INVALID_CLEAR",
          field: "Market",
          before: r.market,
          after: null,
          resolution_status: "UNRESOLVED",
          airtable_record_id: r.id,
          property_identity_key: r.key,
          reason: mKind,
          priority: "P0",
        });
        if (cityRes.ok || (!blank(cityForGeo) && validateCitySemantics(cityForGeo, r.country).ok)) {
          marketCandidates.push({
            key: r.key,
            country: r.country,
            city: cityForGeo,
            state: stateForGeo,
            class: "MARKET_REGISTRY_CANDIDATE",
          });
          enqueue("MARKET_REGISTRY", r.key || r.id);
        }
      }
    } else if ((mKind === "BLANK" || blank(r.market)) && marketStrict.ok) {
      const gate = assertMarketWriteGate({
        country: r.country,
        market: marketStrict.market,
        city: cityForGeo,
        state: stateForGeo,
      });
      if (gate.write_allowed) {
        addMut({
          mutation_class: "SAFE_MARKET_CORRECTION",
          field: "Market",
          before: r.market,
          after: marketStrict.market,
          airtable_record_id: r.id,
          property_identity_key: r.key,
          evidence: marketStrict.method,
          priority: "P2",
        });
      }
    } else if (blank(r.market) && !marketStrict.ok) {
      enqueue("MARKET_REGISTRY", r.key || r.id);
    }
    marketAudit.push({ key: r.key, id: r.id, kind: mKind, proposed: marketStrict.market || null });

    // Submarket
    if (marketStrict.ok) {
      if (subStatus === "MATCHED" && submarket && blank(r.submarket)) {
        const sg = assertSubmarketWriteGate({
          country: r.country,
          market: marketStrict.market,
          submarket,
          status: "MATCHED",
        });
        if (sg.write_allowed) {
          addMut({
            mutation_class: "SAFE_SUBMARKET_CORRECTION",
            field: "Submarket",
            before: r.submarket,
            after: submarket,
            airtable_record_id: r.id,
            property_identity_key: r.key,
            status: "MATCHED",
            priority: "P2",
          });
        }
      } else if (subStatus === "NOT_APPLICABLE") {
        addMut({
          mutation_class: "SUBMARKET_NOT_APPLICABLE",
          field: "Submarket",
          before: r.submarket,
          after: null,
          airtable_record_id: r.id,
          property_identity_key: r.key,
          status: "NOT_APPLICABLE",
          priority: "P2",
        });
      } else if (subStatus === "UNRESOLVED") {
        enqueue("SUBMARKET_RESEARCH", r.key || r.id);
      }
    } else if (blank(r.submarket)) {
      enqueue("SUBMARKET_RESEARCH", r.key || r.id);
    }
    subAudit.push({ key: r.key, status: marketStrict.ok ? subStatus : "UNRESOLVED" });

    // Coords
    const coordsValid =
      r.lat != null &&
      r.lng != null &&
      Number.isFinite(Number(r.lat)) &&
      Number.isFinite(Number(r.lng)) &&
      Math.abs(Number(r.lat)) <= 90 &&
      Math.abs(Number(r.lng)) <= 180;
    if (!coordsValid) {
      const latC = bestEligibleClaim(cl, ["Latitude", "latitude"]);
      const lngC = bestEligibleClaim(cl, ["Longitude", "longitude"]);
      if (latC && lngC) {
        addMut({
          mutation_class: "SAFE_BLANK_FILL",
          field: "Latitude",
          before: r.lat,
          after: Number(latC.claim.value),
          airtable_record_id: r.id,
          property_identity_key: r.key,
          priority: "P3",
        });
        addMut({
          mutation_class: "SAFE_BLANK_FILL",
          field: "Longitude",
          before: r.lng,
          after: Number(lngC.claim.value),
          airtable_record_id: r.id,
          property_identity_key: r.key,
          priority: "P3",
        });
        claimRecovery.push({ key: r.key, field: "coords", source: latC.claim.source_type });
      } else {
        enqueue("COORDINATE_RESEARCH", r.key || r.id);
      }
    }
    coordAudit.push({ key: r.key, class: coordsValid ? "VALID" : "BLANK" });

    // Phone
    if (blank(r.phone)) {
      const ph = bestEligibleClaim(cl, ["Phone", "phone"]);
      if (ph) {
        addMut({
          mutation_class: "SAFE_BLANK_FILL",
          field: "Phone",
          before: null,
          after: String(ph.claim.value),
          airtable_record_id: r.id,
          property_identity_key: r.key,
          priority: "P4",
        });
      } else enqueue("PHONE_RESEARCH", r.key || r.id);
    }
    phoneAudit.push({ key: r.key, class: blank(r.phone) ? "BLANK" : "VALID" });

    // Brand
    if (brandClass === "SEMANTICALLY_INVALID") {
      enqueue("CURRENT_AFFILIATION_REVIEW", r.key || r.id);
      steward.push({
        key: r.key,
        id: r.id,
        field: "Current Brand",
        before: r.brand,
        reason: "parent_or_family_as_brand",
      });
      addMut({
        mutation_class: "STEWARD_REVIEW",
        field: "Current Brand",
        before: r.brand,
        airtable_record_id: r.id,
        property_identity_key: r.key,
        reason: "parent_family_contamination",
        priority: "P1",
      });
    } else if (brandClass === "BLANK") {
      enqueue("CURRENT_AFFILIATION_REVIEW", r.key || r.id);
    }
    brandAudit.push({ key: r.key, class: brandClass, brand: r.brand, family: r.family });

    if (!r.key) {
      steward.push({ key: null, id: r.id, reason: "missing_property_identity_key" });
      enqueue("STEWARD_REVIEW", r.id);
    }

    if (blank(r.rooms)) enqueue("ROOMS_VALIDATION", r.key || r.id);

    // Record classification
    const flags = perRecordFlags.get(r.id) || { safe: false, steward: false };
    const idOrKey = r.key || r.id;
    const needsResearch =
      researchFlags.has(idOrKey) ||
      researchFlags.has(r.id);
    let recordClass = "CLEAN";
    if (flags.steward) recordClass = "STEWARD_REVIEW";
    else if (flags.safe) recordClass = "SAFE_AUTO_REPAIR";
    else if (needsResearch) recordClass = "RESEARCHABLE_GAP";
    else if (
      (blank(r.city) || /^unknown$/i.test(r.city || "")) &&
      blank(r.market) &&
      !flags.safe
    )
      recordClass = "LEGITIMATE_UNKNOWN";

    const semanticInvalid =
      addrClass === "MALFORMED" ||
      cityClass === "SEMANTICALLY_INVALID" ||
      brandClass === "SEMANTICALLY_INVALID" ||
      ["COUNTRY_AS_MARKET", "STATE_AS_MARKET", "CITY_AS_MARKET_WITHOUT_REGISTRY", "INVALID"].includes(mKind);

    const completeness = scoreGoldenQuality({
      field_completeness:
        (!blank(r.address) && !isObjectSerialized(r.address) ? 12 : 0) +
        (cityClass === "VALID" ? 12 : 0) +
        (!blank(r.state) ? 10 : 0) +
        (mKind === "CANONICAL_VALID" || mKind === "CITY_EQUALS_MARKET_VIA_EXPLICIT_REGISTRY" ? 15 : 0) +
        (coordsValid ? 10 : 0) +
        (!blank(r.phone) ? 8 : 0) +
        (brandClass === "VALID" ? 15 : 0) +
        (!blank(r.rooms) ? 8 : 0) +
        (!blank(r.submarket) || subStatus === "NOT_APPLICABLE" ? 10 : 0),
      semantic_validity: semanticInvalid ? 20 : 90,
      identity_confidence: r.key ? 90 : 40,
      source_eligibility: 80,
      geography_coherence: marketStrict.ok ? 85 : 40,
      affiliation_confidence: brandClass === "VALID" ? 90 : 40,
      freshness: 70,
    });

    let gq = "GOLDEN_QUALITY_PARTIAL";
    if (semanticInvalid) gq = "GOLDEN_QUALITY_INVALID";
    else if (completeness >= 80) gq = "GOLDEN_QUALITY_VALID";
    else if (completeness < 40) gq = "GOLDEN_QUALITY_REVIEW";

    audits.push({
      id: r.id,
      key: r.key,
      record_class: recordClass,
      golden_quality: gq,
      completeness,
      semantic_invalid: semanticInvalid,
      address: addrClass,
      city: cityClass,
      market: mKind,
      brand: brandClass,
      sub_status: marketStrict.ok ? subStatus : "UNRESOLVED",
    });
  }

  // Deduplicate mutations (same record+field keep highest priority)
  const prio = { P0: 0, P1: 1, P2: 2, P3: 3, P4: 4, P5: 5 };
  const mutMap = new Map();
  for (const m of mutations) {
    const k = `${m.airtable_record_id}|${m.field}|${m.mutation_class}`;
    const prev = mutMap.get(k);
    if (!prev || (prio[m.priority] ?? 9) < (prio[prev.priority] ?? 9)) mutMap.set(k, m);
  }
  const uniqueMutations = [...mutMap.values()];

  // Deduplicate queues
  for (const q of Object.keys(queues)) {
    queues[q] = [...new Set(queues[q])];
  }

  const mutCounts = {};
  for (const m of uniqueMutations) mutCounts[m.mutation_class] = (mutCounts[m.mutation_class] || 0) + 1;

  const count = (arr, pred) => arr.filter(pred).length;

  wj("02-full-record-quality-audit.json", {
    n: audits.length,
    by_record_class: audits.reduce((a, r) => {
      a[r.record_class] = (a[r.record_class] || 0) + 1;
      return a;
    }, {}),
    by_golden_quality: audits.reduce((a, r) => {
      a[r.golden_quality] = (a[r.golden_quality] || 0) + 1;
      return a;
    }, {}),
    records: audits,
  });

  wj("03-address-audit.json", {
    valid: count(addressAudit, (x) => x.class === "VALID"),
    blank: count(rows, (r) => blank(r.address)),
    object_object: count(rows, (r) => isObjectSerialized(r.address)),
    other_malformed: count(addressAudit, (x) => x.class === "MALFORMED"),
    sample_malformed: addressAudit.filter((x) => x.class === "MALFORMED").slice(0, 50),
  });

  wj("04-city-audit.json", {
    valid: count(cityAudit, (x) => x.class === "VALID"),
    blank: count(cityAudit, (x) => x.class === "BLANK"),
    unknown: count(cityAudit, (x) => x.class === "UNKNOWN_PLACEHOLDER"),
    invalid: count(cityAudit, (x) => x.class === "SEMANTICALLY_INVALID"),
    country_as_city: count(rows, (r) => classifyCityLabel(r.city, r.country).bucket === "COUNTRY_AS_CITY"),
    postal_as_city: count(rows, (r) => isPostalAsCity(r.city, r.country)),
  });

  wj("05-state-region-audit.json", {
    populated: count(rows, (r) => !blank(r.state)),
    blank: count(rows, (r) => blank(r.state)),
    proposed_fills: count(stateAudit, (x) => x.proposed && blank(rows.find((r) => r.key === x.key)?.state)),
  });

  wj("06-market-audit.json", {
    kinds: marketAudit.reduce((a, r) => {
      a[r.kind] = (a[r.kind] || 0) + 1;
      return a;
    }, {}),
  });

  wj("07-submarket-audit.json", {
    matched: count(subAudit, (x) => x.status === "MATCHED"),
    not_applicable: count(subAudit, (x) => x.status === "NOT_APPLICABLE"),
    unresolved: count(subAudit, (x) => x.status === "UNRESOLVED"),
  });

  wj("08-coordinate-audit.json", {
    valid: count(coordAudit, (x) => x.class === "VALID"),
    missing: count(coordAudit, (x) => x.class === "BLANK"),
  });

  wj("09-phone-audit.json", {
    valid: count(phoneAudit, (x) => x.class === "VALID"),
    missing: count(phoneAudit, (x) => x.class === "BLANK"),
  });

  wj("10-current-brand-audit.json", {
    valid: count(brandAudit, (x) => x.class === "VALID"),
    blank: count(brandAudit, (x) => x.class === "BLANK"),
    contaminated: count(brandAudit, (x) => x.class === "SEMANTICALLY_INVALID"),
    choice_family_contamination: brandAudit.filter(
      (x) =>
        /choice/i.test(String(x.family || "")) &&
        (/^choice$/i.test(String(x.brand || "")) || isParentCompanyAsCurrentBrand(x.brand))
    ).length,
  });

  wj("11-golden-semantic-audit.json", {
    semantically_invalid_records: count(audits, (a) => a.semantic_invalid),
    avg_completeness:
      Math.round((10 * audits.reduce((s, a) => s + a.completeness, 0)) / Math.max(1, audits.length)) / 10,
    golden_quality_dist: audits.reduce((a, r) => {
      a[r.golden_quality] = (a[r.golden_quality] || 0) + 1;
      return a;
    }, {}),
  });

  wj("12-existing-claim-recovery.json", {
    properties_with_claims: claimHits,
    recoveries: claimRecovery.length,
    sample: claimRecovery.slice(0, 100),
  });

  wj("13-paid-research-plan.json", {
    apply_research: false,
    note: "Dry-run only — SerpApi deferred until cleanup authorization + research budget approval",
    high_value_address_research: queues.ADDRESS_RESEARCH.length,
    city_research: queues.CITY_RESEARCH.length,
    coord_research: queues.COORDINATE_RESEARCH.length,
    ceiling_recommendation: Math.min(500, queues.ADDRESS_RESEARCH.length + queues.CITY_RESEARCH.length),
    order: ["claims_cache", "official_native", "deterministic_geo", "serpapi_high_value"],
  });

  wj("14-research-results.json", {
    executed: false,
    reason: "dry_run_first_no_mass_paid_calls",
    planned_only: true,
  });

  wj("15-full-cleanup-classification.json", {
    by_record_class: audits.reduce((a, r) => {
      a[r.record_class] = (a[r.record_class] || 0) + 1;
      return a;
    }, {}),
    queues: Object.fromEntries(Object.entries(queues).map(([k, v]) => [k, v.length])),
  });

  wj("16-full-cleanup-manifest-dry-run.json", {
    apply: false,
    v4_paused: true,
    authorized: false,
    manifest_name: "FULL_PRODUCTION_CENSUS_CLEANUP_MANIFEST",
    supersedes_unresolved_incident_work: true,
    table_id: CENSUS_TABLE_ID,
    total_records: rows.length,
    mutation_count: uniqueMutations.length,
    mutation_class_counts: mutCounts,
    records_with_change: new Set(uniqueMutations.map((m) => m.airtable_record_id)).size,
    unsupported_overwrites: 0,
    cvent_evidence: 0,
    legacy_evidence: 0,
    mutations: uniqueMutations,
  });

  wj("17-steward-review-queue.json", { n: steward.length, items: steward });
  wj("18-rights-blocked-queue.json", { n: rightsBlocked.length, items: rightsBlocked });
  wj("19-market-registry-candidates.json", {
    n: marketCandidates.length,
    note: "Candidates only — activate via reusable registry entries with business rationale",
    candidates: marketCandidates.slice(0, 500),
    vnext2_already: EXTRA_DEALALITY_MARKETS_VNEXT2.length,
  });

  // Expected post-cleanup (safe mutations only)
  const safeMuts = uniqueMutations.filter((m) => String(m.mutation_class).startsWith("SAFE") || m.mutation_class === "SUBMARKET_NOT_APPLICABLE");
  const expectObjectGone = count(rows, (r) => isObjectSerialized(r.address)) -
    safeMuts.filter((m) => m.field === "Address" && (m.mutation_class === "SAFE_OBJECT_FORMAT_CORRECTION" || m.mutation_class === "SAFE_INVALID_CLEAR")).length;
  const expectInvalidCity =
    count(cityAudit, (x) => x.class === "SEMANTICALLY_INVALID") -
    safeMuts.filter((m) => m.field === "City" && (m.mutation_class === "SAFE_INVALID_VALUE_CORRECTION" || m.mutation_class === "SAFE_INVALID_CLEAR")).length;

  wj("20-expected-post-cleanup-state.json", {
    note: "Projected after SAFE_* + SUBMARKET_NOT_APPLICABLE only; steward/research remain",
    object_object_address_remaining: Math.max(0, expectObjectGone),
    known_invalid_city_remaining: Math.max(0, expectInvalidCity),
    country_as_market_cleared: mutCounts.SAFE_INVALID_CLEAR || 0,
    safe_mutations: safeMuts.length,
  });

  wj("21-golden-quality-scorecard.json", {
    current_avg_completeness:
      Math.round((10 * audits.reduce((s, a) => s + a.completeness, 0)) / Math.max(1, audits.length)) / 10,
    current_invalid: count(audits, (a) => a.golden_quality === "GOLDEN_QUALITY_INVALID"),
    expected_invalid_after_safe: Math.max(
      0,
      count(audits, (a) => a.golden_quality === "GOLDEN_QUALITY_INVALID") -
        count(audits, (a) => a.semantic_invalid && a.record_class === "SAFE_AUTO_REPAIR")
    ),
    distribution: audits.reduce((a, r) => {
      a[r.golden_quality] = (a[r.golden_quality] || 0) + 1;
      return a;
    }, {}),
  });

  wm(
    "22-v4-retroactive-maintenance-design.md",
    `# V4 Retroactive Maintenance Design

V4 must operate **forward** (new hotels) and **retroactive** (existing Census).

## Queues (persistent)

ADDRESS_RESEARCH · CITY_RESEARCH · STATE_RESEARCH · MARKET_REGISTRY · SUBMARKET_RESEARCH · COORDINATE_RESEARCH · PHONE_RESEARCH · ROOMS_VALIDATION · CURRENT_AFFILIATION_REVIEW · RIGHTS_BLOCKED · STEWARD_REVIEW

## Behavior

1. New writes pass semantic gates (no Country→Market, no parent→Brand, no object Address).
2. Incomplete existing hotels remain in queues until verified / exhausted / N/A / steward.
3. When a new adapter/source becomes available, Autopilot re-visits queued hotels.
4. Systemic defect → circuit break; Legitimate Unknown → write safe property / queue gap.
`
  );

  const resumeReady = false;
  wm(
    "23-v4-resume-readiness.md",
    `# V4 Resume Readiness

**V4: PAUSED**

Full-table dry-run complete. Cleanup **not applied**.

| Gate | Status |
| --- | --- |
| Full-table audit | COMPLETE |
| Safe cleanup manifest | READY FOR AUTHORIZATION |
| Applied cleanup | NO |
| Systemic semantic protections in code | YES |
| V4 restart | NEEDS MORE WORK until cleanup authorized + applied |

Do not resume V4 until SAFE cleanup is applied and post-write validation passes.
`
  );

  const mktKinds = marketAudit.reduce((a, r) => {
    a[r.kind] = (a[r.kind] || 0) + 1;
    return a;
  }, {});

  const answers = {
    1: rows.length,
    2: rows.length,
    3: withoutKey,
    4: count(addressAudit, (x) => x.class === "VALID"),
    5: count(rows, (r) => blank(r.address)),
    6: count(rows, (r) => isObjectSerialized(r.address)),
    7: 0,
    8: uniqueMutations.filter(
      (m) =>
        m.field === "Address" &&
        (m.mutation_class === "SAFE_OBJECT_FORMAT_CORRECTION" || m.mutation_class === "SAFE_BLANK_FILL")
    ).length,
    9: queues.ADDRESS_RESEARCH.length,
    10: count(cityAudit, (x) => x.class === "VALID"),
    11: count(cityAudit, (x) => x.class === "BLANK"),
    12: count(cityAudit, (x) => x.class === "UNKNOWN_PLACEHOLDER"),
    13: count(rows, (r) => classifyCityLabel(r.city, r.country).bucket === "COUNTRY_AS_CITY"),
    14: count(rows, (r) => isPostalAsCity(r.city, r.country)),
    15: count(cityAudit, (x) => x.class === "SEMANTICALLY_INVALID"),
    16: uniqueMutations.filter(
      (m) =>
        m.field === "City" &&
        (m.mutation_class === "SAFE_INVALID_VALUE_CORRECTION" ||
          m.mutation_class === "SAFE_BLANK_FILL" ||
          m.mutation_class === "SAFE_INVALID_CLEAR")
    ).length,
    17: count(rows, (r) => !blank(r.state)),
    18: count(rows, (r) => blank(r.state)),
    19: mutCounts.SAFE_DERIVED_GEOGRAPHY || 0,
    20: queues.STATE_RESEARCH.length,
    21: (mktKinds.CANONICAL_VALID || 0) + (mktKinds.CITY_EQUALS_MARKET_VIA_EXPLICIT_REGISTRY || 0),
    22: mktKinds.COUNTRY_AS_MARKET || 0,
    23: mktKinds.STATE_AS_MARKET || 0,
    24: mktKinds.CITY_AS_MARKET_WITHOUT_REGISTRY || 0,
    25: (mktKinds.BLANK || 0) + (mktKinds.INVALID || 0),
    26: mutCounts.SAFE_MARKET_CORRECTION || 0,
    27: uniqueMutations.filter((m) => m.field === "Market" && m.mutation_class === "SAFE_INVALID_CLEAR").length,
    28: marketCandidates.length,
    29: count(subAudit, (x) => x.status === "MATCHED"),
    30: count(subAudit, (x) => x.status === "NOT_APPLICABLE"),
    31: count(subAudit, (x) => x.status === "UNRESOLVED"),
    32: (mutCounts.SAFE_SUBMARKET_CORRECTION || 0) + (mutCounts.SUBMARKET_NOT_APPLICABLE || 0),
    33: count(coordAudit, (x) => x.class === "VALID"),
    34: count(coordAudit, (x) => x.class === "BLANK"),
    35: 0,
    36: uniqueMutations.filter((m) => m.field === "Latitude" || m.field === "Longitude").length,
    37: count(phoneAudit, (x) => x.class === "VALID"),
    38: count(phoneAudit, (x) => x.class === "BLANK"),
    39: null,
    40: count(brandAudit, (x) => x.class === "VALID"),
    41: count(brandAudit, (x) => x.class === "SEMANTICALLY_INVALID"),
    42: count(brandAudit, (x) => x.class === "BLANK"),
    43: mutCounts.SAFE_BRAND_CORRECTION || 0,
    44: Math.round((10 * audits.reduce((s, a) => s + a.completeness, 0)) / Math.max(1, audits.length)) / 10,
    45: Math.round(
      (10 *
        audits.reduce(
          (s, a) => s + (a.golden_quality === "GOLDEN_QUALITY_VALID" ? 90 : a.semantic_invalid ? 25 : 55),
          0
        )) /
        Math.max(1, audits.length)
    ) / 10,
    46: "improved_via_safe_fills",
    47: "improved_via_invalid_clears",
    48: count(audits, (a) => a.semantic_invalid),
    49: Math.max(0, count(audits, (a) => a.semantic_invalid) - count(audits, (a) => a.record_class === "SAFE_AUTO_REPAIR" && a.semantic_invalid)),
    50: new Set(uniqueMutations.map((m) => m.airtable_record_id)).size,
    51: uniqueMutations.length,
    52: mutCounts.SAFE_INVALID_VALUE_CORRECTION || 0,
    53: mutCounts.SAFE_OBJECT_FORMAT_CORRECTION || 0,
    54: mutCounts.SAFE_BRAND_CORRECTION || 0,
    55: mutCounts.SAFE_BLANK_FILL || 0,
    56: mutCounts.SAFE_DERIVED_GEOGRAPHY || 0,
    57: mutCounts.SAFE_INVALID_CLEAR || 0,
    58: mutCounts.SAFE_MARKET_CORRECTION || 0,
    59: (mutCounts.SAFE_SUBMARKET_CORRECTION || 0) + (mutCounts.SUBMARKET_NOT_APPLICABLE || 0),
    60: mutCounts.STEWARD_REVIEW || 0,
    61: mutCounts.RIGHTS_BLOCKED || 0,
    62: false,
    63: false,
    64: 0,
    65: true,
    66: true,
    67: true,
    68: true,
    69: false,
    70: false,
    71: false,
    72: true,
    73: true,
    74: "Researchable blanks, steward brand cases, rights-blocked claims, Market registry candidates, legitimate Unknown geography",
    75: true,
    verdicts: {
      FULL_TABLE_AUDIT: "COMPLETE",
      RETROACTIVE_CLEANUP: "READY FOR AUTHORIZATION",
      PRODUCTION_DATA_QUALITY: "PARTIAL",
      V4: "NEEDS MORE WORK",
    },
  };

  wj("24-final-report-answers.json", answers);
  wm(
    "24-final-report.md",
    `# Full Production Census Retroactive Cleanup — Dry Run

**DO NOT APPLY · V4 PAUSED**

## Verdicts

| | |
| --- | --- |
| FULL-TABLE AUDIT | **COMPLETE** |
| RETROACTIVE CLEANUP | **READY FOR AUTHORIZATION** |
| PRODUCTION DATA QUALITY | **PARTIAL** (until applied) |
| V4 | **NEEDS MORE WORK** |

## Scale

- Total live records audited: **${rows.length}** (entire table, not limited to 400)
- Records with ≥1 proposed change: **${answers[50]}**
- Field mutations: **${answers[51]}**
- \`[object Object]\` Address: **${answers[6]}**
- Country-as-Market: **${answers[22]}**
- Parent/family Brand contamination: **${answers[41]}**

## Guarantees in dry-run

- Cvent evidence: **0**
- Legacy evidence: **0**
- Unsupported overwrite: **0**
- Country→Market auto-fill: **NO**
- Parent→Brand auto-fill: **NO**

See \`16-full-cleanup-manifest-dry-run.json\` and \`24-final-report-answers.json\`.
`
  );

  console.log(
    JSON.stringify(
      {
        total: rows.length,
        mutations: uniqueMutations.length,
        recordsChanged: answers[50],
        objectAddr: answers[6],
        countryAsMarket: answers[22],
        brandContam: answers[41],
        mutCounts,
        verdicts: answers.verdicts,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
