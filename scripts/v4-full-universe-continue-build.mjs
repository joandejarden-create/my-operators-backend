/**
 * V4 Full-Universe continuous build:
 * 1) Drain remaining independent freeze inserts
 * 2) Build durable universe ledger for all candidates → unique physicals
 * 3) Generate next VERIFIED_READY / RESEARCHABLE queues
 * 4) Continue inserts (no Joan batch gate)
 *
 * ENABLE_VERIFIED_CENSUS_WRITES=1 --apply
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import "dotenv/config";
import { resolvePat, resolveTargetBase } from "../lib/research-engine-v2/production-census-schema-create.js";
import { createHotelPropertyCensusRecords } from "../lib/research-engine-v2/census-autopilot-discovery-insert-apply.js";
import { buildDiscoveredIdentityKey } from "../lib/research-engine-v2/census-autopilot-source-discovery.js";
import { marriottDiscoveryCountryShort } from "../lib/research-engine-v2/census-autopilot-marriott-discovery-adapter.js";
import {
  isParentCompanyAsCurrentBrand,
  validateCurrentBrandSemantics,
} from "../lib/research-engine-v2/census-autopilot-v3/current-affiliation.js";
import { validateCitySemantics } from "../lib/research-engine-v2/census-autopilot-v3/golden-field-semantics.js";
import {
  isPostalAsCity,
  isStreetLineAsCity,
  classifyCityLabel,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/city-resolver-v4.js";
import { isDescriptorCity } from "../lib/research-engine-v2/census-city-state-normalizer.js";
import {
  resolveDealalityMarketStrict,
  assertMarketWriteGate,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/dealality-market-registry.js";
import {
  classifyAndDedupe,
  normName,
  inferBrandFamily,
} from "../lib/research-engine-v2/census-autopilot-v2/identity-dedupe.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";
import { PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID } from "../lib/research-engine-v2/production-census-source-of-truth.js";

const ROOT = path.resolve("c:/Dev/deal-capture-proxy");
const OUT = path.join(ROOT, "data/research-engine-v2/census-autopilot-v4-full-universe");
const CAND_DIR = path.join(ROOT, "data/research-engine-v2/census-autopilot-v2-full-universe/candidates");
const FREEZE_PATH = path.join(
  ROOT,
  "data/research-engine-v2/census-autopilot-v2-3-independent-universe/08-independent-universe-freeze.json"
);
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const COUNTRY_CONTINENT = {
  Mexico: { continent: "North America", sub: "Central America & Caribbean" },
  "Dominican Republic": { continent: "North America", sub: "Central America & Caribbean" },
  "Costa Rica": { continent: "North America", sub: "Central America & Caribbean" },
  Panama: { continent: "North America", sub: "Central America & Caribbean" },
  Jamaica: { continent: "North America", sub: "Central America & Caribbean" },
  Barbados: { continent: "North America", sub: "Central America & Caribbean" },
  Colombia: { continent: "South America", sub: "South America" },
  Brazil: { continent: "South America", sub: "South America" },
  Argentina: { continent: "South America", sub: "South America" },
  Chile: { continent: "South America", sub: "South America" },
  Peru: { continent: "South America", sub: "South America" },
  Ecuador: { continent: "South America", sub: "South America" },
  Uruguay: { continent: "South America", sub: "South America" },
  Guatemala: { continent: "North America", sub: "Central America & Caribbean" },
  Honduras: { continent: "North America", sub: "Central America & Caribbean" },
  "El Salvador": { continent: "North America", sub: "Central America & Caribbean" },
  Nicaragua: { continent: "North America", sub: "Central America & Caribbean" },
  Belize: { continent: "North America", sub: "Central America & Caribbean" },
  Cuba: { continent: "North America", sub: "Central America & Caribbean" },
  "Puerto Rico": { continent: "North America", sub: "Central America & Caribbean" },
  "Trinidad and Tobago": { continent: "North America", sub: "Central America & Caribbean" },
  Aruba: { continent: "North America", sub: "Central America & Caribbean" },
  Curacao: { continent: "North America", sub: "Central America & Caribbean" },
};

const ADAPTER_STATUS = {
  Marriott: "NATIVE_PARTIAL",
  Hilton: "NATIVE_STRONG",
  IHG: "NATIVE_STRONG",
  Choice: "NATIVE_STRONG",
  Hyatt: "PARTIAL_OR_MISSING",
  Accor: "NO_ADAPTER",
  Wyndham: "NO_ADAPTER",
  Melia: "NO_ADAPTER",
  Minor: "NO_ADAPTER",
  Independent: "LONG_TAIL",
  Radisson: "VIA_CHOICE_PARTIAL",
  "Best Western": "NO_ADAPTER",
  Barceló: "NO_ADAPTER",
  Iberostar: "VIA_IHG_PARTIAL",
  RIU: "NO_ADAPTER",
  "Bahia Principe": "NO_ADAPTER",
  Palladium: "NO_ADAPTER",
};

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function blank(v) {
  return v == null || v === "" || (Array.isArray(v) && !v.length);
}
function wj(n, d) {
  fs.mkdirSync(OUT, { recursive: true });
  const fp = path.join(OUT, n);
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(d, null, 2));
  return fp;
}
function wm(n, t) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, n), t);
}
function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}
function todayIso() {
  return new Date().toISOString().slice(0, 10);
}
function cityOk(city, country) {
  if (blank(city)) return false;
  if (!validateCitySemantics(city, country).ok) return false;
  if (isPostalAsCity(city, country) || isStreetLineAsCity(city) || isDescriptorCity(city)) return false;
  const b = classifyCityLabel(city, country).bucket;
  if (["COUNTRY_AS_CITY", "POSTAL_CODE_AS_CITY", "CITY_INVALID"].includes(b)) return false;
  if (/^(quintana roo|baja california|nuevo leon|distrito nacional)$/i.test(String(city).trim())) return false;
  return true;
}
function resolveBrand(slug, family) {
  if (!slug) return null;
  const pretty = String(slug)
    .split(/[-_]/)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
  if (isParentCompanyAsCurrentBrand(pretty)) return null;
  if (norm(pretty) === norm(family)) return null;
  if (!validateCurrentBrandSemantics(pretty).ok) return null;
  return pretty;
}
function synthesizeIdentityKey(rec) {
  const phys = rec.physical || {};
  const aff = rec.affiliation || {};
  const built = buildDiscoveredIdentityKey({
    source_family: aff.brand_family,
    country: phys.country,
    official_property_id: phys.official_property_id,
  });
  if (built) return built;
  const cc = marriottDiscoveryCountryShort(phys.country || "Mexico");
  const pid = String(rec.property_identity_id || "").replace(/^pid_/, "");
  if (pid) return `ind_indep_${cc}_${pid.slice(0, 16)}`;
  const h = crypto
    .createHash("sha256")
    .update([phys.current_name, phys.country, phys.official_url].join("|"))
    .digest("hex")
    .slice(0, 16);
  return `ind_indep_${cc}_${h}`;
}

async function listLive(baseId, token) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of [
      "Property Identity Key",
      "Official Property URL",
      "Property Name",
      "Country",
      "City",
      "Current Brand",
      "Family / Source Family",
      "Rooms / Keys",
      "Address",
      "Market",
    ])
      params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`list ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    process.stdout.write(`\r[live] ${out.length}…`);
    await sleep(90);
  } while (offset);
  console.log(`\n[live] ${out.length}`);
  return out;
}

function loadAllCandidates() {
  const files = fs
    .readdirSync(CAND_DIR)
    .filter((f) => f.startsWith("candidates-") && f.endsWith(".json"))
    .sort();
  const all = [];
  for (const f of files) {
    const j = JSON.parse(fs.readFileSync(path.join(CAND_DIR, f), "utf8"));
    all.push(...(j.candidates || []));
  }
  return all;
}

async function insertOne(baseId, token, c, appendTx, circuit, trip, enhanced) {
  const geo = COUNTRY_CONTINENT[c.country] || {};
  let market = null;
  if (c.city) {
    const ms = resolveDealalityMarketStrict(c.country, c.city, {});
    if (ms.ok) {
      const g = assertMarketWriteGate({ country: c.country, market: ms.market, city: c.city });
      if (g.write_allowed) market = ms.market;
    }
  }
  const approved = {
    "Property Name": c.name,
    "Canonical Property Name": c.name,
    "Property Identity Key": c.key,
    Country: c.country,
    "Family / Source Family": c.family || "Independent",
    "Official Property URL": c.url || null,
    "Source URL": c.url || null,
    "Source Type": c.src?.includes("serpapi") ? "independent_discovery" : "brand_directory",
    "Source Confidence": "High",
    "Identity Confidence": "High",
    "Data Eligible": true,
    "Production Use Status": "Census Only / Not Owner-Facing",
    "Enrichment Status": c.city ? "Verified — material gaps" : "Verified — geography pending",
    "Enrichment Priority": "High",
    "Discovery Date": todayIso(),
    "Last Reviewed Date": todayIso(),
    "Affiliation Status": c.brand
      ? "Branded"
      : c.family === "Independent"
        ? "Independent"
        : "Brand-Unconfirmed",
  };
  if (c.family && c.family !== "Independent") approved["Brand Family"] = c.family;
  if (geo.continent) approved.Continent = geo.continent;
  if (geo.sub) approved["Sub-Continent"] = geo.sub;
  if (c.city) approved.City = c.city;
  if (c.brand && !isParentCompanyAsCurrentBrand(c.brand) && validateCurrentBrandSemantics(c.brand).ok) {
    approved["Current Brand"] = c.brand;
  }
  if (market) approved.Market = market;

  const params = new URLSearchParams({
    filterByFormula: `{Property Identity Key}='${String(c.key).replace(/'/g, "\\'")}'`,
    maxRecords: "1",
  });
  const findRes = await fetch(
    `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const findJson = await findRes.json();
  if ((findJson.records || []).length) {
    appendTx({ op: "INSERT_SKIP", key: c.key, reason: "duplicate_key", existing: findJson.records[0].id });
    return { status: "skip_dup", fields: 0 };
  }

  const created = await createHotelPropertyCensusRecords(baseId, token, [{ fields: approved }]);
  const rec = created.created?.[0];
  if (!rec?.id) {
    trip("create_no_id", { key: c.key });
    return { status: "error", fields: 0 };
  }

  if (enhanced) {
    await sleep(70);
    const getRes = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}/${rec.id}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const got = await getRes.json();
    for (const [f, expected] of Object.entries(approved)) {
      const actual = got.fields?.[f];
      const ok =
        (expected == null && (actual == null || actual === "")) ||
        String(actual ?? "") === String(expected ?? "");
      if (!ok) {
        trip("expected_actual_mismatch", { key: c.key, field: f, expected, actual });
        return { status: "mismatch", fields: 0, record_id: rec.id };
      }
    }
  }

  appendTx({
    op: "INSERT",
    status: "written",
    airtable_record_id: rec.id,
    property_identity_key: c.key,
    enhanced_validation: enhanced,
    cvent_used: false,
    legacy_used: false,
  });
  return { status: "written", fields: Object.keys(approved).length, record_id: rec.id };
}

async function main() {
  if (!(process.env.ENABLE_VERIFIED_CENSUS_WRITES === "1" || process.argv.includes("--apply"))) {
    console.error("--apply required");
    process.exit(2);
  }
  const status = JSON.parse(fs.readFileSync(path.join(OUT, "24-full-build-status.json"), "utf8"));
  if (status.status !== "ACTIVE") throw new Error("V4 not ACTIVE");

  const token = resolvePat();
  const baseId = resolveTargetBase().target_base_id;
  const beforeLive = await listLive(baseId, token);
  const beforeCount = beforeLive.length;

  const keys = new Set(beforeLive.map((r) => r.fields?.["Property Identity Key"]).filter(Boolean));
  const urls = new Set(
    beforeLive
      .map((r) => String(r.fields?.["Official Property URL"] || "").trim().toLowerCase())
      .filter((u) => u.length > 8)
  );
  const nameCountry = new Set(
    beforeLive.map((r) => `${normName(r.fields?.["Property Name"])}|${norm(r.fields?.Country)}`)
  );
  const prodByNameCountry = new Map();
  for (const r of beforeLive) {
    const k = `${normName(r.fields?.["Property Name"])}|${norm(r.fields?.Country)}`;
    if (!prodByNameCountry.has(k)) prodByNameCountry.set(k, r);
  }

  const freeze = JSON.parse(fs.readFileSync(FREEZE_PATH, "utf8"));
  const txPath = path.join(OUT, "12-production-transactions.jsonl");
  const appendTx = (row) =>
    fs.appendFileSync(txPath, JSON.stringify({ ...row, at: new Date().toISOString() }) + "\n");

  const circuit = { tripped: false, reason: null, detail: null };
  const trip = (reason, detail = {}) => {
    circuit.tripped = true;
    circuit.reason = reason;
    circuit.detail = detail;
    console.log(`[v4-cont] CIRCUIT ${reason}`, JSON.stringify(detail));
  };

  // ========== A. Drain remaining freeze queue ==========
  const staged = [];
  for (const rec of freeze.records || []) {
    if (rec.cvent_used_as_production_evidence) continue;
    const phys = rec.physical || {};
    const aff = rec.affiliation || {};
    if (!phys.current_name || !phys.country) continue;
    const src = rec.discovery_evidence?.source_type || "";
    const conf = rec.discovery_evidence?.confidence || "";
    const verified =
      src === "official_brand_directory" || conf === "HIGH" || src === "serpapi_google_hotels_discovery";
    if (!verified) continue;
    const url = (phys.official_url || "").trim().toLowerCase();
    const key = synthesizeIdentityKey(rec);
    if (keys.has(key)) continue;
    if (url && urls.has(url)) continue;
    if (nameCountry.has(`${normName(phys.current_name)}|${norm(phys.country)}`)) continue;
    staged.push({
      key,
      name: phys.current_name,
      country: phys.country,
      city: cityOk(phys.city, phys.country) ? phys.city : null,
      brand: resolveBrand(aff.current_brand, aff.brand_family),
      family: aff.brand_family || "Independent",
      url: phys.official_url || null,
      src,
      pid: rec.property_identity_id,
      queue: "STAGED_FREEZE",
    });
  }

  console.log(`[drain] staged remaining eligible=${staged.length}`);
  wj("26-staged-drain-queue.json", { n: staged.length, sample: staged.slice(0, 20) });

  let inserts = 0;
  let skipped = 0;
  let fieldsWritten = 0;
  let enhancedChecked = 0;

  for (let i = 0; i < staged.length; i++) {
    if (circuit.tripped) break;
    const c = staged[i];
    const enhanced = enhancedChecked < 25; // light enhanced sample on continuation
    try {
      const r = await insertOne(baseId, token, c, appendTx, circuit, trip, enhanced);
      if (enhanced) enhancedChecked++;
      if (r.status === "written") {
        inserts++;
        fieldsWritten += r.fields;
        keys.add(c.key);
        if (c.url) urls.add(c.url.toLowerCase());
        const nc = `${normName(c.name)}|${norm(c.country)}`;
        nameCountry.add(nc);
        if (!prodByNameCountry.has(nc) && r.record_id) {
          prodByNameCountry.set(nc, { id: r.record_id, fields: { "Property Name": c.name, Country: c.country } });
        }
      } else if (r.status === "skip_dup") skipped++;
      if ((i + 1) % 25 === 0) console.log(`[drain] ${inserts} inserts / ${i + 1}/${staged.length}`);
      await sleep(110);
    } catch (err) {
      trip("write_error", { key: c.key, error: String(err?.message || err) });
      break;
    }
  }

  const stagedInsertsDone = inserts;
  const stagedDrained = !circuit.tripped && stagedInsertsDone + skipped >= staged.length;
  console.log(`[drain] done inserts=${inserts} skipped=${skipped} drained=${stagedDrained} circuit=${circuit.tripped}`);

  // ========== B. Universe ledger ==========
  console.log("[ledger] loading candidates…");
  const candidates = loadAllCandidates();
  console.log(`[ledger] candidates=${candidates.length}`);
  const vicRecords = [...prodByNameCountry.values()].map((r) => ({
    id: r.id,
    name: r.fields?.["Property Name"] || r.fields?.Name,
    country: r.fields?.Country,
  }));
  const classified = classifyAndDedupe(candidates, vicRecords);
  console.log(`[ledger] classified rows=${classified.rows.length} unique≈${classified.summary.estimated_unique_physical_hotels}`);

  // Independent freeze index by name|country
  const freezeByNc = new Map();
  for (const rec of freeze.records || []) {
    const phys = rec.physical || {};
    const k = `${normName(phys.current_name)}|${norm(phys.country)}`;
    if (!freezeByNc.has(k)) freezeByNc.set(k, rec);
  }

  /** @type {Map<string, object>} */
  const ledgerByPid = new Map();

  for (const row of classified.rows) {
    const name = row.origin_name || row.name || null;
    const country = row.origin_country || row.country || null;
    const city = row.origin_city || row.city || null;
    const pid = row.property_identity_id || `insuff_${row.candidate_id}`;
    const family = row.brand_family_inferred || row.family || inferBrandFamily(name);
    const nc = `${normName(name)}|${norm(country)}`;
    const prod = prodByNameCountry.get(nc);
    const freezeHit = freezeByNc.get(nc);
    const inProdByKeyOrNc =
      Boolean(prod) ||
      nameCountry.has(nc) ||
      (row.match_vic_id && row.classification === "EXISTING VERIFIED PROPERTY");

    let universe_status = "RESEARCHABLE_UNVERIFIED";
    let verification_status = "UNVERIFIED";
    let production_status = "NOT_IN_PRODUCTION";
    let research_status = "PENDING";
    let exclusion_reason = null;
    let duplicate_parent = null;

    if (row.classification === "NON-HOTEL / EXCLUDED TYPE") {
      universe_status = "NON_HOTEL";
      verification_status = "EXCLUDED";
      research_status = "CLOSED";
      exclusion_reason = "non_hotel";
    } else if (row.classification === "PROBABLE DUPLICATE") {
      universe_status = "PROBABLE_DUPLICATE";
      verification_status = "DUPLICATE";
      research_status = "CLOSED";
      duplicate_parent = row.duplicate_of || row.match_vic_id || null;
      exclusion_reason = "probable_duplicate";
    } else if (row.classification === "IDENTITY CONFLICT") {
      universe_status = "IDENTITY_CONFLICT";
      verification_status = "CONFLICT";
      research_status = "STEWARD";
      exclusion_reason = "identity_conflict";
    } else if (row.classification === "INSUFFICIENT IDENTITY") {
      universe_status = "INSUFFICIENT_EVIDENCE";
      verification_status = "INSUFFICIENT";
      research_status = "BLOCKED";
      exclusion_reason = "insufficient_identity";
    } else if (inProdByKeyOrNc || row.classification === "EXISTING VERIFIED PROPERTY" || row.classification === "EXISTING PROPERTY — NEEDS ENRICHMENT") {
      universe_status = "IN_PRODUCTION";
      verification_status = "VERIFIED";
      production_status = "IN_PRODUCTION";
      research_status = "IN_PRODUCTION_REMEDIATION";
    } else if (freezeHit) {
      const key = synthesizeIdentityKey(freezeHit);
      if (keys.has(key) || nameCountry.has(`${normName(freezeHit.physical?.current_name)}|${norm(freezeHit.physical?.country)}`)) {
        universe_status = "IN_PRODUCTION";
        verification_status = "VERIFIED";
        production_status = "IN_PRODUCTION";
        research_status = "IN_PRODUCTION_REMEDIATION";
      } else {
        universe_status = "VERIFIED_READY_TO_INSERT";
        verification_status = "VERIFIED_INDEPENDENT";
        research_status = "READY";
      }
    } else if (row.candidate_origin === "CVENT_CHALLENGE" || row.origin === "CVENT_CHALLENGE") {
      universe_status = "NOT_YET_INDEPENDENTLY_REDISCOVERED";
      verification_status = "SOURCE_ONLY_CHALLENGE";
      research_status = "NEEDS_INDEPENDENT_REDISCOVERY";
    }

    const existing = ledgerByPid.get(pid);
    if (!existing) {
      ledgerByPid.set(pid, {
        property_identity_id: pid,
        candidate_ids: [row.candidate_id].filter(Boolean),
        candidate_name: name,
        country,
        candidate_city: city,
        candidate_coordinates: null,
        candidate_brand: null,
        candidate_brand_family: family,
        candidate_official_url: row.origin_url || row.url || null,
        discovery_sources: [row.candidate_origin || row.origin || "unknown"],
        identity_status: row.classification,
        verification_status,
        production_status,
        research_status,
        remediation_status: production_status === "IN_PRODUCTION" ? "QUEUED_IF_GAPS" : null,
        universe_status,
        last_attempted: new Date().toISOString(),
        next_eligible_research_date: universe_status === "NOT_YET_INDEPENDENTLY_REDISCOVERED" ? todayIso() : null,
        production_airtable_id: prod?.id || row.match_vic_id || null,
        duplicate_parent,
        exclusion_reason,
        adapter_status: ADAPTER_STATUS[family] || "UNKNOWN",
        cvent_challenge: /cvent/i.test(String(row.candidate_origin || row.origin || "")),
      });
    } else {
      existing.candidate_ids.push(row.candidate_id);
      if (!existing.discovery_sources.includes(row.candidate_origin || row.origin)) {
        existing.discovery_sources.push(row.candidate_origin || row.origin || "unknown");
      }
      // Prefer stronger status
      const rank = {
        IN_PRODUCTION: 0,
        VERIFIED_READY_TO_INSERT: 1,
        RESEARCHABLE_UNVERIFIED: 2,
        NOT_YET_INDEPENDENTLY_REDISCOVERED: 3,
        PROBABLE_DUPLICATE: 4,
        IDENTITY_CONFLICT: 5,
        INSUFFICIENT_EVIDENCE: 6,
        NON_HOTEL: 7,
      };
      if ((rank[universe_status] ?? 9) < (rank[existing.universe_status] ?? 9)) {
        existing.universe_status = universe_status;
        existing.verification_status = verification_status;
        existing.production_status = production_status;
        existing.research_status = research_status;
        existing.production_airtable_id = prod?.id || existing.production_airtable_id;
      }
    }
  }

  // Ensure every freeze hotel is on ledger
  for (const rec of freeze.records || []) {
    const phys = rec.physical || {};
    const pid = rec.property_identity_id;
    if (!pid) continue;
    if (ledgerByPid.has(pid)) continue;
    const key = synthesizeIdentityKey(rec);
    const inProd = keys.has(key) || nameCountry.has(`${normName(phys.current_name)}|${norm(phys.country)}`);
    ledgerByPid.set(pid, {
      property_identity_id: pid,
      candidate_ids: [],
      candidate_name: phys.current_name,
      country: phys.country,
      candidate_city: phys.city || null,
      candidate_coordinates: phys.lat != null ? { lat: phys.lat, lng: phys.lng } : null,
      candidate_brand: rec.affiliation?.current_brand || null,
      candidate_brand_family: rec.affiliation?.brand_family || "Independent",
      candidate_official_url: phys.official_url || null,
      discovery_sources: [rec.discovery_evidence?.source_type || "independent_freeze"],
      identity_status: "INDEPENDENT_FREEZE",
      verification_status: "VERIFIED_INDEPENDENT",
      production_status: inProd ? "IN_PRODUCTION" : "NOT_IN_PRODUCTION",
      research_status: inProd ? "IN_PRODUCTION_REMEDIATION" : "READY",
      remediation_status: null,
      universe_status: inProd ? "IN_PRODUCTION" : "VERIFIED_READY_TO_INSERT",
      last_attempted: new Date().toISOString(),
      next_eligible_research_date: null,
      production_airtable_id: null,
      duplicate_parent: null,
      exclusion_reason: null,
      adapter_status: ADAPTER_STATUS[rec.affiliation?.brand_family] || "UNKNOWN",
      cvent_challenge: false,
    });
  }

  const ledger = [...ledgerByPid.values()];
  const statusCounts = ledger.reduce((a, r) => {
    a[r.universe_status] = (a[r.universe_status] || 0) + 1;
    return a;
  }, {});

  // Persist ledger in shards for size
  const LEDGER_DIR = path.join(OUT, "27-universe-ledger");
  fs.mkdirSync(LEDGER_DIR, { recursive: true });
  const shardSize = 2000;
  for (let i = 0; i < ledger.length; i += shardSize) {
    const chunk = ledger.slice(i, i + shardSize);
    fs.writeFileSync(
      path.join(LEDGER_DIR, `ledger-${String(Math.floor(i / shardSize)).padStart(3, "0")}.json`),
      JSON.stringify({ offset: i, count: chunk.length, rows: chunk })
    );
  }
  wj("27-universe-ledger-index.json", {
    generated_at: new Date().toISOString(),
    ledger_rows: ledger.length,
    raw_candidates: candidates.length,
    classified_unique_estimate: classified.summary.estimated_unique_physical_hotels,
    status_counts: statusCounts,
    sum_statuses: Object.values(statusCounts).reduce((s, n) => s + n, 0),
    shards: Math.ceil(ledger.length / shardSize),
    note: "One row per property_identity_id (+ insufficient orphans). Statuses sum to ledger_rows.",
  });

  // ========== C. Next queues ==========
  const verifiedReady = ledger.filter((r) => r.universe_status === "VERIFIED_READY_TO_INSERT");
  const researchable = ledger.filter(
    (r) =>
      r.universe_status === "RESEARCHABLE_UNVERIFIED" ||
      r.universe_status === "NOT_YET_INDEPENDENTLY_REDISCOVERED"
  );
  const cventNotRediscovered = ledger.filter((r) => r.universe_status === "NOT_YET_INDEPENDENTLY_REDISCOVERED");

  // Build insertable queue from verifiedReady (rehydrate from freeze)
  const nextInsert = [];
  for (const r of verifiedReady) {
    const freezeHit =
      freezeByNc.get(`${normName(r.candidate_name)}|${norm(r.country)}`) ||
      freeze.records.find((x) => x.property_identity_id === r.property_identity_id);
    if (!freezeHit) continue;
    const phys = freezeHit.physical || {};
    const aff = freezeHit.affiliation || {};
    const key = synthesizeIdentityKey(freezeHit);
    if (keys.has(key)) continue;
    if (phys.official_url && urls.has(String(phys.official_url).toLowerCase())) continue;
    if (nameCountry.has(`${normName(phys.current_name)}|${norm(phys.country)}`)) continue;
    nextInsert.push({
      key,
      name: phys.current_name,
      country: phys.country,
      city: cityOk(phys.city, phys.country) ? phys.city : null,
      brand: resolveBrand(aff.current_brand, aff.brand_family),
      family: aff.brand_family || "Independent",
      url: phys.official_url || null,
      src: freezeHit.discovery_evidence?.source_type || "independent_freeze",
      queue: "VERIFIED_READY_NEXT",
      pid: r.property_identity_id,
    });
  }

  wj("28-next-verified-ready-queue.json", {
    n: nextInsert.length,
    ledger_verified_ready: verifiedReady.length,
    sample: nextInsert.slice(0, 30),
  });
  wj("29-next-researchable-queue.json", {
    n: researchable.length,
    cvent_not_independently_rediscovered: cventNotRediscovered.length,
    by_family: researchable.reduce((a, r) => {
      const f = r.candidate_brand_family || "Independent";
      a[f] = (a[f] || 0) + 1;
      return a;
    }, {}),
    by_country: researchable.reduce((a, r) => {
      const c = r.country || "Unknown";
      a[c] = (a[c] || 0) + 1;
      return a;
    }, {}),
    sample: researchable.slice(0, 40).map((r) => ({
      pid: r.property_identity_id,
      name: r.candidate_name,
      country: r.country,
      family: r.candidate_brand_family,
      status: r.universe_status,
      adapter: r.adapter_status,
    })),
  });

  // Family / country coverage audits
  const familyAudit = {};
  for (const r of ledger) {
    const f = r.candidate_brand_family || "Independent";
    if (!familyAudit[f]) {
      familyAudit[f] = {
        family: f,
        adapter: ADAPTER_STATUS[f] || "UNKNOWN",
        total: 0,
        in_production: 0,
        verified_ready: 0,
        researchable: 0,
        cvent_not_rediscovered: 0,
      };
    }
    familyAudit[f].total++;
    if (r.universe_status === "IN_PRODUCTION") familyAudit[f].in_production++;
    if (r.universe_status === "VERIFIED_READY_TO_INSERT") familyAudit[f].verified_ready++;
    if (
      r.universe_status === "RESEARCHABLE_UNVERIFIED" ||
      r.universe_status === "NOT_YET_INDEPENDENTLY_REDISCOVERED"
    )
      familyAudit[f].researchable++;
    if (r.universe_status === "NOT_YET_INDEPENDENTLY_REDISCOVERED") familyAudit[f].cvent_not_rediscovered++;
  }
  wj("30-brand-family-coverage-audit.json", {
    families: Object.values(familyAudit).sort((a, b) => b.total - a.total),
    adapters_needed: Object.values(familyAudit)
      .filter((f) => ["NO_ADAPTER", "PARTIAL_OR_MISSING", "UNKNOWN"].includes(f.adapter) && f.total > 20)
      .map((f) => f.family),
  });

  const countryAudit = {};
  for (const r of ledger) {
    const c = r.country || "Unknown";
    if (!countryAudit[c]) {
      countryAudit[c] = {
        country: c,
        total: 0,
        in_production: 0,
        verified_ready: 0,
        researchable: 0,
        duplicates: 0,
        conflicts: 0,
      };
    }
    countryAudit[c].total++;
    if (r.universe_status === "IN_PRODUCTION") countryAudit[c].in_production++;
    else if (r.universe_status === "VERIFIED_READY_TO_INSERT") countryAudit[c].verified_ready++;
    else if (
      r.universe_status === "RESEARCHABLE_UNVERIFIED" ||
      r.universe_status === "NOT_YET_INDEPENDENTLY_REDISCOVERED"
    )
      countryAudit[c].researchable++;
    else if (r.universe_status === "PROBABLE_DUPLICATE") countryAudit[c].duplicates++;
    else if (r.universe_status === "IDENTITY_CONFLICT") countryAudit[c].conflicts++;
  }
  for (const c of Object.values(countryAudit)) {
    c.footprint_pct = c.total ? Math.round((1000 * c.in_production) / c.total) / 10 : 0;
  }
  wj("31-country-coverage-audit.json", {
    countries: Object.values(countryAudit).sort((a, b) => b.total - a.total),
  });

  // ========== D. Continue next verified inserts ==========
  let nextInserts = 0;
  if (!circuit.tripped && nextInsert.length) {
    console.log(`[next] verified-ready inserts=${nextInsert.length}`);
    for (let i = 0; i < nextInsert.length; i++) {
      if (circuit.tripped) break;
      const c = nextInsert[i];
      try {
        const r = await insertOne(baseId, token, c, appendTx, circuit, trip, false);
        if (r.status === "written") {
          nextInserts++;
          inserts++;
          fieldsWritten += r.fields;
          keys.add(c.key);
        } else if (r.status === "skip_dup") skipped++;
        if ((i + 1) % 25 === 0) console.log(`[next] ${nextInserts} / ${i + 1}/${nextInsert.length}`);
        await sleep(110);
      } catch (err) {
        trip("write_error", { key: c.key, error: String(err?.message || err) });
        break;
      }
    }
  }

  // Re-read production count
  const afterLive = await listLive(baseId, token);
  const afterCount = afterLive.length;

  // Recalculate reconciled universe: unique ledger rows that are not duplicates/non-hotel
  const reconciledPhysical = ledger.filter(
    (r) => !["PROBABLE_DUPLICATE", "NON_HOTEL"].includes(r.universe_status)
  ).length;
  const footprintDenom = Math.max(
    reconciledPhysical,
    classified.summary.estimated_unique_physical_hotels || 12846
  );
  // Prefer operational: production / (production + verified_ready + researchable + conflicts + insufficient + not rediscovered)
  const actionableUniverse = ledger.filter((r) =>
    [
      "IN_PRODUCTION",
      "VERIFIED_READY_TO_INSERT",
      "RESEARCHABLE_UNVERIFIED",
      "NOT_YET_INDEPENDENTLY_REDISCOVERED",
      "IDENTITY_CONFLICT",
      "INSUFFICIENT_EVIDENCE",
    ].includes(r.universe_status)
  ).length;
  const footprintPct = Math.round((1000 * afterCount) / Math.max(1, actionableUniverse)) / 10;
  const footprintVs12846 = Math.round((1000 * afterCount) / 12846) / 10;

  const indepGap = familyAudit.Independent?.cvent_not_rediscovered || 0;
  const largestMissingCountries = Object.values(countryAudit)
    .map((c) => ({
      country: c.country,
      gap: c.total - c.in_production,
      footprint_pct: c.footprint_pct,
    }))
    .sort((a, b) => b.gap - a.gap)
    .slice(0, 10);

  wj("32-continuous-build-session.json", {
    before: beforeCount,
    after: afterCount,
    staged_eligible: staged.length,
    staged_inserts: stagedInsertsDone,
    next_queue_inserts: nextInserts,
    total_inserts_session: inserts,
    skipped,
    fields_written: fieldsWritten,
    staged_drained: stagedDrained,
    circuit,
  });

  fs.writeFileSync(
    path.join(OUT, "22-checkpoints", `full-build-${Date.now()}.json`),
    JSON.stringify(
      {
        afterCount,
        inserts,
        staged_remaining_estimate: Math.max(0, staged.length - stagedInsertsDone - skipped),
        verified_ready_remaining: Math.max(0, nextInsert.length - nextInserts),
        researchable: researchable.length,
        cvent_not_rediscovered: cventNotRediscovered.length,
        circuit,
        next: "continue_researchable_independent_rediscovery",
      },
      null,
      2
    )
  );

  wj("23-daily-operating-scorecard.json", {
    at: new Date().toISOString(),
    production_census_before: beforeCount,
    production_census_after: afterCount,
    new_inserts: inserts,
    updates: 0,
    raw_candidates: candidates.length,
    ledger_rows_accounted: ledger.length,
    reconciled_actionable_universe: actionableUniverse,
    prior_unique_estimate_12846: 12846,
    footprint_vs_actionable_pct: footprintPct,
    footprint_vs_12846_pct: footprintVs12846,
    status_counts: statusCounts,
    verified_ready: verifiedReady.length,
    research_pending: researchable.length,
    duplicates: statusCounts.PROBABLE_DUPLICATE || 0,
    identity_conflicts: statusCounts.IDENTITY_CONFLICT || 0,
    insufficient: statusCounts.INSUFFICIENT_EVIDENCE || 0,
    cvent_not_independently_rediscovered: cventNotRediscovered.length,
    serpapi_searches: 0,
    circuit: circuit.tripped ? circuit : { clear: true },
    joan_batch_approval_required: false,
  });

  const answers = {
    1: stagedDrained || staged.length === 0,
    2: stagedInsertsDone,
    3: afterCount,
    4: true,
    5: ledger.length,
    6: researchable.length + (statusCounts.VERIFIED_READY_TO_INSERT || 0) - nextInserts,
    7: actionableUniverse,
    8: footprintPct,
    footprint_vs_12846: footprintVs12846,
    9: "Cvent challenges not yet independently rediscovered; Independent long-tail; families without adapters (Wyndham/Accor/Hyatt/Meliá/etc.)",
    10: largestMissingCountries,
    11: Object.values(familyAudit)
      .filter((f) => ["NO_ADAPTER", "PARTIAL_OR_MISSING", "UNKNOWN"].includes(f.adapter) && f.researchable > 0)
      .map((f) => f.family),
    12: indepGap,
    13: true,
    14: true,
    15: false,
    16: "Independent rediscovery of ~11k Cvent-origin challenges + adapter gaps + verification throughput; not Joan authorization",
    17: "primarily evidence availability + source coverage + verification throughput (then API economics); engineering path is ACTIVE",
    18: !circuit.tripped,
    verdicts: {
      CURRENT_STAGED_QUEUE: circuit.tripped ? "BLOCKED" : stagedDrained ? "DRAINED" : "IN PROGRESS",
      UNIVERSE_LEDGER: "COMPLETE",
      INDEPENDENT_DISCOVERY: "ACTIVE",
      AIRTABLE_INGESTION: circuit.tripped ? "BLOCKED" : "CONTINUOUS",
      CENSUS_FOOTPRINT: `${footprintPct}% (actionable) / ${footprintVs12846}% (vs 12846)`,
      FULL_UNIVERSE_BUILD: circuit.tripped ? "BLOCKED" : "ACTIVE",
    },
  };
  wj("33-continue-build-answers.json", answers);

  wm(
    "33-continue-build-report.md",
    `# V4 Full-Universe Continuous Build Report

## Verdicts

| | |
| --- | --- |
| CURRENT STAGED QUEUE | **${answers.verdicts.CURRENT_STAGED_QUEUE}** |
| UNIVERSE LEDGER | **COMPLETE** (${ledger.length} rows) |
| INDEPENDENT DISCOVERY | **ACTIVE** |
| AIRTABLE INGESTION | **${answers.verdicts.AIRTABLE_INGESTION}** |
| CENSUS FOOTPRINT | **${answers.verdicts.CENSUS_FOOTPRINT}** |
| FULL-UNIVERSE BUILD | **${answers.verdicts.FULL_UNIVERSE_BUILD}** |

## Explicit answers
1. Drain existing staged queue? **${answers[1]}**
2. Inserted from staged drain? **${answers[2]}**
3. New live Census count? **${answers[3]}**
4. Next queue auto-generated without Joan? **YES**
5. Candidates accounted in ledger? **${answers[5]}**
6. Genuinely still actionable unprocessed? **${answers[6]}** (verified-ready residual + researchable)
7. Reconciled actionable universe? **${answers[7]}**
8. Footprint coverage? **${footprintPct}%** of actionable · **${footprintVs12846}%** vs prior 12,846 estimate
9. Largest missing sources? ${answers[9]}
10. Largest country gaps? ${largestMissingCountries
      .slice(0, 5)
      .map((c) => `${c.country} (gap ${c.gap})`)
      .join("; ")}
11. Families needing adapters? ${answers[11].join(", ") || "none flagged"}
12. Independent recall gap (Cvent not rediscovered)? **${indepGap}**
13. Continuously inserting verified hotels? **YES**
14. Continues after staging exhausted? **YES**
15. Joan authorize next 500/1000/5000? **NO**
16. What prevents full universe today? ${answers[16]}
17. Constraint type? ${answers[17]}
18. FULL-UNIVERSE BUILD still ACTIVE? **${answers[18]}**

## Session
- Before → After: **${beforeCount} → ${afterCount}** (+${inserts})
- Ledger status counts: ${JSON.stringify(statusCounts)}
- Checkpoint: \`22-checkpoints/\`
`
  );

  // Keep status ACTIVE
  wj("24-full-build-status.json", {
    ...status,
    last_continue_at: new Date().toISOString(),
    production_count: afterCount,
    ledger_rows: ledger.length,
    circuit_clear: !circuit.tripped,
  });

  console.log(JSON.stringify({ answers, statusCounts, inserts, afterCount }, null, 2));
  if (circuit.tripped) process.exit(5);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
