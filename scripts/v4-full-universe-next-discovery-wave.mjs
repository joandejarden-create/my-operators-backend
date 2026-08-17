/**
 * V4 next wave: official directory discovery → verified inserts (no Joan gate).
 * Standing authorization ACTIVE. ENABLE_VERIFIED_CENSUS_WRITES=1 --apply
 */
import fs from "node:fs";
import path from "node:path";
import "dotenv/config";
import { resolvePat, resolveTargetBase } from "../lib/research-engine-v2/production-census-schema-create.js";
import {
  discoverCalaProperties,
  buildDiscoveredIdentityKey,
  classifyDiscoveredAgainstCensus,
  MATCH_CLASS,
} from "../lib/research-engine-v2/census-autopilot-source-discovery.js";
import { createHotelPropertyCensusRecords } from "../lib/research-engine-v2/census-autopilot-discovery-insert-apply.js";
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
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";
import { PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID } from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { normName } from "../lib/research-engine-v2/census-autopilot-v2/identity-dedupe.js";
import {
  listCountriesWithDiscoveryAdapter,
  listCalaCountriesFromRadar,
} from "../lib/research-engine-v2/production-census-cala-region-config.js";

const ROOT = path.resolve("c:/Dev/deal-capture-proxy");
const OUT = path.join(ROOT, "data/research-engine-v2/census-autopilot-v4-full-universe");
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;
const SESSION_CAP = Number(process.env.V4_DISCOVERY_INSERT_CAP || 500);

/** Full-universe: all countries with any ready directory adapter (not pilot-5 only). */
function buildFullDiscoveryCountries() {
  const set = new Set([
    ...listCalaCountriesFromRadar(),
    ...listCountriesWithDiscoveryAdapter("Hilton"),
    ...listCountriesWithDiscoveryAdapter("IHG"),
    ...listCountriesWithDiscoveryAdapter("Choice"),
    ...listCountriesWithDiscoveryAdapter("Marriott"),
    ...listCountriesWithDiscoveryAdapter("Accor"),
    ...listCountriesWithDiscoveryAdapter("Wyndham"),
  ]);
  return [...set].sort();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function blank(v) {
  return v == null || v === "" || (Array.isArray(v) && !v.length);
}
function wj(n, d) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, n), JSON.stringify(d, null, 2));
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
  return true;
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

  console.log("[discover] running official CALA directory discovery (full-country set)…");
  const discoveryCountries = buildFullDiscoveryCountries();
  console.log(`[discover] countries=${discoveryCountries.length}`);
  const { discovered, sourceReport } = await discoverCalaProperties({
    discoverAllOfficialParents: true,
    delayMs: 60,
    discoveryCountries,
  });
  console.log(`[discover] discovered=${discovered.length} families=${(sourceReport.families_used || []).join(",")}`);

  const censusRecords = beforeLive.map((r) => ({
    id: r.id,
    fields: r.fields,
    name: r.fields?.["Property Name"],
    country: r.fields?.Country,
    property_identity_key: r.fields?.["Property Identity Key"],
    official_url: r.fields?.["Official Property URL"],
  }));
  const match = classifyDiscoveredAgainstCensus(discovered, censusRecords, {});
  const newCandidates = match.by_class[MATCH_CLASS.NEW_CANDIDATE] || [];
  console.log(`[discover] NEW_CANDIDATE=${newCandidates.length}`);

  const queue = [];
  for (const d of newCandidates) {
    const name = d.property_name || d.name || d.current_name;
    const country = d.country;
    const key =
      d.identity_key ||
      d.property_identity_key ||
      buildDiscoveredIdentityKey({
        source_family: d.source_family || d.brand_family || d.parent_company || d.family,
        country,
        official_property_id: d.official_property_id || d.property_code,
      });
    if (!name || !country || !key) continue;
    if (keys.has(key)) continue;
    const url = (d.official_property_url || d.url || "").trim();
    const urlLc = url.toLowerCase();
    if (urlLc && urls.has(urlLc)) continue;
    if (nameCountry.has(`${normName(name)}|${norm(country)}`)) continue;
    const family = d.source_family || d.brand_family || d.parent_company || d.family || "Independent";
    const brand = d.brand || d.current_brand || null;
    queue.push({
      key,
      name,
      country,
      city: cityOk(d.city, country) ? d.city : null,
      brand,
      family,
      url: url || null,
      source_url: d.official_directory_url || url || null,
      raw: d,
    });
    if (queue.length >= SESSION_CAP) break;
  }

  console.log(`[discover] insert_queue=${queue.length} (from ${newCandidates.length} NEW_CANDIDATE)`);

  wj("34-next-discovery-wave-queue.json", {
    discovered: discovered.length,
    new_candidates: newCandidates.length,
    insert_queue: queue.length,
    session_cap: SESSION_CAP,
    discovery_countries: discoveryCountries,
    source_report: sourceReport,
    sample: queue.slice(0, 25).map((q) => ({ key: q.key, name: q.name, country: q.country, family: q.family })),
  });

  const txPath = path.join(OUT, "12-production-transactions.jsonl");
  const appendTx = (row) =>
    fs.appendFileSync(txPath, JSON.stringify({ ...row, at: new Date().toISOString(), wave: "discovery" }) + "\n");

  const circuit = { tripped: false, reason: null, detail: null };
  const trip = (reason, detail = {}) => {
    circuit.tripped = true;
    circuit.reason = reason;
    circuit.detail = detail;
    console.log(`[wave] CIRCUIT ${reason}`, JSON.stringify(detail));
  };

  let inserts = 0;
  let skipped = 0;
  for (let i = 0; i < queue.length; i++) {
    if (circuit.tripped) break;
    const c = queue[i];
    try {
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
        "Source URL": c.source_url || c.url || null,
        "Source Type": "brand_directory",
        "Source Confidence": "High",
        "Identity Confidence": "High",
        "Data Eligible": true,
        "Production Use Status": "Census Only / Not Owner-Facing",
        "Enrichment Status": c.city ? "Verified — material gaps" : "Verified — geography pending",
        "Enrichment Priority": "High",
        "Discovery Date": todayIso(),
        "Last Reviewed Date": todayIso(),
        "Affiliation Status": c.brand ? "Branded" : "Brand-Unconfirmed",
      };
      if (c.family && c.family !== "Independent") approved["Brand Family"] = c.family;
      if (c.city) approved.City = c.city;
      if (
        c.brand &&
        !isParentCompanyAsCurrentBrand(c.brand) &&
        validateCurrentBrandSemantics(c.brand).ok
      ) {
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
        skipped++;
        appendTx({ op: "INSERT_SKIP", key: c.key, reason: "duplicate_key" });
        continue;
      }

      const created = await createHotelPropertyCensusRecords(baseId, token, [{ fields: approved }]);
      const rec = created.created?.[0];
      if (!rec?.id) {
        trip("create_no_id", { key: c.key });
        break;
      }
      inserts++;
      keys.add(c.key);
      if (c.url) urls.add(c.url.toLowerCase());
      nameCountry.add(`${normName(c.name)}|${norm(c.country)}`);
      appendTx({
        op: "INSERT",
        status: "written",
        airtable_record_id: rec.id,
        property_identity_key: c.key,
        family: c.family,
        wave: "official_directory",
      });
      if ((i + 1) % 25 === 0) console.log(`[wave] ${inserts} inserts / ${i + 1}/${queue.length}`);
      await sleep(110);
    } catch (err) {
      trip("write_error", { key: c.key, error: String(err?.message || err) });
      break;
    }
  }

  const afterLive = await listLive(baseId, token);
  const summary = {
    before: beforeCount,
    after: afterLive.length,
    inserts,
    skipped,
    queue: queue.length,
    discovered: discovered.length,
    new_candidates: newCandidates.length,
    families_used: sourceReport.families_used,
    adapter_errors: sourceReport.adapter_errors,
    circuit,
    joan_batch_approval_required: false,
  };
  wj("35-discovery-wave-session.json", summary);
  fs.writeFileSync(
    path.join(OUT, "22-checkpoints", `discovery-wave-${Date.now()}.json`),
    JSON.stringify(summary, null, 2)
  );
  wj("24-full-build-status.json", {
    ...status,
    last_discovery_wave_at: new Date().toISOString(),
    production_count: afterLive.length,
    circuit_clear: !circuit.tripped,
  });
  console.log(JSON.stringify(summary, null, 2));
  if (circuit.tripped) process.exit(5);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
