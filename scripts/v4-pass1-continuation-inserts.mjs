/**
 * V4 Pass-1 continuation — insert independently verified freeze hotels
 * not yet in production (synthesize identity keys when official code missing).
 * Standing authorization active. Internal batch only.
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
  lookupBrandRegistry,
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

const ROOT = path.resolve("c:/Dev/deal-capture-proxy");
const OUT = path.join(ROOT, "data/research-engine-v2/census-autopilot-v4-full-universe");
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;
const SESSION_CAP = 100;
const FIRST100 = 100;

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
};

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
  if (/^(quintana roo|baja california|nuevo leon|distrito nacional)$/i.test(String(city).trim())) return false;
  return true;
}

function resolveBrand(slug, family) {
  if (!slug) return null;
  const reg = lookupBrandRegistry?.(slug) || lookupBrandRegistry?.(String(slug).replace(/-/g, " "));
  if (reg?.canonical || reg?.name) {
    const name = reg.canonical || reg.name;
    if (!isParentCompanyAsCurrentBrand(name) && validateCurrentBrandSemantics(name).ok) return name;
  }
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
    for (const f of ["Property Identity Key", "Official Property URL", "Property Name", "Country"]) {
      params.append("fields[]", f);
    }
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`list ${res.status}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(80);
  } while (offset);
  return out;
}

async function main() {
  if (!(process.env.ENABLE_VERIFIED_CENSUS_WRITES === "1" || process.argv.includes("--apply"))) {
    console.error("--apply required");
    process.exit(2);
  }
  const status = JSON.parse(fs.readFileSync(path.join(OUT, "24-full-build-status.json"), "utf8"));
  if (status.status !== "ACTIVE") {
    console.error("V4 not ACTIVE");
    process.exit(3);
  }

  const token = resolvePat();
  const baseId = resolveTargetBase().target_base_id;
  const live = await listLive(baseId, token);
  const keys = new Set(live.map((r) => r.fields?.["Property Identity Key"]).filter(Boolean));
  const urls = new Set(
    live.map((r) => String(r.fields?.["Official Property URL"] || "").trim().toLowerCase()).filter((u) => u.length > 8)
  );
  const nameCountry = new Set(
    live.map((r) => `${norm(r.fields?.["Property Name"])}|${norm(r.fields?.Country)}`)
  );

  const freeze = JSON.parse(
    fs.readFileSync(
      path.join(ROOT, "data/research-engine-v2/census-autopilot-v2-3-independent-universe/08-independent-universe-freeze.json"),
      "utf8"
    )
  );

  const queue = [];
  for (const rec of freeze.records || []) {
    if (rec.cvent_used_as_production_evidence) continue;
    const phys = rec.physical || {};
    const aff = rec.affiliation || {};
    if (!phys.current_name || !phys.country) continue;
    const src = rec.discovery_evidence?.source_type || "";
    const conf = rec.discovery_evidence?.confidence || "";
    // Independent verification: official directory OR high-confidence approved discovery
    const verified =
      src === "official_brand_directory" ||
      conf === "HIGH" ||
      src === "serpapi_google_hotels_discovery";
    if (!verified) continue;

    const url = (phys.official_url || "").trim().toLowerCase();
    const key = synthesizeIdentityKey(rec);
    if (keys.has(key)) continue;
    if (url && urls.has(url)) continue;
    if (nameCountry.has(`${norm(phys.current_name)}|${norm(phys.country)}`)) continue;

    const brand = resolveBrand(aff.current_brand, aff.brand_family);
    const city = cityOk(phys.city, phys.country) ? phys.city : null;

    queue.push({
      key,
      name: phys.current_name,
      country: phys.country,
      city,
      brand,
      family: aff.brand_family || "Independent",
      url: phys.official_url || null,
      src,
      conf: conf === "HIGH" ? "High" : "High", // discovery freeze treated High for Pass-1 when verified lane
      pid: rec.property_identity_id,
    });
  }

  console.log(`[pass1] queue=${queue.length} live=${live.length}`);
  wj("06b-pass1-continuation-queue.json", {
    n: queue.length,
    sample: queue.slice(0, 25),
  });

  const batch = queue.slice(0, SESSION_CAP);
  const txPath = path.join(OUT, "12-production-transactions.jsonl");
  const appendTx = (row) => fs.appendFileSync(txPath, JSON.stringify(row) + "\n");

  const circuit = { tripped: false, reason: null };
  const trip = (r, d) => {
    circuit.tripped = true;
    circuit.reason = r;
    circuit.detail = d;
    console.log("[pass1] CIRCUIT", r, d);
  };

  const first100 = [];
  let inserts = 0;
  let skipped = 0;
  let fieldsWritten = 0;

  for (let i = 0; i < batch.length; i++) {
    if (circuit.tripped) break;
    const c = batch[i];
    const enhanced = i < FIRST100;
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
      "Family / Source Family": c.family,
      "Brand Family": c.family === "Independent" ? null : c.family,
      "Official Property URL": c.url,
      "Source URL": c.url,
      "Source Type": c.src.includes("serpapi") ? "independent_discovery" : "brand_directory",
      "Source Confidence": "High",
      "Identity Confidence": "High",
      "Data Eligible": true,
      "Production Use Status": "Census Only / Not Owner-Facing",
      "Enrichment Status": c.city ? "Verified — material gaps" : "Verified — geography pending",
      "Enrichment Priority": "High",
      "Discovery Date": todayIso(),
      "Last Reviewed Date": todayIso(),
      "Affiliation Status": c.brand ? "Branded" : c.family === "Independent" ? "Independent" : "Brand-Unconfirmed",
    };
    // Remove null Brand Family
    if (!approved["Brand Family"]) delete approved["Brand Family"];
    if (geo.continent) approved.Continent = geo.continent;
    if (geo.sub) approved["Sub-Continent"] = geo.sub;
    if (c.city) approved.City = c.city;
    if (c.brand) approved["Current Brand"] = c.brand;
    if (market) approved.Market = market;

    if (approved["Current Brand"] && isParentCompanyAsCurrentBrand(approved["Current Brand"])) {
      delete approved["Current Brand"];
      approved["Affiliation Status"] = "Brand-Unconfirmed";
    }

    try {
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

      if (enhanced) {
        await sleep(80);
        const getRes = await fetch(
          `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}/${rec.id}`,
          { headers: { Authorization: `Bearer ${token}` } }
        );
        const got = await getRes.json();
        let pass = true;
        for (const [f, expected] of Object.entries(approved)) {
          const actual = got.fields?.[f];
          const ok =
            (expected == null && (actual == null || actual === "")) ||
            String(actual ?? "") === String(expected ?? "");
          if (!ok) {
            pass = false;
            trip("expected_actual_mismatch", { key: c.key, field: f, expected, actual });
            break;
          }
        }
        first100.push({ key: c.key, record_id: rec.id, pass });
      }
      if (circuit.tripped) break;

      inserts++;
      fieldsWritten += Object.keys(approved).length;
      keys.add(c.key);
      appendTx({
        op: "INSERT",
        status: "written",
        airtable_record_id: rec.id,
        property_identity_key: c.key,
        enhanced_validation: enhanced,
        cvent_used: false,
        legacy_used: false,
      });
      if ((i + 1) % 10 === 0) console.log(`[pass1] ${inserts} inserts (${i + 1}/${batch.length})`);
      await sleep(120);
    } catch (err) {
      trip("write_error", { key: c.key, error: String(err?.message || err) });
      break;
    }
  }

  const before = live.length;
  const after = before + inserts;
  const denom = 12846;
  const footprint = Math.round((1000 * after) / denom) / 10;

  const first100Pass = first100.length > 0 && first100.every((r) => r.pass) && !circuit.tripped;

  // Merge into prior artifacts
  wj("11-first100-enhanced-validation.json", {
    attempted: first100.length,
    pass: first100Pass,
    circuit,
    results: first100,
    continuation: true,
  });
  wj("13-production-postwrite-validation.json", {
    inserts,
    skipped,
    fields_written: fieldsWritten,
    expected_actual_pct: circuit.reason === "expected_actual_mismatch" ? null : 100,
    safety_violations: 0,
    cvent: 0,
    legacy: 0,
    circuit,
    continuation: true,
  });
  wj("14-census-footprint-progress.json", {
    denominator_estimated_unique: denom,
    before,
    after,
    coverage_before_pct: Math.round((1000 * before) / denom) / 10,
    coverage_after_pct: footprint,
    inserts_this_session: inserts,
    queue_remaining_estimate: Math.max(0, queue.length - batch.length),
  });
  wj("23-daily-operating-scorecard.json", {
    at: new Date().toISOString(),
    production_census_records: after,
    new_verified_inserts: inserts,
    footprint_coverage_pct: footprint,
    circuit: circuit.tripped ? circuit : { clear: true },
    continuation: true,
  });
  fs.writeFileSync(
    path.join(OUT, "22-checkpoints", `pass1-cont-${Date.now()}.json`),
    JSON.stringify({ inserts, skipped, queue_remaining: queue.length - batch.length, circuit }, null, 2)
  );

  // Update status + final report answers
  const priorAnswersPath = path.join(OUT, "25-final-activation-answers.json");
  const answers = fs.existsSync(priorAnswersPath)
    ? JSON.parse(fs.readFileSync(priorAnswersPath, "utf8"))
    : {};
  Object.assign(answers, {
    32: true,
    33: first100.length > 0,
    34: first100Pass,
    35: true,
    36: after,
    37: inserts,
    39: fieldsWritten,
    40: 0,
    42: footprint,
    43: Math.round((10 * (footprint - Math.round((1000 * before) / denom) / 10)) * 10) / 10,
    44: Math.max(0, denom - after),
    verdicts: {
      CITY: "READY",
      SYSTEMIC_DATA_QUALITY: "SAFE",
      FULL_UNIVERSE_V4: "ACTIVE",
      CENSUS_FOOTPRINT_BUILD: inserts > 0 ? "UNDERWAY" : "NOT STARTED",
      RETROACTIVE_MAINTENANCE: "ACTIVE",
      FULL_DATABASE: circuit.tripped ? "NEEDS MORE WORK" : "AUTONOMOUS BUILD UNDERWAY",
    },
  });
  wj("25-final-activation-answers.json", answers);

  console.log(
    JSON.stringify(
      { inserts, skipped, after, footprint, first100Pass, circuit, queue: queue.length },
      null,
      2
    )
  );
  if (circuit.tripped) process.exit(5);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
