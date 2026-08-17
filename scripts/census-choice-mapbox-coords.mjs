#!/usr/bin/env node
/**
 * Fill Choice Hotels International HPC Latitude/Longitude gaps via Mapbox Permanent.
 *
 * Priority:
 * 1) Existing High street Address → Mapbox permanent geocode
 * 2) Official Choice URL JSON-LD geo / address when present
 * 3) Optional Google Places High/Medium street address (contact only; never Google coords)
 * 4) Mapbox permanent geocode
 *
 * Never invents coords. Never Google stored coords.
 * Default dry-run. Live: --apply --enable-production-writes + confirms + env.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  resolveMapboxCoordinates,
  MAPBOX_COORDINATE_STATUSES,
} from "../lib/research-engine-v2/census-mapbox-coordinate-provider.js";
import { isStreetLevelAddress } from "../lib/research-engine-v2/production-census-geocoding-providers.js";
import { resolveDirectoryAddressCandidate } from "../lib/research-engine-v2/census-autopilot-family-directory-adapters.js";
import { lookupHotelOfficialUrlWithGoogle } from "../lib/independent-census/google-places-hotel-url-lookup.js";
import { resolveGoogleApiKey } from "../lib/location-verification/google-api-config.js";
import { INTAKE_APPLY_CONFIRMS } from "../lib/independent-census/intake-autopilot-controlled.js";
import { checkIntakeApplyEnv } from "../lib/independent-census/intake-autopilot-apply.js";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";
import {
  assertProductionCensusWriteTarget,
  productionHotelPropertyCensus,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const USER_AGENT =
  "Mozilla/5.0 (compatible; DealalityCensusBot/1.0; +https://dealality.com; research)";
const FETCH_TIMEOUT_MS = 12000;

const READ_FIELDS = [
  "Property Name",
  "Current Brand",
  "Brand Family",
  "City",
  "Country",
  "State / Region",
  "Address",
  "Address Confidence",
  "Latitude",
  "Longitude",
  "Official Property URL",
  "Property Identity Key",
];

/** City centers for Mapbox proximity only — never written. */
const CITY_PROXIMITY = Object.freeze({
  bogota: { latitude: 4.711, longitude: -74.0721 },
  "bogotá": { latitude: 4.711, longitude: -74.0721 },
  cartagena: { latitude: 10.391, longitude: -75.4794 },
  barranquilla: { latitude: 10.9685, longitude: -74.7813 },
  cali: { latitude: 3.4516, longitude: -76.532 },
  medellin: { latitude: 6.2476, longitude: -75.5658 },
  "medellín": { latitude: 6.2476, longitude: -75.5658 },
  cucuta: { latitude: 7.8891, longitude: -72.4967 },
  "cúcuta": { latitude: 7.8891, longitude: -72.4967 },
  "panama city": { latitude: 8.9824, longitude: -79.5199 },
  panama: { latitude: 8.9824, longitude: -79.5199 },
  colon: { latitude: 9.3592, longitude: -79.9012 },
  "colón": { latitude: 9.3592, longitude: -79.9012 },
  "santo domingo": { latitude: 18.4861, longitude: -69.9312 },
  "punta cana": { latitude: 18.5601, longitude: -68.3725 },
  "mexico city": { latitude: 19.4326, longitude: -99.1332 },
  "ciudad de mexico": { latitude: 19.4326, longitude: -99.1332 },
  tijuana: { latitude: 32.5149, longitude: -117.0382 },
  hermosillo: { latitude: 29.0729, longitude: -110.9559 },
  leon: { latitude: 21.125, longitude: -101.686 },
  "león": { latitude: 21.125, longitude: -101.686 },
  mexicali: { latitude: 32.6245, longitude: -115.4523 },
  queretaro: { latitude: 20.5888, longitude: -100.3899 },
  "querétaro": { latitude: 20.5888, longitude: -100.3899 },
  monterrey: { latitude: 25.6866, longitude: -100.3161 },
  guadalajara: { latitude: 20.6597, longitude: -103.3496 },
  "playa del carmen": { latitude: 20.6296, longitude: -87.0739 },
  cancun: { latitude: 21.1619, longitude: -86.8515 },
  "cancún": { latitude: 21.1619, longitude: -86.8515 },
  veracruz: { latitude: 19.1738, longitude: -96.1342 },
  saltillo: { latitude: 25.4383, longitude: -100.9737 },
  torreon: { latitude: 25.5428, longitude: -103.4068 },
  "torreón": { latitude: 25.5428, longitude: -103.4068 },
  zapopan: { latitude: 20.7211, longitude: -103.3918 },
  "san jose": { latitude: 9.9281, longitude: -84.0907 },
  "san josé": { latitude: 9.9281, longitude: -84.0907 },
  alajuela: { latitude: 10.0162, longitude: -84.2116 },
  cariari: { latitude: 9.993, longitude: -84.14 },
  "san miguel de allende": { latitude: 20.9144, longitude: -100.7452 },
  "tuxtla gutierrez": { latitude: 16.7516, longitude: -93.1167 },
  "tuxtla gutiérrez": { latitude: 16.7516, longitude: -93.1167 },
  amador: { latitude: 8.941, longitude: -79.55 },
  chitre: { latitude: 7.9878, longitude: -80.4297 },
  "chitré": { latitude: 7.9878, longitude: -80.4297 },
  "cerro punta": { latitude: 8.85, longitude: -82.57 },
  "juan dolio": { latitude: 18.427, longitude: -69.287 },
  "puerto plata": { latitude: 19.7934, longitude: -70.6884 },
  mazatlan: { latitude: 23.2494, longitude: -106.4111 },
  "mazatlán": { latitude: 23.2494, longitude: -106.4111 },
  chihuahua: { latitude: 28.633, longitude: -106.0691 },
  zacatecas: { latitude: 22.7709, longitude: -102.5832 },
  irapuato: { latitude: 20.6767, longitude: -101.3563 },
  villahermosa: { latitude: 17.9895, longitude: -92.9475 },
  "cuajimalpa de morelos": { latitude: 19.357, longitude: -99.297 },
  cuajimalpa: { latitude: 19.357, longitude: -99.297 },
  acapulco: { latitude: 16.8531, longitude: -99.8237 },
  "cabo san lucas": { latitude: 22.8905, longitude: -109.9167 },
  chame: { latitude: 8.58, longitude: -79.88 },
  coronado: { latitude: 8.54, longitude: -79.89 },
  "playa caracol": { latitude: 8.58, longitude: -79.88 },
});

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function normKey(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim();
}

function parseArgs(argv = process.argv.slice(2)) {
  const get = (name, fb = "") => {
    const i = argv.indexOf(name);
    return i >= 0 ? argv[i + 1] : fb;
  };
  const confirms = {};
  for (const f of INTAKE_APPLY_CONFIRMS) confirms[f] = argv.includes(f);
  return {
    apply: argv.includes("--apply") && argv.includes("--enable-production-writes"),
    limit: Number(get("--limit", "80")) || 80,
    delayMs: Number(get("--delay-ms", "250")) || 250,
    skipOfficialFetch: argv.includes("--skip-official-fetch"),
    preferOfficialGeo: argv.includes("--prefer-official-geo"),
    /** Discover street Address via Places High/Medium (never store Google coords). */
    fillPlacesAddress: argv.includes("--fill-places-address"),
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

function isUsableStreetAddress(address) {
  const a = String(address || "").trim();
  if (!isStreetLevelAddress(a)) return false;
  if (/^[A-Z0-9]{4,}\+[A-Z0-9]{2,}\b/i.test(a) && !/\b(calle|av\.?|carr|km|blvd)\b/i.test(a)) {
    return false;
  }
  return true;
}

/** Choice official directory may use intersections without a house number. */
function isUsableChoiceDirectoryAddress(address) {
  if (isUsableStreetAddress(address)) return true;
  const a = String(address || "").trim();
  if (a.length < 15) return false;
  if (/^[A-Z0-9]{4,}\+[A-Z0-9]{2,}\b/i.test(a)) return false;
  return /\b(avenue|ave\.?|street|st\.?|calle|carrera|avenida|carretera|blvd|boulevard|via|vía|road|camino|autopista|periferico|periférico)\b/i.test(
    a
  );
}

function placesBrandAligned(fields, googleName) {
  const hay = String(googleName || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
  if (!hay) return false;
  const brand = String(fields["Current Brand"] || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
  const name = String(fields["Property Name"] || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
  const tokens = [
    "sleep inn",
    "comfort inn",
    "quality inn",
    "radisson",
    "ascend",
    "faranda",
    "park inn",
    "emotions",
    "fiesta americana",
    "hodelpa",
    "bambito",
    "guayacanes",
    "factoria",
    "bolivar",
    "bolívar",
  ];
  for (const t of tokens) {
    if ((brand.includes(t) || name.includes(t)) && hay.includes(t)) return true;
  }
  const parts = name
    .split(/[^a-z0-9]+/)
    .filter(
      (t) =>
        t.length >= 6 &&
        !["hotel", "collection", "inclusive", "choice", "radisson", "individuals", "member"].includes(
          t
        )
    );
  return parts.some((t) => hay.includes(t));
}

function placesSearchNames(fields, city) {
  const raw = String(fields["Property Name"] || "").trim();
  const brand = String(fields["Current Brand"] || "").trim();
  const brandClean = brand
    .replace(/\s+Individuals by Choice$/i, "")
    .replace(/\s+by Choice$/i, "")
    .trim();
  const names = [];
  if (raw && !/^choice property\b/i.test(raw) && !/^unknown$/i.test(raw)) names.push(raw);
  if (brand && city && city !== "Unknown") names.push(`${brand} ${city}`);
  if (brandClean && city && city !== "Unknown") names.push(`${brandClean} ${city}`);
  if (brandClean && city && city !== "Unknown") names.push(`${brandClean} hotel ${city}`);
  return [...new Set(names.filter(Boolean))];
}

function resolveCity(fields) {
  let city = String(fields.City || "").trim();
  if (city && !/^unknown$/i.test(city)) {
    if (/^panama$/i.test(city)) return "Panama City";
    return city;
  }
  const url = String(fields["Official Property URL"] || "").toLowerCase();
  // choicehotels.com/{region}/{city}/...
  const m = url.match(/choicehotels\.com\/[^/]+\/([^/]+)\//);
  if (m?.[1]) {
    const slug = m[1].replace(/-/g, " ");
    if (/cuidad de colon|ciudad de colon|colon/.test(slug)) return "Colón";
    if (/amador/.test(slug)) return "Amador";
    if (/panama/.test(slug)) return "Panama City";
    if (/cuajimalpa/.test(slug)) return "Cuajimalpa de Morelos";
    return slug.replace(/\b\w/g, (c) => c.toUpperCase());
  }
  return null;
}

function proximityForCity(city) {
  return CITY_PROXIMITY[normKey(city)] || null;
}

function parseJsonLd(html) {
  const blocks = [];
  const re =
    /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let m;
  while ((m = re.exec(html))) {
    try {
      blocks.push(JSON.parse(m[1].trim()));
    } catch {
      /* ignore */
    }
  }
  return blocks;
}

function walkLd(node, out = []) {
  if (!node) return out;
  if (Array.isArray(node)) {
    for (const x of node) walkLd(x, out);
    return out;
  }
  if (typeof node === "object") {
    out.push(node);
    if (node["@graph"]) walkLd(node["@graph"], out);
  }
  return out;
}

function extractOfficialGeoAndAddress(html) {
  const nodes = [];
  for (const b of parseJsonLd(html)) walkLd(b, nodes);
  let address = "";
  let lat = null;
  let lng = null;
  for (const n of nodes) {
    if (!address && n.address) {
      if (typeof n.address === "string") address = n.address;
      else if (typeof n.address === "object") {
        address = [
          n.address.streetAddress,
          n.address.addressLocality,
          n.address.addressRegion,
          n.address.postalCode,
          n.address.addressCountry,
        ]
          .filter(Boolean)
          .join(", ");
      }
    }
    const geo = n.geo || {};
    const gLat = Number(geo.latitude);
    const gLng = Number(geo.longitude);
    if (lat == null && Number.isFinite(gLat) && Number.isFinite(gLng)) {
      lat = gLat;
      lng = gLng;
    }
  }
  return { address: address.trim(), latitude: lat, longitude: lng };
}

async function fetchOfficial(url) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      signal: ctrl.signal,
      headers: {
        "User-Agent": USER_AGENT,
        Accept: "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
      },
      redirect: "follow",
    });
    if (!res.ok) return { ok: false, reason: `http_${res.status}` };
    const html = await res.text();
    return { ok: true, ...extractOfficialGeoAndAddress(html), final_url: String(res.url || url) };
  } catch (err) {
    return { ok: false, reason: err.message || "fetch_failed" };
  } finally {
    clearTimeout(t);
  }
}

async function listChoiceMissingCoords(baseId, token) {
  const formula = `AND(FIND('Choice Hotels International',{Brand Family}&''),OR({Latitude}=BLANK(),{Longitude}=BLANK()))`;
  const out = [];
  let offset;
  do {
    const p = new URLSearchParams({ filterByFormula: formula, pageSize: "100" });
    for (const f of READ_FIELDS) p.append("fields[]", f);
    if (offset) p.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${p}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json.error || json));
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
}

async function patchRecords(baseId, token, records) {
  const updated = [];
  for (let i = 0; i < records.length; i += 10) {
    const chunk = records.slice(i, i + 10);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}`,
      {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ records: chunk, typecast: true }),
      }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json.error || json));
    updated.push(...(json.records || []));
  }
  return updated;
}

function placesSearchName(fields, city) {
  return placesSearchNames(fields, city)[0] || "";
}

function cityInAddress(city, address) {
  const cityCue = String(city || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
  const addrCue = String(address || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
  if (!cityCue || cityCue === "unknown") return false;
  if (addrCue.includes(cityCue)) return true;
  if (cityCue === "panama city" || cityCue === "amador") {
    return (
      addrCue.includes("panama city") ||
      addrCue.includes("ciudad de panama") ||
      addrCue.includes("amador") ||
      (/\bpanama\b/.test(addrCue) &&
        !/oeste|chame|coronado|playa caracol|veracruz|colon|col[oó]n|chiriqui|chitre|chitr[eé]/.test(
          addrCue
        ))
    );
  }
  if (cityCue === "cuajimalpa de morelos" || cityCue === "cuajimalpa") {
    return (
      addrCue.includes("cuajimalpa") ||
      addrCue.includes("mexico city") ||
      addrCue.includes("ciudad de mexico") ||
      addrCue.includes("cdmx")
    );
  }
  if (cityCue === "san nicolas de los garza" && addrCue.includes("monterrey")) return true;
  if (cityCue === "zapopan" && addrCue.includes("guadalajara")) return true;
  if (cityCue === "chitre" || cityCue === "chitré") {
    return addrCue.includes("chitre") || addrCue.includes("chitr");
  }
  if (cityCue === "cerro punta") {
    return (
      addrCue.includes("cerro punta") ||
      addrCue.includes("volcan") ||
      addrCue.includes("chiriqui")
    );
  }
  if (cityCue === "juan dolio") {
    return addrCue.includes("juan dolio") || addrCue.includes("guayacanes");
  }
  return false;
}

/** Normalize noisy Choice/LATAM street strings for Mapbox (no invented streets). */
function addressQueryVariants(address) {
  const a = String(address || "").trim();
  if (!a) return [];
  const out = [a];
  const cra = a
    .replace(/\bCra\.?\s*(?=\d)/gi, "Carrera ")
    .replace(/\bCarr\.?\s*(?=\d)/gi, "Carrera ")
    .replace(/\bAv\.?\s*(?=[A-Za-z0-9])/gi, "Avenida ")
    .replace(/\bBlvd\.?\s*(?=[A-Za-z0-9])/gi, "Boulevard ")
    .replace(/\bLib\.?\s*/gi, "Libramiento ")
    .replace(/\bPerif\.?\s*/gi, "Periferico ")
    .replace(/\bAutop\.?\s*/gi, "Autopista ")
    .replace(/\bGral\.?\s*/gi, "General ")
    .replace(/\bKM\.?\s*/gi, "Kilometro ")
    .replace(/^Sobre,?\s*/i, "")
    .replace(/\s+/g, " ")
    .trim();
  if (cra !== a) out.push(cra);
  const m = cra.match(
    /\b((?:Avenida|Boulevard|Carrera|Calle|Callej[oó]n|Libramiento|Periferico|Autopista|Av\.?|Blvd\.?|Cra\.?)\b.+)$/i
  );
  if (m?.[1] && !out.includes(m[1])) out.push(m[1]);
  // Drop "Sin Nombre de Col …" noise common in MX Places strings
  const cleaned = cra.replace(/,?\s*Sin Nombre de Col[^,]*/gi, "").replace(/\s+/g, " ").trim();
  if (cleaned && cleaned !== cra) out.push(cleaned);
  return [...new Set(out)];
}

async function tryMapbox(fields, address, city) {
  const proximity = proximityForCity(city);
  const attempts = [
    { proximity },
    { proximity, omitPropertyName: true },
    { proximity, omitPropertyName: true, dropState: true },
    { proximity, types: "poi,address", allowPoi: true, minRelevance: 0.8 },
    {
      proximity,
      omitPropertyName: true,
      types: "poi,address",
      allowPoi: true,
      minRelevance: 0.8,
    },
  ];
  let last = null;
  for (const addrVariant of addressQueryVariants(address)) {
    const mbInput = {
      propertyName: fields["Property Name"],
      brand: fields["Current Brand"],
      address: addrVariant,
      city,
      stateRegion: fields["State / Region"],
      country: fields.Country,
      sourceUrl: fields["Official Property URL"],
    };
    for (const attempt of attempts) {
      const input = attempt.dropState
        ? { ...mbInput, stateRegion: undefined }
        : mbInput;
      const { dropState: _d, ...opts } = attempt;
      last = await resolveMapboxCoordinates(input, opts);
      if (last.status === MAPBOX_COORDINATE_STATUSES.RESOLVED_HIGH) return last;
    }
  }
  return last;
}

async function main() {
  const args = parseArgs();
  const writeTarget = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!writeTarget.ok) {
    console.error(JSON.stringify({ ok: false, blocked: "wrong_write_target" }));
    process.exit(1);
  }

  const token = resolvePat();
  const baseId = resolveTargetBase()?.target_base_id;
  const rows = await listChoiceMissingCoords(baseId, token);
  // Prefer rows that already have a High street address so --limit does not
  // burn the batch on stub names waiting on address discovery.
  const ranked = [...rows].sort((a, b) => {
    const aOk = isUsableStreetAddress(a.fields?.Address) ? 0 : 1;
    const bOk = isUsableStreetAddress(b.fields?.Address) ? 0 : 1;
    return aOk - bOk;
  });
  const limited = ranked.slice(0, args.limit);

  const proposals = [];
  const steward = [];
  let mapboxLookups = 0;
  let officialFetches = 0;
  let placesLookups = 0;
  const fieldHits = { Address: 0, Latitude: 0 };
  const apiKey = args.fillPlacesAddress ? resolveGoogleApiKey() : "";

  for (const rec of limited) {
    const f = rec.fields || {};
    /** @type {Record<string, unknown>} */
    const patch = {};
    const reasons = [];
    const city = resolveCity(f);
    let address = String(f.Address || "").trim();

    // Official Choice page only when address is missing (or --prefer-official-geo).
    const url = String(f["Official Property URL"] || "").trim();
    const preferOfficialGeo = args.preferOfficialGeo === true;
    if (
      !args.skipOfficialFetch &&
      url &&
      /choicehotels\.com/i.test(url) &&
      (!isUsableStreetAddress(address) || preferOfficialGeo)
    ) {
      const off = await fetchOfficial(url);
      officialFetches += 1;
      await sleep(args.delayMs);
      if (off.ok) {
        if (
          Number.isFinite(off.latitude) &&
          Number.isFinite(off.longitude) &&
          !(Math.abs(off.latitude) < 0.01 && Math.abs(off.longitude) < 0.01)
        ) {
          patch.Latitude = off.latitude;
          patch.Longitude = off.longitude;
          patch["Coordinate Source Type"] = "structured_data_extraction";
          patch["Coordinate Confidence"] = "High";
          patch["Geocode Provider"] = "Official Page";
          patch["Geocode Method"] = "structured_data_extraction";
          patch["Geocode Reviewed Date"] = todayIsoDate();
          reasons.push("coords_from_choice_official_jsonld_geo");
        }
        if (!isUsableStreetAddress(address) && isUsableStreetAddress(off.address)) {
          address = off.address;
          patch.Address = off.address;
          patch["Address Confidence"] = "High";
          patch["Address Source URL"] = off.final_url || url;
          reasons.push("address_from_choice_official_jsonld");
        }
      } else {
        reasons.push(`official_fetch_${off.reason || "failed"}`);
      }
    }

    // Choice regional directory (official card) — join line1+line2 when needed.
    if (!isUsableStreetAddress(address) && !isUsableChoiceDirectoryAddress(patch.Address)) {
      try {
        const dir = await resolveDirectoryAddressCandidate({
          fields: f,
          identityKey: f["Property Identity Key"],
          family: "Choice",
        });
        if (dir.ok && isUsableChoiceDirectoryAddress(dir.address)) {
          address = dir.address;
          patch.Address = dir.address;
          patch["Address Confidence"] = "High";
          patch["Address Source URL"] = dir.source_url || url || "";
          reasons.push(`address_from_choice_directory:${dir.method || "ok"}`);
          if (
            dir.city &&
            (!city || /^unknown$/i.test(String(city)) || /^panama city$/i.test(String(city)))
          ) {
            const dirCity = String(dir.city).trim();
            // Prefer directory locality for Panama coastal properties (Chame / Amador / etc.).
            if (
              dirCity &&
              !/^unknown$/i.test(dirCity) &&
              (!city ||
                /^unknown$/i.test(String(city)) ||
                (/^panama city$/i.test(String(city)) &&
                  !/^panama$/i.test(dirCity) &&
                  !/^panama city$/i.test(dirCity)))
            ) {
              patch.City = dirCity;
              reasons.push("city_from_choice_directory");
            }
          }
        } else {
          reasons.push(`directory_${dir.reason || "no_address"}`);
        }
      } catch (err) {
        reasons.push(`directory_error:${err?.message || String(err)}`);
      }
    }

    // Places contact address only — never store Google lat/lng.
    const cityForPlaces = String(patch.City || city || "").trim();
    if (
      args.fillPlacesAddress &&
      !isUsableStreetAddress(address) &&
      !isUsableChoiceDirectoryAddress(patch.Address) &&
      cityForPlaces &&
      !/^unknown$/i.test(cityForPlaces)
    ) {
      if (!apiKey) {
        reasons.push("places_skipped_no_api_key");
      } else {
        const prox = proximityForCity(cityForPlaces);
        let accepted = false;
        for (const searchName of placesSearchNames(f, cityForPlaces).slice(0, 3)) {
          const places = await lookupHotelOfficialUrlWithGoogle(
            {
              property_name: searchName,
              current_brand: f["Current Brand"],
              city: cityForPlaces,
              country: f.Country || "",
              source_record_id: f["Property Identity Key"],
              latitude: prox?.latitude,
              longitude: prox?.longitude,
            },
            { apiKey, maxResults: 5 }
          );
          placesLookups += 1;
          await sleep(args.delayMs);
          const gConf = String(places?.match_confidence || "").toLowerCase();
          const gOk = gConf === "high" || gConf === "medium";
          const placesAddr = places?.place?.google_formatted_address || "";
          const gName =
            places?.place?.google_name || places?.place?.googleName || "";
          const brandOk =
            gConf === "high" || placesBrandAligned(f, gName);
          if (
            places?.status === "matched" &&
            gOk &&
            brandOk &&
            isUsableStreetAddress(placesAddr) &&
            cityInAddress(cityForPlaces, placesAddr)
          ) {
            address = placesAddr;
            patch.Address = placesAddr;
            patch["Address Confidence"] = gConf === "high" ? "High" : "Medium";
            patch["Address Source URL"] =
              places.place.google_maps_uri || places.place.google_website_uri || "";
            reasons.push(`address_from_google_places_${gConf}`);
            accepted = true;
            break;
          }
          if (places?.status === "matched") {
            reasons.push(
              `places_match_${gConf || "low"}_skipped:${searchName.slice(0, 40)}`
            );
          } else {
            reasons.push(`places_${places?.status || "no_result"}`);
          }
        }
        if (!accepted && !reasons.some((r) => String(r).startsWith("places_"))) {
          reasons.push("places_no_usable_address");
        }
      }
    }

    // Mapbox if still no coords and we have street + city
    const cityForGeo = String(patch.City || city || "").trim();
    if (patch.Latitude == null) {
      const addr = String(patch.Address || address || "").trim();
      const addrOk =
        isUsableStreetAddress(addr) || isUsableChoiceDirectoryAddress(addr);
      if (addrOk && cityForGeo && !/^unknown$/i.test(cityForGeo)) {
        const mb = await tryMapbox(
          { ...f, Address: addr, City: cityForGeo },
          addr,
          cityForGeo
        );
        mapboxLookups += 1;
        await sleep(Math.min(args.delayMs, 200));
        if (mb?.status === MAPBOX_COORDINATE_STATUSES.RESOLVED_HIGH) {
          patch.Latitude = mb.latitude;
          patch.Longitude = mb.longitude;
          patch["Coordinate Source Type"] = "official_address_geocode";
          patch["Coordinate Confidence"] = "High";
          patch["Geocode Provider"] = "Mapbox";
          patch["Geocode Method"] =
            mb.geocode_method || "permanent_geocoding_official_address";
          patch["Geocode Reviewed Date"] = todayIsoDate();
          reasons.push(`coords_from_mapbox_permanent_high:${mb.reason || "ok"}`);
          if (!String(f.City || "").trim() || /^unknown$/i.test(String(f.City || ""))) {
            patch.City = cityForGeo;
            reasons.push("city_inferred_for_geocode");
          }
        } else {
          reasons.push(`mapbox_${mb?.status || "unresolved"}:${mb?.reason || ""}`);
        }
      } else {
        reasons.push(
          !cityForGeo || /^unknown$/i.test(cityForGeo)
            ? "blocked_missing_city"
            : "blocked_missing_street_address"
        );
      }
    }

    if (Object.keys(patch).length) {
      patch["Last Reviewed Date"] = todayIsoDate();
      if (patch.Address) fieldHits.Address += 1;
      if (patch.Latitude != null) fieldHits.Latitude += 1;
      proposals.push({
        id: rec.id,
        property_name: f["Property Name"],
        identity_key: f["Property Identity Key"],
        brand: f["Current Brand"],
        city: city || f.City || null,
        country: f.Country || null,
        patch,
        reasons,
      });
    } else {
      steward.push({
        id: rec.id,
        name: f["Property Name"],
        key: f["Property Identity Key"],
        city: city || f.City || null,
        country: f.Country || null,
        reasons,
      });
    }
  }

  const envCheck = checkIntakeApplyEnv();
  const doWrite = Boolean(args.apply && args.allConfirmsOk && envCheck.allOk);
  let patched = [];
  if (doWrite && proposals.length) {
    patched = await patchRecords(
      baseId,
      token,
      proposals.map((p) => ({ id: p.id, fields: p.patch }))
    );
  }

  const report = {
    status: doWrite ? "applied" : "dry_run",
    hard_rule:
      "Choice Hotels International only; Mapbox permanent or Choice official JSON-LD geo; never invent",
    generated_at: new Date().toISOString(),
    scanned: rows.length,
    processed: limited.length,
    proposal_count: proposals.length,
    patched_count: patched.length,
    steward_count: steward.length,
    field_hits: fieldHits,
    official_fetches: officialFetches,
    places_lookups: placesLookups,
    mapbox_lookups: mapboxLookups,
    airtable_writes: doWrite,
    proposals,
    steward: steward.slice(0, 40),
  };

  mkdirSync(join(root, "reports"), { recursive: true });
  const outRel = doWrite
    ? "reports/census-choice-mapbox-coords-applied.json"
    : "reports/census-choice-mapbox-coords-dry-run.json";
  writeFileSync(join(root, outRel), JSON.stringify(report, null, 2));

  console.log(
    JSON.stringify(
      {
        ok: true,
        status: report.status,
        output: outRel,
        scanned: report.scanned,
        proposal_count: report.proposal_count,
        field_hits: report.field_hits,
        official_fetches: report.official_fetches,
        places_lookups: report.places_lookups,
        mapbox_lookups: report.mapbox_lookups,
        steward_count: report.steward_count,
        airtable_writes: report.airtable_writes,
        sample: proposals.slice(0, 10).map((p) => ({
          n: p.property_name,
          city: p.city,
          country: p.country,
          reasons: p.reasons,
          lat: p.patch.Latitude,
          lng: p.patch.Longitude,
        })),
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
