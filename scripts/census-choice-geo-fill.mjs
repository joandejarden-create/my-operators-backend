#!/usr/bin/env node
/**
 * Fill Choice Hotels International HPC geography gaps:
 * - City = Unknown / blank
 * - State / Region blank or dirty ISO/numeric codes
 * - Address blank
 *
 * Sources (priority): Choice regional directory → city→state map → Choice URL state slug
 * Optional: --fill-places-address (Places High/Medium, brand-gated; never Google coords)
 *
 * Default dry-run. Live: --apply --enable-production-writes + confirms + env.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  resolveDirectoryAddressCandidate,
  lookupChoiceRegionalRow,
} from "../lib/research-engine-v2/census-autopilot-family-directory-adapters.js";
import {
  resolveStateRegionFromCity,
  resolveStateFromChoiceOfficialUrl,
  isDirtyStateRegionValue,
} from "../lib/research-engine-v2/census-city-to-state-map.js";
import { isStreetLevelAddress } from "../lib/research-engine-v2/production-census-geocoding-providers.js";
import { isDescriptorCity } from "../lib/research-engine-v2/census-city-state-normalizer.js";
import { extractChoicePropertyId } from "../lib/research-engine-v2/census-autopilot-family-directory-adapters.js";
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

const READ_FIELDS = [
  "Property Name",
  "Current Brand",
  "Brand Family",
  "City",
  "Country",
  "State / Region",
  "Address",
  "Address Confidence",
  "Official Property URL",
  "Property Identity Key",
];

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function blank(v) {
  return v == null || String(v).trim() === "";
}
function unknownCity(v) {
  return blank(v) || /^unknown$/i.test(String(v).trim()) || isDescriptorCity(v);
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
    limit: Number(get("--limit", "120")) || 120,
    delayMs: Number(get("--delay-ms", "200")) || 200,
    fillPlacesAddress: argv.includes("--fill-places-address"),
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

function titleCaseSlug(slug) {
  return String(slug || "")
    .replace(/-/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function normalizeDirectoryCity(raw, address = "") {
  let city = String(raw || "").trim();
  const addr = String(address || "").toLowerCase();
  if (!city || /^unknown$/i.test(city)) city = "";
  if (/^cuidad de colon$|^ciudad de colon$/i.test(city)) return "Colón";
  if (/^colon$|^colón$/i.test(city)) return "Colón";
  if (/^panama$/i.test(city) && /chame|playa caracol|coronado/.test(addr)) {
    return "Chame";
  }
  if (/^panama$/i.test(city)) return "Panama City";
  return city || null;
}

function cityFromChoiceUrl(url) {
  const raw = String(url || "").toLowerCase();
  const m = raw.match(/choicehotels\.com\/[^/]+\/([^/]+)\//);
  if (!m?.[1]) return null;
  const slug = m[1];
  if (/cuidad-de-colon|ciudad-de-colon|colon/.test(slug)) return "Colón";
  if (/amador/.test(slug)) return "Amador";
  if (/chame|coronado|playa-caracol/.test(slug)) return titleCaseSlug(slug);
  if (/panama/.test(slug)) return "Panama City";
  if (/cuajimalpa/.test(slug)) return "Cuajimalpa de Morelos";
  return titleCaseSlug(slug);
}

function isUsableChoiceAddress(address) {
  const a = String(address || "").trim();
  if (!a || a.length < 12) return false;
  if (/^[A-Z0-9]{4,}\+[A-Z0-9]{2,}\b/i.test(a) && !/\b(calle|av\.?|carr|km|blvd)\b/i.test(a)) {
    return false;
  }
  if (isStreetLevelAddress(a)) return true;
  return /\b(avenue|ave\.?|street|st\.?|calle|carrera|avenida|carretera|blvd|boulevard|via|vía|road|camino|autopista|paseo|cuspide|cúspide)\b/i.test(
    a
  );
}

function placesChoiceUrlAligned(fields, places, propertyId) {
  const website = String(
    places?.place?.google_website_uri || places?.place?.googleWebsiteUri || ""
  ).toLowerCase();
  const maps = String(
    places?.place?.google_maps_uri || places?.place?.googleMapsUri || ""
  ).toLowerCase();
  const censusUrl = String(fields["Official Property URL"] || "").toLowerCase();
  const id = String(propertyId || "").toLowerCase();
  if (id && website.includes(id)) return true;
  if (id && website.includes("choicehotels.com") && website.includes(`/${id}`)) {
    return true;
  }
  // Medium+ only when Places website is the same Choice property URL host path
  if (
    censusUrl.includes("choicehotels.com") &&
    website.includes("choicehotels.com") &&
    id &&
    website.includes(id)
  ) {
    return true;
  }
  // Do not accept maps-only URIs as Choice alignment
  void maps;
  return false;
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
    "paraiso",
    "paraíso",
  ];
  for (const t of tokens) {
    if ((brand.includes(t) || name.includes(t)) && hay.includes(t)) return true;
  }
  const parts = name
    .split(/[^a-z0-9]+/)
    .filter((t) => t.length >= 6 && !["hotel", "collection", "choice", "radisson"].includes(t));
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
  if (raw && !/^choice property\b/i.test(raw)) names.push(raw);
  if (brand && city && city !== "Unknown") names.push(`${brand} ${city}`);
  if (brandClean && city && city !== "Unknown") names.push(`${brandClean} ${city}`);
  return [...new Set(names.filter(Boolean))];
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
  if (cityCue === "mexico city") {
    return addrCue.includes("mexico city") || addrCue.includes("cdmx") || addrCue.includes("ciudad de mexico");
  }
  return false;
}

async function listChoice(baseId, token) {
  const formula = "{Brand Family}='Choice Hotels International'";
  let offset;
  const out = [];
  do {
    const p = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
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

function needsGeoFill(fields) {
  const cityBad = unknownCity(fields.City);
  const stateBad =
    blank(fields["State / Region"]) || isDirtyStateRegionValue(fields["State / Region"]);
  const addrBad = blank(fields.Address);
  return cityBad || stateBad || addrBad;
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
  const all = await listChoice(baseId, token);
  const gaps = all.filter((r) => needsGeoFill(r.fields || {}));
  const limited = gaps.slice(0, args.limit);
  const apiKey = args.fillPlacesAddress ? resolveGoogleApiKey() : "";

  const proposals = [];
  const steward = [];
  const fieldHits = { City: 0, "State / Region": 0, Address: 0 };
  let placesLookups = 0;
  let directoryLookups = 0;

  for (const rec of limited) {
    const f = rec.fields || {};
    /** @type {Record<string, unknown>} */
    const patch = {};
    const reasons = [];
    let city = String(f.City || "").trim();
    let state = String(f["State / Region"] || "").trim();
    let address = String(f.Address || "").trim();
    const url = String(f["Official Property URL"] || "").trim();
    const country = String(f.Country || "").trim();

    let dirHit = null;
    try {
      dirHit = await lookupChoiceRegionalRow(f, f["Property Identity Key"]);
      directoryLookups += 1;
    } catch (err) {
      reasons.push(`directory_error:${err?.message || String(err)}`);
    }

    const dirRow = dirHit?.ok ? dirHit.row : null;
    const dirCombined = dirRow
      ? [dirRow.addressLine1, dirRow.addressLine2].filter(Boolean).join(", ")
      : "";

    // City
    if (unknownCity(city)) {
      const fromDir = normalizeDirectoryCity(dirRow?.city, dirCombined || address);
      const fromUrl = cityFromChoiceUrl(url);
      const next = fromDir || fromUrl;
      if (next && !unknownCity(next)) {
        city = next;
        patch.City = next;
        reasons.push(
          fromDir ? "city_from_choice_directory" : "city_from_choice_url_slug"
        );
      } else {
        reasons.push("city_unresolved");
      }
    }

    // Address (blank only — do not overwrite existing weak streets here)
    if (blank(address)) {
      if (isUsableChoiceAddress(dirCombined)) {
        address = dirCombined;
        patch.Address = dirCombined;
        patch["Address Confidence"] = "High";
        patch["Address Source URL"] =
          (url.includes("choicehotels.com") ? url : null) || dirRow?.propertyUrl || "";
        reasons.push("address_from_choice_directory");
      } else {
        try {
          const dirAddr = await resolveDirectoryAddressCandidate({
            fields: { ...f, City: city },
            identityKey: f["Property Identity Key"],
            family: "Choice",
          });
          if (dirAddr.ok && isUsableChoiceAddress(dirAddr.address)) {
            address = dirAddr.address;
            patch.Address = dirAddr.address;
            patch["Address Confidence"] = "High";
            patch["Address Source URL"] = dirAddr.source_url || url || "";
            reasons.push(`address_from_choice_directory:${dirAddr.method}`);
          } else {
            reasons.push(`directory_address_${dirAddr.reason || "none"}`);
          }
        } catch (err) {
          reasons.push(`directory_address_error:${err?.message || String(err)}`);
        }
      }

      if (
        blank(patch.Address) &&
        args.fillPlacesAddress &&
        apiKey &&
        city &&
        !unknownCity(city)
      ) {
        const propertyId = extractChoicePropertyId(f, f["Property Identity Key"]);
        let accepted = false;
        for (const searchName of placesSearchNames(f, city).slice(0, 3)) {
          const places = await lookupHotelOfficialUrlWithGoogle(
            {
              property_name: searchName,
              current_brand: f["Current Brand"],
              city,
              country,
              source_record_id: f["Property Identity Key"],
            },
            { apiKey, maxResults: 5 }
          );
          placesLookups += 1;
          await sleep(args.delayMs);
          const gConf = String(places?.match_confidence || "").toLowerCase();
          const placesAddr = places?.place?.google_formatted_address || "";
          const gName = places?.place?.google_name || places?.place?.googleName || "";
          const brandOk = gConf === "high" || placesBrandAligned(f, gName);
          const choiceUrlOk = placesChoiceUrlAligned(f, places, propertyId);
          // Prefer Choice website alignment; allow High+brand without URL only as fallback
          const acceptGate = choiceUrlOk || (gConf === "high" && brandOk);
          if (
            places?.status === "matched" &&
            (gConf === "high" || gConf === "medium") &&
            acceptGate &&
            brandOk &&
            isUsableChoiceAddress(placesAddr) &&
            cityInAddress(city, placesAddr)
          ) {
            address = placesAddr;
            patch.Address = placesAddr;
            patch["Address Confidence"] = choiceUrlOk
              ? gConf === "high"
                ? "High"
                : "Medium"
              : "Medium";
            patch["Address Source URL"] =
              places.place.google_maps_uri || places.place.google_website_uri || "";
            reasons.push(
              choiceUrlOk
                ? `address_from_google_places_${gConf}_choice_url`
                : `address_from_google_places_${gConf}_brand_high`
            );
            accepted = true;
            break;
          }
          reasons.push(
            `places_${gConf || places?.status || "none"}_skipped:url=${choiceUrlOk}:brand=${brandOk}`
          );
        }
        if (!accepted) reasons.push("places_no_usable_address");
      }
    }

    // State / Region
    if (blank(state) || isDirtyStateRegionValue(state)) {
      const st = resolveStateRegionFromCity({
        city: patch.City || city,
        country,
        state: isDirtyStateRegionValue(state) ? "" : state,
      });
      if (st.ok && st.state) {
        patch["State / Region"] = st.state;
        reasons.push(`state_from_city_map:${st.method || "ok"}`);
      } else {
        const fromUrl = resolveStateFromChoiceOfficialUrl(url);
        if (fromUrl.ok && fromUrl.state) {
          patch["State / Region"] = fromUrl.state;
          reasons.push("state_from_choice_url_slug");
        } else {
          const dirState = String(dirRow?.state || "").trim();
          if (dirState && !isDirtyStateRegionValue(dirState)) {
            patch["State / Region"] = dirState;
            reasons.push("state_from_choice_directory");
          } else {
            reasons.push(
              `state_unresolved:${st.reason || fromUrl.reason || "no_source"}`
            );
          }
        }
      }
    }

    if (Object.keys(patch).length) {
      patch["Last Reviewed Date"] = todayIsoDate();
      if (patch.City) fieldHits.City += 1;
      if (patch["State / Region"]) fieldHits["State / Region"] += 1;
      if (patch.Address) fieldHits.Address += 1;
      proposals.push({
        id: rec.id,
        property_name: f["Property Name"],
        identity_key: f["Property Identity Key"],
        country,
        before: {
          city: f.City || null,
          state: f["State / Region"] || null,
          address: f.Address || null,
        },
        patch,
        reasons,
      });
    } else {
      steward.push({
        id: rec.id,
        name: f["Property Name"],
        key: f["Property Identity Key"],
        city: f.City || null,
        country,
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
      "Choice geo fill: directory/city-map/URL only; never invent; never Google coords; never write dirty state codes",
    generated_at: new Date().toISOString(),
    scanned_gaps: gaps.length,
    processed: limited.length,
    proposal_count: proposals.length,
    patched_count: patched.length,
    steward_count: steward.length,
    field_hits: fieldHits,
    directory_lookups: directoryLookups,
    places_lookups: placesLookups,
    airtable_writes: doWrite,
    proposals,
    steward: steward.slice(0, 40),
  };

  mkdirSync(join(root, "reports"), { recursive: true });
  const outRel = doWrite
    ? "reports/census-choice-geo-fill-applied.json"
    : "reports/census-choice-geo-fill-dry-run.json";
  writeFileSync(join(root, outRel), JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        status: report.status,
        output: outRel,
        scanned_gaps: report.scanned_gaps,
        proposal_count: report.proposal_count,
        field_hits: report.field_hits,
        places_lookups: report.places_lookups,
        steward_count: report.steward_count,
        airtable_writes: report.airtable_writes,
        sample: proposals.slice(0, 12).map((p) => ({
          n: p.property_name,
          before: p.before,
          patch: p.patch,
          reasons: p.reasons,
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
