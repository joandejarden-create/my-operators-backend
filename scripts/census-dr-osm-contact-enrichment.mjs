#!/usr/bin/env node
/**
 * DR OSM HPC — Address / Phone / Rooms dry-run from Google Places + Official URL JSON-LD.
 * Report-only by default. Live apply: --apply --enable-production-writes + confirms + env.
 *
 * Policy:
 * - Prefer Official Property URL JSON-LD when present (Address / Phone / numberOfRooms)
 * - Else Google Places High/Medium match (Address / Phone only — never Rooms from Google)
 * - Rooms only from JSON-LD numberOfRooms / lodging room count (High integer)
 */
import "../load-env.js";
import { mkdirSync, readFileSync, writeFileSync, existsSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import * as cheerio from "cheerio";
import { identityKeyToOsmSourceId } from "../lib/independent-census/dr-osm-hpc-field-enrichment.js";
import { isBrandHomepageOfficialUrl } from "../lib/independent-census/official-property-url-quality.js";
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
import { isStreetLevelAddress } from "../lib/research-engine-v2/production-census-geocoding-providers.js";

/** Reject city+postal / vague Google locality strings. */
function isUsableStreetAddress(address) {
  const a = String(address || "").trim();
  if (!isStreetLevelAddress(a)) return false;
  if (/^(playa de|dominicus|bayahibe)\b/i.test(a) && !/\b(calle|av|carr|km)\b/i.test(a)) {
    return false;
  }
  if (/^\d{4,5}\s+[A-Za-zÁÉÍÓÚáéíóú]/.test(a) && !/\b(calle|av\.?|carr|km|hotel)\b/i.test(a)) {
    return false;
  }
  // Require a street-ish token or "Hotel … number" pattern
  if (
    !/\b(calle|c\.|av\.?|ave|avenue|carr\.?|carretera|blvd|boulevard|street|st\.|road|rd\.|km|plaza|paseo|highway|hotel|chalet)\b/i.test(
      a
    ) &&
    !/\d+\s*[ªºnN°]?\s*[A-Za-záéíóúñ]/i.test(a)
  ) {
    return false;
  }
  return true;
}

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const READ_FIELDS = [
  "Property Name",
  "Current Brand",
  "City",
  "Address",
  "Phone",
  "Rooms / Keys",
  "Official Property URL",
  "Property Identity Key",
  "Human Review Required",
];

const USER_AGENT = "DealalityCensusContactEnrichment/1.0 (research; dry-run)";
const FETCH_TIMEOUT_MS = 25000;
const MAX_FETCH = 40;

function parseArgs(argv = process.argv.slice(2)) {
  const get = (name, fb = "") => {
    const i = argv.indexOf(name);
    return i >= 0 ? argv[i + 1] : fb;
  };
  const confirms = {};
  for (const f of INTAKE_APPLY_CONFIRMS) confirms[f] = argv.includes(f);
  return {
    apply: argv.includes("--apply") && argv.includes("--enable-production-writes"),
    fetchOfficial: !argv.includes("--no-fetch"),
    maxFetch: Number(get("--max-fetch", String(MAX_FETCH))) || MAX_FETCH,
    googlePath: get(
      "--google",
      "reports/census-intake-google-places-url-dry-run-osm-dominican-republic-hotel-focused-2026-08-07.json"
    ),
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

function loadGoogleById(rel) {
  const p = join(root, rel);
  if (!existsSync(p)) return new Map();
  const json = JSON.parse(readFileSync(p, "utf8"));
  const map = new Map();
  for (const r of json.results || []) {
    if (r.source_record_id) map.set(String(r.source_record_id), r);
  }
  return map;
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

function extractFromJsonLd(blocks) {
  const nodes = [];
  for (const b of blocks) walkLd(b, nodes);
  let address = "";
  let phone = "";
  let rooms = null;
  for (const n of nodes) {
    const types = []
      .concat(n["@type"] || [])
      .map((t) => String(t).toLowerCase());
    const isHotel = types.some((t) =>
      /hotel|lodging|resort|motel|guesthouse/.test(t)
    );
    if (!isHotel && !n.address && n.telephone == null && n.numberOfRooms == null) {
      continue;
    }
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
    if (!phone && (n.telephone || n.phone)) {
      phone = String(n.telephone || n.phone);
    }
    if (rooms == null && n.numberOfRooms != null) {
      const num = Number.parseInt(String(n.numberOfRooms), 10);
      if (Number.isFinite(num) && num > 0 && num < 5000) rooms = num;
    }
  }
  return { address: address.trim(), phone: phone.trim(), rooms };
}

async function fetchOfficialMeta(url) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      signal: ctrl.signal,
      headers: { "User-Agent": USER_AGENT, Accept: "text/html" },
      redirect: "follow",
    });
    if (!res.ok) return { ok: false, reason: `http_${res.status}` };
    const html = await res.text();
    const fromLd = extractFromJsonLd(parseJsonLd(html));
    // Lightweight meta fallbacks
    const $ = cheerio.load(html);
    if (!fromLd.phone) {
      const tel = $('a[href^="tel:"]').first().attr("href") || "";
      if (tel) fromLd.phone = tel.replace(/^tel:/i, "").trim();
    }
    return { ok: true, ...fromLd, final_url: String(res.url || url) };
  } catch (err) {
    return { ok: false, reason: err.message || "fetch_failed" };
  } finally {
    clearTimeout(t);
  }
}

async function listOsmDr(baseId, token) {
  const out = [];
  let offset;
  const formula =
    "AND({Country}='Dominican Republic',FIND('independent_census_dr_osm',{VIC Freeze Hash}&''))";
  const base = new URLSearchParams({ filterByFormula: formula });
  for (const f of READ_FIELDS) base.append("fields[]", f);
  do {
    const params = new URLSearchParams(base);
    if (offset) params.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`,
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

  const googleById = loadGoogleById(args.googlePath);
  const token = resolvePat();
  const baseId = resolveTargetBase()?.target_base_id;
  const rows = await listOsmDr(baseId, token);

  const needWork = rows.filter((r) => {
    const f = r.fields || {};
    const addrOk = isUsableStreetAddress(f.Address || "");
    const phoneOk = Boolean(String(f.Phone || "").trim());
    const roomsOk = f["Rooms / Keys"] != null && f["Rooms / Keys"] !== "";
    return !addrOk || !phoneOk || !roomsOk;
  });

  const proposals = [];
  const fieldHits = { Address: 0, Phone: 0, "Rooms / Keys": 0 };
  let fetchCount = 0;

  for (const rec of needWork) {
    const f = rec.fields || {};
    const sid = identityKeyToOsmSourceId(f["Property Identity Key"]);
    const google = googleById.get(sid) || null;
    /** @type {Record<string, unknown>} */
    const patch = {};
    const reasons = [];

    const needAddr = !isUsableStreetAddress(f.Address || "");
    const needPhone = !String(f.Phone || "").trim();
    const needRooms = f["Rooms / Keys"] == null || f["Rooms / Keys"] === "";

    let officialMeta = null;
    const url = String(f["Official Property URL"] || "").trim();
    if (
      args.fetchOfficial &&
      fetchCount < args.maxFetch &&
      url &&
      !isBrandHomepageOfficialUrl(url) &&
      (needAddr || needPhone || needRooms)
    ) {
      officialMeta = await fetchOfficialMeta(url);
      fetchCount++;
      if (officialMeta.ok) {
        if (needAddr && isUsableStreetAddress(officialMeta.address)) {
          patch.Address = officialMeta.address;
          patch["Address Confidence"] = "High";
          patch["Address Source URL"] = officialMeta.final_url || url;
          reasons.push("address_from_official_jsonld");
        }
        if (needPhone && officialMeta.phone) {
          patch.Phone = officialMeta.phone;
          reasons.push("phone_from_official_page");
        }
        if (needRooms && officialMeta.rooms != null) {
          patch["Rooms / Keys"] = officialMeta.rooms;
          patch["Rooms Confidence"] = "High";
          patch["Rooms Source URL"] = officialMeta.final_url || url;
          reasons.push("rooms_from_official_jsonld");
        }
      } else {
        reasons.push(`official_fetch_${officialMeta.reason || "failed"}`);
      }
    }

    const gConf = String(google?.match_confidence || "").toLowerCase();
    const gOk = gConf === "high" || gConf === "medium";
    if (gOk && google?.place) {
      if (
        needAddr &&
        !patch.Address &&
        isUsableStreetAddress(google.place.google_formatted_address || "")
      ) {
        patch.Address = google.place.google_formatted_address;
        patch["Address Confidence"] = gConf === "high" ? "High" : "Medium";
        patch["Address Source URL"] =
          google.place.google_maps_uri ||
          google.suggested_official_property_url ||
          "";
        reasons.push("address_from_google_places");
      }
      if (needPhone && !patch.Phone && google.place.google_phone) {
        patch.Phone = google.place.google_phone;
        reasons.push("phone_from_google_places");
      }
    }

    if (!Object.keys(patch).length) continue;
    for (const k of Object.keys(fieldHits)) {
      if (patch[k] != null) fieldHits[k]++;
    }
    proposals.push({
      id: rec.id,
      property_name: f["Property Name"],
      current_brand: f["Current Brand"],
      identity_key: f["Property Identity Key"],
      patch,
      reasons,
      official_fetch: officialMeta
        ? { ok: officialMeta.ok, reason: officialMeta.reason || null }
        : null,
    });
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
    scanned: rows.length,
    need_work: needWork.length,
    proposal_count: proposals.length,
    patched_count: patched.length,
    field_hits: fieldHits,
    official_fetches: fetchCount,
    airtable_writes: doWrite,
    policy: {
      rooms_from_google: false,
      rooms_from_official_jsonld_only: true,
      brand_homepage_urls_skipped: true,
    },
    proposals,
  };

  mkdirSync(join(root, "reports"), { recursive: true });
  const out = doWrite
    ? "reports/census-dr-osm-contact-enrichment-applied.json"
    : "reports/census-dr-osm-contact-enrichment-dry-run.json";
  writeFileSync(join(root, out), JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        status: report.status,
        output: out,
        need_work: report.need_work,
        proposal_count: report.proposal_count,
        field_hits: report.field_hits,
        official_fetches: report.official_fetches,
        airtable_writes: report.airtable_writes,
        sample: proposals.slice(0, 12).map((p) => ({
          n: p.property_name,
          reasons: p.reasons,
          patch: p.patch,
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
