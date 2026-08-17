#!/usr/bin/env node
/**
 * Choice Hotels — High field completion (directory + city/state maps).
 * Does NOT write Choice central reservation phones or default rooms=25.
 * Census only. Dry-run unless --apply.
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { ensureChoiceCalaRegionalCache } from "../lib/research-engine-v2/census-autopilot-choice-cala-discovery-adapter.js";
import { extractChoicePropertyId } from "../lib/research-engine-v2/census-autopilot-family-directory-adapters.js";
import {
  resolveStateRegionFromCity,
  resolveStateFromChoiceOfficialUrl,
  isDirtyStateRegionValue,
} from "../lib/research-engine-v2/census-city-to-state-map.js";
import {
  resolveMarketFromCity,
  resolveSubmarketHighOnly,
} from "../lib/research-engine-v2/census-region-market-map.js";
import { isStreetLevelAddress } from "../lib/research-engine-v2/production-census-geocoding-providers.js";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";

const APPLY = process.argv.includes("--apply");
const ROOT = process.cwd();
const tableId = "tbl9aY5ijiuIzzWam";
const token = resolvePat();
const baseId = resolveTargetBase().target_base_id;
const today = new Date().toISOString().slice(0, 10);

function blank(v) {
  return v == null || String(v).trim() === "";
}

async function listChoice() {
  const formula =
    "OR(FIND('Choice', {Brand Family}), FIND('choicehotels.com', {Official Property URL}))";
  const fields = [
    "Property Name",
    "Canonical Property Name",
    "Current Brand",
    "Brand Family",
    "City",
    "State / Region",
    "Country",
    "Market",
    "Submarket",
    "Address",
    "Address Confidence",
    "Address Source URL",
    "Phone",
    "Rooms / Keys",
    "Latitude",
    "Longitude",
    "Official Property URL",
    "Source URL",
    "Property Identity Key",
  ];
  const rows = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    params.set("filterByFormula", formula);
    for (const f of fields) params.append("fields[]", f);
    if (offset) params.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const j = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(j));
    rows.push(...(j.records || []));
    offset = j.offset;
  } while (offset);
  return rows;
}

function buildPatch(rec, cache) {
  const f = rec.fields || {};
  const patch = {};
  const blockers = [];
  const methods = [];
  const propertyId = extractChoicePropertyId(f, f["Property Identity Key"]);
  const dir = propertyId ? cache.get(propertyId) : null;
  const city = f.City;
  const country = f.Country;

  // State / Region — overwrite dirty codes (ATL/CUN/BOL/…)
  if (blank(f["State / Region"]) || isDirtyStateRegionValue(f["State / Region"])) {
    const st = resolveStateRegionFromCity({
      city,
      country,
      state: f["State / Region"],
    });
    if (st.ok && st.state) {
      patch["State / Region"] = st.state;
      methods.push(st.method);
    } else {
      const fromUrl = resolveStateFromChoiceOfficialUrl(
        f["Official Property URL"] || f["Source URL"]
      );
      if (fromUrl.ok && fromUrl.state) {
        patch["State / Region"] = fromUrl.state;
        methods.push(fromUrl.method);
      } else {
        blockers.push({
          field: "State / Region",
          reason: st.reason || fromUrl.reason,
          current: f["State / Region"] || null,
        });
      }
    }
  }

  // Market
  if (blank(f.Market) && city) {
    const m = resolveMarketFromCity({ city, country });
    if (m.ok && m.market) {
      patch.Market = m.market;
      methods.push(m.method || "market_map");
    } else blockers.push({ field: "Market", reason: m.reason });
  }

  // Address from Choice regional directory
    if (blank(f.Address) && dir) {
    const addr = String(dir.addressLine1 || "").trim();
    const line2 = String(dir.addressLine2 || "").trim();
    const combined = line2 && !addr.includes(line2) ? `${addr}, ${line2}` : addr;
    const official =
      String(f["Official Property URL"] || "").trim() ||
      String(dir.propertyUrl || "").trim();
    const hasRoadToken =
      /\b(av\.?|ave\.?|avenida|calle|blvd\.?|boulevard|carr\.?|carretera|road|street|st\.?|via|vía|diagonal|transversal|km\.?)\b/i.test(
        addr
      );
    const hasGeo =
      Number.isFinite(Number(dir.latitude)) && Number.isFinite(Number(dir.longitude));
    const addressOk =
      isStreetLevelAddress(addr) ||
      (addr.length >= 12 && hasRoadToken && hasGeo);
    if (
      addressOk &&
      official &&
      /choicehotels\.com/i.test(official) &&
      !/regional-hotels/i.test(official)
    ) {
      patch.Address = combined;
      patch["Address Confidence"] = "High";
      patch["Address Source URL"] = official;
      methods.push(
        isStreetLevelAddress(addr)
          ? "choice_regional_address"
          : "choice_regional_address_road_token_with_geo"
      );
    } else {
      blockers.push({
        field: "Address",
        reason: !addr
          ? "directory_address_blank"
          : !addressOk
            ? "not_street_level"
            : "missing_property_url",
      });
    }
  }

  // Coords from Choice regional directory
  if ((blank(f.Latitude) || blank(f.Longitude)) && dir) {
    const lat = Number(dir.latitude);
    const lng = Number(dir.longitude);
    if (
      Number.isFinite(lat) &&
      Number.isFinite(lng) &&
      !(lat === 0 && lng === 0)
    ) {
      patch.Latitude = lat;
      patch.Longitude = lng;
      patch["Coordinate Source Type"] = "official_brand_directory";
      patch["Coordinate Confidence"] = "High";
      patch["Geocode Provider"] = "Choice regional directory";
      patch["Geocode Method"] = "choice_regional_geoLocation";
      patch["Geocode Reviewed Date"] = today;
      methods.push("choice_regional_geo");
    }
  }

  // Submarket High only
  if (blank(f.Submarket)) {
    const sub = resolveSubmarketHighOnly({
      market: patch.Market || f.Market,
      city,
      address: patch.Address || f.Address,
      propertyName: f["Canonical Property Name"] || f["Property Name"],
    });
    if (sub.ok && sub.submarket) {
      patch.Submarket = sub.submarket;
      methods.push(sub.method || "submarket_high");
    } else {
      blockers.push({ field: "Submarket", reason: sub.reason || "submarket_not_high" });
    }
  }

  // Phone / Rooms — explicitly blocked for this pass
  if (blank(f.Phone)) {
    blockers.push({
      field: "Phone",
      reason: "choice_property_phone_unavailable_central_hotline_only",
    });
  }
  if (blank(f["Rooms / Keys"])) {
    blockers.push({
      field: "Rooms / Keys",
      reason: "choice_rooms_blocked_sitewide_default_or_403",
    });
  }

  if (!Object.keys(patch).length) {
    return {
      id: rec.id,
      propertyId,
      skip: true,
      blockers,
      name: f["Canonical Property Name"] || f["Property Name"],
    };
  }

  patch["Last Reviewed Date"] = today;
  patch["Enrichment Status"] =
    "Choice Level-2 directory + geography completion (High only)";

  return {
    id: rec.id,
    propertyId,
    skip: false,
    name: f["Canonical Property Name"] || f["Property Name"],
    before: {
      state: f["State / Region"] || null,
      market: f.Market || null,
      submarket: f.Submarket || null,
      address: f.Address || null,
      lat: f.Latitude ?? null,
      phone: f.Phone || null,
      rooms: f["Rooms / Keys"] ?? null,
    },
    patch,
    methods,
    blockers,
  };
}

async function patchBatch(records) {
  let applied = 0;
  const errors = [];
  for (let i = 0; i < records.length; i += 10) {
    const chunk = records.slice(i, i + 10);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}`,
      {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ records: chunk, typecast: true }),
      }
    );
    const j = await res.json().catch(() => ({}));
    if (!res.ok) {
      for (const rec of chunk) {
        const res1 = await fetch(
          `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}`,
          {
            method: "PATCH",
            headers: {
              Authorization: `Bearer ${token}`,
              "Content-Type": "application/json",
            },
            body: JSON.stringify({ records: [rec], typecast: true }),
          }
        );
        const j1 = await res1.json().catch(() => ({}));
        if (!res1.ok) errors.push({ id: rec.id, error: j1.error || j1 });
        else applied += 1;
        await new Promise((r) => setTimeout(r, 160));
      }
      errors.push({ batch: true, error: j.error || j });
    } else {
      applied += (j.records || []).length;
    }
    await new Promise((r) => setTimeout(r, 180));
  }
  return { applied, errors };
}

const cache = await ensureChoiceCalaRegionalCache({});
const census = await listChoice();
const proposals = census.map((r) => buildPatch(r, cache));
const writable = proposals.filter((p) => !p.skip);
const fieldCounts = {
  state: 0,
  market: 0,
  submarket: 0,
  address: 0,
  coords: 0,
};
for (const p of writable) {
  if (p.patch["State / Region"]) fieldCounts.state += 1;
  if (p.patch.Market) fieldCounts.market += 1;
  if (p.patch.Submarket) fieldCounts.submarket += 1;
  if (p.patch.Address) fieldCounts.address += 1;
  if (p.patch.Latitude != null) fieldCounts.coords += 1;
}

const dirtyBefore = census.filter((r) =>
  isDirtyStateRegionValue(r.fields?.["State / Region"])
).length;

const report = {
  mode: APPLY ? "apply" : "dry-run",
  choice_records: census.length,
  dirty_state_before: dirtyBefore,
  writable: writable.length,
  field_counts: fieldCounts,
  phone_writes: 0,
  rooms_writes: 0,
  phone_blocker: "choice_central_reservation_hotline_rejected",
  rooms_blocker: "choice_sitewide_default_or_property_page_403",
  dirty_state_samples: writable
    .filter((p) => p.before?.state && isDirtyStateRegionValue(p.before.state))
    .map((p) => ({
      id: p.id,
      name: p.name,
      before: p.before.state,
      after: p.patch["State / Region"],
    })),
  sample: writable.slice(0, 20).map((p) => ({
    id: p.id,
    name: p.name,
    before: p.before,
    patch_keys: Object.keys(p.patch),
    methods: p.methods,
  })),
};

const outDir = path.join(ROOT, "reports/research-engine-v2");
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(
  path.join(outDir, "choice-level2-directory-completion.json"),
  `${JSON.stringify(report, null, 2)}\n`
);

console.log(JSON.stringify(report, null, 2));

if (!APPLY) {
  console.log("\nRe-run with --apply to write High patches.");
  process.exit(0);
}

if (
  process.env.ALLOW_CENSUS_AUTOPILOT_APPLY !== "1" ||
  process.env.CONFIRM_WRITE_TO_PRODUCTION_CENSUS !== "1"
) {
  console.error("Missing confirm env vars");
  process.exit(1);
}

const result = await patchBatch(
  writable.map((p) => ({ id: p.id, fields: p.patch }))
);
console.log(JSON.stringify({ applied: result.applied, errors: result.errors }, null, 2));
