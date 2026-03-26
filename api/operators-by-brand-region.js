/**
 * Operator Intelligence
 * Uses Hotel Census (ALT base): Property–Brand–Operator dataset.
 * Ranks 3rd-party operators by keys, then hotel count, then segment match.
 */

import Airtable from "airtable";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY_READONLY }).base(process.env.AIRTABLE_BASE_ID_ALT);

// In-memory cache for ranked list (same filter params = same response)
const cache = new Map();
const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

function getCacheKey(query) {
  return `operators-by-brand-region-${JSON.stringify(query)}`;
}

function getFromCache(key) {
  const entry = cache.get(key);
  if (entry && Date.now() - entry.timestamp < CACHE_TTL_MS) return entry.data;
  return null;
}

function setCache(key, data) {
  cache.set(key, { data, timestamp: Date.now() });
}

const F = {
  table: "Hotel Census",
  name: "name",
  brand: "Affiliation",
  parentCompany: "Parent Company",
  status: "status",
  city: "city",
  country: "country",
  region: "Region",
  locationType: "Location",
  rooms: "rooms",
  chainScale: "Chain Scale",
  lat: "Latitude",
  lng: "Longitude",
  managementCompany: "Management Company",
  operationType: "Operation Type",
  // Optional – use if present in your base
  openDate: "Open Date",
  lastRenovation: "Last Renovation",
  sourceUrl: "Source URL",
};

// Region filter: map UI regions to countries (matches Radar dropdown)
const REGION_UI_TO_COUNTRIES = {
  "Caribbean & Latin America": [
    "Mexico", "Jamaica", "Dominican Republic", "Puerto Rico", "Cuba", "Bahamas", "Aruba",
    "Curaçao", "Cayman Islands", "Trinidad and Tobago", "Barbados", "Haiti",
    "Saint Lucia", "Antigua and Barbuda", "Grenada", "Saint Vincent and the Grenadines",
    "Dominica", "Saint Kitts and Nevis", "Turks and Caicos", "British Virgin Islands",
    "U.S. Virgin Islands", "Martinique", "Guadeloupe", "Bonaire",
    "Colombia", "Brazil", "Argentina", "Chile", "Peru", "Ecuador", "Costa Rica",
    "Panama", "Guatemala", "Honduras", "El Salvador", "Nicaragua", "Venezuela", "Uruguay", "Paraguay", "Bolivia",
  ],
  "North America": ["United States", "USA", "Canada", "United States of America"],
  "Europe": [
    "United Kingdom", "France", "Germany", "Spain", "Italy", "Portugal", "Netherlands",
    "Ireland", "Switzerland", "Austria", "Belgium", "Greece", "Poland", "Turkey",
    "Russia", "Czech Republic", "Hungary", "Romania", "Sweden", "Norway", "Denmark", "Finland",
    "Iceland", "Luxembourg", "Malta", "Cyprus", "Croatia", "Bulgaria", "Serbia", "Ukraine",
  ],
  "Middle East & Africa": [
    "United Arab Emirates", "Saudi Arabia", "Qatar", "Israel", "Egypt", "Jordan",
    "Lebanon", "Bahrain", "Kuwait", "Oman", "South Africa", "Morocco", "Kenya",
    "Nigeria", "Ethiopia", "Tanzania", "Ghana", "Tunisia", "Mauritius", "Rwanda",
  ],
  "Asia Pacific": [
    "China", "Japan", "India", "Singapore", "Thailand", "Indonesia", "Malaysia",
    "South Korea", "Vietnam", "Philippines", "Australia", "New Zealand", "Hong Kong",
    "Taiwan", "Sri Lanka", "Maldives", "Cambodia", "Myanmar", "Macau", "Pakistan", "Bangladesh",
  ],
};

function countryInRegionUI(country, regionKey) {
  if (!country || !regionKey) return false;
  const list = REGION_UI_TO_COUNTRIES[regionKey];
  if (!list) return false;
  const c = normalize(country);
  return list.some((r) => normalize(r) === c);
}

function countryToRegionUI(country) {
  if (!country || !normalize(country)) return "Other";
  const c = normalize(country);
  for (const [regionKey, list] of Object.entries(REGION_UI_TO_COUNTRIES)) {
    if (list.some((r) => normalize(r) === c)) return regionKey;
  }
  return "Other";
}

// Asset type → match against Chain Scale or Location
const ASSET_TYPE_KEYWORDS = {
  resort: ["resort", "Resort"],
  "select-service": ["select service", "select-service", "Select Service", "Midscale", "Upper Midscale", "Midscale"],
  urban: ["urban", "Urban", "city", "City", "upscale", "Upscale", "Luxury", "Upper Upscale"],
};

function normalize(s) {
  if (s == null || typeof s !== "string") return "";
  return s.toLowerCase().trim();
}

function matchesAssetType(chainScale, locationType, assetType) {
  if (!assetType) return true;
  const keywords = ASSET_TYPE_KEYWORDS[assetType];
  if (!keywords) return true;
  const scale = normalize(chainScale || "");
  const loc = normalize(locationType || "");
  const combined = `${scale} ${loc}`;
  return keywords.some((k) => combined.includes(normalize(k)));
}

function isRecentOpening(openDateStr, withinMonths = 36) {
  if (!openDateStr) return false;
  const d = new Date(openDateStr);
  if (Number.isNaN(d.getTime())) return false;
  const cutoff = new Date();
  cutoff.setMonth(cutoff.getMonth() - withinMonths);
  return d >= cutoff;
}

function matchesSearch(prop, searchStr) {
  if (!searchStr || !normalize(searchStr)) return true;
  const q = normalize(searchStr);
  const name = normalize(prop.property_name || "");
  const city = normalize(prop.city || "");
  const country = normalize(prop.country || "");
  return name.includes(q) || city.includes(q) || country.includes(q);
}

/**
 * GET /api/operators-by-brand-region
 * Query: search, parentCompany, brand, status, chainScale, region, locationType, operationType
 */
export async function getLargestOperatorsByBrandRegion(req, res) {
  try {
    const {
      search: searchQuery,
      parentCompany: parentCompanyFilter,
      brand: brandFilter,
      status: statusFilter,
      chainScale: chainScaleFilter,
      region: regionFilter,
      locationType: locationTypeFilter,
      operationType: operationTypeFilter,
    } = req.query;
    const brandFamily = parentCompanyFilter;

    const queryKey = {
      search: searchQuery,
      parentCompany: parentCompanyFilter,
      brand: brandFilter,
      status: statusFilter,
      chainScale: chainScaleFilter,
      region: regionFilter,
      locationType: locationTypeFilter,
      operationType: operationTypeFilter,
    };
    const cacheKey = getCacheKey(queryKey);
    const cached = getFromCache(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    const requiredFields = [
      F.name, F.brand, F.parentCompany, F.status, F.city, F.country, F.region,
      F.locationType, F.rooms, F.chainScale, F.lat, F.lng, F.managementCompany,
      F.operationType,
    ];

    // Fetch all records (Airtable paginates by pageSize; maxRecords caps how many we request)
    const MAX_RECORDS = 100000;
    const records = await base(F.table)
      .select({ fields: requiredFields, maxRecords: MAX_RECORDS, pageSize: 100 })
      .all();

    const properties = [];
    const byOperator = new Map();
    const byBrand = new Map();
    const byParentCompany = new Map();
    const byRegion = new Map();
    const byChainScale = new Map();
    const byStatus = new Map();
    let skippedBlankOperatorCount = 0;

    for (const rec of records) {
      const operatorName = (rec.fields[F.managementCompany] || "").toString().trim();
      const hasValidOperator = !!operatorName;

      const parentCompany = (rec.fields[F.parentCompany] || "").toString().trim();
      if (brandFamily && parentCompany && normalize(parentCompany) !== normalize(brandFamily)) continue;

      const brandVal = (rec.fields[F.brand] || "").toString().trim();
      if (brandFilter && brandVal && normalize(brandVal) !== normalize(brandFilter)) continue;

      const locationType = (rec.fields[F.locationType] || "").toString().trim();
      if (locationTypeFilter && locationType && normalize(locationType) !== normalize(locationTypeFilter)) continue;

      const operationTypeVal = (rec.fields[F.operationType] || "").toString().trim();
      if (operationTypeFilter && operationTypeVal && normalize(operationTypeVal) !== normalize(operationTypeFilter)) continue;

      const statusVal = (rec.fields[F.status] || "").toString().trim();
      if (statusFilter && statusVal && normalize(statusVal) !== normalize(statusFilter)) continue;

      const chainScaleVal = (rec.fields[F.chainScale] || "").toString().trim();
      if (chainScaleFilter && chainScaleVal && normalize(chainScaleVal) !== normalize(chainScaleFilter)) continue;

      const country = (rec.fields[F.country] || "").toString().trim();
      if (regionFilter && country && !countryInRegionUI(country, regionFilter)) continue;
      const chainScaleRaw = rec.fields[F.chainScale] ?? rec.fields["Chain scale"] ?? rec.fields["chain scale"];
      let chainScale = "";
      if (chainScaleRaw != null) {
        if (Array.isArray(chainScaleRaw)) chainScale = (chainScaleRaw[0] != null ? String(chainScaleRaw[0]).trim() : "");
        else chainScale = String(chainScaleRaw).trim();
      }
      if (!chainScale) chainScale = getField(rec, "Chain Scale", "Chain scale", "chain scale");
      const rooms = parseInt(rec.fields[F.rooms], 10) || 0;
      const openDateRaw = rec.fields[F.openDate];
      const openDateStr =
        openDateRaw != null
          ? typeof openDateRaw === "string"
            ? openDateRaw
            : (openDateRaw && typeof openDateRaw === "object" && "split" in openDateRaw ? openDateRaw.split("T")[0] : "")
          : "";

      // Operation Type: exact column "Operation Type". Single select returns string; multi-select returns array.
      const raw = rec.fields[F.operationType];
      let operation_type = "";
      if (raw != null) {
        if (Array.isArray(raw)) {
          const first = raw[0];
          operation_type = first != null ? (typeof first === "string" ? first : (first && first.name) || String(first)).trim() : "";
        } else if (typeof raw === "object" && raw !== null && "name" in raw) {
          operation_type = String(raw.name || "").trim();
        } else {
          operation_type = String(raw).trim();
        }
      }

      const prop = {
        id: rec.id,
        property_name: (rec.fields[F.name] || "Unknown").toString(),
        city: (rec.fields[F.city] || "").toString(),
        country,
        chain_scale: chainScale || null,
        brand: (rec.fields[F.brand] || "").toString(),
        flag: parentCompany || null,
        operator_name: operatorName,
        keys: rooms,
        open_date: openDateStr || null,
        last_renovation: rec.fields[F.lastRenovation] ?? null,
        source_url: rec.fields[F.sourceUrl] ?? null,
        lat: parseFloat(rec.fields[F.lat]) || null,
        lng: parseFloat(rec.fields[F.lng]) || null,
        status: (rec.fields[F.status] || "").toString(),
        operation_type: operation_type || null,
      };
      if (searchQuery && !matchesSearch(prop, searchQuery)) continue;
      properties.push(prop);

      if (hasValidOperator) {
        if (!byOperator.has(operatorName)) {
          byOperator.set(operatorName, {
            operator_name: operatorName,
            hotel_count: 0,
            total_keys: 0,
            brands: new Set(),
            hotel_types: new Set(),
            parent_companies: new Set(),
            recent_openings: [],
            properties: [],
          });
        }
        const op = byOperator.get(operatorName);
        op.hotel_count += 1;
        op.total_keys += rooms;
        if (prop.brand) op.brands.add(prop.brand);
        if (chainScale) op.hotel_types.add(chainScale);
        if (locationType) op.hotel_types.add(locationType);
        if (parentCompany) op.parent_companies.add(parentCompany);
        if (isRecentOpening(openDateStr)) op.recent_openings.push(prop);
        op.properties.push(prop);
      } else {
        skippedBlankOperatorCount += 1;
      }

      const brandKey = (prop.brand || "").trim() || "(no brand)";
      if (!byBrand.has(brandKey)) {
        byBrand.set(brandKey, { name: brandKey, hotel_count: 0, total_keys: 0, properties: [] });
      }
      const b = byBrand.get(brandKey);
      b.hotel_count += 1;
      b.total_keys += rooms;
      b.properties.push(prop);

      const parentKey = (parentCompany || "").trim() || "(no parent company)";
      if (!byParentCompany.has(parentKey)) {
        byParentCompany.set(parentKey, { name: parentKey, hotel_count: 0, total_keys: 0, properties: [] });
      }
      const pc = byParentCompany.get(parentKey);
      pc.hotel_count += 1;
      pc.total_keys += rooms;
      pc.properties.push(prop);

      const regionFromField = getField(rec, "Region", "region", F.region);
      const regionKey = (regionFromField && regionFromField.trim()) || countryToRegionUI(country);
      if (regionKey) {
        if (!byRegion.has(regionKey)) {
          byRegion.set(regionKey, { name: regionKey, hotel_count: 0, total_keys: 0, properties: [] });
        }
        const r = byRegion.get(regionKey);
        r.hotel_count += 1;
        r.total_keys += rooms;
        r.properties.push(prop);
      }

      const chainKey = chainScale || "(no chain scale)";
      if (!byChainScale.has(chainKey)) {
        byChainScale.set(chainKey, { name: chainKey, hotel_count: 0, total_keys: 0, properties: [] });
      }
      const cs = byChainScale.get(chainKey);
      cs.hotel_count += 1;
      cs.total_keys += rooms;
      cs.properties.push(prop);

      const statusRaw = rec.fields[F.status] ?? rec.fields["Status"] ?? rec.fields["status"];
      const statusKey = Array.isArray(statusRaw)
        ? (statusRaw[0] != null ? String(statusRaw[0]).trim() : "")
        : (statusRaw != null ? String(statusRaw).trim() : "");
      const statusKeyFinal = statusKey || "(no status)";
      if (!byStatus.has(statusKeyFinal)) {
        byStatus.set(statusKeyFinal, { name: statusKeyFinal, hotel_count: 0, total_keys: 0, properties: [] });
      }
      const st = byStatus.get(statusKeyFinal);
      st.hotel_count += 1;
      st.total_keys += rooms;
      st.properties.push(prop);
    }

    // brands = distinct Affiliation values per operator (from Hotel Census Affiliation column)
    const operators = Array.from(byOperator.values()).map((op) => {
      const parentCompanies = [...op.parent_companies].filter(Boolean).sort();
      return {
        operator_name: op.operator_name,
        hotel_count: op.hotel_count,
        total_keys: op.total_keys,
        brands: [...op.brands].sort(),
        hotel_types: [...op.hotel_types].filter(Boolean).sort(),
        parent_company: parentCompanies[0] || null,
        parent_companies: parentCompanies,
        recent_openings: op.recent_openings.slice(0, 10),
        recent_openings_count: op.recent_openings.length,
        properties: op.properties,
      };
    });

    operators.sort((a, b) => {
      if (b.total_keys !== a.total_keys) return b.total_keys - a.total_keys;
      if (b.hotel_count !== a.hotel_count) return b.hotel_count - a.hotel_count;
      return (b.brands.length + b.hotel_types.length) - (a.brands.length + a.hotel_types.length);
    });

    const brands = Array.from(byBrand.values());
    brands.sort((a, b) => {
      if (b.total_keys !== a.total_keys) return b.total_keys - a.total_keys;
      if (b.hotel_count !== a.hotel_count) return b.hotel_count - a.hotel_count;
      return (a.name || "").localeCompare(b.name || "");
    });

    const parent_companies = Array.from(byParentCompany.values());
    parent_companies.sort((a, b) => {
      if (b.total_keys !== a.total_keys) return b.total_keys - a.total_keys;
      if (b.hotel_count !== a.hotel_count) return b.hotel_count - a.hotel_count;
      return (a.name || "").localeCompare(b.name || "");
    });

    const regions = Array.from(byRegion.values());
    regions.sort((a, b) => {
      if (b.total_keys !== a.total_keys) return b.total_keys - a.total_keys;
      if (b.hotel_count !== a.hotel_count) return b.hotel_count - a.hotel_count;
      return (a.name || "").localeCompare(b.name || "");
    });

    const chain_scales = Array.from(byChainScale.values());
    chain_scales.sort((a, b) => {
      if (b.total_keys !== a.total_keys) return b.total_keys - a.total_keys;
      if (b.hotel_count !== a.hotel_count) return b.hotel_count - a.hotel_count;
      return (a.name || "").localeCompare(b.name || "");
    });

    const statuses = Array.from(byStatus.values());
    statuses.sort((a, b) => {
      if (b.total_keys !== a.total_keys) return b.total_keys - a.total_keys;
      if (b.hotel_count !== a.hotel_count) return b.hotel_count - a.hotel_count;
      return (a.name || "").localeCompare(b.name || "");
    });

    const payload = {
      success: true,
      operators,
      brands,
      parent_companies,
      regions,
      chain_scales,
      statuses,
      properties,
      total_properties: properties.length,
      total_in_census: records.length,
      skipped_blank_operator_count: skippedBlankOperatorCount,
    };
    setCache(cacheKey, payload);
    res.json(payload);
  } catch (error) {
    console.error("Error in getLargestOperatorsByBrandRegion:", error);
    res.status(500).json({
      success: false,
      error: "Internal Server Error",
      details: error.message,
    });
  }
}

// Try multiple possible Airtable field names (Census may vary by base)
function getField(rec, ...keys) {
  const fields = rec.fields || {};
  for (const k of keys) {
    const v = fields[k];
    if (v != null && String(v).trim() !== "") return String(v).trim();
  }
  return "";
}

// Get field value(s) - handles single string or Airtable multi-select array; try multiple keys.
function getFieldValues(rec, ...keys) {
  const fields = rec.fields || {};
  for (const k of keys) {
    const v = fields[k];
    if (v == null) continue;
    if (Array.isArray(v)) {
      return v.map((x) => String(x).trim()).filter(Boolean);
    }
    const s = String(v).trim();
    if (s) return [s];
  }
  return [];
}

/**
 * GET /api/operators-by-brand-region/filters
 * Returns parent companies, brands (Affiliation), and location types for dropdowns.
 */
export async function getOperatorsByBrandRegionFilters(req, res) {
  try {
    const records = await base(F.table)
      .select({ maxRecords: 20000, pageSize: 100 })
      .all();

    const parentCompanies = new Set();
    const brands = new Set();
    const locationTypes = new Set();
    const operationTypes = new Set();
    const chainScales = new Set();

    const addToSet = (val, set) => {
      if (val == null) return;
      if (Array.isArray(val)) val.forEach((x) => { const s = String(x).trim(); if (s) set.add(s); });
      else { const s = String(val).trim(); if (s) set.add(s); }
    };

    records.forEach((rec) => {
      const fields = rec.fields || {};
      const pc = getField(rec, "Parent Company", "Parent company", "parent_company");
      const brand = getField(rec, "Affiliation", "Brand", "brand");
      const loc = getField(rec, "Location", "Location Type", "location_type", "locationType");
      if (pc) parentCompanies.add(pc);
      if (brand) brands.add(brand);
      if (loc) locationTypes.add(loc);
      addToSet(fields["Operation Type"], operationTypes);
      addToSet(fields["Chain Scale"], chainScales);
    });

    const regions = Object.keys(REGION_UI_TO_COUNTRIES);

    // Chain scale dropdown order: Luxury → Economy → Independant (display order)
    const CHAIN_SCALE_ORDER = [
      "Luxury",
      "Upper Upscale",
      "Upscale",
      "Upper Midscale",
      "Midscale",
      "Economy",
      "Independant",
      "Independent",
    ];
    const sortChainScales = (a, b) => {
      const raw = (v) => (v || "").toString().trim();
      const key = (v) => raw(v).replace(/\s+chain\s*$/i, "") || raw(v);
      const indexOf = (v) => {
        const k = key(v).toLowerCase();
        const i = CHAIN_SCALE_ORDER.findIndex((o) => k === o.toLowerCase() || k.startsWith(o.toLowerCase() + " "));
        return i >= 0 ? i : CHAIN_SCALE_ORDER.length;
      };
      const ia = indexOf(a);
      const ib = indexOf(b);
      if (ia !== ib) return ia - ib;
      return raw(a).localeCompare(raw(b));
    };
    const sortedChainScales = [...chainScales].sort(sortChainScales);

    res.json({
      success: true,
      parentCompanies: [...parentCompanies].sort(),
      brands: [...brands].sort(),
      locationTypes: [...locationTypes].sort(),
      operationTypes: [...operationTypes].sort(),
      chainScales: sortedChainScales,
      regions,
    });
  } catch (error) {
    console.error("Error in getOperatorsByBrandRegionFilters:", error);
    res.status(500).json({
      success: false,
      error: "Internal Server Error",
      details: error.message,
    });
  }
}
