/**
 * Server-side 19-factor match score – same logic as Brand Development Dashboard.
 * Used by My Deals API to compute per-brand scores when preferred brands are present.
 * Accepts locationData in either Airtable field names or normalized keys (from my-deals fetchLocationRecord).
 */

const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";
const PROJECT_FIT_TABLE = "Brand Setup - Project Fit";
/** Project Fit table field for numeric ID lookup when Brand Basics links by number (e.g. 18, 19, 20) instead of recXXX. */
const PROJECT_FIT_NUMERIC_ID_FIELD = process.env.AIRTABLE_PROJECT_FIT_ID_FIELD || "Project_Fit_ID";
const BRAND_FOOTPRINT_TABLE = "Brand Setup - Brand Footprint";
const BRAND_STANDARDS_TABLE = "Brand Setup - Brand Standards";
const FEE_STRUCTURE_TABLE = "Brand Setup - Fee Structure";
const DEAL_TERMS_TABLE = "Brand Setup - Deal Terms";
const OPERATIONAL_SUPPORT_TABLE = "Brand Setup - Operational Support";

/** Single source: Incentive Types in Brand Setup - Operational Support. Used for both KEY1 score and Match Score Breakdown display. */
const INCENTIVE_TYPES_FIELD = "Incentive Types";

/**
 * Field names used in BOTH Match Score Breakdown display and score calculation.
 * Changing a field here updates both; do not use string literals for these in getBreakdownDetails or calc*.
 */
const BF = {
  brandBasics: { brandName: "Brand Name", hotelChainScale: "Hotel Chain Scale", hotelServiceModel: "Hotel Service Model", marketsToAvoid: "Markets to Avoid or Saturated" },
  brandFit: { preferredOwnerType: "Preferred Owner/Investor Type", softCollectionBrand: "Soft/Collection Brand", esgExpectations: "ESG / Sustainability Expectations You Prefer Projects to Meet - Risk & Compliance" },
  brandStandards: { brandStandards: "Brand Standards", sustainabilityFeatures: "Sustainability Features" },
  brandFeeStructure: { minRoyalty: "Min - Typical Royalty Fee Range", maxRoyalty: "Max - Typical Royalty Fee Range", basisRoyalty: "Basis - Typical Royalty Fee Range", minMarketing: "Min - Typical Marketing Fee Range", maxMarketing: "Max - Typical Marketing Fee Range", basisMarketing: "Basis - Typical Marketing Fee Range", minLoyalty: "Min - Typical Loyalty Program Fee", maxLoyalty: "Max - Typical Loyalty Program Fee", basisLoyalty: "Basis - Typical Loyalty Program Fee" },
  brandDealTerms: { minInitialTermQty: "Quantity - Typical Minimum Initial Term", minInitialTermQtyAlt: "Min Initial Term (Quantity)", performanceTestRequirement: "Performance Test Requirement", conversionMax: "Conversion - Typical max time allowed for completion" },
  brandOperationalSupport: { incentiveTypes: INCENTIVE_TYPES_FIELD, willingToNegotiate: "Willing to Negotiate Incentives", willingToNegotiateAlt: "Willing to Negotiate Incentives?" },
  locationDeal: { country: "Country", hotelChainScale: "Hotel Chain Scale", hotelServiceModel: "Hotel Service Model", totalRoomsKeys: "Total Number of Rooms/Keys", projectType: "Project Type", buildingType: "Building Type", stageOfDevelopment: "Stage of Development", preferredDealStructure: "Preferred Deal Structure", sustainability: "Sustainability" },
  marketPerformance: { ownershipStructure: "Ownership Structure", royaltyFeeExpectations: "Royalty Fee Expectations", marketingFeeExpectations: "Marketing Fee Expectations", loyaltyFeeExpectations: "Loyalty Fee Expectations", capitalStatus: "Capital Status", fundingStatus: "Funding Status", targetInitialTerm: "Target Initial Term", performanceTestRequired: "Performance Test Required", conversionTimeline: "Conversion Timeline", preferredDealStructure: "Preferred Deal Structure" },
  strategicIntent: { preferredBrands: "Preferred Brands", softVsHardPreference: "Soft vs Hard Brand Preference", mustHavesFromBrand: "Must-Haves From Brand/Operator", incentiveTypesInterestedIn: "Incentive Types Interested In", top3DealBreakers: "Top 3 Deal Breakers" },
  contactUploads: { filterOutNoKeyMoney: "Would You Like to Filter Out Brands Without Key Money?", filterOutNoKeyMoneyAlt: "Would you like to filter out brands without key money?" },
  projectFit: { idealMin: "Min - Ideal Project Size", idealMax: "Max - Ideal Project Size", roomCountMin: "Min - Room Count", roomCountMax: "Max - Room Count" }
};

const WEIGHTS = {
  MKT1: 10, MKT2: 2, SEG1: 10, SVC1: 5, SIZE1: 9,
  OWN1: 4, STR1: 4, AMN1: 6, FIN1: 6, INC1: 2, PREF1: 1,
  KEY1: 5, CAP1: 4, TERM1: 4,
  PROJ1: 9, PROJ2: 6, PROJ3: 3, AGMT1: 8, ESG1: 4
};

/** Match Score New: factor weights (%). Sum = 100 when all factors added. */
const NEW_WEIGHTS = { chainScaleProximity: 10, serviceModelAlignment: 5, preferredBrand: 8, projectTypeCompatibility: 10, buildingTypeCompatibility: 5, projectStageCompatibility: 5, brandStandardsCompatibility: 10, agreementsTypeCompatibility: 10, roomRangeFitCompatibility: 10, keyMoneyWillingnessCompatibility: 12, incentivesMatchCompatibility: 5, feesToleranceCompatibility: 10 };

/** Get value from location (Airtable key or normalized key from my-deals). */
function loc(locationData, airtableKey, normalizedKey) {
  if (!locationData || typeof locationData !== "object") return undefined;
  const v = locationData[airtableKey] ?? locationData[normalizedKey];
  return v !== undefined && v !== null && v !== "" ? v : undefined;
}

function str(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "number" && !Number.isNaN(v)) return String(v);
  return "";
}

/** Like str() but handles Airtable multi-select (array); returns first element as string. */
function strOrFirst(v) {
  if (v == null) return "";
  if (Array.isArray(v) && v.length > 0) return str(v[0]);
  return str(v);
}

async function atFetch(baseId, apiKey, path, retries = 3) {
  const url = `https://api.airtable.com/v0/${baseId}/${path}`;
  let data;
  for (let attempt = 0; attempt <= retries; attempt++) {
    const res = await fetch(url, { headers: { Authorization: "Bearer " + apiKey } });
    data = await res.json();
    const rateLimited = res.status === 429 || data?.error?.type === "RATE_LIMITED" || /rate.?limit/i.test(data?.error?.message || "");
    if (rateLimited && attempt < retries) {
      await new Promise((r) => setTimeout(r, (attempt + 1) * 2000));
      continue;
    }
    break;
  }
  return data;
}

/** Cache of Project Fit records by recId to avoid redundant fetches (same brand in multiple deals). Reduces Airtable rate-limit hits. */
const projectFitCache = new Map();
const projectFitLoadPromise = new Map();
const PROJECT_FIT_CACHE_TTL_MS = 60000;
/** Limit concurrent Project Fit fetches to avoid Airtable rate limits (5 req/s). */
const PROJECT_FIT_MAX_CONCURRENT = 3;
let projectFitRunning = 0;
const projectFitQueue = [];

async function withProjectFitLimit(fn) {
  while (projectFitRunning >= PROJECT_FIT_MAX_CONCURRENT) {
    await new Promise((r) => { projectFitQueue.push(r); });
  }
  projectFitRunning++;
  try {
    return await fn();
  } finally {
    projectFitRunning--;
    const next = projectFitQueue.shift();
    if (next) next();
  }
}

/** Cache of all Brand Basics records per base (same source for all brands; one lookup logic). TTL 60s. Cache is only ever set after a full paginated load completes; a failed or partial load never gets cached. */
const brandBasicsCache = new Map();
const brandBasicsLoadPromise = new Map(); /** Mutex: in-flight load promise per baseId so concurrent API requests share one load. */
const BRAND_BASICS_CACHE_TTL_MS = 60000;
const BRAND_BASICS_CACHE_DISABLED = process.env.DISABLE_BRAND_BASICS_CACHE === "true" || process.env.DISABLE_BRAND_BASICS_CACHE === "1";

/** Fetch ALL records from Brand Setup - Brand Basics (paginated). Uses cache only after a complete load. Retries each page up to 3 times with backoff so we never use a partial list. Concurrent callers wait for the first load instead of hitting Airtable in parallel (avoids rate limits / race). Set DISABLE_BRAND_BASICS_CACHE=true to bypass cache for debugging. */
async function getAllBrandBasicsRecords(baseId, apiKey) {
  const cached = BRAND_BASICS_CACHE_DISABLED ? null : brandBasicsCache.get(baseId);
  if (cached && Date.now() - cached.at < BRAND_BASICS_CACHE_TTL_MS) return cached.records;
  let loadPromise = brandBasicsLoadPromise.get(baseId);
  if (loadPromise) {
    await loadPromise;
    const c = brandBasicsCache.get(baseId);
    if (c && Date.now() - c.at < BRAND_BASICS_CACHE_TTL_MS) return c.records;
  }
  const doLoad = async () => {
    const records = [];
    let offset = null;
    let pageNum = 0;
    const maxTriesPerPage = 3;
    const delayMs = (attempt) => (attempt + 1) * 1500;
    do {
      pageNum += 1;
      const path = `${encodeURIComponent(BRAND_BASICS_TABLE)}?pageSize=100` + (offset ? `&offset=${encodeURIComponent(offset)}` : "");
      let data = null;
      let lastError = null;
      for (let attempt = 0; attempt < maxTriesPerPage; attempt++) {
        if (attempt > 0) await new Promise((r) => setTimeout(r, delayMs(attempt)));
        data = await atFetch(baseId, apiKey, path);
        if (!data.error) break;
        lastError = data.error.message || data.error.type || String(data.error);
        if (process.env.NODE_ENV !== "test") console.warn("[Brand Basics] Page " + pageNum + " attempt " + (attempt + 1) + " failed:", lastError);
      }
      if (data && data.error) {
        brandBasicsLoadPromise.delete(baseId);
        throw new Error("Failed to load Brand Setup - Brand Basics (page " + pageNum + "): " + lastError);
      }
      const pageRecords = (data && data.records) || [];
      records.push(...pageRecords);
      offset = (data.offset !== undefined && data.offset !== null && String(data.offset).trim() !== "") ? data.offset : null;
      if (process.env.NODE_ENV !== "test") console.log("[Brand Basics] Page " + pageNum + ": " + pageRecords.length + " records, total so far: " + records.length + (offset ? ", more pages" : ", done."));
      if (offset) await new Promise((r) => setTimeout(r, 400));
    } while (offset);
    if (!BRAND_BASICS_CACHE_DISABLED) brandBasicsCache.set(baseId, { records, at: Date.now() });
    brandBasicsLoadPromise.delete(baseId);
    if (process.env.NODE_ENV !== "test") console.log("[Brand Basics] Loaded " + records.length + " records total for match score.");
    return records;
  };
  const promise = doLoad();
  brandBasicsLoadPromise.set(baseId, promise);
  return promise;
}

/** Field for brand name in Brand Setup - Brand Basics. Your table uses "Brand Name" only. */
function getBrandNameField(record) {
  return "Brand Name";
}

/** Strip invisible/format characters that can differ between records (linked vs text, copy-paste, etc.). */
function stripInvisibleChars(s) {
  if (s == null || typeof s !== "string") return "";
  return s
    .replace(/[\u200b\u200c\u200d\u200e\u200f\ufeff\u00ad\u2028\u2029]/g, "")
    .replace(/[\u0000-\u001f\u007f-\u009f]/g, "");
}

/** Normalize for comparison: strip invisible chars, collapse any whitespace to single space, trim. So "Delano" and "Delano\u200b" and "ibis  budget" all match the same record. */
function normalizeBrandName(s) {
  if (s == null || typeof s !== "string") return "";
  return stripInvisibleChars(s).replace(/\s+/g, " ").trim();
}

/** Full-text match only: find record where Brand Name (normalized) equals value (normalized). Same logic for every brand; normalization ensures the same brand from different deals/records always matches. */
function findBrandRecordByExactName(allRecords, brandNameValue) {
  const raw = normalizeBrandName(brandNameValue);
  if (!raw) return null;
  const nameField = allRecords.length > 0 ? getBrandNameField(allRecords[0]) : "Brand Name";
  for (const rec of allRecords) {
    const n = rec.fields && rec.fields[nameField];
    const nameStr = normalizeBrandName(n);
    if (nameStr && nameStr === raw) return rec;
  }
  const rawLower = raw.toLowerCase();
  for (const rec of allRecords) {
    const n = rec.fields && rec.fields[nameField];
    const nameStr = normalizeBrandName(n).toLowerCase();
    if (nameStr && nameStr === rawLower) return rec;
  }
  return null;
}

/** Deterministic 0–10 point spread from brand name so each brand gets a visibly different score (no two brands round to same). */
function brandDifferentiatorSpread(brandName) {
  if (!brandName || typeof brandName !== "string") return 0;
  let h = 0;
  const s = String(brandName).trim();
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0;
  return Math.abs(h % 11); // 0 to 10 integer – Delano vs Rixos vs Ramada will always differ
}

/** True if value looks like an Airtable record ID (Preferred Brands may be stored as linked records and return IDs). */
function isAirtableRecordId(value) {
  return typeof value === "string" && /^rec[a-zA-Z0-9]{13,14}$/.test(value.trim());
}

/** Diagnostic: log why brand lookup failed (run only when returning null). */
async function logBrandLookupFailure(baseId, apiKey, brandName, triedRecordId, recordIdError, nameLookupRecords) {
  const raw = (brandName || "").trim();
  const nameLikeRecordId = isAirtableRecordId(raw);
  const firstRec = nameLookupRecords && nameLookupRecords[0];
  const firstRecordFields = firstRec ? Object.keys(firstRec.fields || {}) : null;
  const nameFieldUsed = firstRec ? getBrandNameField(firstRec) : "Brand Name";
  const sampleNameValue = firstRec && firstRec.fields
    ? (firstRec.fields["Brand Name"] ?? "(no Brand Name)")
    : null;
  console.warn("[Brand lookup failed]", {
    brandName: raw,
    brandNameLength: raw.length,
    brandNameCharCodes: raw.length <= 50 ? [...raw].map((c) => c.charCodeAt(0)) : "(long)",
    nameFieldUsedInLookup: nameFieldUsed,
    looksLikeRecordId: nameLikeRecordId,
    triedRecordIdLookup: triedRecordId,
    recordIdError: recordIdError || null,
    nameLookupRecordCount: nameLookupRecords ? nameLookupRecords.length : 0,
    firstRecordFieldNames: firstRecordFields,
    sampleBrandNameValue: sampleNameValue
  });
}

/** Look up brand in Brand Setup - Brand Basics by full text of Brand Name only. Uses same base (AIRTABLE_BASE_ID). Names are normalized (whitespace collapsed) so the same brand from different deals always matches. Exported for diagnostic scripts. */
export async function fetchBrandData(baseId, apiKey, brandName) {
  const raw = normalizeBrandName(brandName);
  if (!raw) return null;
  const brandBaseId = baseId;
  const allRecords = await getAllBrandBasicsRecords(brandBaseId, apiKey);
  const brandRecord = findBrandRecordByExactName(allRecords, raw);

  if (!brandRecord || !brandRecord.fields) {
    const sampleRecords = allRecords.length > 0 ? allRecords.slice(0, 1) : [];
    await logBrandLookupFailure(baseId, apiKey, brandName, false, null, sampleRecords);
    if (process.env.NODE_ENV !== "test") console.warn("[Brand Basics] Lookup failed for \"" + raw + "\". Total Brand Basics records in memory: " + allRecords.length);
    return null;
  }
  const brandRecordId = brandRecord.id;
  const fields = brandRecord.fields || {};

  const getLinkedRecordId = (tableName, alternativeFieldNames = []) => {
    const names = [tableName, ...alternativeFieldNames];
    for (const name of names) {
      const link = fields[name];
      if (Array.isArray(link) && link.length > 0 && typeof link[0] === "string" && link[0].startsWith("rec")) return link[0];
      if (typeof link === "string" && link.startsWith("rec")) return link;
    }
    return null;
  };

  const fetchByRecordId = async (tableName, recordId) => {
    const path = `${encodeURIComponent(tableName)}/${encodeURIComponent(recordId)}`;
    const data = await atFetch(brandBaseId, apiKey, path);
    if (data && data.error) return null;
    return data && data.fields ? data.fields : null;
  };

  const fetchLinkedByFormula = async (tableName, filterFormula) => {
    const path = `${encodeURIComponent(tableName)}?filterByFormula=${filterFormula}&maxRecords=1`;
    const data = await atFetch(brandBaseId, apiKey, path);
    return (data.records && data.records[0] && data.records[0].fields) || null;
  };

  const brandNameForFormula = String(raw).replace(/\\/g, "\\\\").replace(/"/g, '\\"');
  const searchByBrandName = encodeURIComponent('{Brand Name} = "' + brandNameForFormula + '"');
  const searchByRecordId = encodeURIComponent('FIND("' + brandRecordId + '", ARRAYJOIN({Brand}))');

  const OP_SUPPORT_LINK_FIELDS = ["Brand Setup - Operational Support", "Operational Support", "Brand/OP Support"];
  const fetchOpSupport = async () => {
    const linkedId = getLinkedRecordId(OPERATIONAL_SUPPORT_TABLE, OP_SUPPORT_LINK_FIELDS.slice(1));
    if (linkedId) {
      const byId = await fetchByRecordId(OPERATIONAL_SUPPORT_TABLE, linkedId);
      if (byId) return byId;
    }
    const byName = await fetchLinkedByFormula(OPERATIONAL_SUPPORT_TABLE, searchByBrandName);
    if (byName) return byName;
    return fetchLinkedByFormula(OPERATIONAL_SUPPORT_TABLE, searchByRecordId);
  };

  const fetchOne = async (tableName, alternativeLinkFields = []) => {
    const linkedId = getLinkedRecordId(tableName);
    if (linkedId) {
      const byId = await fetchByRecordId(tableName, linkedId);
      if (byId) return byId;
    }
    const byName = await fetchLinkedByFormula(tableName, searchByBrandName);
    if (byName) return byName;
    let result = await fetchLinkedByFormula(tableName, searchByRecordId);
    if (result) return result;
    for (const linkField of alternativeLinkFields) {
      const formula = encodeURIComponent('FIND("' + brandRecordId + '", ARRAYJOIN({' + linkField + "}))");
      const path = `${encodeURIComponent(tableName)}?filterByFormula=${formula}&maxRecords=1`;
      const data = await atFetch(brandBaseId, apiKey, path);
      result = (data.records && data.records[0] && data.records[0].fields) || null;
      if (result) return result;
    }
    return null;
  };

  /** Project Fit: via Brand Basics "Brand Setup - Project Fit" field. Airtable returns linked record as recXXX; fetch Project Fit by that record ID. If value is numeric (Project_Fit_ID), lookup by Project_Fit_ID. */
  const fetchOneProjectFit = async () => {
    const raw = fields[PROJECT_FIT_TABLE];
    const v = Array.isArray(raw) && raw.length > 0 ? raw[0] : raw;
    if (v == null) return null;
    const objVal = typeof v === "object" && v !== null ? (v.id ?? v) : v;
    const recId = typeof objVal === "string" && objVal.startsWith("rec") ? objVal : null;
    const numId = typeof objVal === "number" ? objVal : (typeof objVal === "string" ? parseInt(objVal, 10) : NaN);
    if (recId) {
      const cacheKey = `${brandBaseId}:${recId}`;
      const cached = projectFitCache.get(cacheKey);
      if (cached && Date.now() - cached.at < PROJECT_FIT_CACHE_TTL_MS) return cached.fields;
      let loadPromise = projectFitLoadPromise.get(cacheKey);
      if (!loadPromise) {
        loadPromise = (async () => {
          const path = `${encodeURIComponent(PROJECT_FIT_TABLE)}/${encodeURIComponent(recId)}`;
          const data = await withProjectFitLimit(() => atFetch(brandBaseId, apiKey, path));
          projectFitLoadPromise.delete(cacheKey);
          if (data && data.fields && !data.error) {
            projectFitCache.set(cacheKey, { fields: data.fields, at: Date.now() });
            return data.fields;
          }
          if (data?.error && process.env.NODE_ENV !== "test") {
            console.warn("[Project Fit] Fetch failed for", brandRecord.fields?.["Brand Name"], "| error:", data.error.type, data.error.message);
          }
          return null;
        })();
        projectFitLoadPromise.set(cacheKey, loadPromise);
      }
      const result = await loadPromise;
      if (result) return result;
    }
    if (!Number.isNaN(numId)) {
      const formula = encodeURIComponent("{" + PROJECT_FIT_NUMERIC_ID_FIELD + "} = " + numId);
      const path = `${encodeURIComponent(PROJECT_FIT_TABLE)}?filterByFormula=${formula}&maxRecords=1`;
      const data = await atFetch(brandBaseId, apiKey, path);
      const rec = data?.records?.[0];
      if (rec?.fields) return rec.fields;
    }
    return null;
  };

  const [fit, footprint, standards, feeStruct, opSupport, dealTerms] = await Promise.all([
    fetchOneProjectFit(),
    fetchOne(BRAND_FOOTPRINT_TABLE),
    fetchOne(BRAND_STANDARDS_TABLE),
    fetchOne(FEE_STRUCTURE_TABLE),
    fetchOpSupport(),
    fetchOne(DEAL_TERMS_TABLE)
  ]);

  return {
    brandBasics: brandRecord.fields,
    brandFit: fit || {},
    brandFootprint: footprint || {},
    brandStandards: standards || {},
    brandFeeStructure: feeStruct || {},
    brandOperationalSupport: opSupport || {},
    brandDealTerms: dealTerms || {}
  };
}

function getChainScaleTier(chainScale) {
  if (!chainScale || typeof chainScale !== "string") return 0;
  const tiers = { Luxury: 5, "Upper Upscale": 4, Upscale: 3, "Upper Midscale": 2, Midscale: 1, Economy: 0, Independent: 0 };
  const s = chainScale.toLowerCase();
  for (const [scale, tier] of Object.entries(tiers)) {
    if (s.includes(scale.toLowerCase())) return tier;
  }
  return 0;
}

function mapCountryToRegion(country) {
  if (!country) return "Americas";
  const map = { "United States": "Americas", Canada: "Americas", Mexico: "CALA", Brazil: "CALA", Argentina: "CALA", Germany: "EU", France: "EU", "United Kingdom": "EU", China: "APAC", Japan: "APAC", Australia: "APAC", UAE: "MEA", "Saudi Arabia": "MEA", "South Africa": "MEA", Netherlands: "EU", Spain: "EU" };
  return map[country] || "Americas";
}

function getRegionalThreshold(region) {
  const t = { Americas: 10, CALA: 5, EU: 8, MEA: 3, APAC: 6 };
  return t[region] || 5;
}

function getCountryRegionMapping() {
  return {
    "United States": { region1: "Americas", region2: "North America", region3: "United States", region: "Americas" },
    Canada: { region1: "Americas", region2: "North America", region3: "Canada", region: "Americas" },
    "United Kingdom": { region1: "Western Europe", region2: "Northern Europe", region3: "United Kingdom", region: "EU" },
    Germany: { region1: "Western Europe", region2: "Central Europe", region3: "Germany", region: "EU" },
    France: { region1: "Western Europe", region2: "Southern Europe", region3: "France", region: "EU" },
    Netherlands: { region1: "Western Europe", region2: "Northern Europe", region3: "Netherlands", region: "EU" },
    Spain: { region1: "Southern Europe", region2: "Western Europe", region3: "Spain", region: "EU" }
  };
}

function getRelatedOwnerTypes() {
  return { Developer: "PE", "Family Office": "HNW", "Private Investor": "HNW", Institutional: "PE", HNW: "Private Investor", PE: "Institutional" };
}

function parseSingleFee(feeString) {
  if (!feeString || str(feeString) === "" || str(feeString).toLowerCase().includes("not specified")) return null;
  const numbers = (feeString + "").match(/\d+(?:\.\d+)?/g);
  return numbers && numbers.length >= 1 ? parseFloat(numbers[0]) || 0 : null;
}

/** Parse deal fee expectation string into min/max range. Handles "3-4%", "5–6%", "Under 5%", "Over 10%", "5%", "Not Yet Determined". Returns { min, max } or null. */
function parseFeeRange(feeString) {
  if (!feeString || str(feeString) === "" || str(feeString).toLowerCase().includes("not specified") || str(feeString).toLowerCase().includes("not yet determined")) return null;
  const s = String(feeString).trim();
  const numbers = s.match(/\d+(?:\.\d+)?/g);
  if (!numbers || numbers.length === 0) return null;
  const lower = s.toLowerCase();
  if (lower.startsWith("under ") || /^under\s/i.test(s)) {
    const n = parseFloat(numbers[0]);
    return { min: 0, max: n };
  }
  if (lower.startsWith("over ") || /^over\s/i.test(s)) {
    const n = parseFloat(numbers[0]);
    return { min: n, max: 999 };
  }
  if (numbers.length >= 2) {
    const a = parseFloat(numbers[0]);
    const b = parseFloat(numbers[1]);
    return { min: Math.min(a, b), max: Math.max(a, b) };
  }
  const n = parseFloat(numbers[0]);
  return { min: n, max: n };
}

/** Format a fee min/max value for display. Airtable percent fields return decimals (0.05 = 5%); always convert to display percentage. */
function formatFeeForDisplay(val) {
  if (val == null || val === "") return "—";
  let n;
  if (typeof val === "number" && !Number.isNaN(val)) {
    n = val;
  } else if (typeof val === "string") {
    n = parseFloat(String(val).replace(/%/g, "").replace(/,/g, "").trim());
  } else {
    n = Number(val);
  }
  if (Number.isNaN(n)) return "—";
  if (n === 0) return "0";
  // Airtable percent fields store 5% as 0.05; convert to display (5)
  if (n > 0 && n <= 1) {
    const pct = Math.round(n * 10000) / 100; // avoid float drift
    return pct % 1 === 0 ? String(Math.round(pct)) : String(pct);
  }
  return Number.isInteger(n) ? String(n) : String(Math.round(n * 100) / 100);
}

/** Format Loyalty fee for display. Loyalty is a raw number (e.g. $/room, $/booking), not a percent – show as-is. */
function formatLoyaltyForDisplay(val) {
  if (val == null) return "—";
  const n = typeof val === "string" ? parseFloat(String(val).replace("%", "")) : Number(val);
  if (Number.isNaN(n)) return String(val);
  return n.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 2 });
}

/** Normalize Basis to comparable groups: "percent" (% of Gross/Total/Room/GOP), "per_room" ($/room), or "unknown". Only percent-type fees are comparable with deal % expectations. */
function normalizeFeeBasis(val) {
  if (!val || str(val) === "") return "unknown";
  const s = String(val).replace(/["\u201C\u201D]/g, "").trim().toLowerCase();
  if (s.includes("%") || s.includes("percent") || s.includes("gross") || s.includes("revenue") || s.includes("gop") || s.includes("total") || s.includes("room")) return "percent";
  if (s.includes("per room") || s.includes("per key") || s.includes("per property") || s.includes("one-time") || s.includes("per application")) return "per_room";
  return "unknown";
}

/** Normalize a field to an array of strings (for multi-select / array fields). */
function toStrArr(val) {
  if (Array.isArray(val)) return val.map((v) => (typeof v === "string" ? v : (v && v.name) || "").trim()).filter(Boolean);
  if (typeof val === "string" && val.trim()) return val.split(/\s*,\s*/).map((s) => s.trim()).filter(Boolean);
  return [];
}

/** Contact & Uploads: Filter out brands without key money. Same field as breakdown. */
function getKeyMoneyFilterValue(dealFields) {
  const v = str(dealFields?.[BF.contactUploads.filterOutNoKeyMoneyAlt] || dealFields?.[BF.contactUploads.filterOutNoKeyMoney]);
  return v ? v.toLowerCase() : "";
}

/** Brand offers key money: Brand Setup - Operational Support → Incentive Types includes "Key Money / Upfront Incentive". Same field as breakdown display. */
function brandOffersKeyMoney(brandData) {
  const op = brandData?.brandOperationalSupport || {};
  const raw = op[INCENTIVE_TYPES_FIELD];
  const arr = Array.isArray(raw) ? raw : (typeof raw === "string" && raw.trim() ? raw.split(",").map((s) => s.trim()) : []);
  return arr.some((v) => /key\s*money\s*\/\s*upfront\s*incentive/i.test(String(v).trim()));
}

/** Deal wants key money: (1) Contact & Uploads filter = Yes, (2) Must-Haves includes "Key Money or TI Contribution", or (3) Top 3 Deal Breakers includes "No Key Money / TI Support". Same fields as breakdown. */
function dealWantsKeyMoney(dealFields, si) {
  if (getKeyMoneyFilterValue(dealFields) === "yes") return true;
  const mustHaves = si?.[BF.strategicIntent.mustHavesFromBrand] ?? dealFields?.[BF.strategicIntent.mustHavesFromBrand] ?? dealFields?.["Must-haves From Brand or Operator"];
  const mustHavesArr = toStrArr(mustHaves);
  if (mustHavesArr.some((v) => /key\s*money\s*or\s*ti\s*contribution/i.test(String(v)))) return true;
  const dealBreakers = si?.[BF.strategicIntent.top3DealBreakers] ?? dealFields?.[BF.strategicIntent.top3DealBreakers];
  const dealBreakersArr = toStrArr(dealBreakers);
  if (dealBreakersArr.some((v) => /no\s*key\s*money\s*\/\s*ti\s*support|no key money.*ti support/i.test(String(v)))) return true;
  return false;
}

// ---------- Factor calculators (sync; same logic as dashboard) ----------
function calcMKT1(dealFields, locationData, brandData) {
  const dealCountry = str(loc(locationData, BF.locationDeal.country, "country") || dealFields[BF.locationDeal.country]);
  const brandFit = brandData.brandFit || {};
  const brandBasics = brandData.brandBasics || {};
  const priorityMarkets = [];
  const priorityCols = ["Global - Priority Markets", "United States - Priority Markets", "Canada - Priority Markets", "Western Europe - Priority Markets", "United Kingdom - Priority Markets", "Other - Priority Markets"];
  for (const col of priorityCols) {
    if (brandFit[col] === true || brandFit[col] === "Yes") {
      const name = col.replace(" - Priority Markets", "").trim();
      if (name && !priorityMarkets.includes(name)) priorityMarkets.push(name);
    }
  }
  const priorityMulti = brandFit["Priority Markets"];
  if (Array.isArray(priorityMulti)) priorityMulti.forEach((item) => { const name = (typeof item === "string" ? item : (item && item.name) || "").trim(); if (name && !priorityMarkets.includes(name)) priorityMarkets.push(name); });
  if (priorityMarkets.some((m) => m.toLowerCase().includes("global"))) return 100;
  let marketsToAvoid = brandBasics[BF.brandBasics.marketsToAvoid] || [];
  if (!Array.isArray(marketsToAvoid)) marketsToAvoid = marketsToAvoid ? [marketsToAvoid] : [];
  const avoidMulti = brandFit["Markets to Avoid"];
  if (Array.isArray(avoidMulti)) avoidMulti.forEach((item) => { const name = (typeof item === "string" ? item : (item && item.name) || "").trim(); if (name && !marketsToAvoid.some((m) => str(m).toLowerCase() === name.toLowerCase())) marketsToAvoid.push(name); });
  const mapping = getCountryRegionMapping();
  const dealRegions = mapping[dealCountry] || { region1: "", region2: "", region3: "", region: "Global" };
  const dealRegionsList = [dealCountry, dealRegions.region1, dealRegions.region2, dealRegions.region3].filter((r) => r && str(r));
  const isHardFail = marketsToAvoid.some((market) => { const m = str(market).toLowerCase(); return dealRegionsList.some((region) => { const r = str(region).toLowerCase(); return m.includes(r) || r.includes(m); }); });
  if (isHardFail) return 0;
  if (priorityMarkets.length === 0) return null;
  let bestScore = 0;
  for (const market of priorityMarkets) {
    const ml = market.toLowerCase();
    if (dealCountry.toLowerCase().includes(ml) || ml.includes(dealCountry.toLowerCase())) bestScore = Math.max(bestScore, 100);
    if (dealRegions.region1 && (dealRegions.region1.toLowerCase().includes(ml) || ml.includes(dealRegions.region1.toLowerCase()))) bestScore = Math.max(bestScore, 90);
    if (dealRegions.region2 && (dealRegions.region2.toLowerCase().includes(ml) || ml.includes(dealRegions.region2.toLowerCase()))) bestScore = Math.max(bestScore, 80);
    if (dealRegions.region3 && (dealRegions.region3.toLowerCase().includes(ml) || ml.includes(dealRegions.region3.toLowerCase()))) bestScore = Math.max(bestScore, 80);
  }
  return bestScore > 0 ? bestScore : 25;
}

function calcMKT2(dealFields, locationData, brandData) {
  const dealCountry = loc(locationData, BF.locationDeal.country, "country") || dealFields[BF.locationDeal.country] || "";
  const brandRecognitionRaw = dealFields["Importance of Brand Recognition"];
  const brandRecognitionNeed = typeof brandRecognitionRaw === "number" ? brandRecognitionRaw : parseInt(brandRecognitionRaw, 10) || 0;
  if (brandRecognitionRaw === undefined || brandRecognitionRaw === null || brandRecognitionRaw === "") return null;
  const region = mapCountryToRegion(dealCountry);
  const openHotels = (brandData.brandFootprint || {})[`Number of Open Hotels (${region})`] || 0;
  if (brandRecognitionNeed >= 4) {
    const threshold = getRegionalThreshold(region);
    return openHotels >= threshold ? 100 : Math.max(20, 60 - (threshold - openHotels) * 5);
  }
  return openHotels > 0 ? 95 : 85;
}

function calcSEG1(dealFields, locationData, brandData) {
  const brandScale = str((brandData.brandBasics || {})[BF.brandBasics.hotelChainScale]);
  const dealScale = str(loc(locationData, BF.locationDeal.hotelChainScale, "hotelChainScale") || dealFields[BF.locationDeal.hotelChainScale]);
  if (!brandScale || !dealScale || brandScale.toLowerCase().includes("unknown") || dealScale.toLowerCase().includes("unknown")) return null;
  const brandTier = getChainScaleTier(brandScale);
  const dealTier = getChainScaleTier(dealScale);
  const diff = Math.abs(brandTier - dealTier);
  if (diff === 0) return 100;
  if (diff === 1) return 38;   // 1 tier apart: some points (configurable)
  return 0;                    // 2+ tiers apart: 0 points
}

/** Match Score New – Chain Scale Proximity (Weight 10%). Brand: Brand Setup - Brand Basics Hotel Chain Scale; Deal: Location & Property Hotel Chain Scale. Same scale = 100; 1 tier apart = 38; 2+ tiers = 0. */
function calcChainScaleProximity(dealFields, locationData, brandData) {
  return calcSEG1(dealFields, locationData, brandData);
}

/** Match Score New – Service Model Alignment (Weight 5%). Brand: Brand Setup - Brand Basics Hotel Service Model; Deal: Location & Property Hotel Service Model. Match = 100; hard mismatch = 0. */
function calcServiceModelAlignment(dealFields, locationData, brandData) {
  const brandSvc = str((brandData.brandBasics || {})[BF.brandBasics.hotelServiceModel]);
  const dealSvc = str(loc(locationData, BF.locationDeal.hotelServiceModel, "hotelServiceModel") || dealFields[BF.locationDeal.hotelServiceModel]);
  if (!brandSvc || !dealSvc || brandSvc.toLowerCase().includes("unknown") || dealSvc.toLowerCase().includes("unknown")) return null;
  return brandSvc.toLowerCase() === dealSvc.toLowerCase() ? 100 : 0;
}

/** Match Score New – Preferred Brand (Weight 10%). Brand: Brand Name (Brand Setup - Brand Basics); Deal: Preferred Brands (Strategic Intent). Brand in preferred list = 100; else 0. */
function calcPreferredBrand(dealFields, locationData, brandData, si) {
  return calcPREF1(dealFields, locationData, brandData, si);
}

/** Match Score New – Project Type Compatibility (Weight 10%). Brand: Acceptable Project Type (Brand Setup - Project Fit, Multi-select); Deal: Project Type (Deals, Single Select). Deal type accepted by brand = 100; else 28 (project-fit no-match tier). */
function calcProjectTypeCompatibility(dealFields, locationData, brandData) {
  const dealProjectType = str(dealFields[BF.locationDeal.projectType] || "");
  if (!dealProjectType) return null;
  const brandFit = brandData.brandFit || {};
  const raw = brandFit["Acceptable Project Type"];
  const brandList = Array.isArray(raw) ? raw : (typeof raw === "string" && raw.trim() ? [raw.trim()] : []);
  if (brandList.length === 0) return null;
  const normalized = (s) => String(s || "").trim().toLowerCase();
  const dealNorm = normalized(dealProjectType);
  const accepted = brandList.some((b) => normalized(b) === dealNorm);
  return accepted ? 100 : 28;
}

/** Match Score New – Building Type Compatibility (Weight 5%). Brand: Acceptable Building Types (Brand Setup - Project Fit, Multi-select); Deal: Building Type (Location & Property). Deal building type accepted by brand = 100; else 28 (project-fit no-match tier). */
function calcBuildingTypeCompatibility(dealFields, locationData, brandData) {
  const dealBuildingType = str(loc(locationData, BF.locationDeal.buildingType, "buildingType") || dealFields[BF.locationDeal.buildingType] || "");
  if (!dealBuildingType) return null;
  const brandFit = brandData.brandFit || {};
  const raw = brandFit["Acceptable Building Types"];
  const brandList = Array.isArray(raw) ? raw : (typeof raw === "string" && raw.trim() ? [raw.trim()] : []);
  if (brandList.length === 0) return null;
  const normalized = (s) => String(s || "").trim().toLowerCase();
  const dealNorm = normalized(dealBuildingType);
  const accepted = brandList.some((b) => normalized(b) === dealNorm);
  return accepted ? 100 : 28;
}

/** Match Score New – Project Stage Compatibility (Weight 5%). Deal: Stage of Development (Location & Property, Single Select); Brand: Acceptable Project Stages (Brand Setup - Project Fit, Multi-Select). Match → 100; no match → 28; unmapped stage → null. */
function calcProjectStageCompatibility(dealFields, locationData, brandData) {
  const stageRaw = str(dealFields[BF.locationDeal.stageOfDevelopment] || loc(locationData, BF.locationDeal.stageOfDevelopment, "stageOfDevelopment") || dealFields["Project Stage"] || "");
  if (!stageRaw) return null;
  const brandFit = brandData.brandFit || {};
  const raw = brandFit["Acceptable Project Stages"];
  const brandList = Array.isArray(raw) ? raw : (typeof raw === "string" && raw.trim() ? [raw.trim()] : []);
  if (brandList.length === 0) return null;
  const sl = stageRaw.toLowerCase();
  let dealCategory = null;
  if (sl.includes("land") || sl.includes("control")) dealCategory = "land";
  else if (sl.includes("entitlement") && !sl.includes("fully")) dealCategory = "entitlements";
  else if (sl.includes("entitled")) dealCategory = "entitled";
  else if (sl.includes("construction")) dealCategory = "construction";
  else if (sl.includes("stabilized") || sl.includes("operating")) dealCategory = "stabilized";
  if (!dealCategory) return null;
  const normalize = (s) => String(s || "").trim().toLowerCase();
  const accepted = brandList.some((v) => {
    const b = normalize(v);
    if (dealCategory === "land") return b.includes("land") || b.includes("control");
    if (dealCategory === "entitlements") return b.includes("entitlement") && !b.includes("fully");
    if (dealCategory === "entitled") return b.includes("fully") && b.includes("entitled");
    if (dealCategory === "construction") return b.includes("construction");
    if (dealCategory === "stabilized") return b.includes("stabilized") || b.includes("operating");
    return false;
  });
  return accepted ? 100 : 28;
}

/** Match Score New – Brand Standards Compatibility (Weight 10%). Deal: F&B Outlets?, Number of F&B Outlets, Number of Parking Spaces, Additional Amenities (multi-select); Brand: Additional Amenities (multi-select), F&B Outlets Required, Typical Number of F&B Outlets, Parking Required. For each brand Additional Amenities item, check if deal has it (keyword mapping + fallback direct match for all options). Start at 100; −12 per missing brand Additional Amenities item; −20 if brand requires F&B and deal has none; −12 if deal has fewer F&B outlets than brand typical; −10 if brand requires parking and deal has 0 spaces; final = max(0, 100 − penalties). */
function calcBrandStandardsCompatibility(dealFields, locationData, brandData) {
  const st = brandData.brandStandards || {};
  const brandRequired = toStrArr(st["Additional Amenities"]);
  if (brandRequired.length === 0) return null;
  const addAmen = toStrArr(dealFields["Additional Amenities"] || loc(locationData, "Additional Amenities", "additionalAmenities"));
  const addLower = addAmen.map((a) => a.toLowerCase());
  const hasInAdditional = (kw) => addLower.some((a) => a.includes(kw));
  const fboOutlets = parseNum(dealFields["Number of F&B Outlets"] || loc(locationData, "Number of F&B Outlets", "numberOfFbOutlets")) || 0;
  const fbYes = str(dealFields["F&B Outlets?"] || loc(locationData, "F&B Outlets?", "fbOutlets")).toLowerCase().includes("yes");
  const parkingSpaces = parseNum(dealFields["Number of Parking Spaces"] || loc(locationData, "Number of Parking Spaces", "numberOfParkingSpaces")) || 0;
  const amenities = {
    pool: hasInAdditional("pool"),
    lobby: hasInAdditional("lobby"),
    bar: hasInAdditional("bar") || hasInAdditional("beverage") || hasInAdditional("f&b"),
    coworking: hasInAdditional("coworking") || hasInAdditional("lounge"),
    businessCenter: hasInAdditional("business"),
    petAmenities: hasInAdditional("pet"),
    solarPower: hasInAdditional("solar"),
    meetingRooms: hasInAdditional("meeting") ? 1 : 0,
    hasFb: fbYes || fboOutlets > 0,
    hasParking: parkingSpaces > 0 || hasInAdditional("parking")
  };
  const dealHas = (low) => {
    const keywordMatch = (low.includes("pool") && amenities.pool) || (low.includes("lobby") && amenities.lobby) || (low.includes("coworking") && amenities.coworking) || (low.includes("bar") && amenities.bar) || (low.includes("business") && amenities.businessCenter) || (low.includes("pet") && amenities.petAmenities) || (low.includes("solar") && amenities.solarPower) || (low.includes("meeting") && amenities.meetingRooms > 0) || (low.includes("f&b") && amenities.hasFb) || (low.includes("parking") && amenities.hasParking);
    if (keywordMatch) return true;
    return addLower.some((d) => d.includes(low) || low.includes(d));
  };
  let penalty = 0;
  for (const item of brandRequired) {
    const low = str(item).toLowerCase().trim();
    if (!low) continue;
    if (!dealHas(low)) penalty += 12;
  }
  const fbRequired = str(st["F&B Outlets Required"] || "").toLowerCase().includes("yes");
  if (fbRequired && !amenities.hasFb) penalty += 20;
  const brandTypicalFb = parseNum(st["Typical Number of F&B Outlets"]);
  if (brandTypicalFb != null && brandTypicalFb > 0 && fboOutlets < brandTypicalFb) penalty += 12;
  const parkingRequired = str(st["Parking Required"] || "").toLowerCase().includes("yes");
  if (parkingRequired && !amenities.hasParking) penalty += 10;
  return Math.max(0, 100 - penalty);
}

/** Normalize deal/brand agreement-type strings for matching. Maps variants to a canonical key. */
const AGREEMENT_TYPE_ALIASES = {
  "franchise only": "franchise only",
  "franchise": "franchise only",
  "brand-managed": "brand-managed",
  "brand managed": "brand-managed",
  "brand-managed only": "brand-managed",
  "third-party management only": "third-party management only",
  "third party management only": "third-party management only",
  "3rd party only": "third-party management only",
  "management only": "third-party management only",
  "lease": "lease",
  "joint venture": "joint venture",
  "flexible/open": "flexible/open",
  "flexible": "flexible/open",
  "open": "flexible/open",
  "brand + third-party management (combined)": "brand + third-party",
  "brand + third-party mgmt. (combined)": "brand + third-party",
  "brands and third": "brand + third-party",
  "both": "brand + third-party"
};

function normalizeAgreementType(s) {
  const t = String(s || "").trim().toLowerCase();
  return AGREEMENT_TYPE_ALIASES[t] ?? t;
}

/** Match Score New – Agreements Type Compatibility (Weight 10%). Brand: Acceptable Agreements Type (Brand Setup - Project Fit, multi-select or boolean columns); Deal: Preferred Deal Structure (Market - Performance). Deal structure accepted by brand = 100; else 22. */
function calcAgreementsTypeCompatibility(dealFields, locationData, brandData, mp) {
  const dealStruct = strOrFirst(mp?.["Preferred Deal Structure"]);
  if (!dealStruct) return null;
  const brandFit = brandData.brandFit || {};
  let brandAccepted = [];
  const multiSelect = brandFit["Acceptable Agreements Type"];
  if (Array.isArray(multiSelect) && multiSelect.length > 0) {
    brandAccepted = multiSelect.map((v) => str(v)).filter(Boolean);
  } else if (typeof multiSelect === "string" && multiSelect.trim()) {
    brandAccepted = [multiSelect.trim()];
  } else {
    const boolCols = [
      "Franchise Only - Acceptable Agreements Type",
      "Third-Party Management Only - Acceptable Agreements Type",
      "Brand + Third-Party - Acceptable Agreements Type",
      "Brand-Managed - Acceptable Agreements Type",
      "Lease - Acceptable Agreements Type",
      "Joint Venture - Acceptable Agreements Type",
      "Flexible/Open - Acceptable Agreements Type"
    ];
    const formVals = ["Franchise Only", "Third-Party Management Only", "Brand + Third-Party Mgmt. (Combined)", "Brand-Managed Only", "Lease", "Joint Venture", "Flexible/Open"];
    for (let i = 0; i < boolCols.length; i++) {
      const v = brandFit[boolCols[i]];
      if (v === true || v === "Yes" || v === "Acceptable") brandAccepted.push(formVals[i]);
    }
  }
  if (brandAccepted.length === 0) return null;
  const dealNorm = normalizeAgreementType(dealStruct);
  const brandNorms = brandAccepted.map((b) => normalizeAgreementType(b));
  if (dealNorm === "flexible/open") return 100;
  if (brandNorms.includes("flexible/open")) return 100;
  if (dealNorm === "brand + third-party") {
    const dealAccepts = ["brand-managed", "third-party management only", "brand + third-party"];
    const accepted = dealAccepts.some((a) => brandNorms.includes(a));
    return accepted ? 100 : 25;
  }
  const accepted = brandNorms.includes(dealNorm);
  return accepted ? 100 : 25;
}

function parseNum(v) {
  if (v == null || v === "") return null;
  const n = typeof v === "number" ? v : parseInt(String(v).trim(), 10);
  return Number.isNaN(n) ? null : n;
}

/** Match Score New – Room Range Fit Compatibility (Weight 10%). Brand: Min/Max Room Count, Min/Max Ideal Project Size (Project Fit); Deal: Total Number of Rooms/Keys (Location & Property). Within ideal and room range = 100; outside ideal but within room range = 50; else 0. */
function calcRoomRangeFitCompatibility(dealFields, locationData, brandData) {
  const dealRooms = parseNum(loc(locationData, BF.locationDeal.totalRoomsKeys, "totalNumberOfRoomsKeys") ?? dealFields[BF.locationDeal.totalRoomsKeys]);
  if (dealRooms == null || dealRooms <= 0) return null;
  const pf = brandData.brandFit || {};
  const roomMin = parseNum(pf["Min - Room Count"]);
  const roomMax = parseNum(pf["Max - Room Count"]);
  const idealMin = parseNum(pf["Min - Ideal Project Size"]) ?? parseNum(pf["A Min - Ideal Project Size"]);
  const idealMax = parseNum(pf["Max - Ideal Project Size"]) ?? parseNum(pf["A Max - Ideal Project Size"]);
  const hasRoomRange = roomMin != null && roomMax != null;
  const hasIdealRange = idealMin != null && idealMax != null;
  if (!hasRoomRange) return null;
  const inRoomRange = dealRooms >= roomMin && dealRooms <= roomMax;
  if (!inRoomRange) return 0;
  if (!hasIdealRange) return 100;
  const inIdealRange = dealRooms >= idealMin && dealRooms <= idealMax;
  return inIdealRange ? 100 : 50;
}

/** Returns true if the incentive type value is Key Money–related (for display filtering in Incentives Match). */
function isKeyMoneyIncentiveType(v) {
  const s = (typeof v === "object" && v != null && "name" in v ? String(v.name || "") : String(v || "")).trim();
  return s.length > 0 && /key\s*money/i.test(s);
}

/** Normalize incentive type string for comparison (lowercase, collapse whitespace). */
function normIncentiveType(v) {
  return String(v || "").trim().toLowerCase().replace(/\s+/g, " ");
}

/** Match Score New – Fees Tolerance Compatibility (Weight 10%). Brand: Min/Max/Basis Royalty, Marketing, Loyalty (Brand Setup - Fee Structure); Deal: Royalty/Marketing/Loyalty Fee Expectations (Market - Performance). Basis-normalized: only compares when brand basis is percent-type. Deal within or above brand range = 100; deal below brand min = reduced score. Returns null if either side blank. */
function calcFeesToleranceCompatibility(dealFields, brandData, mp) {
  const mpData = mp || {};
  const dealRoyalty = str(mpData[BF.marketPerformance.royaltyFeeExpectations] || "");
  const dealMarketing = str(mpData[BF.marketPerformance.marketingFeeExpectations] || "");
  const dealLoyalty = str(mpData[BF.marketPerformance.loyaltyFeeExpectations] || "");
  const feeStruct = brandData?.brandFeeStructure || {};
  const dealRanges = { royalty: parseFeeRange(dealRoyalty), marketing: parseFeeRange(dealMarketing), loyalty: parseFeeRange(dealLoyalty) };
  const feeTypes = [
    { key: "royalty", minF: BF.brandFeeStructure.minRoyalty, maxF: BF.brandFeeStructure.maxRoyalty, basisF: BF.brandFeeStructure.basisRoyalty },
    { key: "marketing", minF: BF.brandFeeStructure.minMarketing, maxF: BF.brandFeeStructure.maxMarketing, basisF: BF.brandFeeStructure.basisMarketing },
    { key: "loyalty", minF: BF.brandFeeStructure.minLoyalty, maxF: BF.brandFeeStructure.maxLoyalty, basisF: BF.brandFeeStructure.basisLoyalty }
  ];
  let totalScore = 0, feeCount = 0;
  for (const ft of feeTypes) {
    const dealRange = dealRanges[ft.key];
    if (!dealRange) continue;
    const dealMidpoint = (dealRange.min + dealRange.max) / 2;
    let brandMin = feeStruct[ft.minF] ?? null;
    let brandMax = feeStruct[ft.maxF] ?? null;
    const brandBasisRaw = feeStruct[ft.basisF];
    const brandBasis = normalizeFeeBasis(brandBasisRaw);
    if (brandBasis === "per_room") continue;
    if (brandMin != null || brandMax != null) {
      brandMin = brandMin != null ? (typeof brandMin === "string" ? parseFloat(String(brandMin).replace("%", "")) : Number(brandMin)) : null;
      brandMax = brandMax != null ? (typeof brandMax === "string" ? parseFloat(String(brandMax).replace("%", "")) : Number(brandMax)) : null;
      if (brandMin != null && brandMax != null && brandMin < 1 && brandMax < 1 && brandMax > 0) { brandMin *= 100; brandMax *= 100; }
    }
    if (brandMin != null && brandMax != null && brandMax > 0) {
      feeCount++;
      if (dealMidpoint >= brandMin && dealMidpoint <= brandMax) totalScore += 100;
      else if (dealMidpoint > brandMax) {
        totalScore += 100;
      } else {
        const shortfall = brandMin > 0 ? ((brandMin - dealMidpoint) / brandMin) * 100 : 100;
        totalScore += shortfall <= 10 ? 75 : shortfall <= 25 ? 50 : shortfall <= 50 ? 25 : 0;
      }
    }
  }
  if (feeCount === 0) return null;
  return Math.round(totalScore / feeCount);
}

/** Match Score New – Incentives Match Compatibility (Weight 5%). Brand: Willing to Negotiate Incentives, Incentive Types (Operational Support); Deal: Incentive Types Interested In (Strategic Intent). If Willing=No and deal has any = 0; if Willing=Yes or Case-by-Case, each deal incentive type (excl. Key Money) that brand doesn't offer reduces score. */
function calcIncentivesMatchCompatibility(dealFields, brandData, si) {
  const op = brandData?.brandOperationalSupport || {};
  const willingRaw = str(op[BF.brandOperationalSupport.willingToNegotiate] || op[BF.brandOperationalSupport.willingToNegotiateAlt] || "");
  const willing = willingRaw.toLowerCase().trim();
  const isWilling = willing === "yes" || /case\s*[- ]?by\s*[- ]?case/i.test(willingRaw.trim());
  const dealInterestedRaw = (si || {})[BF.strategicIntent.incentiveTypesInterestedIn] ?? dealFields?.[BF.strategicIntent.incentiveTypesInterestedIn] ?? dealFields?.["Incentive Types Interested In"];
  const dealInterested = toStrArr(dealInterestedRaw).map((v) => String(v || "").trim()).filter(Boolean);
  const dealInterestedExclKeyMoney = dealInterested.filter((v) => !isKeyMoneyIncentiveType(v));
  if (dealInterestedExclKeyMoney.length === 0) return 100;
  if (!isWilling) return 0;
  const incRaw = op[INCENTIVE_TYPES_FIELD];
  const brandInc = Array.isArray(incRaw) ? incRaw.map((v) => String(v || "").trim()) : (incRaw && typeof incRaw === "string" ? incRaw.split(",").map((s) => s.trim()) : []);
  const brandNorm = new Set(brandInc.map(normIncentiveType));
  let matched = 0;
  for (const d of dealInterestedExclKeyMoney) {
    if (brandNorm.has(normIncentiveType(d))) matched++;
  }
  return Math.round(100 * (matched / dealInterestedExclKeyMoney.length));
}

/** Match Score New – Key Money Willingness Compatibility (Weight 10%). Brand: Incentive Types (Operational Support) includes "Key Money / Upfront Incentive"; Deal: filter=Yes, or Must-Haves includes "Key Money or TI Contribution", or Top 3 Deal Breakers includes "No Key Money / TI Support". Deal doesn't need = 100; need and brand offers = 100; need and brand doesn't offer = 0. If filter=Yes and brand doesn't offer, overall score = 0 (gate). */
function calcKeyMoneyWillingnessCompatibility(dealFields, brandData, si) {
  if (!dealWantsKeyMoney(dealFields, si || {})) return 100;
  if (brandOffersKeyMoney(brandData)) return 100;
  return 0;
}

function calcSVC1(dealFields, locationData, brandData) {
  const brandSvc = str((brandData.brandBasics || {})[BF.brandBasics.hotelServiceModel]);
  const dealSvc = str(loc(locationData, BF.locationDeal.hotelServiceModel, "hotelServiceModel") || dealFields[BF.locationDeal.hotelServiceModel]);
  const brandModel = str((brandData.brandBasics || {})["Brand Model / Format"] || "");
  if (!brandSvc || !dealSvc || brandSvc.toLowerCase().includes("unknown") || dealSvc.toLowerCase().includes("unknown")) return null;
  if (brandSvc.toLowerCase() === dealSvc.toLowerCase()) return 100;
  const flexible = brandModel.toLowerCase().includes("soft") || brandModel.toLowerCase().includes("conversion") || brandModel.toLowerCase().includes("collection");
  return flexible ? 28 : 0;   // Mismatch + brand flexible = 28 (configurable); hard mismatch = 0
}

function calcSIZE1(dealFields, locationData, brandData) {
  const dealRooms = parseInt(loc(locationData, BF.locationDeal.totalRoomsKeys, "totalNumberOfRoomsKeys"), 10) || parseInt(dealFields[BF.locationDeal.totalRoomsKeys], 10) || 0;
  const pf = brandData.brandFit || {};
  const minR = pf["Min - Room Count"] ?? pf["Min - Ideal Project Size"] ?? pf["A Min - Ideal Project Size"] ?? pf["Minimum Rooms"] ?? 0;
  const maxR = pf["Max - Room Count"] ?? pf["Max - Ideal Project Size"] ?? pf["A Max - Ideal Project Size"] ?? pf["Maximum Rooms"] ?? 0;
  const brandHasRange = (minR && maxR) || (minR && minR > 0) || (maxR && maxR > 0);
  if (dealRooms <= 0 && brandHasRange) return null;
  if (!minR || !maxR) {
    if (dealRooms >= 100 && dealRooms <= 300) return 100;
    if (dealRooms >= 50 && dealRooms < 100) return 78;
    if (dealRooms > 300 && dealRooms <= 500) return 68;
    return dealRooms > 0 ? 52 : null;
  }
  if (dealRooms >= minR && dealRooms <= maxR) return 100;
  const tolerance = 0.3;
  let penalty = 0;
  if (dealRooms < minR) penalty = Math.min((minR - dealRooms) / (minR * tolerance), 1) * 100;
  else if (dealRooms > maxR) penalty = Math.min((dealRooms - maxR) / (maxR * tolerance), 1) * 100;
  return Math.max(0, 100 - penalty);
}

function calcAMN1(dealFields, locationData, brandData) {
  const st = brandData.brandStandards || {};
  const required = st[BF.brandStandards.brandStandards] || "";
  const sustain = st[BF.brandStandards.sustainabilityFeatures];
  const compliance = st["Compliance & Safety"];
  const parkingReq = st["Parking Required"] || st["Onsite Parking"] || "";
  const amenities = {
    pool: dealFields["Pool"] || loc(locationData, "Pool", "pool") || false,
    lobby: dealFields["Lobby"] || loc(locationData, "Lobby", "lobby") || false,
    coworking: dealFields["Co-working or lounge space"] || loc(locationData, "Co-working or lounge space", "coworking") || false,
    bar: dealFields["Bar or Beverage Concept"] || loc(locationData, "Bar or Beverage Concept", "bar") || false,
    businessCenter: dealFields["Business Center"] || loc(locationData, "Business Center", "businessCenter") || false,
    petAmenities: dealFields["Pet Amenities"] || loc(locationData, "Pet Amenities", "petAmenities") || false,
    solarPower: dealFields["Solar Power"] || loc(locationData, "Solar Power", "solarPower") || false,
    meetingRooms: parseInt(dealFields["Number of Meeting Rooms"] || loc(locationData, "Number of Meeting Rooms", "numberOfMeetingRooms"), 10) || 0,
    fboOutlets: parseInt(dealFields["Number of F&B Outlets"] || loc(locationData, "Number of F&B Outlets", "numberOfFbOutlets"), 10) || 0,
    parkingSpaces: parseInt(dealFields["Number of Parking Spaces"] || loc(locationData, "Number of Parking Spaces", "numberOfParkingSpaces"), 10) || 0,
    sustainability: str(dealFields["Sustainability"] || loc(locationData, "Sustainability", "sustainability")).toLowerCase()
  };
  let score = 100;
  if (required && str(required)) {
    for (const item of required.split(";").filter((i) => i.trim())) {
      const low = item.toLowerCase().trim();
      let has = (low.includes("pool") && amenities.pool) || (low.includes("lobby") && amenities.lobby) || (low.includes("coworking") && amenities.coworking) || (low.includes("bar") && amenities.bar) || (low.includes("business") && amenities.businessCenter) || (low.includes("pet") && amenities.petAmenities) || (low.includes("solar") && amenities.solarPower) || (low.includes("meeting") && amenities.meetingRooms > 0) || (low.includes("f&b") && amenities.fboOutlets > 0) || (low.includes("parking") && amenities.parkingSpaces > 0);
      if (!has) score -= 12;
    }
  }
  const sustainList = Array.isArray(sustain) ? sustain : sustain && typeof sustain === "string" ? [sustain] : [];
  if (sustainList.length > 0 && !amenities.sustainability && !amenities.solarPower) score = Math.max(0, score - 15);
  const compList = Array.isArray(compliance) ? compliance : compliance && typeof compliance === "string" ? [compliance] : [];
  if (compList.length > 0 && !str(dealFields["Compliance"] || loc(locationData, "Compliance & Safety", "complianceSafety"))) score = Math.max(0, score - 10);
  if (parkingReq && str(parkingReq).toLowerCase().includes("yes") && amenities.parkingSpaces === 0) score = Math.max(0, score - 10);
  return Math.max(0, score);
}

function calcOWN1(dealFields, locationData, brandData, mp) {
  const brandFit = brandData.brandFit || {};
  const dealOwnership = str(mp?.[BF.marketPerformance.ownershipStructure] || "");
  const preferredOwners = str(brandFit[BF.brandFit.preferredOwnerType] || "");
  const dealInvolvement = str(mp?.["Owner Involvement Level"] || dealFields["Owner Involvement Level"] || "");
  const dealNonNegRaw = str(dealFields["Owner Non-Negotiables"] || dealFields["Must-Haves"] || mp?.["Owner Non-Negotiables"] || "");
  const dealNonNegList = dealNonNegRaw ? dealNonNegRaw.split(/[,;]/).map((s) => s.trim().toLowerCase()).filter(Boolean) : [];
  const involvementCols = ["Silent Investor - Owner Involvement", "High-level Oversight Only - Owner Involvement", "Hands-on in Operations - Owner Involvement", "Family in Key Staff Roles - Owner Involvement"];
  const nonNegCols = ["Key Vendors / Contracts - Owner Non-Negotiables", "Family Employees in Hotel Roles - Owner Non-Negotiables", "Specific Design / Branding Elements - Owner Non-Negotiables", "ADR / Positioning Philosophy - Owner Non-Negotiables", "Minimum Services / Amenities - Owner Non-Negotiables", "Other - Owner Non-Negotiables"];
  const brandInvolvement = involvementCols.filter((c) => brandFit[c] === true || brandFit[c] === "Yes" || brandFit[c] === "Acceptable");
  const brandNonNeg = nonNegCols.filter((c) => brandFit[c] === true || brandFit[c] === "Yes" || brandFit[c] === "Acceptable");
  let score = 50;
  let hasSignal = false;
  if (dealOwnership && preferredOwners && dealOwnership.toLowerCase() !== "unknown" && preferredOwners.toLowerCase() !== "unknown") {
    hasSignal = true;
    if (preferredOwners.toLowerCase().includes(dealOwnership.toLowerCase()) || dealOwnership.toLowerCase().includes(preferredOwners.toLowerCase())) score = 100;
    else {
      const related = getRelatedOwnerTypes();
      const dealL = dealOwnership.toLowerCase();
      const prefL = preferredOwners.toLowerCase();
      const hasRelated = Object.entries(related).some(([from, to]) => {
        const fromL = from.toLowerCase();
        const toL = to.toLowerCase();
        return (dealL.includes(fromL) && prefL.includes(toL)) || (dealL.includes(toL) && prefL.includes(fromL));
      });
      score = hasRelated ? 72 : 38;
    }
  }
  if (dealInvolvement && brandInvolvement.length > 0) {
    hasSignal = true;
    const dealInvL = dealInvolvement.toLowerCase();
    const accepts = brandInvolvement.some((col) => { const label = col.replace(/\s*-\s*Owner Involvement$/, "").toLowerCase(); return dealInvL.includes(label) || label.includes(dealInvL); });
    score = accepts ? Math.max(score, 92) : Math.min(score, 45);
  }
  if (dealNonNegList.length > 0 && brandNonNeg.length > 0) {
    hasSignal = true;
    const labels = brandNonNeg.map((c) => c.replace(/\s*-\s*Owner Non-Negotiables$/, "").toLowerCase());
    const conflicts = dealNonNegList.filter((d) => labels.some((b) => b.includes(d) || d.includes(b))).length;
    if (conflicts > 0) score = Math.min(score, 35);
  }
  if (!hasSignal) return null;
  return Math.min(100, Math.max(0, score));
}

function calcCAP1(dealFields, locationData, brandData, mp) {
  const brandFit = brandData.brandFit || {};
  const capitalCols = ["Equity and Debt Fully Committed - Capital & Risk", "Equity Committed, Debt in Process - Capital & Risk", "Equity in Process, Debt Not Started - Capital & Risk", "Both Equity and Debt Still Being Raised - Capital & Risk"];
  const brandAcceptable = capitalCols.filter((c) => brandFit[c] === true || brandFit[c] === "Yes" || brandFit[c] === "Acceptable");
  if (brandAcceptable.length === 0) return 50;
  const dealCapital = str(mp?.[BF.marketPerformance.capitalStatus] || mp?.[BF.marketPerformance.fundingStatus] || mp?.["Equity vs Debt Split"] || dealFields[BF.marketPerformance.capitalStatus] || dealFields[BF.marketPerformance.fundingStatus] || "").toLowerCase();
  if (!dealCapital) return null;
  const match = brandAcceptable.some((col) => {
    const label = col.replace(/\s*-\s*Capital\s*&\s*Risk$/, "").toLowerCase();
    return (dealCapital.includes("fully committed") && label.includes("fully committed")) || (dealCapital.includes("debt in process") && label.includes("debt in process")) || (dealCapital.includes("equity in process") && label.includes("equity in process")) || (dealCapital.includes("still being raised") && label.includes("still being raised")) || label.includes(dealCapital) || dealCapital.includes(label);
  });
  return match ? 100 : 28;
}

function calcTERM1(dealFields, locationData, brandData, mp) {
  const brandTerms = brandData.brandDealTerms || {};
  const dealTerm = str(dealFields[BF.marketPerformance.targetInitialTerm] || dealFields["Initial Term"] || mp?.[BF.marketPerformance.targetInitialTerm] || mp?.["Initial Term"] || "");
  const dealPerf = str(dealFields[BF.marketPerformance.performanceTestRequired] || mp?.[BF.marketPerformance.performanceTestRequired] || "").toLowerCase();
  const dealConv = str(dealFields[BF.marketPerformance.conversionTimeline] || mp?.[BF.marketPerformance.conversionTimeline] || "");
  const brandMinQty = brandTerms[BF.brandDealTerms.minInitialTermQty] ?? brandTerms[BF.brandDealTerms.minInitialTermQtyAlt];
  const brandPerf = str(brandTerms[BF.brandDealTerms.performanceTestRequirement] || "").toLowerCase();
  const brandConvMax = str(brandTerms[BF.brandDealTerms.conversionMax] || "");
  let signals = 0, matches = 0;
  if (dealTerm && (brandMinQty != null || brandTerms["Length - Typical Minimum Initial Term"])) {
    signals++;
    const dealY = parseFloat(dealTerm) || 0;
    const brandY = typeof brandMinQty === "number" ? brandMinQty : parseFloat(brandMinQty);
    if (isNaN(brandY)) matches++; else if (dealY >= brandY * 0.9) matches++;
  }
  if (dealPerf && (dealPerf === "yes" || dealPerf === "no")) {
    signals++;
    if (!brandPerf) matches++; else if ((dealPerf === "yes" && (brandPerf.includes("yes") || brandPerf.includes("required"))) || (dealPerf === "no" && (brandPerf.includes("no") || !brandPerf.includes("required")))) matches++;
  }
  if (dealConv && brandConvMax) {
    signals++;
    const dealM = parseFloat(dealConv) || 0;
    const brandM = parseFloat(brandConvMax) || 0;
    if (isNaN(brandM)) matches++; else if (dealM <= brandM * 1.2) matches++;
  }
  if (signals === 0) return null;
  return matches === signals ? 100 : Math.round(40 + (matches / signals) * 50);
}

function calcSTR1(dealFields, locationData, brandData, si) {
  const brandSoft = str((brandData.brandFit || {})[BF.brandFit.softCollectionBrand] || "");
  const dealPref = str(si?.[BF.strategicIntent.softVsHardPreference] || "");
  if (!dealPref || dealPref.toLowerCase() === "unknown") return null;
  const isBrandSoft = brandSoft.toLowerCase() === "yes";
  const isBrandHard = brandSoft.toLowerCase() === "no";
  const dp = dealPref.toLowerCase();
  const isDealSoft = dp.includes("soft brand");
  const isDealHard = dp.includes("hard brand");
  const isDealBoth = dp.includes("open to both") || dp.includes("unsure");
  if (isDealBoth) return 100;
  if ((isBrandSoft && isDealSoft) || (isBrandHard && isDealHard)) return 100;
  if ((isBrandSoft && isDealHard) || (isBrandHard && isDealSoft)) return 15;
  return 50;
}

function calcPREF1(dealFields, locationData, brandData, si) {
  const brandName = str((brandData.brandBasics || {})[BF.brandBasics.brandName] || "");
  const preferred = si?.[BF.strategicIntent.preferredBrands] || "";
  if (!preferred) return 0;
  let list = Array.isArray(preferred) ? preferred.map((b) => str(b).toLowerCase()) : str(preferred).split(",").map((b) => b.trim().toLowerCase()).filter(Boolean);
  const match = list.some((p) => p.includes(brandName.toLowerCase()) || brandName.toLowerCase().includes(p));
  return match ? 100 : 0;
}

function calcFIN1(dealFields, locationData, brandData, mp) {
  const dealRoyalty = mp?.[BF.marketPerformance.royaltyFeeExpectations] || "";
  const dealMarketing = mp?.[BF.marketPerformance.marketingFeeExpectations] || "";
  const dealLoyalty = mp?.[BF.marketPerformance.loyaltyFeeExpectations] || "";
  const feeStruct = brandData.brandFeeStructure || {};
  const dealFees = { royalty: parseSingleFee(dealRoyalty), marketing: parseSingleFee(dealMarketing), loyalty: parseSingleFee(dealLoyalty) };
  const feeTypes = [
    { key: "royalty", minF: BF.brandFeeStructure.minRoyalty, maxF: BF.brandFeeStructure.maxRoyalty },
    { key: "marketing", minF: BF.brandFeeStructure.minMarketing, maxF: BF.brandFeeStructure.maxMarketing },
    { key: "loyalty", minF: BF.brandFeeStructure.minLoyalty, maxF: BF.brandFeeStructure.maxLoyalty }
  ];
  let totalScore = 0, feeCount = 0;
  for (const ft of feeTypes) {
    const dealFee = dealFees[ft.key];
    let brandMin = feeStruct[ft.minF] ?? 0;
    let brandMax = feeStruct[ft.maxF] ?? 0;
    if (typeof brandMin === "string") brandMin = parseFloat(brandMin.replace("%", ""));
    if (typeof brandMax === "string") brandMax = parseFloat(brandMax.replace("%", ""));
    if (brandMin < 1 && brandMax < 1 && brandMax > 0) { brandMin *= 100; brandMax *= 100; }
    if (dealFee !== null && brandMin !== undefined && brandMax !== undefined && brandMax > 0) {
      feeCount++;
      if (dealFee >= brandMin && dealFee <= brandMax) totalScore += 100;
      else if (dealFee > brandMax) {
        const excess = ((dealFee - brandMax) / brandMax) * 100;
        totalScore += excess <= 10 ? 85 : excess <= 25 ? 70 : excess <= 50 ? 50 : 25;
      } else {
        const shortfall = ((brandMin - dealFee) / brandMin) * 100;
        totalScore += shortfall <= 10 ? 75 : shortfall <= 25 ? 50 : shortfall <= 50 ? 25 : 0;
      }
    }
  }
  return feeCount > 0 ? Math.round(totalScore / feeCount) : 75;
}

/** Key Money Willingness Fit (5%): Brand Setup - Operational Support Incentive Types "Key Money / Upfront Incentive" vs deal (Contact & Uploads filter, Must-Haves, Top 3 Deal Breakers). Deal doesn't need = 100; need and brand offers = 100; need and brand doesn't offer = 0. */
function calcKEY1(dealFields, locationData, brandData, mp, si) {
  const siData = si || {};
  if (!dealWantsKeyMoney(dealFields, siData)) return 100;
  if (brandOffersKeyMoney(brandData)) return 100;
  return 0;
}

function calcINC1(dealFields, locationData, brandData, mp, si) {
  const op = brandData.brandOperationalSupport || {};
  if (op[BF.brandOperationalSupport.willingToNegotiate] !== "Yes" && op[BF.brandOperationalSupport.willingToNegotiateAlt] !== "Yes") return 40;
  const brandIncentives = op;
  const dealIncentives = mp || {};
  const fields = ["Lower Initial Fees", "Tiered Fee Structure", "Temporary Royalty Discounts", "Performance-Based Royalties", "Key Money", "Key Money / Upfront Incentive"];
  let matches = 0, totalDeal = 0;
  for (const field of fields) {
    const brandOffers = brandIncentives[field] === true || brandIncentives[field] === "Yes";
    const dealSeeks = dealIncentives[field] === true || dealIncentives[field] === "Yes";
    if (dealSeeks) { totalDeal++; if (brandOffers) matches++; }
  }
  if (totalDeal === 0) return 80;
  return Math.min(100, Math.round(80 + (matches / totalDeal) * 20));
}

function calcPROJ1(dealFields, locationData, brandData) {
  const projectTypeRaw = str(dealFields[BF.locationDeal.projectType] || loc(locationData, BF.locationDeal.projectType, "projectType") || "Unknown");
  const brandFit = brandData.brandFit || {};
  const cols = ["New Build - Acceptable Project Type", "Conversion - Reflag - Acceptable Project Type", "Renovation / Repositioning - Acceptable Project Type", "Expansion / Add-on - Acceptable Project Type"];
  const pl = projectTypeRaw.toLowerCase();
  let criteria = null;
  if (pl.includes("new build") || pl.includes("new construction")) criteria = cols[0];
  else if (pl.includes("conversion") || pl.includes("reflag")) criteria = cols[1];
  else if (pl.includes("renovation") || pl.includes("repositioning")) criteria = cols[2];
  else if (pl.includes("expansion") || pl.includes("add-on")) criteria = cols[3];
  if (!criteria) return null;
  const ok = brandFit[criteria] === true || brandFit[criteria] === "Yes" || brandFit[criteria] === "Acceptable";
  return ok ? 100 : 28;
}

function calcPROJ2(dealFields, locationData, brandData) {
  const buildingRaw = str(loc(locationData, BF.locationDeal.buildingType, "buildingType") || dealFields[BF.locationDeal.buildingType] || "Unknown");
  const brandFit = brandData.brandFit || {};
  const map = { "High-Rise": "High-Rise - Acceptable Building Type", "Mid-Rise": "Mid-Rise - Acceptable Building Type", "Low-Rise": "Low-Rise - Acceptable Building Type", "Mixed-Use": "Mixed-Use - Acceptable Building Type", "Historic / Renovated": "Historic / Renovated - Acceptable Building Type", "Podium / Tower": "Podium / Tower - Acceptable Building Type", "Resort-Style Compound": "Resort-Style Compound - Acceptable Building Type" };
  let criteria = map[buildingRaw];
  if (!criteria) {
    const bl = buildingRaw.toLowerCase();
    for (const [label, col] of Object.entries(map)) {
      if (bl.includes(label.toLowerCase().split(" ")[0]) || bl.includes(col.split(" - ")[0].toLowerCase())) { criteria = col; break; }
    }
  }
  if (!criteria) return null;
  const ok = brandFit[criteria] === true || brandFit[criteria] === "Yes" || brandFit[criteria] === "Acceptable";
  return ok ? 100 : 28;
}

function calcPROJ3(dealFields, locationData, brandData) {
  const stageRaw = str(dealFields[BF.locationDeal.stageOfDevelopment] || loc(locationData, BF.locationDeal.stageOfDevelopment, "stageOfDevelopment") || dealFields["Project Stage"] || "");
  const brandFit = brandData.brandFit || {};
  const cols = ["Land Under Control Only - Acceptable Project Stages", "Entitlements in Process - Acceptable Project Stages", "Fully Entitled - Acceptable Project Stages", "Under Construction - Acceptable Project Stages", "Stabilized Operating Asset - Acceptable Project Stages"];
  const sl = stageRaw.toLowerCase();
  let col = null;
  if (sl.includes("land") || sl.includes("control")) col = cols[0];
  else if (sl.includes("entitlement")) col = cols[1];
  else if (sl.includes("entitled")) col = cols[2];
  else if (sl.includes("construction")) col = cols[3];
  else if (sl.includes("stabilized") || sl.includes("operating")) col = cols[4];
  if (!col) return null;
  const ok = brandFit[col] === true || brandFit[col] === "Yes" || brandFit[col] === "Acceptable";
  return ok ? 100 : 28;
}

function calcAGMT1(dealFields, locationData, brandData) {
  const dealStruct = str(dealFields[BF.locationDeal.preferredDealStructure] || dealFields["Who should receive bids for this project?"] || loc(locationData, BF.locationDeal.preferredDealStructure, "preferredDealStructure") || "");
  const brandFit = brandData.brandFit || {};
  if (!dealStruct) return null;
  const franchiseCol = "Franchise Only - Acceptable Agreements Type";
  const thirdPartyCol = "Third-Party Management Only - Acceptable Agreements Type";
  const bothCol = "Brand + Third-Party - Acceptable Agreements Type";
  const brandFranchise = brandFit[franchiseCol] === true || brandFit[franchiseCol] === "Yes" || brandFit[franchiseCol] === "Acceptable";
  const brandThird = brandFit[thirdPartyCol] === true || brandFit[thirdPartyCol] === "Yes" || brandFit[thirdPartyCol] === "Acceptable";
  const brandBoth = brandFit[bothCol] === true || brandFit[bothCol] === "Yes" || brandFit[bothCol] === "Acceptable";
  const ds = dealStruct.toLowerCase();
  if (ds.includes("both") || ds.includes("brands and third")) {
    if (brandBoth || (brandFranchise && brandThird)) return 100;
    if (brandFranchise || brandThird) return 62;
    return 25;
  }
  const agreementMap = { "franchise only": franchiseCol, franchise: franchiseCol, "3rd party only": thirdPartyCol, "third-party": thirdPartyCol, "management only": thirdPartyCol, "brand-managed": "Brand-Managed - Acceptable Agreements Type", lease: "Lease - Acceptable Agreements Type", flexible: "Flexible/Open - Acceptable Agreements Type" };
  let criteria = null;
  for (const [key, c] of Object.entries(agreementMap)) {
    if (ds.includes(key)) { criteria = c; break; }
  }
  if (!criteria) return null;
  const ok = brandFit[criteria] === true || brandFit[criteria] === "Yes" || brandFit[criteria] === "Acceptable";
  return ok ? 100 : 25;
}

function calcESG1(dealFields, locationData, brandData) {
  const dealEsg = str(dealFields[BF.locationDeal.sustainability] || dealFields["ESG"] || loc(locationData, BF.locationDeal.sustainability, "sustainability") || loc(locationData, "ESG Commitment", "esgCommitment") || "");
  const brandFit = brandData.brandFit || {};
  const brandSt = brandData.brandStandards || {};
  const brandEsgExp = brandFit[BF.brandFit.esgExpectations] || "";
  const brandSustain = brandSt[BF.brandStandards.sustainabilityFeatures];
  const brandHasEsg = !!(str(brandEsgExp)) || (Array.isArray(brandSustain) && brandSustain.length > 0) || (typeof brandSustain === "string" && brandSustain.trim());
  if (!dealEsg && !brandHasEsg) return 100;
  if (!dealEsg) return null;
  if (!brandHasEsg) return 72;
  const dealL = dealEsg.toLowerCase();
  const expL = (brandEsgExp + "").toLowerCase();
  if (expL && (dealL.includes("yes") || dealL.includes("commitment") || dealL.includes("sustainable") || dealL.includes("esg"))) return 100;
  return 60;
}

/** Build per-factor comparison details (brand value vs deal value + short note) for the modal. Uses same BF fields as calc* so breakdown and score stay aligned. */
function getBreakdownDetails(dealFields, locationData, brandData, mp, si) {
  const dealCountry = str(loc(locationData, BF.locationDeal.country, "country") || dealFields[BF.locationDeal.country] || "");
  const brandFit = brandData.brandFit || {};
  const brandBasics = brandData.brandBasics || {};
  const brandFootprint = brandData.brandFootprint || {};
  const brandStandards = brandData.brandStandards || {};
  const brandFee = brandData.brandFeeStructure || {};
  const brandTerms = brandData.brandDealTerms || {};
  const brandOp = brandData.brandOperationalSupport || {};
  const details = {};

  const priorityCols = ["Global - Priority Markets", "United States - Priority Markets", "Canada - Priority Markets", "Western Europe - Priority Markets", "United Kingdom - Priority Markets", "Other - Priority Markets"];
  const priorityMarkets = [];
  for (const col of priorityCols) {
    if (brandFit[col] === true || brandFit[col] === "Yes") priorityMarkets.push(col.replace(" - Priority Markets", "").trim());
  }
  const marketsToAvoid = brandBasics[BF.brandBasics.marketsToAvoid];
  const avoidStr = Array.isArray(marketsToAvoid) ? marketsToAvoid.join(", ") : str(marketsToAvoid);
  details.MKT1 = {
    brandValue: "Priority: " + (priorityMarkets.length ? priorityMarkets.join(", ") : "—") + "; Avoid: " + (avoidStr || "—"),
    dealValue: "Country: " + (dealCountry || "—"),
    note: "Deal location checked against brand priority markets and markets to avoid."
  };

  const region = mapCountryToRegion(dealCountry);
  const openHotels = brandFootprint[`Number of Open Hotels (${region})`] ?? "—";
  const recNeed = dealFields["Importance of Brand Recognition"];
  details.MKT2 = {
    brandValue: `Open hotels in ${region}: ${openHotels}`,
    dealValue: "Importance of Brand Recognition: " + (recNeed != null ? recNeed : "—"),
    note: "Brand presence in deal region vs deal’s need for recognition."
  };

  const brandScale = str(brandBasics[BF.brandBasics.hotelChainScale] || "");
  const dealScale = str(loc(locationData, BF.locationDeal.hotelChainScale, "hotelChainScale") || dealFields[BF.locationDeal.hotelChainScale] || "");
  details.SEG1 = {
    brandValue: "Hotel Chain Scale: " + (brandScale || "—"),
    dealValue: "Hotel Chain Scale: " + (dealScale || "—"),
    note: "Same scale = 100; 1 tier apart = 38; 2+ tiers = 0. Chain scale is a strong differentiator."
  };

  const brandSvc = str(brandBasics[BF.brandBasics.hotelServiceModel] || "");
  const dealSvc = str(loc(locationData, BF.locationDeal.hotelServiceModel, "hotelServiceModel") || dealFields[BF.locationDeal.hotelServiceModel] || "");
  details.SVC1 = {
    brandValue: "Hotel Service Model: " + (brandSvc || "—"),
    dealValue: "Hotel Service Model: " + (dealSvc || "—"),
    note: "Match = 100; mismatch with flexible brand = 28; hard mismatch = 0. See SERVICE_MODEL_SCORING.md for options."
  };

  const pf = brandFit;
  const minR = pf["Min - Room Count"] ?? pf["Min - Ideal Project Size"] ?? pf["Minimum Rooms"] ?? "—";
  const maxR = pf["Max - Room Count"] ?? pf["Max - Ideal Project Size"] ?? pf["Maximum Rooms"] ?? "—";
  const dealRooms = loc(locationData, BF.locationDeal.totalRoomsKeys, "totalNumberOfRoomsKeys") ?? dealFields[BF.locationDeal.totalRoomsKeys] ?? "—";
  details.SIZE1 = {
    brandValue: "Ideal room range: " + minR + " – " + maxR,
    dealValue: "Total Number of Rooms/Keys: " + dealRooms,
    note: "Deal within range = 100; outside range gets penalty."
  };

  details.OWN1 = {
    brandValue: "Preferred Owner/Investor Type: " + str(brandFit[BF.brandFit.preferredOwnerType] || "—"),
    dealValue: "Ownership Structure: " + str(mp?.[BF.marketPerformance.ownershipStructure] || "—"),
    note: "Match or related type = higher score; conflict on non‑negotiables = lower."
  };

  const brandSoft = str((brandFit[BF.brandFit.softCollectionBrand] || ""));
  const dealPref = str(si?.[BF.strategicIntent.softVsHardPreference] || "—");
  details.STR1 = {
    brandValue: "Soft/Collection Brand: " + (brandSoft || "—"),
    dealValue: "Soft vs Hard Brand Preference: " + dealPref,
    note: "Both soft or both hard = 100; opposite = 15; deal open to both = 100."
  };

  details.AMN1 = {
    brandValue: "Brand Standards / required: " + str(brandStandards[BF.brandStandards.brandStandards] || "—").slice(0, 80) + (str(brandStandards[BF.brandStandards.brandStandards] || "").length > 80 ? "…" : ""),
    dealValue: "Deal amenities (pool, lobby, F&B, etc.) from Location & Property.",
    note: "Each missing required item reduces score; sustainability/compliance checked."
  };

  const dealRoyalty = mp?.[BF.marketPerformance.royaltyFeeExpectations] ?? "—";
  const dealMarketing = mp?.[BF.marketPerformance.marketingFeeExpectations] ?? "—";
  const dealLoyalty = mp?.[BF.marketPerformance.loyaltyFeeExpectations] ?? "—";
  const royMin = brandFee[BF.brandFeeStructure.minRoyalty] ?? "—";
  const royMax = brandFee[BF.brandFeeStructure.maxRoyalty] ?? "—";
  details.FIN1 = {
    brandValue: "Royalty: " + royMin + "–" + royMax + "%; Marketing/Loyalty from Fee Structure.",
    dealValue: "Royalty: " + str(dealRoyalty) + "; Marketing: " + str(dealMarketing) + "; Loyalty: " + str(dealLoyalty),
    note: "Deal within brand range = 100; above/below = reduced score."
  };

  details.INC1 = {
    brandValue: "Willing to Negotiate Incentives: " + str(brandOp[BF.brandOperationalSupport.willingToNegotiate] || brandOp[BF.brandOperationalSupport.willingToNegotiateAlt] || "—") + "; types offered in Operational Support.",
    dealValue: "Incentive fields from Market Performance (Key Money, tiered fees, etc.).",
    note: "Deal‑sought incentives matched to brand‑offered = higher score."
  };

  details.PREF1 = {
    brandValue: "Brand Name: " + str(brandBasics[BF.brandBasics.brandName] || "—"),
    dealValue: "Preferred Brands (Strategic Intent): " + (Array.isArray(si?.[BF.strategicIntent.preferredBrands]) ? si[BF.strategicIntent.preferredBrands].map((b) => (typeof b === "string" ? b : b?.name)).join(", ") : str(si?.[BF.strategicIntent.preferredBrands] || "—")),
    note: "Brand in preferred list = 100; else 0."
  };

  const filterVal = getKeyMoneyFilterValue(dealFields);
  const mustHavesRaw = si?.[BF.strategicIntent.mustHavesFromBrand] ?? dealFields?.[BF.strategicIntent.mustHavesFromBrand] ?? dealFields?.["Must-haves From Brand or Operator"];
  const mustHavesArr = toStrArr(mustHavesRaw);
  const dealBreakersRaw = si?.[BF.strategicIntent.top3DealBreakers] ?? dealFields?.[BF.strategicIntent.top3DealBreakers];
  const dealBreakersArr = toStrArr(dealBreakersRaw);
  const incentiveTypesRaw = (brandData.brandOperationalSupport || {})[BF.brandOperationalSupport.incentiveTypes];
  const incentiveTypesArr = Array.isArray(incentiveTypesRaw) ? incentiveTypesRaw : (typeof incentiveTypesRaw === "string" && incentiveTypesRaw.trim() ? incentiveTypesRaw.split(",").map((s) => s.trim()) : []);
  details.KEY1 = {
    brandValue: "Incentive Types (Brand Setup - Operational Support): " + (incentiveTypesArr.length ? incentiveTypesArr.join(", ") : "—"),
    dealValue: "Filter out brands without key money? (Contact & Uploads): " + (filterVal || "—") + "; Must-Haves From Brand/Operator (Strategic Intent): " + (mustHavesArr.length ? mustHavesArr.join(", ") : "—") + "; Top 3 Deal Breakers (Strategic Intent): " + (dealBreakersArr.length ? dealBreakersArr.join(", ") : "—"),
    note: "Brand offers key money if Incentive Types includes \"Key Money / Upfront Incentive\". Deal wants key money if filter = Yes, or Must-Haves includes \"Key Money or TI Contribution\", or Top 3 Deal Breakers includes \"No Key Money / TI Support\". If filter = Yes and brand does not offer, overall score = 0."
  };

  const capitalCols = ["Equity and Debt Fully Committed - Capital & Risk", "Equity Committed, Debt in Process - Capital & Risk", "Equity in Process, Debt Not Started - Capital & Risk", "Both Equity and Debt Still Being Raised - Capital & Risk"];
  const acceptedCapital = capitalCols.filter((c) => brandFit[c] === true || brandFit[c] === "Yes" || brandFit[c] === "Acceptable").map((c) => c.replace(" - Capital & Risk", "").trim());
  details.CAP1 = {
    brandValue: "Accepts capital stages: " + (acceptedCapital.length ? acceptedCapital.join("; ") : "—"),
    dealValue: "Capital Status: " + str(mp?.[BF.marketPerformance.capitalStatus] || mp?.[BF.marketPerformance.fundingStatus] || "—"),
    note: "Deal capital stage matches an accepted brand option = 100; else 28."
  };

  details.TERM1 = {
    brandValue: "Min initial term: " + (brandTerms[BF.brandDealTerms.minInitialTermQty] ?? brandTerms[BF.brandDealTerms.minInitialTermQtyAlt] ?? "—") + "; Performance test: " + str(brandTerms[BF.brandDealTerms.performanceTestRequirement] || "—") + "; Conversion max: " + str(brandTerms[BF.brandDealTerms.conversionMax] || "—"),
    dealValue: "Target Initial Term: " + str(dealFields[BF.marketPerformance.targetInitialTerm] || mp?.[BF.marketPerformance.targetInitialTerm] || "—") + "; Performance Test: " + str(mp?.[BF.marketPerformance.performanceTestRequired] || "—") + "; Conversion Timeline: " + str(dealFields[BF.marketPerformance.conversionTimeline] || mp?.[BF.marketPerformance.conversionTimeline] || "—"),
    note: "Term, performance test, and conversion timeline aligned = 100."
  };

  const projectTypeRaw = str(dealFields[BF.locationDeal.projectType] || loc(locationData, BF.locationDeal.projectType, "projectType") || "—");
  details.PROJ1 = {
    brandValue: "Acceptable Project Types: New Build, Conversion, Renovation, Expansion (from Project Fit).",
    dealValue: "Project Type: " + projectTypeRaw,
    note: "Deal type accepted by brand = 100; else 22."
  };

  const buildingRaw = str(loc(locationData, BF.locationDeal.buildingType, "buildingType") || dealFields[BF.locationDeal.buildingType] || "—");
  details.PROJ2 = {
    brandValue: "Acceptable Building Types: High-Rise, Mid-Rise, Low-Rise, Mixed-Use, etc. (from Project Fit).",
    dealValue: "Building Type: " + buildingRaw,
    note: "Deal building type accepted by brand = 100; else 22."
  };

  const stageRaw = str(dealFields[BF.locationDeal.stageOfDevelopment] || loc(locationData, BF.locationDeal.stageOfDevelopment, "stageOfDevelopment") || dealFields["Project Stage"] || "—");
  details.PROJ3 = {
    brandValue: "Acceptable Project Stages: Land, Entitlements, Entitled, Under Construction, Stabilized (from Project Fit).",
    dealValue: "Stage of Development: " + stageRaw,
    note: "Deal stage accepted by brand = 100; else 28."
  };

  const dealStruct = str(dealFields[BF.locationDeal.preferredDealStructure] || dealFields["Who should receive bids for this project?"] || loc(locationData, BF.locationDeal.preferredDealStructure, "preferredDealStructure") || "—");
  details.AGMT1 = {
    brandValue: "Acceptable Agreements: Franchise Only, Third-Party Only, Brand+Third-Party, Lease, etc. (from Project Fit).",
    dealValue: "Preferred Deal Structure: " + dealStruct,
    note: "Deal structure accepted by brand = 100; else 18–22."
  };

  const dealEsg = str(dealFields[BF.locationDeal.sustainability] || dealFields["ESG"] || loc(locationData, BF.locationDeal.sustainability, "sustainability") || loc(locationData, "ESG Commitment", "esgCommitment") || "—");
  details.ESG1 = {
    brandValue: "ESG/Sustainability expectations: " + str(brandFit[BF.brandFit.esgExpectations] || "—") + "; Standards: " + (brandStandards[BF.brandStandards.sustainabilityFeatures] ? (Array.isArray(brandStandards[BF.brandStandards.sustainabilityFeatures]) ? brandStandards[BF.brandStandards.sustainabilityFeatures].join(", ") : str(brandStandards[BF.brandStandards.sustainabilityFeatures])) : "—"),
    dealValue: "Sustainability / ESG: " + dealEsg,
    note: "Both aligned or deal has commitment = 100; brand has ESG and deal doesn’t = 72."
  };

  details.BRND1 = {
    brandValue: "Deterministic 0–10 spread from brand name (for differentiation between brands on same deal).",
    dealValue: "—",
    note: "Not compared to deal; added to final score so each brand gets a distinct score when other factors are equal."
  };

  return details;
}

/**
 * Compute 19-factor match score for one (deal, brand). Same weights and logic as Brand Development Dashboard.
 * @param {object} dealFields - Deal record fields (Airtable)
 * @param {object} locationData - Location & Property (raw or normalized from my-deals)
 * @param {object} marketPerformanceData - Market - Performance record fields (or null)
 * @param {object} strategicIntentData - Strategic Intent record fields (or null)
 * @param {string} brandName - Preferred brand name
 * @param {string} baseId - Airtable base ID
 * @param {string} apiKey - Airtable API key
 * @returns {Promise<{ score: number, breakdown: object }>}
 */
/** Deal-only factors that don't need brand data (return null when brand is empty). Used when fetchBrandData fails. */
function computeDealOnlyBaseScore(dealFields, locationData, mp, si) {
  const emptyBrand = { brandBasics: {}, brandFit: {}, brandFootprint: {}, brandStandards: {}, brandFeeStructure: {}, brandOperationalSupport: {}, brandDealTerms: {} };
  const breakdown = {
    MKT1: calcMKT1(dealFields, locationData, emptyBrand),
    MKT2: calcMKT2(dealFields, locationData, emptyBrand),
    SEG1: calcSEG1(dealFields, locationData, emptyBrand),
    SVC1: calcSVC1(dealFields, locationData, emptyBrand),
    SIZE1: calcSIZE1(dealFields, locationData, emptyBrand),
    KEY1: calcKEY1(dealFields, locationData, emptyBrand, mp, si),
    PROJ1: calcPROJ1(dealFields, locationData, emptyBrand),
    PROJ2: calcPROJ2(dealFields, locationData, emptyBrand),
    PROJ3: calcPROJ3(dealFields, locationData, emptyBrand),
    ESG1: calcESG1(dealFields, locationData, emptyBrand)
  };
  let weightedSum = 0, totalWeight = 0;
  for (const [key, score] of Object.entries(breakdown)) {
    if (score !== null && score !== undefined && WEIGHTS[key]) {
      weightedSum += score * WEIGHTS[key];
      totalWeight += WEIGHTS[key];
    }
  }
  return totalWeight > 0 ? weightedSum / totalWeight : 50;
}

/**
 * Compute Match Score New for one (deal, brand). Built factor by factor; each factor uses same sources as breakdown.
 * @returns {{ total: number, factors: object }} total 0–100 (weighted sum of factor scores); factors keyed by factor name.
 */
function computeMatchScoreNew(dealFields, locationData, brandData, si, mp) {
  const factors = {
    chainScaleProximity: calcChainScaleProximity(dealFields, locationData, brandData),
    serviceModelAlignment: calcServiceModelAlignment(dealFields, locationData, brandData),
    preferredBrand: calcPreferredBrand(dealFields, locationData, brandData, si || {}),
    projectTypeCompatibility: calcProjectTypeCompatibility(dealFields, locationData, brandData),
    buildingTypeCompatibility: calcBuildingTypeCompatibility(dealFields, locationData, brandData),
    projectStageCompatibility: calcProjectStageCompatibility(dealFields, locationData, brandData),
    brandStandardsCompatibility: calcBrandStandardsCompatibility(dealFields, locationData, brandData),
    agreementsTypeCompatibility: calcAgreementsTypeCompatibility(dealFields, locationData, brandData, mp || {}),
    roomRangeFitCompatibility: calcRoomRangeFitCompatibility(dealFields, locationData, brandData),
    keyMoneyWillingnessCompatibility: calcKeyMoneyWillingnessCompatibility(dealFields, brandData, si || {}),
    incentivesMatchCompatibility: calcIncentivesMatchCompatibility(dealFields, brandData, si || {}),
    feesToleranceCompatibility: calcFeesToleranceCompatibility(dealFields, brandData, mp || {})
  };
  let weightedSum = 0;
  let totalWeight = 0;
  for (const [key, score] of Object.entries(factors)) {
    const w = NEW_WEIGHTS[key];
    if (w != null && typeof w === "number") {
      totalWeight += w;
      if (score != null && score !== undefined && !Number.isNaN(score)) weightedSum += (w / 100) * score;
    }
  }
  const total = totalWeight > 0 ? (weightedSum / totalWeight) * 100 : 0;
  return { total: Math.min(100, Math.max(0, Math.round(total * 10) / 10)), factors };
}

/** Human-readable breakdown for Match Score New (for View Details modal). Same fields as displayed in breakdown. */
function getBreakdownNewDetails(dealFields, locationData, brandData, si, mp) {
  const brandBasics = brandData.brandBasics || {};
  const brandScale = str(brandBasics[BF.brandBasics.hotelChainScale] || "");
  const dealScale = str(loc(locationData, BF.locationDeal.hotelChainScale, "hotelChainScale") || dealFields[BF.locationDeal.hotelChainScale] || "");
  const brandSvc = str(brandBasics[BF.brandBasics.hotelServiceModel] || "");
  const dealSvc = str(loc(locationData, BF.locationDeal.hotelServiceModel, "hotelServiceModel") || dealFields[BF.locationDeal.hotelServiceModel] || "");
  const preferredRaw = (si || {})[BF.strategicIntent.preferredBrands];
  const preferredStr = Array.isArray(preferredRaw) ? preferredRaw.map((b) => typeof b === "string" ? b : (b && b.name) || "").join(", ") : str(preferredRaw || "");
  return {
    chainScaleProximity: {
      label: "Chain Scale Proximity",
      weight: NEW_WEIGHTS.chainScaleProximity,
      brandValue: "Hotel Chain Scale: " + (brandScale || "—"),
      dealValue: "Hotel Chain Scale: " + (dealScale || "—"),
      note: "Compares chain scale tiers (e.g., Luxury, Upscale). Closer alignment scores higher; same tier is a strong match, one tier apart is partial.",
      score: calcChainScaleProximity(dealFields, locationData, brandData) ?? "—"
    },
    serviceModelAlignment: {
      label: "Service Model Alignment",
      weight: NEW_WEIGHTS.serviceModelAlignment,
      brandValue: "Hotel Service Model: " + (brandSvc || "—"),
      dealValue: "Hotel Service Model: " + (dealSvc || "—"),
      note: "Must match; different service models (e.g., full-service vs select-service) score lower.",
      score: calcServiceModelAlignment(dealFields, locationData, brandData) ?? "—"
    },
    preferredBrand: {
      label: "Preferred Brand",
      weight: NEW_WEIGHTS.preferredBrand,
      brandValue: "Brand Name: " + (str(brandBasics[BF.brandBasics.brandName] || "") || "—"),
      dealValue: "Preferred Brands: " + (preferredStr || "—"),
      note: "Brand in your preferred list scores full; otherwise it does not contribute.",
      score: calcPreferredBrand(dealFields, locationData, brandData, si || {}) ?? "—"
    },
    projectTypeCompatibility: {
      label: "Project Type Compatibility",
      weight: NEW_WEIGHTS.projectTypeCompatibility,
      brandValue: "Acceptable Project Type: " + (() => {
        const brandFit = brandData.brandFit || {};
        const raw = brandFit["Acceptable Project Type"];
        if (Array.isArray(raw) && raw.length > 0) return raw.map((v) => str(v)).filter(Boolean).join(", ");
        return "—";
      })(),
      dealValue: "Project Type: " + (str(dealFields[BF.locationDeal.projectType]) || "—"),
      note: "Deal project type must be one the brand accepts (e.g., new build, conversion); otherwise scores lower.",
      score: calcProjectTypeCompatibility(dealFields, locationData, brandData) ?? "—"
    },
    buildingTypeCompatibility: {
      label: "Building Type Compatibility",
      weight: NEW_WEIGHTS.buildingTypeCompatibility,
      brandValue: "Acceptable Building Types: " + (() => {
        const brandFit = brandData.brandFit || {};
        const raw = brandFit["Acceptable Building Types"];
        if (Array.isArray(raw) && raw.length > 0) return raw.map((v) => str(v)).filter(Boolean).join(", ");
        return "—";
      })(),
      dealValue: "Building Type: " + (str(loc(locationData, BF.locationDeal.buildingType, "buildingType") || dealFields[BF.locationDeal.buildingType]) || "—"),
      note: "Deal building type must be one the brand accepts; otherwise scores lower.",
      score: calcBuildingTypeCompatibility(dealFields, locationData, brandData) ?? "—"
    },
    projectStageCompatibility: {
      label: "Project Stage Compatibility",
      weight: NEW_WEIGHTS.projectStageCompatibility,
      brandValue: "Acceptable Project Stages: " + (() => {
        const brandFit = brandData.brandFit || {};
        const raw = brandFit["Acceptable Project Stages"];
        if (Array.isArray(raw) && raw.length > 0) return raw.map((v) => str(v)).filter(Boolean).join(", ");
        return (typeof raw === "string" && raw.trim()) ? raw.trim() : "—";
      })(),
      dealValue: "Stage of Development: " + (str(dealFields[BF.locationDeal.stageOfDevelopment] || loc(locationData, BF.locationDeal.stageOfDevelopment, "stageOfDevelopment") || dealFields["Project Stage"]) || "—"),
      note: "Deal stage must be one the brand accepts (e.g., entitled, under construction); otherwise scores lower.",
      score: calcProjectStageCompatibility(dealFields, locationData, brandData) ?? "—"
    },
    brandStandardsCompatibility: {
      label: "Brand Standards Compatibility",
      weight: NEW_WEIGHTS.brandStandardsCompatibility,
      brandValue: (() => {
        const st = brandData.brandStandards || {};
        const parts = [];
        const add = toStrArr(st["Additional Amenities"]);
        if (add.length) parts.push("Additional Amenities: " + add.join(", "));
        const fbReq = str(st["F&B Outlets Required"] || "");
        if (fbReq) parts.push("F&B Required: " + fbReq);
        const fbTypical = parseNum(st["Typical Number of F&B Outlets"]);
        if (fbTypical != null) parts.push("Typical F&B Outlets: " + fbTypical);
        const parkReq = str(st["Parking Required"] || "");
        if (parkReq) parts.push("Parking Required: " + parkReq);
        return parts.length ? parts.join("; ") : "—";
      })(),
      dealValue: (() => {
        const parts = [];
        const fb = str(dealFields["F&B Outlets?"] || loc(locationData, "F&B Outlets?", "fbOutlets"));
        const fbNum = parseNum(dealFields["Number of F&B Outlets"] || loc(locationData, "Number of F&B Outlets", "numberOfFbOutlets"));
        if (fb) parts.push("F&B Outlets?: " + fb);
        if (fbNum != null) parts.push("Number of F&B: " + fbNum);
        const park = parseNum(dealFields["Number of Parking Spaces"] || loc(locationData, "Number of Parking Spaces", "numberOfParkingSpaces"));
        if (park != null) parts.push("Parking Spaces: " + park);
        const add = toStrArr(dealFields["Additional Amenities"] || loc(locationData, "Additional Amenities", "additionalAmenities"));
        if (add.length) parts.push("Additional Amenities: " + add.join(", "));
        return parts.length ? parts.join("; ") : "—";
      })(),
      note: "Compares required amenities and standards (F&B, parking, additional amenities). Missing required items reduce the score; closer alignment scores higher.",
      score: calcBrandStandardsCompatibility(dealFields, locationData, brandData) ?? "—"
    },
    agreementsTypeCompatibility: {
      label: "Agreements Type Compatibility",
      weight: NEW_WEIGHTS.agreementsTypeCompatibility,
      brandValue: "Acceptable Agreements Type: " + (() => {
        const brandFit = brandData.brandFit || {};
        const raw = brandFit["Acceptable Agreements Type"];
        if (Array.isArray(raw) && raw.length > 0) return raw.map((v) => str(v)).filter(Boolean).join(", ");
        const boolCols = ["Franchise Only - Acceptable Agreements Type", "Third-Party Management Only - Acceptable Agreements Type", "Brand + Third-Party - Acceptable Agreements Type", "Brand-Managed - Acceptable Agreements Type", "Lease - Acceptable Agreements Type", "Joint Venture - Acceptable Agreements Type", "Flexible/Open - Acceptable Agreements Type"];
        const formVals = ["Franchise Only", "Third-Party Management Only", "Brand + Third-Party Mgmt. (Combined)", "Brand-Managed Only", "Lease", "Joint Venture", "Flexible/Open"];
        const accepted = [];
        for (let i = 0; i < boolCols.length; i++) {
          const v = brandFit[boolCols[i]];
          if (v === true || v === "Yes" || v === "Acceptable") accepted.push(formVals[i]);
        }
        return accepted.length > 0 ? accepted.join(", ") : "—";
      })(),
      dealValue: "Preferred Deal Structure: " + (strOrFirst(mp?.["Preferred Deal Structure"]) || "—"),
      note: "Deal structure (franchise, management, lease, etc.) must align with what the brand accepts. Flexible or open on either side can match; partial overlap may score lower.",
      score: calcAgreementsTypeCompatibility(dealFields, locationData, brandData, mp || {}) ?? "—"
    },
    roomRangeFitCompatibility: {
      label: "Room Range Fit",
      weight: NEW_WEIGHTS.roomRangeFitCompatibility,
      brandValue: (() => {
        const pf = brandData.brandFit || {};
        const roomMin = pf["Min - Room Count"] ?? pf["Min - Ideal Project Size"];
        const roomMax = pf["Max - Room Count"] ?? pf["Max - Ideal Project Size"];
        const idealMin = pf["Min - Ideal Project Size"] ?? pf["A Min - Ideal Project Size"];
        const idealMax = pf["Max - Ideal Project Size"] ?? pf["A Max - Ideal Project Size"];
        const room = roomMin != null && roomMax != null ? `${roomMin} – ${roomMax}` : "—";
        const ideal = idealMin != null && idealMax != null ? `${idealMin} – ${idealMax}` : "—";
        return `Room Count: ${room}; Ideal Project Size: ${ideal}`;
      })(),
      dealValue: "Total Number of Rooms/Keys: " + (parseNum(loc(locationData, BF.locationDeal.totalRoomsKeys, "totalNumberOfRoomsKeys") ?? dealFields[BF.locationDeal.totalRoomsKeys]) ?? "—"),
      note: "Compares your room count to the brand's ideal and acceptable range. Within ideal scores highest; outside ideal but within range scores lower; outside range scores lowest.",
      score: calcRoomRangeFitCompatibility(dealFields, locationData, brandData) ?? "—"
    },
    keyMoneyWillingnessCompatibility: {
      label: "Key Money Willingness",
      weight: NEW_WEIGHTS.keyMoneyWillingnessCompatibility,
      brandValue: (() => {
        const op = brandData.brandOperationalSupport || {};
        const inc = op[INCENTIVE_TYPES_FIELD];
        const arr = Array.isArray(inc) ? inc : (inc && typeof inc === "string" ? inc.split(",").map((s) => s.trim()) : []);
        const offers = arr.some((v) => /key\s*money\s*\/\s*upfront\s*incentive/i.test(String(v)));
        return "Incentive Types: " + (arr.length ? arr.join(", ") : "—") + "; Brand offers Key Money: " + (offers ? "Yes" : "No");
      })(),
      dealValue: (() => {
        const filterVal = getKeyMoneyFilterValue(dealFields);
        const mustHaves = toStrArr((si || {})[BF.strategicIntent.mustHavesFromBrand] ?? dealFields?.[BF.strategicIntent.mustHavesFromBrand] ?? dealFields?.["Must-haves From Brand or Operator"]);
        const dealBreakers = toStrArr((si || {})[BF.strategicIntent.top3DealBreakers] ?? dealFields?.[BF.strategicIntent.top3DealBreakers]);
        const parts = [];
        parts.push("Filter out brands without key money?: " + (filterVal || "—"));
        if (mustHaves.length) parts.push("Must-Haves: " + mustHaves.join(", "));
        if (dealBreakers.length) parts.push("Top 3 Deal Breakers: " + dealBreakers.join(", "));
        return parts.length ? parts.join("; ") : "—";
      })(),
      note: "If you need key money and the brand offers it, this factor scores full. If you filter for key money and the brand does not offer it, the overall match is not considered.",
      score: calcKeyMoneyWillingnessCompatibility(dealFields, brandData, si || {}) ?? "—"
    },
    incentivesMatchCompatibility: {
      label: "Incentives Match Compatibility",
      weight: NEW_WEIGHTS.incentivesMatchCompatibility,
      brandValue: (() => {
        const op = brandData.brandOperationalSupport || {};
        const willing = str(op[BF.brandOperationalSupport.willingToNegotiate] || op[BF.brandOperationalSupport.willingToNegotiateAlt] || "—");
        const inc = op[INCENTIVE_TYPES_FIELD];
        const arr = Array.isArray(inc) ? inc : (inc && typeof inc === "string" ? inc.split(",").map((s) => s.trim()) : []);
        const filtered = arr.filter((v) => !isKeyMoneyIncentiveType(v));
        return "Willing to Negotiate Incentives: " + willing + "; Incentive Types (excl. Key Money): " + (filtered.length ? filtered.join(", ") : "—");
      })(),
      dealValue: "Incentive Types Interested In: " + (() => {
        const inc = toStrArr((si || {})[BF.strategicIntent.incentiveTypesInterestedIn] ?? dealFields?.[BF.strategicIntent.incentiveTypesInterestedIn] ?? dealFields?.["Incentive Types Interested In"]);
        return inc.length ? inc.join(", ") : "—";
      })(),
      note: "Brand willingness to negotiate incentives is compared with what you seek. When you want incentives and the brand does not negotiate, this scores lowest.",
      score: calcIncentivesMatchCompatibility(dealFields, brandData, si || {}) ?? "—"
    },
    feesToleranceCompatibility: {
      label: "Fees Tolerance",
      weight: NEW_WEIGHTS.feesToleranceCompatibility,
      brandValue: (() => {
        const fs = brandData.brandFeeStructure || {};
        const royMin = fs[BF.brandFeeStructure.minRoyalty];
        const royMax = fs[BF.brandFeeStructure.maxRoyalty];
        const royBasis = fs[BF.brandFeeStructure.basisRoyalty];
        const mktMin = fs[BF.brandFeeStructure.minMarketing];
        const mktMax = fs[BF.brandFeeStructure.maxMarketing];
        const mktBasis = fs[BF.brandFeeStructure.basisMarketing];
        const loyMin = fs[BF.brandFeeStructure.minLoyalty];
        const loyMax = fs[BF.brandFeeStructure.maxLoyalty];
        const loyBasis = fs[BF.brandFeeStructure.basisLoyalty];
        const parts = [];
        if (royMin != null || royMax != null) parts.push("Royalty: " + formatFeeForDisplay(royMin) + "–" + formatFeeForDisplay(royMax) + "%" + (royBasis ? " (" + royBasis + ")" : ""));
        if (mktMin != null || mktMax != null) parts.push("Marketing: " + formatFeeForDisplay(mktMin) + "–" + formatFeeForDisplay(mktMax) + "%" + (mktBasis ? " (" + mktBasis + ")" : ""));
        if (loyMin != null || loyMax != null) parts.push("Loyalty: " + formatLoyaltyForDisplay(loyMin) + "–" + formatLoyaltyForDisplay(loyMax) + (loyBasis ? " (" + loyBasis + ")" : ""));
        return parts.length ? parts.join("; ") : "—";
      })(),
      dealValue: (() => {
        const mpData = mp || {};
        const roy = str(mpData[BF.marketPerformance.royaltyFeeExpectations] || "");
        const mkt = str(mpData[BF.marketPerformance.marketingFeeExpectations] || "");
        const loy = str(mpData[BF.marketPerformance.loyaltyFeeExpectations] || "");
        const parts = [];
        if (roy) parts.push("Royalty: " + roy);
        if (mkt) parts.push("Marketing: " + mkt);
        if (loy) parts.push("Loyalty: " + loy);
        return parts.length ? parts.join("; ") : "—";
      })(),
      note: "Compares your fee expectations (royalty, marketing, loyalty) to the brand's range. Within or above their range scores higher; expecting lower fees than the brand typically offers reduces the score.",
      score: calcFeesToleranceCompatibility(dealFields, brandData, mp || {}) ?? "—"
    }
  };
}

/** Resolve a Preferred Brands value to the Brand Name string. If value is an Airtable record ID (linked record), fetches that Brand Basics record and returns its Brand Name; otherwise returns the value normalized. Always returns normalized string so same brand from different records matches. */
export async function resolvePreferredBrandToName(baseId, apiKey, value) {
  const v = typeof value === "string" ? value.trim() : (value && value.name) ? String(value.name).trim() : "";
  if (!v) return "";
  if (!isAirtableRecordId(v)) return normalizeBrandName(v);
  const path = `${encodeURIComponent(BRAND_BASICS_TABLE)}/${encodeURIComponent(v)}`;
  const data = await atFetch(baseId, apiKey, path);
  if (data && data.fields && data.fields["Brand Name"] != null) {
    const name = String(data.fields["Brand Name"]).trim() || v;
    return normalizeBrandName(name);
  }
  return normalizeBrandName(v);
}

/** Returns Brand Basics record ID for a brand name, or null if not found. Used when adding brand to Preferred Brands (linked record). */
export async function getBrandBasicsRecordId(baseId, apiKey, brandName) {
  if (!brandName || str(brandName) === "" || str(brandName).toLowerCase() === "not specified") return null;
  const allRecords = await getAllBrandBasicsRecords(baseId, apiKey);
  const rec = findBrandRecordByExactName(allRecords, brandName);
  return rec ? rec.id : null;
}

/**
 * Strict pre-filters for recommended brand: brand must pass all to be eligible for full score computation.
 * Returns true if brand passes (or if data is missing to evaluate, we allow through).
 */
function passesStrictPreFilters(dealFields, locationData, brandData, mp, si) {
  const pf = brandData?.brandFit || {};
  const bb = brandData?.brandBasics || {};

  if (getKeyMoneyFilterValue(dealFields) === "yes" && !brandOffersKeyMoney(brandData)) return false;

  const brandScale = str(bb[BF.brandBasics.hotelChainScale]);
  const dealScale = str(loc(locationData, BF.locationDeal.hotelChainScale, "hotelChainScale") || dealFields[BF.locationDeal.hotelChainScale]);
  if (brandScale && dealScale && !brandScale.toLowerCase().includes("unknown") && !dealScale.toLowerCase().includes("unknown")) {
    const brandTier = getChainScaleTier(brandScale);
    const dealTier = getChainScaleTier(dealScale);
    if (Math.abs(brandTier - dealTier) > 1) return false;
  }

  const dealStruct = strOrFirst(mp?.["Preferred Deal Structure"]);
  if (dealStruct) {
    let brandAccepted = [];
    const multiSelect = pf["Acceptable Agreements Type"];
    if (Array.isArray(multiSelect) && multiSelect.length > 0) brandAccepted = multiSelect.map((v) => str(v)).filter(Boolean);
    else if (typeof multiSelect === "string" && multiSelect.trim()) brandAccepted = [multiSelect.trim()];
    else {
      const boolCols = ["Franchise Only - Acceptable Agreements Type", "Third-Party Management Only - Acceptable Agreements Type", "Brand + Third-Party - Acceptable Agreements Type", "Brand-Managed - Acceptable Agreements Type", "Lease - Acceptable Agreements Type", "Joint Venture - Acceptable Agreements Type", "Flexible/Open - Acceptable Agreements Type"];
      const formVals = ["Franchise Only", "Third-Party Management Only", "Brand + Third-Party Mgmt. (Combined)", "Brand-Managed Only", "Lease", "Joint Venture", "Flexible/Open"];
      for (let i = 0; i < boolCols.length; i++) {
        const v = pf[boolCols[i]];
        if (v === true || v === "Yes" || v === "Acceptable") brandAccepted.push(formVals[i]);
      }
    }
    if (brandAccepted.length > 0) {
      const dealNorm = normalizeAgreementType(dealStruct);
      const brandNorms = brandAccepted.map((b) => normalizeAgreementType(b));
      if (dealNorm !== "flexible/open" && !brandNorms.includes("flexible/open")) {
        if (dealNorm === "brand + third-party") {
          const dealAccepts = ["brand-managed", "third-party management only", "brand + third-party"];
          if (!dealAccepts.some((a) => brandNorms.includes(a))) return false;
        } else if (!brandNorms.includes(dealNorm)) return false;
      }
    }
  }

  const dealProjectType = str(dealFields[BF.locationDeal.projectType] || "");
  if (dealProjectType) {
    const raw = pf["Acceptable Project Type"];
    const brandList = Array.isArray(raw) ? raw : (typeof raw === "string" && raw.trim() ? [raw.trim()] : []);
    if (brandList.length > 0) {
      const normalized = (s) => String(s || "").trim().toLowerCase();
      if (!brandList.some((b) => normalized(b) === normalized(dealProjectType))) return false;
    }
  }

  const dealRooms = parseNum(loc(locationData, BF.locationDeal.totalRoomsKeys, "totalNumberOfRoomsKeys") ?? dealFields[BF.locationDeal.totalRoomsKeys]);
  if (dealRooms != null && dealRooms > 0) {
    const roomMin = parseNum(pf["Min - Room Count"]);
    const roomMax = parseNum(pf["Max - Room Count"]);
    if (roomMin != null && roomMax != null && (dealRooms < roomMin || dealRooms > roomMax)) return false;
  }

  const brandSvc = str(bb[BF.brandBasics.hotelServiceModel]);
  const dealSvc = str(loc(locationData, BF.locationDeal.hotelServiceModel, "hotelServiceModel") || dealFields[BF.locationDeal.hotelServiceModel]);
  if (brandSvc && dealSvc && !brandSvc.toLowerCase().includes("unknown") && !dealSvc.toLowerCase().includes("unknown")) {
    if (brandSvc.toLowerCase() !== dealSvc.toLowerCase()) return false;
  }

  const dealBuilding = str(loc(locationData, BF.locationDeal.buildingType, "buildingType") || dealFields[BF.locationDeal.buildingType] || "");
  if (dealBuilding) {
    const raw = pf["Acceptable Building Types"];
    const brandList = Array.isArray(raw) ? raw : (typeof raw === "string" && raw.trim() ? [raw.trim()] : []);
    if (brandList.length > 0) {
      const normalized = (s) => String(s || "").trim().toLowerCase();
      if (!brandList.some((b) => normalized(b) === normalized(dealBuilding))) return false;
    }
  }

  const stageRaw = str(dealFields[BF.locationDeal.stageOfDevelopment] || loc(locationData, BF.locationDeal.stageOfDevelopment, "stageOfDevelopment") || dealFields["Project Stage"] || "");
  if (stageRaw) {
    const raw = pf["Acceptable Project Stages"];
    const brandList = Array.isArray(raw) ? raw : (typeof raw === "string" && raw.trim() ? [raw.trim()] : []);
    if (brandList.length > 0) {
      const sl = stageRaw.toLowerCase();
      let dealCategory = null;
      if (sl.includes("land") || sl.includes("control")) dealCategory = "land";
      else if (sl.includes("entitlement") && !sl.includes("fully")) dealCategory = "entitlements";
      else if (sl.includes("entitled")) dealCategory = "entitled";
      else if (sl.includes("construction")) dealCategory = "construction";
      else if (sl.includes("stabilized") || sl.includes("operating")) dealCategory = "stabilized";
      if (dealCategory) {
        const norm = (s) => String(s || "").trim().toLowerCase();
        const accepted = brandList.some((v) => {
          const b = norm(v);
          if (dealCategory === "land") return b.includes("land") || b.includes("control");
          if (dealCategory === "entitlements") return b.includes("entitlement") && !b.includes("fully");
          if (dealCategory === "entitled") return b.includes("fully") && b.includes("entitled");
          if (dealCategory === "construction") return b.includes("construction");
          if (dealCategory === "stabilized") return b.includes("stabilized") || b.includes("operating");
          return false;
        });
        if (!accepted) return false;
      }
    }
  }

  return true;
}

async function getAllBrandNames(baseId, apiKey) {
  const records = await getAllBrandBasicsRecords(baseId, apiKey);
  const names = [];
  for (const rec of records || []) {
    const n = rec.fields && rec.fields["Brand Name"];
    const s = str(n);
    if (s && s.toLowerCase() !== "not specified") names.push(s);
  }
  return names;
}

/** Get candidate brand names with tier, filtered by chain scale (within 2 tiers). Sorted by tier proximity for faster early exit. */
async function getCandidateBrandNames(baseId, apiKey, dealChainScale) {
  const records = await getAllBrandBasicsRecords(baseId, apiKey);
  const dealTier = dealChainScale ? getChainScaleTier(dealChainScale) : null;
  const items = [];
  for (const rec of records || []) {
    const n = rec.fields && rec.fields["Brand Name"];
    const s = str(n);
    if (!s || s.toLowerCase() === "not specified") continue;
    let brandTier = 0;
    if (dealTier != null) {
      const brandScale = str(rec.fields && rec.fields[BF.brandBasics.hotelChainScale]);
      if (brandScale && !brandScale.toLowerCase().includes("unknown")) {
        brandTier = getChainScaleTier(brandScale);
        if (Math.abs(brandTier - dealTier) > 2) continue;
      }
    }
    items.push({ name: s, brandTier });
  }
  if (dealTier != null && items.length > 1) {
    items.sort((a, b) => Math.abs(a.brandTier - dealTier) - Math.abs(b.brandTier - dealTier));
  }
  return items.map((x) => x.name);
}

const RECOMMENDED_BRAND_CONCURRENCY = 4;
/** Max brands to score – bounds worst-case time. Early exit if score >= EARLY_EXIT_THRESHOLD. */
const RECOMMENDED_BRAND_MAX_SCORED = 48;
const EARLY_EXIT_THRESHOLD = 90;

/**
 * Compute recommended brand (highest Match Score New among candidates, excluding preferred).
 * Uses limited concurrency, chain-scale filter, cap on brands scored, and early exit for speed.
 * Returns { brand, scoreNew, breakdownNewDetails } or { found: false, totalBrands, skippedPreferred, skippedNoData }.
 */
export async function computeRecommendedBrand(dealFields, locationData, mpData, siData, preferredBrandsSet, baseId, apiKey) {
  if (!baseId || !apiKey) return { found: false, totalBrands: 0, skippedPreferred: 0, skippedNoData: 0 };
  const preferred = new Set([...(preferredBrandsSet || [])].map((b) => normalizeBrandName(b)));
  const dealScale = str(loc(locationData, BF.locationDeal.hotelChainScale, "hotelChainScale") || dealFields[BF.locationDeal.hotelChainScale]);
  const allCandidates = await getCandidateBrandNames(baseId, apiKey, dealScale);
  const candidateBrands = allCandidates.slice(0, RECOMMENDED_BRAND_MAX_SCORED);
  let best = null;
  let bestScore = -1;
  let skippedPreferred = 0;
  let skippedNoData = 0;
  let earlyExit = false;

  const processBatch = async (batch) => {
    const results = await Promise.all(batch.map(async (brandName) => {
      if (preferred.has(normalizeBrandName(brandName))) return { skip: "preferred" };
      const brandData = await fetchBrandData(baseId, apiKey, brandName);
      if (!brandData) return { skip: "noData" };
      const { scoreNew, breakdownNewDetails } = await computeMatchScoreForDealBrand(dealFields, locationData, mpData, siData, brandName, baseId, apiKey);
      const s = scoreNew != null && scoreNew !== "" ? Number(scoreNew) : -1;
      return { brand: brandName, scoreNew: s, breakdownNewDetails };
    }));
    return results;
  };

  for (let i = 0; i < candidateBrands.length && !earlyExit; i += RECOMMENDED_BRAND_CONCURRENCY) {
    const batch = candidateBrands.slice(i, i + RECOMMENDED_BRAND_CONCURRENCY);
    const results = await processBatch(batch);
    for (const r of results) {
      if (r.skip === "preferred") skippedPreferred++;
      else if (r.skip === "noData") skippedNoData++;
      else if (r.brand && !Number.isNaN(r.scoreNew) && r.scoreNew > bestScore) {
        bestScore = r.scoreNew;
        best = { brand: r.brand, scoreNew: r.scoreNew, breakdownNewDetails: r.breakdownNewDetails || {} };
        if (bestScore >= EARLY_EXIT_THRESHOLD) earlyExit = true;
      }
    }
  }
  if (best) return best;
  return { found: false, totalBrands: candidateBrands.length, skippedPreferred, skippedNoData };
}

/** Max brands to score when computing top alternatives (same cap as single best). */
const TOP_ALTERNATIVES_MAX_SCORED = 48;

/**
 * Compute top N alternative brands by Match Score New (excluding preferred). Same batching and filters as computeRecommendedBrand.
 * Returns { preferredBrand, preferredScore, alternatives: [{ brand, score, breakdownNewDetails? }] }.
 */
export async function computeTopAlternativeBrands(dealFields, locationData, mpData, siData, preferredBrandsSet, baseId, apiKey, limit = 5) {
  const preferred = new Set([...(preferredBrandsSet || [])].map((b) => normalizeBrandName(b)));
  const preferredList = [...preferred];
  const dealScale = str(loc(locationData, BF.locationDeal.hotelChainScale, "hotelChainScale") || dealFields[BF.locationDeal.hotelChainScale]);
  const allCandidates = await getCandidateBrandNames(baseId, apiKey, dealScale);
  const candidateBrands = allCandidates.slice(0, TOP_ALTERNATIVES_MAX_SCORED);
  const scored = [];

  const processBatch = async (batch) => {
    return Promise.all(batch.map(async (brandName) => {
      if (preferred.has(normalizeBrandName(brandName))) return { skip: "preferred" };
      const brandData = await fetchBrandData(baseId, apiKey, brandName);
      if (!brandData) return { skip: "noData" };
      const { scoreNew, breakdownNewDetails } = await computeMatchScoreForDealBrand(dealFields, locationData, mpData, siData, brandName, baseId, apiKey);
      const s = scoreNew != null && scoreNew !== "" ? Number(scoreNew) : -1;
      return { brand: brandName, scoreNew: s, breakdownNewDetails };
    }));
  };

  for (let i = 0; i < candidateBrands.length; i += RECOMMENDED_BRAND_CONCURRENCY) {
    const batch = candidateBrands.slice(i, i + RECOMMENDED_BRAND_CONCURRENCY);
    const results = await processBatch(batch);
    for (const r of results) {
      if (r.brand && !Number.isNaN(r.scoreNew) && r.scoreNew >= 0) {
        scored.push({ brand: r.brand, score: r.scoreNew, breakdownNewDetails: r.breakdownNewDetails || {} });
      }
    }
  }

  scored.sort((a, b) => b.score - a.score);
  const alternatives = scored.slice(0, Math.max(0, limit)).map(({ brand, score, breakdownNewDetails }) => ({ brand, score, breakdownNewDetails }));

  let preferredBrand = preferredList[0] || null;
  let preferredScore = null;
  let preferredBreakdown = {};
  if (preferredBrand) {
    const res = await computeMatchScoreForDealBrand(dealFields, locationData, mpData, siData, preferredBrand, baseId, apiKey);
    preferredScore = res.scoreNew != null && res.scoreNew !== "" ? Number(res.scoreNew) : null;
    preferredBreakdown = res.breakdownNewDetails || {};
  }

  return { preferredBrand, preferredScore, preferredBreakdown, alternatives };
}

export async function computeMatchScoreForDealBrand(dealFields, locationData, marketPerformanceData, strategicIntentData, brandName, baseId, apiKey) {
  if (!brandName || str(brandName) === "" || str(brandName).toLowerCase() === "not specified") {
    return { score: 0, breakdown: {}, scoreNew: 0, breakdownNew: {}, breakdownNewDetails: {} };
  }
  const mp = marketPerformanceData || {};
  const si = strategicIntentData || {};
  const brandSpread = brandDifferentiatorSpread(brandName);

  const brandData = await fetchBrandData(baseId, apiKey, brandName);
  if (!brandData) {
    return { score: 0, breakdown: {}, scoreNew: 0, breakdownNew: {}, breakdownNewDetails: {} };
  }

  const filterOutNoKeyMoney = getKeyMoneyFilterValue(dealFields) === "yes";
  const keyMoneyGate = filterOutNoKeyMoney && !brandOffersKeyMoney(brandData);

  const breakdown = {
    MKT1: calcMKT1(dealFields, locationData, brandData),
    MKT2: calcMKT2(dealFields, locationData, brandData),
    SEG1: calcSEG1(dealFields, locationData, brandData),
    SVC1: calcSVC1(dealFields, locationData, brandData),
    SIZE1: calcSIZE1(dealFields, locationData, brandData),
    OWN1: calcOWN1(dealFields, locationData, brandData, mp),
    STR1: calcSTR1(dealFields, locationData, brandData, si),
    AMN1: calcAMN1(dealFields, locationData, brandData),
    FIN1: calcFIN1(dealFields, locationData, brandData, mp),
    INC1: calcINC1(dealFields, locationData, brandData, mp, si),
    PREF1: calcPREF1(dealFields, locationData, brandData, si),
    KEY1: calcKEY1(dealFields, locationData, brandData, mp, si),
    CAP1: calcCAP1(dealFields, locationData, brandData, mp),
    TERM1: calcTERM1(dealFields, locationData, brandData, mp),
    PROJ1: calcPROJ1(dealFields, locationData, brandData),
    PROJ2: calcPROJ2(dealFields, locationData, brandData),
    PROJ3: calcPROJ3(dealFields, locationData, brandData),
    AGMT1: calcAGMT1(dealFields, locationData, brandData),
    ESG1: calcESG1(dealFields, locationData, brandData)
  };
  let weightedSum = 0, totalWeight = 0;
  for (const [key, score] of Object.entries(breakdown)) {
    if (score !== null && score !== undefined && WEIGHTS[key]) {
      weightedSum += score * WEIGHTS[key];
      totalWeight += WEIGHTS[key];
    }
  }
  const rawScore = totalWeight > 0 ? weightedSum / totalWeight : 0;
  let brandOffset = 0;
  if (brandName) {
    let h = 0;
    for (let i = 0; i < brandName.length; i++) h = (h * 31 + brandName.charCodeAt(i)) | 0;
    brandOffset = ((h % 101) - 50) / 100;
  }
  const withOffset = rawScore + brandOffset;
  let withSpread = Math.min(100, Math.max(0, withOffset + brandSpread));
  let finalScore = Math.round(withSpread * 10) / 10;
  breakdown.BRND1 = brandSpread * 10;
  const breakdownDetails = getBreakdownDetails(dealFields, locationData, brandData, mp, si);
  const { total: scoreNew, factors: breakdownNew } = computeMatchScoreNew(dealFields, locationData, brandData, si, mp);
  const breakdownNewDetails = getBreakdownNewDetails(dealFields, locationData, brandData, si, mp);
  if (keyMoneyGate) {
    return { score: 0, breakdown, breakdownDetails, keyMoneyGateReason: "Overall score is 0 because \"Filter out brands without key money?\" (Contact & Uploads) is Yes and this brand does not have \"Key Money / Upfront Incentive\" in Incentive Types (Brand Setup - Operational Support).", scoreNew: 0, breakdownNew, breakdownNewDetails };
  }
  return { score: finalScore, breakdown, breakdownDetails, scoreNew, breakdownNew, breakdownNewDetails };
}
