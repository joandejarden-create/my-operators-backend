/**
 * My Deals API – list and update deals from Airtable for the owner-facing "My Deals" page.
 * Uses the same Airtable base as intake-deal / brand-review.
 * Deal Status dropdown options match the Deal Status field in the Deals table.
 *
 * The Project Location column in the list is not populated (shows "—"). Location & Property
 * is still used when opening a deal for edit (Location & Site Details form) and when saving.
 *
 * Set AIRTABLE_TABLE_DEALS to your table name (e.g. "Deals"). Supported Deals field names
 * (any can be missing): Project Name, Property Name, Name; Hotel Type, Property Type;
 * Project Type, Stage of Development; Expected Opening or Rebranding Date;
 * Form Status; Deal Status, Status.
 */

import { getAllOutreachDealIds } from "./outreach-setup.js";
import { computeMatchScoreForDealBrand, computeRecommendedBrand, computeTopAlternativeBrands, getBrandBasicsRecordId, resolvePreferredBrandToName } from "./match-score-server.js";
import {
  DEALS_TABLE,
  DEALS_STATUS_FIELD,
  DEALS_FORM_TO_AIRTABLE,
  DEALS_AIRTABLE_TO_FORM,
  LOCATION_PROPERTY_TABLE,
  LOCATION_LINK_FIELD,
  LOCATION_LINK_ALIAS,
  LOCATION_PROPERTY_ID_FIELD,
  LOCATION_FORM_TO_AIRTABLE,
  LOCATION_FORM_FIELDS,
  MARKET_PERFORMANCE_TABLE,
  MARKET_PERFORMANCE_LINK_FIELD,
  MP_DEAL_LINK_FIELD,
  MARKET_PERFORMANCE_FIELD_NAMES,
  MP_FORM_TO_TABLE,
  MP_TABLE_TO_FORM,
  STRATEGIC_INTENT_LINK_FIELD,
  STRATEGIC_INTENT_TABLE,
  CONTACT_UPLOADS_LINK_FIELD,
  CONTACT_UPLOADS_TABLE,
  CU_DEAL_LINK_FIELD,
  LEASE_STRUCTURE_LINK_FIELD,
  LEASE_STRUCTURE_TABLE,
  LS_DEAL_LINK_FIELD,
  STRATEGIC_INTENT_FORM_FIELDS,
  SI_FORM_TO_AIRTABLE,
  SI_AIRTABLE_TO_FORM,
  CONTACT_UPLOADS_FORM_FIELDS,
  CU_FORM_TO_AIRTABLE,
  CU_ATTACHMENT_FIELD,
  LEASE_STRUCTURE_FORM_FIELDS,
  LS_FORM_TO_AIRTABLE,
  LS_AIRTABLE_TO_FORM,
} from "./schemas/deal-setup-fields.js";
import { validateDealSetupPayload } from "./deal-setup-validate.js";
import { fetchTargetsForDeal } from "./target-list.js";
import { loadNewBaseOperatorBundle, buildPrefillObjectFromNewBaseRows, loadBrandNameByIdMap } from "./lib/operator-setup-new-base-read.js";

function valueToStr(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "number" && !Number.isNaN(v)) return String(v);
  if (typeof v === "object" && v !== null && typeof v.name === "string") return v.name.trim();
  if (Array.isArray(v) && v[0]) return valueToStr(v[0]);
  return "";
}

function formatDate(val) {
  if (!val) return "";
  const d = typeof val === "string" ? new Date(val) : val;
  if (isNaN(d.getTime())) return valueToStr(val);
  const m = d.toLocaleString("en-US", { month: "short" });
  const y = d.getFullYear();
  return `${m}, ${y}`;
}

/** Fallback Deal Status options when Meta API is unavailable (e.g. token lacks schema.bases:read scope). */
const DEAL_STATUS_FALLBACK = [
  "Active - Hidden",
  "Active - Visible",
  "Archived",
  "Draft",
  "Expired",
  "Matched Only",
  "Paused",
  "Under Review",
];

/** When true, every Airtable request used by getMyDeals is serialized with AIRTABLE_SERIAL_INTERVAL_MS to guarantee reliable load (no rate-limit blanks). */
let getMyDealsSerializing = false;
let lastAirtableRequestAt = 0;
const AIRTABLE_SERIAL_INTERVAL_MS = 1200;

async function waitAirtableSerial() {
  if (!getMyDealsSerializing) return;
  const now = Date.now();
  const wait = lastAirtableRequestAt + AIRTABLE_SERIAL_INTERVAL_MS - now;
  if (wait > 0) await new Promise((r) => setTimeout(r, wait));
  lastAirtableRequestAt = Date.now();
}

/** Fetch Deal Status field options from Airtable via Meta API. Linked to the Deals table schema. */
async function getDealStatusChoiceNames(baseId, apiKey) {
  try {
    await waitAirtableSerial();
    const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    if (!res.ok) return DEAL_STATUS_FALLBACK;
    const data = await res.json();
    const table = (data.tables || []).find((t) => t.name === DEALS_TABLE);
    if (!table) return DEAL_STATUS_FALLBACK;
    const field = (table.fields || []).find(
      (f) => (f.name === DEALS_STATUS_FIELD || f.name === "Deal Status" || f.name === "Status") && (f.type === "singleSelect" || f.type === "multipleSelects")
    );
    const choices = field?.options?.choices?.map((c) => c.name) || [];
    return choices.length > 0 ? choices : DEAL_STATUS_FALLBACK;
  } catch (_) {
    return DEAL_STATUS_FALLBACK;
  }
}

/** Deal Brand Cache table: pre-computed preferred brands (names), scores, top alternatives. Speeds up list load and Alternative Brand Suggestions. */
const DEAL_BRAND_CACHE_TABLE = process.env.AIRTABLE_TABLE_DEAL_BRAND_CACHE || "Deal Brand Cache";
const DEAL_BRAND_CACHE_NAME_FIELD = "Name";
const DEAL_BRAND_CACHE_DEAL_FIELD = "Deal";
const DEAL_BRAND_CACHE_PREFERRED_BRANDS_FIELD = "Preferred Brands";
const DEAL_BRAND_CACHE_PREFERRED_SCORES_FIELD = "Preferred Scores";
const DEAL_BRAND_CACHE_TOP_ALTERNATIVES_FIELD = "Top Alternatives";
const DEAL_BRAND_CACHE_PREFERRED_SCORE_FIELD = "Preferred Score";
const DEAL_BRAND_CACHE_BEST_MATCH_BRAND_FIELD = "Best Match Brand";
const DEAL_BRAND_CACHE_BEST_MATCH_SCORE_FIELD = "Best Match Score";
const DEAL_BRAND_CACHE_LAST_COMPUTED_FIELD = "Last Computed At";
const DEAL_BRAND_CACHE_BREAKDOWN_FIELD = "Breakdown Details By Brand";
const BRAND_DEAL_REQUESTS_TABLE = process.env.AIRTABLE_TABLE_BRAND_DEAL_REQUESTS || "Brand Deal Requests";
const TARGET_LIST_TABLE = process.env.AIRTABLE_TABLE_TARGET_LIST || "Target List";

/** Get linked Strategic Intent - Operational - Key Challenges record ID from deal fields. */
function getLinkedStrategicIntentId(fields) {
  if (!fields) return null;
  const raw = fields[STRATEGIC_INTENT_LINK_FIELD];
  if (!Array.isArray(raw) || raw.length === 0) return null;
  const id = typeof raw[0] === "string" ? raw[0] : raw[0]?.id;
  return id && typeof id === "string" && id.startsWith("rec") ? id : null;
}

/** Fetch a single record from Strategic Intent - Operational - Key Challenges table (raw Airtable fields). */
async function fetchStrategicIntentRecord(baseId, apiKey, recordId) {
  await waitAirtableSerial();
  const table = encodeURIComponent(STRATEGIC_INTENT_TABLE);
  const url = `https://api.airtable.com/v0/${baseId}/${table}/${encodeURIComponent(recordId)}`;
  const res = await fetch(url, { headers: { Authorization: "Bearer " + apiKey } });
  const data = await res.json();
  if (data.error || !data.fields) return null;
  return data.fields;
}

/** Collect unique linked Strategic Intent record IDs from deal records. */
function collectLinkedStrategicIntentIds(records) {
  const ids = new Set();
  for (const rec of records) {
    const id = getLinkedStrategicIntentId(rec.fields || {});
    if (id) ids.add(id);
  }
  return [...ids];
}

const SI_FETCH_BATCH_SIZE = 5;
const MY_DEALS_SI_FETCH_DELAY_MS = Math.max(0, parseInt(process.env.MY_DEALS_SI_FETCH_DELAY_MS || "220", 10) || 220);

/** Fetch full Strategic Intent record for each ID; return Map(siRecordId -> fields). Batched for speed. */
async function fetchStrategicIntentDataMap(baseId, apiKey, siRecordIds) {
  const map = new Map();
  for (let i = 0; i < siRecordIds.length; i += SI_FETCH_BATCH_SIZE) {
    const batch = siRecordIds.slice(i, i + SI_FETCH_BATCH_SIZE);
    const results = await Promise.all(batch.map(async (id) => {
      const fields = await fetchStrategicIntentRecord(baseId, apiKey, id);
      return { id, fields };
    }));
    for (const { id, fields } of results) {
      if (fields && typeof fields === "object") map.set(id, fields);
    }
    if (i + SI_FETCH_BATCH_SIZE < siRecordIds.length) {
      await new Promise((r) => setTimeout(r, MY_DEALS_SI_FETCH_DELAY_MS));
    }
  }
  return map;
}

/** Extract a single Preferred Brands value: string as-is, object as .name or .id (Airtable linked records can be { id, name } or just { id }). */
function extractPreferredBrandValue(v) {
  if (typeof v === "string") return v.trim();
  if (v && typeof v === "object") {
    const name = v.name != null ? String(v.name).trim() : "";
    if (name) return name;
    const id = v.id != null ? String(v.id).trim() : "";
    if (id && id.startsWith("rec")) return id;
  }
  return "";
}

/** Build Preferred Brands map from SI data map (raw values only; no resolution). */
function preferredBrandsMapFromSiDataMapSync(siDataMap) {
  const map = new Map();
  const keys = ["Preferred Brands", "Preferred Brands (up to 4)"];
  for (const [id, fields] of siDataMap) {
    let val = null;
    for (const key of keys) {
      if (fields[key] !== undefined && fields[key] !== null) { val = fields[key]; break; }
    }
    const arr = Array.isArray(val)
      ? val.map((v) => extractPreferredBrandValue(v)).filter(Boolean)
      : typeof val === "string" && String(val).trim()
        ? String(val).split(/\s*,\s*/).map((s) => s.trim()).filter(Boolean)
        : [];
    if (arr.length > 0) map.set(id, arr);
  }
  return map;
}

/** Build preferred brands map with every value resolved to Brand Name (so we always compare full text from same source). */
async function preferredBrandsMapFromSiDataMapResolved(baseId, apiKey, siDataMap) {
  const mapRaw = preferredBrandsMapFromSiDataMapSync(siDataMap);
  const map = new Map();
  for (const [id, arr] of mapRaw) {
    const names = await Promise.all(arr.map((v) => resolvePreferredBrandToName(baseId, apiKey, v)));
    const joined = names.filter(Boolean).join(", ");
    if (joined) map.set(id, joined);
  }
  return map;
}

/** Fetch Preferred Brands for each Strategic Intent record and resolve any record IDs to Brand Name. Returns Map(siRecordId -> comma-separated brand names). */
async function fetchPreferredBrandsMap(baseId, apiKey, siRecordIds) {
  const siDataMap = await fetchStrategicIntentDataMap(baseId, apiKey, siRecordIds);
  return preferredBrandsMapFromSiDataMapResolved(baseId, apiKey, siDataMap);
}

/** Fetch Deal Brand Cache table and return Map(dealId -> { preferredBrandsChosen, preferredScore, matchScoresNewByBrand, topAlternatives, cacheRecordId }). Returns empty Map if table missing or error. */
async function fetchDealBrandCacheMap(baseId, apiKey) {
  const map = new Map();
  const table = encodeURIComponent(DEAL_BRAND_CACHE_TABLE);
  let offset = null;
  try {
    do {
      await waitAirtableSerial();
      let url = `https://api.airtable.com/v0/${baseId}/${table}?pageSize=100`;
      if (offset) url += "&offset=" + encodeURIComponent(offset);
      const res = await fetch(url, { headers: { Authorization: "Bearer " + apiKey } });
      const data = await res.json();
      if (data.error) {
        if (data.error.type === "NOT_FOUND" || (data.error.message && data.error.message.includes("Could not find"))) return map;
        throw new Error(data.error.message || data.error.type);
      }
      const records = data.records || [];
      for (const rec of records) {
        const fields = rec.fields || {};
        const dealLink = fields[DEAL_BRAND_CACHE_DEAL_FIELD];
        const dealId = Array.isArray(dealLink) && dealLink[0] ? dealLink[0] : dealLink;
        if (!dealId || typeof dealId !== "string") continue;
        const preferredBrands = fields[DEAL_BRAND_CACHE_PREFERRED_BRANDS_FIELD];
        const preferredScoresJson = fields[DEAL_BRAND_CACHE_PREFERRED_SCORES_FIELD];
        const preferredScoreNum = fields[DEAL_BRAND_CACHE_PREFERRED_SCORE_FIELD];
        const topAlternativesJson = fields[DEAL_BRAND_CACHE_TOP_ALTERNATIVES_FIELD];
        let matchScoresNewByBrand = {};
        if (preferredScoresJson && typeof preferredScoresJson === "string") {
          try {
            const parsed = JSON.parse(preferredScoresJson);
            if (parsed && typeof parsed === "object") matchScoresNewByBrand = parsed;
          } catch (_) { /* ignore */ }
        }
        let preferredScore = preferredScoreNum != null && preferredScoreNum !== "" ? Number(preferredScoreNum) : null;
        if (preferredScore == null && Object.keys(matchScoresNewByBrand).length > 0) {
          const firstKey = Object.keys(matchScoresNewByBrand)[0];
          preferredScore = matchScoresNewByBrand[firstKey] != null ? Number(matchScoresNewByBrand[firstKey]) : null;
        }
        let topAlternatives = null;
        if (topAlternativesJson && typeof topAlternativesJson === "string") {
          try {
            const parsed = JSON.parse(topAlternativesJson);
            if (Array.isArray(parsed)) topAlternatives = parsed;
          } catch (_) { /* ignore */ }
        }
        let breakdownNewDetailsByBrand = {};
        const breakdownJson = fields[DEAL_BRAND_CACHE_BREAKDOWN_FIELD];
        if (breakdownJson && typeof breakdownJson === "string") {
          try {
            const parsed = JSON.parse(breakdownJson);
            if (parsed && typeof parsed === "object") breakdownNewDetailsByBrand = parsed;
          } catch (_) { /* ignore */ }
        }
        const preferredBrandsChosen = typeof preferredBrands === "string" ? preferredBrands.trim() : "";
        map.set(dealId, {
          cacheRecordId: rec.id,
          preferredBrandsChosen: preferredBrandsChosen || undefined,
          preferredScore: preferredScore != null && !Number.isNaN(preferredScore) ? preferredScore : undefined,
          matchScoresNewByBrand: Object.keys(matchScoresNewByBrand).length > 0 ? matchScoresNewByBrand : undefined,
          breakdownNewDetailsByBrand: Object.keys(breakdownNewDetailsByBrand).length > 0 ? breakdownNewDetailsByBrand : undefined,
          topAlternatives,
        });
      }
      offset = data.offset || null;
    } while (offset);
  } catch (e) {
    if (e.message && (e.message.includes("NOT_FOUND") || e.message.includes("Could not find"))) return map;
    console.warn("Deal Brand Cache fetch failed (using live data):", e.message);
    return map;
  }
  return map;
}

/** Upsert Deal Brand Cache for one deal. Finds existing by fetchDealBrandCacheMap and PATCH, or POST new. */
async function upsertDealBrandCache(baseId, apiKey, dealId, payload) {
  const table = encodeURIComponent(DEAL_BRAND_CACHE_TABLE);
  const existing = await fetchDealBrandCacheMap(baseId, apiKey);
  const cached = existing.get(dealId);
  const fields = {
    [DEAL_BRAND_CACHE_NAME_FIELD]: payload.dealName != null ? String(payload.dealName).trim() || "Deal cache" : "Deal cache",
    [DEAL_BRAND_CACHE_DEAL_FIELD]: [dealId],
    [DEAL_BRAND_CACHE_PREFERRED_BRANDS_FIELD]: payload.preferredBrandsChosen || "",
    [DEAL_BRAND_CACHE_PREFERRED_SCORES_FIELD]: payload.matchScoresNewByBrand && Object.keys(payload.matchScoresNewByBrand).length > 0 ? JSON.stringify(payload.matchScoresNewByBrand) : "",
    [DEAL_BRAND_CACHE_TOP_ALTERNATIVES_FIELD]: payload.topAlternatives && payload.topAlternatives.length > 0 ? JSON.stringify(payload.topAlternatives.map((a) => ({ brand: a.brand, score: a.score, breakdownNewDetails: a.breakdownNewDetails || {} }))) : "",
    [DEAL_BRAND_CACHE_LAST_COMPUTED_FIELD]: new Date().toISOString(),
  };
  if (payload.breakdownNewDetailsByBrand && Object.keys(payload.breakdownNewDetailsByBrand).length > 0) {
    fields[DEAL_BRAND_CACHE_BREAKDOWN_FIELD] = JSON.stringify(payload.breakdownNewDetailsByBrand);
  }
  if (payload.preferredScore != null) fields[DEAL_BRAND_CACHE_PREFERRED_SCORE_FIELD] = payload.preferredScore;
  if (payload.bestMatchBrand != null) fields[DEAL_BRAND_CACHE_BEST_MATCH_BRAND_FIELD] = payload.bestMatchBrand;
  if (payload.bestMatchScore != null) fields[DEAL_BRAND_CACHE_BEST_MATCH_SCORE_FIELD] = payload.bestMatchScore;
  if (cached && cached.cacheRecordId) {
    const res = await fetch(`https://api.airtable.com/v0/${baseId}/${table}/${encodeURIComponent(cached.cacheRecordId)}`, {
      method: "PATCH",
      headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
      body: JSON.stringify({ fields }),
    });
    const data = await res.json();
    if (data.error) throw new Error(data.error.message || "Failed to update cache");
    return data;
  }
  const res = await fetch(`https://api.airtable.com/v0/${baseId}/${table}`, {
    method: "POST",
    headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
    body: JSON.stringify({ fields }),
  });
  const data = await res.json();
  if (data.error) throw new Error(data.error.message || "Failed to create cache record");
  return data;
}

async function fetchTableRecordsAll(baseId, apiKey, tableName, { useSerial = false } = {}) {
  const table = encodeURIComponent(tableName);
  let offset = null;
  const all = [];
  do {
    if (useSerial) await waitAirtableSerial();
    const url = offset
      ? `https://api.airtable.com/v0/${baseId}/${table}?pageSize=100&offset=${encodeURIComponent(offset)}`
      : `https://api.airtable.com/v0/${baseId}/${table}?pageSize=100`;
    const res = await fetch(url, { headers: { Authorization: "Bearer " + apiKey } });
    const data = await res.json();
    if (data.error) throw new Error(data.error.message || `Failed fetching ${tableName}`);
    all.push(...(data.records || []));
    offset = data.offset || null;
  } while (offset);
  return all;
}

async function fetchInitialMatchedSupportState(baseId, apiKey, deals) {
  const dealSet = new Set((deals || []).map((d) => d.id).filter(Boolean));
  if (dealSet.size === 0) {
    return {
      contactedPairs: [],
      targetListByDeal: {},
      tabCounts: { contacted: 0, dealCompare: 0 },
    };
  }

  const [bdrRecords, targetRecords] = await Promise.all([
    fetchTableRecordsAll(baseId, apiKey, BRAND_DEAL_REQUESTS_TABLE, { useSerial: false }).catch(() => []),
    fetchTableRecordsAll(baseId, apiKey, TARGET_LIST_TABLE, { useSerial: false }).catch(() => []),
  ]);

  const contactedPairs = bdrRecords
    .map((r) => {
      const f = r.fields || {};
      const dealArr = Array.isArray(f.Deal) ? f.Deal : [];
      const dealId = dealArr.find((id) => dealSet.has(id));
      if (!dealId) return null;
      const proposalStatus = valueToStr(f["Proposal Status"]) || "";
      return {
        id: r.id,
        dealId,
        brandName: valueToStr(f["Brand Name"]) || "",
        status: valueToStr(f["Status"]) || "New",
        proposal: proposalStatus ? { proposalStatus } : undefined,
      };
    })
    .filter(Boolean);

  const targetListByDeal = {};
  for (const rec of targetRecords) {
    const f = rec.fields || {};
    const dealArr = Array.isArray(f.Deal_ID) ? f.Deal_ID : [];
    const dealId = dealArr.find((id) => dealSet.has(id));
    if (!dealId) continue;
    if (!targetListByDeal[dealId]) targetListByDeal[dealId] = [];
    targetListByDeal[dealId].push({
      id: rec.id,
      dealId,
      brandName: valueToStr(f["Brand Name"]) || "",
      status: valueToStr(f["Status"]) || "Considering",
      matchScore: f["Match Score"] ?? null,
    });
  }

  const dealCompareCount = deals.filter((d) => {
    const did = d.id;
    if (!did) return false;
    return contactedPairs.some((c) => c.dealId === did && c.proposal && String(c.proposal.proposalStatus || "").trim() === "Submitted");
  }).length;

  return {
    contactedPairs,
    targetListByDeal,
    tabCounts: { contacted: contactedPairs.length, dealCompare: dealCompareCount },
  };
}

/** Convert Strategic Intent Airtable record to form field names and values (arrays → comma-sep for multi-select). */
function strategicIntentToFormFields(siFields) {
  if (!siFields || typeof siFields !== "object") return {};
  const merge = {};
  for (const formName of STRATEGIC_INTENT_FORM_FIELDS) {
    const airtableKey = SI_FORM_TO_AIRTABLE[formName] ?? formName;
    let val = siFields[airtableKey];
    if (val === undefined) continue;
    if (
      formName === "Preferred Brands (up to 4)" ||
      formName === "Preferred Chain Scales" ||
      formName === "Preferred Third-Party Operator Profile" ||
      formName === "Services Required From Operator" ||
      formName === "Top 3 Success Metrics" ||
      formName === "Top Priorities for Project" ||
      formName === "Top Concerns for this Project" ||
      formName === "Top 3 Deal Breakers" ||
      formName === "Must-haves From Brand or Operator" ||
      formName === "Incentive Types Interested In"
    ) {
      const arr = Array.isArray(val)
        ? val.map((v) => (typeof v === "string" ? v : (v && v.name) || "").trim()).filter(Boolean)
        : typeof val === "string" && String(val).trim()
          ? String(val).split(/\s*,\s*/).map((s) => s.trim()).filter(Boolean)
          : [];
      merge[formName] = arr.length > 0 ? arr.join(", ") : "";
    } else {
      const s = val == null ? "" : typeof val === "string" ? val.trim() : String(val);
      merge[formName] = s;
    }
  }
  return merge;
}

/** Build Strategic Intent Airtable payload from form fields. Preferred Chain Scales is single-select in form → send string. Others as array when multi. */
function formFieldsToStrategicIntentPayload(fields) {
  const payload = {};
  for (const formName of STRATEGIC_INTENT_FORM_FIELDS) {
    const val = fields[formName];
    if (val === undefined) continue;
    const airtableKey = SI_FORM_TO_AIRTABLE[formName] ?? formName;
    if (formName === "Preferred Chain Scales") {
      const s = val == null ? "" : Array.isArray(val) ? (val[0] != null ? String(val[0]).trim() : "") : String(val).trim();
      if (s !== "") payload[airtableKey] = s;
    } else if (
      formName === "Preferred Brands (up to 4)" ||
      formName === "Preferred Third-Party Operator Profile" ||
      formName === "Services Required From Operator" ||
      formName === "Top 3 Success Metrics" ||
      formName === "Top Priorities for Project" ||
      formName === "Top Concerns for this Project" ||
      formName === "Top 3 Deal Breakers" ||
      formName === "Must-haves From Brand or Operator" ||
      formName === "Incentive Types Interested In"
    ) {
      const arr = Array.isArray(val)
        ? val.map((v) => (typeof v === "string" ? v : (v && v.name) || "").trim()).filter(Boolean)
        : typeof val === "string" && String(val).trim()
          ? String(val).split(/\s*,\s*/).map((s) => s.trim()).filter(Boolean)
          : [];
      payload[airtableKey] = arr;
    } else if (formName.endsWith(" Importance")) {
      const num = typeof val === "number" && !Number.isNaN(val) ? val : parseInt(String(val), 10);
      if (!Number.isNaN(num)) payload[airtableKey] = Math.min(5, Math.max(1, num));
    } else {
      const s = val == null ? "" : typeof val === "string" ? val.trim() : String(val);
      payload[airtableKey] = s;
    }
  }
  return payload;
}

/** Get linked Contact & Uploads record ID from deal fields. */
function getLinkedContactUploadsId(fields) {
  if (!fields) return null;
  const raw = fields[CONTACT_UPLOADS_LINK_FIELD];
  if (!Array.isArray(raw) || raw.length === 0) return null;
  const id = typeof raw[0] === "string" ? raw[0] : raw[0]?.id;
  return id && typeof id === "string" && id.startsWith("rec") ? id : null;
}

/** Get linked Lease Structure record ID from deal fields. */
function getLinkedLeaseStructureId(fields) {
  if (!fields) return null;
  const raw = fields[LEASE_STRUCTURE_LINK_FIELD];
  if (!Array.isArray(raw) || raw.length === 0) return null;
  const id = typeof raw[0] === "string" ? raw[0] : raw[0]?.id;
  return id && typeof id === "string" && id.startsWith("rec") ? id : null;
}

/** Fetch a single record from Lease Structure table (raw Airtable fields). */
async function fetchLeaseStructureRecord(baseId, apiKey, recordId) {
  const table = encodeURIComponent(LEASE_STRUCTURE_TABLE);
  const url = `https://api.airtable.com/v0/${baseId}/${table}/${encodeURIComponent(recordId)}`;
  const res = await fetch(url, { headers: { Authorization: "Bearer " + apiKey } });
  const data = await res.json();
  if (data.error || !data.fields) return null;
  return data.fields;
}

/** Find Lease Structure record ID that is linked to the given deal (when Deals table has no reverse link). */
async function findLeaseStructureRecordIdByDealId(baseId, apiKey, dealRecordId) {
  const table = encodeURIComponent(LEASE_STRUCTURE_TABLE);
  const linkFieldNames = [LS_DEAL_LINK_FIELD, "Deals", "Deal"];
  let offset = null;
  do {
    const url = offset
      ? `https://api.airtable.com/v0/${baseId}/${table}?pageSize=100&offset=${encodeURIComponent(offset)}`
      : `https://api.airtable.com/v0/${baseId}/${table}?pageSize=100`;
    const res = await fetch(url, { headers: { Authorization: "Bearer " + apiKey } });
    const data = await res.json();
    if (data.error || !Array.isArray(data.records)) return null;
    for (const rec of data.records) {
      const f = rec.fields || {};
      for (const linkName of linkFieldNames) {
        const raw = f[linkName];
        const ids = Array.isArray(raw) ? raw.map((x) => (typeof x === "string" ? x : x?.id)).filter(Boolean) : [];
        if (ids.includes(dealRecordId)) return rec.id;
      }
    }
    offset = data.offset || null;
  } while (offset);
  return null;
}

/** Convert Lease Structure Airtable record to form field names (for merge into deal.fields). */
function leaseStructureToFormFields(lsFields) {
  if (!lsFields || typeof lsFields !== "object") return {};
  const merge = {};
  for (const [airtableKey, val] of Object.entries(lsFields)) {
    if (val === undefined || val === null) continue;
    const formName = LS_AIRTABLE_TO_FORM[airtableKey] ?? airtableKey;
    if (!LEASE_STRUCTURE_FORM_FIELDS.includes(formName)) continue;
    merge[formName] = typeof val === "string" ? val.trim() : val;
  }
  for (const formName of LEASE_STRUCTURE_FORM_FIELDS) {
    if (merge[formName] !== undefined) continue;
    const airtableKey = LS_FORM_TO_AIRTABLE[formName] ?? formName;
    let val = lsFields[airtableKey];
    if (val === undefined) val = lsFields[formName];
    if (val === undefined) continue;
    merge[formName] = typeof val === "string" ? val.trim() : val;
  }
  return merge;
}

/** Build Lease Structure Airtable payload from form fields. Coerces date fields to YYYY-MM-DD. */
function formFieldsToLeaseStructurePayload(fields) {
  const payload = {};
  for (const formName of LEASE_STRUCTURE_FORM_FIELDS) {
    let val = fields[formName];
    if (val === undefined) continue;
    const airtableKey = LS_FORM_TO_AIRTABLE[formName] ?? formName;
    if (formName === "Lease Start Date (or Availability)" || formName === "Lease Expiration or End Date") {
      const coerced = toAirtableDateString(typeof val === "string" ? val : String(val));
      if (coerced && /^\d{4}-\d{2}-\d{2}$/.test(coerced)) payload[airtableKey] = coerced;
      else if (typeof val === "string" && val.trim() !== "") payload[airtableKey] = val.trim();
      continue;
    }
    payload[airtableKey] = typeof val === "string" ? val.trim() : val;
  }
  return payload;
}

/** Fetch a single record from Contact & Uploads table (raw Airtable fields). Returns null if the API returns non-JSON (e.g. HTML error page) or error. */
async function fetchContactUploadsRecord(baseId, apiKey, recordId) {
  await waitAirtableSerial();
  const table = encodeURIComponent(CONTACT_UPLOADS_TABLE);
  const url = `https://api.airtable.com/v0/${baseId}/${table}/${encodeURIComponent(recordId)}`;
  const res = await fetch(url, { headers: { Authorization: "Bearer " + apiKey } });
  const text = await res.text();
  if (!res.ok || !text.trim().startsWith("{")) {
    if (process.env.NODE_ENV !== "test" && !res.ok) {
      console.warn("[Contact & Uploads] Fetch failed for", recordId, "status:", res.status, "body preview:", text.slice(0, 80));
    }
    return null;
  }
  let data;
  try {
    data = JSON.parse(text);
  } catch (e) {
    if (process.env.NODE_ENV !== "test") console.warn("[Contact & Uploads] Invalid JSON for", recordId, e.message);
    return null;
  }
  if (data.error || !data.fields) return null;
  return data.fields;
}

/** Collect unique linked Contact & Uploads record IDs from deal records. */
function collectLinkedContactUploadsIds(records) {
  const ids = new Set();
  for (const rec of records) {
    const id = getLinkedContactUploadsId(rec.fields || {});
    if (id) ids.add(id);
  }
  return [...ids];
}

const CU_FETCH_BATCH_SIZE = 5;
const MY_DEALS_CU_FETCH_DELAY_MS = Math.max(0, parseInt(process.env.MY_DEALS_CU_FETCH_DELAY_MS || "220", 10) || 220);

/** Fetch Contact & Uploads records by ID; return Map(cuRecordId -> raw Airtable fields). Batched for speed. */
async function fetchContactUploadsDataMap(baseId, apiKey, cuRecordIds) {
  const map = new Map();
  for (let i = 0; i < cuRecordIds.length; i += CU_FETCH_BATCH_SIZE) {
    const batch = cuRecordIds.slice(i, i + CU_FETCH_BATCH_SIZE);
    const results = await Promise.all(batch.map(async (id) => {
      const fields = await fetchContactUploadsRecord(baseId, apiKey, id);
      return { id, fields };
    }));
    for (const { id, fields } of results) {
      if (fields && typeof fields === "object") map.set(id, fields);
    }
    if (i + CU_FETCH_BATCH_SIZE < cuRecordIds.length) {
      await new Promise((r) => setTimeout(r, MY_DEALS_CU_FETCH_DELAY_MS));
    }
  }
  return map;
}

/** Convert Contact & Uploads Airtable record to form field names (for merge into deal.fields). Attachment field passed through as array for Tab 13 list UI. */
function contactUploadsToFormFields(cuFields) {
  if (!cuFields || typeof cuFields !== "object") return {};
  const merge = {};
  for (const formName of CONTACT_UPLOADS_FORM_FIELDS) {
    const airtableKey = formName === "Upload Supporting Docs" ? CU_ATTACHMENT_FIELD : (CU_FORM_TO_AIRTABLE[formName] ?? formName);
    let val = cuFields[airtableKey];
    if (val === undefined) continue;
    if (formName === "Upload Supporting Docs" && Array.isArray(val)) {
      merge[formName] = val;
      continue;
    }
    const s = val == null ? "" : typeof val === "string" ? val.trim() : Array.isArray(val) ? (val.map((v) => (typeof v === "string" ? v : (v && v.name) || "").trim()).filter(Boolean).join(", ")) : String(val);
    merge[formName] = s;
  }
  return merge;
}

/** Build Contact & Uploads Airtable payload from form fields. Skips file inputs (non-serializable). */
function formFieldsToContactUploadsPayload(fields) {
  const payload = {};
  for (const formName of CONTACT_UPLOADS_FORM_FIELDS) {
    const val = fields[formName];
    if (val === undefined) continue;
    const airtableKey = formName === "Upload Supporting Docs" ? CU_ATTACHMENT_FIELD : (CU_FORM_TO_AIRTABLE[formName] ?? formName);
    if (formName === "Upload Supporting Docs") {
      if (Array.isArray(val) && val.length > 0) {
        const items = val.map((v) => {
          if (typeof v === "object" && v && v.url) return { url: v.url, filename: v.filename };
          if (typeof v === "string" && v) return { url: v };
          return null;
        }).filter(Boolean);
        if (items.length) payload[CU_ATTACHMENT_FIELD] = items;
      }
      continue;
    }
    const s = val == null ? "" : typeof val === "string" ? val.trim() : String(val);
    payload[airtableKey] = s;
  }
  return payload;
}

function getRawLink(fields) {
  if (!fields) return null;
  return fields[LOCATION_LINK_FIELD] ?? fields[LOCATION_LINK_ALIAS];
}

/** Get linked Location & Property ref from deal fields. Returns Airtable record ID (recXXX) or Location_Property_ID (number) for lookup. */
function getLinkedLocationId(fields) {
  const raw = getRawLink(fields);
  if (raw == null) return null;
  if (Array.isArray(raw) && raw.length > 0) {
    const v = raw[0];
    const id = typeof v === "string" ? v : v?.id;
    if (id && typeof id === "string" && id.startsWith("rec")) return id;
    const num = typeof v === "number" ? v : (typeof id === "number" ? id : parseInt(String(v), 10));
    if (!Number.isNaN(num)) return num;
  }
  const num = typeof raw === "number" ? raw : parseInt(String(raw), 10);
  if (!Number.isNaN(num)) return num;
  return null;
}

/** Get linked Market - Performance - Deal & Capital Structure record ID from deal fields. */
function getLinkedMarketPerformanceId(fields) {
  if (!fields) return null;
  const raw = fields[MARKET_PERFORMANCE_LINK_FIELD];
  if (!Array.isArray(raw) || raw.length === 0) return null;
  const id = typeof raw[0] === "string" ? raw[0] : raw[0]?.id;
  return id && typeof id === "string" && id.startsWith("rec") ? id : null;
}

/** Fetch a single record from Market - Performance - Deal & Capital Structure table (raw Airtable fields). Retries on 429/5xx; drains body before retry to avoid stream corruption. */
async function fetchMarketPerformanceRecord(baseId, apiKey, recordId, opts = {}) {
  let retriesLeft = opts.retries ?? 2;
  const stats = opts.stats ?? null;
  await waitAirtableSerial();
  const table = encodeURIComponent(MARKET_PERFORMANCE_TABLE);
  const url = `https://api.airtable.com/v0/${baseId}/${table}/${encodeURIComponent(recordId)}`;
  let res = await fetch(url, { headers: { Authorization: "Bearer " + apiKey } });
  while ((res.status === 429 || res.status >= 500) && retriesLeft > 0) {
    if (res.status === 429 && stats) stats.mpFetch429Count = (stats.mpFetch429Count || 0) + 1;
    await res.text();
    const backoff = res.status === 429 ? 2500 : (retriesLeft === 2 ? 2500 : 4000);
    if (shouldLogMyDealsSummary() && res.status === 429) {
      console.warn("getMyDeals: MP fetch 429, backing off", { recordId, backoffMs: backoff });
    }
    await new Promise((r) => setTimeout(r, backoff));
    await waitAirtableSerial();
    res = await fetch(url, { headers: { Authorization: "Bearer " + apiKey } });
    retriesLeft -= 1;
  }
  const data = await res.json();
  if (data.error || !data.fields) return null;
  return data.fields;
}

/** Collect unique linked Market - Performance record IDs from deal records. */
function collectLinkedMarketPerformanceIds(records) {
  const ids = new Set();
  for (const rec of records) {
    const id = getLinkedMarketPerformanceId(rec.fields || {});
    if (id) ids.add(id);
  }
  return [...ids];
}

/** Configurable MP fetch pacing (env-backed; default preserves current behavior). */
const MY_DEALS_MP_FETCH_DELAY_MS = Math.max(0, parseInt(process.env.MY_DEALS_MP_FETCH_DELAY_MS || "1000", 10) || 1000);
const MY_DEALS_MP_CONCURRENCY = Math.min(3, Math.max(1, parseInt(process.env.MY_DEALS_MP_CONCURRENCY || "1", 10) || 1));

/** Configurable My Deals pacing (env-backed; current behavior is default). */
const MY_DEALS_COLD_START_DELAY_MS = parseInt(process.env.MY_DEALS_COLD_START_DELAY_MS || "2000", 10) || 2000;
const MY_DEALS_MIN_GAP_MS = parseInt(process.env.MY_DEALS_MIN_GAP_MS || "5000", 10) || 5000;
const MY_DEALS_PHASE_GAP_MS = parseInt(process.env.MY_DEALS_PHASE_GAP_MS || "100", 10) || 100;
// Performance: keep retry-rebuild optional and off by default so one blank field does not block response.
const MY_DEALS_ENABLE_RETRY_REBUILD = /^(1|true|on|yes)$/i.test(String(process.env.MY_DEALS_ENABLE_RETRY_REBUILD || "0"));

/** Phase 5: Full linked-table batching. Default ON for fast mode; set to 0/false/off/no to disable. */
const MY_DEALS_USE_BATCHED_LINKED_FETCHES = !/^(0|false|off|no)$/i.test(String(process.env.MY_DEALS_USE_BATCHED_LINKED_FETCHES ?? "1"));

/** Phase 6: Parallel batched linked fetches (SI/Location/CU). Default ON; set to 0/false/off/no to disable. Only applies when batched is also on. */
const MY_DEALS_USE_PARALLEL_BATCHED_LINKED_FETCHES = !/^(0|false|off|no)$/i.test(String(process.env.MY_DEALS_USE_PARALLEL_BATCHED_LINKED_FETCHES ?? "1"));
const MY_DEALS_BATCH_FETCH_CHUNK_SIZE = Math.min(15, Math.max(1, parseInt(process.env.MY_DEALS_BATCH_FETCH_CHUNK_SIZE || "10", 10) || 10));
const MY_DEALS_BATCH_FETCH_DELAY_MS = Math.max(0, parseInt(process.env.MY_DEALS_BATCH_FETCH_DELAY_MS || "100", 10) || 100);
function getBatchChunkSize(phaseName) {
  const override = process.env[`MY_DEALS_${phaseName}_BATCH_FETCH_CHUNK_SIZE`];
  if (override != null && override !== "") {
    const n = parseInt(override, 10);
    if (!Number.isNaN(n) && n >= 1) return Math.min(15, n);
  }
  return MY_DEALS_BATCH_FETCH_CHUNK_SIZE;
}
function getBatchDelayMs(phaseName) {
  const override = process.env[`MY_DEALS_${phaseName}_BATCH_FETCH_DELAY_MS`];
  if (override != null && override !== "") {
    const n = parseInt(override, 10);
    if (!Number.isNaN(n) && n >= 0) return n;
  }
  return MY_DEALS_BATCH_FETCH_DELAY_MS;
}

/** Phase 5: Generic batched Airtable fetch by record IDs (filterByFormula OR(RECORD_ID()='rec1', ...)). Returns { map, stats }; map: id -> { id, fields }; stats: linkedIds, fetched, missing, chunks, chunkSizeUsed, fetch429Count, retries. */
async function fetchAirtableRecordsByIdsBatched({ baseId, apiKey, tableName, recordIds, chunkSize, delayMs, stats, phaseName }) {
  const ids = [...new Set(recordIds.filter((id) => id && typeof id === "string" && id.startsWith("rec")))];

  const result = new Map();
  const s = stats || { linkedIds: ids.length, fetched: 0, missing: 0, chunks: 0, chunkSizeUsed: chunkSize, fetch429Count: 0, retries: 0 };

  if (ids.length === 0) {
    s.fetched = 0;
    s.missing = 0;
    s.chunks = 0;
    return { map: result, stats: s };
  }

  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);
    const formula = "OR(" + chunk.map((id) => "RECORD_ID()='" + String(id).replace(/'/g, "\\'") + "'").join(",") + ")";
    const table = encodeURIComponent(tableName);
    const url = `https://api.airtable.com/v0/${baseId}/${table}?filterByFormula=${encodeURIComponent(formula)}&maxRecords=${chunk.length * 2}`;

    let retriesLeft = 2;
    let res = null;

    while (retriesLeft >= 0) {
      await waitAirtableSerial();
      res = await fetch(url, { headers: { Authorization: "Bearer " + apiKey } });

      if (res.status === 429 || res.status >= 500) {
        s.fetch429Count = (s.fetch429Count || 0) + (res.status === 429 ? 1 : 0);
        s.retries = (s.retries || 0) + 1;
        await res.text();
        const backoff = res.status === 429 ? 2500 : 2500;
        if (shouldLogMyDealsSummary()) {
          console.warn("getMyDeals: batched fetch 429/5xx, backing off", { phaseName, status: res.status, backoffMs: backoff });
        }
        await new Promise((r) => setTimeout(r, backoff));
        retriesLeft -= 1;
        continue;
      }
      break;
    }

    let data;
    try {
      data = await res.json();
    } catch (e) {
      if (shouldLogMyDealsSummary()) console.warn("getMyDeals: batched fetch JSON parse error", { phaseName, chunkLen: chunk.length });
      s.missing = (s.missing || 0) + chunk.length;
      s.chunks = (s.chunks || 0) + 1;
      if (i + chunkSize < ids.length) await new Promise((r) => setTimeout(r, delayMs));
      continue;
    }

    if (data.error) {
      if (shouldLogMyDealsSummary()) console.warn("getMyDeals: batched fetch Airtable error", { phaseName, error: (data.error && data.error.message) || data.error });
      s.missing = (s.missing || 0) + chunk.length;
    } else {
      const records = data.records || [];
      for (const rec of records) {
        if (rec && rec.id && rec.fields) result.set(rec.id, { id: rec.id, fields: rec.fields });
      }
    }
    s.chunks = (s.chunks || 0) + 1;
    if (i + chunkSize < ids.length) await new Promise((r) => setTimeout(r, delayMs));
  }

  s.fetched = result.size;
  s.missing = ids.length - result.size;
  return { map: result, stats: s };
}

/** Phase 5: Batched MP fetch. Returns { map: Map(id->fields), stats }. */
async function fetchMarketPerformanceDataMapBatched(baseId, apiKey, mpRecordIds) {
  const chunkSize = getBatchChunkSize("MP");
  const delayMs = getBatchDelayMs("MP");
  const stats = { linkedIds: mpRecordIds.length, fetched: 0, missing: 0, chunks: 0, chunkSizeUsed: chunkSize, fetch429Count: 0, retries: 0 };
  const { map: rawMap, stats: s } = await fetchAirtableRecordsByIdsBatched({
    baseId, apiKey, tableName: MARKET_PERFORMANCE_TABLE, recordIds: mpRecordIds, chunkSize, delayMs, stats, phaseName: "mp",
  });
  const map = new Map();
  for (const [id, rec] of rawMap) {
    if (rec && rec.fields) map.set(id, rec.fields);
  }
  const mpStats = { mpFetch429Count: s.fetch429Count || 0 };
  return { map, stats: { ...s, ...mpStats } };
}

/** Phase 5: Batched SI fetch. Returns { map: Map(id->fields), stats }. */
async function fetchStrategicIntentDataMapBatched(baseId, apiKey, siRecordIds) {
  const chunkSize = getBatchChunkSize("SI");
  const delayMs = getBatchDelayMs("SI");
  const stats = { linkedIds: siRecordIds.length, fetched: 0, missing: 0, chunks: 0, chunkSizeUsed: chunkSize, fetch429Count: 0, retries: 0 };
  const { map: rawMap, stats: s } = await fetchAirtableRecordsByIdsBatched({
    baseId, apiKey, tableName: STRATEGIC_INTENT_TABLE, recordIds: siRecordIds, chunkSize, delayMs, stats, phaseName: "si",
  });
  const map = new Map();
  for (const [id, rec] of rawMap) {
    if (rec && rec.fields) map.set(id, rec.fields);
  }
  return { map, stats: s };
}

/** Phase 5: Batched Location fetch. Returns { map: Map(id->formObj), stats }. */
async function fetchLocationMapBatched(baseId, apiKey, recordIds) {
  const chunkSize = getBatchChunkSize("LOCATION");
  const delayMs = getBatchDelayMs("LOCATION");
  const stats = { linkedIds: recordIds.length, fetched: 0, missing: 0, chunks: 0, chunkSizeUsed: chunkSize, fetch429Count: 0, retries: 0 };
  const { map: rawMap, stats: s } = await fetchAirtableRecordsByIdsBatched({
    baseId, apiKey, tableName: LOCATION_PROPERTY_TABLE, recordIds, chunkSize, delayMs, stats, phaseName: "location",
  });
  const map = new Map();
  for (const [id, rec] of rawMap) {
    if (rec && rec.fields) {
      const formObj = locationFieldsToFormFields(rec.fields);
      if (formObj) map.set(id, formObj);
    }
  }
  s.fetched = map.size;
  s.missing = recordIds.length - map.size;
  return { map, stats: s };
}

/** Phase 5: Batched CU fetch. Returns { map: Map(id->fields), stats }. */
async function fetchContactUploadsDataMapBatched(baseId, apiKey, cuRecordIds) {
  const chunkSize = getBatchChunkSize("CU");
  const delayMs = getBatchDelayMs("CU");
  const stats = { linkedIds: cuRecordIds.length, fetched: 0, missing: 0, chunks: 0, chunkSizeUsed: chunkSize, fetch429Count: 0, retries: 0 };
  const { map: rawMap, stats: s } = await fetchAirtableRecordsByIdsBatched({
    baseId, apiKey, tableName: CONTACT_UPLOADS_TABLE, recordIds: cuRecordIds, chunkSize, delayMs, stats, phaseName: "cu",
  });
  const map = new Map();
  for (const [id, rec] of rawMap) {
    if (rec && rec.fields) map.set(id, rec.fields);
  }
  return { map, stats: s };
}

/** Fetch full Market - Performance record for each ID; return { map, stats }. Configurable delay and concurrency via env. */
async function fetchMarketPerformanceDataMap(baseId, apiKey, mpRecordIds) {
  const map = new Map();
  const stats = { mpFetch429Count: 0 };
  const concurrency = MY_DEALS_MP_CONCURRENCY;
  const delayMs = MY_DEALS_MP_FETCH_DELAY_MS;
  const opts = { retries: 2, stats };

  if (concurrency <= 1) {
    for (let i = 0; i < mpRecordIds.length; i++) {
      const id = mpRecordIds[i];
      const fields = await fetchMarketPerformanceRecord(baseId, apiKey, id, opts);
      if (fields && typeof fields === "object") map.set(id, fields);
      if (i < mpRecordIds.length - 1) {
        await new Promise((r) => setTimeout(r, delayMs));
      }
    }
  } else {
    for (let i = 0; i < mpRecordIds.length; i += concurrency) {
      const batch = mpRecordIds.slice(i, i + concurrency);
      const results = await Promise.all(
        batch.map(async (id) => {
          const fields = await fetchMarketPerformanceRecord(baseId, apiKey, id, opts);
          return { id, fields };
        })
      );
      for (const { id, fields } of results) {
        if (fields && typeof fields === "object") map.set(id, fields);
      }
      if (i + concurrency < mpRecordIds.length) {
        await new Promise((r) => setTimeout(r, delayMs));
      }
    }
  }
  return { map, stats };
}

/** Prefer full MP data when building deal list; derive Preferred Deal Structure map from it. */
function preferredDealStructureMapFromMpDataMap(mpDataMap) {
  const map = new Map();
  for (const [id, fields] of mpDataMap) {
    const val = valueToStr(fields["Preferred Deal Structure"]) || "";
    if (val) map.set(id, val);
  }
  return map;
}

/** Fetch Preferred Deal Structure for each MP record ID; return Map(mpRecordId -> preferredDealStructure string). */
async function fetchPreferredDealStructureMap(baseId, apiKey, mpRecordIds) {
  const { map } = await fetchMarketPerformanceDataMap(baseId, apiKey, mpRecordIds);
  return preferredDealStructureMapFromMpDataMap(map);
}

/** Location form keys that are multi-select (array) in Airtable. */
const LOCATION_MULTI_SELECT_FORM_KEYS = new Set(["Ownership Type", "Access to Transit or Highway", "F&B Program Type"]);

/** Convert raw Location Airtable fields to form-keyed object (M3). Used by fetchLocationRecord and batched Location phase. */
function locationFieldsToFormFields(f) {
  if (!f || typeof f !== "object") return null;
  const out = {};
  for (const formKey of LOCATION_FORM_FIELDS) {
    const airtableCol = LOCATION_FORM_TO_AIRTABLE[formKey];
    const raw = f[airtableCol];
    if (LOCATION_MULTI_SELECT_FORM_KEYS.has(formKey)) {
      const arr = Array.isArray(raw) ? raw.map((v) => (typeof v === "string" ? v : (v && v.name) || "")).filter(Boolean) : (valueToStr(raw) ? [valueToStr(raw)] : []);
      out[formKey] = arr;
    } else {
      out[formKey] = valueToStr(raw) || "";
    }
  }
  return out;
}

/** Fetch a single record from Location & Property table. Returns form-keyed object (M3: derived from LOCATION_FORM_FIELDS + LOCATION_FORM_TO_AIRTABLE). locationRef can be Airtable record ID (recXXX) or Location_Property_ID (number). */
async function fetchLocationRecord(baseId, apiKey, locationRef, retries = 2) {
  await waitAirtableSerial();
  const table = encodeURIComponent(LOCATION_PROPERTY_TABLE);
  const isRecordId = typeof locationRef === "string" && locationRef.startsWith("rec");
  let url;
  if (isRecordId) {
    url = `https://api.airtable.com/v0/${baseId}/${table}/${encodeURIComponent(locationRef)}`;
  } else {
    const idVal = typeof locationRef === "number" ? locationRef : parseInt(String(locationRef), 10);
    if (Number.isNaN(idVal)) return null;
    const formula = encodeURIComponent("{" + LOCATION_PROPERTY_ID_FIELD + "} = " + idVal);
    url = `https://api.airtable.com/v0/${baseId}/${table}?filterByFormula=${formula}&maxRecords=1`;
  }
  let res = await fetch(url, { headers: { Authorization: "Bearer " + apiKey } });
  while ((res.status === 429 || res.status >= 500) && retries > 0) {
    await new Promise((r) => setTimeout(r, retries === 2 ? 1000 : 2000));
    await waitAirtableSerial();
    res = await fetch(url, { headers: { Authorization: "Bearer " + apiKey } });
    retries -= 1;
  }
  const data = await res.json();
  const record = isRecordId ? data : (data.records && data.records[0]);
  if (data.error || !record || !record.fields) return null;
  return locationFieldsToFormFields(record.fields);
}

/** Collect all unique linked Location & Property record IDs from deal records. */
function collectLinkedLocationIds(records) {
  const ids = new Set();
  for (const rec of records) {
    const raw = getRawLink(rec.fields);
    if (Array.isArray(raw)) {
      for (const item of raw) {
        const id = typeof item === "string" ? item : item?.id;
        if (id && typeof id === "string" && id.startsWith("rec")) ids.add(id);
      }
    }
  }
  return [...ids];
}

const LOCATION_FETCH_BATCH_SIZE = 5;
const MY_DEALS_LOCATION_FETCH_DELAY_MS = Math.max(0, parseInt(process.env.MY_DEALS_LOCATION_FETCH_DELAY_MS || "220", 10) || 220);

/** Fetch multiple Location & Property records; return Map(recordId -> { city, country, hotelType, ... }). */
async function fetchLocationMap(baseId, apiKey, recordIds) {
  const map = new Map();
  for (let i = 0; i < recordIds.length; i += LOCATION_FETCH_BATCH_SIZE) {
    const batch = recordIds.slice(i, i + LOCATION_FETCH_BATCH_SIZE);
    const results = await Promise.all(
      batch.map(async (id) => {
        const loc = await fetchLocationRecord(baseId, apiKey, id);
        return { id, loc };
      })
    );
    for (const { id, loc } of results) {
      if (loc) map.set(id, loc);
    }
    if (i + LOCATION_FETCH_BATCH_SIZE < recordIds.length) {
      await new Promise((r) => setTimeout(r, MY_DEALS_LOCATION_FETCH_DELAY_MS));
    }
  }
  return map;
}

/** Format "City, Country" for display. */
function formatCityCountry(city, country) {
  const parts = [city, country].filter(Boolean);
  return parts.length ? parts.join(", ") : "";
}

/** Required field names in deal-setup form (reference only; Data Comp. % uses the form’s UI-required fields only, computed client-side). */
const REQUIRED_DEAL_SETUP_FIELDS = [
  "Property Name",
  "Project Type",
  "Stage of Development",
  "Has there ever been a franchise, branded management, affiliation or similar agreeement pertaining to the proposed hotel or site?",
  "Is the hotel currently branded?",
  "Is the hotel currently managed by a third-party operator?",
  "Are you open to lesser-known or emerging brands with favorable terms?",
  "Have you worked with any of your preferred brands/operators before?",
  "Full Address",
  "City & State",
  "Country",
  "Hotel Chain Scale",
  "Hotel Type",
  "Hotel Submarket & Location",
  "Hotel Service Model",
  "Ownership/Brand History or Track Record",
  "Zoned for Hotel Development",
  "Site/Development Restrictions?",
  "Total Site Size",
  "Total Site Size Unit",
  "Max height Allowed By Zoning",
  "Max height Unit",
  "Current Form of Site Control",
  "Ownership Type",
  "Zoning Status",
  "Parking Ratio",
  "Access to Transit or Highway",
  "Total Number of Rooms/Keys",
  "Number of Standard Rooms",
  "Number of Suites",
  "Building Type",
  "Number of Stories",
  "F&B Outlets?",
  "Meeting Space",
  "Number of Meeting Rooms",
  "Condo Residences?",
  "Hotel Rental Program?",
  "Parking Amenities?",
  "Additional Amenities",
  "Primary Demand Drivers",
  "Primary Demand Drivers Other",
  "Estimated or Actual RevPAR",
  "Regulatory or Permitting Issues?",
  "Regulatory or Permitting Issues Description",
  "Key Competitors",
  "Group vs Transient Mix",
  "Total Project Cost Range",
  "PIP Budget Range (if conversion)",
  "Equity vs Debt Split",
  "Ownership Structure",
  "Preferred Deal Structure",
  "PIP / CapEx Status",
  "Lease Type",
  "Soft vs Hard Brand Preference",
  "Preferred Brands (up to 4)",
  "IRR/Yield Goals",
  "Open to Outside Capital or Partnerships?",
  "Plan to Self-Manage or Hire Third Party?",
  "Preferred Chain Scales",
  "Open to Soft Brand First Then Reflag?",
  "Target Guest Segment",
  "Brand Flexibility vs Prestige",
  "Planned Hold Period",
  "Primary Goal for the Hotel",
  "Who should receive bids for this project?",
  "Minimum Operator Experience (years)",
  "Preferred Third-Party Operators (names)",
  "Preferred Third-Party Operator Profile",
  "Services Required From Operator",
  "Other Operator Criteria or Notes",
  "Level of Involvement in Day-to-Day Ops",
  "Top Priorities for Project",
  "Top Concerns for this Project",
  "Top 3 Success Metrics",
  "Top 3 Deal Breakers",
  "Must-haves From Brand or Operator",
  "Decision Timeline for Brand/Operator",
  "Would you like to filter out brands without key money?",
  "Would you like to meet consultants?",
  "Legal Support Needed?",
  "Financial Model Available?",
  "Proposal Deadline",
  "Would you like to receive regular updates?",
  "Working with Broker/Advisor?",
  "Other Projects Nearing Contract Expiration?",
  "Main Contact Name",
  "Entity or Company Name",
  "Company HQ Location",
  "Email Address",
];

/** Return true if the field value is considered "filled" for completion. */
function isFieldFilled(val) {
  if (val == null) return false;
  if (typeof val === "number" && !Number.isNaN(val)) return true;
  if (typeof val === "string") return val.trim() !== "";
  if (Array.isArray(val)) return val.length > 0 && (val.length > 1 || (val[0] != null && String(val[0]).trim() !== ""));
  if (typeof val === "object" && val !== null && typeof val.name === "string") return val.name.trim() !== "";
  return false;
}

/** Round to one decimal and clamp 0–100 for display differentiation. */
function toOneDecimal(score) {
  const n = Number(score);
  if (Number.isNaN(n)) return undefined;
  return Math.min(100, Math.max(0, Math.round(n * 10) / 10));
}

/**
 * Deterministic offset (-7 to +7) from brand name so same deal shows clearly different scores per brand.
 * Spread is wide enough that e.g. base 96 yields scores from ~89 to 100 across four brands.
 */
function brandScoreOffset(brandName) {
  if (!brandName || typeof brandName !== "string") return 0;
  let h = 0;
  const s = brandName.trim().toLowerCase();
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0;
  return ((h % 141) - 70) / 10; // -7.0 to +7.0
}

/**
 * Calculate a simple match score (0-100) when no Match Score field exists in Airtable.
 * Uses the same kinds of signals as the Brand Development Dashboard (chain scale, project type,
 * deal type, preferred brands) but without brand-specific criteria. The full weighted breakdown
 * (MKT1, SEG1, etc.) is computed only in the Brand Development Dashboard.
 * Returns score with one decimal for differentiation.
 */
function calculateMatchScoreFromDealData({ hotelChainScale, projectType, dealType, preferredBrandsChosen }, singleBrandName = null) {
  let score = 70;
  const scale = (hotelChainScale || "").toLowerCase();
  if (scale) {
    if (scale.includes("luxury") || scale.includes("upper upscale")) score += 12;
    else if (scale.includes("upscale")) score += 8;
    else if (scale.includes("midscale")) score += 4;
  }
  const proj = (projectType || "").toLowerCase();
  if (proj) {
    if (proj.includes("new build") || proj.includes("conversion") || proj.includes("reflag")) score += 6;
    else score += 3;
  }
  const dtype = (dealType || "").toLowerCase();
  if (dtype) {
    if (dtype.includes("lease") || dtype.includes("brand-managed") || dtype.includes("franchise")) score += 4;
  }
  const hasPreferred = preferredBrandsChosen && String(preferredBrandsChosen).trim() !== "" && String(preferredBrandsChosen).trim() !== "—";
  if (hasPreferred) score += 8;
  const base = Math.min(100, Math.max(0, score));
  const offset = singleBrandName ? brandScoreOffset(singleBrandName) : 0;
  return toOneDecimal(base + offset);
}

/** Compute completion % from a flat field list (reference/fallback only; Data Comp. % in app is from form’s UI-required fields). */
/** Normalize Airtable record to a consistent deal shape.
 * Project Location: from linked Location & Property record — City column + Country column in that table.
 * When mpDataMap, siDataMap, baseId, apiKey are provided and deal has preferred brands, uses 19-factor match score.
 * cuDataMap: Map(contactUploadsRecordId -> raw fields) so "Would You Like to Filter Out Brands Without Key Money?" is read from Contact & Uploads (deal setup).
 * dealBrandCacheMap: optional Map(dealId -> { preferredBrandsChosen, preferredScore, matchScoresNewByBrand }) from Deal Brand Cache table; when present and has data, skips per-brand score computation. */
async function recordToDeal(rec, locationMap = null, mpMap = null, outreachDealIds = null, siPreferredBrandsMap = null, mpDataMap = null, siDataMap = null, baseId = null, apiKey = null, cuDataMap = null, dealBrandCacheMap = null) {
  const f = { ...(rec.fields || {}) };
  const cuLinkedId = getLinkedContactUploadsId(rec.fields || {});
  if (cuDataMap && cuLinkedId) {
    const cuFields = cuDataMap.get(cuLinkedId);
    if (cuFields && typeof cuFields === "object") {
      const filterVal = cuFields["Would You Like to Filter Out Brands Without Key Money?"];
      if (filterVal !== undefined && filterVal !== null) {
        f["Would you like to filter out brands without key money?"] = typeof filterVal === "string" ? filterVal.trim() : String(filterVal);
      }
    }
  }
  const projectName =
    valueToStr(f["Project Name"]) ||
    valueToStr(f["Property Name"]) ||
    valueToStr(f["Name"]) ||
    "";
  let hotelLocation = "—";
  let hotelType = valueToStr(f["Hotel Type"]) || valueToStr(f["Property Type"]) || "";
  let hotelChainScale = "";
  const linkedId = getLinkedLocationId(f);
  let propertyDescription = "";
  let roomCountFromLoc = "";
  if (linkedId && locationMap) {
    const loc = locationMap.get(linkedId);
    if (loc && typeof loc === "object") {
      const cityVal = loc["City & State"] != null ? String(loc["City & State"]).trim() : "";
      const countryVal = loc["Country"] != null ? String(loc["Country"]).trim() : "";
      hotelLocation = formatCityCountry(cityVal, countryVal) || "—";
      if (loc["Hotel Type"]) hotelType = loc["Hotel Type"];
      if (loc["Hotel Chain Scale"]) hotelChainScale = loc["Hotel Chain Scale"];
      roomCountFromLoc = valueToStr(loc["Total Number of Rooms/Keys"]) || "";
      propertyDescription = valueToStr(loc["Company Executive Summary"]) || "";
    }
  }
  if (!propertyDescription) {
    propertyDescription = valueToStr(f["Property Description"]) || valueToStr(f["Description"]) || "";
  }
  if (!propertyDescription && linkedId && locationMap) {
    const loc = locationMap.get(linkedId);
    if (loc && typeof loc === "object") {
      const rooms = valueToStr(loc["Total Number of Rooms/Keys"]);
      const scale = hotelChainScale || valueToStr(loc["Hotel Chain Scale"]);
      const htype = hotelType || valueToStr(loc["Hotel Type"]);
      const parts = [];
      if (rooms) parts.push(rooms + "-room");
      if (scale) parts.push(scale);
      if (htype) parts.push(htype);
      propertyDescription = parts.length ? parts.join(" ") + " property" : "";
    }
  }
  /* Ensure property description includes room count when available and is a short one-liner. */
  if (propertyDescription && propertyDescription !== "—") {
    const alreadyHasRoomCount = /^\d+\s*-?\s*room\b/i.test(propertyDescription.trim());
    if (roomCountFromLoc && !alreadyHasRoomCount) {
      propertyDescription = roomCountFromLoc + "-room " + propertyDescription.trim();
    }
    propertyDescription = propertyDescription.replace(/\s*[\r\n]+\s*/g, " ").replace(/\s{2,}/g, " ").trim();
  }
  if (!propertyDescription) propertyDescription = "—";
  /* Deal bid type: who receives bids (franchise only / 3rd party only / both). From Deals table field "Who should receive bids for this project?" */
  const whoBidsRaw = valueToStr(f["Who should receive bids for this project?"]) || "";
  const dealBidType =
    whoBidsRaw === "Hotel brands only (franchise/license)"
      ? "Franchise only"
      : whoBidsRaw === "Third-party operators only (management)"
        ? "3rd party only"
        : whoBidsRaw === "Both brands and third-party operators"
          ? "Both"
          : "";
  /* Project type on My Deal page is from the "Project Type" column in the Deals table. */
  const projectType = valueToStr(f["Project Type"]) || valueToStr(f["Stage of Development"]) || "";
  const targetOpening =
    f["Expected Opening or Rebranding Date"] != null
      ? formatDate(f["Expected Opening or Rebranding Date"])
      : valueToStr(f["Expected Opening or Rebranding Date"]) ||
        (f["Target Opening Date"] != null
          ? formatDate(f["Target Opening Date"])
          : valueToStr(f["Target Opening Date"]) || "");
  const formStatus = valueToStr(f["Form Status"]) || "";
  const dealStatus = valueToStr(f[DEALS_STATUS_FIELD]) || valueToStr(f["Deal Status"]) || valueToStr(f["Status"]) || "";
  let dealType = valueToStr(f["Preferred Deal Structure"]) || "";
  if (mpMap && !dealType) {
    const mpLinkedId = getLinkedMarketPerformanceId(f);
    if (mpLinkedId) dealType = mpMap.get(mpLinkedId) || "";
  }
  const hasOutreachSetup = outreachDealIds ? outreachDealIds.has(rec.id) : false;
  let preferredBrandsChosen = "";
  if (siPreferredBrandsMap) {
    const siId = getLinkedStrategicIntentId(f);
    if (siId) preferredBrandsChosen = siPreferredBrandsMap.get(siId) || "";
  }
  const matchScoreFieldNames = [
    process.env.AIRTABLE_DEALS_MATCH_SCORE_FIELD,
    "Match Score",
    "Match_Score",
    "Match score",
    "match score",
    "MatchScore",
    "matchScore",
  ].filter(Boolean);
  let matchScoreRaw = undefined;
  for (const name of matchScoreFieldNames) {
    if (f[name] !== undefined && f[name] !== null && f[name] !== "") {
      matchScoreRaw = f[name];
      break;
    }
  }
  let matchScore =
    typeof matchScoreRaw === "number" && !Number.isNaN(matchScoreRaw)
      ? toOneDecimal(matchScoreRaw)
      : typeof matchScoreRaw === "string" && matchScoreRaw.trim() !== ""
        ? toOneDecimal(parseFloat(matchScoreRaw))
        : undefined;
  const brandsListRaw = preferredBrandsChosen && String(preferredBrandsChosen).trim() !== "" && String(preferredBrandsChosen).trim() !== "—"
    ? String(preferredBrandsChosen).split(/\s*,\s*/).map((s) => s.trim()).filter(Boolean)
    : [];
  const MAX_PREFERRED_BRANDS = 5;
  const brandsList = brandsListRaw.length > MAX_PREFERRED_BRANDS ? brandsListRaw.slice(0, MAX_PREFERRED_BRANDS) : brandsListRaw;
  const preferredBrandsChosenCapped = brandsList.length > 0 ? brandsList.join(", ") : "";
  const cache = dealBrandCacheMap ? dealBrandCacheMap.get(rec.id) : null;
  const useCache = cache && (cache.preferredBrandsChosen || cache.matchScoresNewByBrand) && (cache.preferredScore != null || (cache.matchScoresNewByBrand && Object.keys(cache.matchScoresNewByBrand).length > 0));
  if (useCache && brandsList.length > 0) {
    const cachedScores = cache.matchScoresNewByBrand || {};
    const firstKey = brandsList[0] && String(brandsList[0]).trim();
    const firstScore = cachedScores[firstKey] != null ? toOneDecimal(cachedScores[firstKey]) : (cache.preferredScore != null ? toOneDecimal(cache.preferredScore) : undefined);
    return {
      id: rec.id,
      projectName: projectName || "—",
      hotelLocation: hotelLocation || "—",
      hotelType: hotelType || "—",
      hotelChainScale: hotelChainScale || "—",
      propertyDescription: propertyDescription || "—",
      dealBidType: dealBidType || "—",
      projectType: projectType || "—",
      targetOpeningDate: targetOpening || "—",
      formStatus: formStatus || "—",
      dealType: dealType || "—",
      dealStatus: dealStatus || "—",
      hasOutreachSetup,
      preferredBrandsChosen: preferredBrandsChosenCapped || undefined,
      matchScore: firstScore,
      matchScoresByBrand: Object.fromEntries(brandsList.map((b) => [String(b).trim(), cachedScores[String(b).trim()] != null ? toOneDecimal(cachedScores[String(b).trim()]) : undefined])),
      matchScoreNew: firstScore,
      matchScoresNewByBrand: cachedScores,
      matchBreakdownByBrand: {},
      matchBreakdownDetailsByBrand: {},
      matchBreakdownNewDetailsByBrand: cache.breakdownNewDetailsByBrand || {},
      matchKeyMoneyGateReasonByBrand: {},
    };
  }
  if (brandsList.length > 0 && baseId && apiKey && mpDataMap && siDataMap) {
    const linkedLocId = getLinkedLocationId(f);
    const mpId = getLinkedMarketPerformanceId(f);
    const siId = getLinkedStrategicIntentId(f);
    const locationData = linkedLocId && locationMap ? locationMap.get(linkedLocId) : null;
    const mpData = mpId ? mpDataMap.get(mpId) : null;
    const siData = siId ? siDataMap.get(siId) : null;
    const matchScoresByBrand = {};
    const matchScoresNewByBrand = {};
    const matchBreakdownByBrand = {};
    const matchBreakdownDetailsByBrand = {};
    const matchBreakdownNewDetailsByBrand = {};
    const matchKeyMoneyGateReasonByBrand = {};
    for (const brand of brandsList) {
      const key = String(brand).trim();
      const { score, breakdown, breakdownDetails, keyMoneyGateReason, scoreNew, breakdownNew, breakdownNewDetails } = await computeMatchScoreForDealBrand(f, locationData, mpData, siData, brand, baseId, apiKey);
      matchScoresByBrand[key] = score == null ? 0 : toOneDecimal(score);
      matchScoresNewByBrand[key] = scoreNew != null && scoreNew !== "" ? toOneDecimal(scoreNew) : null;
      if (breakdown && typeof breakdown === "object") matchBreakdownByBrand[key] = breakdown;
      if (breakdownDetails && typeof breakdownDetails === "object") matchBreakdownDetailsByBrand[key] = breakdownDetails;
      if (breakdownNewDetails && typeof breakdownNewDetails === "object") matchBreakdownNewDetailsByBrand[key] = breakdownNewDetails;
      if (keyMoneyGateReason && typeof keyMoneyGateReason === "string") matchKeyMoneyGateReasonByBrand[key] = keyMoneyGateReason;
    }
    const firstKey = String(brandsList[0]).trim();
    const firstBrandScore = matchScoresByBrand[firstKey];
    const firstBrandScoreNew = matchScoresNewByBrand[firstKey];
    return {
      id: rec.id,
      projectName: projectName || "—",
      hotelLocation: hotelLocation || "—",
      hotelType: hotelType || "—",
      hotelChainScale: hotelChainScale || "—",
      propertyDescription: propertyDescription || "—",
      dealBidType: dealBidType || "—",
      projectType: projectType || "—",
      targetOpeningDate: targetOpening || "—",
      formStatus: formStatus || "—",
      dealType: dealType || "—",
      dealStatus: dealStatus || "—",
      hasOutreachSetup,
      preferredBrandsChosen: preferredBrandsChosenCapped || undefined,
      matchScore: firstBrandScore,
      matchScoresByBrand,
      matchScoreNew: firstBrandScoreNew,
      matchScoresNewByBrand,
      matchBreakdownByBrand,
      matchBreakdownDetailsByBrand,
      matchBreakdownNewDetailsByBrand,
      matchKeyMoneyGateReasonByBrand,
    };
  }
  if (matchScore === undefined) {
    if (brandsList.length === 0) {
      matchScore = calculateMatchScoreFromDealData({ hotelChainScale, projectType, dealType, preferredBrandsChosen: "" });
    } else if (brandsList.length === 1) {
      matchScore = calculateMatchScoreFromDealData(
        { hotelChainScale, projectType, dealType, preferredBrandsChosen },
        brandsList[0]
      );
    } else {
      const matchScoresByBrand = {};
      brandsList.forEach((brand) => {
        const key = String(brand).trim();
        matchScoresByBrand[key] = calculateMatchScoreFromDealData(
          { hotelChainScale, projectType, dealType, preferredBrandsChosen },
          brand
        );
      });
      matchScore = matchScoresByBrand[String(brandsList[0]).trim()];
      return {
        id: rec.id,
        projectName: projectName || "—",
        hotelLocation: hotelLocation || "—",
        hotelType: hotelType || "—",
        hotelChainScale: hotelChainScale || "—",
        propertyDescription: propertyDescription || "—",
        dealBidType: dealBidType || "—",
        projectType: projectType || "—",
        targetOpeningDate: targetOpening || "—",
        formStatus: formStatus || "—",
        dealType: dealType || "—",
        dealStatus: dealStatus || "—",
        hasOutreachSetup,
        preferredBrandsChosen: preferredBrandsChosenCapped || undefined,
        matchScore,
        matchScoresByBrand,
        matchScoreNew: undefined,
        matchScoresNewByBrand: {},
        matchBreakdownNewDetailsByBrand: {},
      };
    }
  } else if (brandsList.length > 1) {
    const matchScoresByBrand = {};
    brandsList.forEach((brand) => {
      const key = String(brand).trim();
      const base = typeof matchScore === "number" ? matchScore : parseFloat(matchScore) || 70;
      matchScoresByBrand[key] = toOneDecimal(base + brandScoreOffset(brand));
    });
    return {
      id: rec.id,
      projectName: projectName || "—",
      hotelLocation: hotelLocation || "—",
      hotelType: hotelType || "—",
      hotelChainScale: hotelChainScale || "—",
      propertyDescription: propertyDescription || "—",
      dealBidType: dealBidType || "—",
      projectType: projectType || "—",
      targetOpeningDate: targetOpening || "—",
      formStatus: formStatus || "—",
      dealType: dealType || "—",
        dealStatus: dealStatus || "—",
        hasOutreachSetup,
        preferredBrandsChosen: preferredBrandsChosenCapped || undefined,
        matchScore: matchScoresByBrand[String(brandsList[0]).trim()],
      matchScoresByBrand,
      matchScoreNew: undefined,
      matchScoresNewByBrand: {},
      matchBreakdownNewDetailsByBrand: {},
    };
  }
  return {
    id: rec.id,
    projectName: projectName || "—",
    hotelLocation: hotelLocation || "—",
    hotelType: hotelType || "—",
    hotelChainScale: hotelChainScale || "—",
    propertyDescription: propertyDescription || "—",
    dealBidType: dealBidType || "—",
    projectType: projectType || "—",
    targetOpeningDate: targetOpening || "—",
    formStatus: formStatus || "—",
    dealType: dealType || "—",
    dealStatus: dealStatus || "—",
    hasOutreachSetup,
    preferredBrandsChosen: preferredBrandsChosenCapped || undefined,
    matchScore,
    matchScoreNew: undefined,
    matchScoresNewByBrand: {},
    matchBreakdownNewDetailsByBrand: {},
  };
}

// Core view: fields needed for first paint of Deal Information tab without linked-table fanout.
function recordToCoreDeal(rec) {
  const f = { ...(rec.fields || {}) };
  // Recommended Deals lookup/denormalized fields for fast core rendering:
  // - "Project Location (Core)" <= from Location & Property: City & State + Country (or preformatted location)
  // - "Chain Scale (Core)" <= from Location & Property: Hotel Chain Scale
  // - "Deal Type (Core)" <= from Market - Performance - Deal & Capital Structure: Preferred Deal Structure
  // Keep legacy/lookup fallbacks so rollout is backward-compatible while Airtable fields populate.
  const pick = function(names) {
    for (const n of names) {
      const v = valueToStr(f[n]);
      if (v) return v;
    }
    return "";
  };
  const locationCity = pick(["City & State", "City", "City & State (from Location & Property)"]);
  const locationCountry = pick(["Country", "Country (from Location & Property)"]);
  const hotelLocation =
    pick([
      "Project Location (Core)",
      "Project Location",
      "Hotel Location",
      "Location",
      "Hotel Submarket & Location"
    ]) ||
    formatCityCountry(locationCity, locationCountry) ||
    "";
  const projectName = valueToStr(f["Project Name"]) || valueToStr(f["Property Name"]) || valueToStr(f["Name"]) || "";
  const hotelType = valueToStr(f["Hotel Type"]) || valueToStr(f["Property Type"]) || "";
  const hotelChainScale = pick([
    "Chain Scale (Core)",
    "Hotel Chain Scale",
    "Hotel Chain Scale (from Location & Property)",
    "Chain Scale",
    "Hotel Scale"
  ]);
  const propertyDescription = valueToStr(f["Property Description"]) || valueToStr(f["Description"]) || "";
  const whoBidsRaw = valueToStr(f["Who should receive bids for this project?"]) || "";
  const dealBidType =
    whoBidsRaw === "Hotel brands only (franchise/license)"
      ? "Franchise only"
      : whoBidsRaw === "Third-party operators only (management)"
        ? "3rd party only"
        : whoBidsRaw === "Both brands and third-party operators"
          ? "Both"
          : "";
  const projectType = valueToStr(f["Project Type"]) || valueToStr(f["Stage of Development"]) || "";
  const targetOpening =
    f["Expected Opening or Rebranding Date"] != null
      ? formatDate(f["Expected Opening or Rebranding Date"])
      : valueToStr(f["Expected Opening or Rebranding Date"]) ||
        (f["Target Opening Date"] != null ? formatDate(f["Target Opening Date"]) : valueToStr(f["Target Opening Date"]) || "");
  const formStatus = valueToStr(f["Form Status"]) || "";
  const dealStatus = valueToStr(f[DEALS_STATUS_FIELD]) || valueToStr(f["Deal Status"]) || valueToStr(f["Status"]) || "";
  const dealType = pick([
    "Deal Type (Core)",
    "Preferred Deal Structure",
    "Preferred Deal Structure (from Market - Performance - Deal & Capital Structure)",
    "Deal Type",
    "Deal Structure"
  ]);
  return {
    id: rec.id,
    projectName: projectName || "—",
    hotelLocation: hotelLocation || "—",
    hotelType: hotelType || "—",
    hotelChainScale: hotelChainScale || "—",
    propertyDescription: propertyDescription || "—",
    dealBidType: dealBidType || "—",
    projectType: projectType || "—",
    targetOpeningDate: targetOpening || "—",
    formStatus: formStatus || "—",
    dealType: dealType || "—",
    dealStatus: dealStatus || "—",
    hasOutreachSetup: false,
    preferredBrandsChosen: undefined,
    matchScore: undefined,
    matchScoreNew: undefined,
    matchScoresNewByBrand: {},
    matchBreakdownNewDetailsByBrand: {},
  };
}

/**
 * GET /api/my-deals – list all deals from Airtable (optionally filter by owner later via query).
 */
export async function getMyDeals(req, res) {
  const t0 = Date.now();
  const requestId = getMyDealsRequestId();
  const view = String((req.query && req.query.view) || "").trim().toLowerCase();
  const coreView = view === "core";
  const initialView = view === "initial";
  // TEMP MARKER: remove after runtime path verification.
  console.log("[RUNTIME-MARKER][my-deals][2026-03-26-v2]", {
    requestId,
    view: coreView ? "core" : (initialView ? "initial" : "full"),
    retryRebuildEnabled: MY_DEALS_ENABLE_RETRY_REBUILD,
  });
  let tLast = t0;
  let tNow = t0;
  const timingSummary = {
    requestId,
    startedAt: t0,
    elapsedMs: 0,
    config: {
      coldStartDelayMs: MY_DEALS_COLD_START_DELAY_MS,
      minGapMs: MY_DEALS_MIN_GAP_MS,
      phaseGapMs: MY_DEALS_PHASE_GAP_MS,
      retryRebuildEnabled: MY_DEALS_ENABLE_RETRY_REBUILD,
      view: coreView ? "core" : (initialView ? "initial" : "full"),
      mpFetchDelayMs: MY_DEALS_MP_FETCH_DELAY_MS,
      mpConcurrency: MY_DEALS_MP_CONCURRENCY,
      siFetchDelayMs: MY_DEALS_SI_FETCH_DELAY_MS,
      locationFetchDelayMs: MY_DEALS_LOCATION_FETCH_DELAY_MS,
      cuFetchDelayMs: MY_DEALS_CU_FETCH_DELAY_MS,
      ...(MY_DEALS_USE_BATCHED_LINKED_FETCHES && { useBatchedLinkedFetches: true, batchFetchChunkSize: MY_DEALS_BATCH_FETCH_CHUNK_SIZE, batchFetchDelayMs: MY_DEALS_BATCH_FETCH_DELAY_MS }),
      ...(MY_DEALS_USE_BATCHED_LINKED_FETCHES && MY_DEALS_USE_PARALLEL_BATCHED_LINKED_FETCHES && { useParallelBatchedLinkedFetches: true }),
    },
    waits: { coldStartMs: undefined, minGapMs: undefined },
    counts: { dealsFetched: 0, returnedDeals: 0, blankDealTypeBeforeRetry: undefined, blankDealTypeAfterRetry: undefined, stubCount: 0 },
    countsByPhase: {},
    mpDiagnostics: {},
    phasesMs: {},
    retryRan: false,
    status: "success",
  };
  if (shouldLogMyDealsSummary()) console.log("getMyDeals: start", { requestId, t0 });
  getMyDealsSerializing = true;
  try {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      timingSummary.elapsedMs = Date.now() - t0;
      timingSummary.status = "error";
      timingSummary.errorMessage = "Airtable API credentials not configured";
      logMyDealsSummary(timingSummary);
      return res.status(500).json({
        success: false,
        error: "Airtable API credentials not configured (AIRTABLE_BASE_ID, AIRTABLE_API_KEY).",
      });
    }

    const tableIdOrName = encodeURIComponent(DEALS_TABLE);
    let allRecords = [];
    let offset = null;

    try {
      do {
        await waitAirtableSerial();
        let url = `https://api.airtable.com/v0/${baseId}/${tableIdOrName}?pageSize=100`;
        if (offset) url += "&offset=" + encodeURIComponent(offset);
        const pageRes = await fetch(url, {
          headers: { Authorization: "Bearer " + apiKey },
        });
        const data = await pageRes.json();
        if (data.error) {
          timingSummary.elapsedMs = Date.now() - t0;
          timingSummary.status = "error";
          timingSummary.errorMessage = (data.error.message || "Airtable API error").slice(0, 200);
          logMyDealsSummary(timingSummary);
          return res.status(400).json({
            success: false,
            error: data.error.message || "Airtable API error",
          });
        }
        allRecords = allRecords.concat(data.records || []);
        offset = data.offset || null;
      } while (offset);
      const tDealsEnd = Date.now();
      timingSummary.phasesMs.dealsFetch = tDealsEnd - tLast;
      timingSummary.counts.dealsFetched = allRecords.length;
      tLast = tDealsEnd;
      if (shouldLogMyDealsSummary()) console.log("getMyDeals: deals fetched", { requestId, count: allRecords.length, elapsed: tDealsEnd - t0 });
    } catch (e) {
      const raw = (e && e.message) ? e.message : String(e);
      const msg = /fetch failed|failed to fetch|ECONNREFUSED|ENOTFOUND|ETIMEDOUT|network/i.test(raw)
        ? "Server could not reach Airtable (network error). Ensure this machine has outbound HTTPS access to api.airtable.com and that AIRTABLE_BASE_ID and AIRTABLE_API_KEY are set in .env."
        : "Failed while fetching deals from Airtable: " + raw;
      timingSummary.elapsedMs = Date.now() - t0;
      timingSummary.status = "error";
      timingSummary.errorMessage = msg.length > 200 ? msg.slice(0, 200) + "…" : msg;
      logMyDealsSummary(timingSummary);
      console.error("getMyDeals:", msg);
      if (e && e.stack) console.error(e.stack);
      return res.status(500).json({ success: false, error: msg });
    }

    const locationIds = collectLinkedLocationIds(allRecords);
    const mpIds = collectLinkedMarketPerformanceIds(allRecords);
    const siIds = collectLinkedStrategicIntentIds(allRecords);
    const cuIds = initialView ? [] : collectLinkedContactUploadsIds(allRecords);
    let locationMap, mpDataMap, siDataMap, cuDataMap, outreachDealIds, dealBrandCacheMap;
    let mpFetch429Total = 0;
    if (coreView) {
      const deals = allRecords.map((rec) => recordToCoreDeal(rec));
      const dealStatuses = [...new Set(deals.map((d) => d.dealStatus).filter((s) => s && s !== "—"))].sort();
      const projectTypes = [...new Set(deals.map((d) => d.projectType).filter((s) => s && s !== "—"))].sort();
      const hotelChainScales = [...new Set(deals.map((d) => d.hotelChainScale).filter((s) => s && s !== "—"))].sort();
      const dealTypes = [...new Set(deals.map((d) => d.dealType).filter((s) => s && s !== "—"))].sort();
      timingSummary.phasesMs.coreBuild = Date.now() - tLast;
      timingSummary.counts.returnedDeals = deals.length;
      timingSummary.elapsedMs = Date.now() - t0;
      timingSummary.status = "success";
      logMyDealsSummary(timingSummary);
      if (shouldLogMyDealsSummary()) console.log("getMyDeals: response sent (core)", { requestId, totalCount: deals.length, elapsed: timingSummary.elapsedMs });
      return res.json({
        success: true,
        view: "core",
        deals,
        totalCount: deals.length,
        filterOptions: { dealStatuses, projectTypes, hotelChainScales, dealTypes },
      });
    }
    try {
      /* Cold-start: first load after restart needs warm-up. */
      if (getMyDealsColdStart) {
        getMyDealsColdStart = false;
        timingSummary.waits.coldStartMs = MY_DEALS_COLD_START_DELAY_MS;
        if (shouldLogMyDealsSummary()) console.log("getMyDeals: cold start delay", { requestId, ms: MY_DEALS_COLD_START_DELAY_MS, elapsed: Date.now() - t0 });
        await new Promise((r) => setTimeout(r, MY_DEALS_COLD_START_DELAY_MS));
      } else {
        /* Enforce minimum gap between list loads so rapid hard refresh doesn't stack and cause blanks. */
        const elapsed = Date.now() - lastGetMyDealsFinishedAt;
        if (lastGetMyDealsFinishedAt > 0 && elapsed < MY_DEALS_MIN_GAP_MS) {
          const wait = MY_DEALS_MIN_GAP_MS - elapsed;
          timingSummary.waits.minGapMs = wait;
          if (shouldLogMyDealsSummary()) console.log("getMyDeals: min gap wait", { requestId, waitMs: wait, elapsed: Date.now() - t0 });
          await new Promise((r) => setTimeout(r, wait));
        }
      }
      tLast = Date.now();
      /* Fully sequential linked-data fetch so Deal Information and Matched Brands stay consistent (no parallel Airtable bursts). */
      if (mpIds.length > 0) {
        if (MY_DEALS_USE_BATCHED_LINKED_FETCHES) {
          const { map, stats } = await fetchMarketPerformanceDataMapBatched(baseId, apiKey, mpIds);
          mpDataMap = map;
          mpFetch429Total += stats.mpFetch429Count || stats.fetch429Count || 0;
          timingSummary.countsByPhase.mp = { linkedIds: mpIds.length, fetched: map.size, missing: mpIds.length - map.size, chunks: stats.chunks, chunkSizeUsed: stats.chunkSizeUsed };
          if ((stats.fetch429Count || stats.mpFetch429Count || 0) > 0) timingSummary.countsByPhase.mp.fetch429Count = stats.fetch429Count || stats.mpFetch429Count;
          if ((stats.retries || 0) > 0) timingSummary.countsByPhase.mp.retries = stats.retries;
        } else {
          const { map, stats } = await fetchMarketPerformanceDataMap(baseId, apiKey, mpIds);
          mpDataMap = map;
          mpFetch429Total += stats.mpFetch429Count || 0;
          timingSummary.countsByPhase.mp = { linkedIds: mpIds.length, fetched: mpDataMap.size, missing: mpIds.length - mpDataMap.size };
          if (mpFetch429Total > 0) timingSummary.countsByPhase.mp.mpFetch429Count = mpFetch429Total;
        }
      } else {
        mpDataMap = new Map();
        timingSummary.countsByPhase.mp = { linkedIds: 0, fetched: 0, missing: 0 };
      }
      tNow = Date.now();
      timingSummary.phasesMs.mp = tNow - tLast;
      tLast = tNow;
      if (shouldLogMyDealsSummary()) console.log("getMyDeals: phase mp done", { requestId, elapsed: tNow - t0, linkedIds: mpIds.length, fetched: mpDataMap.size });
      await new Promise((r) => setTimeout(r, MY_DEALS_PHASE_GAP_MS));

      /* Phase 6: When both batched and parallel flags on, run SI + Location + CU in parallel; else sequential. */
      if (MY_DEALS_USE_BATCHED_LINKED_FETCHES && MY_DEALS_USE_PARALLEL_BATCHED_LINKED_FETCHES) {
        const tParallelStart = Date.now();
        if (shouldLogMyDealsSummary()) console.log("getMyDeals: parallel linked phases start", { requestId, phases: initialView ? ["si", "location"] : ["si", "location", "cu"] });

        const runSi = async () => {
          const t0 = Date.now();
          if (siIds.length > 0) {
            const { map, stats } = await fetchStrategicIntentDataMapBatched(baseId, apiKey, siIds);
            const phaseMs = Date.now() - t0;
            const counts = { linkedIds: siIds.length, fetched: map.size, missing: siIds.length - map.size, chunks: stats.chunks, chunkSizeUsed: stats.chunkSizeUsed };
            if ((stats.fetch429Count || 0) > 0) counts.fetch429Count = stats.fetch429Count;
            if ((stats.retries || 0) > 0) counts.retries = stats.retries;
            return { map, counts, phaseMs };
          }
          return { map: new Map(), counts: { linkedIds: 0, fetched: 0, missing: 0 }, phaseMs: Date.now() - t0 };
        };
        const runLocation = async () => {
          const t0 = Date.now();
          if (locationIds.length > 0) {
            const { map, stats } = await fetchLocationMapBatched(baseId, apiKey, locationIds);
            const phaseMs = Date.now() - t0;
            const counts = { linkedIds: locationIds.length, fetched: map.size, missing: locationIds.length - map.size, chunks: stats.chunks, chunkSizeUsed: stats.chunkSizeUsed };
            if ((stats.fetch429Count || 0) > 0) counts.fetch429Count = stats.fetch429Count;
            if ((stats.retries || 0) > 0) counts.retries = stats.retries;
            return { map, counts, phaseMs };
          }
          return { map: new Map(), counts: { linkedIds: 0, fetched: 0, missing: 0 }, phaseMs: Date.now() - t0 };
        };
        const runCu = async () => {
          const t0 = Date.now();
          if (cuIds.length > 0) {
            const { map, stats } = await fetchContactUploadsDataMapBatched(baseId, apiKey, cuIds);
            const phaseMs = Date.now() - t0;
            const counts = { linkedIds: cuIds.length, fetched: map.size, missing: cuIds.length - map.size, chunks: stats.chunks, chunkSizeUsed: stats.chunkSizeUsed };
            if ((stats.fetch429Count || 0) > 0) counts.fetch429Count = stats.fetch429Count;
            if ((stats.retries || 0) > 0) counts.retries = stats.retries;
            return { map, counts, phaseMs };
          }
          return { map: new Map(), counts: { linkedIds: 0, fetched: 0, missing: 0 }, phaseMs: Date.now() - t0 };
        };

        const [siResult, locResult, cuResult] = initialView
          ? await Promise.all([runSi(), runLocation(), Promise.resolve({ map: new Map(), counts: { linkedIds: 0, fetched: 0, missing: 0 }, phaseMs: 0 })])
          : await Promise.all([runSi(), runLocation(), runCu()]);

        siDataMap = siResult.map;
        locationMap = locResult.map;
        cuDataMap = cuResult.map;
        timingSummary.countsByPhase.si = siResult.counts;
        timingSummary.countsByPhase.location = locResult.counts;
        timingSummary.countsByPhase.cu = cuResult.counts;
        timingSummary.phasesMs.si = siResult.phaseMs;
        timingSummary.phasesMs.location = locResult.phaseMs;
        timingSummary.phasesMs.cu = cuResult.phaseMs;
        timingSummary.phasesMs.parallelLinkedBlock = Date.now() - tParallelStart;
        tLast = Date.now();
        if (shouldLogMyDealsSummary()) console.log("getMyDeals: parallel linked phases done", { requestId, elapsed: Date.now() - t0, parallelBlockMs: timingSummary.phasesMs.parallelLinkedBlock, siMs: siResult.phaseMs, locationMs: locResult.phaseMs, cuMs: cuResult.phaseMs });
        await new Promise((r) => setTimeout(r, MY_DEALS_PHASE_GAP_MS));
      } else {
        if (siIds.length > 0) {
          if (MY_DEALS_USE_BATCHED_LINKED_FETCHES) {
            const { map, stats } = await fetchStrategicIntentDataMapBatched(baseId, apiKey, siIds);
            siDataMap = map;
            timingSummary.countsByPhase.si = { linkedIds: siIds.length, fetched: map.size, missing: siIds.length - map.size, chunks: stats.chunks, chunkSizeUsed: stats.chunkSizeUsed };
            if ((stats.fetch429Count || 0) > 0) timingSummary.countsByPhase.si.fetch429Count = stats.fetch429Count;
            if ((stats.retries || 0) > 0) timingSummary.countsByPhase.si.retries = stats.retries;
          } else {
            siDataMap = await fetchStrategicIntentDataMap(baseId, apiKey, siIds);
            timingSummary.countsByPhase.si = { linkedIds: siIds.length, fetched: siDataMap.size, missing: siIds.length - siDataMap.size };
          }
        } else {
          siDataMap = new Map();
          timingSummary.countsByPhase.si = { linkedIds: 0, fetched: 0, missing: 0 };
        }
        tNow = Date.now();
        timingSummary.phasesMs.si = tNow - tLast;
        tLast = tNow;
        if (shouldLogMyDealsSummary()) console.log("getMyDeals: phase si done", { requestId, elapsed: tNow - t0, linkedIds: siIds.length, fetched: siDataMap.size });
        await new Promise((r) => setTimeout(r, MY_DEALS_PHASE_GAP_MS));
        if (locationIds.length > 0) {
          if (MY_DEALS_USE_BATCHED_LINKED_FETCHES) {
            const { map, stats } = await fetchLocationMapBatched(baseId, apiKey, locationIds);
            locationMap = map;
            timingSummary.countsByPhase.location = { linkedIds: locationIds.length, fetched: map.size, missing: locationIds.length - map.size, chunks: stats.chunks, chunkSizeUsed: stats.chunkSizeUsed };
            if ((stats.fetch429Count || 0) > 0) timingSummary.countsByPhase.location.fetch429Count = stats.fetch429Count;
            if ((stats.retries || 0) > 0) timingSummary.countsByPhase.location.retries = stats.retries;
          } else {
            locationMap = await fetchLocationMap(baseId, apiKey, locationIds);
            timingSummary.countsByPhase.location = { linkedIds: locationIds.length, fetched: locationMap.size, missing: locationIds.length - locationMap.size };
          }
        } else {
          locationMap = new Map();
          timingSummary.countsByPhase.location = { linkedIds: 0, fetched: 0, missing: 0 };
        }
        tNow = Date.now();
        timingSummary.phasesMs.location = tNow - tLast;
        tLast = tNow;
        if (shouldLogMyDealsSummary()) console.log("getMyDeals: phase location done", { requestId, elapsed: tNow - t0, linkedIds: locationIds.length, fetched: locationMap.size });
        await new Promise((r) => setTimeout(r, MY_DEALS_PHASE_GAP_MS));
        if (cuIds.length > 0) {
          if (MY_DEALS_USE_BATCHED_LINKED_FETCHES) {
            const { map, stats } = await fetchContactUploadsDataMapBatched(baseId, apiKey, cuIds);
            cuDataMap = map;
            timingSummary.countsByPhase.cu = { linkedIds: cuIds.length, fetched: map.size, missing: cuIds.length - map.size, chunks: stats.chunks, chunkSizeUsed: stats.chunkSizeUsed };
            if ((stats.fetch429Count || 0) > 0) timingSummary.countsByPhase.cu.fetch429Count = stats.fetch429Count;
            if ((stats.retries || 0) > 0) timingSummary.countsByPhase.cu.retries = stats.retries;
          } else {
            cuDataMap = await fetchContactUploadsDataMap(baseId, apiKey, cuIds);
            timingSummary.countsByPhase.cu = { linkedIds: cuIds.length, fetched: cuDataMap.size, missing: cuIds.length - cuDataMap.size };
          }
        } else {
          cuDataMap = new Map();
          timingSummary.countsByPhase.cu = { linkedIds: 0, fetched: 0, missing: 0 };
        }
        tNow = Date.now();
        timingSummary.phasesMs.cu = tNow - tLast;
        tLast = tNow;
        if (shouldLogMyDealsSummary()) console.log("getMyDeals: phase cu done", { requestId, elapsed: tNow - t0, linkedIds: cuIds.length, fetched: cuDataMap.size });
        await new Promise((r) => setTimeout(r, MY_DEALS_PHASE_GAP_MS));
      }
      outreachDealIds = await getAllOutreachDealIds(baseId, apiKey, { beforeRequest: () => waitAirtableSerial() });
      tNow = Date.now();
      timingSummary.phasesMs.outreach = tNow - tLast;
      const outreachSize = outreachDealIds && typeof outreachDealIds.size === "number" ? outreachDealIds.size : (Array.isArray(outreachDealIds) ? outreachDealIds.length : 0);
      timingSummary.countsByPhase.outreach = { linkedIds: allRecords.length, fetched: outreachSize, missing: undefined };
      tLast = tNow;
      if (shouldLogMyDealsSummary()) console.log("getMyDeals: phase outreach done", { requestId, elapsed: tNow - t0, dealsWithOutreach: outreachSize });
      await new Promise((r) => setTimeout(r, MY_DEALS_PHASE_GAP_MS));
      try {
        dealBrandCacheMap = await fetchDealBrandCacheMap(baseId, apiKey);
      } catch (cacheErr) {
        dealBrandCacheMap = new Map();
      }
      tNow = Date.now();
      timingSummary.phasesMs.cache = tNow - tLast;
      const cacheSize = dealBrandCacheMap ? dealBrandCacheMap.size : 0;
      timingSummary.countsByPhase.cache = { linkedIds: allRecords.length, fetched: cacheSize, missing: allRecords.length - cacheSize };
      tLast = tNow;
      if (shouldLogMyDealsSummary()) console.log("getMyDeals: phase cache done", { requestId, elapsed: tNow - t0, dealsWithCache: cacheSize });
    } catch (e) {
      const raw = (e && e.message) ? e.message : String(e);
      const msg = /fetch failed|failed to fetch|ECONNREFUSED|ENOTFOUND|ETIMEDOUT|network/i.test(raw)
        ? "Server could not reach Airtable (network error). Ensure this machine has outbound HTTPS access to api.airtable.com."
        : "Failed while fetching linked data (Location/Market Performance/Strategic Intent/Outreach): " + raw;
      timingSummary.elapsedMs = Date.now() - t0;
      timingSummary.status = "error";
      timingSummary.errorMessage = msg.length > 200 ? msg.slice(0, 200) + "…" : msg;
      logMyDealsSummary(timingSummary);
      console.error("getMyDeals:", msg);
      if (e && e.stack) console.error(e.stack);
      return res.status(500).json({ success: false, error: msg });
    }
    let mpMap = preferredDealStructureMapFromMpDataMap(mpDataMap);
    let siPreferredBrandsMap;
    try {
      siPreferredBrandsMap = await preferredBrandsMapFromSiDataMapResolved(baseId, apiKey, siDataMap);
    } catch (e) {
      const raw = (e && e.message) ? e.message : String(e);
      const msg = /fetch failed|failed to fetch|ECONNREFUSED|ENOTFOUND|ETIMEDOUT|network/i.test(raw)
        ? "Server could not reach Airtable (network error). Ensure this machine has outbound HTTPS access to api.airtable.com."
        : "Failed while resolving preferred brands: " + raw;
      timingSummary.elapsedMs = Date.now() - t0;
      timingSummary.status = "error";
      timingSummary.errorMessage = msg.length > 200 ? msg.slice(0, 200) + "…" : msg;
      logMyDealsSummary(timingSummary);
      console.error("getMyDeals:", msg);
      if (e && e.stack) console.error(e.stack);
      return res.status(500).json({ success: false, error: msg });
    }
    let deals = [];
    let stubCount = 0;
    for (const rec of allRecords) {
      try {
        // Initial view includes Deal Info + Matched data but skips heavy per-brand recomputation.
        const scoreBaseId = initialView ? null : baseId;
        const scoreApiKey = initialView ? null : apiKey;
        const scoreMpDataMap = initialView ? null : mpDataMap;
        const scoreSiDataMap = initialView ? null : siDataMap;
        const d = await recordToDeal(
          rec,
          locationMap,
          mpMap,
          outreachDealIds,
          siPreferredBrandsMap,
          scoreMpDataMap,
          scoreSiDataMap,
          scoreBaseId,
          scoreApiKey,
          cuDataMap,
          dealBrandCacheMap
        );
        deals.push(d);
      } catch (dealErr) {
        const name = (rec.fields && (rec.fields["Project Name"] || rec.fields["Property Name"] || rec.fields["Name"])) || rec.id;
        stubCount += 1;
        if (shouldLogMyDealsSummary()) console.warn("getMyDeals: stub row", { requestId, dealId: rec.id, name, error: dealErr.message });
        if (dealErr.stack) console.error(dealErr.stack);
        deals.push({
          id: rec.id,
          projectName: valueToStr(rec.fields?.["Project Name"] || rec.fields?.["Property Name"] || rec.fields?.["Name"]) || "—",
          hotelLocation: "—",
          hotelType: "—",
          hotelChainScale: "—",
          dealBidType: "—",
          projectType: "—",
          targetOpeningDate: "—",
          formStatus: "—",
          dealType: "—",
          dealStatus: "—",
          hasOutreachSetup: false,
          matchScore: undefined,
          matchScoreNew: undefined,
          matchScoresByBrand: {},
          matchScoresNewByBrand: {},
          matchBreakdownNewDetailsByBrand: {},
        });
      }
    }
    tNow = Date.now();
    timingSummary.phasesMs.recordToDeal = tNow - tLast;
    timingSummary.counts.stubCount = stubCount;
    tLast = tNow;
    if (shouldLogMyDealsSummary()) console.log("getMyDeals: recordToDeal done", { requestId, dealsCount: deals.length, stubCount, elapsed: tNow - t0 });

    /* If any Deal Type (or Preferred Brands) are blank, retry MP+SI once after a pause; often fills in after rate-limit window passes. */
    const blankDealTypeCount = deals.filter((d) => !d.dealType || d.dealType === "—").length;
    timingSummary.counts.blankDealTypeBeforeRetry = blankDealTypeCount;
    if (!initialView && blankDealTypeCount > 0 && MY_DEALS_ENABLE_RETRY_REBUILD) {
      timingSummary.retryRan = true;
      const tRetryStart = Date.now();
      if (shouldLogMyDealsSummary()) console.log("getMyDeals: blank Deal Type before retry", { requestId, blankDealTypeCount, elapsed: tRetryStart - t0 });
      if (shouldLogMyDealsSummary()) console.log("getMyDeals: " + blankDealTypeCount + " deals with blank Deal Type; retrying MP+SI in 5s…");
      await new Promise((r) => setTimeout(r, 5000));
      if (mpIds.length > 0) {
        if (MY_DEALS_USE_BATCHED_LINKED_FETCHES) {
          const { map, stats } = await fetchMarketPerformanceDataMapBatched(baseId, apiKey, mpIds);
          mpDataMap = map;
          mpFetch429Total += stats.mpFetch429Count || stats.fetch429Count || 0;
          if (mpFetch429Total > 0) timingSummary.countsByPhase.mp.fetch429Count = mpFetch429Total;
        } else {
          const { map, stats } = await fetchMarketPerformanceDataMap(baseId, apiKey, mpIds);
          mpDataMap = map;
          mpFetch429Total += stats.mpFetch429Count || 0;
          if (mpFetch429Total > 0) timingSummary.countsByPhase.mp.mpFetch429Count = mpFetch429Total;
        }
      } else {
        mpDataMap = new Map();
      }
      await new Promise((r) => setTimeout(r, MY_DEALS_PHASE_GAP_MS));
      if (siIds.length > 0) {
        siDataMap = MY_DEALS_USE_BATCHED_LINKED_FETCHES
          ? (await fetchStrategicIntentDataMapBatched(baseId, apiKey, siIds)).map
          : await fetchStrategicIntentDataMap(baseId, apiKey, siIds);
      } else {
        siDataMap = new Map();
      }
      mpMap = preferredDealStructureMapFromMpDataMap(mpDataMap);
      try {
        siPreferredBrandsMap = await preferredBrandsMapFromSiDataMapResolved(baseId, apiKey, siDataMap);
      } catch (_) {
        siPreferredBrandsMap = new Map();
      }
      deals = [];
      stubCount = 0;
      for (const rec of allRecords) {
        try {
          const d = await recordToDeal(rec, locationMap, mpMap, outreachDealIds, siPreferredBrandsMap, mpDataMap, siDataMap, baseId, apiKey, cuDataMap, dealBrandCacheMap);
          deals.push(d);
        } catch (dealErr) {
          const name = (rec.fields && (rec.fields["Project Name"] || rec.fields["Property Name"] || rec.fields["Name"])) || rec.id;
          stubCount += 1;
          if (shouldLogMyDealsSummary()) console.warn("getMyDeals: stub row (after retry)", { requestId, dealId: rec.id, name, error: dealErr.message });
          deals.push({
            id: rec.id,
            projectName: valueToStr(rec.fields?.["Project Name"] || rec.fields?.["Property Name"] || rec.fields?.["Name"]) || "—",
            hotelLocation: "—",
            hotelType: "—",
            hotelChainScale: "—",
            dealBidType: "—",
            projectType: "—",
            targetOpeningDate: "—",
            formStatus: "—",
            dealType: "—",
            dealStatus: "—",
            hasOutreachSetup: false,
            matchScore: undefined,
            matchScoreNew: undefined,
            matchScoresByBrand: {},
            matchScoresNewByBrand: {},
            matchBreakdownNewDetailsByBrand: {},
          });
        }
      }
      timingSummary.phasesMs.retryRebuild = Date.now() - tRetryStart;
      const blankAfterRetry = deals.filter((d) => !d.dealType || d.dealType === "—").length;
      timingSummary.counts.blankDealTypeAfterRetry = blankAfterRetry;
      timingSummary.counts.stubCount = stubCount;
      if (shouldLogMyDealsSummary()) console.log("getMyDeals: blank Deal Type after retry", { requestId, blankDealTypeCount: blankAfterRetry, stubCount, elapsed: Date.now() - t0 });
    } else if (blankDealTypeCount > 0 && shouldLogMyDealsSummary()) {
      // Diagnostics retained without delaying user response.
      console.warn("getMyDeals: blank Deal Type detected; retry-rebuild skipped (disabled)", {
        requestId,
        blankDealTypeCount,
        enableWithEnv: "MY_DEALS_ENABLE_RETRY_REBUILD=1",
      });
    }

    /* Disabled: background cache refresh hammers Airtable and causes rate limits; subsequent loads (or refreshes) then get empty Deal Type, Preferred Brands, Match Score. Use npm run refresh-all-deal-brand-cache or per-deal refresh instead. */
    /* startBackgroundFullCacheRefresh(baseId, apiKey, allRecords.map((r) => r.id)); */

    let dealStatuses = [];
    try {
      const choices = await getDealStatusChoiceNames(baseId, apiKey);
      if (Array.isArray(choices) && choices.length > 0) dealStatuses = [...choices].sort();
    } catch (_) { /* use empty */ }
    const projectTypes = [
      ...new Set(deals.map((d) => d.projectType).filter((s) => s && s !== "—")),
    ].sort();
    const hotelChainScales = [
      ...new Set(deals.map((d) => d.hotelChainScale).filter((s) => s && s !== "—")),
    ].sort();
    const dealTypes = [
      ...new Set(deals.map((d) => d.dealType).filter((s) => s && s !== "—")),
    ].sort();

    /* MP-specific diagnostics: how many deals depend on MP for dealType, how many got it from MP vs Deals field. */
    let dealsWithMpLink = 0;
    let dealsDependingOnMpForDealType = 0;
    let dealTypeFromDealsField = 0;
    let dealTypeFromMp = 0;
    for (const rec of allRecords) {
      const f = rec.fields || {};
      const mpLinkedId = getLinkedMarketPerformanceId(f);
      const fromDeals = valueToStr(f["Preferred Deal Structure"]) || "";
      if (mpLinkedId) dealsWithMpLink += 1;
      if (!fromDeals && mpLinkedId) dealsDependingOnMpForDealType += 1;
      if (fromDeals) dealTypeFromDealsField += 1;
      if (!fromDeals && mpLinkedId && mpMap.get(mpLinkedId)) dealTypeFromMp += 1;
    }
    const blankAfter = timingSummary.counts.blankDealTypeAfterRetry ?? timingSummary.counts.blankDealTypeBeforeRetry;
    timingSummary.mpDiagnostics = {
      dealsWithMpLink,
      dealsDependingOnMpForDealType,
      dealTypeFromDealsField,
      dealTypeFromMp,
      blankDealTypeBeforeRetry: timingSummary.counts.blankDealTypeBeforeRetry,
      blankDealTypeAfterRetry: blankAfter,
    };
    if (shouldLogMyDealsSummary()) console.log("getMyDeals: MP diagnostics", { requestId, ...timingSummary.mpDiagnostics });

    let initialMatchedSupport = null;
    if (initialView) {
      const tInitialSupport = Date.now();
      initialMatchedSupport = await fetchInitialMatchedSupportState(baseId, apiKey, deals);
      timingSummary.phasesMs.initialMatchedSupport = Date.now() - tInitialSupport;
    }

    timingSummary.counts.returnedDeals = deals.length;
    if (timingSummary.counts.blankDealTypeAfterRetry === undefined) timingSummary.counts.blankDealTypeAfterRetry = timingSummary.counts.blankDealTypeBeforeRetry;
    lastGetMyDealsFinishedAt = Date.now();
    timingSummary.elapsedMs = Date.now() - t0;
    logMyDealsSummary(timingSummary);
    if (shouldLogMyDealsSummary()) console.log("getMyDeals: response sent", { requestId, totalCount: deals.length, stubCount, elapsed: timingSummary.elapsedMs });
    res.json({
      success: true,
      view: initialView ? "initial" : "full",
      deals,
      totalCount: deals.length,
      filterOptions: { dealStatuses, projectTypes, hotelChainScales, dealTypes },
      ...(initialView && initialMatchedSupport ? { initialMatchedSupport } : {}),
    });
  } catch (err) {
    console.error("Error in getMyDeals:", err);
    if (err && err.stack) console.error(err.stack);
    const errMsg = (err && err.message) ? String(err.message) : "Internal Server Error";
    if (typeof timingSummary !== "undefined") {
      timingSummary.elapsedMs = Date.now() - t0;
      timingSummary.status = "error";
      timingSummary.errorMessage = errMsg.length > 200 ? errMsg.slice(0, 200) + "…" : errMsg;
      logMyDealsSummary(timingSummary);
    }
    res.status(500).json({
      success: false,
      error: errMsg,
    });
  } finally {
    getMyDealsSerializing = false;
  }
}

/**
 * GET /api/my-deals/:recordId – fetch a single deal by record ID (for view/edit page).
 * Returns the full Airtable record (id + fields) so the deal-setup form can bind and save.
 */
/**
 * POST /api/my-deals – create a new draft deal.
 * Body: { dealName (required), dealType?, market? }
 * Returns: { success, recordId, deal }
 */
export async function createDeal(req, res) {
  try {
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const dealName = typeof body.dealName === "string" ? body.dealName.trim() : "";
    if (!dealName) {
      return res.status(400).json({ success: false, error: "dealName is required" });
    }
    const dealType = typeof body.dealType === "string" ? body.dealType.trim() : "";
    const market = typeof body.market === "string" ? body.market.trim() : "";

    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    const dealFields = {
      "Project Name": dealName,
      "Property Name": dealName,
      [DEALS_STATUS_FIELD]: "Draft",
    };

    await waitAirtableSerial();
    const dealsTable = encodeURIComponent(DEALS_TABLE);
    const createRes = await fetch(`https://api.airtable.com/v0/${baseId}/${dealsTable}`, {
      method: "POST",
      headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
      body: JSON.stringify({ fields: dealFields, typecast: true }),
    });
    const createData = await createRes.json();
    if (createData.error) {
      return res.status(400).json({ success: false, error: "Deal create failed: " + (createData.error.message || "unknown") });
    }
    const recordId = createData.id;
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(500).json({ success: false, error: "Deal create did not return record ID" });
    }

    if (dealType) {
      const mpTable = encodeURIComponent(MARKET_PERFORMANCE_TABLE);
      const mpFields = { [MP_DEAL_LINK_FIELD]: [recordId], "Preferred Deal Structure": dealType };
      const mpRes = await fetch(`https://api.airtable.com/v0/${baseId}/${mpTable}`, {
        method: "POST",
        headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
        body: JSON.stringify({ fields: mpFields, typecast: true }),
      });
      const mpData = await mpRes.json();
      if (!mpData.error && mpData.id) {
        await waitAirtableSerial();
        await fetch(`https://api.airtable.com/v0/${baseId}/${dealsTable}/${encodeURIComponent(recordId)}`, {
          method: "PATCH",
          headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
          body: JSON.stringify({ fields: { [MARKET_PERFORMANCE_LINK_FIELD]: [mpData.id] }, typecast: true }),
        });
      }
    }

    if (market) {
      const locTable = encodeURIComponent(LOCATION_PROPERTY_TABLE);
      const locFields = { Deal_ID: [recordId], City: market };
      const locRes = await fetch(`https://api.airtable.com/v0/${baseId}/${locTable}`, {
        method: "POST",
        headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
        body: JSON.stringify({ fields: locFields, typecast: true }),
      });
      const locData = await locRes.json();
      if (!locData.error && locData.id) {
        await waitAirtableSerial();
        await fetch(`https://api.airtable.com/v0/${baseId}/${dealsTable}/${encodeURIComponent(recordId)}`, {
          method: "PATCH",
          headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
          body: JSON.stringify({ fields: { [LOCATION_LINK_FIELD]: [locData.id] }, typecast: true }),
        });
      }
    }

    const { deal } = await fetchDealWithMergedLinkedRecords(baseId, apiKey, recordId);
    return res.json({ success: true, recordId, deal: deal || { id: recordId, fields: dealFields } });
  } catch (err) {
    console.error("Error in createDeal:", err);
    res.status(500).json({ success: false, error: err.message || "Create failed" });
  }
}

export async function getDealById(req, res) {
  try {
    const recordId = req.params.recordId;
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid record ID is required" });
    }
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    const full = await fetchDealWithMergedLinkedRecords(baseId, apiKey, recordId);
    if (!full) {
      return res.status(404).json({ success: false, error: "Deal not found" });
    }
    res.json({ success: true, deal: full.deal, normalized: full.normalized });
  } catch (err) {
    console.error("Error in getDealById:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}

/**
 * POST /api/my-deals/:recordId/add-recommended-brand
 * Computes the recommended brand (highest Match Score New, passes pre-filters) for this deal and saves it to Strategic Intent Preferred Brands.
 * Optional body: { brand: "Brand Name" } – add the specified brand instead of computing best match.
 * Returns { success: true, brand } or { success: false, error }.
 */
export async function addRecommendedBrand(req, res) {
  try {
    const recordId = req.params.recordId;
    const requestedBrand = req.body && typeof req.body.brand === "string" ? req.body.brand.trim() : null;
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid deal ID is required" });
    }
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    const tableIdOrName = encodeURIComponent(DEALS_TABLE);
    const getRes = await fetch(`https://api.airtable.com/v0/${baseId}/${tableIdOrName}/${encodeURIComponent(recordId)}`, {
      headers: { Authorization: "Bearer " + apiKey },
    });
    const dealData = await getRes.json();
    if (dealData.error || !dealData.fields) {
      return res.status(404).json({ success: false, error: "Deal not found" });
    }

    const f = { ...(dealData.fields || {}) };
    const linkedLocId = getLinkedLocationId(f);
    const mpId = getLinkedMarketPerformanceId(f);
    const siLinkedId = getLinkedStrategicIntentId(f);
    const cuLinkedId = getLinkedContactUploadsId(f);

    if (!siLinkedId) {
      return res.status(400).json({ success: false, error: "Deal has no Strategic Intent record. Add Strategic Intent first." });
    }

    const [locationData, mpData, siData, cuFields] = await Promise.all([
      linkedLocId ? fetchLocationRecord(baseId, apiKey, linkedLocId) : null,
      mpId ? fetchMarketPerformanceRecord(baseId, apiKey, mpId) : null,
      fetchStrategicIntentRecord(baseId, apiKey, siLinkedId),
      cuLinkedId ? fetchContactUploadsRecord(baseId, apiKey, cuLinkedId) : null,
    ]);

    if (cuFields && typeof cuFields === "object") {
      const filterVal = cuFields["Would You Like to Filter Out Brands Without Key Money?"];
      if (filterVal !== undefined && filterVal !== null) {
        f["Would you like to filter out brands without key money?"] = typeof filterVal === "string" ? filterVal.trim() : String(filterVal);
      }
    }

    const siDataMap = new Map([[siLinkedId, siData || {}]]);
    const resolvedMap = await preferredBrandsMapFromSiDataMapResolved(baseId, apiKey, siDataMap);
    const preferredStr = resolvedMap.get(siLinkedId) || "";
    const preferredBrandsSet = preferredStr ? preferredStr.split(/\s*,\s*/).map((s) => s.trim()).filter(Boolean) : [];

    const MAX_PREFERRED_BRANDS = 5;
    const targets = await fetchTargetsForDeal(recordId).catch(() => []);
    const activePreferredCount = preferredBrandsSet.filter((pb) => {
      const norm = String(pb).trim().toLowerCase();
      const isDeleted = targets.some((t) => String(t.brandName || "").trim().toLowerCase() === norm && (t.status || "") === "Deleted");
      return !isDeleted;
    }).length;
    if (activePreferredCount >= MAX_PREFERRED_BRANDS) {
      return res.status(400).json({
        success: false,
        error: `Maximum ${MAX_PREFERRED_BRANDS} preferred brands. Mark one as Deleted to make room before adding another.`,
      });
    }

    let recommended;
    if (requestedBrand) {
      const alreadyPreferred = preferredBrandsSet.some((n) => String(n).trim().toLowerCase() === String(requestedBrand).trim().toLowerCase());
      if (alreadyPreferred) {
        return res.status(200).json({ success: true, brand: requestedBrand, message: "Brand already in preferred list." });
      }
      recommended = { brand: requestedBrand, scoreNew: null, found: true };
    } else {
      recommended = await computeRecommendedBrand(f, locationData, mpData, siData, preferredBrandsSet, baseId, apiKey);
    }
    if (!recommended || recommended.found === false) {
      const d = recommended && typeof recommended === "object" ? recommended : {};
      const total = d.totalBrands ?? 0;
      const skipPref = d.skippedPreferred ?? 0;
      const skipNoData = d.skippedNoData ?? 0;
      let msg = "No recommended brand found. ";
      if (total === 0) msg += "No brands in Brand Setup.";
      else if (skipPref >= total) msg += "All " + total + " brands are already in your preferred list.";
      else if (skipNoData > 0 && total - skipPref === skipNoData) msg += "Could not load data for any of the " + (total - skipPref) + " candidate brands.";
      else msg += total + " brands checked (" + skipPref + " in preferred); no score returned.";
      return res.status(200).json({ success: false, error: msg });
    }

    const brandRecId = await getBrandBasicsRecordId(baseId, apiKey, recommended.brand);
    if (!brandRecId) {
      return res.status(500).json({ success: false, error: "Could not find Brand Basics record for: " + recommended.brand });
    }

    const preferredField = siData?.["Preferred Brands"] ?? siData?.["Preferred Brands (up to 4)"];
    const airtableFieldName = (siData && siData["Preferred Brands"] !== undefined) ? "Preferred Brands" : "Preferred Brands (up to 4)";
    const isTextField = typeof preferredField === "string";
    const rawArr = Array.isArray(preferredField) ? preferredField : (preferredField != null ? [preferredField] : []);

    let currentIds = [];
    const looksLikeLinkedRecords = rawArr.some((v) => {
      const s = typeof v === "string" ? v : (v && typeof v === "object" && v.id != null ? String(v.id) : "");
      return s.startsWith("rec");
    });
    for (const v of rawArr) {
      const id = typeof v === "string" && v.startsWith("rec") ? v : (v && typeof v === "object" && (v.id != null) ? String(v.id) : null);
      if (id && id.startsWith("rec")) currentIds.push(id);
    }
    if (currentIds.length === 0 && preferredBrandsSet.length > 0) {
      for (const name of preferredBrandsSet) {
        const id = await getBrandBasicsRecordId(baseId, apiKey, name);
        if (id && !currentIds.includes(id)) currentIds.push(id);
      }
    }
    if (currentIds.includes(brandRecId)) {
      return res.status(200).json({ success: true, brand: recommended.brand, message: "Brand already in preferred list." });
    }

    const currentNamesForMultiSelect = rawArr.map((v) => (typeof v === "string" ? v.trim() : (v && (v.name || v.id) && typeof (v.name || v.id) === "string" ? String(v.name || v.id).trim() : ""))).filter(Boolean);
    const alreadyHasBrand = preferredBrandsSet.some((n) => String(n).trim().toLowerCase() === String(recommended.brand).trim().toLowerCase()) ||
      currentNamesForMultiSelect.some((n) => String(n).trim().toLowerCase() === String(recommended.brand).trim().toLowerCase());
    if (alreadyHasBrand) {
      return res.status(200).json({ success: true, brand: recommended.brand, message: "Brand already in preferred list." });
    }

    const siTable = encodeURIComponent(STRATEGIC_INTENT_TABLE);
    let patchBody;
    let savedAsIds = false;
    if (isTextField) {
      const currentNames = preferredBrandsSet.length > 0 ? preferredBrandsSet : (typeof preferredField === "string" ? preferredField.split(/\s*,\s*/).map((s) => s.trim()).filter(Boolean) : []);
      const namesToSave = [...currentNames, recommended.brand].filter(Boolean);
      patchBody = { fields: { [airtableFieldName]: namesToSave.join(", ") }, typecast: true };
    } else if (rawArr.length > 0 && !looksLikeLinkedRecords) {
      const namesToSave = [...currentNamesForMultiSelect, recommended.brand].filter(Boolean);
      patchBody = { fields: { [airtableFieldName]: namesToSave }, typecast: true };
    } else {
      // Preferred Brands is Multi-select (form question): only ever send brand names, never record IDs.
      const newIds = [...currentIds, brandRecId];
      const resolvedNames = await Promise.all(newIds.map((id) => resolvePreferredBrandToName(baseId, apiKey, id)));
      const namesToSave = resolvedNames.filter(Boolean);
      if (namesToSave.length === 0) {
        return res.status(400).json({
          success: false,
          error: "Could not resolve brand names. Ensure 'Brand Setup - Brand Basics' has a 'Brand Name' field and records are valid.",
        });
      }
      patchBody = { fields: { [airtableFieldName]: namesToSave }, typecast: true };
      console.log("[Add Best Match] Preferred Brands (Multi-select): PATCH with " + namesToSave.length + " brand names.");
    }

    const patchRes = await fetch(`https://api.airtable.com/v0/${baseId}/${siTable}/${encodeURIComponent(siLinkedId)}`, {
      method: "PATCH",
      headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
      body: JSON.stringify(patchBody),
    });
    const patchData = await patchRes.json();
    if (patchData.error) {
      const errMsg = patchData.error.message || patchData.error.type || "Strategic Intent update failed";
      const isMultiSelectPath = Array.isArray(patchBody.fields[airtableFieldName]) &&
        patchBody.fields[airtableFieldName].some((v) => typeof v === "string" && !v.startsWith("rec"));
      if (isMultiSelectPath) {
        return res.status(400).json({
          success: false,
          error: "Preferred Brands is Multi-select. Airtable rejected the names: " + errMsg + ". Add these brand names as options in the Preferred Brands field (Edit field → Options), or ensure typecast is allowed so new options can be created.",
        });
      }
      if (isTextField) {
        const newIds = [...currentIds, brandRecId];
        const fallbackRes = await fetch(`https://api.airtable.com/v0/${baseId}/${siTable}/${encodeURIComponent(siLinkedId)}`, {
          method: "PATCH",
          headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
          body: JSON.stringify({ fields: { [airtableFieldName]: newIds }, typecast: true }),
        });
        const fallbackData = await fallbackRes.json();
        if (fallbackData.error) {
          return res.status(400).json({ success: false, error: "Failed to save: " + (fallbackData.error.message || "Strategic Intent update failed") });
        }
      } else {
        return res.status(400).json({ success: false, error: "Failed to save: " + errMsg });
      }
    }

    const displayHint = savedAsIds
      ? "To show brand names in Airtable: Table \"Brand Setup - Brand Basics\" → Customize table → set Primary field to \"Brand Name\"."
      : null;
    res.json({
      success: true,
      brand: recommended.brand,
      scoreNew: recommended.scoreNew,
      savedAsIds: !!savedAsIds,
      displayHint,
      primaryFieldHint: displayHint,
    });
  } catch (err) {
    console.error("Error in addRecommendedBrand:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}

/**
 * GET /api/my-deals/:dealId/alternative-brands?limit=5
 * Returns top N alternative brands by Match Score New (server-side batched scoring). Use for Alternative Brand Suggestions to avoid slow client-side loops.
 */
export async function getAlternativeBrands(req, res) {
  try {
    const recordId = req.params.recordId;
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid deal ID is required" });
    }
    const limit = Math.min(20, Math.max(1, parseInt(req.query.limit, 10) || 5));
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    const tableIdOrName = encodeURIComponent(DEALS_TABLE);
    const getRes = await fetch(`https://api.airtable.com/v0/${baseId}/${tableIdOrName}/${encodeURIComponent(recordId)}`, {
      headers: { Authorization: "Bearer " + apiKey },
    });
    const dealData = await getRes.json();
    if (dealData.error || !dealData.fields) {
      return res.status(404).json({ success: false, error: "Deal not found" });
    }

    const f = { ...(dealData.fields || {}) };
    const linkedLocId = getLinkedLocationId(f);
    const mpId = getLinkedMarketPerformanceId(f);
    const siLinkedId = getLinkedStrategicIntentId(f);

    if (!siLinkedId) {
      return res.status(400).json({ success: false, error: "Deal has no Strategic Intent record." });
    }

    const [locationData, mpData, siData, dealBrandCacheMap] = await Promise.all([
      linkedLocId ? fetchLocationRecord(baseId, apiKey, linkedLocId) : null,
      mpId ? fetchMarketPerformanceRecord(baseId, apiKey, mpId) : null,
      fetchStrategicIntentRecord(baseId, apiKey, siLinkedId),
      fetchDealBrandCacheMap(baseId, apiKey),
    ]);

    const cache = dealBrandCacheMap.get(recordId);
    if (cache && cache.topAlternatives && cache.topAlternatives.length > 0) {
      const preferredBrand = cache.preferredBrandsChosen ? cache.preferredBrandsChosen.split(/\s*,\s*/).map((s) => s.trim()).filter(Boolean)[0] : null;
      
      // Get preferred brand breakdown from breakdownNewDetailsByBrand (not from topAlternatives)
      let preferredBreakdown = {};
      if (preferredBrand && cache.breakdownNewDetailsByBrand && cache.breakdownNewDetailsByBrand[preferredBrand]) {
        preferredBreakdown = cache.breakdownNewDetailsByBrand[preferredBrand];
      }
      
      return res.json({
        preferredBrand: preferredBrand || null,
        preferredScore: cache.preferredScore != null ? cache.preferredScore : null,
        preferredBreakdown: preferredBreakdown,
        alternatives: cache.topAlternatives.map((a) => (typeof a === "object" && a && a.brand != null ? { brand: a.brand, score: a.score, breakdownNewDetails: a.breakdownNewDetails || {} } : { brand: String(a), score: null, breakdownNewDetails: {} })),
      });
    }

    const siDataMap = new Map([[siLinkedId, siData || {}]]);
    const resolvedMap = await preferredBrandsMapFromSiDataMapResolved(baseId, apiKey, siDataMap);
    const preferredStr = resolvedMap.get(siLinkedId) || "";
    const preferredBrandsSet = preferredStr ? preferredStr.split(/\s*,\s*/).map((s) => s.trim()).filter(Boolean) : [];

    const result = await computeTopAlternativeBrands(f, locationData, mpData, siData, preferredBrandsSet, baseId, apiKey, limit);
    res.json({
      preferredBrand: result.preferredBrand,
      preferredScore: result.preferredScore,
      preferredBreakdown: result.preferredBreakdown || {},
      alternatives: result.alternatives.map((a) => ({ brand: a.brand, score: a.score, breakdownNewDetails: a.breakdownNewDetails })),
    });
  } catch (err) {
    console.error("Error in getAlternativeBrands:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}

const OPERATOR_MATCH_WEIGHTS = {
  geographyMarkets: 18,
  chainScale: 8,
  assetProjectStageFit: 14,
  dealStructureAssignment: 12,
  feeCommercial: 10,
  serviceOfferings: 8,
  systemsReporting: 6,
  ownerRelations: 6,
  brandPortfolioRelevance: 6,
  negativeFitPenalty: 2,
};

function toStr(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "number" && Number.isFinite(v)) return String(v);
  if (Array.isArray(v)) return v.map((x) => toStr(x)).filter(Boolean).join(", ");
  if (typeof v === "object" && v && typeof v.name === "string") return String(v.name).trim();
  return String(v).trim();
}

function toList(v) {
  if (v == null) return [];
  if (Array.isArray(v)) return v.map((x) => toStr(x)).filter(Boolean);
  const s = toStr(v);
  if (!s) return [];
  return s.split(/\s*,\s*/).map((x) => x.trim()).filter(Boolean);
}

function locValue(locationData, airtableKey, normalizedKey) {
  if (!locationData || typeof locationData !== "object") return "";
  return locationData[airtableKey] ?? locationData[normalizedKey] ?? "";
}

function firstPresent(obj, keys) {
  if (!obj || typeof obj !== "object") return "";
  for (const k of keys) {
    const v = obj[k];
    if (v != null && toStr(v)) return toStr(v);
  }
  return "";
}

function collectPresentList(obj, keys) {
  const out = [];
  const seen = new Set();
  for (const k of keys || []) {
    const vals = toList(obj && obj[k]);
    for (const v of vals) {
      const t = String(v || "").trim();
      if (!t) continue;
      const lk = t.toLowerCase();
      if (seen.has(lk)) continue;
      seen.add(lk);
      out.push(t);
    }
  }
  return out;
}

function collectValuesByKeyToken(obj, tokens, max = 8) {
  if (!obj || typeof obj !== "object") return [];
  const tks = (tokens || []).map((t) => String(t || "").toLowerCase()).filter(Boolean);
  const out = [];
  const seen = new Set();
  for (const [k, v] of Object.entries(obj)) {
    const key = String(k || "").toLowerCase();
    if (!tks.some((tk) => key.includes(tk))) continue;
    const vals = toList(v);
    for (const val of vals) {
      const s = String(val || "").trim();
      if (!s) continue;
      const lk = s.toLowerCase();
      if (seen.has(lk)) continue;
      seen.add(lk);
      out.push(s);
      if (out.length >= max) return out;
    }
  }
  return out;
}

function overlapScore(dealVals, operatorVals, partial = 35) {
  const d = new Set((dealVals || []).map((x) => String(x).trim().toLowerCase()).filter(Boolean));
  const o = new Set((operatorVals || []).map((x) => String(x).trim().toLowerCase()).filter(Boolean));
  if (d.size === 0 || o.size === 0) return null;
  let intersection = 0;
  for (const v of d) if (o.has(v)) intersection += 1;
  if (intersection === 0) return partial;
  const ratio = intersection / d.size;
  return Math.min(100, Math.max(0, Math.round((40 + ratio * 60) * 10) / 10));
}

function resolveBrandIdsToNames(values, brandNameById) {
  const arr = Array.isArray(values) ? values : [];
  return arr.map((v) => {
    const s = String(v || "").trim();
    if (!s) return "";
    if (/^rec[a-zA-Z0-9]{14,}$/.test(s) && brandNameById && typeof brandNameById.get === "function") {
      return brandNameById.get(s) || s;
    }
    return s;
  }).filter(Boolean);
}

function includesAnyToken(text, tokens) {
  const t = String(text || "").toLowerCase();
  if (!t) return false;
  return (tokens || []).some((tk) => tk && t.includes(String(tk).toLowerCase()));
}

function scoreOperatorMatchForDeal(dealFields, locationData, mpData, siData, operatorPrefill, brandNameById = null) {
  const dealCountry = toStr(locValue(locationData, "Country", "country") || dealFields?.Country || dealFields?.country);
  const dealScale = toStr(locValue(locationData, "Hotel Chain Scale", "hotelChainScale") || dealFields?.["Hotel Chain Scale"]);
  const dealProjectType = toStr(dealFields?.["Project Type"]);
  const dealBuildingType = toStr(locValue(locationData, "Building Type", "buildingType") || dealFields?.["Building Type"]);
  const dealStage = toStr(dealFields?.["Stage of Development"] || locValue(locationData, "Stage of Development", "stageOfDevelopment"));
  const dealStructure = toStr((mpData || {})["Preferred Deal Structure"]);
  const dealPreferredBrands = toList((siData || {})["Preferred Brands"]);
  const dealBreakers = toList((siData || {})["Top 3 Deal Breakers"]);
  const dealMustHaves = toList((siData || {})["Must-Haves From Brand/Operator"] || (siData || {})["Must-Haves From Brand or Operator"]);
  const dealRoy = toStr((mpData || {})["Royalty Fee Expectations"]);
  const dealMktFee = toStr((mpData || {})["Marketing Fee Expectations"]);
  const dealLoyaltyFee = toStr((mpData || {})["Loyalty Fee Expectations"]);

  const op = operatorPrefill || {};
  const opMarkets = toList(firstPresent(op, ["specificMarkets", "market_fit", "topMarkets", "regionsSupported", "bestFitGeographies"]));
  const opScale = toList(firstPresent(op, ["chainScale", "chainScalesYouSupport", "chain_scales"]));
  const opProject = (() => {
    const base = toList(firstPresent(op, [
      "bestFitAssetTypes",
      "propertyTypesManaged",
      "hotel_types",
      "asset_classes",
      "propertyTypes",
      "projectTypes",
      "assetType",
    ]));
    const extra = collectValuesByKeyToken(op, ["asset", "property type", "project type", "tower", "podium", "resort", "urban"], 10);
    return [...new Set([...base, ...extra].map((x) => String(x || "").trim()).filter(Boolean))];
  })();
  const opStages = (() => {
    const base = toList(firstPresent(op, ["operatingSituations", "projectStages", "operating_situations", "stageOfDevelopment"]));
    const extra = collectValuesByKeyToken(op, ["stage", "construction", "pre-opening", "opening", "conversion", "transition", "stabilized"], 10);
    return [...new Set([...base, ...extra].map((x) => String(x || "").trim()).filter(Boolean))];
  })();
  const opStructures = (() => {
    const base = toList(firstPresent(op, ["bestFitDealStructures", "typicalAssignmentTypes", "serviceModels", "service_models"]));
    const extra = collectValuesByKeyToken(op, ["structure", "assignment", "franchise", "management", "lease", "contract"], 8);
    return [...new Set([...base, ...extra].map((x) => String(x || "").trim()).filter(Boolean))];
  })();
  const opBrands = resolveBrandIdsToNames(
    toList(firstPresent(op, ["brands", "brandsManaged", "brands_managed"])),
    brandNameById
  );
  const opServices = collectPresentList(op, [
    "primaryServices",
    "additionalServices",
    "primary_services",
    "additional_services",
    // New Two / granular service arrays
    "revenueManagementServices",
    "salesMarketingSupport",
    "accountingReporting",
    "procurementServices",
    "hrTrainingServices",
    "technologyServices",
    "designRenovationSupport",
    "developmentServices",
    // Narrative fallback used on some records
    "serviceDifferentiators",
  ]);
  const opSystems = toList(firstPresent(op, ["technologySystems", "systemsStack", "primaryPMS", "reportTypesProvided"]));
  const opReporting = toStr(firstPresent(op, ["ownerReportingCadence", "reportingFrequency", "ownerCommunicationStyle"]));
  const opOwnerRel = toStr(firstPresent(op, ["ownerCommunicationStyle", "operatingCollaborationMode", "typicalResponseTimeForOwnerInquiries", "ownerReferencesAvailable"]));
  const opLessIdeal = toStr(firstPresent(op, ["lessIdealSituations", "less_proven_areas"]));
  const opFee = (() => {
    const specific = toStr(firstPresent(op, [
      "feeStructureSummary",
      "operatorFeeApproach",
      "dealTermsSummary",
      "cap_profile_commercial",
      "comm_profile_commercial",
      "dealTermsFeesSummary",
    ]));
    if (specific) return specific;
    const inferred = collectValuesByKeyToken(op, ["commercial", "fee", "incentive", "term", "contract", "econom"], 8);
    return inferred.join(", ");
  })();

  const factors = {
    geographyMarkets: {
      label: "Geography & Markets",
      weight: OPERATOR_MATCH_WEIGHTS.geographyMarkets,
      dealValue: "Country: " + (dealCountry || "—"),
      operatorValue: "Supported markets: " + (opMarkets.join(", ") || "—"),
      note: "Compares deal market/country with operator's supported regions and markets.",
      score: (() => {
        if (!dealCountry && opMarkets.length === 0) return null;
        if (!dealCountry) return 60;
        if (opMarkets.length === 0) return 35;
        const direct = opMarkets.some((m) => String(m).toLowerCase().includes(dealCountry.toLowerCase()));
        return direct ? 100 : 35;
      })(),
    },
    chainScale: {
      label: "Chain Scale",
      weight: OPERATOR_MATCH_WEIGHTS.chainScale,
      dealValue: "Hotel Chain Scale: " + (dealScale || "—"),
      operatorValue: "Supported chain scales: " + (opScale.join(", ") || "—"),
      note: "Checks whether the operator works in the same chain-scale band.",
      score: (() => {
        if (!dealScale) return null;
        if (opScale.length === 0) return 45;
        const same = opScale.some((s) => String(s).toLowerCase() === dealScale.toLowerCase());
        const partial = opScale.some((s) => String(s).toLowerCase().includes(dealScale.toLowerCase()) || dealScale.toLowerCase().includes(String(s).toLowerCase()));
        if (same) return 100;
        if (partial) return 65;
        return 25;
      })(),
    },
    assetProjectStageFit: {
      label: "Asset / Project / Stage Fit",
      weight: OPERATOR_MATCH_WEIGHTS.assetProjectStageFit,
      dealValue: "Project Type: " + (dealProjectType || "—") + "; Building Type: " + (dealBuildingType || "—") + "; Stage: " + (dealStage || "—"),
      operatorValue: "Best-fit assets: " + (opProject.join(", ") || "—") + "; Operating situations: " + (opStages.join(", ") || "—"),
      note: "Evaluates whether the operator's target assets and delivery stage match this deal.",
      score: (() => {
        const projectScore = overlapScore([dealProjectType, dealBuildingType].filter(Boolean), opProject, 30);
        const stageScore = overlapScore([dealStage].filter(Boolean), opStages, 35);
        if (projectScore == null && stageScore == null) return null;
        if (projectScore == null) return stageScore;
        if (stageScore == null) return projectScore;
        return Math.round(((projectScore * 0.7) + (stageScore * 0.3)) * 10) / 10;
      })(),
    },
    dealStructureAssignment: {
      label: "Deal Structure / Assignment",
      weight: OPERATOR_MATCH_WEIGHTS.dealStructureAssignment,
      dealValue: "Preferred Deal Structure: " + (dealStructure || "—"),
      operatorValue: "Accepted structures: " + (opStructures.join(", ") || "—"),
      note: "Compares preferred deal structure with the operator's assignment and structure profile.",
      score: (() => {
        if (!dealStructure) return null;
        if (opStructures.length === 0) return 45;
        const lower = dealStructure.toLowerCase();
        const exact = opStructures.some((s) => String(s).toLowerCase() === lower);
        const partial = opStructures.some((s) => String(s).toLowerCase().includes(lower) || lower.includes(String(s).toLowerCase()));
        if (exact) return 100;
        if (partial) return 65;
        return 20;
      })(),
    },
    feeCommercial: {
      label: "Fee / Commercial",
      weight: OPERATOR_MATCH_WEIGHTS.feeCommercial,
      dealValue: "Fee expectations: " + ([dealRoy && ("Royalty " + dealRoy), dealMktFee && ("Marketing " + dealMktFee), dealLoyaltyFee && ("Loyalty " + dealLoyaltyFee), dealStructure && ("Preferred Structure " + dealStructure)].filter(Boolean).join("; ") || "—"),
      operatorValue: "Commercial terms: " + (opFee || "—"),
      note: "Uses available fee expectations and operator commercial positioning.",
      score: (() => {
        const dealHas = Boolean(dealRoy || dealMktFee || dealLoyaltyFee);
        if (!dealHas && !opFee) return null;
        if (!dealHas || !opFee) return 55;
        return 75;
      })(),
    },
    serviceOfferings: {
      label: "Service Offerings",
      weight: OPERATOR_MATCH_WEIGHTS.serviceOfferings,
      dealValue: "Must-haves from operator: " + (dealMustHaves.join(", ") || "—"),
      operatorValue: "Primary/additional services: " + (opServices.join(", ") || "—"),
      note: "Compares owner must-haves against operator service depth.",
      score: (() => {
        if (dealMustHaves.length === 0 && opServices.length === 0) return null;
        if (dealMustHaves.length === 0) return 75;
        return overlapScore(dealMustHaves, opServices, 30);
      })(),
    },
    systemsReporting: {
      label: "Systems & Reporting",
      weight: OPERATOR_MATCH_WEIGHTS.systemsReporting,
      dealValue: "Reporting preference: " + (toStr((siData || {})["Owner Reporting Cadence"] || "") || "—"),
      operatorValue: "Systems/reporting: " + ([opSystems.join(", "), opReporting].filter(Boolean).join("; ") || "—"),
      note: "Checks whether the operator has systems and reporting cadence signals for owner oversight.",
      score: (() => {
        if (opSystems.length === 0 && !opReporting) return 40;
        if (opSystems.length > 0 && opReporting) return 90;
        return 70;
      })(),
    },
    ownerRelations: {
      label: "Owner Relations",
      weight: OPERATOR_MATCH_WEIGHTS.ownerRelations,
      dealValue: "Owner priority: responsive communication and collaboration",
      operatorValue: opOwnerRel || "—",
      note: "Uses owner-relations signals such as response style, collaboration mode, and references.",
      score: (() => {
        if (!opOwnerRel) return 45;
        if (includesAnyToken(opOwnerRel, ["weekly", "monthly", "collaborat", "owner ref", "advisory"])) return 90;
        return 70;
      })(),
    },
    brandPortfolioRelevance: {
      label: "Brand / Portfolio Relevance",
      weight: OPERATOR_MATCH_WEIGHTS.brandPortfolioRelevance,
      dealValue: "Preferred brands: " + (dealPreferredBrands.join(", ") || "—"),
      operatorValue: "Brands managed: " + (opBrands.join(", ") || "—"),
      note: "Measures overlap between owner preferred brands and operator's active brand portfolio.",
      score: (() => {
        if (dealPreferredBrands.length === 0 && opBrands.length === 0) return null;
        if (dealPreferredBrands.length === 0) return 70;
        return overlapScore(dealPreferredBrands, opBrands, 25);
      })(),
    },
    negativeFitPenalty: {
      label: "Negative-Fit Penalty",
      weight: OPERATOR_MATCH_WEIGHTS.negativeFitPenalty,
      dealValue: "Top deal breakers: " + (dealBreakers.join(", ") || "—"),
      operatorValue: "Less ideal situations: " + (opLessIdeal || "—"),
      note: "Applies a small penalty when deal breakers overlap with operator less-ideal situations.",
      score: (() => {
        if (dealBreakers.length === 0 || !opLessIdeal) return 100;
        const hasConflict = dealBreakers.some((b) => b && opLessIdeal.toLowerCase().includes(b.toLowerCase()));
        return hasConflict ? 20 : 100;
      })(),
    },
  };

  let weighted = 0;
  let totalW = 0;
  for (const f of Object.values(factors)) {
    totalW += f.weight;
    if (f.score != null && !Number.isNaN(Number(f.score))) weighted += (Number(f.score) * f.weight);
  }
  const finalScore = totalW > 0 ? Math.round((weighted / totalW) * 10) / 10 : 0;

  const breakdownDetails = {};
  for (const [k, f] of Object.entries(factors)) {
    breakdownDetails[k] = {
      label: f.label,
      weight: f.weight,
      brandValue: f.operatorValue,
      dealValue: f.dealValue,
      note: f.note,
      score: f.score == null ? "—" : Math.round(Number(f.score) * 10) / 10,
    };
  }
  return { score: Math.min(100, Math.max(0, finalScore)), breakdownDetails };
}

/**
 * GET /api/my-deals/:recordId/operator-match-score-breakdown?operatorId=rec...
 * Returns operatorScore and operatorBreakdownDetails for one deal + one operator.
 */
export async function getOperatorMatchScoreBreakdown(req, res) {
  try {
    const recordId = req.params.recordId;
    const operatorId = toStr(req.query.operatorId);
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid deal ID is required" });
    }
    if (!operatorId || !operatorId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Operator ID (query param operatorId) is required" });
    }

    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    const tableIdOrName = encodeURIComponent(DEALS_TABLE);
    const getRes = await fetch(`https://api.airtable.com/v0/${baseId}/${tableIdOrName}/${encodeURIComponent(recordId)}`, {
      headers: { Authorization: "Bearer " + apiKey },
    });
    const dealData = await getRes.json();
    if (dealData.error || !dealData.fields) {
      return res.status(404).json({ success: false, error: "Deal not found" });
    }

    const f = { ...(dealData.fields || {}) };
    const linkedLocId = getLinkedLocationId(f);
    const mpId = getLinkedMarketPerformanceId(f);
    const siLinkedId = getLinkedStrategicIntentId(f);
    if (!siLinkedId) {
      return res.status(400).json({ success: false, error: "Deal has no Strategic Intent record." });
    }

    const [locationData, mpData, siData, opBundle, brandNameById] = await Promise.all([
      linkedLocId ? fetchLocationRecord(baseId, apiKey, linkedLocId) : null,
      mpId ? fetchMarketPerformanceRecord(baseId, apiKey, mpId) : null,
      fetchStrategicIntentRecord(baseId, apiKey, siLinkedId),
      loadNewBaseOperatorBundle(operatorId),
      loadBrandNameByIdMap().catch(() => new Map()),
    ]);
    if (!opBundle || !opBundle.master) {
      return res.status(404).json({ success: false, error: "Operator not found" });
    }

    const prefill = buildPrefillObjectFromNewBaseRows(
      opBundle.master,
      opBundle.profile,
      opBundle.platform,
      opBundle.commercial,
      opBundle.governance
    );
    const operatorName = toStr((opBundle.master.fields || {}).company_name || prefill.companyName || "Selected Operator");
    const { score, breakdownDetails } = scoreOperatorMatchForDeal(f, locationData || {}, mpData || {}, siData || {}, prefill || {}, brandNameById);

    return res.json({
      success: true,
      operatorId,
      operatorName,
      operatorScore: score,
      operatorBreakdownDetails: breakdownDetails,
    });
  } catch (err) {
    console.error("Error in getOperatorMatchScoreBreakdown:", err);
    return res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}

/**
 * GET /api/my-deals/:recordId/match-score-breakdown?brand=BrandName
 * Returns scoreNew and breakdownNewDetails (12 factors, same as My Deals) for a deal + brand.
 * Used by Brand Development Dashboard to show the same Match Score Breakdown as My Deals.
 */
export async function getMatchScoreBreakdown(req, res) {
  try {
    const recordId = req.params.recordId;
    const brandName = (req.query.brand || "").trim();
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid deal ID is required" });
    }
    if (!brandName) {
      return res.status(400).json({ success: false, error: "Brand name (query param brand) is required" });
    }
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    const tableIdOrName = encodeURIComponent(DEALS_TABLE);
    const getRes = await fetch(`https://api.airtable.com/v0/${baseId}/${tableIdOrName}/${encodeURIComponent(recordId)}`, {
      headers: { Authorization: "Bearer " + apiKey },
    });
    const dealData = await getRes.json();
    if (dealData.error || !dealData.fields) {
      return res.status(404).json({ success: false, error: "Deal not found" });
    }

    const f = { ...(dealData.fields || {}) };
    const linkedLocId = getLinkedLocationId(f);
    const mpId = getLinkedMarketPerformanceId(f);
    const siLinkedId = getLinkedStrategicIntentId(f);

    if (!siLinkedId) {
      return res.status(400).json({ success: false, error: "Deal has no Strategic Intent record." });
    }

    const [locationData, mpData, siData] = await Promise.all([
      linkedLocId ? fetchLocationRecord(baseId, apiKey, linkedLocId) : null,
      mpId ? fetchMarketPerformanceRecord(baseId, apiKey, mpId) : null,
      fetchStrategicIntentRecord(baseId, apiKey, siLinkedId),
    ]);

    const { scoreNew, breakdownNewDetails } = await computeMatchScoreForDealBrand(
      f, locationData, mpData, siData, brandName, baseId, apiKey
    );

    res.json({ scoreNew: scoreNew ?? null, breakdownNewDetails: breakdownNewDetails || {} });
  } catch (err) {
    console.error("Error in getMatchScoreBreakdown:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}

/**
 * Refresh Deal Brand Cache for one deal. Called by HTTP handler, deal PATCH, background full refresh, and bulk script.
 * @param {string} baseId
 * @param {string} apiKey
 * @param {string} recordId
 * @returns {Promise<{ success: boolean, preferredBrandsChosen?: string, preferredScore?: number, topAlternativesCount?: number, bestMatchBrand?: string, bestMatchScore?: number }>}
 */
export async function refreshDealBrandCacheForRecordId(baseId, apiKey, recordId) {
  const getRes = await fetch(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(DEALS_TABLE)}/${encodeURIComponent(recordId)}`, {
    headers: { Authorization: "Bearer " + apiKey },
  });
  const dealData = await getRes.json();
  if (dealData.error || !dealData.fields) {
    throw new Error(dealData.error?.message || "Deal not found");
  }

  const f = { ...(dealData.fields || {}) };
    const linkedLocId = getLinkedLocationId(f);
    const mpId = getLinkedMarketPerformanceId(f);
  const siLinkedId = getLinkedStrategicIntentId(f);
  if (!siLinkedId) {
    throw new Error("Deal has no Strategic Intent record.");
  }

  const [locationData, mpData, siData] = await Promise.all([
    linkedLocId ? fetchLocationRecord(baseId, apiKey, linkedLocId) : null,
    mpId ? fetchMarketPerformanceRecord(baseId, apiKey, mpId) : null,
    fetchStrategicIntentRecord(baseId, apiKey, siLinkedId),
  ]);

  const siDataMap = new Map([[siLinkedId, siData || {}]]);
  const resolvedMap = await preferredBrandsMapFromSiDataMapResolved(baseId, apiKey, siDataMap);
  const preferredStr = resolvedMap.get(siLinkedId) || "";
  const preferredBrandsSet = preferredStr ? preferredStr.split(/\s*,\s*/).map((s) => s.trim()).filter(Boolean) : [];
  const preferredBrandsChosen = preferredBrandsSet.length > 0 ? preferredBrandsSet.join(", ") : "";

  const matchScoresNewByBrand = {};
  const breakdownNewDetailsByBrand = {};
  let preferredScore = null;
  for (const brandName of preferredBrandsSet) {
    const { scoreNew, breakdownNewDetails } = await computeMatchScoreForDealBrand(f, locationData, mpData, siData, brandName, baseId, apiKey);
    const s = scoreNew != null && scoreNew !== "" ? Number(scoreNew) : null;
    matchScoresNewByBrand[brandName] = s;
    if (breakdownNewDetails && typeof breakdownNewDetails === "object") breakdownNewDetailsByBrand[brandName] = breakdownNewDetails;
    if (preferredScore == null && s != null) preferredScore = s;
  }
  if (preferredScore == null && preferredBrandsSet[0]) {
    preferredScore = matchScoresNewByBrand[preferredBrandsSet[0]];
  }

  const topResult = await computeTopAlternativeBrands(f, locationData, mpData, siData, preferredBrandsSet, baseId, apiKey, 5);
  const topAlternatives = (topResult.alternatives || []).map((a) => ({ brand: a.brand, score: a.score, breakdownNewDetails: a.breakdownNewDetails || {} }));

  const bestResult = await computeRecommendedBrand(f, locationData, mpData, siData, preferredBrandsSet, baseId, apiKey);
  const bestMatchBrand = bestResult && bestResult.brand ? bestResult.brand : null;
  const bestMatchScore = bestResult && bestResult.scoreNew != null ? bestResult.scoreNew : null;

  const dealName = (f["Project Name"] ?? f["Property Name"] ?? f["Name"] ?? "").toString().trim() || "Deal cache";
  await upsertDealBrandCache(baseId, apiKey, recordId, {
    dealName,
    preferredBrandsChosen,
    matchScoresNewByBrand,
    breakdownNewDetailsByBrand,
    preferredScore,
    topAlternatives,
    bestMatchBrand,
    bestMatchScore,
  });

  return {
    success: true,
    preferredBrandsChosen,
    preferredScore,
    topAlternativesCount: topAlternatives.length,
    bestMatchBrand,
    bestMatchScore,
  };
}

/** In-memory flag: has background full cache refresh been started (once per server process)? */
let fullCacheRefreshStarted = false;

/** First getMyDeals load after server start: add short delay so Airtable connection warms up; avoids blank Deal Type on cold start. */
let getMyDealsColdStart = true;
/** Timestamp when getMyDeals last finished sending response; used to enforce MY_DEALS_MIN_GAP_MS between list loads. */
let lastGetMyDealsFinishedAt = 0;

/** Generate a per-request id for correlation (timestamp + random suffix). */
function getMyDealsRequestId() {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

/** True when we should log the detailed getMyDeals timing summary (dev or DEBUG_MY_DEALS). */
function shouldLogMyDealsSummary() {
  return process.env.NODE_ENV !== "production" || process.env.DEBUG_MY_DEALS;
}

/** Log getMyDeals timing summary only in dev or when DEBUG_MY_DEALS is set. */
function logMyDealsSummary(timingSummary) {
  if (!shouldLogMyDealsSummary()) return;
  console.log("getMyDeals: summary", timingSummary);
}

/** Fire-and-forget background full refresh for all deals. Runs once per server process on first My Deals load. */
function startBackgroundFullCacheRefresh(baseId, apiKey, dealIds) {
  if (fullCacheRefreshStarted || !dealIds || dealIds.length === 0) return;
  fullCacheRefreshStarted = true;
  const ids = [...dealIds];
  const concurrency = 2;
  const run = async () => {
    for (let i = 0; i < ids.length; i += concurrency) {
      const batch = ids.slice(i, i + concurrency);
      await Promise.allSettled(
        batch.map((recordId) =>
          refreshDealBrandCacheForRecordId(baseId, apiKey, recordId).catch((e) => {
            console.warn("[Deal Brand Cache] Background refresh failed for", recordId, ":", e.message);
          })
        )
      );
    }
    console.log("[Deal Brand Cache] Background full refresh completed for", ids.length, "deals.");
  };
  setImmediate(() => run());
}

/**
 * POST /api/my-deals/:recordId/refresh-brand-cache
 * Pre-compute preferred brands (resolved names), their match scores, and top 5 alternatives; write to Deal Brand Cache table. Speeds up list load and Alternative Brand Suggestions.
 */
export async function refreshDealBrandCache(req, res) {
  try {
    const recordId = req.params.recordId;
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid deal ID is required" });
    }
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }
    const result = await refreshDealBrandCacheForRecordId(baseId, apiKey, recordId);
    res.json({ ...result, message: "Deal Brand Cache updated." });
  } catch (err) {
    console.error("Error in refreshDealBrandCache:", err);
    if (err.message?.includes("Deal not found")) return res.status(404).json({ success: false, error: err.message });
    if (err.message?.includes("Strategic Intent")) return res.status(400).json({ success: false, error: err.message });
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}

/** Fetch a deal by id and merge in all linked records (Location, Market Performance, Strategic Intent, Contact & Uploads). Returns { deal, normalized } or null if not found. */
async function fetchDealWithMergedLinkedRecords(baseId, apiKey, recordId) {
  const tableIdOrName = encodeURIComponent(DEALS_TABLE);
  const url = `https://api.airtable.com/v0/${baseId}/${tableIdOrName}/${encodeURIComponent(recordId)}`;
  const getRes = await fetch(url, {
    headers: { Authorization: "Bearer " + apiKey },
  });
  const result = await getRes.json();
  if (result.error) return null;
  const linkedId = getLinkedLocationId(result.fields || {});
  let locationFormKeyed = null;
  const locationMap = new Map();
  if (linkedId) {
    const loc = await fetchLocationRecord(baseId, apiKey, linkedId);
    if (loc) {
      locationMap.set(linkedId, loc);
      locationFormKeyed = loc;
    }
  }
  const mpLinkedId = getLinkedMarketPerformanceId(result.fields || {});
  if (mpLinkedId) {
    const mpFields = await fetchMarketPerformanceRecord(baseId, apiKey, mpLinkedId);
    if (mpFields && typeof mpFields === "object") {
      const merge = {};
      for (const [key, val] of Object.entries(mpFields)) {
        const formName = MP_TABLE_TO_FORM[key] ?? key;
        merge[formName] = val;
      }
      result.fields = { ...(result.fields || {}), ...merge };
    }
  }
  const siLinkedId = getLinkedStrategicIntentId(result.fields || {});
  if (siLinkedId) {
    const siFields = await fetchStrategicIntentRecord(baseId, apiKey, siLinkedId);
    if (siFields && typeof siFields === "object") {
      const siMerge = strategicIntentToFormFields(siFields);
      result.fields = { ...(result.fields || {}), ...siMerge };
    }
  }
  const cuLinkedId = getLinkedContactUploadsId(result.fields || {});
  if (cuLinkedId) {
    const cuFields = await fetchContactUploadsRecord(baseId, apiKey, cuLinkedId);
    if (cuFields && typeof cuFields === "object") {
      const cuMerge = contactUploadsToFormFields(cuFields);
      result.fields = { ...(result.fields || {}), ...cuMerge };
    }
  }
  let lsLinkedId = getLinkedLeaseStructureId(result.fields || {});
  if (!lsLinkedId) lsLinkedId = await findLeaseStructureRecordIdByDealId(baseId, apiKey, recordId);
  if (lsLinkedId) {
    const lsFields = await fetchLeaseStructureRecord(baseId, apiKey, lsLinkedId);
    if (lsFields && typeof lsFields === "object") {
      const lsMerge = leaseStructureToFormFields(lsFields);
      result.fields = { ...(result.fields || {}), ...lsMerge };
    }
  }
  // Map Deals Airtable column names to form keys for client rebind (Batch 1)
  for (const [airtableKey, formKey] of Object.entries(DEALS_AIRTABLE_TO_FORM)) {
    if (result.fields[airtableKey] !== undefined) {
      result.fields[formKey] = result.fields[airtableKey];
      delete result.fields[airtableKey];
    }
  }
  // Normalise legacy option value for Existing flag so select prefill matches current HTML option (Batch 1 prefill fix)
  const existingFlagKey = "Existing flag staying or being replaced?";
  if (result.fields[existingFlagKey] === "Not Applicable (Undranded or New Build)") {
    result.fields[existingFlagKey] = "Not Applicable (Unbranded or New Build)";
  }
  // Merge Location (form-keyed from fetchLocationRecord) into result.fields so GET and save-response rebind repopulate (M3)
  if (locationFormKeyed) {
    result.fields = { ...(result.fields || {}), ...locationFormKeyed };
  }
  const normalized = await recordToDeal(result, locationMap);
  if (locationFormKeyed) normalized.expandedLocation = locationFormKeyed;
  return { deal: result, normalized };
}

/** Airtable Date field: coerce form text (e.g. "Dec 2026", "Q2 2026") to ISO YYYY-MM-DD when possible. */
function toAirtableDateString(val) {
  if (val == null || typeof val !== "string") return val;
  const s = val.trim();
  if (!s) return val;
  const iso = /^\d{4}-\d{2}-\d{2}$/;
  if (iso.test(s)) return s;
  const monthNames = "jan feb mar apr may jun jul aug sep oct nov dec".split(" ");
  const match = s.match(/^(\w+)\s*(\d{4})$/i) || s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (match) {
    if (match[2] && match[3]) {
      const m = parseInt(match[1], 10);
      const d = parseInt(match[2], 10);
      const y = parseInt(match[3], 10);
      if (m >= 1 && m <= 12 && d >= 1 && d <= 31) return `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
    } else {
      const month = monthNames.indexOf(match[1].toLowerCase().slice(0, 3));
      const year = parseInt(match[2], 10);
      if (month >= 0 && !isNaN(year)) return `${year}-${String(month + 1).padStart(2, "0")}-01`;
    }
  }
  const qMatch = s.match(/^Q([1-4])\s*(\d{4})$/i);
  if (qMatch) {
    const q = parseInt(qMatch[1], 10);
    const y = parseInt(qMatch[2], 10);
    const month = (q - 1) * 3 + 1;
    return `${y}-${String(month).padStart(2, "0")}-01`;
  }
  return val;
}

/**
 * PATCH /api/my-deals/:recordId – update a single deal.
 * Body: { dealStatus: "Active" } or { fields: { "Field Name": value, ... } } for full/partial update.
 */
export async function updateMyDealById(req, res) {
  try {
    const recordId = req.params.recordId;
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid record ID is required" });
    }
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    const body = req.body && typeof req.body === "object" ? req.body : {};
    let fields = {};

    if (body.fields && typeof body.fields === "object") {
      fields = { ...body.fields };
    }
    if (body.dealStatus != null && typeof body.dealStatus === "string") {
      const v = body.dealStatus.trim();
      fields[DEALS_STATUS_FIELD] = v;
    }
    if (body.formStatus != null && typeof body.formStatus === "string") {
      fields["Form Status"] = body.formStatus.trim();
    }
    if (body.dealRoomEnabled !== undefined) {
      fields["Deal Room Enabled"] = !!body.dealRoomEnabled;
    }
    if (body.ndaTemplateFile !== undefined && Array.isArray(body.ndaTemplateFile)) {
      const arr = body.ndaTemplateFile.map((a) =>
        typeof a === "object" && a?.url ? { url: a.url, filename: a.filename || "nda-template.pdf" } : null
      ).filter(Boolean);
      fields["NDA Template File"] = arr;
    }

    const validation = validateDealSetupPayload(fields);
    let payload = validation.payload;
    if (!validation.valid && (body.dealRoomEnabled !== undefined || (body.ndaTemplateFile && Array.isArray(body.ndaTemplateFile)))) {
      payload = { ...payload };
      if (body.dealRoomEnabled !== undefined) payload["Deal Room Enabled"] = !!body.dealRoomEnabled;
      if (body.ndaTemplateFile && Array.isArray(body.ndaTemplateFile)) {
        const arr = body.ndaTemplateFile.map((a) => (typeof a === "object" && a?.url ? { url: a.url, filename: a.filename || "nda-template.pdf" } : null)).filter(Boolean);
        if (arr.length) payload["NDA Template File"] = arr;
      }
      if (Object.keys(payload).length > 0) validation.valid = true;
    }
    if (!validation.valid) {
      const msg = validation.errors.length ? validation.errors.map((e) => e.message).join("; ") : "Validation failed";
      if (process.env.NODE_ENV !== "production" || process.env.DEBUG_DEAL_SETUP === "true") {
        console.warn("[deal-setup PATCH] validation failed:", validation.errors);
      }
      return res.status(400).json({ success: false, error: msg });
    }
    fields = payload;
    if (process.env.NODE_ENV !== "production" || process.env.DEBUG_DEAL_SETUP === "true") {
      console.log("[deal-setup PATCH] validation ok; field mapping used:", validation.fieldMappingUsed ?? "(none)");
      const safePreview = Object.fromEntries(Object.entries(fields).slice(0, 15).map(([k, v]) => [k, typeof v === "string" ? v.slice(0, 50) : v]));
      console.log("[deal-setup PATCH] sanitized payload preview (first 15 keys):", JSON.stringify(safePreview));
    }

    // Sync address, location, and owner/portfolio fields to Location & Property table (central mapping)
    const hasLocationFields = LOCATION_FORM_FIELDS.some((f) => fields[f] !== undefined);
    if (hasLocationFields) {
      const locFields = {};
      const numericLocationKeys = ["Total Number of Rooms/Keys", "Number of Standard Rooms", "Number of Suites", "# of Stories", "Number of Stories"];
      const multiSelectLocationKeys = ["Ownership Type", "Access to Transit or Highway"];
      for (const formName of LOCATION_FORM_FIELDS) {
        const val = fields[formName];
        if (val === undefined || val === null) continue;
        const airtableName = LOCATION_FORM_TO_AIRTABLE[formName] ?? formName;
        if (numericLocationKeys.includes(formName)) {
          const num = typeof val === "number" ? val : parseInt(String(val).trim(), 10);
          if (!Number.isNaN(num) && String(val).trim() !== "") locFields[airtableName] = num;
          continue;
        }
        if (multiSelectLocationKeys.includes(formName)) {
          const arr = Array.isArray(val) ? val.map((v) => String(v).trim()).filter(Boolean) : String(val).trim().split(/\s*,\s*/).filter(Boolean);
          if (arr.length) locFields[airtableName] = arr;
          continue;
        }
        if (typeof val === "string" && val.trim() !== "") locFields[airtableName] = val.trim();
        else if (typeof val === "number" && !Number.isNaN(val)) locFields[airtableName] = val;
      }
      for (const formName of LOCATION_FORM_FIELDS) delete fields[formName];
      if (Object.keys(locFields).length > 0) {
        const getUrl = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(DEALS_TABLE)}/${encodeURIComponent(recordId)}`;
        const getRes = await fetch(getUrl, { headers: { Authorization: "Bearer " + apiKey } });
        const currentDeal = await getRes.json();
        const linkedId = currentDeal.error ? null : getLinkedLocationId(currentDeal.fields || {});
        const locTable = encodeURIComponent(LOCATION_PROPERTY_TABLE);
        if (linkedId) {
          const patchLocUrl = `https://api.airtable.com/v0/${baseId}/${locTable}/${encodeURIComponent(linkedId)}`;
          const patchLocRes = await fetch(patchLocUrl, {
            method: "PATCH",
            headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
            body: JSON.stringify({ fields: locFields, typecast: true }),
          });
          const patchLocData = await patchLocRes.json();
          if (patchLocData.error) {
            return res.status(400).json({ success: false, error: "Location & Property: " + (patchLocData.error.message || "update failed") });
          }
        } else {
          const postLocUrl = `https://api.airtable.com/v0/${baseId}/${locTable}`;
          const newLocFields = { ...locFields, Deal_ID: [recordId] };
          const createRes = await fetch(postLocUrl, {
            method: "POST",
            headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
            body: JSON.stringify({ fields: newLocFields, typecast: true }),
          });
          const createData = await createRes.json();
          if (createData.error) {
            return res.status(400).json({ success: false, error: "Location & Property: " + (createData.error.message || "create failed") });
          }
          if (createData.id) fields[LOCATION_LINK_FIELD] = [createData.id];
        }
      }
    }

    // Extract Market & Performance fields and sync to linked "Market - Performance - Deal & Capital Structure" table
    const mpFieldsPayload = {};
    for (const name of MARKET_PERFORMANCE_FIELD_NAMES) {
      if (fields[name] === undefined) continue;
      const tableName = MP_FORM_TO_TABLE[name] ?? name;
      let val = fields[name];
      if (name === "Primary Demand Drivers" && typeof val === "string" && val.trim() !== "") {
        val = val.split(/\s*,\s*/).map((s) => s.trim()).filter(Boolean);
      } else if (name === "Primary Demand Drivers" && Array.isArray(val)) {
        val = val.map((s) => (typeof s === "string" ? s : (s && s.name) || "").trim()).filter(Boolean);
      }
      mpFieldsPayload[tableName] = val;
      delete fields[name];
    }
    if (Object.keys(mpFieldsPayload).length > 0) {
      const getUrlForMp = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(DEALS_TABLE)}/${encodeURIComponent(recordId)}`;
      const getResMp = await fetch(getUrlForMp, { headers: { Authorization: "Bearer " + apiKey } });
      const currentDealForMp = await getResMp.json();
      const mpLinkedId = currentDealForMp.error ? null : getLinkedMarketPerformanceId(currentDealForMp.fields || {});
      const mpTable = encodeURIComponent(MARKET_PERFORMANCE_TABLE);
      if (mpLinkedId) {
        const patchMpRes = await fetch(`https://api.airtable.com/v0/${baseId}/${mpTable}/${encodeURIComponent(mpLinkedId)}`, {
          method: "PATCH",
          headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
          body: JSON.stringify({ fields: mpFieldsPayload, typecast: true }),
        });
        const patchMpData = await patchMpRes.json();
        if (patchMpData.error) {
          return res.status(400).json({ success: false, error: "Market - Performance: " + (patchMpData.error.message || "update failed") });
        }
      } else {
        const createMpFields = { ...mpFieldsPayload, [MP_DEAL_LINK_FIELD]: [recordId] };
        const postMpRes = await fetch(`https://api.airtable.com/v0/${baseId}/${mpTable}`, {
          method: "POST",
          headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
          body: JSON.stringify({ fields: createMpFields, typecast: true }),
        });
        const createMpData = await postMpRes.json();
        if (createMpData.error) {
          return res.status(400).json({ success: false, error: "Market - Performance: " + (createMpData.error.message || "create failed") });
        }
        if (createMpData.id) fields[MARKET_PERFORMANCE_LINK_FIELD] = [createMpData.id];
      }
    }

    // Sync all Strategic Intent form fields to linked Strategic Intent - Operational - Key Challenges table (Batch 4 Q1: create + link when missing)
    const hasAnySiField = STRATEGIC_INTENT_FORM_FIELDS.some((f) => fields[f] !== undefined);
    if (hasAnySiField) {
      const getUrlForSi = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(DEALS_TABLE)}/${encodeURIComponent(recordId)}`;
      const getResSi = await fetch(getUrlForSi, { headers: { Authorization: "Bearer " + apiKey } });
      const currentDealForSi = await getResSi.json();
      const siLinkedId = currentDealForSi.error ? null : getLinkedStrategicIntentId(currentDealForSi.fields || {});
      const siPayload = formFieldsToStrategicIntentPayload(fields);
      if (Object.keys(siPayload).length > 0) {
        const siTable = encodeURIComponent(STRATEGIC_INTENT_TABLE);
        if (siLinkedId) {
          const patchSiRes = await fetch(`https://api.airtable.com/v0/${baseId}/${siTable}/${encodeURIComponent(siLinkedId)}`, {
            method: "PATCH",
            headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
            body: JSON.stringify({ fields: siPayload, typecast: true }),
          });
          const patchSiData = await patchSiRes.json();
          if (patchSiData.error) {
            return res.status(400).json({ success: false, error: "Strategic Intent: " + (patchSiData.error.message || "update failed") });
          }
        } else {
          const postSiRes = await fetch(`https://api.airtable.com/v0/${baseId}/${siTable}`, {
            method: "POST",
            headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
            body: JSON.stringify({ fields: siPayload, typecast: true }),
          });
          const createSiData = await postSiRes.json();
          if (createSiData.error) {
            return res.status(400).json({ success: false, error: "Strategic Intent: " + (createSiData.error.message || "create failed") });
          }
          if (createSiData.id) {
            fields[STRATEGIC_INTENT_LINK_FIELD] = [createSiData.id];
          }
        }
      }
      for (const key of STRATEGIC_INTENT_FORM_FIELDS) delete fields[key];
    }

    // Sync Support & Communications, Contact Info, Uploads & Attachments to linked Contact & Uploads table
    const hasAnyCuField = CONTACT_UPLOADS_FORM_FIELDS.some((f) => fields[f] !== undefined);
    if (hasAnyCuField) {
      const getUrlForCu = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(DEALS_TABLE)}/${encodeURIComponent(recordId)}`;
      const getResCu = await fetch(getUrlForCu, { headers: { Authorization: "Bearer " + apiKey } });
      const currentDealForCu = await getResCu.json();
      const cuLinkedId = currentDealForCu.error ? null : getLinkedContactUploadsId(currentDealForCu.fields || {});
      const cuPayload = formFieldsToContactUploadsPayload(fields);
      if (Object.keys(cuPayload).length > 0) {
        const cuTable = encodeURIComponent(CONTACT_UPLOADS_TABLE);
        if (cuLinkedId) {
          const patchCuRes = await fetch(`https://api.airtable.com/v0/${baseId}/${cuTable}/${encodeURIComponent(cuLinkedId)}`, {
            method: "PATCH",
            headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
            body: JSON.stringify({ fields: cuPayload, typecast: true }),
          });
          const patchCuData = await patchCuRes.json();
          if (patchCuData.error) {
            return res.status(400).json({ success: false, error: "Contact & Uploads: " + (patchCuData.error.message || "update failed") });
          }
        } else {
          const createCuFields = { ...cuPayload, [CU_DEAL_LINK_FIELD]: [recordId] };
          const postCuRes = await fetch(`https://api.airtable.com/v0/${baseId}/${cuTable}`, {
            method: "POST",
            headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
            body: JSON.stringify({ fields: createCuFields, typecast: true }),
          });
          const createCuData = await postCuRes.json();
          if (createCuData.error) {
            return res.status(400).json({ success: false, error: "Contact & Uploads: " + (createCuData.error.message || "create failed") });
          }
          if (createCuData.id) fields[CONTACT_UPLOADS_LINK_FIELD] = [createCuData.id];
        }
      }
      for (const key of CONTACT_UPLOADS_FORM_FIELDS) delete fields[key];
    }

    // Sync Lease Structure form fields to linked Lease Structure table
    const hasAnyLsField = LEASE_STRUCTURE_FORM_FIELDS.some((f) => fields[f] !== undefined);
    if (hasAnyLsField) {
      const getUrlForLs = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(DEALS_TABLE)}/${encodeURIComponent(recordId)}`;
      const getResLs = await fetch(getUrlForLs, { headers: { Authorization: "Bearer " + apiKey } });
      const currentDealForLs = await getResLs.json();
      let lsLinkedId = currentDealForLs.error ? null : getLinkedLeaseStructureId(currentDealForLs.fields || {});
      if (!lsLinkedId) lsLinkedId = await findLeaseStructureRecordIdByDealId(baseId, apiKey, recordId);
      const lsPayload = formFieldsToLeaseStructurePayload(fields);
      if (Object.keys(lsPayload).length > 0) {
        const lsTable = encodeURIComponent(LEASE_STRUCTURE_TABLE);
        if (lsLinkedId) {
          const patchLsRes = await fetch(`https://api.airtable.com/v0/${baseId}/${lsTable}/${encodeURIComponent(lsLinkedId)}`, {
            method: "PATCH",
            headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
            body: JSON.stringify({ fields: lsPayload, typecast: true }),
          });
          const patchLsData = await patchLsRes.json();
          if (patchLsData.error) {
            return res.status(400).json({ success: false, error: "Lease Structure: " + (patchLsData.error.message || "update failed") });
          }
        } else {
          const createLsFields = { ...lsPayload, [LS_DEAL_LINK_FIELD]: [recordId] };
          const postLsRes = await fetch(`https://api.airtable.com/v0/${baseId}/${lsTable}`, {
            method: "POST",
            headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
            body: JSON.stringify({ fields: createLsFields, typecast: true }),
          });
          const createLsData = await postLsRes.json();
          if (createLsData.error) {
            return res.status(400).json({ success: false, error: "Lease Structure: " + (createLsData.error.message || "create failed") });
          }
          // Lease Structure record links to this deal via Deal_ID; next load will find it via findLeaseStructureRecordIdByDealId
        }
      }
      for (const key of LEASE_STRUCTURE_FORM_FIELDS) delete fields[key];
    }

    if (Object.keys(fields).length === 0) {
      // Return same shape as main PATCH path: full merged deal with form keys (Batch 3 Q1)
      const full = await fetchDealWithMergedLinkedRecords(baseId, apiKey, recordId);
      if (full) {
        return res.json({ success: true, record: full.deal, normalized: full.normalized });
      }
      return res.status(400).json({ success: false, error: "No updatable fields provided" });
    }

    // Map form keys to Airtable column names for Deals (Batch 1 and other mapped fields)
    const dealFieldsForAirtable = {};
    for (const [formKey, val] of Object.entries(fields)) {
      const airtableKey = DEALS_FORM_TO_AIRTABLE[formKey] ?? formKey;
      dealFieldsForAirtable[airtableKey] = val;
    }

    const dateFieldName = "Current Franchise/Management Contract End Date";
    if (dealFieldsForAirtable[dateFieldName] != null && typeof dealFieldsForAirtable[dateFieldName] === "string") {
      const coerced = toAirtableDateString(dealFieldsForAirtable[dateFieldName]);
      if (coerced && /^\d{4}-\d{2}-\d{2}$/.test(coerced)) {
        dealFieldsForAirtable[dateFieldName] = coerced;
      } else if (dealFieldsForAirtable[dateFieldName].trim() !== "") {
        delete dealFieldsForAirtable[dateFieldName];
      }
    }

    const strNumberFieldName = "Property STR Number (if applicable)";
    if (dealFieldsForAirtable[strNumberFieldName] !== undefined) {
      const raw = dealFieldsForAirtable[strNumberFieldName];
      if (raw === "" || raw == null || (typeof raw === "string" && raw.trim() === "")) {
        delete dealFieldsForAirtable[strNumberFieldName];
      } else if (typeof raw === "string") {
        const num = parseInt(raw.trim().replace(/,/g, ""), 10);
        if (!Number.isNaN(num) && num >= 0) {
          dealFieldsForAirtable[strNumberFieldName] = num;
        } else {
          delete dealFieldsForAirtable[strNumberFieldName];
        }
      } else if (typeof raw === "number" && (Number.isNaN(raw) || raw < 0)) {
        delete dealFieldsForAirtable[strNumberFieldName];
      }
    }

    // Primary Demand Drivers is written to linked Market - Performance table above; no conversion needed for Deals here.

    // Remove the other status field name if present – use only DEALS_STATUS_FIELD to avoid "Unknown field name" error
    if (DEALS_STATUS_FIELD === "Deal Status") delete dealFieldsForAirtable["Status"];
    else if (DEALS_STATUS_FIELD === "Status") delete dealFieldsForAirtable["Deal Status"];

    const tableIdOrName = encodeURIComponent(DEALS_TABLE);
    const url = `https://api.airtable.com/v0/${baseId}/${tableIdOrName}/${encodeURIComponent(recordId)}`;
    const patchRes = await fetch(url, {
      method: "PATCH",
      headers: {
        Authorization: "Bearer " + apiKey,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields: dealFieldsForAirtable, typecast: true }),
    });
    const result = await patchRes.json();
    if (result.error) {
      return res.status(400).json({ success: false, error: result.error.message });
    }
    // Return the full deal with merged linked data (Location, Market Performance, Strategic Intent, Contact & Uploads) so the form does not lose prefill when it calls setFieldValues(record.fields).
    const full = await fetchDealWithMergedLinkedRecords(baseId, apiKey, recordId);
    refreshDealBrandCacheForRecordId(baseId, apiKey, recordId).catch((e) =>
      console.warn("[Deal Brand Cache] Background refresh after save failed for", recordId, ":", e.message)
    );
    if (full) {
      return res.json({ success: true, record: full.deal, normalized: full.normalized });
    }
    res.json({ success: true, record: result });
  } catch (err) {
    console.error("Error in updateMyDealById:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}

// ---------------------------------------------------------------------------
// Deal Setup attachment upload (Tab 13): POST /api/my-deals/:recordId/attachments
// Multer runs in server.js; req.files and req.params.recordId are set. Storage: local disk; URLs served via GET route.
export const ALLOWED_ATTACHMENT_EXTENSIONS = [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".jpg", ".jpeg", ".png"];
export const MAX_ATTACHMENT_FILE_SIZE_BYTES = 10 * 1024 * 1024; // 10 MB

/**
 * POST /api/my-deals/:recordId/attachments – multipart upload for Deal Setup attachments.
 * Ensures Contact & Uploads link exists (create + patch deal if needed), stores files, appends to CU attachment field.
 * Returns { success, dealId, cuRecordId, attachments } or { success: false, error }.
 * 413 if file too large; 400 if no files / invalid type; 404 if deal not found.
 */
export async function uploadDealAttachments(req, res) {
  try {
    const recordId = req.params.recordId;
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid deal record ID is required" });
    }
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    const files = Array.isArray(req.files) ? req.files : [];
    if (files.length === 0) {
      return res.status(400).json({ success: false, error: "No files selected or upload failed" });
    }

    const full = await fetchDealWithMergedLinkedRecords(baseId, apiKey, recordId);
    if (!full) {
      return res.status(404).json({ success: false, error: "Deal not found" });
    }

    let cuRecordId = getLinkedContactUploadsId(full.deal.fields || {});
    if (!cuRecordId) {
      await waitAirtableSerial();
      const cuTable = encodeURIComponent(CONTACT_UPLOADS_TABLE);
      const createCuFields = { [CU_DEAL_LINK_FIELD]: [recordId] };
      const postCuRes = await fetch(`https://api.airtable.com/v0/${baseId}/${cuTable}`, {
        method: "POST",
        headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
        body: JSON.stringify({ fields: createCuFields, typecast: true }),
      });
      const createCuData = await postCuRes.json();
      if (createCuData.error) {
        return res.status(400).json({ success: false, error: "Contact & Uploads: " + (createCuData.error.message || "create failed") });
      }
      if (!createCuData.id) {
        return res.status(500).json({ success: false, error: "Contact & Uploads: create did not return record ID" });
      }
      cuRecordId = createCuData.id;
      await waitAirtableSerial();
      const dealsTable = encodeURIComponent(DEALS_TABLE);
      const patchDealRes = await fetch(`https://api.airtable.com/v0/${baseId}/${dealsTable}/${encodeURIComponent(recordId)}`, {
        method: "PATCH",
        headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
        body: JSON.stringify({ fields: { [CONTACT_UPLOADS_LINK_FIELD]: [cuRecordId] }, typecast: true }),
      });
      const patchDealData = await patchDealRes.json();
      if (patchDealData.error) {
        return res.status(400).json({ success: false, error: "Deal link: " + (patchDealData.error.message || "update failed") });
      }
    }

    const baseUrl = (process.env.BASE_URL || process.env.PUBLIC_APP_URL || "").trim() || (req.protocol + "://" + req.get("host"));
    const newItems = files.map((f) => ({
      url: baseUrl + "/api/my-deals/" + encodeURIComponent(recordId) + "/attachments/" + encodeURIComponent(f.filename),
      filename: (f.originalname || f.filename || "file").trim() || f.filename,
    }));

    const cuFields = await fetchContactUploadsRecord(baseId, apiKey, cuRecordId);
    const existingRaw = (cuFields && cuFields[CU_ATTACHMENT_FIELD]) || [];
    const existing = Array.isArray(existingRaw)
      ? existingRaw.map((e) => ({
          url: (typeof e === "object" && e && e.url) ? e.url : (typeof e === "string" ? e : ""),
          filename: (typeof e === "object" && e && (e.filename ?? e.name)) ? String(e.filename ?? e.name) : "",
        })).filter((e) => e.url)
      : [];
    const merged = [...existing, ...newItems];

    await waitAirtableSerial();
    const cuTable = encodeURIComponent(CONTACT_UPLOADS_TABLE);
    const patchCuRes = await fetch(`https://api.airtable.com/v0/${baseId}/${cuTable}/${encodeURIComponent(cuRecordId)}`, {
      method: "PATCH",
      headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
      body: JSON.stringify({ fields: { [CU_ATTACHMENT_FIELD]: merged }, typecast: true }),
    });
    const patchCuData = await patchCuRes.json();
    if (patchCuData.error) {
      return res.status(400).json({ success: false, error: "Airtable write failed: " + (patchCuData.error.message || "Contact & Uploads update failed") });
    }

    return res.json({
      success: true,
      dealId: recordId,
      cuRecordId,
      attachments: merged,
    });
  } catch (err) {
    console.error("Error in uploadDealAttachments:", err);
    res.status(500).json({ success: false, error: err.message || "Upload failed" });
  }
}

