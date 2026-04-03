/**
 * Lists operators from **Operator Setup — …** tables (Master + Profile, Platform, Case Studies,
 * Diligence) plus **Brand Setup — Brand Basics** for linked brand name resolution.
 *
 * Mounted at: GET /api/intake/third-party-operators, /api/third-party-operators/list, /api/third-party-operators
 * Query: `activeOnly=1` (or `explorer=1`) limits to Master submission_status Active — Operator Explorer.
 */

import {
  NEW_BASE_MASTER_TABLE,
  NEW_BASE_PROFILE_TABLE,
  NEW_BASE_PLATFORM_TABLE,
  NEW_BASE_CASE_STUDIES_TABLE,
  NEW_BASE_DILIGENCE_TABLE,
  fetchAllRecordsRest,
  buildNewBaseListRow,
  logOperatorReadPath,
} from "./lib/operator-setup-new-base-read.js";

const BRAND_BASICS_TABLE =
  process.env.AIRTABLE_BRAND_SETUP_BASICS_TABLE || "Brand Setup - Brand Basics";

const AIRTABLE_READ_HINT =
  "Airtable returned 403: this token cannot list records on those tables. Operators use the same base as Brand Setup (your AIRTABLE_BASE_ID). For a Personal Access Token, enable **data.records:read** for that base.";

function formatListValue(val) {
  if (val == null) return "";
  if (typeof val === "string") return val.trim();
  if (typeof val === "number" && Number.isFinite(val)) return String(val);
  if (Array.isArray(val)) {
    return val
      .map((v) => {
        if (typeof v === "string") return v.trim();
        if (v && typeof v === "object" && typeof v.name === "string") return v.name.trim();
        return "";
      })
      .filter(Boolean)
      .join(", ");
  }
  if (typeof val === "object" && val !== null && typeof val.name === "string") {
    return val.name.trim();
  }
  return String(val).trim();
}

/** First linked row per Master id (Operator field → Master record id). */
function mapFirstLinkedByMaster(rows) {
  const m = new Map();
  for (const r of rows || []) {
    const op = r.fields && r.fields.Operator;
    const mid = Array.isArray(op) && op[0] ? op[0] : null;
    if (mid && !m.has(mid)) m.set(mid, r);
  }
  return m;
}

/** Group child rows by Master id (Operator field). */
function groupChildrenByMaster(rows) {
  const out = new Map();
  for (const r of rows || []) {
    const op = r.fields && r.fields.Operator;
    const ids = Array.isArray(op) ? op : [];
    for (const mid of ids) {
      if (!out.has(mid)) out.set(mid, []);
      out.get(mid).push(r);
    }
  }
  return out;
}

/**
 * Paginated listRecords — same pattern as api/brand-library.js getBrandLibraryBrands.
 */
async function fetchAllRecordsFromAirtable(tableName) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) {
    const err = new Error("Airtable not configured");
    err.statusCode = 503;
    throw err;
  }

  const tableSegment = encodeURIComponent(tableName);
  const allRecords = [];
  let offset = null;

  do {
    let url = `https://api.airtable.com/v0/${baseId}/${tableSegment}?pageSize=100`;
    if (offset) url += "&offset=" + encodeURIComponent(offset);

    const pageRes = await fetch(url, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const pageData = await pageRes.json().catch(() => ({}));

    if (!pageRes.ok || pageData.error) {
      const msg =
        (pageData.error && pageData.error.message) ||
        pageRes.statusText ||
        "Airtable API error";
      const err = new Error(msg);
      err.statusCode = pageRes.status;
      err.error = pageData.error && pageData.error.type;
      throw err;
    }

    allRecords.push(...(pageData.records || []));
    offset = pageData.offset || null;
  } while (offset);

  return allRecords;
}

const OPERATOR_STATUS_FALLBACK = [
  "Active",
  "Inactive",
  "Archived",
  "Draft",
  "Expired",
  "Paused",
  "Under Review",
];

/** Status dropdown choices from Operator Setup — Master `submission_status`. */
async function getMasterSubmissionStatusChoiceNames(baseId, apiKey) {
  if (!baseId || !apiKey) return OPERATOR_STATUS_FALLBACK;
  try {
    const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    if (!res.ok) return OPERATOR_STATUS_FALLBACK;

    const data = await res.json();
    const table = (data.tables || []).find((t) => t.name === NEW_BASE_MASTER_TABLE);
    if (!table) return OPERATOR_STATUS_FALLBACK;

    const field = (table.fields || []).find(
      (f) =>
        (f.name === "submission_status" || /submission\s*status/i.test(String(f.name || ""))) &&
        (f.type === "singleSelect" || f.type === "multipleSelects")
    );

    const choices = field?.options?.choices?.map((c) => c.name) || [];
    return choices.length > 0 ? choices : OPERATOR_STATUS_FALLBACK;
  } catch {
    return OPERATOR_STATUS_FALLBACK;
  }
}

export default async function listThirdPartyOperators(req, res) {
  try {
    const [
      brandBasicsRecords,
      masterRecords,
      profileRows,
      platformRows,
      newBaseCaseRows,
      newBaseDiligenceRows,
    ] = await Promise.all([
      fetchAllRecordsFromAirtable(BRAND_BASICS_TABLE).catch(() => []),
      fetchAllRecordsRest(NEW_BASE_MASTER_TABLE).catch(() => []),
      fetchAllRecordsRest(NEW_BASE_PROFILE_TABLE).catch(() => []),
      fetchAllRecordsRest(NEW_BASE_PLATFORM_TABLE).catch(() => []),
      fetchAllRecordsRest(NEW_BASE_CASE_STUDIES_TABLE).catch(() => []),
      fetchAllRecordsRest(NEW_BASE_DILIGENCE_TABLE).catch(() => []),
    ]);

    const brandNameById = new Map();
    for (const brec of brandBasicsRecords) {
      const bf = brec.fields || {};
      const nm = formatListValue(bf["Brand Name"]);
      if (brec.id && nm) brandNameById.set(brec.id, nm);
    }

    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    const operatorStatuses = await getMasterSubmissionStatusChoiceNames(baseId, apiKey).catch(
      () => OPERATOR_STATUS_FALLBACK
    );

    const profileByMaster = mapFirstLinkedByMaster(profileRows);
    const platformByMaster = mapFirstLinkedByMaster(platformRows);
    const newBaseCaseByMaster = groupChildrenByMaster(newBaseCaseRows);
    const newBaseDiligenceByMaster = groupChildrenByMaster(newBaseDiligenceRows);

    const rows = (masterRecords || []).map((master) =>
      buildNewBaseListRow({
        master,
        profile: profileByMaster.get(master.id) || null,
        platform: platformByMaster.get(master.id) || null,
        caseStudyRows: newBaseCaseByMaster.get(master.id) || [],
        diligenceRows: newBaseDiligenceByMaster.get(master.id) || [],
        brandNameById,
      })
    );

    /** When `activeOnly=1` (or `true`), return only Master `submission_status` === Active — used by Operator Explorer. */
    const isActiveDealStatus = (dealStatus) =>
      String(dealStatus || "")
        .trim()
        .toLowerCase() === "active";

    const activeOnly =
      req.query &&
      (req.query.activeOnly === "1" ||
        String(req.query.activeOnly || "").toLowerCase() === "true" ||
        req.query.explorer === "1");

    let operators = [...rows];
    if (activeOnly) {
      operators = operators.filter((o) => isActiveDealStatus(o.dealStatus));
    }
    operators.sort((a, b) =>
      (a.companyName || "").localeCompare(b.companyName || "", undefined, { sensitivity: "base" })
    );

    const allDealStatuses = new Set(operatorStatuses);
    const serviceModelSet = new Set();
    for (const o of operators) {
      if (o.dealStatus && o.dealStatus !== "—") allDealStatuses.add(o.dealStatus);
      const sm = String(o.primaryServiceModel || "").trim();
      if (sm) serviceModelSet.add(sm);
    }
    const dealStatuses = Array.from(allDealStatuses).sort((a, b) => String(a).localeCompare(String(b)));
    const serviceModels = Array.from(serviceModelSet).sort((a, b) =>
      a.localeCompare(b, undefined, { sensitivity: "base" })
    );

    logOperatorReadPath("third_party_operators_list", {
      read_path: "operator_setup",
      record_count: operators.length,
    });

    return res.json({
      success: true,
      operators,
      totalCount: operators.length,
      filterOptions: { dealStatuses, serviceModels },
    });
  } catch (err) {
    const status = err && typeof err.statusCode === "number" ? err.statusCode : undefined;
    const msg = (err && err.message) || "Failed to load operators";
    const errType = err && err.error;
    console.error("[third-party-operators-list]", status, errType || "", msg);

    if (status === 503 || msg === "Airtable not configured") {
      return res.status(503).json({
        success: false,
        error: "Airtable not configured",
        operators: [],
      });
    }

    const isNotAuthorized =
      status === 403 ||
      errType === "NOT_AUTHORIZED" ||
      /not authorized to perform/i.test(msg);

    if (isNotAuthorized) {
      return res.status(403).json({
        success: false,
        error: msg,
        hint: AIRTABLE_READ_HINT,
        operators: [],
      });
    }

    return res.status(status && status >= 400 && status < 600 ? status : 500).json({
      success: false,
      error: msg,
      operators: [],
    });
  }
}
