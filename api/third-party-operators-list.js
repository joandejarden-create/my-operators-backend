/**
 * Lists rows from the Third Party Operators table in the same base as Brand Setup
 * (`AIRTABLE_BASE_ID` — identical to api/brand-library.js and third-party-operator-intake.js).
 *
 * Uses the Airtable REST API (same approach as getBrandLibraryBrands) so HTTP status
 * codes (403, 404, etc.) are reliable; the JS SDK eachPage path did not always preserve statusCode.
 */

import { parseMultiValue } from "./lib/build-third-party-operator-prefill.js";

const TABLE_NAME = process.env.AIRTABLE_THIRD_PARTY_OPERATORS_TABLE || "3rd Party Operator - Basics";
const CASE_STUDIES_TABLE =
  process.env.AIRTABLE_THIRD_PARTY_OPERATOR_CASE_STUDIES_TABLE || "3rd Party Operator - Case Studies";
const OWNER_DILIGENCE_QA_TABLE =
  process.env.AIRTABLE_THIRD_PARTY_OPERATOR_OWNER_DILIGENCE_QA_TABLE || "3rd Party Operator - Owner Diligence QA";
const FOOTPRINT_TABLE =
  process.env.AIRTABLE_THIRD_PARTY_OPERATOR_FOOTPRINT_TABLE || "3rd Party Operator - Footprint";
const BRAND_BASICS_TABLE =
  process.env.AIRTABLE_BRAND_SETUP_BASICS_TABLE || "Brand Setup - Brand Basics";
const OPERATOR_BASICS_LINK_FIELD = "Operator (Basics Link)";

const AIRTABLE_READ_HINT =
  "Airtable returned 403: this token cannot list records on that table. Operators use the same base as Brand Setup (your AIRTABLE_BASE_ID). For a Personal Access Token, enable **data.records:read** for that base—submitting the intake only needs write access; the My 3rd Party Ops. page also needs read access to the Third Party Operators table.";

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

function isLikelyAirtableRecordId(s) {
  return typeof s === "string" && /^rec[a-zA-Z0-9]{14,}$/.test(s.trim());
}

/** Combine Basics + linked Footprint multi-values into a single display string. */
function mergeMultiFieldDisplay(basicsVal, footprintVal) {
  const a = parseMultiValue(basicsVal);
  const b = parseMultiValue(footprintVal);
  const seen = new Set();
  const out = [];
  for (const x of [...a, ...b]) {
    const k = String(x).trim();
    if (!k || seen.has(k)) continue;
    seen.add(k);
    out.push(k);
  }
  return out.join(", ");
}

function firstLinkedRowForOperator(rows, operatorRecordId) {
  return (
    rows.find((r) => {
      const links = (r.fields && r.fields[OPERATOR_BASICS_LINK_FIELD]) || [];
      return Array.isArray(links) && links.includes(operatorRecordId);
    }) || null
  );
}

/**
 * Brands Managed may be multiple select (labels) or linked Brand Basics record IDs.
 */
function formatBrandsManagedDisplay(raw, brandNameById) {
  if (raw == null || raw === "") return "";
  if (Array.isArray(raw)) {
    const parts = [];
    const seen = new Set();
    for (const item of raw) {
      let s = "";
      if (typeof item === "string") s = item.trim();
      else if (item && typeof item === "object" && typeof item.name === "string") s = item.name.trim();
      if (!s) continue;
      const label = isLikelyAirtableRecordId(s) ? brandNameById.get(s) || s : s;
      if (label && !seen.has(label)) {
        seen.add(label);
        parts.push(label);
      }
    }
    return parts.join(", ");
  }
  if (typeof raw === "string") return raw.trim();
  return formatListValue(raw);
}

function attachmentUrl(fields) {
  const att = fields["Company Logo"];
  if (!Array.isArray(att) || att.length === 0) return "";
  const first = att[0];
  if (first && typeof first.url === "string") return first.url;
  return "";
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

function safeParseJsonArray(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value;
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  return [];
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

async function getOperatorStatusChoiceNames(baseId, apiKey) {
  if (!baseId || !apiKey) return OPERATOR_STATUS_FALLBACK;
  try {
    const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    if (!res.ok) return OPERATOR_STATUS_FALLBACK;

    const data = await res.json();
    const table = (data.tables || []).find((t) => t.name === TABLE_NAME);
    if (!table) return OPERATOR_STATUS_FALLBACK;

    const fieldNameCandidates = ["Operator Status", "Deal Status", "Status"];
    const field = (table.fields || []).find((f) => {
      const nameMatches =
        fieldNameCandidates.includes(f.name) || fieldNameCandidates.includes(String(f.name || ""));
      const typeOk = f.type === "singleSelect" || f.type === "multipleSelects";
      return nameMatches && typeOk;
    });

    const choices = field?.options?.choices?.map((c) => c.name) || [];
    return choices.length > 0 ? choices : OPERATOR_STATUS_FALLBACK;
  } catch {
    return OPERATOR_STATUS_FALLBACK;
  }
}

/**
 * List operator intake records for the My 3rd Party Ops. dashboard.
 * Mounted at: GET /api/intake/third-party-operators, /api/third-party-operators/list, /api/third-party-operators
 */
export default async function listThirdPartyOperators(req, res) {
  try {
    const [records, caseStudyRecords, ownerQaRecords, footprintRecords, brandBasicsRecords] =
      await Promise.all([
        fetchAllRecordsFromAirtable(TABLE_NAME),
        fetchAllRecordsFromAirtable(CASE_STUDIES_TABLE).catch(() => []),
        fetchAllRecordsFromAirtable(OWNER_DILIGENCE_QA_TABLE).catch(() => []),
        fetchAllRecordsFromAirtable(FOOTPRINT_TABLE).catch(() => []),
        fetchAllRecordsFromAirtable(BRAND_BASICS_TABLE).catch(() => []),
      ]);

    const brandNameById = new Map();
    for (const brec of brandBasicsRecords) {
      const bf = brec.fields || {};
      const nm = formatListValue(bf["Brand Name"]);
      if (brec.id && nm) brandNameById.set(brec.id, nm);
    }

    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    const operatorStatuses = await getOperatorStatusChoiceNames(baseId, apiKey).catch(
      () => OPERATOR_STATUS_FALLBACK
    );

    const caseStudiesByOperator = new Map();
    for (const rec of caseStudyRecords) {
      const f = rec.fields || {};
      const operatorId = formatListValue(f["Operator Record ID"]);
      if (!operatorId) continue;
      const row = {
        hotel_type: formatListValue(f["Hotel Type"]),
        region: formatListValue(f["Region"]),
        branded_independent: formatListValue(f["Branded / Independent"]),
        situation: formatListValue(f["Situation"]),
        services: formatListValue(f["Services"]),
        outcome: formatListValue(f["Outcome"]),
        owner_relevance: formatListValue(f["Owner Relevance"]),
      };
      if (!caseStudiesByOperator.has(operatorId)) caseStudiesByOperator.set(operatorId, []);
      caseStudiesByOperator.get(operatorId).push(row);
    }

    const ownerQaByOperator = new Map();
    for (const rec of ownerQaRecords) {
      const f = rec.fields || {};
      const operatorId = formatListValue(f["Operator Record ID"]);
      if (!operatorId) continue;
      const row = {
        category: formatListValue(f["Category"]),
        question: formatListValue(f["Question"]),
        answer: formatListValue(f["Answer"]),
      };
      if (!ownerQaByOperator.has(operatorId)) ownerQaByOperator.set(operatorId, []);
      ownerQaByOperator.get(operatorId).push(row);
    }

    const operators = records.map((rec) => {
      const f = rec.fields || {};
      const footprintRow = firstLinkedRowForOperator(footprintRecords, rec.id);
      const ff = (footprintRow && footprintRow.fields) || {};
      const caseStudies =
        caseStudiesByOperator.get(rec.id) || safeParseJsonArray(f["Case Studies Detail"]);
      const ownerDiligenceQa =
        ownerQaByOperator.get(rec.id) || safeParseJsonArray(f["Owner Diligence Q&A"]);

      const brandsManaged = formatBrandsManagedDisplay(f["Brands Managed"], brandNameById);
      const regionsSupported = mergeMultiFieldDisplay(
        f["Regions Supported"] || f["Regions"],
        ff["Regions Supported"] || ff["Regions"]
      );
      const chainScale = mergeMultiFieldDisplay(
        f["Chain Scales You Support"] || f["Chain Scale"],
        ff["Chain Scale"]
      );

      const rawBrandsField = f["Brands Managed"];
      const brandCountFallback = Array.isArray(rawBrandsField)
        ? rawBrandsField.length
        : parseMultiValue(rawBrandsField).length;
      const numberFromField =
        f["Number of Brands Supported"] != null && String(f["Number of Brands Supported"]).trim() !== ""
          ? formatListValue(f["Number of Brands Supported"])
          : "";

      return {
        id: rec.id,
        companyName: formatListValue(f["Company Name"]) || "—",
        logo: attachmentUrl(f),
        website: formatListValue(f["Website"]),
        headquarters: formatListValue(f["Headquarters"] || f["Headquarters Location"]),
        contactEmail: formatListValue(f["Contact Email"]),
        contactPhone: formatListValue(f["Contact Phone"]),
        yearEstablished: f["Year Established"] != null ? formatListValue(f["Year Established"]) : "",
        yearsInBusiness: f["Years in Business"] != null ? formatListValue(f["Years in Business"]) : "",
        companyDescription: formatListValue(f["Company Description"]),
        primaryServiceModel: formatListValue(f["Primary Service Model"]),
        numberOfBrands: numberFromField || (brandCountFallback > 0 ? String(brandCountFallback) : ""),
        brandsManaged,
        regionsSupported,
        totalProperties:
          f["Total Properties Managed"] != null ? formatListValue(f["Total Properties Managed"]) : "",
        totalRooms: f["Total Rooms Managed"] != null ? formatListValue(f["Total Rooms Managed"]) : "",
        chainScale,
        dealStatus:
          formatListValue(f["Operator Status"]) ||
          formatListValue(f["Deal Status"]) ||
          formatListValue(f["Status"]),
        submittedAt: formatListValue(f["Submitted At"]),
        caseStudiesCount: Array.isArray(caseStudies) ? caseStudies.length : 0,
        ownerDiligenceQaCount: Array.isArray(ownerDiligenceQa) ? ownerDiligenceQa.length : 0,
        caseStudiesDetail: caseStudies,
        ownerDiligenceQa,
      };
    });

    operators.sort((a, b) =>
      (a.companyName || "").localeCompare(b.companyName || "", undefined, { sensitivity: "base" })
    );

    // Ensure the dropdown always contains observed values, even when Meta API access is restricted.
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
