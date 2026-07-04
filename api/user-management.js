/**
 * Company user admin API — CRUD on the Airtable **Users** table (platform users).
 * Used by Company Settings > User Management page.
 * Legacy User Management table (tblQEpYKf2aYNKKjw) is migrated via scripts/migrate-user-management-to-users.mjs.
 */

import Airtable from "airtable";
import {
  PLATFORM_USERS_TABLE_ID,
  PLATFORM_USERS_COMPANY_TABLE_ID,
  PUF,
  REGION_CODE_TO_CHECKBOX_FIELDS,
  companyFilterFormula,
  companyProfileIdFromFields,
  emailFromUserFields,
  profilePhotoUrlFromFields,
} from "../lib/airtable/platform-users-table.js";

const CONFIG = {
  AIRTABLE_API_KEY: process.env.AIRTABLE_API_KEY || "",
  AIRTABLE_BASE_ID: process.env.AIRTABLE_BASE_ID || "",
  PLATFORM_USERS_TABLE_ID,
  PLATFORM_USERS_COMPANY_TABLE_ID,
  MAX_RECORDS: 50000,
};

const PLATFORM_USERS_TABLE = CONFIG.PLATFORM_USERS_TABLE_ID;

function getBase() {
  if (!CONFIG.AIRTABLE_API_KEY || !CONFIG.AIRTABLE_BASE_ID) {
    return null;
  }
  return new Airtable({ apiKey: CONFIG.AIRTABLE_API_KEY }).base(CONFIG.AIRTABLE_BASE_ID);
}

const F = PUF;

const REGION_FIELD_NAMES = [
  "Region - America",
  "Region - Caribbean & Latin America",
  "Region - Europe",
  "Region - Middle East & Africa",
  "Region - Asia Pacific",
];

// Map Airtable region values to Partner Directory codes (same as front-end)
function toRegionCode(r) {
  const u = (typeof r === "string" ? r : String(r || "")).trim().replace(/\s+/g, " ").toUpperCase();
  if (!u) return null;
  if (u.indexOf("GLOBAL") >= 0) return "GLOBAL";
  if (u.indexOf("CARIBBEAN") >= 0 || u.indexOf("LATIN") >= 0 || u === "CALA") return "CALA";
  if (u.indexOf("EUROPE") >= 0 || u === "EU") return "EUROPE";
  if ((u.indexOf("MIDDLE") >= 0 && u.indexOf("EAST") >= 0) || u.indexOf("MEA") >= 0 || u.indexOf("AFRICA") >= 0) return "MEA";
  if ((u.indexOf("ASIA") >= 0 && u.indexOf("PACIFIC") >= 0) || u === "AP") return "AP";
  if (u.indexOf("AMERICAS") >= 0 || (u.indexOf("AMERICA") >= 0 && u.indexOf("LATIN") < 0 && u.indexOf("CARIBBEAN") < 0)) return "AMERICAS";
  return null;
}

const ALL_FIVE_REGIONS = new Set(["AMERICAS", "CALA", "EUROPE", "MEA", "AP"]);

function normalizeRegionFocus(rawList) {
  const codes = new Set();
  const arr = Array.isArray(rawList) ? rawList : (typeof rawList === "string" ? rawList.split(",").map((s) => s.trim()) : []);
  for (const item of arr) {
    const code = toRegionCode(item);
    if (code) codes.add(code);
  }
  if (codes.has("GLOBAL")) return ["GLOBAL"];
  if (ALL_FIVE_REGIONS.size === codes.size && [...ALL_FIVE_REGIONS].every((r) => codes.has(r))) return ["GLOBAL"];
  return [...codes];
}

function optionalNumberFromBody(value) {
  if (value === undefined || value === null || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function formatRecord(record) {
  const fields = record.fields || {};
  const companyProfileId = companyProfileIdFromFields(fields);
  const companyEmail = emailFromUserFields(fields);
  const platformRole = fields[F.platformRole] || "";

  let regionFocusRaw = [];
  for (const name of REGION_FIELD_NAMES) {
    if (fields[name] === true) regionFocusRaw.push(name);
    else if (Array.isArray(fields[name])) regionFocusRaw.push(...fields[name]);
  }
  const regionFocus = normalizeRegionFocus(regionFocusRaw);

  return {
    id: record.id,
    firstName: fields[F.firstName] || "",
    lastName: fields[F.lastName] || "",
    companyTitle: fields[F.companyTitle] || "",
    phoneNumber: fields[F.phoneNumber] || "",
    companyEmail: companyEmail,
    companyProfileId: companyProfileId || null,
    platformRole: platformRole,
    profilePhotoUrl: profilePhotoUrlFromFields(fields),
    regionFocus: regionFocus,
    contactVisibility: (() => {
      const cv = fields[F.contactVisibility];
      if (typeof cv === "object" && cv && cv.name) return cv.name;
      return (typeof cv === "string" ? cv : "") || "";
    })(),
    dealAccess: (() => {
      const v = fields[F.dealAccess];
      if (typeof v === "object" && v && v.name) return v.name;
      return (typeof v === "string" ? v : "") || "";
    })(),
    documentAccess: (() => {
      const v = fields[F.documentAccess];
      if (typeof v === "object" && v && v.name) return v.name;
      return (typeof v === "string" ? v : "") || "";
    })(),
    country: fields[F.country] || "",
    closedDeals: optionalNumberFromBody(fields[F.closedDeals]) ?? 0,
    uniqueBrandsDeals: optionalNumberFromBody(fields[F.uniqueBrandsDeals]) ?? 0,
    submittedBids: optionalNumberFromBody(fields[F.submittedBids]) ?? 0,
  };
}

function buildFieldsFromBody(body) {
  const fields = {};
  if (body.firstName != null) fields[F.firstName] = String(body.firstName).trim();
  if (body.lastName != null) fields[F.lastName] = String(body.lastName).trim();
  if (body.companyTitle != null) fields[F.companyTitle] = String(body.companyTitle).trim();
  if (body.phoneNumber != null) fields[F.phoneNumber] = String(body.phoneNumber).trim();
  if (body.companyEmail != null) {
    const em = String(body.companyEmail).trim();
    fields[F.email] = em;
    fields[F.companyEmail] = em;
  }
  if (body.companyProfileId != null) {
    const link = body.companyProfileId === "" ? [] : [body.companyProfileId];
    fields[F.companyProfile] = link;
  }
  if (body.platformRole != null) fields[F.platformRole] = String(body.platformRole).trim();
  if (body.contactVisibility != null) {
    fields[F.contactVisibility] = String(body.contactVisibility).trim();
  }
  if (body.dealAccess != null && String(body.dealAccess).trim() !== "") {
    fields[F.dealAccess] = String(body.dealAccess).trim();
  }
  if (body.documentAccess != null && String(body.documentAccess).trim() !== "") {
    fields[F.documentAccess] = String(body.documentAccess).trim();
  }
  if (body.country != null) fields[F.country] = String(body.country).trim();

  const closedDealsNum = optionalNumberFromBody(body.closedDeals);
  if (closedDealsNum !== null) fields[F.closedDeals] = closedDealsNum;
  const uniqueBrandsDealsNum = optionalNumberFromBody(body.uniqueBrandsDeals);
  if (uniqueBrandsDealsNum !== null) fields[F.uniqueBrandsDeals] = uniqueBrandsDealsNum;
  const submittedBidsNum = optionalNumberFromBody(body.submittedBids);
  if (submittedBidsNum !== null) fields[F.submittedBids] = submittedBidsNum;

  // Map Region Focus to Airtable checkbox columns (same as edit form checkmarks in Airtable)
  if (body.regionFocus != null) {
    const codes = new Set(
      Array.isArray(body.regionFocus)
        ? body.regionFocus.filter(Boolean).map((s) => String(s).trim().toUpperCase())
        : typeof body.regionFocus === "string"
          ? body.regionFocus.split(",").map((s) => s.trim().toUpperCase()).filter(Boolean)
          : []
    );
    const isGlobal = codes.has("GLOBAL") || (codes.size >= 5 && ["AMERICAS", "CALA", "EUROPE", "MEA", "AP"].every((r) => codes.has(r)));
    for (const [code, columnNames] of Object.entries(REGION_CODE_TO_CHECKBOX_FIELDS)) {
      const checked = isGlobal || codes.has(code);
      for (const col of columnNames) {
        fields[col] = checked;
      }
    }
  }

  return fields;
}

/** GET /api/user-management – list all User Management records (optionally filter by companyProfileId) */
export async function listUsers(req, res) {
  try {
    const base = getBase();
    if (!base) {
      return res.status(500).json({ error: "Airtable not configured" });
    }
    const companyProfileId = (req.query.companyProfileId || req.query.company || "").trim().replace(/'/g, "\\'");
    const limitRaw = parseInt(String(req.query.limit || ""), 10);
    const maxRecords =
      Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 200) : undefined;
    const table = base(PLATFORM_USERS_TABLE);
    const records = [];

    await new Promise((resolve, reject) => {
      const options = { pageSize: 100 };
      if (maxRecords) options.maxRecords = maxRecords;
      if (companyProfileId) {
        options.filterByFormula = companyFilterFormula(companyProfileId);
      }
      table
        .select(options)
        .eachPage(
          (pageRecords, fetchNextPage) => {
            records.push(...pageRecords);
            fetchNextPage();
          },
          (err) => (err ? reject(err) : resolve())
        );
    });

    res.json({ users: records.map(formatRecord) });
  } catch (error) {
    console.error("User Management list error:", error);
    res.status(500).json({ error: "Failed to list users", details: error.message });
  }
}

/** POST /api/user-management – create a User Management record */
export async function createUser(req, res) {
  try {
    const base = getBase();
    if (!base) {
      return res.status(500).json({ error: "Airtable not configured" });
    }
    const body = req.body || {};
    if (!body.firstName || !body.lastName || !body.companyEmail) {
      return res.status(400).json({ error: "Missing required fields: firstName, lastName, companyEmail" });
    }
    if (!body.companyProfileId || String(body.companyProfileId).trim() === "") {
      return res.status(400).json({ error: "Company is required" });
    }
    if (!body.country || String(body.country).trim() === "") {
      return res.status(400).json({ error: "Based (Country) is required" });
    }

    const fields = buildFieldsFromBody(body);
    if (!fields[F.platformRole]) fields[F.platformRole] = "Company Admin";
    if (!fields[F.contactVisibility]) fields[F.contactVisibility] = "Show Contact";

    const record = await base(PLATFORM_USERS_TABLE).create(fields, { typecast: true });
    res.status(201).json({ user: formatRecord(record), message: "User created successfully" });
  } catch (error) {
    console.error("User Management create error:", error);
    res.status(500).json({ error: "Failed to create user", details: error.message });
  }
}

/** PATCH /api/user-management/:recordId – update a User Management record */
export async function updateUser(req, res) {
  try {
    const base = getBase();
    if (!base) {
      return res.status(500).json({ error: "Airtable not configured" });
    }
    const recordId = req.params.recordId;
    if (!recordId) return res.status(400).json({ error: "Record ID is required" });

    const body = req.body || {};
    const rawFields = buildFieldsFromBody(body);
    // Remove undefined/null so Airtable doesn't reject the payload
    let fields = Object.fromEntries(
      Object.entries(rawFields).filter(([, v]) => v !== undefined && v !== null)
    );
    if (Object.keys(fields).length === 0) {
      return res.status(400).json({ error: "No fields to update" });
    }
    const regionCheckboxNames = new Set(
      Object.values(REGION_CODE_TO_CHECKBOX_FIELDS).flat()
    );

    // Same pattern as brand-library PATCH (Status column): update with no options — no typecast.
    let record;
    try {
      record = await base(PLATFORM_USERS_TABLE).update(recordId, fields);
    } catch (firstError) {
      // If update fails with a field error and we sent region checkboxes, retry without them.
      const isFieldError =
        /INVALID|Unknown field|could not be parsed|invalid value/i.test(
          firstError.error?.message || firstError.message || ""
        );
      if (isFieldError && Object.keys(fields).some((k) => regionCheckboxNames.has(k))) {
        const fieldsWithoutRegion = Object.fromEntries(
          Object.entries(fields).filter(([k]) => !regionCheckboxNames.has(k))
        );
        if (Object.keys(fieldsWithoutRegion).length > 0) {
          try {
            record = await base(PLATFORM_USERS_TABLE).update(recordId, fieldsWithoutRegion);
            return res.json({ user: formatRecord(record), message: "User updated successfully" });
          } catch (retryErr) {
            // Fall through to return first error
          }
        }
      }
      const msg =
        firstError.error?.message ||
        firstError.message ||
        (firstError.error && typeof firstError.error === "object" ? JSON.stringify(firstError.error) : null) ||
        String(firstError);
      const details = [msg, firstError.statusCode ? `Status: ${firstError.statusCode}` : null].filter(Boolean).join(" — ");
      console.error("User Management update error:", details);
      const statusCode = firstError.statusCode;
      const httpStatus = typeof statusCode === "number" && statusCode >= 400 && statusCode < 600 ? statusCode : 500;
      return res.status(httpStatus).json({
        error: "Failed to update user",
        details: details || msg,
      });
    }
    return res.json({ user: formatRecord(record), message: "User updated successfully" });
  } catch (error) {
    const msg =
      error.error?.message ||
      error.message ||
      (error.error && typeof error.error === "object" ? JSON.stringify(error.error) : null) ||
      String(error);
    const details = [msg, error.statusCode ? `Status: ${error.statusCode}` : null].filter(Boolean).join(" — ");
    console.error("User Management update error:", details);
    const statusCode = error.statusCode;
    const httpStatus = typeof statusCode === "number" && statusCode >= 400 && statusCode < 600 ? statusCode : 500;
    return res.status(httpStatus).json({
      error: "Failed to update user",
      details: details || msg,
    });
  }
}

/** DELETE /api/user-management/:recordId – delete a User Management record */
export async function deleteUser(req, res) {
  try {
    const base = getBase();
    if (!base) {
      return res.status(500).json({ error: "Airtable not configured" });
    }
    const recordId = req.params.recordId;
    if (!recordId) return res.status(400).json({ error: "Record ID is required" });

    await base(PLATFORM_USERS_TABLE).destroy(recordId);
    res.json({ id: recordId, message: "User removed successfully" });
  } catch (error) {
    const msg =
      error.error?.message ||
      error.message ||
      (error.error && typeof error.error === "object" ? JSON.stringify(error.error) : null) ||
      String(error);
    const details = [msg, error.statusCode ? `Status: ${error.statusCode}` : null].filter(Boolean).join(" — ");
    console.error("User Management delete error:", details);
    const statusCode = error.statusCode;
    const httpStatus = typeof statusCode === "number" && statusCode >= 400 && statusCode < 600 ? statusCode : 500;
    res.status(httpStatus).json({ error: "Failed to delete user", details: details || msg });
  }
}

/** POST /api/user-management/bulk-delete – delete multiple User Management records */
export async function bulkDeleteUsers(req, res) {
  try {
    const base = getBase();
    if (!base) {
      return res.status(500).json({ error: "Airtable not configured" });
    }
    const { recordIds } = req.body || {};
    if (!Array.isArray(recordIds) || recordIds.length === 0) {
      return res.status(400).json({ error: "recordIds array is required" });
    }
    const ids = recordIds.filter((id) => id && String(id).startsWith("rec"));
    if (ids.length === 0) return res.status(400).json({ error: "No valid record IDs" });

    await base(PLATFORM_USERS_TABLE).destroy(ids);
    res.json({ deleted: ids.length, message: "Users removed successfully" });
  } catch (error) {
    console.error("User Management bulk delete error:", error);
    res.status(500).json({ error: "Failed to delete users", details: error.message });
  }
}

/** GET /api/user-management/companies – list companies (Company Profile) for dropdown */
export async function listCompanies(req, res) {
  const base = getBase();
  if (!base) {
    return res.status(200).json({ companies: [] });
  }
  const COMPANY_PROFILE_TABLE_ID = CONFIG.PLATFORM_USERS_COMPANY_TABLE_ID;
  try {
    const records = [];
    await new Promise((resolve, reject) => {
      base(COMPANY_PROFILE_TABLE_ID)
        .select({ fields: ["Company Name"], pageSize: 100 })
        .eachPage(
          (pageRecords, fetchNextPage) => {
            records.push(...pageRecords);
            fetchNextPage();
          },
          (err) => (err ? reject(err) : resolve())
        );
    });
    const companies = records.map((r) => ({
      id: r.id,
      name: (r.fields && r.fields["Company Name"]) || "",
    })).filter((c) => c.name);
    return res.json({ companies });
  } catch (error) {
    console.error("User Management listCompanies error:", error.message || error);
    // Return empty list so the page still loads; Company dropdown will be empty
    return res.status(200).json({ companies: [] });
  }
}
