/**
 * Minimal Brand Library API for My Brands page only.
 * Use with server-my-brands.js to reduce memory usage.
 */
import { BRAND_STATUS_ACTIVE_FORMULA, isBrandStatusActive } from "../lib/brand-status-active.js";

const TABLE = "Brand Setup - Brand Basics";
const F = {
  name: "Brand Name",
  parentCompany: "Parent Company",
  chainScale: "Hotel Chain Scale",
  brandModel: "Brand Model",
  serviceModel: "Hotel Service Model",
  status: "Brand Status",
  positioning: "Brand Positioning",
  tagline: "Brand Tagline",
  architecture: "Brand Architecture",
};

function valueToStr(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "object" && v !== null && typeof v.name === "string") return v.name.trim();
  if (Array.isArray(v) && v.length > 0) {
    const first = v[0];
    return typeof first === "string" ? first.trim() : (first?.name ? String(first.name).trim() : "");
  }
  return "";
}

function extractLogoUrl(fields) {
  if (!fields || typeof fields !== "object") return "";
  const logo = fields["Logo"] || fields["logo"];
  if (Array.isArray(logo) && logo[0]?.url) return logo[0].url;
  if (typeof logo === "string" && logo.startsWith("http")) return logo;
  return "";
}

export async function getBrandLibraryBrands(req, res) {
  try {
    const allStatuses = req.query?.allStatuses === "1" || req.query?.allStatuses === "true";
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) throw new Error("Airtable API credentials not configured");

    const tableName = encodeURIComponent(TABLE);
    const useFilter = !allStatuses;
    const formula = encodeURIComponent(BRAND_STATUS_ACTIVE_FORMULA);

    let allRecords = [];
    let offset = null;
    do {
      let url = `https://api.airtable.com/v0/${baseId}/${tableName}?pageSize=100`;
      if (useFilter) url += `&filterByFormula=${formula}`;
      if (offset) url += "&offset=" + encodeURIComponent(offset);
      const pageRes = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
      const pageData = await pageRes.json();
      if (pageData.error) throw new Error(pageData.error.message || "Airtable API error");
      allRecords = allRecords.concat(pageData.records || []);
      offset = pageData.offset || null;
    } while (offset);

    const allFieldNames = new Set();
    allRecords.forEach((rec) => Object.keys(rec.fields || {}).forEach((k) => allFieldNames.add(k)));
    const architectureFieldKey = [...allFieldNames].find((k) => k.toLowerCase().includes("architecture")) || null;

    const brandList = allRecords.map((rec) => {
      const fields = rec.fields || {};
      const archVal = architectureFieldKey ? valueToStr(fields[architectureFieldKey]) : valueToStr(fields[F.architecture]);
      return {
        id: rec.id,
        name: (fields[F.name] || "").toString().trim() || "Unknown Brand",
        logo: extractLogoUrl(fields),
        parentCompany: (fields[F.parentCompany] || "").toString().trim(),
        chainScale: valueToStr(fields[F.chainScale]),
        brandModel: valueToStr(fields[F.brandModel]),
        serviceModel: valueToStr(fields[F.serviceModel]),
        architecture: archVal,
        status: valueToStr(fields[F.status]),
        positioning: (fields[F.positioning] || "").toString().trim(),
        tagline: (fields[F.tagline] || "").toString().trim(),
      };
    });

    const visibleBrands = allStatuses
      ? brandList
      : brandList.filter((b) => isBrandStatusActive(b.status));

    const parentCompanies = [...new Set(visibleBrands.map((b) => (b.parentCompany || "").trim()).filter(Boolean))].sort();
    const chainScales = [...new Set(visibleBrands.map((b) => (b.chainScale || "").trim()).filter(Boolean))].sort();
    const brandModels = [...new Set(visibleBrands.map((b) => (b.brandModel || "").trim()).filter(Boolean))].sort();
    const serviceModels = [...new Set(visibleBrands.map((b) => (b.serviceModel || "").trim()).filter(Boolean))].sort();
    const architectures = [...new Set(visibleBrands.map((b) => (b.architecture || "").trim()).filter(Boolean))].sort();

    res.json({
      success: true,
      brands: visibleBrands,
      totalCount: visibleBrands.length,
      filterOptions: { parentCompanies, chainScales, brandModels, serviceModels, architectures },
    });
  } catch (error) {
    console.error("Error fetching brands:", error);
    res.status(500).json({ error: "Internal Server Error", details: error.message });
  }
}

export async function getBrandStatusOptions(req, res) {
  try {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) throw new Error("Airtable API credentials not configured");

    const schemaRes = await fetch(
      `https://api.airtable.com/v0/meta/bases/${baseId}/tables`,
      { headers: { Authorization: `Bearer ${apiKey}` } }
    );
    if (!schemaRes.ok) throw new Error("Schema fetch failed");
    const schemaData = await schemaRes.json();
    const table = (schemaData.tables || []).find((t) => t.name === TABLE);
    if (!table) throw new Error("Brand Basics table not found");
    const statusField = (table.fields || []).find((f) => f.name === F.status && f.type === "singleSelect");
    const options = statusField?.options?.choices?.map((c) => c.name) || [];
    res.json({ success: true, options });
  } catch (error) {
    console.error("Error fetching status options:", error);
    res.status(500).json({ success: false, error: error.message, options: [] });
  }
}

export async function updateBrandStatusById(req, res) {
  try {
    const recordId = req.params.recordId;
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid record ID is required" });
    }
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const status = body.brandStatus;
    if (!status || typeof status !== "string") {
      return res.status(400).json({ success: false, error: "brandStatus is required" });
    }
    const Airtable = (await import("airtable")).default;
    const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
    await base(TABLE).update(recordId, { [F.status]: status.trim() });
    res.json({ success: true });
  } catch (error) {
    console.error("Error updating brand status:", error);
    res.status(500).json({ success: false, error: error.message });
  }
}
