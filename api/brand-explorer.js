/**
 * Brand Explorer API
 * Uses Brand Setup tables as source of truth. New tables: Brand Explorer - Updates, Brand Explorer - Modules (optional).
 * Env: AIRTABLE_BRAND_EXPLORER_UPDATES_TABLE, AIRTABLE_BRAND_EXPLORER_MODULES_TABLE (optional)
 */

import Airtable from "airtable";

function getBase() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    throw new Error("Airtable API credentials not configured");
  }
  return new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
}

const TABLES = {
  brandBasics: "Brand Setup - Brand Basics",
  projectFit: "Brand Setup - Project Fit",
  feeStructure: "Brand Setup - Fee Structure",
  dealTerms: "Brand Setup - Deal Terms",
  operationalSupport: "Brand Setup - Operational Support",
  loyaltyCommercial: "Brand Setup - Loyalty & Commercial",
  brandFootprint: "Brand Setup - Brand Footprint",
  updates: process.env.AIRTABLE_BRAND_EXPLORER_UPDATES_TABLE || "Brand Explorer - Updates",
  modules: process.env.AIRTABLE_BRAND_EXPLORER_MODULES_TABLE || "Brand Explorer - Modules",
};

/** Derive brand_key from Brand Name: lowercase, non-alphanumeric → underscore */
function toBrandKey(name) {
  if (!name || typeof name !== "string") return "";
  return name.trim().toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_|_$/g, "") || name;
}

function valueToStr(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "object" && !Array.isArray(v) && v !== null) {
    if (typeof v.name === "string") return v.name.trim();
    if (typeof v.value === "string") return v.value.trim();
  }
  if (Array.isArray(v) && v.length > 0) {
    const parts = v.map((item) =>
      typeof item === "string" ? item.trim() : item && typeof item === "object" && item.name ? item.name.trim() : ""
    ).filter(Boolean);
    return parts.join(", ");
  }
  return String(v);
}

function getField(fields, keys) {
  if (fields == null || typeof fields !== "object") return undefined;
  const arr = Array.isArray(keys) ? keys : [keys];
  for (const k of arr) {
    const v = fields[k];
    if (v !== undefined && v !== null && v !== "") return v;
  }
  return undefined;
}

async function findLinkedRecordByBrand(base, tableName, brandRecordId, brandName) {
  const escapedName = (brandName || "").replace(/"/g, '\\"');
  const linkFieldNames = ["Brand", "Brand_Basic_ID", "Brand Setup - Brand Basics", "Brand Basics"];
  for (const linkField of linkFieldNames) {
    try {
      const formula = `FIND("${brandRecordId}", ARRAYJOIN({${linkField}})) > 0`;
      const records = await base(tableName).select({ filterByFormula: formula, maxRecords: 1 }).all();
      if (records.length > 0) return records[0];
    } catch (_) {}
  }
  if (escapedName) {
    try {
      const records = await base(tableName).select({ filterByFormula: `{Brand Name} = "${escapedName}"`, maxRecords: 1 }).all();
      if (records.length > 0) return records[0];
    } catch (_) {}
  }
  return null;
}

/** Resolve brand_key to Brand Basics record (by slug match on Brand Name) */
async function findBrandByKey(base, brandKey) {
  const brands = await base(TABLES.brandBasics)
    .select({
      filterByFormula: "OR(FIND('Active', {Brand Status}) > 0, FIND('Live', {Brand Status}) > 0)",
      maxRecords: 500,
    })
    .all();
  const key = (brandKey || "").toLowerCase().replace(/[^a-z0-9_]/g, "");
  for (const rec of brands) {
    const name = (rec.fields["Brand Name"] || "").trim();
    if (toBrandKey(name) === key) return rec;
    if (name.toLowerCase().replace(/[^a-z0-9]/g, "_") === key) return rec;
  }
  return null;
}

/** Fetch Updates for brand (status=Live only). Supports brand_key or Brand Key column. */
async function fetchUpdatesForBrand(base, brandKey) {
  try {
    const table = TABLES.updates;
    const escaped = (brandKey || "").replace(/"/g, '\\"');
    const records = await base(table)
      .select({
        filterByFormula: `AND({status} = "Live", OR({brand_key} = "${escaped}", {Brand Key} = "${escaped}"))`,
        sort: [{ field: "published_date", direction: "desc" }],
        maxRecords: 20,
      })
      .all();
    return records.map((r) => ({
      brand_key: r.fields.brand_key,
      title: r.fields.title || "",
      body: r.fields.body || "",
      type: r.fields.type || "Update",
      published_date: r.fields.published_date,
      last_updated: r.fields.last_updated || r.fields.published_date,
    }));
  } catch (e) {
    if (e?.message && /could not find|NOT_FOUND|invalid|unknown field/i.test(e.message)) return [];
    if (e?.error?.type === "NOT_FOUND" || e?.statusCode === 404) return [];
    console.warn("[Brand Explorer] fetchUpdatesForBrand:", e?.message || e);
    return [];
  }
}

/** Fetch Modules for brand; if status=Live use curated text, else null (derive from structured) */
async function fetchModulesForBrand(base, brandKey) {
  try {
    const table = TABLES.modules;
    const escaped = (brandKey || "").replace(/"/g, '\\"');
    const records = await base(table)
      .select({
        filterByFormula: `AND({status} = "Live", OR({brand_key} = "${escaped}", {Brand Key} = "${escaped}"))`,
        maxRecords: 1,
      })
      .all();
    if (records.length > 0 && records[0].fields) {
      const f = records[0].fields;
      return {
        dna_card: f.dna_card || f["DNA Card"] || "",
        owner_lens: f.owner_lens || f["Owner Lens"] || "",
        status: "Live",
        last_updated: f.last_updated || "",
      };
    }
  } catch (e) {
    if (e?.message && /could not find|NOT_FOUND|invalid|unknown field/i.test(e.message)) return null;
    if (e?.error?.type === "NOT_FOUND" || e?.statusCode === 404) return null;
    console.warn("[Brand Explorer] fetchModulesForBrand:", e?.message || e);
    return null;
  }
  return null;
}

/** Derive DNA card from structured fields */
function deriveDnaCard(brandBasics, projectFit, footprint) {
  const b = brandBasics?.fields || {};
  const pf = projectFit?.fields || {};
  const fp = footprint?.fields || {};

  const guestSegments = valueToStr(getField(b, ["Target Guest Segments", "Target Guest Segments (multi-select)"]));
  const psychographics = valueToStr(getField(b, ["Guest Psychographics Description", "Guest Psychographics"]));
  const preferredOwner = valueToStr(getField(pf, ["Preferred Owner/Investor Type", "Preferred Owner Type"]));
  const ownerExp = valueToStr(getField(pf, ["Owner / Sponsor Hotel Experience", "Owner Hotel Experience"]));
  const chainScale = valueToStr(getField(b, ["Hotel Chain Scale", "Chain Scale"]));
  const positioning = valueToStr(getField(b, ["Brand Positioning", "Brand Positioning Description"]));
  const model = valueToStr(getField(b, ["Brand Model", "Brand Model Format"]));
  const serviceModel = valueToStr(getField(b, ["Hotel Service Model", "Service Model"]));
  const tagline = valueToStr(getField(b, ["Brand Tagline", "Brand Tagline Motto"]));
  const pillars = valueToStr(getField(b, ["Brand Pillars", "Brand Pillars (multi-select)"]));
  const promise = valueToStr(getField(b, ["Brand Customer Promise", "Brand Customer Promise"]));
  const valueProp = valueToStr(getField(b, ["Brand Value Proposition", "Brand Value Proposition"]));
  const differentiators = valueToStr(getField(b, ["Key Brand Differentiators", "Key Differentiators"]));
  const ownerNonNeg = valueToStr(getField(pf, ["Owner Non-Negotiables & Decision Rights", "Owner Non-Negotiables"]));
  const redFlags = valueToStr(getField(pf, ["Red Flag Items That Typically Make You Decline or Proceed With Caution - Risk & Compliance", "Red Flags", "Known Red Flags"]));
  const priorityMarkets = valueToStr(getField(pf, ["Other - Priority Markets Text", "Priority Markets"]));
  const acceptableTypes = valueToStr(getField(pf, ["Acceptable Project Type", "Acceptable Project Types"]));

  return {
    who_for_guest: [guestSegments, psychographics].filter(Boolean).join(". ") || null,
    who_for_owner: [preferredOwner, ownerExp].filter(Boolean).join(". ") || null,
    positioning: [chainScale, positioning, model, serviceModel, tagline].filter(Boolean).join(" • ") || null,
    pillars: pillars ? (typeof pillars === "string" ? pillars.split(",").map((s) => s.trim()) : [pillars]) : [],
    guest_promises: [promise, valueProp, differentiators].filter(Boolean).join(" ").trim() || null,
    non_negotiables: [ownerNonNeg, redFlags].filter(Boolean).join(". ") || null,
    where_it_wins: [priorityMarkets, acceptableTypes].filter(Boolean).join(". ") || null,
  };
}

/** Derive Owner Lens from structured fields */
function deriveOwnerLens(loyalty, fee, dealTerms, opSupport, projectFit, brandPortfolio) {
  const lc = loyalty?.fields || {};
  const feeF = fee?.fields || {};
  const dt = dealTerms?.fields || {};
  const op = opSupport?.fields || {};
  const pf = projectFit?.fields || {};

  const loyaltyPct = getField(lc, ["Typical % of Rooms from Loyalty (est.)", "Typical Loyalty Rooms Percent"]);
  const directPct = getField(lc, ["Typical Direct Booking % (est.)", "Typical Direct Booking Percent"]);
  const otaPct = getField(lc, ["Typical OTA Reliance % (est.)", "Typical OTA Reliance Percent"]);

  const pp = brandPortfolio?.fields || {};
  const revpar = valueToStr(getField(pp, "Typical RevPAR Improvement (Brand Benchmark)"));
  const occ = valueToStr(getField(pp, "Typical Occupancy Improvement (Brand Benchmark)"));
  const noi = valueToStr(getField(pp, "Typical NOI Improvement (Brand Benchmark)"));

  const royMin = getField(feeF, ["Min - Typical Royalty Fee Range", "Typical Royalty Min"]);
  const royMax = getField(feeF, ["Max - Typical Royalty Fee Range", "Typical Royalty Max"]);
  const mktMin = getField(feeF, ["Min - Typical Marketing Fee Range"]);
  const mktMax = getField(feeF, ["Max - Typical Marketing Fee Range"]);
  const techMin = getField(feeF, ["Min - Typical Tech", "Typical Technology Fee Min"]);
  const techMax = getField(feeF, ["Max - Typical Tech", "Typical Technology Fee Max"]);

  const feeRanges = [
    royMin != null || royMax != null ? `Royalty: ${royMin ?? "—"}–${royMax ?? "—"}%` : null,
    mktMin != null || mktMax != null ? `Marketing: ${mktMin ?? "—"}–${mktMax ?? "—"}%` : null,
    techMin != null || techMax != null ? `Tech: $${techMin ?? "—"}–$${techMax ?? "—"}` : null,
  ].filter(Boolean);

  const responseTime = valueToStr(getField(op, ["Typical Response Time for Owner Inquiries", "Owner Response Time"]));
  const conversionPIP = valueToStr(getField(dt, ["Mandatory PIP for Conversions", "PIP for Conversions"]));
  const incentives = valueToStr(getField(feeF, ["Typical Incentives Offered", "Incentives"]));
  const coBrand = valueToStr(getField(pf, ["Co-Branding Allowed", "Co-Brand Allowed"]));
  const mixedUse = valueToStr(getField(pf, ["Mixed-Use Development Allowed", "Mixed-Use Allowed"]));

  return {
    demand_engine: {
      loyalty_pct: loyaltyPct != null ? (typeof loyaltyPct === "number" && loyaltyPct <= 1 ? Math.round(loyaltyPct * 100) : loyaltyPct) : null,
      direct_pct: directPct != null ? (typeof directPct === "number" && directPct <= 1 ? Math.round(directPct * 100) : directPct) : null,
      ota_reliance_pct: otaPct != null ? (typeof otaPct === "number" && otaPct <= 1 ? Math.round(otaPct * 100) : otaPct) : null,
      distribution_strengths: [directPct != null && directPct >= 0.3 ? "Direct channel strength" : null, loyaltyPct != null && loyaltyPct >= 0.2 ? "Loyalty base" : null].filter(Boolean),
    },
    revenue_levers: { revpar_improvement: revpar, occ_improvement: occ, noi_improvement: noi, differentiators: [revpar, occ, noi].filter(Boolean).join(", ") || null },
    cost_complexity: { fee_ranges: feeRanges, intensity_notes: responseTime ? `Response time: ${responseTime}` : null },
    flexibility: {
      conversion_friendly: conversionPIP ? (String(conversionPIP).toLowerCase().includes("yes") || String(conversionPIP).toLowerCase().includes("flexible") ? "Yes" : "See PIP") : null,
      co_brand: coBrand,
      mixed_use: mixedUse,
      incentives: incentives,
    },
    owner_support: {
      pre_opening: valueToStr(getField(op, ["Pre-Opening Support", "Pre-Opening Services"])),
      opening: valueToStr(getField(op, ["Opening Support", "Opening Services"])),
      services_summary: valueToStr(getField(op, ["Service Offering Summary", "Ongoing Support Included"])),
    },
  };
}

/** Generate diligence questions from triggers */
function generateDiligenceQuestions(brand, fee, dealTerms, opSupport, projectFit) {
  const questions = [];
  const dt = dealTerms?.fields || {};
  const feeF = fee?.fields || {};
  const pf = projectFit?.fields || {};
  const op = opSupport?.fields || {};

  if (getField(dt, ["Performance Test Requirement", "Performance Test"])) {
    questions.push("What is the performance test methodology and cure period?");
  }
  if (getField(dt, ["Mandatory PIP for Conversions", "Mandatory PIP at Renewal"])) {
    questions.push("What is the typical PIP scope and cost for conversions and renewals?");
  }
  if (getField(feeF, ["Typical Incentives Offered"]) || getField(feeF, ["Key Money / Co-Investment"])) {
    questions.push("What incentives or key money are typically available for this deal type?");
  }
  if (getField(dt, ["Conversion - Typical max time allowed for completion"]) || getField(dt, ["conversionMaxTimeQty"])) {
    questions.push("What is the conversion timeline and extension options?");
  }
  if (getField(op, ["Typical Response Time for Owner Inquiries"])) {
    questions.push("What is the expected response time for owner inquiries and approvals?");
  }
  const redFlags = valueToStr(getField(pf, ["Red Flag Items That Typically Make You Decline or Proceed With Caution - Risk & Compliance"]));
  if (redFlags) {
    questions.push("Are there any red-flag items in our project that we should address proactively?");
  }
  questions.push("What attachments and documents are required for initial application?");
  questions.push("What are common friction points in negotiations for similar deals?");
  return questions.slice(0, 15);
}

/** Build compare_fields for side-by-side comparison */
function buildCompareFields(brandBasics, projectFit, fee, dealTerms) {
  const b = brandBasics?.fields || {};
  const pf = projectFit?.fields || {};
  const feeF = fee?.fields || {};
  const dt = dealTerms?.fields || {};
  return {
    brand_name: valueToStr(getField(b, "Brand Name")),
    parent_company: valueToStr(getField(b, "Parent Company")),
    chain_scale: valueToStr(getField(b, "Hotel Chain Scale")),
    brand_model: valueToStr(getField(b, "Brand Model")),
    service_model: valueToStr(getField(b, "Hotel Service Model")),
    positioning: valueToStr(getField(b, "Brand Positioning")),
    royalty_range: [getField(feeF, "Min - Typical Royalty Fee Range"), getField(feeF, "Max - Typical Royalty Fee Range")].filter((x) => x != null).map(String).join("–") + "%",
    marketing_range: [getField(feeF, "Min - Typical Marketing Fee Range"), getField(feeF, "Max - Typical Marketing Fee Range")].filter((x) => x != null).map(String).join("–") + "%",
    tech_range: [getField(feeF, "Min - Typical Tech"), getField(feeF, "Max - Typical Tech")].filter((x) => x != null).map((x) => (typeof x === "number" ? "$" + x : String(x))).join("–"),
    initial_term: valueToStr(getField(dt, ["Quantity - Typical Minimum Initial Term", "Typical Minimum Initial Term"])),
    renewal_option: valueToStr(getField(dt, ["Quantity - Typical Renewal Option", "Typical Renewal Option"])),
    pip_conversions: valueToStr(getField(dt, ["Mandatory PIP for Conversions", "PIP for Conversions"])),
    preferred_owner: valueToStr(getField(pf, "Preferred Owner/Investor Type")),
    acceptable_project_types: valueToStr(getField(pf, "Acceptable Project Type")),
    room_range: [getField(pf, "Min - Room Count"), getField(pf, "Max - Room Count")].filter((x) => x != null).join("–"),
  };
}

/** Fit-to-deal: compute 5 dimension scores + evidence + questions */
function computeFitScores(brandData, dealData) {
  const scores = {};
  const evidence = {};
  const dims = [
    "project_fit",
    "economics",
    "deal_structure",
    "operations",
    "flexibility",
  ];

  const attrs = dealData?.attributes || {};
  const pf = brandData?.projectFit?.fields || {};
  const feeF = brandData?.feeStructure?.fields || {};
  const dt = brandData?.dealTerms?.fields || {};
  const op = brandData?.operationalSupport?.fields || {};

  const projectType = attrs.projectType || attrs["Project Type"] || "";
  const roomCount = parseInt(attrs.roomCount || attrs["Total Number of Rooms/Keys"] || attrs.totalRooms || 0, 10);
  const stage = attrs.stageOfDevelopment || attrs["Stage of Development"] || "";
  const chainScale = attrs.hotelChainScale || attrs["Hotel Chain Scale"] || "";
  const region = attrs.country || attrs.region || "";

  const roomMin = parseFloat(getField(pf, ["Min - Room Count", "Min - Ideal Project Size"]) || 0);
  const roomMax = parseFloat(getField(pf, ["Max - Room Count", "Max - Ideal Project Size"]) || 9999);
  const roomFit = roomCount >= roomMin && roomCount <= roomMax ? 100 : roomCount > 0 ? 50 : 40;
  scores.project_fit = Math.min(100, roomFit + (projectType ? 10 : 0) + (chainScale ? 5 : 0));
  evidence.project_fit = [`Room count ${roomCount} ${roomCount >= roomMin && roomCount <= roomMax ? "within" : "outside"} ideal range (${roomMin}–${roomMax})`];

  const royMin = parseFloat(getField(feeF, "Min - Typical Royalty Fee Range") || 0);
  const royMax = parseFloat(getField(feeF, "Max - Typical Royalty Fee Range") || 10);
  scores.economics = 75;
  evidence.economics = [`Typical royalty ${royMin}–${royMax}%`, getField(feeF, "Typical Incentives Offered") ? "Incentives may be available" : null].filter(Boolean);

  scores.deal_structure = 70;
  evidence.deal_structure = [
    `Initial term: ${valueToStr(getField(dt, "Quantity - Typical Minimum Initial Term")) || "—"}`,
    getField(dt, "Mandatory PIP for Conversions") ? "PIP required for conversions" : null,
  ].filter(Boolean);

  scores.operations = getField(op, "Typical Response Time for Owner Inquiries") ? 80 : 65;
  evidence.operations = [getField(op, "Service Offering Summary") ? "Support services documented" : null, getField(op, "Typical Response Time for Owner Inquiries") ? "Response time specified" : null].filter(Boolean);

  const conversionFriendly = valueToStr(getField(dt, "Mandatory PIP for Conversions")).toLowerCase();
  scores.flexibility = conversionFriendly.includes("flexible") || conversionFriendly.includes("case") ? 85 : 65;
  evidence.flexibility = [getField(pf, "Co-Branding Allowed") ? "Co-branding allowed" : null, getField(pf, "Mixed-Use Development Allowed") ? "Mixed-use allowed" : null].filter(Boolean);

  const questions = generateDiligenceQuestions(brandData.brandBasics, brandData.feeStructure, brandData.dealTerms, brandData.operationalSupport, brandData.projectFit);

  return { scores, evidence, questions };
}

// --- API handlers ---

/** GET /api/brand-explorer/brands */
export async function listBrands(req, res) {
  try {
    const base = getBase();
    const brands = await base(TABLES.brandBasics)
      .select({
        filterByFormula: "OR(FIND('Active', {Brand Status}) > 0, FIND('Live', {Brand Status}) > 0)",
        maxRecords: 500,
      })
      .all();

    const list = brands.map((rec) => {
      const f = rec.fields || {};
      const name = valueToStr(getField(f, "Brand Name")) || "Unknown";
      const brandKey = toBrandKey(name);
      const chainScale = valueToStr(getField(f, "Hotel Chain Scale"));
      return {
        brand_key: brandKey,
        name,
        record_id: rec.id,
        logo: Array.isArray(f.Logo) && f.Logo[0]?.url ? f.Logo[0].url : (f.Logo?.url || null),
        parent_company: valueToStr(getField(f, "Parent Company")),
        chain_scale: chainScale,
        segment: valueToStr(getField(f, "Brand Positioning")),
        tags: chainScale ? [chainScale] : [],
        last_updated: null,
      };
    });

    list.sort((a, b) => (a.name || "").localeCompare(b.name || "", undefined, { sensitivity: "base" }));

    const filterOptions = {
      chain_scales: [...new Set(list.map((b) => b.chain_scale).filter(Boolean))].sort(),
      segments: [...new Set(list.map((b) => b.segment).filter(Boolean))].slice(0, 20),
    };

    res.json({ success: true, brands: list, filterOptions });
  } catch (error) {
    console.error("[Brand Explorer] listBrands:", error);
    res.status(500).json({ success: false, error: error.message });
  }
}

/** GET /api/brand-explorer/brand/:brand_key */
export async function getBrand(req, res) {
  try {
    const brandKey = (req.params.brand_key || "").trim();
    if (!brandKey) return res.status(400).json({ success: false, error: "brand_key required" });

    const base = getBase();
    const brandRec = await findBrandByKey(base, brandKey);
    if (!brandRec) return res.status(404).json({ success: false, error: "Brand not found" });

    const name = valueToStr(brandRec.fields["Brand Name"]) || "Unknown";
    const brandRecordId = brandRec.id;

    const [projectFit, feeStructure, dealTerms, operationalSupport, loyaltyCommercial, brandFootprint, portfolioPerf, modules, updates] = await Promise.all([
      findLinkedRecordByBrand(base, TABLES.projectFit, brandRecordId, name),
      findLinkedRecordByBrand(base, TABLES.feeStructure, brandRecordId, name),
      findLinkedRecordByBrand(base, TABLES.dealTerms, brandRecordId, name),
      findLinkedRecordByBrand(base, TABLES.operationalSupport, brandRecordId, name),
      findLinkedRecordByBrand(base, TABLES.loyaltyCommercial, brandRecordId, name),
      findLinkedRecordByBrand(base, TABLES.brandFootprint, brandRecordId, name),
      findLinkedRecordByBrand(base, "Brand Setup - Portfolio & Performance", brandRecordId, name).catch(() => null),
      fetchModulesForBrand(base, brandKey),
      fetchUpdatesForBrand(base, brandKey),
    ]);

    const deriveDna = deriveDnaCard(brandRec, projectFit, brandFootprint);
    const deriveOwner = deriveOwnerLens(loyaltyCommercial, feeStructure, dealTerms, operationalSupport, projectFit, portfolioPerf);

    const dna_card = modules?.status === "Live" && modules?.dna_card ? { override: modules.dna_card } : { derived: deriveDna };
    const owner_lens = modules?.status === "Live" && modules?.owner_lens ? { override: modules.owner_lens } : { derived: deriveOwner };

    const diligence_toolkit = {
      questions: generateDiligenceQuestions(brandRec, feeStructure, dealTerms, operationalSupport, projectFit),
      attachments_checklist: ["Franchise application", "Financial statements", "Site/building details", "Ownership structure", "Management resume"],
      friction_points: [],
    };
    const redFlags = valueToStr(projectFit?.fields?.["Red Flag Items That Typically Make You Decline or Proceed With Caution - Risk & Compliance"]);
    if (redFlags) {
      diligence_toolkit.friction_points.push("Review red-flag criteria: " + redFlags.slice(0, 150) + (redFlags.length > 150 ? "…" : ""));
    }

    const partner_page = {
      brand_name: name,
      parent_company: valueToStr(brandRec.fields["Parent Company"]),
      website: valueToStr(brandRec.fields["Brand Website"]),
      logo: Array.isArray(brandRec.fields?.Logo) && brandRec.fields.Logo[0]?.url ? brandRec.fields.Logo[0].url : (brandRec.fields?.Logo?.url || null),
    };

    const compare_fields = buildCompareFields(brandRec, projectFit, feeStructure, dealTerms);

    res.json({
      success: true,
      brand_key: toBrandKey(name),
      dna_card,
      owner_lens,
      diligence_toolkit,
      partner_page,
      compare_fields,
      updates: updates || [],
      last_updated: modules?.last_updated || null,
    });
  } catch (error) {
    console.error("[Brand Explorer] getBrand:", error);
    res.status(500).json({ success: false, error: error.message });
  }
}

/** POST /api/brand-explorer/fit-to-deal */
export async function fitToDeal(req, res) {
  try {
    const { brand_key, deal_id, attributes } = req.body || {};
    const brandKey = (brand_key || "").trim();
    if (!brandKey) return res.status(400).json({ success: false, error: "brand_key required" });

    const base = getBase();
    const brandRec = await findBrandByKey(base, brandKey);
    if (!brandRec) return res.status(404).json({ success: false, error: "Brand not found" });

    const name = valueToStr(brandRec.fields["Brand Name"]);
    const brandRecordId = brandRec.id;

    let dealData = { attributes: attributes || {} };
    if (deal_id && !attributes) {
      try {
        const { getDealById } = await import("./my-deals.js");
        let payload = null;
        const reqMock = { params: { recordId: deal_id } };
        const resMock = {
          status: () => resMock,
          json: (p) => { payload = p; return resMock; },
        };
        await getDealById(reqMock, resMock);
        if (payload?.success && payload?.deal) {
          const d = payload.deal;
          const n = payload.normalized || {};
          dealData.attributes = {
            projectType: d["Project Type"] || n.projectType,
            stageOfDevelopment: d["Stage of Development"] || n.stageOfDevelopment,
            "Total Number of Rooms/Keys": d["Total Number of Rooms/Keys"] || n["Total Number of Rooms/Keys"],
            roomCount: d["Total Number of Rooms/Keys"] ?? n.roomCount ?? n["Total Number of Rooms/Keys"],
            "Hotel Chain Scale": d["Hotel Chain Scale"] || n.hotelChainScale,
            country: d.Country || d.country || n.country,
            region: d.region || n.region,
            ...d,
            ...n,
          };
        }
      } catch (e) {
        console.warn("[Brand Explorer] Could not load deal:", e.message);
      }
    }

    const [projectFit, feeStructure, dealTerms, operationalSupport] = await Promise.all([
      findLinkedRecordByBrand(base, TABLES.projectFit, brandRecordId, name),
      findLinkedRecordByBrand(base, TABLES.feeStructure, brandRecordId, name),
      findLinkedRecordByBrand(base, TABLES.dealTerms, brandRecordId, name),
      findLinkedRecordByBrand(base, TABLES.operationalSupport, brandRecordId, name),
    ]);

    const brandData = { brandBasics: brandRec, projectFit, feeStructure, dealTerms, operationalSupport };
    const { scores, evidence, questions } = computeFitScores(brandData, dealData);

    res.json({
      success: true,
      brand_key: toBrandKey(name),
      brand_name: name,
      dimensions: [
        { id: "project_fit", label: "Project Fit", score: scores.project_fit, evidence: evidence.project_fit || [] },
        { id: "economics", label: "Economics", score: scores.economics, evidence: evidence.economics || [] },
        { id: "deal_structure", label: "Deal Structure", score: scores.deal_structure, evidence: evidence.deal_structure || [] },
        { id: "operations", label: "Operations & Support", score: scores.operations, evidence: evidence.operations || [] },
        { id: "flexibility", label: "Flexibility", score: scores.flexibility, evidence: evidence.flexibility || [] },
      ],
      top_questions: questions,
    });
  } catch (error) {
    console.error("[Brand Explorer] fitToDeal:", error);
    res.status(500).json({ success: false, error: error.message });
  }
}
