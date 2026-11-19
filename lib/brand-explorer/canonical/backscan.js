/**
 * Read-only Active/Live backscan. Never writes Airtable.
 */
import { readFileSync, mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import { repoRoot } from "./product-shell.js";
import { evaluateMasterRelease } from "./master-release.js";
import { CANONICAL_TAB_IDS } from "./product-shell.js";
import { classifyPropertyImage } from "./image-evidence.js";

const BASICS = "Brand Setup - Brand Basics";
const PRESENTATION = "Brand Setup - Brand Explorer Presentation";

async function airtableList(table, { formula, fields }) {
  const key = process.env.AIRTABLE_API_KEY;
  const base = process.env.AIRTABLE_BASE_ID;
  if (!key || !base) {
    return { ok: false, reason: "AIRTABLE_API_KEY or AIRTABLE_BASE_ID missing", records: [] };
  }
  const records = [];
  let offset;
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    if (formula) params.set("filterByFormula", formula);
    for (const field of fields) params.append("fields[]", field);
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${base}/${encodeURIComponent(table)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${key}` } });
    if (!res.ok) {
      return { ok: false, reason: `Airtable ${res.status} on ${table}`, records };
    }
    const data = await res.json();
    records.push(...(data.records || []));
    offset = data.offset;
  } while (offset);
  return { ok: true, records };
}

function profileFromBrand(brand, slots) {
  const regions = slots
    .filter((row) => String(row.slotKey || "").startsWith("footprint.region."))
    .map((row) => ({ label: row.title || "" }));
  const openings = slots.filter((row) => row.slotKey === "footprint.openings");
  const properties = openings.map((row) => ({
    name: row.title || "property",
    identities: String(row.title || "").includes(" / ") ? String(row.title).split(" / ") : [row.title || "property"],
    imageEvidence: row.hasImage ? "UNVERIFIED_PROPERTY_IDENTITY" : "MISSING_PROPERTY_IMAGE",
    sameBrandDonor: false,
  }));
  const items = [
    {
      itemId: "standards.residences",
      disposition: slots.some((row) => /residence/i.test(row.slotKey || "")) ? "APPLICABLE_POPULATED_POSITIVE" : "RESEARCH_REQUIRED",
      body: "",
      sourceIds: [],
      purpose: "Negative intelligence parent",
    },
  ];
  return {
    identity: { brandName: brand.name, slug: brand.id, brandStatus: brand.status },
    foundation: { buildBriefPresent: false },
    tabIds: [...CANONICAL_TAB_IDS],
    items,
    collections: {
      flexibility: [],
      bestAt: [],
      recentMomentum: slots.filter((row) => row.slotKey === "footprint.momentum").map((row) => ({ title: row.title, url: "" })),
      similarBrands: slots.filter((row) => row.slotKey === "insight.similar"),
      geographicRegions: regions,
      representativeProperties: properties,
    },
    regions,
    properties,
    metrics: [],
    facts: [],
    geometry: {},
    liveValidation: null,
  };
}

export async function runActiveUniverseBackscan() {
  const basics = await airtableList(BASICS, {
    formula: "OR({Brand Status}='Active',{Brand Status}='Live')",
    fields: ["Brand Name", "Brand Status"],
  });
  if (!basics.ok) {
    return { ok: false, reason: basics.reason, writes: 0 };
  }
  const brands = basics.records.map((row) => ({
    id: row.id,
    name: row.fields["Brand Name"] || "",
    status: row.fields["Brand Status"] || "",
  }));

  const presentation = await airtableList(PRESENTATION, {
    fields: ["Brand", "Slot Key", "Title", "Image"],
  });
  const slotsByBrand = new Map();
  if (presentation.ok) {
    for (const row of presentation.records) {
      const links = row.fields.Brand || [];
      const slot = {
        slotKey: row.fields["Slot Key"] || "",
        title: row.fields.Title || "",
        hasImage: Array.isArray(row.fields.Image) && row.fields.Image.length > 0,
      };
      for (const id of links) {
        if (!slotsByBrand.has(id)) slotsByBrand.set(id, []);
        slotsByBrand.get(id).push(slot);
      }
    }
  }

  const results = brands.map((brand) => {
    const evaluated = evaluateMasterRelease(profileFromBrand(brand, slotsByBrand.get(brand.id) || []));
    const images = (slotsByBrand.get(brand.id) || [])
      .filter((slot) => slot.slotKey === "footprint.openings")
      .map((slot) => classifyPropertyImage({ imageEvidence: slot.hasImage ? "UNVERIFIED_PROPERTY_IDENTITY" : "MISSING_PROPERTY_IMAGE" }).state);
    return {
      id: brand.id,
      name: brand.name,
      status: brand.status,
      masterPass: evaluated.MASTER_RELEASE_CONTRACT_PASS,
      ready: evaluated.status,
      defectClasses: [...new Set(evaluated.findings.map((f) => f.defectClass))],
      failedGates: evaluated.gates.filter((g) => !g.pass).map((g) => g.name),
      images,
    };
  });

  const probes = await airtableList(BASICS, {
    formula: "OR({Brand Name}='21c Museum Hotel',{Brand Name}='Caption by Hyatt')",
    fields: ["Brand Name", "Brand Status"],
  });
  const probeResults = (probes.records || []).map((row) => {
    const brand = { id: row.id, name: row.fields["Brand Name"], status: row.fields["Brand Status"] };
    const evaluated = evaluateMasterRelease(profileFromBrand(brand, slotsByBrand.get(brand.id) || []));
    return {
      id: brand.id,
      name: brand.name,
      status: brand.status,
      masterPass: evaluated.MASTER_RELEASE_CONTRACT_PASS,
      ready: evaluated.status,
      failedGates: evaluated.gates.filter((g) => !g.pass).map((g) => g.name),
    };
  });

  let baselineSlugs = [];
  try {
    const baseline = JSON.parse(readFileSync(join(repoRoot(), "reports/brand-explorer-62-active-public-full-baseline.json"), "utf8"));
    const rows = baseline.brands || baseline.rows || baseline.profiles || [];
    baselineSlugs = rows.map((row) => row.slug).filter(Boolean);
  } catch {
    baselineSlugs = [];
  }

  const defectCounts = {};
  const imageCounts = {};
  for (const row of results) {
    for (const defect of row.defectClasses) defectCounts[defect] = (defectCounts[defect] || 0) + 1;
    for (const state of row.images) imageCounts[state] = (imageCounts[state] || 0) + 1;
  }

  const report = {
    ok: true,
    writes: 0,
    presentationRead: presentation.ok,
    presentationError: presentation.ok ? null : presentation.reason,
    activeCount: brands.length,
    byStatus: brands.reduce((acc, brand) => {
      acc[brand.status] = (acc[brand.status] || 0) + 1;
      return acc;
    }, {}),
    cleanCount: results.filter((row) => row.masterPass).length,
    failingCount: results.filter((row) => !row.masterPass).length,
    defectCounts,
    imageEvidenceDistribution: imageCounts,
    protected62FreezeCount: baselineSlugs.length,
    note: "68 is the live Brand Status Active count. 62 is the 2026-08-08 public-full freeze, not the live universe. No brand is grandfathered into MASTER_RELEASE_CONTRACT_PASS.",
    probes: probeResults,
    brands: results.map((row) => ({ id: row.id, name: row.name, status: row.status, ready: row.ready })),
  };

  const outDir = join(repoRoot(), "reports/brand-explorer-system");
  mkdirSync(outDir, { recursive: true });
  writeFileSync(join(outDir, "active-universe-backscan.json"), JSON.stringify(report, null, 2));
  return report;
}
