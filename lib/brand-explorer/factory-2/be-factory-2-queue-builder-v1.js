/**
 * Build complete Factory 2.0 brand queue from Airtable Brand Basics.
 */
import {
  BE_FACTORY_2_0_BATCH_V1,
  FACTORY_2_STATUSES,
  PROTECTED_BRAND_IDS,
  AUTOGRAPH_BRAND_ID,
  DEFAULT_PAID_RESEARCH_BUDGET_USD,
} from "./be-factory-2-contracts-v1.js";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const SLEEP_MS = 160;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function atFetch(baseId, token, table, { offset, filterByFormula, fields } = {}) {
  const params = new URLSearchParams({ pageSize: "100" });
  if (offset) params.set("offset", offset);
  if (filterByFormula) params.set("filterByFormula", filterByFormula);
  for (const f of fields || []) params.append("fields[]", f);
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}?${params}`;
  let attempt = 0;
  while (attempt < 8) {
    attempt += 1;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = await res.json().catch(() => ({}));
    if (res.status === 429) {
      await sleep(SLEEP_MS * attempt * 3);
      continue;
    }
    if (!res.ok) throw new Error(`Airtable ${res.status}: ${JSON.stringify(json).slice(0, 200)}`);
    return json;
  }
  throw new Error("Airtable rate-limit retries exhausted");
}

async function listAll(baseId, token, table, opts = {}) {
  const out = [];
  let offset;
  do {
    const page = await atFetch(baseId, token, table, { ...opts, offset });
    out.push(...(page.records || []));
    offset = page.offset;
    await sleep(SLEEP_MS);
  } while (offset);
  return out;
}

function presentationCountByBrand(presentationRecords) {
  const map = new Map();
  for (const r of presentationRecords) {
    const f = r.fields || {};
    const links =
      f.Brand ||
      f["Brand Setup - Brand Basics"] ||
      f["Brand Basics"] ||
      f.Brand_Basic_ID ||
      [];
    const ids = Array.isArray(links) ? links : [];
    for (const id of ids) {
      map.set(id, (map.get(id) || 0) + 1);
    }
  }
  return map;
}

function estimateEffort(presentationCount) {
  if (presentationCount >= 120) return { effort: "M", hours: 4 };
  if (presentationCount >= 80) return { effort: "M", hours: 6 };
  if (presentationCount >= 40) return { effort: "H", hours: 8 };
  return { effort: "H", hours: 10 };
}

function researchRequirements(presentationCount) {
  const req = ["ten_tab_audit", "founder_quality_lessons", "permanent_loyalty_region_standards"];
  if (presentationCount < 80) req.push("commercial_loyalty_economics_gap_fill");
  if (presentationCount < 40) req.push("deep_research_required");
  return req;
}

/**
 * @param {{ root: string, token: string, baseId: string, existingAutographStage?: object|null }} opts
 */
export async function buildCompleteBrandQueue(opts) {
  const { token, baseId, existingAutographStage = null } = opts;
  const basics = await listAll(baseId, token, BASICS_TABLE, {
    filterByFormula: "OR({Brand Status}='Active',{Brand Status}='Live')",
    fields: ["Brand Name", "Brand Status", "Parent Company"],
  });
  const presentations = await listAll(baseId, token, PRESENTATION_TABLE, {
    // Link field is `Brand` (see brand-explorer-airtable-integration-sot-v1.md)
    fields: ["Brand", "Slot Key", "Active"],
  });
  const counts = presentationCountByBrand(presentations);

  const brands = [];
  for (const rec of basics) {
    const id = rec.id;
    const name = nz(rec.fields?.["Brand Name"]) || id;
    const parent = nz(rec.fields?.["Parent Company"]);
    const status = nz(rec.fields?.["Brand Status"]);
    const presentationCount = counts.get(id) || 0;
    const protectedBrand = PROTECTED_BRAND_IDS.includes(id);
    const effort = estimateEffort(presentationCount);

    let factoryStatus = FACTORY_2_STATUSES.QUEUED;
    let note = "Eligible for Factory 2.0 batch research";
    if (protectedBrand) {
      factoryStatus = FACTORY_2_STATUSES.SKIPPED_PROTECTED;
      note = "Curio/Ascend verified Presentation — excluded from reprocessing";
    } else if (id === AUTOGRAPH_BRAND_ID) {
      factoryStatus =
        existingAutographStage?.factoryStatus || FACTORY_2_STATUSES.QUEUED;
      note = "Preserve Factory 1.0 Autograph staged candidate — do not discard";
    }

    brands.push({
      brandId: id,
      brandName: name,
      parentCompany: parent,
      brandStatus: status,
      presentationCount,
      structuredIntelligence: {
        presentationRows: presentationCount,
        hasPresentation: presentationCount > 0,
      },
      researchRequirements: protectedBrand ? [] : researchRequirements(presentationCount),
      // Temporary sort keys — renumbered below
      priority: protectedBrand ? 9999 : 100,
      estimatedEffort: effort.effort,
      estimatedHours: effort.hours,
      estimatedResearchSpendUsd: DEFAULT_PAID_RESEARCH_BUDGET_USD,
      status: factoryStatus,
      writebackAuthorized: false,
      note,
    });
  }

  // Priority: Autograph first (preserve in-flight), then richest Presentation inventory.
  brands.sort((a, b) => {
    if (a.status === FACTORY_2_STATUSES.SKIPPED_PROTECTED && b.status !== FACTORY_2_STATUSES.SKIPPED_PROTECTED)
      return 1;
    if (b.status === FACTORY_2_STATUSES.SKIPPED_PROTECTED && a.status !== FACTORY_2_STATUSES.SKIPPED_PROTECTED)
      return -1;
    if (a.brandId === AUTOGRAPH_BRAND_ID) return -1;
    if (b.brandId === AUTOGRAPH_BRAND_ID) return 1;
    return (
      (b.presentationCount || 0) - (a.presentationCount || 0) ||
      a.brandName.localeCompare(b.brandName)
    );
  });

  // Re-number eligible priorities densely
  let p = 1;
  for (const b of brands) {
    if (b.status === FACTORY_2_STATUSES.SKIPPED_PROTECTED) {
      b.priority = 9999;
      continue;
    }
    b.priority = p++;
  }

  return {
    contract: BE_FACTORY_2_0_BATCH_V1,
    generatedAt: new Date().toISOString(),
    universe: {
      basicsTable: BASICS_TABLE,
      presentationTable: PRESENTATION_TABLE,
      activeLiveCount: basics.length,
      presentationRowCount: presentations.length,
      protectedExcluded: [...PROTECTED_BRAND_IDS],
    },
    brands,
    summary: {
      total: brands.length,
      eligible: brands.filter((b) => b.status !== FACTORY_2_STATUSES.SKIPPED_PROTECTED).length,
      skippedProtected: brands.filter((b) => b.status === FACTORY_2_STATUSES.SKIPPED_PROTECTED)
        .length,
      autographPreserved: brands.some((b) => b.brandId === AUTOGRAPH_BRAND_ID),
    },
  };
}
