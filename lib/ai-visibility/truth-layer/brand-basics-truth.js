/**
 * Brand Basics truth facts — read-only (P0D-A). No Airtable writes.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { normalizeMatchKey } from "../normalize-entities.js";
import { brandBasicsFieldEligibility } from "./truth-eligibility.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.join(__dirname, "..", "..", "..");

export const DEFAULT_BASICS_AUDIT_PATH = path.join(
  REPO_ROOT,
  "data",
  "ai-visibility",
  "phase3a7-showcase-brand-basics-audit.json"
);

const FIELD_MAP = Object.freeze({
  brandName: "Brand Name",
  parentCompany: "Parent Company",
  chainScale: "Hotel Chain Scale",
  brandModel: "Brand Model",
  brandArchitecture: "Brand Architecture",
  brandPositioning: "Brand Positioning",
});

function normalizeParent(value) {
  if (!value) return null;
  return String(value)
    .replace(/,?\s*(Inc\.?|LLC|Ltd\.?|Corporation)$/i, "")
    .trim();
}

function parentKeys(value) {
  const raw = String(value || "").trim();
  if (!raw) return [];
  const norm = normalizeMatchKey(normalizeParent(raw));
  const full = normalizeMatchKey(raw);
  return [...new Set([norm, full].filter(Boolean))];
}

/**
 * Load governed Brand Basics truth index from audit fixture + optional live loader.
 * @param {object} [options]
 */
export async function loadBrandBasicsTruthIndex(options = {}) {
  const auditPath = options.auditPath || DEFAULT_BASICS_AUDIT_PATH;
  const byId = new Map();
  const conflicts = [];

  if (fs.existsSync(auditPath)) {
    const audit = JSON.parse(fs.readFileSync(auditPath, "utf8"));
    for (const row of audit.rows || []) {
      if (!row.BRAND_ID) continue;
      const parentCanonical = row.CANONICAL_PARENT || row.CURRENT_PARENT || null;
      byId.set(row.BRAND_ID, {
        brandId: row.BRAND_ID,
        brandName: row.BRAND_NAME_LIVE || row.BRAND,
        parentCompany: parentCanonical,
        parentCompanyRaw: row.CURRENT_PARENT || null,
        chainScale: row.CHAIN_SCALE || null,
        brandModel: row.BRAND_MODEL || null,
        brandArchitecture: inferArchitectureFromModel(row.BRAND_MODEL),
        brandStatus: row.BRAND_STATUS || null,
        activeLive: row.ACTIVE_LIVE === true,
        source: "Brand Setup - Brand Basics",
        governanceState: "STRUCTURED_GOVERNED_FACT",
        parentNormalizedKeys: parentKeys(parentCanonical),
      });
      if (row.CURRENT_PARENT && row.CANONICAL_PARENT && normalizeParent(row.CURRENT_PARENT) !== normalizeParent(row.CANONICAL_PARENT)) {
        conflicts.push({ brandId: row.BRAND_ID, field: "Parent Company", raw: row.CURRENT_PARENT, canonical: row.CANONICAL_PARENT });
      }
    }
  }

  if (options.loadLive && !options.fixtureOnly) {
    try {
      const { loadLiveBrandEntities } = await import("../load-brands-live.js");
      const live = await loadLiveBrandEntities(options.liveDeps || {});
      for (const e of live.entities || []) {
        const existing = byId.get(e.id);
        const fact = {
          brandId: e.id,
          brandName: e.name,
          parentCompany: normalizeParent(e.parentCompany),
          parentCompanyRaw: e.parentCompany,
          chainScale: e.chainScale,
          brandModel: e.brandModel,
          brandArchitecture: inferArchitectureFromModel(e.brandModel),
          brandStatus: e.brandStatus,
          activeLive: true,
          source: "Brand Setup - Brand Basics (live)",
          governanceState: "STRUCTURED_GOVERNED_FACT",
          parentNormalizedKeys: parentKeys(e.parentCompany),
        };
        if (existing && existing.parentCompany && fact.parentCompany && normalizeMatchKey(existing.parentCompany) !== normalizeMatchKey(fact.parentCompany)) {
          conflicts.push({ brandId: e.id, field: "Parent Company", fixture: existing.parentCompany, live: fact.parentCompany });
        }
        byId.set(e.id, { ...existing, ...fact });
      }
    } catch {
      // Fixture-only mode when live unavailable
    }
  }

  return { byId, conflicts, CENSUS_READS: 0 };
}

function inferArchitectureFromModel(brandModel) {
  const m = String(brandModel || "").toLowerCase();
  if (m.includes("collection") || m.includes("soft")) return "Soft/Collection Brand";
  if (m.includes("hard")) return "Standalone Brand";
  return null;
}

/**
 * Audit Brand Basics fields for truth eligibility.
 * @param {Map<string, object>} byId
 */
export function auditBrandBasicsFields(byId = new Map()) {
  const rows = Object.values(Object.fromEntries([...byId.entries()].map(([id, v]) => [id, v])));
  const fields = [
    { field: "Brand Name", key: "brandName" },
    { field: "Parent Company", key: "parentCompany" },
    { field: "Hotel Chain Scale", key: "chainScale" },
    { field: "Brand Model", key: "brandModel" },
    { field: "Brand Architecture", key: "brandArchitecture" },
  ];

  return fields.map(({ field, key }) => {
    let present = 0;
    let missing = 0;
    for (const row of rows) {
      if (row[key]) present += 1;
      else missing += 1;
    }
    const gov = brandBasicsFieldEligibility(field);
    return {
      field,
      presentCount: present,
      missingCount: missing,
      conflictCount: 0,
      governanceState: gov.governance,
      safeForTruthLayer: gov.eligibility,
    };
  });
}

/**
 * @param {string} brandId
 * @param {Map} byId
 */
export function getBrandBasicsTruthFact(brandId, byId, claimType) {
  const row = byId.get(brandId);
  if (!row || !row.activeLive) return null;

  switch (claimType) {
    case "PARENT_COMPANY":
    case "BRAND_FAMILY":
      return row.parentCompany
        ? { factType: claimType, factValue: row.parentCompany, governanceState: "COMPANY_PUBLISHED", source: row.source, eligibility: "ELIGIBLE" }
        : null;
    case "CHAIN_SCALE":
      return row.chainScale
        ? { factType: claimType, factValue: row.chainScale, governanceState: "STRUCTURED_GOVERNED_FACT", source: row.source, eligibility: "ELIGIBLE" }
        : null;
    case "BRAND_MODEL":
      return row.brandModel
        ? { factType: claimType, factValue: row.brandModel, governanceState: "STRUCTURED_GOVERNED_FACT", source: row.source, eligibility: "ELIGIBLE" }
        : null;
    case "SOFT_BRAND_COLLECTION":
    case "COLLECTION_STATUS":
      return row.brandArchitecture || row.brandModel
        ? {
            factType: "SOFT_BRAND_COLLECTION",
            factValue: row.brandArchitecture || row.brandModel,
            governanceState: "STRUCTURED_GOVERNED_FACT",
            source: row.source,
            eligibility: "ELIGIBLE",
          }
        : null;
    case "POSITIONING":
    case "STRUCTURED_POSITIONING":
      return null;
    case "CONVERSION_ORIENTATION":
      return null;
    default:
      return null;
  }
}

/**
 * Compare AI parent claim to governed parent.
 */
export function parentCompanyMatches(aiClaimValue, dealalityFact, siblingParentKeys = []) {
  if (!aiClaimValue || !dealalityFact?.factValue) return { match: false, ambiguous: false };
  const cleaned = String(aiClaimValue)
    .replace(/^the\s+/i, "")
    .replace(/\s+portfolio$/i, "")
    .trim();
  const aiKeys = parentKeys(cleaned);
  const dealKeys = dealalityFact.parentNormalizedKeys || parentKeys(dealalityFact.factValue);

  for (const ak of aiKeys) {
    for (const dk of dealKeys) {
      if (ak === dk || ak.includes(dk) || dk.includes(ak)) {
        return { match: true, ambiguous: false };
      }
    }
  }

  for (const sk of siblingParentKeys) {
    for (const ak of aiKeys) {
      if (ak === sk || ak.includes(sk)) {
        return { match: false, ambiguous: false, siblingLeak: true };
      }
    }
  }

  return { match: false, ambiguous: false };
}

export { FIELD_MAP, normalizeParent, parentKeys };
