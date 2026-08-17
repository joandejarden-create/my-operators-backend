/**
 * Autopilot active-brand-setup scope — read-only Brand Setup Active/Live control list.
 * Never writes Brand Setup / Brand Explorer.
 */

import fs from "node:fs";
import path from "node:path";

import { isBrandStatusActive } from "../brand-status-active.js";
import {
  ACTIVE_UNIVERSE_SOURCE,
  ACTIVE_UNIVERSE_VERSION,
} from "../partner-intelligence/brand-explorer-active-universe.js";
import { resolveExtractorFamily } from "./census-family-extractor-registry.js";
import { inferParentCompanyForAutopilot } from "./census-autopilot-parent-inference.js";
import { loadActiveBrandUniverse } from "./production-census-first-pass-enrichment.js";

export const HELD_EXCLUDED_SLUGS = Object.freeze([
  "four-points-flex-by-sheraton",
  "radisson-collection",
  "house-of-originals",
  "morgans-originals",
]);

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function parentFromBrand(b) {
  return b.parentCompany || b.parentPlatform || b.parent || b.family || null;
}

function familyFromParent(parent, slug) {
  return resolveExtractorFamily(parent || slug || "").family;
}

function parentMatchesFilter(parent, slug, parentCompanyFilter) {
  if (!parentCompanyFilter) return true;
  const pNorm = norm(parentCompanyFilter);
  const parentNorm = norm(parent);
  const wantFam = resolveExtractorFamily(parentCompanyFilter).family;
  const gotFam = familyFromParent(parent, slug);
  if (wantFam !== "generic" && gotFam === wantFam) return true;
  // Preferred / Preferred Hotels & Resorts
  if (/preferred/i.test(pNorm) && /preferred/i.test(parentNorm || "")) return true;
  if (parentNorm && (parentNorm.includes(pNorm) || pNorm.includes(parentNorm))) return true;
  return false;
}

/**
 * @param {{
 *   brands?: object[],
 *   region?: string,
 *   parentCompany?: string|null,
 *   includeHeldProbe?: boolean,
 *   skipUniverseLoad?: boolean,
 * }} [opts]
 */
export function buildActiveBrandSetupControlList(opts = {}) {
  const region = opts.region || "CALA";
  let brands = opts.brands;
  const source = {
    ...ACTIVE_UNIVERSE_SOURCE,
    version: ACTIVE_UNIVERSE_VERSION,
    read_only: true,
    brand_setup_writes: false,
    brand_explorer_writes: false,
  };

  if (!brands) {
    if (opts.skipUniverseLoad) brands = [];
    else {
      const universe = loadActiveBrandUniverse();
      brands = universe.brands || [];
      source.baseline_path = "reports/brand-explorer-62-active-public-full-baseline.json";
      source.active_count = universe.activeCount;
    }
  }

  const included = [];
  const excluded = [];
  let aliasIndex = null;
  try {
    if (!opts.skipUniverseLoad) aliasIndex = loadActiveBrandUniverse();
  } catch {
    aliasIndex = null;
  }

  for (const b of brands) {
    const slug = String(b.slug || "").trim();
    const name = String(b.brandName || b.name || "").trim();
    const status = b.brandStatus || b.status || "Active";
    const recordId = b.recordId || b.id || null;
    const parentRaw = parentFromBrand(b);
    const inferred = inferParentCompanyForAutopilot({
      brand_slug: slug,
      slug,
      parent_company: parentRaw,
      parentCompany: parentRaw,
    });
    const parent = inferred.parent_company;

    if (HELD_EXCLUDED_SLUGS.includes(slug)) {
      excluded.push({
        slug,
        brand_name: name,
        reason: "held_or_not_active_live",
        brand_status: status,
      });
      continue;
    }
    if (!isBrandStatusActive(status)) {
      excluded.push({
        slug,
        brand_name: name,
        reason: "not_active_or_live",
        brand_status: status,
      });
      continue;
    }
    if (!parentMatchesFilter(parent, slug, opts.parentCompany)) {
      excluded.push({
        slug,
        brand_name: name,
        reason: "parent_company_filter",
        parent_company: parent,
        parent_company_raw: inferred.parent_company_raw,
      });
      continue;
    }

    const fam = familyFromParent(parent, slug);
    const aliases = new Set();
    if (name) aliases.add(norm(name));
    if (slug) aliases.add(slug.replace(/-/g, " "));
    if (aliasIndex?.byName) {
      for (const [key, hit] of aliasIndex.byName.entries()) {
        if (hit.slug === slug) aliases.add(key);
      }
    }

    included.push({
      brand_name: name,
      brand_slug: slug,
      parent_company: parent || (fam !== "generic" ? fam : null),
      parent_company_raw: inferred.parent_company_raw,
      parent_inferred: Boolean(inferred.inferred),
      parent_inference_confidence: inferred.inference_confidence,
      parent_inference_source: inferred.inference_source,
      brand_family: fam,
      brand_status: /live/i.test(String(status)) ? "Live" : "Active",
      brand_setup_record_id: recordId,
      region_eligibility: b.regionBasis || region,
      census_matching_aliases: [...aliases],
      extractor_family: fam,
      known_source_family: fam,
    });
  }

  if (opts.includeHeldProbe) {
    const flexSlug = "four-points-flex-by-sheraton";
    if (!excluded.some((e) => e.slug === flexSlug) && !included.some((i) => i.brand_slug === flexSlug)) {
      excluded.push({
        slug: flexSlug,
        brand_name: "Four Points Flex by Sheraton",
        reason: "held_or_not_active_live",
        brand_status: "Under Review",
      });
    }
  }

  const parents = [...new Set(included.map((b) => b.parent_company).filter(Boolean))].sort();
  const inferredParents = included
    .filter((b) => b.parent_inferred)
    .map((b) => ({
      brand_slug: b.brand_slug,
      parent_company: b.parent_company,
      confidence: b.parent_inference_confidence,
      source: b.parent_inference_source,
    }));

  return {
    version: "census-autopilot-active-brand-setup-control-list-v2",
    generated_at: new Date().toISOString(),
    scope: "active-brand-setup",
    region,
    parent_company_filter: opts.parentCompany || null,
    source,
    brand_setup_read_only: true,
    brand_explorer_untouched: true,
    parent_inference_read_only: true,
    active_brands_in_scope: included.length,
    parent_companies_in_scope: parents,
    inferred_parents: inferredParents,
    inferred_parent_count: inferredParents.length,
    brands: included,
    excluded,
    held_excluded_slugs: HELD_EXCLUDED_SLUGS,
  };
}

export function writeActiveBrandSetupControlList(runDir, controlList) {
  fs.mkdirSync(runDir, { recursive: true });
  const fp = path.join(runDir, "active-brand-setup-control-list.json");
  fs.writeFileSync(fp, JSON.stringify(controlList, null, 2), "utf8");
  return fp;
}
