/**
 * Load Brand AI Visibility entitlements for an authenticated Dealality user.
 * Record-link sources only — no name/parent inference.
 */

import Airtable from "airtable";
import { extractLinkedRecordIds, cellToString } from "../airtable-utils.js";
import {
  buildFixtureEntitlementGraph,
  MAP_AI_VISIBILITY_ENTITLEMENT,
  resolveEntitledBrands,
} from "./entitlements.js";
import { loadPeerSetConfig, resolvePeerSetMembership } from "./peer-sets.js";

const USERS_TABLE = process.env.AIRTABLE_ME_USERS_TABLE || "Users";
const COMPANY_TABLE =
  process.env.AIRTABLE_COMPANY_PROFILE_TABLE || "Company Profile";
const BRAND_BASICS =
  process.env.AIRTABLE_BRAND_SETUP_BASICS_TABLE || "Brand Setup - Brand Basics";
const BRAND_NAME_FIELD = process.env.AIRTABLE_BRAND_NAME_FIELD || "Brand Name";
const BRAND_WEBSITE_FIELD =
  process.env.AIRTABLE_BRAND_WEBSITE_FIELD || "Brand Website";
const PARENT_COMPANY_WEBSITE_FIELD =
  process.env.AIRTABLE_PARENT_COMPANY_WEBSITE_FIELD || "Parent Company Website";
const BRANDED_RESIDENCES_URL_FIELD =
  process.env.AIRTABLE_BRANDED_RESIDENCES_SOURCE_URL_FIELD ||
  "Branded Residences Source URL";
const BRAND_DEVELOPMENT_URL_FIELD =
  process.env.AIRTABLE_BRAND_DEVELOPMENT_URL_FIELD || "Brand Development URL";
const FRANCHISE_DEVELOPMENT_URL_FIELD =
  process.env.AIRTABLE_FRANCHISE_DEVELOPMENT_URL_FIELD ||
  "Franchise Development URL";
const REGIONAL_OFFICIAL_URL_FIELD =
  process.env.AIRTABLE_REGIONAL_OFFICIAL_URL_FIELD || "Regional Official URL";

function getBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    const err = new Error("Airtable not configured");
    err.code = "server_misconfigured";
    throw err;
  }
  return new Airtable({ apiKey }).base(baseId);
}

/**
 * @param {object} dealalityUser
 * @param {{ entitlementGraph?: object, peerSetId?: string, commercialRegion?: string }} [options]
 */
export async function loadBrandViewerEntitlements(dealalityUser, options = {}) {
  if (options.entitlementGraph) {
    return {
      ok: true,
      entitlementGraph: options.entitlementGraph,
      brandNamesById: options.brandNamesById || {},
      source: "injected",
    };
  }

  // Test/dev injection via env JSON (never use in production paths without explicit flag)
  if (
    process.env.AI_VISIBILITY_ENTITLEMENT_FIXTURE &&
    process.env.AI_VISIBILITY_ALLOW_ENTITLEMENT_FIXTURE === "true"
  ) {
    const parsed = JSON.parse(process.env.AI_VISIBILITY_ENTITLEMENT_FIXTURE);
    return {
      ok: true,
      entitlementGraph: buildFixtureEntitlementGraph(parsed),
      brandNamesById: parsed.brandNamesById || {},
      source: "env_fixture",
    };
  }

  const base = getBase();
  let companyFields = null;
  let userFields = null;

  if (dealalityUser?.userRecordId) {
    try {
      const userRec = await base(USERS_TABLE).find(dealalityUser.userRecordId);
      userFields = userRec.fields || {};
    } catch (err) {
      if (process.env.NODE_ENV !== "production") {
        console.warn("[ai-visibility] users fetch failed:", err.message);
      }
    }
  }

  const companyId = dealalityUser?.companyId;
  if (companyId) {
    try {
      const companyRec = await base(COMPANY_TABLE).find(companyId);
      companyFields = companyRec.fields || {};
    } catch (err) {
      if (process.env.NODE_ENV !== "production") {
        console.warn("[ai-visibility] company profile fetch failed:", err.message);
      }
    }
  }

  const brands = resolveEntitledBrands({
    viewerContext: {
      viewerCompanyId: companyId,
    },
    companyFields,
    userFields,
  });

  const peerSetId = options.peerSetId || "peers_upper_upscale_brands_global_v1";
  const membership = resolvePeerSetMembership({
    peerSetId,
    commercialRegion: options.commercialRegion || null,
  });

  const entitlementGraph = buildFixtureEntitlementGraph({
    entitledBrandIds: brands.brandIds || [],
    peerBrandIds: membership.entityIds || [],
    source: brands.source || "live_links",
  });

  const brandMeta = await fetchBrandBasicsMeta(base, [
    ...new Set([...(brands.brandIds || []), ...(membership.entityIds || [])]),
  ]);

  return {
    ok: true,
    entitlementGraph,
    brandNamesById: brandMeta.brandNamesById,
    brandBasicsById: brandMeta.brandBasicsById,
    ownedDomainCoverage: brandMeta.ownedDomainCoverage,
    source: brands.source,
    textInferenceUsed: false,
    AIRTABLE_WRITES: 0,
  };
}

/**
 * Fetch Brand Name + governed website fields for owned-domain / Discoverability wiring.
 * Never invents URLs. Missing website → MISSING_GOVERNED_SOURCE downstream.
 */
async function fetchBrandBasicsMeta(base, ids) {
  const brandNamesById = {};
  const brandBasicsById = {};
  const unique = [...new Set((ids || []).filter((id) => typeof id === "string" && id.startsWith("rec")))];
  const batchSize = 8;

  // Core fields only — optional parent/residences/development fields may not exist in every base.
  const coreFields = [BRAND_NAME_FIELD, BRAND_WEBSITE_FIELD];
  const optionalFields = [
    PARENT_COMPANY_WEBSITE_FIELD,
    BRANDED_RESIDENCES_URL_FIELD,
    BRAND_DEVELOPMENT_URL_FIELD,
    FRANCHISE_DEVELOPMENT_URL_FIELD,
    REGIONAL_OFFICIAL_URL_FIELD,
  ].filter((f) => f && !coreFields.includes(f));

  for (let i = 0; i < unique.length; i += batchSize) {
    const chunk = unique.slice(i, i + batchSize);
    const orParts = chunk.map((id) => `RECORD_ID() = '${id.replace(/'/g, "\\'")}'`);
    const formula = orParts.length === 1 ? orParts[0] : `OR(${orParts.join(",")})`;

    let rows = [];
    try {
      rows = await base(BRAND_BASICS)
        .select({
          filterByFormula: formula,
          maxRecords: chunk.length,
          fields: [...coreFields, ...optionalFields],
        })
        .firstPage();
    } catch (err) {
      const msg = String(err?.message || err);
      if (/unknown field name/i.test(msg)) {
        if (process.env.NODE_ENV !== "production") {
          console.warn(
            "[ai-visibility] brand basics optional fields unavailable; retrying with Brand Name + Brand Website only:",
            msg
          );
        }
        try {
          rows = await base(BRAND_BASICS)
            .select({
              filterByFormula: formula,
              maxRecords: chunk.length,
              fields: coreFields,
            })
            .firstPage();
        } catch (err2) {
          if (process.env.NODE_ENV !== "production") {
            console.warn("[ai-visibility] brand basics core fetch failed:", err2.message);
          }
          try {
            rows = await base(BRAND_BASICS)
              .select({ filterByFormula: formula, maxRecords: chunk.length })
              .firstPage();
          } catch (err3) {
            if (process.env.NODE_ENV !== "production") {
              console.warn("[ai-visibility] brand basics unfiltered fetch failed:", err3.message);
            }
            rows = [];
          }
        }
      } else {
        if (process.env.NODE_ENV !== "production") {
          console.warn("[ai-visibility] brand basics fetch failed:", msg);
        }
        try {
          rows = await base(BRAND_BASICS)
            .select({ filterByFormula: formula, maxRecords: chunk.length })
            .firstPage();
        } catch (err2) {
          if (process.env.NODE_ENV !== "production") {
            console.warn("[ai-visibility] brand name fallback failed:", err2.message);
          }
          rows = [];
        }
      }
    }

    for (const row of rows) {
      const name = cellToString(row.fields?.[BRAND_NAME_FIELD]) || null;
      const brandWebsite =
        cellToString(row.fields?.[BRAND_WEBSITE_FIELD]) ||
        cellToString(row.fields?.["Brand Website"]) ||
        null;
      const parentCompanyWebsite =
        cellToString(row.fields?.[PARENT_COMPANY_WEBSITE_FIELD]) ||
        cellToString(row.fields?.["Parent Company Website"]) ||
        null;
      const brandedResidencesSourceUrl =
        cellToString(row.fields?.[BRANDED_RESIDENCES_URL_FIELD]) ||
        cellToString(row.fields?.["Branded Residences Source URL"]) ||
        null;
      const brandDevelopmentUrl =
        cellToString(row.fields?.[BRAND_DEVELOPMENT_URL_FIELD]) ||
        cellToString(row.fields?.["Brand Development URL"]) ||
        null;
      const franchiseDevelopmentUrl =
        cellToString(row.fields?.[FRANCHISE_DEVELOPMENT_URL_FIELD]) ||
        cellToString(row.fields?.["Franchise Development URL"]) ||
        null;
      const regionalOfficialUrl =
        cellToString(row.fields?.[REGIONAL_OFFICIAL_URL_FIELD]) ||
        cellToString(row.fields?.["Regional Official URL"]) ||
        null;
      brandNamesById[row.id] = name;
      if (brandWebsite) brandNamesById[`website:${row.id}`] = brandWebsite;
      brandBasicsById[row.id] = {
        brandId: row.id,
        brandName: name,
        brandWebsite,
        parentCompanyWebsite,
        brandedResidencesSourceUrl,
        brandDevelopmentUrl,
        franchiseDevelopmentUrl,
        regionalOfficialUrl,
      };
    }
  }

  let configured = 0;
  let missing = 0;
  const missingBrandIds = [];
  for (const id of unique) {
    const row = brandBasicsById[id];
    if (row?.brandWebsite) configured += 1;
    else {
      missing += 1;
      missingBrandIds.push(id);
    }
  }

  return {
    brandNamesById,
    brandBasicsById,
    ownedDomainCoverage: {
      ELIGIBLE_WITH_WEBSITE: configured,
      MISSING_GOVERNED_SOURCE: missing,
      missingBrandIds,
      FABRICATED_URLS: 0,
    },
  };
}

/** Public helper for demo/showcase entitlement paths that skip full company-link resolution. */
export async function fetchBrandBasicsMetaForIds(ids = []) {
  const base = getBase();
  return fetchBrandBasicsMeta(base, ids);
}

export { MAP_AI_VISIBILITY_ENTITLEMENT, extractLinkedRecordIds };
