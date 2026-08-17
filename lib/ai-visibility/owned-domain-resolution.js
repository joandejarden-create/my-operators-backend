/**
 * Governed owned-domain resolution for Citation Intelligence + Discoverability.
 * Never infer ownership from name similarity alone.
 */

import { parseDomain } from "./extract-citations.js";

export const OWNED_DOMAIN_RESOLUTION_VERSION =
  "ai_visibility_owned_domain_resolution_v2";

export const OWNED_DOMAIN_FIELD_HIERARCHY = Object.freeze([
  {
    tier: "A_BRAND_OFFICIAL",
    field: "Brand Website",
    keys: ["brandWebsite", "Brand Website"],
  },
  {
    tier: "B_PARENT_OFFICIAL",
    field: "Parent Company Website",
    keys: ["parentCompanyWebsite", "Parent Company Website"],
  },
  {
    tier: "C_DEVELOPMENT",
    field: "Brand Development URL",
    keys: ["brandDevelopmentUrl", "Brand Development URL"],
  },
  {
    tier: "C_FRANCHISE",
    field: "Franchise Development URL",
    keys: ["franchiseDevelopmentUrl", "Franchise Development URL"],
  },
  {
    tier: "C_RESIDENCES",
    field: "Branded Residences Source URL",
    keys: ["brandedResidencesSourceUrl", "Branded Residences Source URL"],
  },
  {
    tier: "D_REGIONAL",
    field: "Regional Official URL",
    keys: ["regionalOfficialUrl", "Regional Official URL"],
  },
]);

/**
 * Normalize a URL or hostname to a comparable host key (www stripped).
 */
export function normalizeOwnedDomain(input) {
  if (!input) return null;
  let raw = String(input).trim().toLowerCase();
  if (!raw) return null;
  if (!/^https?:\/\//i.test(raw) && raw.includes(".")) {
    raw = `https://${raw}`;
  }
  let hostname = null;
  let pathname = "/";
  try {
    const u = new URL(raw);
    hostname = u.hostname.replace(/\.$/, "");
    pathname = u.pathname || "/";
  } catch {
    hostname = parseDomain(raw);
    if (!hostname) return null;
  }
  if (!hostname) return null;
  const withoutWww = hostname.startsWith("www.") ? hostname.slice(4) : hostname;
  return {
    hostname: withoutWww,
    rootDomain: withoutWww,
    pathname: pathname.replace(/\/+$/, "") || "/",
    original: String(input).trim(),
  };
}

/**
 * Matching contract:
 * - Exact hostname match after www / case / trailing-dot normalization.
 * - Path is ignored for ownership (official host is the owned unit).
 * - Subdomain inheritance ONLY when owned entry sets inheritSubdomains: true.
 * - Never treat arbitrary *.parent.com as owned from parent.com alone by default.
 */
export function hostnameMatchesOwnedDomain(citedInput, ownedEntry) {
  const cited = normalizeOwnedDomain(citedInput);
  if (!cited) return false;

  const entry =
    typeof ownedEntry === "string"
      ? { domain: ownedEntry, inheritSubdomains: false }
      : ownedEntry || {};
  const owned = normalizeOwnedDomain(entry.domain || entry.hostname || entry.url);
  if (!owned) return false;

  if (cited.hostname === owned.hostname) return true;
  if (entry.inheritSubdomains === true) {
    return cited.hostname.endsWith(`.${owned.hostname}`);
  }
  return false;
}

/**
 * Resolve owned domains from Brand Matrix / Brand Basics governed fields.
 * Hierarchy: brand site → parent → development/franchise → residences → regional.
 */
export function resolveOwnedDomainsFromBrandRow(brandRow = {}) {
  const sources = [];
  const push = (url, tier, field, inheritSubdomains = false) => {
    const n = normalizeOwnedDomain(url);
    if (!n) return;
    sources.push({
      url: url || n.original,
      domain: n.rootDomain,
      hostname: n.hostname,
      pathname: n.pathname,
      tier,
      field,
      inheritSubdomains: inheritSubdomains === true,
      GOVERNED: true,
    });
  };

  for (const spec of OWNED_DOMAIN_FIELD_HIERARCHY) {
    let url = null;
    for (const key of spec.keys) {
      if (brandRow[key]) {
        url = brandRow[key];
        break;
      }
    }
    if (url) push(url, spec.tier, spec.field, false);
  }

  const byDomain = new Map();
  for (const s of sources) {
    if (!byDomain.has(s.domain)) byDomain.set(s.domain, s);
  }
  const domains = [...byDomain.values()];

  return {
    version: OWNED_DOMAIN_RESOLUTION_VERSION,
    brandId: brandRow.brandId || brandRow.id || null,
    domains,
    ownedDomainList: domains.map((d) => d.domain),
    OWNED_DOMAIN_STATUS: domains.length
      ? "CONFIGURED"
      : "MISSING_GOVERNED_SOURCE",
    FABRICATED_URLS: 0,
    INFERRED_FROM_NAME_SIMILARITY: false,
    SUFFIX_ONLY_INFERENCE: false,
    ROOT_DOMAIN_RULE:
      "Exact host match after www-strip; path ignored; no automatic sibling subdomain ownership.",
    SUBDOMAIN_RULE:
      "Subdomains count as owned only when inheritSubdomains:true is explicitly set on the governed entry.",
  };
}

/**
 * Build owned-domain index for many brands; count missing governance.
 */
export function buildOwnedDomainIndex(brandRows = []) {
  const byBrandId = {};
  let configured = 0;
  let missing = 0;
  const missingBrands = [];

  for (const row of brandRows) {
    const resolved = resolveOwnedDomainsFromBrandRow(row);
    const id = resolved.brandId || row.brandId;
    if (!id) continue;
    byBrandId[id] = resolved;
    if (resolved.OWNED_DOMAIN_STATUS === "CONFIGURED") configured += 1;
    else {
      missing += 1;
      missingBrands.push({
        brandId: id,
        brandName: row.brandName || row["Brand Name"] || null,
      });
    }
  }

  return {
    version: OWNED_DOMAIN_RESOLUTION_VERSION,
    ELIGIBLE_BRANDS: configured,
    MISSING_GOVERNED_SOURCE: missing,
    missingBrands,
    byBrandId,
  };
}

/**
 * Classify a cited hostname against owned domain list (strings or governed entries).
 */
export function classifyCitedDomain(domain, ownedDomainList = []) {
  const n = normalizeOwnedDomain(domain);
  if (!n) return { type: "UNKNOWN", owned: false };
  for (const owned of ownedDomainList) {
    if (hostnameMatchesOwnedDomain(n.hostname, owned)) {
      const o = normalizeOwnedDomain(
        typeof owned === "string" ? owned : owned.domain || owned.hostname || owned.url
      );
      return {
        type: n.hostname === o?.hostname ? "OWNED_ROOT" : "OWNED_SUBDOMAIN",
        owned: true,
        ownedRoot: o?.hostname || null,
      };
    }
  }
  return { type: "THIRD_PARTY", owned: false };
}

export function listAvailableGovernedDomainFields() {
  return OWNED_DOMAIN_FIELD_HIERARCHY.map((f) => ({
    tier: f.tier,
    field: f.field,
    keys: f.keys,
  }));
}
