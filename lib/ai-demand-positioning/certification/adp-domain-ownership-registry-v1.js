/**
 * Centrally governed brand/portfolio domain ownership registry (global ADP).
 * Used for source attribution — not hotel-specific classification code.
 */

export const ADP_DOMAIN_OWNERSHIP_REGISTRY_VERSION = "adp_domain_ownership_registry_v1";

/** @typedef {'PROPERTY_OWNED'|'BRAND_OWNED'|'COMPETITOR_OWNED'|'GENERAL_MARKET'|'UNKNOWN'} OwnershipType */

/**
 * Canonical brand/portfolio hosts. Property-specific domains live on the identity contract.
 * ownershipType here is BRAND_OWNED when the host is the brand's controlled domain.
 */
export const BRAND_DOMAIN_OWNERSHIP_REGISTRY_V1 = Object.freeze({
  "hilton.com": { brand: "Hilton", portfolio: "Hilton", ownershipType: "BRAND_OWNED" },
  "hilton.com.cn": { brand: "Hilton", portfolio: "Hilton", ownershipType: "BRAND_OWNED" },
  "marriott.com": { brand: "Marriott", portfolio: "Marriott", ownershipType: "BRAND_OWNED" },
  "marriott.co.uk": { brand: "Marriott", portfolio: "Marriott", ownershipType: "BRAND_OWNED" },
  "hyatt.com": { brand: "Hyatt", portfolio: "Hyatt", ownershipType: "BRAND_OWNED" },
  "ihg.com": { brand: "IHG", portfolio: "IHG", ownershipType: "BRAND_OWNED" },
  "ihgplc.com": { brand: "IHG", portfolio: "IHG", ownershipType: "BRAND_OWNED" },
  "choicehotels.com": { brand: "Choice", portfolio: "Choice", ownershipType: "BRAND_OWNED" },
  "wyndhamhotels.com": { brand: "Wyndham", portfolio: "Wyndham", ownershipType: "BRAND_OWNED" },
  "wyndham.com": { brand: "Wyndham", portfolio: "Wyndham", ownershipType: "BRAND_OWNED" },
  "accor.com": { brand: "Accor", portfolio: "Accor", ownershipType: "BRAND_OWNED" },
  "all.accor.com": { brand: "Accor", portfolio: "Accor", ownershipType: "BRAND_OWNED" },
  "radissonhotels.com": { brand: "Radisson", portfolio: "Radisson", ownershipType: "BRAND_OWNED" },
  "radissonhotelsamericas.com": {
    brand: "Radisson",
    portfolio: "Radisson",
    ownershipType: "BRAND_OWNED",
  },
  "bestwestern.com": { brand: "Best Western", portfolio: "Best Western", ownershipType: "BRAND_OWNED" },
  "minorhotels.com": { brand: "Minor", portfolio: "Minor", ownershipType: "BRAND_OWNED" },
  "melia.com": { brand: "Meliá", portfolio: "Meliá", ownershipType: "BRAND_OWNED" },
  "yotel.com": { brand: "YOTEL", portfolio: "YOTEL", ownershipType: "BRAND_OWNED" },
  "whotels.com": { brand: "W Hotels", portfolio: "Marriott", ownershipType: "BRAND_OWNED" },
});

const COMPETITOR_HINT_BRANDS = Object.freeze([
  "hilton",
  "marriott",
  "hyatt",
  "ihg",
  "choice",
  "wyndham",
  "accor",
  "radisson",
  "best western",
  "melia",
  "yotel",
]);

export function normalizeDomainHost(value) {
  return String(value || "")
    .replace(/^https?:\/\//i, "")
    .replace(/^www\./i, "")
    .split("/")[0]
    .trim()
    .toLowerCase();
}

export function lookupBrandDomainOwnership(domain) {
  const host = normalizeDomainHost(domain);
  if (!host) return null;
  if (BRAND_DOMAIN_OWNERSHIP_REGISTRY_V1[host]) {
    return { domain: host, ...BRAND_DOMAIN_OWNERSHIP_REGISTRY_V1[host] };
  }
  // suffix match (e.g. en.hilton.com)
  for (const [reg, meta] of Object.entries(BRAND_DOMAIN_OWNERSHIP_REGISTRY_V1)) {
    if (host === reg || host.endsWith(`.${reg}`)) {
      return { domain: host, registryDomain: reg, ...meta };
    }
  }
  return null;
}

/**
 * Resolve ownership relative to a subject property profile.
 * Competitor brand domains are COMPETITOR_OWNED when they are not the subject's brand.
 */
export function resolveDomainOwnershipForProperty(domain, propertyProfile) {
  const host = normalizeDomainHost(domain);
  if (!host) {
    return { domain: null, ownershipType: "UNKNOWN", brand: null, portfolio: null };
  }

  const owned = new Set(
    [
      propertyProfile?.canonicalPropertyDomain,
      ...(propertyProfile?.ownedDomains || []),
      ...(propertyProfile?.additionalApprovedOwnedDomains || []),
    ]
      .map(normalizeDomainHost)
      .filter(Boolean)
  );
  if (owned.has(host) || [...owned].some((d) => host === d || host.endsWith(`.${d}`))) {
    return {
      domain: host,
      ownershipType: "PROPERTY_OWNED",
      brand: propertyProfile?.brand || propertyProfile?.affiliation || null,
      portfolio: propertyProfile?.parentCompany || null,
      propertyId: propertyProfile?.propertyId || null,
    };
  }

  const brandHost = normalizeDomainHost(propertyProfile?.officialBrandDomain);
  const reg = lookupBrandDomainOwnership(host);
  if (reg) {
    const subjectBrand = String(propertyProfile?.brand || propertyProfile?.affiliation || "")
      .toLowerCase();
    const subjectParent = String(propertyProfile?.parentCompany || "").toLowerCase();
    const regBrand = String(reg.brand || "").toLowerCase();
    const isSubjectBrand =
      (brandHost && (host === brandHost || host.endsWith(`.${brandHost}`))) ||
      (regBrand && (subjectBrand.includes(regBrand) || subjectParent.includes(regBrand)));

    if (isSubjectBrand) {
      return {
        domain: host,
        ownershipType: "BRAND_OWNED",
        brand: reg.brand,
        portfolio: reg.portfolio,
        propertyId: propertyProfile?.propertyId || null,
      };
    }
    return {
      domain: host,
      ownershipType: "COMPETITOR_OWNED",
      brand: reg.brand,
      portfolio: reg.portfolio,
      propertyId: null,
    };
  }

  return {
    domain: host,
    ownershipType: "GENERAL_MARKET",
    brand: null,
    portfolio: null,
    propertyId: null,
  };
}

export function isCompetitorBrandDomainForProperty(domain, propertyProfile) {
  return resolveDomainOwnershipForProperty(domain, propertyProfile).ownershipType === "COMPETITOR_OWNED";
}

export function subjectBrandTokens(propertyProfile) {
  const tokens = [
    propertyProfile?.brand,
    propertyProfile?.affiliation,
    propertyProfile?.parentCompany,
  ]
    .map((x) => String(x || "").toLowerCase())
    .filter(Boolean);
  return { tokens, competitorHints: COMPETITOR_HINT_BRANDS };
}
