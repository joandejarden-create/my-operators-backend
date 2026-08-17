#!/usr/bin/env node
/**
 * Owned-domain governance + matching regression tests.
 * No Presence changes. No provider calls.
 */
import assert from "node:assert/strict";
import {
  normalizeOwnedDomain,
  hostnameMatchesOwnedDomain,
  classifyCitedDomain,
  resolveOwnedDomainsFromBrandRow,
  listAvailableGovernedDomainFields,
} from "../lib/ai-visibility/owned-domain-resolution.js";
import {
  resolvePortfolioOwnedDomains,
  resolveOwnedDomainsForBrand,
  inferShowcaseCompanyKeyFromBrandIds,
} from "../lib/ai-visibility/brand-website-wiring.js";
import { classifySourceOwnership } from "../lib/ai-visibility/cited-source-intelligence.js";
import {
  computeResponseCitationRates,
  buildSourceExecutivePanel,
} from "../lib/ai-visibility/citation-intelligence.js";
import { buildObservationFromExtracted } from "../lib/ai-visibility/metrics.js";
import { getShowcasePortfolioBrandIds } from "../lib/ai-visibility/brand-ai-showcase-companies.js";

let passed = 0;
let failed = 0;

function check(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}`);
    console.error(err?.stack || err);
  }
}

check("available_governed_fields", () => {
  const fields = listAvailableGovernedDomainFields();
  assert.ok(fields.some((f) => f.field === "Brand Website"));
  assert.ok(fields.some((f) => f.field === "Parent Company Website"));
  assert.ok(fields.some((f) => f.field === "Brand Development URL"));
  assert.ok(fields.some((f) => f.field === "Branded Residences Source URL"));
  assert.ok(fields.some((f) => f.field === "Regional Official URL"));
});

check("brand_owned_domain", () => {
  const row = resolveOwnedDomainsFromBrandRow({
    brandId: "b1",
    brandWebsite: "https://autograph-hotels.marriott.com/",
  });
  assert.ok(row.ownedDomainList.includes("autograph-hotels.marriott.com"));
});

check("parent_company_owned_domain", () => {
  const row = resolveOwnedDomainsFromBrandRow({
    brandId: "b1",
    parentCompanyWebsite: "https://www.marriott.com/",
  });
  assert.ok(row.ownedDomainList.includes("marriott.com"));
});

check("development_domain", () => {
  const row = resolveOwnedDomainsFromBrandRow({
    brandId: "b1",
    brandDevelopmentUrl: "https://www.hotel-development.marriott.com/",
  });
  assert.ok(row.ownedDomainList.includes("hotel-development.marriott.com"));
});

check("residences_domain", () => {
  const row = resolveOwnedDomainsFromBrandRow({
    brandId: "b1",
    brandedResidencesSourceUrl: "https://www.marriottresidences.com/",
  });
  assert.ok(row.ownedDomainList.includes("marriottresidences.com"));
});

check("regional_official_domain", () => {
  const row = resolveOwnedDomainsFromBrandRow({
    brandId: "b1",
    regionalOfficialUrl: "https://www.marriott.com/latin-america/",
  });
  assert.ok(row.ownedDomainList.includes("marriott.com"));
});

check("external_competitor_domain", () => {
  const owned = ["marriott.com", "hotel-development.marriott.com"];
  assert.equal(classifySourceOwnership("stories.hilton.com", owned).type, "THIRD_PARTY");
  assert.equal(classifyCitedDomain("stories.hilton.com", owned).owned, false);
});

check("root_and_www_normalization", () => {
  const n1 = normalizeOwnedDomain("https://WWW.Marriott.com/foo/");
  const n2 = normalizeOwnedDomain("marriott.com");
  assert.equal(n1.hostname, "marriott.com");
  assert.equal(n2.hostname, "marriott.com");
  assert.equal(hostnameMatchesOwnedDomain("https://www.marriott.com/x", "marriott.com"), true);
});

check("path_normalization_ignored_for_host_match", () => {
  assert.equal(
    hostnameMatchesOwnedDomain(
      "https://hotel-development.marriott.com/brands/premium-brands",
      "hotel-development.marriott.com"
    ),
    true
  );
});

check("no_suffix_only_false_positive", () => {
  // marriott.com without inheritSubdomains must NOT own hotel-development.marriott.com
  assert.equal(
    hostnameMatchesOwnedDomain("hotel-development.marriott.com", {
      domain: "marriott.com",
      inheritSubdomains: false,
    }),
    false
  );
  assert.equal(
    classifySourceOwnership("hotel-development.marriott.com", ["marriott.com"]).type,
    "THIRD_PARTY"
  );
});

check("inheritSubdomains_opt_in", () => {
  assert.equal(
    hostnameMatchesOwnedDomain("help.marriott.com", {
      domain: "marriott.com",
      inheritSubdomains: true,
    }),
    true
  );
});

check("similar_name_external_remains_external", () => {
  const owned = ["marriott.com"];
  assert.equal(
    classifySourceOwnership("notmarriott.com", owned).type,
    "THIRD_PARTY"
  );
  assert.equal(
    classifySourceOwnership("marriott-hotels.example.com", owned).type,
    "THIRD_PARTY"
  );
});

check("individual_brand_scope_autograph_fixture", () => {
  const { owned } = resolveOwnedDomainsForBrand("recEJCTDj1zrsjPM6");
  assert.ok(owned.ownedDomainList.includes("autograph-hotels.marriott.com"));
  assert.ok(owned.ownedDomainList.includes("marriott.com"));
  assert.ok(owned.ownedDomainList.includes("hotel-development.marriott.com"));
  assert.ok(owned.ownedDomainList.includes("marriottresidences.com"));
});

check("portfolio_scope_marriott", () => {
  const brandIds = getShowcasePortfolioBrandIds("marriott").brandIds;
  assert.equal(inferShowcaseCompanyKeyFromBrandIds(brandIds), "marriott");
  const portfolio = resolvePortfolioOwnedDomains({ brandIds });
  assert.ok(portfolio.ownedDomainList.includes("marriott.com"));
  assert.ok(portfolio.ownedDomainList.includes("hotel-development.marriott.com"));
  assert.ok(portfolio.ownedDomainList.includes("marriottresidences.com"));
  assert.ok(portfolio.ownedDomainList.includes("autograph-hotels.marriott.com"));
  assert.ok(
    portfolio.DOMAIN_SOURCE_MAPPING.some(
      (m) => m.domain === "hotel-development.marriott.com" && m.field === "Brand Development URL"
    )
  );
});

check("response_both_owned_and_external", () => {
  const owned = resolvePortfolioOwnedDomains({
    brandIds: getShowcasePortfolioBrandIds("marriott").brandIds,
  }).ownedDomainEntries;
  const obs = buildObservationFromExtracted({
    observationId: "ev_both",
    promptId: "p",
    provider: "openai",
    success: true,
    citations: [
      { domain: "hotel-development.marriott.com", url: "https://hotel-development.marriott.com/" },
      { domain: "stories.hilton.com", url: "https://stories.hilton.com/a" },
    ],
  });
  const rates = computeResponseCitationRates([obs], { ownedDomains: owned });
  assert.equal(rates.CITATION_RATE.display, "100%");
  assert.equal(rates.OWNED_SOURCE_CITATION_RATE.display, "100%");
  assert.equal(rates.THIRD_PARTY_CITATION_RATE.display, "100%");
  assert.equal(rates.EXTERNAL_SOURCE_CITATION_RATE.display, "100%");
  assert.equal(rates.RATES_MAY_OVERLAP, true);
});

check("panel_top_owned_external_aliases", () => {
  const owned = resolvePortfolioOwnedDomains({
    brandIds: getShowcasePortfolioBrandIds("marriott").brandIds,
  }).ownedDomainEntries;
  const obs = buildObservationFromExtracted({
    observationId: "ev_top",
    promptId: "p",
    provider: "openai",
    success: true,
    citations: [
      { domain: "hotel-development.marriott.com", url: "https://hotel-development.marriott.com/" },
      { domain: "stories.hilton.com", url: "https://stories.hilton.com/a" },
    ],
  });
  const panel = buildSourceExecutivePanel([obs], { ownedDomains: owned });
  assert.equal(panel.TOP_OWNED_DOMAIN?.domain, "hotel-development.marriott.com");
  assert.equal(panel.TOP_EXTERNAL_DOMAIN?.domain, "stories.hilton.com");
  assert.equal(panel.TOP_THIRD_PARTY_DOMAIN?.domain, "stories.hilton.com");
});

console.log(JSON.stringify({ TOTAL: passed + failed, PASS: passed, FAIL: failed }, null, 2));
if (failed > 0) process.exit(1);
