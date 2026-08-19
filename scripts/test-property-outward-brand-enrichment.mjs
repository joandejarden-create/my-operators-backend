/**
 * Property-outward brand enrichment — unit tests (no live Airtable / crawl).
 */
import test from "node:test";
import assert from "node:assert/strict";

import {
  parseWebsiteDomainIntelligence,
  extractBrandClaimsFromHtml,
  evaluateIndependentEvidence,
  isIdentityHigh,
  runPropertyOutwardDomainIntelligence,
  runPropertyOutwardWebsiteDiscovery,
} from "../lib/research-engine-v2/property-outward-brand-enrichment-v1.js";
import {
  classifyBrandResolution,
  computeBrandResolutionMetrics,
  BRAND_RESOLUTION_CLASS,
} from "../lib/research-engine-v2/brand-resolution-metrics-v1.js";
import {
  rankCompanyAdapterDemand,
  topUnresolvedBrandCompanyDemand,
} from "../lib/research-engine-v2/company-adapter-demand-v1.js";
import {
  migrateAdaptivePhaseStatus,
  ADAPTIVE_PHASES,
  allAvailableResearchModesExhausted,
  RESEARCH_MODE_PHASE_IDS,
} from "../lib/research-engine-v2/adaptive-overnight-engine-v1.js";
import { MAP_MASTER } from "../lib/research-engine-v2/master-census-enrichment-v1.js";
import { MAP_BRAND } from "../lib/research-engine-v2/master-brand-portfolio-validation-v1.js";

test("parseWebsiteDomainIntelligence routes marriott.com to Marriott family", () => {
  const d = parseWebsiteDomainIntelligence("https://www.marriott.com/hotels/travel/miabr-");
  assert.equal(d.ok, true);
  assert.equal(d.official_family, "Marriott");
  assert.equal(d.is_group_domain, true);
});

test("parseWebsiteDomainIntelligence handles boutique owned domain", () => {
  const d = parseWebsiteDomainIntelligence("https://www.casadelmar.mx/");
  assert.equal(d.ok, true);
  assert.equal(d.official_family, null);
  assert.equal(d.is_group_domain, false);
});

test("extractBrandClaimsFromHtml finds Curio affiliation text", () => {
  const claims = extractBrandClaimsFromHtml(
    "<p>Welcome to our Curio Collection by Hilton hotel in Cancun.</p>"
  );
  assert.equal(claims.length, 1);
  assert.match(claims[0].brand, /Curio/i);
});

test("classifyBrandResolution separates branded vs independent vs unresolved", () => {
  assert.equal(
    classifyBrandResolution({ [MAP_MASTER.currentBrand]: "Holiday Inn Express" }),
    BRAND_RESOLUTION_CLASS.BRANDED_VALIDATED
  );
  assert.equal(
    classifyBrandResolution({ "Affiliation Status": "Independent" }),
    BRAND_RESOLUTION_CLASS.INDEPENDENT_VALIDATED
  );
  assert.equal(
    classifyBrandResolution({ [MAP_MASTER.propertyName]: "Hotel X" }),
    BRAND_RESOLUTION_CLASS.BRAND_UNRESOLVED
  );
});

test("computeBrandResolutionMetrics calculates resolution rate", () => {
  const records = [
    { fields: { [MAP_MASTER.currentBrand]: "A" } },
    { fields: { "Affiliation Status": "Independent" } },
    { fields: {} },
    { fields: {} },
  ];
  const m = computeBrandResolutionMetrics(records);
  assert.equal(m.TOTAL_PROPERTIES, 4);
  assert.equal(m.BRANDED_VALIDATED, 1);
  assert.equal(m.INDEPENDENT_VALIDATED, 1);
  assert.equal(m.BRAND_UNRESOLVED, 2);
  assert.equal(m.BRAND_RESOLUTION_RATE, 50);
});

test("rankCompanyAdapterDemand prioritizes IHG website + candidate signals", () => {
  const records = [
    {
      id: "rec1",
      fields: {
        [MAP_MASTER.officialUrl]: "https://www.ihg.com/holidayinn/hotels/us/en/cancun/cunex/hoteldetail",
        [MAP_BRAND.candidateBrand]: "Holiday Inn Express",
        [MAP_MASTER.country]: "Mexico",
        [MAP_MASTER.city]: "Cancun",
      },
    },
    {
      id: "rec2",
      fields: {
        [MAP_MASTER.officialUrl]: "https://www.fourseasons.com/capital/",
        [MAP_MASTER.country]: "Mexico",
      },
    },
  ];
  const ranked = rankCompanyAdapterDemand(records);
  assert.ok(ranked.length >= 1);
  assert.equal(ranked[0].company, "IHG");
  assert.ok(ranked[0].demand_score > 0);
});

test("runPropertyOutwardDomainIntelligence analyzes website-populated blank-brand rows", () => {
  const records = [
    {
      id: "recA",
      fields: {
        [MAP_MASTER.officialUrl]: "https://www.hilton.com/en/hotels/",
        [MAP_MASTER.country]: "Mexico",
        [MAP_MASTER.city]: "Cancun",
      },
    },
    {
      id: "recB",
      fields: {
        [MAP_MASTER.currentBrand]: "Existing Brand",
        [MAP_MASTER.officialUrl]: "https://www.hilton.com/",
      },
    },
  ];
  const res = runPropertyOutwardDomainIntelligence({ censusRecords: records });
  assert.equal(res.WEBSITE_DOMAIN_PROPERTIES_ANALYZED, 1);
  assert.equal(res.OFFICIAL_GROUP_DOMAINS_IDENTIFIED, 1);
  assert.ok(Array.isArray(res.TOP_20_UNRESOLVED_BRAND_COMPANY_DEMAND));
});

test("evaluateIndependentEvidence requires high confidence signals", () => {
  const rec = {
    fields: {
      [MAP_MASTER.propertyName]: "Casa Del Mar",
      [MAP_MASTER.country]: "Mexico",
      [MAP_MASTER.city]: "Cancun",
    },
  };
  const domain = parseWebsiteDomainIntelligence("https://www.casadelmar.mx/");
  const html =
    "<title>Casa Del Mar Cancun</title><p>Independently owned boutique hotel.</p>";
  const ev = evaluateIndependentEvidence(rec, html, domain);
  assert.equal(ev.ok, true);
  assert.equal(ev.confidence, "high");
});

test("migrateAdaptivePhaseStatus maps legacy phase_2_live_directories", () => {
  const migrated = migrateAdaptivePhaseStatus({
    phase_2_live_directories: "EXHAUSTED",
    phase_1_structured: "PLATEAUED",
  });
  assert.equal(migrated.phase_2e_demand_adapters, "EXHAUSTED");
  assert.equal(migrated.phase_2_property_outward_domain, "READY");
  for (const p of ADAPTIVE_PHASES) {
    assert.ok(migrated[p.id] != null, `missing ${p.id}`);
  }
});

test("allAvailableResearchModesExhausted requires all property-outward lanes", () => {
  const partial = Object.fromEntries(
    RESEARCH_MODE_PHASE_IDS.map((id, i) => [id, i === 0 ? "READY" : "EXHAUSTED"])
  );
  assert.equal(allAvailableResearchModesExhausted(partial), false);
  const done = Object.fromEntries(
    RESEARCH_MODE_PHASE_IDS.map((id) => [id, "EXHAUSTED"])
  );
  assert.equal(allAvailableResearchModesExhausted(done), true);
});

test("runPropertyOutwardWebsiteDiscovery matches cached portfolio HIGH identity", () => {
  const records = [
    {
      id: "recMatch",
      fields: {
        [MAP_MASTER.propertyName]: "Holiday Inn Express Cancun",
        [MAP_MASTER.country]: "Mexico",
        [MAP_MASTER.city]: "Cancun",
        [MAP_MASTER.stateRegion]: "Quintana Roo",
      },
    },
  ];
  const res = runPropertyOutwardWebsiteDiscovery({
    censusRecords: records,
    portfolioRows: [
      {
        company: "IHG",
        brand: "Holiday Inn Express",
        name: "Holiday Inn Express Cancun",
        url: "https://www.ihg.com/holidayinnexpress/hotels/us/en/cancun/cuncn/hoteldetail",
        city: "Cancun",
        country: "Mexico",
      },
    ],
  });
  assert.equal(res.WEBSITE_WRITES, 1);
  assert.ok(res.proposals[0].fields[MAP_MASTER.officialUrl]?.includes("ihg.com"));
});

test("isIdentityHigh requires country and city or address", () => {
  assert.equal(
    isIdentityHigh({
      [MAP_MASTER.propertyName]: "Hotel",
      [MAP_MASTER.country]: "Mexico",
      [MAP_MASTER.city]: "Cancun",
    }),
    true
  );
  assert.equal(
    isIdentityHigh({
      [MAP_MASTER.propertyName]: "Hotel",
      [MAP_MASTER.country]: "Mexico",
    }),
    false
  );
});
