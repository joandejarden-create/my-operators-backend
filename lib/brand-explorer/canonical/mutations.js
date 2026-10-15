/**
 * Cumulative mutation suite. Each case breaks one rule on an otherwise passing profile.
 */
import { CANONICAL_TAB_IDS, COLLECTION_CONTRACT, PRESENTATION_CONTRACT, CONTENT_CONTRACT } from "./product-shell.js";
import { evaluateMasterRelease } from "./master-release.js";
import { evaluateLessons } from "./lessons.js";

function list(n, map) {
  return Array.from({ length: n }, (_, i) => map(i + 1));
}

export function buildPassingProfile() {
  const body = "Owners can use this brand when the asset, market, and operating model fit the stated proposition.";
  const items = CONTENT_CONTRACT.staticRequired.map((item) => ({
    ...item,
    disposition: item.itemId === "standards.residences" ? "APPLICABLE_POPULATED_NEGATIVE" : "APPLICABLE_POPULATED_POSITIVE",
    body: item.itemId === "standards.residences" ? "Branded residences are not offered." : item.purpose === "Flexibility" ? "Moderate" : body,
    sourceIds: ["src-official"],
  }));
  const regions = PRESENTATION_CONTRACT.geography.regions.map((region) => ({ label: region.label, slot: region.slot }));
  const flexibility = PRESENTATION_CONTRACT.flexibility.indicators.map((row) => ({ slot: row.slot, level: "Moderate" }));
  const collections = {};
  for (const spec of COLLECTION_CONTRACT.collections) {
    if (spec.mode === "CONDITIONAL") {
      collections[spec.id] = [];
      continue;
    }
    const n = spec.count || 0;
    if (spec.id === "flexibility") collections[spec.id] = flexibility;
    else if (spec.id === "geographicRegions") collections[spec.id] = regions;
    else if (spec.id === "recentMomentum") {
      collections[spec.id] = list(n, (i) => ({ title: `Opening ${i}`, url: `https://example.com/momentum-${i}` }));
    } else if (spec.id === "representativeProperties") {
      collections[spec.id] = list(n, (i) => ({ name: `Property ${i}` }));
    } else collections[spec.id] = list(n, (i) => ({ id: `${spec.id}-${i}` }));
  }
  return {
    identity: { brandName: "Canonical Fixture", slug: "canonical-fixture", brandStatus: "Under Review" },
    foundation: { buildBriefPresent: true },
    tabIds: [...CANONICAL_TAB_IDS],
    items,
    collections,
    conditionals: { conventionalLoyaltyProgram: false },
    regions,
    properties: list(3, (i) => ({
      name: `Property ${i}`,
      identities: [`Property ${i}`],
      imageEvidence: "VERIFIED_EXACT_PROPERTY",
      sameBrandDonor: false,
    })),
    metrics: [
      { id: "open_hotels", value: 2, unit: "hotels", scope: "global open", evidenceState: "KNOWN_VALUE" },
      { id: "pipeline", value: null, unit: null, scope: null, evidenceState: "NOT_PUBLICLY_DISCLOSED" },
    ],
    facts: [
      { id: "brand_name", value: "Canonical Fixture" },
      { id: "residences", value: "not_offered" },
    ],
    geometry: {
      overviewScenarios: { columns: 3, partialWidth: false },
      bestAt: { columns: 3, partialWidth: false },
      ownerOutcomes: { columns: 2, partialWidth: false },
      flexibility: { partialWidth: false },
      geographicRegions: { partialWidth: false },
      representativeProperties: { partialWidth: false },
      brandMaterials: { columns: 3, partialWidth: false },
    },
    liveValidation: {
      pid: 4242,
      buildSignature: "fixture-build",
      expected: 10,
      activated: 10,
      measurable: 10,
      audited: 10,
      serverRestarted: false,
    },
    reconciliation: { planned: 10, written: 10, rendered: 10 },
    masterReleasePass: true,
  };
}

function withMutation(name, mutate) {
  const profile = buildPassingProfile();
  mutate(profile);
  return { name, profile };
}

export function mutationCases() {
  return [
    withMutation("empty_item", (p) => {
      p.items.find((i) => i.itemId === "overview.scenario.1").body = "";
    }),
    withMutation("thin_item", (p) => {
      p.items.find((i) => i.itemId === "overview.scenario.1").body = "Short line";
    }),
    withMutation("generic_template", (p) => {
      p.items.find((i) => i.itemId === "overview.scenario.1").body = "This source pack is only a match by property name.";
    }),
    withMutation("invalid_suppression", (p) => {
      p.items.push({ itemId: "standards.residences.detail", disposition: "DEPENDENT_SUPPRESSED_JUSTIFIED", body: "", sourceIds: ["src-official"] });
    }),
    withMutation("hidden_negative", (p) => {
      const row = p.items.find((i) => i.itemId === "standards.residences");
      row.disposition = "DEPENDENT_SUPPRESSED_JUSTIFIED";
      row.body = "";
    }),
    withMutation("collection_underpopulation", (p) => {
      p.collections.bestAt = p.collections.bestAt.slice(0, 1);
    }),
    withMutation("missing_loyalty_tier", (p) => {
      p.conditionals.conventionalLoyaltyProgram = true;
      p.sourceMemberCounts = { loyaltyTiers: 3 };
      p.collections.loyaltyTiers = [{ name: "Member" }];
      p.collections.loyaltyBenefits = [{ name: "Wifi" }, { name: "Late checkout" }, { name: "Upgrade" }];
    }),
    withMutation("wrong_geography_label", (p) => {
      p.regions[0] = { label: "Europe / other international" };
      p.collections.geographicRegions[0] = p.regions[0];
    }),
    withMutation("wrong_grid_cardinality", (p) => {
      p.geometry.bestAt.columns = 1;
    }),
    withMutation("partial_width_geometry", (p) => {
      p.geometry.bestAt.partialWidth = true;
    }),
    withMutation("property_multi_identity", (p) => {
      p.properties[0].identities = ["Hotel A", "Hotel B"];
    }),
    withMutation("property_image_mismatch", (p) => {
      p.properties[0].imageEvidence = "VERIFIED_WRONG_PROPERTY";
    }),
    withMutation("same_brand_donor", (p) => {
      p.properties[0].sameBrandDonor = true;
    }),
    withMutation("blank_metric", (p) => {
      p.metrics.push({ id: "markets", value: "", evidenceState: "" });
    }),
    withMutation("missing_metric_scope", (p) => {
      p.metrics[0].unit = "";
      p.metrics[0].scope = "";
    }),
    withMutation("invalid_zero", (p) => {
      p.metrics[0].value = 0;
      p.metrics[0].evidenceState = "KNOWN_VALUE";
    }),
    withMutation("fact_contradiction", (p) => {
      p.facts.push({ id: "brand_name", value: "Other Name" });
    }),
    withMutation("interrupted_live_validation", (p) => {
      p.liveValidation.serverRestarted = true;
    }),
    withMutation("subsystem_ready_bypass", (p) => {
      p.readyForFounderReview = true;
      p.masterReleasePass = false;
    }),
    withMutation("noncanonical_flexibility_level", (p) => {
      p.collections.flexibility[0].level = "Somewhat flexible";
    }),
    withMutation("momentum_missing_url", (p) => {
      p.collections.recentMomentum[0].url = "";
    }),
    withMutation("tab_parity_drift", (p) => {
      p.tabIds = CANONICAL_TAB_IDS.filter((id) => id !== "atelier-loyalty");
    }),
  ];
}

export function runMutationSuite() {
  const baseline = evaluateMasterRelease(buildPassingProfile());
  const cases = mutationCases().map((row) => {
    const findings = evaluateLessons(row.profile);
    const hit = findings.some((f) => f.validator === row.name || f.defectClass === row.name);
    return { name: row.name, detected: hit, findingCount: findings.length };
  });
  const failed = cases.filter((row) => !row.detected);
  return {
    baselinePass: baseline.MASTER_RELEASE_CONTRACT_PASS === true,
    baselineStatus: baseline.status,
    cases,
    pass: baseline.MASTER_RELEASE_CONTRACT_PASS === true && failed.length === 0,
    failed: failed.map((row) => row.name),
  };
}
